//! Owned native-source HEVC playback; no vendor process or external service.
use bytes::Bytes;
use flussonix::{
    m4f::Frame,
    m4s::Track,
    rtsp,
    server::{App, Options},
    wire,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
#[path = "support/tls.rs"]
mod tls_fixture;
#[path = "support/udp.rs"]
mod udp_fixture;
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
struct Lab {
    _dir: tempfile::TempDir,
    cert: tls_fixture::Certificates,
    app: Arc<App>,
    url: String,
    cancel: CancellationToken,
    tasks: Vec<AbortOnDropHandle<()>>,
    hits: Arc<AtomicUsize>,
    expected: HashSet<String>,
    audio: bool,
    pool: Option<Arc<rtsp::udp::Pool>>,
    audio_expected: std::collections::HashMap<u32, Vec<f64>>,
}
async fn fixture_profile(transport: &str, video: bool, audio: &str) -> Lab {
    let selected = [audio];
    fixture_tracks(
        transport,
        video,
        if audio == "none" { &[] } else { &selected },
    )
    .await
}
async fn fixture_tracks(transport: &str, video: bool, audios: &[&str]) -> Lab {
    let dir = tempfile::tempdir().unwrap();
    let cert = tls_fixture::Certificates::new();
    let mut tracks = vec![Track {
        id: 205,
        codec: "hevc".into(),
        config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
    }];
    let timing: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    let mut frames = vec![];
    let mut elementary = flussonix::hevc::Configuration::parse(&tracks[0].config)
        .unwrap()
        .annex_b();
    for cycle in 0..40u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            let body = std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap();
            if cycle == 0 {
                elementary.extend(
                    flussonix::hevc::Configuration::parse(&tracks[0].config)
                        .unwrap()
                        .access_unit(&body)
                        .unwrap()
                        .annex_b,
                );
            }
            frames.push(Frame {
                track_id: 205,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body,
            });
        }
    }
    let file = dir.path().join("expected.hevc");
    std::fs::write(&file, elementary).unwrap();
    let decoded = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(file)
        .args(["-threads", "1", "-f", "framemd5", "-"])
        .output()
        .await
        .unwrap();
    assert!(decoded.status.success() && decoded.stderr.is_empty());
    let expected = hashes(&decoded.stdout, 0);
    assert_eq!(expected.len(), 12);
    let expected = if video {
        expected
    } else {
        tracks.clear();
        frames.clear();
        HashSet::new()
    };
    let mut audio_expected = std::collections::HashMap::new();
    for (index, audio) in audios.iter().copied().enumerate() {
        let id = 88 + index as u32 * 19;
        let tone = format!("sine=frequency={}:sample_rate=48000", 880 + index * 137);
        if audio == "aac" {
            // Independently encode a continuous AAC source, then retain its ADTS payloads.
            let encoded = tokio::process::Command::new("ffmpeg")
                .args([
                    "-v", "error", "-f", "lavfi", "-i", &tone, "-t", "20", "-c:a", "aac", "-f",
                    "adts", "pipe:1",
                ])
                .output()
                .await
                .unwrap();
            assert!(encoded.status.success());
            if audios.len() > 1 {
                audio_expected.insert(
                    id,
                    audio_oracle(dir.path(), id, "aac", &encoded.stdout).await,
                );
            }
            tracks.push(Track {
                id,
                codec: "aac".into(),
                config: vec![0x11, 0x88],
            });
            let mut at = 0;
            let mut n = 0;
            while at < encoded.stdout.len() {
                let h = &encoded.stdout[at..];
                let size = (usize::from(h[3] & 3) << 11)
                    | (usize::from(h[4]) << 3)
                    | usize::from(h[5] >> 5);
                frames.push(Frame {
                    track_id: id,
                    dts: 90000 + n * 1920,
                    pts_offset: 0,
                    key: true,
                    body: h[7..size].to_vec(),
                });
                at += size;
                n += 1;
            }
        } else if audio != "none" {
            let (codec, encoder, rate, kbps) = match audio {
                "m2a" => ("m2a", "mp2", 32000u64, 384u64),
                "mp3" => ("mp3", "libmp3lame", 22050, 64),
                "mp3-mpeg1" => ("mp3", "libmp3lame", 32000, 320),
                _ => panic!("unknown owned audio profile"),
            };
            let mut cmd = tokio::process::Command::new("ffmpeg");
            cmd.args([
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                &tone,
                "-t",
                "20",
                "-c:a",
                encoder,
                "-ar",
                &rate.to_string(),
                "-ac",
                "2",
                "-b:a",
                &format!("{kbps}k"),
                "-f",
                if codec == "m2a" { "mp2" } else { "mp3" },
            ]);
            if codec == "mp3" {
                cmd.args([
                    "-write_xing",
                    "0",
                    "-id3v2_version",
                    "0",
                    "-write_id3v1",
                    "0",
                ]);
            }
            cmd.arg("pipe:1");
            let encoded = cmd.output().await.unwrap();
            assert!(
                encoded.status.success(),
                "{}",
                String::from_utf8_lossy(&encoded.stderr)
            );
            if audios.len() > 1 {
                audio_expected.insert(
                    id,
                    audio_oracle(dir.path(), id, "mp3", &encoded.stdout).await,
                );
            }
            tracks.push(Track {
                id,
                codec: codec.into(),
                config: vec![],
            });
            let mut at = 0;
            let mut n = 0u64;
            while at < encoded.stdout.len() {
                let h = &encoded.stdout[at..];
                assert_eq!(h[0], 255);
                let version = (h[1] >> 3) & 3;
                let samples = if codec == "mp3" && version != 3 {
                    576
                } else {
                    1152
                };
                let size = (samples / 8 * kbps * 1000 / rate + u64::from((h[2] >> 1) & 1)) as usize;
                frames.push(Frame {
                    track_id: id,
                    dts: 90000 + n * samples * 90000 / rate,
                    pts_offset: 0,
                    key: true,
                    body: h[..size].to_vec(),
                });
                at += size;
                n += 1;
            }
        }
    }
    tracks.reverse(); // Nonsequential audio IDs precede video metadata.
    frames.sort_by_key(|f| f.dts);
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let tracks = Arc::new(tracks);
    let frames = Arc::new(frames);
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let input = format!(
        "m4s://{}/owned?token=owned-source",
        source.local_addr().unwrap()
    );
    let routes=axum::Router::new().route("/owned/m4s",axum::routing::get(move |axum::extract::Query(q):axum::extract::Query<std::collections::HashMap<String,String>>| {
        let stop=stop.clone();let tracks=tracks.clone();let frames=frames.clone();let count=count.clone();async move {
            assert_eq!(q.get("token").map(String::as_str),Some("owned-source"));count.fetch_add(1,Ordering::SeqCst);
            let info=Bytes::from(wire::encode_info(&tracks).unwrap());let start=tokio::time::Instant::now();
            axum::body::Body::from_stream(futures_util::stream::unfold((Some(info),0usize,tracks,frames,stop,start),|(mut info,index,tracks,frames,stop,start)|async move {
                if stop.is_cancelled(){return None;}let metadata=info.is_some();let bytes=if let Some(info)=info.take(){info}else {let f=frames.get(index)?;tokio::select!{biased;_=stop.cancelled()=>return None,_=tokio::time::sleep_until(start+Duration::from_micros((f.dts-90000)*1000000/90000))=>{}}Bytes::from(wire::encode_frame(tracks.iter().find(|t|t.id==f.track_id).unwrap(),f).unwrap())};let next=index+usize::from(!metadata);
                Some((Ok::<_,std::io::Error>(bytes),(info,next,tracks,frames,stop,start)))
            }))
        }
    }));
    let source_stop = cancel.clone();
    let source_task = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(source, routes)
            .with_graceful_shutdown(source_stop.cancelled_owned())
            .await
            .unwrap()
    }));
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":input}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "{}://{}/owned",
        if transport == "tls" { "rtsps" } else { "rtsp" },
        listener.local_addr().unwrap()
    );
    let pool = if transport == "udp" {
        let mut pool = None;
        for _ in 0..8 {
            let (range, held) =
                udp_fixture::reserved(2 * (audios.len() + usize::from(video)) as u16);
            drop(held);
            match rtsp::udp::Pool::bind("127.0.0.1".parse().unwrap(), range).await {
                Ok(p) => {
                    pool = Some(p);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {}
                Err(e) => panic!("{e}"),
            }
        }
        Some(pool.expect("owned UDP pool"))
    } else {
        None
    };
    let a = app.clone();
    let c = cancel.clone();
    let tls = cert.server();
    let tls_mode = transport == "tls";
    let served_pool = pool.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        if tls_mode {
            rtsp::serve_tls(listener, a, c, tls).await.unwrap()
        } else {
            rtsp::serve_with_udp(listener, a, c, served_pool, 100.0)
                .await
                .unwrap()
        }
    }));
    Lab {
        _dir: dir,
        cert,
        app,
        url,
        cancel,
        tasks: vec![source_task, task],
        hits,
        expected,
        audio: !audios.is_empty(),
        pool,
        audio_expected,
    }
}
async fn audio_oracle(dir: &std::path::Path, id: u32, format: &str, bytes: &[u8]) -> Vec<f64> {
    let file = dir.join(format!("audio-{id}.{format}"));
    std::fs::write(&file, bytes).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-f", format, "-i"])
            .arg(&file)
            .args([
                "-t",
                "2",
                "-threads",
                "1",
                "-ac",
                "1",
                "-ar",
                "48000",
                "-c:a",
                "pcm_s16le",
                "-f",
                "s16le",
                "-",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "independent source audio decode {id}/{format}: {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let signature = tone_signature(&output.stdout);
    let index = ((id - 88) / 19) as usize;
    assert!(signature[index] > 0.8, "source tone {id}: {signature:?}");
    for (other, energy) in signature.iter().enumerate() {
        if other != index {
            assert!(*energy < 0.02, "source tone identity {id}: {signature:?}");
        }
    }
    signature
}
// Project a half-second window of mono 48 kHz PCM onto the fixture's distinct
// source frequencies. Phase-independent energy tolerates late-join decoder
// history, while rejecting silence, duplicated tracks and swapped native IDs.
fn tone_signature(bytes: &[u8]) -> Vec<f64> {
    assert_eq!(bytes.len() % 2, 0);
    let samples: Vec<f64> = bytes
        .chunks_exact(2)
        .map(|sample| f64::from(i16::from_le_bytes([sample[0], sample[1]])))
        .skip(4096)
        .take(24000)
        .collect();
    assert_eq!(samples.len(), 24000, "decoded tone window");
    let power: f64 = samples.iter().map(|sample| sample * sample).sum();
    assert!(power > 1.0, "decoded tone is silent");
    (0..8)
        .map(|index| {
            let step = std::f64::consts::TAU * (880 + index * 137) as f64 / 48000.0;
            let (real, imaginary) =
                samples
                    .iter()
                    .enumerate()
                    .fold((0.0, 0.0), |(real, imaginary), (n, sample)| {
                        let phase = n as f64 * step;
                        (
                            real + sample * phase.cos(),
                            imaginary + sample * phase.sin(),
                        )
                    });
            2.0 * (real * real + imaginary * imaginary) / (samples.len() as f64 * power)
        })
        .collect()
}

fn hashes(data: &[u8], id: usize) -> HashSet<String> {
    String::from_utf8_lossy(data)
        .lines()
        .filter(|l| l.starts_with(&format!("{id},")))
        .map(|l| l.rsplit(',').next().unwrap().trim().to_owned())
        .collect()
}
async fn connect(lab: &Lab) -> BufReader<Box<dyn Io>> {
    let u = url::Url::parse(&lab.url).unwrap();
    let tcp = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    let socket: Box<dyn Io> = if u.scheme() == "rtsps" {
        Box::new(
            tokio_rustls::TlsConnector::from(lab.cert.client())
                .connect(
                    tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    tcp,
                )
                .await
                .unwrap(),
        )
    } else {
        Box::new(tcp)
    };
    BufReader::new(socket)
}
async fn request(
    socket: &mut BufReader<Box<dyn Io>>,
    method: &str,
    url: &str,
    headers: &str,
) -> (u16, String, Vec<u8>) {
    socket
        .get_mut()
        .write_all(format!("{method} {url} RTSP/1.0\r\nCSeq: 1\r\n{headers}\r\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut h = vec![];
        loop {
            h.push(socket.read_u8().await.unwrap());
            if h.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let h = String::from_utf8(h).unwrap();
        let status = h.split(' ').nth(1).unwrap().parse().unwrap();
        let len = h
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut body = vec![0; len];
        socket.read_exact(&mut body).await.unwrap();
        (status, h, body)
    })
    .await
    .unwrap()
}
impl Lab {
    async fn stop(self) {
        self.app.media.stop_all().await;
        self.cancel.cancel();
        for task in self.tasks {
            tokio::time::timeout(Duration::from_secs(6), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
async fn playback(transport: &str, audio: bool) {
    playback_profile(transport, true, if audio { "aac" } else { "none" }).await;
}
async fn playback_profile(transport: &str, video: bool, audio: &str) {
    let lab = fixture_profile(transport, video, audio).await;
    let mut socket = connect(&lab).await;
    assert_eq!(request(&mut socket, "DESCRIBE", &lab.url, "").await.0, 403);
    assert_eq!(lab.app.media.count().await, 0);
    assert_eq!(lab.hits.load(Ordering::SeqCst), 0);
    let protected = format!("{}?token=owned-viewer", lab.url);
    let (status, _, sdp) = request(&mut socket, "DESCRIBE", &protected, "").await;
    assert_eq!(status, 200);
    let sdp = String::from_utf8(sdp).unwrap();
    assert_eq!(sdp.contains("H265/90000"), video);
    if audio.starts_with("mp3") || audio == "m2a" {
        assert!(sdp.contains("MPA/90000"));
        assert!(!sdp.contains("a=fmtp:14"));
    }
    // Exercise a late join, including the rolling audio-only bootstrap.
    if audio.starts_with("mp3") || audio == "m2a" {
        tokio::time::sleep(Duration::from_millis(2500)).await;
    }
    drop(socket);
    let bridge = if transport == "tls" {
        Some(
            flussonix::tls_input::Bridge::start(&protected, Some(&lab.cert.ca))
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let decode_url = bridge.as_ref().map(|b| b.local_url()).unwrap_or(&protected);
    let mut cmd = tokio::process::Command::new("ffmpeg");
    cmd.args([
        "-nostdin",
        "-v",
        "error",
        "-rtsp_transport",
        if transport == "udp" { "udp" } else { "tcp" },
        "-i",
        decode_url,
        "-t",
        "3",
    ]);
    if video {
        cmd.args(["-map", "0:v:0"]);
    }
    if audio != "none" {
        cmd.args(["-map", "0:a:0"]);
    }
    cmd.args(["-threads", "1", "-f", "framemd5", "-"])
        .kill_on_drop(true);
    let out = tokio::time::timeout(Duration::from_secs(25), cmd.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        out.status.success() && out.stderr.is_empty(),
        "decode {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    if video {
        assert!(
            text.lines().filter(|l| l.starts_with("0,")).count() >= 50,
            "video_count={} audio_count={} worker={}",
            text.lines().filter(|l| l.starts_with("0,")).count(),
            text.lines().filter(|l| l.starts_with("1,")).count(),
            lab.app.media.stats("owned").await
        );
        let actual = hashes(&out.stdout, 0);
        assert_eq!(
            actual, lab.expected,
            "all twelve independently decoded source pictures must appear"
        );
    }
    if lab.audio {
        let prefix = if video { "1," } else { "0," };
        let minimum = if audio == "aac" || audio == "mp3" {
            100
        } else {
            60
        };
        assert!(
            text.lines().filter(|l| l.starts_with(prefix)).count() >= minimum,
            "decoded audio frame count: actual={} minimum={} worker={}",
            text.lines().filter(|l| l.starts_with(prefix)).count(),
            minimum,
            lab.app.media.stats("owned").await
        );
        if audio != "aac" {
            let rate = if audio == "mp3" { "22050" } else { "32000" };
            assert!(
                text.lines().any(|l| l.starts_with("#sample_rate")
                    && l.rsplit(':')
                        .next()
                        .is_some_and(|value| value.trim() == rate)),
                "decoded sample rate"
            );
            assert!(
                text.lines()
                    .any(|l| l.starts_with("#channel_layout") && l.trim_end().ends_with("stereo")),
                "decoded channel layout"
            );
            assert!(
                hashes(&out.stdout, usize::from(video)).len() >= 10,
                "nonconstant decoded MPEG audio"
            );
        }
    }
    assert_eq!(lab.app.media.count().await, 1);
    assert_eq!(lab.hits.load(Ordering::SeqCst), 1);
    assert!(lab.app.rtsp_egress.load(Ordering::Relaxed) > 0);
    if let Some(b) = bridge {
        b.close().await;
    }
    lab.stop().await;
}
#[tokio::test]
async fn hevc_aac_tcp_playback_decodes_owned_pictures() {
    playback("tcp", true).await
}
#[tokio::test]
async fn hevc_aac_udp_playback_decodes_owned_pictures() {
    playback("udp", true).await
}
#[tokio::test]
async fn hevc_aac_verified_tls_playback_decodes_owned_pictures() {
    playback("tls", true).await
}
#[tokio::test]
async fn hevc_video_only_tcp_playback_decodes_owned_pictures() {
    playback("tcp", false).await
}
async fn revoked_profile(transport: &str, audio: &str, track: u32) {
    let lab = fixture_profile(transport, true, audio).await;
    let mut socket = connect(&lab).await;
    assert_eq!(
        request(
            &mut socket,
            "DESCRIBE",
            &format!("{}?token=owned-viewer", lab.url),
            ""
        )
        .await
        .0,
        200
    );
    let (status, h, _) = request(
        &mut socket,
        "SETUP",
        &format!("{}/trackID={track}", lab.url),
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n",
    )
    .await;
    assert_eq!(status, 200);
    let session = h
        .lines()
        .find_map(|l| l.strip_prefix("Session: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    assert_eq!(
        request(
            &mut socket,
            "PLAY",
            &lab.url,
            &format!("Session: {session}\r\n")
        )
        .await
        .0,
        200
    );
    let mut header = [0; 4];
    tokio::time::timeout(Duration::from_secs(3), socket.read_exact(&mut header))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(header[0], b'$');
    let id = lab.app.playback_auth.snapshots()[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(lab.app.playback_auth.revoke(&id));
    let mut rest = vec![];
    let closed = tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut rest))
        .await
        .expect("revocation must close HEVC media");
    // Revocation immediately drops the session; TLS may report an abrupt
    // authenticated peer EOF rather than an orderly close_notify.
    assert!(
        closed.is_ok()
            || (transport == "tls"
                && closed
                    .as_ref()
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::UnexpectedEof)),
        "{closed:?}"
    );
    let worker = lab
        .app
        .media
        .ensure("owned", &lab.app.config.effective("owned").unwrap())
        .await
        .unwrap();
    assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    lab.stop().await;
}
#[tokio::test]
async fn hevc_plain_and_tls_sessions_close_on_revocation() {
    for transport in ["tcp", "tls"] {
        revoked_profile(transport, "aac", 205).await;
    }
}
#[tokio::test]
async fn mpeg_plain_and_tls_sessions_close_on_revocation() {
    for codec in ["m2a", "mp3"] {
        for transport in ["tcp", "tls"] {
            revoked_profile(transport, codec, 88).await;
        }
    }
}
#[tokio::test]
async fn mpeg_layer_two_and_three_tcp_playback_decode_original_audio() {
    for codec in ["m2a", "mp3"] {
        playback_profile("tcp", true, codec).await
    }
}
#[tokio::test]
async fn mpeg_layer_two_and_three_udp_playback_decode_original_audio() {
    for codec in ["m2a", "mp3"] {
        playback_profile("udp", true, codec).await
    }
}
#[tokio::test]
async fn mpeg_layer_two_and_three_verified_tls_playback_decode_original_audio() {
    for codec in ["m2a", "mp3"] {
        playback_profile("tls", true, codec).await
    }
}
#[tokio::test]
async fn mpeg_layer_two_and_three_audio_only_playback_decode_original_audio() {
    for codec in ["m2a", "mp3"] {
        playback_profile("tcp", false, codec).await
    }
}
#[tokio::test]
async fn mpeg_one_layer_three_large_frames_decode_after_rtp_fragmentation() {
    playback_profile("tcp", true, "mp3-mpeg1").await
}

async fn multitrack_playback(transport: &str, video: bool, audios: &[&str]) {
    use futures_util::FutureExt;
    let lab = fixture_tracks(transport, video, audios).await;
    let result = std::panic::AssertUnwindSafe(async {
        let mut socket = connect(&lab).await;
        assert_eq!(request(&mut socket, "DESCRIBE", &lab.url, "").await.0, 403);
        assert_eq!(lab.app.media.count().await, 0);
        assert_eq!(lab.hits.load(Ordering::SeqCst), 0);
        let protected = format!("{}?token=owned-viewer", lab.url);
        let (status, _, sdp) = request(&mut socket, "DESCRIBE", &protected, "").await;
        assert_eq!(status, 200);
        let sdp = String::from_utf8(sdp).unwrap();
        assert_eq!(
            sdp.lines()
                .filter(|line| line.starts_with("m=audio "))
                .count(),
            audios.len()
        );
        assert_eq!(
            sdp.lines()
                .filter(|line| line.starts_with("a=control:trackID="))
                .count(),
            audios.len() + usize::from(video)
        );
        // A second receiver joins the same worker after the native source is running.
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(socket);
        let bridge = if transport == "tls" {
            Some(
                flussonix::tls_input::Bridge::start(&protected, Some(&lab.cert.ca))
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let decode_url = bridge.as_ref().map(|b| b.local_url()).unwrap_or(&protected);
        let mut command = tokio::process::Command::new("ffmpeg");
        command.args([
            "-nostdin",
            "-v",
            "error",
            "-rtsp_transport",
            if transport == "udp" { "udp" } else { "tcp" },
            "-i",
            decode_url,
            "-t",
            "2",
        ]);
        if video {
            command.args(["-map", "0:v:0"]);
        }
        command
            .args(["-map", "0:a", "-threads", "1", "-f", "framemd5", "-"])
            .kill_on_drop(true);
        for index in 0..audios.len() {
            command
                .args([
                    "-map",
                    &format!("0:a:{index}"),
                    "-t",
                    "2",
                    "-ac",
                    "1",
                    "-ar",
                    "48000",
                    "-c:a",
                    "pcm_s16le",
                    "-f",
                    "s16le",
                ])
                .arg(lab._dir.path().join(format!("delivered-{index}.pcm")));
        }
        let output = tokio::time::timeout(Duration::from_secs(25), command.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "strict multitrack decode: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8_lossy(&output.stdout);
        if video {
            assert_eq!(hashes(&output.stdout, 0), lab.expected);
        }
        let mut seen = HashSet::new();
        // SDP reverses the nonsequential native track order, so map each retained
        // audio stream in that same order and verify it has its own decoded content.
        for (index, codec) in audios.iter().rev().enumerate() {
            let stream = index + usize::from(video);
            let prefix = format!("{stream},");
            assert!(
                text.lines()
                    .filter(|line| line.starts_with(&prefix))
                    .count()
                    >= 40,
                "missing decoded audio stream {stream}"
            );
            let rate = match *codec {
                "aac" => 48000,
                "mp3" => 22050,
                _ => 32000,
            };
            assert!(
                text.lines()
                    .any(|line| line.starts_with(&format!("#sample_rate {stream}:"))
                        && line.trim_end().ends_with(&rate.to_string())),
                "sample rate for stream {stream}"
            );
            let content = hashes(&output.stdout, stream);
            assert!(content.len() >= 10, "nonconstant audio for stream {stream}");
            let id = 88 + (audios.len() - 1 - index) as u32 * 19;
            let samples =
                std::fs::read(lab._dir.path().join(format!("delivered-{index}.pcm"))).unwrap();
            let signature = tone_signature(&samples);
            let source = &lab.audio_expected[&id];
            let identity = ((id - 88) / 19) as usize;
            assert!(
                signature[identity] > 0.8,
                "decoded native audio track {id}: {signature:?}"
            );
            for (tone, (actual, expected)) in signature.iter().zip(source).enumerate() {
                assert!(
                    (actual - expected).abs() < 0.05,
                    "track {id}, tone {tone}: {signature:?} vs {source:?}"
                );
            }
            let signature = content.iter().min().unwrap().clone();
            assert!(seen.insert(signature), "audio streams were duplicated");
        }
        assert_eq!(lab.app.media.count().await, 1);
        assert_eq!(
            lab.hits.load(Ordering::SeqCst),
            1,
            "receivers must share one native source pull"
        );
        if let Some(bridge) = bridge {
            bridge.close().await;
        }
        let worker = lab
            .app
            .media
            .ensure("owned", &lab.app.config.effective("owned").unwrap())
            .await
            .unwrap();
        for _ in 0..100 {
            if worker.viewers.load(Ordering::Relaxed) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            worker.viewers.load(Ordering::Relaxed),
            0,
            "playback ownership must be released"
        );
        if let Some(pool) = &lab.pool {
            // Every playback track must return its lease after the independent
            // recorder closes; prove all eight pairs can be leased again.
            let (range, held) = udp_fixture::reserved(2);
            let _ = range;
            let ports = rtsp::protocol::ClientPorts {
                rtp: held[0].local_addr().unwrap().port(),
                rtcp: held[1].local_addr().unwrap().port(),
            };
            let mut leases = Vec::new();
            for _ in 0..audios.len() + usize::from(video) {
                leases.push(
                    pool.lease("127.0.0.1".parse().unwrap(), ports)
                        .await
                        .unwrap(),
                );
            }
            assert_eq!(
                pool.lease("127.0.0.1".parse().unwrap(), ports)
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
#[tokio::test]
async fn multitrack_aac_mpeg_tcp_playback_preserves_distinct_audio_and_hevc() {
    multitrack_playback("tcp", true, &["aac", "m2a", "mp3"]).await;
}
#[tokio::test]
async fn multitrack_aac_mpeg_verified_tls_playback_preserves_distinct_audio_and_hevc() {
    multitrack_playback("tls", true, &["aac", "m2a", "mp3"]).await;
}
#[tokio::test]
async fn multitrack_eight_mpeg_audio_udp_playback_decodes_and_reclaims_every_pair() {
    multitrack_playback("udp", false, &["m2a"; 8]).await;
}

#[tokio::test]
async fn multitrack_seven_aac_udp_playback_keeps_dynamic_payloads_distinct() {
    multitrack_playback("udp", true, &["aac"; 7]).await;
}

#[tokio::test]
async fn multitrack_later_audio_selection_and_revocation_preserve_session_ownership() {
    use futures_util::FutureExt;
    let lab = fixture_tracks("tcp", true, &["aac", "m2a", "mp3"]).await;
    let result = std::panic::AssertUnwindSafe(async {
        let mut socket = connect(&lab).await;
        let protected = format!("{}?token=owned-viewer", lab.url);
        let (status, _, body) = request(&mut socket, "DESCRIBE", &protected, "").await;
        assert_eq!(status, 200);
        let sdp = String::from_utf8(body).unwrap();
        let audio = sdp
            .split("m=")
            .find(|section| section.contains("a=control:trackID=88\r\n"))
            .unwrap();
        let payload: u8 = audio
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .last()
            .unwrap()
            .parse()
            .unwrap();
        let (status, headers, _) = request(
            &mut socket,
            "SETUP",
            &format!("{}/trackID=88", lab.url),
            "Transport: RTP/AVP/TCP;unicast;interleaved=12-13\r\n",
        )
        .await;
        assert_eq!(status, 200);
        let session = headers
            .lines()
            .find_map(|line| line.strip_prefix("Session: "))
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let (status, headers, _) = request(
            &mut socket,
            "PLAY",
            &lab.url,
            &format!("Session: {session}\r\n"),
        )
        .await;
        assert_eq!(status, 200);
        let info = headers
            .lines()
            .find_map(|line| line.strip_prefix("RTP-Info: "))
            .unwrap();
        assert!(
            info.contains("trackID=88;") && !info.contains(',') && !info.contains("trackID=205")
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut received = 0;
            while received < 12 {
                let mut header = [0; 4];
                socket.read_exact(&mut header).await.unwrap();
                assert_eq!(header[0], b'$');
                assert!(
                    [12, 13].contains(&header[1]),
                    "unselected track emitted media"
                );
                let mut body = vec![0; u16::from_be_bytes([header[2], header[3]]) as usize];
                socket.read_exact(&mut body).await.unwrap();
                if header[1] == 12 {
                    assert!(body.len() >= 12 && body[0] >> 6 == 2);
                    assert_eq!(body[1] & 127, payload, "selected third audio mapping");
                    received += 1;
                }
            }
        })
        .await
        .unwrap();
        let worker = lab
            .app
            .media
            .ensure("owned", &lab.app.config.effective("owned").unwrap())
            .await
            .unwrap();
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 1);
        let id = lab.app.playback_auth.snapshots()[0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(lab.app.playback_auth.revoke(&id));
        let mut rest = vec![];
        tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut rest))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert_eq!(lab.app.media.count().await, 1);
        assert_eq!(lab.hits.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn multitrack_mixed_aac_mpeg_udp_playback_preserves_distinct_audio_and_hevc() {
    multitrack_playback("udp", true, &["aac", "m2a", "mp3"]).await;
}
