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
}
async fn fixture(transport: &str, audio: bool) -> Lab {
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
    if audio {
        // Independently encode a continuous AAC source, then retain its ADTS payloads.
        let encoded = tokio::process::Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=880:sample_rate=48000",
                "-t",
                "20",
                "-c:a",
                "aac",
                "-f",
                "adts",
                "pipe:1",
            ])
            .output()
            .await
            .unwrap();
        assert!(encoded.status.success());
        tracks.push(Track {
            id: 88,
            codec: "aac".into(),
            config: vec![0x11, 0x88],
        });
        let mut at = 0;
        let mut n = 0;
        while at < encoded.stdout.len() {
            let h = &encoded.stdout[at..];
            let size =
                (usize::from(h[3] & 3) << 11) | (usize::from(h[4]) << 3) | usize::from(h[5] >> 5);
            frames.push(Frame {
                track_id: 88,
                dts: 90000 + n * 1920,
                pts_offset: 0,
                key: true,
                body: h[7..size].to_vec(),
            });
            at += size;
            n += 1;
        }
        tracks.reverse(); // Audio metadata precedes video; IDs are nonsequential.
    }
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
            let (range, held) = udp_fixture::reserved(4);
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
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        if tls_mode {
            rtsp::serve_tls(listener, a, c, tls).await.unwrap()
        } else {
            rtsp::serve_with_udp(listener, a, c, pool, 100.0)
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
        audio,
    }
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
    let lab = fixture(transport, audio).await;
    let mut socket = connect(&lab).await;
    assert_eq!(request(&mut socket, "DESCRIBE", &lab.url, "").await.0, 403);
    assert_eq!(lab.app.media.count().await, 0);
    assert_eq!(lab.hits.load(Ordering::SeqCst), 0);
    let protected = format!("{}?token=owned-viewer", lab.url);
    let (status, _, sdp) = request(&mut socket, "DESCRIBE", &protected, "").await;
    assert_eq!(status, 200);
    assert!(String::from_utf8(sdp).unwrap().contains("H265/90000"));
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
        "-map",
        "0:v:0",
    ]);
    if audio {
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
    assert!(text.lines().filter(|l| l.starts_with("0,")).count() >= 50);
    let actual = hashes(&out.stdout, 0);
    assert_eq!(
        actual, lab.expected,
        "all twelve independently decoded source pictures must appear"
    );
    if lab.audio {
        assert!(text.lines().filter(|l| l.starts_with("1,")).count() >= 100);
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
#[tokio::test]
async fn hevc_plain_and_tls_sessions_close_on_revocation() {
    for transport in ["tcp", "tls"] {
        let lab = fixture(transport, true).await;
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
            &format!("{}/trackID=205", lab.url),
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
}
