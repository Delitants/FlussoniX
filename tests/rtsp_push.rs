//! Copy-only RTSP delivery qualified against independent receivers.
use flussonix::config::ConfigStore;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{net::TcpListener, process::Command};
#[path = "support/tls.rs"]
mod tls_fixture;
#[path = "support/udp.rs"]
mod udp_fixture;

struct LabDir {
    path: std::path::PathBuf,
    _cleanup: Option<tempfile::TempDir>,
}
impl LabDir {
    fn path(&self) -> &Path {
        &self.path
    }
}
fn labdir() -> LabDir {
    if let Some(parent) = std::env::var_os("FLUSSONIX_RTSP_PUSH_EVIDENCE") {
        let parent = std::path::PathBuf::from(parent).join("labs");
        std::fs::create_dir_all(&parent).unwrap();
        LabDir {
            path: tempfile::tempdir_in(parent).unwrap().keep(),
            _cleanup: None,
        }
    } else {
        let dir = tempfile::tempdir().unwrap();
        LabDir {
            path: dir.path().to_path_buf(),
            _cleanup: Some(dir),
        }
    }
}
fn owned_engine(dir: &Path) -> flussonix::media::Engine {
    flussonix::media::Engine::new(dir.join("media"), "ffmpeg")
}

#[test]
fn mixed_push_config_roundtrips_inherits_and_rejects_invalid_profiles_without_secrets() {
    let dir = labdir();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    let entries = json!([{"url":"srt://localhost:19990","disabled":true},{"url":"rtsp://localhost/channel?password=owned-secret","disabled":true,"connect_timeout":2,"retry_timeout":7},{"url":"rtsps://localhost:13220/channel","disabled":true}]);
    store
        .put("templates", "owned", json!({"pushes":entries}))
        .expect("RTSP/RTSPS push configuration must be accepted");
    store
        .put(
            "streams",
            "owned",
            json!({"template":"owned","inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    assert_eq!(store.effective("owned").unwrap()["pushes"], entries);
    store
        .put(
            "streams",
            "owned",
            json!({"template":"owned","inputs":[{"url":"testsrc://"}],"pushes":[]}),
        )
        .unwrap();
    assert_eq!(store.effective("owned").unwrap()["pushes"], json!([]));
    for entry in [
        json!({"url":"rtsp://:owned-secret@localhost/channel"}),
        json!({"url":"rtsp://user%3Aname:owned-secret@localhost/channel"}),
        json!({"url":"rtsp://user:owned-secret%0A@localhost/channel"}),
        json!({"url":"rtsp://localhost/"}),
        json!({"url":"rtsp://localhost:0/channel"}),
        json!({"url":"rtsp://localhost/channel#owned-secret"}),
        json!({"url":"rtsp://localhost/channel?password=%GGowned-secret"}),
        json!({"url":"rtsp://localhost/channel","flussonix_tls_ca":"/missing/owned-secret"}),
        json!({"url":"rtsps://localhost/channel","flussonix_tls_ca":"relative.pem"}),
        json!({"url":"rtsp://localhost/channel","rtsp_transport":"invalid"}),
        json!({"url":"rtsp://localhost/channel","passphrase":"owned-secret"}),
        json!({"url":"rtsp://localhost/channel","disabled":"true"}),
        json!({"url":"rtsp://localhost/channel","connect_timeout":0}),
        json!({"url":"rtsp://localhost/channel","retry_timeout":301}),
    ] {
        let error = store
            .put(
                "streams",
                "bad",
                json!({"inputs":[{"url":"testsrc://"}],"pushes":[entry]}),
            )
            .unwrap_err();
        assert!(
            !error.contains("owned-secret"),
            "validation error leaked credentials"
        );
    }
    assert!(
        store
            .put(
                "templates",
                "bad",
                json!({"pushes":vec![entries[1].clone();5]})
            )
            .is_err()
    );
}

async fn listening(child: &mut tokio::process::Child, port: u16) {
    listening_socket(child, port, "tcp", "0A").await;
}
async fn listening_socket(child: &mut tokio::process::Child, port: u16, table: &str, state: &str) {
    let pid = child.id().unwrap();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "receiver exited before listening"
            );
            let inodes: Vec<String> = std::fs::read_dir(format!("/proc/{pid}/fd"))
                .unwrap()
                .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
                .filter_map(|p| {
                    p.to_str()?
                        .strip_prefix("socket:[")?
                        .strip_suffix(']')
                        .map(str::to_owned)
                })
                .collect();
            let listening = std::fs::read_to_string(format!("/proc/net/{table}"))
                .unwrap()
                .lines()
                .skip(1)
                .any(|line| {
                    let fields: Vec<_> = line.split_whitespace().collect();
                    fields.get(3) == Some(&state)
                        && fields
                            .get(1)
                            .is_some_and(|v| v.ends_with(&format!(":{port:04X}")))
                        && fields
                            .get(9)
                            .is_some_and(|n| inodes.iter().any(|inode| inode == n))
                });
            if listening {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("actual independent receiver must own its listening socket");
}
async fn receiver(path: &Path) -> (u16, tokio::process::Child) {
    receiver_for(path, 3).await
}
async fn receiver_for(path: &Path, seconds: u64) -> (u16, tokio::process::Child) {
    receiver_transport(path, seconds, "tcp").await
}
async fn receiver_transport(
    path: &Path,
    seconds: u64,
    transport: &str,
) -> (u16, tokio::process::Child) {
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let log = std::fs::File::create(path.with_extension("log")).unwrap();
    let mut child = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-rtsp_flags",
            "listen",
            "-listen_timeout",
            "15",
            "-rtsp_transport",
            transport,
            "-i",
            &format!("rtsp://127.0.0.1:{port}/owned"),
            "-map",
            "0",
            "-c",
            "copy",
            // FFmpeg MPEG4-GENERIC depacketization provides no audio key flag.
            // Keep every raw AAC AU; strict mapped-track decoding remains below.
            "-copyinkf",
            "-t",
            &seconds.to_string(),
            "-f",
            "mpegts",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    listening(&mut child, port).await;
    (port, child)
}
async fn received(
    child: &mut tokio::process::Child,
    path: &Path,
    video: Option<&str>,
    audio: &str,
) {
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("receiver media deadline")
        .unwrap();
    assert!(
        status.success(),
        "receiver failed: {}",
        std::fs::read_to_string(path.with_extension("log")).unwrap()
    );
    let result = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .await
        .unwrap();
    assert!(result.status.success());
    let tracks: Value = serde_json::from_slice(&result.stdout).unwrap();
    let codecs: Vec<_> = tracks["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["codec_name"].as_str().unwrap())
        .collect();
    if let Some(video) = video {
        assert!(codecs.contains(&video), "missing video: {codecs:?}");
    }
    assert!(codecs.contains(&audio), "missing audio: {codecs:?}");
    let mut decode = Command::new("ffmpeg");
    decode
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(path);
    if video.is_some() {
        decode.args(["-map", "0:v:0"]);
    }
    let output = decode
        .args(["-map", "0:a:0", "-f", "framemd5", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "strict decode: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let frames = String::from_utf8(output.stdout).unwrap();
    for track in 0..if video.is_some() { 2 } else { 1 } {
        assert!(
            frames
                .lines()
                .filter(|line| line.starts_with(&format!("{track},")))
                .count()
                > 20,
            "decoded track {track} must contain actual frames"
        );
    }
}
async fn wait_push(worker: &flussonix::media::Worker, index: usize, state: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let s = worker.stats()["flussonix_pushes"][index].clone();
            if s["status"] == state {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("push never reached {state}: {}", worker.stats()))
}
#[tokio::test]
async fn independent_receiver_decodes_native_rtsp_push() {
    let dir = labdir();
    let path = dir.path().join("received.ts");
    let (port, mut receiver) = receiver(&path).await;
    let engine = owned_engine(dir.path());
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned?token=owned-secret"),"retry_timeout":1}]});
    let worker = engine
        .ensure("owned", &cfg)
        .await
        .expect("RTSP push must start");
    let stats = wait_push(&worker, 0, "sending").await;
    assert!(stats["rtp_bytes"].as_u64().unwrap() > 0);
    assert!(!worker.stats().to_string().contains("owned-secret"));
    received(&mut receiver, &path, Some("h264"), "aac").await;
    engine.stop_all().await;
    assert_eq!(worker.stats()["flussonix_pushes"][0]["pid"], 0);
}

#[tokio::test]
async fn independent_receiver_preserves_all_six_codec_pairs() {
    for (video, encoder) in [("h264", "libx264"), ("hevc", "libx265")] {
        for (audio, acodec, ab) in [
            ("aac", "aac", 96),
            ("mp2", "mp2a", 192),
            ("mp3", "mp3", 128),
        ] {
            let dir = labdir();
            let path = dir.path().join("received.ts");
            let (port, mut receiver) = receiver(&path).await;
            let engine = owned_engine(dir.path());
            let cfg = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":encoder,"acodec":acodec,"ab":ab},"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned")}]});
            let worker = engine.ensure("owned", &cfg).await.unwrap();
            wait_push(&worker, 0, "sending").await;
            received(&mut receiver, &path, Some(video), audio).await;
            engine.stop_all().await;
        }
    }
}

#[tokio::test]
async fn trusted_rtsps_delivers_hevc_mp3_to_independent_receiver() {
    use tokio_rustls::TlsAcceptor;
    let dir = labdir();
    let certificates = tls_fixture::Certificates::new();
    let path = dir.path().join("secure.ts");
    let (plain, mut receiver) = receiver(&path).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = TlsAcceptor::from(certificates.server());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        drop(listener);
        let mut secure = acceptor.accept(socket).await.unwrap();
        let mut plain = tokio::net::TcpStream::connect(("127.0.0.1", plain))
            .await
            .unwrap();
        let _ = tokio::io::copy_bidirectional(&mut secure, &mut plain).await;
    });
    let engine = owned_engine(dir.path());
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx265","acodec":"mp3","ab":128},"pushes":[{"url":format!("rtsps://localhost:{port}/owned?password=owned-secret"),"flussonix_tls_ca":certificates.ca}]});
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    received(&mut receiver, &path, Some("hevc"), "mp3").await;
    engine.stop_all().await;
    task.abort();
    let _ = task.await;
    assert!(!worker.stats().to_string().contains("owned-secret"));
}

#[tokio::test]
async fn untrusted_wrong_identity_and_expired_tls_never_forward_rtsp() {
    use tokio::{io::AsyncReadExt, sync::oneshot};
    use tokio_rustls::TlsAcceptor;
    for reason in ["untrusted", "identity", "expired"] {
        let certificates = tls_fixture::Certificates::new();
        if reason == "expired" {
            certificates.expire();
        }
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = TlsAcceptor::from(certificates.server());
        let (sent, received) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let count = match acceptor.accept(socket).await {
                Ok(mut socket) => {
                    let mut b = [0u8; 4096];
                    socket.read(&mut b).await.unwrap_or(0)
                }
                Err(_) => 0,
            };
            let _ = sent.send(count);
        });
        let dir = labdir();
        let engine = owned_engine(dir.path());
        let host = if reason == "identity" {
            "127.0.0.2"
        } else {
            "127.0.0.1"
        };
        let mut push = json!({"url":format!("rtsps://user:owned-secret@{host}:{port}/owned?token=owned-secret"),"retry_timeout":10});
        if reason != "untrusted" {
            push["flussonix_tls_ca"] = json!(certificates.ca);
        }
        let worker = engine
            .ensure(
                "owned",
                &json!({"inputs":[{"url":"testsrc://"}],"pushes":[push]}),
            )
            .await
            .unwrap();
        let stats = wait_push(&worker, 0, "retrying").await;
        assert_eq!(stats["rtp_bytes"], 0);
        assert_eq!(stats["pid"], 0);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), received)
                .await
                .unwrap()
                .unwrap(),
            0,
            "{reason} trust failure leaked RTSP data"
        );
        engine.stop_all().await;
        task.await.unwrap();
    }
}

#[tokio::test]
async fn receiver_publication_password_is_independent_of_viewer_and_management_auth() {
    for udp in [false, true] {
        receiver_password_case(udp).await;
    }
}
async fn receiver_password_case(udp: bool) {
    use flussonix::server::{App, Options};
    use tokio_util::sync::CancellationToken;
    let dir = labdir();
    let app = App::new(
        dir.path().join("sink.json"),
        dir.path().join("sink-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"publish://"}],"password":"owned-publisher"}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel = CancellationToken::new();
    let pool = if udp {
        let (range, reserved) = udp_fixture::reserved(8);
        drop(reserved);
        Some(
            flussonix::rtsp::udp::Pool::bind("127.0.0.1".parse().unwrap(), range)
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let task = tokio::spawn(flussonix::rtsp::serve_with_udp(
        listener,
        app.clone(),
        cancel.clone(),
        pool.clone(),
        100.0,
    ));
    let engine = flussonix::media::Engine::new(dir.path().join("source-media"), "ffmpeg");
    let mut cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned?password=owned-admin"),"retry_timeout":10}]});
    if udp {
        cfg["pushes"][0]["rtsp_transport"] = json!("udp");
    }
    let wrong = engine.ensure("owned", &cfg).await.unwrap();
    wait_push(&wrong, 0, "retrying").await;
    assert_eq!(app.media.count().await, 0);
    assert_eq!(wrong.stats()["flussonix_pushes"][0]["rtp_bytes"], 0);
    cfg["pushes"][0]["url"] = json!(format!(
        "rtsp://127.0.0.1:{port}/owned?password=owned-publisher"
    ));
    let right = engine.ensure("owned", &cfg).await.unwrap();
    assert!(!Arc::ptr_eq(&wrong, &right));
    wait_push(&right, 0, "sending").await;
    tokio::time::timeout(Duration::from_secs(15), async {
        while !app.media.ready("owned").await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(app.media.count().await, 1);
    engine.stop_all().await;
    if let Some(pool) = pool {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let mut leases = Vec::new();
                for _ in 0..4 {
                    if let Ok(lease) = pool
                        .lease(
                            "127.0.0.1".parse().unwrap(),
                            flussonix::rtsp::protocol::ClientPorts {
                                rtp: 6000,
                                rtcp: 6001,
                            },
                        )
                        .await
                    {
                        leases.push(lease);
                    } else {
                        break;
                    }
                }
                if leases.len() == 4 {
                    break;
                }
                drop(leases);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("stopping UDP push must release the receiver leases");
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
    app.media.stop_all().await;
}

#[tokio::test]
async fn unreachable_destination_isolated_retry_reconnect_and_replacement_stop_sessions() {
    let dir = labdir();
    let path = dir.path().join("first.ts");
    let (port, mut rx) = receiver(&path).await;
    let unavailable = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad = unavailable.local_addr().unwrap().port();
    drop(unavailable);
    let engine = owned_engine(dir.path());
    let mut cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{bad}/owned"),"connect_timeout":1,"retry_timeout":1},{"url":format!("rtsp://127.0.0.1:{port}/owned"),"retry_timeout":1},{"url":"srt://127.0.0.1:19990","disabled":true}]});
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    wait_push(&worker, 0, "retrying").await;
    wait_push(&worker, 1, "sending").await;
    received(&mut rx, &path, Some("h264"), "aac").await;
    assert!(engine.read("owned", "index.m3u8").await.is_ok());
    assert_eq!(engine.count().await, 1);
    // Rebind the SAME known receiver port; live retry must recover this destination.
    let path2 = dir.path().join("second.ts");
    let log = std::fs::File::create(path2.with_extension("log")).unwrap();
    let mut second = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-y",
            "-rtsp_flags",
            "listen",
            "-listen_timeout",
            "15",
            "-rtsp_transport",
            "tcp",
            "-i",
            &format!("rtsp://127.0.0.1:{port}/owned"),
            "-map",
            "0",
            "-c",
            "copy",
            // FFmpeg MPEG4-GENERIC depacketization provides no audio key flag.
            // Keep every raw AAC AU; strict mapped-track decoding remains below.
            "-copyinkf",
            "-t",
            "3",
            "-f",
            "mpegts",
        ])
        .arg(&path2)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    listening(&mut second, port).await;
    received(&mut second, &path2, Some("h264"), "aac").await;
    assert!(
        worker.stats()["flussonix_pushes"][1]["attempts"]
            .as_u64()
            .unwrap()
            >= 2
    );
    let pids: Vec<_> = worker.stats()["flussonix_pushes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["pid"].as_u64())
        .filter(|p| *p > 0)
        .collect();
    cfg["pushes"][1]["disabled"] = json!(true);
    let replacement = engine.ensure("owned", &cfg).await.unwrap();
    assert_eq!(worker.stats()["flussonix_pushes"][1]["pid"], 0);
    assert_eq!(replacement.stats()["flussonix_pushes"][1]["attempts"], 0);
    for pid in pids {
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }
    tokio::time::timeout(Duration::from_secs(3), engine.stop_all())
        .await
        .unwrap();
}

#[tokio::test]
async fn audio_only_push_preserves_aac_layer_ii_and_mp3_tracks_together() {
    use tokio::io::AsyncReadExt;
    let dir = labdir();
    let path = dir.path().join("audio.ts");
    let (port, mut rx) = receiver(&path).await;
    let engine = owned_engine(dir.path());
    let cfg = json!({"inputs":[{"url":"publish://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned")}],"flussonix_input_timeout":5});
    let mut publication = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let mut source = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=700:sample_rate=48000",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=800:sample_rate=48000",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=900:sample_rate=48000",
            "-map",
            "0:a",
            "-map",
            "1:a",
            "-map",
            "2:a",
            "-c:a:0",
            "aac",
            "-c:a:1",
            "mp2",
            "-b:a:1",
            "192k",
            "-c:a:2",
            "libmp3lame",
            "-b:a:2",
            "128k",
            "-threads",
            "1",
            "-t",
            "15",
            "-f",
            "mpegts",
            "pipe:1",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = source.stdout.take().unwrap();
    let mut input = publication.stdin.take().unwrap();
    let writer = tokio::spawn(async move { tokio::io::copy(&mut output, &mut input).await });
    wait_push(&publication.worker, 0, "sending").await;
    received(&mut rx, &path, None, "aac").await;
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
        ])
        .arg(&path)
        .output()
        .await
        .unwrap();
    let tracks: Value = serde_json::from_slice(&output.stdout).unwrap();
    let codecs: Vec<_> = tracks["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["codec_name"].as_str().unwrap())
        .collect();
    assert_eq!(codecs, vec!["aac", "mp2", "mp3"]);
    let output = Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(&path)
        .args(["-map", "0:a", "-f", "framemd5", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    for track in 0..3 {
        assert!(
            text.lines()
                .filter(|l| l.starts_with(&format!("{track},")))
                .count()
                > 20,
            "missing decoded audio track {track}"
        );
    }
    engine.stop_all().await;
    let _ = source.kill().await;
    let _ = source.wait().await;
    writer.abort();
    let _ = writer.await;
    // Child diagnostic pipe is fixture-only and bounded by finite synthetic input.
    if let Some(mut stderr) = source.stderr.take() {
        let mut log = Vec::new();
        let _ = stderr.read_to_end(&mut log).await;
        std::fs::write(dir.path().join("source.log"), log).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires a qualified H264 VAAPI render device and driver environment"]
async fn vaapi_h264_rtsp_push_strictly_decodes_at_independent_receiver() {
    let dir = labdir();
    let path = dir.path().join("vaapi.ts");
    let (port, mut rx) = receiver(&path).await;
    let engine = owned_engine(dir.path());
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"h264_vaapi","acodec":"mp2a","ab":192},"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned")} ]});
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    received(&mut rx, &path, Some("h264"), "mp2").await;
    engine.stop_all().await;
}

#[path = "support/dvb_fixture.rs"]
mod dvb_fixture;
#[tokio::test]
async fn retained_dvb_subtitles_fail_destination_without_silent_track_omission() {
    use tokio::io::AsyncWriteExt;
    let dir = labdir();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let engine = owned_engine(dir.path());
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":"preserve","pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned"),"retry_timeout":10}]});
    let bytes = dvb_fixture::transport();
    let mut publication = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let mut input = publication.stdin.take().unwrap();
    let (close, done) = tokio::sync::oneshot::channel();
    let (fed, ready) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        input.write_all(&bytes).await.unwrap();
        let _ = fed.send(());
        let _ = done.await;
        drop(input);
    });
    let stats = wait_push(&publication.worker, 0, "retrying").await;
    assert_eq!(stats["last_error"], "push_profile_unsupported");
    assert_eq!(stats["rtp_bytes"], 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "unsupported retained track must fail before connecting"
    );
    assert!(
        publication
            .worker
            .alive
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    // Finish the finite fixture write before intentionally closing its input pipe.
    tokio::time::timeout(Duration::from_secs(10), ready)
        .await
        .unwrap()
        .unwrap();
    engine.stop_all().await;
    let _ = close.send(());
    writer.await.unwrap();
}

#[tokio::test]
async fn rtsp_push_keeps_on_demand_stream_active_but_disabled_and_publication_wait() {
    use flussonix::server::{App, Options};
    let dir = labdir();
    let app = App::new(
        dir.path().join("app.json"),
        dir.path().join("app-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let owned_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let owned_url = format!(
        "rtsp://127.0.0.1:{}/owned",
        owned_listener.local_addr().unwrap().port()
    );
    let push = json!({"url":owned_url,"connect_timeout":1});
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],"pushes":[push]}),
        )
        .unwrap();
    app.config.put("streams","disabled",json!({"static":false,"inputs":[{"url":"testsrc://"}],"pushes":[{"url":owned_url,"disabled":true}]})).unwrap();
    app.config
        .put(
            "streams",
            "publication",
            json!({"static":false,"inputs":[{"url":"publish://"}],"pushes":[push]}),
        )
        .unwrap();
    app.reconcile().await;
    assert_eq!(app.media.count().await, 1);
    assert_eq!(app.media.stats("disabled").await["status"], "waiting");
    assert_eq!(app.media.stats("publication").await["status"], "waiting");
    app.media.stop_all().await;
}

#[tokio::test]
async fn independent_receiver_survives_native_publisher_options_keepalive() {
    let dir = labdir();
    let path = dir.path().join("long.ts");
    let (receiver_port, mut rx) = receiver_for(&path, 18).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let methods = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let observed = methods.clone();
    // Transparent independent recording observer; media bytes are never altered.
    let observer = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (local, _) = listener.accept().await.unwrap();
        drop(listener);
        let remote = tokio::net::TcpStream::connect(("127.0.0.1", receiver_port))
            .await
            .unwrap();
        let (mut incoming, mut outgoing) = local.into_split();
        let (mut response, mut request) = remote.into_split();
        let forward = async {
            loop {
                let first = incoming.read_u8().await?;
                let mut header = vec![first];
                let size = if first == b'$' {
                    header.push(incoming.read_u8().await?);
                    let size = incoming.read_u16().await?;
                    header.extend(size.to_be_bytes());
                    usize::from(size)
                } else {
                    while !header.ends_with(b"\r\n\r\n") {
                        assert!(header.len() < 16384, "owned request header bound");
                        header.push(incoming.read_u8().await?);
                    }
                    let text = std::str::from_utf8(&header).unwrap();
                    observed
                        .lock()
                        .unwrap()
                        .push(text.split(' ').next().unwrap().to_owned());
                    text.lines()
                        .find_map(|l| {
                            l.split_once(':')
                                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                .map(|(_, v)| v.trim())
                        })
                        .map(|s| s.trim().parse::<usize>().unwrap())
                        .unwrap_or(0)
                };
                assert!(size <= 65536, "owned request body bound");
                let mut body = vec![0; size];
                incoming.read_exact(&mut body).await?;
                request.write_all(&header).await?;
                request.write_all(&body).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {_=forward=>{},_=tokio::io::copy(&mut response,&mut outgoing)=>{}}
    });
    let engine = owned_engine(dir.path());
    let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned"),"retry_timeout":10}]})).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    received(&mut rx, &path, Some("h264"), "aac").await;
    let methods = methods.lock().unwrap().clone();
    assert!(
        methods.iter().any(|m| m == "OPTIONS"),
        "independent receiver must observe a real OPTIONS keepalive: {methods:?}"
    );
    let probe = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
        ])
        .arg(&path)
        .output()
        .await
        .unwrap();
    assert!(probe.status.success());
    let result: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let duration = result["format"]["duration"]
        .as_str()
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert!(
        duration >= 17.5,
        "18-second recording must survive keepalive, received {duration} seconds"
    );
    assert_eq!(worker.stats()["flussonix_pushes"][0]["attempts"], 1);
    engine.stop_all().await;
    observer.await.unwrap();
}

#[tokio::test]
async fn stop_during_tls_and_partial_setup_closes_owned_connections_without_retry() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for secure in [false, true] {
        let dir = labdir();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let engine = owned_engine(dir.path());
        let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("{}://127.0.0.1:{port}/owned",if secure {"rtsps"} else {"rtsp"}),"connect_timeout":30,"retry_timeout":1}]})).await.unwrap();
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut first = [0; 5];
        tokio::time::timeout(Duration::from_secs(5), socket.read_exact(&mut first))
            .await
            .unwrap()
            .unwrap();
        if secure {
            assert_eq!(first[0], 22, "real TLS client hello required");
        } else {
            assert_eq!(&first, b"ANNOU");
            // A partial response exercises the permanent reader while its parser waits.
            socket.write_all(b"RTSP/1.0 200 OK\r\nCSeq:").await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), engine.stop_all())
            .await
            .expect("stop must cancel blocked TLS/setup before connection deadline");
        let mut remainder = Vec::new();
        let closure =
            tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut remainder))
                .await
                .unwrap();
        // Immediate cancellation can reset a TCP socket with unread control/TLS data.
        assert!(
            closure.is_ok()
                || closure
                    .as_ref()
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset),
            "destination must close with EOF or TCP reset: {closure:?}"
        );
        assert_eq!(worker.stats()["flussonix_pushes"][0]["status"], "stopped");
        assert_eq!(worker.stats()["flussonix_pushes"][0]["attempts"], 1);
        assert_eq!(worker.stats()["flussonix_pushes"][0]["rtp_bytes"], 0);
        assert_eq!(worker.stats()["flussonix_pushes"][0]["pid"], 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "cancelled destination must never retry"
        );
    }
}

#[tokio::test]
async fn early_metadata_keeps_the_shorter_tls_connection_timeout() {
    use tokio::io::AsyncReadExt;
    let dir = labdir();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let engine = owned_engine(dir.path());
    let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsps://127.0.0.1:{port}/owned"),"connect_timeout":2,"retry_timeout":10}]})).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(6), async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut first = [0; 5];
        socket.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], 22, "actual TLS ClientHello required");
        let mut remainder = Vec::new();
        let closure =
            tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut remainder)).await;
        if let Ok(ref closure) = closure {
            assert!(
                closure.is_ok()
                    || closure
                        .as_ref()
                        .is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset),
                "expired setup must close its socket: {closure:?}"
            );
        }
        closure.map(|_| ())
    })
    .await;
    engine.stop_all().await;
    result
        .expect("early metadata must reach the receiver")
        .expect("connection timeout must remain shorter than the startup budget");
    let stats = worker.stats()["flussonix_pushes"][0].clone();
    assert_eq!(stats["status"], "stopped");
    assert_eq!(stats["attempts"], 1);
    assert_eq!(stats["rtp_bytes"], 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err(),
        "cancelled destination must not retry"
    );
}

#[tokio::test]
async fn delayed_metadata_cannot_extend_stalled_tls_past_the_startup_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = labdir();
    let source = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=700:sample_rate=48000",
            "-t",
            "5",
            "-c:a",
            "mp2",
            "-b:a",
            "192k",
            "-threads",
            "1",
            "-f",
            "mpegts",
            "pipe:1",
        ])
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(source.status.success(), "owned MP2 source must encode");
    assert!(source.stdout.len() > 100_000, "real source media required");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let engine = owned_engine(dir.path());
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_input_timeout":30,"pushes":[{"url":format!("rtsps://127.0.0.1:{port}/owned"),"connect_timeout":8,"retry_timeout":5}]});
    let mut publication = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let worker = publication.worker.clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        while worker.stats()["flussonix_pushes"][0]["attempts"] != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let started = tokio::time::Instant::now();
    let mut input = publication.stdin.take().unwrap();
    let (release, hold) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        // Metadata consumes ten seconds of the advertised thirteen-second startup window.
        tokio::time::sleep(Duration::from_secs(10)).await;
        input.write_all(&source.stdout).await.unwrap();
        let _ = hold.await;
        drop(input);
    });
    let mut saw_tls = false;
    let result = tokio::time::timeout_at(started + Duration::from_millis(14_500), async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut first = [0; 5];
        socket.read_exact(&mut first).await.unwrap();
        assert_eq!(
            first[0], 22,
            "actual TLS ClientHello required before deadline"
        );
        saw_tls = true;
        eprintln!(
            "Owned delayed source reached actual TLS after {:?}",
            started.elapsed()
        );
        assert!(started.elapsed() >= Duration::from_secs(10));
        let mut remainder = Vec::new();
        let closure = socket.read_to_end(&mut remainder).await;
        assert!(
            closure.is_ok()
                || closure
                    .as_ref()
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset),
            "expired setup must close its socket: {closure:?}"
        );
        loop {
            let stats = worker.stats()["flussonix_pushes"][0].clone();
            if stats["status"] == "retrying" {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    engine.stop_all().await;
    let _ = release.send(());
    writer.await.unwrap();
    assert!(
        saw_tls,
        "the fixture must reach TLS before testing socket expiry"
    );
    let stats =
        result.expect("late metadata must not give the stalled handshake a fresh eight seconds");
    assert_eq!(
        stats["attempts"], 1,
        "retry backoff starts after owned socket closure"
    );
    assert_eq!(stats["last_error"], "push_connect_failed");
    assert_eq!(stats["rtp_bytes"], 0);
    assert_eq!(worker.stats()["flussonix_pushes"][0]["status"], "stopped");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err(),
        "cancelled destination must not retry"
    );
}

#[tokio::test]
async fn mixed_encrypted_srt_and_rtsp_deliver_from_one_worker_and_stop_owned_sessions() {
    let dir = labdir();
    let rtsp_path = dir.path().join("rtsp.ts");
    let srt_path = dir.path().join("srt.ts");
    let (rtsp_port, mut rtsp) = receiver(&rtsp_path).await;
    let reserved = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let srt_port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let log = std::fs::File::create(srt_path.with_extension("log")).unwrap();
    let secret = "owned-mixed-srt-secret";
    let mut srt=Command::new("ffmpeg").args(["-nostdin","-v","error","-y","-passphrase",secret,"-f","mpegts","-i",&format!("srt://127.0.0.1:{srt_port}?mode=listener&listen_timeout=15000000&timeout=5000000&enforced_encryption=1"),"-map","0","-c","copy","-t","3","-f","mpegts"]).arg(&srt_path).stdout(std::process::Stdio::null()).stderr(log).kill_on_drop(true).spawn().unwrap();
    listening_socket(&mut srt, srt_port, "udp", "07").await;
    let engine = owned_engine(dir.path());
    let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{rtsp_port}/owned")},{"url":format!("srt://127.0.0.1:{srt_port}"),"passphrase":secret}]})).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    let stats = wait_push(&worker, 1, "sending").await;
    assert_eq!(stats["index"], 1);
    let srt_pid = stats["pid"].as_u64().unwrap();
    assert!(srt_pid > 0);
    assert_eq!(worker.stats()["flussonix_pushes"][0]["pid"], 0);
    assert_eq!(engine.count().await, 1);
    received(&mut rtsp, &rtsp_path, Some("h264"), "aac").await;
    received(&mut srt, &srt_path, Some("h264"), "aac").await;
    assert!(!worker.stats().to_string().contains(secret));
    engine.stop_all().await;
    assert!(!Path::new(&format!("/proc/{srt_pid}")).exists());
    for entry in worker.stats()["flussonix_pushes"].as_array().unwrap() {
        assert_eq!(entry["status"], "stopped");
        assert_eq!(entry["pid"], 0);
    }
}

// A TS-only eligibility probe cannot see retained native text tracks.
#[tokio::test]
async fn native_text_preserve_rejects_rtsp_before_connect_and_drop_delivers_audio() {
    use axum::{Router, body::Body, routing::get};
    use bytes::Bytes;
    use flussonix::{m4f::Frame, m4s::Track};
    use futures_util::StreamExt;
    for protocol in ["m4s", "m4f"] {
        for preserve in [true, false] {
            let dir = labdir();
            let tracks = vec![
                Track {
                    id: 2,
                    codec: "m2a".into(),
                    config: vec![],
                },
                Track {
                    id: 7,
                    codec: "subtitle".into(),
                    config: vec![],
                },
                Track {
                    id: 8,
                    codec: "subtitle".into(),
                    config: vec![],
                },
            ];
            let mut frames: Vec<_> = (0..600)
                .map(|n| Frame {
                    track_id: 2,
                    dts: 90000 + n * 2160,
                    pts_offset: 0,
                    key: true,
                    body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
                })
                .collect();
            for (id, text) in [(7, "AMERICA HELLO"), (8, "EUROPE GRÜSSE")] {
                frames.push(Frame {
                    track_id: id,
                    dts: 117000,
                    pts_offset: 63000,
                    key: true,
                    body: text.as_bytes().to_vec(),
                });
            }
            frames.sort_by_key(|f| f.dts);
            let mut control = flussonix::wire::encode_info(&tracks).unwrap();
            for frame in &frames {
                control.extend(
                    flussonix::wire::encode_frame(
                        tracks.iter().find(|t| t.id == frame.track_id).unwrap(),
                        frame,
                    )
                    .unwrap(),
                );
            }
            let segment = Bytes::from(flussonix::m4f::pack(&tracks, &frames, 1_296_000).unwrap());
            let control = if protocol == "m4f" {
                Bytes::from_static(b"0 2023/11/14/22/13/20-14400\n")
            } else {
                Bytes::from(control)
            };
            let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let source_addr = source.local_addr().unwrap();
            let routes = Router::new()
                .route(
                    &format!("/owned/{protocol}"),
                    get(move || {
                        let control = control.clone();
                        async move {
                            Body::from_stream(
                                futures_util::stream::once(std::future::ready(Ok::<
                                    _,
                                    std::io::Error,
                                >(
                                    control
                                )))
                                .chain(futures_util::stream::pending()),
                            )
                        }
                    }),
                )
                .route(
                    "/owned/2023/11/14/22/13/20.m4f",
                    get(move || {
                        let segment = segment.clone();
                        async move { segment }
                    }),
                );
            let source_task =
                tokio::spawn(async move { axum::serve(source, routes).await.unwrap() });
            let path = dir.path().join("native-text.ts");
            let denied_listener = if preserve {
                Some(TcpListener::bind("127.0.0.1:0").await.unwrap())
            } else {
                None
            };
            let (port, mut receiver) = if let Some(listener) = &denied_listener {
                (listener.local_addr().unwrap().port(), None)
            } else {
                let (port, child) = receiver(&path).await;
                (port, Some(child))
            };
            let engine = owned_engine(dir.path());
            let worker = engine.ensure("owned", &json!({
                "inputs":[{"url":format!("{protocol}://{source_addr}/owned")}],
                "flussonix_subtitle_tracks":if preserve {"preserve"} else {"drop"},
                "pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned"),"retry_timeout":10}]
            })).await.unwrap();
            if let Some(listener) = denied_listener {
                let stats = wait_push(&worker, 0, "retrying").await;
                assert_eq!(
                    stats["last_error"], "push_profile_unsupported",
                    "{protocol} native text must not disappear"
                );
                assert_eq!(stats["rtp_bytes"], 0);
                assert_eq!(worker.stats()["native_subtitle_tracks"], 2);
                assert!(
                    tokio::time::timeout(Duration::from_millis(150), listener.accept())
                        .await
                        .is_err(),
                    "retained native text must fail before connecting"
                );
                assert!(
                    !worker.is_closed(),
                    "destination failure must leave common worker alive"
                );
            } else {
                // A finite native GOP can finish before a transient status is observed.
                // The independently decoded recording and cumulative RTP prove delivery.
                received(receiver.as_mut().unwrap(), &path, None, "mp2").await;
                assert!(
                    worker.stats()["flussonix_pushes"][0]["rtp_bytes"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
            }
            engine.stop_all().await;
            source_task.abort();
            let _ = source_task.await;
        }
    }
}

async fn authentication_gateway(
    dir: &Path,
    receiver: u16,
    profile: &str,
    rotate: bool,
    reject: bool,
    origin_file: Option<&Path>,
) -> (u16, tokio::process::Child) {
    let mut command = Command::new("python3");
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/rtsp_auth_gateway.py"
        ))
        .args(["--receiver", &receiver.to_string(), "--profile", profile])
        .arg("--port-file")
        .arg(dir.join("gateway.port"))
        .arg("--events")
        .arg(dir.join("gateway.jsonl"));
    if let Some(path) = origin_file {
        command.arg("--origin-file").arg(path);
    }
    if rotate {
        command.arg("--rotate");
    }
    if reject {
        command.arg("--reject");
    }
    let mut child = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(dir.join("gateway.log")).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let port = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "authentication gateway exited"
            );
            if let Ok(text) = std::fs::read_to_string(dir.join("gateway.port")) {
                break text.parse::<u16>().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    (port, child)
}
fn gateway_evidence(dir: &Path) -> Vec<Value> {
    std::fs::read_to_string(dir.join("gateway.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn auth_events(dir: &Path) -> Vec<Value> {
    gateway_evidence(dir)
        .into_iter()
        .filter(|event| event.get("method").is_some())
        .collect()
}
fn received_auth_media(dir: &Path) -> u64 {
    let events = gateway_evidence(dir);
    let summary = events.last().expect("independent receiver summary");
    assert_eq!(summary["kind"], "summary");
    summary["media_frames"].as_u64().unwrap()
}

#[tokio::test]
async fn independent_basic_and_digest_receivers_authenticate_and_decode_both_tracks() {
    for profile in [
        "basic",
        "MD5-legacy",
        "MD5-sess-legacy",
        "SHA-256-legacy",
        "SHA-256-sess-legacy",
        "MD5",
        "MD5-sess",
        "SHA-256",
        "SHA-256-sess",
    ] {
        let dir = labdir();
        let path = dir.path().join("authenticated.ts");
        let (rx_port, mut rx) = receiver(&path).await;
        let (port, mut gateway) =
            authentication_gateway(dir.path(), rx_port, profile, false, false, None).await;
        let url =
            format!("rtsp://user%20name:owned%3Asecret@127.0.0.1:{port}/owned?token=owned-query");
        let engine = owned_engine(dir.path());
        let worker = engine
            .ensure(
                "owned",
                &json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":url,"retry_timeout":30}]}),
            )
            .await
            .expect("userinfo credentials must configure a native RTSP destination");
        wait_push(&worker, 0, "sending").await;
        received(&mut rx, &path, Some("h264"), "aac").await;
        engine.stop_all().await;
        gateway.wait().await.unwrap();
        assert!(received_auth_media(dir.path()) > 20);
        let events = auth_events(dir.path());
        assert_eq!(
            events.iter().filter(|e| e["accepted"] == false).count(),
            1,
            "only initial ANNOUNCE is unsigned: {profile}: {events:?}"
        );
        for method in ["ANNOUNCE", "SETUP", "RECORD"] {
            assert!(
                events
                    .iter()
                    .any(|e| e["method"] == method && e["accepted"] == true),
                "missing authenticated {method} for {profile}"
            );
        }
        let stats = worker.stats()["flussonix_pushes"].to_string();
        for secret in ["owned:secret", "user%20name", "owned-query", "/owned"] {
            assert!(!stats.contains(secret), "stats leaked destination data");
        }
        assert!(
            std::fs::read_to_string(dir.path().join("gateway.log"))
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn digest_nonce_renewal_keeps_media_and_authenticates_options_without_reconnect() {
    let dir = labdir();
    let path = dir.path().join("renewed.ts");
    let (rx_port, mut rx) = receiver_for(&path, 19).await;
    let (port, mut gateway) =
        authentication_gateway(dir.path(), rx_port, "SHA-256-sess", true, false, None).await;
    let engine = owned_engine(dir.path());
    let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://user%20name:owned%3Asecret@127.0.0.1:{port}/owned?token=owned-query"),"retry_timeout":30}]})).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    received(&mut rx, &path, Some("h264"), "aac").await;
    let attempts = worker.stats()["flussonix_pushes"][0]["attempts"].clone();
    engine.stop_all().await;
    gateway.wait().await.unwrap();
    let events = auth_events(dir.path());
    assert!(
        events
            .iter()
            .any(|e| e["method"] == "OPTIONS" && e["stale"] == true)
    );
    assert!(events.iter().any(|e| e["method"] == "OPTIONS"
        && e["accepted"] == true
        && e["nonce"] == "owned-nonce-2"));
    assert_eq!(
        attempts, 1,
        "stale challenge must not reconnect or interrupt media"
    );
    assert!(
        std::fs::read_to_string(dir.path().join("gateway.log"))
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn wrong_credentials_and_unsupported_or_ambiguous_challenges_fail_without_media() {
    for (profile, reject, password, expected_requests) in [
        ("basic", false, "wrong-secret", 2),
        ("MD5", true, "owned%3Asecret", 2),
        ("unsupported", false, "owned%3Asecret", 1),
        ("ambiguous", false, "owned%3Asecret", 1),
        ("stale-loop", false, "owned%3Asecret", 3),
    ] {
        let dir = labdir();
        let path = dir.path().join("denied.ts");
        let (rx_port, mut rx) = receiver(&path).await;
        let (port, mut gateway) =
            authentication_gateway(dir.path(), rx_port, profile, false, reject, None).await;
        let engine = owned_engine(dir.path());
        let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://user%20name:{password}@127.0.0.1:{port}/owned?token=owned-query"),"retry_timeout":30}]})).await.unwrap();
        let stats = wait_push(&worker, 0, "retrying").await;
        assert_eq!(stats["rtp_bytes"], 0);
        assert_eq!(stats["last_error"], "push_auth_rejected");
        engine.stop_all().await;
        gateway.wait().await.unwrap();
        rx.kill().await.unwrap();
        rx.wait().await.unwrap();
        assert_eq!(
            received_auth_media(dir.path()),
            0,
            "independent receiver must observe zero RTP/RTCP"
        );
        let events = auth_events(dir.path());
        assert_eq!(
            events.len(),
            expected_requests,
            "bounded challenge attempts: {profile}"
        );
        assert!(
            events
                .iter()
                .all(|e| e["method"] == "ANNOUNCE" && e["accepted"] == false)
        );
        assert!(
            std::fs::read_to_string(dir.path().join("gateway.log"))
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn verified_tls_authenticates_the_original_secure_uri_and_decodes_hevc_mp3() {
    use tokio_rustls::TlsAcceptor;
    let dir = labdir();
    let certificates = tls_fixture::Certificates::new();
    let path = dir.path().join("authenticated-secure.ts");
    let (rx_port, mut rx) = receiver(&path).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let origin_file = dir.path().join("original-origin");
    std::fs::write(&origin_file, format!("rtsps://localhost:{port}")).unwrap();
    let (gateway_port, mut gateway) = authentication_gateway(
        dir.path(),
        rx_port,
        "SHA-256",
        false,
        false,
        Some(&origin_file),
    )
    .await;
    let acceptor = TlsAcceptor::from(certificates.server());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        drop(listener);
        let mut secure = acceptor.accept(socket).await.unwrap();
        let mut plain = tokio::net::TcpStream::connect(("127.0.0.1", gateway_port))
            .await
            .unwrap();
        let _ = tokio::io::copy_bidirectional(&mut secure, &mut plain).await;
    });
    let engine = owned_engine(dir.path());
    let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx265","acodec":"mp3","ab":128},"pushes":[{"url":format!("rtsps://user%20name:owned%3Asecret@localhost:{port}/owned?token=owned-query"),"flussonix_tls_ca":certificates.ca,"retry_timeout":30}]})).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    received(&mut rx, &path, Some("hevc"), "mp3").await;
    engine.stop_all().await;
    gateway.wait().await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(received_auth_media(dir.path()) > 20);
    let events = auth_events(dir.path());
    assert!(events.iter().filter(|e| e["accepted"] == true).all(|e| {
        e["target"]
            .as_str()
            .unwrap()
            .starts_with(&format!("rtsps://localhost:{port}/"))
    }));
    assert!(
        std::fs::read_to_string(dir.path().join("gateway.log"))
            .unwrap()
            .is_empty()
    );
}

async fn auth_control(socket: &mut tokio::net::TcpStream) -> (String, u32, String) {
    use tokio::io::AsyncReadExt;
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        assert!(header.len() < 16384);
        header.push(socket.read_u8().await.unwrap());
    }
    let text = String::from_utf8(header).unwrap();
    let value = |key: &str| {
        text.lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(k, _)| k.eq_ignore_ascii_case(key))
                    .map(|(_, v)| v.trim())
            })
            .unwrap()
    };
    let seq = value("cseq").parse().unwrap();
    let size: usize = value("content-length").parse().unwrap();
    assert!(size <= 65536);
    let mut body = vec![0; size];
    socket.read_exact(&mut body).await.unwrap();
    (text.split(' ').next().unwrap().to_owned(), seq, text)
}

#[tokio::test]
async fn authentication_retries_share_the_startup_deadline_and_stop_closes_pending_auth() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for cancel in [false, true] {
        let dir = labdir();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let (method, seq, request) = auth_control(&mut socket).await;
            assert_eq!(method, "ANNOUNCE");
            assert!(!request.contains("Authorization:"));
            if !cancel {
                tokio::time::sleep(Duration::from_millis(2800)).await;
            }
            socket.write_all(format!("RTSP/1.0 401 Unauthorized\r\nCSeq: {seq}\r\nWWW-Authenticate: Basic realm=\"owned\"\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
            let (method, next, request) = auth_control(&mut socket).await;
            assert_eq!(method, "ANNOUNCE");
            assert_eq!(next, seq + 1);
            assert!(request.lines().any(|line| {
                line.split_once(':').is_some_and(|(key, value)| {
                    key.eq_ignore_ascii_case("authorization")
                        && value.trim() == "Basic dXNlcjpvd25lZC1zZWNyZXQ="
                })
            }));
            assert!(!request.contains("user:owned-secret@"));
            if !cancel {
                tokio::time::sleep(Duration::from_millis(2800)).await;
                socket
                    .write_all(
                        format!("RTSP/1.0 200 OK\r\nCSeq: {next}\r\nContent-Length: 0\r\n\r\n")
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                let (method, _, request) = auth_control(&mut socket).await;
                assert_eq!(method, "SETUP");
                assert!(request.lines().any(|line| {
                    line.split_once(':').is_some_and(|(key, value)| {
                        key.eq_ignore_ascii_case("authorization")
                            && value.trim() == "Basic dXNlcjpvd25lZC1zZWNyZXQ="
                    })
                }));
            }
            ready.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(4), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0,
                "pending authentication/setup must close without media"
            );
        });
        let engine = owned_engine(dir.path());
        let started = tokio::time::Instant::now();
        let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://user:owned-secret@127.0.0.1:{port}/owned"),"connect_timeout":4,"retry_timeout":30}]})).await.unwrap();
        tokio::time::timeout(Duration::from_secs(9), waiting)
            .await
            .unwrap()
            .unwrap();
        if cancel {
            tokio::time::timeout(Duration::from_secs(2), engine.stop_all())
                .await
                .unwrap();
        } else {
            let stats = wait_push(&worker, 0, "retrying").await;
            assert_eq!(stats["last_error"], "push_setup_timeout");
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "challenge retry extended the startup deadline: {:?}",
                started.elapsed()
            );
            engine.stop_all().await;
        }
        assert_eq!(worker.stats()["flussonix_pushes"][0]["rtp_bytes"], 0);
        assert_eq!(worker.stats()["flussonix_pushes"][0]["attempts"], 1);
        task.await.unwrap();
    }
}

#[tokio::test]
async fn authenticated_and_unsigned_destinations_share_worker_without_credential_crossover() {
    let dir = labdir();
    let good = dir.path().join("authenticated.ts");
    let plain = dir.path().join("unsigned.ts");
    let denied_dir = dir.path().join("denied");
    std::fs::create_dir(&denied_dir).unwrap();
    let (good_rx_port, mut good_rx) = receiver(&good).await;
    let (plain_port, mut plain_rx) = receiver(&plain).await;
    let (denied_rx_port, mut denied_rx) = receiver(&denied_dir.join("denied.ts")).await;
    let (good_port, mut good_gateway) =
        authentication_gateway(dir.path(), good_rx_port, "basic", false, false, None).await;
    let (denied_port, mut denied_gateway) =
        authentication_gateway(&denied_dir, denied_rx_port, "MD5", false, false, None).await;
    let engine = owned_engine(dir.path());
    let worker = engine.ensure("owned", &json!({"inputs":[{"url":"testsrc://"}],"pushes":[
        {"url":format!("rtsp://user%20name:owned%3Asecret@127.0.0.1:{good_port}/owned?token=owned-query"),"retry_timeout":30},
        {"url":format!("rtsp://127.0.0.1:{plain_port}/owned"),"retry_timeout":30},
        {"url":format!("rtsp://user%20name:wrong-secret@127.0.0.1:{denied_port}/owned?token=owned-query"),"retry_timeout":30}
    ]})).await.unwrap();
    wait_push(&worker, 0, "sending").await;
    wait_push(&worker, 1, "sending").await;
    let denied = wait_push(&worker, 2, "retrying").await;
    assert_eq!(denied["rtp_bytes"], 0);
    assert_eq!(denied["last_error"], "push_auth_rejected");
    assert_eq!(engine.count().await, 1);
    received(&mut good_rx, &good, Some("h264"), "aac").await;
    received(&mut plain_rx, &plain, Some("h264"), "aac").await;
    engine.stop_all().await;
    good_gateway.wait().await.unwrap();
    denied_gateway.wait().await.unwrap();
    denied_rx.kill().await.unwrap();
    denied_rx.wait().await.unwrap();
    assert_eq!(auth_events(&denied_dir).len(), 2);
    assert_eq!(received_auth_media(&denied_dir), 0);
    assert!(received_auth_media(dir.path()) > 20);
    assert!(
        std::fs::read_to_string(dir.path().join("gateway.log"))
            .unwrap()
            .is_empty()
    );
    assert!(
        std::fs::read_to_string(denied_dir.join("gateway.log"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn udp_push_roundtrips_templates_and_rejects_secure_or_invalid_transport() {
    let dir = labdir();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    let entries = json!([{"url":"rtsp://localhost/owned","rtsp_transport":"udp","disabled":true}]);
    store
        .put("templates", "udp", json!({"pushes":entries}))
        .expect("UDP push must configure");
    store
        .put(
            "streams",
            "owned",
            json!({"template":"udp","inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    assert_eq!(store.effective("owned").unwrap()["pushes"], entries);
    for entry in [
        json!({"url":"rtsps://localhost/owned","rtsp_transport":"udp"}),
        json!({"url":"rtsp://localhost/owned","rtsp_transport":"multicast"}),
        json!({"url":"rtsp://localhost/owned","rtsp_transport":1}),
    ] {
        assert!(
            store
                .put("streams", "bad", json!({"pushes":[entry]}))
                .is_err()
        );
    }
}

#[tokio::test]
async fn independent_udp_receiver_decodes_all_six_codec_pairs_and_releases_owned_processes() {
    use futures_util::FutureExt;
    for (video, encoder) in [("h264", "libx264"), ("hevc", "libx265")] {
        for (audio, acodec, ab) in [
            ("aac", "aac", 96),
            ("mp2", "mp2a", 192),
            ("mp3", "mp3", 128),
        ] {
            let dir = labdir();
            let path = dir.path().join("received.ts");
            let (port, mut receiver) = receiver_transport(&path, 3, "udp").await;
            let engine = owned_engine(dir.path());
            let result=std::panic::AssertUnwindSafe(async {
                let cfg=json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":encoder,"acodec":acodec,"ab":ab},"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned?token=owned-secret"),"rtsp_transport":"udp","retry_timeout":30}]});
                let worker=engine.ensure("owned",&cfg).await.expect("UDP push must start");
                let stats=wait_push(&worker,0,"sending").await;
                assert!(stats["rtp_bytes"].as_u64().unwrap()>0);
                assert_eq!(stats["transport"],"udp");
                assert!(!worker.stats().to_string().contains("owned-secret"));
                received(&mut receiver,&path,Some(video),audio).await;
            }).catch_unwind().await;
            if receiver.try_wait().unwrap().is_none() {
                receiver.kill().await.unwrap();
            }
            let _ = receiver.wait().await;
            engine.stop_all().await;
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

#[tokio::test]
async fn udp_transport_substitution_closes_before_record_and_reclaims_ports() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for tail in [
        ";server_port=6000-6001;source=127.0.0.2",
        ";server_port=6001-6002",
        ";server_port=6000-6001;server_port=6002-6003",
        "",
        "collision",
    ] {
        let dir = labdir();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let (method, seq, _) = auth_control(&mut socket).await;
            assert_eq!(method, "ANNOUNCE");
            socket
                .write_all(
                    format!("RTSP/1.0 200 OK\r\nCSeq: {seq}\r\nContent-Length: 0\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let (method, seq, wire) = auth_control(&mut socket).await;
            assert_eq!(method, "SETUP");
            let value = wire
                .lines()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(k, _)| k.eq_ignore_ascii_case("transport"))
                        .map(|(_, v)| v.trim())
                })
                .unwrap();
            assert!(value.starts_with("RTP/AVP;unicast;client_port="));
            assert!(!value.contains("interleaved"));
            let pair = value
                .split("client_port=")
                .nth(1)
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .to_owned();
            let response_tail = if tail == "collision" {
                ";server_port=6000-6001"
            } else {
                tail
            };
            socket.write_all(format!("RTSP/1.0 200 OK\r\nCSeq: {seq}\r\nSession: owned\r\nTransport: RTP/AVP;unicast;client_port={pair}{response_tail}\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
            let pair = if tail == "collision" {
                let (method, seq, wire) = auth_control(&mut socket).await;
                assert_eq!(method, "SETUP");
                let second = wire
                    .lines()
                    .find_map(|l| {
                        l.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("transport"))
                            .map(|(_, v)| v.trim())
                    })
                    .unwrap()
                    .split("client_port=")
                    .nth(1)
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap()
                    .to_owned();
                socket.write_all(format!("RTSP/1.0 200 OK\r\nCSeq: {seq}\r\nSession: owned\r\nTransport: RTP/AVP;unicast;client_port={second};server_port=6000-6001\r\nContent-Length: 0\r\n\r\n").as_bytes()).await.unwrap();
                format!("{pair},{second}")
            } else {
                pair
            };
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0,
                "bad transport must close without RECORD/media"
            );
            pair
        });
        let engine = owned_engine(dir.path());
        let result = std::panic::AssertUnwindSafe(async {
            let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned"),"rtsp_transport":"udp","retry_timeout":30}]})).await.unwrap();
            let stats = wait_push(&worker, 0, "retrying").await;
            assert_eq!(stats["rtp_bytes"], 0);
            task.await.unwrap()
        });
        use futures_util::FutureExt;
        let result = result.catch_unwind().await;
        engine.stop_all().await;
        match result {
            Ok(pair) => {
                for pair in pair.split(',') {
                    let (a, b) = pair.split_once('-').unwrap();
                    let a = tokio::net::UdpSocket::bind(("127.0.0.1", a.parse::<u16>().unwrap()))
                        .await
                        .unwrap();
                    let b = tokio::net::UdpSocket::bind(("127.0.0.1", b.parse::<u16>().unwrap()))
                        .await
                        .unwrap();
                    drop((a, b));
                }
            }
            Err(p) => std::panic::resume_unwind(p),
        }
    }
}

#[tokio::test]
async fn independent_udp_basic_and_digest_receivers_authenticate_and_decode() {
    use futures_util::FutureExt;
    for profile in ["basic", "SHA-256"] {
        let dir = labdir();
        let path = dir.path().join("authenticated-udp.ts");
        let (rx_port, mut rx) = receiver_transport(&path, 3, "udp").await;
        let (port, mut gateway) =
            authentication_gateway(dir.path(), rx_port, profile, false, false, None).await;
        let engine = owned_engine(dir.path());
        let result=std::panic::AssertUnwindSafe(async {
            let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://user%20name:owned%3Asecret@127.0.0.1:{port}/owned?token=owned-query"),"rtsp_transport":"udp","retry_timeout":30}]})).await.unwrap();
            wait_push(&worker,0,"sending").await;received(&mut rx,&path,Some("h264"),"aac").await;
            let events=auth_events(dir.path());assert_eq!(events.iter().filter(|e|e["accepted"]==false).count(),1);
            for method in ["ANNOUNCE","SETUP","RECORD"] {assert!(events.iter().any(|e|e["method"]==method && e["accepted"]==true));}
        }).catch_unwind().await;
        engine.stop_all().await;
        for child in [&mut rx, &mut gateway] {
            if child.try_wait().unwrap().is_none() {
                let _ = child.kill().await;
            }
            let _ = child.wait().await;
        }
        if let Err(p) = result {
            std::panic::resume_unwind(p);
        }
    }
}

#[tokio::test]
async fn udp_push_updates_process_uplink_and_keeps_lifetime_bytes_after_stop() {
    use flussonix::server::{App, Options, router};
    use futures_util::FutureExt;
    use tower::ServiceExt;
    let dir = labdir();
    let path = dir.path().join("uplink.ts");
    let (port, mut receiver) = receiver_transport(&path, 3, "udp").await;
    let source = App::new(
        dir.path().join("source.json"),
        dir.path().join("source-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-key".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    source.sample_metrics();
    let result=std::panic::AssertUnwindSafe(async {
        let worker=source.media.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("rtsp://127.0.0.1:{port}/owned"),"rtsp_transport":"udp","retry_timeout":30}]})).await.unwrap();
        wait_push(&worker,0,"sending").await;tokio::time::sleep(Duration::from_millis(250)).await;source.sample_metrics();
        let request=axum::http::Request::builder().uri("/flussonix/api/v1/node").header("Authorization","Basic YWRtaW46b3duZWQtYWRtaW4=").body(axum::body::Body::empty()).unwrap();
        let response=router(source.clone()).oneshot(request).await.unwrap();assert_eq!(response.status(),200);
        let body=axum::body::to_bytes(response.into_body(),1048576).await.unwrap();let node:Value=serde_json::from_slice(&body).unwrap();
        assert!(node["egress_mbps"].as_f64().unwrap()>0.0,"UDP push must contribute to process uplink: {node}");
        assert!(node["rtsp_egress_mbps"].as_f64().unwrap()>0.0);
        assert!(node["bytes_out"].as_u64().unwrap()>=worker.stats()["flussonix_pushes"][0]["rtp_bytes"].as_u64().unwrap());
        received(&mut receiver,&path,Some("h264"),"aac").await;node["bytes_out"].as_u64().unwrap()
    }).catch_unwind().await;
    source.media.stop_all().await;
    if receiver.try_wait().unwrap().is_none() {
        let _ = receiver.kill().await;
    }
    let _ = receiver.wait().await;
    let before = match result {
        Ok(before) => before,
        Err(p) => std::panic::resume_unwind(p),
    };
    source.sample_metrics();
    let request = axum::http::Request::builder()
        .uri("/flussonix/api/v1/node")
        .header("Authorization", "Basic YWRtaW46b3duZWQtYWRtaW4=")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = router(source.clone()).oneshot(request).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), 1048576)
        .await
        .unwrap();
    let node: Value = serde_json::from_slice(&body).unwrap();
    assert!(node["bytes_out"].as_u64().unwrap() >= before);
}
