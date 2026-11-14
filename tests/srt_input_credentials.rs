use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::process::Command;

#[tokio::test]
async fn encrypted_srt_input_delivers_media_without_exposing_upstream_credentials_in_argv() {
    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dir = tempfile::tempdir().unwrap();
    let mut source = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=15",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "12",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "30",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            "-passphrase",
            "owned-input-secret-123",
            "-pbkeylen",
            "24",
        ])
        .arg(format!(
            "srt://127.0.0.1:{port}?mode=listener&listen_timeout=15000000&tsbpd=1"
        ))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let worker = engine
        .ensure(
            "owned",
            &json!({"inputs":[{"url":format!("srt://127.0.0.1:{port}?mode=caller&passphrase=owned-input-secret-123&pbkeylen=24&tsbpdmode=1&streamid=owned-sensitive-id")}]}),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        while worker.bytes.load(Ordering::Relaxed) < 32_000 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("SRT input did not deliver media: {}", worker.stats()));
    let pid = worker.pid();
    assert!(pid > 0);
    let args = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap();
    for secret in ["owned-input-secret-123", "owned-sensitive-id"] {
        assert!(
            !args
                .windows(secret.len())
                .any(|part| part == secret.as_bytes()),
            "SRT input FFmpeg argv exposed a credential"
        );
    }
    engine.stop_all().await;
    let _ = source.kill().await;
    let _ = source.wait().await;
}

#[tokio::test]
async fn encrypted_srt_listener_receives_aes256_without_tsbpd_or_argv_secrets() {
    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let worker = engine
        .ensure(
            "owned",
            &json!({"inputs":[{"url":format!("srt://127.0.0.1:{port}?mode=listener&passphrase=owned-listener-secret&pbkeylen=32&tsbpdmode=0&listen_timeout=10000000")}]}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut source = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=15",
            "-t",
            "12",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "30",
            "-an",
            "-f",
            "mpegts",
            "-passphrase",
            "owned-listener-secret",
            "-pbkeylen",
            "32",
        ])
        .arg(format!("srt://127.0.0.1:{port}?mode=caller&tsbpd=0"))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        while worker.bytes.load(Ordering::Relaxed) < 16_000 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("SRT listener did not deliver media: {}", worker.stats()));
    let args = std::fs::read(format!("/proc/{}/cmdline", worker.pid())).unwrap();
    assert!(
        !args
            .windows("owned-listener-secret".len())
            .any(|part| part == b"owned-listener-secret")
    );
    engine.stop_all().await;
    let _ = source.kill().await;
    let _ = source.wait().await;
}

#[tokio::test]
async fn srt_input_accepts_literal_hash_inside_a_flussonic_style_passphrase() {
    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dir = tempfile::tempdir().unwrap();
    let mut source = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=15",
            "-t",
            "12",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "30",
            "-an",
            "-f",
            "mpegts",
            "-passphrase",
            "owned#input-secret-123",
            "-pbkeylen",
            "16",
        ])
        .arg(format!(
            "srt://127.0.0.1:{port}?mode=listener&listen_timeout=15000000&tsbpd=0"
        ))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let worker = engine
        .ensure(
            "owned",
            &json!({"inputs":[{"url":format!("srt://127.0.0.1:{port}#passphrase=owned#input-secret-123")}]}),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        while worker.bytes.load(Ordering::Relaxed) < 16_000 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "SRT hash-secret source did not deliver media: {}",
            worker.stats()
        )
    });
    let args = std::fs::read(format!("/proc/{}/cmdline", worker.pid())).unwrap();
    assert!(
        !args
            .windows("owned#input-secret-123".len())
            .any(|part| part == b"owned#input-secret-123")
    );
    engine.stop_all().await;
    let _ = source.kill().await;
    let _ = source.wait().await;
}

#[tokio::test]
async fn wrong_srt_input_secret_starts_no_media_and_reaps_decoder() {
    let port = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dir = tempfile::tempdir().unwrap();
    let mut source = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=15",
            "-t",
            "10",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-an",
            "-f",
            "mpegts",
            "-passphrase",
            "correct-input-secret",
        ])
        .arg(format!(
            "srt://127.0.0.1:{port}?mode=listener&listen_timeout=8000000"
        ))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let worker = engine
        .ensure(
            "owned",
            &json!({"inputs":[{"url":format!("srt://127.0.0.1:{port}?passphrase=wrong-input-secret&connect_timeout=2000")}]}),
        )
        .await
        .unwrap();
    let pid = worker.pid();
    assert!(pid > 0);
    let args = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap();
    assert!(
        !args
            .windows("wrong-input-secret".len())
            .any(|part| part == b"wrong-input-secret")
    );
    tokio::time::timeout(Duration::from_secs(6), async {
        while worker.alive.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("bad SRT key was not rejected: {}", worker.stats()));
    assert_eq!(worker.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(worker.pid(), 0);
    engine.stop_all().await;
    let _ = source.kill().await;
    let _ = source.wait().await;
}
