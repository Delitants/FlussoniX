use serde_json::Value;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
fn command(dir: &std::path::Path, address: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_flussonix"));
    cmd.args(["--listen", "127.0.0.1:0", "--srt-play-listen", address])
        .arg("--config")
        .arg(dir.join("config.json"))
        .arg("--media-dir")
        .arg(dir.join("media"))
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin-secret")
        .env("FLUSSONIX_PEER_KEY", "owned-cli-peer-secret")
        .env("FLUSSONIX_SRT_PLAY_PASSPHRASE", "owned-hidden-passphrase")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}
#[tokio::test]
async fn cli_exposes_actual_encrypted_listener_and_stops_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = command(dir.path(), "127.0.0.1:0").spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .expect("No owned startup status");
    let status: Value = serde_json::from_str(&line).unwrap();
    assert!(!line.contains("owned-hidden-passphrase"));
    let address = status["srt_play_listen"]
        .as_str()
        .expect("No SRT listener status");
    assert_ne!(address.rsplit(':').next().unwrap(), "0");
    let url = format!(
        "http://{}/flussonix/api/v1/node",
        status["listen"].as_str().unwrap()
    );
    let node = reqwest::Client::new()
        .get(url)
        .basic_auth("admin", Some("owned-admin-secret"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(node["srt_playback"]["listen"], address);
    assert_eq!(node["srt_playback"]["encrypted"], true);
    assert_eq!(node["srt_playback"]["client_limit"], 128);
    assert_eq!(node["srt_playback"]["latency_ms"], 120);
    assert_eq!(node["srt_playback"]["direct_tokens_allowed"], false);
    assert!(node["srt_playback"]["public_url"].is_null());
    assert!(!node.to_string().contains("owned-hidden-passphrase"));
    // SAFETY: Signal only the exact live child spawned by this test.
    assert_eq!(
        unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGTERM) },
        0
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}
#[tokio::test]
async fn public_url_requires_srt_listener_and_rejects_unreachable_or_credentialed_roots() {
    for raw in [
        "srt://0.0.0.0:9000",
        "srt://[::]:9000",
        "srt://user:secret@127.0.0.1:9000",
        "srt://127.0.0.1:9000/stream",
        "srt://127.0.0.1:9000?passphrase=secret",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let output = command(dir.path(), "127.0.0.1:0")
            .args(["--srt-play-public-url", raw])
            .output();
        let output = tokio::time::timeout(Duration::from_secs(5), output)
            .await
            .expect("invalid public SRT URL unexpectedly started a daemon")
            .unwrap();
        assert!(!output.status.success(), "accepted {raw}");
        assert!(!dir.path().join("config.json").exists());
    }
    let dir = tempfile::tempdir().unwrap();
    let mut child = command(dir.path(), "127.0.0.1:0")
        .args([
            "--srt-play-public-url",
            "srt://127.0.0.1:9000",
            "--srt-play-allow-direct-token",
        ])
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let status: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(status["srt_play_public_url"], "srt://127.0.0.1:9000");
    assert_eq!(status["srt_play_direct_tokens_allowed"], true);
    child.kill().await.unwrap();
    let _ = child.wait().await;

    let output = Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .args(["--srt-play-public-url", "srt://127.0.0.1:9000"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    let output = Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .arg("--srt-play-allow-direct-token")
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
}
#[tokio::test]
async fn wildcard_listener_can_start_without_advertising_a_public_url() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = command(dir.path(), "0.0.0.0:0").spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let status: Value = serde_json::from_str(&line).unwrap();
    assert!(
        status["srt_play_listen"]
            .as_str()
            .unwrap()
            .starts_with("0.0.0.0:")
    );
    assert!(status["srt_play_public_url"].is_null());
    child.kill().await.unwrap();
    let _ = child.wait().await;
}
#[tokio::test]
async fn invalid_secret_limits_and_occupied_udp_port_fail_before_config_or_workers() {
    for option in ["secret", "latency", "port"] {
        let dir = tempfile::tempdir().unwrap();
        let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = if option == "port" {
            occupied.local_addr().unwrap().to_string()
        } else {
            "127.0.0.1:0".into()
        };
        let mut cmd = command(dir.path(), &address);
        if option == "secret" {
            cmd.env("FLUSSONIX_SRT_PLAY_PASSPHRASE", "owned-invalid\nsecret");
        }
        if option == "latency" {
            cmd.args(["--srt-play-latency", "0"]);
        }
        let output = tokio::time::timeout(Duration::from_secs(5), cmd.output())
            .await
            .unwrap()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains("owned-invalid") && !stderr.contains("owned-hidden-passphrase"));
        assert!(!dir.path().join("config.json").exists());
    }
}
