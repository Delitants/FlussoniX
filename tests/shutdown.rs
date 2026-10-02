//! A real process must terminate and reap its worker while a live response stays open.
use serde_json::json;
use std::{process::Stdio, time::Duration};
#[tokio::test]
async fn sigterm_with_active_ts_viewer_drains_and_reaps_worker() {
    let d = tempfile::tempdir().unwrap();
    let config = d.path().join("config.json");
    std::fs::write(&config,json!({"streams":[{"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]}],"templates":[],"peers":[],"sources":[],"auth_backends":[]}).to_string()).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .args(["--listen", &address.to_string(), "--config"])
        .arg(&config)
        .arg("--media-dir")
        .arg(d.path().join("media"))
        .env("FLUSSONIX_ADMIN_PASSWORD", "shutdown-test-admin")
        .env("FLUSSONIX_PEER_KEY", "shutdown-peer-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for _ in 0..100 {
        if client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let live = tokio::time::timeout(
        Duration::from_secs(10),
        client.get(format!("http://{address}/owned/mpegts")).send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(live.status(), 200);
    let status: serde_json::Value = client
        .get(format!("http://{address}/streamer/api/v3/streams/owned"))
        .basic_auth("admin", Some("shutdown-test-admin"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let worker = status["stats"]["pid"].as_u64().unwrap();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let result = tokio::time::timeout(Duration::from_secs(8), child.wait()).await;
    assert!(
        result.is_ok(),
        "SIGTERM must complete while the TS response is held open"
    );
    assert!(result.unwrap().unwrap().success());
    assert!(!std::path::Path::new(&format!("/proc/{worker}")).exists());
    drop(live);
}
#[tokio::test]
async fn optional_rtsp_listener_is_supervised_and_drained_on_sigterm() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let d = tempfile::tempdir().unwrap();
    let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_address = http.local_addr().unwrap();
    let rtsp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rtsp_address = rtsp.local_addr().unwrap();
    drop(http);
    drop(rtsp);
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .args([
            "--listen",
            &http_address.to_string(),
            "--rtsp-listen",
            &rtsp_address.to_string(),
            "--config",
        ])
        .arg(d.path().join("config.json"))
        .arg("--media-dir")
        .arg(d.path().join("media"))
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
        .env("FLUSSONIX_PEER_KEY", "owned-peer-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut socket = None;
    for _ in 0..100 {
        if let Ok(s) = tokio::net::TcpStream::connect(rtsp_address).await {
            socket = Some(s);
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "daemon must accept the optional RTSP listener flag"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut socket = socket.expect("RTSP listener ready");
    socket
        .write_all(b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n")
        .await
        .unwrap();
    let mut response = [0; 1024];
    let n = socket.read(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response[..n]).starts_with("RTSP/1.0 200"));
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(8), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}
