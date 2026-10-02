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
