#![cfg(all(target_os = "linux", target_env = "gnu"))]

use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};

async fn daemon(dir: &Path) -> (Child, String) {
    daemon_with_preload(dir, None).await
}
async fn daemon_with_preload(dir: &Path, preload: Option<&Path>) -> (Child, String) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_flussonix"));
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("FLUSSONIX_")) {
        command.env_remove(name);
    }
    command.env_remove("LD_PRELOAD");
    if let Some(preload) = preload {
        command.env("LD_PRELOAD", preload);
    }
    let mut child = command
        .args(["--listen", "127.0.0.1:0"])
        .arg("--config")
        .arg(dir.join("config.json"))
        .arg("--media-dir")
        .arg(dir.join("media"))
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-memory-admin")
        .env("FLUSSONIX_PEER_KEY", "owned-memory-peer-secret")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let status: Value = serde_json::from_str(&line).unwrap();
    (
        child,
        format!("http://{}", status["listen"].as_str().unwrap()),
    )
}
async fn node(client: &reqwest::Client, base: &str) -> Value {
    client
        .get(format!("{base}/flussonix/api/v1/node"))
        .basic_auth("admin", Some("owned-memory-admin"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn stop(mut child: Child) {
    // SAFETY: Signal only the exact live daemon owned by this test.
    assert_eq!(
        unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGTERM) },
        0
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(8), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap()
}

#[tokio::test]
async fn daemon_reports_supported_idle_cleanup_without_releasing_at_empty_boot() {
    let dir = tempfile::tempdir().unwrap();
    let (child, base) = daemon(dir.path()).await;
    let value = node(&client(), &base).await;
    stop(child).await;
    let cleanup = &value["idle_memory_cleanup"];
    assert_eq!(
        cleanup["supported"], true,
        "daemon must report idle cleanup support"
    );
    assert_eq!(cleanup["enabled"], true);
    assert_eq!(cleanup["runs"], 0);
    assert_eq!(cleanup["active_operations"], 0);
    assert_eq!(cleanup["idle_grace_ms"], 5000);
    assert_eq!(cleanup["cooldown_ms"], 300000);
}

#[tokio::test]
async fn automatic_cleanup_waits_for_worker_retirement_then_runs_once() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("config.json"),
        serde_json::to_vec(&json!({
            "streams":[{"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]}],
            "templates":[],"sources":[],"peers":[],"auth_backends":[]
        }))
        .unwrap(),
    )
    .unwrap();
    let (child, base) = daemon(dir.path()).await;
    let client = client();
    let mut response = client
        .get(format!("{base}/owned/mpegts"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(!response.chunk().await.unwrap().unwrap().is_empty());
    let active = node(&client, &base).await;
    assert_eq!(active["idle_memory_cleanup"]["runs"], 0);
    assert!(
        active["idle_memory_cleanup"]["active_operations"]
            .as_u64()
            .unwrap()
            > 0
    );
    drop(response);
    client
        .post(format!("{base}/streamer/api/v3/streams/owned/stop"))
        .basic_auth("admin", Some("owned-memory-admin"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let retired = node(&client, &base).await;
    assert_eq!(
        retired["idle_memory_cleanup"]["runs"], 0,
        "must respect idle grace after retirement"
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let value = node(&client, &base).await;
            if value["idle_memory_cleanup"]["runs"] == 1
                && value["idle_memory_cleanup"]["last_duration_ms"].is_number()
            {
                assert_eq!(value["idle_memory_cleanup"]["active_operations"], 0);
                assert!(value["idle_memory_cleanup"]["last_duration_ms"].is_number());
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(node(&client, &base).await["idle_memory_cleanup"]["runs"], 1);
    let mut response = client
        .get(format!("{base}/owned/mpegts"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert!(!response.chunk().await.unwrap().unwrap().is_empty());
    assert_eq!(node(&client, &base).await["idle_memory_cleanup"]["runs"], 1);
    drop(response);
    client
        .post(format!("{base}/streamer/api/v3/streams/owned/stop"))
        .basic_auth("admin", Some("owned-memory-admin"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(
        node(&client, &base).await["idle_memory_cleanup"]["runs"],
        1,
        "a new idle epoch cannot bypass the cooldown"
    );
    stop(child).await;
}

#[tokio::test]
async fn unsupported_allocator_entry_point_never_runs_automatic_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("owned-unsupported.c");
    let library = dir.path().join("owned-unsupported.so");
    std::fs::write(
        &source,
        "#include <stddef.h>\n#include <unistd.h>\nint malloc_trim(size_t pad) { _exit(97); }\n",
    )
    .unwrap();
    let output = std::process::Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "owned allocator fixture did not compile"
    );
    let (child, base) = daemon_with_preload(dir.path(), Some(&library)).await;
    let client = client();
    let denied = client
        .get(format!("{base}/missing/index.m3u8"))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 404);
    let _ = denied.bytes().await.unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    let value = node(&client, &base).await;
    stop(child).await;
    assert_eq!(value["idle_memory_cleanup"]["supported"], false);
    assert_eq!(value["idle_memory_cleanup"]["enabled"], false);
    assert_eq!(value["idle_memory_cleanup"]["runs"], 0);
}
