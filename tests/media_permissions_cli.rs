#![cfg(unix)]

use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::Path, process::Stdio, time::Duration};
use tokio::{io::AsyncBufReadExt, io::BufReader, process::Command};

fn command(directory: &Path, media: &Path) -> Command {
    let mut command = Command::new("/bin/sh");
    command
        .args([
            "-c",
            "umask 022; exec \"$@\"",
            "sh",
            env!("CARGO_BIN_EXE_flussonix"),
        ])
        .args(["--listen", "127.0.0.1:0", "--config"])
        .arg(directory.join("config.json"))
        .arg("--media-dir")
        .arg(media)
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-media-test-admin")
        .env("FLUSSONIX_PEER_KEY", "owned-media-test-peer-key")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

fn find_playlist(directory: &Path) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(directory).ok()? {
        let path = entry.ok()?.path();
        if path.file_name().is_some_and(|name| name == "index.m3u8") {
            return Some(path);
        }
        if path.is_dir() {
            if let Some(found) = find_playlist(&path) {
                return Some(found);
            }
        }
    }
    None
}

#[tokio::test]
async fn direct_daemon_creates_private_media_and_hls_under_umask_022() {
    let dir = tempfile::tempdir().unwrap();
    let media = dir.path().join("media");
    std::fs::write(
        dir.path().join("config.json"),
        json!({"streams":[{"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]}],
               "templates":[],"sources":[],"peers":[],"auth_backends":[]})
        .to_string(),
    )
    .unwrap();
    let mut child = command(dir.path(), &media).spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let status: Value = serde_json::from_str(&line).unwrap();
    let base = format!("http://{}", status["listen"].as_str().unwrap());
    assert_eq!(
        std::fs::metadata(&media).unwrap().permissions().mode() & 0o777,
        0o700
    );

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let playlist = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let _ = client.get(format!("{base}/owned/index.m3u8")).send().await;
            if let Some(path) = find_playlist(&media) {
                if std::fs::metadata(&path).unwrap().len() > 0 {
                    break path;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("HLS playlist was not created");
    assert_eq!(
        std::fs::metadata(playlist).unwrap().permissions().mode() & 0o077,
        0
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

#[tokio::test]
async fn direct_daemon_rejects_existing_permissive_media_root_without_modifying_it() {
    let dir = tempfile::tempdir().unwrap();
    let media = dir.path().join("media");
    std::fs::create_dir(&media).unwrap();
    std::fs::set_permissions(&media, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), command(dir.path(), &media).output())
        .await
        .expect("unsafe media root unexpectedly started a daemon")
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("media directory"),
        "unexpected startup failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::metadata(&media).unwrap().permissions().mode() & 0o777,
        0o755
    );
}

#[tokio::test]
async fn direct_daemon_rejects_a_media_root_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("private");
    std::fs::create_dir(&target).unwrap();
    std::os::unix::fs::symlink(&target, dir.path().join("media")).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        command(dir.path(), &dir.path().join("media")).output(),
    )
    .await
    .expect("symlinked media root unexpectedly started a daemon")
    .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("media directory"));
}
