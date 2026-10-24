#![cfg(target_os = "linux")]

use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::{Child, Command};

const ADMIN_PASSWORD: &str = "owned-iptv-admin";
const PEER_KEY: &str = "owned-iptv-peer-key";

struct Daemon {
    child: Child,
    client: Client,
    base: String,
}

impl Daemon {
    async fn start(root: &Path, port: u16) -> Self {
        let base = format!("http://127.0.0.1:{port}");
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("daemon.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_flussonix"));
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
            command.env_remove(key);
        }
        command
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .arg("--config")
            .arg(root.join("state/config.json"))
            .arg("--iptv-data")
            .arg(root.join("state/iptv.json"))
            .arg("--media-dir")
            .arg(root.join("media"))
            .args(["--iptv-public-origin", &base])
            .env("PATH", "/usr/bin:/bin")
            .env_remove("LD_LIBRARY_PATH")
            .env("FLUSSONIX_ADMIN_PASSWORD", ADMIN_PASSWORD)
            .env("FLUSSONIX_PEER_KEY", PEER_KEY)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let client = Client::new();
        for _ in 0..100 {
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status() == StatusCode::OK)
            {
                return Self {
                    child,
                    client,
                    base,
                };
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "isolated IPTV daemon exited before readiness"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("isolated IPTV daemon did not become ready");
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn edit(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Response {
        let mut request = self
            .client
            .request(method, self.url(path))
            .basic_auth("admin", Some(ADMIN_PASSWORD));
        if let Some(body) = body {
            request = request.json(&body);
        }
        tokio::time::timeout(Duration::from_secs(10), request.send())
            .await
            .unwrap()
            .unwrap()
    }

    async fn put(&self, path: &str, body: Value) -> Response {
        self.edit(reqwest::Method::PUT, path, Some(body)).await
    }

    async fn get(&self, path: &str) -> Response {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.client.get(self.url(path)).send(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn stop(mut self) {
        // SAFETY: This PID belongs to the child this test spawned and still owns.
        assert_eq!(
            unsafe { libc::kill(self.child.id().unwrap() as i32, libc::SIGTERM) },
            0
        );
        let result = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("isolated IPTV daemon shutdown timed out")
            .unwrap();
        assert!(
            result.success(),
            "isolated IPTV daemon failed to stop cleanly"
        );
    }
}

fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn hls_segment(daemon: &Daemon) -> Vec<u8> {
    for _ in 0..70 {
        let playlist_url = daemon.url("/news/index.m3u8?token=first-token");
        let response = daemon.get("/news/index.m3u8?token=first-token").await;
        if response.status() == StatusCode::OK {
            let body = tokio::time::timeout(Duration::from_secs(5), response.text())
                .await
                .unwrap()
                .unwrap();
            if let Some(segment) = body
                .lines()
                .find(|line| !line.is_empty() && !line.starts_with('#'))
            {
                let path = url::Url::parse(&playlist_url)
                    .unwrap()
                    .join(segment)
                    .unwrap();
                let media = daemon.client.get(path).send().await.unwrap();
                if media.status() == StatusCode::OK {
                    let bytes = tokio::time::timeout(Duration::from_secs(10), media.bytes())
                        .await
                        .unwrap()
                        .unwrap();
                    if !bytes.is_empty() {
                        return bytes.to_vec();
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("subscriber HLS segment did not become ready");
}

async fn independently_decode_one_frame(path: PathBuf) {
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        Command::new("/usr/bin/ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(path)
            .args([
                "-map",
                "0:v:0",
                "-frames:v",
                "1",
                "-pix_fmt",
                "gray",
                "-f",
                "rawvideo",
                "-",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("independent HLS decode timed out")
    .unwrap();
    assert!(
        output.status.success(),
        "independent HLS video decode failed"
    );
    assert!(
        !output.stdout.is_empty(),
        "HLS segment decoded no video frame"
    );
}

#[tokio::test]
async fn subscriber_api_playlist_media_revoke_restart_and_failed_save_use_external_data() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("state")).unwrap();
    let port = unused_port();
    let daemon = Daemon::start(root.path(), port).await;
    assert_eq!(
        daemon
            .put("/streamer/api/v3/iptv", json!({}))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        daemon
            .put(
                "/streamer/api/v3/streams/news",
                json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"})
            )
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        daemon
            .put(
                "/streamer/api/v3/iptv/packages/basic",
                json!({"channels":["news"]})
            )
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        daemon
            .put(
                "/streamer/api/v3/iptv/subscribers/alice",
                json!({"token":"first-token","packages":["basic"],"max_sessions":2})
            )
            .await
            .status(),
        StatusCode::OK
    );

    let playlist = daemon.get("/tv/playlists/alice?token=first-token").await;
    assert_eq!(playlist.status(), StatusCode::OK);
    let playlist = playlist.text().await.unwrap();
    assert!(playlist.starts_with("#EXTM3U\n"));
    assert!(playlist.contains(&daemon.url("/news/index.m3u8?token=first-token")));
    assert_eq!(
        daemon.get("/tv/playlists/alice?token=wrong").await.status(),
        StatusCode::FORBIDDEN
    );

    let segment = hls_segment(&daemon).await;
    let segment_file = root.path().join("owned-segment.ts");
    fs::write(&segment_file, segment).unwrap();
    independently_decode_one_frame(segment_file).await;
    let mut viewer = daemon.get("/news/mpegts?token=first-token").await;
    assert_eq!(viewer.status(), StatusCode::OK);
    assert!(
        !tokio::time::timeout(Duration::from_secs(10), viewer.chunk())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .is_empty()
    );

    assert_eq!(
        daemon
            .put(
                "/streamer/api/v3/iptv/subscribers/alice",
                json!({"token":"second-token"})
            )
            .await
            .status(),
        StatusCode::OK
    );
    tokio::time::timeout(Duration::from_secs(8), async {
        while let Ok(Some(_)) = viewer.chunk().await {}
    })
    .await
    .expect("old MPEG-TS viewer stayed open after token rotation");
    assert_eq!(
        daemon
            .get("/news/index.m3u8?token=first-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        daemon
            .get("/news/index.m3u8?token=second-token")
            .await
            .status(),
        StatusCode::OK
    );

    let state = root.path().join("state");
    let backup = root.path().join("state-backup");
    fs::rename(&state, &backup).unwrap();
    fs::write(&state, b"blocks the IPTV data directory").unwrap();
    let failed = daemon
        .put(
            "/streamer/api/v3/iptv/subscribers/alice",
            json!({"token":"not-saved"}),
        )
        .await;
    fs::remove_file(&state).unwrap();
    fs::rename(&backup, &state).unwrap();
    assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        daemon
            .get("/news/index.m3u8?token=second-token")
            .await
            .status(),
        StatusCode::OK
    );
    daemon.stop().await;

    let daemon = Daemon::start(root.path(), port).await;
    assert_eq!(
        daemon
            .get("/tv/playlists/alice?token=second-token")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        daemon
            .get("/tv/playlists/alice?token=not-saved")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    let saved = daemon
        .edit(
            reqwest::Method::GET,
            "/streamer/api/v3/iptv/subscribers/alice",
            None,
        )
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(saved["token"], "second-token");
    let mode = fs::metadata(root.path().join("state/iptv.json"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "subscriber data is not owner-only");
    assert_eq!(
        daemon
            .edit(reqwest::Method::DELETE, "/streamer/api/v3/iptv", None)
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    daemon.stop().await;

    let daemon = Daemon::start(root.path(), port).await;
    assert_eq!(
        daemon
            .edit(reqwest::Method::GET, "/streamer/api/v3/iptv", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        daemon
            .get("/tv/playlists/alice?token=second-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        daemon
            .get("/news/index.m3u8?token=second-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    daemon.stop().await;
}
