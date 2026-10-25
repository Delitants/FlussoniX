#![cfg(target_os = "linux")]

use axum::Router;
use flussonix::{
    config::ConfigStore,
    server::{App, Options, router},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{net::SocketAddr, os::unix::process::CommandExt, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{Child, Command},
};

const PEER_KEY: &str = "owned-cluster-peer-123";
const TOKEN: &str = "private-viewer-token";

struct Cdn {
    child: Child,
    base: String,
}

impl Cdn {
    async fn start(root: &std::path::Path, name: &str, authority: &url::Url) -> Self {
        let config = root.join(format!("{name}-config.json"));
        ConfigStore::open(&config)
            .unwrap()
            .put(
                "streams",
                "news",
                json!({
                    "inputs":[{"url":"testsrc://"}],
                    "on_play":"iptv://"
                }),
            )
            .unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_flussonix"));
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
            cmd.env_remove(key);
        }
        cmd.args([
            "--listen",
            "127.0.0.1:0",
            "--role",
            "cdn",
            "--node-name",
            name,
        ])
        .arg("--config")
        .arg(config)
        .arg("--iptv-data")
        .arg(root.join(format!("{name}-iptv.json")))
        .arg("--media-dir")
        .arg(root.join(format!("{name}-media")))
        .args([
            "--iptv-authority",
            authority.as_str(),
            "--iptv-authority-allow-insecure-loopback",
        ])
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
        .env("FLUSSONIX_PEER_KEY", PEER_KEY)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
            .await
            .expect("CDN did not start")
            .unwrap()
            .unwrap();
        let status: serde_json::Value = serde_json::from_str(&line).unwrap();
        let base = format!("http://{}", status["listen"].as_str().unwrap());
        Self { child, base }
    }
}

impl Drop for Cdn {
    fn drop(&mut self) {
        if let Some(pgid) = self.child.id() {
            // SAFETY: Each CDN was started in a fresh, test-owned process group.
            unsafe { libc::kill(-(pgid as i32), libc::SIGKILL) };
        }
    }
}

async fn hls_segment(client: &reqwest::Client, cdn: &Cdn) -> Vec<u8> {
    let playlist_url = format!("{}/news/index.m3u8?token={TOKEN}", cdn.base);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let response = client.get(&playlist_url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let playlist = response.text().await.unwrap();
            if let Some(segment) = playlist
                .lines()
                .find(|line| !line.is_empty() && !line.starts_with('#'))
            {
                let segment_url = url::Url::parse(&playlist_url)
                    .unwrap()
                    .join(segment)
                    .unwrap();
                let media = client.get(segment_url).send().await.unwrap();
                if media.status() == reqwest::StatusCode::OK {
                    let bytes = media.bytes().await.unwrap();
                    if !bytes.is_empty() {
                        return bytes.to_vec();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("HLS media did not become available")
}

#[tokio::test]
async fn two_cdn_daemons_share_one_iptv_limit_during_hls_and_continuous_playback() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: PEER_KEY.into(),
            role: "lb".into(),
            iptv_data: Some(dir.path().join("authority-iptv.json")),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({
                "token":TOKEN,"packages":["basic"],"max_sessions":2
            }),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let service: Router = router(authority.clone());
    let server = tokio::spawn(
        axum::serve(
            listener,
            service.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let (first, second) = tokio::join!(
        Cdn::start(dir.path(), "cdn-first", &origin),
        Cdn::start(dir.path(), "cdn-second", &origin),
    );
    assert_ne!(first.child.id(), second.child.id());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;

    let ts_url = format!("{}/news/mpegts?token={TOKEN}", first.base);
    let (ts, segment) = tokio::join!(
        async {
            tokio::time::timeout(Duration::from_secs(8), client.get(&ts_url).send())
                .await
                .unwrap()
                .unwrap()
        },
        hls_segment(&client, &second),
    );
    assert_eq!(ts.status(), reqwest::StatusCode::OK);
    assert!(!segment.is_empty());
    let mut ts_body = ts.bytes_stream();
    let first_ts = tokio::time::timeout(Duration::from_secs(8), ts_body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first_ts.is_empty());
    assert_eq!(authority.iptv_leases.active_leases(), 2);

    let overflow = tokio::time::timeout(
        Duration::from_secs(8),
        client
            .get(format!("{}/news/mpegts?token={TOKEN}", second.base))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(overflow.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(authority.iptv_leases.active_leases(), 2);
    drop((ts_body, first, second));
    server.abort();
}
