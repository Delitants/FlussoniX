#[cfg(target_os = "linux")]
use flussonix::config::ConfigStore;
use flussonix::{
    playback_auth::{AuthOutcome, Policy, ViewerRequest},
    server::{App, Options, router},
};
#[cfg(target_os = "linux")]
use futures_util::StreamExt;
use futures_util::future::join_all;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc, time::Duration};
#[cfg(target_os = "linux")]
use std::{os::unix::process::CommandExt, process::Stdio};
use tokio::net::TcpListener;
#[cfg(target_os = "linux")]
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};

#[cfg(target_os = "linux")]
struct OwnedDaemon(Child);

#[cfg(target_os = "linux")]
impl OwnedDaemon {
    async fn crash(&mut self) {
        let pgid = self.0.id().expect("owned CDN still running") as i32;
        // The isolated process group also contains its owned ffmpeg workers.
        assert_eq!(unsafe { libc::kill(-pgid, libc::SIGKILL) }, 0);
        tokio::time::timeout(Duration::from_secs(5), self.0.wait())
            .await
            .expect("owned CDN failed to stop")
            .unwrap();
    }
}

#[cfg(target_os = "linux")]
impl Drop for OwnedDaemon {
    fn drop(&mut self) {
        if let Some(pgid) = self.0.id() {
            // SAFETY: This process group was created only for this test child.
            unsafe { libc::kill(-(pgid as i32), libc::SIGKILL) };
        }
    }
}

fn node(dir: &tempfile::TempDir, name: &str, authority: url::Url) -> Arc<App> {
    App::new(
        dir.path().join(format!("{name}-config.json")),
        dir.path().join(format!("{name}-media")),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "cdn".into(),
            node_name: name.into(),
            iptv_data: Some(dir.path().join(format!("{name}-iptv.json"))),
            iptv_authority: Some(authority),
            iptv_authority_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn competing_cdn_admission_burst_never_exceeds_cap_and_reclaims_all_records() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
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
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":32}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdns = [
        node(&dir, "cdn-a", origin.clone()),
        node(&dir, "cdn-b", origin),
    ];
    let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
    let snapshots = [
        cdns[0]
            .playback_auth
            .publish("news", Some(policy.clone()))
            .unwrap(),
        cdns[1].playback_auth.publish("news", Some(policy)).unwrap(),
    ];
    tokio::time::sleep(Duration::from_secs(11)).await;
    let attempts = (0..128).map(|index| {
        let node = cdns[index % 2].clone();
        let snapshot = snapshots[index % 2].clone();
        async move {
            node.playback_auth
                .authorize(
                    snapshot,
                    ViewerRequest {
                        name: "news".into(),
                        proto: "mpegts".into(),
                        ip: format!("198.51.100.{}", index + 1),
                        token: "private-viewer-token".into(),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    let outcomes = tokio::time::timeout(Duration::from_secs(12), join_all(attempts))
        .await
        .expect("bounded authority burst stalled");
    let granted = outcomes
        .into_iter()
        .filter_map(|outcome| match outcome {
            AuthOutcome::Allowed(grant) => Some(grant),
            AuthOutcome::Denied => None,
            AuthOutcome::Redirect(_) => panic!("IPTV authority cannot redirect a viewer"),
        })
        .collect::<Vec<_>>();
    assert!(!granted.is_empty(), "all healthy burst admissions failed");
    assert!(granted.len() <= 32, "subscriber cap exceeded");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(authority.iptv_leases.active_leases(), granted.len());
    drop(granted);
    for cdn in &cdns {
        cdn.playback_auth.invalidate_iptv();
    }
    tokio::time::timeout(Duration::from_secs(8), async {
        while authority.iptv_leases.active_leases() != 0
            || authority.iptv_leases.subscriber_records() != 0
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("revoked CDN burst left authority lease records");
    server.abort();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn crashed_cdn_lease_expires_and_another_node_can_admit_the_viewer() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
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
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn_config = dir.path().join("crashed-config.json");
    ConfigStore::open(&cdn_config)
        .unwrap()
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_flussonix"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
        command.env_remove(key);
    }
    command
        .args(["--listen", "127.0.0.1:0", "--role", "cdn"])
        .arg("--config")
        .arg(cdn_config)
        .arg("--iptv-data")
        .arg(dir.path().join("crashed-iptv.json"))
        .arg("--media-dir")
        .arg(dir.path().join("crashed-media"))
        .args([
            "--iptv-authority",
            origin.as_str(),
            "--iptv-authority-allow-insecure-loopback",
        ])
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
        .env("FLUSSONIX_PEER_KEY", "owned-cluster-peer-123")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut daemon = OwnedDaemon(command.spawn().unwrap());
    let mut lines = BufReader::new(daemon.0.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
        .await
        .expect("isolated CDN startup timed out")
        .unwrap()
        .unwrap();
    let status: serde_json::Value = serde_json::from_str(&line).unwrap();
    let base = format!("http://{}", status["listen"].as_str().unwrap());
    tokio::time::sleep(Duration::from_secs(11)).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap()
        .get(format!("{base}/news/mpegts?token=private-viewer-token"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut media = response.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(8), media.next())
        .await
        .expect("crashed CDN did not start media")
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    assert_eq!(authority.iptv_leases.active_leases(), 1);

    let replacement = node(&dir, "replacement", origin);
    let snapshot = replacement
        .playback_auth
        .publish(
            "news",
            Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
        )
        .unwrap();
    let viewer = ViewerRequest {
        name: "news".into(),
        proto: "mpegts".into(),
        ip: "198.51.100.88".into(),
        token: "private-viewer-token".into(),
        ..Default::default()
    };
    assert!(matches!(
        replacement
            .playback_auth
            .authorize(snapshot.clone(), viewer.clone())
            .await,
        AuthOutcome::Denied
    ));
    daemon.crash().await;
    drop(media);
    assert!(matches!(
        replacement
            .playback_auth
            .authorize(snapshot.clone(), viewer.clone())
            .await,
        AuthOutcome::Denied
    ));
    tokio::time::sleep(Duration::from_secs(11)).await;
    let AuthOutcome::Allowed(grant) = replacement.playback_auth.authorize(snapshot, viewer).await
    else {
        panic!("expired crashed-CDN slot was not reclaimed");
    };
    assert_eq!(authority.iptv_leases.active_leases(), 1);
    drop(grant);
    replacement.playback_auth.invalidate_iptv();
    tokio::time::timeout(Duration::from_secs(4), async {
        while authority.iptv_leases.active_leases() != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replacement CDN left a subscriber slot");
    server.abort();
}
