use flussonix::{
    playback_auth::{AuthOutcome, Policy, ViewerRequest},
    server::{App, Options, router},
};
use futures_util::future::join_all;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;

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
