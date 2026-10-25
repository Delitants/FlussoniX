use axum::{body::Body, http::Request};
use flussonix::server::router;
use flussonix::server::{App, Options};
use std::path::PathBuf;
use std::time::Duration;
use tower::ServiceExt;

fn start(url: &str, insecure_loopback: bool, ca: Option<PathBuf>) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let options = Options {
        admin_password: "owned-admin-password".into(),
        peer_key: "owned-peer-key-123".into(),
        role: "cdn".into(),
        iptv_authority: Some(url::Url::parse(url).unwrap()),
        iptv_authority_ca: ca,
        iptv_authority_allow_insecure_loopback: insecure_loopback,
        ..Options::default()
    };
    App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        options,
    )
    .map(|_| ())
}

#[test]
fn authority_accepts_https_and_only_explicit_literal_loopback_http() {
    assert!(start("https://authority.example:8443/", false, None).is_ok());
    assert!(start("http://127.0.0.1:18443/", false, None).is_err());
    assert!(start("http://127.0.0.1:18443/", true, None).is_ok());
    assert!(start("http://[::1]:18443/", true, None).is_ok());
    assert!(start("http://localhost:18443/", true, None).is_err());
    assert!(start("http://192.0.2.1:18443/", true, None).is_err());
}

#[test]
fn authority_rejects_credentials_paths_queries_fragments_and_http_ca() {
    for url in [
        "https://user:secret@authority.example/",
        "https://authority.example/other",
        "https://authority.example/?token=secret",
        "https://authority.example/#fragment",
    ] {
        assert!(start(url, false, None).is_err(), "{url}");
    }
    assert!(
        start(
            "http://127.0.0.1:18443/",
            true,
            Some(PathBuf::from("/tmp/ca.pem"))
        )
        .is_err()
    );
}

#[tokio::test]
async fn authority_status_becomes_ready_after_restart_quarantine_and_reflects_iptv_disable() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin-password".into(),
            peer_key: "owned-peer-key-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            ..Options::default()
        },
    )
    .unwrap();
    app.iptv.set_enabled(true).unwrap();

    async fn status(app: &std::sync::Arc<App>) -> serde_json::Value {
        let response = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri("/flussonix/api/v1/node")
                    .header("x-flussonix-peer", "owned-peer-key-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 1_000_000)
                .await
                .unwrap(),
        )
        .unwrap()
    }

    let quarantined = status(&app).await;
    assert_eq!(quarantined["iptv_cluster"]["mode"], "authority");
    assert_eq!(quarantined["iptv_cluster"]["ready"], false);

    tokio::time::sleep(Duration::from_millis(11_050)).await;
    assert_eq!(status(&app).await["iptv_cluster"]["ready"], true);

    app.iptv.set_enabled(false).unwrap();
    assert_eq!(status(&app).await["iptv_cluster"]["ready"], false);
}

#[tokio::test]
async fn cdn_status_reports_missing_or_unusable_authority_without_secrets() {
    for configured in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-password".into(),
                peer_key: "owned-peer-key-123".into(),
                role: "cdn".into(),
                iptv_authority: configured
                    .then(|| url::Url::parse("https://authority.example/").unwrap()),
                ..Options::default()
            },
        )
        .unwrap();
        let response = router(app)
            .oneshot(
                Request::builder()
                    .uri("/flussonix/api/v1/node")
                    .header("x-flussonix-peer", "owned-peer-key-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let node: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(node["iptv_cluster"]["ready"], false);
        assert_eq!(node["iptv_cluster"]["configured"], configured);
        assert!(
            node["iptv_cluster"]["reason"]
                .as_str()
                .unwrap()
                .contains("authority")
        );
        assert!(!String::from_utf8_lossy(&bytes).contains("owned-peer-key-123"));
    }
}

#[tokio::test]
async fn standalone_with_remote_authority_reports_cluster_admission_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin-password".into(),
            peer_key: "owned-peer-key-123".into(),
            iptv_authority: Some(url::Url::parse("https://authority.example/").unwrap()),
            ..Options::default()
        },
    )
    .unwrap();
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/node")
                .header("x-flussonix-peer", "owned-peer-key-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    let node: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(node["iptv_cluster"]["mode"], "client");
    assert_eq!(node["iptv_cluster"]["ready"], false);
}

#[tokio::test]
async fn authority_clients_cannot_serve_or_edit_local_iptv_catalogs_or_mint_playlists() {
    for remote_standalone in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-password".into(),
                peer_key: "owned-peer-key-123".into(),
                role: if remote_standalone {
                    "standalone"
                } else {
                    "cdn"
                }
                .into(),
                iptv_authority: remote_standalone
                    .then(|| url::Url::parse("https://authority.example/").unwrap()),
                ..Options::default()
            },
        )
        .unwrap();
        app.iptv.set_enabled(true).unwrap();
        for (method, uri) in [
            ("GET", "/streamer/api/v3/iptv"),
            ("PUT", "/streamer/api/v3/iptv"),
            ("GET", "/tv/playlists/alice?token=private-viewer-token"),
        ] {
            let response = router(app.clone())
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header(
                            "authorization",
                            format!(
                                "Basic {}",
                                base64::Engine::encode(
                                    &base64::engine::general_purpose::STANDARD,
                                    "admin:owned-admin-password"
                                )
                            ),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "{uri}"
            );
        }
    }
}
