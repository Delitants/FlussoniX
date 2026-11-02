use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flussonix::server::{App, Options, router};
use serde_json::Value;
use tower::ServiceExt;

#[cfg(unix)]
#[path = "support/tls.rs"]
mod tls_fixture;

#[tokio::test]
async fn public_liveness_uses_reference_whoami_shape_without_credentials() {
    let state = tempfile::tempdir().unwrap();
    let app = App::new(
        state.path().join("config.json"),
        state.path().join("media"),
        Options {
            admin_password: "private-admin-test-marker".into(),
            peer_key: "private-peer-test-marker-1234".into(),
            ..Options::default()
        },
    )
    .unwrap();
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/monitoring/liveness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert!(
        value["now"]
            .as_u64()
            .is_some_and(|now| now > 1_000_000_000_000)
    );
    assert_eq!(value["build"], 0);
    assert_eq!(value["server_version"], env!("CARGO_PKG_VERSION"));
    assert!(value.get("started_at").is_none());
    assert_eq!(value.as_object().unwrap().len(), 3);
    assert!(!String::from_utf8_lossy(&body).contains("private-"));
}

#[tokio::test]
async fn public_readiness_includes_stable_start_time() {
    let state = tempfile::tempdir().unwrap();
    let app = App::new(
        state.path().join("config.json"),
        state.path().join("media"),
        Options {
            admin_password: "private-admin-test-marker".into(),
            peer_key: "private-peer-test-marker-1234".into(),
            ..Options::default()
        },
    )
    .unwrap();
    let request = || {
        Request::builder()
            .uri("/streamer/api/v3/monitoring/readiness")
            .body(Body::empty())
            .unwrap()
    };
    let first = router(app.clone()).oneshot(request()).await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["content-type"], "application/json");
    let first: Value =
        serde_json::from_slice(&axum::body::to_bytes(first.into_body(), 4096).await.unwrap())
            .unwrap();
    let second = router(app).oneshot(request()).await.unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second: Value = serde_json::from_slice(
        &axum::body::to_bytes(second.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    let started = first["started_at"].as_u64().unwrap();
    assert!(started > 1_000_000_000);
    assert!(started <= first["now"].as_u64().unwrap() / 1000);
    assert_eq!(second["started_at"], started);
    assert_eq!(first["build"], 0);
    assert_eq!(first["server_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(first.as_object().unwrap().len(), 4);
}

#[cfg(unix)]
#[tokio::test]
async fn readiness_fails_closed_when_ha_database_is_unavailable() {
    use flussonix::iptv::ha::connection::HaOptions;
    use std::{fs, net::TcpListener, os::unix::fs::PermissionsExt};

    let certificates = tls_fixture::Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let path = certificates.dir.path().join("unavailable-db.json");
    fs::write(
        &path,
        serde_json::json!({
            "cluster_id":"a3192376-6df5-4b76-a15c-43942fd3d466",
            "host":"127.0.0.1",
            "port":listener.local_addr().unwrap().port(),
            "database":"postgres",
            "user":"ha_test",
            "password":"private-database-test-marker",
            "ca_pem":certificates.ca
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let app = App::new(
        certificates.dir.path().join("config.json"),
        certificates.dir.path().join("media"),
        Options {
            admin_password: "private-admin-test-marker".into(),
            peer_key: "private-peer-test-marker-1234".into(),
            iptv_lease_authority: true,
            iptv_ha: Some(HaOptions::load(&path).unwrap()),
            ..Options::default()
        },
    )
    .unwrap();
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/monitoring/readiness")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("private-"));
}
