use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

async fn stop(app: Arc<App>, origin: Option<&str>, fetch_site: Option<&str>) -> StatusCode {
    let mut request = Request::builder()
        .method("POST")
        .uri("/streamer/api/v3/streams/owned/stop")
        .header("Host", "console.example:8080")
        .header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("admin:edit-password")),
        );
    if let Some(origin) = origin {
        request = request.header("Origin", origin);
    }
    if let Some(site) = fetch_site {
        request = request.header("Sec-Fetch-Site", site);
    }
    router(app)
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn cross_origin_browser_cannot_stop_stream_with_cached_basic_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "edit-password".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"inputs": [{"url": "testsrc://"}]}),
        )
        .unwrap();
    app.media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    assert_eq!(app.media.count().await, 1);

    assert_eq!(
        stop(
            app.clone(),
            Some("https://attacker.example"),
            Some("cross-site")
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(app.media.count().await, 1);
    assert_eq!(
        stop(
            app.clone(),
            Some("http://console.example:8080"),
            Some("same-origin")
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(app.media.count().await, 0);
    app.media.stop_all().await;
}

#[tokio::test]
async fn browser_fetch_metadata_blocks_cross_site_mutation_without_origin_header() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "edit-password".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"inputs": [{"url": "testsrc://"}]}),
        )
        .unwrap();
    app.media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();

    assert_eq!(
        stop(app.clone(), None, Some("cross-site")).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(app.media.count().await, 1);
    assert_eq!(stop(app.clone(), None, None).await, StatusCode::OK);
    app.media.stop_all().await;
}

#[tokio::test]
async fn legacy_browser_form_without_origin_cannot_stop_stream() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "edit-password".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"inputs": [{"url": "testsrc://"}]}),
        )
        .unwrap();
    app.media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/streams/owned/stop")
                .header("Host", "console.example:8080")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:edit-password")),
                )
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("unused=1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(app.media.count().await, 1);
    app.media.stop_all().await;
}
