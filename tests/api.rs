use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use tower::ServiceExt;
fn options() -> Options {
    Options {
        admin_user: "admin".into(),
        admin_password: "secret".into(),
        view_user: Some("viewer".into()),
        view_password: Some("view-secret".into()),
        peer_key: "peer-secret-1234".into(),
        ..Default::default()
    }
}
#[tokio::test]
async fn api_auth_nested_names_persistence_and_validate_only() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        options(),
    )
    .unwrap();
    let r = router(app.clone());
    let req = |method: &str, path: &str, body: Value, cred: Option<&str>| {
        let mut b = Request::builder()
            .method(method)
            .uri(path)
            .header("Content-Type", "application/json");
        if let Some(c) = cred {
            b = b.header("Authorization", format!("Basic {}", STANDARD.encode(c)))
        }
        b.body(Body::from(body.to_string())).unwrap()
    };
    assert_eq!(
        r.clone()
            .oneshot(req("GET", "/streamer/api/v3/streams", json!(null), None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        r.clone()
            .oneshot(req(
                "PUT",
                "/streamer/api/v3/streams/region/news",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                Some("viewer:view-secret")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        r.clone()
            .oneshot(req(
                "PUT",
                "/streamer/api/v3/streams/region/news",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                Some("admin:secret")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let before = app.config.snapshot();
    assert_eq!(
        r.clone()
            .oneshot(req(
                "POST",
                "/streamer/api/v3/config",
                json!({"streams":[]}),
                Some("admin:secret")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(app.config.snapshot(), before);
    let response = r
        .oneshot(req(
            "GET",
            "/streamer/api/v3/streams/region/news",
            json!(null),
            Some("admin:secret"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let b = axum::body::to_bytes(response.into_body(), 100000)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["name"],
        "region/news"
    );
    app.media.stop_all().await;
}
#[tokio::test]
async fn media_auth_precedes_worker_start_including_segments() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        options(),
    )
    .unwrap();
    app.config.put("streams","private",json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":"http://127.0.0.1:1/auth"})).unwrap();
    let r = router(app.clone());
    for path in [
        "/private/index.m3u8",
        "/private/index0.ts",
        "/private/fmp4/init.mp4",
        "/private/mpegts",
    ] {
        let res = r
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }
    assert_eq!(app.media.count().await, 0);
}
#[tokio::test]
async fn metadata_edit_does_not_restart_unrelated_stream_workers() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(d.path().join("c.json"), d.path().join("media"), options()).unwrap();
    for name in ["first", "second"] {
        app.config
            .put("streams", name, json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
    }
    app.reconcile().await;
    let first = app
        .media
        .ensure("first", &app.config.effective("first").unwrap())
        .await
        .unwrap();
    let r = router(app.clone());
    let res = r
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/streamer/api/v3/streams/second")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"title":"Updated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let after = app
        .media
        .ensure("first", &app.config.effective("first").unwrap())
        .await
        .unwrap();
    assert_eq!(first.pid(), after.pid());
    app.media.stop_all().await;
}

#[tokio::test]
async fn media_redirects_and_errors_allow_cross_origin_players_without_cookie_credentials() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(d.path().join("c.json"), d.path().join("media"), options()).unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/missing/index.m3u8")
                .header("origin", "https://player.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|h| h.to_str().ok()),
        Some("*")
    );
    assert!(
        response
            .headers()
            .get("access-control-allow-credentials")
            .is_none()
    );
    let preflight = router(app)
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/owned/index.m3u8")
                .header("origin", "https://player.example")
                .header("access-control-request-headers", "authorization,range")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
}
