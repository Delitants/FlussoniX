use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn stop_stream_returns_empty_no_content_response() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "owned-edit-password".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"inputs":[{"url":"testsrc://"}],"static":false}),
        )
        .unwrap();

    let response = router(app.clone())
        .oneshot(
            Request::post("/streamer/api/v3/streams/owned/stop")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:owned-edit-password")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(app.media.count().await, 0);
}
