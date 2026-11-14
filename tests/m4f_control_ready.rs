use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flussonix::{
    m4s::Track,
    server::{App, Options, router},
};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn metadata_without_a_cached_m4f_segment_is_retryable_unavailable() {
    let directory = tempfile::tempdir().unwrap();
    let app = App::new(
        directory.path().join("config.json"),
        directory.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let config = json!({"inputs":[{"url":"publish://"}]});
    app.config.put("streams", "owned", config.clone()).unwrap();
    let publisher = app
        .media
        .publish_guarded("owned", &config, std::future::ready(true))
        .await
        .unwrap();
    publisher
        .worker
        .wire
        .info(vec![Track {
            id: 1,
            codec: "h264".into(),
            config: vec![1, 100, 0, 40],
        }])
        .unwrap();
    assert!(publisher.worker.wire.has_info());
    assert!(!publisher.worker.wire.has_m4f_segment());

    let request = Request::builder()
        .uri("/owned/m4f")
        .body(Body::empty())
        .unwrap();
    let response = router(Arc::clone(&app)).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    drop(publisher);
    app.media.stop_all().await;
}
