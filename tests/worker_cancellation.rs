use axum::{body::Body, http::Request};
use flussonix::server::{App, Options, router};
use http_body_util::BodyExt;
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use tower::ServiceExt;

#[tokio::test]
async fn stopped_worker_never_drains_a_saved_wire_bootstrap() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let w = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    for _ in 0..100 {
        if w.wire.m4s_subscribe().0.len() > 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(w.wire.m4s_subscribe().0.len() > 2);
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/owned/m4s")
                .header("X-Flussonix-Peer", "owned-peer-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    app.media.stop("owned").await;
    assert!(!w.alive.load(Ordering::Relaxed));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .unwrap()
            .is_none()
    );
}
