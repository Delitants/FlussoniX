use axum::{body::Body, http::Request};
use flussonix::{
    config::ConfigStore,
    media::{Engine, media_signature},
    server::{App, Options, router},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

#[test]
fn subtitle_policy_inherits_overrides_and_clears_without_changing_defaults() {
    let d = tempfile::tempdir().unwrap();
    let c = ConfigStore::open(d.path().join("config.json")).unwrap();
    c.put("streams", "plain", json!({"inputs":[{"url":"testsrc://"}]}))
        .unwrap();
    assert!(
        c.effective("plain")
            .unwrap()
            .get("flussonix_subtitle_tracks")
            .is_none()
    );
    c.put(
        "templates",
        "regional",
        json!({"flussonix_subtitle_tracks":"preserve"}),
    )
    .unwrap();
    c.put("streams", "owned", json!({"template":"regional"}))
        .unwrap();
    assert_eq!(
        c.effective("owned").unwrap()["flussonix_subtitle_tracks"],
        "preserve"
    );
    c.put(
        "streams",
        "owned",
        json!({"flussonix_subtitle_tracks":"drop"}),
    )
    .unwrap();
    assert_eq!(
        c.effective("owned").unwrap()["flussonix_subtitle_tracks"],
        "drop"
    );
    c.put(
        "streams",
        "owned",
        json!({"flussonix_subtitle_tracks":null,"title":"New title"}),
    )
    .unwrap();
    let cfg = c.effective("owned").unwrap();
    assert_eq!(cfg["flussonix_subtitle_tracks"], "preserve");
    assert!(
        cfg["config_on_disk"]
            .get("flussonix_subtitle_tracks")
            .is_none()
    );
    for bad in [
        json!(false),
        json!(42),
        json!({"hls":"convert"}),
        json!("convert"),
        json!("ocr_replace"),
        json!(""),
    ] {
        assert!(
            c.put("templates", "bad", json!({"flussonix_subtitle_tracks":bad}))
                .is_err()
        );
    }
}

#[test]
fn subtitle_policy_changes_media_generation_identity() {
    let cfg = json!({"inputs":[{"url":"publish://"}]});
    let preserve = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":"preserve"});
    assert_ne!(media_signature(&cfg), media_signature(&preserve));
}

#[tokio::test]
async fn subtitle_policy_discovery_is_authenticated_and_has_no_publisher_secret() {
    let d = tempfile::tempdir().unwrap();
    let a = App::new(
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
    a.config.put("streams", "owned", json!({"static":false,"inputs":[{"url":"publish://"}],"password":"owned-secret","flussonix_subtitle_tracks":"preserve"})).unwrap();
    let denied = router(a.clone())
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/stream/owned")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    let reply = router(a.clone())
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/stream/owned")
                .header("x-flussonix-peer", "owned-peer-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.status(), 200);
    let body = reply.into_body().collect().await.unwrap().to_bytes();
    let cfg: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(cfg["flussonix_subtitle_tracks"], "preserve");
    assert!(cfg.get("inputs").is_none());
    assert!(!String::from_utf8_lossy(&body).contains("owned-secret"));
}

#[tokio::test]
async fn worker_reports_effective_separate_track_policy() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    for (name, policy) in [("default", None), ("preserved", Some("preserve"))] {
        let mut cfg = json!({"inputs":[{"url":"testsrc://"}]});
        if let Some(policy) = policy {
            cfg["flussonix_subtitle_tracks"] = json!(policy);
        }
        let worker = e.ensure(name, &cfg).await.unwrap();
        assert_eq!(worker.stats()["subtitle_tracks"], policy.unwrap_or("drop"));
    }
    e.stop_all().await;
}
