use flussonix::media::Engine;
use futures_util::FutureExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

// A missing or management-request-driven sampler loses a live worker's rate.
#[tokio::test]
async fn shared_worker_measures_output_before_the_first_stats_request() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let result = std::panic::AssertUnwindSafe(async {
        let worker = engine
            .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
            .await
            .unwrap();
        let mut receiver = worker.subscribe();
        tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        // No stats or management requests occur during this observation period.
        tokio::time::sleep(Duration::from_millis(2300)).await;
        let stats = engine.stats("owned").await;
        assert!(
            stats["flussonix_output_mbps"]
                .as_f64()
                .is_some_and(|v| v.is_finite() && v > 0.0),
            "live shared output must report its measured rate: {stats}"
        );
        assert!(
            stats["flussonix_output_rate_age_ms"]
                .as_u64()
                .is_some_and(|age| age <= 3000)
        );
        assert_eq!(engine.count().await, 1);
        assert!(stats["bytes_in"].as_u64().unwrap() > 0);
        engine.stop("owned").await;
        assert!(engine.stats("owned").await["flussonix_output_mbps"].is_null());
        engine
            .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
            .await
            .unwrap();
        let replacement = engine.stats("owned").await;
        assert!(
            replacement["flussonix_output_mbps"].is_null(),
            "new worker cannot inherit old rate"
        );
        assert!(replacement["flussonix_output_rate_age_ms"].is_null());
    })
    .catch_unwind()
    .await;
    engine.stop_all().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn admission(
    app: &Arc<flussonix::server::App>,
    body: serde_json::Value,
) -> axum::response::Response {
    flussonix::server::router(app.clone())
        .oneshot(
            axum::http::Request::post("/flussonix/api/v1/admit")
                .header("X-Flussonix-Peer", &app.options.peer_key)
                .header("Content-Type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn node(app: &Arc<flussonix::server::App>) -> serde_json::Value {
    let response = flussonix::server::router(app.clone())
        .oneshot(
            axum::http::Request::get("/flussonix/api/v1/node")
                .header("X-Flussonix-Peer", &app.options.peer_key)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}

fn app(dir: &std::path::Path) -> Arc<flussonix::server::App> {
    let app = flussonix::server::App::new(
        dir.join("config.json"),
        dir.join("media"),
        flussonix::server::Options {
            role: "cdn".into(),
            uplink_interface: "process".into(),
            uplink_mbps: 1000.0,
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
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
    app
}

#[tokio::test]
async fn admissions_with_invalid_bitrate_hints_cannot_be_silently_replaced_or_reserved() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    for hint in [
        json!(0),
        json!(-1),
        json!("2"),
        json!(null),
        json!([]),
        json!(1_000_001),
    ] {
        let response = admission(&app, json!({"name":"owned","bitrate_mbps":hint})).await;
        assert_eq!(response.status(), 400, "hint {hint}");
    }
    let snapshot = node(&app).await;
    assert_eq!(snapshot["reserved"], 0);
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn admissions_above_one_hundred_mbps_are_not_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        app.sample_metrics();
        let snapshot = node(&app).await;
        if snapshot["cpu"].as_f64().is_some_and(|v| v < 0.9)
            && snapshot["ram"].as_f64().is_some_and(|v| v < 0.95)
        {
            break;
        }
    }
    let response = admission(&app, json!({"name":"owned","bitrate_mbps":120})).await;
    assert_eq!(response.status(), 200);
    let snapshot = node(&app).await;
    assert_eq!(
        snapshot["reserved_mbps"], 120.0,
        "the ledger must keep the actual admitted cost"
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn cdn_rechecks_its_own_live_output_before_accepting_a_smaller_hint() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    let result = std::panic::AssertUnwindSafe(async {
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"transcoder":{"vb":12000},"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let config = app.config.snapshot()["streams"][0].clone();
        app.media.ensure("owned", &config).await.unwrap();
        let mut observed = false;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            app.sample_metrics();
            let snapshot = node(&app).await;
            if snapshot["stream_bitrates"]["owned"]["mbps"]
                .as_f64()
                .is_some_and(|v| v > 2.0)
                && snapshot["cpu"].as_f64().is_some_and(|v| v < 0.9)
                && snapshot["ram"].as_f64().is_some_and(|v| v < 0.95)
            {
                observed = true;
                break;
            }
        }
        assert!(
            observed,
            "owned high-rate fixture never produced fresh output above 2 Mbps"
        );
        let response = admission(&app, json!({"name":"owned","bitrate_mbps":0.1})).await;
        assert_eq!(response.status(), 200);
        let snapshot = node(&app).await;
        let reserved = snapshot["reserved_mbps"].as_f64().unwrap();
        assert!(
            reserved > 2.5,
            "CDN must use its own higher live estimate: {snapshot}"
        );
        assert_eq!(snapshot["reserved"], 1);
        assert_eq!(
            app.media.count().await,
            1,
            "admission must reuse the shared worker"
        );
    })
    .catch_unwind()
    .await;
    app.media.stop_all().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
