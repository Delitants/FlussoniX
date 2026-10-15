//! Exercise diagnostics through real routed response bodies, not a logging helper.
use super::*;
use crate::server::{App, Options, router};
use axum::{body::Body, http::Request};
use futures_util::FutureExt;
use http_body_util::BodyExt;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};
use tower::ServiceExt;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt};

type Rows = Arc<std::sync::Mutex<Vec<BTreeMap<String, Value>>>>;
struct Capture(Rows);
struct Fields(BTreeMap<String, Value>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), json!(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().into(), json!(value));
    }
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        if fields.0.get("event") == Some(&json!("native_ts_delivery_end")) {
            self.0.lock().unwrap().push(fields.0);
        }
    }
}
fn capture() -> (Rows, tracing::subscriber::DefaultGuard) {
    let rows = Arc::new(std::sync::Mutex::new(vec![]));
    let subscriber = tracing_subscriber::registry().with(Capture(rows.clone()));
    (rows, tracing::subscriber::set_default(subscriber))
}
fn setup() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    // A real idle worker keeps lifecycle/cancellation ownership intact while
    // tests inject exact chunks into its production TS queue. No codec mocking.
    let idle = dir.path().join("owned-idle-ffmpeg");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            ffmpeg: idle.to_str().unwrap().into(),
            admin_password: "private-admin-password".into(),
            peer_key: "private-peer-password".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,
        "inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":30}),
        )
        .unwrap();
    (dir, app)
}
async fn checked(app: &Arc<App>, work: impl std::future::Future<Output = ()>) {
    let result = std::panic::AssertUnwindSafe(work).catch_unwind().await;
    app.media.stop_all().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
async fn body(app: &Arc<App>, token: &str) -> Body {
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/owned/mpegts?token={token}"))
                .header("User-Agent", "private-user-agent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "video/mp2t");
    response.into_body()
}
async fn worker(app: &App) -> Arc<Worker> {
    app.media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap()
}
fn send(worker: &Worker, bytes: Bytes) {
    worker
        .bytes
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    worker.tx.send(bytes).unwrap();
}
async fn next(body: &mut Body) -> Option<Bytes> {
    tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .map(|frame| frame.unwrap().into_data().unwrap())
}
fn one(rows: &Rows, reason: &str) -> BTreeMap<String, Value> {
    let rows = rows.lock().unwrap();
    assert_eq!(
        rows.len(),
        1,
        "a terminated body must emit exactly one diagnostic"
    );
    assert_eq!(rows[0]["reason"], reason);
    assert!(rows[0]["delivery_id"].as_u64().is_some_and(|id| id > 0));
    let text = serde_json::to_string(&*rows).unwrap();
    for secret in [
        "private-viewer-token",
        "private-admin-password",
        "private-peer-password",
        "private-user-agent",
        "testsrc://",
    ] {
        assert!(!text.contains(secret), "diagnostics exposed {secret}");
    }
    rows[0].clone()
}

#[tokio::test]
async fn lag_is_identified_without_stopping_another_ts_viewer() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut slow = body(&app, "private-viewer-token").await;
        let mut fast = body(&app, "another-private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"prefix"));
        assert_eq!(next(&mut slow).await.unwrap(), "prefix");
        assert_eq!(next(&mut fast).await.unwrap(), "prefix");
        for _ in 0..513 {
            let bytes = Bytes::from(vec![0x47; 8192]);
            send(&worker, bytes.clone());
            assert_eq!(next(&mut fast).await.unwrap(), bytes);
        }
        assert!(next(&mut slow).await.is_none());
        drop(slow);
        let row = one(&rows, "queue_lagged");
        assert_eq!(row["missed_records"], 1);
        assert_eq!(row["bytes_yielded"], 6);
        assert_eq!(row["worker_bytes_since_start"], 4_202_502);
        assert_eq!(row["backlog_bytes"], 4_194_304);
        assert_eq!(row["backlog_records"], 512);
        assert_eq!(row["worker_pid"], worker.pid());
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 1);
        assert_eq!(worker.tx.retained(), (0, 0));
        send(&worker, Bytes::from_static(b"tail"));
        assert_eq!(next(&mut fast).await.unwrap(), "tail");
        drop(fast);
    })
    .await;
}

#[tokio::test]
async fn revocation_wins_over_shutdown_and_queued_ts() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"must-not-escape"));
        let sessions = app.playback_auth.snapshots();
        assert_eq!(sessions.len(), 1);
        assert!(
            app.playback_auth
                .revoke(sessions[0]["id"].as_str().unwrap())
        );
        worker.cancel.cancel();
        assert!(next(&mut body).await.is_none());
        drop(body);
        let row = one(&rows, "authorization_revoked");
        assert_eq!(row["bytes_yielded"], 0);
        assert_eq!(row["authorization_cancelled"], true);
        assert_eq!(row["worker_closed"], true);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    })
    .await;
}

#[tokio::test]
async fn stopped_worker_is_identified_before_queued_data_can_escape() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"must-not-escape"));
        app.media.stop("owned").await;
        assert!(next(&mut body).await.is_none());
        drop(body);
        let row = one(&rows, "worker_stopped");
        assert_eq!(row["bytes_yielded"], 0);
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], true);
        assert_eq!(row["worker_alive"], false);
    })
    .await;
}

#[tokio::test]
async fn unpolled_body_drop_is_recorded_and_releases_viewer_and_backlog() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"pending"));
        drop(body);
        let row = one(&rows, "body_dropped");
        assert_eq!(row["bytes_yielded"], 0);
        assert_eq!(row["backlog_bytes"], 7);
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert_eq!(worker.tx.retained(), (0, 0));
    })
    .await;
}

#[tokio::test]
async fn pending_body_drop_records_only_yielded_bytes_and_releases_unread_backlog() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"prefix"));
        assert_eq!(next(&mut body).await.unwrap(), "prefix");
        assert!(body.frame().now_or_never().is_none());
        send(&worker, Bytes::from_static(b"pending"));
        drop(body);
        let row = one(&rows, "body_dropped");
        assert_eq!(row["bytes_yielded"], 6);
        assert_eq!(row["worker_bytes_since_start"], 13);
        assert_eq!(row["backlog_bytes"], 7);
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert_eq!(worker.tx.retained(), (0, 0));
    })
    .await;
}

#[tokio::test]
async fn producer_queue_close_is_distinct_and_drains_remaining_ts() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"last"));
        worker.tx.close();
        assert_eq!(next(&mut body).await.unwrap(), "last");
        assert!(next(&mut body).await.is_none());
        drop(body);
        let row = one(&rows, "queue_closed");
        assert_eq!(row["bytes_yielded"], 4);
        assert_eq!(row["missed_records"], 0);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(row["backlog_bytes"], 0);
    })
    .await;
}
