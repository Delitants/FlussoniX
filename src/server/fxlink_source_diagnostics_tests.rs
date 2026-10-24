//! Termination diagnostics through real native-provider ownership and queues.
use super::*;
use futures_util::{FutureExt, StreamExt};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt};

type Rows = Arc<std::sync::Mutex<Vec<BTreeMap<String, Value>>>>;
struct Capture(Rows, bool);
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
        if fields.0.get("event") == Some(&json!("fxlink_native_source_end"))
            || self.1 && event.metadata().target() == "flussonix::fxlink::service_timing"
        {
            self.0.lock().unwrap().push(fields.0);
        }
    }
}
fn capture() -> (Rows, tracing::subscriber::DefaultGuard) {
    let rows = Arc::new(std::sync::Mutex::new(vec![]));
    let subscriber = tracing_subscriber::registry().with(Capture(rows.clone(), false).with_filter(
        tracing_subscriber::EnvFilter::new("warn,flussonix::fxlink=debug"),
    ));
    (rows, tracing::subscriber::set_default(subscriber))
}
fn setup() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    // A quiet process owns the real worker lifecycle; exact native records are
    // injected below, so codec timing cannot create incidental queue eviction.
    let idle = dir.path().join("owned-idle-ffmpeg");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            ffmpeg: idle.to_str().unwrap().into(),
            admin_password: "private-source-admin".into(),
            peer_key: "private-source-peer".into(),
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
async fn source(app: &Arc<App>) -> (Source, Arc<Worker>) {
    let worker = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    if !worker.wire.has_info() {
        worker
            .wire
            .info(vec![crate::m4s::Track {
                id: 1,
                codec: "mp3".into(),
                config: vec![],
            }])
            .unwrap();
    }
    let mut source = app
        .fxlink_provider()
        .subscribe(
            LinkRequest {
                name: "owned".into(),
                peer_key: "private-source-peer".into(),
                token: "private-source-viewer".into(),
            },
            "127.0.0.1:12345".parse().unwrap(),
        )
        .await
        .unwrap();
    assert!(source.records.next().await.unwrap().is_ok()); // metadata bootstrap
    (source, worker)
}
fn one(rows: &Rows, reason: &str) -> BTreeMap<String, Value> {
    let rows = rows.lock().unwrap();
    assert_eq!(rows.len(), 1, "one source retirement needs one diagnostic");
    assert_eq!(rows[0]["reason"], reason);
    assert!(rows[0]["worker_pid"].as_u64().is_some_and(|pid| pid > 0));
    let text = serde_json::to_string(&*rows).unwrap();
    for secret in [
        "private-source-viewer",
        "private-source-peer",
        "private-source-admin",
        "testsrc://",
    ] {
        assert!(
            !text.contains(secret),
            "termination diagnostic exposed {secret}"
        );
    }
    rows[0].clone()
}

#[tokio::test]
async fn lag_reports_missed_records_without_retiring_a_fast_native_subscriber() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let (mut slow, worker) = source(&app).await;
        let (mut fast, _) = source(&app).await;
        for sequence in 0..257u64 {
            let bytes = Bytes::copy_from_slice(&sequence.to_be_bytes());
            worker.wire.m4s.send(bytes.clone()).unwrap();
            assert_eq!(fast.records.next().await.unwrap().unwrap(), bytes);
        }
        assert!(slow.records.next().await.is_none());
        assert!(slow.cancelled.is_cancelled());
        assert!(!(slow.current)()); // repeated fence must not overwrite lag
        let event = one(&rows, "queue_lagged");
        assert_eq!(event["missed_records"], 1);
        assert_eq!(event["queued_records"], 256);
        assert_eq!(event["queued_bytes"], 2048);
        assert!((fast.current)());
        worker.wire.m4s.send(Bytes::from_static(b"fresh")).unwrap();
        assert_eq!(fast.records.next().await.unwrap().unwrap(), "fresh");
        drop((slow, fast));
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    })
    .await;
}

#[tokio::test]
async fn clean_native_eof_is_distinct_from_queue_lag_and_worker_cancellation() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let (mut source, worker) = source(&app).await;
        worker.wire.finish();
        assert!(source.records.next().await.is_none());
        assert!(source.cancelled.is_cancelled());
        assert!(!worker.is_closed());
        let event = one(&rows, "queue_closed");
        assert_eq!(event["missed_records"], 0);
        drop(source);
    })
    .await;
}

#[tokio::test]
async fn viewer_revocation_reports_a_fence_once_without_inventing_queue_loss() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let (mut source, worker) = source(&app).await;
        let id = app.playback_auth.snapshots()[0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(app.playback_auth.revoke(&id));
        for _ in 0..8 {
            assert!(!(source.current)());
        }
        assert!(source.records.next().await.is_none());
        let event = one(&rows, "authority_fenced");
        assert_eq!(event["missed_records"], 0);
        assert_eq!(event["worker_closed"], false);
        drop(source);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    })
    .await;
}

// Break caught: native unread/bootstrap demand is guessed from worker liveness,
// consumes records, survives eviction or retains a viewer after stream drop.
#[tokio::test]
async fn native_demand_tracks_bootstrap_and_each_reader_without_owning_viewers() {
    let (_dir, app) = setup();
    checked(&app, async {
        let (first, worker) = source(&app).await;
        let first_pending = first.pending_records.as_ref().unwrap().clone();
        assert_eq!(first_pending(), Some(false));
        let mut second = app
            .fxlink_provider()
            .subscribe(
                LinkRequest {
                    name: "owned".into(),
                    peer_key: "private-source-peer".into(),
                    token: "private-source-viewer".into(),
                },
                "127.0.0.1:12345".parse().unwrap(),
            )
            .await
            .unwrap();
        let pending = second.pending_records.as_ref().unwrap().clone();
        assert_eq!(pending(), Some(true), "metadata bootstrap is already ready");
        second.records.next().await.unwrap().unwrap();
        assert_eq!(pending(), Some(false));
        worker.wire.m4s.send(Bytes::from_static(b"one")).unwrap();
        worker.wire.m4s.send(Bytes::from_static(b"two")).unwrap();
        assert_eq!(pending(), Some(true));
        assert_eq!(first_pending(), Some(true));
        assert_eq!(second.records.next().await.unwrap().unwrap(), b"one"[..]);
        assert_eq!(pending(), Some(true));
        assert_eq!(second.records.next().await.unwrap().unwrap(), b"two"[..]);
        assert_eq!(pending(), Some(false));
        drop((first, second));
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert!(
            pending().is_none() && first_pending().is_none(),
            "retained hints cannot retain readers or leases"
        );
    })
    .await;
}
#[tokio::test]
async fn native_demand_stops_on_eviction_or_worker_cancellation_before_next_poll() {
    let (_dir, app) = setup();
    checked(&app, async {
        let (source, worker) = source(&app).await;
        let pending = source.pending_records.as_ref().unwrap().clone();
        for _ in 0..256 {
            worker.wire.m4s.send(Bytes::from_static(b"record")).unwrap();
        }
        assert_eq!(pending(), Some(true));
        worker.wire.m4s.send(Bytes::from_static(b"evicts")).unwrap();
        assert_eq!(pending(), None);
        drop(source);
        let (source, worker) = self::source(&app).await;
        worker.wire.m4s.send(Bytes::from_static(b"record")).unwrap();
        let pending = source.pending_records.as_ref().unwrap().clone();
        assert_eq!(pending(), Some(true));
        worker.cancellation_token().cancel();
        assert_eq!(pending(), None);
        drop(source);
    })
    .await;
}

#[tokio::test]
async fn native_timing_binds_actual_reader_and_throttles_nonconsuming_metadata() {
    let rows = Arc::new(std::sync::Mutex::new(vec![]));
    let subscriber = tracing_subscriber::registry().with(Capture(rows.clone(), true).with_filter(
        tracing_subscriber::EnvFilter::new("off,flussonix::fxlink::service_timing=trace"),
    ));
    let _capture = tracing::subscriber::set_default(subscriber);
    let (_dir, app) = setup();
    checked(&app, async {
        let (mut source, worker) = source(&app).await;
        let pending = source.pending_records.as_ref().unwrap().clone();
        assert_eq!(pending(), Some(false));
        worker.wire.m4s.send(Bytes::from_static(b"one")).unwrap();
        for _ in 0..100 {
            assert_eq!(pending(), Some(true));
        }
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert_eq!(pending(), Some(true));
        assert_eq!(source.records.next().await.unwrap().unwrap(), b"one"[..]);
        {
            let events = rows.lock().unwrap();
            let binding: Vec<_> = events
                .iter()
                .filter(|r| r.contains_key("pending_id"))
                .collect();
            assert_eq!(binding.len(), 1);
            assert_eq!(
                binding[0]["pending_id"],
                Arc::as_ptr(&pending) as *const () as usize
            );
            assert_eq!(binding[0]["bootstrap_records"], 1);
            assert_eq!(binding[0]["worker_pid"], worker.pid());
            let snapshots: Vec<_> = events
                .iter()
                .filter(|r| r.contains_key("producer_next"))
                .collect();
            assert!(snapshots.len() >= 2);
            for pair in snapshots.windows(2) {
                assert!(
                    pair[1]["elapsed_ms"].as_u64().unwrap()
                        >= pair[0]["elapsed_ms"].as_u64().unwrap() + 100
                );
            }
            let last = snapshots.last().unwrap();
            // wire.info() published absolute sequence0 before this subscription;
            // its bootstrap copy did not advance the live queue cursor.
            for (name, want) in [
                ("producer_next", 2),
                ("reader_next", 1),
                ("unread_records", 1),
                ("missed_records", 0),
                ("bootstrap_records", 0),
            ] {
                assert_eq!(last[name], want);
            }
            assert_eq!(last["reader_id"], binding[0]["reader_id"]);
            let text = serde_json::to_string(&*events).unwrap();
            for secret in [
                "private-source-viewer",
                "private-source-peer",
                "private-source-admin",
                "testsrc://",
            ] {
                assert!(!text.contains(secret));
            }
        }
        drop(source);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert_eq!(pending(), None);
    })
    .await;
}

#[tokio::test]
async fn native_timing_records_terminal_eviction_and_close_without_changing_fence() {
    for closed in [false, true] {
        let rows = Arc::new(std::sync::Mutex::new(vec![]));
        let subscriber =
            tracing_subscriber::registry().with(Capture(rows.clone(), true).with_filter(
                tracing_subscriber::EnvFilter::new("off,flussonix::fxlink::service_timing=trace"),
            ));
        let _capture = tracing::subscriber::set_default(subscriber);
        let (_dir, app) = setup();
        checked(&app, async {
            let (source, worker) = source(&app).await;
            let pending = source.pending_records.as_ref().unwrap().clone();
            if closed {
                worker.wire.finish();
            } else {
                for _ in 0..257 {
                    worker.wire.m4s.send(Bytes::from_static(b"record")).unwrap();
                }
            }
            for _ in 0..100 {
                assert_eq!(pending(), None);
            }
            {
                let events = rows.lock().unwrap();
                let terminal: Vec<_> = events
                    .iter()
                    .filter(|r| r.get("terminal") == Some(&json!(true)))
                    .collect();
                assert_eq!(
                    terminal.len(),
                    1,
                    "one invalid queue must emit final metadata once"
                );
                assert_eq!(terminal[0]["closed"], closed);
                assert_eq!(terminal[0]["missed_records"], if closed { 0 } else { 1 });
                assert_eq!(terminal[0]["reader_next"], 1);
                assert_eq!(terminal[0]["producer_next"], if closed { 1 } else { 258 });
            }
            drop(source);
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        })
        .await;
    }
}
