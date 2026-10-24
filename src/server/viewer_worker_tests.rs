use super::*;
use std::{future::Future, pin::Pin, task::Poll};
use tower::ServiceExt;

#[derive(Clone, Copy, Debug)]
enum Flow {
    Http,
    Rtsp,
}
impl Flow {
    fn proto(self) -> &'static str {
        match self {
            Self::Http => "mpegts",
            Self::Rtsp => "rtsp",
        }
    }
}
fn viewer(token: &str, proto: &str) -> ViewerRequest {
    ViewerRequest {
        name: "owned".into(),
        proto: proto.into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        ..Default::default()
    }
}
fn admission(app: Arc<App>, flow: Flow) -> Pin<Box<dyn Future<Output = u16> + Send>> {
    Box::pin(async move {
        match flow {
            Flow::Http => router(app)
                .oneshot(
                    Request::builder()
                        .uri("/owned/mpegts?token=pending")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
                .as_u16(),
            Flow::Rtsp => match app
                .rtsp_admit(
                    viewer("pending", flow.proto()),
                    &url::Url::parse("rtsp://127.0.0.1/owned?token=pending").unwrap(),
                    false,
                )
                .await
            {
                Ok(_) => 200,
                Err(status) => status,
            },
        }
    })
}
struct Fixture {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    task: tokio::task::JoinHandle<()>,
    policy: PolicySnapshot,
    victim: ts_access::Playback,
    pending: Grant,
}
impl Fixture {
    async fn new(flow: Flow) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let callback = format!("http://{}/auth", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/auth", axum::routing::get(|axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>| async move {
                let account = if q.get("token").is_some_and(|t| t == "victim") { "independent-victim" } else { "own-account" };
                ([("x-userid", account), ("x-unique", "true"), ("x-authduration", "180")], "")
            }))).await.unwrap();
        });
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-worker-race-admin".into(),
                peer_key: "owned-worker-race-peer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":callback}),
            )
            .unwrap();
        let mut victim = app.ts_admit(viewer("victim", "srt")).await.unwrap();
        victim.attach();
        let mut receiver = victim.worker.subscribe();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                .await
                .unwrap()
                .is_ok()
        );
        let policy = app.resolve("owned").await.unwrap().policy;
        let AuthOutcome::Allowed(pending) = app
            .playback_auth
            .authorize_control(policy.clone(), viewer("pending", flow.proto()))
            .await
        else {
            panic!("pending viewer denied")
        };
        Self {
            _dir: dir,
            app,
            task,
            policy,
            victim,
            pending,
        }
    }
    async fn replace_pending(&self, flow: Flow) -> Grant {
        let AuthOutcome::Allowed(replacement) = self
            .app
            .playback_auth
            .authorize_control(self.policy.clone(), viewer("replacement", flow.proto()))
            .await
        else {
            panic!("replacement viewer denied")
        };
        assert!(
            self.pending.is_cancelled(),
            "own-account unique admission must cancel pending viewer"
        );
        assert!(
            self.victim.current(),
            "different account must retain its grant"
        );
        replacement
    }
    async fn finish(&self) {
        self.app.media.stop_all().await;
        self.task.abort();
    }
}
async fn guarded_cancellation(flow: Flow) {
    let f = Fixture::new(flow).await;
    let pid = f.victim.worker.pid();
    let startups = f.app.media.hold_startups_for_test().await;
    let mut admitting = admission(f.app.clone(), flow);
    // Authorization intentionally yields once in unit tests; the second poll
    // reaches the held worker-map lock before we take the route lock.
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    assert_eq!(f.app.playback_auth.live_grants(), 3);
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    let mirrors = f.app.mirrors.lock().await;
    drop(startups);
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    let _replacement = f.replace_pending(flow).await;
    drop(mirrors);
    let status = tokio::time::timeout(Duration::from_secs(3), admitting)
        .await
        .unwrap();
    let alive = !f.victim.worker.is_closed();
    let same = f.app.media.count().await == 1 && f.victim.worker.pid() == pid;
    let mut receiver = f.victim.worker.subscribe();
    let delivered = alive
        && tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .is_ok_and(|r| r.is_ok_and(|b| !b.is_empty()));
    f.finish().await;
    eprintln!(
        "flow={flow:?} status={status} victim_alive={alive} same_worker={same} fresh_delivery={delivered}"
    );
    assert!(status >= 400);
    assert!(
        alive && same && delivered,
        "canceling a pending {flow:?} viewer stopped another account's shared worker"
    );
}
#[tokio::test]
async fn unique_canceled_http_admission_preserves_shared_worker() {
    guarded_cancellation(Flow::Http).await;
}
#[tokio::test]
async fn unique_canceled_rtsp_admission_preserves_shared_worker() {
    guarded_cancellation(Flow::Rtsp).await;
}

#[derive(Clone, Copy, Debug)]
enum FinalChange {
    Cancel,
    Stale,
    Replacement,
    CancelAndStale,
    CancelAndReplacement,
}
async fn final_route_change(flow: Flow, change: FinalChange) {
    let f = Fixture::new(flow).await;
    let pid = f.victim.worker.pid();
    let startups = f.app.media.hold_startups_for_test().await;
    let mut admitting = admission(f.app.clone(), flow);
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    let mirrors = f.app.mirrors.lock().await;
    drop(startups);
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    // Queue a second holder behind the guarded-start lookup. Tokio's mutex
    // is FIFO: after that lookup returns W, the final lookup must wait here.
    let mut next = Box::pin(f.app.mirrors.lock());
    assert!(matches!(futures_util::poll!(next.as_mut()), Poll::Pending));
    drop(mirrors);
    assert!(matches!(
        futures_util::poll!(admitting.as_mut()),
        Poll::Pending
    ));
    let mirrors = next.await;
    let mut replacement_worker = None;
    let mut replacement_grant = None;
    if matches!(
        change,
        FinalChange::Cancel | FinalChange::CancelAndStale | FinalChange::CancelAndReplacement
    ) {
        replacement_grant = Some(f.replace_pending(flow).await);
    }
    if matches!(
        change,
        FinalChange::Stale
            | FinalChange::Replacement
            | FinalChange::CancelAndStale
            | FinalChange::CancelAndReplacement
    ) {
        let mut disk = f.app.config.snapshot()["streams"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == "owned")
            .unwrap()
            .clone();
        disk["transcoder"] = json!({"encoder":"libx264","vb":900});
        f.app.config.put("streams", "owned", disk).unwrap();
        let cfg = f.app.config.effective("owned").unwrap();
        assert_ne!(
            crate::media::media_signature(&cfg),
            f.victim.worker.signature()
        );
        if matches!(
            change,
            FinalChange::Replacement | FinalChange::CancelAndReplacement
        ) {
            replacement_worker = Some(f.app.media.ensure("owned", &cfg).await.unwrap());
        }
    }
    drop(mirrors);
    let status = tokio::time::timeout(Duration::from_secs(3), admitting)
        .await
        .unwrap();
    let alive = !f.victim.worker.is_closed();
    let count = f.app.media.count().await;
    let replacement_alive = replacement_worker
        .as_ref()
        .is_some_and(|w| !w.is_closed() && w.pid() != pid);
    let mut receiver = f.victim.worker.subscribe();
    let delivered = alive
        && tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .is_ok_and(|r| r.is_ok_and(|b| !b.is_empty()));
    f.finish().await;
    drop(replacement_grant);
    eprintln!(
        "flow={flow:?} final_change={change:?} status={status} old_alive={alive} workers={count} replacement_alive={replacement_alive}"
    );
    assert!(
        status >= 400,
        "canceled or stale final admission was accepted"
    );
    match change {
        FinalChange::Cancel => assert!(alive && count == 1 && delivered),
        FinalChange::Stale | FinalChange::CancelAndStale => assert!(!alive && count == 0),
        FinalChange::Replacement | FinalChange::CancelAndReplacement => {
            assert!(!alive && count == 1 && replacement_alive)
        }
    }
}
#[tokio::test]
async fn final_route_wait_cancellation_preserves_shared_worker() {
    for flow in [Flow::Http, Flow::Rtsp] {
        final_route_change(flow, FinalChange::Cancel).await;
    }
}
#[tokio::test]
async fn final_route_mismatch_retires_exact_stale_worker() {
    for flow in [Flow::Http, Flow::Rtsp] {
        final_route_change(flow, FinalChange::Stale).await;
    }
}
#[tokio::test]
async fn final_route_mismatch_preserves_replacement_generation() {
    for flow in [Flow::Http, Flow::Rtsp] {
        final_route_change(flow, FinalChange::Replacement).await;
    }
}
#[tokio::test]
async fn cancellation_with_stale_http_route_still_retires_exact_worker() {
    final_route_change(Flow::Http, FinalChange::CancelAndStale).await;
}
#[tokio::test]
async fn cancellation_with_stale_rtsp_route_still_retires_exact_worker() {
    final_route_change(Flow::Rtsp, FinalChange::CancelAndStale).await;
}
#[tokio::test]
async fn cancellation_with_stale_route_preserves_replacement_worker() {
    for flow in [Flow::Http, Flow::Rtsp] {
        final_route_change(flow, FinalChange::CancelAndReplacement).await;
    }
}
