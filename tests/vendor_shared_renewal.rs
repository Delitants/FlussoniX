//! Renewal uses owned real HTTP; no vendor instance or runtime is required.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use flussonix::{
    config::ConfigStore,
    m4f::Frame,
    m4s::Track,
    playback_auth::{PlaybackAuth, ViewerRequest},
    source_directory::modern_coordinator::{
        AdmissionOutcome, AdmittedCandidate, CoordinatedInventory, ModernSourceCoordinator,
        SharedInputOptions, SharedInputs, SharedViewer, SourceHandle,
    },
    wire,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc, oneshot};
type Feed = mpsc::UnboundedSender<Result<Bytes, io::Error>>;
const DENIED_TOKEN: &str = "0000000000000000000000000000000000000000000000000000000000000000";
struct Remote {
    id: String,
    rows: Value,
    status: StatusCode,
    hold: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}
struct Fixture {
    _dir: tempfile::TempDir,
    config: Arc<ConfigStore>,
    owner: Arc<ModernSourceCoordinator>,
    auth: PlaybackAuth,
    handle: SourceHandle,
    key: String,
    remote: Arc<Mutex<Remote>>,
    calls: Arc<Mutex<Vec<String>>>,
    connections: mpsc::UnboundedReceiver<Feed>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn row(name: &str) -> Value {
    json!({"name":name,"groups":["same"],"named_by":"config","static":true})
}
impl Fixture {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", listener.local_addr().unwrap());
        let remote = Arc::new(Mutex::new(Remote {
            id: "origin".into(),
            rows: json!([row("one"), row("two")]),
            status: StatusCode::OK,
            hold: None,
        }));
        let calls = Arc::new(Mutex::new(vec![]));
        let (tx, connections) = mpsc::unbounded_channel();
        let (r, c) = (remote.clone(), calls.clone());
        let router = Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                let (r, c, tx) = (r.clone(), c.clone(), tx.clone());
                async move {
                    c.lock().unwrap().push(uri.path().into());
                    if !uri.path().starts_with("/streamer/") {
                        assert!(headers.contains_key("cluster-auth-key"));
                        let (feed, rx) = mpsc::unbounded_channel();
                        tx.send(feed).unwrap();
                        return Response::new(Body::from_stream(
                            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
                        ));
                    }
                    assert!(headers.contains_key("authorization"));
                    let (value, status, hold) = {
                        let mut r = r.lock().unwrap();
                        if uri.path().ends_with("/config") {
                            (json!({"stats":{"id":r.id}}), StatusCode::OK, None)
                        } else {
                            (
                                json!({"server_id":r.id,"streams":r.rows}),
                                r.status,
                                r.hold.take(),
                            )
                        }
                    };
                    if let Some((started, release)) = hold {
                        let _ = started.send(());
                        let _ = release.await;
                    }
                    Response::builder()
                        .status(status)
                        .body(Body::from(value.to_string()))
                        .unwrap()
                }
            },
        ));
        let server = tokio::spawn(async move {
            axum::serve(listener, vendor_fixture::acknowledge_media(router))
                .await
                .unwrap()
        });
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
        config
            .put(
                "sources",
                &key,
                json!({"url":key,"flussonix_source_kind":"flussonic",
                       "cluster_key":"owned-private-key","on_play":"true",
                       "group_config":{"same":{"on_play":"true"},
                                       "deny":{"on_play":"true","flussonix_token_sha256":DENIED_TOKEN}},
                       "flussonix_group_pins":{"one":["same"],"two":["same"]}}),
            )
            .unwrap();
        let owner = Arc::new(ModernSourceCoordinator::new(config.clone(), "local").unwrap());
        let handle = install(&owner, &key);
        Self {
            _dir: dir,
            config,
            owner,
            auth: PlaybackAuth::new(32),
            handle,
            key,
            remote,
            calls,
            connections,
            server,
        }
    }
    async fn inventory(&self, lifetime: Duration) -> CoordinatedInventory {
        self.owner.inventory(&self.handle, lifetime).await.unwrap()
    }
    async fn admit(
        &self,
        rows: &CoordinatedInventory,
        name: &str,
        token: &str,
    ) -> Box<AdmittedCandidate> {
        let AdmissionOutcome::Allowed(a) = self
            .owner
            .admit(
                rows.candidate(name).unwrap(),
                &self.auth,
                ViewerRequest {
                    name: name.into(),
                    token: token.into(),
                    proto: "m4s".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        else {
            panic!("denied")
        };
        a
    }
    fn hold(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (s, started) = oneshot::channel();
        let (release, r) = oneshot::channel();
        self.remote.lock().unwrap().hold = Some((s, r));
        (started, release)
    }
    async fn run(
        &mut self,
        registry: &SharedInputs,
        rows: &CoordinatedInventory,
        name: &str,
    ) -> (SharedViewer, tokio::task::JoinHandle<()>, Feed) {
        let a = registry
            .attach(
                self.admit(rows, name, name).await,
                SharedInputOptions::default(),
            )
            .unwrap();
        let viewer = a.viewer;
        let mut rx = viewer.subscribe_m4s().unwrap();
        let input = a.input.unwrap();
        let task = tokio::spawn(async move {
            let _ = input.run().await;
        });
        let feed = tokio::time::timeout(Duration::from_secs(2), self.connections.recv())
            .await
            .unwrap()
            .unwrap();
        feed.send(Ok(info())).unwrap();
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        (viewer, task, feed)
    }
    fn media_calls(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| !p.starts_with("/streamer/"))
            .count()
    }
}
fn install(owner: &ModernSourceCoordinator, key: &str) -> SourceHandle {
    owner
        .install(
            owner
                .capture(key)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap()
}
fn info() -> Bytes {
    Bytes::from(
        wire::encode_info(&[Track {
            id: 1,
            codec: "mp3".into(),
            config: vec![],
        }])
        .unwrap(),
    )
}
fn sample() -> Bytes {
    Bytes::from(
        wire::encode_frame(
            &Track {
                id: 1,
                codec: "mp3".into(),
                config: vec![],
            },
            &Frame {
                track_id: 1,
                dts: 90000,
                pts_offset: 0,
                key: true,
                body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
            },
        )
        .unwrap(),
    )
}
async fn joined(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}
async fn after(deadline: Instant) {
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        deadline + Duration::from_millis(20),
    ))
    .await;
}

#[tokio::test]
async fn equivalent_renewal_keeps_input_and_existing_viewer_but_not_standalone_admission() {
    let mut f = Fixture::start().await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_millis(600)).await;
    let old = f.admit(&rows, "one", "standalone").await;
    let previous = f.auth.snapshots();
    let (viewer, task, feed) = f.run(&registry, &rows, "one").await;
    let first_id = f
        .auth
        .snapshots()
        .into_iter()
        .find(|r| !previous.iter().any(|old| old["id"] == r["id"]))
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut rx = viewer.subscribe_m4s().unwrap();
    rx.recv().await.unwrap();
    let (started, release) = f.hold();
    let renewal = registry.renew(&f.handle, Duration::from_secs(3));
    tokio::pin!(renewal);
    tokio::select! {r=&mut renewal=>panic!("early {}",r.is_ok()),_=started=>{}}
    viewer.current().unwrap();
    feed.send(Ok(sample())).unwrap();
    assert_eq!(rx.recv().await.unwrap(), sample());
    release.send(()).unwrap();
    let result = renewal.await.unwrap();
    assert_eq!((result.renewed, result.retired), (1, 0));
    after(rows.expires_at()).await;
    viewer.current().unwrap();
    assert!(f.owner.admitted_current(&old).is_err());
    let next = registry
        .attach(
            f.admit(&result.inventory, "one", "fresh").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    assert!(next.input.is_none());
    assert_eq!(next.viewer.generation(), viewer.generation());
    assert_eq!(f.media_calls(), 1);
    assert!(f.auth.revoke(&first_id));
    assert!(viewer.current().is_err());
    next.viewer.current().unwrap();
    registry.shutdown();
    joined(task).await;
    tokio::time::timeout(Duration::from_secs(2), feed.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn renewal_retires_removed_or_changed_routes_without_stopping_equivalent_routes() {
    for change in [
        "removed",
        "kind",
        "policy",
        "relay",
        "groups",
        "id",
        "equal-policy",
    ] {
        let mut f = Fixture::start().await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(3)).await;
        let (a, ta, _fa) = f.run(&registry, &rows, "one").await;
        let (b, tb, _fb) = f.run(&registry, &rows, "two").await;
        {
            let mut remote = f.remote.lock().unwrap();
            let mut first = row("one");
            match change {
                "removed" => remote.rows = json!([row("two")]),
                "kind" => {
                    first["static"] = json!(false);
                    remote.rows = json!([first, row("two")]);
                }
                "policy" | "equal-policy" => {
                    first["groups"] = json!([if change == "policy" { "deny" } else { "same" }]);
                    remote.rows = json!([first, row("two")]);
                }
                "relay" => {
                    first["named_by"] = json!("remote");
                    remote.rows = json!([first, row("two")]);
                }
                "groups" => {
                    first["groups"] = json!([]);
                    remote.rows = json!([first, row("two")]);
                }
                "id" => remote.id = "different-origin".into(),
                _ => unreachable!(),
            }
        }
        let result = registry
            .renew(&f.handle, Duration::from_secs(3))
            .await
            .unwrap();
        let expected = if change == "equal-policy" {
            (2, 0)
        } else if change == "id" {
            (0, 2)
        } else {
            (1, 1)
        };
        assert_eq!((result.renewed, result.retired), expected, "{change}");
        assert_eq!(a.current().is_ok(), change == "equal-policy", "{change}");
        assert_eq!(b.current().is_ok(), change != "id", "{change}");
        registry.shutdown();
        joined(ta).await;
        joined(tb).await;
    }
}

#[tokio::test]
async fn failed_cancelled_or_late_renewal_never_extends_the_published_deadline() {
    for mode in ["failure", "cancel", "late"] {
        let mut f = Fixture::start().await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_millis(350)).await;
        let (v, t, _feed) = f.run(&registry, &rows, "one").await;
        if mode == "failure" {
            f.remote.lock().unwrap().status = StatusCode::FORBIDDEN;
            assert!(
                registry
                    .renew(&f.handle, Duration::from_secs(3))
                    .await
                    .is_err()
            );
        } else {
            let (started, release) = f.hold();
            {
                let renewal = registry.renew(&f.handle, Duration::from_secs(3));
                tokio::pin!(renewal);
                tokio::select! {r=&mut renewal=>panic!("early {}",r.is_ok()),_=started=>{}}
                v.current().unwrap();
                if mode == "late" {
                    after(rows.expires_at()).await;
                    let _ = release.send(());
                    assert!(renewal.await.is_err());
                }
            }
        }
        if Instant::now() < rows.expires_at() {
            v.current().unwrap();
        }
        after(rows.expires_at()).await;
        assert!(v.current().is_err());
        joined(t).await;
    }
}

#[tokio::test]
async fn scheduled_renewals_serialize_and_observe_after_prior_commit() {
    let mut f = Fixture::start().await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(3)).await;
    let (v, t, _feed) = f.run(&registry, &rows, "one").await;
    let (started, release) = f.hold();
    let old = registry.renew(&f.handle, Duration::from_secs(3));
    tokio::pin!(old);
    tokio::select! {r=&mut old=>panic!("early {}",r.is_ok()),_=started=>{}}
    let before = f.calls.lock().unwrap().len();
    let next = registry.renew(&f.handle, Duration::from_secs(3));
    tokio::pin!(next);
    tokio::select! {
        result = &mut next => panic!("overlapping observation: {}", result.is_ok()),
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
    }
    assert_eq!(f.calls.lock().unwrap().len(), before);
    release.send(()).unwrap();
    assert_eq!(old.await.unwrap().renewed, 1);
    let next = next.await.unwrap();
    assert_eq!(
        f.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/streams"))
            .count(),
        3
    );
    v.current().unwrap();
    let a = registry
        .attach(
            f.admit(&next.inventory, "one", "new").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    assert!(a.input.is_none());
    registry.shutdown();
    joined(t).await;
}

#[tokio::test]
async fn pending_renewal_cannot_cross_config_identity_install_refresh_or_shutdown() {
    for mode in [
        "config", "identity", "install", "refresh", "shutdown", "retire",
    ] {
        let mut f = Fixture::start().await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(3)).await;
        let (v, t, _feed) = f.run(&registry, &rows, "one").await;
        let (started, release) = f.hold();
        let renewal = registry.renew(&f.handle, Duration::from_secs(3));
        tokio::pin!(renewal);
        tokio::select! {r=&mut renewal=>panic!("early {}",r.is_ok()),_=started=>{}}
        match mode {
            "config" => {
                f.config
                    .put("templates", "changed", json!({"static":false}))
                    .unwrap();
            }
            "identity" => f.owner.replace_aliases(&["alias"]).unwrap(),
            "install" => {
                install(&f.owner, &f.key);
            }
            "refresh" => {
                f.inventory(Duration::from_secs(3)).await;
            }
            "shutdown" => registry.shutdown(),
            "retire" => assert!(registry.retire(&v)),
            _ => unreachable!(),
        }
        release.send(()).unwrap();
        let result = renewal.await;
        assert!(result.is_err() || result.unwrap().renewed == 0, "{mode}");
        assert!(v.current().is_err(), "{mode}");
        registry.shutdown();
        joined(t).await;
    }
}

#[tokio::test]
async fn invalid_foreign_expired_or_empty_renewal_does_not_send_inventory_requests() {
    let mut f = Fixture::start().await;
    let registry = SharedInputs::new(f.owner.clone());
    let before = f.calls.lock().unwrap().len();
    assert!(
        registry
            .renew(&f.handle, Duration::from_secs(3))
            .await
            .is_err()
    );
    assert_eq!(before, f.calls.lock().unwrap().len());
    let rows = f.inventory(Duration::from_millis(250)).await;
    let (v, t, _feed) = f.run(&registry, &rows, "one").await;
    let other = Fixture::start().await;
    let before = f.calls.lock().unwrap().len();
    for lifetime in [Duration::ZERO, Duration::from_secs(31)] {
        assert!(registry.renew(&f.handle, lifetime).await.is_err());
    }
    assert!(
        registry
            .renew(&other.handle, Duration::from_secs(3))
            .await
            .is_err()
    );
    assert_eq!(before, f.calls.lock().unwrap().len());
    v.current().unwrap();
    after(rows.expires_at()).await;
    assert!(
        registry
            .renew(&f.handle, Duration::from_secs(3))
            .await
            .is_err()
    );
    assert_eq!(before, f.calls.lock().unwrap().len());
    joined(t).await;
}

#[tokio::test]
async fn proposed_deadline_and_newer_failed_attempt_cannot_restore_older_refresh() {
    let mut f = Fixture::start().await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(3)).await;
    let (v, t, _feed) = f.run(&registry, &rows, "one").await;
    let (started, release) = f.hold();
    let short = registry.renew(&f.handle, Duration::from_millis(40));
    tokio::pin!(short);
    tokio::select! {r=&mut short=>panic!("early {}",r.is_ok()),_=started=>{}}
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _ = release.send(());
    assert!(short.await.is_err());
    v.current().unwrap();
    let (started, release) = f.hold();
    let older = registry.renew(&f.handle, Duration::from_secs(3));
    tokio::pin!(older);
    tokio::select! {r=&mut older=>panic!("early {}",r.is_ok()),_=started=>{}}
    f.remote.lock().unwrap().status = StatusCode::FORBIDDEN;
    let before = f.calls.lock().unwrap().len();
    let newer = registry.renew(&f.handle, Duration::from_secs(3));
    tokio::pin!(newer);
    tokio::select! {
        result = &mut newer => panic!("overlapping observation: {}", result.is_ok()),
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
    }
    assert_eq!(f.calls.lock().unwrap().len(), before);
    v.current().unwrap();
    release.send(()).unwrap();
    assert_eq!(older.await.unwrap().renewed, 1);
    assert!(newer.await.is_err());
    v.current().unwrap();
    f.remote.lock().unwrap().status = StatusCode::OK;
    assert_eq!(
        registry
            .renew(&f.handle, Duration::from_secs(3))
            .await
            .unwrap()
            .renewed,
        1
    );
    registry.shutdown();
    joined(t).await;
}

#[tokio::test]
async fn renewed_worker_still_fences_media_after_configuration_change() {
    let mut f = Fixture::start().await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(3)).await;
    let (v, t, feed) = f.run(&registry, &rows, "one").await;
    registry
        .renew(&f.handle, Duration::from_secs(3))
        .await
        .unwrap();
    let mut rx = v.subscribe_m4s().unwrap();
    rx.recv().await.unwrap();
    feed.send(Ok(sample())).unwrap();
    rx.recv().await.unwrap();
    f.config
        .put("templates", "changed", json!({"static":false}))
        .unwrap();
    let _ = feed.send(Ok(sample()));
    assert!(rx.recv().await.is_err());
    assert!(v.tracks().is_err());
    joined(t).await;
}

#[tokio::test]
async fn renewal_never_revives_a_stale_unstarted_worker_beside_a_fresh_route() {
    for expired in [false, true] {
        let f = Fixture::start().await;
        let registry = SharedInputs::new(f.owner.clone());
        let old = f
            .inventory(if expired {
                Duration::from_millis(30)
            } else {
                Duration::from_secs(3)
            })
            .await;
        let a = registry
            .attach(
                f.admit(&old, "one", "old").await,
                SharedInputOptions::default(),
            )
            .unwrap();
        if expired {
            after(old.expires_at()).await;
        }
        let fresh = f.inventory(Duration::from_secs(3)).await;
        assert!(a.viewer.current().is_err());
        let b = registry
            .attach(
                f.admit(&fresh, "two", "fresh").await,
                SharedInputOptions::default(),
            )
            .unwrap();
        let result = registry
            .renew(&f.handle, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!((result.renewed, result.retired), (1, 1));
        assert!(a.viewer.current().is_err(), "invalidated route was revived");
        b.viewer.current().unwrap();
        assert_eq!(f.media_calls(), 0);
        registry.shutdown();
        drop((a.input, b.input));
    }
}

#[tokio::test]
async fn eof_waits_for_scheduled_then_observes_again() {
    let mut f = Fixture::start().await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(3)).await;
    let (_viewer, worker, _feed) = f.run(&registry, &rows, "one").await;
    let (started, release) = f.hold();
    let first_registry = registry.clone();
    let first_handle = f.handle.clone();
    let scheduled = tokio::spawn(async move {
        first_registry
            .renew(&first_handle, Duration::from_secs(3))
            .await
    });
    started.await.unwrap();
    let before = f.calls.lock().unwrap().len();
    let eof_registry = registry.clone();
    let eof_handle = f.handle.clone();
    let eof = tokio::spawn(async move {
        eof_registry
            .renew_after_eof(&eof_handle, Duration::from_secs(3))
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.calls.lock().unwrap().len(), before);
    release.send(()).unwrap();
    assert_eq!(scheduled.await.unwrap().unwrap().renewed, 1);
    assert_eq!(eof.await.unwrap().unwrap().renewed, 1);
    assert_eq!(
        f.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/streams"))
            .count(),
        3
    );
    registry.shutdown();
    joined(worker).await;
}

#[tokio::test]
async fn eof_renewal_capacity_and_timeout_are_bounded() {
    let mut f = Fixture::start().await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_millis(350)).await;
    let (_viewer, worker, _feed) = f.run(&registry, &rows, "one").await;
    let (started, _release) = f.hold();
    let mut attempts = Vec::new();
    for _ in 0..5 {
        let registry = registry.clone();
        let handle = f.handle.clone();
        attempts.push(tokio::spawn(async move {
            registry
                .renew_after_eof(&handle, Duration::from_secs(3))
                .await
        }));
    }
    started.await.unwrap();
    let before = f.calls.lock().unwrap().len();
    for attempt in attempts {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), attempt)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
    assert_eq!(f.calls.lock().unwrap().len(), before);
    registry.shutdown();
    joined(worker).await;
}

#[tokio::test]
async fn eof_renewal_uses_four_slots_across_distinct_sources() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
    let holding = Arc::new(AtomicBool::new(false));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let mut sources = Vec::new();
    let mut releases = Vec::new();
    let mut servers = Vec::new();
    for index in 0..5 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4f://{}", listener.local_addr().unwrap());
        let name = format!("source-{index}");
        let id = format!("origin-{index}");
        let release = Arc::new(Semaphore::new(0));
        let router = Router::new().fallback(get({
            let (name, id) = (name.clone(), id.clone());
            let (release, holding, started) =
                (release.clone(), holding.clone(), started_tx.clone());
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                let (name, id) = (name.clone(), id.clone());
                let (release, holding, started) =
                    (release.clone(), holding.clone(), started.clone());
                async move {
                    assert!(uri.path().starts_with("/streamer/"));
                    assert!(headers.contains_key("authorization"));
                    let body = if uri.path().ends_with("/config") {
                        json!({"stats":{"id":id}})
                    } else {
                        assert!(uri.path().ends_with("/streams"));
                        if holding.load(Ordering::SeqCst) {
                            started.send(index).unwrap();
                            release.acquire().await.unwrap().forget();
                        }
                        json!({"server_id":id,"streams":[row(&name)]})
                    };
                    Response::new(Body::from(body.to_string()))
                }
            }
        }));
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
        config
            .put(
                "sources",
                &key,
                json!({"url":key,"flussonix_source_kind":"flussonic",
                       "cluster_key":"owned-private-key","on_play":"true",
                       "group_config":{"same":{"on_play":"true"}},
                       "flussonix_group_pins":{(name.clone()):["same"]}}),
            )
            .unwrap();
        sources.push((key, name));
        releases.push(release);
    }
    let owner = Arc::new(ModernSourceCoordinator::new(config, "local").unwrap());
    let registry = Arc::new(SharedInputs::new(owner.clone()));
    let auth = PlaybackAuth::new(32);
    let mut handles = Vec::new();
    let mut attachments = Vec::new();
    for (key, name) in &sources {
        let handle = install(&owner, key);
        let rows = owner
            .inventory(&handle, Duration::from_secs(8))
            .await
            .unwrap();
        let AdmissionOutcome::Allowed(admission) = owner
            .admit(
                rows.candidate(name).unwrap(),
                &auth,
                ViewerRequest {
                    name: name.clone(),
                    token: name.clone(),
                    proto: "m4f".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        else {
            panic!("owned source denied");
        };
        let attachment = registry
            .attach(admission, SharedInputOptions::default())
            .unwrap();
        assert!(attachment.input.is_some());
        handles.push(handle);
        attachments.push(attachment);
    }
    holding.store(true, Ordering::SeqCst);
    let tasks = handles
        .into_iter()
        .map(|handle| {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .renew_after_eof(&handle, Duration::from_secs(8))
                    .await
            })
        })
        .collect::<Vec<_>>();
    let mut first = HashSet::new();
    for _ in 0..4 {
        let index = tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(first.insert(index), "a source entered twice before release");
    }
    assert_eq!(first.len(), 4);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), started_rx.recv())
            .await
            .is_err(),
        "fifth distinct source entered without a free EOF slot"
    );
    releases[*first.iter().next().unwrap()].add_permits(1);
    let fifth = tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!first.contains(&fifth));
    for release in &releases {
        release.add_permits(1);
    }
    for task in tasks {
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!((result.renewed, result.retired), (1, 0));
    }
    registry.shutdown();
    drop(attachments);
    for server in servers {
        server.abort();
    }
}
