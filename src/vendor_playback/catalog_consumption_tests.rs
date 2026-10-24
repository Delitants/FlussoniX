//! Real discovery barriers catch stale replica observations and authority bypasses.
use super::*;
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::oneshot;

type Hold = Arc<std::sync::Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>>;
struct Origin {
    key: String,
    status: Arc<std::sync::Mutex<StatusCode>>,
    hold: Hold,
    catalogs: Arc<AtomicUsize>,
    disabled: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

#[tokio::test]
async fn accepted_renewal_rejection_invalidates_older_catalog_publication() {
    use crate::source_directory::{
        LookupFailure,
        flussonic::modern::ObservedServerId,
        flussonic_config::{BindingFailure, LeaseFailure, SourceAuthority},
    };
    let origin = Origin::start("renew-order").await;
    let source = json!({"url": origin.key, "cluster_key": "owned-probe-key", "on_play": "true"});
    let authority =
        SourceAuthority::new_modern(&source, &json!([]), Duration::from_secs(3)).unwrap();
    let local = ObservedServerId::parse("local").unwrap();
    let older = authority
        .leased_modern_inventory(&local, Duration::from_secs(30))
        .await
        .unwrap();
    let candidate = older.candidate("owned").unwrap();
    let pending = authority
        .begin_modern_renewal(&candidate, Duration::from_secs(30))
        .unwrap();
    *origin.status.lock().unwrap() = StatusCode::BAD_REQUEST;
    let observed = pending.observe(&[local]).await;
    let rejected = authority.commit_modern_renewal(pending, observed, |_| {}, |_, _| ((), || {}));
    assert!(matches!(
        rejected,
        Err(LeaseFailure::Binding(BindingFailure::Inventory(
            LookupFailure::Invalid
        )))
    ));
    assert!(matches!(
        authority.with_modern_inventory_current(&older, || ()),
        Err(LeaseFailure::Superseded)
    ));
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Origin {
    async fn start(id: &'static str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", listener.local_addr().unwrap());
        let status = Arc::new(std::sync::Mutex::new(StatusCode::OK));
        let hold: Hold = Arc::new(std::sync::Mutex::new(None));
        let catalogs = Arc::new(AtomicUsize::new(0));
        let disabled = Arc::new(AtomicBool::new(false));
        let d = disabled.clone();
        let (s, h, c) = (status.clone(), hold.clone(), catalogs.clone());
        let service=Router::new().fallback(get(move |OriginalUri(uri):OriginalUri,headers:HeaderMap| {
            let(s,h,c,d)=(s.clone(),h.clone(),c.clone(),d.clone()); async move {
                let auth=headers["authorization"].to_str().unwrap().strip_prefix("Cluster ").unwrap();
                let(time,hash)=auth.split_once(':').unwrap();
                let digest=ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY,format!("{time}:owned-probe-key").as_bytes()).as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>();
                assert_eq!(hash,digest);
                if uri.path().ends_with("/config") {
                    let pause=h.lock().unwrap().take();
                    if let Some((started,resume))=pause {let _=started.send(());let _=resume.await;}
                }
                let status=*s.lock().unwrap();
                if status!=StatusCode::OK {return Response::builder().status(status).body(Body::empty()).unwrap();}
                let body=if uri.path().ends_with("/config") {json!({"stats":{"id":id}})} else {
                    assert_eq!(uri.path(),"/streamer/api/v3/streams");
                    c.fetch_add(1,Ordering::SeqCst);
                    json!({"server_id":id,"streams":[{"name":"owned","named_by":"config","static":false,"disabled":d.load(Ordering::SeqCst),"groups":[]}]})
                };
                Response::new(Body::from(body.to_string()))
            }
        }));
        Self {
            key,
            status,
            hold,
            catalogs,
            disabled,
            task: tokio::spawn(async move { axum::serve(listener, service).await.unwrap() }),
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Refresh {
    None,
    Healthy,
    Unavailable,
    Disabled,
    InFlight,
    ConfigChanged,
    Cancelled,
    EarlierHealthy,
}
struct Fixture {
    origins: [Origin; 3],
    app: Arc<App>,
    runtime: Arc<Runtime>,
    cancel: CancellationToken,
    driver: tokio::task::JoinHandle<Result<(), String>>,
    _dir: tempfile::TempDir,
}
impl Fixture {
    async fn start() -> Self {
        let origins = [
            Origin::start("probe-a").await,
            Origin::start("probe-b").await,
            Origin::start("probe-c").await,
        ];
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            crate::server::Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-native-peer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        for origin in &origins {
            app.config.put("sources",&origin.key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-probe-key","on_play":"true","only":[],"flussonix_source_group":"probe-replicas","flussonix_content_id":"music"})).unwrap();
        }
        let owner =
            Arc::new(ModernSourceCoordinator::new(app.config.clone(), "probe-local").unwrap());
        let runtime = Arc::new(Runtime::new(owner));
        let cancel = CancellationToken::new();
        let (r, a, c) = (runtime.clone(), app.clone(), cancel.clone());
        let driver = tokio::spawn(async move { r.run(&a, c).await });
        while !runtime.running() {
            tokio::task::yield_now().await;
        }
        Self {
            origins,
            app,
            runtime,
            cancel,
            driver,
            _dir: dir,
        }
    }
    async fn stop(self) {
        self.cancel.cancel();
        self.driver.await.unwrap().unwrap();
        self.app.media.stop_all().await;
        assert_eq!(self.app.playback_auth.live_grants(), 0);
    }
}
fn hold(origin: &Origin) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (started, wait) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    *origin.hold.lock().unwrap() = Some((started, resume));
    (wait, release)
}
async fn run(refresh: Refresh) {
    let f = Fixture::start().await;
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    *f.origins[1].status.lock().unwrap() = if refresh == Refresh::EarlierHealthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let (wait, release) = hold(&f.origins[1]);
    let (a, r, key) = (f.app.clone(), f.runtime.clone(), f.origins[0].key.clone());
    let revision = f.app.config.revision();
    let mut query = tokio::spawn(async move { r.candidate(&a, &key, revision, "owned").await });
    tokio::time::timeout(Duration::from_secs(2), wait)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.origins[2].catalogs.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, old) = f
        .runtime
        .replica_inventory(&f.app, &f.origins[2].key, revision, replicas::Probe::Cached)
        .await
        .unwrap();
    let old = old.unwrap();
    assert!(f.runtime.owner.at_inventory_current(&old, || ()).is_ok());
    assert_eq!(f.origins[2].catalogs.load(Ordering::SeqCst), 1);
    assert!(
        !query.is_finished(),
        "predecessor must remain held through the refresh"
    );
    let in_flight = matches!(
        refresh,
        Refresh::InFlight | Refresh::ConfigChanged | Refresh::Cancelled
    );
    let mut pending = None;
    let mut resume_refresh = None;
    if refresh != Refresh::None {
        // Existing probe cooldown must expire; no private timing hook or retuned deadline.
        tokio::time::sleep(Duration::from_millis(501)).await;
        if refresh == Refresh::Unavailable {
            *f.origins[2].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
        }
        if refresh == Refresh::Disabled {
            f.origins[2].disabled.store(true, Ordering::SeqCst);
        }
        if in_flight {
            let (wait, release) = hold(&f.origins[2]);
            let (a, r, key) = (f.app.clone(), f.runtime.clone(), f.origins[2].key.clone());
            pending = Some(tokio::spawn(async move {
                r.replica_inventory(&a, &key, revision, replicas::Probe::Fresh)
                    .await
            }));
            tokio::time::timeout(Duration::from_secs(2), wait)
                .await
                .unwrap()
                .unwrap();
            resume_refresh = Some(release);
        } else {
            let (_, rows) = f
                .runtime
                .replica_inventory(&f.app, &f.origins[2].key, revision, replicas::Probe::Fresh)
                .await
                .unwrap();
            assert_eq!(rows.is_ok(), refresh != Refresh::Unavailable);
        }
        assert_eq!(
            f.runtime.owner.at_inventory_current(&old, || ()),
            Err(
                crate::source_directory::modern_coordinator::CoordinationFailure::Lease(
                    crate::source_directory::flussonic_config::LeaseFailure::Superseded
                )
            )
        );
    }
    if refresh == Refresh::ConfigChanged {
        f.app
            .config
            .put("templates", "replacement", json!({}))
            .unwrap();
        assert_ne!(f.app.config.revision(), revision);
    }
    release.send(()).unwrap();
    let mut stayed_pending = true;
    let result = if refresh == Refresh::Cancelled {
        f.cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), &mut query)
            .await
            .unwrap()
            .unwrap();
        // The candidate must stop while a live source lock is still held.
        assert!(matches!(result, Err(Failure::Closed)));
        pending.take().unwrap().abort();
        result
    } else {
        if refresh == Refresh::InFlight {
            match tokio::time::timeout(Duration::from_millis(100), &mut query).await {
                Err(_) => {}
                Ok(done) => {
                    stayed_pending = false;
                    query = tokio::spawn(async move { done.unwrap() });
                }
            }
        }
        if let Some(resume) = resume_refresh.take() {
            let _ = resume.send(());
        }
        if let Some(task) = pending.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), query)
            .await
            .unwrap()
            .unwrap()
    };
    let selected = result.as_ref().ok().map(|c| c.source_key().to_owned());
    let expected = if refresh == Refresh::EarlierHealthy {
        f.origins[1].key.clone()
    } else {
        f.origins[2].key.clone()
    };
    let catalogs = f.origins[2].catalogs.load(Ordering::SeqCst);
    if let Some(resume) = resume_refresh.take() {
        let _ = resume.send(());
    }
    f.stop().await;
    assert!(
        stayed_pending,
        "selection must await the in-flight replacement, within the existing group budget"
    );
    if matches!(
        refresh,
        Refresh::Unavailable | Refresh::Disabled | Refresh::ConfigChanged | Refresh::Cancelled
    ) {
        assert!(
            result.is_err(),
            "failed, rejected, replaced or cancelled catalog must not admit retained facts"
        );
    } else {
        assert_eq!(
            selected.as_deref(),
            Some(expected.as_str()),
            "healthy current replica must remain reachable in declared order"
        );
        assert_eq!(catalogs, if refresh == Refresh::None { 1 } else { 2 });
    }
}
#[tokio::test]
async fn held_predecessor_without_refresh_uses_healthy_last_replica() {
    run(Refresh::None).await;
}
#[tokio::test]
async fn healthy_refresh_during_held_predecessor_uses_current_last_replica() {
    run(Refresh::Healthy).await;
}
#[tokio::test]
async fn failed_refresh_during_held_predecessor_never_reuses_old_catalog() {
    run(Refresh::Unavailable).await;
}
#[tokio::test]
async fn disabled_refresh_during_held_predecessor_never_reuses_old_catalog() {
    run(Refresh::Disabled).await;
}
#[tokio::test]
async fn in_flight_refresh_is_consumed_after_it_completes() {
    run(Refresh::InFlight).await;
}
#[tokio::test]
async fn config_replacement_during_refresh_cannot_admit_old_group() {
    run(Refresh::ConfigChanged).await;
}
#[tokio::test]
async fn owner_cancellation_stops_selection_while_refresh_holds_source_lock() {
    run(Refresh::Cancelled).await;
}
#[tokio::test]
async fn earlier_healthy_replica_wins_over_later_refreshed_catalog() {
    run(Refresh::EarlierHealthy).await;
}
#[tokio::test]
async fn selected_authoritative_denial_prevents_alternative_discovery() {
    let f = Fixture::start().await;
    f.origins[0].disabled.store(true, Ordering::SeqCst);
    let result = f
        .runtime
        .candidate(&f.app, &f.origins[0].key, f.app.config.revision(), "owned")
        .await;
    assert!(result.is_err());
    assert_eq!(f.origins[1].catalogs.load(Ordering::SeqCst), 0);
    assert_eq!(f.origins[2].catalogs.load(Ordering::SeqCst), 0);
    f.stop().await;
}
