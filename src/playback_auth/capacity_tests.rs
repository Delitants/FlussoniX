use super::*;

struct Backend {
    url: String,
    status: Arc<AtomicU64>,
    requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<Semaphore>,
    stop: CancellationToken,
    task: tokio_util::task::AbortOnDropHandle<()>,
}

impl Backend {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/auth", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU64::new(200));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let router = axum::Router::new().route(
            "/auth",
            axum::routing::get({
                let status = status.clone();
                let requests = requests.clone();
                let entered = entered.clone();
                let release = release.clone();
                move |axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>| {
                    let status = status.clone();
                    let requests = requests.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        let blocked = q["token"] == "seed-0";
                        requests.lock().unwrap().push(q);
                        if blocked {
                            entered.notify_one();
                            release.acquire().await.unwrap().forget();
                        }
                        (
                            axum::http::StatusCode::from_u16(status.load(Ordering::Relaxed) as u16)
                                .unwrap(),
                            [("location", "https://example.test/watch")],
                        )
                    }
                }
            }),
        );
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(cancel.cancelled_owned())
                .await
                .unwrap();
        }));
        Self {
            url,
            status,
            requests,
            entered,
            release,
            stop,
            task,
        }
    }

    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

fn policy(auth: &PlaybackAuth, backend: &Backend) -> PolicySnapshot {
    auth.publish(
        "owned",
        Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
    )
    .unwrap()
}

fn request(token: &str) -> ViewerRequest {
    ViewerRequest {
        name: "owned".into(),
        proto: "hls".into(),
        token: token.into(),
        ..Default::default()
    }
}

fn seed(auth: &PlaybackAuth, snapshot: &PolicySnapshot, count: usize, decision: Decision) {
    let now = Instant::now();
    let mut all = auth.entries.lock().unwrap();
    let sessions = all.by_stream.entry("owned".into()).or_default();
    for i in 0..count {
        let request = request(&format!("seed-{i}"));
        let identity = snapshot.policy.identity(&request);
        sessions.insert(
            identity,
            Arc::new(Entry {
                id: uuid::Uuid::new_v4().to_string(),
                state: Mutex::new(State {
                    policy: snapshot.policy.clone(),
                    request,
                    decision: decision.clone(),
                    next_check: now + Duration::from_secs(180),
                    allowed_until: now,
                    created: now,
                    last_seen: now,
                    playback_seen: None,
                    segmented_seen: None,
                    continuous_live: 0,
                    control_live: 0,
                    segmented_live: 0,
                    segmented_control_live: 0,
                    handoff_live: 0,
                    effective_max: None,
                    unique: false,
                    number: 0,
                    generation: 0,
                    user_id: "owned".into(),
                    available: true,
                    revoked_until: None,
                    cancel: CancellationToken::new(),
                }),
                flight: AsyncMutex::new(()),
                remote: AsyncMutex::new(RemoteState::default()),
                remote_notify: Notify::new(),
                remote_epoch: Instant::now(),
                remote_deadline_micros: AtomicU64::new(0),
                live: AtomicU64::new(0),
                admissions: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            }),
        );
    }
}

fn cached(auth: &PlaybackAuth, snapshot: &PolicySnapshot, token: &str) -> Arc<Entry> {
    auth.entries.lock().unwrap().by_stream["owned"][&snapshot.policy.identity(&request(token))]
        .clone()
}

// Retaining ordinary failures to their callback TTL must not exhaust admission.
#[tokio::test]
async fn capacity_pressure_reclaims_unused_denied_unknown_and_redirect_entries() {
    for decision in [
        Decision::Deny,
        Decision::Unknown,
        Decision::Redirect("https://example.test/watch".into()),
    ] {
        let auth = PlaybackAuth::new(32);
        let p = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        seed(&auth, &p, 20_000, decision);
        let result = auth.authorize_control(p, request("fresh-valid")).await;
        assert!(
            matches!(result, AuthOutcome::Allowed(_)),
            "unused failures blocked a valid new viewer"
        );
        assert!(auth.entries.lock().unwrap().len() <= 20_000);
    }
}

// Background callbacks own Arc<Entry> without incrementing request admissions.
#[tokio::test]
async fn ordinary_sweeps_keep_external_owners_and_explicit_revocation_holds() {
    for background in [false, true] {
        let auth = PlaybackAuth::new(32);
        let p = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        seed(&auth, &p, 2, Decision::Deny);
        let callback_owner = cached(&auth, &p, "seed-0");
        let revocation = cached(&auth, &p, "seed-1");
        let revoked_id = revocation.id.clone();
        assert!(auth.revoke(&revoked_id));
        for entry in [&callback_owner, &revocation] {
            let mut state = entry.state.lock().unwrap();
            state.last_seen = Instant::now() - Duration::from_secs(60);
            state.next_check = Instant::now();
        }
        drop(revocation);
        if background {
            auth.renew_due().await;
        } else {
            assert!(matches!(
                auth.authorize_control(p.clone(), request("fresh")).await,
                AuthOutcome::Allowed(_)
            ));
        }
        {
            let all = auth.entries.lock().unwrap();
            assert!(
                all.contains_key("owned", &p.policy.identity(&request("seed-0"))),
                "in-flight callback owner disappeared"
            );
            assert!(
                all.values().any(|e| e.id == revoked_id),
                "explicit revocation disappeared before its deadline"
            );
        }
        assert!(matches!(
            auth.authorize_control(p, request("seed-1")).await,
            AuthOutcome::Denied
        ));
    }
}

// Pressure may not discard a live, pending, recent, external, allowed or revoked record.
#[tokio::test]
async fn capacity_pressure_fails_closed_when_every_record_is_protected() {
    let auth = PlaybackAuth::new(32);
    let p = auth
        .publish(
            "owned",
            Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
        )
        .unwrap();
    seed(&auth, &p, 20_000, Decision::Allow);
    let mut external = None;
    for (i, protection) in ["live", "admission", "recent", "external", "revoke"]
        .iter()
        .enumerate()
    {
        let entry = cached(&auth, &p, &format!("seed-{i}"));
        {
            let mut state = entry.state.lock().unwrap();
            state.decision = Decision::Deny;
            match *protection {
                "live" => {
                    entry.live.store(1, Ordering::Relaxed);
                }
                "admission" => {
                    entry.admissions.store(1, Ordering::Relaxed);
                }
                "recent" => {
                    state.playback_seen = Some(Instant::now());
                }
                "external" => {
                    external = Some(entry.clone());
                }
                "revoke" => {
                    state.revoked_until = Some(Instant::now() + Duration::from_secs(180));
                }
                _ => unreachable!(),
            }
        }
    }
    let result = auth.authorize_control(p.clone(), request("fresh")).await;
    assert!(matches!(result, AuthOutcome::Denied));
    assert_eq!(auth.entries.lock().unwrap().len(), 20_000);
    for i in 0..5 {
        assert!(
            auth.entries
                .lock()
                .unwrap()
                .contains_key("owned", &p.policy.identity(&request(&format!("seed-{i}"))))
        );
    }
    drop(external);
    let result = auth.authorize_control(p, request("fresh")).await;
    assert!(
        matches!(result, AuthOutcome::Allowed(_)),
        "released external owner did not become reclaimable"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capacity_pressure_serializes_concurrent_and_duplicate_admissions() {
    let auth = Arc::new(PlaybackAuth::new(64));
    let p = auth
        .publish(
            "owned",
            Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
        )
        .unwrap();
    seed(&auth, &p, 20_000, Decision::Deny);
    let start = Arc::new(tokio::sync::Barrier::new(32));
    let mut tasks = Vec::new();
    for i in 0..32 {
        let auth = auth.clone();
        let p = p.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            let token = if i < 16 {
                "duplicate".into()
            } else {
                format!("viewer-{i}")
            };
            auth.authorize_control(p, request(&token)).await
        }));
    }
    let mut grants = Vec::new();
    for task in tasks {
        let AuthOutcome::Allowed(grant) = task.await.unwrap() else {
            panic!("concurrent valid viewer rejected")
        };
        grants.push(grant);
    }
    let all = auth.entries.lock().unwrap();
    assert!(all.len() <= 20_000);
    let duplicate = &all.by_stream["owned"][&p.policy.identity(&request("duplicate"))];
    assert_eq!(duplicate.live.load(Ordering::Relaxed), 16);
    assert_eq!(
        grants
            .iter()
            .filter(|g| g.entry.as_ref().is_some_and(|e| Arc::ptr_eq(e, duplicate)))
            .count(),
        16
    );
    drop(all);
    drop(grants);
    assert_eq!(auth.live_grants(), 0);
}

// Reclamation must not inherit an unrelated allow or the old callback session.
#[tokio::test]
async fn capacity_pressure_recreated_failures_require_a_fresh_backend_session() {
    for status in [403, 302, 503] {
        let backend = Backend::new().await;
        let auth = PlaybackAuth::new(32);
        let p = policy(&auth, &backend);
        backend.status.store(status, Ordering::Relaxed);
        let initial = auth
            .authorize_control(p.clone(), request("recreated"))
            .await;
        assert!(if status == 302 {
            matches!(initial, AuthOutcome::Redirect(_))
        } else {
            matches!(initial, AuthOutcome::Denied)
        });
        let old_id = cached(&auth, &p, "recreated").id.clone();
        seed(&auth, &p, 19_999, Decision::Deny);
        backend.status.store(200, Ordering::Relaxed);
        assert!(matches!(
            auth.authorize_control(p.clone(), request("fresh")).await,
            AuthOutcome::Allowed(_)
        ));
        backend.status.store(403, Ordering::Relaxed);
        assert!(matches!(
            auth.authorize_control(p.clone(), request("recreated"))
                .await,
            AuthOutcome::Denied
        ));
        let new_id = cached(&auth, &p, "recreated").id.clone();
        assert_ne!(old_id, new_id, "failed cached identity was not recreated");
        {
            let queries = backend.requests.lock().unwrap();
            assert_eq!(queries.len(), 3);
            assert_eq!(queries[2]["token"], "recreated");
            assert_eq!(queries[2]["session_id"], new_id);
            assert_eq!(queries[2]["request_type"], "new_session");
            assert_eq!(queries[2]["request_number"], "0");
        }
        backend.close().await;
    }
}

// A suspended renewal must stay reachable by operator revoke during pressure.
#[tokio::test]
async fn capacity_pressure_during_background_callback_preserves_revocation_fence() {
    let backend = Backend::new().await;
    let auth = Arc::new(PlaybackAuth::new(32));
    let p = policy(&auth, &backend);
    seed(&auth, &p, 20_000, Decision::Deny);
    let entry = cached(&auth, &p, "seed-0");
    let id = entry.id.clone();
    {
        let mut state = entry.state.lock().unwrap();
        state.decision = Decision::Unknown;
        state.next_check = Instant::now();
        state.last_seen = Instant::now() - Duration::from_secs(60);
    }
    let renewal = tokio::spawn({
        let auth = auth.clone();
        async move { auth.refresh(&entry).await }
    });
    tokio::time::timeout(Duration::from_secs(2), backend.entered.notified())
        .await
        .unwrap();
    let fresh = auth.authorize_control(p.clone(), request("fresh")).await;
    let still_reachable = auth.revoke(&id);
    backend.release.add_permits(1);
    renewal.await.unwrap();
    assert!(matches!(fresh, AuthOutcome::Allowed(_)));
    assert!(
        still_reachable,
        "pressure detached an in-flight background callback from revoke"
    );
    assert!(
        matches!(
            auth.authorize_control(p, request("seed-0")).await,
            AuthOutcome::Denied
        ),
        "late approval restored an explicitly revoked identity"
    );
    assert_eq!(backend.requests.lock().unwrap().len(), 2);
    backend.close().await;
}

// Pending request callbacks and their duplicate waiters must share one owner.
#[tokio::test]
async fn capacity_pressure_during_pending_admission_keeps_duplicate_single_flight() {
    let backend = Backend::new().await;
    let auth = Arc::new(PlaybackAuth::new(32));
    let p = policy(&auth, &backend);
    seed(&auth, &p, 19_999, Decision::Deny);
    let spawn_admission = || {
        let auth = auth.clone();
        let p = p.clone();
        tokio::spawn(async move { auth.authorize_control(p, request("seed-0-pending")).await })
    };
    // Use the fixture's blocked token; it is not one of the primed identities.
    auth.entries
        .lock()
        .unwrap()
        .by_stream
        .get_mut("owned")
        .unwrap()
        .remove(&p.policy.identity(&request("seed-0")));
    let pending = tokio::spawn({
        let auth = auth.clone();
        let p = p.clone();
        async move { auth.authorize_control(p, request("seed-0")).await }
    });
    tokio::time::timeout(Duration::from_secs(2), backend.entered.notified())
        .await
        .unwrap();
    let second = tokio::spawn({
        let auth = auth.clone();
        let p = p.clone();
        async move { auth.authorize_control(p, request("seed-0")).await }
    });
    // Fill the last slot without touching the pending identity.
    let filler = spawn_admission().await.unwrap();
    let fresh = auth.authorize_control(p.clone(), request("fresh")).await;
    backend.release.add_permits(1);
    let first = pending.await.unwrap();
    let second = second.await.unwrap();
    let (AuthOutcome::Allowed(a), AuthOutcome::Allowed(b)) = (first, second) else {
        panic!("pending callbacks lost their shared approval")
    };
    assert!(matches!(filler, AuthOutcome::Allowed(_)));
    assert!(matches!(fresh, AuthOutcome::Allowed(_)));
    assert!(Arc::ptr_eq(
        a.entry.as_ref().unwrap(),
        b.entry.as_ref().unwrap()
    ));
    assert_eq!(
        backend
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|q| q["token"] == "seed-0")
            .count(),
        1
    );
    assert!(auth.entries.lock().unwrap().len() <= 20_000);
    drop((a, b));
    backend.close().await;
}
