use super::*;
use axum::extract::Query;
use std::collections::HashMap;
use tokio::sync::oneshot;

type Hold = Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>;
struct Auth {
    url: String,
    hold: Arc<Mutex<Hold>>,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Auth {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Auth {
    async fn start() -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/auth", tcp.local_addr().unwrap());
        let hold: Arc<Mutex<Hold>> = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let (h, c) = (hold.clone(), calls.clone());
        let router = Router::new().fallback(get(
            move |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| {
                let (h, c) = (h.clone(), c.clone());
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    assert!(!headers.contains_key("cluster-auth-key"));
                    assert!(!headers.contains_key("authorization"));
                    assert!(!headers.contains_key("x-flussonix-peer"));
                    assert_eq!(query.get("name").unwrap(), NAME);
                    let held = h.lock().unwrap().take();
                    if let Some((entered, released)) = held {
                        entered.send(()).unwrap();
                        released.await.unwrap();
                    }
                    let status = match query.get("token").map(String::as_str) {
                        Some("deny") => StatusCode::FORBIDDEN,
                        Some("redirect") => StatusCode::FOUND,
                        _ => StatusCode::OK,
                    };
                    Response::builder()
                        .status(status)
                        .header("X-UserId", "owned-account")
                        .header("Location", "https://viewer.invalid/next")
                        .body(Body::empty())
                        .unwrap()
                }
            },
        ));
        let task = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
        Self {
            url,
            hold,
            calls,
            task,
        }
    }
    fn pause(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (tx, rx) = oneshot::channel();
        let (release, released) = oneshot::channel();
        assert!(self.hold.lock().unwrap().replace((tx, released)).is_none());
        (rx, release)
    }
    fn configure(&self, f: &Fixture) {
        for origin in &f.origins {
            f.config
                .put(
                    "sources",
                    &origin.key,
                    json!({"on_play":{"url":self.url,"max_sessions":1}}),
                )
                .unwrap();
        }
    }
}

#[tokio::test]
async fn native_denial_redirect_and_session_limits_do_not_rotate_or_open_media() {
    let f = Fixture::new().await;
    let callback = Auth::start().await;
    callback.configure(&f);
    let s = f.selector();
    let a = f.handle(0);
    let auth = PlaybackAuth::new(16);

    for token in ["deny", "redirect", "first", "second"] {
        let mut lookup = s.begin(NAME).unwrap();
        let choice = lookup
            .observe(
                &a,
                Ok(f.owner
                    .inventory(&a, Duration::from_secs(30))
                    .await
                    .unwrap()),
            )
            .unwrap()
            .unwrap();
        let outcome = s.admit(choice, &auth, viewer(token)).await.unwrap();
        match token {
            "deny" => assert!(matches!(outcome, ReplicaAdmissionOutcome::Denied)),
            "redirect" => assert!(
                matches!(outcome,ReplicaAdmissionOutcome::Redirect(ref u) if u=="https://viewer.invalid/next")
            ),
            "first" => {
                let ReplicaAdmissionOutcome::Allowed(admitted) = outcome else {
                    panic!("denied")
                };
                s.publish_admitted(&admitted, |_, _, _| ()).unwrap();
                drop(admitted);
            }
            _ => assert!(matches!(outcome, ReplicaAdmissionOutcome::Denied)),
        }
    }
    assert_eq!(s.status(NAME).unwrap().switches, 0);
    assert!(!s.status(NAME).unwrap().blocked);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 4);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn selection_switch_during_callback_rejects_old_origin_even_while_source_lease_is_valid() {
    let f = Fixture::new().await;
    let callback = Auth::start().await;
    callback.configure(&f);
    let s = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let auth = Arc::new(PlaybackAuth::new(16));
    let inventory = f
        .owner
        .inventory(&a, Duration::from_secs(30))
        .await
        .unwrap();
    let witness = inventory.candidate(NAME).unwrap();
    let mut lookup = s.begin(NAME).unwrap();
    let choice = lookup.observe(&a, Ok(inventory)).unwrap().unwrap();
    let (entered, release) = callback.pause();
    let (sc, au) = (s.clone(), auth.clone());
    let pending = tokio::spawn(async move { sc.admit(choice, &au, viewer("held")).await });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    // A separate real management probe reports failure without replacing A's
    // coordinator lease. Thus only the selection-generation fence can reject it.
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let client = ModernInventoryClient::new(
        &f.origins[0].key.replacen("m4s", "http", 1),
        &f.origins[0].secret,
        None,
        Duration::from_secs(3),
    )
    .unwrap();
    let error = client
        .inventory(&ObservedServerId::parse("owned-selector").unwrap())
        .await
        .err()
        .unwrap();
    let mut newer = s.begin(NAME).unwrap();
    assert!(
        newer
            .observe(
                &a,
                Err(CoordinationFailure::Lease(LeaseFailure::Binding(
                    BindingFailure::Inventory(error)
                )))
            )
            .unwrap()
            .is_none()
    );
    let selected = newer
        .observe(&b, f.owner.inventory(&b, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    s.current(&selected).unwrap();
    f.owner.publish(&witness, |_, _| ()).unwrap();
    release.send(()).unwrap();
    assert!(pending.await.unwrap().is_err());
    assert!(auth.snapshots().is_empty());
    assert_eq!(s.status(NAME).unwrap().selected_source, f.origins[1].key);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn configured_local_stream_blocks_replica_admission() {
    let f = Fixture::new().await;
    f.config
        .put("streams", NAME, json!({"inputs":[{"url":"testsrc://"}]}))
        .unwrap();
    let s = f.selector();
    let a = f.handle(0);
    let auth = PlaybackAuth::new(16);
    let mut lookup = s.begin(NAME).unwrap();
    let choice = lookup
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    assert!(s.admit(choice, &auth, viewer("owned")).await.is_err());
    assert!(auth.snapshots().is_empty());
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn idle_private_pull_stops_on_lease_expiry_revocation_selection_stop_and_caller_cancel() {
    use flussonix::source_directory::modern_coordinator::PrivatePullOptions;
    for mode in 0..4 {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        let auth = Arc::new(PlaybackAuth::new(16));
        let cancel = tokio_util::sync::CancellationToken::new();
        let lifetime = if mode == 0 {
            Duration::from_millis(350)
        } else {
            Duration::from_secs(30)
        };
        let mut lookup = s.begin(NAME).unwrap();
        let choice = lookup
            .observe(&a, f.owner.inventory(&a, lifetime).await)
            .unwrap()
            .unwrap();
        let ReplicaAdmissionOutcome::Allowed(admitted) =
            s.admit(choice, &auth, viewer("owned")).await.unwrap()
        else {
            panic!("denied")
        };
        let (sc, c) = (s.clone(), cancel.clone());
        let (tx, rx) = oneshot::channel();
        let pull = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let result = sc
                .pull_admitted(
                    &admitted,
                    &mut bytes,
                    None,
                    tx,
                    PrivatePullOptions {
                        cancel: Some(&c),
                        ..Default::default()
                    },
                )
                .await;
            (result, bytes)
        });
        tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .unwrap()
            .unwrap();
        match mode {
            0 => {}
            1 => {
                auth.publish(NAME, None);
            }
            2 => s.shutdown(),
            _ => cancel.cancel(),
        }
        let (result, bytes) = tokio::time::timeout(Duration::from_secs(2), pull)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err());
        assert!(!bytes.is_empty());
        assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    }
}
