//! Regressions catch obsolete global config/identity/source generations reaching
//! publication, and direct self aliases walking inventory or opening media.
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    config::ConfigStore,
    source_directory::modern_coordinator::{
        CoordinatedCandidate, CoordinationFailure, ModernSourceCoordinator,
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
const NAME: &str = "Music %2F HD";
struct Hold {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<StatusCode>,
}
struct Server {
    url: String,
    hold: Arc<Mutex<Option<Hold>>>,
    paths: Arc<Mutex<Vec<String>>>,
    id: Arc<Mutex<String>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start() -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        let hold: Arc<Mutex<Option<Hold>>> = Arc::new(Mutex::new(None));
        let paths = Arc::new(Mutex::new(vec![]));
        let id = Arc::new(Mutex::new("remote-coordinator".to_owned()));
        let rows = Arc::new(Mutex::new(
            json!([{ "name":NAME,"named_by":"config","static":true,"groups":[] }]),
        ));
        let ids = id.clone();
        let rs = rows.clone();
        let h = hold.clone();
        let p = paths.clone();
        let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri, header: HeaderMap| {
            let h = h.clone();
            let p = p.clone();
            let ids = ids.clone();
            let rs = rs.clone();
            async move {
                p.lock().unwrap().push(uri.path().to_owned());
                if uri.path().ends_with("/m4s") {
                    assert!(header.contains_key("cluster-auth-key"));
                    assert!(!header.contains_key("authorization"));
                } else {
                    assert!(header.contains_key("authorization"));
                    assert!(!header.contains_key("cluster-auth-key"));
                }
                if uri.path().ends_with("/config") {
                    let held = h.lock().unwrap().take();
                    if let Some(held) = held {
                        let _ = held.entered.send(());
                        let status = held.release.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
                        if status != StatusCode::OK {
                            return Response::builder().status(status).body(Body::empty()).unwrap();
                        }
                    }
                    json_body(json!({"stats":{"id":ids.lock().unwrap().clone()}}))
                } else if uri.path().ends_with("/streamer/api/v3/streams") {
                    json_body(json!({"server_id":ids.lock().unwrap().clone(),"streams":rs.lock().unwrap().clone()}))
                } else if uri.path().ends_with("/m4s") {
                    let track = flussonix::m4s::Track { id:1, codec:"mp3".into(), config:vec![] };
                    let frame = flussonix::m4f::Frame {
                        track_id:1, dts:90000, pts_offset:0, key:true,
                        body:include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
                    };
                    let mut bytes = flussonix::wire::encode_info(std::slice::from_ref(&track)).unwrap();
                    bytes.extend(flussonix::wire::encode_frame(&track,&frame).unwrap());
                    Response::new(Body::from(bytes))
                } else {
                    Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap()
                }
            }
        }));
        let task = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
        Self {
            url,
            hold,
            paths,
            id,
            task,
        }
    }
    fn source(&self) -> Value {
        json!({"url":self.url.replacen("http","m4s",1),"cluster_key":"owned-coordinator-key","only":[NAME]})
    }
    fn hold_probe(&self) -> (oneshot::Receiver<()>, oneshot::Sender<StatusCode>) {
        let (entered, rx) = oneshot::channel();
        let (tx, release) = oneshot::channel();
        assert!(
            self.hold
                .lock()
                .unwrap()
                .replace(Hold { entered, release })
                .is_none()
        );
        (rx, tx)
    }
}
fn json_body(value: Value) -> Response {
    Response::new(Body::from(value.to_string()))
}
fn setup(
    s: &Server,
) -> (
    tempfile::TempDir,
    Arc<ConfigStore>,
    Arc<ModernSourceCoordinator>,
) {
    let d = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigStore::open(d.path().join("config.json")).unwrap());
    config
        .put("sources", s.source()["url"].as_str().unwrap(), s.source())
        .unwrap();
    let owner =
        Arc::new(ModernSourceCoordinator::new(config.clone(), "local-coordinator").unwrap());
    (d, config, owner)
}
fn handle(
    c: &ModernSourceCoordinator,
    s: &Server,
) -> flussonix::source_directory::modern_coordinator::SourceHandle {
    c.install(
        c.capture(s.source()["url"].as_str().unwrap())
            .unwrap()
            .with_policy(&json!({"on_play":"true"}), &json!({}))
            .unwrap()
            .prepare(Duration::from_secs(3))
            .unwrap(),
    )
    .unwrap()
}
fn publish(
    c: &ModernSourceCoordinator,
    candidate: &CoordinatedCandidate,
) -> Result<(), CoordinationFailure> {
    c.publish(candidate, |view, _| {
        assert_eq!(view.name, NAME);
        assert!(view.selected_static);
        assert!(view.policy.url.is_none());
    })
}
async fn entered(rx: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .expect("request not entered")
        .unwrap();
}

#[tokio::test]
async fn current_daemon_config_constructs_actual_media_and_exposes_start_based_lease() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let h = handle(&c, &s);
    let start = std::time::Instant::now();
    let rows = c.inventory(&h, Duration::from_secs(20)).await.unwrap();
    assert_eq!(rows.names().collect::<Vec<_>>(), [NAME]);
    assert!(rows.expires_at() >= start + Duration::from_secs(20));
    assert!(rows.expires_at() <= std::time::Instant::now() + Duration::from_secs(20));
    let candidate = rows.candidate(NAME).unwrap();
    assert_eq!(candidate.server_id().as_str(), "remote-coordinator");
    let input = c.publish(&candidate, |_, input| input).unwrap();
    let mut out = vec![];
    let (tx, rx) = oneshot::channel();
    let _ = tokio::time::timeout(Duration::from_secs(2), input.pull_ready(&mut out, None, tx))
        .await
        .unwrap();
    assert_eq!(rx.await.unwrap()[0].codec, "mp3");
    assert!(!out.is_empty());
    assert_eq!(s.paths.lock().unwrap().len(), 4);
}
#[tokio::test]
async fn unrelated_global_edit_fences_capture_handle_result_and_candidate() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let h = handle(&c, &s);
    let old = c
        .capture(s.source()["url"].as_str().unwrap())
        .unwrap()
        .prepare(Duration::from_secs(3))
        .unwrap();
    let rows = c.inventory(&h, Duration::from_secs(30)).await.unwrap();
    let candidate = rows.candidate(NAME).unwrap();
    config.put("templates", "new", json!({})).unwrap();
    assert!(matches!(c.install(old), Err(CoordinationFailure::Stale)));
    assert!(matches!(
        c.inventory(&h, Duration::from_secs(30)).await,
        Err(CoordinationFailure::Stale)
    ));
    assert_eq!(publish(&c, &candidate), Err(CoordinationFailure::Stale));
    assert_eq!(s.paths.lock().unwrap().len(), 3);
    publish(
        &c,
        &c.inventory(&handle(&c, &s), Duration::from_secs(30))
            .await
            .unwrap()
            .candidate(NAME)
            .unwrap(),
    )
    .unwrap();
}
#[tokio::test]
async fn late_preparation_cannot_replace_installed_source_after_config_edit() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let old = c.capture(s.source()["url"].as_str().unwrap()).unwrap();
    config
        .put(
            "sources",
            s.source()["url"].as_str().unwrap(),
            json!({"only":[]}),
        )
        .unwrap();
    let current = handle(&c, &s);
    assert!(matches!(
        c.install(old.prepare(Duration::from_secs(3)).unwrap()),
        Err(CoordinationFailure::Stale)
    ));
    let rows = c
        .inventory(&current, Duration::from_secs(30))
        .await
        .unwrap();
    c.publish(&rows.candidate(NAME).unwrap(), |v, _| {
        assert!(!v.selected_static)
    })
    .unwrap();
}
#[tokio::test]
async fn remove_readd_does_not_revive_old_handle() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let h = handle(&c, &s);
    let key = s.source()["url"].as_str().unwrap().to_owned();
    config.delete("sources", &key).unwrap();
    assert!(matches!(
        c.capture(&key),
        Err(CoordinationFailure::Unavailable)
    ));
    config.put("sources", &key, s.source()).unwrap();
    assert!(matches!(
        c.inventory(&h, Duration::from_secs(30)).await,
        Err(CoordinationFailure::Stale)
    ));
}
#[tokio::test]
async fn same_revision_install_replaces_handle_without_reviving_it() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let h = handle(&c, &s);
    let rows = c.inventory(&h, Duration::from_secs(30)).await.unwrap();
    let candidate = rows.candidate(NAME).unwrap();
    let newest = handle(&c, &s);
    assert_eq!(publish(&c, &candidate), Err(CoordinationFailure::Stale));
    assert!(matches!(
        c.inventory(&h, Duration::from_secs(30)).await,
        Err(CoordinationFailure::Stale)
    ));
    publish(
        &c,
        &c.inventory(&newest, Duration::from_secs(30))
            .await
            .unwrap()
            .candidate(NAME)
            .unwrap(),
    )
    .unwrap();
}
#[tokio::test]
async fn cross_coordinator_preparation_handle_and_candidate_are_rejected() {
    let s = Server::start().await;
    let (_d, config, a) = setup(&s);
    let b = ModernSourceCoordinator::new(config, "local-coordinator").unwrap();
    let h = handle(&a, &s);
    assert!(matches!(
        b.install(
            a.capture(s.source()["url"].as_str().unwrap())
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap()
        ),
        Err(CoordinationFailure::Stale)
    ));
    assert!(matches!(
        b.inventory(&h, Duration::from_secs(30)).await,
        Err(CoordinationFailure::Stale)
    ));
    let candidate = a
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    assert_eq!(publish(&b, &candidate), Err(CoordinationFailure::Stale));
}
#[tokio::test]
async fn alias_edit_fences_preparation_handles_and_candidates_even_when_equal() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let h = handle(&c, &s);
    let p = c.capture(s.source()["url"].as_str().unwrap()).unwrap();
    let candidate = c
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    c.replace_aliases(&[]).unwrap();
    assert!(matches!(
        c.install(p.prepare(Duration::from_secs(3)).unwrap()),
        Err(CoordinationFailure::Stale)
    ));
    assert_eq!(publish(&c, &candidate), Err(CoordinationFailure::Stale));
    assert!(matches!(
        c.inventory(&h, Duration::from_secs(30)).await,
        Err(CoordinationFailure::Stale)
    ));
}
#[tokio::test]
async fn explicit_self_alias_is_rejected_before_pages() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    c.replace_aliases(&["remote-coordinator"]).unwrap();
    assert!(matches!(
        c.inventory(&handle(&c, &s), Duration::from_secs(30)).await,
        Err(CoordinationFailure::Lease(_))
    ));
    assert_eq!(
        s.paths.lock().unwrap().as_slice(),
        ["/streamer/api/v3/config"]
    );
}
#[tokio::test]
async fn uuid_self_aliases_are_semantic_and_custom_ids_remain_exact() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    *s.id.lock().unwrap() = "urn:uuid:01234567-89AB-4CDE-8F01-23456789ABCD\r\n".into();
    c.replace_aliases(&["01234567-89ab-4cde-8f01-23456789abcd"])
        .unwrap();
    assert!(
        c.inventory(&handle(&c, &s), Duration::from_secs(30))
            .await
            .is_err()
    );
    assert_eq!(s.paths.lock().unwrap().len(), 1);
    *s.id.lock().unwrap() = "custom\n".into();
    c.replace_aliases(&["custom"]).unwrap();
    assert!(
        c.inventory(&handle(&c, &s), Duration::from_secs(30))
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn invalid_alias_edits_are_atomic_and_do_not_supersede_lease() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let candidate = c
        .inventory(&handle(&c, &s), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    for aliases in [
        vec![""],
        vec!["local-coordinator"],
        vec!["same", "same"],
        vec!["00000000-0000-0000-0000-000000000000"],
        vec!["id"; 17],
        vec![
            "01234567-89ab-4cde-8f01-23456789abcd",
            "urn:uuid:01234567-89AB-4CDE-8F01-23456789ABCD",
        ],
    ] {
        assert_eq!(
            c.replace_aliases(&aliases),
            Err(CoordinationFailure::Invalid)
        );
        publish(&c, &candidate).unwrap();
    }
}
#[tokio::test]
async fn held_success_and_error_are_fenced_by_global_edit() {
    for status in [StatusCode::OK, StatusCode::FORBIDDEN] {
        let s = Server::start().await;
        let (_d, config, c) = setup(&s);
        let h = handle(&c, &s);
        let (rx, tx) = s.hold_probe();
        let cc = c.clone();
        let task = tokio::spawn(async move { cc.inventory(&h, Duration::from_secs(30)).await });
        entered(rx).await;
        config.put("templates", "edit", json!({})).unwrap();
        tx.send(status).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            Err(CoordinationFailure::Stale)
        ));
    }
}
#[tokio::test]
async fn held_success_and_error_are_fenced_by_alias_edit_and_new_install() {
    for alias_edit in [true, false] {
        for status in [StatusCode::OK, StatusCode::FORBIDDEN] {
            let s = Server::start().await;
            let (_d, _config, c) = setup(&s);
            let h = handle(&c, &s);
            let (rx, tx) = s.hold_probe();
            let cc = c.clone();
            let task = tokio::spawn(async move { cc.inventory(&h, Duration::from_secs(30)).await });
            entered(rx).await;
            if alias_edit {
                c.replace_aliases(&["other-alias"]).unwrap();
            } else {
                let _ = handle(&c, &s);
            }
            tx.send(status).unwrap();
            assert!(matches!(
                task.await.unwrap(),
                Err(CoordinationFailure::Stale)
            ));
        }
    }
}
#[tokio::test]
async fn lease_expiry_and_failed_refresh_are_preserved_through_coordinator() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let h = handle(&c, &s);
    let rows = c.inventory(&h, Duration::from_millis(100)).await.unwrap();
    let old = rows.candidate(NAME).unwrap();
    tokio::time::sleep_until(tokio::time::Instant::from_std(rows.expires_at())).await;
    assert!(matches!(
        publish(&c, &old),
        Err(CoordinationFailure::Lease(_))
    ));
    let old = c
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (rx, tx) = s.hold_probe();
    let cc = c.clone();
    let task = tokio::spawn(async move { cc.inventory(&h, Duration::from_secs(30)).await });
    entered(rx).await;
    assert!(matches!(
        publish(&c, &old),
        Err(CoordinationFailure::Lease(_))
    ));
    tx.send(StatusCode::FORBIDDEN).unwrap();
    assert!(task.await.unwrap().is_err());
    assert!(publish(&c, &old).is_err());
}
#[tokio::test]
async fn unsupported_normalized_native_settings_fail_without_io_or_old_handle_revival() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let h = handle(&c, &s);
    config
        .put(
            "sources",
            s.source()["url"].as_str().unwrap(),
            json!({"drain":true}),
        )
        .unwrap();
    assert!(matches!(
        c.capture(s.source()["url"].as_str().unwrap())
            .unwrap()
            .prepare(Duration::from_secs(3)),
        Err(CoordinationFailure::Unsupported)
    ));
    assert!(matches!(
        c.inventory(&h, Duration::from_secs(30)).await,
        Err(CoordinationFailure::Stale)
    ));
    assert!(s.paths.lock().unwrap().is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_config_and_alias_mutations_wait_for_synchronous_publication() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let candidate = c
        .inventory(&handle(&c, &s), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entry, entered) = std::sync::mpsc::channel();
    let (release, rx) = std::sync::mpsc::channel();
    let cc = c.clone();
    let published = std::thread::spawn(move || {
        cc.publish(&candidate, |_, _| {
            entry.send(()).unwrap();
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        })
    });
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let (started, ready) = std::sync::mpsc::channel();
    let (finished, done) = std::sync::mpsc::channel();
    let cfg = config.clone();
    let changer = std::thread::spawn(move || {
        started.send(()).unwrap();
        cfg.put("templates", "edit", json!({})).unwrap();
        finished.send(()).unwrap();
    });
    let (alias_started, alias_ready) = std::sync::mpsc::channel();
    let (alias_finished, alias_done) = std::sync::mpsc::channel();
    let c2 = c.clone();
    let aliases = std::thread::spawn(move || {
        alias_started.send(()).unwrap();
        c2.replace_aliases(&[]).unwrap();
        alias_finished.send(()).unwrap();
    });
    ready.recv_timeout(Duration::from_secs(2)).unwrap();
    alias_ready.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        done.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(matches!(
        alias_done.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    published.join().unwrap().unwrap();
    changer.join().unwrap();
    aliases.join().unwrap();
    done.recv_timeout(Duration::from_secs(2)).unwrap();
    alias_done.recv_timeout(Duration::from_secs(2)).unwrap();
}
#[tokio::test]
async fn invalid_primary_identity_is_unavailable_and_startup_name_is_captured() {
    let d = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigStore::open(d.path().join("config.json")).unwrap());
    assert!(matches!(
        ModernSourceCoordinator::new(config, ""),
        Err(CoordinationFailure::Invalid)
    ));
    let mut options = flussonix::server::Options {
        admin_password: "owned-password".into(),
        peer_key: "owned-peer-key".into(),
        node_name: "startup-name".into(),
        ..Default::default()
    };
    let app = flussonix::server::App::new(
        d.path().join("app.json"),
        d.path().join("media"),
        options.clone(),
    )
    .unwrap();
    assert!(app.modern_source_coordinator().is_ok());
    options.node_name = "".into();
    let invalid = flussonix::server::App::new(
        d.path().join("invalid.json"),
        d.path().join("media2"),
        options,
    )
    .unwrap();
    assert!(matches!(
        invalid.modern_source_coordinator(),
        Err(CoordinationFailure::Invalid)
    ));
}

#[tokio::test]
async fn older_preparation_at_same_global_revision_cannot_win_late() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let key = s.source()["url"].as_str().unwrap().to_owned();
    let old = c.capture(&key).unwrap();
    let newest = handle(&c, &s);
    assert!(matches!(
        c.install(old.prepare(Duration::from_secs(3)).unwrap()),
        Err(CoordinationFailure::Stale)
    ));
    publish(
        &c,
        &c.inventory(&newest, Duration::from_secs(30))
            .await
            .unwrap()
            .candidate(NAME)
            .unwrap(),
    )
    .unwrap();
}
#[tokio::test]
async fn invalid_config_edit_and_failed_preparation_preserve_current_handle() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let h = handle(&c, &s);
    let candidate = c
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    assert!(
        config
            .put(
                "sources",
                s.source()["url"].as_str().unwrap(),
                json!({"cluster_key":"short"})
            )
            .is_err()
    );
    assert!(matches!(
        c.capture(s.source()["url"].as_str().unwrap())
            .unwrap()
            .prepare(Duration::ZERO),
        Err(CoordinationFailure::Invalid)
    ));
    publish(&c, &candidate).unwrap();
}
#[tokio::test]
async fn cancelled_refresh_never_restores_previous_coordinated_lease() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let h = handle(&c, &s);
    let old = c
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (rx, tx) = s.hold_probe();
    let cc = c.clone();
    let task = tokio::spawn(async move { cc.inventory(&h, Duration::from_secs(30)).await });
    entered(rx).await;
    task.abort();
    assert!(matches!(task.await,Err(e) if e.is_cancelled()));
    let _ = tx.send(StatusCode::OK);
    assert!(matches!(
        publish(&c, &old),
        Err(CoordinationFailure::Lease(_))
    ));
}

#[tokio::test]
async fn policy_is_explicit_and_prepared_policy_change_supersedes_old_candidate() {
    let s = Server::start().await;
    let (_d, _config, c) = setup(&s);
    let key = s.source()["url"].as_str().unwrap().to_owned();
    let implicit = c
        .install(
            c.capture(&key)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap();
    let unresolved = c
        .inventory(&implicit, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(matches!(
        unresolved.candidate(NAME),
        Err(CoordinationFailure::Lease(_))
    ));
    let h = handle(&c, &s);
    let old = c
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let callback = "https://example.invalid/auth";
    let prepared = c
        .capture(&key)
        .unwrap()
        .with_policy(&json!({"on_play":callback}), &json!({}))
        .unwrap()
        .prepare(Duration::from_secs(3))
        .unwrap();
    let h = c.install(prepared).unwrap();
    assert_eq!(publish(&c, &old), Err(CoordinationFailure::Stale));
    let rows = c.inventory(&h, Duration::from_secs(30)).await.unwrap();
    c.publish(&rows.candidate(NAME).unwrap(), |v, _| {
        assert_eq!(v.policy.url.as_deref(), Some(callback))
    })
    .unwrap();
    assert!(matches!(
        c.capture(&key)
            .unwrap()
            .with_policy(&json!({"inputs":[]}), &json!({})),
        Err(CoordinationFailure::Unsupported)
    ));
}

#[tokio::test]
async fn daemon_coordinator_shares_the_management_config_and_actual_startup_identity() {
    let s = Server::start().await;
    let d = tempfile::tempdir().unwrap();
    let options = flussonix::server::Options {
        admin_password: "owned-password".into(),
        peer_key: "owned-peer-key".into(),
        node_name: "actual-daemon-name".into(),
        ..Default::default()
    };
    let mut app =
        flussonix::server::App::new(d.path().join("app.json"), d.path().join("media"), options)
            .unwrap();
    app.config
        .put("sources", s.source()["url"].as_str().unwrap(), s.source())
        .unwrap();
    *s.id.lock().unwrap() = "actual-daemon-name".into();
    // Options are startup inputs. Mutating the retained public copy must not change
    // the owner's actual identity or the name advertised by node telemetry.
    Arc::get_mut(&mut app).unwrap().options.node_name = "later-copy".into();
    let c = app.modern_source_coordinator().unwrap();
    let h = handle(c, &s);
    assert!(c.inventory(&h, Duration::from_secs(30)).await.is_err());
    assert_eq!(s.paths.lock().unwrap().len(), 1);
    *s.id.lock().unwrap() = "remote-coordinator".into();
    let candidate = c
        .inventory(&h, Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    app.config.put("templates", "edit", json!({})).unwrap();
    assert_eq!(publish(c, &candidate), Err(CoordinationFailure::Stale));
    use tower::ServiceExt;
    let request = axum::http::Request::builder()
        .uri("/flussonix/api/v1/node")
        .header("X-Flussonix-Peer", "owned-peer-key")
        .body(Body::empty())
        .unwrap();
    let response = flussonix::server::router(app)
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    use http_body_util::BodyExt;
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["name"], "actual-daemon-name");
}
#[tokio::test]
async fn normalized_transport_must_match_source_and_source_capture_count_is_bounded() {
    let s = Server::start().await;
    let (_d, config, c) = setup(&s);
    let key = s.source()["url"].as_str().unwrap().to_owned();
    config
        .put("sources", &key, json!({"flussonix_transport":"m4f"}))
        .unwrap();
    assert!(matches!(
        c.capture(&key).unwrap().prepare(Duration::from_secs(3)),
        Err(CoordinationFailure::Unsupported)
    ));
    let rows:Vec<Value>=(0..65).map(|n|json!({"url":format!("m4s://owned-{n}.invalid"),"only":[],"cluster_key":"owned-peer-key"})).collect();
    config.replace(json!({"sources":rows})).unwrap();
    for n in 0..64 {
        c.capture(&format!("m4s://owned-{n}.invalid")).unwrap();
    }
    assert!(matches!(
        c.capture("m4s://owned-64.invalid"),
        Err(CoordinationFailure::Invalid)
    ));
    config.put("templates", "epoch", json!({})).unwrap();
    assert!(c.capture("m4s://owned-64.invalid").is_ok());
    assert!(s.paths.lock().unwrap().is_empty());
}
