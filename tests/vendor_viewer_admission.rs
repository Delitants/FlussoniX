//! Catch private pulls before viewer permission and obsolete callback decisions.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, Query},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    config::ConfigStore,
    playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest},
    source_directory::modern_coordinator::{
        AdmissionFailure as Failure, AdmissionOutcome as Outcome, CoordinatedInventory,
        CoordinationFailure, ModernSourceCoordinator, SourceHandle,
    },
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::oneshot;
const NAME: &str = "Owned %2F channel";
struct Hold {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<StatusCode>,
}
struct Fixture {
    _dir: tempfile::TempDir,
    config: Arc<ConfigStore>,
    owner: Arc<ModernSourceCoordinator>,
    auth: Arc<PlaybackAuth>,
    handle: SourceHandle,
    key: String,
    callback: String,
    calls: Arc<Mutex<Vec<String>>>,
    hold: Arc<Mutex<Option<Hold>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn start(max: u64) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", listener.local_addr().unwrap());
        let callback = format!("{root}/auth");
        let key = root.replacen("http", "m4s", 1);
        let calls = Arc::new(Mutex::new(vec![]));
        let hold: Arc<Mutex<Option<Hold>>> = Arc::new(Mutex::new(None));
        let (paths, pending) = (calls.clone(), hold.clone());
        let router = Router::new().fallback(get(move |OriginalUri(uri):OriginalUri, headers:HeaderMap, Query(query):Query<HashMap<String,String>>| {
            let (paths,pending)=(paths.clone(),pending.clone());
            async move {
                paths.lock().unwrap().push(uri.path().to_owned());
                if uri.path()=="/auth" {
                    assert!(!headers.contains_key("cluster-auth-key"));
                    assert!(!headers.contains_key("authorization"));
                    assert_eq!(query.get("name").map(String::as_str),Some(NAME));
                    let held=pending.lock().unwrap().take();
                    let status=if let Some(held)=held {
                        let _=held.entered.send(());
                        held.release.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
                    } else {
                        match query.get("token").map(String::as_str) {
                            Some("deny")=>StatusCode::FORBIDDEN,
                            Some("redirect")=>StatusCode::FOUND,
                            _=>StatusCode::OK,
                        }
                    };
                    return Response::builder().status(status)
                        .header("X-UserId","owned-account")
                        .header("Location","https://viewer.invalid/elsewhere")
                        .body(Body::empty()).unwrap();
                }
                if uri.path().ends_with("/m4s") {
                    assert!(headers.contains_key("cluster-auth-key"));
                    assert!(!headers.contains_key("authorization"));
                    assert!(uri.query().is_none());
                    let track=flussonix::m4s::Track{id:1,codec:"mp3".into(),config:vec![]};
                    let frame=flussonix::m4f::Frame{track_id:1,dts:90000,pts_offset:0,key:true,body:include_bytes!("fixtures/codecs/mp3.bin").to_vec()};
                    let mut bytes=flussonix::wire::encode_info(std::slice::from_ref(&track)).unwrap();
                    bytes.extend(flussonix::wire::encode_frame(&track,&frame).unwrap());
                    return Response::new(Body::from(bytes));
                }
                assert!(headers.contains_key("authorization"));
                assert!(!headers.contains_key("cluster-auth-key"));
                let value=if uri.path().ends_with("/config") {
                    json!({"stats":{"id":"owned-origin"}})
                } else {
                    assert_eq!(uri.path(),"/streamer/api/v3/streams");
                    json!({"server_id":"owned-origin","streams":[{"name":NAME,"groups":[],"static":true,"named_by":"config"}]})
                };
                Response::new(Body::from(value.to_string()))
            }
        }));
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
                json!({"url":key,"cluster_key":"owned-private-key"}),
            )
            .unwrap();
        let owner = Arc::new(ModernSourceCoordinator::new(config.clone(), "owned-local").unwrap());
        let handle = owner
            .install(
                owner
                    .capture(&key)
                    .unwrap()
                    .with_policy_templates(
                        &json!({"template":"paid"}),
                        &json!({}),
                        &json!({"paid":{"on_play":{"url":callback,"max_sessions":max}}}),
                    )
                    .unwrap()
                    .prepare(Duration::from_secs(3))
                    .unwrap(),
            )
            .unwrap();
        Self {
            _dir: dir,
            config,
            owner,
            auth: Arc::new(PlaybackAuth::new(16)),
            handle,
            key,
            callback,
            calls,
            hold,
            server,
        }
    }
    async fn inventory(&self, lifetime: Duration) -> CoordinatedInventory {
        self.owner.inventory(&self.handle, lifetime).await.unwrap()
    }
    fn replacement(&self, policy: Value) -> SourceHandle {
        self.owner
            .install(
                self.owner
                    .capture(&self.key)
                    .unwrap()
                    .with_policy(&policy, &json!({}))
                    .unwrap()
                    .prepare(Duration::from_secs(3))
                    .unwrap(),
            )
            .unwrap()
    }
    fn held(&self) -> (oneshot::Receiver<()>, oneshot::Sender<StatusCode>) {
        let (tx, rx) = oneshot::channel();
        let (release, send) = oneshot::channel();
        assert!(
            self.hold
                .lock()
                .unwrap()
                .replace(Hold {
                    entered: tx,
                    release: send
                })
                .is_none()
        );
        (rx, release)
    }
    fn media_calls(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/m4s"))
            .count()
    }
    fn auth_calls(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.as_str() == "/auth")
            .count()
    }
}
fn viewer(token: &str) -> ViewerRequest {
    ViewerRequest {
        name: NAME.into(),
        proto: "hls".into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        ..Default::default()
    }
}

async fn expire(rows: &CoordinatedInventory) {
    tokio::time::sleep_until(
        tokio::time::Instant::from_std(rows.expires_at()) + Duration::from_millis(5),
    )
    .await;
}

#[tokio::test]
async fn allowed_viewer_precedes_actual_private_pull_and_uses_native_session() {
    let f = Fixture::start(1).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let (entered, release) = f.held();
    let (owner, auth) = (f.owner.clone(), f.auth.clone());
    let candidate = rows.candidate(NAME).unwrap();
    let task = tokio::spawn(async move { owner.admit(candidate, &auth, viewer("allowed")).await });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.media_calls(), 0);
    release.send(StatusCode::OK).unwrap();
    let Outcome::Allowed(admitted) = task.await.unwrap().unwrap() else {
        panic!("viewer denied")
    };
    assert!(!admitted.is_cancelled());
    assert_eq!(f.owner.admitted_current(&admitted), Ok(()));
    assert_eq!(f.auth.active(), 1);
    assert_eq!(f.media_calls(), 0);
    let input = f
        .owner
        .publish_admitted(&admitted, |view, input, grant| {
            assert_eq!(view.name, NAME);
            assert!(!grant.is_cancelled());
            input
        })
        .unwrap();
    let (tx, rx) = oneshot::channel();
    let mut bytes = vec![];
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        input.pull_ready(&mut bytes, None, tx),
    )
    .await
    .unwrap();
    assert_eq!(rx.await.unwrap()[0].codec, "mp3");
    assert!(!bytes.is_empty());
    assert_eq!(f.media_calls(), 1);
}

#[tokio::test]
async fn denial_redirect_and_wrong_name_do_not_open_private_media() {
    let f = Fixture::start(1).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("deny"))
            .await
            .unwrap(),
        Outcome::Denied
    ));
    let Outcome::Redirect(target) = f
        .owner
        .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("redirect"))
        .await
        .unwrap()
    else {
        panic!("missing redirect")
    };
    assert_eq!(target, "https://viewer.invalid/elsewhere");
    let mut wrong = viewer("allowed");
    wrong.name = "different".into();
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, wrong)
            .await,
        Err(Failure::Invalid)
    ));
    assert_eq!(f.auth_calls(), 2);
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.auth.active(), 0);
}

#[tokio::test]
async fn builtin_token_denial_happens_before_callback_or_private_media() {
    let f = Fixture::start(1).await;
    let handle=f.replacement(json!({"on_play":f.callback,"flussonix_token_sha256":"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"}));
    let rows = f
        .owner
        .inventory(&handle, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("wrong"))
            .await
            .unwrap(),
        Outcome::Denied
    ));
    assert_eq!(f.auth_calls(), 0);
    assert_eq!(f.media_calls(), 0);
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("secret"))
            .await
            .unwrap(),
        Outcome::Allowed(_)
    ));
    assert_eq!(f.auth_calls(), 1);
    assert_eq!(f.media_calls(), 0);
}

#[tokio::test]
async fn configured_local_stream_wins_without_replacing_its_viewer_authority() {
    let f = Fixture::start(1).await;
    f.config
        .put("streams", NAME, json!({"disabled":true}))
        .unwrap();
    let handle = f.replacement(json!({"on_play":f.callback}));
    let rows = f
        .owner
        .inventory(&handle, Duration::from_secs(30))
        .await
        .unwrap();
    let local = f
        .auth
        .publish(
            NAME,
            Some(
                Policy::from_config(
                    &json!({"flussonix_token_sha256":"a".repeat(64)}),
                    &json!({}),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("allowed"))
            .await,
        Err(Failure::LocalStream)
    ));
    assert!(matches!(
        f.auth.authorize(local, viewer("allowed")).await,
        AuthOutcome::Denied
    ));
    assert_eq!(f.auth_calls(), 0);
    assert_eq!(f.media_calls(), 0);
}

#[tokio::test]
async fn held_allow_deny_and_redirect_cannot_survive_any_candidate_fence_change() {
    for mutation in 0..5 {
        for status in [StatusCode::OK, StatusCode::FORBIDDEN, StatusCode::FOUND] {
            let f = Fixture::start(1).await;
            let rows = f
                .inventory(if mutation == 4 {
                    Duration::from_millis(500)
                } else {
                    Duration::from_secs(30)
                })
                .await;
            let candidate = rows.candidate(NAME).unwrap();
            let (entered, release) = f.held();
            let (owner, auth) = (f.owner.clone(), f.auth.clone());
            let task =
                tokio::spawn(async move { owner.admit(candidate, &auth, viewer("held")).await });
            tokio::time::timeout(Duration::from_secs(2), entered)
                .await
                .unwrap()
                .unwrap();
            match mutation {
                0 => {
                    f.config
                        .put("templates", "unrelated", json!({"static":false}))
                        .unwrap();
                }
                1 => {
                    f.owner.replace_aliases(&["new-alias"]).unwrap();
                }
                2 => {
                    f.replacement(json!({"on_play":f.callback}));
                }
                3 => {
                    f.inventory(Duration::from_secs(30)).await;
                }
                _ => {
                    expire(&rows).await;
                }
            }
            release.send(status).unwrap();
            assert!(
                matches!(task.await.unwrap(), Err(Failure::Coordination(_))),
                "mutation {mutation} accepted an obsolete {status}"
            );
            assert_eq!(f.auth.active(), 0);
            assert_eq!(f.media_calls(), 0);
        }
    }
}

#[tokio::test]
async fn native_session_limit_holds_until_control_grant_is_dropped() {
    let f = Fixture::start(1).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let Outcome::Allowed(first) = f
        .owner
        .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("one"))
        .await
        .unwrap()
    else {
        panic!("first denied")
    };
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("two"))
            .await
            .unwrap(),
        Outcome::Denied
    ));
    drop(first);
    assert_eq!(f.auth.active(), 0);
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("three"))
            .await
            .unwrap(),
        Outcome::Allowed(_)
    ));
    assert_eq!(f.media_calls(), 0);
}

#[tokio::test]
async fn revoked_admission_cannot_publish_and_does_not_revoke_another_viewer() {
    let f = Fixture::start(2).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let Outcome::Allowed(first) = f
        .owner
        .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("one"))
        .await
        .unwrap()
    else {
        panic!("first denied")
    };
    let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let Outcome::Allowed(other) = f
        .owner
        .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("two"))
        .await
        .unwrap()
    else {
        panic!("other denied")
    };
    assert!(f.auth.revoke(&id));
    assert!(first.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), first.cancelled())
        .await
        .unwrap();
    let entered = AtomicBool::new(false);
    assert_eq!(
        f.owner
            .publish_admitted(&first, |_, _, _| entered.store(true, Ordering::SeqCst)),
        Err(Failure::Revoked)
    );
    assert!(!entered.load(Ordering::SeqCst));
    assert!(!other.is_cancelled());
    assert_eq!(f.owner.admitted_current(&first), Err(Failure::Revoked));
    assert_eq!(f.owner.admitted_current(&other), Ok(()));
    assert_eq!(f.media_calls(), 0);
}

#[tokio::test]
async fn admitted_candidate_rechecks_every_fence_before_handoff() {
    for mutation in 0..5 {
        let f = Fixture::start(1).await;
        let rows = f
            .inventory(if mutation == 4 {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(30)
            })
            .await;
        let Outcome::Allowed(admitted) = f
            .owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("allowed"))
            .await
            .unwrap()
        else {
            panic!("denied")
        };
        let foreign = ModernSourceCoordinator::new(f.config.clone(), "owned-local").unwrap();
        assert!(matches!(
            foreign.publish_admitted(&admitted, |_, _, _| ()),
            Err(Failure::Coordination(CoordinationFailure::Stale))
        ));
        match mutation {
            0 => {
                f.config
                    .put("templates", "edit", json!({"static":false}))
                    .unwrap();
            }
            1 => {
                f.owner.replace_aliases(&["new-alias"]).unwrap();
            }
            2 => {
                f.replacement(json!({"on_play":f.callback}));
            }
            3 => {
                f.inventory(Duration::from_secs(30)).await;
            }
            _ => {
                expire(&rows).await;
            }
        }
        let entered = AtomicBool::new(false);
        assert!(matches!(
            f.owner
                .publish_admitted(&admitted, |_, _, _| entered.store(true, Ordering::SeqCst)),
            Err(Failure::Coordination(_))
        ));
        assert!(!entered.load(Ordering::SeqCst));
        assert!(f.owner.admitted_current(&admitted).is_err());
        assert_eq!(f.media_calls(), 0);
    }
}

#[tokio::test]
async fn expired_or_foreign_candidate_is_rejected_before_policy_or_callback() {
    let f = Fixture::start(1).await;
    let rows = f.inventory(Duration::from_millis(500)).await;
    let foreign = ModernSourceCoordinator::new(f.config.clone(), "owned-local").unwrap();
    assert!(matches!(
        foreign
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("foreign"))
            .await,
        Err(Failure::Coordination(CoordinationFailure::Stale))
    ));
    expire(&rows).await;
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, viewer("expired"))
            .await,
        Err(Failure::Coordination(_))
    ));
    assert_eq!(f.auth_calls(), 0);
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.auth.active(), 0);
}

#[tokio::test]
async fn cancelling_pending_authorization_releases_native_capacity_without_media() {
    let f = Fixture::start(1).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let candidate = rows.candidate(NAME).unwrap();
    let (entered, release) = f.held();
    let (owner, auth) = (f.owner.clone(), f.auth.clone());
    let task = tokio::spawn(async move { owner.admit(candidate, &auth, viewer("pending")).await });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(e) if e.is_cancelled()));
    let _ = release.send(StatusCode::OK);
    assert_eq!(f.auth.active(), 0);
    assert_eq!(f.media_calls(), 0);
}
