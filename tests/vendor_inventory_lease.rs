//! Breaks caught: indefinite inventory authority, refresh reordering, failure or
//! cancellation retaining old facts, and expiry/source edits bypassing publication.
use axum::{
    Router, body::Body, extract::OriginalUri, http::StatusCode, response::Response, routing::get,
};
use flussonix::source_directory::{
    LookupFailure,
    flussonic_config::{
        BindingFailure, LeaseFailure, LeasedIdentityCandidate, ProjectionFailure, SourceAuthority,
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use uuid::Uuid;

const NAME: &str = "Music %2F HD";
fn local() -> Uuid {
    Uuid::parse_str("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa").unwrap()
}
fn remote() -> Uuid {
    Uuid::parse_str("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb").unwrap()
}
struct Hold {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<StatusCode>,
}
struct Server {
    url: String,
    hold: Arc<Mutex<Option<Hold>>>,
    paths: Arc<Mutex<Vec<String>>>,
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
        let h = hold.clone();
        let p = paths.clone();
        let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
            let h = h.clone();
            let p = p.clone();
            async move {
                p.lock().unwrap().push(uri.path().to_owned());
                if uri.path().ends_with("/config") {
                    let held = h.lock().unwrap().take();
                    if let Some(held) = held {
                        let _ = held.entered.send(());
                        let status = held.release.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
                        if status != StatusCode::OK {
                            return Response::builder().status(status).body(Body::empty()).unwrap();
                        }
                    }
                    json_body(json!({"stats":{"id":remote().to_string()}}))
                } else if uri.path().ends_with("/-/streams") {
                    json_body(json!({"streams":[{"name":NAME,"named_by":"config","static":true,"groups":[]}]}))
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
            task,
        }
    }
    fn source(&self) -> Value {
        json!({"url":self.url.replacen("http","m4s",1),"cluster_key":"owned-lease-key","on_play":"true","only":[NAME]})
    }
    fn authority(&self) -> SourceAuthority {
        SourceAuthority::new(&self.source(), &json!([])).unwrap()
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
fn binding(failure: ProjectionFailure) -> LeaseFailure {
    LeaseFailure::Binding(BindingFailure::Projection(failure))
}
fn publish(a: &SourceAuthority, c: &LeasedIdentityCandidate) -> Result<(), LeaseFailure> {
    a.at_leased_current(c, |view, _| {
        assert_eq!(view.name, NAME);
        assert!(view.selected_static);
        assert!(view.policy.url.is_none());
    })
}
async fn expire(deadline: Instant) {
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
}

#[tokio::test]
async fn current_lease_preserves_policy_identity_and_actual_configured_media_request() {
    let s = Server::start().await;
    let a = s.authority();
    let rows = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(rows.server_id(), remote());
    assert_eq!(rows.names().collect::<Vec<_>>(), [NAME]);
    let c = rows.candidate(NAME).unwrap();
    assert_eq!(c.server_id(), remote());
    assert_eq!(c.expires_at(), rows.expires_at());
    let input = a
        .at_leased_current(&c, |view, input| {
            assert_eq!(view.name, NAME);
            input
        })
        .unwrap();
    let mut out = vec![];
    let (tx, rx) = oneshot::channel();
    let _ = tokio::time::timeout(Duration::from_secs(3), input.pull_ready(&mut out, None, tx))
        .await
        .unwrap();
    let tracks = rx.await.unwrap();
    assert_eq!(tracks[0].codec, "mp3");
    assert!(!out.is_empty());
    assert_eq!(
        *s.paths.lock().unwrap(),
        [
            "/streamer/api/v3/config",
            "/-/streams",
            "/streamer/api/v3/config",
            "/Music%20%252F%20HD/m4s",
        ]
    );
}

#[tokio::test]
async fn expired_lease_never_enters_publication_or_requests_media() {
    let s = Server::start().await;
    let a = s.authority();
    let rows = a
        .leased_identity_inventory(local(), Duration::from_millis(200))
        .await
        .unwrap();
    let c = rows.candidate(NAME).unwrap();
    publish(&a, &c).unwrap();
    expire(rows.expires_at()).await;
    let mut entered = false;
    assert_eq!(
        a.at_leased_current(&c, |_, _| entered = true),
        Err(LeaseFailure::Expired)
    );
    assert!(!entered);
    assert_eq!(s.paths.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn successful_refresh_supersedes_equal_old_facts() {
    let s = Server::start().await;
    let a = s.authority();
    let old = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap();
    let c = old.candidate(NAME).unwrap();
    let new = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(publish(&a, &c), Err(LeaseFailure::Superseded));
    publish(&a, &new.candidate(NAME).unwrap()).unwrap();
}

#[tokio::test]
async fn starting_a_valid_refresh_revokes_previous_publication_before_io_completes() {
    let s = Server::start().await;
    let a = Arc::new(s.authority());
    let old = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_identity_inventory(local(), Duration::from_secs(30))
            .await
    });
    entered.await.unwrap();
    assert_eq!(publish(&a, &old), Err(LeaseFailure::Superseded));
    release.send(StatusCode::OK).unwrap();
    publish(
        &a,
        &refresh.await.unwrap().unwrap().candidate(NAME).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn older_held_success_or_failure_cannot_overwrite_a_newer_refresh() {
    for status in [StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE] {
        let s = Server::start().await;
        let a = Arc::new(s.authority());
        let (entered, release) = s.hold_probe();
        let b = a.clone();
        let older = tokio::spawn(async move {
            b.leased_identity_inventory(local(), Duration::from_secs(30))
                .await
        });
        entered.await.unwrap();
        let new = a
            .leased_identity_inventory(local(), Duration::from_secs(30))
            .await
            .unwrap()
            .candidate(NAME)
            .unwrap();
        release.send(status).unwrap();
        assert!(matches!(
            older.await.unwrap(),
            Err(LeaseFailure::Superseded)
        ));
        publish(&a, &new).unwrap();
    }
}

#[tokio::test]
async fn failed_refresh_does_not_restore_the_previous_lease() {
    let s = Server::start().await;
    let a = Arc::new(s.authority());
    let old = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_identity_inventory(local(), Duration::from_secs(30))
            .await
    });
    entered.await.unwrap();
    release.send(StatusCode::SERVICE_UNAVAILABLE).unwrap();
    assert!(matches!(
        refresh.await.unwrap(),
        Err(LeaseFailure::Binding(BindingFailure::Inventory(
            LookupFailure::Unavailable
        )))
    ));
    assert_eq!(publish(&a, &old), Err(LeaseFailure::Superseded));
}

#[tokio::test]
async fn cancelling_refresh_keeps_old_lease_revoked_and_allows_a_fresh_attempt() {
    let s = Server::start().await;
    let a = Arc::new(s.authority());
    let old = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_identity_inventory(local(), Duration::from_secs(30))
            .await
    });
    entered.await.unwrap();
    refresh.abort();
    match refresh.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("cancelled refresh completed"),
    }
    assert_eq!(publish(&a, &old), Err(LeaseFailure::Superseded));
    let _ = release.send(StatusCode::OK);
    let new = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    publish(&a, &new).unwrap();
}

#[tokio::test]
async fn time_spent_fetching_counts_against_the_inventory_lease() {
    let s = Server::start().await;
    let a = Arc::new(s.authority());
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_identity_inventory(local(), Duration::from_millis(50))
            .await
    });
    entered.await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    release.send(StatusCode::OK).unwrap();
    assert!(matches!(refresh.await.unwrap(), Err(LeaseFailure::Expired)));
}

#[tokio::test]
async fn source_changes_fence_held_refresh_success_errors_and_old_candidates() {
    for remove in [false, true] {
        for status in [StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE] {
            let s = Server::start().await;
            let a = Arc::new(s.authority());
            let old = a
                .leased_identity_inventory(local(), Duration::from_secs(30))
                .await
                .unwrap()
                .candidate(NAME)
                .unwrap();
            let (entered, release) = s.hold_probe();
            let b = a.clone();
            let refresh = tokio::spawn(async move {
                b.leased_identity_inventory(local(), Duration::from_secs(30))
                    .await
            });
            entered.await.unwrap();
            if remove {
                a.remove().unwrap()
            } else {
                a.replace(&s.source(), &json!([])).unwrap()
            }
            release.send(status).unwrap();
            assert!(matches!(
                refresh.await.unwrap(),
                Err(LeaseFailure::Binding(BindingFailure::Projection(
                    ProjectionFailure::Stale
                )))
            ));
            assert_eq!(publish(&a, &old), Err(binding(ProjectionFailure::Stale)));
        }
    }
}

#[tokio::test]
async fn equal_settings_in_another_authority_cannot_publish_a_leased_candidate() {
    let s = Server::start().await;
    let a = s.authority();
    let b = s.authority();
    let c = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    assert_eq!(publish(&b, &c), Err(binding(ProjectionFailure::Stale)));
    publish(&a, &c).unwrap();
}

#[tokio::test]
async fn invalid_lifetime_or_nil_identity_does_no_io_and_preserves_the_current_lease() {
    let s = Server::start().await;
    let a = s.authority();
    let c = a
        .leased_identity_inventory(local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    for lifetime in [Duration::ZERO, Duration::from_secs(31), Duration::MAX] {
        assert!(matches!(
            a.leased_identity_inventory(local(), lifetime).await,
            Err(LeaseFailure::InvalidLifetime)
        ));
    }
    assert!(matches!(
        a.leased_identity_inventory(Uuid::nil(), Duration::from_secs(30))
            .await,
        Err(LeaseFailure::Binding(BindingFailure::Inventory(
            LookupFailure::Invalid
        )))
    ));
    publish(&a, &c).unwrap();
    assert_eq!(s.paths.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn unleased_apis_neither_supersede_nor_extend_a_lease() {
    let s = Server::start().await;
    let a = s.authority();
    let rows = a
        .leased_identity_inventory(local(), Duration::from_secs(1))
        .await
        .unwrap();
    let c = rows.candidate(NAME).unwrap();
    let identity = a
        .identity_inventory(local())
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let ordinary = a.inventory().await.unwrap().candidate(NAME).unwrap();
    publish(&a, &c).unwrap();
    expire(rows.expires_at()).await;
    assert_eq!(publish(&a, &c), Err(LeaseFailure::Expired));
    a.at_identity_current(&identity, |_, _| ()).unwrap();
    a.at_bound_current(&ordinary, |_, _| ()).unwrap();
    assert_eq!(s.paths.lock().unwrap().len(), 7);
}
