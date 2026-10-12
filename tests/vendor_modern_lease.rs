//! Breaks caught: modern refreshes retaining indefinite authority, reordering or
//! restoring failed/cancelled leases, and bypassing expiry/profile/media fences.
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
use flussonix::source_directory::{
    LookupFailure,
    flussonic::modern::ObservedServerId,
    flussonic_config::{
        BindingFailure, LeaseFailure, ModernLeasedCandidate, ProjectionFailure, SourceAuthority,
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const NAME: &str = "Music %2F HD";
fn local() -> ObservedServerId {
    ObservedServerId::parse("owned-modern-local").unwrap()
}
fn remote() -> ObservedServerId {
    ObservedServerId::parse("owned-modern-remote\n").unwrap()
}
struct Hold {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<StatusCode>,
}
struct Server {
    url: String,
    hold: Arc<Mutex<Option<Hold>>>,
    paths: Arc<Mutex<Vec<String>>>,
    headers: Arc<Mutex<Vec<HeaderMap>>>,
    rows: Arc<Mutex<Value>>,
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
        let headers = Arc::new(Mutex::new(vec![]));
        let rows = Arc::new(Mutex::new(
            json!([{ "name":NAME,"named_by":"config","static":true,"groups":[] }]),
        ));
        let hs = headers.clone();
        let rs = rows.clone();
        let h = hold.clone();
        let p = paths.clone();
        let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri, header: HeaderMap| {
            let h = h.clone();
            let p = p.clone();
            let hs = hs.clone();
            let rs = rs.clone();
            async move {
                p.lock().unwrap().push(uri.path().to_owned());
                hs.lock().unwrap().push(header);
                if uri.path().ends_with("/config") {
                    let held = h.lock().unwrap().take();
                    if let Some(held) = held {
                        let _ = held.entered.send(());
                        let status = held.release.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
                        if status != StatusCode::OK {
                            return Response::builder().status(status).body(Body::empty()).unwrap();
                        }
                    }
                    json_body(json!({"stats":{"id":remote().as_str()}}))
                } else if uri.path().ends_with("/streamer/api/v3/streams") {
                    json_body(json!({"server_id":remote().as_str(),"streams":rs.lock().unwrap().clone()}))
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
        let task = tokio::spawn(async move {
            axum::serve(tcp, vendor_fixture::acknowledge_media(router))
                .await
                .unwrap()
        });
        Self {
            url,
            hold,
            paths,
            headers,
            rows,
            task,
        }
    }
    fn source(&self) -> Value {
        json!({"url":self.url.replacen("http","m4s",1),"cluster_key":"owned-lease-key","on_play":"true","only":[NAME]})
    }
    fn authority(&self) -> SourceAuthority {
        SourceAuthority::new_modern(&self.source(), &json!([]), Duration::from_secs(3)).unwrap()
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
fn publish(a: &SourceAuthority, c: &ModernLeasedCandidate) -> Result<(), LeaseFailure> {
    a.at_modern_leased_current(c, |view, _| {
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
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap();
    assert!(rows.server_id() == &remote());
    assert_eq!(rows.names().collect::<Vec<_>>(), [NAME]);
    let c = rows.candidate(NAME).unwrap();
    assert!(c.server_id() == &remote());
    assert_eq!(c.expires_at(), rows.expires_at());
    let input = a
        .at_modern_leased_current(&c, |view, input| {
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
    for (i, headers) in s.headers.lock().unwrap().iter().enumerate() {
        let (time, hash) = if i < 3 {
            assert!(!headers.contains_key("cluster-auth-key"));
            headers["authorization"]
                .to_str()
                .unwrap()
                .strip_prefix("Cluster ")
                .unwrap()
                .split_once(':')
                .unwrap()
        } else {
            assert!(!headers.contains_key("authorization"));
            (
                headers["cluster-auth-time"].to_str().unwrap(),
                headers["cluster-auth-key"].to_str().unwrap(),
            )
        };
        assert!(time.parse::<u64>().is_ok());
        let plaintext = if i < 3 {
            format!("{time}:owned-lease-key")
        } else {
            format!("{time}:owned-lease-key:{NAME}")
        };
        use std::io::Write;
        let mut child = std::process::Command::new("openssl")
            .args(["dgst", "-sha1"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(plaintext.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        assert_eq!(
            hash,
            String::from_utf8(output.stdout)
                .unwrap()
                .split_whitespace()
                .last()
                .unwrap()
        );
    }
    assert_eq!(
        *s.paths.lock().unwrap(),
        [
            "/streamer/api/v3/config",
            "/streamer/api/v3/streams",
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
        .leased_modern_inventory(&local(), Duration::from_millis(200))
        .await
        .unwrap();
    let c = rows.candidate(NAME).unwrap();
    publish(&a, &c).unwrap();
    expire(rows.expires_at()).await;
    let mut entered = false;
    assert_eq!(
        a.at_modern_leased_current(&c, |_, _| entered = true),
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
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap();
    let c = old.candidate(NAME).unwrap();
    let new = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
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
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_modern_inventory(&local(), Duration::from_secs(30))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
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
            b.leased_modern_inventory(&local(), Duration::from_secs(30))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered)
            .await
            .unwrap()
            .unwrap();
        let new = a
            .leased_modern_inventory(&local(), Duration::from_secs(30))
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
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_modern_inventory(&local(), Duration::from_secs(30))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
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
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let refresh = tokio::spawn(async move {
        b.leased_modern_inventory(&local(), Duration::from_secs(30))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    refresh.abort();
    match refresh.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("cancelled refresh completed"),
    }
    assert_eq!(publish(&a, &old), Err(LeaseFailure::Superseded));
    let _ = release.send(StatusCode::OK);
    let new = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    publish(&a, &new).unwrap();
}

#[tokio::test]
async fn time_spent_fetching_counts_against_the_inventory_lease() {
    for status in [StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE] {
        let s = Server::start().await;
        let a = Arc::new(s.authority());
        let (entered, release) = s.hold_probe();
        let b = a.clone();
        let refresh = tokio::spawn(async move {
            b.leased_modern_inventory(&local(), Duration::from_millis(50))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        release.send(status).unwrap();
        assert!(matches!(refresh.await.unwrap(), Err(LeaseFailure::Expired)));
    }
}

#[tokio::test]
async fn leased_publication_serializes_source_replacement_until_the_closure_finishes() {
    let s = Server::start().await;
    let a = Arc::new(s.authority());
    let c = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let publisher = a.clone();
    let publication = std::thread::spawn(move || {
        publisher.at_modern_leased_current(&c, |view, _| {
            assert_eq!(view.name, NAME);
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        })
    });
    entered_rx.await.unwrap();
    let (started, started_rx) = oneshot::channel();
    let (done, mut done_rx) = oneshot::channel();
    let editor = a.clone();
    let source = s.source();
    let replacement = std::thread::spawn(move || {
        started.send(()).unwrap();
        let result = editor.replace(&source, &json!([]));
        let _ = done.send(result);
        result
    });
    started_rx.await.unwrap();
    let blocked = tokio::time::timeout(Duration::from_millis(500), &mut done_rx)
        .await
        .is_err();
    // Release and join both owned threads before asserting the observed ordering.
    release.send(()).unwrap();
    publication.join().unwrap().unwrap();
    replacement.join().unwrap().unwrap();
    assert!(
        blocked,
        "source replacement entered during leased publication"
    );
}

#[tokio::test]
async fn source_changes_fence_held_refresh_success_errors_and_old_candidates() {
    for remove in [false, true] {
        for status in [StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE] {
            let s = Server::start().await;
            let a = Arc::new(s.authority());
            let old = a
                .leased_modern_inventory(&local(), Duration::from_secs(30))
                .await
                .unwrap()
                .candidate(NAME)
                .unwrap();
            let (entered, release) = s.hold_probe();
            let b = a.clone();
            let refresh = tokio::spawn(async move {
                b.leased_modern_inventory(&local(), Duration::from_secs(30))
                    .await
            });
            tokio::time::timeout(Duration::from_secs(2), entered)
                .await
                .unwrap()
                .unwrap();
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
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    assert_eq!(publish(&b, &c), Err(binding(ProjectionFailure::Stale)));
    publish(&a, &c).unwrap();
}

#[tokio::test]
async fn invalid_lifetime_and_wrong_mode_do_no_io_and_preserve_the_current_lease() {
    let s = Server::start().await;
    let a = s.authority();
    let c = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    for lifetime in [Duration::ZERO, Duration::from_secs(31), Duration::MAX] {
        assert!(matches!(
            a.leased_modern_inventory(&local(), lifetime).await,
            Err(LeaseFailure::InvalidLifetime)
        ));
    }
    assert!(matches!(
        a.leased_identity_inventory(uuid::Uuid::new_v4(), Duration::from_secs(30))
            .await,
        Err(LeaseFailure::Binding(BindingFailure::Projection(
            ProjectionFailure::Unsupported
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
        .leased_modern_inventory(&local(), Duration::from_secs(1))
        .await
        .unwrap();
    let c = rows.candidate(NAME).unwrap();
    let unleased = a
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate(NAME)
        .unwrap();
    publish(&a, &c).unwrap();
    expire(rows.expires_at()).await;
    assert_eq!(publish(&a, &c), Err(LeaseFailure::Expired));
    a.at_modern_current(&unleased, |_, _| ()).unwrap();
    assert_eq!(s.paths.lock().unwrap().len(), 6);
}

// Break caught: treating a long modern wire budget as a lease starting at completion.
#[tokio::test]
async fn long_modern_refresh_uses_the_independent_wire_budget_and_start_based_lease() {
    let s = Server::start().await;
    let a = Arc::new(s.authority());
    let (entered, release) = s.hold_probe();
    let b = a.clone();
    let started = Instant::now();
    let refresh = tokio::spawn(async move {
        b.leased_modern_inventory(&local(), Duration::from_secs(2))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    release.send(StatusCode::OK).unwrap();
    let rows = refresh.await.unwrap().unwrap();
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert!(rows.expires_at() <= started + Duration::from_millis(2100));
    publish(&a, &rows.candidate(NAME).unwrap()).unwrap();
    expire(rows.expires_at()).await;
    assert_eq!(
        publish(&a, &rows.candidate(NAME).unwrap()),
        Err(LeaseFailure::Expired)
    );
    assert_eq!(s.paths.lock().unwrap().len(), 3);
}

// Break caught: reserving a lease before the async method is polled or in legacy mode.
#[tokio::test]
async fn unpolled_refresh_and_legacy_mode_rejection_never_reserve_or_send_requests() {
    let s = Server::start().await;
    let a = s.authority();
    let rows = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap();
    let id = local();
    drop(a.leased_modern_inventory(&id, Duration::from_secs(30)));
    publish(&a, &rows.candidate(NAME).unwrap()).unwrap();
    let legacy = SourceAuthority::new(&s.source(), &json!([])).unwrap();
    assert!(matches!(
        legacy
            .leased_modern_inventory(&local(), Duration::from_secs(30))
            .await,
        Err(LeaseFailure::Binding(BindingFailure::Projection(
            ProjectionFailure::Unsupported
        )))
    ));
    assert_eq!(s.paths.lock().unwrap().len(), 3);
}

// Break caught: failed identity checks retaining a previous lease, or projection bypass.
#[tokio::test]
async fn direct_self_refresh_revokes_old_lease_and_unknown_or_relayed_rows_stay_ineligible() {
    let s = Server::start().await;
    let a = s.authority();
    let old = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap();
    assert!(matches!(
        a.leased_modern_inventory(&remote(), Duration::from_secs(30))
            .await,
        Err(LeaseFailure::Binding(BindingFailure::Inventory(
            LookupFailure::Invalid
        )))
    ));
    assert_eq!(
        publish(&a, &old.candidate(NAME).unwrap()),
        Err(LeaseFailure::Superseded)
    );
    for (row, reason) in [
        (
            json!({"name":NAME,"named_by":"config"}),
            flussonix::source_directory::flussonic::EligibilityFailure::UnknownGroups,
        ),
        (
            json!({"name":NAME,"named_by":"remote","groups":[]}),
            flussonix::source_directory::flussonic::EligibilityFailure::Relayed,
        ),
        (
            json!({"name":NAME,"named_by":"config","groups":[],"disabled":true}),
            flussonix::source_directory::flussonic::EligibilityFailure::Disabled,
        ),
    ] {
        *s.rows.lock().unwrap() = json!([row]);
        let rows = a
            .leased_modern_inventory(&local(), Duration::from_secs(30))
            .await
            .unwrap();
        assert!(
            matches!(rows.candidate(NAME),Err(LeaseFailure::Binding(BindingFailure::Projection(ProjectionFailure::Ineligible(r)))) if r == reason)
        );
    }
    assert_eq!(s.paths.lock().unwrap().len(), 13);
}

// Break caught: invalid source preparation revoking a still-current lease.
#[tokio::test]
async fn invalid_source_edit_preserves_lease_and_remove_readd_never_revives_it() {
    let s = Server::start().await;
    let a = s.authority();
    let old = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap();
    let mut bad = s.source();
    bad["cluster_key"] = json!("");
    assert_eq!(a.replace(&bad, &json!([])), Err(ProjectionFailure::Invalid));
    publish(&a, &old.candidate(NAME).unwrap()).unwrap();
    a.remove().unwrap();
    a.replace(&s.source(), &json!([])).unwrap();
    assert_eq!(
        publish(&a, &old.candidate(NAME).unwrap()),
        Err(binding(ProjectionFailure::Stale))
    );
    let new = a
        .leased_modern_inventory(&local(), Duration::from_secs(30))
        .await
        .unwrap();
    publish(&a, &new.candidate(NAME).unwrap()).unwrap();
    assert_eq!(s.paths.lock().unwrap().len(), 6);
}
