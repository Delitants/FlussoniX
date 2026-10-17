//! Breaks caught: modern observations detached from configured authority,
//! stale grants, mode downgrade, endpoint/key/trust substitution and policy bypass.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, RawQuery},
    http::HeaderMap,
    response::Response,
    routing::get,
};
use bytes::Bytes;
use flussonix::{
    m4_ingest::flussonic::MediaInput,
    m4f::{self, Frame},
    m4s::Track,
    source_directory::{
        LookupFailure,
        flussonic::modern::ObservedServerId,
        flussonic::{CandidateKind, EligibilityFailure},
        flussonic_config::{BindingFailure, ProjectionFailure, SourceAuthority},
        flussonic_policy::PolicyFailure,
    },
    wire::{self, Hub},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::AsyncReadExt;
#[path = "support/tls.rs"]
mod tls_fixture;
type Captures = Arc<Mutex<Vec<(String, HeaderMap)>>>;
struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(router: Router, cert: Option<&tls_fixture::Certificates>) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "{}://{}",
            if cert.is_some() { "https" } else { "http" },
            tcp.local_addr().unwrap()
        );
        let tls = cert.map(|c| c.server());
        let task = tokio::spawn(async move {
            if let Some(tls) = tls {
                axum::serve(
                    flussonix::http_tls::Listener::new(tcp, tls),
                    vendor_fixture::acknowledge_media(router),
                )
                .await
                .unwrap();
            } else {
                axum::serve(tcp, vendor_fixture::acknowledge_media(router))
                    .await
                    .unwrap();
            }
        });
        Self { url, task }
    }
}
fn source(url: &str) -> Value {
    json!({"url":url,"cluster_key":"synthetic:ключ","on_play":"true"})
}
fn row(name: &str) -> Value {
    json!({"name":name,"named_by":"config","groups":[]})
}
fn payload() -> (Vec<Track>, Bytes, Bytes) {
    let tracks = vec![Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }];
    let frames = vec![Frame {
        track_id: 1,
        dts: 90000,
        pts_offset: 0,
        key: true,
        body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
    }];
    let mut stream = wire::encode_info(&tracks).unwrap();
    stream.extend(wire::encode_frame(&tracks[0], &frames[0]).unwrap());
    let segment = m4f::pack(&tracks, &frames, 180000).unwrap();
    (tracks, stream.into(), segment.into())
}
async fn fixture(rows: Value, cert: Option<&tls_fixture::Certificates>) -> (Server, Captures) {
    let captures = Arc::new(Mutex::new(vec![]));
    let got = captures.clone();
    let (_, stream, segment) = payload();
    let router = Router::new().fallback(get(
        move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
            got.lock().unwrap().push((uri.to_string(), headers));
            let rows = rows.clone();
            let stream = stream.clone();
            let segment = segment.clone();
            async move {
                Response::new(Body::from(
                    if uri.path().ends_with("/streamer/api/v3/config") {
                        Bytes::from(json!({"stats":{"id":"owned-remote\n"}}).to_string())
                    } else if uri.path().ends_with("/streamer/api/v3/streams") {
                        Bytes::from(
                            json!({"server_id":"owned-remote\n","streams":rows}).to_string(),
                        )
                    } else if uri.path().ends_with("/m4s") {
                        stream
                    } else if uri.path().ends_with("/m4f") {
                        Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
                    } else {
                        segment
                    },
                ))
            }
        },
    ));
    (Server::start(router, cert).await, captures)
}
async fn pull(input: &MediaInput) -> (Result<(), String>, Option<Vec<Track>>, Vec<u8>, Hub) {
    let hub = Hub::new();
    let mut out = vec![];
    let (tx, rx) = tokio::sync::oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(4),
        input.pull_ready(&mut out, Some(&hub), tx),
    )
    .await
    .unwrap();
    (result, rx.await.ok(), out, hub)
}
fn sha1(s: &str) -> String {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let mut p = Command::new("openssl")
        .args(["dgst", "-sha1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    p.stdin.take().unwrap().write_all(s.as_bytes()).unwrap();
    let out = p.wait_with_output().unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout)
        .unwrap()
        .split_whitespace()
        .last()
        .unwrap()
        .into()
}
fn credentials(headers: &HeaderMap, name: Option<&str>) {
    credentials_with_key(headers, name, "synthetic:ключ");
}
fn credentials_with_key(headers: &HeaderMap, name: Option<&str>, key: &str) {
    assert!(!headers.contains_key("x-flussonix-peer"));
    if let Some(name) = name {
        assert!(!headers.contains_key("authorization"));
        let time = headers["cluster-auth-time"].to_str().unwrap();
        assert!(time.parse::<u64>().is_ok());
        assert_eq!(
            headers["cluster-auth-key"],
            sha1(&format!("{time}:{key}:{name}"))
        );
    } else {
        assert!(!headers.contains_key("cluster-auth-key"));
        let signed = headers["authorization"]
            .to_str()
            .unwrap()
            .strip_prefix("Cluster ")
            .unwrap();
        let (time, hash) = signed.split_once(':').unwrap();
        assert!(time.parse::<u64>().is_ok());
        assert_eq!(hash, sha1(&format!("{time}:{key}")));
    }
}

fn local() -> ObservedServerId {
    ObservedServerId::parse("owned-local").unwrap()
}
fn modern(c: &Value) -> SourceAuthority {
    SourceAuthority::new_modern(c, &json!([]), Duration::from_secs(3)).unwrap()
}
// Break caught: modern binding uses legacy discovery or loses native media transport/auth.
#[tokio::test]
async fn configured_modern_defaults_deliver_both_media_transports() {
    let cert = tls_fixture::Certificates::new();
    for scheme in ["m4f", "m4fs", "m4s", "m4ss"] {
        let secure = matches!(scheme, "m4fs" | "m4ss");
        let (server, captures) = fixture(json!([row("news")]), secure.then_some(&cert)).await;
        let mut config = source(&format!(
            "{scheme}://{}",
            server.url.split_once("://").unwrap().1
        ));
        if secure {
            config["flussonix_tls_ca"] = json!(cert.ca);
            config["flussonix_media_tls_ca"] = json!(cert.ca);
        }
        let a = modern(&config);
        let list = a.modern_inventory(&local()).await.unwrap();
        assert_eq!(list.server_id().as_str(), "owned-remote\n");
        assert_eq!(list.names().collect::<Vec<_>>(), ["news"]);
        let c = list.candidate("news").unwrap();
        assert_eq!(c.server_id().as_str(), "owned-remote\n");
        let input = a
            .at_modern_current(&c, |v, input| {
                assert_eq!(v.name, "news");
                input
            })
            .unwrap();
        let (result, tracks, out, hub) = pull(&input).await;
        assert_eq!(result.unwrap_err(), "source closed");
        assert_eq!(tracks.unwrap(), payload().0);
        assert!(!out.is_empty() && out.chunks_exact(188).all(|p| p[0] == 0x47));
        let protocol = if scheme.starts_with("m4f") {
            "m4f"
        } else {
            "m4s"
        };
        let seen = captures.lock().unwrap();
        assert_eq!(seen[0].0, "/streamer/api/v3/config");
        assert_eq!(
            seen[1].0,
            "/streamer/api/v3/streams?select=name%2Cnamed_by%2Cgroups%2Cstatic%2Cdisabled&limit=1000"
        );
        assert_eq!(seen[2].0, "/streamer/api/v3/config");
        assert_eq!(seen[3].0, format!("/news/{protocol}"));
        for (_, h) in &seen[..3] {
            credentials(h, None);
        }
        for (_, h) in &seen[3..] {
            credentials(h, Some("news"));
        }
        if protocol == "m4s" {
            assert_eq!(hub.m4s_subscribe().0.concat(), payload().1.as_ref());
        } else {
            assert_eq!(hub.segment("2026/10/03/01/00/00.m4f").unwrap(), payload().2);
        }
    }
}
// Break caught: remote inputs/policies substitute for prepared origins or bypass local filters.
#[tokio::test]
async fn split_origins_preserve_literal_names_filters_and_local_policy() {
    let name = "region/space %2F name";
    let mut item = row(name);
    item["groups"] = json!(["paid"]);
    item["inputs"] = json!([{"url":"http://attacker.invalid"}]);
    item["cluster_key"] = json!("remote-key");
    item["on_play"] = json!("true");
    let (management, mgmt) =
        fixture(json!([item, row("outside"), row("region/blocked")]), None).await;
    let (media, seen) = fixture(json!([]), None).await;
    let mut config = source("m4f://logical.invalid:8080");
    config["api_url"] = json!(format!("{}/management/", management.url));
    config["private_payload_url"] = json!(format!("{}/private/", media.url));
    config["prefix"] = json!("region");
    config["except"] = json!(["blocked"]);
    config["only"] = json!([]);
    config["group_config"] = json!({"paid":{"on_play":"https://billing.invalid/check"}});
    let a = modern(&config);
    let list = a.modern_inventory(&local()).await.unwrap();
    for n in ["outside", "region/blocked"] {
        assert_eq!(
            list.candidate(n).err(),
            Some(BindingFailure::Projection(ProjectionFailure::Excluded))
        );
    }
    assert_eq!(
        list.candidate("absent").err(),
        Some(BindingFailure::Missing)
    );
    let c = list.candidate(name).unwrap();
    let input = a
        .at_modern_current(&c, |v, input| {
            assert_eq!(v.source, "m4f://logical.invalid:8080");
            assert_eq!(
                v.policy.url.as_deref(),
                Some("https://billing.invalid/check")
            );
            assert!(!v.selected_static);
            input
        })
        .unwrap();
    assert!(pull(&input).await.1.is_some());
    let mgmt = mgmt.lock().unwrap();
    assert_eq!(mgmt.len(), 3);
    assert!(
        mgmt.iter()
            .all(|(u, _)| u.starts_with("/management/streamer/api/v3/"))
    );
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].0, "/private/region/space%20%252F%20name/m4f");
    assert_eq!(seen.len(), 2);
    for (_, h) in seen.iter() {
        credentials(h, Some(name));
    }
}
// Break caught: equal source values permit candidates to cross independent authorities.
#[tokio::test]
async fn opaque_modern_candidates_cannot_cross_equal_authorities() {
    let (server, _) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    let a = modern(&config);
    let b = modern(&config);
    let c = a
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate("news")
        .unwrap();
    let mut called = false;
    assert_eq!(
        b.at_modern_current(&c, |_, _| called = true),
        Err(ProjectionFailure::Stale)
    );
    assert!(!called);
    assert!(a.at_modern_current(&c, |_, _| ()).is_ok());
}
// Break caught: replacement/removal preserves old or subsequently constructed candidates.
#[tokio::test]
async fn edits_invalidate_existing_and_late_modern_candidates() {
    let (server, _) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    for edit in [
        None,
        Some(("cluster_key", json!("new-key"))),
        Some(("url", json!("m4f://other.invalid"))),
        Some(("api_url", json!("http://other.invalid/api"))),
        Some(("private_payload_url", json!("http://other.invalid/media"))),
        Some(("on_play", json!("https://billing.invalid/check"))),
    ] {
        let a = modern(&config);
        let list = a.modern_inventory(&local()).await.unwrap();
        let c = list.candidate("news").unwrap();
        let mut next = config.clone();
        if let Some((k, v)) = edit {
            next[k] = v;
        }
        a.replace(&next, &json!([])).unwrap();
        for old in [c, list.candidate("news").unwrap()] {
            let mut called = false;
            assert_eq!(
                a.at_modern_current(&old, |_, _| called = true),
                Err(ProjectionFailure::Stale)
            );
            assert!(!called);
        }
    }
    let a = modern(&config);
    let c = a
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate("news")
        .unwrap();
    a.remove().unwrap();
    assert_eq!(
        a.at_modern_current(&c, |_, _| ()),
        Err(ProjectionFailure::Stale)
    );
    assert_eq!(
        a.modern_inventory(&local()).await.err(),
        Some(BindingFailure::Projection(ProjectionFailure::Removed))
    );
}
// Break caught: held successes or failures escape the current-profile check.
#[tokio::test]
async fn held_modern_success_and_error_are_stale_after_source_changes() {
    for success in [true, false] {
        for action in ["replace", "remove", "readd"] {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let gates = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
            let server = Server::start(
                Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                    let gate = if uri.path().ends_with("/streams") {
                        gates.lock().unwrap().take()
                    } else {
                        None
                    };
                    async move {
                        if let Some((tx, rx)) = gate {
                            tx.send(()).unwrap();
                            rx.await.unwrap();
                            if !success {
                                return Response::builder()
                                    .status(503)
                                    .body(Body::empty())
                                    .unwrap();
                            }
                            Response::new(Body::from(
                                json!({"server_id":"remote","streams":[row("news")]}).to_string(),
                            ))
                        } else {
                            Response::new(Body::from(json!({"stats":{"id":"remote"}}).to_string()))
                        }
                    }
                })),
                None,
            )
            .await;
            let config = source(&server.url.replacen("http", "m4s", 1));
            let a = modern(&config);
            let id = local();
            let mut future = Box::pin(a.modern_inventory(&id));
            tokio::select! {r=&mut future=>panic!("premature completion: {}",r.is_ok()),r=entered_rx=>r.unwrap()};
            if action == "replace" {
                a.replace(&config, &json!([])).unwrap();
            } else {
                a.remove().unwrap();
                if action == "readd" {
                    a.replace(&config, &json!([])).unwrap();
                }
            }
            release_tx.send(()).unwrap();
            assert_eq!(
                future.await.err(),
                Some(BindingFailure::Projection(ProjectionFailure::Stale))
            );
        }
    }
}
// Break caught: explicit mode silently falls back to another endpoint or legacy lease API.
#[tokio::test]
async fn explicit_modes_reject_wrong_discovery_apis_without_io() {
    let (server, seen) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    let old = SourceAuthority::new(&config, &json!([])).unwrap();
    let a = modern(&config);
    for _ in 0..2 {
        assert_eq!(
            old.modern_inventory(&local()).await.err(),
            Some(BindingFailure::Projection(ProjectionFailure::Unsupported))
        );
        assert_eq!(
            a.inventory().await.err(),
            Some(BindingFailure::Projection(ProjectionFailure::Unsupported))
        );
        assert_eq!(
            a.identity_inventory(uuid::Uuid::new_v4()).await.err(),
            Some(BindingFailure::Projection(ProjectionFailure::Unsupported))
        );
        assert_eq!(
            a.leased_identity_inventory(uuid::Uuid::new_v4(), Duration::from_secs(1))
                .await
                .err(),
            Some(
                flussonix::source_directory::flussonic_config::LeaseFailure::Binding(
                    BindingFailure::Projection(ProjectionFailure::Unsupported)
                )
            )
        );
        old.replace(&config, &json!([])).unwrap();
        a.replace(&config, &json!([])).unwrap();
    }
    assert!(seen.lock().unwrap().is_empty());
}
// Break caught: replacement changes modern refresh budget or reverts to legacy750ms.
#[tokio::test]
async fn configured_modern_budget_is_retained_across_replacement() {
    let server = Server::start(
        Router::new().fallback(get(|OriginalUri(uri): OriginalUri| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Response::new(Body::from(
                if uri.path().ends_with("/config") {
                    json!({"stats":{"id":"remote"}})
                } else {
                    json!({"server_id":"remote","streams":[row("news")]})
                }
                .to_string(),
            ))
        })),
        None,
    )
    .await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    for (budget, want) in [
        (Duration::from_secs(3), true),
        (Duration::from_millis(100), false),
    ] {
        let a = SourceAuthority::new_modern(&config, &json!([]), budget).unwrap();
        for _ in 0..2 {
            assert_eq!(a.modern_inventory(&local()).await.is_ok(), want);
            a.replace(&config, &json!([])).unwrap();
        }
    }
}
// Break caught: bound candidate projection invents memberships/provenance or ignores group conflict.
#[tokio::test]
async fn modern_candidates_enforce_origin_membership_and_group_policy() {
    let rows = json!([
        {"name":"disabled","named_by":"config","groups":[],"disabled":true},
        {"name":"relay","named_by":"remote","groups":[]},
        {"name":"external","named_by":"external","groups":[]},
        {"name":"provenance","groups":[]},
        {"name":"membership","named_by":"config"},
        {"name":"conflict","named_by":"user","groups":["a","b"]},
        {"name":"ondemand","named_by":"user","groups":[],"static":false}]);
    let (server, _) = fixture(rows, None).await;
    let mut config = source(&server.url.replacen("http", "m4s", 1));
    config["group_config"] = json!({"a":{"on_play":"https://a.invalid/check"},"b":{"on_play":"https://b.invalid/check"}});
    let a = modern(&config);
    let list = a.modern_inventory(&local()).await.unwrap();
    for (name, why) in [
        ("disabled", EligibilityFailure::Disabled),
        ("relay", EligibilityFailure::Relayed),
        ("external", EligibilityFailure::External),
        ("provenance", EligibilityFailure::UnknownProvenance),
        ("membership", EligibilityFailure::UnknownGroups),
    ] {
        assert_eq!(
            list.candidate(name).err(),
            Some(BindingFailure::Projection(ProjectionFailure::Ineligible(
                why
            )))
        );
    }
    assert_eq!(
        list.candidate("conflict").err(),
        Some(BindingFailure::Projection(ProjectionFailure::Policy(
            PolicyFailure::Ambiguous
        )))
    );
    let c = list.candidate("ondemand").unwrap();
    assert_eq!(
        a.at_modern_current(&c, |v, _| (v.kind, v.selected_static))
            .unwrap(),
        (CandidateKind::OnDemand, false)
    );
}
// Break caught: invalid edits mutate current policy/key/trust or the mode admits unsupported fields.
#[tokio::test]
async fn invalid_modern_profiles_leave_existing_binding_current() {
    let (server, seen) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    let a = modern(&config);
    let c = a
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate("news")
        .unwrap();
    for (field, value) in [
        ("cluster_key", json!("")),
        ("api_url", json!("http://user:secret@host")),
        ("private_payload_url", json!("ftp://host")),
        ("flussonix_tls_ca", json!("/missing-owned-modern-ca.pem")),
        ("group_config", json!({"a":{"template":"unknown"}})),
        ("template", json!("unknown")),
        ("prefix", json!("../bad")),
    ] {
        let mut bad = config.clone();
        bad[field] = value;
        assert!(a.replace(&bad, &json!([])).is_err());
        assert!(SourceAuthority::new_modern(&bad, &json!([]), Duration::from_secs(3)).is_err());
        assert!(a.at_modern_current(&c, |_, _| ()).is_ok());
    }
    assert!(
        a.replace(&config, &json!([{"name":"bad","url":3}]))
            .is_err()
    );
    assert!(a.at_modern_current(&c, |_, _| ()).is_ok());
    for budget in [
        Duration::ZERO,
        Duration::from_secs(30) + Duration::from_nanos(1),
    ] {
        assert!(SourceAuthority::new_modern(&config, &json!([]), budget).is_err());
    }
    assert_eq!(seen.lock().unwrap().len(), 3);
}
// Break caught: the bound adapter bypasses identity/self/drift validation.
#[tokio::test]
async fn modern_binding_keeps_custom_self_and_page_identity_checks() {
    for (id, caller, page_id, expected_count) in [
        ("owned-local", "owned-local", "owned-local", 1),
        (
            "AAAAAAAA-1111-4111-8111-AAAAAAAAAAAA\n",
            "urn:uuid:aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
            "remote",
            1,
        ),
        ("remote", "owned-local", "changed", 2),
    ] {
        let seen = Arc::new(Mutex::new(0));
        let got = seen.clone();
        let server = Server::start(
            Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                *got.lock().unwrap() += 1;
                async move {
                    Response::new(Body::from(
                        if uri.path().ends_with("/config") {
                            json!({"stats":{"id":id}})
                        } else {
                            json!({"server_id":page_id,"streams":[row("news")]})
                        }
                        .to_string(),
                    ))
                }
            })),
            None,
        )
        .await;
        let a = modern(&source(&server.url.replacen("http", "m4s", 1)));
        assert_eq!(
            a.modern_inventory(&ObservedServerId::parse(caller).unwrap())
                .await
                .err(),
            Some(BindingFailure::Inventory(LookupFailure::Invalid))
        );
        assert_eq!(*seen.lock().unwrap(), expected_count);
    }
}
// Break caught: an incomplete later page is converted to an authority-bound partial success.
#[tokio::test]
async fn complete_modern_chain_is_required_before_binding() {
    let server = Server::start(
        Router::new().fallback(get(
            |OriginalUri(uri): OriginalUri, RawQuery(query): RawQuery| async move {
                Response::new(Body::from(
                    if uri.path().ends_with("/config") {
                        json!({"stats":{"id":"remote"}})
                    } else if query.as_ref().is_some_and(|q| q.contains("cursor=")) {
                        json!({"server_id":"remote","streams":[row("news")]})
                    } else {
                        json!({"server_id":"remote","streams":[row("news")],"next":"later"})
                    }
                    .to_string(),
                ))
            },
        )),
        None,
    )
    .await;
    let a = modern(&source(&server.url.replacen("http", "m4s", 1)));
    assert_eq!(
        a.modern_inventory(&local()).await.err(),
        Some(BindingFailure::Inventory(LookupFailure::Invalid))
    );
}
// Break caught: changed files are reread per discovery/media instead of frozen per-leg trust.
#[tokio::test]
async fn modern_management_and_media_trust_stay_frozen_until_valid_replacement() {
    let mgmt_cert = tls_fixture::Certificates::new();
    let media_cert = tls_fixture::Certificates::new();
    let (management, _) = fixture(json!([row("news")]), Some(&mgmt_cert)).await;
    let (media, _) = fixture(json!([]), Some(&media_cert)).await;
    let mut config = source("m4ss://logical.invalid");
    config["api_url"] = json!(management.url);
    config["private_payload_url"] = json!(media.url);
    config["flussonix_tls_ca"] = json!(mgmt_cert.ca);
    config["flussonix_media_tls_ca"] = json!(media_cert.ca);
    let a = modern(&config);
    let mgmt_bytes = std::fs::read(&mgmt_cert.ca).unwrap();
    let media_bytes = std::fs::read(&media_cert.ca).unwrap();
    std::fs::write(&mgmt_cert.ca, b"changed CA").unwrap();
    std::fs::write(&media_cert.ca, b"changed CA").unwrap();
    let list = a.modern_inventory(&local()).await.unwrap();
    let c = list.candidate("news").unwrap();
    let input = a.at_modern_current(&c, |_, input| input).unwrap();
    assert!(pull(&input).await.1.is_some());
    assert!(a.replace(&config, &json!([])).is_err());
    assert!(a.at_modern_current(&c, |_, _| ()).is_ok());
    std::fs::write(&mgmt_cert.ca, mgmt_bytes).unwrap();
    std::fs::write(&media_cert.ca, media_bytes).unwrap();
    a.replace(&config, &json!([])).unwrap();
    assert_eq!(
        a.at_modern_current(&c, |_, _| ()),
        Err(ProjectionFailure::Stale)
    );
    assert!(a.modern_inventory(&local()).await.is_ok());
}
// Break caught: missing/wrong management CA falls back to media trust and vice versa.
#[tokio::test]
async fn modern_binding_never_falls_back_between_independent_trust_legs() {
    let mgmt_cert = tls_fixture::Certificates::new();
    let media_cert = tls_fixture::Certificates::new();
    let (management, mgmt_seen) = fixture(json!([row("news")]), Some(&mgmt_cert)).await;
    let (media, media_seen) = fixture(json!([]), Some(&media_cert)).await;
    for profile in ["mgmt_missing", "mgmt_wrong", "media_missing", "media_wrong"] {
        let mut config = source("m4ss://logical.invalid");
        config["api_url"] = json!(management.url);
        config["private_payload_url"] = json!(media.url);
        if profile != "mgmt_missing" {
            config["flussonix_tls_ca"] = json!(if profile == "mgmt_wrong" {
                &media_cert.ca
            } else {
                &mgmt_cert.ca
            });
        }
        if profile != "media_missing" {
            config["flussonix_media_tls_ca"] = json!(if profile == "media_wrong" {
                &mgmt_cert.ca
            } else {
                &media_cert.ca
            });
        }
        let a = modern(&config);
        let before = mgmt_seen.lock().unwrap().len();
        let result = a.modern_inventory(&local()).await;
        if profile.starts_with("mgmt") {
            assert_eq!(
                result.err(),
                Some(BindingFailure::Inventory(LookupFailure::Unavailable))
            );
            assert_eq!(mgmt_seen.lock().unwrap().len(), before);
        } else {
            let c = result.unwrap().candidate("news").unwrap();
            let input = a.at_modern_current(&c, |_, input| input).unwrap();
            let (_, tracks, out, _) = pull(&input).await;
            assert!(tracks.is_none() && out.is_empty());
            assert!(media_seen.lock().unwrap().is_empty());
        }
    }
}
// Break caught: publication releases the source lock before the synchronous closure ends.
#[tokio::test]
async fn modern_publication_serializes_source_replacement() {
    let (server, _) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    let a = Arc::new(modern(&config));
    let c = a
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate("news")
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
    let (edited_tx, edited_rx) = std::sync::mpsc::channel();
    let copy = a.clone();
    let publisher = std::thread::spawn(move || {
        copy.at_modern_current(&c, |_, _| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let copy = a.clone();
    let editor = std::thread::spawn(move || {
        attempt_tx.send(()).unwrap();
        let result = copy.replace(&config, &json!([]));
        edited_tx.send(()).unwrap();
        result
    });
    attempt_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let during = edited_rx.recv_timeout(Duration::from_millis(100));
    release_tx.send(()).unwrap();
    publisher.join().unwrap().unwrap();
    editor.join().unwrap().unwrap();
    assert!(matches!(
        during,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    edited_rx.recv_timeout(Duration::from_secs(3)).unwrap();
}
// Break caught: a detached authority lookup retains the cancelled HTTP request.
#[tokio::test]
async fn cancelling_modern_binding_drops_pending_probe() {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = tcp.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
            assert!(request.len() < 16384);
        }
        entered_tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), tcp.accept())
                .await
                .is_err()
        );
    });
    let a = modern(&source(&format!("m4s://{addr}")));
    let id = local();
    let mut future = Box::pin(a.modern_inventory(&id));
    tokio::select! {r=&mut future=>panic!("premature completion: {}",r.is_ok()),r=entered_rx=>r.unwrap()};
    drop(future);
    task.await.unwrap();
}
// Break caught: a valid replacement retains the previous prepared client/key or media transport.
#[tokio::test]
async fn replacement_prepares_new_management_and_media_from_new_profile() {
    let (first, old_seen) = fixture(json!([row("news")]), None).await;
    let (second, new_seen) = fixture(json!([row("news")]), None).await;
    let config = source(&first.url.replacen("http", "m4s", 1));
    let a = modern(&config);
    let old = a
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate("news")
        .unwrap();
    let mut next = source(&second.url.replacen("http", "m4f", 1));
    next["cluster_key"] = json!("replacement:key");
    next["on_play"] = json!("https://new-billing.invalid/check");
    a.replace(&next, &json!([])).unwrap();
    assert_eq!(
        a.at_modern_current(&old, |_, _| ()),
        Err(ProjectionFailure::Stale)
    );
    let list = a.modern_inventory(&local()).await.unwrap();
    let c = list.candidate("news").unwrap();
    let input = a
        .at_modern_current(&c, |v, input| {
            assert_eq!(
                v.policy.url.as_deref(),
                Some("https://new-billing.invalid/check")
            );
            input
        })
        .unwrap();
    assert!(pull(&input).await.1.is_some());
    assert_eq!(old_seen.lock().unwrap().len(), 3);
    let seen = new_seen.lock().unwrap();
    assert_eq!(seen.len(), 5);
    assert_eq!(seen[3].0, "/news/m4f");
    for (_, h) in &seen[..3] {
        credentials_with_key(h, None, "replacement:key");
    }
    for (_, h) in &seen[3..] {
        credentials_with_key(h, Some("news"), "replacement:key");
    }
}

// Unknown membership may bind only when local authority proves it cannot affect policy.
#[tokio::test]
async fn omitted_groups_modern_binding_preserves_uniform_callback_and_token_guards() {
    let (server, _) = fixture(
        json!([{"name":"news","named_by":"config","static":false}]),
        None,
    )
    .await;
    for groups in [
        json!({}),
        json!({"same":{}}),
        json!({"same":{"on_play":"https://billing.invalid/check","flussonix_token_sha256":"a".repeat(64)}}),
    ] {
        let mut config = source(&server.url.replacen("http", "m4s", 1));
        config["on_play"] = json!("https://billing.invalid/check");
        config["flussonix_token_sha256"] = json!("a".repeat(64));
        config["group_config"] = groups;
        let authority = modern(&config);
        let list = authority.modern_inventory(&local()).await.unwrap();
        let candidate = list
            .candidate("news")
            .expect("membership-independent local policy should bind");
        authority.at_modern_current(&candidate, |view, _| {
            assert_eq!(view.policy.url.as_deref(), Some("https://billing.invalid/check"));
            assert_eq!(view.policy, &flussonix::playback_auth::Policy::from_config(&json!({"on_play":"https://billing.invalid/check","flussonix_token_sha256":"a".repeat(64)}), &json!({})).unwrap());
            assert_eq!(view.kind, CandidateKind::OnDemand);
            assert!(!view.selected_static);
        }).unwrap();
    }
    drop(server);
}

#[tokio::test]
async fn omitted_groups_modern_binding_rejects_nonuniform_policy_and_nonlocal_origins() {
    let (server, _) = fixture(
        json!([
            {"name":"news","named_by":"config"},
            {"name":"relay","named_by":"remote"},
            {"name":"external","named_by":"external"},
            {"name":"unknown"},
            {"name":"disabled","named_by":"config","disabled":true}
        ]),
        None,
    )
    .await;
    for group in [
        json!({"on_play":"https://other.invalid/check"}),
        json!({"flussonix_token_sha256":"b".repeat(64)}),
    ] {
        let mut config = source(&server.url.replacen("http", "m4s", 1));
        config["group_config"] = json!({"protected":group});
        let authority = modern(&config);
        let list = authority.modern_inventory(&local()).await.unwrap();
        assert_eq!(
            list.candidate("news").err(),
            Some(BindingFailure::Projection(ProjectionFailure::Ineligible(
                EligibilityFailure::UnknownGroups
            )))
        );
    }
    let config = source(&server.url.replacen("http", "m4s", 1));
    let authority = modern(&config);
    let list = authority.modern_inventory(&local()).await.unwrap();
    for (name, why) in [
        ("relay", EligibilityFailure::Relayed),
        ("external", EligibilityFailure::External),
        ("unknown", EligibilityFailure::UnknownProvenance),
        ("disabled", EligibilityFailure::Disabled),
    ] {
        assert_eq!(
            list.candidate(name).err(),
            Some(BindingFailure::Projection(ProjectionFailure::Ineligible(
                why
            )))
        );
    }
    let mut missing = config.clone();
    missing.as_object_mut().unwrap().remove("on_play");
    let no_policy = modern(&missing);
    assert!(
        no_policy
            .modern_inventory(&local())
            .await
            .unwrap()
            .candidate("news")
            .is_err()
    );
    let mut excluded = config.clone();
    excluded["except"] = json!(["news"]);
    assert_eq!(
        modern(&excluded)
            .modern_inventory(&local())
            .await
            .unwrap()
            .candidate("news")
            .err(),
        Some(BindingFailure::Projection(ProjectionFailure::Excluded))
    );
    drop(server);
}

#[tokio::test]
async fn omitted_groups_policy_change_invalidates_old_binding_and_rejects_new_lookup() {
    let (server, _) = fixture(json!([{"name":"news","named_by":"config"}]), None).await;
    let mut config = source(&server.url.replacen("http", "m4s", 1));
    let authority = modern(&config);
    let old = authority
        .modern_inventory(&local())
        .await
        .unwrap()
        .candidate("news")
        .unwrap();
    config["group_config"] = json!({"protected":{"on_play":"https://other.invalid/check"}});
    authority.replace(&config, &json!([])).unwrap();
    assert_eq!(
        authority.at_modern_current(&old, |_, _| ()).err(),
        Some(ProjectionFailure::Stale)
    );
    assert_eq!(
        authority
            .modern_inventory(&local())
            .await
            .unwrap()
            .candidate("news")
            .err(),
        Some(BindingFailure::Projection(ProjectionFailure::Ineligible(
            EligibilityFailure::UnknownGroups
        )))
    );
    drop(server);
}
