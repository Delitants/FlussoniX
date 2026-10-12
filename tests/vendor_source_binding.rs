//! Breaks caught: cross-source facts, stale fetches, wrong origins/principals,
//! mutable CA trust and adapter paths that bypass the qualified wire clients.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, RawQuery},
    http::{HeaderMap, StatusCode},
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
        flussonic_config::{BindingFailure, ProjectionFailure, SourceAuthority},
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
                Response::new(Body::from(if uri.path().ends_with("/-/streams") {
                    Bytes::from(json!({"streams":rows}).to_string())
                } else if uri.path().ends_with("/m4s") {
                    stream
                } else if uri.path().ends_with("/m4f") {
                    Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
                } else {
                    segment
                }))
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
    assert!(!headers.contains_key("x-flussonix-peer"));
    if let Some(name) = name {
        assert!(!headers.contains_key("authorization"));
        let time = headers["cluster-auth-time"].to_str().unwrap();
        assert!(time.parse::<u64>().is_ok());
        assert_eq!(
            headers["cluster-auth-key"],
            sha1(&format!("{time}:synthetic:ключ:{name}"))
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
        assert_eq!(hash, sha1(&format!("{time}:synthetic:ключ")));
    }
}
// Break caught: default origins or transport/key use native credentials or another source.
#[tokio::test]
async fn default_origins_keep_wire_transport_and_auth() {
    let cert = tls_fixture::Certificates::new();
    for scheme in ["m4f", "m4fs", "m4s", "m4ss"] {
        let secure = scheme.ends_with('s') && scheme != "m4s";
        let (server, captures) = fixture(json!([row("news")]), secure.then_some(&cert)).await;
        let protocol = if scheme.starts_with("m4f") {
            "m4f"
        } else {
            "m4s"
        };
        let mut config = source(&format!(
            "{scheme}://{}",
            server.url.split_once("://").unwrap().1
        ));
        if secure {
            config["flussonix_tls_ca"] = json!(cert.ca);
            config["flussonix_media_tls_ca"] = json!(cert.ca);
        }
        let a = SourceAuthority::new(&config, &json!([])).unwrap();
        let inventory = a.inventory().await.unwrap();
        assert_eq!(inventory.names().collect::<Vec<_>>(), ["news"]);
        let c = inventory.candidate("news").unwrap();
        let input = a
            .at_bound_current(&c, |v, input| {
                assert_eq!(v.name, "news");
                input
            })
            .unwrap();
        let (result, metadata, out, hub) = pull(&input).await;
        assert_eq!(result.unwrap_err(), "source closed");
        assert_eq!(metadata.unwrap(), payload().0);
        assert!(!out.is_empty());
        assert!(out.chunks_exact(188).all(|p| p[0] == 0x47));
        let seen = captures.lock().unwrap();
        assert_eq!(seen[0].0, "/-/streams");
        credentials(&seen[0].1, None);
        assert_eq!(seen[1].0, format!("/news/{protocol}"));
        for (_, h) in &seen[1..] {
            credentials(h, Some("news"));
        }
        if protocol == "m4s" {
            assert_eq!(hub.m4s_subscribe().0.concat(), payload().1.as_ref());
        } else {
            assert_eq!(hub.segment("2026/10/03/01/00/00.m4f").unwrap(), payload().2);
        }
    }
}
// Break caught: remote URLs/keys/policy redirect media or proxy prefixes/name bytes are lost.
#[tokio::test]
async fn explicit_origins_keep_literal_names_and_local_policy() {
    let name = "region/space %2F name";
    let mut item = row(name);
    item["groups"] = json!(["paid"]);
    item["url"] = json!("http://attacker.invalid");
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
    let a = SourceAuthority::new(&config, &json!([])).unwrap();
    let list = a.inventory().await.unwrap();
    assert_eq!(
        list.candidate("outside").err(),
        Some(BindingFailure::Projection(ProjectionFailure::Excluded))
    );
    assert_eq!(
        list.candidate("region/blocked").err(),
        Some(BindingFailure::Projection(ProjectionFailure::Excluded))
    );
    assert_eq!(
        list.candidate("absent").err(),
        Some(BindingFailure::Missing)
    );
    let c = list.candidate(name).unwrap();
    let input = a
        .at_bound_current(&c, |v, input| {
            assert_eq!(v.source, "m4f://logical.invalid:8080");
            assert_eq!(
                v.policy.url.as_deref(),
                Some("https://billing.invalid/check")
            );
            assert!(!v.selected_static);
            input
        })
        .unwrap();
    let (_, metadata, _, _) = pull(&input).await;
    assert!(metadata.is_some());
    let mgmt = mgmt.lock().unwrap();
    assert_eq!(mgmt[0].0, "/management/-/streams");
    credentials(&mgmt[0].1, None);
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].0, "/private/region/space%20%252F%20name/m4f");
    assert_eq!(seen.len(), 2);
    for (_, h) in seen.iter() {
        credentials(h, Some(name));
    }
}
// Break caught: malformed endpoints/CAs are accepted or a failed edit changes authority.
#[tokio::test]
async fn invalid_endpoint_and_trust_edits_leave_bound_candidate_current() {
    let (server, _) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    let a = SourceAuthority::new(&config, &json!([])).unwrap();
    let c = a.inventory().await.unwrap().candidate("news").unwrap();
    for field in ["api_url", "private_payload_url"] {
        for value in [
            json!(null),
            json!(false),
            json!("ftp://host"),
            json!("http://user:secret@host"),
            json!("http://host?x"),
            json!("http://host#x"),
            json!("http://host:0"),
            json!("http://host\\path"),
            json!(" http://host"),
            json!("http://host/\n"),
        ] {
            let mut bad = config.clone();
            bad[field] = value;
            assert!(a.replace(&bad, &json!([])).is_err());
            assert!(a.at_bound_current(&c, |_, _| ()).is_ok());
        }
    }
    for field in ["flussonix_tls_ca", "flussonix_media_tls_ca"] {
        for value in [
            json!(null),
            json!(false),
            json!("relative.pem"),
            json!("/missing-owned-binding-ca.pem"),
        ] {
            let mut bad = config.clone();
            bad[field] = value;
            assert!(a.replace(&bad, &json!([])).is_err());
            assert!(a.at_bound_current(&c, |_, _| ()).is_ok());
        }
    }
}
// Break caught: identical URLs/key/revision permit crossing independent authority objects.
#[tokio::test]
async fn bound_results_cannot_cross_equal_authority_instances() {
    let (server, _) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    let a = SourceAuthority::new(&config, &json!([])).unwrap();
    let b = SourceAuthority::new(&config, &json!([])).unwrap();
    let c = a.inventory().await.unwrap().candidate("news").unwrap();
    let mut called = false;
    assert_eq!(
        b.at_bound_current(&c, |_, _| {
            called = true;
        }),
        Err(ProjectionFailure::Stale)
    );
    assert!(!called);
    assert!(a.at_bound_current(&c, |_, _| ()).is_ok());
}
// Break caught: a binding/key/transport edit resurrects existing or late old-result candidates.
#[tokio::test]
async fn changed_binding_settings_invalidate_existing_and_late_candidates() {
    let (server, _) = fixture(json!([row("news")]), None).await;
    let config = source(&server.url.replacen("http", "m4s", 1));
    for edit in [
        None,
        Some(("cluster_key", json!("other-key"))),
        Some(("url", json!("m4f://other.invalid"))),
        Some(("api_url", json!("http://other.invalid/api"))),
        Some(("private_payload_url", json!("http://other.invalid/private"))),
    ] {
        let a = SourceAuthority::new(&config, &json!([])).unwrap();
        let list = a.inventory().await.unwrap();
        let c = list.candidate("news").unwrap();
        let mut new = config.clone();
        if let Some((field, value)) = edit {
            new[field] = value;
        }
        a.replace(&new, &json!([])).unwrap();
        for old in [c, list.candidate("news").unwrap()] {
            assert_eq!(
                a.at_bound_current(&old, |_, _| ()),
                Err(ProjectionFailure::Stale)
            );
        }
    }
}
// Break caught: a delayed success/error from replaced or removed settings becomes current discovery.
#[tokio::test]
async fn delayed_results_cannot_survive_replacement_or_removal() {
    for success in [true, false] {
        for action in ["replace", "remove", "readd"] {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let gates = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
            let server = Server::start(
                Router::new().route(
                    "/-/streams",
                    get(move || {
                        let (tx, rx) = gates.lock().unwrap().take().unwrap();
                        async move {
                            let _ = tx.send(());
                            rx.await.unwrap();
                            if success {
                                Response::new(Body::from(
                                    json!({"streams":[row("news")]}).to_string(),
                                ))
                            } else {
                                Response::builder().status(503).body(Body::empty()).unwrap()
                            }
                        }
                    }),
                ),
                None,
            )
            .await;
            let config = source(&server.url.replacen("http", "m4s", 1));
            let a = SourceAuthority::new(&config, &json!([])).unwrap();
            let mut future = Box::pin(a.inventory());
            tokio::select! {r=&mut future=>panic!("request completed before gate: {}",r.is_ok()),r=entered_rx=>r.unwrap()}
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
// Break caught: a valid first page is bound before a malformed later page is validated.
#[tokio::test]
async fn incomplete_invalid_chain_never_exposes_bound_result() {
    let server = Server::start(
        Router::new().route(
            "/-/streams",
            get(|RawQuery(q): RawQuery| async move {
                Body::from(
                    if q.is_none() {
                        json!({"streams":[row("news")],"next":"page2"})
                    } else {
                        json!({"streams":[{"name":"bad","groups":null}]})
                    }
                    .to_string(),
                )
            }),
        ),
        None,
    )
    .await;
    let a =
        SourceAuthority::new(&source(&server.url.replacen("http", "m4s", 1)), &json!([])).unwrap();
    assert_eq!(
        a.inventory().await.err(),
        Some(BindingFailure::Inventory(LookupFailure::Invalid))
    );
}
// Break caught: the adapter spawns detached discovery that outlives cancellation.
#[tokio::test]
async fn cancelling_inventory_drops_the_inflight_connection() {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = tcp.accept().await.unwrap();
        let mut request = vec![];
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
            assert!(request.len() < 8192);
        }
        entered_tx.send(()).unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_millis(500), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let a = SourceAuthority::new(&source(&format!("m4s://{address}")), &json!([])).unwrap();
    let mut future = Box::pin(a.inventory());
    tokio::select! {r=&mut future=>panic!("premature result: {}",r.is_ok()),r=entered_rx=>r.unwrap()};
    drop(future);
    assert_eq!(server.await.unwrap(), 0);
}
// Break caught: media reconstructs trust from a changed CA path instead of immutable profile trust.
#[tokio::test]
async fn tls_trust_is_frozen_per_leg_until_valid_replacement() {
    let mgmt_cert = tls_fixture::Certificates::new();
    let media_cert = tls_fixture::Certificates::new();
    let (management, _) = fixture(json!([row("news")]), Some(&mgmt_cert)).await;
    let (media, _) = fixture(json!([]), Some(&media_cert)).await;
    let mut config = source("m4ss://logical.invalid");
    config["api_url"] = json!(management.url);
    config["private_payload_url"] = json!(media.url);
    config["flussonix_tls_ca"] = json!(mgmt_cert.ca);
    config["flussonix_media_tls_ca"] = json!(media_cert.ca);
    let a = SourceAuthority::new(&config, &json!([])).unwrap();
    let mgmt_bytes = std::fs::read(&mgmt_cert.ca).unwrap();
    let media_bytes = std::fs::read(&media_cert.ca).unwrap();
    std::fs::write(&mgmt_cert.ca, b"changed invalid CA").unwrap();
    std::fs::write(&media_cert.ca, b"changed invalid CA").unwrap();
    let list = a.inventory().await.unwrap();
    let c = list.candidate("news").unwrap();
    let input = a.at_bound_current(&c, |_, input| input).unwrap();
    assert!(pull(&input).await.1.is_some());
    assert!(a.replace(&config, &json!([])).is_err());
    assert!(a.at_bound_current(&c, |_, _| ()).is_ok());
    std::fs::write(&mgmt_cert.ca, mgmt_bytes).unwrap();
    std::fs::write(&media_cert.ca, media_bytes).unwrap();
    a.replace(&config, &json!([])).unwrap();
    assert_eq!(
        a.at_bound_current(&c, |_, _| ()),
        Err(ProjectionFailure::Stale)
    );
    assert!(a.inventory().await.is_ok());
}
// Break caught: custom trust leaks across management/private legs or verification is bypassed.
#[tokio::test]
async fn trust_does_not_fall_back_across_legs_or_to_untrusted_roots() {
    let mgmt_cert = tls_fixture::Certificates::new();
    let media_cert = tls_fixture::Certificates::new();
    let (management, mgmt_seen) = fixture(json!([row("news")]), Some(&mgmt_cert)).await;
    let (media, media_seen) = fixture(json!([]), Some(&media_cert)).await;
    for profile in ["mgmt_missing", "mgmt_wrong", "media_missing", "media_wrong"] {
        let mut c = source("m4ss://logical.invalid");
        c["api_url"] = json!(management.url);
        c["private_payload_url"] = json!(media.url);
        if profile != "mgmt_missing" {
            c["flussonix_tls_ca"] = json!(if profile == "mgmt_wrong" {
                &media_cert.ca
            } else {
                &mgmt_cert.ca
            });
        }
        if profile != "media_missing" {
            c["flussonix_media_tls_ca"] = json!(if profile == "media_wrong" {
                &mgmt_cert.ca
            } else {
                &media_cert.ca
            });
        }
        let a = SourceAuthority::new(&c, &json!([])).unwrap();
        let before = mgmt_seen.lock().unwrap().len();
        let result = a.inventory().await;
        if profile.starts_with("mgmt") {
            assert_eq!(
                result.err(),
                Some(BindingFailure::Inventory(LookupFailure::Unavailable))
            );
            assert_eq!(mgmt_seen.lock().unwrap().len(), before);
        } else {
            let c = result.unwrap().candidate("news").unwrap();
            let input = a.at_bound_current(&c, |_, input| input).unwrap();
            let (result, metadata, out, _) = pull(&input).await;
            assert!(result.is_err());
            assert!(metadata.is_none());
            assert!(out.is_empty());
        }
    }
    assert!(media_seen.lock().unwrap().is_empty());
}
// Break caught: a bound client follows a redirect and leaks credentials to another configured origin.
#[tokio::test]
async fn bound_clients_keep_redirect_confinement() {
    let (target, seen) = fixture(json!([row("news")]), None).await;
    let location = target.url.clone();
    let redirect = Server::start(
        Router::new().fallback(get(move || {
            let location = location.clone();
            async move {
                Response::builder()
                    .status(302)
                    .header("location", location)
                    .body(Body::empty())
                    .unwrap()
            }
        })),
        None,
    )
    .await;
    let a = SourceAuthority::new(
        &source(&redirect.url.replacen("http", "m4s", 1)),
        &json!([]),
    )
    .unwrap();
    assert_eq!(
        a.inventory().await.err(),
        Some(BindingFailure::Inventory(LookupFailure::Unavailable))
    );
    assert!(seen.lock().unwrap().is_empty());
    let mut config = source("m4ss://logical.invalid");
    config["api_url"] = json!(target.url);
    config["private_payload_url"] = json!(redirect.url);
    let a = SourceAuthority::new(&config, &json!([])).unwrap();
    let c = a.inventory().await.unwrap().candidate("news").unwrap();
    let input = a.at_bound_current(&c, |_, input| input).unwrap();
    assert!(pull(&input).await.1.is_none());
    assert_eq!(seen.lock().unwrap().len(), 1);
}
// Break caught: rejected inventory credentials trigger fallback/probing or bind empty success.
#[tokio::test]
async fn rejected_inventory_auth_does_not_probe_fallback() {
    let seen: Captures = Arc::new(Mutex::new(vec![]));
    let got = seen.clone();
    let server = Server::start(
        Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                got.lock().unwrap().push((uri.to_string(), headers));
                async { StatusCode::UNAUTHORIZED }
            },
        )),
        None,
    )
    .await;
    let a =
        SourceAuthority::new(&source(&server.url.replacen("http", "m4s", 1)), &json!([])).unwrap();
    assert_eq!(
        a.inventory().await.err(),
        Some(BindingFailure::Inventory(LookupFailure::Unavailable))
    );
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    credentials(&seen[0].1, None);
}
