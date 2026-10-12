//! Breaks caught: self-source discovery, identity drift, independent per-request
//! budgets, credential fallback, stale publication and detached identity probes.
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
    flussonic_config::{BindingFailure, ProjectionFailure, SourceAuthority},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;
use uuid::Uuid;
#[path = "support/tls.rs"]
mod tls_fixture;
const LOCAL: &str = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";
const REMOTE: &str = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb";
const OTHER: &str = "cccccccc-3333-4333-8333-cccccccccccc";
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
                axum::serve(flussonix::http_tls::Listener::new(tcp, tls), router)
                    .await
                    .unwrap();
            } else {
                axum::serve(tcp, router).await.unwrap();
            }
        });
        Self { url, task }
    }
}
fn body(value: Value) -> Response {
    Response::new(Body::from(value.to_string()))
}
fn config(id: &str) -> Value {
    json!({"stats":{"id":id}})
}
fn inventory() -> Value {
    json!({"streams":[{"name":"Music %2F HD","named_by":"config","groups":[]}]})
}
fn authority(url: &str) -> SourceAuthority {
    SourceAuthority::new(
        &json!({"url":url.replacen("http","m4s",1),"cluster_key":"owned:ключ","on_play":"true"}),
        &json!([]),
    )
    .unwrap()
}
async fn fixture<F>(f: F, cert: Option<&tls_fixture::Certificates>) -> (Server, Captures)
where
    F: Fn(usize, &str) -> Response + Send + Sync + 'static,
{
    let got: Captures = Arc::new(Mutex::new(vec![]));
    let capture = got.clone();
    let f = Arc::new(f);
    let router = Router::new().fallback(get(
        move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
            let mut captures = capture.lock().unwrap();
            let index = captures.len();
            captures.push((uri.to_string(), headers));
            let response = f(index, uri.path());
            async move { response }
        },
    ));
    (Server::start(router, cert).await, got)
}
fn invalid(
    result: Result<
        flussonix::source_directory::flussonic_config::IdentityInventory,
        BindingFailure,
    >,
) {
    assert!(matches!(
        result,
        Err(BindingFailure::Inventory(LookupFailure::Invalid))
    ));
}
fn local() -> Uuid {
    Uuid::parse_str(LOCAL).unwrap()
}
fn credentials(headers: &HeaderMap) {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let signed = headers["authorization"]
        .to_str()
        .unwrap()
        .strip_prefix("Cluster ")
        .unwrap();
    let (time, digest) = signed.split_once(':').unwrap();
    assert!(time.parse::<u64>().is_ok());
    let mut p = Command::new("openssl")
        .args(["dgst", "-sha1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    p.stdin
        .take()
        .unwrap()
        .write_all(format!("{time}:owned:ключ").as_bytes())
        .unwrap();
    let output = p.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        digest,
        String::from_utf8(output.stdout)
            .unwrap()
            .split_whitespace()
            .last()
            .unwrap()
    );
    assert!(!headers.contains_key("x-flussonix-peer"));
    assert!(!headers.contains_key("cluster-auth-key"));
}
#[tokio::test]
async fn identity_checked_discovery_keeps_proxy_path_auth_and_bound_media_default() {
    let (server, got) = fixture(
        |n, path| {
            if path.ends_with("/config") {
                body(config(REMOTE))
            } else if path.ends_with("/-/streams") {
                body(inventory())
            } else {
                assert_eq!(n, 3);
                Response::new(Body::empty())
            }
        },
        None,
    )
    .await;
    let source = json!({"url":"m4s://unused.invalid","api_url":format!("{}/proxy",server.url),"cluster_key":"owned:ключ","on_play":"true"});
    let a = SourceAuthority::new(&source, &json!([])).unwrap();
    let rows = a.identity_inventory(local()).await.unwrap();
    assert_eq!(rows.server_id(), Uuid::parse_str(REMOTE).unwrap());
    assert_eq!(rows.names().collect::<Vec<_>>(), ["Music %2F HD"]);
    let candidate = rows.candidate("Music %2F HD").unwrap();
    assert_eq!(candidate.server_id(), rows.server_id());
    let input = a
        .at_identity_current(&candidate, |view, input| {
            assert_eq!(view.name, "Music %2F HD");
            assert_eq!(view.policy.url, None);
            input
        })
        .unwrap();
    let mut out = vec![];
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let _ = input.pull_ready(&mut out, None, tx).await;
    let captures = got.lock().unwrap();
    assert_eq!(
        captures.iter().map(|x| x.0.as_str()).collect::<Vec<_>>(),
        [
            "/proxy/streamer/api/v3/config",
            "/proxy/-/streams",
            "/proxy/streamer/api/v3/config",
            "/proxy/Music%20%252F%20HD/m4s"
        ]
    );
    for (_, h) in &captures[..3] {
        credentials(h);
    }
    assert!(captures[3].1.contains_key("cluster-auth-key"));
    assert!(!captures[3].1.contains_key("authorization"));
}
#[tokio::test]
async fn self_identity_and_nil_local_never_fetch_inventory() {
    for id in [LOCAL, "AAAAAAAA-1111-4111-8111-AAAAAAAAAAAA"] {
        let id = id.to_owned();
        let (server, got) = fixture(move |_, _| body(config(&id)), None).await;
        let a = authority(&server.url);
        invalid(a.identity_inventory(local()).await);
        assert_eq!(got.lock().unwrap().len(), 1);
        invalid(a.identity_inventory(Uuid::nil()).await);
        assert_eq!(got.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn missing_malformed_and_nil_remote_ids_fail_closed() {
    for value in [
        json!({}),
        json!({"stats":null}),
        json!({"stats":[]}),
        json!({"stats":{}}),
        json!({"stats":{"id":null}}),
        json!({"stats":{"id":1}}),
        config(""),
        config("not-a-uuid"),
        config("00000000-0000-0000-0000-000000000000"),
        config("bbbbbbbb222242228222bbbbbbbbbbbb"),
    ] {
        let (server, got) = fixture(move |_, _| body(value.clone()), None).await;
        invalid(authority(&server.url).identity_inventory(local()).await);
        assert_eq!(got.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn second_probe_rejects_changed_self_or_missing_identity() {
    for after in [config(OTHER), config(LOCAL), json!({"stats":{}})] {
        let (server, got) = fixture(
            move |n, _| {
                body(match n {
                    0 => config(REMOTE),
                    1 => inventory(),
                    _ => after.clone(),
                })
            },
            None,
        )
        .await;
        invalid(authority(&server.url).identity_inventory(local()).await);
        assert_eq!(got.lock().unwrap().len(), 3);
    }
    // Different UUID letter case is the same identity, not a restart.
    let (server, _) = fixture(
        |n, _| {
            body(if n == 1 {
                inventory()
            } else {
                config(if n == 0 {
                    REMOTE
                } else {
                    "BBBBBBBB-2222-4222-8222-BBBBBBBBBBBB"
                })
            })
        },
        None,
    )
    .await;
    assert!(
        authority(&server.url)
            .identity_inventory(local())
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn complete_cursor_chain_precedes_second_probe_and_invalid_page_is_atomic() {
    for valid in [true, false] {
        let(server,got)=fixture(move|n,_|body(match n{0|3=>config(REMOTE),1=>json!({"streams":[{"name":"one","named_by":"user","groups":[]}],"next":"https://bad.invalid/a?b=1&c=2"}),_=>if valid{inventory()}else{json!({"streams":[{"name":"bad","groups":null}]})}}),None).await;
        let result = authority(&server.url).identity_inventory(local()).await;
        if valid {
            assert_eq!(
                result.unwrap().names().collect::<Vec<_>>(),
                ["Music %2F HD", "one"]
            );
        } else {
            invalid(result);
        }
        let captures = got.lock().unwrap();
        assert_eq!(captures.len(), if valid { 4 } else { 3 });
        assert_eq!(
            captures[2].0,
            "/-/streams?cursor=https%3A%2F%2Fbad.invalid%2Fa%3Fb%3D1%26c%3D2"
        );
    }
}
#[tokio::test]
async fn probes_and_inventory_share_one_byte_budget() {
    let config_base = config(REMOTE).to_string();
    let rows = inventory().to_string();
    for (extra, chunked) in [(0, false), (1, false), (1, true)] {
        let padded = json!({"stats":{"id":REMOTE},"pad":""}).to_string();
        let padding = 1024 * 1024 - config_base.len() - rows.len() - padded.len() + extra;
        let second = json!({"stats":{"id":REMOTE},"pad":"x".repeat(padding)}).to_string();
        let first = config_base.clone();
        let rows = rows.clone();
        let (server, got) = fixture(
            move |n, _| {
                let text = match n {
                    0 => first.clone(),
                    1 => rows.clone(),
                    _ => second.clone(),
                };
                if chunked {
                    Response::new(Body::from_stream(futures_util::stream::iter([Ok::<
                        _,
                        std::io::Error,
                    >(
                        bytes::Bytes::from(text),
                    )])))
                } else {
                    Response::new(Body::from(text))
                }
            },
            None,
        )
        .await;
        let result = authority(&server.url).identity_inventory(local()).await;
        if extra == 0 {
            assert!(result.is_ok());
        } else {
            invalid(result);
        }
        assert_eq!(got.lock().unwrap().len(), 3);
    }
}
#[tokio::test]
async fn probes_and_inventory_share_one_total_deadline() {
    let got = Arc::new(Mutex::new(0usize));
    let capture = got.clone();
    let router = Router::new().fallback(get(move || {
        let mut n = capture.lock().unwrap();
        let index = *n;
        *n += 1;
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            body(if index == 1 {
                inventory()
            } else {
                config(REMOTE)
            })
        }
    }));
    let server = Server::start(router, None).await;
    let began = Instant::now();
    let result = authority(&server.url).identity_inventory(local()).await;
    assert!(matches!(
        result,
        Err(BindingFailure::Inventory(LookupFailure::Unavailable))
    ));
    assert!(began.elapsed() < Duration::from_millis(1250));
    assert_eq!(*got.lock().unwrap(), 3);
}
#[tokio::test]
async fn failed_or_redirected_identity_probe_never_falls_back() {
    let (target, hits) = fixture(|_, _| body(config(REMOTE)), None).await;
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::PARTIAL_CONTENT,
        StatusCode::FOUND,
    ] {
        let location = target.url.clone();
        let (server, got) = fixture(
            move |_, _| {
                Response::builder()
                    .status(status)
                    .header("Location", &location)
                    .body(Body::from(config(REMOTE).to_string()))
                    .unwrap()
            },
            None,
        )
        .await;
        assert!(matches!(
            authority(&server.url).identity_inventory(local()).await,
            Err(BindingFailure::Inventory(LookupFailure::Unavailable))
        ));
        assert_eq!(got.lock().unwrap().len(), 1);
        assert!(hits.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn identity_probes_use_the_profiles_frozen_verified_management_tls() {
    let cert = tls_fixture::Certificates::new();
    let wrong = tls_fixture::Certificates::new();
    let (server, got) = fixture(
        |n, _| body(if n == 1 { inventory() } else { config(REMOTE) }),
        Some(&cert),
    )
    .await;
    let mut source = json!({"url":server.url.replacen("https","m4ss",1),"cluster_key":"owned:ключ","on_play":"true","flussonix_tls_ca":cert.ca});
    let a = SourceAuthority::new(&source, &json!([])).unwrap();
    std::fs::write(&cert.ca, b"replaced invalid CA").unwrap();
    assert!(a.identity_inventory(local()).await.is_ok());
    assert_eq!(got.lock().unwrap().len(), 3);
    source["flussonix_tls_ca"] = json!(wrong.ca);
    assert!(
        SourceAuthority::new(&source, &json!([]))
            .unwrap()
            .identity_inventory(local())
            .await
            .is_err()
    );
    assert_eq!(got.lock().unwrap().len(), 3);
}
#[tokio::test]
async fn source_changes_fence_held_identity_success_and_failure() {
    for phase in 0..3 {
        for failure in [false, true] {
            let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let gate = Arc::new(Mutex::new(Some((reached_tx, release_rx))));
            let capture = gate.clone();
            let index = Arc::new(Mutex::new(0usize));
            let counter = index.clone();
            let router = Router::new().fallback(get(move || {
                let mut n = counter.lock().unwrap();
                let i = *n;
                *n += 1;
                let gated = if i == phase {
                    capture.lock().unwrap().take()
                } else {
                    None
                };
                async move {
                    if let Some((tx, rx)) = gated {
                        let _ = tx.send(());
                        let _ = rx.await;
                    }
                    if i == phase && failure {
                        Response::builder().status(503).body(Body::empty()).unwrap()
                    } else {
                        body(if i == 1 { inventory() } else { config(REMOTE) })
                    }
                }
            }));
            let server = Server::start(router, None).await;
            let source = json!({"url":server.url.replacen("http","m4s",1),"cluster_key":"owned:ключ","on_play":"true"});
            let a = Arc::new(SourceAuthority::new(&source, &json!([])).unwrap());
            let other = a.clone();
            let task = tokio::spawn(async move { other.identity_inventory(local()).await });
            tokio::time::timeout(Duration::from_secs(2), reached_rx)
                .await
                .unwrap()
                .unwrap();
            a.replace(&source, &json!([])).unwrap();
            release_tx.send(()).unwrap();
            assert!(matches!(
                task.await.unwrap(),
                Err(BindingFailure::Projection(ProjectionFailure::Stale))
            ));
        }
    }
}
#[tokio::test]
async fn identity_candidates_cannot_cross_authorities_or_survive_removal() {
    let (server, _) = fixture(
        |_, path| {
            body(if path.ends_with("/config") {
                config(REMOTE)
            } else {
                inventory()
            })
        },
        None,
    )
    .await;
    let a = authority(&server.url);
    let b = authority(&server.url);
    let rows = a.identity_inventory(local()).await.unwrap();
    let candidate = rows.candidate("Music %2F HD").unwrap();
    assert!(a.at_identity_current(&candidate, |_, _| ()).is_ok());
    assert_eq!(
        b.at_identity_current(&candidate, |_, _| panic!("cross-authority publication")),
        Err(ProjectionFailure::Stale)
    );
    a.remove().unwrap();
    assert_eq!(
        a.at_identity_current(&candidate, |_, _| panic!("stale publication")),
        Err(ProjectionFailure::Stale)
    );
}
#[tokio::test]
async fn cancelling_identity_discovery_drops_the_probe_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![];
        loop {
            let mut b = [0];
            assert_eq!(socket.read(&mut b).await.unwrap(), 1);
            request.push(b[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        reached_tx.send(()).unwrap();
        let mut b = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), socket.read(&mut b))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let a = authority(&url);
    let mut fetch = Box::pin(a.identity_inventory(local()));
    tokio::select! {result=&mut fetch=>panic!("unexpected completed probe: {}",result.is_ok()),result=reached_rx=>result.unwrap()};
    drop(fetch);
    server.await.unwrap();
}
