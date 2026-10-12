//! Real owned HTTP/TLS fixtures for the explicit modern discovery profile.
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
    flussonic::{
        EligibilityFailure, StreamProvenance,
        modern::{ModernInventoryClient, ObservedServerId},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
#[path = "support/tls.rs"]
mod tls_fixture;
const LOCAL: &str = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa";
const REMOTE: &str = "cdn-custom-q01\n";
type Captures = Arc<Mutex<Vec<(String, HeaderMap)>>>;
struct Server {
    url: String,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
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
        Self {
            url,
            task: Some(task),
        }
    }
    async fn stop(mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        let _ = task.await;
    }
}
fn response(value: Value) -> Response {
    Response::new(Body::from(value.to_string()))
}
fn config(id: &str) -> Value {
    json!({"stats":{"id":id},"cluster_key":"untrusted-response-secret"})
}
fn page(id: &str, rows: Value, next: Value) -> Value {
    json!({"server_id":id,"streams":rows,"next":next})
}
fn row(name: &str) -> Value {
    json!({"name":name,"named_by":"config","groups":[]})
}
fn local() -> ObservedServerId {
    ObservedServerId::parse(LOCAL).unwrap()
}
fn client(url: &str, budget: Duration) -> ModernInventoryClient {
    ModernInventoryClient::new(url, "owned:ключ", None, budget).unwrap()
}
async fn fixture<F>(f: F) -> (Server, Captures)
where
    F: Fn(usize, &str) -> Response + Send + Sync + 'static,
{
    let captures: Captures = Arc::new(Mutex::new(vec![]));
    let got = captures.clone();
    let f = Arc::new(f);
    let router = Router::new().fallback(get(
        move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
            let mut got = got.lock().unwrap();
            let n = got.len();
            got.push((uri.to_string(), headers));
            let result = f(n, uri.path());
            async move { result }
        },
    ));
    (Server::start(router, None).await, captures)
}
fn assert_invalid<T>(r: Result<T, LookupFailure>) {
    assert!(matches!(r, Err(LookupFailure::Invalid)));
}
fn verify_auth(headers: &HeaderMap) {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let (timestamp, digest) = headers["authorization"]
        .to_str()
        .unwrap()
        .strip_prefix("Cluster ")
        .unwrap()
        .split_once(':')
        .unwrap();
    assert!(timestamp.parse::<u64>().is_ok());
    let mut p = Command::new("openssl")
        .args(["dgst", "-sha1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    p.stdin
        .take()
        .unwrap()
        .write_all(format!("{timestamp}:owned:ключ").as_bytes())
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
    assert!(!headers["authorization"].to_str().unwrap().contains("owned"));
}

// Catches endpoint fallback, discarded proxy prefix, credential mistakes and cursor navigation.
#[tokio::test]
async fn complete_custom_id_discovery_keeps_fixed_origin_selected_fields_and_credentials() {
    let (s, got) = fixture(|n, path| {
        if path.ends_with("/config") {
            response(config(REMOTE))
        } else {
            assert_eq!(path, "/proxy/streamer/api/v3/streams");
            response(if n == 1 {
                page(
                    REMOTE,
                    json!([row("Music %2F HD")]),
                    json!("https://other.invalid/?token=opaque +日本語"),
                )
            } else {
                page(REMOTE, json!([row("日本語/番組")]), Value::Null)
            })
        }
    })
    .await;
    let result = client(&format!("{}/proxy/", s.url), Duration::from_secs(2))
        .inventory(&local())
        .await
        .unwrap();
    s.stop().await;
    assert_eq!(result.server_id().as_str(), REMOTE);
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["Music %2F HD", "日本語/番組"]
    );
    let got = got.lock().unwrap();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0].0, "/proxy/streamer/api/v3/config");
    assert_eq!(got[3].0, "/proxy/streamer/api/v3/config");
    for (_, h) in got.iter() {
        verify_auth(h);
    }
    for (index, query) in [
        got[1].0.split_once('?').unwrap().1,
        got[2].0.split_once('?').unwrap().1,
    ]
    .into_iter()
    .enumerate()
    {
        let pairs = url::form_urlencoded::parse(query.as_bytes())
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            pairs.get("select").unwrap(),
            "name,named_by,groups,static,disabled"
        );
        assert_eq!(pairs.get("limit").unwrap(), "100");
        if index == 0 {
            assert!(!pairs.contains_key("cursor"));
        } else {
            assert_eq!(
                pairs.get("cursor").unwrap(),
                "https://other.invalid/?token=opaque +日本語"
            );
        }
    }
}

// Catches inventing missing policy/origin facts or coercing relays into local candidates.
#[tokio::test]
async fn discovery_retains_relay_and_unknown_group_facts_without_importing_remote_policy() {
    let (s,_)=fixture(|_,path|response(if path.ends_with("/config"){config(REMOTE)}else{page(REMOTE,json!([
  {"name":"relay","named_by":"remote","on_play":"true","inputs":[{"url":"http://secret.invalid"}]},
  {"name":"unknown","named_by":"config"},
  {"name":"explicit","named_by":"user","groups":[],"static":false},
  {"name":"grouped","named_by":"config","groups":["日本語","News HD","percent%2Fgroup"]}
 ]),Value::Null)})).await;
    let result = client(&s.url, Duration::from_secs(2))
        .inventory(&local())
        .await
        .unwrap();
    s.stop().await;
    let rows = result.rows();
    assert_eq!(rows[0].named_by, Some(StreamProvenance::Remote));
    assert_eq!(
        rows[0].local_origin_candidate(),
        Err(EligibilityFailure::Relayed)
    );
    assert_eq!(rows[1].groups, None);
    assert_eq!(
        rows[1].local_origin_candidate(),
        Err(EligibilityFailure::UnknownGroups)
    );
    assert!(rows[2].local_origin_candidate().is_ok());
    assert!(!rows[2].is_static);
    assert_eq!(
        rows[3].groups.as_deref(),
        Some(
            ["日本語", "News HD", "percent%2Fgroup"]
                .map(str::to_owned)
                .as_slice()
        )
    );
    assert!(!format!("{rows:?}").contains("secret.invalid"));
}

// Catches treating custom IDs as UUIDs, collapsing unrelated custom IDs or character-count bounds.
#[test]
fn opaque_identity_preserves_exact_custom_bytes_and_enforces_bounded_nonempty_values() {
    for text in [
        REMOTE,
        "cdn-custom-q01",
        "CDN-custom-q01\n",
        "cdn custom q01\r\n",
        &"x".repeat(256),
        &format!("{}x", "界".repeat(85)),
    ] {
        let id = ObservedServerId::parse(text).unwrap();
        assert_eq!(id.as_str(), text);
    }
    assert!(
        ObservedServerId::parse(REMOTE).unwrap()
            != ObservedServerId::parse("cdn-custom-q01").unwrap()
    );
    for text in [
        "",
        " ",
        "\r\n",
        "nul\0id",
        "bad\tid",
        "bad\nid",
        &"x".repeat(257),
        &"界".repeat(86),
    ] {
        assert_invalid(ObservedServerId::parse(text));
    }
}

// Catches native self-ID bypass using UUID spelling/newline aliases or a nil UUID.
#[tokio::test]
async fn uuid_aliases_remain_self_and_nil_identity_never_becomes_opaque() {
    for text in [
        LOCAL.to_uppercase(),
        format!("{LOCAL}\n"),
        format!("{{{LOCAL}}}"),
        format!("urn:uuid:{LOCAL}"),
        LOCAL.replace('-', ""),
    ] {
        assert!(ObservedServerId::parse(&text).unwrap() == local());
        let text = text.clone();
        let (s, got) = fixture(move |_, _| response(config(&text))).await;
        assert_invalid(
            client(&s.url, Duration::from_secs(2))
                .inventory(&local())
                .await,
        );
        s.stop().await;
        assert_eq!(got.lock().unwrap().len(), 1);
    }
    for text in [
        "00000000-0000-0000-0000-000000000000",
        "00000000000000000000000000000000",
        "00000000-0000-0000-0000-000000000000\n",
    ] {
        assert_invalid(ObservedServerId::parse(text));
    }
}

// Catches issuing inventory requests after an exact custom self claim.
#[tokio::test]
async fn custom_self_claim_stops_before_inventory() {
    let (s, got) = fixture(|_, _| response(config(REMOTE))).await;
    assert_invalid(
        client(&s.url, Duration::from_secs(2))
            .inventory(&ObservedServerId::parse(REMOTE).unwrap())
            .await,
    );
    s.stop().await;
    assert_eq!(got.lock().unwrap().len(), 1);
}

// Catches accepting pages from a different observed server or returning earlier rows.
#[tokio::test]
async fn page_identity_drift_rejects_entire_chain_before_final_probe() {
    let (s, got) = fixture(|n, _| {
        response(match n {
            0 => config(REMOTE),
            1 => page(REMOTE, json!([row("first")]), json!("next")),
            _ => page("other", json!([row("second")]), Value::Null),
        })
    })
    .await;
    assert_invalid(
        client(&s.url, Duration::from_secs(2))
            .inventory(&local())
            .await,
    );
    s.stop().await;
    assert_eq!(got.lock().unwrap().len(), 3);
}

// Catches skipping the last identity probe or normalizing custom IDs across a refresh.
#[tokio::test]
async fn final_config_identity_drift_rejects_complete_rows() {
    let (s, got) = fixture(|n, _| {
        response(match n {
            0 => config(REMOTE),
            1 => page(REMOTE, json!([row("one")]), Value::Null),
            _ => config("cdn-custom-q01"),
        })
    })
    .await;
    assert_invalid(
        client(&s.url, Duration::from_secs(2))
            .inventory(&local())
            .await,
    );
    s.stop().await;
    assert_eq!(got.lock().unwrap().len(), 3);
}

// Catches inventing missing/invalid IDs or accepting malformed successful management bodies.
#[tokio::test]
async fn malformed_config_and_page_identities_are_not_discovery() {
    for value in [
        json!({}),
        json!({"stats":null}),
        json!({"stats":{"id":1}}),
        config(""),
        config("\r\n"),
        config("00000000-0000-0000-0000-000000000000"),
        config(&"x".repeat(257)),
    ] {
        let (s, got) = fixture(move |_, _| response(value.clone())).await;
        assert_invalid(
            client(&s.url, Duration::from_secs(2))
                .inventory(&local())
                .await,
        );
        s.stop().await;
        assert_eq!(got.lock().unwrap().len(), 1);
    }
    for value in [
        json!({"streams":[]}),
        json!({"server_id":null,"streams":[]}),
        json!({"server_id":3,"streams":[]}),
        page("other", json!([]), Value::Null),
    ] {
        let (s, _) = fixture(move |n, _| {
            response(if n == 0 {
                config(REMOTE)
            } else {
                value.clone()
            })
        })
        .await;
        assert_invalid(
            client(&s.url, Duration::from_secs(2))
                .inventory(&local())
                .await,
        );
        s.stop().await;
    }
}

// Catches partial publication, duplicate names and relaxed existing row validation.
#[tokio::test]
async fn malformed_later_rows_and_duplicate_names_reject_the_entire_inventory() {
    for later in [
        json!([row("first")]),
        json!([{"name":"../bad"}]),
        json!([{"name":"later","groups":["g","g"]}]),
        json!([{"name":"later","named_by":"invented"}]),
        json!([{"name":"later","disabled":null}]),
        json!([{"name":"later","static":"yes"}]),
        json!([{"name":"later","groups":null}]),
        json!({}),
        json!([null]),
    ] {
        let (s, _) = fixture(move |n, _| {
            response(match n {
                0 => config(REMOTE),
                1 => page(REMOTE, json!([row("first")]), json!("b")),
                _ => page(REMOTE, later.clone(), Value::Null),
            })
        })
        .await;
        assert_invalid(
            client(&s.url, Duration::from_secs(2))
                .inventory(&local())
                .await,
        );
        s.stop().await;
    }
}

// Catches infinite pagination, malformed cursors and navigation to a cursor URL.
#[tokio::test]
async fn repeated_and_malformed_cursors_fail_without_endpoint_fallback() {
    for next in [
        json!(""),
        json!(false),
        json!({"url":"http://other.invalid"}),
        json!("x".repeat(4097)),
        json!("repeat"),
    ] {
        let (s, got) = fixture(move |n, _| {
            response(if n == 0 {
                config(REMOTE)
            } else {
                page(REMOTE, json!([row(&format!("s{n}"))]), next.clone())
            })
        })
        .await;
        assert_invalid(
            client(&s.url, Duration::from_secs(2))
                .inventory(&local())
                .await,
        );
        s.stop().await;
        assert!(got.lock().unwrap().len() <= 3);
    }
}

// Catches carrying the legacy750ms budget into an explicitly longer background refresh.
#[tokio::test]
async fn explicit_background_budget_allows_a_complete_refresh_longer_than_legacy_deadline() {
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    let router = Router::new().fallback(get(move || {
        let index = n.fetch_add(1, Ordering::SeqCst);
        async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            response(if index == 0 || index == 3 {
                config(REMOTE)
            } else {
                page(
                    REMOTE,
                    json!([row(&format!("s{index}"))]),
                    if index == 1 { json!("b") } else { Value::Null },
                )
            })
        }
    }));
    let s = Server::start(router, None).await;
    let result = client(&s.url, Duration::from_secs(3))
        .inventory(&local())
        .await
        .unwrap();
    s.stop().await;
    assert_eq!(result.rows().len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

// Catches resetting a whole-refresh deadline for each successful request.
#[tokio::test]
async fn entire_refresh_shares_one_deadline_including_identity_probes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    let router = Router::new().fallback(get(move || {
        let index = n.fetch_add(1, Ordering::SeqCst);
        async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            response(if index == 0 || index == 3 {
                config(REMOTE)
            } else {
                page(
                    REMOTE,
                    json!([row(&format!("s{index}"))]),
                    if index == 1 { json!("b") } else { Value::Null },
                )
            })
        }
    }));
    let s = Server::start(router, None).await;
    let start = Instant::now();
    assert!(matches!(
        client(&s.url, Duration::from_millis(180))
            .inventory(&local())
            .await,
        Err(LookupFailure::Unavailable)
    ));
    s.stop().await;
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(calls.load(Ordering::SeqCst) < 4);
}

// Catches allowing one stalled request to consume the whole long refresh budget.
#[tokio::test]
async fn stalled_request_is_bounded_independently_of_the_whole_refresh() {
    let s = Server::start(
        Router::new().fallback(get(|| async {
            tokio::time::sleep(Duration::from_secs(9)).await;
            response(config(REMOTE))
        })),
        None,
    )
    .await;
    let start = Instant::now();
    assert!(matches!(
        client(&s.url, Duration::from_secs(12))
            .inventory(&local())
            .await,
        Err(LookupFailure::Unavailable)
    ));
    s.stop().await;
    assert!(start.elapsed() < Duration::from_secs(8));
}

// Catches resetting bytes between config/pages/config or failing exact budget boundaries.
#[tokio::test]
async fn complete_refresh_shares_one_byte_budget_including_both_identity_probes() {
    for extra in [0usize, 1] {
        let c = config(REMOTE).to_string();
        let mut p = page(REMOTE, json!([row("one")]), Value::Null);
        p["padding"] = json!("");
        let base = p.to_string().len();
        p["padding"] = json!("x".repeat(1_048_576 - c.len() * 2 - base + extra));
        let wire = p.to_string();
        let (s, got) = fixture(move |_, path| {
            if path.ends_with("/config") {
                response(config(REMOTE))
            } else {
                Response::new(Body::from(wire.clone()))
            }
        })
        .await;
        let result = client(&s.url, Duration::from_secs(3))
            .inventory(&local())
            .await;
        s.stop().await;
        if extra == 0 {
            assert_eq!(result.unwrap().rows().len(), 1);
            assert_eq!(got.lock().unwrap().len(), 3);
        } else {
            assert_invalid(result);
        }
    }
}

// Catches unbounded pages even when each page and every stream is individually valid.
#[tokio::test]
async fn page_budget_never_publishes_a_truncated_chain() {
    let (s, got) = fixture(|n, _| {
        response(if n == 0 {
            config(REMOTE)
        } else {
            page(
                REMOTE,
                json!([row(&format!("s{n}"))]),
                json!(format!("cursor{n}")),
            )
        })
    })
    .await;
    assert_invalid(
        client(&s.url, Duration::from_secs(10))
            .inventory(&local())
            .await,
    );
    s.stop().await;
    assert_eq!(got.lock().unwrap().len(), 101);
}

// Catches off-by-one row limits or resetting row counts on subsequent pages.
#[tokio::test]
async fn row_budget_accepts_exact_boundary_and_rejects_one_more() {
    for total in [10000usize, 10001] {
        let (s, _) = fixture(move |n, _| {
            response(if n == 0 || n == 3 {
                config(REMOTE)
            } else {
                let (start, end, next) = if n == 1 {
                    (0, 5000, json!("b"))
                } else {
                    (5000, total, Value::Null)
                };
                page(
                    REMOTE,
                    json!(
                        (start..end)
                            .map(|i| json!({"name":format!("s{i}")}))
                            .collect::<Vec<_>>()
                    ),
                    next,
                )
            })
        })
        .await;
        let result = client(&s.url, Duration::from_secs(5))
            .inventory(&local())
            .await;
        s.stop().await;
        if total == 10000 {
            assert_eq!(result.unwrap().rows().len(), 10000);
        } else {
            assert_invalid(result);
        }
    }
}

// Catches group-budget reset between pages and broadening per-row membership validation.
#[tokio::test]
async fn membership_budget_and_per_row_limits_are_preserved() {
    for total in [10000usize, 10001] {
        let (s,_)=fixture(move |n,_|response(if n==0||n==3 {config(REMOTE)}else{let (start,end,next)=if n==1{(0,80,json!("b"))}else{(80,total.div_ceil(64),Value::Null)};page(REMOTE,json!((start..end).map(|i|json!({"name":format!("s{i}"),"groups":(0..64.min(total-i*64)).map(|g|format!("g{g}")).collect::<Vec<_>>()})).collect::<Vec<_>>()),next)})).await;
        let result = client(&s.url, Duration::from_secs(3))
            .inventory(&local())
            .await;
        s.stop().await;
        if total == 10000 {
            assert_eq!(
                result
                    .unwrap()
                    .rows()
                    .iter()
                    .map(|r| r.groups.as_ref().unwrap().len())
                    .sum::<usize>(),
                10000
            );
        } else {
            assert_invalid(result);
        }
    }
    for groups in [
        json!((0..65).map(|i| format!("g{i}")).collect::<Vec<_>>()),
        json!(["x".repeat(257)]),
        json!(["same", "same"]),
    ] {
        let (s, _) = fixture(move |n, _| {
            response(if n == 0 {
                config(REMOTE)
            } else {
                page(REMOTE, json!([{"name":"one","groups":groups}]), Value::Null)
            })
        })
        .await;
        assert_invalid(
            client(&s.url, Duration::from_secs(2))
                .inventory(&local())
                .await,
        );
        s.stop().await;
    }
}

// Catches forwarding a key to redirects or using legacy endpoints after denial/failure.
#[tokio::test]
async fn management_denials_and_redirects_never_fallback_or_forward_credentials() {
    let redirected = Arc::new(AtomicUsize::new(0));
    let hits = redirected.clone();
    let target = Server::start(
        Router::new().fallback(get(move || {
            hits.fetch_add(1, Ordering::SeqCst);
            async { response(config(REMOTE)) }
        })),
        None,
    )
    .await;
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::FOUND,
        StatusCode::TEMPORARY_REDIRECT,
    ] {
        for failing in [0usize, 1] {
            let location = target.url.clone();
            let (s, got) = fixture(move |n, _| {
                if n == failing {
                    let mut r = Response::new(Body::from("upstream-secret"));
                    *r.status_mut() = status;
                    r.headers_mut()
                        .insert("location", location.parse().unwrap());
                    r
                } else {
                    response(config(REMOTE))
                }
            })
            .await;
            assert!(matches!(
                client(&s.url, Duration::from_secs(2))
                    .inventory(&local())
                    .await,
                Err(LookupFailure::Unavailable)
            ));
            s.stop().await;
            assert_eq!(got.lock().unwrap().len(), failing + 1);
        }
    }
    target.stop().await;
    assert_eq!(redirected.load(Ordering::SeqCst), 0);
}

// Catches sending a privileged key before TLS trust/identity validation.
#[tokio::test]
async fn modern_discovery_requires_verified_tls_and_captured_custom_trust() {
    let cert = tls_fixture::Certificates::new();
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
        count.fetch_add(1, Ordering::SeqCst);
        async move {
            response(if uri.path().ends_with("/config") {
                config(REMOTE)
            } else {
                page(REMOTE, json!([row("one")]), Value::Null)
            })
        }
    }));
    let s = Server::start(router, Some(&cert)).await;
    let trusted =
        ModernInventoryClient::new(&s.url, "owned:ключ", Some(&cert.ca), Duration::from_secs(3))
            .unwrap();
    std::fs::write(&cert.ca, "changed after preparation").unwrap();
    assert_eq!(trusted.inventory(&local()).await.unwrap().rows().len(), 1);
    assert!(matches!(
        client(&s.url, Duration::from_secs(2))
            .inventory(&local())
            .await,
        Err(LookupFailure::Unavailable)
    ));
    s.stop().await;
    assert_eq!(hits.load(Ordering::SeqCst), 3);
}

// Catches detached pagination continuing after caller cancellation.
#[tokio::test]
async fn dropping_refresh_closes_the_pending_request_without_background_pagination() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn read_headers(stream: &mut tokio::net::TcpStream, buffer: &mut [u8]) {
        let mut bytes = Vec::new();
        loop {
            let n = stream.read(buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
            assert!(bytes.len() <= 16384);
            if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                break;
            }
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (entered, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut b = [0; 4096];
        read_headers(&mut first, &mut b).await;
        let body = config(REMOTE).to_string();
        first
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        read_headers(&mut second, &mut b).await;
        entered.send(()).unwrap();
        assert_eq!(second.read(&mut b).await.unwrap(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let client = client(&url, Duration::from_secs(3));
    let id = local();
    let mut refresh = Box::pin(client.inventory(&id));
    tokio::select! {r=&mut refresh=>panic!("refresh unexpectedly completed: {}",r.is_ok()),r=rx=>r.unwrap()};
    drop(refresh);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

// Catches unbounded budgets or invalid origins/key/CA configuration before any I/O.
#[test]
fn modern_profile_rejects_invalid_budgets_origins_and_credentials() {
    for budget in [Duration::ZERO, Duration::from_secs(31), Duration::MAX] {
        assert_invalid(ModernInventoryClient::new(
            "https://example.invalid",
            "key",
            None,
            budget,
        ));
    }
    for base in [
        "m4s://example.invalid",
        "http://user:password@example.invalid",
        "http://example.invalid/?token=x",
        "http://example.invalid/#x",
        "http://example.invalid:0",
        "http://example.invalid/\\bad",
        "http://example.invalid/white space",
    ] {
        assert_invalid(ModernInventoryClient::new(
            base,
            "key",
            None,
            Duration::from_secs(2),
        ));
    }
    for key in ["".to_owned(), "x".repeat(4097)] {
        assert_invalid(ModernInventoryClient::new(
            "https://example.invalid",
            &key,
            None,
            Duration::from_secs(2),
        ));
    }
    assert_invalid(ModernInventoryClient::new(
        "http://example.invalid",
        "key",
        Some(std::path::Path::new("/missing-ca")),
        Duration::from_secs(2),
    ));
}
