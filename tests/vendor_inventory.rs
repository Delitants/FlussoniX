//! Independent HTTP fixtures for the opt-in inventory wire client; no vendor runtime.
use axum::{
    Router,
    body::Body,
    extract::RawQuery,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::source_directory::{
    LookupFailure,
    flussonic::{InventoryClient, StreamProvenance},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[path = "support/tls.rs"]
mod tls_fixture;

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
    async fn start(router: Router) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
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
fn body(value: Value) -> Response {
    Response::new(Body::from(value.to_string()))
}
async fn fixture(value: Value) -> Server {
    Server::start(Router::new().route(
        "/-/streams",
        get(move || {
            let value = value.clone();
            async move { body(value) }
        }),
    ))
    .await
}
fn independent_sha1(message: &str) -> String {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let mut child = Command::new("openssl")
        .args(["dgst", "-sha1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(message.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .last()
        .unwrap()
        .to_owned()
}

// Break caught: raw/native credentials, wrong hash/timestamp, cursor interpreted as a URL,
// discarded proxy prefix, or returning only the first page.
#[tokio::test]
async fn paginated_inventory_hashes_credentials_and_keeps_opaque_cursor_on_origin() {
    let captures = Arc::new(Mutex::new(Vec::new()));
    let got = captures.clone();
    let server = Server::start(Router::new().route("/gateway/-/streams", get(
        move |headers: HeaderMap, RawQuery(query): RawQuery| {
            got.lock().unwrap().push((headers, query.clone()));
            async move {
                if query.is_none() {
                    body(json!({"streams":[{"name":"region/news"}],"next":"https://other.invalid/?a=1&token=opaque +日本語"}))
                } else {
                    body(json!({"streams":[{"name":"region/space name","static":false,"disabled":true,"named_by":"remote"}],"next":null}))
                }
            }
        }
    ))).await;
    let client =
        InventoryClient::new(&format!("{}/gateway/", server.url), "synthetic:ключ", None).unwrap();
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let result = client.inventory().await;
    let finished = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    server.stop().await;
    let rows = result.unwrap();
    assert_eq!(
        rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        ["region/news", "region/space name"]
    );
    assert!(rows[0].is_static && !rows[0].disabled);
    assert_eq!(rows[0].named_by, None);
    assert!(!rows[1].is_static && rows[1].disabled);
    assert_eq!(rows[1].named_by, Some(StreamProvenance::Remote));
    let captures = captures.lock().unwrap();
    assert_eq!(captures.len(), 2);
    for (headers, _) in captures.iter() {
        assert!(!headers.contains_key("x-flussonix-peer"));
        assert!(!headers.contains_key("cluster-auth-key"));
        let authorization = headers["authorization"].to_str().unwrap();
        let (seconds, digest) = authorization
            .strip_prefix("Cluster ")
            .unwrap()
            .split_once(':')
            .unwrap();
        let timestamp: u64 = seconds.parse().unwrap();
        assert!((started..=finished).contains(&timestamp));
        assert_eq!(
            digest,
            independent_sha1(&format!("{seconds}:synthetic:ключ"))
        );
        assert!(!authorization.contains("synthetic"));
    }
    assert_eq!(captures[0].1, None);
    let pairs =
        url::form_urlencoded::parse(captures[1].1.as_ref().unwrap().as_bytes()).collect::<Vec<_>>();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].0, "cursor");
    assert_eq!(
        pairs[0].1,
        "https://other.invalid/?a=1&token=opaque +日本語"
    );
}

// Break caught: malformed later pages publish the earlier page or duplicate names escape
// validation because they are disabled/on-demand.
#[tokio::test]
async fn bad_later_page_and_cross_page_duplicates_fail_the_whole_inventory() {
    for later in [
        json!({"streams":[{"name":"one","static":false}]}),
        json!({"streams":[{"name":"one","disabled":true}]}),
        json!({"streams":[{"name":"../bad"}]}),
        json!({"streams":{}}),
    ] {
        let server = Server::start(Router::new().route(
            "/-/streams",
            get(move |RawQuery(query): RawQuery| {
                let later = later.clone();
                async move {
                    body(if query.is_none() {
                        json!({"streams":[{"name":"one"}],"next":"page2"})
                    } else {
                        later
                    })
                }
            }),
        ))
        .await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(result, Err(LookupFailure::Invalid));
    }
}

// Break caught: defaulting malformed boolean/provenance/name fields into valid inventory.
#[tokio::test]
async fn malformed_rows_and_envelopes_are_rejected() {
    for value in [
        json!([]),
        json!({}),
        json!({"streams":null}),
        json!({"streams":[null]}),
        json!({"streams":[{}]}),
        json!({"streams":[{"name":""}]}),
        json!({"streams":[{"name":"a//b"}]}),
        json!({"streams":[{"name":"a?token=x"}]}),
        json!({"streams":[{"name":"a\nb"}]}),
        json!({"streams":[{"name":"x".repeat(257)}]}),
        json!({"streams":[{"name":"one","static":null}]}),
        json!({"streams":[{"name":"one","static":"true"}]}),
        json!({"streams":[{"name":"one","disabled":0}]}),
        json!({"streams":[{"name":"one","disabled":null}]}),
        json!({"streams":[{"name":"one","named_by":"unknown-origin"}]}),
        json!({"streams":[{"name":"one","named_by":null}]}),
    ] {
        let server = fixture(value.clone()).await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(result, Err(LookupFailure::Invalid), "{value}");
    }
}

// Break caught: dropping on-demand/disabled rows or interpreting metadata as viewer policy.
#[tokio::test]
async fn inventory_preserves_static_disabled_and_provenance_facts() {
    let server = fixture(json!({"streams":[
        {"name":"日本語/番組","named_by":"config","on_play":{"url":"auth://deny"},"inputs":[{"url":"http://untrusted.invalid"}]},
        {"name":"user","named_by":"user","static":false},
        {"name":"external","named_by":"external","disabled":true},
        {"name":"relay","named_by":"remote"}
    ]})).await;
    let result = InventoryClient::new(&server.url, "owned-key", None)
        .unwrap()
        .inventory()
        .await;
    server.stop().await;
    let rows = result.unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter().map(|r| r.named_by).collect::<Vec<_>>(),
        [
            Some(StreamProvenance::Config),
            Some(StreamProvenance::User),
            Some(StreamProvenance::External),
            Some(StreamProvenance::Remote)
        ]
    );
    assert_eq!(rows[0].name, "日本語/番組");
    assert!(!rows[1].is_static && rows[2].disabled);
    assert!(!format!("{rows:?}").contains("untrusted.invalid"));
}

// Break caught: empty/non-string/oversized cursors treated as successful termination.
#[tokio::test]
async fn invalid_cursors_fail_before_another_request() {
    for next in [
        json!(""),
        json!(3),
        json!([]),
        json!({}),
        json!("x".repeat(4097)),
        json!("界".repeat(1366)),
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let server = Server::start(Router::new().route(
            "/-/streams",
            get(move || {
                hits.fetch_add(1, Ordering::SeqCst);
                let next = next.clone();
                async move { body(json!({"streams":[],"next":next})) }
            }),
        ))
        .await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(result, Err(LookupFailure::Invalid));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

// Break caught: cursor cycles spin until timeout or fetch a repeated page.
#[tokio::test]
async fn repeated_cursors_are_rejected_without_fetching_them_again() {
    let count = Arc::new(AtomicUsize::new(0));
    let hits = count.clone();
    let server = Server::start(Router::new().route(
        "/-/streams",
        get(move || {
            let call = hits.fetch_add(1, Ordering::SeqCst);
            async move { body(json!({"streams":[],"next":if call==1{"b"}else{"a"}})) }
        }),
    ))
    .await;
    let result = InventoryClient::new(&server.url, "owned-key", None)
        .unwrap()
        .inventory()
        .await;
    server.stop().await;
    assert_eq!(result, Err(LookupFailure::Invalid));
    assert_eq!(count.load(Ordering::SeqCst), 3);
}

// Break caught: only per-page bytes are bounded, or a missing Content-Length bypasses the cap.
#[tokio::test]
async fn response_bytes_are_bounded_across_pages_and_chunked_bodies() {
    for chunked in [false, true] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let server=Server::start(Router::new().route("/-/streams",get(move ||{
            let page=hits.fetch_add(1,Ordering::SeqCst);
            let value=json!({"streams":[],"padding":"x".repeat(600000),"next":if page==0{json!("b")}else{Value::Null}}).to_string();
            async move{
                let bytes=if chunked { Body::from_stream(futures_util::stream::iter(value.into_bytes().chunks(65536).map(|v|Ok::<_,std::io::Error>(bytes::Bytes::copy_from_slice(v))).collect::<Vec<_>>())) }else{Body::from(value)};
                Response::new(bytes)
            }
        }))).await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(result, Err(LookupFailure::Invalid));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}

// Break caught: a huge first page is read before its advertised length is checked.
#[tokio::test]
async fn oversized_content_length_is_rejected_before_reading_a_hung_body() {
    let server = Server::start(Router::new().route(
        "/-/streams",
        get(|| async {
            Response::builder()
                .header("content-length", "1048577")
                .body(Body::from_stream(futures_util::stream::pending::<
                    Result<bytes::Bytes, std::io::Error>,
                >()))
                .unwrap()
        }),
    ))
    .await;
    let start = Instant::now();
    let result = InventoryClient::new(&server.url, "owned-key", None)
        .unwrap()
        .inventory()
        .await;
    server.stop().await;
    assert_eq!(result, Err(LookupFailure::Invalid));
    assert!(start.elapsed() < Duration::from_millis(700));
}

// Break caught: row/page limits reset for each page or stop one page too early.
#[tokio::test]
async fn row_limit_is_aggregate_and_accepts_the_exact_boundary() {
    for total in [10000usize, 10001] {
        let server=Server::start(Router::new().route("/-/streams",get(move |RawQuery(q):RawQuery|async move{
            let (start,end,next)=if q.is_none(){(0,5000,json!("b"))}else{(5000,total,Value::Null)};
            body(json!({"streams":(start..end).map(|n|json!({"name":format!("s{n}")})).collect::<Vec<_>>(),"next":next}))
        }))).await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        if total == 10000 {
            assert_eq!(result.unwrap().len(), 10000)
        } else {
            assert_eq!(result, Err(LookupFailure::Invalid))
        }
    }
}

#[tokio::test]
async fn page_limit_accepts_a_complete_chain_and_rejects_an_unfinished_chain() {
    for terminal in [true, false] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let server=Server::start(Router::new().route("/-/streams",get(move ||{
            let page=hits.fetch_add(1,Ordering::SeqCst)+1;
            async move{body(json!({"streams":[],"next":if terminal&&page==100{Value::Null}else{json!(page.to_string())}}))}
        }))).await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(count.load(Ordering::SeqCst), 100);
        assert_eq!(
            result,
            if terminal {
                Ok(vec![])
            } else {
                Err(LookupFailure::Invalid)
            }
        );
    }
}

// Break caught: resetting the 750 ms deadline for each page or only timing response headers.
#[tokio::test]
async fn one_deadline_covers_pagination_and_body_reads() {
    for hung_body in [false, true] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let server = Server::start(Router::new().route(
            "/-/streams",
            get(move |RawQuery(query): RawQuery| {
                hits.fetch_add(1, Ordering::SeqCst);
                async move {
                    if query.is_none() {
                        tokio::time::sleep(Duration::from_millis(450)).await;
                        body(json!({"streams":[{"name":"one"}],"next":"b"}))
                    } else if hung_body {
                        Response::new(Body::from_stream(futures_util::stream::pending::<
                            Result<bytes::Bytes, std::io::Error>,
                        >()))
                    } else {
                        tokio::time::sleep(Duration::from_millis(450)).await;
                        body(json!({"streams":[]}))
                    }
                }
            }),
        ))
        .await;
        let client = InventoryClient::new(&server.url, "owned-key", None).unwrap();
        let start = Instant::now();
        let result = client.inventory().await;
        server.stop().await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(result, Err(LookupFailure::Unavailable));
        assert!(start.elapsed() < Duration::from_millis(1200));
    }
}

// Break caught: following either same-origin or cross-origin redirects with the credential.
#[tokio::test]
async fn redirects_never_forward_credentials_or_probe_a_native_endpoint() {
    for same_origin in [false, true] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let target = Server::start(Router::new().fallback(get(move || {
            hits.fetch_add(1, Ordering::SeqCst);
            async { body(json!({"streams":[]})) }
        })))
        .await;
        let location = if same_origin {
            "/redirected".to_owned()
        } else {
            target.url.clone()
        };
        let source_count = Arc::new(AtomicUsize::new(0));
        let source_hits = source_count.clone();
        let initial_count = Arc::new(AtomicUsize::new(0));
        let initial_hits = initial_count.clone();
        let server = Server::start(
            Router::new()
                .route(
                    "/-/streams",
                    get(move || {
                        initial_hits.fetch_add(1, Ordering::SeqCst);
                        let location = location.clone();
                        async move {
                            Response::builder()
                                .status(307)
                                .header("location", location)
                                .body(Body::empty())
                                .unwrap()
                        }
                    }),
                )
                .fallback(get(move || {
                    source_hits.fetch_add(1, Ordering::SeqCst);
                    async { body(json!({"streams":[]})) }
                })),
        )
        .await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        target.stop().await;
        assert_eq!(result, Err(LookupFailure::Unavailable));
        assert_eq!(initial_count.load(Ordering::SeqCst), 1);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(source_count.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn http_errors_fail_without_credential_fallback() {
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::INTERNAL_SERVER_ERROR,
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let server = Server::start(Router::new().fallback(get(move || {
            hits.fetch_add(1, Ordering::SeqCst);
            async move { status }
        })))
        .await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(result, Err(LookupFailure::Unavailable));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

// Break caught: treating a partial-content or asynchronous response as a complete
// inventory just because its body happens to be valid JSON.
#[tokio::test]
async fn inventory_requires_a_complete_http_200_response() {
    for status in [StatusCode::ACCEPTED, StatusCode::PARTIAL_CONTENT] {
        let server = Server::start(Router::new().route(
            "/-/streams",
            get(move || async move {
                Response::builder()
                    .status(status)
                    .body(Body::from("{\"streams\":[{\"name\":\"partial\"}]}"))
                    .unwrap()
            }),
        ))
        .await;
        let result = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        assert_eq!(result, Err(LookupFailure::Unavailable));
    }
}

// Break caught: credentials/query carried in a URL or invalid trust silently downgraded.
#[test]
fn endpoint_credentials_queries_fragments_and_invalid_keys_are_rejected() {
    for endpoint in [
        "ftp://host",
        "http://user:secret@host",
        "http://host?token=x",
        "http://host?",
        "http://host#fragment",
        "http://host#",
        "http://host:0",
        "http://host\\else",
        " http://host",
        "http://host/\npath",
        "",
    ] {
        assert_eq!(
            InventoryClient::new(endpoint, "owned-key", None).err(),
            Some(LookupFailure::Invalid),
            "{endpoint}"
        );
    }
    for key in ["".to_owned(), "x".repeat(4097)] {
        assert_eq!(
            InventoryClient::new("http://host", &key, None).err(),
            Some(LookupFailure::Invalid)
        );
    }
    assert_eq!(
        InventoryClient::new(
            "http://host",
            "owned-key",
            Some(std::path::Path::new("/missing-ca.pem"))
        )
        .err(),
        Some(LookupFailure::Invalid)
    );
    assert_eq!(
        InventoryClient::new(
            "https://host",
            "owned-key",
            Some(std::path::Path::new("relative-ca.pem"))
        )
        .err(),
        Some(LookupFailure::Invalid)
    );
}

// Break caught: ignoring custom CA files, accepting an untrusted certificate, or
// bypassing hostname/expiry validation before sending a privileged HTTP credential.
#[tokio::test]
async fn https_inventory_verifies_custom_trust_identity_and_expiry() {
    use flussonix::http_tls::Listener;
    use std::process::Command;
    let cert = tls_fixture::Certificates::new();
    for profile in ["valid", "wrong_name", "expired"] {
        if profile == "wrong_name" {
            std::fs::write(cert.dir.path().join("wrong-extensions"),"subjectAltName=DNS:wrong.invalid\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
            let output = Command::new("openssl")
                .current_dir(cert.dir.path())
                .args([
                    "x509",
                    "-req",
                    "-in",
                    "server.csr",
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca.key",
                    "-CAcreateserial",
                    "-out",
                    "server.pem",
                    "-days",
                    "1",
                    "-extfile",
                    "wrong-extensions",
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "owned wrong-name certificate creation failed"
            );
        } else if profile == "expired" {
            cert.expire();
        }
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", tcp.local_addr().unwrap());
        let router = Router::new().route(
            "/-/streams",
            get(move || {
                hits.fetch_add(1, Ordering::SeqCst);
                async { body(json!({"streams":[{"name":"private"}]})) }
            }),
        );
        let tls = cert.server();
        let task =
            tokio::spawn(
                async move { axum::serve(Listener::new(tcp, tls), router).await.unwrap() },
            );
        // Keep ownership of the certificate bundle outside the server task.
        let server = Server {
            url,
            task: Some(task),
        };
        let client = InventoryClient::new(&server.url, "owned-key", Some(&cert.ca)).unwrap();
        let result = client.inventory().await;
        let untrusted = InventoryClient::new(&server.url, "owned-key", None)
            .unwrap()
            .inventory()
            .await;
        server.stop().await;
        if profile == "valid" {
            assert_eq!(result.unwrap()[0].name, "private");
            assert_eq!(count.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(result, Err(LookupFailure::Unavailable));
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }
        assert_eq!(untrusted, Err(LookupFailure::Unavailable));
    }
}
