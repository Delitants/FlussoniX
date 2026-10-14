#[path = "support/tls.rs"]
mod tls_fixture;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing::get,
};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tower::ServiceExt;

type Hold = Arc<
    Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;
#[derive(Clone)]
struct StateData {
    hold: Hold,
    config_calls: Arc<AtomicU64>,
    delay_ms: Arc<AtomicU64>,
    flip: Arc<std::sync::atomic::AtomicBool>,
    calls: Arc<AtomicU64>,
    config: Arc<Mutex<Value>>,
    stream: Arc<Mutex<Value>>,
}
struct Vendor {
    url: String,
    state: StateData,
    task: tokio::task::JoinHandle<()>,
}
impl Vendor {
    async fn new() -> Self {
        let state = StateData {
            hold: Arc::new(Mutex::new(None)),
            config_calls: Arc::new(AtomicU64::new(0)),
            delay_ms: Arc::new(AtomicU64::new(0)),
            flip: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            calls: Arc::new(AtomicU64::new(0)),
            config: Arc::new(Mutex::new(
                json!({"stats":{"id":"owned-vendor-server","cpu_usage":10,"memory_usage":20,"output_kbit":10000,"total_bandwidth":100000000,"total_clients":1,"streamer_status":"running"}}),
            )),
            stream: Arc::new(Mutex::new(
                json!({"name":"remote/news","named_by":"remote","disabled":false,"stats":{"running":true,"bitrate":4000}}),
            )),
        };
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        async fn check(s: &StateData, h: &HeaderMap) {
            s.calls.fetch_add(1, Ordering::SeqCst);
            assert!(!h.contains_key("x-flussonix-peer"));
            let a = h["authorization"]
                .to_str()
                .unwrap()
                .strip_prefix("Cluster ")
                .unwrap();
            let (stamp, digest) = a.split_once(':').unwrap();
            let want = ring::digest::digest(
                &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
                format!("{stamp}:owned-vendor-cluster-key").as_bytes(),
            )
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
            assert_eq!(digest, want);
        }
        let service = Router::new()
            .route(
                "/streamer/api/v3/config",
                get(|State(s): State<StateData>, h: HeaderMap| async move {
                    check(&s, &h).await;
                    let held = s.hold.lock().unwrap().take();
                    if let Some((entered, release)) = held {
                        let _ = entered.send(());
                        let _ = release.await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        s.delay_ms.load(Ordering::SeqCst),
                    ))
                    .await;
                    let turn = s.config_calls.fetch_add(1, Ordering::SeqCst);
                    let mut v = s.config.lock().unwrap().clone();
                    v["stats"]["now"] = json!(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
                    );
                    if s.flip.load(Ordering::SeqCst) && turn % 2 == 1 {
                        v["stats"]["id"] = json!("other-vendor");
                    }
                    axum::Json(v)
                }),
            )
            .route(
                "/streamer/api/v3/streams/{name}",
                get(|State(s): State<StateData>, h: HeaderMap| async move {
                    check(&s, &h).await;
                    axum::Json(s.stream.lock().unwrap().clone())
                }),
            )
            .route(
                "/remote/news/mpegts",
                get(
                    |headers: HeaderMap, uri: axum::extract::OriginalUri| async move {
                        assert!(!headers.contains_key("authorization"));
                        assert!(!headers.contains_key("x-flussonix-peer"));
                        let allowed =
                            url::form_urlencoded::parse(uri.0.query().unwrap_or("").as_bytes())
                                .any(|(k, v)| k == "token" && v == "allowed-on-cdn");
                        if allowed {
                            (StatusCode::OK, "owned viewer accepted")
                        } else {
                            (StatusCode::FORBIDDEN, "CDN viewer denied")
                        }
                    },
                ),
            )
            .fallback(|| async {
                panic!("vendor must not receive native admission or unexpected media request");
                #[allow(unreachable_code)]
                StatusCode::INTERNAL_SERVER_ERROR
            })
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        Self { url, state, task }
    }
    fn peer(&self) -> Value {
        json!({"api_url":self.url,"public_payload_url":"https://edge.example/base","cluster_key":"owned-vendor-cluster-key","flussonix_peer_kind":"flussonic","flussonix_stream_map":[{"name":"local/news","remote_name":"remote/news"}]})
    }
}
impl Drop for Vendor {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn app() -> (tempfile::TempDir, Arc<App>) {
    let d = tempfile::tempdir().unwrap();
    let a = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        Options {
            role: "lb".into(),
            node_name: "owned-lb".into(),
            admin_password: "owned-admin".into(),
            peer_key: "owned-native-peer-key".into(),
            ..Default::default()
        },
    )
    .unwrap();
    a.config
        .put(
            "streams",
            "local/news",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    (d, a)
}
async fn request(a: &Arc<App>, path: &str) -> axum::response::Response {
    router(a.clone())
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}
// Routing to a vendor through the native admission protocol must fail this test.
#[tokio::test]
async fn mapped_vendor_destination_preserves_viewer_query_and_never_pulls_lb_media() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    let r = request(&a, "/local/news/index.m3u8?token=a%2Bb&x=1&x=2&empty=&flag").await;
    assert_eq!(r.status(), StatusCode::FOUND);
    assert_eq!(
        r.headers()["location"],
        "https://edge.example/base/remote/news/index.m3u8?token=a%2Bb&x=1&x=2&empty=&flag"
    );
    assert_eq!(a.media.count().await, 0);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 3);
}
// Without atomic per-LB holds, concurrent redirects oversubscribe one fresh sample.
#[tokio::test]
async fn concurrent_vendor_redirects_reserve_bandwidth_on_this_lb() {
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    let mut tasks = vec![];
    for _ in 0..8 {
        let a = a.clone();
        tasks.push(tokio::spawn(async move {
            request(&a, "/local/news/mpegts?token=owned").await.status()
        }))
    }
    let mut found = 0;
    for task in tasks {
        let status = task.await.unwrap();
        if status == StatusCode::FOUND {
            found += 1
        } else {
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE)
        }
    }
    assert_eq!(
        found, 1,
        "one 5 Mbps viewer fits below the strict 90% threshold"
    );
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn denied_and_unmapped_viewers_do_not_discover_vendor_destination() {
    use sha2::{Digest, Sha256};
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    a.config
        .put(
            "streams",
            "local/news",
            json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}),
        )
        .unwrap();
    assert_eq!(
        request(&a, "/local/news/m4s?token=wrong").await.status(),
        403
    );
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    a.config
        .put(
            "streams",
            "unmapped",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    assert_eq!(request(&a, "/unmapped/m4f").await.status(), 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn malformed_overloaded_and_unavailable_vendor_destinations_fail_closed() {
    for (field, bad) in [
        ("cpu_usage", json!(90)),
        ("memory_usage", json!(95)),
        ("output_kbit", json!(90000)),
        ("total_bandwidth", json!(0)),
        ("cpu_usage", json!(-1)),
        ("memory_usage", json!("20")),
        ("total_clients", json!(null)),
        ("streamer_status", json!("starting")),
    ] {
        let v = Vendor::new().await;
        v.state.config.lock().unwrap()["stats"][field] = bad;
        let (_d, a) = app();
        a.config.put("peers", "edge", v.peer()).unwrap();
        let r = request(&a, "/local/news/m4s").await;
        assert_eq!(r.status(), 503, "{field}");
        assert!(!r.headers().contains_key("location"));
        assert_eq!(a.media.count().await, 0);
    }
    for (field, bad) in [
        ("name", json!("other")),
        ("disabled", json!(true)),
        ("disabled", json!("false")),
        ("named_by", json!("user")),
        ("named_by", json!(null)),
        ("stats", json!({})),
    ] {
        let v = Vendor::new().await;
        v.state.stream.lock().unwrap()[field] = bad;
        let (_d, a) = app();
        a.config.put("peers", "edge", v.peer()).unwrap();
        assert_eq!(
            request(&a, "/local/news/mpegts").await.status(),
            503,
            "{field}"
        );
        assert_eq!(a.media.count().await, 0);
    }
}
#[tokio::test]
async fn vendor_identity_changes_and_self_claims_expose_no_redirect() {
    let v = Vendor::new().await;
    v.state.flip.store(true, Ordering::SeqCst);
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    v.state.flip.store(false, Ordering::SeqCst);
    v.state.config.lock().unwrap()["stats"]["id"] = json!("owned-lb");
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn source_and_policy_changes_during_vendor_telemetry_expose_no_redirect() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    let (entered, seen) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    *v.state.hold.lock().unwrap() = Some((entered, held));
    let c = a.clone();
    let task = tokio::spawn(async move { request(&c, "/local/news/m4s").await });
    tokio::time::timeout(std::time::Duration::from_secs(1), seen)
        .await
        .unwrap()
        .unwrap();
    a.config
        .put("streams", "local/news", json!({"disabled":true}))
        .unwrap();
    let _ = release.send(());
    let r = task.await.unwrap();
    assert!([403, 503].contains(&r.status().as_u16()));
    assert!(!r.headers().contains_key("location"));
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn clear_vendor_destination_is_excluded_for_secure_viewer() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    let mut peer = v.peer();
    peer["public_payload_url"] = json!("http://edge.invalid");
    a.config.put("peers", "edge", peer).unwrap();
    let cert = tls_fixture::Certificates::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(flussonix::http_tls::serve(
        listener,
        cert.server(),
        a.clone(),
        cancel.clone(),
    ));
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .use_preconfigured_tls((*cert.client()).clone())
        .build()
        .unwrap();
    let r = client
        .get(format!("https://{address}/local/news/index.m3u8"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
}
#[test]
fn vendor_destination_profiles_require_explicit_keys_safe_urls_and_unique_maps() {
    let (_d, a) = app();
    let base = json!({"api_url":"http://edge.example","public_payload_url":"https://edge.example","cluster_key":"owned-vendor-cluster-key","flussonix_peer_kind":"flussonic","flussonix_stream_map":[{"name":"local/news","remote_name":"remote/news"}]});
    for (field, bad) in [
        ("cluster_key", json!(null)),
        ("api_url", json!("http://u:p@edge.example")),
        ("api_url", json!("http://edge.example?token=secret")),
        ("public_payload_url", json!("https://u:p@edge.example")),
        (
            "public_payload_url",
            json!("https://edge.example?token=secret"),
        ),
        ("flussonix_peer_kind", json!("auto")),
        ("flussonix_stream_map", json!([])),
        (
            "flussonix_stream_map",
            json!([{"name":"a","remote_name":"b"},{"name":"a","remote_name":"c"}]),
        ),
        (
            "flussonix_stream_map",
            json!([{"name":"a","remote_name":"../b"}]),
        ),
        ("flussonix_rtsp_url", json!("rtsp://edge.example")),
    ] {
        let mut peer = base.clone();
        peer[field] = bad;
        assert!(a.config.put("peers", "edge", peer).is_err(), "{field}");
    }
}
#[tokio::test]
async fn vendor_holds_expire_and_cached_destination_identity_cannot_change_silently() {
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 302);
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    tokio::time::sleep(std::time::Duration::from_millis(5100)).await;
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 302);
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    v.state.config.lock().unwrap()["stats"]["id"] = json!("replacement-cdn");
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn draining_and_cold_vendor_routes_do_not_bypass_capacity_policy() {
    let v = Vendor::new().await;
    v.state.stream.lock().unwrap()["stats"]["running"] = json!(false);
    let (_d, a) = app();
    let mut p = v.peer();
    p["drain"] = json!(true);
    a.config.put("peers", "edge", p.clone()).unwrap();
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    p["drain"] = json!(false);
    a.config.put("peers", "edge", p).unwrap();
    let r = request(&a, "/local/news/m4f").await;
    assert_eq!(r.status(), 302);
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn successful_vendor_redirect_retains_lb_viewer_session_accounting() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    let r = request(&a, "/local/news/index.m3u8?token=viewer").await;
    assert_eq!(r.status(), 302);
    assert_eq!(
        a.playback_auth.active(),
        1,
        "successful placement must retain the authorized viewer session"
    );
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn destination_independently_denies_viewers_after_lb_allows_placement() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    let mut peer = v.peer();
    peer["public_payload_url"] = json!(v.url);
    a.config.put("peers", "edge", peer).unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    for (token, status) in [("denied-on-cdn", 403), ("allowed-on-cdn", 200)] {
        let r = request(&a, &format!("/local/news/mpegts?token={token}")).await;
        assert_eq!(r.status(), 302);
        let target = r.headers()["location"].to_str().unwrap();
        assert!(!target.contains("flussonix_ticket"));
        assert!(!target.contains("owned-vendor-cluster-key"));
        let result = client.get(target).send().await.unwrap();
        assert_eq!(result.status(), status);
    }
    assert_eq!(a.media.count().await, 0);
}
#[tokio::test]
async fn mixed_native_and_vendor_peers_retry_the_other_admission_protocol() {
    use axum::routing::post;
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["cpu_usage"] = json!(70);
    let (_d, a) = app();
    a.config.put("peers", "vendor", v.peer()).unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", tcp.local_addr().unwrap());
    let admits = Arc::new(AtomicU64::new(0));
    let calls = admits.clone();
    let service=Router::new().route("/flussonix/api/v1/node",get(|h:HeaderMap|async move {assert_eq!(h["x-flussonix-peer"],"owned-native-peer-key");assert!(!h.contains_key("authorization"));axum::Json(json!({"role":"cdn","uplink":0.01,"uplink_mbps":100,"cpu":0.01,"ram":0.01,"drain":false,"age_ms":0,"active":0,"reserved":0,"reserved_mbps":0,"limit":100,"streams":[],"stream_bitrates":{}}))})).route("/flussonix/api/v1/admit",post(move|h:HeaderMap|{let c=calls.clone();async move{assert_eq!(h["x-flussonix-peer"],"owned-native-peer-key");assert!(!h.contains_key("authorization"));c.fetch_add(1,Ordering::SeqCst);StatusCode::SERVICE_UNAVAILABLE}}));
    let native = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
    a.config
        .put(
            "peers",
            "native",
            json!({"api_url":api,"public_payload_url":"https://native.invalid"}),
        )
        .unwrap();
    let r = request(&a, "/local/news/index.m3u8?token=viewer").await;
    assert_eq!(r.status(), 302);
    assert!(
        r.headers()["location"]
            .to_str()
            .unwrap()
            .starts_with("https://edge.example/base/")
    );
    assert_eq!(admits.load(Ordering::SeqCst), 1);
    assert_eq!(a.media.count().await, 0);
    native.abort();
    let _ = native.await;
}
// Every regression below failed against the immutable reviewed head.
#[tokio::test]
async fn unrelated_config_save_does_not_erase_vendor_capacity_holds() {
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 302);
    a.config
        .put(
            "streams",
            "local/news",
            json!({"title":"unrelated metadata"}),
        )
        .unwrap();
    assert_eq!(
        request(&a, "/local/news/m4s").await.status(),
        503,
        "config save erased already-issued capacity hold"
    );
}
#[tokio::test]
async fn new_mapping_cannot_accept_a_changed_destination_identity() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config
        .put(
            "streams",
            "local/other",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let mut p = v.peer();
    p["flussonix_stream_map"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"local/other","remote_name":"remote/other"}));
    a.config.put("peers", "edge", p).unwrap();
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 302);
    v.state.config.lock().unwrap()["stats"]["id"] = json!("replacement-cdn");
    v.state.stream.lock().unwrap()["name"] = json!("remote/other");
    assert_eq!(
        request(&a, "/local/other/m4s").await.status(),
        503,
        "new stream slot forgot established peer identity"
    );
}
#[tokio::test]
async fn aliases_of_one_vendor_server_share_pending_egress() {
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    a.config.put("peers", "alias", v.peer()).unwrap();
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 302);
    assert_eq!(
        request(&a, "/local/news/m4s").await.status(),
        503,
        "peer alias reused the same server headroom"
    );
}
#[tokio::test]
async fn bounded_vendor_metadata_budget_allows_realistic_network_latency() {
    let v = Vendor::new().await;
    v.state.delay_ms.store(800, Ordering::SeqCst);
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    assert_eq!(
        request(&a, "/local/news/m4s").await.status(),
        302,
        "650ms individual deadline excludes working remote CDN"
    );
}
