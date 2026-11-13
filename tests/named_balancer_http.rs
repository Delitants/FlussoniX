use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use axum::{
    extract::ConnectInfo,
    routing::{get, post},
};
use base64::Engine;
use bytes::Bytes;
use flussonix::server::{App, Options, router};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tower::ServiceExt;

fn fixture() -> (tempfile::TempDir, Arc<App>) {
    fixture_with_geoip(false)
}

fn fixture_with_geoip(use_geoip: bool) -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let geoip_db = use_geoip.then(|| {
        let path = dir.path().join("country.mmdb");
        std::fs::write(&path, include_bytes!("fixtures/GeoIP2-Country-Test.mmdb")).unwrap();
        path
    });
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            role: "lb".into(),
            peer_key: "owned-peer-key".into(),
            admin_password: "owned-admin".into(),
            uplink_interface: "process".into(),
            geoip_db,
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "channel",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],
        "flussonix_token_sha256":format!("{:x}", Sha256::digest(b"viewer"))}),
        )
        .unwrap();
    app.config
        .put(
            "peers",
            "cdn-a",
            json!({"api_url":"http://127.0.0.1:1",
        "public_payload_url":"https://cdn.example"}),
        )
        .unwrap();
    app.config
        .put("balancers", "lb0", json!({"servers":[{"name":"cdn-a"}]}))
        .unwrap();
    (dir, app)
}

async fn request(app: &Arc<App>, method: &str, uri: &str, body: Body) -> StatusCode {
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    if status == StatusCode::NOT_FOUND && uri.contains("tracks-") {
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        eprintln!("named track 404: {uri}: {}", String::from_utf8_lossy(&body));
    }
    status
}

#[tokio::test]
async fn named_http_paths_use_the_underlying_stream_and_never_start_an_lb_worker() {
    let (_dir, app) = fixture();
    for suffix in [
        "index.m3u8",
        "video.m3u8",
        "tracks-v1a1/mono.m3u8",
        "tracks-v1a1/g1_1.ts",
        "mpegts",
        "m4s",
        "m4f",
        "fmp4/seg.m4f",
    ] {
        let uri = format!("/lb0/channel/{suffix}?token=viewer&x=%2B");
        assert_eq!(
            request(&app, "GET", &uri, Body::empty()).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "{uri}"
        );
    }
    assert_eq!(
        request(
            &app,
            "GET",
            "/%6c%620/channel/index.m3u8?token=viewer",
            Body::empty()
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/lb0/channel/index.m3u8?token=wrong",
            Body::empty()
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/lb0/channel/tracks-v1a1/mono.m3u8?token=wrong",
            Body::empty()
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn named_post_rejects_before_reading_body_or_using_publisher_credentials() {
    let (_dir, app) = fixture();
    let never = Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
    let result = tokio::time::timeout(
        Duration::from_millis(250),
        router(app.clone()).oneshot(
            Request::builder()
                .method("POST")
                .uri("/lb0/channel/mpegts")
                .header("Authorization", "Basic cHVibGlzaGVyOnNlY3JldA==")
                .body(never)
                .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        result.unwrap().unwrap().status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn named_path_rejects_ambiguous_encoding_traversal_and_admission_markers_early() {
    let (_dir, app) = fixture();
    for uri in [
        "/lb0/channel/%2e%2e/index.m3u8?token=wrong",
        "/lb0/channel/%GG/index.m3u8?token=wrong",
        "/lb0/channel/fmp4/%2e%2e/seg.m4f?token=wrong",
        "/lb0%2fother/channel/index.m3u8?token=wrong",
        "/lb0/channel%2fother/index.m3u8?token=wrong",
        "/lb0/channel/index.m3u8?token=wrong&flussonix_ticket=a&flussonix_ticket=b",
        "/lb0/channel/index.m3u8?token=wrong&flussonix_ticket=a&flussonix_handoff=b",
    ] {
        assert_eq!(
            request(&app, "GET", uri, Body::empty()).await,
            StatusCode::BAD_REQUEST,
            "{uri}"
        );
    }
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn a_similarly_named_ordinary_stream_is_not_shadowed_by_an_encoded_prefix() {
    let (_dir, app) = fixture();
    app.config
        .put(
            "streams",
            "lb0B",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],
                "flussonix_token_sha256":format!("{:x}", Sha256::digest(b"viewer"))}),
        )
        .unwrap();
    assert_eq!(
        request(
            &app,
            "GET",
            "/lb0%42/index.m3u8?token=viewer",
            Body::empty()
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn named_hls_redirect_preserves_media_suffix_and_viewer_query() {
    let (_dir, app) = fixture();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let cdn = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route(
                    "/flussonix/api/v1/node",
                    get(|| async {
                        axum::Json(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000.0,
                    "cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,
                    "reserved":0,"reserved_mbps":0.0,"limit":1000,
                    "output_kbit":1000,"bandwidth_usage":10.0,"total_clients":0,
                    "total_streams_count":1,"streams":[{"name":"channel","ready":true}],
                    "stream_bitrates":{"channel":{"mbps":2.0,"age_ms":0}}}))
                    }),
                )
                .route(
                    "/flussonix/api/v1/admit",
                    post(|| async {
                        axum::Json(json!({"ticket":"owned-cdn-ticket","expires_in":5}))
                    }),
                ),
        )
        .await
        .unwrap();
    });
    app.config
        .put(
            "peers",
            "cdn-a",
            json!({"api_url":api,
        "public_payload_url":"https://cdn.example","flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/lb0/channel/index.m3u8?token=viewer&x=%2B")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    let location = response.headers()["location"].to_str().unwrap();
    let url = url::Url::parse(location).unwrap();
    assert_eq!(url.scheme(), "https");
    assert_eq!(url.path(), "/channel/index.m3u8");
    let pairs = url
        .query_pairs()
        .into_owned()
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(pairs.get("token").map(String::as_str), Some("viewer"));
    assert_eq!(pairs.get("x").map(String::as_str), Some("+"));
    assert_eq!(
        pairs.get("flussonix_ticket").map(String::as_str),
        Some("owned-cdn-ticket")
    );
    assert_eq!(app.media.count().await, 0);
    cdn.abort();
    let _ = cdn.await;
}

async fn metric_peer(output_kbit: u64, cpu: f64) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route(
                    "/flussonix/api/v1/node",
                    get(move || async move {
                        axum::Json(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000.0,
                    "cpu":cpu,"ram":0.1,"drain":false,"age_ms":0,"active":0,
                    "reserved":0,"reserved_mbps":0.0,"limit":1000,
                    "output_kbit":output_kbit,"bandwidth_usage":10.0,"total_clients":0,
                    "total_streams_count":1,"streams":[{"name":"channel","ready":true}],
                    "stream_bitrates":{"channel":{"mbps":2.0,"age_ms":0}}}))
                    }),
                )
                .route(
                    "/flussonix/api/v1/admit",
                    post(|| async {
                        axum::Json(json!({"ticket":"owned-cdn-ticket","expires_in":5}))
                    }),
                ),
        )
        .await
        .unwrap();
    });
    (api, task)
}

#[tokio::test]
async fn named_bitrate_mode_overrides_native_pressure_without_bypassing_admission() {
    let (_dir, app) = fixture();
    let (a_url, a_task) = metric_peer(5_000, 0.05).await;
    let (b_url, b_task) = metric_peer(1_000, 0.25).await;
    app.config
        .put(
            "peers",
            "cdn-a",
            json!({"api_url":a_url,
        "public_payload_url":"https://cdn-a.example","flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    app.config
        .put(
            "peers",
            "cdn-b",
            json!({"api_url":b_url,
        "public_payload_url":"https://cdn-b.example","flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"bitrate","servers":[{"name":"cdn-a"},{"name":"cdn-b"}]}),
        )
        .unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/lb0/channel/index.m3u8?token=viewer")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        url::Url::parse(response.headers()["location"].to_str().unwrap())
            .unwrap()
            .host_str(),
        Some("cdn-b.example")
    );
    assert_eq!(app.media.count().await, 0);
    a_task.abort();
    b_task.abort();
    let _ = a_task.await;
    let _ = b_task.await;
}

#[tokio::test]
async fn returning_viewer_keeps_eligible_cdn_until_balancer_revision_changes() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let (_dir, app) = fixture();
    let mut tasks = Vec::new();
    let mut rates = Vec::new();
    for (name, initial) in [("cdn-a", 100_u64), ("cdn-b", 200_u64)] {
        let rate = Arc::new(AtomicU64::new(initial));
        let observed = rate.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        tasks.push(tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route(
                        "/flussonix/api/v1/node",
                        get(move || {
                            let observed = observed.clone();
                            async move {
                                axum::Json(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000.0,
                                    "cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,
                                    "reserved":0,"reserved_mbps":0.0,"limit":1000,
                                    "output_kbit":observed.load(Ordering::Relaxed),"bandwidth_usage":10.0,
                                    "total_clients":0,"total_streams_count":1,
                                    "streams":[{"name":"channel","ready":true}],
                                    "stream_bitrates":{"channel":{"mbps":2.0,"age_ms":0}}}))
                            }
                        }),
                    )
                    .route(
                        "/flussonix/api/v1/admit",
                        post(|| async { axum::Json(json!({"ticket":"owned-cdn-ticket","expires_in":5})) }),
                    ),
            )
            .await
            .unwrap();
        }));
        app.config
            .put(
                "peers",
                name,
                json!({"api_url":api,"public_payload_url":format!("https://{name}.example"),
                    "flussonix_allow_insecure_peer_http":true}),
            )
            .unwrap();
        rates.push(rate);
    }
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"bitrate","servers":[{"name":"cdn-a"},{"name":"cdn-b"}]}),
        )
        .unwrap();
    let host = |response: axum::response::Response| {
        assert_eq!(response.status(), StatusCode::FOUND);
        url::Url::parse(response.headers()["location"].to_str().unwrap())
            .unwrap()
            .host_str()
            .unwrap()
            .to_owned()
    };
    let path = "/lb0/channel/index.m3u8?token=viewer";
    let first = router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(host(first), "cdn-a.example");
    rates[0].store(300, Ordering::Relaxed);
    rates[1].store(200, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let repeated = router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(host(repeated), "cdn-a.example");
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"bitrate","servers":[{"name":"cdn-a"},{"name":"cdn-b"}]}),
        )
        .unwrap();
    let revised = router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(host(revised), "cdn-b.example");
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn named_balancer_retries_next_eligible_cdn_after_admission_refusal() {
    let (_dir, app) = fixture();
    let (a_url, a_task) = metric_peer(5_000, 0.05).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b_url = format!("http://{}", listener.local_addr().unwrap());
    let b_task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route(
                    "/flussonix/api/v1/node",
                    get(|| async {
                        axum::Json(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000.0,
                        "cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,
                        "reserved":0,"reserved_mbps":0.0,"limit":1000,
                        "output_kbit":1000,"bandwidth_usage":10.0,"total_clients":0,
                        "total_streams_count":1,"streams":[{"name":"channel","ready":true}],
                        "stream_bitrates":{"channel":{"mbps":2.0,"age_ms":0}}}))
                    }),
                )
                .route(
                    "/flussonix/api/v1/admit",
                    post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
                ),
        )
        .await
        .unwrap();
    });
    app.config
        .put("peers", "cdn-a", json!({"api_url":a_url,
            "public_payload_url":"https://cdn-a.example","flussonix_allow_insecure_peer_http":true}))
        .unwrap();
    app.config
        .put("peers", "cdn-b", json!({"api_url":b_url,
            "public_payload_url":"https://cdn-b.example","flussonix_allow_insecure_peer_http":true}))
        .unwrap();
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"bitrate","servers":[
            {"name":"cdn-a"},{"name":"cdn-b"}]}),
        )
        .unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/lb0/channel/index.m3u8?token=viewer")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        url::Url::parse(response.headers()["location"].to_str().unwrap())
            .unwrap()
            .host_str(),
        Some("cdn-a.example")
    );
    assert_eq!(app.media.count().await, 0);
    a_task.abort();
    b_task.abort();
    let _ = a_task.await;
    let _ = b_task.await;
}

#[tokio::test]
async fn country_rule_uses_direct_peer_ip_and_ignores_untrusted_forwarded_header() {
    let (_dir, app) = fixture_with_geoip(true);
    let (a_url, a_task) = metric_peer(5_000, 0.05).await;
    let (b_url, b_task) = metric_peer(1_000, 0.25).await;
    app.config
        .put(
            "peers",
            "cdn-a",
            json!({"api_url":a_url,
        "public_payload_url":"https://cdn-a.example","flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    app.config
        .put(
            "peers",
            "cdn-b",
            json!({"api_url":b_url,
        "public_payload_url":"https://cdn-b.example","flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"bitrate","servers":[
        {"name":"cdn-a","countries":["SE"]},
        {"name":"cdn-b","countries_default":true}]}),
        )
        .unwrap();
    let mut swedish = Request::builder()
        .uri("/lb0/channel/index.m3u8?token=viewer")
        .header("X-Forwarded-For", "192.0.2.1")
        .body(Body::empty())
        .unwrap();
    swedish.extensions_mut().insert(ConnectInfo(
        "89.160.20.128:39000".parse::<SocketAddr>().unwrap(),
    ));
    let selected = router(app.clone()).oneshot(swedish).await.unwrap();
    assert_eq!(selected.status(), StatusCode::FOUND);
    assert_eq!(
        url::Url::parse(selected.headers()["location"].to_str().unwrap())
            .unwrap()
            .host_str(),
        Some("cdn-a.example")
    );
    let mut other = Request::builder()
        .uri("/lb0/channel/index.m3u8?token=viewer")
        .header("X-Forwarded-For", "89.160.20.128")
        .body(Body::empty())
        .unwrap();
    other.extensions_mut().insert(ConnectInfo(
        "192.0.2.1:39001".parse::<SocketAddr>().unwrap(),
    ));
    let selected = router(app.clone()).oneshot(other).await.unwrap();
    assert_eq!(selected.status(), StatusCode::FOUND);
    assert_eq!(
        url::Url::parse(selected.headers()["location"].to_str().unwrap())
            .unwrap()
            .host_str(),
        Some("cdn-b.example")
    );
    a_task.abort();
    b_task.abort();
    let _ = a_task.await;
    let _ = b_task.await;
}

#[tokio::test]
async fn node_status_reports_local_geoip_readiness_without_exposing_a_database_path() {
    let (_dir, app) = fixture_with_geoip(true);
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("admin:owned-admin")
    );
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/node")
                .header("Authorization", auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    let node: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(node["geoip"]["configured"], true);
    assert_eq!(node["geoip"]["revision"].as_str().unwrap().len(), 16);
    assert!(!String::from_utf8_lossy(&body).contains("country.mmdb"));
}

#[tokio::test]
async fn config_edit_during_named_admission_suppresses_stale_redirect() {
    let (_dir, app) = fixture();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let entered_signal = entered.clone();
    let release_signal = release.clone();
    let cdn = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route(
                    "/flussonix/api/v1/node",
                    get(|| async {
                        axum::Json(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000.0,
                    "cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,
                    "reserved":0,"reserved_mbps":0.0,"limit":1000,
                    "output_kbit":1000,"bandwidth_usage":10.0,"total_clients":0,
                    "total_streams_count":1,"streams":[{"name":"channel","ready":true}],
                    "stream_bitrates":{"channel":{"mbps":2.0,"age_ms":0}}}))
                    }),
                )
                .route(
                    "/flussonix/api/v1/admit",
                    post(move || {
                        let entered = entered_signal.clone();
                        let release = release_signal.clone();
                        async move {
                            entered.notify_one();
                            release.notified().await;
                            axum::Json(json!({"ticket":"stale-ticket","expires_in":5}))
                        }
                    }),
                ),
        )
        .await
        .unwrap();
    });
    app.config
        .put(
            "peers",
            "cdn-a",
            json!({"api_url":api,
        "public_payload_url":"https://cdn.example","flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    let client = app.clone();
    let placement = tokio::spawn(async move {
        router(client)
            .oneshot(
                Request::builder()
                    .uri("/lb0/channel/index.m3u8?token=viewer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"clients",
        "servers":[{"name":"cdn-a"}]}),
        )
        .unwrap();
    release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(3), placement)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!response.headers().contains_key("location"));
    assert_eq!(app.media.count().await, 0);
    cdn.abort();
    let _ = cdn.await;
}

#[tokio::test]
async fn named_callback_redirect_is_suppressed_after_balancer_edit() {
    let (_dir, app) = fixture();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", listener.local_addr().unwrap());
    let entered_signal = entered.clone();
    let release_signal = release.clone();
    let callback = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/auth",
                get(move || {
                    let entered = entered_signal.clone();
                    let release = release_signal.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        (
                            StatusCode::FOUND,
                            [("location", "https://owned-billing.invalid/permission")],
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    app.config
        .put("streams", "channel", json!({"on_play":url}))
        .unwrap();
    let service = router(app.clone());
    let pending = tokio::spawn(async move {
        service
            .oneshot(
                Request::builder()
                    .uri("/lb0/channel/index.m3u8?token=viewer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    app.config
        .put(
            "balancers",
            "lb0",
            json!({"mode":"clients","servers":[{"name":"cdn-a"}]}),
        )
        .unwrap();
    release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap();
    let status = response.status();
    let redirected = response.headers().contains_key("location");
    let workers = app.media.count().await;
    callback.abort();
    let _ = callback.await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!redirected);
    assert_eq!(workers, 0);
}
