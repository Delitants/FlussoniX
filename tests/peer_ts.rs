use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use flussonix::peer_hls::PeerHls;
use futures_util::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[path = "support/tls.rs"]
mod tls;

#[tokio::test]
async fn continuous_ts_never_forwards_peer_credentials_on_redirect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let foreign = format!("{url}/foreign/mpegts");
    let app = Router::new()
        .route(
            "/owned/mpegts",
            get(move || {
                let location = foreign.clone();
                async move {
                    Response::builder()
                        .status(302)
                        .header("location", location)
                        .body(Body::empty())
                        .unwrap()
                }
            }),
        )
        .route(
            "/foreign/mpegts",
            get(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    "foreign"
                }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let bridge = PeerHls::start(&format!("{url}/owned/mpegts"), "owned-peer-secret")
        .await
        .unwrap();
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&bridge.url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    drop(bridge);
    server.abort();
}

#[tokio::test]
async fn secure_peer_ts_rejects_an_untrusted_certificate_before_media_startup() {
    use flussonix::{
        http_tls,
        server::{App, Options},
    };
    let dir = tempfile::tempdir().unwrap();
    let certificates = tls::Certificates::new();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            serde_json::json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}/owned/mpegts", listener.local_addr().unwrap());
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(http_tls::serve(
        listener,
        certificates.server(),
        app.clone(),
        cancel.clone(),
    ));
    let bridge = PeerHls::start(&endpoint, "owned-peer-secret")
        .await
        .unwrap();
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&bridge.url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert_eq!(app.media.count().await, 0);
    drop(bridge);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn progressing_peer_ts_can_run_longer_than_finite_hls_request_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/owned/mpegts", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/owned/mpegts",
        get(|| async {
            let stream = futures_util::stream::unfold(0u8, |n| async move {
                if n == 120 {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                Some((Ok::<_, std::io::Error>(Bytes::from(vec![n; 188])), n + 1))
            });
            Response::builder().body(Body::from_stream(stream)).unwrap()
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let bridge = PeerHls::start(&url, "owned-peer-secret").await.unwrap();
    let bytes = tokio::time::timeout(Duration::from_secs(16), async {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(&bridge.url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(bytes.len(), 120 * 188);
    assert_eq!(&bytes[119 * 188..], &[119; 188]);
    drop(bridge);
    task.abort();
}

async fn reject_metadata(oversized: bool) {
    use flussonix::media::Engine;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("tshttp://{}/owned/mpegts", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/owned/mpegts",
        get(move || async move {
            if oversized {
                Response::builder()
                    .body(Body::from(vec![0x47; 2 * 1024 * 1024]))
                    .unwrap()
            } else {
                let stream = futures_util::stream::unfold((), |()| async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(&[0x47])), ()))
                });
                Response::builder().body(Body::from_stream(stream)).unwrap()
            }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let worker=engine.ensure("owned",&serde_json::json!({"inputs":[{"url":endpoint}],"flussonix_peer_key":"owned-peer-secret"})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(7), worker.closed())
        .await
        .unwrap();
    assert_eq!(worker.pid(), 0, "invalid metadata spawned a packager");
    assert_eq!(
        worker.stats()["last_error"],
        if oversized {
            "metadata_limit"
        } else {
            "startup_timeout"
        }
    );
    engine.stop_all().await;
    task.abort();
}
#[tokio::test]
async fn oversized_ts_metadata_is_rejected_before_packager_start() {
    reject_metadata(true).await;
}
#[tokio::test]
async fn progressing_without_pmt_has_a_total_startup_deadline() {
    reject_metadata(false).await;
}

#[tokio::test]
async fn stalled_peer_metadata_does_not_block_other_stream_startups() {
    use flussonix::media::Engine;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("tshttp://{}/owned/mpegts", listener.local_addr().unwrap());
    let connected = Arc::new(AtomicUsize::new(0));
    let seen = connected.clone();
    let app = Router::new().route(
        "/owned/mpegts",
        get(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            async {
                let stream = futures_util::stream::unfold((), |()| async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(&[0x47])), ()))
                });
                Response::builder().body(Body::from_stream(stream)).unwrap()
            }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let e = engine.clone();
    let stalled = tokio::spawn(async move {
        e.ensure("stalled",&serde_json::json!({"inputs":[{"url":endpoint}],"flussonix_peer_key":"owned-peer-secret"})).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while connected.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    let healthy = tokio::time::timeout(
        Duration::from_secs(1),
        engine.ensure(
            "healthy",
            &serde_json::json!({"inputs":[{"url":"testsrc://"}]}),
        ),
    )
    .await;
    stalled.await.unwrap().ok();
    engine.stop_all().await;
    task.abort();
    assert!(
        healthy.is_ok_and(|result| result.is_ok()),
        "one bad source serialized another stream's startup"
    );
}

#[tokio::test]
async fn fixed_peer_ts_stream_delivers_before_eof_and_cancels_its_upstream() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!(
        "http://{}/owned/mpegts?routing=owned",
        listener.local_addr().unwrap()
    );
    let active = Arc::new(AtomicUsize::new(0));
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let count = active.clone();
    let app = Router::new().route(
        "/owned/mpegts",
        get(move |h: HeaderMap| {
            let count = count.clone();
            async move {
                if h.get("x-flussonix-peer").and_then(|v| v.to_str().ok())
                    != Some("owned-peer-secret")
                {
                    return Response::builder().status(403).body(Body::empty()).unwrap();
                }
                count.fetch_add(1, Ordering::SeqCst);
                let guard = Guard(count);
                let stream =
                    futures_util::stream::unfold((true, guard), |(first, guard)| async move {
                        if first {
                            Some((
                                Ok::<_, std::io::Error>(Bytes::from(vec![0x47; 188])),
                                (false, guard),
                            ))
                        } else {
                            std::future::pending().await
                        }
                    });
                Response::builder()
                    .header("content-type", "video/mp2t")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let bridge = PeerHls::start(&upstream, "owned-peer-secret")
        .await
        .expect("continuous peer TS must be supported");
    assert!(!bridge.url.contains("owned-peer-secret"));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut parts = bridge.url.rsplitn(3, '/');
    let _file = parts.next().unwrap();
    let _resource = parts.next().unwrap();
    let prefix = parts.next().unwrap();
    let forged = format!(
        "{prefix}/{}/resource.bin",
        URL_SAFE_NO_PAD.encode(upstream.replace("/owned/mpegts", "/other/mpegts"))
    );
    assert_eq!(
        client.get(forged).send().await.unwrap().status(),
        StatusCode::BAD_GATEWAY
    );
    let response = tokio::time::timeout(Duration::from_secs(2), client.get(&bridge.url).send())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .as_ref(),
        &[0x47; 188]
    );
    assert_eq!(active.load(Ordering::SeqCst), 1);
    // Hold the permit through the body, preventing a second upstream subscription.
    assert_eq!(
        client.get(&bridge.url).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(bridge);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .is_none()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    server.abort();
}
