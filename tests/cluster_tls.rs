use flussonix::{
    http_tls,
    server::{App, Options, router},
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;

struct Node {
    app: Arc<App>,
    cert: Certificates,
    url: String,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    sampling: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}
async fn node(role: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let cert = Certificates::new();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            role: role.into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    app.set_http_delivery(None, Some(address));
    let cancel = CancellationToken::new();
    let task = tokio::spawn(http_tls::serve(
        listener,
        cert.server(),
        app.clone(),
        cancel.clone(),
    ));
    let a = app.clone();
    let stop = cancel.clone();
    let sampling = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {_=stop.cancelled()=>break,_=interval.tick()=>a.sample_metrics()}
        }
    });
    Node {
        app,
        cert,
        url: format!("https://{address}"),
        cancel,
        task,
        sampling,
        _dir: dir,
    }
}
impl Node {
    async fn stop(self) {
        self.app.media.stop_all().await;
        self.cancel.cancel();
        self.sampling.await.unwrap();
        tokio::time::timeout(Duration::from_secs(6), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
struct MediaEndpoint {
    cert: Certificates,
    url: String,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
async fn media_endpoint(app: Arc<App>) -> MediaEndpoint {
    let cert = Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(http_tls::serve(
        listener,
        cert.server(),
        app,
        cancel.clone(),
    ));
    MediaEndpoint {
        cert,
        url,
        cancel,
        task,
    }
}
async fn topology(transport: &str, separate_media: bool) {
    let source = node("source").await;
    let cdn = node("cdn").await;
    let lb = node("lb").await;
    let private = if separate_media {
        Some(media_endpoint(source.app.clone()).await)
    } else {
        None
    };
    source.app.config.put("streams","region/owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":900},"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})).unwrap();
    for app in [&cdn.app, &lb.app] {
        app.config.put("sources","origin",json!({"api_url":source.url,"flussonix_tls_ca":source.cert.ca,"flussonix_transport":transport})).unwrap();
        if let Some(private) = &private {
            app.config.put("sources","origin",json!({"private_payload_url":private.url,"flussonix_media_tls_ca":private.cert.ca})).unwrap();
        }
    }
    lb.app
        .config
        .put(
            "peers",
            "edge",
            json!({"api_url":cdn.url,"public_payload_url":cdn.url,"flussonix_tls_ca":cdn.cert.ca}),
        )
        .unwrap();
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(18))
        .redirect(reqwest::redirect::Policy::none());
    for n in [&source, &cdn, &lb] {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&n.cert.ca).unwrap()).unwrap(),
        );
    }
    if let Some(private) = &private {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&private.cert.ca).unwrap()).unwrap(),
        );
    }
    let client = builder.build().unwrap();
    assert_eq!(
        client
            .get(format!("{}/region/owned/index.m3u8", lb.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(source.app.media.count().await, 0);
    assert_eq!(cdn.app.media.count().await, 0);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let n = client
                .get(format!("{}/flussonix/api/v1/node", cdn.url))
                .header("X-Flussonix-Peer", "owned-peer-secret")
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            if n["cpu"].as_f64().is_some_and(|v| v < 0.9)
                && n["ram"].as_f64().is_some_and(|v| v < 0.95)
                && n["uplink"].as_f64().is_some_and(|v| v < 0.8)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let redirect = client
        .get(format!(
            "{}/region/owned/index.m3u8?token=owned-viewer",
            lb.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        redirect.status(),
        302,
        "secure {transport} LB discovery and admission"
    );
    let ticket = redirect.headers()["location"].to_str().unwrap().to_string();
    assert!(ticket.starts_with(&cdn.url));
    assert!(!ticket.contains("owned-peer-secret"));
    let redeem = client.get(&ticket).send().await.unwrap();
    assert_eq!(redeem.status(), 302);
    let canonical = redeem.headers()["location"].to_str().unwrap();
    assert!(!canonical.contains("flussonix_ticket"));
    let canonical = format!("{}{canonical}", cdn.url);
    let r = client.get(&canonical).send().await.unwrap();
    assert_eq!(
        r.status(),
        200,
        "{transport} private media: {}",
        cdn.app.media.stats("region/owned").await
    );
    let playlist = r.text().await.unwrap();
    assert_eq!(
        client.get(&ticket).send().await.unwrap().status(),
        503,
        "single-use admission"
    );
    for r in futures_util::future::join_all((0..4).map(|_| client.get(&canonical).send())).await {
        assert_eq!(r.unwrap().status(), 200)
    }
    assert_eq!(source.app.media.count().await, 1);
    assert_eq!(cdn.app.media.count().await, 1);
    let segment = playlist
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap();
    let protected = format!("{}/region/owned/{segment}", cdn.url);
    assert_eq!(
        client
            .get(format!(
                "{}/region/owned/{}",
                cdn.url,
                segment.split('?').next().unwrap()
            ))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let response = client.get(protected).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let file = cdn._dir.path().join("owned.ts");
    std::fs::write(&file, response.bytes().await.unwrap()).unwrap();
    let decoded = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(file)
        .args(["-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success(),
        "independent secure {transport} decode: {}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    if transport == "m4f" {
        let response = client
            .get(format!("{}/region/owned/m4f?token=owned-viewer", cdn.url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut signals = response.bytes_stream();
        let bytes = signals.next().await.unwrap().unwrap();
        let signal = String::from_utf8_lossy(&bytes);
        let stamp = signal
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        let path = format!("region/owned/{stamp}.m4f?token=owned-viewer");
        let edge = client
            .get(format!("{}/{path}", cdn.url))
            .send()
            .await
            .unwrap();
        let original = client
            .get(format!("{}/{path}", source.url))
            .send()
            .await
            .unwrap();
        assert_eq!(edge.status(), 200);
        assert_eq!(original.status(), 200);
        assert_eq!(edge.bytes().await.unwrap(), original.bytes().await.unwrap());
        drop(signals);
    }
    drop(client);
    lb.stop().await;
    cdn.stop().await;
    source.stop().await;
    if let Some(private) = private {
        private.cancel.cancel();
        private.task.await.unwrap().unwrap();
    }
}
#[tokio::test]
async fn private_ca_lb_cdn_source_hls() {
    topology("hls", false).await;
}
#[tokio::test]
async fn private_ca_lb_cdn_source_mpegts() {
    topology("mpegts", false).await;
}
#[tokio::test]
async fn private_ca_lb_cdn_source_m4s() {
    topology("m4s", false).await;
}
#[tokio::test]
async fn private_ca_lb_cdn_source_m4f() {
    topology("m4f", false).await;
}

#[tokio::test]
async fn source_management_and_private_media_can_use_different_private_cas() {
    topology("hls", true).await;
}

struct Metadata {
    cert: Certificates,
    url: String,
    gets: Arc<AtomicUsize>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
async fn metadata(expired: bool, redirect: Option<String>, disabled: bool) -> Metadata {
    let cert = Certificates::new();
    if expired {
        cert.expire()
    }
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let url = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let gets = Arc::new(AtomicUsize::new(0));
    let count = gets.clone();
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let app = axum::Router::new().fallback(axum::routing::get(move |h: axum::http::HeaderMap| {
        count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(h["X-Flussonix-Peer"], "owned-peer-secret");
        let redirect = redirect.clone();
        async move {
            if let Some(location) = redirect {
                axum::response::IntoResponse::into_response((
                    axum::http::StatusCode::FOUND,
                    [("Location", location)],
                ))
            } else {
                axum::response::IntoResponse::into_response(axum::Json(
                    json!({"name":"owned","disabled":disabled}),
                ))
            }
        }
    }));
    let tls = http_tls::Listener::new(listener, cert.server());
    let task = tokio::spawn(async move {
        axum::serve(tls, app)
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap()
    });
    Metadata {
        cert,
        url,
        gets,
        cancel,
        task,
    }
}
async fn request(app: Arc<App>) -> axum::http::StatusCode {
    router(app)
        .oneshot(
            axum::http::Request::builder()
                .uri("/owned/index.m3u8")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}
#[tokio::test]
async fn discovery_tls_rejects_bad_trust_identity_and_expiry_before_peer_http() {
    for case in 0..5 {
        let m = metadata(case == 2, None, true).await;
        let wrong = Certificates::new();
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut s = json!({"api_url":m.url});
        if case != 0 {
            s["flussonix_tls_ca"] = json!(m.cert.ca)
        }
        if case == 1 {
            s["api_url"] = json!(m.url.replace("127.0.0.1", "127.0.0.2"))
        }
        if case == 3 {
            s["flussonix_tls_ca"] = json!(wrong.ca)
        }
        app.config.put("sources", "origin", s).unwrap();
        let status = request(app.clone()).await;
        assert_eq!(m.gets.load(Ordering::SeqCst), usize::from(case == 4));
        assert_eq!(
            status, 404,
            "disabled or unavailable discovery never admits a viewer"
        );
        assert_eq!(app.media.count().await, 0);
        if case == 4 {
            let original_ca = std::fs::read(&m.cert.ca).unwrap();
            std::fs::copy(&wrong.ca, &m.cert.ca).unwrap();
            app.config
                .put("sources", "origin", json!({"drain":false}))
                .unwrap();
            assert_eq!(request(app.clone()).await, 404);
            assert_eq!(
                m.gets.load(Ordering::SeqCst),
                1,
                "a saved revision must reload changed CA contents rather than reuse an old pool"
            );
            std::fs::write(&m.cert.ca, original_ca).unwrap();
            app.config
                .put("sources", "origin", json!({"flussonix_transport":"mpegts"}))
                .unwrap();
            assert_eq!(request(app.clone()).await, 404);
            assert_eq!(
                m.gets.load(Ordering::SeqCst),
                2,
                "restored trust admits peer HTTP again"
            );
            app.config
                .put("sources", "origin", json!({"flussonix_tls_ca":wrong.ca}))
                .unwrap();
            assert_eq!(request(app.clone()).await, 404);
            assert_eq!(
                m.gets.load(Ordering::SeqCst),
                2,
                "saving changed trust must not reuse an old client or mirror"
            );
        }
        app.media.stop_all().await;
        m.cancel.cancel();
        m.task.await.unwrap();
    }
}
#[tokio::test]
async fn discovery_with_custom_ca_never_redirects_peer_credentials() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let n = count.clone();
    let foreign = tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            n.fetch_add(1, Ordering::SeqCst);
            drop(s)
        }
    });
    for scheme in ["http", "https"] {
        let m = metadata(false, Some(format!("{scheme}://{addr}/foreign")), true).await;
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put(
                "sources",
                "origin",
                json!({"api_url":m.url,"flussonix_tls_ca":m.cert.ca}),
            )
            .unwrap();
        assert_eq!(request(app).await, 404);
        assert_eq!(m.gets.load(Ordering::SeqCst), 1);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        m.cancel.cancel();
        m.task.await.unwrap();
    }
    foreign.abort();
    let _ = foreign.await;
}

#[tokio::test]
async fn private_hls_and_ts_ca_rejection_sends_no_peer_http_to_media() {
    for transport in ["hls", "mpegts"] {
        let api = metadata(false, None, false).await;
        let media = metadata(false, None, false).await;
        let wrong = Certificates::new();
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config.put("sources","origin",json!({"api_url":api.url,"flussonix_tls_ca":api.cert.ca,"private_payload_url":media.url,"flussonix_media_tls_ca":wrong.ca,"flussonix_transport":transport})).unwrap();
        let status = tokio::time::timeout(Duration::from_secs(12), request(app.clone()))
            .await
            .unwrap();
        assert_eq!(status, 503);
        assert_eq!(api.gets.load(Ordering::SeqCst), 1);
        assert_eq!(media.gets.load(Ordering::SeqCst), 0);
        app.media.stop_all().await;
        api.cancel.cancel();
        media.cancel.cancel();
        api.task.await.unwrap();
        media.task.await.unwrap();
    }
}
#[tokio::test]
async fn private_hls_and_ts_custom_ca_redirects_never_reach_another_origin() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let n = count.clone();
    let foreign = tokio::spawn(async move {
        while let Ok((s, _)) = listener.accept().await {
            n.fetch_add(1, Ordering::SeqCst);
            drop(s)
        }
    });
    for transport in ["hls", "mpegts"] {
        let api = metadata(false, None, false).await;
        let media = metadata(false, Some(format!("https://{addr}/foreign")), false).await;
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config.put("sources","origin",json!({"api_url":api.url,"flussonix_tls_ca":api.cert.ca,"private_payload_url":media.url,"flussonix_media_tls_ca":media.cert.ca,"flussonix_transport":transport})).unwrap();
        let status = tokio::time::timeout(Duration::from_secs(12), request(app.clone()))
            .await
            .unwrap();
        assert_eq!(status, 503);
        assert_eq!(api.gets.load(Ordering::SeqCst), 1);
        assert_eq!(media.gets.load(Ordering::SeqCst), 1);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        app.media.stop_all().await;
        api.cancel.cancel();
        media.cancel.cancel();
        api.task.await.unwrap();
        media.task.await.unwrap();
    }
    foreign.abort();
    let _ = foreign.await;
}
