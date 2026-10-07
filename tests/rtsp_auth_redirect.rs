//! Owned callback routing through real RTSP/TLS sockets; no vendor runtime.
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use flussonix::{
    rtsp,
    server::{App, Options, router},
};
use futures_util::FutureExt;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tower::ServiceExt;
#[path = "support/tls.rs"]
mod tls_fixture;
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
#[derive(Default)]
struct Backend {
    target: Mutex<Option<String>>,
    queries: Mutex<Vec<HashMap<String, String>>>,
    pause: AtomicBool,
    omit_location: AtomicBool,
    entered: Notify,
    release: Notify,
}
async fn callback(
    State(state): State<Arc<Backend>>,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, HeaderMap) {
    state.queries.lock().unwrap().push(query);
    let target = state.target.lock().unwrap().clone();
    if state.pause.load(Ordering::SeqCst) {
        state.entered.notify_one();
        state.release.notified().await;
    }
    let mut headers = HeaderMap::new();
    headers.insert("x-authduration", "60".parse().unwrap());
    if let Some(target) = target {
        if !state.omit_location.load(Ordering::SeqCst) {
            headers.insert("location", target.parse().unwrap());
        }
        (StatusCode::FOUND, headers)
    } else {
        (StatusCode::FORBIDDEN, headers)
    }
}
struct Lab {
    _dir: tempfile::TempDir,
    cert: tls_fixture::Certificates,
    app: Arc<App>,
    backend: Arc<Backend>,
    plain: String,
    tls: String,
    callback: String,
    cancel: CancellationToken,
    tasks: Vec<AbortOnDropHandle<()>>,
}
impl Drop for Lab {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl Lab {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cert = tls_fixture::Certificates::new();
        let backend = Arc::new(Backend::default());
        let cancel = CancellationToken::new();
        let cb = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let callback_url = format!("http://{}/auth", cb.local_addr().unwrap());
        let state = backend.clone();
        let stop = cancel.clone();
        let callback_task = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(
                cb,
                axum::Router::new()
                    .route("/auth", get(callback))
                    .with_state(state),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
        }));
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
            .put("templates", "routing", json!({"on_play":callback_url}))
            .unwrap();
        app.config
            .put(
                "streams",
                "nested/owned",
                json!({"static":false,"template":"routing","inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let plain = format!("rtsp://{}/nested/owned", l.local_addr().unwrap());
        let node = app.clone();
        let stop = cancel.clone();
        let plain_task = AbortOnDropHandle::new(tokio::spawn(async move {
            rtsp::serve(l, node, stop).await.unwrap()
        }));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls = format!("rtsps://{}/nested/owned", l.local_addr().unwrap());
        let node = app.clone();
        let stop = cancel.clone();
        let server = cert.server();
        let tls_task = AbortOnDropHandle::new(tokio::spawn(async move {
            rtsp::serve_tls(l, node, stop, server).await.unwrap()
        }));
        Self {
            _dir: dir,
            cert,
            app,
            backend,
            plain,
            tls,
            callback: callback_url,
            cancel,
            tasks: vec![callback_task, plain_task, tls_task],
        }
    }
    fn target(&self, target: Option<&str>) {
        *self.backend.target.lock().unwrap() = target.map(str::to_owned);
    }
    async fn socket(&self, secure: bool) -> BufReader<Box<dyn Io>> {
        let u = url::Url::parse(if secure { &self.tls } else { &self.plain }).unwrap();
        let tcp = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
            .await
            .unwrap();
        let io: Box<dyn Io> = if secure {
            Box::new(
                tokio_rustls::TlsConnector::from(self.cert.client())
                    .connect(
                        tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                        tcp,
                    )
                    .await
                    .unwrap(),
            )
        } else {
            Box::new(tcp)
        };
        BufReader::new(io)
    }
    async fn describe(&self, secure: bool, token: &str) -> (u16, String, Vec<u8>) {
        let uri = format!(
            "{}?token={token}&customer=a%26b&empty=",
            if secure { &self.tls } else { &self.plain }
        );
        request(&mut self.socket(secure).await, "DESCRIBE", &uri).await
    }
    async fn stop(&mut self) {
        self.app.media.stop_all().await;
        self.cancel.cancel();
        for task in self.tasks.drain(..) {
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
async fn request(
    socket: &mut BufReader<Box<dyn Io>>,
    method: &str,
    uri: &str,
) -> (u16, String, Vec<u8>) {
    socket
        .get_mut()
        .write_all(
            format!("{method} {uri} RTSP/1.0\r\nCSeq: 23\r\nAccept: application/sdp\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut h = vec![];
        while !h.ends_with(b"\r\n\r\n") {
            assert!(h.len() < 16384);
            h.push(socket.read_u8().await.unwrap());
        }
        let h = String::from_utf8(h).unwrap();
        let code = h.split(' ').nth(1).unwrap().parse().unwrap();
        let len = h
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut body = vec![0; len];
        socket.read_exact(&mut body).await.unwrap();
        (code, h, body)
    })
    .await
    .unwrap()
}
fn redirected(reply: (u16, String, Vec<u8>), target: &str) {
    assert_eq!(reply.0, 302, "{}", reply.1);
    assert!(reply.1.starts_with("RTSP/1.0 302 Moved Temporarily\r\n"));
    assert!(reply.1.contains("CSeq: 23\r\n"));
    assert!(reply.1.contains(&format!("Location: {target}\r\n")));
    assert!(!reply.1.contains("Session:"));
    assert!(reply.2.is_empty());
}
#[tokio::test]
async fn cached_callback_routes_rtsp_without_a_worker_or_media_session() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let target = "rtsp://127.0.0.1:9/alternative?token=owned%2Bvalue&customer=a%26b&empty=";
        lab.target(Some(target));
        for _ in 0..2 {
            redirected(lab.describe(false, "owned%2Bvalue").await, target);
        }
        let queries = lab.backend.queries.lock().unwrap().clone();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["name"], "nested/owned");
        assert_eq!(queries[0]["proto"], "rtsp");
        assert_eq!(queries[0]["token"], "owned+value");
        assert_eq!(
            queries[0]["qs"],
            "token=owned%2Bvalue&customer=a%26b&empty="
        );
        assert_eq!(lab.app.media.count().await, 0);
        assert_eq!(lab.app.playback_auth.active(), 0);
        assert_eq!(
            request(&mut lab.socket(false).await, "SETUP", &lab.plain)
                .await
                .0,
            454
        );
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn encrypted_callback_routing_preserves_location_and_allows_plaintext_upgrade() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let target = "rtsps://[::1]:9443/destination?token=owned%2Bvalue&item=%2f";
        lab.target(Some(target));
        redirected(lab.describe(true, "tls").await, target);
        redirected(lab.describe(false, "upgrade").await, target);
        let mut socket = lab.socket(true).await;
        let alias = lab.tls.replacen("rtsps:", "rtsp:", 1) + "?token=alias";
        redirected(request(&mut socket, "DESCRIBE", &alias).await, target);
        assert_eq!(lab.app.media.count().await, 0);
        assert!(
            lab.backend
                .queries
                .lock()
                .unwrap()
                .iter()
                .all(|q| q["proto"] == "rtsp")
        );
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn cached_plaintext_callback_cannot_downgrade_an_encrypted_control_connection() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let target = "rtsp://127.0.0.1:9/destination?token=other";
        lab.target(Some(target));
        redirected(lab.describe(false, "same").await, target);
        let denied = lab.describe(true, "same").await;
        assert_eq!(denied.0, 403);
        assert!(!denied.1.contains("Location:"));
        // Security follows the actual TLS socket, even with a plain URI alias.
        let mut socket = lab.socket(true).await;
        let alias = lab.tls.replacen("rtsps:", "rtsp:", 1) + "?token=same&customer=a%26b&empty=";
        assert_eq!(request(&mut socket, "DESCRIBE", &alias).await.0, 403);
        assert_eq!(lab.backend.queries.lock().unwrap().len(), 1);
        assert_eq!(lab.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn callback_rejects_wrong_protocol_unsafe_or_oversized_destinations() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let mut invalid = vec![
            "http://127.0.0.1:9/other".to_owned(),
            "https://127.0.0.1:9/other".into(),
            "file:///etc/passwd".into(),
            "/other".into(),
            "//127.0.0.1/other".into(),
            "rtsp:///other".into(),
            "rtsp://user:secret@127.0.0.1:9/other".into(),
            "rtsp://127.0.0.1:9/other#fragment".into(),
            "rtsp://127.0.0.1:9/path with space".into(),
            "rtsp://127.0.0.1:9/path?x=a\tb".into(),
            "rtsp://127.0.0.1:9/path\\other".into(),
            "rtsp://127.0.0.1:65536/path".into(),
        ];
        invalid.push(format!("rtsp://127.0.0.1:9/{}", "a".repeat(8192)));
        for (index, target) in invalid.iter().enumerate() {
            lab.target(Some(target));
            let reply = lab.describe(false, &format!("bad{index}")).await;
            assert_eq!(reply.0, 403, "destination {index}: {}", reply.1);
            assert!(!reply.1.contains("Location:"));
        }
        lab.target(Some("rtsp://127.0.0.1:9/other"));
        lab.backend.omit_location.store(true, Ordering::SeqCst);
        assert_eq!(lab.describe(false, "missing").await.0, 403);
        assert_eq!(lab.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn direct_self_redirects_include_encoded_paths_and_tls_uri_aliases() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        let target = lab.plain.replace("/nested/owned", "/nested/%6fwned/")
            + "?token=self&customer=a%26b&empty=";
        lab.target(Some(&target));
        assert_eq!(lab.describe(false, "self").await.0, 403);
        let target = lab.tls.clone() + "?token=tls&customer=a%26b&empty=";
        lab.target(Some(&target));
        let uri = target.replacen("rtsps:", "rtsp:", 1);
        assert_eq!(
            request(&mut lab.socket(true).await, "DESCRIBE", &uri)
                .await
                .0,
            403
        );
        assert_eq!(lab.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn configuration_change_fences_an_in_flight_callback_redirect() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        lab.target(Some("rtsp://127.0.0.1:9/destination"));
        lab.backend.pause.store(true, Ordering::SeqCst);
        let mut socket = lab.socket(false).await;
        let uri = lab.plain.clone() + "?token=stale";
        let pending = AbortOnDropHandle::new(tokio::spawn(async move {
            request(&mut socket, "DESCRIBE", &uri).await
        }));
        tokio::time::timeout(Duration::from_secs(3), lab.backend.entered.notified())
            .await
            .unwrap();
        // No explicit reconcile: the admission fence must observe the root revision itself.
        lab.app
            .config
            .put(
                "streams",
                "nested/owned",
                json!({"on_play":format!("{}?replacement=true",lab.callback)}),
            )
            .unwrap();
        lab.backend.release.notify_one();
        let reply = pending.await.unwrap();
        assert_eq!(reply.0, 503);
        assert!(!reply.1.contains("Location:"));
        assert_eq!(lab.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn manual_revocation_rejects_pending_and_cached_callback_redirects() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        lab.target(Some("rtsp://127.0.0.1:9/destination"));
        lab.backend.pause.store(true, Ordering::SeqCst);
        let mut socket = lab.socket(false).await;
        let uri = lab.plain.clone() + "?token=revoked&customer=a%26b&empty=";
        let pending = AbortOnDropHandle::new(tokio::spawn(async move {
            request(&mut socket, "DESCRIBE", &uri).await
        }));
        tokio::time::timeout(Duration::from_secs(3), lab.backend.entered.notified())
            .await
            .unwrap();
        let id = lab.backend.queries.lock().unwrap()[0]["session_id"].clone();
        assert!(lab.app.playback_auth.revoke(&id));
        lab.backend.pause.store(false, Ordering::SeqCst);
        lab.backend.release.notify_one();
        assert_eq!(pending.await.unwrap().0, 403);
        assert_eq!(lab.describe(false, "revoked").await.0, 403);
        assert_eq!(lab.backend.queries.lock().unwrap().len(), 1);
        assert_eq!(lab.app.media.count().await, 0);
        redirected(
            lab.describe(false, "cached").await,
            "rtsp://127.0.0.1:9/destination",
        );
        let id = lab.backend.queries.lock().unwrap()[1]["session_id"].clone();
        assert!(lab.app.playback_auth.revoke(&id));
        assert_eq!(lab.describe(false, "cached").await.0, 403);
        assert_eq!(lab.backend.queries.lock().unwrap().len(), 2);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn viewer_token_denial_precedes_callback_routing() {
    use sha2::{Digest, Sha256};
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        lab.app
            .config
            .put(
                "streams",
                "nested/owned",
                json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"known"))}),
            )
            .unwrap();
        lab.target(Some("rtsp://127.0.0.1:9/other"));
        assert_eq!(lab.describe(false, "wrong").await.0, 403);
        assert!(lab.backend.queries.lock().unwrap().is_empty());
        redirected(
            lab.describe(false, "known").await,
            "rtsp://127.0.0.1:9/other",
        );
        assert_eq!(lab.backend.queries.lock().unwrap().len(), 1);
        assert_eq!(lab.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_callback_redirects_stay_http_and_protocol_caches_remain_separate() {
    let mut lab = Lab::new().await;
    let result = std::panic::AssertUnwindSafe(async {
        lab.target(Some("https://example.invalid/alternative?token=a%2Bb"));
        let response = router(lab.app.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/nested/owned/mpegts?token=shared")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 302);
        assert_eq!(
            response.headers()["location"],
            "https://example.invalid/alternative?token=a%2Bb"
        );
        assert_eq!(lab.describe(false, "shared").await.0, 403);
        lab.target(Some("rtsp://127.0.0.1:9/other"));
        let response = router(lab.app.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/nested/owned/mpegts?token=rtsp-target")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
        assert_eq!(lab.backend.queries.lock().unwrap().len(), 3);
        assert_eq!(lab.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn independent_client_follows_callback_to_authorized_decoded_media() {
    use sha2::{Digest, Sha256};
    let mut lab = Lab::new().await;
    let dir = tempfile::tempdir().unwrap();
    let destination = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-destination-admin".into(),
            peer_key: "owned-destination-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    destination.config.put("streams","actual",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-target"))})).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!(
        "rtsp://{}/actual?token=owned-target&customer=a%26b",
        listener.local_addr().unwrap()
    );
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let app = destination.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        rtsp::serve(listener, app, stop).await.unwrap()
    }));
    let result = std::panic::AssertUnwindSafe(async {
        let mut unauthorized = BufReader::new(Box::new(
            TcpStream::connect(
                url::Url::parse(&target)
                    .unwrap()
                    .socket_addrs(|| Some(554))
                    .unwrap()[0],
            )
            .await
            .unwrap(),
        ) as Box<dyn Io>);
        assert_eq!(
            request(
                &mut unauthorized,
                "DESCRIBE",
                &target.replace("owned-target", "wrong")
            )
            .await
            .0,
            403
        );
        assert_eq!(destination.media.count().await, 0);
        lab.target(Some(&target));
        let output = tokio::time::timeout(
            Duration::from_secs(25),
            tokio::process::Command::new("ffmpeg")
                .args([
                    "-nostdin",
                    "-v",
                    "error",
                    "-rtsp_transport",
                    "tcp",
                    "-i",
                    &format!("{}?token=owned-entry", lab.plain),
                    "-t",
                    "2",
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0",
                    "-threads",
                    "1",
                    "-f",
                    "framemd5",
                    "-",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "strict redirected decode: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let recording = String::from_utf8_lossy(&output.stdout);
        assert!(
            recording.contains("#media_type 0: video")
                && recording.contains("#media_type 1: audio")
        );
        assert!(recording.lines().filter(|l| l.starts_with("0,")).count() >= 20);
        assert!(recording.lines().filter(|l| l.starts_with("1,")).count() >= 40);
        assert_eq!(lab.app.media.count().await, 0);
        assert_eq!(destination.media.count().await, 1);
        assert_eq!(lab.backend.queries.lock().unwrap().len(), 1);
    })
    .catch_unwind()
    .await;
    destination.media.stop_all().await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}
