use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct SyntheticPeer<L> {
    listener: L,
    address: SocketAddr,
}

impl<L> SyntheticPeer<L> {
    fn remote(listener: L) -> Self {
        Self {
            listener,
            address: "198.51.100.7:40000".parse().unwrap(),
        }
    }
}

impl<L: Listener<Addr = SocketAddr>> Listener for SyntheticPeer<L> {
    type Io = L::Io;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (io, _) = self.listener.accept().await;
        (io, self.address)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

#[tokio::test]
async fn remote_plaintext_denies_management_before_routing_but_preserves_media_and_peer_paths() {
    use axum::routing::any;
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        SyntheticPeer::remote(tcp),
        Router::new().fallback(any(|| async { "routed" })),
        stop,
        TransportPolicy::default(),
    ));
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    for path in [
        "/admin",
        "/admin/",
        "/admin/assets/main.js",
        "/streamer/api/v3/config",
        "/flussonix/api/v1/node",
    ] {
        let response = client
            .get(format!("{base}{path}"))
            .header("Authorization", "Basic YWRtaW46c2VjcmV0")
            .header("Host", "localhost")
            .header("Forwarded", "for=127.0.0.1;proto=https")
            .header("X-Forwarded-Proto", "https")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN, "{path}");
        assert!(response.headers().get("www-authenticate").is_none());
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    for path in [
        "/health",
        "/live/index.m3u8",
        "/live/mpegts",
        "/live/m4f",
        "/live/m4s",
        "/flussonix/api/v1/admit",
    ] {
        let response = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{path}");
        assert_eq!(response.text().await.unwrap(), "routed");
    }
    let publish = client
        .post(format!("{base}/live/mpegts"))
        .header("Authorization", "Basic cHVibGlzaGVyOnNlY3JldA==")
        .body("fixture")
        .send()
        .await
        .unwrap();
    assert_eq!(publish.status(), reqwest::StatusCode::OK);
    let peer_basic = client
        .get(format!("{base}/flussonix/api/v1/admit"))
        .header("Authorization", "Basic cGVlcjpzZWNyZXQ=")
        .send()
        .await
        .unwrap();
    assert_eq!(peer_basic.status(), reqwest::StatusCode::FORBIDDEN);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn remote_plaintext_rejects_credential_paths_while_local_tls_and_override_allow_them() {
    use axum::routing::any;
    use tokio_rustls::rustls::pki_types::ServerName;

    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        SyntheticPeer::remote(tcp),
        Router::new().fallback(any(|| async { "routed" })),
        stop,
        TransportPolicy::default(),
    ));
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    for (method, path, authorization) in [
        (reqwest::Method::GET, "/admin/", None),
        (reqwest::Method::PUT, "/streamer/api/v3/config", None),
        (
            reqwest::Method::GET,
            "/flussonix/api/v1/node",
            Some("Bearer owned"),
        ),
    ] {
        let mut request = client.request(method, format!("{base}{path}"));
        if let Some(value) = authorization {
            request = request.header("Authorization", value);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            reqwest::StatusCode::FORBIDDEN,
            "{path}"
        );
    }
    assert_eq!(
        client
            .get(format!("{base}/flussonix/api/v1/admit"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK,
    );
    server.abort();
    let _ = server.await;

    for (remote, policy) in [
        (false, TransportPolicy::default()),
        (
            true,
            TransportPolicy {
                allow_insecure_remote_management: true,
                ..Default::default()
            },
        ),
    ] {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let stop = CancellationToken::new();
        let router = Router::new().fallback(any(|| async { "routed" }));
        let server = if remote {
            tokio::spawn(serve_listener(
                SyntheticPeer::remote(tcp),
                router,
                stop,
                policy,
            ))
        } else {
            tokio::spawn(serve_listener(tcp, router, stop, policy))
        };
        let response = client
            .get(format!("http://{address}/admin/"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        server.abort();
        let _ = server.await;
    }

    let cert = tls_fixture::Certificates::new();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        SyntheticPeer::remote(crate::http_tls::Listener::new(tcp, cert.server())),
        Router::new().fallback(any(|| async { "routed" })),
        stop,
        TransportPolicy {
            secure: true,
            ..Default::default()
        },
    ));
    let socket = TcpStream::connect(address).await.unwrap();
    let connector = tokio_rustls::TlsConnector::from(cert.client());
    let mut stream = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    stream
        .write_all(b"GET /admin/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn ipv4_mapped_peer_uses_embedded_ipv4_address_for_management_boundary() {
    use axum::routing::any;
    let client = reqwest::Client::new();
    for (peer, expected) in [
        ("[::ffff:127.0.0.1]:40000", reqwest::StatusCode::OK),
        (
            "[::ffff:198.51.100.7]:40000",
            reqwest::StatusCode::FORBIDDEN,
        ),
    ] {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve_listener(
            SyntheticPeer {
                listener: tcp,
                address: peer.parse().unwrap(),
            },
            Router::new().fallback(any(|| async { "routed" })),
            stop,
            TransportPolicy::default(),
        ));
        for path in ["/admin/", "/streamer/api/v3/config"] {
            let response = client
                .get(format!("http://{address}{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{peer} {path}");
        }
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn remote_plaintext_guard_wraps_the_real_management_router() {
    let dir = tempfile::tempdir().unwrap();
    let app = crate::server::App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        crate::server::Options {
            admin_password: "owned-admin-secret".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let server = tokio::spawn(serve_listener(
        SyntheticPeer::remote(tcp),
        crate::server::router(app),
        CancellationToken::new(),
        TransportPolicy::default(),
    ));
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    assert_eq!(
        client
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{base}/admin/"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("{base}/streamer/api/v3/config"))
            .header("Authorization", "Basic YWRtaW46b3duZWQtYWRtaW4tc2VjcmV0")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("{base}/flussonix/api/v1/node"))
            .header("Authorization", "Basic YWRtaW46b3duZWQtYWRtaW4tc2VjcmV0")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .get(format!("{base}/flussonix/api/v1/node"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn stale_watch_cannot_abort_reused_or_unrelated_connection() {
    let control = Control::default();
    let unrelated = Control::default();
    let old_cancel = CancellationToken::new();
    let token = old_cancel.clone();
    let old = control.watch(async move {
        token.cancelled().await;
        Reason::Worker
    });
    let identity = old.identity.clone();
    let selected = old.reason.clone();
    drop(old);
    let fresh_cancel = CancellationToken::new();
    let token = fresh_cancel.clone();
    let fresh = control.watch(async move {
        token.cancelled().await;
        Reason::Authorization
    });
    // Simulate a ready callback that lost the unregister race.
    control.abort_if_current(&identity, &selected, Reason::Worker);
    old_cancel.cancel();
    tokio::task::yield_now().await;
    assert!(!control.abort.is_cancelled());
    assert_eq!(fresh.reason(), None);
    fresh_cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(1), control.abort.cancelled())
        .await
        .unwrap();
    assert_eq!(fresh.reason(), Some(Reason::Authorization));
    assert!(!unrelated.abort.is_cancelled());
}
#[tokio::test]
async fn body_drop_unregisters_even_after_reason_is_selected() {
    let control = Control::default();
    let mut old = control.watch(std::future::pending());
    old.unregister();
    old.abort(Reason::Worker);
    assert_eq!(old.reason(), None);
    assert!(!control.abort.is_cancelled());
}
struct Pending {
    entered: Arc<tokio::sync::Notify>,
    resets: Arc<AtomicUsize>,
}
impl ResetSocket for Pending {
    fn reset_on_drop(&self) -> io::Result<()> {
        self.resets.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
impl AsyncRead for Pending {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.entered.notify_one();
        Poll::Pending
    }
}
impl AsyncWrite for Pending {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        self.entered.notify_one();
        Poll::Pending
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.entered.notify_one();
        Poll::Pending
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.entered.notify_one();
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.entered.notify_one();
        Poll::Pending
    }
}
#[tokio::test]
async fn cancellation_wakes_pending_read_write_vectored_flush_and_shutdown() {
    for operation in 0..5 {
        let control = Control::default();
        let entered = Arc::new(tokio::sync::Notify::new());
        let resets = Arc::new(AtomicUsize::new(0));
        let mut io = Io {
            inner: Pending {
                entered: entered.clone(),
                resets: resets.clone(),
            },
            cancelled: Box::pin(control.abort.clone().cancelled_owned()),
            reset: false,
            first_request: Arc::new(AtomicBool::new(false)),
            header_deadline: None,
            _reservation: None,
        };
        let task = tokio::spawn(async move {
            let error = match operation {
                0 => io.read(&mut [0; 1]).await.unwrap_err(),
                1 => io.write(&[1]).await.unwrap_err(),
                2 => io
                    .write_vectored(&[io::IoSlice::new(&[1])])
                    .await
                    .unwrap_err(),
                3 => io.flush().await.unwrap_err(),
                _ => io.shutdown().await.unwrap_err(),
            };
            assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        control.abort.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resets.load(Ordering::Relaxed), 1);
    }
}

use crate::transport_tls_fixture as tls_fixture;

#[tokio::test]
async fn incomplete_http_headers_expire_before_routing() {
    use axum::routing::get;
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        tcp,
        Router::new().route("/", get(|| async { "ready" })),
        stop,
        TransportPolicy::default(),
    ));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: local")
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(12), async {
        stream.read(&mut [0; 1]).await
    })
    .await;
    server.abort();
    let _ = server.await;
    assert!(
        result.is_ok(),
        "incomplete header socket outlived its admission deadline"
    );
}

#[tokio::test]
async fn incomplete_second_keepalive_request_releases_its_connection() {
    use axum::routing::get;
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        tcp,
        Router::new().route("/", get(|| async { "ready" })),
        stop,
        TransportPolicy::default(),
    ));
    let stream = TcpStream::connect(address).await.unwrap();
    assert_second_header_expires(stream).await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn incomplete_second_https_request_releases_its_connection() {
    use axum::routing::get;
    use tokio_rustls::rustls::pki_types::ServerName;
    let cert = tls_fixture::Certificates::new();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        crate::http_tls::Listener::new(tcp, cert.server()),
        Router::new().route("/", get(|| async { "ready" })),
        stop,
        TransportPolicy {
            secure: true,
            ..Default::default()
        },
    ));
    let socket = TcpStream::connect(address).await.unwrap();
    let connector = tokio_rustls::TlsConnector::from(cert.client());
    let stream = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    assert_second_header_expires(stream).await;
    server.abort();
    let _ = server.await;
}

async fn assert_second_header_expires<T: AsyncRead + AsyncWrite + Unpin>(mut stream: T) {
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: local\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !response.ends_with(b"ready") {
            let mut bytes = [0; 1024];
            let n = stream.read(&mut bytes).await.unwrap();
            assert!(n > 0, "first response closed the keepalive connection");
            response.extend_from_slice(&bytes[..n]);
        }
    })
    .await
    .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: local")
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(7), async {
        let mut tail = Vec::new();
        stream.read_to_end(&mut tail).await
    })
    .await;
    assert!(
        matches!(result, Ok(Ok(_)) | Ok(Err(_))),
        "incomplete second header retained its admission slot"
    );
}

#[tokio::test]
async fn incomplete_https_headers_expire_after_tls_handshake() {
    use axum::routing::get;
    use tokio_rustls::rustls::pki_types::ServerName;
    let cert = tls_fixture::Certificates::new();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        crate::http_tls::Listener::new(tcp, cert.server()),
        Router::new().route("/", get(|| async { "ready" })),
        stop,
        TransportPolicy {
            secure: true,
            ..Default::default()
        },
    ));
    let socket = TcpStream::connect(address).await.unwrap();
    let connector = tokio_rustls::TlsConnector::from(cert.client());
    let mut stream = connector
        .connect(ServerName::try_from("localhost").unwrap(), socket)
        .await
        .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: local")
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(12), async {
        stream.read(&mut [0; 1]).await
    })
    .await;
    server.abort();
    let _ = server.await;
    assert!(
        result.is_ok(),
        "incomplete HTTPS header socket outlived its admission deadline"
    );
}

#[tokio::test]
async fn admission_rejects_excess_sockets_and_releases_capacity_on_drop() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let mut guarded = Guarded(tcp, Arc::new(Admission::new(1, 1)));
    let first_client = TcpStream::connect(address).await.unwrap();
    let (first_io, _) = guarded.accept().await;
    let accepting = tokio::spawn(async move { guarded.accept().await });

    let mut excess_client = TcpStream::connect(address).await.unwrap();
    let mut byte = [0; 1];
    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        excess_client.read(&mut byte),
    )
    .await
    .unwrap();
    assert!(matches!(rejected, Ok(0) | Err(_)));

    drop(first_io);
    drop(first_client);
    let _replacement_client = TcpStream::connect(address).await.unwrap();
    let (_replacement_io, _) = tokio::time::timeout(std::time::Duration::from_secs(1), accepting)
        .await
        .unwrap()
        .unwrap();
}
struct HeldBody {
    body: axum::body::Body,
    _watch: Watch,
}
impl http_body::Body for HeldBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
#[tokio::test]
async fn completed_http_and_https_bodies_keep_the_same_connection_after_late_cancellation() {
    use axum::{body::Body, routing::get};
    use futures_util::FutureExt;
    for tls in [false, true] {
        let old = CancellationToken::new();
        let peers = Arc::new(Mutex::new(vec![]));
        let (remember, signal) = (peers.clone(), old.clone());
        let router = Router::new().fallback(get(
            move |ConnectInfo(peer): ConnectInfo<SocketAddr>, request: Request| {
                let (peers, signal) = (remember.clone(), signal.clone());
                async move {
                    peers.lock().unwrap().push(peer);
                    let body = if request.uri().path() == "/first" {
                        let control = request.extensions().get::<Control>().unwrap();
                        let watch = control.watch(async move {
                            signal.cancelled().await;
                            Reason::Worker
                        });
                        Body::new(HeldBody {
                            body: Body::from("first"),
                            _watch: watch,
                        })
                    } else {
                        Body::from("second")
                    };
                    Response::new(body)
                }
            },
        ));
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = tcp.local_addr().unwrap().port();
        let cert = tls.then(tls_fixture::Certificates::new);
        let stop = CancellationToken::new();
        let server = if let Some(cert) = &cert {
            tokio::spawn(serve_listener(
                crate::http_tls::Listener::new(tcp, cert.server()),
                router,
                stop,
                TransportPolicy {
                    secure: true,
                    ..Default::default()
                },
            ))
        } else {
            tokio::spawn(serve_listener(
                tcp,
                router,
                stop,
                TransportPolicy::default(),
            ))
        };
        let outcome = std::panic::AssertUnwindSafe(async {
            let mut builder = reqwest::Client::builder()
                .http1_only()
                .timeout(std::time::Duration::from_secs(3));
            if let Some(cert) = &cert {
                builder = builder.add_root_certificate(
                    reqwest::Certificate::from_pem(&std::fs::read(&cert.ca).unwrap()).unwrap(),
                );
            }
            let client = builder.build().unwrap();
            let scheme = if tls { "https" } else { "http" };
            let base = format!("{scheme}://localhost:{port}");
            assert_eq!(
                client
                    .get(format!("{base}/first"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
                "first"
            );
            old.cancel();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(
                client
                    .get(format!("{base}/second"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
                "second"
            );
            let peers = peers.lock().unwrap();
            assert_eq!(peers.len(), 2);
            assert_eq!(
                peers[0], peers[1],
                "HTTP client must reuse the actual connection, not hide a reset by retrying"
            );
        })
        .catch_unwind()
        .await;
        server.abort();
        let _ = server.await;
        if let Err(error) = outcome {
            std::panic::resume_unwind(error);
        }
    }
}
