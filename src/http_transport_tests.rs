use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
            ))
        } else {
            tokio::spawn(serve_listener(tcp, router, stop))
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

#[tokio::test]
async fn incomplete_http_header_expires_without_stopping_healthy_requests() {
    use axum::routing::get;

    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        tcp,
        Router::new().route("/healthy", get(|| async { "ready" })),
        stop.clone(),
    ));

    let mut slow = TcpStream::connect(address).await.unwrap();
    slow.write_all(b"GET /healthy HTTP/1.1\r\nHost: localhost\r\nX-Hold:")
        .await
        .unwrap();
    let mut response = Vec::new();
    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        slow.read_to_end(&mut response),
    )
    .await;
    let healthy = reqwest::get(format!("http://{address}/healthy"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    stop.cancel();
    server.abort();
    let _ = server.await;

    assert!(closed.is_ok(), "an incomplete header must have a deadline");
    assert_eq!(healthy, "ready");
}

#[tokio::test]
async fn incomplete_https_header_expires_after_tls_handshake() {
    use axum::routing::get;

    let cert = tls_fixture::Certificates::new();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        crate::http_tls::Listener::new(tcp, cert.server()),
        Router::new().route("/healthy", get(|| async { "ready" })),
        stop.clone(),
    ));

    let socket = TcpStream::connect(address).await.unwrap();
    let connector = tokio_rustls::TlsConnector::from(cert.client());
    let mut slow = connector
        .connect(
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    slow.write_all(b"GET /healthy HTTP/1.1\r\nHost: localhost\r\nX-Hold:")
        .await
        .unwrap();
    let mut received = Vec::new();
    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        slow.read_to_end(&mut received),
    )
    .await;
    stop.cancel();
    server.abort();
    let _ = server.await;

    assert!(
        closed.is_ok(),
        "completed TLS must not disable the HTTP deadline"
    );
}

#[tokio::test]
async fn second_keepalive_request_gets_a_fresh_header_deadline() {
    use axum::routing::get;

    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener(
        tcp,
        Router::new().route("/healthy", get(|| async { "ready" })),
        stop.clone(),
    ));

    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(b"GET /healthy HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut first = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !first.windows(5).any(|bytes| bytes == b"ready") {
            let mut chunk = [0; 1024];
            let count = socket.read(&mut chunk).await.unwrap();
            assert!(count > 0, "first response closed unexpectedly");
            first.extend_from_slice(&chunk[..count]);
        }
    })
    .await
    .unwrap();
    socket
        .write_all(b"GET /healthy HTTP/1.1\r\nHost: localhost\r\nX-Hold:")
        .await
        .unwrap();
    let mut second = Vec::new();
    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        socket.read_to_end(&mut second),
    )
    .await;
    stop.cancel();
    server.abort();
    let _ = server.await;

    assert!(
        closed.is_ok(),
        "keep-alive request headers need a new deadline"
    );
}

#[tokio::test]
async fn established_connection_limit_rejects_excess_and_recovers_after_deadline() {
    use axum::routing::get;

    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve_listener_bounded(
        tcp,
        Router::new().route("/healthy", get(|| async { "ready" })),
        stop.clone(),
        1,
    ));

    let mut slow = TcpStream::connect(address).await.unwrap();
    slow.write_all(b"GET /healthy HTTP/1.1\r\nHost: localhost\r\nX-Hold:")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let mut excess = TcpStream::connect(address).await.unwrap();
    excess
        .write_all(b"GET /healthy HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut received = Vec::new();
    let closed = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        excess.read_to_end(&mut received),
    )
    .await;
    let mut ignored = Vec::new();
    let slow_expired = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        slow.read_to_end(&mut ignored),
    )
    .await;
    let healthy = reqwest::get(format!("http://{address}/healthy"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    stop.cancel();
    server.abort();
    let _ = server.await;

    assert!(closed.is_ok(), "excess connection must be closed promptly");
    assert!(
        received.is_empty(),
        "excess connection must not receive media"
    );
    assert!(
        slow_expired.is_ok(),
        "a partial request must release its slot"
    );
    assert_eq!(healthy, "ready");
}
