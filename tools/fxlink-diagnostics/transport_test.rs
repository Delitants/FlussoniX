//! Installed only into the private source copy. Own ephemeral loopback sockets; no FFmpeg.
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use fx_packet_trace::{stage, Recorder};
use quinn::{AsyncUdpSocket, Runtime, UdpPoller};
use std::{
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
#[derive(Debug, Default)]
struct Gate {
    mode: AtomicUsize,
    waker: Mutex<Option<Waker>>,
}
impl Gate {
    fn release(&self) {
        self.mode.store(0, Ordering::SeqCst);
        if let Some(w) = self.waker.lock().unwrap().take() {
            w.wake();
        }
    }
}
#[derive(Debug)]
struct Socket {
    inner: Arc<dyn AsyncUdpSocket>,
    gate: Arc<Gate>,
}
#[derive(Debug)]
struct Poller {
    inner: Pin<Box<dyn UdpPoller>>,
    gate: Arc<Gate>,
}
impl UdpPoller for Poller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.gate.mode.load(Ordering::SeqCst) == 2 {
            *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        self.inner.as_mut().poll_writable(cx)
    }
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Poller {
            inner: self.inner.clone().create_io_poller(),
            gate: self.gate.clone(),
        })
    }
    fn try_send(&self, t: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        if self
            .gate
            .mode
            .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.inner.try_send(t)
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, buf, meta)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}
async fn wait_for(r: &Recorder, stage: u16, key: [u8; 32]) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if r.snapshot()
                .events
                .iter()
                .any(|e| e.stage == stage && e.key == key)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("missing real boundary hook");
}
fn key(data: &[u8]) -> [u8; 32] {
    fx_packet_trace::digest(data)
}
#[tokio::test(flavor = "current_thread")]
async fn real_transport_boundaries() {
    tokio::time::timeout(Duration::from_secs(20), run())
        .await
        .unwrap();
}
async fn run() {
    let r = Arc::new(Recorder::new(131072));
    assert!(fx_packet_trace::install(r.clone()));
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert_der = cert.cert.der().clone();
    let server_tls = quinn::rustls::ServerConfig::builder_with_provider(Arc::new(
        quinn::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert_der.clone()],
        quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into()),
    )
    .unwrap();
    let mut server_cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_tls).unwrap(),
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.datagram_receive_buffer_size(Some(2 * (482 + 32)));
    server_cfg.transport_config(Arc::new(transport));
    let server = quinn::Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let tls = quinn::rustls::ClientConfig::builder_with_provider(Arc::new(
        quinn::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    let runtime = Arc::new(quinn::TokioRuntime);
    let gate = Arc::new(Gate::default());
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let inner = runtime.wrap_udp_socket(sock).unwrap();
    let socket = Arc::new(Socket {
        inner,
        gate: gate.clone(),
    });
    let mut client =
        quinn::Endpoint::new_with_abstract_socket(Default::default(), None, socket, runtime)
            .unwrap();
    client.set_default_client_config(config);
    let (a, b) = tokio::join!(
        client
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap(),
        async { server.accept().await.unwrap().await }
    );
    let a = a.unwrap();
    let b = b.unwrap();
    // Real Quinn runtime gets WouldBlock, retains ciphertext, then its poller reports Pending.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let data = Bytes::from(vec![81; 482]);
    gate.mode.store(1, Ordering::SeqCst);
    a.send_datagram_wait(data.clone()).await.unwrap();
    wait_for(&r, stage::FRAME, key(&data)).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let s = r.snapshot();
            if s.events.iter().any(|e| e.stage == stage::WOULD_BLOCK)
                && s.events.iter().any(|e| e.stage == stage::PENDING)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let snapshot = r.snapshot();
    let blocked = snapshot
        .events
        .iter()
        .rev()
        .find(|e| e.stage == stage::WOULD_BLOCK)
        .unwrap()
        .key;
    assert!(!snapshot
        .events
        .iter()
        .any(|e| e.stage == stage::ACCEPTED && e.key == blocked));
    assert!(b.read_datagram().now_or_never().is_none());
    gate.release();
    assert_eq!(b.read_datagram().await.unwrap(), data);
    wait_for(&r, stage::INGRESS, blocked).await;
    wait_for(&r, stage::ACCEPTED, blocked).await;
    wait_for(&r, stage::DEQUEUE, key(&data)).await;
    // Receive-buffer overflow is explicit, before application dequeue.
    for marker in 90..93 {
        a.send_datagram_wait(Bytes::from(vec![marker; 482]))
            .await
            .unwrap();
    }
    wait_for(&r, stage::RX_DROP, key(&vec![90; 482])).await;
    assert_eq!(b.read_datagram().await.unwrap(), Bytes::from(vec![91; 482]));
    assert_eq!(b.read_datagram().await.unwrap(), Bytes::from(vec![92; 482]));
    // Identical data/repair copies retain distinct packet numbers at the real hook.
    let same = Bytes::from(vec![73; 482]);
    for _ in 0..3 {
        a.send_datagram_wait(same.clone()).await.unwrap();
        assert_eq!(b.read_datagram().await.unwrap(), same);
    }
    let frames = r
        .snapshot()
        .events
        .into_iter()
        .filter(|e| e.stage == stage::FRAME && e.key == key(&same))
        .collect::<Vec<_>>();
    assert_eq!(frames.len(), 3);
    assert_eq!(
        frames
            .iter()
            .map(|e| e.numbers[0])
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
    a.close(0u32.into(), b"done");
    b.close(0u32.into(), b"done");
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    client.wait_idle().await;
    server.wait_idle().await;
    // Full FXLink v7 round trip: admission and dispatcher hooks are exercised in their real locations.
    struct Feed(Bytes);
    impl flussonix::fxlink::Provider for Feed {
        fn subscribe(
            &self,
            _: flussonix::fxlink::Request,
            _: SocketAddr,
        ) -> futures_util::future::BoxFuture<'_, Result<flussonix::fxlink::Source, u16>> {
            let data = self.0.clone();
            Box::pin(async move {
                Ok(flussonix::fxlink::Source {
                    records: Box::pin(
                        futures_util::stream::iter([Ok(data)])
                            .chain(futures_util::stream::pending()),
                    ),
                    pending_records: None,
                    current: Arc::new(|| true),
                    cancelled: tokio_util::sync::CancellationToken::new(),
                })
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let settings = flussonix::fxlink::Settings {
        latency_ms: 500,
        ..Default::default()
    };
    let features = flussonix::fxlink::Features {
        trimmed_records: true,
        repair_parity: true,
    };
    let listener = flussonix::fxlink::Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert_path,
        &key_path,
        settings.clone(),
        features,
    )
    .unwrap();
    let addr = listener.address();
    let mut record = vec![55; 398];
    record[..4].copy_from_slice(&394u32.to_be_bytes());
    let record = Bytes::from(record);
    let stop = tokio_util::sync::CancellationToken::new();
    let serving = tokio::spawn(listener.serve(Arc::new(Feed(record.clone())), stop.clone()));
    let pool = flussonix::fxlink::ClientPool::with_features(features);
    let mut sub = pool
        .subscribe(
            &format!("fxlink://127.0.0.1:{}/owned", addr.port()),
            "private-peer-token",
            "private-viewer-token",
            Some(&cert_path),
            settings,
            stop.child_token(),
        )
        .await
        .unwrap();
    assert_eq!(sub.recv().await.unwrap(), record);
    assert_eq!(sub.telemetry().snapshot()["protocol_version"], 7);
    let s = r.snapshot();
    let admits = s
        .events
        .iter()
        .filter(|e| e.stage == stage::ADMIT)
        .collect::<Vec<_>>();
    assert!(admits.iter().any(|e| e.len == 482));
    assert!(admits.iter().any(|a| s
        .events
        .iter()
        .any(|e| e.stage == stage::DISPATCH && e.key == a.key)));
    drop(sub);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    let s = r.snapshot();
    assert_eq!(s.busy, 0);
    assert_eq!(s.overwritten, 0);
    let path = std::env::var_os("FX_TRACE_TEST_OUTPUT").expect("private test output path");
    r.close_and_dump(std::path::Path::new(&path)).unwrap();
}
