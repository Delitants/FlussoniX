//! Owned encrypted endpoints; not a sustained-link qualification.
use super::*;
use crate::fxlink::{Budget, Capabilities, ClientPool, Features, Receiver};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::sync::atomic::AtomicBool;

fn features() -> Features {
    Features {
        geometry_hints: false,
        trimmed_records: true,
        repair_parity: true,
        bootstrap_repair: true,
    }
}
fn record(id: u8, len: usize) -> Bytes {
    let mut b = vec![id; len];
    b[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(b)
}
struct Feed(Vec<Bytes>);
impl Provider for Feed {
    fn subscribe(&self, r: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let records = self.0.clone();
        Box::pin(async move {
            if r.peer_key != "owned-peer" || r.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::iter(records.into_iter().map(Ok))
                        .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
fn forced_server(cert: &test_tls::Certificates, version: u8) -> quinn::ServerConfig {
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport_config());
    config
}
async fn raw_client(
    address: SocketAddr,
    cert: &test_tls::Certificates,
    version: u8,
) -> (quinn::Endpoint, quinn::Connection) {
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport_config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let c = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    assert_eq!(control::version(&c).unwrap(), version);
    (endpoint, c)
}
#[tokio::test]
async fn bootstrap_optin_and_fallback_matrix() {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    for version in 1..=8 {
        for old_client in [false, true] {
            let cert = test_tls::Certificates::new();
            let expected = record(8, 9781);
            let listener = Listener::bind_with_features(
                "127.0.0.1:0".parse().unwrap(),
                &cert.cert,
                &cert.key,
                Settings::default(),
                features(),
            )
            .unwrap();
            if !old_client {
                listener
                    .endpoint
                    .set_server_config(Some(forced_server(&cert, version)));
            }
            let address = listener.address();
            let stop = CancellationToken::new();
            let server =
                tokio::spawn(listener.serve(Arc::new(Feed(vec![expected.clone()])), stop.clone()));
            if old_client {
                let (endpoint, c) = raw_client(address, &cert, version).await;
                let (mut send, mut recv) = c.open_bi().await.unwrap();
                let id = send.id().index();
                control::write(
                    &mut send,
                    &Control::Subscribe {
                        version,
                        id,
                        generation: 1,
                        profile: control::profile(&Settings::default()),
                        name: "owned".into(),
                        peer_key: "owned-peer".into(),
                        token: "owned-viewer".into(),
                    },
                )
                .await
                .unwrap();
                assert!(matches!(
                    control::read(&mut recv).await.unwrap(),
                    Control::Accepted
                ));
                let budget = Budget::new(2 << 20);
                let mut receiver = Receiver::new(Settings::default(), id, 1, budget.clone())
                    .unwrap()
                    .with_capabilities(Capabilities::for_version(version));
                let got = tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let p = c.read_datagram().await.unwrap();
                        let out = receiver.ingest_charged(&p, 0).unwrap();
                        if !out.is_empty() {
                            break out;
                        }
                    }
                })
                .await
                .unwrap();
                assert_eq!(got.len(), 1);
                assert_eq!(got[0].bytes, expected);
                drop((got, receiver));
                assert_eq!(budget.used(), 0);
                c.close(0u32.into(), b"done");
                endpoint.wait_idle().await;
            } else {
                let pool = ClientPool::with_features(features());
                let url = format!("fxlink://{address}/owned");
                let mut sub = pool
                    .subscribe(
                        &url,
                        "owned-peer",
                        "owned-viewer",
                        Some(&cert.ca),
                        Settings::default(),
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    crate::fxlink::client::negotiated_version_for_test(&sub),
                    version
                );
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), sub.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                    expected
                );
                drop(sub);
                pool.close().await;
            }
            stop.cancel();
            server.await.unwrap().unwrap();
            assert_eq!(
                global().used(),
                baseline,
                "version{version} oldclient{old_client}"
            );
        }
    }
}

// One-shot owned loss injection: suppress only record1's original packets and
// its announcement; then turn off before repairs. Normal builds have no hook.
static OMIT: AtomicBool = AtomicBool::new(false);
static REQUESTS: AtomicU64 = AtomicU64::new(0);
static REPLIES: AtomicU64 = AtomicU64::new(0);
static GRANT: AtomicU64 = AtomicU64::new(0);
pub(super) fn omit_announcement(id: u64) -> bool {
    id <= 1 && OMIT.load(Ordering::Acquire)
}
pub(super) fn omit_packet(h: &Header) -> bool {
    if h.record_id > 1 || !OMIT.load(Ordering::Acquire) {
        return false;
    }
    if h.record_id == 1 && h.shard_index + 1 == h.data_count + h.parity_count {
        OMIT.store(false, Ordering::Release);
    }
    true
}
pub(super) fn request(block: u64, limit: u16) {
    if block == 2048 {
        REQUESTS.fetch_add(1, Ordering::Relaxed);
        GRANT.store(u64::from(limit), Ordering::Relaxed);
    }
}
pub(super) fn reply(block: u64) {
    if block == 2048 {
        REPLIES.fetch_add(1, Ordering::Relaxed);
    }
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        OMIT.store(false, Ordering::Release);
    }
}
#[tokio::test]
async fn bootstrap_production_client_and_listener_repair_unknown_middle() {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let _reset = Reset;
    let progress = crate::fxlink::client::BootstrapProgressGuard::new();
    let cert = test_tls::Certificates::new();
    let expected = [
        record(0, 4835),
        record(1, 9781),
        record(2, 20000),
        record(3, 20000),
        record(4, 20000),
    ];
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    REQUESTS.store(0, Ordering::Release);
    REPLIES.store(0, Ordering::Release);
    GRANT.store(0, Ordering::Release);
    OMIT.store(true, Ordering::Release);
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
        features(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Feed(expected.to_vec())), stop.clone()));
    let pool = ClientPool::with_features(features());
    async {
        let url = format!("fxlink://{address}/owned");
        assert!(
            pool.subscribe(
                &url,
                "bad-peer",
                "owned-viewer",
                Some(&cert.ca),
                settings.clone(),
                CancellationToken::new()
            )
            .await
            .is_err()
        );
        assert!(
            pool.subscribe(
                &url,
                "owned-peer",
                "bad-viewer",
                Some(&cert.ca),
                settings.clone(),
                CancellationToken::new()
            )
            .await
            .is_err()
        );
        let mut sub = pool
            .subscribe(
                &url,
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                settings.clone(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(crate::fxlink::client::negotiated_version_for_test(&sub), 8);
        for wire in &expected {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), sub.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                *wire
            );
        }
        let mut sibling = pool
            .subscribe(
                &format!("fxlink://{address}/sibling"),
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                settings.clone(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(sub.connection_id(), sibling.connection_id());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), sibling.recv())
                .await
                .unwrap()
                .unwrap(),
            expected[0]
        );
        assert!(sub.current() && sibling.current());
        tokio::time::timeout(Duration::from_millis(100), async {
            while !progress.entered() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(REQUESTS.load(Ordering::Acquire), 1);
        assert_eq!(GRANT.load(Ordering::Acquire), 20);
        assert_eq!(REPLIES.load(Ordering::Acquire), 11);
    }
    .await;
    drop(progress);
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn bootstrap_tls_rejection_does_not_damage_healthy_subscription() {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let other = test_tls::Certificates::new();
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
        features(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Feed(vec![record(1, 600)])), stop.clone()));
    let pool = ClientPool::with_features(features());
    let url = format!("fxlink://{address}/owned");
    assert!(
        pool.subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&other.ca),
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![b"fxlink/8".to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    assert!(
        endpoint
            .connect(address, "wrong-owned.invalid")
            .unwrap()
            .await
            .is_err()
    );
    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    let mut sub = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap(),
        record(1, 600)
    );
    drop(sub);
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(global().used(), baseline);
}
