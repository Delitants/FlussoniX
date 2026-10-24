use super::*;
use crate::fxlink::{ClientPool, transport};
use futures_util::future::BoxFuture;
use std::sync::Arc;

#[tokio::test]
async fn unvalidated_quic_initial_receives_retry_before_preauth_accounting() {
    use tokio::net::UdpSocket;

    let _serial = transport::test_lock().lock().await;
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Ready), stop.clone()));

    let proxy = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = control::protocols();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    let pending = endpoint
        .connect(proxy.local_addr().unwrap(), "localhost")
        .unwrap();
    let mut packet = [0u8; 1500];
    let (len, _client) = tokio::time::timeout(Duration::from_secs(2), proxy.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    proxy.send_to(&packet[..len], address).await.unwrap();
    let (len, _) = tokio::time::timeout(Duration::from_secs(2), proxy.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    assert!(len > 1);
    assert_eq!(
        packet[0] & 0xf0,
        0xf0,
        "server must issue QUIC Retry before charging a spoofable address"
    );

    drop(pending);
    endpoint.close(0u32.into(), b"done");
    stop.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn preauth_lifetime_includes_connection_setup_delay() {
    let _serial = transport::test_lock().lock().await;
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let gate = PreAuthGate::new();
    let lease = gate.acquire("127.0.0.1".parse().unwrap()).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = control::protocols();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    let client = endpoint.connect(listener.address(), "localhost").unwrap();
    let server = async { listener.endpoint.accept().await.unwrap().await.unwrap() };
    let (client, server) = tokio::join!(client, server);
    let client = client.unwrap();
    let budget = peer_budget(server.remote_address().ip()).unwrap();
    let _queues = budget.reserve(QUEUE_BYTES).unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve_peer(
        server,
        Arc::new(Ready),
        Settings::default(),
        budget,
        stop.clone(),
        Arc::new(AtomicU64::new(0)),
        Some(lease),
    ));
    let closed = tokio::time::timeout(Duration::from_secs(19), client.closed())
        .await
        .is_ok();
    stop.cancel();
    client.close(0u32.into(), b"done");
    task.await.unwrap();
    endpoint.close(0u32.into(), b"done");
    listener.endpoint.close(0u32.into(), b"done");
    assert!(
        closed,
        "the setup delay extended the pre-auth lease beyond 20 seconds"
    );
}

struct Ready;
impl Provider for Ready {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-peer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::pending()),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}

#[tokio::test]
async fn idle_quic_handshakes_cannot_occupy_every_fxlink_connection_slot() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Ready), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = control::protocols();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport::config());
    let mut endpoints = Vec::new();
    let mut held = Vec::new();
    for ip in ["127.0.0.2", "127.0.0.3"] {
        let mut endpoint = quinn::Endpoint::client(format!("{ip}:0").parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config.clone());
        for _ in 0..32 {
            if let Ok(Ok(connection)) = tokio::time::timeout(
                Duration::from_secs(2),
                endpoint.connect(address, "localhost").unwrap(),
            )
            .await
            {
                held.push(connection);
            }
        }
        endpoints.push(endpoint);
    }
    assert!(held.len() >= 8, "test must occupy real preauth QUIC slots");
    let pool = ClientPool::default();
    let healthy = tokio::time::timeout(
        Duration::from_secs(3),
        pool.subscribe(
            &format!("fxlink://{address}/owned"),
            "owned-peer",
            "viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        ),
    )
    .await;
    assert!(
        matches!(healthy.as_ref(), Ok(Ok(_))),
        "legitimate FXLink subscription starved by {} idle handshakes",
        held.len()
    );
    drop(healthy);
    pool.close().await;
    for connection in held {
        connection.close(0u32.into(), b"done");
    }
    for endpoint in endpoints {
        endpoint.close(0u32.into(), b"done");
        endpoint.wait_idle().await;
    }
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
}

#[tokio::test]
async fn accepted_subscriptions_release_preauth_share_for_same_address() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Ready), stop.clone()));
    let mut owners = Vec::new();
    for index in 0..6 {
        let pool = ClientPool::default();
        let subscription = tokio::time::timeout(
            Duration::from_secs(3),
            pool.subscribe(
                &format!("fxlink://{address}/owned-{index}"),
                "owned-peer",
                "viewer",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("bounded valid setup")
        .expect("valid subscription");
        owners.push((pool, subscription));
    }
    for (pool, subscription) in owners {
        drop(subscription);
        pool.close().await;
    }
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
}

#[tokio::test]
async fn idle_fxlink_handshake_closes_after_absolute_preauth_lifetime() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Ready), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = control::protocols();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport::config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(25), connection.closed())
            .await
            .is_ok(),
        "idle connection outlived absolute preauth deadline despite QUIC keepalives"
    );
    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
}
