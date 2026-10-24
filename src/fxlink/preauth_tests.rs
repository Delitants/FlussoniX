use super::*;
use crate::fxlink::{ClientPool, transport};
use futures_util::future::BoxFuture;
use std::sync::Arc;

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
