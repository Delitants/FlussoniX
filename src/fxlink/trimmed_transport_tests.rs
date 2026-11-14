//! Real TLS/QUIC compatibility, pooling and source authorization.
use super::*;
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
fn native() -> Bytes {
    let mut b = vec![0x73; 401];
    b[..4].copy_from_slice(&397u32.to_be_bytes());
    Bytes::from(b)
}
struct Feed {
    start: CancellationToken,
    calls: Arc<AtomicUsize>,
}
impl Provider for Feed {
    fn subscribe(&self, r: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let start = self.start.clone();
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            if r.peer_key != "owned-peer" || r.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::once(async move {
                        start.cancelled().await;
                        Ok(native())
                    })
                    .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
fn pool(enabled: bool) -> ClientPool {
    ClientPool::with_features(Features {
        trimmed_records: enabled,
        ..Default::default()
    })
}
#[tokio::test]
async fn trimmed_transport_tests_both_opt_in_negotiate_v6_and_preserve_auth_and_pooling() {
    mode_case(true, true).await;
}
#[tokio::test]
async fn trimmed_transport_tests_mixed_and_default_peers_keep_v5_bytes() {
    for (server, client) in [(false, false), (true, false), (false, true)] {
        mode_case(server, client).await;
    }
}
async fn mode_case(server_enabled: bool, client_enabled: bool) {
    let _serial = super::transport::test_lock().lock().await;
    let baseline = super::transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
        Features {
            trimmed_records: server_enabled,
            ..Default::default()
        },
    )
    .unwrap();
    let egress = listener.egress();
    let address = listener.address();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(listener.serve(
        Arc::new(Feed {
            start: start.clone(),
            calls: calls.clone(),
        }),
        stop.clone(),
    ));
    let pool = pool(client_enabled);
    let url = format!("fxlink://{address}/owned-one");
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
    let version = super::client::negotiated_version_for_test(&sub);
    let id = sub.connection_id();
    let bad = pool
        .subscribe(
            &url,
            "owned-peer",
            "bad-token",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await;
    let denied = bad.is_err();
    drop(bad);
    start.cancel();
    let first = tokio::time::timeout(Duration::from_secs(2), sub.recv()).await;
    let mut second = pool
        .subscribe(
            &format!("fxlink://{address}/owned-two"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let same_connection = id == second.connection_id();
    let second_version = super::client::negotiated_version_for_test(&second);
    let next = tokio::time::timeout(Duration::from_secs(2), second.recv()).await;
    let healthy = sub.current() && second.current();
    let states = [sub.telemetry(), second.telemetry()];
    let snapshots = [sub.telemetry().snapshot(), second.telemetry().snapshot()];
    drop((sub, second));
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(super::transport::global().used(), baseline);
    assert!(states.iter().all(|s| s.snapshot()["state_bytes"] == 0));
    let expected = if server_enabled && client_enabled {
        6
    } else {
        5
    };
    assert_eq!(version, expected, "highest mutually enabled ALPN");
    assert_eq!(second_version, expected);
    assert!(snapshots.iter().all(|s| s["protocol_version"] == expected));
    assert!(
        snapshots
            .iter()
            .all(|s| s["trimmed_shards"] == if expected == 6 { 1 } else { 0 })
    );
    assert_eq!(first.unwrap().unwrap(), native());
    assert_eq!(next.unwrap().unwrap(), native());
    assert!(denied && healthy && same_connection);
    assert_eq!(
        calls.load(Ordering::Acquire),
        3,
        "denial triggered another source attempt"
    );
    assert_eq!(
        egress.load(Ordering::Acquire),
        2 * if expected == 6 { 485 } else { 1088 }
    );
}

#[tokio::test]
async fn trimmed_transport_tests_v6_client_falls_back_to_each_older_alpn() {
    let _serial = super::transport::test_lock().lock().await;
    let baseline = super::transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    for expected in 1..=4u8 {
        let mut tls = (*cert.server()).clone();
        tls.alpn_protocols = vec![format!("fxlink/{expected}").into_bytes()];
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(super::transport::config());
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let url = format!("fxlink://{}/owned-old", endpoint.local_addr().unwrap());
        let stop = CancellationToken::new();
        let server = {
            let endpoint = endpoint.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let c = endpoint.accept().await.unwrap().await.unwrap();
                let version = super::control::version(&c).unwrap();
                let (mut send, mut recv) = c.accept_bi().await.unwrap();
                let super::control::Control::Subscribe {
                    version: requested,
                    id,
                    peer_key,
                    token,
                    ..
                } = super::control::read(&mut recv).await.unwrap()
                else {
                    panic!("subscribe")
                };
                super::control::write(&mut send, &super::control::Control::Accepted)
                    .await
                    .unwrap();
                let mut e = Encoder::new(Settings::default(), id, 1, Budget::new(2 << 20)).unwrap();
                let encoded = e.encode(native(), 0).unwrap();
                super::control::write(
                    &mut send,
                    &super::control::Control::Announce {
                        record: 0,
                        len: 401,
                    },
                )
                .await
                .unwrap();
                c.send_datagram_wait(encoded.datagrams[0].clone())
                    .await
                    .unwrap();
                stop.cancelled().await;
                (
                    version,
                    requested,
                    peer_key,
                    token,
                    encoded.datagrams[0].len(),
                )
            })
        };
        let pool = pool(true);
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
        let version = super::client::negotiated_version_for_test(&sub);
        let result = tokio::time::timeout(Duration::from_secs(2), sub.recv()).await;
        drop(sub);
        pool.close().await;
        stop.cancel();
        let report = server.await.unwrap();
        endpoint.close(0u32.into(), b"done");
        endpoint.wait_idle().await;
        assert_eq!(result.unwrap().unwrap(), native());
        assert_eq!(version, expected);
        assert_eq!(
            report,
            (
                expected,
                expected,
                "owned-peer".into(),
                "owned-viewer".into(),
                1088
            )
        );
        assert_eq!(super::transport::global().used(), baseline);
    }
}

#[tokio::test]
async fn trimmed_transport_tests_v5_route_rejects_trimmed_without_harming_sibling() {
    let _serial = super::transport::test_lock().lock().await;
    let baseline = super::transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/5".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(super::transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let start = CancellationToken::new();
    let stop = CancellationToken::new();
    let server = {
        let endpoint = endpoint.clone();
        let start = start.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let c = endpoint.accept().await.unwrap().await.unwrap();
            let mut sends = Vec::new();
            let mut packets = Vec::new();
            for cap in [Capabilities::V6, Capabilities::V5] {
                let (mut send, mut recv) = c.accept_bi().await.unwrap();
                let super::control::Control::Subscribe { id, version, .. } =
                    super::control::read(&mut recv).await.unwrap()
                else {
                    panic!("subscribe")
                };
                assert_eq!(version, 5);
                super::control::write(&mut send, &super::control::Control::Accepted)
                    .await
                    .unwrap();
                let mut e = Encoder::new(Settings::default(), id, 1, Budget::new(2 << 20))
                    .unwrap()
                    .with_capabilities(cap);
                packets.push(e.encode(native(), 0).unwrap());
                sends.push((send, recv));
            }
            start.cancelled().await;
            for p in &packets {
                c.send_datagram_wait(p.datagrams[0].clone()).await.unwrap();
            }
            stop.cancelled().await;
        })
    };
    let pool = pool(true);
    let url = format!("fxlink://{}/owned-route", endpoint.local_addr().unwrap());
    let mut bad = pool
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
    let mut good = pool
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
    let same = bad.connection_id() == good.connection_id();
    start.cancel();
    let failed = tokio::time::timeout(Duration::from_secs(2), bad.recv()).await;
    let passed = tokio::time::timeout(Duration::from_secs(2), good.recv()).await;
    let healthy = good.current();
    let bad_stats = bad.telemetry();
    drop((bad, good));
    pool.close().await;
    stop.cancel();
    server.await.unwrap();
    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    assert_eq!(super::transport::global().used(), baseline);
    assert!(failed.unwrap().is_err());
    assert_eq!(passed.unwrap().unwrap(), native());
    assert!(healthy && same);
    assert_eq!(bad_stats.snapshot()["received_data_bytes"], 0);
    assert_eq!(bad_stats.snapshot()["state_bytes"], 0);
}

fn indexed_native(index: u8) -> Bytes {
    let mut bytes = native().to_vec();
    bytes[8] = 0x73u8.wrapping_add(index);
    Bytes::from(bytes)
}
struct BurstFeed(CancellationToken);
impl Provider for BurstFeed {
    fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let start = self.0.clone();
        Box::pin(async move {
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::once(async move {
                        start.cancelled().await;
                    })
                    .flat_map(|_| {
                        futures_util::stream::iter((0..16).map(|n| Ok(indexed_native(n))))
                    })
                    .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
#[tokio::test]
async fn trimmed_transport_tests_udp_loss_uses_cached_trimmed_repairs_with_actual_byte_credit() {
    use std::sync::atomic::AtomicBool;
    let _serial = super::transport::test_lock().lock().await;
    let baseline = super::transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
        Features {
            trimmed_records: true,
            ..Default::default()
        },
    )
    .unwrap();
    let remote = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(BurstFeed(start.clone())), stop.clone()));
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let arm = armed.clone();
    let relay_stop = CancellationToken::new();
    let cancel = relay_stop.clone();
    let relay = tokio::spawn(async move {
        let mut client = None;
        let mut dropped = Vec::new();
        let mut buffer = [0u8; 65536];
        loop {
            tokio::select! {
                _=cancel.cancelled()=>break,
                received=socket.recv_from(&mut buffer)=>{
                    let (len,from)=received.unwrap();let to=if from==remote {client.unwrap()} else {client=Some(from);remote};
                    if from==remote && len>=500 && arm.swap(false,Ordering::AcqRel) {dropped.push(len);continue;}
                    socket.send_to(&buffer[..len],to).await.unwrap();
                }
            }
        }
        dropped
    });
    let pool = pool(true);
    let mut sub = pool
        .subscribe(
            &format!("fxlink://{address}/owned-loss"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    armed.store(true, Ordering::Release);
    start.cancel();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut received = Vec::new();
        for _ in 0..16 {
            received.push(sub.recv().await?);
        }
        Ok::<_, String>(received)
    })
    .await;
    let healthy = sub.current();
    let stats = sub.telemetry().snapshot();
    let sent = egress.load(Ordering::Acquire);
    let state = sub.telemetry();
    drop(sub);
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    relay_stop.cancel();
    let dropped = relay.await.unwrap();
    assert_eq!(super::transport::global().used(), baseline);
    assert_eq!(state.snapshot()["state_bytes"], 0);
    assert_eq!(
        dropped.len(),
        1,
        "loss fixture did not drop exactly one encrypted UDP packet"
    );
    assert_eq!(
        result.unwrap().unwrap(),
        (0..16).map(indexed_native).collect::<Vec<_>>()
    );
    assert!(healthy);
    assert!(
        stats["repair_requested_shards"].as_u64().unwrap() > 0,
        "reliable-only packet loss did not exercise native repair"
    );
    assert_eq!(stats["fec_recovered_shards"], 0);
    assert_eq!(stats["expired_records"], 0);
    assert!(sent > 16 * 485, "no cached repair was admitted");
    assert!(
        sent - 16 * 485 <= 16 * 485 / 4,
        "removed padding granted excess repair credit"
    );
    assert_eq!(
        sent % 485,
        0,
        "repair inflated back to fixed-width legacy encoding"
    );
}

#[tokio::test]
async fn trimmed_transport_tests_removed_padding_cannot_fund_a_fifth_cached_repair() {
    let _serial = super::transport::test_lock().lock().await;
    let baseline = super::transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
        Features {
            trimmed_records: true,
            ..Default::default()
        },
    )
    .unwrap();
    let address = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(BurstFeed(start.clone())), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![b"fxlink/6".to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(super::transport::config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    let id = send.id().index();
    super::control::write(
        &mut send,
        &super::control::Control::Subscribe {
            version: 6,
            id,
            generation: 1,
            profile: super::control::profile(&Settings::default()),
            name: "owned-credit".into(),
            peer_key: "owned-peer".into(),
            token: "owned-viewer".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        super::control::read(&mut recv).await.unwrap(),
        super::control::Control::Accepted
    ));
    start.cancel();
    let originals = tokio::time::timeout(Duration::from_secs(1), async {
        let mut packets = Vec::new();
        for _ in 0..16 {
            packets.push(connection.read_datagram().await.unwrap());
        }
        packets
    })
    .await
    .unwrap();
    let requests: Vec<_> = (0..5)
        .map(|n| RepairRequest {
            block_id: n * 2048,
            shard_indices: vec![0],
        })
        .collect();
    let mut sequence = 1;
    for frame in
        super::feedback::encode_sequenced(id, &requests, &Settings::default(), &mut sequence)
            .unwrap()
    {
        connection.send_datagram(frame).unwrap();
    }
    let repaired = tokio::time::timeout(Duration::from_millis(500), async {
        let mut packets = Vec::new();
        for _ in 0..4 {
            packets.push(connection.read_datagram().await.unwrap());
        }
        packets
    })
    .await;
    let extra = tokio::time::timeout(Duration::from_millis(150), connection.read_datagram()).await;
    let sent = egress.load(Ordering::Acquire);
    connection.close(0u32.into(), b"done");
    drop((send, recv));
    endpoint.wait_idle().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(super::transport::global().used(), baseline);
    assert!(
        originals
            .iter()
            .all(|p| p.len() == 485 && packet::Header::parse(p).unwrap().layout == Layout::Trimmed)
    );
    let repaired = repaired.unwrap();
    assert_eq!(repaired.len(), 4);
    assert!(
        repaired
            .iter()
            .all(|p| p.len() == 485 && packet::Header::parse(p).unwrap().layout == Layout::Trimmed)
    );
    assert!(
        extra.is_err(),
        "a fifth485-byte repair exceeded1940bytes of actual credit"
    );
    assert_eq!(sent, 16 * 485 + 1940);
}

#[tokio::test]
async fn trimmed_transport_tests_one_encrypted_udp_loss_removes_multiple_native_records() {
    use std::sync::atomic::AtomicBool;
    let _serial = super::transport::test_lock().lock().await;
    let baseline = super::transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/6".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    cfg.transport_config(super::transport::config());
    let endpoint = quinn::Endpoint::server(cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
    let remote = endpoint.local_addr().unwrap();
    let media = CancellationToken::new();
    let repair_seen = CancellationToken::new();
    let release = CancellationToken::new();
    let stop = CancellationToken::new();
    let server = {
        let endpoint = endpoint.clone();
        let media = media.clone();
        let repair_seen = repair_seen.clone();
        let release = release.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let c = endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = c.accept_bi().await.unwrap();
            let super::control::Control::Subscribe {
                id,
                version,
                peer_key,
                token,
                ..
            } = super::control::read(&mut recv).await.unwrap()
            else {
                panic!("subscribe")
            };
            assert_eq!(version, 6);
            assert_eq!(peer_key, "owned-peer");
            assert_eq!(token, "owned-viewer");
            let budget = Budget::new(2 << 20);
            let routes = super::feedback::Routes::default();
            let mut route = routes.admit(id, 6, &budget).unwrap();
            super::control::write(&mut send, &super::control::Control::Accepted)
                .await
                .unwrap();
            media.cancelled().await;
            let created = super::transport::now();
            let mut encoder = Encoder::new(Settings::default(), id, 1, budget)
                .unwrap()
                .with_capabilities(Capabilities::V6);
            let cache: Vec<_> = (0..16)
                .map(|n| encoder.encode(indexed_native(n), created).unwrap())
                .collect();
            let mut allowance = super::repair::Allowance::new(created, 1000);
            for record in &cache {
                super::control::write(
                    &mut send,
                    &super::control::Control::Announce {
                        record: record.record_id,
                        len: 401,
                    },
                )
                .await
                .unwrap();
            }
            // Queue the complete bounded batch before yielding to the QUIC driver.
            for record in &cache {
                let packet = &record.datagrams[0];
                c.send_datagram(packet.clone()).unwrap();
                assert!(allowance.original(created, super::transport::now(), packet.len()));
            }
            let mut repaired = Vec::new();
            let feedback_settings = Settings::default();
            loop {
                tokio::select! {
                    _=stop.cancelled()=>break,
                    packet=c.read_datagram()=>{let Ok(packet)=packet else {break};routes.receive(packet,super::transport::now());},
                    request=route.next(&feedback_settings)=>{
                        let crate::fxlink::feedback::DatagramRequest::Ordinary(crate::fxlink::RepairRequest {block_id:block,shard_indices:shards})=request else {panic!("repair")};
                        repair_seen.cancel();tokio::select!{_=stop.cancelled()=>break,_=release.cancelled()=>{}}
                        if block%2048==0 && shards.contains(&0) && let Some(record)=cache.get((block/2048) as usize) {
                            let packet=&record.datagrams[0];
                            if allowance.repair(created,super::transport::now(),packet.len()) {
                                c.send_datagram_wait(packet.clone()).await.unwrap();repaired.push(record.record_id);
                            }
                        }
                    }
                }
            }
            repaired
        })
    };
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let arm = armed.clone();
    let relay_stop = CancellationToken::new();
    let cancel = relay_stop.clone();
    let relay = tokio::spawn(async move {
        let mut client = None;
        let mut eligible = 0;
        let mut dropped = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            tokio::select! {_=cancel.cancelled()=>break,r=socket.recv_from(&mut buf)=>{
                let (len,from)=r.unwrap();let to=if from==remote {client.unwrap()}else{client=Some(from);remote};
                if from==remote && arm.load(Ordering::Acquire) && len>=900 {
                    eligible+=1;
                    if eligible==2 {dropped.push(len);arm.store(false,Ordering::Release);continue;}
                }
                socket.send_to(&buf[..len],to).await.unwrap();
            }}
        }
        dropped
    });
    let pool = pool(true);
    let mut sub = pool
        .subscribe(
            &format!("fxlink://{address}/owned-coalesced"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    armed.store(true, Ordering::Release);
    media.cancel();
    let observed = tokio::time::timeout(Duration::from_millis(500), async {
        repair_seen.cancelled().await;
        loop {
            if sub.telemetry().snapshot()["trimmed_shards"] == 14 {
                break;
            }
            if !sub.current() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let before = sub.telemetry().snapshot();
    release.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut out = Vec::new();
        for _ in 0..16 {
            out.push(sub.recv().await?);
        }
        Ok::<_, String>(out)
    })
    .await;
    let healthy = sub.current();
    let stats = sub.telemetry().snapshot();
    drop(sub);
    pool.close().await;
    stop.cancel();
    let repairs = server.await.unwrap();
    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    relay_stop.cancel();
    let dropped = relay.await.unwrap();
    assert_eq!(super::transport::global().used(), baseline);
    assert_eq!(dropped.len(), 1);
    assert!(
        observed.is_ok(),
        "one UDP drop did not establish two missing native records: {before}"
    );
    assert_eq!(
        before["trimmed_shards"], 14,
        "correlated loss was not exercised before repairs"
    );
    assert_eq!(
        result.unwrap().unwrap(),
        (0..16).map(indexed_native).collect::<Vec<_>>()
    );
    assert!(healthy);
    assert!(
        (2..=4).contains(&repairs.len()),
        "actual485-byte repair cap changed: {repairs:?}"
    );
    assert_eq!(stats["trimmed_shards"], 16);
    assert_eq!(stats["expired_records"], 0);
    assert_eq!(stats["queue_dropped_shards"], 0);
    eprintln!(
        "coalesced encrypted loss: UDP={dropped:?}, pre-repair accepted=14/16, cached repairs={repairs:?}"
    );
}
