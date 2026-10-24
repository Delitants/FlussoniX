//! Encrypted protocol integration. Captured receiver clocks are injected; this
//! is not an impaired-network, sustained playback or fairness qualification.
use super::*;
use super::{
    control::{self, Control},
    packet::Header,
};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
fn record(id: u8, len: usize) -> Bytes {
    let mut b = vec![id; len];
    b[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(b)
}
fn features(parity: bool) -> Features {
    Features {
        geometry_hints: false,
        trimmed_records: true,
        repair_parity: parity,
        bootstrap_repair: false,
    }
}
struct Feed(Vec<Bytes>);
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let records = self.0.clone();
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
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
fn select(
    receiver: &Receiver,
    pacing: &mut super::repair::Pacing,
    at: u64,
    rtt: u64,
) -> Vec<RepairRequest> {
    pacing.prune(at, 0);
    let requests = receiver.repair_requests_if(at, rtt, 5, pacing.batch_eligible(at));
    for r in &requests {
        pacing.sent(r.block_id, &r.shard_indices, at, rtt).unwrap();
    }
    requests
}
async fn replies(connection: &quinn::Connection, count: usize) -> Vec<Bytes> {
    replies_within(connection, count, Duration::from_millis(150)).await
}
async fn replies_within(
    connection: &quinn::Connection,
    count: usize,
    limit: Duration,
) -> Vec<Bytes> {
    let mut packets = Vec::new();
    let result = tokio::time::timeout(limit, async {
        for _ in 0..count {
            packets.push(connection.read_datagram().await.unwrap());
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "expected {count} cached replies, got{}",
        packets.len()
    );
    packets
}
#[tokio::test]
async fn actual_listener_returns_requested_parity_for_delayed_geometry_gap() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let expected = [
        record(0, 20000),
        record(1, 600),
        record(2, 5125),
        record(3, 600),
        record(4, 20000),
    ];
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
        features(true),
    )
    .unwrap();
    let address = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Feed(expected.to_vec())), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![b"fxlink/7".to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport::config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let active = connection.clone();
    let exercise = tokio::spawn(async move {
        let (mut send, mut recv) = active.open_bi().await.unwrap();
        let id = send.id().index();
        control::write(
            &mut send,
            &Control::Subscribe {
                version: 7,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "owned-gap".into(),
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
        let budget = Budget::new(4 << 20);
        let mut receiver = Receiver::new(settings.clone(), id, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(7));
        let mut pacing = super::repair::Pacing::new(budget.clone(), 64).unwrap();
        let mut output = Vec::new();
        // Exactly57 original datagrams:24+1+7+1+24; short records are too
        // large for compact copies. Drop all head/middle originals deliberately.
        for packet in replies(&active, 57).await {
            let h = Header::parse(&packet).unwrap();
            if h.record_id != 1 && h.record_id != 2 {
                output.extend(receiver.ingest(&packet, 0).unwrap());
            }
        }
        assert_eq!(output, [expected[0].clone()]);
        let mut sequence = 1;
        for frame in super::feedback::encode_parity(
            id,
            &select(&receiver, &mut pacing, 53, 195),
            &settings,
            &mut sequence,
        )
        .unwrap()
        {
            active.send_datagram(frame).unwrap();
        }
        let first = replies(&active, 2).await;
        let middle = first
            .into_iter()
            .find(|p| Header::parse(p).unwrap().record_id == 2)
            .unwrap();
        // Reliable source announcements exist, but this fixture applies their
        // geometry at the captured logical times. The async client is tested below.
        receiver.announce(1, 600, 180).unwrap();
        receiver.announce(2, 5125, 198).unwrap();
        let second = select(&receiver, &mut pacing, 202, 190);
        assert_eq!(second[0].shard_indices, [1, 2, 3, 4, 5, 6]);
        for frame in super::feedback::encode_parity(id, &second, &settings, &mut sequence).unwrap()
        {
            active.send_datagram(frame).unwrap();
        }
        let later = replies(&active, 6).await;
        output.extend(receiver.ingest(&middle, 245).unwrap());
        for frame in super::feedback::encode_parity(
            id,
            &select(&receiver, &mut pacing, 253, 191),
            &settings,
            &mut sequence,
        )
        .unwrap()
        {
            active.send_datagram(frame).unwrap();
        }
        let head = replies(&active, 1).await;
        for packet in later {
            let h = Header::parse(&packet).unwrap();
            assert_eq!(h.record_id, 2);
            if h.shard_index != 3 {
                output.extend(receiver.ingest(&packet, 399).unwrap());
            }
        }
        output.extend(receiver.ingest(&head[0], 452).unwrap());
        assert_eq!(output, expected);
        assert!(receiver.stats().fec_recovered_shards >= 1);
        receiver.tick(500).unwrap();
        // Real Listener enqueued8,984 repair bytes from its original61,208;
        // all responses passed its unchanged25% Allowance and source TTL.
        assert_eq!(egress.load(Ordering::Acquire), 70192);
        drop(receiver);
        drop(pacing);
        assert_eq!(budget.used(), 0);
        drop((send, recv));
    });
    let result = exercise.await;
    connection.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
    result.unwrap();
}
#[tokio::test]
async fn actual_client_emits_negotiated_feedback_and_uses_parity_reply() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/7".to_vec()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let address = endpoint.local_addr().unwrap();
    let peer = endpoint.clone();
    let stop = CancellationToken::new();
    let halt = stop.clone();
    let expected = [record(0, 20000), record(1, 5125), record(2, 600)];
    let source_records = expected.clone();
    let server = tokio::spawn(async move {
        let connection = peer.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            version,
            id,
            generation,
            peer_key,
            token,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("subscription")
        };
        assert_eq!(version, 7);
        assert_eq!(
            (peer_key.as_str(), token.as_str()),
            ("owned-peer", "owned-viewer")
        );
        let budget = Budget::new(4 << 20);
        let routes = super::feedback::Routes::default();
        let mut route = routes.admit(id, 7, &budget).unwrap();
        let settings = Settings {
            latency_ms: 500,
            ..Settings::default()
        };
        let mut encoder = Encoder::new(settings.clone(), id, generation, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(7));
        let frames = source_records
            .iter()
            .map(|r| encoder.encode(r.clone(), 0).unwrap())
            .collect::<Vec<_>>();
        let mut allowance = super::repair::Allowance::new(0, 500);
        control::write(&mut send, &Control::Accepted).await.unwrap();
        for (r, frame) in frames.iter().enumerate() {
            control::write(
                &mut send,
                &Control::Announce {
                    record: r as u64,
                    len: source_records[r].len(),
                },
            )
            .await
            .unwrap();
            for packet in &frame.datagrams {
                assert!(allowance.original(0, 0, packet.len()));
                if r != 1 {
                    connection.send_datagram_wait(packet.clone()).await.unwrap();
                }
            }
        }
        let mut repaired = false;
        loop {
            tokio::select! {
                _=halt.cancelled()=>break,
                packet=connection.read_datagram()=>{let Ok(packet)=packet else {break};assert_eq!(packet[4],3,"v7 must not emit legacy feedback");routes.receive(packet,0);},
                request=route.next(&settings)=>{
                    let crate::fxlink::feedback::DatagramRequest::Ordinary(crate::fxlink::RepairRequest {block_id:block,shard_indices:shards})=request else {panic!("repair")};assert_eq!(block,2048);assert_eq!(shards,[0,1,2,3,4,5,6]);
                    if !repaired {for packet in &frames[1].datagrams {let h=Header::parse(packet).unwrap();if h.shard_index!=3 {assert!(allowance.repair(0,1,packet.len()));connection.send_datagram_wait(packet.clone()).await.unwrap();}}repaired=true;}
                }
            }
        }
        assert!(repaired);
        connection.close(0u32.into(), b"done");
        drop(route);
        drop(frames);
        drop(encoder);
        assert_eq!(budget.used(), 0);
    });
    let pool = Arc::new(ClientPool::with_features(features(true)));
    let client = pool.clone();
    let ca = cert.ca.clone();
    let exercise = tokio::spawn(async move {
        let mut sub = client
            .subscribe(
                &format!("fxlink://{address}/owned"),
                "owned-peer",
                "owned-viewer",
                Some(&ca),
                Settings {
                    latency_ms: 500,
                    ..Settings::default()
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(super::client::negotiated_version_for_test(&sub), 7);
        for record in expected {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                record
            );
        }
    });
    let result = exercise.await;
    stop.cancel();
    pool.close().await;
    let server_result = server.await;
    endpoint.wait_idle().await;
    assert_eq!(transport::global().used(), baseline);
    result.unwrap();
    server_result.unwrap();
}

#[tokio::test]
async fn opt_in_and_mixed_peers_keep_authentication_and_negotiated_layouts() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    for (server_features, client_features, version) in [
        (features(true), features(true), 7),
        (features(true), features(false), 6),
        (features(false), features(true), 6),
        (Features::default(), features(true), 5),
        (features(true), Features::default(), 5),
    ] {
        let cert = crate::transport_tls_fixture::Certificates::new();
        let input = record(1, 600);
        let listener = Listener::bind_with_features(
            "127.0.0.1:0".parse().unwrap(),
            &cert.cert,
            &cert.key,
            Settings::default(),
            server_features,
        )
        .unwrap();
        let address = listener.address();
        let stop = CancellationToken::new();
        let server =
            tokio::spawn(listener.serve(Arc::new(Feed(vec![input.clone()])), stop.clone()));
        let pool = Arc::new(ClientPool::with_features(client_features));
        let client = pool.clone();
        let ca = cert.ca.clone();
        let exercise = tokio::spawn(async move {
            let url = format!("fxlink://{address}/owned");
            assert!(
                client
                    .subscribe(
                        &url,
                        "owned-peer",
                        "wrong-viewer",
                        Some(&ca),
                        Settings::default(),
                        CancellationToken::new()
                    )
                    .await
                    .is_err()
            );
            let mut sub = client
                .subscribe(
                    &url,
                    "owned-peer",
                    "owned-viewer",
                    Some(&ca),
                    Settings::default(),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(super::client::negotiated_version_for_test(&sub), version);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), sub.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                input
            );
            assert_eq!(
                sub.telemetry().stats.lock().unwrap().trimmed_shards,
                u64::from(version >= 6)
            );
        });
        let result = exercise.await;
        pool.close().await;
        stop.cancel();
        server.await.unwrap().unwrap();
        assert_eq!(transport::global().used(), baseline);
        result.unwrap();
    }
}
#[tokio::test]
async fn sender_omits_unfunded_parity_and_expired_cached_repairs() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
        features(true),
    )
    .unwrap();
    let address = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Feed(vec![record(0, 5125)])), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![b"fxlink/7".to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport::config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let active = connection.clone();
    let exercise = tokio::spawn(async move {
        let (mut send, mut recv) = active.open_bi().await.unwrap();
        let id = send.id().index();
        control::write(
            &mut send,
            &Control::Subscribe {
                version: 7,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "owned-credit".into(),
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
        let originals = replies(&active, 7).await;
        assert!(originals.iter().all(|p| p.len() == 1088));
        let mut sequence = 1;
        let requests = [RepairRequest {
            block_id: 0,
            shard_indices: vec![3, 6],
        }];
        for packet in
            super::feedback::encode_parity(id, &requests, &settings, &mut sequence).unwrap()
        {
            active.send_datagram(packet).unwrap();
        }
        let response = replies(&active, 1).await;
        assert_eq!(Header::parse(&response[0]).unwrap().shard_index, 3);
        //7originals fund1,904bytes: one1,088byte data reply fits; parity does not.
        assert!(
            tokio::time::timeout(Duration::from_millis(80), active.read_datagram())
                .await
                .is_err()
        );
        assert_eq!(egress.load(Ordering::Acquire), 8704);
        tokio::time::sleep(Duration::from_millis(500)).await;
        for packet in
            super::feedback::encode_parity(id, &requests, &settings, &mut sequence).unwrap()
        {
            active.send_datagram(packet).unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(80), active.read_datagram())
                .await
                .is_err()
        );
        assert_eq!(egress.load(Ordering::Acquire), 8704);
        drop((send, recv));
    });
    let result = exercise.await;
    connection.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
    result.unwrap();
}

#[tokio::test]
async fn cached_upper_indices_return_at_most_one_parity_and_reject_absent_blocks() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 1500,
        data_shards: 128,
        parity_shards: 128,
        shard_size: 21,
        max_repair_shards: 256,
        ..Settings::default()
    };
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
        features(true),
    )
    .unwrap();
    let address = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(Feed(vec![record(0, 128)])), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![b"fxlink/7".to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport::config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let active = connection.clone();
    let exercise = tokio::spawn(async move {
        let (mut send, mut recv) = active.open_bi().await.unwrap();
        let id = send.id().index();
        control::write(
            &mut send,
            &Control::Subscribe {
                version: 7,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "owned-indices".into(),
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
        assert_eq!(
            replies_within(&active, 256, Duration::from_secs(1))
                .await
                .len(),
            256
        );
        let mut sequence = 1;
        let requests = [RepairRequest {
            block_id: 0,
            shard_indices: vec![127, 128, 255],
        }];
        for packet in
            super::feedback::encode_parity(id, &requests, &settings, &mut sequence).unwrap()
        {
            active.send_datagram(packet).unwrap();
        }
        let actual = replies(&active, 2)
            .await
            .into_iter()
            .map(|p| Header::parse(&p).unwrap().shard_index)
            .collect::<Vec<_>>();
        assert_eq!(actual, [127, 128]);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), active.read_datagram())
                .await
                .is_err()
        );
        let requests = [
            RepairRequest {
                block_id: 0,
                shard_indices: vec![255],
            },
            RepairRequest {
                block_id: 1,
                shard_indices: vec![128],
            },
        ];
        for packet in
            super::feedback::encode_parity(id, &requests, &settings, &mut sequence).unwrap()
        {
            active.send_datagram(packet).unwrap();
        }
        assert_eq!(
            Header::parse(&replies(&active, 1).await[0])
                .unwrap()
                .shard_index,
            255
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), active.read_datagram())
                .await
                .is_err()
        );
        assert_eq!(egress.load(Ordering::Acquire), 22015); //256originals+3repairs,85bytes each.
        drop((send, recv));
    });
    let result = exercise.await;
    connection.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
    result.unwrap();
}
