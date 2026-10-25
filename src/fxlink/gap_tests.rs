//! A missing whole record must be repairable before delayed control metadata.
use super::{
    Budget, ClientPool, Encoder, Settings,
    control::{self, Control},
    packet::Header,
    transport,
};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

// Break caught: reserving another full RTT at the sender rejects a useful
// repair even though its cache TTL and the receiver's original deadline fit.
#[tokio::test]
async fn real_listener_repairs_a_lost_head_with_500ms_receive_budget_at_300ms_rtt() {
    real_listener_case(false, false).await;
}

#[tokio::test]
async fn v3_repairs_at_300ms_rtt_while_its_progress_writer_is_stalled() {
    real_listener_case(true, false).await;
}

#[tokio::test]
async fn v8_repairs_at_300ms_rtt_after_its_progress_writer_is_stalled() {
    real_listener_case(true, true).await;
}
async fn real_listener_case(block_progress: bool, bootstrap: bool) {
    if std::env::var_os("FLUSSONIX_GAP_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn,flussonix::fxlink=trace")
            .with_test_writer()
            .try_init();
    }
    use futures_util::{StreamExt, future::BoxFuture};
    use std::{
        cmp::Reverse,
        collections::BinaryHeap,
        net::SocketAddr,
        sync::atomic::{AtomicBool, Ordering},
    };
    use tokio::{net::UdpSocket, time::Instant};

    struct Feed(CancellationToken, CancellationToken);
    impl super::Provider for Feed {
        fn subscribe(
            &self,
            r: super::Request,
            _: SocketAddr,
        ) -> BoxFuture<'_, Result<super::Source, u16>> {
            let start = self.0.clone();
            let rest = self.1.clone();
            Box::pin(async move {
                if r.peer_key != "owned-peer" || r.token != "owned-viewer" {
                    return Err(403);
                }
                let first = futures_util::stream::once(async move {
                    start.cancelled().await;
                    Ok(record(0, 1000))
                });
                let second = futures_util::stream::once(async move {
                    rest.cancelled().await;
                    Ok(record(1, 1000))
                });
                Ok(super::Source {
                    pending_records: None,
                    records: Box::pin(
                        first
                            .chain(second)
                            .chain(futures_util::stream::iter(
                                (2..5).map(|n| Ok(record(n, 1000))),
                            ))
                            .chain(futures_util::stream::pending()),
                    ),
                    current: Arc::new(|| true),
                    cancelled: CancellationToken::new(),
                })
            })
        }
    }

    let _serial = transport::test_lock().lock().await;
    let before = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let features = super::Features {
        geometry_hints: false,
        trimmed_records: bootstrap,
        repair_parity: bootstrap,
        bootstrap_repair: bootstrap,
    };
    let listener = super::Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
        features,
    )
    .unwrap();
    let remote = listener.address();
    let source_egress = listener.egress();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let rest = CancellationToken::new();
    let serving =
        tokio::spawn(listener.serve(Arc::new(Feed(start.clone(), rest.clone())), stop.clone()));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let armed = Arc::new(AtomicBool::new(false));
    let drop_media = armed.clone();
    let relay_stop = CancellationToken::new();
    let relay_cancel = relay_stop.clone();
    let relay = tokio::spawn(async move {
        let mut client = None;
        let mut sequence = 0u64;
        let mut pending = BinaryHeap::new();
        let mut buffer = [0u8; 65536];
        let mut dropped = Vec::new();
        loop {
            let due = pending
                .peek()
                .map(|Reverse((due, _, _, _))| *due)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
            tokio::select! {
                _=relay_cancel.cancelled()=>break,
                _=tokio::time::sleep_until(due), if !pending.is_empty()=>{
                    let Reverse((_, _, bytes, to)): Reverse<(Instant,u64,Vec<u8>,SocketAddr)> = pending.pop().unwrap();
                    socket.send_to(&bytes,to).await.unwrap();
                },
                packet=socket.recv_from(&mut buffer)=>{
                    let (length,from)=packet.unwrap();
                    let to=if from==remote {client.expect("client before response")} else {
                        assert!(client.is_none_or(|known|known==from));
                        client=Some(from);remote
                    };
                    // Media shards are about 1.1 KiB. Exclude >=1200-byte PMTU
                    // probes and arm only after the TLS handshake settles.
                    if from==remote && (1050..1200).contains(&length) && drop_media.swap(false,Ordering::AcqRel) {
                        dropped.push(length);
                        continue;
                    }
                    assert!(pending.len()<8192,"owned relay exceeded its cap");
                    sequence+=1;
                    pending.push(Reverse((Instant::now()+Duration::from_millis(150),sequence,buffer[..length].to_vec(),to)));
                }
            }
        }
        dropped
    });
    let progress = block_progress.then(super::client::review_tests::ProgressGuard::new);
    let pool = ClientPool::with_features(features);
    let mut sub = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        super::client::negotiated_version_for_test(&sub),
        if bootstrap { 8 } else { 5 }
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    let rtt = sub.telemetry().snapshot()["rtt_ms"].as_u64().unwrap();
    armed.store(!block_progress, Ordering::Release);
    let mut began = std::time::Instant::now();
    start.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        let mut records = Vec::new();
        if let Some(progress) = &progress {
            records.push(sub.recv().await?);
            while !progress.entered() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            armed.store(true, Ordering::Release);
            began = std::time::Instant::now();
        }
        rest.cancel();
        while records.len() < 5 {
            records.push(sub.recv().await?);
        }
        Ok::<_, String>(records)
    })
    .await;
    let elapsed = began.elapsed();
    let telemetry = sub.telemetry().snapshot();
    drop(progress);
    drop(sub);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    relay_stop.cancel();
    let dropped = relay.await.unwrap();
    assert_eq!(
        transport::global().used(),
        before,
        "real-listener repair leaked state"
    );
    eprintln!(
        "300ms repair: RTT={rtt}, dropped={dropped:?}, elapsed={elapsed:?}, outcome={:?}, telemetry={telemetry}",
        outcome.as_ref().map(|r| r.as_ref().map(Vec::len))
    );
    assert!(
        (290..=350).contains(&rtt),
        "RTT profile not exercised: {rtt}"
    );
    assert_eq!(dropped.len(), 1, "one original media loss required");
    assert_eq!(
        outcome
            .expect("bounded real repair")
            .expect("sender rejected useful repair"),
        (0..5).map(|n| record(n, 1000)).collect::<Vec<_>>()
    );
    assert_eq!(
        source_egress.load(Ordering::Relaxed),
        if bootstrap { 6504 } else { 6528 },
        "five originals plus one real-listener repair required"
    );
    assert!(
        elapsed < Duration::from_millis(650),
        "original receive gap deadline exceeded"
    );
}

#[test]
fn unknown_head_probes_share_retry_history_and_the_shard_batch_cap() {
    let budget = Budget::new(1 << 20);
    let settings = Settings {
        latency_ms: 500,
        max_repair_shards: 3,
        ..Settings::default()
    };
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    receiver.announce(1, 4000, 100).unwrap();
    let first = receiver.repair_requests_if(120, 100, 5, |b, s| pacing.eligible(b, s, 120));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].block_id, 0);
    assert_eq!(first[0].shard_indices, [0, 1, 2]);
    pacing.sent(0, &[0, 1, 2], 120, 100).unwrap();
    let next = receiver.repair_requests_if(140, 100, 5, |b, s| pacing.eligible(b, s, 140));
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].block_id, super::MAX_BLOCKS_PER_RECORD as u64);
    assert_eq!(next[0].shard_indices, [0, 1, 2]);
    pacing
        .sent(next[0].block_id, &next[0].shard_indices, 140, 100)
        .unwrap();
    assert!(!pacing.eligible(0, 0, 224));
    assert!(pacing.eligible(0, 0, 225));
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20)).unwrap();
    let head = encoder.encode(record(0, 500), 0).unwrap();
    let output = receiver.ingest_charged(&head.datagrams[0], 150).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].bytes, record(0, 500));
    let charged = budget.used();
    pacing.prune(150, 1);
    assert!(
        budget.used() < charged,
        "delivered head retained retry history"
    );
    drop(output);
    drop(pacing);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn lost_short_head_probe_gets_an_rtt_retry_with_one_shard_batches() {
    let settings = Settings {
        latency_ms: 500,
        max_repair_shards: 1,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let head = encoder.encode(record(0, 500), 0).unwrap();
    let successor = encoder.encode(record(1, 1000), 0).unwrap();
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    assert!(
        receiver
            .ingest(&successor.datagrams[0], 0)
            .unwrap()
            .is_empty()
    );
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    let mut head_attempts = Vec::new();
    for at in (20..=140).step_by(20) {
        for request in receiver.repair_requests_if(at, 100, 5, |b, s| pacing.eligible(b, s, at)) {
            pacing
                .sent(request.block_id, &request.shard_indices, at, 100)
                .unwrap();
            if request.block_id == 0 && request.shard_indices.contains(&0) {
                head_attempts.push(at);
            }
        }
    }
    // Lose the first response. Unknown higher indices cannot postpone retry 0.
    assert_eq!(head_attempts, [20, 140]);
    assert_eq!(
        receiver.ingest(&head.datagrams[0], 150).unwrap(),
        vec![record(0, 500), record(1, 1000)]
    );
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

#[test]
fn unknown_head_requires_a_gap_and_cannot_renew_its_original_deadline() {
    let budget = Budget::new(1 << 20);
    let mut receiver = super::Receiver::new(
        Settings {
            latency_ms: 500,
            ..Settings::default()
        },
        0,
        1,
        budget.clone(),
    )
    .unwrap();
    assert!(receiver.repair_requests(10, 100, 5).is_empty());
    receiver.announce(2, 1000, 100).unwrap();
    let first = receiver.repair_requests(120, 100, 5);
    assert_eq!(first[0].block_id, 0);
    assert_eq!(first[0].shard_indices, (0..20).collect::<Vec<_>>());
    receiver.announce(2, 1000, 400).unwrap();
    assert!(receiver.repair_requests(495, 100, 5).is_empty());
    assert!(matches!(
        receiver.tick(600),
        Err(super::Error::Expired { record_id: 0 })
    ));
    assert_eq!(budget.used(), 0);
}

#[test]
fn unknown_head_does_not_request_repairs_that_cannot_fit_the_block_budget() {
    let budget = Budget::new(1 << 20);
    let mut receiver = super::Receiver::new(
        Settings {
            latency_ms: 500,
            max_blocks: 1,
            ..Settings::default()
        },
        0,
        1,
        budget.clone(),
    )
    .unwrap();
    receiver.announce(1, 1000, 100).unwrap();
    // That successor consumes the only block; returning the missing head cannot
    // fit. Do not allocate an extra history slot or spend repair quota for it.
    assert!(receiver.repair_requests(120, 100, 5).is_empty());
    assert!(matches!(
        receiver.tick(600),
        Err(super::Error::Expired { record_id: 0 })
    ));
    assert_eq!(budget.used(), 0);
}

fn record(sequence: u8, length: usize) -> Bytes {
    let mut bytes = vec![sequence; length];
    bytes[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}

// Break caught: waiting for reliable Announce before asking for a wholly lost
// head, even though later datagrams prove that head already exists.
#[tokio::test]
async fn missing_whole_head_recovers_without_its_reliable_announcement() {
    let _serial = transport::test_lock().lock().await;
    for (length, version) in [(500, 2), (3000, 2), (500, 3), (3000, 3)] {
        let baseline = transport::global().used();
        let cert = crate::transport_tls_fixture::Certificates::new();
        let mut tls = (*cert.server()).clone();
        tls.alpn_protocols = vec![if version == 3 {
            b"fxlink/3".to_vec()
        } else {
            b"fxlink/2".to_vec()
        }];
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(transport::config());
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let url = format!("fxlink://{}/live", endpoint.local_addr().unwrap());
        let settings = Settings {
            latency_ms: 500,
            ..Settings::default()
        };
        let source_settings = settings.clone();
        let expected: Vec<_> = (0..25)
            .map(|n| record(n, if n == 0 { length } else { 1000 }))
            .collect();
        let source_records = expected.clone();
        let source_endpoint = endpoint.clone();
        let source = tokio::spawn(async move {
            let connection = source_endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let Control::Subscribe { id, generation, .. } = control::read(&mut recv).await.unwrap()
            else {
                panic!("subscription");
            };
            control::write(&mut send, &Control::Accepted).await.unwrap();
            let mut encoder = Encoder::new(
                source_settings.clone(),
                id,
                generation,
                Budget::new(1 << 20),
            )
            .unwrap();
            let encoded: Vec<_> = source_records
                .into_iter()
                .map(|r| encoder.encode(r, 0).unwrap())
                .collect();
            let began = std::time::Instant::now();
            let mut original = 0usize;
            for r in &encoded[1..] {
                for packet in &r.datagrams {
                    connection.send_datagram_wait(packet.clone()).await.unwrap();
                    original += packet.len();
                }
            }
            let mut repaired = 0usize;
            let mut requests = 0usize;
            loop {
                let message = if version == 3 {
                    tokio::select! {
                        packet=connection.read_datagram()=> {
                            let Ok(packet)=packet else{break};
                            let requests_decoded=super::feedback::decode(&packet,&source_settings).unwrap();
                            assert_eq!(requests_decoded.len(),1,"only missing head requested");
                            let request=requests_decoded.front().unwrap();
                            Control::Repair{block:request.block_id,shards:request.shard_indices.clone()}
                        },
                        message=control::read(&mut recv)=>{let Ok(message)=message else{break};message},
                    }
                } else {
                    let Ok(message) = control::read(&mut recv).await else {
                        break;
                    };
                    message
                };
                if let Control::Repair { block: 0, shards } = message {
                    requests += 1;
                    if version == 3 && requests == 1 {
                        continue;
                    } // intentionally lost feedback
                    // Conservative source-age/RTT guard and the listener's
                    // accepted-original byte cap. This loopback fixture sends
                    // no Announce; the real-listener case covers its age rule.
                    if began.elapsed().as_millis() as u64 + connection.rtt().as_millis() as u64 + 5
                        >= 500
                    {
                        continue;
                    }
                    for packet in &encoded[0].datagrams {
                        let h = Header::parse(packet).unwrap();
                        if h.shard_index < h.data_count && shards.contains(&h.shard_index) {
                            assert!(
                                repaired + packet.len() <= original / 4,
                                "repair allowance exceeded"
                            );
                            connection.send_datagram_wait(packet.clone()).await.unwrap();
                            repaired += packet.len();
                        }
                    }
                }
            }
            (requests, repaired)
        });
        let pool = ClientPool::default();
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            let mut sub = pool
                .subscribe(
                    &url,
                    "owned-peer",
                    "owned-viewer",
                    Some(&cert.ca),
                    settings,
                    CancellationToken::new(),
                )
                .await?;
            let mut records = Vec::new();
            for _ in 0..25 {
                records.push(sub.recv().await?);
            }
            Ok::<_, String>(records)
        })
        .await;
        pool.close().await;
        let (requests, repaired) = source.await.unwrap();
        endpoint.close(0u32.into(), b"test done");
        endpoint.wait_idle().await;
        assert_eq!(
            transport::global().used(),
            baseline,
            "unknown-head recovery leaked state"
        );
        eprintln!(
            "unannounced head length={length}, requests={requests}, repaired_bytes={repaired}, outcome={:?}",
            outcome.as_ref().map(|r| r.as_ref().map(Vec::len))
        );
        assert_eq!(
            outcome
                .expect("bounded missing-head recovery")
                .expect("missing head waited for delayed reliable announcement"),
            expected
        );
        assert!(repaired > 0);
        if version == 3 {
            assert!(requests >= 2, "lost datagram did not retry");
        }
    }
}
