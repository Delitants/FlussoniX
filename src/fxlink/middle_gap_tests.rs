//! Early middle-gap probing and bounded repair-history admission.
use super::{
    Budget, ClientPool, Encoder, Settings,
    control::{self, Control},
    transport,
};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

// Break caught: a middle gap loses its first feedback packet while an older
// head blocks delivery. Probing only the head leaves no RTT for a useful retry.
#[test]
fn early_middle_gap_probe_preserves_time_to_retry_lost_feedback() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let encoded = (0..12)
        .map(|n| encoder.encode(record(n, 1000), 0).unwrap())
        .collect::<Vec<_>>();
    let mut allowance = super::repair::Allowance::new(0, 500);
    for frame in &encoded {
        for packet in &frame.datagrams {
            assert!(allowance.original(0, 0, packet.len()));
        }
    }
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    for (id, frame) in encoded.iter().enumerate() {
        if id != 0 && id != 2 {
            assert!(receiver.ingest(&frame.datagrams[0], 0).unwrap().is_empty());
        }
    }
    let mut attempts = Vec::new();
    let mut output = Vec::new();
    for at in [60, 260] {
        let requests = receiver.repair_requests_if(at, 190, 5, |b, s| pacing.eligible(b, s, at));
        for request in requests {
            pacing
                .sent(request.block_id, &request.shard_indices, at, 190)
                .unwrap();
            let id = (request.block_id / super::MAX_BLOCKS_PER_RECORD as u64) as usize;
            if id == 2 {
                attempts.push(at);
                if attempts.len() == 1 {
                    continue;
                } // Lose the first feedback.
            }
            if request.shard_indices.contains(&0) {
                let packet = &encoded[id].datagrams[0];
                assert!(allowance.repair(0, at + 95, packet.len()));
                output.extend(receiver.ingest(packet, at + 190).unwrap());
            }
        }
    }
    assert_eq!(
        attempts,
        [60, 260],
        "middle gap needs a retry inside its original 500 ms"
    );
    assert_eq!(output, (0..12).map(|n| record(n, 1000)).collect::<Vec<_>>());
    assert_eq!(receiver.stats().expired_records, 0);
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

#[test]
fn middle_gap_prefix_does_not_expand_when_earlier_repairs_enter_cooldown() {
    let budget = Budget::new(1 << 20);
    let settings = Settings {
        latency_ms: 500,
        max_repair_shards: 3,
        ..Settings::default()
    };
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    receiver.announce(0, 2000, 0).unwrap();
    receiver.announce(2, 1000, 0).unwrap();
    let middle = super::MAX_BLOCKS_PER_RECORD as u64;
    for at in [60, 80] {
        let requests = receiver.repair_requests_if(at, 190, 5, |b, s| pacing.eligible(b, s, at));
        for request in requests {
            if request.block_id == middle {
                assert_eq!(at, 60, "fresh unknown indices must not postpone retry 0");
                assert_eq!(request.shard_indices, [0]);
            }
            pacing
                .sent(request.block_id, &request.shard_indices, at, 190)
                .unwrap();
        }
    }
    assert!(pacing.eligible(middle, 0, 255));
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

#[test]
fn new_announcements_do_not_overcommit_repair_history() {
    let budget = Budget::new(1 << 20);
    let settings = Settings {
        latency_ms: 500,
        max_blocks: 3,
        ..Settings::default()
    };
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    receiver.announce(0, 1000, 0).unwrap();
    receiver.announce(3, 1000, 0).unwrap();
    for at in [20, 40, 140] {
        if at == 40 {
            receiver.announce(2, 1000, 30).unwrap();
        }
        pacing.prune(at, 0);
        let requests = receiver.repair_requests_if(at, 100, 5, pacing.batch_eligible(at));
        for request in requests {
            assert!(
                pacing
                    .sent(request.block_id, &request.shard_indices, at, 100)
                    .is_ok(),
                "new metadata cannot retire an otherwise bounded subscription"
            );
        }
    }
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

#[test]
fn speculative_gaps_reserve_earliest_capacity_while_its_probe_is_in_flight() {
    for (records, blocks) in [(3, 2048), (64, 3)] {
        let settings = Settings {
            latency_ms: 500,
            max_records: records,
            max_blocks: blocks,
            ..Settings::default()
        };
        let budget = Budget::new(1 << 20);
        let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
        let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
        receiver.announce(1, 1000, 0).unwrap();
        receiver.announce(3, 1000, 0).unwrap();
        for at in [20, 40] {
            let requests =
                receiver.repair_requests_if(at, 100, 5, |b, s| pacing.eligible(b, s, at));
            assert!(
                requests
                    .iter()
                    .all(|request| request.block_id != 2 * super::MAX_BLOCKS_PER_RECORD as u64),
                "middle gap stole capacity reserved for the outstanding head"
            );
            for request in requests {
                pacing
                    .sent(request.block_id, &request.shard_indices, at, 100)
                    .unwrap();
            }
        }
        drop(receiver);
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn huge_numeric_gap_stays_bounded_and_does_not_probe_beyond_a_successor() {
    let budget = Budget::new(1 << 20);
    let settings = Settings {
        latency_ms: 500,
        max_records: 4,
        ..Settings::default()
    };
    let mut receiver = super::Receiver::new(settings, 0, 1, budget.clone()).unwrap();
    assert!(receiver.repair_requests(0, 100, 5).is_empty());
    let far = u64::MAX / super::MAX_BLOCKS_PER_RECORD as u64;
    receiver.announce(far, 1000, 0).unwrap();
    let requests = receiver.repair_requests(20, 100, 5);
    assert_eq!(
        requests.iter().map(|r| r.block_id).collect::<Vec<_>>(),
        [0, 2048, 4096, far * super::MAX_BLOCKS_PER_RECORD as u64]
    );
    assert!(
        requests
            .iter()
            .map(|r| r.shard_indices.len())
            .sum::<usize>()
            <= 64
    );
    assert!(
        receiver.repair_requests(305, 190, 5).is_empty(),
        "probes cannot renew first gap age"
    );
    assert!(matches!(
        receiver.tick(500),
        Err(super::Error::Expired { record_id: 0 })
    ));
    assert_eq!(budget.used(), 0);
}

// The real encrypted v3 client must request the middle gap twice before this
// peer releases the head. No Announce is sent for either missing record.
#[tokio::test]
async fn encrypted_middle_gap_retries_lost_feedback_while_the_head_is_blocked() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/3".to_vec()];
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
    let expected = (0..16).map(|n| record(n, 1000)).collect::<Vec<_>>();
    let source_records = expected.clone();
    let source_endpoint = endpoint.clone();
    let source = tokio::spawn(async move {
        let connection = source_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe { id, generation, .. } = control::read(&mut recv).await.unwrap()
        else {
            panic!("subscription")
        };
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let mut encoder = Encoder::new(
            source_settings.clone(),
            id,
            generation,
            Budget::new(1 << 20),
        )
        .unwrap();
        let encoded = source_records
            .into_iter()
            .map(|r| encoder.encode(r, 0).unwrap())
            .collect::<Vec<_>>();
        let began = std::time::Instant::now();
        let mut allowance = super::repair::Allowance::new(0, 500);
        for (record_id, frame) in encoded.iter().enumerate() {
            for packet in &frame.datagrams {
                assert!(allowance.original(0, 0, packet.len()));
                if record_id != 0 && record_id != 2 {
                    connection.send_datagram_wait(packet.clone()).await.unwrap();
                }
            }
        }
        let mut middle_attempts = 0;
        let mut head_requested = false;
        let mut middle_repaired = false;
        let mut head_sent = false;
        while let Ok(packet) = connection.read_datagram().await {
            for request in super::feedback::decode(&packet, &source_settings).unwrap() {
                let record_id = (request.block_id / super::MAX_BLOCKS_PER_RECORD as u64) as usize;
                assert!(record_id < encoded.len());
                if !request.shard_indices.contains(&0) {
                    continue;
                }
                if record_id == 0 {
                    head_requested = true;
                    continue;
                }
                if record_id == 2 {
                    middle_attempts += 1;
                    if middle_attempts == 1 {
                        continue;
                    } // Lost first feedback.
                    assert!(
                        !head_sent,
                        "test must repair the middle before releasing the head"
                    );
                    middle_repaired = true;
                }
                let at = began.elapsed().as_millis() as u64;
                let packet = &encoded[record_id].datagrams[0];
                assert!(allowance.repair(0, at, packet.len()));
                connection.send_datagram_wait(packet.clone()).await.unwrap();
            }
            if middle_repaired && head_requested && !head_sent {
                let at = began.elapsed().as_millis() as u64;
                let packet = &encoded[0].datagrams[0];
                assert!(allowance.repair(0, at, packet.len()));
                connection.send_datagram_wait(packet.clone()).await.unwrap();
                head_sent = true;
            }
        }
        (middle_attempts, head_sent)
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
        for _ in 0..16 {
            records.push(sub.recv().await?);
        }
        Ok::<_, String>(records)
    })
    .await;
    pool.close().await;
    let (attempts, head_sent) = source.await.unwrap();
    endpoint.close(0u32.into(), b"test done");
    endpoint.wait_idle().await;
    assert_eq!(
        transport::global().used(),
        baseline,
        "middle-gap recovery leaked state"
    );
    assert_eq!(
        outcome
            .expect("bounded middle-gap recovery")
            .expect("middle-gap repair failed"),
        expected
    );
    assert!(attempts >= 2 && head_sent);
}

#[test]
fn full_history_skips_a_new_gap_without_hiding_an_existing_repair() {
    let settings = Settings {
        latency_ms: 500,
        max_blocks: 3,
        max_repair_shards: 1,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    receiver.announce(0, 1000, 0).unwrap();
    receiver.announce(3, 1000, 0).unwrap();
    for (id, shard) in [(0, 0), (2, 0), (3, 1)] {
        pacing
            .sent(id * super::MAX_BLOCKS_PER_RECORD as u64, &[shard], 20, 100)
            .unwrap();
    }
    let requests = receiver.repair_requests_if(40, 100, 5, pacing.batch_eligible(40));
    assert_eq!(
        requests.iter().map(|r| r.block_id).collect::<Vec<_>>(),
        [3 * super::MAX_BLOCKS_PER_RECORD as u64]
    );
    assert_eq!(requests[0].shard_indices, [0]);
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

#[test]
fn middle_becoming_head_does_not_postpone_its_lost_shard_zero_retry() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut receiver = super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = super::repair::Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    receiver.announce(0, 1000, 0).unwrap();
    receiver.announce(2, 1000, 0).unwrap();
    for request in receiver.repair_requests_if(20, 100, 5, |b, s| pacing.eligible(b, s, 20)) {
        pacing
            .sent(request.block_id, &request.shard_indices, 20, 100)
            .unwrap();
    }
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20)).unwrap();
    let head = encoder.encode(record(0, 1000), 0).unwrap();
    assert_eq!(
        receiver.ingest(&head.datagrams[0], 40).unwrap(),
        [record(0, 1000)]
    );
    for request in receiver.repair_requests_if(40, 100, 5, |b, s| pacing.eligible(b, s, 40)) {
        assert_ne!(
            request.block_id,
            super::MAX_BLOCKS_PER_RECORD as u64,
            "head transition must not expand a shape-free prefix during cooldown"
        );
        pacing
            .sent(request.block_id, &request.shard_indices, 40, 100)
            .unwrap();
    }
    assert!(pacing.eligible(super::MAX_BLOCKS_PER_RECORD as u64, 0, 125));
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

fn record(sequence: u8, length: usize) -> Bytes {
    let mut bytes = vec![sequence; length];
    bytes[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
