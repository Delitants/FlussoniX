//! Controlled limits of receiver-relative grace with a separately aged source.
//! These are counterexamples, not a claim that the retained live loss is located.
use super::*;
use crate::fxlink::{
    Encoder, Receiver,
    repair::{Allowance, Pacing, after_reordering, reordering_grace},
};

fn record(id: u8) -> Bytes {
    let mut b = vec![id; 1000];
    b[..4].copy_from_slice(&996u32.to_be_bytes());
    Bytes::from(b)
}

struct Outcome {
    output: Vec<Bytes>,
    source_ages: Vec<u64>,
    expired: bool,
}
fn delayed_gap(cap: Option<u64>) -> Outcome {
    let settings = Settings {
        latency_ms: 1000,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let encoded = (0..12)
        .map(|id| encoder.encode(record(id), 0).unwrap())
        .collect::<Vec<_>>();
    let mut allowance = Allowance::new(0, 1000);
    for frame in &encoded {
        for packet in &frame.datagrams {
            assert!(allowance.original(0, 0, packet.len()));
        }
    }
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    let mut output = Vec::new();
    for frame in &encoded[..2] {
        output.extend(receiver.ingest(&frame.datagrams[0], 150).unwrap());
    }
    // Startup queueing/control loss delays the successor observation. The head
    // has no metadata until784, but its source lifetime still started at0.
    for frame in &encoded[3..] {
        assert!(
            receiver
                .ingest(&frame.datagrams[0], 432)
                .unwrap()
                .is_empty()
        );
    }
    let mut metadata_known = false;
    let mut first_wake = None;
    let mut attempts = Vec::new();
    let mut responses = std::collections::BTreeMap::<u64, Bytes>::new();
    for at in 432..1432 {
        if at == 784 {
            receiver.announce(2, 1000, at).unwrap();
            metadata_known = true;
        }
        if let Some(packet) = responses.remove(&at) {
            output.extend(receiver.ingest(&packet, at).unwrap());
        }
        if output.len() == 12 {
            break;
        }
        pacing.prune(at, 2);
        let grace = reordering_grace(1000, 310);
        let grace = if metadata_known {
            grace
        } else {
            cap.map_or(grace, |limit| grace.min(limit))
        };
        let requests = receiver.repair_requests_if(at, 310, 5, |b, s| {
            after_reordering(at, 432, grace, &mut first_wake) && pacing.eligible(b, s, at)
        });
        for request in requests {
            pacing
                .sent(request.block_id, &request.shard_indices, at, 310)
                .unwrap();
            assert_eq!(
                request.block_id,
                2 * crate::fxlink::MAX_BLOCKS_PER_RECORD as u64
            );
            let source_at = at + 155;
            attempts.push(source_at);
            let packet = &encoded[2].datagrams[0];
            if allowance.repair(0, source_at, packet.len()) {
                if attempts.len() == 1 {
                    continue;
                } // One lost actual media response.
                responses.insert(at + 310, packet.clone());
            }
        }
    }
    let expired = if output.len() == 12 {
        false
    } else {
        assert!(matches!(
            receiver.tick(1432),
            Err(crate::fxlink::Error::Expired { record_id: 2 })
        ));
        assert!(receiver.is_retired());
        assert_eq!(receiver.stats().expired_records, 1);
        true
    };
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
    Outcome {
        output,
        source_ages: attempts,
        expired,
    }
}

#[test]
fn source_cache_can_expire_before_the_receiver_relative_retry_window() {
    let result = delayed_gap(None);
    assert_eq!(result.source_ages, [774, 1089]);
    assert_eq!(result.output, (0..2).map(record).collect::<Vec<_>>());
    assert!(
        result.expired,
        "late metadata must not renew the source or gap age"
    );
}

#[test]
fn shorter_unknown_grace_recovers_only_the_controlled_delayed_gap() {
    let result = delayed_gap(Some(2 * crate::fxlink::repair::POLL_MS));
    assert_eq!(result.source_ages, [627, 942]);
    assert_eq!(result.output, (0..12).map(record).collect::<Vec<_>>());
    assert!(!result.expired);
}

// A fast unknown probe is not a free remedy. An intact reordered original can
// consume the only repair credit, leaving the subsequent real loss unrecoverable.
fn scarce_credit(cap: Option<u64>) -> (Vec<Bytes>, usize) {
    let settings = Settings {
        latency_ms: 1000,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let encoded = (0..5)
        .map(|id| encoder.encode(record(id), 0).unwrap())
        .collect::<Vec<_>>();
    let mut receiver = Receiver::new(settings, 0, 1, budget.clone()).unwrap();
    let mut allowance = Allowance::new(0, 1000);
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    for frame in &encoded {
        assert!(allowance.original(0, 0, frame.datagrams[0].len()));
    }
    assert_eq!(
        receiver.ingest(&encoded[0].datagrams[0], 0).unwrap(),
        [record(0)]
    );
    assert!(
        receiver
            .ingest(&encoded[2].datagrams[0], 0)
            .unwrap()
            .is_empty()
    );
    let grace = reordering_grace(1000, 300);
    let mut wake = None;
    let first = receiver.repair_requests_if(40, 300, 5, |b, s| {
        after_reordering(40, 0, cap.map_or(grace, |v| grace.min(v)), &mut wake)
            && pacing.eligible(b, s, 40)
    });
    let mut admitted = 0;
    for request in first {
        pacing
            .sent(request.block_id, &request.shard_indices, 40, 300)
            .unwrap();
        assert_eq!(request.block_id, 2048);
        // Source admission at190 precedes knowledge that the intact original
        // arrived at60. The redundant return is340, exactly one full RTT later.
        assert!(allowance.repair(0, 190, encoded[1].datagrams[0].len()));
        admitted += 1;
    }
    let mut output = vec![record(0)];
    output.extend(receiver.ingest(&encoded[1].datagrams[0], 60).unwrap());
    // The real loss becomes visible only after the earlier repair was charged.
    receiver.announce(3, 1000, 200).unwrap();
    assert!(
        receiver
            .ingest(&encoded[4].datagrams[0], 200)
            .unwrap()
            .is_empty()
    );
    let requests = receiver.repair_requests_if(400, 300, 5, |_, _| true);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].block_id, 3 * 2048);
    let packet = &encoded[3].datagrams[0];
    if allowance.repair(0, 550, packet.len()) {
        admitted += 1;
        output.extend(receiver.ingest(packet, 700).unwrap());
    }
    if output.len() != 5 {
        assert!(matches!(
            receiver.tick(1200),
            Err(crate::fxlink::Error::Expired { record_id: 3 })
        ));
    }
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
    (output, admitted)
}

#[test]
fn early_unknown_probe_can_starve_a_later_real_loss_under_unchanged_quota() {
    let normal = scarce_credit(None);
    assert_eq!(normal.0, (0..5).map(record).collect::<Vec<_>>());
    assert_eq!(normal.1, 1);
    let fast = scarce_credit(Some(2 * crate::fxlink::repair::POLL_MS));
    assert_eq!(fast.0, (0..3).map(record).collect::<Vec<_>>());
    assert_eq!(fast.1, 1);
}

// Break caught: a short known-geometry reorder can spend the only full-packet
// repair before a later real loss. This is a controlled counterexample, not a
// replay of the live QUIC trace or a recommendation to change grace globally.
fn known_short_reorder(first_grace: u64) -> (Vec<Bytes>, bool, bool, usize) {
    let settings = Settings::default();
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let records = (0..5)
        .map(|id| {
            let mut bytes = vec![id; 409];
            bytes[..4].copy_from_slice(&405u32.to_be_bytes());
            Bytes::from(bytes)
        })
        .collect::<Vec<_>>();
    let frames = records
        .iter()
        .map(|record| encoder.encode(record.clone(), 0).unwrap())
        .collect::<Vec<_>>();
    let mut allowance = Allowance::new(0, 1000);
    for frame in &frames {
        // V5's compact ceiling excludes these audio-sized records. The one
        // legacy packet has no parity and costs1088bytes, not409bytes.
        assert_eq!(frame.datagrams.len(), 1);
        assert_eq!(frame.datagrams[0].len(), 1088);
        assert!(allowance.original(0, 0, frame.datagrams[0].len()));
    }
    let mut receiver = Receiver::new(settings, 0, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    receiver.announce(0, 409, 0).unwrap();
    let mut wake = None;
    let first = receiver.repair_requests_if(5, 577, 5, |block, shard| {
        after_reordering(5, 0, first_grace, &mut wake) && pacing.eligible(block, shard, 5)
    });
    for request in &first {
        assert_eq!(request.block_id, 0);
        assert_eq!(request.shard_indices, [0]);
        pacing.sent(0, &[0], 5, 577).unwrap();
    }
    // The intact first original arrives after20ms. The repair is still in
    // flight toward the source; local delivery alone does not cancel that request.
    let mut output = receiver.ingest(&frames[0].datagrams[0], 20).unwrap();
    assert_eq!(output, records[..1]);
    for frame in &frames[2..] {
        assert!(
            receiver
                .ingest(&frame.datagrams[0], 100)
                .unwrap()
                .is_empty()
        );
    }
    pacing.prune(105, 1);
    let mut wake = None;
    let missing = receiver.repair_requests_if(105, 577, 5, |block, shard| {
        after_reordering(105, 100, 5, &mut wake) && pacing.eligible(block, shard, 105)
    });
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].block_id, 2048);
    assert_eq!(missing[0].shard_indices, (0..20).collect::<Vec<_>>());
    pacing
        .sent(2048, &missing[0].shard_indices, 105, 577)
        .unwrap();
    // Receiver time0 is source time288, when the first announcement arrives.
    // Feedback takes288ms outward and289ms back. The source intersects the
    // unknown prefix with its sole real data packet before charging it.
    let redundant = !first.is_empty() && allowance.repair(0, 581, 1088);
    let useful = allowance.repair(0, 681, 1088);
    if redundant {
        assert!(
            receiver
                .ingest(&frames[0].datagrams[0], 582)
                .unwrap()
                .is_empty()
        );
    }
    if useful {
        output.extend(receiver.ingest(&frames[1].datagrams[0], 682).unwrap());
        assert_eq!(output, records);
        assert_eq!(receiver.stats().expired_records, 0);
    } else {
        // No response or late announcement can renew the gap observed at100.
        receiver.announce(1, 409, 900).unwrap();
        assert!(receiver.tick(1099).is_ok());
        assert_eq!(
            receiver.tick(1100),
            Err(crate::fxlink::Error::Expired { record_id: 1 })
        );
        assert!(receiver.is_retired());
        assert_eq!(receiver.stats().expired_records, 1);
    }
    let (original, charged) = allowance.counters(0, 681);
    assert_eq!(original, 5440);
    assert_eq!(charged, 1088);
    let remaining = original / 4 - charged;
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    assert_eq!(budget.used(), 0);
    (output, redundant, useful, remaining)
}

#[test]
fn known_short_reorder_can_exhaust_credit_before_a_real_loss() {
    let current = known_short_reorder(reordering_grace(1000, 577));
    assert_eq!(current.0.len(), 1);
    assert!(current.1);
    assert!(!current.2);
    assert_eq!(current.3, 272);
    // Only the first intact record's wait changes in this counterfactual.
    // Keep the later missing record's existing5ms wait and all original ages.
    let comparison = known_short_reorder(40);
    assert_eq!(comparison.0.len(), 5);
    assert!(!comparison.1);
    assert!(comparison.2);
    assert_eq!(comparison.3, 272);
}

struct SettlingOutcome {
    output: Vec<Bytes>,
    queries: Vec<(u64, u64)>,
    source_attempts: Vec<u64>,
    expired: Option<u64>,
}

// The receiver sees the same known two-shard record until the poll at 20 ms.
// Upstream 95 + downstream 482 = RTT 577 ms. Receiver time 0 corresponds to source
// time 482 on the fresh path, or 875 after a further 393 ms announcement delay.
fn known_pair_settling(first_wait: u64, fresh: bool) -> SettlingOutcome {
    let settings = Settings::default();
    let budget = Budget::new(2 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let records = (0..5)
        .map(|id| {
            let mut bytes = vec![id; 1500];
            bytes[..4].copy_from_slice(&1496u32.to_be_bytes());
            Bytes::from(bytes)
        })
        .collect::<Vec<_>>();
    let frames = records
        .iter()
        .map(|r| encoder.encode(r.clone(), 0).unwrap())
        .collect::<Vec<_>>();
    let mut allowance = Allowance::new(0, 1000);
    for frame in &frames {
        assert_eq!(frame.datagrams.len(), 2);
        for packet in &frame.datagrams {
            let header = Header::parse(packet).unwrap();
            assert_eq!((header.data_count, header.parity_count), (2, 0));
            assert_eq!(packet.len(), 1088);
            assert!(allowance.original(0, 0, packet.len()));
        }
    }
    let mut receiver = Receiver::new(settings, 0, 1, budget.clone()).unwrap();
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    receiver.announce(0, 1500, 0).unwrap();
    let mut output = Vec::new();
    let mut batches = Vec::new();
    for at in [20, 25, 40, 100, 105] {
        if fresh && at == 25 {
            for packet in &frames[0].datagrams {
                output.extend(receiver.ingest(packet, at).unwrap());
            }
            assert_eq!(output, records[..1]);
        }
        if fresh && at == 100 {
            for frame in &frames[2..] {
                for packet in &frame.datagrams {
                    assert!(receiver.ingest(packet, at).unwrap().is_empty());
                }
            }
        }
        if at != 20 && at != 40 && !(fresh && at == 105) {
            continue;
        }
        pacing.prune(at, u64::from(fresh && at >= 25));
        let mut wake = None;
        let requests = receiver.repair_requests_if(at, 577, 5, |block, shard| {
            let (first, grace) = if block == 0 {
                (0, first_wait)
            } else {
                (100, 5)
            };
            after_reordering(at, first, grace, &mut wake) && pacing.eligible(block, shard, at)
        });
        for request in requests {
            if request.block_id == 0 {
                assert_eq!(request.shard_indices, [0, 1]);
            } else {
                assert_eq!(request.block_id, 2048);
                assert_eq!(request.shard_indices, (0..20).collect::<Vec<_>>());
            }
            pacing
                .sent(request.block_id, &request.shard_indices, at, 577)
                .unwrap();
            batches.push((at, request));
        }
    }
    let queries = batches
        .iter()
        .map(|(at, request)| (request.block_id, *at))
        .collect();
    let mut source_attempts = Vec::new();
    let mut responses = Vec::new();
    for (at, request) in batches {
        let source_at = if fresh { 482 } else { 875 } + at + 95;
        let frame = &frames[(request.block_id / 2048) as usize];
        for packet in &frame.datagrams {
            let header = Header::parse(packet).unwrap();
            if request.shard_indices.contains(&header.shard_index) {
                source_attempts.push(source_at);
                if allowance.repair(0, source_at, packet.len()) {
                    // Every admitted case also meets the listener's +5ms guard.
                    assert!(source_at + 5 < 1000);
                    responses.push((at + 577, packet.clone()));
                }
            }
        }
    }
    responses.sort_by_key(|(at, _)| *at);
    for (at, packet) in responses {
        output.extend(receiver.ingest(&packet, at).unwrap());
    }
    let expected_len = if fresh { 5 } else { 1 };
    let expired = if output.len() == expected_len {
        assert_eq!(output, records[..expected_len]);
        assert_eq!(receiver.stats().expired_records, 0);
        None
    } else {
        let (at, id) = if fresh { (1100, 1) } else { (1000, 0) };
        assert!(receiver.tick(at - 1).is_ok());
        assert_eq!(
            receiver.tick(at),
            Err(crate::fxlink::Error::Expired { record_id: id })
        );
        assert!(receiver.is_retired());
        Some(id)
    };
    let charged = allowance.counters(0, 999);
    assert_eq!(
        charged,
        (10880, if !fresh && expired.is_some() { 0 } else { 2176 })
    );
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    assert_eq!(budget.used(), 0);
    SettlingOutcome {
        output,
        queries,
        source_attempts,
        expired,
    }
}

// Break caught: redundant multi-shard repair spends 2,176 of 2,720 credit bytes before
// a genuine loss. Only the first known block's wait changes in the comparison.
#[test]
fn longer_known_block_wait_preserves_credit_when_originals_are_reordered() {
    let current = known_pair_settling(reordering_grace(1000, 577), true);
    assert_eq!(current.output.len(), 1);
    assert_eq!(current.queries, [(0, 20), (2048, 105)]);
    assert_eq!(current.source_attempts, [597, 597, 682, 682]);
    assert_eq!(current.expired, Some(1));
    let comparison = known_pair_settling(40, true);
    assert_eq!(comparison.output.len(), 5);
    assert_eq!(comparison.queries, [(2048, 105)]);
    assert_eq!(comparison.source_attempts, [682, 682]);
    assert_eq!(comparison.expired, None);
}

// Break caught: a receiver-only slack guard blesses a wait that misses the
// separately aged source cache. This rejects a blanket 40 ms known-block remedy.
#[test]
fn receiver_deadline_slack_cannot_protect_an_aged_known_block_cache() {
    // Even RTT, scheduling and one full polling interval fit the receiver age.
    let comparison_wait = 40;
    assert!(comparison_wait + 577 + 5 + crate::fxlink::repair::POLL_MS < 1000);
    let current = known_pair_settling(reordering_grace(1000, 577), false);
    assert_eq!(current.output.len(), 1);
    assert_eq!(current.queries, [(0, 20)]);
    assert_eq!(current.source_attempts, [990, 990]);
    assert_eq!(current.expired, None);
    let comparison = known_pair_settling(comparison_wait, false);
    assert!(comparison.output.is_empty());
    assert_eq!(comparison.queries, [(0, 40)]);
    assert_eq!(comparison.source_attempts, [1010, 1010]);
    assert_eq!(comparison.expired, Some(0));
}
