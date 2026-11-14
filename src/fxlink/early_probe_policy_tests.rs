//! Logical-clock controls of the production grace policy. These do not qualify
//! a network impairment profile or replace the encrypted/Listener regressions.
use super::*;
use crate::fxlink::{
    Encoder, MAX_BLOCKS_PER_RECORD, Receiver,
    repair::{Allowance, Pacing, after_reordering, reordering_grace},
};

fn record(id: u8, len: usize) -> Bytes {
    let mut bytes = vec![id; len];
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}

#[test]
fn early_probe_policy_preserves_short_grace_and_legacy_versions() {
    for latency in [500, 1000, 1500] {
        for rtt in [0, 1, 10, 100, 194, 300, 495, u64::MAX] {
            let normal = reordering_grace(latency, rtt);
            assert_eq!(first_repair_grace(7, latency, rtt, false), normal.min(20));
            assert_eq!(first_repair_grace(7, latency, rtt, true), normal);
            for version in 1..=6 {
                assert_eq!(first_repair_grace(version, latency, rtt, false), normal);
                assert_eq!(first_repair_grace(version, latency, rtt, true), normal);
            }
        }
    }
}

// Earlier repair must use the existing retry hint, not an invented intermediate
// poll. The source's independent age still rejects the legacy retry at497ms.
#[test]
fn early_probe_policy_retry_wake_retains_the_500ms_source_age_guard() {
    for version in [6, 7] {
        let settings = Settings {
            latency_ms: 500,
            ..Settings::default()
        };
        let budget = Budget::new(4 << 20);
        let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(version));
        let expected = [record(0, 4835), record(1, 388), record(2, 405)];
        let frames = expected
            .iter()
            .zip([996, 1000, 1008])
            .map(|(r, at)| encoder.encode(r.clone(), at).unwrap())
            .collect::<Vec<_>>();
        let mut receiver = Receiver::new(settings, 0, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(version));
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        for packet in &frames[2].datagrams {
            assert!(receiver.ingest(packet, 0).unwrap().is_empty());
        }
        let mut allowance = Allowance::new(100, 500);
        // A declared creation cohort, not a wire-efficiency measurement.
        assert!(allowance.original(1000, 1000, 96585));
        assert!(allowance.repair(1000, 1001, 6528));
        let mut known = [false, false, true];
        let mut output = Vec::new();
        let mut first_at = None;
        let mut poll_at = 20;
        let mut not_before = 0;
        let mut attempts = Vec::new();
        let mut due = None;
        for at in 0..500 {
            if at == 175 {
                receiver.announce(0, 4835, at).unwrap();
                known[0] = true;
            }
            if at == 417 {
                receiver.announce(1, 388, at).unwrap();
                known[1] = true;
            }
            // Controlled intact predecessor: only data, no parity assumption.
            if let Some(index) = [262, 272, 273, 274, 275].iter().position(|t| *t == at) {
                output.extend(receiver.ingest(&frames[0].datagrams[index], at).unwrap());
            }
            if due == Some(at) {
                output.extend(receiver.ingest(&frames[1].datagrams[0], at).unwrap());
            }
            if output.len() == 3 {
                break;
            }
            let retry = retry_wake_at(Some(&pacing), first_at, true, true, not_before);
            if at < not_before || (at < poll_at && retry.is_none_or(|wake| at < wake)) {
                continue;
            }
            poll_at = at + 20;
            first_at = None;
            pacing.prune(at, output.len() as u64);
            let mut eligible = pacing.batch_eligible(at);
            let requests = receiver.repair_requests_if(at, 194, 5, |block, shard| {
                let id = (block / MAX_BLOCKS_PER_RECORD as u64) as usize;
                after_reordering(
                    at,
                    0,
                    first_repair_grace(version, 500, 194, known[id]),
                    &mut first_at,
                ) && eligible(block, shard)
            });
            drop(eligible);
            if !requests.is_empty() {
                not_before = at + 20;
            }
            for request in requests {
                pacing
                    .sent(request.block_id, &request.shard_indices, at, 194)
                    .unwrap();
                if request.block_id != MAX_BLOCKS_PER_RECORD as u64 {
                    continue;
                }
                assert_eq!(request.shard_indices, [0]);
                let source_age = 147 + at + if attempts.is_empty() { 105 } else { 98 };
                let packet = &frames[1].datagrams[0];
                let admitted =
                    source_age + 5 < 500 && allowance.repair(1000, 1000 + source_age, packet.len());
                attempts.push((at, source_age, admitted));
                // Drop the first admitted response; the retry must beat TTL.
                if admitted && attempts.len() == 2 {
                    due = Some(at + 98 + 95);
                }
            }
        }
        let spent = allowance.counters(1000, 1494).1;
        if version == 7 {
            assert_eq!(attempts, [(20, 272, true), (219, 464, true)]);
            assert_eq!(output, expected);
            assert_eq!(spent, 6528 + 2 * 472);
            assert_eq!(receiver.stats().expired_records, 0);
        } else {
            assert_eq!(attempts, [(53, 305, true), (252, 497, false)]);
            assert_eq!(output, expected[..1]);
            assert_eq!(spent, 6528 + 472);
            assert!(matches!(
                receiver.tick(500),
                Err(crate::fxlink::Error::Expired { record_id: 1 })
            ));
        }
        drop(receiver);
        drop(pacing);
        drop(frames);
        drop(encoder);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn early_probe_policy_false_probe_can_exhaust_credit_for_a_later_loss() {
    for version in [6, 7] {
        let settings = Settings {
            latency_ms: 1000,
            ..Settings::default()
        };
        let budget = Budget::new(1 << 20);
        let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(version));
        let frames = (0..5)
            .map(|id| encoder.encode(record(id, 1000), 0).unwrap())
            .collect::<Vec<_>>();
        let mut receiver = Receiver::new(settings, 0, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(version));
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        let mut allowance = Allowance::new(0, 1000);
        for frame in &frames {
            assert!(allowance.original(0, 0, frame.datagrams[0].len()));
        }
        assert_eq!(allowance.counters(0, 0), (5420, 0));
        let mut output = receiver.ingest(&frames[0].datagrams[0], 0).unwrap();
        assert!(
            receiver
                .ingest(&frames[2].datagrams[0], 0)
                .unwrap()
                .is_empty()
        );
        let mut wake = None;
        let early = receiver.repair_requests_if(40, 300, 5, |b, s| {
            after_reordering(
                40,
                0,
                first_repair_grace(version, 1000, 300, false),
                &mut wake,
            ) && pacing.eligible(b, s, 40)
        });
        assert_eq!(early.len(), usize::from(version == 7));
        for request in early {
            pacing
                .sent(request.block_id, &request.shard_indices, 40, 300)
                .unwrap();
            assert!(allowance.repair(0, 190, frames[1].datagrams[0].len()));
        }
        // Intact original1 arrives before the redundant reply. Consumption
        // progress is deliberately withheld at the source in this component.
        output.extend(receiver.ingest(&frames[1].datagrams[0], 60).unwrap());
        receiver.announce(3, 1000, 200).unwrap();
        assert!(
            receiver
                .ingest(&frames[4].datagrams[0], 200)
                .unwrap()
                .is_empty()
        );
        pacing.prune(400, 3);
        let requests = receiver.repair_requests_if(400, 300, 5, |b, s| {
            after_reordering(
                400,
                200,
                first_repair_grace(version, 1000, 300, true),
                &mut wake,
            ) && pacing.eligible(b, s, 400)
        });
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].block_id, 3 * MAX_BLOCKS_PER_RECORD as u64);
        pacing
            .sent(requests[0].block_id, &requests[0].shard_indices, 400, 300)
            .unwrap();
        let admitted = allowance.repair(0, 550, frames[3].datagrams[0].len());
        assert_eq!(admitted, version == 6);
        if admitted {
            output.extend(receiver.ingest(&frames[3].datagrams[0], 700).unwrap());
        } else {
            assert!(matches!(
                receiver.tick(1200),
                Err(crate::fxlink::Error::Expired { record_id: 3 })
            ));
        }
        assert_eq!(
            output,
            (0..if version == 7 { 3 } else { 5 })
                .map(|id| record(id, 1000))
                .collect::<Vec<_>>()
        );
        assert_eq!(allowance.counters(0, 999), (5420, 1084));
        drop(receiver);
        drop(pacing);
        drop(frames);
        drop(encoder);
        assert_eq!(budget.used(), 0);
    }
}

#[tokio::test]
async fn early_probe_policy_prepared_packet_and_history_still_fence_selection() {
    use crate::fxlink::{packet_queue, transport::Charged};
    let settings = Settings {
        latency_ms: 500,
        max_repair_shards: 2,
        ..Settings::default()
    };
    let budget = Budget::new(4 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let frames = (0..3)
        .map(|id| encoder.encode(record(id, 1000), 0).unwrap())
        .collect::<Vec<_>>();
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    receiver.ingest(&frames[2].datagrams[0], 0).unwrap();
    let mut pacing = Pacing::new(budget.clone(), 1).unwrap(); // Deliberate scarce-history control.
    let mut wake = None;
    let mut eligible = pacing.batch_eligible(20);
    let selected = receiver.repair_requests_if(20, 194, 5, |b, s| {
        after_reordering(20, 0, first_repair_grace(7, 500, 194, false), &mut wake) && eligible(b, s)
    });
    drop(eligible);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].block_id, 0);
    assert_eq!(selected[0].shard_indices, [0, 1]); // Unchanged shard quota.
    pacing.sent(0, &selected[0].shard_indices, 20, 194).unwrap();
    let mut eligible = pacing.batch_eligible(40);
    assert!(
        receiver
            .repair_requests_if(40, 194, 5, &mut eligible)
            .is_empty()
    );
    drop(eligible);
    assert_eq!(
        retry_wake_at(Some(&pacing), Some(20), true, true, 40),
        Some(40)
    );
    assert_eq!(retry_wake_at(Some(&pacing), Some(20), false, true, 0), None);
    assert_eq!(retry_wake_at(Some(&pacing), Some(20), true, false, 0), None);
    drop(pacing);
    // Independent prepared-packet control with ordinary history capacity.
    let pacing = Pacing::new(budget.clone(), 64).unwrap();
    // A prepared shard establishes geometry before decode and remains locally
    // owned. It must keep normal grace and suppress redundant repair.
    let (sender, mut ingress) = packet_queue::channel(&budget).unwrap();
    let bytes = frames[0].datagrams[0].clone();
    let header = Header::parse(&bytes).unwrap();
    sender
        .try_send(
            Charged {
                deadline_ms: 500,
                _guard: budget.reserve(bytes.len()).unwrap(),
                bytes,
            },
            &header,
        )
        .unwrap_or_else(|_| panic!("queue"));
    let packet = ingress.packets.recv().await.unwrap();
    let state = State::new(CancellationToken::new());
    let mut deadlines = std::collections::BTreeMap::from([(2, 500)]);
    let mut discarded = DiscardFence::default();
    let packet = prepare_decode_packet(
        packet,
        &settings,
        &state,
        0,
        &mut deadlines,
        &mut discarded,
        Capabilities::V7,
        None,
    )
    .unwrap()
    .unwrap();
    assert!(deadlines.contains_key(&0));
    assert_eq!(
        first_repair_grace(7, 500, 194, deadlines.contains_key(&0)),
        53
    );
    let owned = ingress.pending.lock();
    let before_grace = receiver.repair_requests_avoiding(
        20,
        194,
        5,
        |b, _| {
            let id = b / MAX_BLOCKS_PER_RECORD as u64;
            after_reordering(
                20,
                0,
                first_repair_grace(7, 500, 194, deadlines.contains_key(&id)),
                &mut wake,
            )
        },
        |b, s| owned.contains(b, s),
    );
    assert_eq!(before_grace.len(), 1);
    assert_eq!(before_grace[0].block_id, MAX_BLOCKS_PER_RECORD as u64);
    assert_eq!(before_grace[0].shard_indices, [0]);
    let mut eligible = pacing.batch_eligible(53);
    let next = receiver.repair_requests_avoiding(
        53,
        194,
        5,
        |b, s| {
            let id = b / MAX_BLOCKS_PER_RECORD as u64;
            after_reordering(
                53,
                0,
                first_repair_grace(7, 500, 194, deadlines.contains_key(&id)),
                &mut wake,
            ) && eligible(b, s)
        },
        |b, s| owned.contains(b, s),
    );
    drop(eligible);
    drop(owned);
    assert_eq!(next[0].block_id, 0);
    assert_eq!(next[0].shard_indices, [1]);
    assert_eq!(next[1].block_id, MAX_BLOCKS_PER_RECORD as u64);
    assert_eq!(next[1].shard_indices, [0]); // Middle prefix remains one.
    drop(packet);
    drop(sender);
    drop(ingress);
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    assert_eq!(budget.used(), 0);
}
