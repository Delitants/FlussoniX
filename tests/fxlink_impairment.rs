//! Deterministic application-shard simulation, not a QUIC/network performance benchmark.
use bytes::Bytes;
use flussonix::fxlink::{Budget, Encoder, Error, Receiver, Settings, packet::Header};
use std::{cmp::Reverse, collections::BinaryHeap};

const SUB_CAP: usize = 32 << 20;
const PEER_CAP: usize = 128 << 20;
const GLOBAL_CAP: usize = 512 << 20;
const SEED: u64 = 0x4658_4c4b_2026_1017;

fn record(body_len: usize) -> Bytes {
    flussonix::m4s::encode_gop(&flussonix::m4s::PackedGop {
        utc: 1_700_000_000,
        dts_ms: 1234.0,
        sequence: 7,
        duration_ms: 40.0,
        body: Bytes::from((0..body_len).map(|i| (i % 251) as u8).collect::<Vec<_>>()),
    })
    .unwrap()
}

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
enum Loss {
    Independent(u64),
    Burst(usize),
    FirstData,
}

#[derive(Debug)]
struct Scenario {
    mbps: u64,
    rtt: u64,
    latency: u64,
    loss: Loss,
    // Zero means ample capacity. Otherwise serialize *wire* bytes at this rate.
    capacity_bps: u64,
    repair: bool,
}

#[derive(Default, Debug)]
struct Outcome {
    delivered: bool,
    fec: u64,
    repairs: usize,
    requests: usize,
    expired: bool,
    duplicates: u64,
    reordered: bool,
    peak: usize,
}

fn simulate(case: &Scenario, settings: Settings, body_len: usize) -> Outcome {
    assert!(body_len < 16 << 20);
    let global = Budget::new(GLOBAL_CAP);
    let peer = global.child(PEER_CAP);
    // Combined sender retention and receiver state, stricter than separate endpoints.
    let subscription = peer.child(SUB_CAP);
    let mut encoder = Encoder::new(settings.clone(), 1, 2, subscription.clone()).unwrap();
    let native = record(body_len);
    let retained = encoder.encode(native.clone(), 0).unwrap();
    let mut receiver = Receiver::new(settings, 1, 2, subscription.clone()).unwrap();
    receiver.announce(0, native.len(), 0).unwrap();
    let mut outcome = Outcome::default();
    let mut random = Random(SEED ^ case.mbps ^ (case.rtt << 16) ^ (case.latency << 32));
    // Events reference the one retained owner: no per-event payload clones.
    let mut events = BinaryHeap::new();
    let mut serialized_bits = 0u64;
    let mut original_bytes = 0usize;
    let mut last_original_arrival = 0;
    let mut previous_index = None;
    let capacity = if case.capacity_bps == 0 {
        1_000_000_000
    } else {
        case.capacity_bps
    };
    for (index, packet) in retained.datagrams.iter().enumerate() {
        let header = Header::parse(packet).unwrap();
        let next_bits = serialized_bits + packet.len() as u64 * 8;
        let sent = (next_bits * 1000).div_ceil(capacity);
        if sent >= case.latency {
            break; // The sender's original cache deadline also bounds original enqueues.
        }
        serialized_bits = next_bits;
        original_bytes += packet.len();
        let lost = match case.loss {
            Loss::Independent(percent) => random.next() % 100 < percent,
            Loss::Burst(count) => (1..=count).contains(&index),
            Loss::FirstData => header.shard_index == 0,
        };
        if lost {
            continue;
        }
        let arrival = sent + case.rtt / 2 + random.next() % 51;
        last_original_arrival = last_original_arrival.max(arrival + 1);
        events.push(Reverse((arrival, index, false)));
        if index % 7 == 0 {
            events.push(Reverse((arrival + 1, index, false)));
        }
    }
    assert!(events.len() <= retained.datagrams.len() * 2);
    let mut repair_bytes = 0;
    let mut delivered = 0;
    for now in 0..=case.latency {
        outcome.peak = outcome.peak.max(subscription.used());
        assert!(subscription.used() <= SUB_CAP);
        assert_eq!(peer.used(), subscription.used());
        assert_eq!(global.used(), peer.used());
        if now == case.latency {
            if delivered == 0 {
                assert_eq!(
                    receiver.tick(now),
                    Err(Error::Expired { record_id: 0 }),
                    "{case:?}"
                );
                outcome.expired = true;
                assert!(receiver.is_retired());
                assert!(receiver.ingest(&retained.datagrams[0], now).is_err());
            }
            break;
        }
        while events.peek().is_some_and(|event| event.0.0 <= now) {
            let Reverse((arrival, index, repaired)) = events.pop().unwrap();
            if !repaired {
                outcome.reordered |= previous_index.is_some_and(|previous| index < previous);
                previous_index = Some(index);
            }
            for complete in receiver
                .ingest_charged(&retained.datagrams[index], arrival)
                .unwrap()
            {
                assert_eq!(complete.bytes, native, "{case:?}");
                assert_eq!(complete.deadline_ms, case.latency);
                assert!(arrival < complete.deadline_ms);
                outcome.peak = outcome.peak.max(subscription.used());
                delivered += 1;
                assert_eq!(delivered, 1, "duplicate delivery: {case:?}");
            }
        }
        if case.repair && now >= last_original_arrival && now % 20 == 0 {
            // Include all modeled jitter in the scheduling allowance. Requests are real
            // missing indices; responses are the retained, byte-identical original shard.
            let requests = receiver.repair_requests(now, case.rtt, 55);
            let requested = requests
                .iter()
                .map(|request| request.shard_indices.len())
                .sum::<usize>();
            assert!(requested <= 64);
            outcome.requests += requested;
            for request in requests {
                for (index, packet) in retained.datagrams.iter().enumerate() {
                    let header = Header::parse(packet).unwrap();
                    if header.block_id != request.block_id
                        || header.shard_index >= header.data_count
                        || !request.shard_indices.contains(&header.shard_index)
                        || now.saturating_sub(retained.created_ms) + case.rtt + 55 >= case.latency
                        || repair_bytes + packet.len() > original_bytes / 4
                    {
                        continue;
                    }
                    // Repairs share the same serialization queue as original/parity
                    // bytes. Capacity shortage cannot give them a fresh age budget.
                    let next_bits =
                        serialized_bits.max(now * capacity / 1000) + packet.len() as u64 * 8;
                    let sent = (next_bits * 1000).div_ceil(capacity);
                    if sent + case.rtt + 55 >= retained.created_ms + case.latency {
                        continue;
                    }
                    serialized_bits = next_bits;
                    repair_bytes += packet.len();
                    outcome.repairs += 1;
                    let arrival = sent + case.rtt + 5 + random.next() % 51;
                    assert!(arrival < case.latency);
                    events.push(Reverse((arrival, index, true)));
                }
            }
        }
        assert!(events.len() <= retained.datagrams.len() * 3);
    }
    assert_eq!(retained.created_ms, 0);
    assert!(repair_bytes <= original_bytes / 4);
    assert!(delivered == 1 || outcome.expired, "{case:?}");
    outcome.delivered = delivered == 1;
    outcome.fec = receiver.stats().fec_recovered_shards;
    outcome.duplicates = receiver.stats().duplicate_shards;
    assert_eq!(receiver.stats().expired_records, u64::from(outcome.expired));
    // Keeping the encoded owner alive charges every event reference and repair cache.
    receiver.cancel();
    drop(receiver);
    drop(retained);
    drop(encoder);
    assert_eq!(subscription.used(), 0);
    assert_eq!(peer.used(), 0);
    assert_eq!(global.used(), 0);
    outcome
}

#[test]
fn deterministic_random_and_burst_matrix_has_exact_bytes_or_bounded_retirement() {
    let mut cases = 0;
    let mut delivered = 0;
    let mut expired = 0;
    let mut fec_only = 0;
    let mut repaired = 0;
    let mut reordered = 0;
    let mut duplicates = 0;
    let mut peak = 0;
    for rtt in [20, 100, 250, 500] {
        for latency in [500, 1000, 1500] {
            for mbps in [1, 10, 20, 50] {
                for loss in [
                    Loss::Independent(0),
                    Loss::Independent(1),
                    Loss::Independent(3),
                    Loss::Independent(5),
                    Loss::Independent(10),
                    Loss::Burst(5),
                    Loss::Burst(20),
                    Loss::Burst(50),
                ] {
                    let case = Scenario {
                        mbps,
                        rtt,
                        latency,
                        loss,
                        capacity_bps: 0,
                        repair: true,
                    };
                    let result = simulate(
                        &case,
                        Settings {
                            latency_ms: latency,
                            ..Settings::default()
                        },
                        (mbps * 1_000_000 / 8 / 25) as usize,
                    );
                    if matches!(loss, Loss::Independent(0)) {
                        assert!(result.delivered, "zero loss must deliver: {case:?}");
                    }
                    cases += 1;
                    delivered += usize::from(result.delivered);
                    expired += usize::from(result.expired);
                    fec_only +=
                        usize::from(result.delivered && result.fec > 0 && result.repairs == 0);
                    repaired += usize::from(result.delivered && result.repairs > 0);
                    reordered += usize::from(result.reordered);
                    duplicates += result.duplicates;
                    peak = peak.max(result.peak);
                }
            }
        }
    }
    assert_eq!(cases, 384);
    assert_eq!(delivered + expired, cases);
    assert!(fec_only > 0 && repaired > 0 && expired > 0);
    assert!(reordered > 0 && duplicates > 0);
    assert!(peak < SUB_CAP);
    println!(
        "matrix cases={cases} delivered={delivered} expired={expired} fec_only={fec_only} repaired={repaired} reordered={reordered} duplicate_shards={duplicates} peak_charged_bytes={peak}"
    );
}

#[test]
fn fec_only_and_actual_nack_replay_are_distinct() {
    let mut case = Scenario {
        mbps: 20,
        rtt: 100,
        latency: 1000,
        loss: Loss::FirstData,
        capacity_bps: 0,
        repair: false,
    };
    let fec = simulate(&case, Settings::default(), 18_000);
    assert!(fec.delivered && fec.fec > 0);
    assert_eq!(fec.repairs, 0);
    case.repair = true;
    // Four data shards have zero proportional parity at 20+2, so only an actual
    // retained original replay can deliver this record. One repair fits the 25% cap.
    let nack = simulate(
        &case,
        Settings {
            parity_shards: 2,
            ..Settings::default()
        },
        3500,
    );
    assert!(nack.delivered && nack.requests > 0 && nack.repairs > 0);
    assert_eq!(nack.fec, 0);
    case.repair = false;
    let unrepaired = simulate(
        &case,
        Settings {
            parity_shards: 2,
            ..Settings::default()
        },
        3500,
    );
    assert!(unrepaired.expired && !unrepaired.delivered);
}

#[test]
fn sustained_erasure_exhausts_actual_repair_share_and_retires() {
    let case = Scenario {
        mbps: 20,
        rtt: 20,
        latency: 1000,
        loss: Loss::Burst(50),
        capacity_bps: 0,
        repair: true,
    };
    let result = simulate(&case, Settings::default(), 20_000);
    assert!(result.expired && !result.delivered);
    // This record has 25 equal-sized datagrams; at most six original replays
    // fit the 25% window cap, even though the core keeps reporting missing data.
    assert_eq!(result.repairs, 6);
    assert!(result.requests > result.repairs);
}

#[test]
fn insufficient_rtt_budget_and_capacity_expire_without_extending_age() {
    let mut case = Scenario {
        mbps: 50,
        rtt: 500,
        latency: 500,
        loss: Loss::Burst(20),
        capacity_bps: 0,
        repair: true,
    };
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let late_repair = simulate(&case, settings.clone(), 20_000);
    assert!(late_repair.expired);
    assert_eq!(late_repair.repairs, 0);
    case.rtt = 20;
    case.loss = Loss::Independent(0);
    case.capacity_bps = 125_000;
    let constrained = simulate(&case, settings, 250_000);
    assert!(constrained.expired && !constrained.delivered);
}

#[test]
fn healthy_subscription_progresses_while_damaged_subscription_retires() {
    let global = Budget::new(GLOBAL_CAP);
    let peer = global.child(PEER_CAP);
    let damaged_budget = peer.child(SUB_CAP);
    let healthy_budget = peer.child(SUB_CAP);
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let mut damaged = Receiver::new(settings.clone(), 1, 2, damaged_budget.clone()).unwrap();
    let mut healthy = Receiver::new(settings.clone(), 3, 4, healthy_budget.clone()).unwrap();
    let mut damaged_encoder = Encoder::new(settings.clone(), 1, 2, damaged_budget.clone()).unwrap();
    let mut healthy_encoder = Encoder::new(settings, 3, 4, healthy_budget.clone()).unwrap();
    let erased = damaged_encoder.encode(record(40_000), 0).unwrap();
    damaged.announce(0, record(40_000).len(), 0).unwrap();
    let mut delivered = 0;
    let mut peak = 0;
    // A deterministic fair scheduler gives each independent subscription a turn.
    for now in (0..=500).step_by(20) {
        if now < 500 {
            damaged.announce(0, record(40_000).len(), now).unwrap();
            assert!(
                damaged
                    .ingest(&erased.datagrams[0], now)
                    .unwrap()
                    .is_empty()
            );
        }
        let native = record(3000 + now as usize);
        let encoded = healthy_encoder.encode(native.clone(), now).unwrap();
        healthy
            .announce(encoded.record_id, native.len(), now)
            .unwrap();
        let mut output = Vec::new();
        for packet in &encoded.datagrams {
            output.extend(healthy.ingest_charged(packet, now).unwrap());
            peak = peak.max(peer.used());
            assert!(damaged_budget.used() <= SUB_CAP && healthy_budget.used() <= SUB_CAP);
            assert!(peer.used() <= PEER_CAP && global.used() <= GLOBAL_CAP);
        }
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].bytes, native);
        delivered += 1;
    }
    assert_eq!(delivered, 26);
    assert_eq!(damaged.tick(500), Err(Error::Expired { record_id: 0 }));
    assert!(damaged.is_retired());
    assert!(!healthy.is_retired());
    assert!(peak > 0);
    assert_eq!(damaged_budget.used(), global.used()); // only retained erased sender remains
    healthy.cancel();
    drop(erased);
    drop(damaged);
    drop(healthy);
    drop(damaged_encoder);
    drop(healthy_encoder);
    assert_eq!(damaged_budget.used(), 0);
    assert_eq!(healthy_budget.used(), 0);
    assert_eq!(peer.used(), 0);
    assert_eq!(global.used(), 0);
}

#[test]
fn hierarchical_admission_and_independent_count_caps_do_not_overcommit() {
    let global = Budget::new(8192);
    let peer = global.child(6144);
    let first = peer.child(4096);
    let second = peer.child(4096);
    let a = first.reserve(4096).unwrap();
    assert!(first.reserve(1).is_err());
    let b = second.reserve(2048).unwrap();
    assert!(second.reserve(1).is_err()); // peer ceiling, even with subscription room
    let other_peer = global.child(4096);
    let c = other_peer.reserve(2048).unwrap();
    assert!(other_peer.reserve(1).is_err()); // process ceiling
    assert_eq!(global.used(), 8192);
    drop((a, b, c));
    assert_eq!(global.used(), 0);
    assert_eq!(peer.used(), 0);
    assert_eq!(first.used(), 0);
    assert_eq!(second.used(), 0);
    let store = Budget::new(SUB_CAP);
    let mut receiver = Receiver::new(
        Settings {
            max_records: 2,
            ..Settings::default()
        },
        1,
        2,
        store.clone(),
    )
    .unwrap();
    receiver.announce(0, 1000, 0).unwrap();
    receiver.announce(1, 1000, 0).unwrap();
    assert!(store.used() < SUB_CAP / 100);
    assert!(receiver.announce(2, 1000, 0).is_err());
    assert!(receiver.is_retired());
    assert_eq!(store.used(), 0);
    let mut encoder = Encoder::new(Settings::default(), 1, 2, store.clone()).unwrap();
    let encoded = encoder.encode(record(1000), 0).unwrap();
    let tiny = Budget::new(1024);
    let mut denied = Receiver::new(Settings::default(), 1, 2, tiny.clone()).unwrap();
    assert!(denied.ingest(&encoded.datagrams[0], 0).is_err());
    assert!(denied.is_retired());
    assert_eq!(tiny.used(), 0);
    drop(encoded);
    assert_eq!(store.used(), 0);
}
