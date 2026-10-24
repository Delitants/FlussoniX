//! Separate opt-in v8 component controls, not a network/media qualification.
//! Real selection, FEC, age/credit accounting and native envelope decoding;
//! source replies and logical delivery times are fixture inputs, not Listener,
//! QUIC, decoder scheduling or native HTTP publication measurements.
use super::*;
use crate::fxlink::{
    EncodedRecord, Encoder, Error, ReceivedRecord, Receiver,
    bootstrap::{self, Slot, View},
    bootstrap_source,
    feedback::DatagramRequest,
    repair::{Allowance, Pacing},
};
use std::collections::BTreeMap;

const CREATED: u64 = 1000;

fn native_record(id: u8, length: usize) -> Bytes {
    let mut header = [0u8; 32];
    header[..4].copy_from_slice(&1u32.to_be_bytes());
    header[4] = 1;
    header[5] = 2;
    header[8..12].copy_from_slice(b"h264");
    header[12..20].copy_from_slice(&(u64::from(id) * 3600).to_be_bytes());
    let frame = crate::m4s::atom(
        b"FRam",
        &[
            crate::m4s::atom(b"fhdr", &header),
            crate::m4s::atom(b"body", &vec![id; length - 60]),
        ]
        .concat(),
    );
    let wire = [((length - 4) as u32).to_be_bytes().to_vec(), frame].concat();
    assert_eq!(wire.len(), length);
    Bytes::from(wire)
}

#[derive(Debug)]
struct Delivered {
    at: u64,
    deadline: u64,
    owner: ReceivedRecord,
}
#[derive(Default)]
struct Outcome {
    requests: Vec<(u64, u64, DatagramRequest)>,
    replies: Vec<(u64, u64, usize)>,
    output: Vec<Delivered>,
    expired: Option<u64>,
    expired_at: Option<u64>,
    known_deadlines: Vec<(u64, u64)>,
    originals: usize,
    repaired: usize,
    held_budget: Option<Budget>,
}

#[derive(Clone, Copy)]
struct Scenario {
    geometry_at: u64,
    extra_credit: bool,
    middle_len: usize,
    first_seen: u64,
    cap: usize,
    lost_feedback: bool,
    primary_lost_copy_survives: bool,
    reply_prefix: Option<usize>,
    lost_parity: bool,
    lost_data: Option<u16>,
    cache_miss: bool,
}
impl Default for Scenario {
    fn default() -> Self {
        Self {
            geometry_at: 335,
            extra_credit: true,
            middle_len: 9781,
            first_seen: 127,
            cap: 64,
            lost_feedback: false,
            primary_lost_copy_survives: false,
            reply_prefix: None,
            lost_parity: false,
            lost_data: None,
            cache_miss: false,
        }
    }
}
fn simulate(scenario: Scenario) -> Outcome {
    let settings = Settings {
        latency_ms: 500,
        max_repair_shards: scenario.cap,
        ..Settings::default()
    };
    let first_seen = CREATED + scenario.first_seen;
    let budget = Budget::new(4 << 20);
    let expected = [
        native_record(0, 4835),
        native_record(1, scenario.middle_len),
        native_record(2, 387),
    ];
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let frames: Vec<EncodedRecord> = expected
        .iter()
        .zip([996, CREATED, 1001])
        .map(|(bytes, at)| encoder.encode(bytes.clone(), at).unwrap())
        .collect();

    let mut allowance = Allowance::new(900, 500);
    for frame in &frames {
        for packet in &frame.datagrams {
            assert!(allowance.original(frame.created_ms, frame.created_ms, packet.len()));
        }
    }
    // Declared contemporaneous originals omitted from this three-record delivery
    // fixture. Credit is still the ordinary 25%, never renewed by a request.
    if scenario.extra_credit {
        assert!(allowance.original(CREATED, CREATED, 81738));
    }
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let mut pacing = Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    let mut deadlines = BTreeMap::new();
    let discarded = DiscardFence::default();
    let mut responses = BTreeMap::<u64, Vec<Bytes>>::new();
    let mut slot = Slot::default();
    let mut gate = bootstrap_source::Gate::default();
    let mut delivered = 0;
    let mut repair_at = CREATED;
    let mut not_before = 0;
    let mut first_wake: Option<u64> = None;
    let mut result = Outcome::default();

    for at in CREATED..=first_seen + 500 {
        if at == first_seen {
            // Every original of records0/1 is absent, including all parity.
            // The successor proves both gaps without revealing their geometry.
            responses
                .entry(at)
                .or_default()
                .push(frames[2].datagrams[0].clone());
        }
        if at == CREATED + scenario.geometry_at {
            let deadline = discarded.deadline(&deadlines, 1, at + 500);
            remember_deadline(&mut deadlines, 1, delivered, settings.max_records, deadline)
                .unwrap();
            result.known_deadlines.push((1, deadline - CREATED));
            receiver.announce(1, expected[1].len(), at).unwrap();
        }
        if let Some(packets) = responses.remove(&at) {
            for packet in packets {
                let header = Header::parse(&packet).unwrap();
                let deadline = discarded.deadline(&deadlines, header.record_id, at + 500);
                remember_deadline(
                    &mut deadlines,
                    header.record_id,
                    delivered,
                    settings.max_records,
                    deadline,
                )
                .unwrap();
                slot.observe_header(header.record_id, at);
                for record in receiver.ingest_charged(&packet, at).unwrap() {
                    let deadline = discarded
                        .deadline(&deadlines, delivered, record.deadline_ms)
                        .min(deadlines.remove(&delivered).unwrap());
                    assert!(at < deadline, "no late record can enter component output");
                    result.output.push(Delivered {
                        at: at - CREATED,
                        deadline: deadline - CREATED,
                        owner: record,
                    });
                    delivered += 1;
                }
            }
        }
        if delivered == 3 {
            break;
        }
        if let Err(error) = receiver.tick(at) {
            let Error::Expired { record_id } = error else {
                panic!("{error}")
            };
            result.expired = Some(record_id);
            result.expired_at = Some(at - CREATED);
            break;
        }
        slot.prune(at, delivered);
        let first = match (first_wake, slot.wake_at()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let retry = retry_wake_at(Some(&pacing), first, true, true, not_before);
        if at < not_before || (at < repair_at && retry.is_none_or(|wake| at < wake)) {
            continue;
        }
        repair_at = at + 20;
        first_wake = None;
        pacing.prune(at, delivered);
        let mut requests = bootstrap::plan(
            View {
                receiver: &receiver,
                pacing: &pacing,
                settings: &settings,
                budget: &budget,
                at,
                rtt: 193,
                deadline: &|id| discarded.deadline(&deadlines, id, at + 500),
                authoritative_shape: &|_| false,
                geometry_known: &|id| deadlines.contains_key(&id),
                pending_shard: &|_, _| false,
                pending_record: &|_| false,
            },
            &mut slot,
            &mut first_wake,
        );
        let prepared = bootstrap::prepare_history(&mut requests, &mut slot, &pacing).unwrap();
        if !requests.is_empty() {
            not_before = at + 20;
        }
        for (entry, prepared) in requests.into_iter().zip(prepared) {
            let request = entry.request;
            let block = request.block_id();
            let id = block / crate::fxlink::MAX_BLOCKS_PER_RECORD as u64;
            let bootstrap = matches!(request, DatagramRequest::Bootstrap { .. });
            let indices = match &request {
                DatagramRequest::Ordinary(r) => r.shard_indices.clone(),
                DatagramRequest::Bootstrap { reply_limit, .. } => (0..*reply_limit).collect(),
            };
            pacing.sent_prepared(prepared, &indices, at, 193).unwrap();
            if let DatagramRequest::Bootstrap { reply_limit, .. } = request {
                slot.admitted(block, reply_limit, at, 193).unwrap();
            }
            result.requests.push((at - CREATED, id, request.clone()));
            let source_at = at
                + 95
                + if bootstrap && scenario.primary_lost_copy_survives {
                    20
                } else {
                    0
                };
            let frame = &frames[id as usize];
            let mut sent = 0;
            // Controlled source reply schedule. Real Allowance accounts every
            // selected cached packet; Listener enqueue/cancellation is not modeled.
            if (id != 1 || !scenario.cache_miss)
                && (!bootstrap
                    || (!scenario.lost_feedback && !scenario.cache_miss && gate.admit(source_at)))
                && bootstrap_source::age_valid(frame.created_ms, source_at, 500)
            {
                let selected: Vec<&Bytes> = match &request {
                    DatagramRequest::Bootstrap { reply_limit, .. } => {
                        bootstrap_source::packets(frame, block, *reply_limit)
                            .collect::<Result<_, _>>()
                            .unwrap()
                    }
                    DatagramRequest::Ordinary(r) => {
                        let mut parity = false;
                        frame
                            .datagrams
                            .iter()
                            .filter(|p| {
                                let h = Header::parse(p).unwrap();
                                if h.block_id != block || !r.shard_indices.contains(&h.shard_index)
                                {
                                    return false;
                                }
                                if h.shard_index >= h.data_count {
                                    if parity {
                                        return false;
                                    }
                                    parity = true;
                                }
                                true
                            })
                            .collect()
                    }
                };
                for packet in selected {
                    let h = Header::parse(packet).unwrap();
                    if allowance.repair(frame.created_ms, source_at, packet.len()) {
                        sent += 1;
                        let dropped = id == 1
                            && ((bootstrap && scenario.reply_prefix.is_some_and(|n| sent > n))
                                || (scenario.lost_parity && h.shard_index >= h.data_count)
                                || scenario.lost_data == Some(h.shard_index));
                        if !dropped {
                            let first_middle = id == 1
                                && (bootstrap
                                    || matches!(&request,DatagramRequest::Ordinary(r) if r.shard_indices==[0]));
                            let arrival = source_at + 95 + if first_middle { 25 } else { 0 };
                            responses.entry(arrival).or_default().push(packet.clone());
                        }
                    } else if bootstrap {
                        break;
                    }
                }
            }
            result.replies.push((source_at - CREATED, id, sent));
        }
    }
    let (originals, repaired) = allowance.counters(CREATED, CREATED + 499);
    result.originals = originals;
    result.repaired = repaired;
    assert!(repaired <= originals / 4);
    drop(responses);
    drop(slot);
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    let held = result
        .output
        .iter()
        .map(|r| r.owner.bytes.len())
        .sum::<usize>();
    assert!(
        budget.used() >= held,
        "returned record owners must stay charged"
    );
    result.held_budget = Some(budget);
    result
}

fn release(mut result: Outcome) {
    let budget = result.held_budget.take().unwrap();
    result.output.clear();
    assert_eq!(
        budget.used(),
        0,
        "all owned FEC/history/output charges released"
    );
}
fn assert_native_output(result: &Outcome, lengths: [usize; 3]) {
    assert_eq!(result.output.len(), 3);
    let mut decoder = crate::m4s::Decoder::default();
    for (id, length) in lengths.into_iter().enumerate() {
        let delivered = &result.output[id];
        let wire = &delivered.owner.bytes;
        assert_eq!(*wire, native_record(id as u8, length));
        assert!(delivered.at < delivered.deadline);
        let events = decoder.push(wire).unwrap();
        let [
            crate::m4s::Event::Frame {
                track_id,
                dts,
                pts_offset,
                key,
                body,
                wire: parsed,
            },
        ] = events.as_slice()
        else {
            panic!("one native frame")
        };
        assert_eq!(
            (*track_id, *dts, *pts_offset, *key),
            (1, id as u64 * 3600, 0, true)
        );
        assert_eq!(body, &vec![id as u8; length - 60]);
        assert_eq!(parsed, wire);
    }
}
#[test]
fn bootstrap_blackout_meets_500ms_source_to_output_budget() {
    let result = simulate(Scenario::default());
    assert_native_output(&result, [4835, 9781, 387]);
    let middle: Vec<_> = result
        .requests
        .iter()
        .filter(|(_, id, _)| *id == 1)
        .cloned()
        .collect();
    assert_eq!(
        middle,
        [(
            181,
            1,
            DatagramRequest::Bootstrap {
                block_id: 2048,
                reply_limit: 20
            }
        )]
    );
    assert_eq!(result.replies, [(242, 0, 6), (276, 1, 11)]);
    assert_eq!(
        result.output.iter().map(|r| r.at).collect::<Vec<_>>(),
        [337, 396, 396]
    );
    assert_eq!(
        result.output.iter().map(|r| r.deadline).collect::<Vec<_>>(),
        [627, 627, 627]
    );
    assert_eq!(result.expired, None);
    assert_eq!((result.originals, result.repaired), (102264, 18496));
    release(result);
}

#[test]
fn bootstrap_negative_and_variation_controls() {
    for (name, scenario) in [
        (
            "earlier",
            Scenario {
                geometry_at: 200,
                ..Scenario::default()
            },
        ),
        (
            "unfunded",
            Scenario {
                extra_credit: false,
                ..Scenario::default()
            },
        ),
        (
            "all_feedback_lost",
            Scenario {
                lost_feedback: true,
                ..Scenario::default()
            },
        ),
        (
            "primary_lost_copy_survives",
            Scenario {
                primary_lost_copy_survives: true,
                ..Scenario::default()
            },
        ),
        (
            "whole_reply_lost",
            Scenario {
                reply_prefix: Some(0),
                ..Scenario::default()
            },
        ),
        (
            "partial_reply",
            Scenario {
                reply_prefix: Some(3),
                ..Scenario::default()
            },
        ),
        (
            "parity_lost",
            Scenario {
                lost_parity: true,
                ..Scenario::default()
            },
        ),
        (
            "one_data_lost",
            Scenario {
                lost_data: Some(3),
                ..Scenario::default()
            },
        ),
        (
            "cache_miss",
            Scenario {
                cache_miss: true,
                ..Scenario::default()
            },
        ),
        (
            "near_expiry",
            Scenario {
                first_seen: 427,
                geometry_at: 600,
                ..Scenario::default()
            },
        ),
        (
            "two_blocks",
            Scenario {
                middle_len: 40000,
                ..Scenario::default()
            },
        ),
        (
            "compact",
            Scenario {
                middle_len: 387,
                ..Scenario::default()
            },
        ),
        (
            "trimmed",
            Scenario {
                middle_len: 1000,
                ..Scenario::default()
            },
        ),
        (
            "one_unit",
            Scenario {
                cap: 1,
                ..Scenario::default()
            },
        ),
        (
            "two_units",
            Scenario {
                cap: 2,
                ..Scenario::default()
            },
        ),
    ] {
        let result = simulate(scenario);
        let output: Vec<_> = result.output.iter().map(|r| r.at).collect();
        eprintln!(
            "{name}:outputs={output:?} expired={:?} credit={}/{} requests={:?} replies={:?}",
            result.expired, result.repaired, result.originals, result.requests, result.replies
        );
        let (expected_output, expected_expiry, expected_spent): (Vec<u64>, Option<u64>, usize) =
            match name {
                "earlier" | "parity_lost" | "one_data_lost" => (vec![337, 396, 396], None, 18496),
                "unfunded" => (vec![], Some(0), 4352),
                "all_feedback_lost" => (vec![337, 589, 589], None, 18496),
                "primary_lost_copy_survives" => (vec![337, 416, 416], None, 25024),
                "whole_reply_lost" => (vec![337], Some(1), 25024),
                "partial_reply" => (vec![337], Some(1), 18496),
                "cache_miss" => (vec![337], Some(1), 6528),
                "near_expiry" => (vec![], Some(0), 0),
                "two_blocks" => (vec![337], Some(1), 34816),
                "compact" => (vec![337, 396, 396], None, 7470),
                "trimmed" => (vec![337, 396, 396], None, 7612),
                "one_unit" => (vec![], Some(0), 5440),
                "two_units" => (vec![551], Some(1), 8704),
                _ => unreachable!(),
            };
        assert_eq!(output, expected_output, "{name}");
        assert_eq!(result.expired, expected_expiry, "{name}");
        assert_eq!(result.repaired, expected_spent, "{name}");
        assert!(result.repaired <= result.originals / 4);
        assert!(result.output.iter().all(|r| r.at < r.deadline));
        assert!(
            result.output.len() == 3 || result.expired.is_some(),
            "bounded completion or original expiry:{name}"
        );
        if result.output.len() == 3 {
            assert_native_output(&result, [4835, scenario.middle_len, 387]);
        }
        if name == "unfunded" {
            assert!(result.output.is_empty());
            assert_eq!(result.expired, Some(0));
            assert_eq!((result.originals, result.repaired), (20526, 4352));
        }
        if name == "parity_lost" || name == "one_data_lost" {
            assert_eq!(output, [337, 396, 396]);
        }
        release(result);
    }
}

#[test]
fn bootstrap_output_keeps_charge_until_downstream_drop() {
    let mut result = simulate(Scenario::default());
    let budget = result.held_budget.as_ref().unwrap().clone();
    let owner = result.output.remove(1).owner;
    drop(result);
    assert!(budget.used() >= owner.bytes.len());
    assert_eq!(owner.bytes, native_record(1, 9781));
    drop(owner);
    assert_eq!(budget.used(), 0);
}

#[test]
fn bootstrap_middle_expiry_after_head_drains() {
    let result = simulate(Scenario {
        geometry_at: 600,
        cache_miss: true,
        ..Scenario::default()
    });
    assert_eq!(result.output.len(), 1);
    assert_eq!(result.output[0].at, 337);
    assert_eq!(result.output[0].owner.bytes, native_record(0, 4835));
    assert_eq!(result.expired, Some(1));
    assert_eq!(result.expired_at, Some(627));
    assert_eq!(result.known_deadlines, [(1, 627)]);
    release(result);
}

#[test]
fn bootstrap_false_probe_credit_control() {
    // Exact intact-reorder/later-loss inputs retained from early_probe_policy_tests.
    let settings = Settings {
        latency_ms: 1000,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let expected: Vec<_> = (0..5)
        .map(|id| {
            let mut b = vec![id; 1000];
            b[..4].copy_from_slice(&996u32.to_be_bytes());
            Bytes::from(b)
        })
        .collect();
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let frames: Vec<_> = expected
        .iter()
        .map(|r| encoder.encode(r.clone(), 0).unwrap())
        .collect();
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    let mut slot = Slot::default();
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
    let early = bootstrap::plan(
        View {
            receiver: &receiver,
            pacing: &pacing,
            settings: &settings,
            budget: &budget,
            at: 40,
            rtt: 300,
            deadline: &|_| 1000,
            authoritative_shape: &|_| false,
            geometry_known: &|id| id == 0 || id == 2,
            pending_shard: &|_, _| false,
            pending_record: &|_| false,
        },
        &mut slot,
        &mut wake,
    );
    assert_eq!(early.len(), 1);
    let DatagramRequest::Ordinary(request) = &early[0].request else {
        panic!("unknown head stays ordinary")
    };
    pacing
        .sent(request.block_id, &request.shard_indices, 40, 300)
        .unwrap();
    assert!(allowance.repair(0, 190, frames[1].datagrams[0].len()));
    output.extend(receiver.ingest(&frames[1].datagrams[0], 60).unwrap());
    receiver.announce(3, 1000, 200).unwrap();
    assert!(
        receiver
            .ingest(&frames[4].datagrams[0], 200)
            .unwrap()
            .is_empty()
    );
    pacing.prune(400, 3);
    let requests = bootstrap::plan(
        View {
            receiver: &receiver,
            pacing: &pacing,
            settings: &settings,
            budget: &budget,
            at: 400,
            rtt: 300,
            deadline: &|_| 1200,
            authoritative_shape: &|_| false,
            geometry_known: &|_| true,
            pending_shard: &|_, _| false,
            pending_record: &|_| false,
        },
        &mut slot,
        &mut wake,
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].request.block_id(), 3 * 2048);
    assert!(
        !allowance.repair(0, 550, frames[3].datagrams[0].len()),
        "preserve actual starvation; no extra credit"
    );
    assert!(matches!(
        receiver.tick(1200),
        Err(Error::Expired { record_id: 3 })
    ));
    assert_eq!(output, expected[..3]);
    assert_eq!(allowance.counters(0, 999), (5420, 1084));
    drop(slot);
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    assert_eq!(budget.used(), 0);
}
