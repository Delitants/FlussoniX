//! Separate causal v9 component controls, not a network/media qualification.
//! Real selection, FEC, age/credit accounting and native envelope decoding;
//! source replies and logical delivery times are fixture inputs, not Listener,
//! QUIC, decoder scheduling or native HTTP publication measurements.
use super::*;
use crate::fxlink::{
    EncodedRecord, Encoder, Error, ReceivedRecord, Receiver,
    bootstrap::{self, Slot, View},
    bootstrap_source,
    feedback::DatagramRequest,
    geometry::{self, receive::ConnectionIngress, send::Hub, wire},
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
    hints: Vec<(i64, u64, u64, usize, usize, bool)>,
    source_hints: serde_json::Value,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hints {
    Early,
    Lost,
    Delayed,
    Off,
}
#[derive(Clone, Copy)]
struct Scenario {
    whole_middle_reply_lost: bool,
    hints: Hints,
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
            whole_middle_reply_lost: false,
            hints: Hints::Early,
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

    // The same 81,738-byte contemporaneous cohort as the retained fixture,
    // now independently encoded and granted packet by packet at original
    // admission. Continuation records3..5 are outside the target delivery set.
    let cohort: Vec<_> = if scenario.extra_credit {
        [62248, 500, 558]
            .into_iter()
            .map(|length| encoder.encode(native_record(9, length), 1001).unwrap())
            .collect()
    } else {
        Vec::new()
    };
    assert_eq!(
        cohort
            .iter()
            .flat_map(|frame| &frame.datagrams)
            .map(Bytes::len)
            .sum::<usize>(),
        if scenario.extra_credit { 81738 } else { 0 }
    );
    let hub = Hub::new(&budget).unwrap();
    let source = hub
        .register(
            0,
            1,
            settings.clone(),
            &budget,
            Arc::new(|| true),
            CancellationToken::new(),
        )
        .unwrap();
    let connection = ConnectionIngress::new(&budget).unwrap();
    let ingress = connection.admit(&budget, &settings).unwrap();
    let mut hint_responses = BTreeMap::<u64, Vec<Bytes>>::new();
    let mut hint_wake = None;
    let mut first_middle_service = true;
    let mut allowance = Allowance::new(900, 500);
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

    for at in CREATED - 4..=first_seen + 500 {
        let mut published = false;
        for frame in frames
            .iter()
            .chain(&cohort)
            .filter(|frame| frame.created_ms == at)
        {
            if scenario.hints != Hints::Off {
                source.encoded(
                    frame.record_id,
                    Header::parse(&frame.datagrams[0]).unwrap().record_len as usize,
                    at,
                );
                published = true;
            }
            for packet in &frame.datagrams {
                assert!(allowance.original(frame.created_ms, at, packet.len()));
                if scenario.hints != Hints::Off {
                    source.original_admitted(packet.len());
                }
            }
        }
        if scenario.hints != Hints::Off && (published || hint_wake.is_some_and(|wake| at >= wake)) {
            hub.attempt_for_test(at, |packet| {
                let parsed = wire::decode(&packet, &settings).unwrap();
                let arrival = at
                    + 95
                    + if scenario.hints == Hints::Delayed {
                        100
                    } else {
                        0
                    };
                let lost = scenario.hints == Hints::Lost;
                result.hints.push((
                    at as i64 - CREATED as i64,
                    arrival - CREATED,
                    parsed.sequence,
                    packet.len(),
                    packet.len() + 128,
                    lost,
                ));
                if !lost {
                    hint_responses.entry(arrival).or_default().push(packet);
                }
                true
            });
        }
        hint_wake = hub.wake_at(at);
        if let Some(packets) = hint_responses.remove(&at) {
            for packet in packets {
                ingress.receive(&connection, &packet, at, delivered);
            }
        }
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
            ingress.authoritative(1, expected[1].len());
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
                ingress.authoritative(header.record_id, header.record_len as usize);
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
                    ingress.publish(delivered);
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
        let view = View {
            receiver: &receiver,
            pacing: &pacing,
            settings: &settings,
            budget: &budget,
            at,
            rtt: 193,
            deadline: &|id| discarded.deadline(&deadlines, id, at + 500),
            authoritative_shape: &|id| deadlines.contains_key(&id),
            geometry_known: &|id| deadlines.contains_key(&id),
            pending_shard: &|_, _| false,
            pending_record: &|_| false,
        };
        let snapshot = ingress.snapshot(at, delivered);
        let mut requests = if let Some(snapshot) = snapshot.filter(|_| scenario.hints != Hints::Off)
        {
            geometry::plan(
                view,
                &snapshot,
                deadlines.keys().copied().max(),
                &mut slot,
                &mut first_wake,
            )
        } else {
            bootstrap::plan(view, &mut slot, &mut first_wake)
        };
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
            let first_middle = id == 1 && first_middle_service;
            if id == 1 {
                first_middle_service = false;
            }
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
                            && (scenario.whole_middle_reply_lost
                                || (bootstrap && scenario.reply_prefix.is_some_and(|n| sent > n))
                                || (scenario.lost_parity && h.shard_index >= h.data_count)
                                || scenario.lost_data == Some(h.shard_index));
                        if !dropped {
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
    result.source_hints = hub.diagnostics();
    drop((
        responses,
        hint_responses,
        cohort,
        source,
        hub,
        ingress,
        connection,
    ));
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
fn causal_v9_hint_delivery_keeps_the_original_sizes_credit_and_service_costs() {
    let result = simulate(Scenario::default());
    assert_native_output(&result, [4835, 9781, 387]);
    eprintln!(
        "v9 causal: hints={:?} counters={} requests={:?} replies={:?} outputs={:?} credit={}/{}",
        result.hints,
        result.source_hints,
        result.requests,
        result.replies,
        result
            .output
            .iter()
            .map(|record| record.at)
            .collect::<Vec<_>>(),
        result.repaired,
        result.originals
    );
    assert!(result.output.iter().all(|record| record.at < 500));
    assert_eq!(result.originals, 102264);
    assert!(
        result.hints.iter().all(
            |(sent, arrival, _, application, modeled, lost)| *arrival as i64 - *sent == 95
                && *modeled == application + 128
                && !lost
        )
    );
    assert!(
        result
            .requests
            .iter()
            .filter(|(_, id, _)| *id < 2)
            .all(|(_, _, request)| matches!(request, DatagramRequest::Ordinary(_)))
    );
    let application: usize = result.hints.iter().map(|hint| hint.3).sum();
    assert_eq!(result.source_hints["application_bytes"], application);
    release(result);
}
#[test]
fn causal_lost_delayed_sparse_and_unaffordable_reply_controls_remain_explicit() {
    for (name, scenario) in [
        (
            "off",
            Scenario {
                hints: Hints::Off,
                ..Scenario::default()
            },
        ),
        (
            "all_hints_lost",
            Scenario {
                hints: Hints::Lost,
                ..Scenario::default()
            },
        ),
        (
            "hints_during_flight",
            Scenario {
                hints: Hints::Delayed,
                ..Scenario::default()
            },
        ),
        (
            "sparse",
            Scenario {
                extra_credit: false,
                ..Scenario::default()
            },
        ),
        (
            "partial_middle_reply_lost",
            Scenario {
                reply_prefix: Some(0),
                lost_data: Some(0),
                lost_parity: true,
                ..Scenario::default()
            },
        ),
        (
            "stale_hint_source",
            Scenario {
                first_seen: 427,
                geometry_at: 600,
                ..Scenario::default()
            },
        ),
        (
            "compact_middle",
            Scenario {
                middle_len: 387,
                ..Scenario::default()
            },
        ),
    ] {
        let result = simulate(scenario);
        eprintln!(
            "{name}: hints={:?} outputs={:?} expiry={:?} spent={}/{} requests={:?}",
            result.hints,
            result
                .output
                .iter()
                .map(|record| record.at)
                .collect::<Vec<_>>(),
            result.expired,
            result.repaired,
            result.originals,
            result.requests
        );
        if name == "stale_hint_source" {
            assert!(result.output.is_empty());
            assert_eq!(result.expired, Some(0));
        }
        if name == "sparse" {
            assert!(result.output.is_empty());
            assert_eq!(result.expired, Some(0));
        }
        if name == "partial_middle_reply_lost" {
            assert!(result.output.len() < 3);
        }
        if matches!(name, "off" | "all_hints_lost" | "hints_during_flight") {
            assert_native_output(&result, [4835, 9781, 387]);
        }
        if name == "compact_middle" {
            assert_native_output(&result, [4835, 387, 387]);
        }
        release(result);
    }
}

#[test]
fn whole_ordinary_middle_reply_loss_is_not_a_partial_drop() {
    let result = simulate(Scenario {
        whole_middle_reply_lost: true,
        ..Scenario::default()
    });
    assert_eq!(
        result
            .output
            .iter()
            .map(|record| record.at)
            .collect::<Vec<_>>(),
        [337],
        "whole reply fixture delivered missing middle media"
    );
    assert_eq!(result.expired, Some(1));
    eprintln!(
        "whole ordinary reply loss: hints={:?} repairs={}/{} expiry={:?}",
        result.hints, result.repaired, result.originals, result.expired
    );
    release(result);
}

#[test]
fn causal_late_hint_preserves_the_retained_false_probe_starvation() {
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
    let hub = Hub::new(&budget).unwrap();
    let source = hub
        .register(
            0,
            1,
            settings.clone(),
            &budget,
            Arc::new(|| true),
            CancellationToken::new(),
        )
        .unwrap();
    let connection = ConnectionIngress::new(&budget).unwrap();
    let ingress = connection.admit(&budget, &settings).unwrap();
    let mut hint = None;
    let mut allowance = Allowance::new(0, 1000);
    for frame in &frames {
        assert!(allowance.original(0, 0, frame.datagrams[0].len()));
        source.encoded(frame.record_id, 1000, 0);
        source.original_admitted(frame.datagrams[0].len());
    }
    assert_eq!(allowance.counters(0, 0), (5420, 0));
    let sent = hub.attempt_for_test(0, |packet| {
        hint = Some(packet);
        true
    });
    assert!(sent.sent);
    assert_eq!(
        (sent.application_bytes, sent.attempted_modeled_bytes),
        (92, 220)
    );
    let early_snapshot = ingress.snapshot(40, 0).unwrap();
    let mut output = receiver.ingest(&frames[0].datagrams[0], 0).unwrap();
    assert!(
        receiver
            .ingest(&frames[2].datagrams[0], 0)
            .unwrap()
            .is_empty()
    );
    let mut wake = None;
    let early = geometry::plan(
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
        &early_snapshot,
        Some(2),
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
    ingress.publish(3);
    // Same 300ms RTT / 150ms outbound leg as this retained sparse control.
    ingress.receive(&connection, &hint.unwrap(), 150, 3);
    receiver.announce(3, 1000, 200).unwrap();
    assert!(
        receiver
            .ingest(&frames[4].datagrams[0], 200)
            .unwrap()
            .is_empty()
    );
    pacing.prune(400, 3);
    let snapshot = ingress.snapshot(400, 3).unwrap();
    let requests = geometry::plan(
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
        &snapshot,
        Some(4),
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
    drop((early_snapshot, snapshot, ingress, connection, source, hub));
    drop(slot);
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    assert_eq!(budget.used(), 0);
}

#[test]
fn causal_eight_entry_coverage_does_not_invent_shapes_for_an_older_gap() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(4 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let hub = Hub::new(&budget).unwrap();
    let source = hub
        .register(
            0,
            1,
            settings.clone(),
            &budget,
            Arc::new(|| true),
            CancellationToken::new(),
        )
        .unwrap();
    let mut frames = Vec::new();
    for _ in 0..16 {
        let frame = encoder.encode(native_record(1, 387), 1000).unwrap();
        source.encoded(frame.record_id, 387, 1000);
        for packet in &frame.datagrams {
            source.original_admitted(packet.len());
        }
        frames.push(frame);
    }
    let mut packet = None;
    let sent = hub.attempt_for_test(1000, |wire| {
        packet = Some(wire);
        true
    });
    assert!(sent.sent);
    assert_eq!(
        (sent.application_bytes, sent.attempted_modeled_bytes),
        (128, 256)
    );
    let wire = packet.unwrap();
    let parsed = wire::decode(&wire, &settings).unwrap();
    assert_eq!(
        parsed.entries.map(|entry| entry.record_id),
        [8, 9, 10, 11, 12, 13, 14, 15]
    );
    let connection = ConnectionIngress::new(&budget).unwrap();
    let ingress = connection.admit(&budget, &settings).unwrap();
    ingress.receive(&connection, &wire, 1095, 0);
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    assert!(
        receiver
            .ingest(&frames[15].datagrams[0], 1127)
            .unwrap()
            .is_empty()
    );
    let snapshot = ingress.snapshot(1147, 0).unwrap();
    assert_eq!(snapshot.shape(0), None);
    assert_eq!(snapshot.shape(8), Some(387));
    let pacing = Pacing::new(budget.clone(), 64).unwrap();
    let requests = geometry::plan(
        View {
            receiver: &receiver,
            pacing: &pacing,
            settings: &settings,
            budget: &budget,
            at: 1147,
            rtt: 193,
            deadline: &|_| 1627,
            geometry_known: &|id| id == 15,
            authoritative_shape: &|id| id == 15,
            pending_shard: &|_, _| false,
            pending_record: &|_| false,
        },
        &snapshot,
        Some(15),
        &mut Slot::default(),
        &mut None,
    );
    let head = requests
        .iter()
        .find(|entry| entry.request.block_id() == 0)
        .unwrap();
    assert_eq!(head.deadline, 1627);
    assert!(
        matches!(&head.request,DatagramRequest::Ordinary(request) if request.shard_indices==(0..20).collect::<Vec<_>>())
    );
    drop((
        requests, pacing, snapshot, receiver, ingress, connection, frames, encoder, source, hub,
    ));
    assert_eq!(budget.used(), 0);
}

#[test]
fn admission_racing_queue_discard_never_exposes_a_conflicting_weak_shape() {
    use std::sync::Barrier;
    for order in 0..66 {
        let budget = Budget::new(4 << 20);
        let settings = Settings::default();
        let state = Arc::new(State::new(CancellationToken::new()));
        let connection = ConnectionIngress::new(&budget).unwrap();
        let mut encoder = Encoder::new(settings.clone(), 7, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::V7);
        drop(encoder.encode(native_record(0, 4835), 0).unwrap());
        let record = encoder.encode(native_record(1, 9781), 0).unwrap();
        let header = Header::parse(&record.datagrams[0]).unwrap();
        if order == 0 {
            // The first header hook saw no table; activation wins before Full.
            activate_geometry(&state, &connection, &budget, &settings);
            observe_geometry_queue_loss(&state, &header, 1000);
        } else if order == 1 {
            // Full wins before activation, so the route keeps ordinary fallback.
            observe_geometry_queue_loss(&state, &header, 1000);
            activate_geometry(&state, &connection, &budget, &settings);
        } else {
            let barrier = Arc::new(Barrier::new(2));
            let admitting = {
                let state = state.clone();
                let connection = connection.clone();
                let budget = budget.clone();
                let settings = settings.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    activate_geometry(&state, &connection, &budget, &settings);
                })
            };
            barrier.wait();
            observe_geometry_queue_loss(&state, &header, 1000);
            admitting.join().unwrap();
        }
        assert_eq!(state.telemetry.queue_dropped.load(Ordering::Acquire), 1);
        if let Some(ingress) = state.geometry.lock().unwrap().clone() {
            let frame = wire::Frame {
                subscription: 7,
                generation: 1,
                sequence: 1,
                count: 1,
                entries: std::array::from_fn(|i| {
                    if i == 0 {
                        wire::Summary {
                            record_id: 1,
                            record_len: 4835,
                        }
                    } else {
                        wire::Summary::default()
                    }
                }),
            };
            ingress.offer(&frame, 100, 0);
            assert_eq!(
                ingress.snapshot(100, 0).unwrap().shape(1),
                None,
                "activation lost a discarded strong header"
            );
        }
        assert_eq!(
            state.take_queue_loss(),
            Some((1, 1000)),
            "fallback changed media age"
        );
        drop((state, connection, record, encoder));
        assert_eq!(budget.used(), 0);
    }
}
