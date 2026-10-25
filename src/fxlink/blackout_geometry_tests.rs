//! Controlled component counterexample, not a network/media qualification.
//! Real selection, FEC, age/credit accounting and native envelope decoding;
//! source replies and logical delivery times are fixture inputs, not Listener,
//! QUIC, decoder scheduling or native HTTP publication measurements.
use super::*;
use crate::fxlink::{
    EncodedRecord, Encoder, Error, Receiver,
    repair::{Allowance, Pacing, after_reordering},
};
use std::collections::BTreeMap;

const CREATED: u64 = 1000;
const FIRST_SEEN: u64 = 1127;

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
    wire: Bytes,
}
#[derive(Debug, Default)]
struct Outcome {
    requests: Vec<(u64, u64, Vec<u16>)>,
    replies: Vec<(u64, u64, usize)>,
    output: Vec<Delivered>,
    expired: Option<u64>,
    originals: usize,
    repaired: usize,
}

fn simulate(geometry_at: u64, extra_cohort_credit: bool) -> Outcome {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(4 << 20);
    let expected = [
        native_record(0, 4835),
        native_record(1, 9781),
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
    assert_eq!(frames[1].datagrams.len(), 12, "10 data plus 2 parity");
    let mut allowance = Allowance::new(900, 500);
    for frame in &frames {
        for packet in &frame.datagrams {
            assert!(allowance.original(frame.created_ms, frame.created_ms, packet.len()));
        }
    }
    // Declared contemporaneous originals omitted from this three-record delivery
    // fixture. Credit is still the ordinary 25%, never renewed by a request.
    if extra_cohort_credit {
        assert!(allowance.original(CREATED, CREATED, 81738));
    }
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let mut pacing = Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    let mut deadlines = BTreeMap::new();
    let discarded = DiscardFence::default();
    let mut responses = BTreeMap::<u64, Vec<Bytes>>::new();
    let mut delivered = 0;
    let mut repair_at = CREATED;
    let mut not_before = 0;
    let mut first_wake = None;
    let mut result = Outcome::default();

    for at in CREATED..=FIRST_SEEN + 500 {
        if at == FIRST_SEEN {
            // Every original of records0/1 is absent, including all parity.
            // The successor proves both gaps without revealing their geometry.
            responses
                .entry(at)
                .or_default()
                .push(frames[2].datagrams[0].clone());
        }
        if at == CREATED + geometry_at {
            let deadline = discarded.deadline(&deadlines, 1, at + 500);
            remember_deadline(&mut deadlines, 1, delivered, settings.max_records, deadline)
                .unwrap();
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
                for record in receiver.ingest_charged(&packet, at).unwrap() {
                    let deadline = discarded
                        .deadline(&deadlines, delivered, record.deadline_ms)
                        .min(deadlines.remove(&delivered).unwrap());
                    assert!(at < deadline, "no late record can enter component output");
                    result.output.push(Delivered {
                        at: at - CREATED,
                        deadline: deadline - CREATED,
                        wire: record.bytes,
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
            break;
        }
        let retry = retry_wake_at(Some(&pacing), first_wake, true, true, not_before);
        if at < not_before || (at < repair_at && retry.is_none_or(|wake| at < wake)) {
            continue;
        }
        repair_at = at + 20;
        first_wake = None;
        pacing.prune(at, delivered);
        let mut eligible = pacing.batch_eligible(at);
        let requests = receiver.repair_requests_if(at, 193, 5, |block, shard| {
            let id = block / crate::fxlink::MAX_BLOCKS_PER_RECORD as u64;
            let deadline = discarded.deadline(&deadlines, id, at + 500);
            after_reordering(
                at,
                deadline.saturating_sub(500),
                first_repair_grace(7, 500, 193, deadlines.contains_key(&id)),
                &mut first_wake,
            ) && eligible(block, shard)
        });
        drop(eligible);
        if !requests.is_empty() {
            not_before = at + 20;
        }
        for request in requests {
            pacing
                .sent(request.block_id, &request.shard_indices, at, 193)
                .unwrap();
            let id = request.block_id / crate::fxlink::MAX_BLOCKS_PER_RECORD as u64;
            result
                .requests
                .push((at - CREATED, id, request.shard_indices.clone()));
            let source_at = at + 95;
            let frame = &frames[id as usize];
            let mut sent = 0;
            // Controlled source reply schedule. Real Allowance accounts every
            // selected cached packet; Listener enqueue/cancellation is not modeled.
            if source_at.saturating_sub(frame.created_ms) + 5 < 500 {
                let mut parity_sent = false;
                for packet in &frame.datagrams {
                    let h = Header::parse(packet).unwrap();
                    if !request.shard_indices.contains(&h.shard_index)
                        || (h.shard_index >= h.data_count && parity_sent)
                    {
                        continue;
                    }
                    if h.shard_index >= h.data_count {
                        parity_sent = true;
                    }
                    if allowance.repair(frame.created_ms, source_at, packet.len()) {
                        let first_middle_probe = id == 1 && request.shard_indices == [0];
                        let arrival = source_at + 95 + if first_middle_probe { 25 } else { 0 };
                        responses.entry(arrival).or_default().push(packet.clone());
                        sent += 1;
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
    drop(receiver);
    drop(pacing);
    drop(frames);
    drop(encoder);
    assert_eq!(
        budget.used(),
        0,
        "all FEC/history/record reservations released"
    );
    result
}

#[test]
#[ignore = "known gap: delayed geometry exceeds the 500ms source-to-output budget; no remedy yet"]
fn whole_record_blackout_meets_500ms_source_to_output_budget() {
    let result = simulate(335, true);
    assert_eq!(result.output.len(), 3, "{result:?}");
    assert!(
        result.output[1].at <= 500,
        "source-to-output budget exceeded: {}ms",
        result.output[1].at
    );
}

fn assert_native_output(result: &Outcome) {
    assert_eq!(result.output.len(), 3);
    let mut decoder = crate::m4s::Decoder::default();
    for (id, length) in [4835, 9781, 387].into_iter().enumerate() {
        let delivered = &result.output[id];
        assert_eq!(delivered.wire, native_record(id as u8, length));
        assert!(delivered.at < delivered.deadline);
        let events = decoder.push(&delivered.wire).unwrap();
        let [
            crate::m4s::Event::Frame {
                track_id,
                dts,
                pts_offset,
                key,
                body,
                wire,
            },
        ] = events.as_slice()
        else {
            panic!("one parsed native frame required")
        };
        assert_eq!(
            (*track_id, *dts, *pts_offset, *key),
            (1, id as u64 * 3600, 0, true)
        );
        assert_eq!(body, &vec![id as u8; length - 60]);
        assert_eq!(wire, &delivered.wire);
    }
}

#[test]
fn delayed_middle_geometry_separates_source_budget_from_receiver_expiry() {
    let result = simulate(335, true);
    assert_native_output(&result);
    let middle: Vec<_> = result
        .requests
        .iter()
        .filter(|(_, id, _)| *id == 1)
        .cloned()
        .collect();
    assert_eq!(
        middle,
        [
            (147, 1, vec![0]),
            (345, 1, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        ]
    );
    assert_eq!(result.requests[0], (147, 0, (0..20).collect()));
    assert_eq!(result.replies, [(242, 0, 6), (242, 1, 1), (440, 1, 11)]);
    assert_eq!(
        result.output.iter().map(|r| r.at).collect::<Vec<_>>(),
        [337, 535, 535]
    );
    assert_eq!(
        result.output.iter().map(|r| r.deadline).collect::<Vec<_>>(),
        [627, 627, 627]
    );
    assert_eq!(result.expired, None);
    assert_eq!((result.originals, result.repaired), (102264, 19584));
}

#[test]
fn earlier_geometry_allows_repair_before_the_same_source_budget() {
    let result = simulate(200, true);
    assert_native_output(&result);
    let middle: Vec<_> = result
        .requests
        .iter()
        .filter(|(_, id, _)| *id == 1)
        .cloned()
        .collect();
    assert_eq!(
        middle,
        [
            (147, 1, vec![0]),
            (207, 1, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        ]
    );
    assert_eq!(
        result.output.iter().map(|r| r.at).collect::<Vec<_>>(),
        [337, 397, 397]
    );
    assert_eq!(result.expired, None);
    assert_eq!((result.originals, result.repaired), (102264, 18496));
}

#[test]
fn repairs_cannot_borrow_unfunded_creation_cohort_credit() {
    let result = simulate(335, false);
    assert!(result.output.is_empty());
    assert_eq!(result.expired, Some(0));
    assert_eq!((result.originals, result.repaired), (20526, 4352));
    assert!(
        result
            .replies
            .iter()
            .all(|(_, id, count)| *id != 1 || *count == 0)
    );
}

#[test]
fn late_geometry_cannot_renew_the_original_gap_deadline() {
    let result = simulate(600, false);
    assert!(result.output.is_empty());
    assert_eq!(result.expired, Some(0));
    assert_eq!((result.originals, result.repaired), (20526, 4352));
}
