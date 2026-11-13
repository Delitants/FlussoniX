// Detect delayed head retries, fictitious shards/gaps and stealing known-loss resources.
use super::{
    plan,
    receive::{Ingress, Snapshot},
    wire::{Frame, Summary},
};
use crate::fxlink::feedback::DatagramRequest;
use crate::fxlink::{
    Budget, Capabilities, Encoder, Receiver, Settings,
    bootstrap::{self, PlannedRepair, Slot, View},
    repair::{Allowance, Pacing},
};
use bytes::Bytes;

fn record(length: usize) -> Bytes {
    let mut raw = vec![8; length];
    raw[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    raw.into()
}
struct Fixture {
    settings: Settings,
    budget: Budget,
    receiver: Receiver,
    pacing: Pacing,
    hints: std::sync::Arc<Ingress>,
}
impl Fixture {
    fn new(lengths: [usize; 3], units: usize, history: usize, blocks: usize) -> Self {
        let settings = Settings {
            latency_ms: 500,
            max_repair_shards: units,
            max_blocks: blocks,
            ..Settings::default()
        };
        let budget = Budget::new(4 << 20);
        let mut encoder = Encoder::new(settings.clone(), 9, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::V7);
        let frames = lengths.map(|length| encoder.encode(record(length), 0).unwrap());
        let mut receiver = Receiver::new(settings.clone(), 9, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::V7);
        receiver
            .ingest_charged(&frames[2].datagrams[0], 127)
            .unwrap();
        let pacing = Pacing::new(budget.clone(), history).unwrap();
        let hints = Ingress::new(&budget, &settings).unwrap();
        Self {
            settings,
            budget,
            receiver,
            pacing,
            hints,
        }
    }
    fn shapes(&self, lengths: &[usize]) -> Snapshot {
        let f = Frame {
            subscription: 9,
            generation: 1,
            sequence: 1,
            count: lengths.len(),
            entries: std::array::from_fn(|i| {
                if i < lengths.len() {
                    Summary {
                        record_id: i as u64,
                        record_len: lengths[i] as u32,
                    }
                } else {
                    Summary::default()
                }
            }),
        };
        self.hints.offer(&f, 127, 0);
        self.hints.snapshot(127, 0).unwrap()
    }
    fn view<'a>(&'a self, at: u64, pending: &'a dyn Fn(u64, u16) -> bool) -> View<'a> {
        View {
            receiver: &self.receiver,
            pacing: &self.pacing,
            settings: &self.settings,
            budget: &self.budget,
            at,
            rtt: 193,
            deadline: &|_| 627,
            authoritative_shape: &|_| false,
            geometry_known: &|id| id == 2,
            pending_shard: pending,
            pending_record: &|_| false,
        }
    }
    fn select(
        &self,
        at: u64,
        snapshot: &Snapshot,
        proof: Option<u64>,
        slot: &mut Slot,
        wake: &mut Option<u64>,
    ) -> Vec<PlannedRepair> {
        plan(self.view(at, &|_, _| false), snapshot, proof, slot, wake)
    }
}
fn shards(selected: &[PlannedRepair], block: u64) -> Vec<u16> {
    selected
        .iter()
        .find_map(|p| match &p.request {
            DatagramRequest::Ordinary(r) if r.block_id == block => Some(r.shard_indices.clone()),
            _ => None,
        })
        .unwrap_or_default()
}
fn comparable(selected: Vec<PlannedRepair>) -> Vec<(DatagramRequest, u64)> {
    selected
        .into_iter()
        .map(|p| (p.request, p.deadline))
        .collect()
}

#[test]
fn hint_shape_preserves_head_grace_and_known_loss_capacity() {
    let f = Fixture::new([4835, 9781, 4000], 24, 64, 2048);
    let hints = f.shapes(&[4835, 9781]);
    let mut slot = Slot::default();
    let mut wake = None;
    assert!(
        f.select(146, &hints, Some(2), &mut slot, &mut wake)
            .is_empty()
    );
    let early = f.select(147, &hints, Some(2), &mut slot, &mut wake);
    assert_eq!(shards(&early, 0), (0..6).collect::<Vec<_>>());
    assert!(shards(&early, 2048).is_empty());
    assert_eq!(wake, Some(147)); // earlier scheduled wake is not renewed by hints.
    assert_eq!(
        slot.wake_at(),
        None,
        "hint must not create a pending bootstrap guard"
    );
    let due = f.select(181, &hints, Some(2), &mut slot, &mut None);
    assert_eq!(shards(&due, 0), (0..6).collect::<Vec<_>>());
    assert_eq!(shards(&due, 2048), (0..11).collect::<Vec<_>>());
    assert_eq!(
        shards(&due, 4096),
        [1, 2, 3],
        "known later data lost its baseline capacity"
    );
    assert!(due.iter().all(|p| p.deadline == 627));
}

#[test]
fn no_hint_or_no_strong_proof_has_exact_v8_fallback() {
    {
        let proof = None;
        let f = Fixture::new([4835, 9781, 387], 64, 64, 2048);
        let hints = f.shapes(&[4835, 9781]);
        let mut old = Slot::default();
        let mut new = Slot::default();
        let (mut old_wake, mut new_wake) = (None, None);
        assert_eq!(
            comparable(f.select(147, &hints, proof, &mut new, &mut new_wake)),
            comparable(bootstrap::plan(
                f.view(147, &|_, _| false),
                &mut old,
                &mut old_wake
            ))
        );
        assert_eq!(old_wake, new_wake);
        assert_eq!(old.wake_at(), new.wake_at());
    }
    let f = Fixture::new([4835, 9781, 387], 64, 64, 2048);
    let empty = f.hints.snapshot(127, 0).unwrap();
    assert_eq!(
        comparable(f.select(181, &empty, Some(2), &mut Slot::default(), &mut None)),
        comparable(bootstrap::plan(
            f.view(181, &|_, _| false),
            &mut Slot::default(),
            &mut None
        ))
    );
}

#[test]
fn hint_only_cannot_create_a_gap_or_request_beyond_real_data() {
    let f = Fixture::new([4835, 388, 387], 64, 64, 2048);
    let hints = f.shapes(&[4835, 388]);
    let selected = f.select(181, &hints, Some(2), &mut Slot::default(), &mut None);
    assert_eq!(
        shards(&selected, 2048),
        [0],
        "single fragment has no hinted compact parity"
    );
    let receiver = Receiver::new(f.settings.clone(), 9, 1, f.budget.clone()).unwrap();
    let mut view = f.view(181, &|_, _| false);
    view.receiver = &receiver;
    assert!(plan(view, &hints, None, &mut Slot::default(), &mut None).is_empty());
    let exact = f.receiver.repair_requests_with_shapes(
        181,
        193,
        5,
        |_, _| true,
        |_, _| false,
        |id| if id == 0 { Some(4835) } else { None },
    );
    assert_eq!(exact[0].shard_indices, [0, 1, 2, 3, 4]);
}

#[test]
fn multiple_blocks_share_real_block_history_and_unit_limits() {
    for (blocks, history) in [(5, 64), (5, 3), (4, 64)] {
        let f = Fixture::new([70_000, 9781, 4000], 64, history, blocks);
        let hints = f.shapes(&[70_000, 9781]);
        let selected = f.select(181, &hints, Some(2), &mut Slot::default(), &mut None);
        assert_eq!(shards(&selected, 4096), [1, 2, 3]);
        assert!(selected.iter().map(|p| p.request.units()).sum::<usize>() <= 64);
        assert!(shards(&selected, 0).iter().all(|i| *i <= 20));
        if history == 3 {
            assert!(shards(&selected, 1).is_empty());
        } else {
            assert!(!shards(&selected, 1).is_empty());
        }
        assert!(
            shards(&selected, 3).is_empty(),
            "hypothetical blocks exceeded shared room"
        );
    }
    let f = Fixture::new([4835, 9781, 4000], 1, 64, 2048);
    let hints = f.shapes(&[4835, 9781]);
    let selected = f.select(181, &hints, Some(2), &mut Slot::default(), &mut None);
    assert_eq!(selected.len(), 1);
    assert_eq!(shards(&selected, 0), [0]);
}

#[test]
fn queued_shards_and_existing_bootstrap_guards_are_not_replaced() {
    let f = Fixture::new([4835, 9781, 387], 64, 64, 2048);
    let hints = f.shapes(&[4835, 9781]);
    let selected = plan(
        f.view(181, &|b, s| b == 0 && s == 0),
        &hints,
        Some(2),
        &mut Slot::default(),
        &mut None,
    );
    assert!(!shards(&selected, 0).contains(&0));
    let mut slot = Slot::default();
    bootstrap::plan(f.view(147, &|_, _| false), &mut slot, &mut None);
    assert_eq!(slot.wake_at(), Some(181));
    f.select(150, &hints, Some(2), &mut slot, &mut None);
    assert_eq!(slot.wake_at(), Some(181));
    let selected = f.select(181, &hints, Some(2), &mut slot, &mut None);
    assert!(
        selected
            .iter()
            .any(|p| matches!(p.request, DatagramRequest::Bootstrap { block_id: 2048, .. }))
    );
    slot.admitted(2048, 20, 181, 193).unwrap();
    f.select(200, &hints, Some(2), &mut slot, &mut None);
    assert_eq!(slot.wake_at(), Some(399));
}

#[test]
fn hinted_single_fragment_keeps_the_retained_early_retry_rescue() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(4 << 20);
    let mut encoder = Encoder::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let expected = [record(4835), record(388), record(405)];
    let frames = expected
        .iter()
        .zip([996, 1000, 1008])
        .map(|(r, at)| encoder.encode(r.clone(), at).unwrap())
        .collect::<Vec<_>>();
    let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    for packet in &frames[2].datagrams {
        receiver.ingest(packet, 0).unwrap();
    }
    let hints = Ingress::new(&budget, &settings).unwrap();
    let frame = Frame {
        subscription: 0,
        generation: 1,
        sequence: 1,
        count: 1,
        entries: std::array::from_fn(|i| {
            if i == 0 {
                Summary {
                    record_id: 1,
                    record_len: 388,
                }
            } else {
                Summary::default()
            }
        }),
    };
    hints.offer(&frame, 0, 0);
    let snapshot = hints.snapshot(0, 0).unwrap();
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    let mut slot = Slot::default();
    let mut allowance = Allowance::new(100, 500);
    assert!(allowance.original(1000, 1000, 96585));
    assert!(allowance.repair(1000, 1001, 6528));
    let mut attempts = Vec::new();
    for (at, source_age) in [(20, 272), (219, 464)] {
        if at == 219 {
            receiver.announce(0, 4835, 175).unwrap();
        }
        let selected = plan(
            View {
                receiver: &receiver,
                pacing: &pacing,
                settings: &settings,
                budget: &budget,
                at,
                rtt: 194,
                deadline: &|_| 500,
                authoritative_shape: &|_| false,
                geometry_known: &|id| id == 2 || (id == 0 && at >= 175),
                pending_shard: &|_, _| false,
                pending_record: &|_| false,
            },
            &snapshot,
            Some(2),
            &mut slot,
            &mut None,
        );
        assert_eq!(shards(&selected, 2048), [0]);
        for selected in selected {
            if let DatagramRequest::Ordinary(r) = selected.request {
                pacing.sent(r.block_id, &r.shard_indices, at, 194).unwrap();
            }
        }
        let admitted = source_age + 5 < 500
            && allowance.repair(1000, 1000 + source_age, frames[1].datagrams[0].len());
        attempts.push((at, source_age, admitted));
    }
    let mut output = Vec::new();
    for (index, at) in [262, 272, 273, 274, 275].into_iter().enumerate() {
        output.extend(receiver.ingest(&frames[0].datagrams[index], at).unwrap());
    }
    output.extend(receiver.ingest(&frames[1].datagrams[0], 412).unwrap());
    assert_eq!(attempts, [(20, 272, true), (219, 464, true)]);
    assert_eq!(output, expected);
    assert_eq!(allowance.counters(1000, 1494).1, 6528 + 2 * 472);
}

#[test]
fn middle_fallback_cannot_spend_freed_head_units_on_early_parity() {
    let f = Fixture::new([4835, 9781, 387], 21, 64, 2048);
    let hints = f.shapes(&[4835, 9781]);
    let mut wake = None;
    let mut slot = Slot::default();
    let early = f.select(147, &hints, Some(2), &mut slot, &mut wake);
    assert_eq!(
        shards(&early, 2048),
        [0],
        "middle expansion bypassed normal grace"
    );
    assert_eq!(shards(&early, 0), [0, 1, 2, 3, 4, 5]);
    assert_eq!(slot.wake_at(), None);
    assert_eq!(wake, Some(181));
}

#[test]
fn retained_snapshot_cannot_extend_weak_shape_past_its_original_expiry() {
    let f = Fixture::new([4835, 9781, 387], 64, 64, 2048);
    let frame = Frame {
        subscription: 9,
        generation: 1,
        sequence: 1,
        count: 1,
        entries: std::array::from_fn(|i| {
            if i == 0 {
                Summary {
                    record_id: 0,
                    record_len: 4835,
                }
            } else {
                Summary::default()
            }
        }),
    };
    f.hints.offer(&frame, 0, 0);
    let hints = f.hints.snapshot(0, 0).unwrap();
    let mut view = f.view(501, &|_, _| false);
    view.rtt = 10;
    let actual = plan(view, &hints, Some(2), &mut Slot::default(), &mut None);
    let expected = bootstrap::plan(view, &mut Slot::default(), &mut None);
    assert_eq!(comparable(actual), comparable(expected));
}

#[test]
fn inherited_discard_age_is_not_authoritative_record_geometry() {
    let f = Fixture::new([4835, 9781, 387], 64, 64, 2048);
    let hints = f.shapes(&[4835, 9781]);
    let mut view = f.view(180, &|_, _| false);
    view.geometry_known = &|_| true;
    assert!(
        plan(view, &hints, Some(2), &mut Slot::default(), &mut None).is_empty(),
        "weak shape shortened existing strong-age grace"
    );
    view.at = 181;
    let selected = plan(view, &hints, Some(2), &mut Slot::default(), &mut None);
    assert_eq!(
        shards(&selected, 0),
        [0, 1, 2, 3, 4, 5],
        "an inherited age was mistaken for an actual header"
    );
    assert_eq!(shards(&selected, 2048), (0..11).collect::<Vec<_>>());
    assert!(selected.iter().all(|request| request.deadline == 627));
}

#[test]
fn header_before_hint_queue_discard_keeps_ordinary_repair_geometry() {
    use crate::fxlink::{packet::Header, packet_queue, transport::Charged};
    let f = Fixture::new([4835, 9781, 387], 64, 64, 2048);
    let mut encoder = Encoder::new(f.settings.clone(), 9, 1, f.budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let head = encoder.encode(record(4835), 0).unwrap();
    let middle = encoder.encode(record(9781), 0).unwrap();
    let filler = &head.datagrams[0];
    let filler_header = Header::parse(filler).unwrap();
    let (sender, queue) = packet_queue::channel(&f.budget).unwrap();
    for _ in 0..packet_queue::CAPACITY {
        assert!(
            sender
                .try_send(
                    Charged::new(filler.clone(), &f.budget).unwrap(),
                    &filler_header
                )
                .is_ok()
        );
    }
    let packet = &middle.datagrams[0];
    let header = Header::parse(packet).unwrap();
    header
        .validate_profile(&f.settings, Capabilities::V7)
        .unwrap();
    // The router observes a validated header before its bounded queue attempt.
    f.hints
        .authoritative(header.record_id, header.record_len as usize);
    assert!(matches!(
        sender.try_send(Charged::new(packet.clone(), &f.budget).unwrap(), &header),
        Err(packet_queue::SendError::Full)
    ));
    assert!(!queue.pending.lock().contains_record(1));
    drop((sender, queue)); // no current queue/Receiver/deferred shape remains
    let hint = Frame {
        subscription: 9,
        generation: 1,
        sequence: 1,
        count: 1,
        entries: std::array::from_fn(|i| {
            if i == 0 {
                Summary {
                    record_id: 1,
                    record_len: 4835,
                }
            } else {
                Summary::default()
            }
        }),
    };
    f.hints.offer(&hint, 128, 0);
    let snapshot = f.hints.snapshot(181, 0).unwrap();
    assert_eq!(
        snapshot.shape(1),
        None,
        "discarded validated header lost to late weak shape"
    );
    let mut old_slot = Slot::default();
    let mut hint_slot = Slot::default();
    let (mut old_wake, mut hint_wake) = (None, None);
    assert_eq!(
        comparable(f.select(181, &snapshot, Some(2), &mut hint_slot, &mut hint_wake)),
        comparable(bootstrap::plan(
            f.view(181, &|_, _| false),
            &mut old_slot,
            &mut old_wake
        ))
    );
    assert_eq!(hint_slot.wake_at(), old_slot.wake_at());
    assert_eq!(hint_wake, old_wake);
}
