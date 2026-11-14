use super::*;
use crate::fxlink::{
    Budget, Capabilities, Encoder, Receiver, Settings, feedback::DatagramRequest, repair::Pacing,
};
use bytes::Bytes;
use std::collections::BTreeSet;

fn record(len: usize) -> Bytes {
    let mut bytes = vec![8; len];
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
struct Fixture {
    settings: Settings,
    budget: Budget,
    receiver: Receiver,
    pacing: Pacing,
    known: BTreeSet<u64>,
}
impl Fixture {
    fn new(cap: usize, history: usize) -> Self {
        let settings = Settings {
            latency_ms: 500,
            max_repair_shards: cap,
            ..Settings::default()
        };
        let budget = Budget::new(4 << 20);
        let mut encoder = Encoder::new(settings.clone(), 9, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::V7);
        let _head = encoder.encode(record(4835), 0).unwrap();
        let _middle = encoder.encode(record(9781), 0).unwrap();
        let successor = encoder.encode(record(387), 0).unwrap();
        let mut receiver = Receiver::new(settings.clone(), 9, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::V7);
        for packet in &successor.datagrams {
            assert!(receiver.ingest_charged(packet, 127).unwrap().is_empty());
        }
        let pacing = Pacing::new(budget.clone(), history).unwrap();
        Self {
            settings,
            budget,
            receiver,
            pacing,
            known: BTreeSet::from([2]),
        }
    }
    fn select(
        &self,
        at: u64,
        rtt: u64,
        slot: &mut Slot,
        pending: &[u64],
        wake: &mut Option<u64>,
    ) -> Vec<PlannedRepair> {
        plan(
            View {
                receiver: &self.receiver,
                pacing: &self.pacing,
                settings: &self.settings,
                budget: &self.budget,
                at,
                rtt,
                deadline: &|_| 627,
                authoritative_shape: &|_| false,
                geometry_known: &|id| self.known.contains(&id),
                pending_shard: &|_, _| false,
                pending_record: &|id| pending.contains(&id),
            },
            slot,
            wake,
        )
    }
}

#[test]
fn bootstrap_waits_normal_grace_and_uses_only_baseline_spare_units() {
    let mut f = Fixture::new(64, 64);
    let mut slot = Slot::default();
    let mut wake = None;
    assert!(f.receiver.unknown_middle(2048));
    assert!(!f.receiver.unknown_middle(0));
    assert!(!f.receiver.unknown_middle(2049));
    let early = f.select(147, 193, &mut slot, &[], &mut wake);
    assert_eq!(early.len(), 1);
    assert_eq!(early[0].request.block_id(), 0);
    assert_eq!(early[0].request.units(), 20);
    assert_eq!(wake, Some(181));
    assert_eq!(slot.wake_at(), Some(181));
    let DatagramRequest::Ordinary(head) = &early[0].request else {
        panic!("head must remain ordinary")
    };
    f.pacing
        .sent(head.block_id, &head.shard_indices, 147, 193)
        .unwrap();
    wake = None;
    let due = f.select(181, 193, &mut slot, &[], &mut wake);
    assert_eq!(due.len(), 1);
    assert_eq!(
        due[0].request,
        DatagramRequest::Bootstrap {
            block_id: 2048,
            reply_limit: 20
        }
    );
    assert_eq!(due[0].deadline, 627);
    assert!(slot.admitted(2048, 20, 181, 193).is_ok());
    assert_eq!(slot.wake_at(), Some(399));
    assert!(!slot.blocked(0, 200));
    assert!(slot.blocked(2048, 200));
}

#[test]
fn tiny_caps_and_bookkeeping_failure_keep_the_baseline() {
    let mut f = Fixture::new(1, 64);
    let mut slot = Slot::default();
    let mut wake = None;
    f.pacing.sent(0, &[0], 147, 193).unwrap();
    let selected = f.select(167, 193, &mut slot, &[], &mut wake);
    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected[0].request,
        DatagramRequest::Ordinary(crate::fxlink::RepairRequest {
            block_id: 2048,
            shard_indices: vec![0]
        })
    );
    assert_eq!(slot.wake_at(), None);
    let f = Fixture::new(64, 64);
    let used = f.budget.used();
    let hold = f.budget.reserve(f.budget.limit() - used).unwrap();
    let baseline = f.select(181, 193, &mut slot, &[], &mut wake);
    assert_eq!(baseline.len(), 2);
    assert!(
        baseline
            .iter()
            .all(|r| matches!(r.request, DatagramRequest::Ordinary(_)))
    );
    assert_eq!(slot.wake_at(), None);
    drop(hold);
    let mut f = Fixture::new(64, 1);
    f.pacing
        .sent(0, &(0..20).collect::<Vec<_>>(), 147, 193)
        .unwrap();
    assert!(f.select(167, 193, &mut slot, &[], &mut wake).is_empty());
    assert_eq!(f.pacing.wake_at(), Some(345));
}

#[test]
fn queued_geometry_and_pending_transition_use_ordinary_repairs() {
    let mut f = Fixture::new(64, 64);
    let mut slot = Slot::default();
    let mut wake = None;
    f.select(147, 193, &mut slot, &[], &mut wake);
    assert_eq!(slot.wake_at(), Some(181));
    let pending = f.select(150, 193, &mut slot, &[1], &mut wake);
    assert!(
        pending
            .iter()
            .all(|r| matches!(r.request, DatagramRequest::Ordinary(_)))
    );
    assert_eq!(slot.wake_at(), None);
    f.select(151, 193, &mut slot, &[], &mut wake);
    assert!(slot.wake_at().is_some());
    f.known.insert(1);
    let prepared = f.select(181, 193, &mut slot, &[], &mut wake);
    assert!(
        prepared
            .iter()
            .all(|r| matches!(r.request, DatagramRequest::Ordinary(_)))
    );
    assert_eq!(slot.wake_at(), None);
    f.receiver.announce(1, 800, 335).unwrap();
    let known = f.select(345, 193, &mut slot, &[], &mut wake);
    assert!(known.iter().any(|r|matches!(&r.request,DatagramRequest::Ordinary(request) if request.block_id==2048 && request.shard_indices==[0])));
    assert!(!f.receiver.unknown_middle(2048));
}

#[test]
fn bootstrap_guard_deadline_and_duplicate_boundaries() {
    let budget = Budget::new(1 << 20);
    let mut slot = Slot::default();
    assert!(slot.pending(2048, 627, 181, 20, &budget));
    slot.admitted(2048, 20, 181, 193).unwrap();
    assert_eq!(slot.wake_at(), Some(399));
    slot.observe_header(0, 395);
    assert_eq!(slot.wake_at(), Some(399));
    slot.observe_header(1, 396);
    assert_eq!(slot.wake_at(), Some(416));
    for at in [397, 400, 410, 415] {
        slot.observe_header(1, at);
        assert_eq!(slot.wake_at(), Some(416));
    }
    slot.prune(399, 0);
    assert!(slot.blocked(2048, 399));
    slot.prune(416, 0);
    assert_eq!(slot.wake_at(), None);
    slot.observe_header(1, 417);
    assert_eq!(slot.wake_at(), None);
    assert_eq!(budget.used(), 0);
    assert!(slot.pending(2048, 500, 181, 20, &budget));
    slot.admitted(2048, 20, 181, 193).unwrap();
    assert_eq!(
        slot.wake_at(),
        Some(379),
        "no extension when base exceeds last useful retry"
    );
    slot.observe_header(1, 370);
    assert_eq!(slot.wake_at(), Some(379));
    slot.prune(500, 0);
    assert_eq!(budget.used(), 0);
}

#[test]
fn first_header_is_one_shot_and_lost_reply_releases_the_guard() {
    let budget = Budget::new(1 << 20);
    let mut slot = Slot::default();
    assert!(slot.pending(2048, 627, 181, 20, &budget));
    slot.admitted(2048, 20, 181, 193).unwrap();
    slot.observe_header(1, 200);
    assert_eq!(slot.wake_at(), Some(399));
    slot.observe_header(1, 398);
    assert_eq!(
        slot.wake_at(),
        Some(399),
        "later trickle cannot become a second first header"
    );
    slot.prune(399, 0);
    assert_eq!(slot.wake_at(), None);
    assert_eq!(budget.used(), 0);
    assert!(slot.pending(2048, 30, 0, 2, &budget));
    slot.admitted(2048, 2, 0, 0).unwrap();
    assert_eq!(slot.wake_at(), Some(24));
    slot.prune(24, 0);
    assert!(!slot.blocked(2048, 24));
    for (deadline, queued, rtt) in [(3, 0, 0), (627, 0, u64::MAX), (u64::MAX, u64::MAX - 5, 0)] {
        assert!(slot.pending(2048, deadline, 0, 2, &budget));
        assert!(slot.admitted(2048, 2, queued, rtt).is_err());
        slot.abandon();
        assert_eq!(budget.used(), 0);
    }
    assert!(slot.pending(2048, 627, 181, 20, &budget));
    slot.admitted(2048, 20, 181, 193).unwrap();
    slot.prune(200, 2);
    assert_eq!(slot.wake_at(), None);
    assert_eq!(budget.used(), 0);
}

#[test]
fn optional_state_charges_actual_owned_metadata_and_releases_on_abandonment() {
    for depth in 1..=4 {
        let mut budget = Budget::new(1 << 20);
        for _ in 1..depth {
            budget = budget.child(1 << 20);
        }
        let mut slot = Slot::default();
        let metadata = depth * std::mem::size_of::<std::sync::Arc<()>>();
        assert_eq!(budget.reservation_metadata_bytes(), metadata);
        assert!(slot.pending(2048, 627, 181, 20, &budget));
        assert!(budget.used() >= 128.max(std::mem::size_of::<Slot>() + metadata));
        slot.observe_header(1, 150);
        assert_eq!(slot.wake_at(), None);
        assert_eq!(budget.used(), 0);
        assert!(slot.pending(2048, 627, 181, 20, &budget));
        slot.abandon();
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn prepared_history_has_no_send_effect_until_actual_admission() {
    let budget = Budget::new(1 << 20);
    let mut pacing = Pacing::new(budget.clone(), 1).unwrap();
    let before = budget.used();
    let prepared = pacing.prepare_block(5).unwrap();
    assert_eq!(budget.used(), before + 256);
    assert_eq!(pacing.wake_at(), None);
    assert!(pacing.eligible(5, 0, 0));
    drop(prepared);
    assert_eq!(budget.used(), before);
    let prepared = pacing.prepare_block(5).unwrap();
    pacing
        .sent_prepared(prepared, &[0, 127, 255], 100, 193)
        .unwrap();
    assert_eq!(pacing.wake_at(), Some(298));
    for index in [0, 127, 255] {
        assert!(!pacing.eligible(5, index, 101));
    }
    assert!(pacing.eligible(5, 1, 101));
    assert!(pacing.prepare_block(6).is_err());
    let existing = pacing.prepare_block(5).unwrap();
    assert_eq!(budget.used(), before + 256);
    drop(existing);
    pacing.prune(298, 0);
    assert_eq!(budget.used(), before);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}

#[test]
fn deferred_bootstrap_preserves_later_known_loss_and_head_slots() {
    let mut f = Fixture::new(64, 2);
    f.receiver = Receiver::new(f.settings.clone(), 9, 1, f.budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    f.receiver.announce(2, 20000, 127).unwrap();
    f.pacing
        .sent(0, &(0..20).collect::<Vec<_>>(), 100, 193)
        .unwrap();
    let mut slot = Slot::default();
    let mut wake = None;
    // Independent, conservative eligibility timestamps exercise reservation
    // ordering. This is not a chronological network/deadline-inheritance trace.
    let selected = plan(
        View {
            receiver: &f.receiver,
            pacing: &f.pacing,
            settings: &f.settings,
            budget: &f.budget,
            at: 160,
            rtt: 193,
            deadline: &|id| if id == 2 { 600 } else { 627 },
            authoritative_shape: &|_| false,
            geometry_known: &|id| id == 2,
            pending_shard: &|_, _| false,
            pending_record: &|_| false,
        },
        &mut slot,
        &mut wake,
    );
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].request.block_id(), 4096);
    assert_eq!(
        selected[0].request.units(),
        21,
        "known data and its admitted parity keep their units"
    );
    assert_eq!(slot.wake_at(), Some(181));
    assert_eq!(f.pacing.wake_at(), Some(298));
    let DatagramRequest::Ordinary(request) = &selected[0].request else {
        panic!("known loss remains ordinary")
    };
    f.pacing
        .sent(request.block_id, &request.shard_indices, 160, 193)
        .unwrap();
    assert!(f.select(181, 193, &mut slot, &[], &mut wake).is_empty());
    assert_eq!(
        slot.wake_at(),
        None,
        "a lost history opportunity abandons only optional pending state"
    );
    assert_eq!(f.pacing.wake_at(), Some(298));
}

#[tokio::test]
async fn decoder_owned_nonzero_shard_excludes_bootstrap_until_owner_drops() {
    let f = Fixture::new(64, 64);
    let mut slot = Slot::default();
    let mut wake = None;
    f.select(147, 193, &mut slot, &[], &mut wake);
    let (sender, mut ingress) = crate::fxlink::packet_queue::channel(&f.budget).unwrap();
    let mut encoder = Encoder::new(f.settings.clone(), 9, 1, f.budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let _head = encoder.encode(record(4835), 0).unwrap();
    let middle = encoder.encode(record(9781), 0).unwrap();
    let bytes = middle.datagrams[3].clone();
    let header = crate::fxlink::packet::Header::parse(&bytes).unwrap();
    sender
        .try_send(
            crate::fxlink::transport::Charged::new(bytes, &f.budget).unwrap(),
            &header,
        )
        .unwrap_or_else(|_| panic!("owned queue"));
    let owned = ingress.packets.recv().await.unwrap();
    let entries = ingress.pending.lock();
    assert!(entries.contains_record(1));
    assert!(!entries.contains_record(0));
    let selected = plan(
        View {
            receiver: &f.receiver,
            pacing: &f.pacing,
            settings: &f.settings,
            budget: &f.budget,
            at: 150,
            rtt: 193,
            deadline: &|_| 627,
            authoritative_shape: &|_| false,
            geometry_known: &|id| id == 2,
            pending_shard: &|block, index| entries.contains(block, index),
            pending_record: &|id| entries.contains_record(id),
        },
        &mut slot,
        &mut wake,
    );
    assert!(
        selected
            .iter()
            .all(|r| matches!(r.request, DatagramRequest::Ordinary(_)))
    );
    assert_eq!(slot.wake_at(), None);
    drop(entries);
    drop(owned);
    assert!(!ingress.pending.lock().contains_record(1));
}

#[test]
fn history_pressure_releases_optional_state_before_baseline_send() {
    let f = Fixture::new(64, 64);
    let mut slot = Slot::default();
    let mut wake = None;
    let before = f.budget.used();
    let mut selected = f.select(181, 193, &mut slot, &[], &mut wake);
    assert_eq!(selected.len(), 2);
    let slot_bytes = f.budget.used() - before;
    let leave = 512usize.checked_sub(slot_bytes).unwrap();
    let hold = f
        .budget
        .reserve(f.budget.limit() - f.budget.used() - leave)
        .unwrap();
    let held = f.budget.used();
    let prepared = prepare_history(&mut selected, &mut slot, &f.pacing)
        .expect("ordinary baseline must keep its existing history storage");
    assert_eq!(prepared.len(), 2);
    assert!(
        selected
            .iter()
            .all(|entry| matches!(entry.request, DatagramRequest::Ordinary(_)))
    );
    assert_eq!(
        selected
            .iter()
            .map(|entry| entry.request.units())
            .sum::<usize>(),
        21
    );
    assert_eq!(slot.wake_at(), None);
    assert_eq!(f.pacing.wake_at(), None, "no pacing before actual send");
    assert_eq!(f.budget.used(), held - slot_bytes + 512);
    drop(prepared);
    assert_eq!(f.budget.used(), held - slot_bytes);
    drop(hold);
    assert_eq!(f.budget.used(), before);
}

#[test]
fn flight_pressure_releases_only_optional_slot_before_ordinary_history() {
    let mut f = Fixture::new(64, 64);
    let mut slot = Slot::default();
    let mut wake = None;
    let mut selected = f.select(181, 193, &mut slot, &[], &mut wake);
    let prepared = prepare_history(&mut selected, &mut slot, &f.pacing).unwrap();
    for (entry, prepared) in selected.into_iter().zip(prepared) {
        match entry.request {
            DatagramRequest::Ordinary(r) => f
                .pacing
                .sent_prepared(prepared, &r.shard_indices, 181, 193)
                .unwrap(),
            DatagramRequest::Bootstrap {
                reply_limit,
                block_id,
            } => {
                f.pacing
                    .sent_prepared(prepared, &(0..reply_limit).collect::<Vec<_>>(), 181, 193)
                    .unwrap();
                slot.admitted(block_id, reply_limit, 181, 193).unwrap();
            }
        }
    }
    assert_eq!(slot.wake_at(), Some(399));
    assert_eq!(f.pacing.wake_at(), Some(379));
    let hold = f
        .budget
        .reserve(f.budget.limit() - f.budget.used() - 255)
        .unwrap();
    let mut unrelated = [PlannedRepair {
        request: DatagramRequest::Ordinary(RepairRequest {
            block_id: 3 * 2048,
            shard_indices: vec![0],
        }),
        deadline: 700,
    }];
    let prepared = prepare_history(&mut unrelated, &mut slot, &f.pacing)
        .expect("optional flight must not retire fundable ordinary repair");
    assert_eq!(slot.wake_at(), None);
    assert_eq!(prepared.len(), 1);
    assert!(!f.pacing.eligible(0, 0, 200));
    assert!(!f.pacing.eligible(2048, 0, 200));
    assert_eq!(f.pacing.wake_at(), Some(379));
    drop(prepared);
    drop(hold);
    let budget = f.budget.clone();
    drop(slot);
    drop(f);
    assert_eq!(budget.used(), 0);
}
#[test]
fn batch_metadata_pressure_sheds_pending_and_flight_without_losing_pacing() {
    for flight in [false, true] {
        let mut f = Fixture::new(64, 64);
        let mut slot = Slot::default();
        let mut wake = None;
        let mut selected = f.select(181, 193, &mut slot, &[], &mut wake);
        if flight {
            let prepared = prepare_history(&mut selected, &mut slot, &f.pacing).unwrap();
            for (entry, prepared) in selected.into_iter().zip(prepared) {
                let block = entry.request.block_id();
                let indices: Vec<u16> = match entry.request {
                    DatagramRequest::Ordinary(r) => r.shard_indices,
                    DatagramRequest::Bootstrap { reply_limit, .. } => (0..reply_limit).collect(),
                };
                f.pacing
                    .sent_prepared(prepared, &indices, 181, 193)
                    .unwrap();
                if block == 2048 {
                    slot.admitted(block, 20, 181, 193).unwrap();
                }
            }
        }
        let expected_wake = f.pacing.wake_at();
        let hold = f
            .budget
            .reserve(f.budget.limit() - f.budget.used() - 65535)
            .unwrap();
        let metadata = reserve_batch(&mut slot, &f.budget)
            .expect("optional slot must not prevent baseline batch metadata");
        assert_eq!(slot.wake_at(), None);
        assert_eq!(f.pacing.wake_at(), expected_wake);
        if flight {
            assert!(!f.pacing.eligible(2048, 0, 200));
        }
        drop(metadata);
        drop(hold);
        let budget = f.budget.clone();
        drop(slot);
        drop(f);
        assert_eq!(budget.used(), 0);
    }
    let budget = Budget::new(65535);
    let mut slot = Slot::default();
    assert!(reserve_batch(&mut slot, &budget).is_err());
    assert_eq!(budget.used(), 0);
}
