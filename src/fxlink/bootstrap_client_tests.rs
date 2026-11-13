//! Clock-injected ownership/timer wiring; encrypted busy-loop controls follow
//! once startup can explicitly negotiate8.
use super::*;
use crate::fxlink::{
    Encoder,
    bootstrap::{self, Slot, View},
    feedback::DatagramRequest,
    packet_queue,
    repair::Pacing,
    transport::Charged,
};

fn pending_slot(budget: &Budget) -> (Slot, Bytes, Settings, Pacing) {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let mut encoder = Encoder::new(settings.clone(), 9, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let make = |len: usize| {
        let mut bytes = vec![8; len];
        bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
        Bytes::from(bytes)
    };
    let _head = encoder.encode(make(4835), 0).unwrap();
    let middle = encoder.encode(make(9781), 0).unwrap();
    let successor = encoder.encode(make(387), 0).unwrap();
    let mut receiver = Receiver::new(settings.clone(), 9, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    for packet in &successor.datagrams {
        receiver.ingest_charged(packet, 127).unwrap();
    }
    let pacing = Pacing::new(budget.clone(), 64).unwrap();
    let mut slot = Slot::default();
    let mut wake = None;
    let requests = bootstrap::plan(
        View {
            receiver: &receiver,
            pacing: &pacing,
            settings: &settings,
            budget,
            at: 147,
            rtt: 193,
            deadline: &|_| 627,
            authoritative_shape: &|_| false,
            geometry_known: &|id| id == 2,
            pending_shard: &|_, _| false,
            pending_record: &|_| false,
        },
        &mut slot,
        &mut wake,
    );
    assert!(
        requests
            .iter()
            .all(|request| matches!(request.request, DatagramRequest::Ordinary(_)))
    );
    assert_eq!(slot.wake_at(), Some(181));
    (slot, middle.datagrams[3].clone(), settings, pacing)
}
#[tokio::test]
async fn profile_valid_header_abandons_pending_bootstrap_before_decode() {
    let budget = Budget::new(4 << 20);
    let (mut slot, bytes, settings, pacing) = pending_slot(&budget);
    let (sender, mut ingress) = packet_queue::channel(&budget).unwrap();
    let header = Header::parse(&bytes).unwrap();
    sender
        .try_send(Charged::new(bytes.clone(), &budget).unwrap(), &header)
        .unwrap_or_else(|_| panic!("queue"));
    sender
        .try_send(Charged::new(bytes, &budget).unwrap(), &header)
        .unwrap_or_else(|_| panic!("queue"));
    let invalid = ingress.packets.recv().await.unwrap();
    let state = State::new(CancellationToken::new());
    let mut deadlines = std::collections::BTreeMap::from([(2, 627)]);
    let mut discarded = DiscardFence::default();
    let mut wrong_profile = settings.clone();
    wrong_profile.parity_shards = 0;
    assert!(
        prepare_decode_packet(
            invalid,
            &wrong_profile,
            &state,
            0,
            &mut deadlines,
            &mut discarded,
            Capabilities::V7,
            Some(&mut slot)
        )
        .is_err()
    );
    assert_eq!(
        slot.wake_at(),
        Some(181),
        "invalid profile must not observe a header"
    );
    assert_eq!(state.telemetry.data.load(Ordering::Relaxed), 0);
    let packet = ingress.packets.recv().await.unwrap();
    let packet = prepare_decode_packet(
        packet,
        &settings,
        &state,
        0,
        &mut deadlines,
        &mut discarded,
        Capabilities::V7,
        Some(&mut slot),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        slot.wake_at(),
        None,
        "header establishes geometry before a FEC permit"
    );
    assert!(deadlines.get(&1).is_some_and(|deadline| *deadline <= 627));
    assert!(ingress.pending.lock().contains_record(1));
    drop(packet);
    drop(sender);
    drop(ingress);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}
#[test]
fn active_decoder_and_blocked_progress_keep_bootstrap_timers_bounded() {
    let budget = Budget::new(4 << 20);
    let (mut slot, bytes, _settings, mut pacing) = pending_slot(&budget);
    let first = slot.wake_at();
    assert_eq!(
        retry_wake_at(Some(&pacing), first, false, true, 0),
        None,
        "active decoder cannot spin a ready timer"
    );
    assert_eq!(
        retry_wake_at(Some(&pacing), first, true, true, 0),
        Some(181),
        "unordered repair is independent of blocked progress"
    );
    slot.admitted(2048, 20, 181, 193).unwrap();
    pacing
        .sent(2048, &(0..20).collect::<Vec<_>>(), 181, 193)
        .unwrap();
    pacing.prune(379, 0);
    assert_eq!(pacing.wake_at(), None);
    assert_eq!(
        retry_wake_at(Some(&pacing), slot.wake_at(), true, true, 0),
        Some(399)
    );
    slot.observe_header(1, 396);
    for at in 399..416 {
        slot.prune(at, 0);
        assert_eq!(
            retry_wake_at(Some(&pacing), slot.wake_at(), true, true, 0),
            Some(416)
        );
    }
    slot.prune(416, 0);
    assert_eq!(
        retry_wake_at(Some(&pacing), slot.wake_at(), true, true, 0),
        None
    );
    drop(bytes);
    drop(slot);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}
