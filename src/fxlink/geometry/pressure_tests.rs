//! Exercise real mandatory owners while v8's pending/flight state survives.
use super::receive::ConnectionIngress;
use super::wire::{Frame, Summary};
use crate::fxlink::{
    Budget, Receiver, Settings,
    bootstrap::{self, Slot, View},
    repair::Pacing,
    transport::Charged,
};
use std::sync::{Arc, Barrier};
#[test]
fn pending_and_flight_keep_age_quiet_and_history_when_mandatory_owners_shed_geometry() {
    for flight in [false, true] {
        for kind in ["batch", "history", "media", "output"] {
            let root = Budget::new(512 * 1024);
            let connection = ConnectionIngress::new(&root).unwrap();
            let budget = connection.scoped(&root);
            let settings = Settings {
                latency_ms: 500,
                ..Settings::default()
            };
            let ingress = connection.admit(&budget, &settings).unwrap();
            ingress.offer(
                &Frame {
                    subscription: 7,
                    generation: 1,
                    sequence: 1,
                    count: 1,
                    entries: std::array::from_fn(|i| {
                        if i == 0 {
                            Summary {
                                record_id: 1,
                                record_len: 9781,
                            }
                        } else {
                            Summary::default()
                        }
                    }),
                },
                127,
                0,
            );
            let snapshot = ingress.snapshot(147, 0).unwrap();
            let mut receiver = Receiver::new(settings.clone(), 7, 1, budget.clone()).unwrap();
            receiver.announce(2, 387, 127).unwrap();
            let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
            pacing.sent(4096, &[0], 127, 193).unwrap();
            let mut slot = Slot::default();
            let mut wake = None;
            let _ = bootstrap::plan(
                View {
                    receiver: &receiver,
                    pacing: &pacing,
                    settings: &settings,
                    budget: &budget,
                    at: 147,
                    rtt: 193,
                    deadline: &|_| 627,
                    geometry_known: &|id| id == 2,
                    authoritative_shape: &|id| id == 2,
                    pending_shard: &|_, _| false,
                    pending_record: &|_| false,
                },
                &mut slot,
                &mut wake,
            );
            assert!(slot.has_attempt(), "no real pending guard in {kind}");
            if flight {
                slot.admitted(2048, 20, 181, 193).unwrap();
            }
            let original_wake = slot.wake_at();
            let need = match kind {
                "batch" => 65536,
                "history" => 256,
                "media" => 3640,
                _ => 1128,
            };
            let held = root
                .reserve(root.limit() - root.used() - (need - 1))
                .unwrap();
            match kind {
                "batch" => {
                    let _owner = bootstrap::reserve_batch(&mut slot, &budget).unwrap();
                }
                "history" => {
                    let prepared = pacing.prepare_block(0).unwrap();
                    pacing.sent_prepared(prepared, &[0], 200, 193).unwrap();
                }
                "media" => receiver.announce(0, 3000, 200).unwrap(),
                _ => {
                    let _owner = Charged::new(bytes::Bytes::from(vec![0; 1000]), &budget).unwrap();
                }
            }
            assert!(
                slot.has_attempt(),
                "geometry pressure abandoned {kind}/{flight} guard"
            );
            assert_eq!(
                slot.wake_at(),
                original_wake,
                "geometry changed bootstrap quiet timer"
            );
            assert!(!pacing.eligible(4096, 0, 200), "old history was reset");
            assert!(
                !receiver.is_retired(),
                "mandatory media failed before geometry shedding"
            );
            assert!(receiver.tick(626).is_ok());
            assert!(
                matches!(
                    receiver.tick(627),
                    Err(crate::fxlink::Error::Expired { record_id: 0 })
                ),
                "original 627 ms gap deadline renewed"
            );
            assert_eq!(
                snapshot.shape(1),
                None,
                "held snapshot retained optional payload"
            );
            assert_eq!(ingress.diagnostics()["pressure_disable_events"], 1);
            drop((
                held, snapshot, ingress, receiver, pacing, slot, connection, budget,
            ));
            assert_eq!(root.used(), 0, "{kind}/{flight} leaked an owner");
        }
    }
}
#[test]
fn registration_racing_connection_disable_never_leaves_a_live_payload() {
    for _ in 0..64 {
        let budget = Budget::new(128 * 1024);
        let connection = ConnectionIngress::new(&budget).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let mut admitted = None;
        std::thread::scope(|scope| {
            let peer = connection.clone();
            let peer_barrier = barrier.clone();
            scope.spawn(move || {
                peer_barrier.wait();
                peer.disable();
            });
            barrier.wait();
            admitted = connection.admit(&budget, &Settings::default());
        });
        if let Some(ingress) = admitted {
            assert!(ingress.snapshot(0, 0).is_none());
        }
        assert!(connection.admit(&budget, &Settings::default()).is_none());
        drop(connection);
        assert_eq!(budget.used(), 0);
    }
}
