// Catch eager media ownership, weak age renewal, stale publication and fake reclamation.
use super::{
    receive::{Ingress, OfferResult},
    wire::{self, Frame, Summary},
};
use crate::fxlink::{Budget, Receiver, Settings};
use futures_util::FutureExt;

fn frame(sequence: u64, start: u64, count: usize, length: u32) -> Frame {
    Frame {
        subscription: 7,
        generation: 1,
        sequence,
        count,
        entries: std::array::from_fn(|i| {
            if i < count {
                Summary {
                    record_id: start + i as u64,
                    record_len: length,
                }
            } else {
                Summary::default()
            }
        }),
    }
}
fn offered(stored: u8, ignored: u8, conflicts: u8) -> OfferResult {
    OfferResult::Admitted {
        stored,
        ignored,
        conflicts,
    }
}

#[test]
fn sixty_four_shapes_cause_no_media_allocations_or_deadlines() {
    let budget = Budget::new(128 * 1024);
    let settings = Settings::default();
    let ingress = Ingress::new(&budget, &settings).unwrap();
    let used = budget.used();
    assert!(used > 0 && used <= 8192);
    let mut receiver = Receiver::new(settings.clone(), 7, 1, budget.clone()).unwrap();
    for batch in 0..8 {
        assert_eq!(
            ingress.offer(&frame(batch + 1, batch * 8, 8, 20_000), 100, 0),
            offered(8, 0, 0)
        );
    }
    let snapshot = ingress.snapshot(100, 0).unwrap();
    for id in 0..64 {
        assert_eq!(snapshot.shape(id), Some(20_000));
    }
    assert_eq!(snapshot.shape(64), None);
    assert!(receiver.repair_requests(1000, 100, 5).is_empty());
    assert!(receiver.tick(u64::MAX).is_ok());
    assert_eq!(
        budget.used(),
        used,
        "hints allocated media/history/snapshot storage"
    );
    assert!(ingress.changed.notified().now_or_never().is_some());
    assert!(
        ingress.changed.notified().now_or_never().is_none(),
        "updates did not coalesce"
    );
    drop(snapshot);
    drop(ingress);
    assert_eq!(budget.used(), 0);
}

#[test]
fn numeric_window_and_publication_fence_reject_late_updates() {
    let budget = Budget::new(8192);
    let settings = Settings::default();
    let ingress = Ingress::new(&budget, &settings).unwrap();
    let mut f = frame(1, 0, 4, 1000);
    for (entry, id) in f.entries.iter_mut().zip([9, 10, 73, 74]) {
        entry.record_id = id;
    }
    assert_eq!(ingress.offer(&f, 100, 10), offered(2, 2, 0));
    let snapshot = ingress.snapshot(100, 10).unwrap();
    assert_eq!(snapshot.shape(10), Some(1000));
    assert_eq!(snapshot.shape(73), Some(1000));
    assert_eq!(snapshot.shape(9), None);
    assert_eq!(snapshot.shape(74), None);
    ingress.publish(11);
    assert_eq!(snapshot.shape(10), None);
    assert_eq!(
        ingress.offer(&frame(2, 10, 1, 1000), 101, 0),
        offered(0, 1, 0)
    );
    assert_eq!(ingress.snapshot(102, 0).unwrap().shape(10), None);
}

#[test]
fn first_observation_expires_to_inert_tombstone_without_refresh() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let ingress = Ingress::new(&Budget::new(8192), &settings).unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 400, 0),
        OfferResult::Replay
    );
    assert_eq!(
        ingress.offer(&frame(2, 0, 1, 1000), 599, 0),
        offered(0, 1, 0)
    );
    assert_eq!(ingress.snapshot(599, 0).unwrap().shape(0), Some(1000));
    assert_eq!(ingress.snapshot(600, 0).unwrap().shape(0), None);
    assert_eq!(
        ingress.offer(&frame(3, 0, 1, 1000), 601, 0),
        offered(0, 1, 0)
    );
    assert_eq!(ingress.snapshot(601, 0).unwrap().shape(0), None);
    ingress.publish(1);
    assert_eq!(
        ingress.offer(&frame(4, 1, 1, 1000), 602, 1),
        offered(1, 0, 0)
    );
    assert_eq!(ingress.snapshot(602, 1).unwrap().shape(1), Some(1000));
}

#[test]
fn observation_overflow_never_creates_an_immortal_shape() {
    let ingress = Ingress::new(&Budget::new(8192), &Settings::default()).unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), u64::MAX - 999, 0),
        offered(0, 1, 0)
    );
    assert_eq!(ingress.snapshot(u64::MAX, 0).unwrap().shape(0), None);
}

#[test]
fn malformed_tail_commits_neither_entries_nor_replay() {
    let settings = Settings::default();
    let ingress = Ingress::new(&Budget::new(8192), &settings).unwrap();
    let good = frame(1, 0, 8, 1000);
    let mut bad = wire::encode(&good, &settings).unwrap().to_vec();
    bad[124..128].copy_from_slice(&11u32.to_be_bytes());
    assert!(ingress.admit_work(100));
    assert!(wire::decode(&bad, &settings).is_err());
    let snapshot = ingress.snapshot(100, 0).unwrap();
    for id in 0..8 {
        assert_eq!(snapshot.shape(id), None);
    }
    assert!(ingress.admit_work(100));
    let decoded = wire::decode(&wire::encode(&good, &settings).unwrap(), &settings).unwrap();
    assert_eq!(ingress.offer(&decoded, 100, 0), offered(8, 0, 0));
    assert!(!ingress.admit_work(100));
}

#[test]
fn contended_update_drops_without_advancing_replay() {
    let ingress = Ingress::new(&Budget::new(8192), &Settings::default()).unwrap();
    let guard = ingress.inner.lock().unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        OfferResult::Contended
    );
    assert!(ingress.snapshot(100, 0).is_none());
    drop(guard);
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
}

#[test]
fn weak_contradictions_keep_first_shape_and_authority_discards_it() {
    let ingress = Ingress::new(&Budget::new(8192), &Settings::default()).unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    assert_eq!(
        ingress.offer(&frame(2, 0, 1, 1100), 101, 0),
        offered(0, 0, 1)
    );
    let snapshot = ingress.snapshot(101, 0).unwrap();
    assert_eq!(snapshot.shape(0), Some(1000));
    assert!(!ingress.authoritative(0, 1000));
    assert_eq!(snapshot.shape(0), None);
    assert!(!ingress.authoritative(0, 900));
    assert_eq!(
        ingress.offer(&frame(3, 0, 1, 1000), 102, 0),
        offered(0, 0, 1)
    );
    assert_eq!(ingress.snapshot(102, 0).unwrap().shape(0), None);
}

#[test]
fn snapshots_are_fixed_and_do_not_retain_optional_payload_on_disable() {
    let ingress = Ingress::new(&Budget::new(8192), &Settings::default()).unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    let first = ingress.snapshot(100, 0).unwrap();
    assert_eq!(
        ingress.offer(&frame(2, 1, 1, 1000), 101, 0),
        offered(1, 0, 0)
    );
    assert_eq!(first.shape(1), None, "router update mutated fixed snapshot");
    let second = ingress.snapshot(101, 0).unwrap();
    assert_eq!(second.shape(1), Some(1000));
    assert_eq!(first.shape(0), None, "superseded handle must fail closed");
    ingress.disable();
    assert_eq!(first.shape(0), None);
    assert_eq!(second.shape(1), None);
    assert_eq!(
        ingress.offer(&frame(3, 2, 1, 1000), 102, 0),
        OfferResult::Disabled
    );
    assert!(!ingress.admit_work(1000));
    assert!(ingress.snapshot(102, 0).is_none());
}

#[test]
fn real_hierarchical_pressure_fits_only_after_optional_payload_destruction() {
    let root = Budget::new(128 * 1024);
    let child = root.child(64 * 1024);
    let ingress = Ingress::new(&child, &Settings::default()).unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    let snapshot = ingress.snapshot(100, 0).unwrap();
    let charged = child.used();
    assert!((3072..=8192).contains(&charged));
    let pressure = child.reserve(child.limit() - charged - 1024).unwrap();
    assert!(child.reserve(4096).is_err());
    ingress.disable();
    assert_eq!(snapshot.shape(0), None);
    let remaining = child.used();
    assert!(remaining < child.limit() - 4096);
    let mandatory = child.reserve(4096).unwrap();
    assert!(
        child.reserve(64 * 1024).is_err(),
        "true exhaustion must stay an error"
    );
    assert_eq!(root.used(), child.used());
    drop(mandatory);
    drop(pressure);
    drop(ingress);
    drop(snapshot);
    assert_eq!(root.used(), 0);
    assert!(Ingress::new(&Budget::new(1), &Settings::default()).is_none());
}

#[test]
fn route_publication_also_fences_geometry() {
    let ingress = Ingress::new(&Budget::new(8192), &Settings::default()).unwrap();
    let state = crate::fxlink::transport::State::new(tokio_util::sync::CancellationToken::new());
    *state.geometry.lock().unwrap() = Some(ingress.clone());
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    let snapshot = ingress.snapshot(100, 0).unwrap();
    state.publish(1);
    assert_eq!(snapshot.shape(0), None);
    assert_eq!(
        ingress.offer(&frame(2, 0, 1, 1000), 101, 0),
        offered(0, 1, 0)
    );
}

#[test]
fn route_retirement_sheds_payload_and_stickily_rejects_hints() {
    let budget = Budget::new(8192);
    let ingress = Ingress::new(&budget, &Settings::default()).unwrap();
    let state = crate::fxlink::transport::State::new(tokio_util::sync::CancellationToken::new());
    *state.geometry.lock().unwrap() = Some(ingress.clone());
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    let snapshot = ingress.snapshot(100, 0).unwrap();
    let used = budget.used();
    state.retire();
    assert_eq!(snapshot.shape(0), None);
    assert!(budget.used() < used);
    assert_eq!(
        ingress.offer(&frame(2, 1, 1, 1000), 101, 0),
        OfferResult::Disabled
    );
    assert!(state.geometry.lock().unwrap().is_none());
    drop(ingress);
    drop(snapshot);
    assert_eq!(budget.used(), 0);
}

#[test]
fn concurrent_publication_never_resurrects_completed_geometry() {
    for _ in 0..64 {
        let ingress = Ingress::new(&Budget::new(8192), &Settings::default()).unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                ingress.offer(&frame(1, 0, 1, 1000), 100, 0);
            });
            barrier.wait();
            ingress.publish(1);
        });
        assert_eq!(ingress.snapshot(101, 0).unwrap().shape(0), None);
        assert_eq!(
            ingress.offer(&frame(2, 0, 1, 1000), 102, 0),
            offered(0, 1, 0)
        );
    }
}

#[test]
fn expired_offer_also_invalidates_an_outstanding_fixed_snapshot() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let ingress = Ingress::new(&Budget::new(8192), &settings).unwrap();
    assert_eq!(
        ingress.offer(&frame(1, 0, 1, 1000), 100, 0),
        offered(1, 0, 0)
    );
    let snapshot = ingress.snapshot(100, 0).unwrap();
    assert_eq!(snapshot.shape(0), Some(1000));
    assert_eq!(
        ingress.offer(&frame(2, 0, 1, 1000), 600, 0),
        offered(0, 1, 0)
    );
    assert_eq!(snapshot.shape(0), None);
}

#[test]
fn connection_pressure_sheds_sibling_payloads_and_preserves_fixed_snapshot_handles() {
    use super::receive::ConnectionIngress;
    let root = Budget::new(16 * 1024);
    let a = root.child(16 * 1024);
    let b = root.child(16 * 1024);
    let connection = ConnectionIngress::new(&root).unwrap();
    let settings = Settings::default();
    let first = connection.admit(&a, &settings).unwrap();
    let second = connection.admit(&b, &settings).unwrap();
    first.offer(&frame(1, 0, 1, 1000), 0, 0);
    second.offer(&frame(1, 0, 1, 1000), 0, 0);
    let first_snapshot = first.snapshot(0, 0).unwrap();
    let second_snapshot = second.snapshot(0, 0).unwrap();
    let pressure = root.reserve(root.limit() - root.used() - 1024).unwrap();
    let scoped = connection.scoped(&a);
    let mandatory = scoped
        .reserve(5000)
        .expect("sibling optional tables blocked mandatory media");
    assert_eq!(first_snapshot.shape(0), None);
    assert_eq!(second_snapshot.shape(0), None);
    assert!(
        connection.admit(&a, &settings).is_none(),
        "pressure disable was not sticky"
    );
    drop(mandatory);
    drop(pressure);
    drop(first);
    drop(second);
    drop(first_snapshot);
    drop(second_snapshot);
    drop(connection);
    assert_eq!(root.used(), 0);
}
#[test]
fn connection_work_gate_and_sixteen_weak_registrations_are_bounded() {
    use super::receive::ConnectionIngress;
    let budget = Budget::new(128 * 1024);
    let connection = ConnectionIngress::new(&budget).unwrap();
    for _ in 0..4 {
        assert!(connection.admit_work(0));
    }
    assert!(!connection.admit_work(0));
    assert!(!connection.admit_work(9));
    assert!(connection.admit_work(10));
    let mut routes = (0..16)
        .map(|_| connection.admit(&budget, &Settings::default()).unwrap())
        .collect::<Vec<_>>();
    assert!(connection.admit(&budget, &Settings::default()).is_none());
    routes.pop();
    assert!(connection.admit(&budget, &Settings::default()).is_some());
    drop(routes);
    drop(connection);
    assert_eq!(budget.used(), 0);
}

#[test]
fn strong_before_weak_survives_late_matching_and_conflicting_hints() {
    let budget = Budget::new(8192);
    let ingress = Ingress::new(&budget, &Settings::default()).unwrap();
    let charged = budget.used();
    assert!(!ingress.authoritative(1, 9781));
    assert_eq!(
        ingress.offer(&frame(1, 1, 1, 9781), 100, 0),
        offered(0, 1, 0)
    );
    assert_eq!(
        ingress.offer(&frame(2, 1, 1, 4835), 101, 0),
        offered(0, 0, 1)
    );
    assert_eq!(ingress.snapshot(101, 0).unwrap().shape(1), None);
    assert_eq!(
        ingress.offer(&frame(3, 1, 1, 4835), 10_000, 0),
        offered(0, 0, 1)
    );
    assert_eq!(ingress.snapshot(10_000, 0).unwrap().shape(1), None);
    assert_eq!(
        budget.used(),
        charged,
        "strong markers must use existing bounded storage"
    );
}

#[test]
fn out_of_window_strong_headers_cannot_replace_live_modulo_aliases() {
    let settings = Settings {
        max_records: 4,
        ..Settings::default()
    };
    let ingress = Ingress::new(&Budget::new(8192), &settings).unwrap();
    ingress.publish(10);
    assert!(!ingress.authoritative(12, 9781));
    assert!(!ingress.authoritative(16, 3000)); // future alias of12
    assert!(!ingress.authoritative(8, 2000)); // stale alias of12
    assert_eq!(
        ingress.offer(&frame(1, 12, 1, 4835), 100, 10),
        offered(0, 0, 1)
    );
    assert_eq!(ingress.snapshot(100, 10).unwrap().shape(12), None);
    assert_eq!(
        ingress.offer(&frame(2, 10, 1, 1000), 101, 10),
        offered(1, 0, 0)
    );
    let held = ingress.snapshot(101, 10).unwrap();
    assert!(!ingress.authoritative(14, 3000)); // future alias of10
    assert_eq!(held.shape(10), Some(1000));
    ingress.publish(13);
    assert_eq!(
        ingress.offer(&frame(3, 16, 1, 1000), 102, 13),
        offered(1, 0, 0)
    );
    assert_eq!(ingress.snapshot(102, 13).unwrap().shape(16), Some(1000));
}
