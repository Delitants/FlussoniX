use flussonix::iptv::lease::{Error, LeaseRequest, Ledger, SessionKey, SubscriberId};
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Barrier},
    time::{Duration, Instant, SystemTime},
};
use uuid::Uuid;

fn request(name: &str, local: Uuid, operation: Uuid, limit: u32, revision: u64) -> LeaseRequest {
    LeaseRequest {
        subscriber: SubscriberId::from_name(name),
        session: SessionKey {
            boot: Uuid::nil(),
            local,
        },
        operation,
        units: 1,
        limit: Some(limit),
        revision,
        subscriber_expires_at: None,
    }
}

#[test]
fn simultaneous_nodes_cannot_exceed_one_subscriber_slot() {
    let start = Instant::now();
    let ledger = Arc::new(Ledger::new(start, 10_000, 1));
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let barrier = Arc::new(Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let ledger = ledger.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
                barrier.wait();
                ledger.acquire(ready, wall, &req)
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let outcomes = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(Error::Limit)))
            .count(),
        1
    );
    assert_eq!(ledger.active_leases(), 1);
}

#[test]
fn idempotent_acquire_commit_renew_and_release_preserve_one_slot() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let first = ledger.acquire(ready, wall, &req).unwrap();
    let retry = ledger.acquire(ready, wall, &req).unwrap();
    assert_eq!(retry.id, first.id);
    assert_eq!(ledger.active_leases(), 1);
    let committed = ledger.commit(ready, wall, &req, first.id).unwrap();
    assert!(committed.deadline > first.deadline);
    let renewed = ledger
        .renew(ready + Duration::from_secs(5), wall, &req, first.id)
        .unwrap();
    assert!(renewed.deadline > committed.deadline);
    assert!(ledger.release(&req.subscriber, &req.session, first.id));
    assert!(!ledger.release(&req.subscriber, &req.session, first.id));
    assert_eq!(ledger.active_leases(), 0);
}

#[test]
fn expired_or_old_revision_lease_stays_counted_until_its_deadline() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let old = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    let lease = ledger.acquire(ready, wall, &old).unwrap();
    ledger.commit(ready, wall, &old, lease.id).unwrap();
    ledger.observe_revision(2);
    let current = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 2);
    assert!(matches!(
        ledger.acquire(ready, wall, &current),
        Err(Error::Limit)
    ));
    assert!(matches!(
        ledger.renew(ready, wall, &current, lease.id),
        Err(Error::Missing)
    ));
    assert!(matches!(
        ledger.renew(ready, wall, &old, lease.id),
        Err(Error::Stale)
    ));
    assert!(
        ledger
            .acquire(ready + Duration::from_secs(11), wall, &current)
            .is_ok()
    );
    assert_eq!(ledger.active_leases(), 1);
}

#[test]
fn quarantine_expiry_and_global_capacity_fail_closed() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let wall = SystemTime::now();
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(matches!(
        ledger.acquire(start, wall, &req),
        Err(Error::Quarantine)
    ));
    let ready = start + Duration::from_secs(11);
    let expired = LeaseRequest {
        subscriber_expires_at: Some(wall),
        ..req.clone()
    };
    assert!(matches!(
        ledger.acquire(ready, wall, &expired),
        Err(Error::Expired)
    ));
    let first = ledger.acquire(ready, wall, &req).unwrap();
    let other = request("bob", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(matches!(
        ledger.acquire(ready, wall, &other),
        Err(Error::Full)
    ));
    assert!(ledger.release(&req.subscriber, &req.session, first.id));
    assert!(ledger.acquire(ready, wall, &other).is_ok());
}

#[test]
fn readiness_reflects_capacity_and_reclaims_expired_leases() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    assert!(!ledger.ready_for_new_leases(start));
    assert!(ledger.ready_for_new_leases(ready));
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let grant = ledger.acquire(ready, wall, &req).unwrap();
    assert!(!ledger.ready_for_new_leases(ready));
    assert!(ledger.ready_for_new_leases(ready + Duration::from_secs(3)));
    assert_eq!(ledger.active_leases(), 0);
    assert!(!ledger.release(&req.subscriber, &req.session, grant.id));
}

#[test]
fn restart_quarantine_rejects_renewal_before_any_lease_lookup() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    assert!(matches!(
        ledger.renew(
            start + Duration::from_secs(10),
            SystemTime::now(),
            &req,
            Uuid::nil()
        ),
        Err(Error::Quarantine)
    ));
    assert!(matches!(
        ledger.renew(
            start + Duration::from_secs(11),
            SystemTime::now(),
            &req,
            Uuid::nil()
        ),
        Err(Error::Missing)
    ));
}

#[test]
fn malformed_unit_reservations_create_no_ledger_records() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let valid = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    for (units, limit) in [(0, Some(1)), (1025, Some(1025)), (1, Some(0))] {
        let malformed = LeaseRequest {
            units,
            limit,
            ..valid.clone()
        };
        assert!(matches!(
            ledger.acquire(ready, wall, &malformed),
            Err(Error::Invalid)
        ));
    }
    assert_eq!(ledger.active_leases(), 0);
    assert_eq!(ledger.subscriber_records(), 0);
}

#[test]
fn failed_extra_slot_does_not_discard_an_existing_committed_lease() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let base = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    let first = ledger.acquire(ready, wall, &base).unwrap();
    ledger.commit(ready, wall, &base, first.id).unwrap();
    let increase = LeaseRequest {
        operation: Uuid::new_v4(),
        units: 2,
        ..base.clone()
    };
    let pending = ledger.acquire(ready, wall, &increase).unwrap();
    assert_ne!(pending.id, first.id);
    assert!(ledger.rollback(&increase, pending.id));
    assert_eq!(ledger.active_leases(), 1);
    assert!(
        ledger
            .renew(ready + Duration::from_secs(5), wall, &base, first.id)
            .is_ok()
    );
    let second = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(ledger.acquire(ready, wall, &second).is_ok());
}

#[test]
fn committed_lease_never_outlives_subscriber_expiration() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let req = LeaseRequest {
        subscriber_expires_at: Some(wall + Duration::from_secs(3)),
        ..request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1)
    };
    let pending = ledger.acquire(ready, wall, &req).unwrap();
    let committed = ledger.commit(ready, wall, &req, pending.id).unwrap();
    assert_eq!(committed.deadline, ready + Duration::from_secs(3));
    assert!(matches!(
        ledger.renew(
            ready + Duration::from_secs(3),
            wall + Duration::from_secs(3),
            &req,
            pending.id
        ),
        Err(Error::Expired)
    ));
}

#[test]
fn timed_out_increase_restores_the_original_lease_and_frees_reserved_units() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let base = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    let first = ledger.acquire(ready, wall, &base).unwrap();
    ledger.commit(ready, wall, &base, first.id).unwrap();
    let increase = LeaseRequest {
        operation: Uuid::new_v4(),
        units: 2,
        ..base.clone()
    };
    ledger.acquire(ready, wall, &increase).unwrap();
    let second = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(matches!(
        ledger.acquire(ready, wall, &second),
        Err(Error::Limit)
    ));
    assert!(
        ledger
            .acquire(ready + Duration::from_secs(3), wall, &second)
            .is_ok()
    );
    assert!(
        ledger
            .renew(ready + Duration::from_secs(3), wall, &base, first.id)
            .is_ok()
    );
    assert_eq!(ledger.active_leases(), 2);
}

#[test]
fn distinct_subscribers_can_each_hold_capacity_up_to_global_bound() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 2, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    for name in ["alice", "bob"] {
        let req = request(name, Uuid::new_v4(), Uuid::new_v4(), 1, 1);
        assert!(ledger.acquire(ready, wall, &req).is_ok());
    }
    let third = request("carol", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    assert!(matches!(
        ledger.acquire(ready, wall, &third),
        Err(Error::Full)
    ));
}

#[test]
fn expired_quiet_subscribers_release_global_capacity_for_new_subscribers() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let old = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let old_lease = ledger.acquire(ready, wall, &old).unwrap();
    ledger.commit(ready, wall, &old, old_lease.id).unwrap();
    let fresh = request("bob", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    assert!(
        ledger
            .acquire(ready + Duration::from_secs(11), wall, &fresh)
            .is_ok()
    );
    assert_eq!(ledger.active_leases(), 1);
}

#[test]
fn denied_and_released_subscriber_churn_does_not_retain_empty_records() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let owner = request("owner", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let grant = ledger.acquire(ready, wall, &owner).unwrap();
    for index in 0..1_000 {
        let denied = request(
            &format!("subscriber-{index}"),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            1,
        );
        assert!(matches!(
            ledger.acquire(ready, wall, &denied),
            Err(Error::Full)
        ));
    }
    assert_eq!(ledger.subscriber_records(), 1);
    assert!(ledger.release(&owner.subscriber, &owner.session, grant.id));
    assert_eq!(ledger.subscriber_records(), 0);
}

#[test]
fn delayed_old_release_cannot_remove_a_replacement_lease() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let base = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    let old = ledger.acquire(ready, wall, &base).unwrap();
    ledger.commit(ready, wall, &base, old.id).unwrap();
    let increase = LeaseRequest {
        operation: Uuid::new_v4(),
        units: 2,
        ..base.clone()
    };
    let replacement = ledger.acquire(ready, wall, &increase).unwrap();
    assert_ne!(old.id, replacement.id);
    assert!(ledger.renew(ready, wall, &base, old.id).is_ok());
    ledger
        .commit(ready, wall, &increase, replacement.id)
        .unwrap();
    assert!(!ledger.release(&base.subscriber, &base.session, old.id));
    let extra = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(matches!(
        ledger.acquire(ready, wall, &extra),
        Err(Error::Limit)
    ));
    assert_eq!(ledger.active_leases(), 1);
}

#[test]
fn released_subscriber_still_rejects_an_older_policy_revision() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 2);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let current = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 2);
    let grant = ledger.acquire(ready, wall, &current).unwrap();
    assert!(ledger.release(&current.subscriber, &current.session, grant.id));
    assert_eq!(ledger.subscriber_records(), 0);
    let stale = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(matches!(
        ledger.acquire(ready, wall, &stale),
        Err(Error::Stale)
    ));
}

#[test]
fn store_revision_change_rejects_old_lease_renewal_without_new_acquire() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let old = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let grant = ledger.acquire(ready, wall, &old).unwrap();
    ledger.commit(ready, wall, &old, grant.id).unwrap();
    ledger.observe_revision(2);
    assert!(matches!(
        ledger.renew(ready, wall, &old, grant.id),
        Err(Error::Stale)
    ));
    assert!(matches!(
        ledger.acquire(ready, wall, &old),
        Err(Error::Stale)
    ));
}

#[test]
fn denied_future_revision_cannot_poison_valid_subscriber_admission() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 10, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let future = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, u64::MAX);
    assert!(matches!(
        ledger.acquire(ready, wall, &future),
        Err(Error::Stale)
    ));
    let valid = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    assert!(ledger.acquire(ready, wall, &valid).is_ok());
}

#[test]
fn store_publication_fences_late_old_revision_across_threads() {
    let start = Instant::now();
    let ledger = Arc::new(Ledger::new(start, 10, 1));
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let old = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let first = ledger.acquire(ready, wall, &old).unwrap();
    ledger.commit(ready, wall, &old, first.id).unwrap();
    let published = Arc::new(Barrier::new(2));
    let writer = {
        let ledger = ledger.clone();
        let published = published.clone();
        std::thread::spawn(move || {
            ledger.observe_revision(2);
            published.wait();
        })
    };
    published.wait();
    let late = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    assert!(matches!(
        ledger.acquire(ready, wall, &late),
        Err(Error::Stale)
    ));
    assert!(matches!(
        ledger.renew(ready, wall, &old, first.id),
        Err(Error::Stale)
    ));
    writer.join().unwrap();
}

#[test]
fn capacity_reclaims_a_lease_that_expires_just_after_a_pressure_sweep() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let pending = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    ledger.acquire(ready, wall, &pending).unwrap();
    let fresh = request("bob", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    assert!(matches!(
        ledger.acquire(ready + Duration::from_millis(1_999), wall, &fresh),
        Err(Error::Full)
    ));
    assert!(
        ledger
            .acquire(ready + Duration::from_millis(2_001), wall, &fresh)
            .is_ok()
    );
}

#[test]
fn full_capacity_without_expiry_does_not_scan_on_every_denial() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let owner = request("owner", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    ledger.acquire(ready, wall, &owner).unwrap();
    for index in 0..1_000 {
        let denied = request(
            &format!("other-{index}"),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            1,
        );
        assert!(matches!(
            ledger.acquire(ready, wall, &denied),
            Err(Error::Full)
        ));
    }
    assert_eq!(ledger.sweep_count(), 0);
    let stale = request("stale", Uuid::new_v4(), Uuid::new_v4(), 1, 2);
    assert!(matches!(
        ledger.acquire(ready + Duration::from_secs(3), wall, &stale),
        Err(Error::Stale)
    ));
    assert_eq!(ledger.sweep_count(), 0);
    let fresh = request("fresh", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    assert!(
        ledger
            .acquire(ready + Duration::from_secs(3), wall, &fresh)
            .is_ok()
    );
    assert_eq!(ledger.sweep_count(), 1);
}

#[test]
fn expired_previous_grant_does_not_keep_pressure_scan_due() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 1, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let base = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 1);
    let old = ledger.acquire(ready, wall, &base).unwrap();
    ledger.commit(ready, wall, &base, old.id).unwrap();
    let pending = LeaseRequest {
        operation: Uuid::new_v4(),
        units: 2,
        ..base.clone()
    };
    ledger
        .acquire(ready + Duration::from_secs(9), wall, &pending)
        .unwrap();
    let prior_sweeps = ledger.sweep_count();
    let after_old_expiry = ready + Duration::from_millis(10_001);
    for index in 0..100 {
        let denied = request(
            &format!("other-{index}"),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            1,
        );
        assert!(matches!(
            ledger.acquire(after_old_expiry, wall, &denied),
            Err(Error::Full)
        ));
    }
    assert_eq!(ledger.sweep_count(), prior_sweeps + 1);
}

#[test]
fn staggered_expirations_scan_only_the_due_shards() {
    let start = Instant::now();
    let ledger = Ledger::new(start, 64, 1);
    let ready = start + Duration::from_secs(11);
    let wall = SystemTime::now();
    let mut originals = vec![None; 64];
    for candidate in 0..10_000 {
        let name = format!("subscriber-{candidate}");
        let shard = usize::from(Sha256::digest(name.as_bytes())[0]) % 64;
        if originals[shard].is_none() {
            originals[shard] = Some(request(&name, Uuid::new_v4(), Uuid::new_v4(), 1, 1));
        }
        if originals.iter().all(Option::is_some) {
            break;
        }
    }
    assert!(originals.iter().all(Option::is_some));
    for (index, original) in originals.into_iter().enumerate() {
        let now = ready + Duration::from_millis(index as u64);
        let original = original.unwrap();
        let pending = ledger.acquire(now, wall, &original).unwrap();
        ledger.commit(now, wall, &original, pending.id).unwrap();
    }
    // Clear provisional two-second hints so this observes the cost of each
    // independently staggered committed deadline.
    ledger.sweep_expired(ready + Duration::from_secs(3));
    let prior_sweeps = ledger.sweep_count();
    let prior_shards = ledger.shards_scanned();
    for index in 0..64 {
        let now = ready + Duration::from_secs(10) + Duration::from_millis(index);
        let fresh = request(
            &format!("replacement-{index}"),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
            1,
        );
        assert!(ledger.acquire(now, wall, &fresh).is_ok());
    }
    assert_eq!(ledger.active_leases(), 64);
    assert_eq!(ledger.sweep_count() - prior_sweeps, 64);
    assert_eq!(ledger.shards_scanned() - prior_shards, 64);
}
