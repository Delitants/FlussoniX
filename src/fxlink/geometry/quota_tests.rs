// Catch overgranting, partial debits, route-churn credit and catch-up bursts.
use super::quota::{self, Bucket, Spacing, WorkGate};

#[test]
fn fractional_original_credit_is_exact_and_failed_attempts_are_not_refunded() {
    let (mut route, mut connection) = (Bucket::route(), Bucket::connection());
    assert!(quota::debit(&mut route, &mut connection, 128));
    assert!(!quota::debit(&mut route, &mut connection, 44));
    route.original_admitted(11007);
    connection.original_admitted(11007);
    assert!(!quota::debit(&mut route, &mut connection, 44));
    route.original_admitted(1);
    connection.original_admitted(1);
    assert!(quota::debit(&mut route, &mut connection, 44));
    // No enqueue success or refund call is needed; a conservative attempt costs.
    assert!(!quota::debit(&mut route, &mut connection, 44));
    route.original_admitted(11008 - 64);
    connection.original_admitted(11008 - 64);
    for _ in 0..63 {
        route.original_admitted(1);
        connection.original_admitted(1);
        assert!(!quota::debit(&mut route, &mut connection, 44));
    }
    route.original_admitted(1);
    connection.original_admitted(1);
    assert!(quota::debit(&mut route, &mut connection, 44));
}

#[test]
fn connection_grant_survives_route_churn_and_dual_debit_is_atomic() {
    let (mut route, mut connection) = (Bucket::route(), Bucket::connection());
    assert!(quota::debit(&mut route, &mut connection, 128));
    let mut fresh = Bucket::route();
    for _ in 0..1000 {
        assert!(!quota::debit(&mut fresh, &mut connection, 128));
    }
    connection.original_admitted(16384);
    // A failed connection debit did not consume the new route's startup credit.
    assert!(quota::debit(&mut fresh, &mut connection, 128));
    connection.original_admitted(16384);
    assert!(!quota::debit(&mut fresh, &mut connection, 128));
    // A failed route debit did not consume connection credit either.
    assert!(quota::debit(&mut Bucket::route(), &mut connection, 128));
}

#[test]
fn bucket_caps_reject_overflow_and_discard_excess_credit() {
    let (mut route, mut connection) = (Bucket::route(), Bucket::connection());
    route.original_admitted(usize::MAX);
    connection.original_admitted(usize::MAX);
    assert!(!quota::debit(&mut route, &mut connection, usize::MAX));
    assert!(!quota::debit(&mut route, &mut connection, 0));
    assert!(quota::debit(&mut route, &mut connection, 128));
    assert!(!quota::debit(&mut route, &mut connection, 44));
    // Saturation retained exactly four maximum frames on the connection.
    for _ in 0..3 {
        assert!(quota::debit(&mut Bucket::route(), &mut connection, 128));
    }
    assert!(!quota::debit(&mut Bucket::route(), &mut connection, 44));
}

#[test]
fn every_prefix_under_churn_respects_actual_original_admission() {
    let mut connection = Bucket::connection();
    let mut routes: [Bucket; 16] = std::array::from_fn(|_| Bucket::route());
    let (mut originals, mut cost) = (0u128, 0u128);
    for event in 0..20_000usize {
        let index = event % 16;
        if event % 7 == 0 {
            routes[index] = Bucket::route();
        }
        let bytes = (event * 53) % 2049;
        routes[index].original_admitted(bytes);
        connection.original_admitted(bytes);
        originals += bytes as u128;
        let length = if event % 3 == 0 { 44 } else { 128 };
        if quota::debit(&mut routes[index], &mut connection, length) {
            cost += (length + 128) as u128;
        }
        assert!(cost <= 256 + originals / 64, "prefix {event}");
        // Even any number of repair/control/hint attempts creates no credit.
        if event % 11 == 0 {
            for _ in 0..20 {
                if quota::debit(&mut routes[index], &mut connection, length) {
                    cost += (length + 128) as u128;
                }
                assert!(cost <= 256 + originals / 64);
            }
        }
    }
    assert!(cost > 100_000);
}

#[test]
fn spacing_uses_attempt_time_without_catch_up_or_clock_wrap() {
    for (mut spacing, period) in [(Spacing::route(), 20), (Spacing::connection(), 10)] {
        assert_eq!(spacing.next_send_at(), Some(0));
        assert!(spacing.commit(0));
        assert_eq!(spacing.next_send_at(), Some(period));
        assert!(!spacing.commit(period - 1));
        assert!(spacing.commit(period));
        assert!(!spacing.commit(0));
        assert!(spacing.commit(1_000_000));
        assert_eq!(spacing.next_send_at(), Some(1_000_000 + period));
        assert!(!spacing.commit(1_000_000));
        assert!(spacing.commit(u64::MAX));
        assert_eq!(spacing.next_send_at(), None);
        assert!(!spacing.commit(u64::MAX));
        assert!(!spacing.commit(0));
    }
}

#[test]
fn receiver_work_gates_bound_invalid_traffic_too() {
    for (mut gate, burst, period) in [(WorkGate::route(), 2, 20), (WorkGate::connection(), 4, 10)] {
        for _ in 0..burst {
            assert!(gate.admit(100));
        }
        assert!(!gate.admit(100));
        assert!(!gate.admit(99));
        assert!(!gate.admit(100 + period - 1));
        assert!(gate.admit(100 + period));
        assert!(!gate.admit(100 + period));
        for _ in 0..burst {
            assert!(gate.admit(1_000_000));
        }
        assert!(!gate.admit(1_000_000));
        assert!(!gate.admit(1_000_000 + period - 1));
        assert!(gate.admit(1_000_000 + period));
        for _ in 0..burst {
            assert!(gate.admit(u64::MAX));
        }
        assert!(!gate.admit(u64::MAX));
        assert!(!gate.admit(0));
    }
}
