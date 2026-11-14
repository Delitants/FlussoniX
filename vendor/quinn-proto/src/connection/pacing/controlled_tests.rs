use super::*;
fn rate(value: u64) -> Option<NonZeroU64> {
    NonZeroU64::new(value)
}
fn drain(pacer: &mut Pacer, rtt: Duration, mtu: u16, window: u64, speed: u64, now: Instant) {
    for _ in 0..256 {
        if pacer
            .delay(rtt, mtu.into(), mtu, window, rate(speed), now)
            .is_some()
        {
            return;
        }
        pacer.on_transmit(mtu);
    }
    assert!(
        pacer
            .delay(rtt, mtu.into(), mtu, window, rate(speed), now)
            .is_some()
    );
}
// Break caught: a controller's rate is ignored, multiplied by1.25 again,
// disabled by zero RTT/large windows, or confused with bits per second.
#[test]
fn explicit_rate_controls_refill_independently_of_window_and_rtt() {
    let now = Instant::now();
    for (speed, rtt, window, wait_ms) in [
        (12_000, Duration::from_millis(100), 1_000_000, 1_000),
        (120_000, Duration::ZERO, 1_000_000, 100),
        (20_000, Duration::from_millis(100), 1u64 << 40, 600),
    ] {
        let mut p = Pacer::new(rtt, window, 1200, rate(speed), now);
        drain(&mut p, rtt, 1200, window, speed, now);
        assert_eq!(
            p.delay(rtt, 1200, 1200, window, rate(speed), now),
            Some(now + Duration::from_millis(wait_ms))
        );
    }
}
// Break caught: a rate increase applies retroactively to time elapsed at the
// old rate, minting an immediate burst instead of preserving actual credit.
#[test]
fn increase_settles_elapsed_time_at_the_old_rate() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 1_000_000, 1200, rate(12_000), now);
    drain(&mut p, rtt, 1200, 1_000_000, 12_000, now);
    assert_eq!(
        p.delay(
            rtt,
            2400,
            1200,
            1_000_000,
            rate(120_000),
            now + Duration::from_millis(100)
        ),
        Some(now + Duration::from_millis(190))
    );
    assert_eq!(
        p.delay(
            rtt,
            12_000,
            1200,
            1_000_000,
            rate(120_000),
            now + Duration::from_millis(190)
        ),
        None
    );
}
// Break caught: a decrease erases credit already accrued at the old rate or
// waits at the old, faster rate after the update.
#[test]
fn decrease_preserves_earned_credit_then_uses_the_new_rate() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 1_000_000, 1200, rate(120_000), now);
    drain(&mut p, rtt, 1200, 1_000_000, 120_000, now);
    assert_eq!(
        p.delay(
            rtt,
            2400,
            1200,
            1_000_000,
            rate(12_000),
            now + Duration::from_millis(10)
        ),
        Some(now + Duration::from_millis(910))
    );
}
// Break caught: frequent polls truncate fractional-byte credit, starving a
// low-rate connection forever even after a complete byte has accrued.
#[test]
fn fractional_credit_survives_frequent_polls() {
    let now = Instant::now();
    let mut p = Pacer::new(Duration::ZERO, 1000, 1, rate(1), now);
    drain(&mut p, Duration::ZERO, 1, 1000, 1, now);
    for tick in 1..100 {
        assert!(
            p.delay(
                Duration::ZERO,
                1,
                1,
                1000,
                rate(1),
                now + Duration::from_millis(tick * 10)
            )
            .is_some()
        );
    }
    assert_eq!(
        p.delay(
            Duration::ZERO,
            1,
            1,
            1000,
            rate(1),
            now + Duration::from_secs(1)
        ),
        None
    );
}
// Break caught: growing the quantum or changing MTU fills the new capacity
// instead of retaining/clamping only the already earned bytes.
#[test]
fn quantum_and_mtu_changes_do_not_mint_tokens() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 1_000_000, 1200, rate(6_000_000), now);
    drain(&mut p, rtt, 1200, 1_000_000, 6_000_000, now);
    assert!(
        p.delay(rtt, 1200, 1200, 1_000_000, rate(60_000_000), now)
            .is_some()
    );
    assert!(
        p.delay(rtt, 1500, 1500, 1_000_000, rate(60_000_000), now)
            .is_some()
    );
}
// Break caught: an early timestamp rewinds the refill anchor and credits the
// same elapsed time twice on a later poll.
#[test]
fn early_timestamps_cannot_duplicate_credit() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 1_000_000, 1200, rate(12_000), now);
    drain(&mut p, rtt, 1200, 1_000_000, 12_000, now);
    assert_eq!(
        p.delay(
            rtt,
            7000,
            1200,
            1_000_000,
            rate(12_000),
            now + Duration::from_millis(500)
        ),
        Some(now + Duration::from_secs(1))
    );
    assert_eq!(
        p.delay(rtt, 7000, 1200, 1_000_000, rate(12_000), now),
        Some(now + Duration::from_secs(1))
    );
    assert_eq!(
        p.delay(
            rtt,
            12_000,
            1200,
            1_000_000,
            rate(12_000),
            now + Duration::from_secs(1)
        ),
        None
    );
}
// Break caught: floor rounding schedules a refill before the required bytes
// exist, or extreme rates/times overflow intermediate token arithmetic.
#[test]
fn rate_arithmetic_rounds_up_and_saturates_without_disabling_pacing() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 1u64 << 40, 1200, rate(1_000_000_001), now);
    drain(&mut p, rtt, 1200, 1u64 << 40, 1_000_000_001, now);
    assert_eq!(
        p.delay(rtt, 307_200, 1200, 1u64 << 40, rate(1_000_000_001), now),
        Some(now + Duration::from_nanos(307_200))
    );
    assert!(
        p.delay(
            rtt,
            307_200,
            1200,
            1u64 << 40,
            rate(1_000_000_001),
            now + Duration::from_nanos(307_199)
        )
        .is_some()
    );
    assert_eq!(
        p.delay(
            rtt,
            307_200,
            1200,
            1u64 << 40,
            rate(1_000_000_001),
            now + Duration::from_nanos(307_200)
        ),
        None
    );
    let mut huge = Pacer::new(rtt, u64::MAX, 1200, rate(u64::MAX), now);
    drain(&mut huge, rtt, 1200, u64::MAX, u64::MAX, now);
    assert!(
        huge.delay(rtt, 1200, 1200, u64::MAX, rate(u64::MAX), now)
            .is_some()
    );
    assert_eq!(
        huge.delay(
            rtt,
            307_200,
            1200,
            u64::MAX,
            rate(u64::MAX),
            now + Duration::from_secs(100_000_000_000)
        ),
        None
    );
}
// Break caught: an unrepresentable/extremely long deadline panics or returns
// unrestricted transmission. Long waits must recheck without granting credit.
#[test]
fn extreme_waits_recheck_within_one_second() {
    let now = Instant::now();
    let mut p = Pacer::new(Duration::ZERO, 1_000_000, 1200, rate(1), now);
    drain(&mut p, Duration::ZERO, 1200, 1_000_000, 1, now);
    assert_eq!(
        p.delay(Duration::ZERO, u64::MAX, 1200, 1_000_000, rate(1), now),
        Some(now + Duration::from_secs(1))
    );
    assert!(
        p.delay(
            Duration::ZERO,
            1200,
            1200,
            1_000_000,
            rate(1),
            now + Duration::from_secs(1)
        )
        .is_some()
    );
}
// Break caught: switching from the legacy lazy-refill pacer treats elapsed
// pre-opt-in time as new controller-rate credit.
#[test]
fn enabling_controlled_pacing_does_not_retroactively_mint_credit() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 12_000, 1200, None, now);
    drain(&mut p, rtt, 1200, 12_000, 0, now);
    assert_eq!(
        p.delay(
            rtt,
            1200,
            1200,
            12_000,
            rate(12_000),
            now + Duration::from_millis(100)
        ),
        Some(now + Duration::from_millis(1100))
    );
}
// Break caught: disabling controlled pacing loses already earned credit or
// accrues the same controlled interval again at the legacy, faster rate.
#[test]
fn returning_to_legacy_settles_controlled_credit_once() {
    let now = Instant::now();
    let rtt = Duration::from_millis(100);
    let mut p = Pacer::new(rtt, 1_000_000, 1200, rate(12_000), now);
    drain(&mut p, rtt, 1200, 1_000_000, 12_000, now);
    assert_eq!(
        p.delay(
            rtt,
            7000,
            1200,
            1_000_000,
            None,
            now + Duration::from_millis(500)
        ),
        Some(now + Duration::from_micros(501_120))
    );
}

// Break caught: the first tracked handshake ACK exposes a unit-gain rate, so
// even the real controller-fed pacer needs more than half an RTT to wake after
// exhausting its quantum. This is a component fixture, not an encoded flight.
#[test]
fn startup_bootstrap_refills_a_cold_media_pacer_within_half_an_rtt() {
    use crate::congestion::{Bbr, BbrConfig, Controller};
    use crate::connection::RttEstimator;
    use std::sync::Arc;

    let sent = Instant::now();
    let rtt = Duration::from_millis(200);
    let mut config = BbrConfig::default();
    config.initial_window(12_000).delivery_rate_sampling(true);
    let mut bbr = Bbr::new(Arc::new(config), 1420);
    bbr.on_application_limited(0);
    let snapshot = bbr.on_packet_sent(sent, 0).unwrap();
    let acked = sent + rtt;
    bbr.on_ack_with_snapshot(
        acked,
        sent,
        1200,
        true,
        &RttEstimator::new(rtt),
        Some(snapshot),
    );
    bbr.on_end_acks(acked, 0, true, Some(0));
    let speed = bbr.pacing_rate().unwrap();
    let mut pacer = Pacer::new(rtt, bbr.window(), 1420, Some(speed), acked);
    drain(&mut pacer, rtt, 1420, bbr.window(), speed.get(), acked);
    let wake = pacer
        .delay(rtt, 1420, 1420, bbr.window(), Some(speed), acked)
        .unwrap();
    println!(
        "startup_rate_Bps={} cold_refill_us={}",
        speed.get(),
        (wake - acked).as_micros()
    );
    assert!(wake - acked < Duration::from_millis(100));
    assert!(
        pacer
            .delay(
                rtt,
                1420,
                1420,
                bbr.window(),
                Some(speed),
                acked + Duration::from_millis(1)
            )
            .is_some()
    );
    assert_eq!(
        pacer.delay(rtt, 1420, 1420, bbr.window(), Some(speed), wake),
        None
    );
    assert_eq!(bbr.initial_window(), 12_000);
}
