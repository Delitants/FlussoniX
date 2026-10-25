//! Pacing of packet transmissions.

use crate::{Duration, Instant};
use std::num::NonZeroU64;

use tracing::warn;

/// A simple token-bucket pacer
///
/// Without a controller rate, capacity is derived from a fraction of the
/// congestion window which can be sent in regular intervals.
/// Once the bucket is empty, further transmission is blocked.
/// The bucket refills at a rate slightly faster
/// than one congestion window per RTT, as recommended in
/// <https://tools.ietf.org/html/draft-ietf-quic-recovery-34#section-7.7>.
/// An opted-in controller instead selects its byte rate directly, with the
/// same bounded burst quantum. Existing protocol probe exemptions remain.
pub(super) struct Pacer {
    capacity: u64,
    last_window: u64,
    last_mtu: u16,
    tokens: u64,
    prev: Instant,
    controller_rate: Option<NonZeroU64>,
    // Sub-byte credit in billionths of a byte, retained across frequent polls.
    fractional_credit: u64,
}

impl Pacer {
    /// Obtains a new [`Pacer`].
    pub(super) fn new(
        smoothed_rtt: Duration,
        window: u64,
        mtu: u16,
        controller_rate: Option<NonZeroU64>,
        now: Instant,
    ) -> Self {
        let capacity = controller_rate.map_or_else(
            || optimal_capacity(smoothed_rtt, window, mtu),
            |rate| controlled_capacity(rate, mtu),
        );
        Self {
            capacity,
            last_window: window,
            last_mtu: mtu,
            tokens: capacity,
            prev: now,
            controller_rate,
            fractional_credit: 0,
        }
    }

    /// Record that a packet has been transmitted.
    pub(super) fn on_transmit(&mut self, packet_length: u16) {
        self.tokens = self.tokens.saturating_sub(packet_length.into())
    }

    /// Return how long we need to wait before sending `bytes_to_send`
    ///
    /// If we can send a packet right away, this returns `None`. Otherwise, returns `Some(d)`,
    /// where `d` is the time before this function should be called again.
    ///
    /// The 5/4 ratio used here comes from the suggestion that N = 1.25 in the draft IETF RFC for
    /// QUIC.
    pub(super) fn delay(
        &mut self,
        smoothed_rtt: Duration,
        bytes_to_send: u64,
        mtu: u16,
        window: u64,
        controller_rate: Option<NonZeroU64>,
        now: Instant,
    ) -> Option<Instant> {
        debug_assert_ne!(
            window, 0,
            "zero-sized congestion control window is nonsense"
        );

        if let Some(rate) = controller_rate {
            return self.controlled_delay(bytes_to_send, mtu, rate, now);
        }
        if self.controller_rate.is_some() {
            // Settle the old rate once before resuming legacy lazy refill.
            self.refill_controlled(now);
            self.controller_rate = None;
            self.fractional_credit = 0;
            self.capacity = optimal_capacity(smoothed_rtt, window, mtu);
            self.tokens = self.tokens.min(self.capacity);
            self.last_window = window;
            self.last_mtu = mtu;
        }

        if window != self.last_window || mtu != self.last_mtu {
            self.capacity = optimal_capacity(smoothed_rtt, window, mtu);

            // Clamp the tokens
            self.tokens = self.capacity.min(self.tokens);
            self.last_window = window;
            self.last_mtu = mtu;
        }

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        // we disable pacing for extremely large windows
        if window > u64::from(u32::MAX) {
            return None;
        }

        let window = window as u32;

        let time_elapsed = now.checked_duration_since(self.prev).unwrap_or_else(|| {
            warn!("received a timestamp early than a previous recorded time, ignoring");
            Default::default()
        });

        if smoothed_rtt.as_nanos() == 0 {
            return None;
        }

        let elapsed_rtts = time_elapsed.as_secs_f64() / smoothed_rtt.as_secs_f64();
        let new_tokens = window as f64 * 1.25 * elapsed_rtts;
        self.tokens = self
            .tokens
            .saturating_add(new_tokens as _)
            .min(self.capacity);

        self.prev = now;

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        let unscaled_delay = smoothed_rtt
            .checked_mul((bytes_to_send.max(self.capacity) - self.tokens) as _)
            .unwrap_or(Duration::MAX)
            / window;

        // divisions come before multiplications to prevent overflow
        // this is the time at which the pacing window becomes empty
        Some(self.prev + (unscaled_delay / 5) * 4)
    }
    /// Credit for the next normal packet at an explicit controller rate,
    /// without refilling, moving timers or changing pacing state. None keeps
    /// legacy window-derived pacing unmeasured. Settle elapsed time at the
    /// previous rate, then clamp to the selected rate/MTU burst capacity.
    pub(super) fn controlled_credit(
        &self,
        mtu: u16,
        rate: Option<NonZeroU64>,
        now: Instant,
    ) -> Option<u64> {
        let selected = rate?;
        let tokens = if let Some(previous) = self.controller_rate {
            let credit = now
                .saturating_duration_since(self.prev)
                .as_nanos()
                .saturating_mul(u128::from(previous.get()))
                .saturating_add(u128::from(self.fractional_credit));
            u128::from(self.tokens)
                .saturating_add(credit / NANOS_PER_SECOND)
                .min(u128::from(self.capacity)) as u64
        } else {
            self.tokens
        };
        Some(tokens.min(controlled_capacity(selected, mtu)))
    }

    /// Refill at the previously selected rate before applying any new rate.
    /// No allocation, floating-point arithmetic or congestion-window conversion.
    fn refill_controlled(&mut self, now: Instant) {
        let rate = self.controller_rate.expect("controlled pacing is active");
        let elapsed = now.saturating_duration_since(self.prev);
        self.prev = self.prev.max(now);
        let credit = elapsed
            .as_nanos()
            .saturating_mul(u128::from(rate.get()))
            .saturating_add(u128::from(self.fractional_credit));
        let tokens = u128::from(self.tokens).saturating_add(credit / NANOS_PER_SECOND);
        self.tokens = tokens.min(u128::from(self.capacity)) as u64;
        self.fractional_credit = if tokens >= u128::from(self.capacity) {
            0
        } else {
            (credit % NANOS_PER_SECOND) as u64
        };
    }

    fn controlled_delay(
        &mut self,
        bytes_to_send: u64,
        mtu: u16,
        rate: NonZeroU64,
        now: Instant,
    ) -> Option<Instant> {
        if self.controller_rate.is_some() {
            self.refill_controlled(now);
        } else {
            // Legacy refill is lazy. Its elapsed time is not credit at a new
            // controller rate; carry only tokens that actually remain.
            self.prev = self.prev.max(now);
            self.fractional_credit = 0;
        }
        if self.controller_rate != Some(rate) || self.last_mtu != mtu {
            self.capacity = controlled_capacity(rate, mtu);
            self.tokens = self.tokens.min(self.capacity);
            if self.tokens == self.capacity {
                self.fractional_credit = 0;
            }
            self.controller_rate = Some(rate);
            self.last_mtu = mtu;
        }
        if self.tokens >= bytes_to_send {
            return None;
        }
        // Refill a quantum, allowing the existing GSO batching. Subtract
        // fractional credit, and round up so a timer cannot wake too early.
        let needed = u128::from(bytes_to_send.max(self.capacity) - self.tokens) * NANOS_PER_SECOND
            - u128::from(self.fractional_credit);
        let nanos = needed.div_ceil(u128::from(rate.get()));
        // Extreme rates/requests must not overflow Instant or disable pacing.
        // A bounded recheck grants no bytes; ordinary waits remain exact.
        let delay = Duration::from_nanos(nanos.min(MAX_CONTROLLED_WAIT_NANOS) as u64);
        Some(self.prev.checked_add(delay).unwrap_or(self.prev))
    }
}

const NANOS_PER_SECOND: u128 = 1_000_000_000;
const MAX_CONTROLLED_WAIT_NANOS: u128 = NANOS_PER_SECOND;

fn controlled_capacity(rate: NonZeroU64, mtu: u16) -> u64 {
    let bytes = u128::from(rate.get()) * BURST_INTERVAL_NANOS / NANOS_PER_SECOND;
    bytes.clamp(
        u128::from(MIN_BURST_SIZE * u64::from(mtu)),
        u128::from(MAX_BURST_SIZE * u64::from(mtu)),
    ) as u64
}

/// Calculates a pacer capacity for a certain window and RTT
///
/// The goal is to emit a burst (of size `capacity`) in timer intervals
/// which compromise between
/// - ideally distributing datagrams over time
/// - constantly waking up the connection to produce additional datagrams
///
/// Too short burst intervals means we will never meet them since the timer
/// accuracy in user-space is not high enough. If we miss the interval by more
/// than 25%, we will lose that part of the congestion window since no additional
/// tokens for the extra-elapsed time can be stored.
///
/// Too long burst intervals make pacing less effective.
fn optimal_capacity(smoothed_rtt: Duration, window: u64, mtu: u16) -> u64 {
    let rtt = smoothed_rtt.as_nanos().max(1);

    let capacity = ((window as u128 * BURST_INTERVAL_NANOS) / rtt) as u64;

    // Small bursts are less efficient (no GSO), could increase latency and don't effectively
    // use the channel's buffer capacity. Large bursts might block the connection on sending.
    capacity.clamp(MIN_BURST_SIZE * mtu as u64, MAX_BURST_SIZE * mtu as u64)
}

/// The burst interval
///
/// The capacity will we refilled in 4/5 of that time.
/// 2ms is chosen here since framework timers might have 1ms precision.
/// If kernel-level pacing is supported later a higher time here might be
/// more applicable.
const BURST_INTERVAL_NANOS: u128 = 2_000_000; // 2ms

/// Allows some usage of GSO, and doesn't slow down the handshake.
const MIN_BURST_SIZE: u64 = 10;

/// Creating 256 packets took 1ms in a benchmark, so larger bursts don't make sense.
const MAX_BURST_SIZE: u64 = 256;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_panic_on_bad_instant() {
        let old_instant = Instant::now();
        let new_instant = old_instant + Duration::from_micros(15);
        let rtt = Duration::from_micros(400);

        assert!(
            Pacer::new(rtt, 30000, 1500, None, new_instant)
                .delay(Duration::from_micros(0), 0, 1500, 1, None, old_instant)
                .is_none()
        );
        assert!(
            Pacer::new(rtt, 30000, 1500, None, new_instant)
                .delay(Duration::from_micros(0), 1600, 1500, 1, None, old_instant)
                .is_none()
        );
        assert!(
            Pacer::new(rtt, 30000, 1500, None, new_instant)
                .delay(
                    Duration::from_micros(0),
                    1500,
                    1500,
                    3000,
                    None,
                    old_instant
                )
                .is_none()
        );
    }

    #[test]
    fn derives_initial_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let pacer = Pacer::new(rtt, window, mtu, None, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, None, now);
        assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(rtt, 1, mtu, None, now);
        assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn adjusts_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, None, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);
        let initial_tokens = pacer.tokens;

        pacer.delay(rtt, mtu as u64, mtu, window * 2, None, now);
        assert_eq!(
            pacer.capacity,
            (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens);

        pacer.delay(rtt, mtu as u64, mtu, window / 2, None, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens / 2);

        pacer.delay(rtt, mtu as u64, mtu * 2, window, None, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );

        pacer.delay(rtt, mtu as u64, 20_000, window, None, now);
        assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
    }

    #[test]
    fn computes_pause_correctly() {
        let window = 2_000_000u64;
        let mtu = 1000;
        let rtt = Duration::from_millis(50);
        let old_instant = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, None, old_instant);
        let packet_capacity = pacer.capacity / mtu as u64;

        for _ in 0..packet_capacity {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, None, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        let pace_duration = Duration::from_nanos((BURST_INTERVAL_NANOS * 4 / 5) as u64);

        assert_eq!(
            pacer
                .delay(rtt, mtu as u64, mtu, window, None, old_instant)
                .expect("Send must be delayed")
                .duration_since(old_instant),
            pace_duration
        );

        // Refill half of the tokens
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                None,
                old_instant + pace_duration / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity / 2);

        for _ in 0..packet_capacity / 2 {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, None, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        // Refill all capacity by waiting more than the expected duration
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                None,
                old_instant + pace_duration * 3 / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity);
    }
}

#[cfg(test)]
mod controlled_tests;

#[cfg(test)]
mod idle_credit_tests {
    use super::*;
    #[test]
    fn readonly_credit_settles_old_rate_and_does_not_grant_a_new_burst_or_move_timers() {
        let t = Instant::now();
        let rate = NonZeroU64::new(64_000);
        let mut pacer = Pacer::new(Duration::from_millis(100), 512_000, 1200, rate, t);
        pacer.on_transmit(11_500);
        let selected = NonZeroU64::new(1_000_000);
        let at = t + Duration::from_millis(10);
        assert_eq!(pacer.controlled_credit(1200, selected, at), Some(1_140));
        assert_eq!(pacer.controlled_credit(1200, selected, at), Some(1_140));
        assert_eq!(pacer.tokens, 500);
        assert_eq!(pacer.prev, t);
        assert_eq!(pacer.controller_rate, rate);
        assert_eq!(pacer.fractional_credit, 0);
        assert_eq!(
            pacer.controlled_credit(1200, selected, t + Duration::from_millis(11)),
            Some(1_204)
        );
        assert_eq!(
            pacer.controlled_credit(1200, None, at),
            None,
            "fallback is unmeasured"
        );
        assert!(
            pacer
                .delay(
                    Duration::from_millis(100),
                    1200,
                    1200,
                    512_000,
                    selected,
                    at
                )
                .is_some()
        );
        assert_eq!(
            pacer.tokens, 1_140,
            "same credit as actual normal admission"
        );
    }
}
