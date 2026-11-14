//! Delivery snapshots use tracked packets, never the most recent send pair.
use crate::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
/// Opaque delivery history captured when a tracked QUIC packet is sent.
pub struct DeliverySnapshot {
    delivered: u64,
    delivered_at: Instant,
    first_sent: Instant,
    pub(super) sequence: u64,
    pub(super) limited: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Rate {
    pub bytes_per_second: u64,
    pub limited: bool,
}

#[derive(Clone, Debug, Default)]
pub(super) struct Sampler {
    first_sent: Option<Instant>,
    delivered_at: Option<Instant>,
    next_sequence: u64,
    last_delivered_sequence: u64,
    limited_until: u64,
    batch: Option<(Instant, DeliverySnapshot)>,
}
impl Sampler {
    pub(super) fn new_path(sequence: u64) -> Self {
        Self {
            next_sequence: sequence,
            last_delivered_sequence: sequence,
            ..Self::default()
        }
    }
    pub(super) fn sent(
        &mut self,
        at: Instant,
        in_flight: u64,
        delivered: u64,
    ) -> Option<DeliverySnapshot> {
        if in_flight == 0 || self.first_sent.is_none() {
            self.first_sent = Some(at);
            self.delivered_at = Some(at);
        }
        self.next_sequence = self.next_sequence.checked_add(1)?;
        Some(DeliverySnapshot {
            delivered,
            delivered_at: self.delivered_at?,
            first_sent: self.first_sent?,
            sequence: self.next_sequence,
            limited: self.limited_until != 0,
        })
    }
    pub(super) fn acked(&mut self, at: Instant, sent: Instant, packet: DeliverySnapshot) {
        self.delivered_at = Some(at);
        if packet.sequence >= self.last_delivered_sequence {
            self.last_delivered_sequence = packet.sequence;
            self.first_sent = Some(sent);
        }
        if self
            .batch
            .is_none_or(|(_, previous)| packet.sequence > previous.sequence)
        {
            self.batch = Some((sent, packet));
        }
    }
    pub(super) fn limited(&mut self, delivered: u64, in_flight: u64) {
        self.limited_until = delivered.saturating_add(in_flight).max(1);
    }
    pub(super) fn finish(
        &mut self,
        at: Instant,
        min_rtt: Duration,
        delivered: u64,
    ) -> Option<Rate> {
        if delivered > self.limited_until {
            self.limited_until = 0;
        }
        let (sent, packet) = self.batch.take()?;
        let interval = sent
            .checked_duration_since(packet.first_sent)?
            .max(at.checked_duration_since(packet.delivered_at)?);
        if interval.is_zero() || interval < min_rtt {
            return None;
        }
        let bytes = delivered.checked_sub(packet.delivered)?;
        if bytes == 0 {
            return None;
        }
        let rate = u128::from(bytes) * 1_000_000_000 / interval.as_nanos();
        Some(Rate {
            bytes_per_second: rate.try_into().ok()?,
            limited: packet.limited,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ms(at: Instant, elapsed: u64) -> Instant {
        at + Duration::from_millis(elapsed)
    }

    // Break caught: a single initial ACK has no previous callback interval.
    #[test]
    fn first_full_flight_has_a_rate_at_exact_min_rtt() {
        let mut s = Sampler::default();
        let t = Instant::now();
        let p = s.sent(t, 0, 0).unwrap();
        s.acked(ms(t, 200), t, p);
        assert_eq!(
            s.finish(ms(t, 200), Duration::from_millis(200), 1000),
            Some(Rate {
                bytes_per_second: 5000,
                limited: false
            })
        );
    }

    // Break caught: same-time ACK callbacks overwrite the batch's delivered bytes.
    #[test]
    fn one_sample_uses_the_whole_ack_batch() {
        let mut s = Sampler::default();
        let t = Instant::now();
        let a = s.sent(t, 0, 0).unwrap();
        let b = s.sent(ms(t, 100), 1000, 0).unwrap();
        s.acked(ms(t, 300), t, a);
        s.acked(ms(t, 300), ms(t, 100), b);
        assert_eq!(
            s.finish(ms(t, 300), Duration::from_millis(200), 2000),
            Some(Rate {
                bytes_per_second: 6666,
                limited: false
            })
        );
        assert_eq!(s.finish(ms(t, 301), Duration::from_millis(200), 2000), None);
    }

    // Break caught: two closely spaced ACKs produce a rate from their 1us gap.
    #[test]
    fn compressed_acks_keep_the_acknowledged_flight_interval() {
        let mut s = Sampler::default();
        let t = Instant::now();
        let a = s.sent(t, 0, 0).unwrap();
        let b = s.sent(ms(t, 100), 1000, 0).unwrap();
        s.acked(ms(t, 200), t, a);
        s.finish(ms(t, 200), Duration::from_millis(200), 1000);
        let at = ms(t, 200) + Duration::from_micros(1);
        s.acked(at, ms(t, 100), b);
        assert_eq!(
            s.finish(at, Duration::from_millis(200), 2000),
            Some(Rate {
                bytes_per_second: 9999,
                limited: false
            })
        );
    }

    // Break caught: idle wall time depresses the first rate after a fresh flight.
    #[test]
    fn idle_resume_starts_a_new_sampling_interval() {
        let mut s = Sampler::default();
        let t = Instant::now();
        let a = s.sent(t, 0, 0).unwrap();
        s.acked(ms(t, 200), t, a);
        s.finish(ms(t, 200), Duration::from_millis(200), 1000);
        let b = s.sent(ms(t, 10_000), 0, 1000).unwrap();
        s.acked(ms(t, 10_200), ms(t, 10_000), b);
        assert_eq!(
            s.finish(ms(t, 10_200), Duration::from_millis(200), 2000),
            Some(Rate {
                bytes_per_second: 5000,
                limited: false
            })
        );
    }

    // Break caught: current application state relabels an already-sent packet.
    #[test]
    fn application_limited_marker_is_captured_at_send() {
        let mut s = Sampler::default();
        let t = Instant::now();
        s.limited(0, 0);
        let a = s.sent(t, 0, 0).unwrap();
        s.acked(ms(t, 200), t, a);
        assert_eq!(
            s.finish(ms(t, 200), Duration::from_millis(200), 1000),
            Some(Rate {
                bytes_per_second: 5000,
                limited: true
            })
        );
        let b = s.sent(ms(t, 201), 0, 1000).unwrap();
        s.acked(ms(t, 401), ms(t, 201), b);
        assert_eq!(
            s.finish(ms(t, 401), Duration::from_millis(200), 2000),
            Some(Rate {
                bytes_per_second: 5000,
                limited: false
            })
        );
    }

    // Break caught: reordered old ACKs rewind the anchor for later sends.
    #[test]
    fn reordered_ack_cannot_rewind_the_next_flight_anchor() {
        let mut s = Sampler::default();
        let t = Instant::now();
        let a = s.sent(t, 0, 0).unwrap();
        let b = s.sent(ms(t, 100), 1000, 0).unwrap();
        s.acked(ms(t, 300), ms(t, 100), b);
        s.acked(ms(t, 300), t, a);
        s.finish(ms(t, 300), Duration::from_millis(200), 2000);
        let c = s.sent(ms(t, 301), 1000, 2000).unwrap();
        s.acked(ms(t, 501), ms(t, 301), c);
        assert_eq!(
            s.finish(ms(t, 501), Duration::from_millis(200), 3000),
            Some(Rate {
                bytes_per_second: 4975,
                limited: false
            })
        );
    }

    // Break caught: zero or implausibly short intervals inflate the model.
    #[test]
    fn zero_short_and_arithmetically_unrepresentable_rates_are_rejected() {
        for ns in [0, 1, 199_999_999] {
            let mut s = Sampler::default();
            let t = Instant::now();
            let p = s.sent(t, 0, 0).unwrap();
            let at = t + Duration::from_nanos(ns);
            s.acked(at, t, p);
            assert_eq!(s.finish(at, Duration::from_millis(200), 1000), None);
        }
        let mut s = Sampler::default();
        let t = Instant::now();
        let p = s.sent(t, 0, 0).unwrap();
        let at = t + Duration::from_nanos(1);
        s.acked(at, t, p);
        assert_eq!(s.finish(at, Duration::ZERO, u64::MAX), None);
    }
}
