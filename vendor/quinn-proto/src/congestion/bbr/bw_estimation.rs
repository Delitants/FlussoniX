use std::fmt::{Debug, Display, Formatter};

use super::delivery::{DeliverySnapshot, Rate, Sampler};
use super::min_max::MinMax;
use crate::{Duration, Instant};

#[derive(Clone, Debug, Default)]
pub(crate) struct BandwidthEstimation {
    total_acked: u64,
    prev_total_acked: u64,
    acked_time: Option<Instant>,
    prev_acked_time: Option<Instant>,
    total_sent: u64,
    prev_total_sent: u64,
    sent_time: Option<Instant>,
    prev_sent_time: Option<Instant>,
    max_filter: MinMax,
    acked_at_last_window: u64,
    delivery: Option<Sampler>,
    pub(super) last_sample: Option<Rate>,
}

impl BandwidthEstimation {
    fn record_delivery_sample(&mut self, round: u64, bandwidth: u64, app_limited: bool) {
        if bandwidth > 0 && (!app_limited || self.max_filter.get() <= bandwidth) {
            self.max_filter.update_max(round, bandwidth);
        }
    }
    pub(super) fn enable_delivery_sampling(&mut self) {
        self.delivery = Some(Sampler::default());
    }
    pub(super) fn packet_sent(&mut self, now: Instant, in_flight: u64) -> Option<DeliverySnapshot> {
        self.delivery
            .as_mut()?
            .sent(now, in_flight, self.total_acked)
    }
    pub(super) fn mark_limited(&mut self, in_flight: u64) {
        if let Some(sampler) = self.delivery.as_mut() {
            sampler.limited(self.total_acked, in_flight);
        }
    }
    pub(super) fn packet_acked(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        packet: DeliverySnapshot,
    ) {
        self.total_acked = self.total_acked.saturating_add(bytes);
        self.delivery.as_mut().unwrap().acked(now, sent, packet);
    }
    pub(super) fn finish_delivery_sample(
        &mut self,
        now: Instant,
        round: u64,
        min_rtt: Duration,
    ) -> Option<bool> {
        self.last_sample = self
            .delivery
            .as_mut()?
            .finish(now, min_rtt, self.total_acked);
        let sample = self.last_sample?;
        self.record_delivery_sample(round, sample.bytes_per_second, sample.limited);
        Some(sample.limited)
    }
    pub(super) fn reset_delivery_path(&mut self, sequence: u64) {
        if self.delivery.is_some() {
            self.delivery = Some(Sampler::new_path(sequence));
        }
        self.acked_at_last_window = self.total_acked;
        self.last_sample = None;
    }
    pub(crate) fn on_sent(&mut self, now: Instant, bytes: u64) {
        self.prev_total_sent = self.total_sent;
        self.total_sent += bytes;
        self.prev_sent_time = self.sent_time;
        self.sent_time = Some(now);
    }

    pub(crate) fn on_ack(
        &mut self,
        now: Instant,
        _sent: Instant,
        bytes: u64,
        round: u64,
        app_limited: bool,
    ) {
        self.prev_total_acked = self.total_acked;
        self.total_acked += bytes;
        self.prev_acked_time = self.acked_time;
        self.acked_time = Some(now);

        let prev_sent_time = match self.prev_sent_time {
            Some(prev_sent_time) => prev_sent_time,
            None => return,
        };

        let send_rate = match self.sent_time {
            Some(sent_time) if sent_time > prev_sent_time => Self::bw_from_delta(
                self.total_sent - self.prev_total_sent,
                sent_time - prev_sent_time,
            )
            .unwrap_or(0),
            _ => u64::MAX, // will take the min of send and ack, so this is just a skip
        };

        let ack_rate = match self.prev_acked_time {
            Some(prev_acked_time) => Self::bw_from_delta(
                self.total_acked - self.prev_total_acked,
                now - prev_acked_time,
            )
            .unwrap_or(0),
            None => 0,
        };

        let bandwidth = send_rate.min(ack_rate);
        if !app_limited && self.max_filter.get() < bandwidth {
            self.max_filter.update_max(round, bandwidth);
        }
    }

    pub(crate) fn bytes_acked_this_window(&self) -> u64 {
        self.total_acked - self.acked_at_last_window
    }

    pub(crate) fn end_acks(&mut self, _current_round: u64, _app_limited: bool) {
        self.acked_at_last_window = self.total_acked;
    }

    pub(crate) fn get_estimate(&self) -> u64 {
        self.max_filter.get()
    }

    pub(crate) const fn bw_from_delta(bytes: u64, delta: Duration) -> Option<u64> {
        let window_duration_ns = delta.as_nanos();
        if window_duration_ns == 0 {
            return None;
        }
        let b_ns = bytes * 1_000_000_000;
        let bytes_per_second = b_ns / (window_duration_ns as u64);
        Some(bytes_per_second)
    }
}

impl Display for BandwidthEstimation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.3} MB/s",
            self.get_estimate() as f32 / (1024 * 1024) as f32
        )
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    // Break caught: lower/equal valid samples never age an old maximum.
    #[test]
    fn lower_nonlimited_samples_expire_the_previous_peak() {
        let mut estimator = BandwidthEstimation::default();
        estimator.record_delivery_sample(0, 1_000_000, false);
        for round in 1..=11 {
            estimator.record_delivery_sample(round, 500_000, false);
        }
        assert_eq!(estimator.get_estimate(), 500_000);
    }
    // Break caught: application bubbles depress a known rate or hide real growth.
    #[test]
    fn limited_samples_can_raise_but_cannot_depress_the_estimate() {
        let mut estimator = BandwidthEstimation::default();
        estimator.record_delivery_sample(0, 1_000_000, false);
        for round in 1..=20 {
            estimator.record_delivery_sample(round, 500_000, true);
        }
        assert_eq!(estimator.get_estimate(), 1_000_000);
        estimator.record_delivery_sample(21, 2_000_000, true);
        assert_eq!(estimator.get_estimate(), 2_000_000);
    }
}
