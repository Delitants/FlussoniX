use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use rand::{RngExt, SeedableRng};
use rand_pcg::Pcg32;

use crate::congestion::ControllerMetrics;
use crate::congestion::bbr::bw_estimation::BandwidthEstimation;
use crate::congestion::bbr::min_max::MinMax;
use crate::connection::RttEstimator;
use crate::{Duration, Instant};

use super::{BASE_DATAGRAM_SIZE, Controller, ControllerFactory};

mod bw_estimation;
mod delivery;
mod min_max;
pub use delivery::DeliverySnapshot;

/// Experimental! Use at your own risk.
///
/// Aims for reduced buffer bloat and improved performance over high bandwidth-delay product networks.
/// Based on google's quiche implementation <https://source.chromium.org/chromium/chromium/src/+/master:net/third_party/quiche/src/quic/core/congestion_control/bbr_sender.cc>
/// of BBR <https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control>.
/// More discussion and links at <https://groups.google.com/g/bbr-dev>.
#[derive(Debug, Clone)]
pub struct Bbr {
    config: Arc<BbrConfig>,
    application_demand: Option<super::ApplicationDemand>,
    current_mtu: u64,
    max_bandwidth: BandwidthEstimation,
    acked_bytes: u64,
    mode: Mode,
    loss_state: LossState,
    recovery_state: RecoveryState,
    recovery_window: u64,
    is_at_full_bandwidth: bool,
    pacing_gain: f32,
    high_gain: f32,
    drain_gain: f32,
    cwnd_gain: f32,
    high_cwnd_gain: f32,
    last_cycle_start: Option<Instant>,
    current_cycle_offset: u8,
    init_cwnd: u64,
    min_cwnd: u64,
    prev_in_flight_count: u64,
    exit_probe_rtt_at: Option<Instant>,
    probe_rtt_last_started_at: Option<Instant>,
    min_rtt: Duration,
    exiting_quiescence: bool,
    pacing_rate: u64,
    max_acked_packet_number: u64,
    max_sent_packet_number: u64,
    end_recovery_at_packet_number: u64,
    cwnd: u64,
    current_round_trip_end_packet_number: u64,
    round_count: u64,
    bw_at_last_round: u64,
    round_wo_bw_gain: u64,
    ack_aggregation: AckAggregationState,
    random_number_generator: Pcg32,
}

impl Bbr {
    /// Construct a state using the given `config` and current time `now`
    pub fn new(config: Arc<BbrConfig>, current_mtu: u16) -> Self {
        let initial_window = config.initial_window;
        let mut max_bandwidth = BandwidthEstimation::default();
        if config.delivery_rate_sampling {
            max_bandwidth.enable_delivery_sampling();
        }
        let application_demand = config
            .delivery_rate_sampling
            .then(super::ApplicationDemand::default);
        Self {
            config,
            application_demand,
            current_mtu: current_mtu as u64,
            max_bandwidth,
            acked_bytes: 0,
            mode: Mode::Startup,
            loss_state: Default::default(),
            recovery_state: RecoveryState::NotInRecovery,
            recovery_window: 0,
            is_at_full_bandwidth: false,
            pacing_gain: K_DEFAULT_HIGH_GAIN,
            high_gain: K_DEFAULT_HIGH_GAIN,
            drain_gain: 1.0 / K_DEFAULT_HIGH_GAIN,
            cwnd_gain: K_DEFAULT_HIGH_GAIN,
            high_cwnd_gain: K_DEFAULT_HIGH_GAIN,
            last_cycle_start: None,
            current_cycle_offset: 0,
            init_cwnd: initial_window,
            min_cwnd: calculate_min_window(current_mtu as u64),
            prev_in_flight_count: 0,
            exit_probe_rtt_at: None,
            probe_rtt_last_started_at: None,
            min_rtt: Default::default(),
            exiting_quiescence: false,
            pacing_rate: 0,
            max_acked_packet_number: 0,
            max_sent_packet_number: 0,
            end_recovery_at_packet_number: 0,
            cwnd: initial_window,
            current_round_trip_end_packet_number: 0,
            round_count: 0,
            bw_at_last_round: 0,
            round_wo_bw_gain: 0,
            ack_aggregation: AckAggregationState::default(),
            random_number_generator: Pcg32::from_rng(&mut rand::rng()),
        }
    }

    fn enter_startup_mode(&mut self) {
        self.mode = Mode::Startup;
        self.pacing_gain = self.high_gain;
        self.cwnd_gain = self.high_cwnd_gain;
    }

    fn enter_probe_bandwidth_mode(&mut self, now: Instant) {
        self.mode = Mode::ProbeBw;
        self.cwnd_gain = K_DERIVED_HIGH_CWNDGAIN;
        self.last_cycle_start = Some(now);
        // Pick a random offset for the gain cycle out of {0, 2..7} range. 1 is
        // excluded because in that case increased gain and decreased gain would not
        // follow each other.
        let mut rand_index = self
            .random_number_generator
            .random_range(0..K_PACING_GAIN.len() as u8 - 1);
        if rand_index >= 1 {
            rand_index += 1;
        }
        self.current_cycle_offset = rand_index;
        self.pacing_gain = K_PACING_GAIN[rand_index as usize];
    }

    fn update_recovery_state(&mut self, is_round_start: bool) {
        // Exit recovery when there are no losses for a round.
        if self.loss_state.has_losses() {
            self.end_recovery_at_packet_number = self.max_sent_packet_number;
        }
        match self.recovery_state {
            // Enter conservation on the first loss.
            RecoveryState::NotInRecovery if self.loss_state.has_losses() => {
                self.recovery_state = RecoveryState::Conservation;
                // This will cause the |recovery_window| to be set to the
                // correct value in CalculateRecoveryWindow().
                self.recovery_window = 0;
                // Since the conservation phase is meant to be lasting for a whole
                // round, extend the current round as if it were started right now.
                self.current_round_trip_end_packet_number = self.max_sent_packet_number;
            }
            RecoveryState::Growth | RecoveryState::Conservation => {
                if self.recovery_state == RecoveryState::Conservation && is_round_start {
                    self.recovery_state = RecoveryState::Growth;
                }
                // Exit recovery if appropriate.
                if !self.loss_state.has_losses()
                    && self.max_acked_packet_number > self.end_recovery_at_packet_number
                {
                    self.recovery_state = RecoveryState::NotInRecovery;
                }
            }
            _ => {}
        }
    }

    fn update_gain_cycle_phase(&mut self, now: Instant, in_flight: u64) {
        // In most cases, the cycle is advanced after an RTT passes.
        let mut should_advance_gain_cycling = self
            .last_cycle_start
            .map(|last_cycle_start| now.duration_since(last_cycle_start) > self.min_rtt)
            .unwrap_or(false);
        // If the pacing gain is above 1.0, the connection is trying to probe the
        // bandwidth by increasing the number of bytes in flight to at least
        // pacing_gain * BDP.  Make sure that it actually reaches the target, as
        // long as there are no losses suggesting that the buffers are not able to
        // hold that much.
        if self.pacing_gain > 1.0
            && !self.loss_state.has_losses()
            && self.prev_in_flight_count < self.get_target_cwnd(self.pacing_gain)
        {
            should_advance_gain_cycling = false;
        }

        // If pacing gain is below 1.0, the connection is trying to drain the extra
        // queue which could have been incurred by probing prior to it.  If the
        // number of bytes in flight falls down to the estimated BDP value earlier,
        // conclude that the queue has been successfully drained and exit this cycle
        // early.
        if self.pacing_gain < 1.0 && in_flight <= self.get_target_cwnd(1.0) {
            should_advance_gain_cycling = true;
        }

        if should_advance_gain_cycling {
            self.current_cycle_offset = (self.current_cycle_offset + 1) % K_PACING_GAIN.len() as u8;
            self.last_cycle_start = Some(now);
            // Stay in low gain mode until the target BDP is hit.  Low gain mode
            // will be exited immediately when the target BDP is achieved.
            if DRAIN_TO_TARGET
                && self.pacing_gain < 1.0
                && (K_PACING_GAIN[self.current_cycle_offset as usize] - 1.0).abs() < f32::EPSILON
                && in_flight > self.get_target_cwnd(1.0)
            {
                return;
            }
            self.pacing_gain = K_PACING_GAIN[self.current_cycle_offset as usize];
        }
    }

    fn maybe_exit_startup_or_drain(&mut self, now: Instant, in_flight: u64) {
        if self.mode == Mode::Startup && self.is_at_full_bandwidth {
            self.mode = Mode::Drain;
            self.pacing_gain = self.drain_gain;
            self.cwnd_gain = self.high_cwnd_gain;
        }
        if self.mode == Mode::Drain && in_flight <= self.get_target_cwnd(1.0) {
            self.enter_probe_bandwidth_mode(now);
        }
    }

    fn is_min_rtt_expired(&self, now: Instant, app_limited: bool) -> bool {
        !app_limited
            && self
                .probe_rtt_last_started_at
                .map(|last| now.saturating_duration_since(last) > Duration::from_secs(10))
                .unwrap_or(true)
    }

    fn maybe_enter_or_exit_probe_rtt(
        &mut self,
        now: Instant,
        is_round_start: bool,
        bytes_in_flight: u64,
        app_limited: bool,
    ) {
        let min_rtt_expired = self.is_min_rtt_expired(now, app_limited);
        if min_rtt_expired && !self.exiting_quiescence && self.mode != Mode::ProbeRtt {
            self.mode = Mode::ProbeRtt;
            self.pacing_gain = 1.0;
            // Do not decide on the time to exit ProbeRtt until the
            // |bytes_in_flight| is at the target small value.
            self.exit_probe_rtt_at = None;
            self.probe_rtt_last_started_at = Some(now);
        }

        if self.mode == Mode::ProbeRtt {
            match self.exit_probe_rtt_at {
                None => {
                    // If the window has reached the appropriate size, schedule exiting
                    // ProbeRtt.  The CWND during ProbeRtt is
                    // kMinimumCongestionWindow, but we allow an extra packet since QUIC
                    // checks CWND before sending a packet.
                    if bytes_in_flight < self.get_probe_rtt_cwnd() + self.current_mtu {
                        const K_PROBE_RTT_TIME: Duration = Duration::from_millis(200);
                        self.exit_probe_rtt_at = Some(now + K_PROBE_RTT_TIME);
                    }
                }
                Some(exit_time) if is_round_start && now >= exit_time => {
                    if !self.is_at_full_bandwidth {
                        self.enter_startup_mode();
                    } else {
                        self.enter_probe_bandwidth_mode(now);
                    }
                }
                Some(_) => {}
            }
        }

        self.exiting_quiescence = false;
    }

    fn get_target_cwnd(&self, gain: f32) -> u64 {
        let bw = self.max_bandwidth.get_estimate();
        let bdp = if self.config.delivery_rate_sampling {
            self.min_rtt.as_micros().saturating_mul(u128::from(bw))
        } else {
            u128::from(self.min_rtt.as_micros() as u64 * bw)
        };
        let bdpf = bdp as f64;
        let cwnd = ((gain as f64 * bdpf) / 1_000_000f64) as u64;
        // BDP estimate will be zero if no bandwidth samples are available yet.
        if cwnd == 0 {
            return self.init_cwnd;
        }
        cwnd.max(self.min_cwnd)
    }

    fn get_probe_rtt_cwnd(&self) -> u64 {
        const K_MODERATE_PROBE_RTT_MULTIPLIER: f32 = 0.75;
        if PROBE_RTT_BASED_ON_BDP {
            return self.get_target_cwnd(K_MODERATE_PROBE_RTT_MULTIPLIER);
        }
        self.min_cwnd
    }

    fn calculate_pacing_rate(&mut self) {
        let bw = self.max_bandwidth.get_estimate();
        if bw == 0 {
            return;
        }
        let target_rate = (bw as f64 * self.pacing_gain as f64) as u64;
        if self.is_at_full_bandwidth {
            self.pacing_rate = target_rate;
            return;
        }

        // Pace at the rate of initial_window / RTT as soon as RTT measurements are
        // available.
        if self.pacing_rate == 0 && self.min_rtt.as_nanos() != 0 {
            let nominal = BandwidthEstimation::bw_from_delta(self.init_cwnd, self.min_rtt).unwrap();
            self.pacing_rate = if self.config.delivery_rate_sampling {
                // Bootstrap the explicit pacer with Startup's probe gain. A
                // small limited handshake is not a path-capacity estimate.
                target_rate.max((nominal as f64 * self.pacing_gain as f64) as u64)
            } else {
                nominal
            };
            return;
        }

        // Do not decrease the pacing rate during startup.
        if self.pacing_rate < target_rate {
            self.pacing_rate = target_rate;
        }
    }

    fn calculate_cwnd(&mut self, bytes_acked: u64, excess_acked: u64) {
        if self.mode == Mode::ProbeRtt {
            return;
        }
        let mut target_window = self.get_target_cwnd(self.cwnd_gain);
        if self.is_at_full_bandwidth {
            // Add the max recently measured ack aggregation to CWND.
            target_window += self.ack_aggregation.max_ack_height.get();
        } else {
            // Add the most recent excess acked.  Because CWND never decreases in
            // STARTUP, this will automatically create a very localized max filter.
            target_window += excess_acked;
        }
        // Instead of immediately setting the target CWND as the new one, BBR grows
        // the CWND towards |target_window| by only increasing it |bytes_acked| at a
        // time.
        if self.is_at_full_bandwidth {
            self.cwnd = target_window.min(self.cwnd + bytes_acked);
        } else if (if self.config.delivery_rate_sampling {
            self.cwnd < target_window
        } else {
            self.cwnd_gain < target_window as f32
        }) || (self.acked_bytes < self.init_cwnd)
        {
            // If the connection is not yet out of startup phase, do not decrease
            // the window.
            self.cwnd += bytes_acked;
        }

        // Enforce the limits on the congestion window.
        if self.cwnd < self.min_cwnd {
            self.cwnd = self.min_cwnd;
        }
    }

    fn calculate_recovery_window(&mut self, bytes_acked: u64, bytes_lost: u64, in_flight: u64) {
        if !self.recovery_state.in_recovery() {
            return;
        }
        // Set up the initial recovery window.
        if self.recovery_window == 0 {
            self.recovery_window = self.min_cwnd.max(in_flight + bytes_acked);
            return;
        }

        // Remove losses from the recovery window, while accounting for a potential
        // integer underflow.
        if self.recovery_window >= bytes_lost {
            self.recovery_window -= bytes_lost;
        } else {
            // k_max_segment_size = current_mtu
            self.recovery_window = self.current_mtu;
        }
        // In CONSERVATION mode, just subtracting losses is sufficient.  In GROWTH,
        // release additional |bytes_acked| to achieve a slow-start-like behavior.
        if self.recovery_state == RecoveryState::Growth {
            self.recovery_window += bytes_acked;
        }

        // Sanity checks.  Ensure that we always allow to send at least an MSS or
        // |bytes_acked| in response, whichever is larger.
        self.recovery_window = self
            .recovery_window
            .max(in_flight + bytes_acked)
            .max(self.min_cwnd);
    }

    /// <https://datatracker.ietf.org/doc/html/draft-cardwell-iccrg-bbr-congestion-control#section-4.3.2.2>
    fn check_if_full_bw_reached(&mut self, app_limited: bool) {
        if app_limited {
            return;
        }
        let target = (self.bw_at_last_round as f64 * K_STARTUP_GROWTH_TARGET as f64) as u64;
        let bw = self.max_bandwidth.get_estimate();
        if bw >= target {
            self.bw_at_last_round = bw;
            self.round_wo_bw_gain = 0;
            self.ack_aggregation.max_ack_height.reset();
            return;
        }

        self.round_wo_bw_gain += 1;
        if self.round_wo_bw_gain >= K_ROUND_TRIPS_WITHOUT_GROWTH_BEFORE_EXITING_STARTUP as u64
            || (self.recovery_state.in_recovery())
        {
            self.is_at_full_bandwidth = true;
        }
    }
}

impl Controller for Bbr {
    fn application_demand(&self) -> Option<super::ApplicationDemand> {
        self.application_demand.clone()
    }
    fn on_packet_sent(&mut self, now: Instant, in_flight: u64) -> Option<DeliverySnapshot> {
        let packet = self.max_bandwidth.packet_sent(now, in_flight)?;
        self.max_sent_packet_number = packet.sequence;
        Some(packet)
    }
    fn on_application_limited(&mut self, in_flight: u64) {
        // Empty transport queues may be the result of bounded upstream
        // admission. Do not start a new bubble while known originals wait.
        // Existing packet snapshots and the old bubble are never rewritten.
        if self
            .application_demand
            .as_ref()
            .is_some_and(|demand| demand.pending())
        {
            return;
        }
        self.max_bandwidth.mark_limited(in_flight);
    }
    fn on_path_change(&mut self) {
        if self.config.delivery_rate_sampling {
            self.max_bandwidth
                .reset_delivery_path(self.max_sent_packet_number);
        }
    }
    fn on_ack_with_snapshot(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
        packet: Option<DeliverySnapshot>,
    ) {
        if !self.config.delivery_rate_sampling {
            self.on_ack(now, sent, bytes, app_limited, rtt);
            return;
        }
        let Some(packet) = packet else {
            return;
        };
        self.max_acked_packet_number = self.max_acked_packet_number.max(packet.sequence);
        self.max_bandwidth.packet_acked(now, sent, bytes, packet);
        self.acked_bytes = self.acked_bytes.saturating_add(bytes);
        if self.min_rtt.is_zero() && !rtt.min().is_zero() {
            // A limited handshake still supplies a valid first RTT. Start its
            // age here; absence of an earlier sample is not an expired sample.
            self.min_rtt = rtt.min();
            self.probe_rtt_last_started_at = Some(now);
        } else if self.is_min_rtt_expired(now, packet.limited) || self.min_rtt > rtt.min() {
            self.min_rtt = rtt.min();
        }
    }
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        if !self.config.delivery_rate_sampling {
            self.max_sent_packet_number = last_packet_number;
            self.max_bandwidth.on_sent(now, bytes);
        }
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.max_bandwidth
            .on_ack(now, sent, bytes, self.round_count, app_limited);
        self.acked_bytes += bytes;
        if self.is_min_rtt_expired(now, app_limited) || self.min_rtt > rtt.min() {
            self.min_rtt = rtt.min();
        }
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        let app_limited = if self.config.delivery_rate_sampling {
            self.max_bandwidth
                .finish_delivery_sample(now, self.round_count, self.min_rtt)
                .unwrap_or(true)
        } else {
            app_limited
        };
        let bytes_acked = self.max_bandwidth.bytes_acked_this_window();
        let excess_acked = self.ack_aggregation.update_ack_aggregation_bytes(
            bytes_acked,
            now,
            self.round_count,
            self.max_bandwidth.get_estimate(),
        );
        self.max_bandwidth.end_acks(self.round_count, app_limited);
        if let Some(largest_acked_packet) =
            largest_packet_num_acked.filter(|_| !self.config.delivery_rate_sampling)
        {
            self.max_acked_packet_number = largest_acked_packet;
        }

        let mut is_round_start = false;
        if bytes_acked > 0 {
            is_round_start =
                self.max_acked_packet_number > self.current_round_trip_end_packet_number;
            if is_round_start {
                self.current_round_trip_end_packet_number = self.max_sent_packet_number;
                self.round_count += 1;
            }
        }

        self.update_recovery_state(is_round_start);

        if self.mode == Mode::ProbeBw {
            self.update_gain_cycle_phase(now, in_flight);
        }

        if is_round_start && !self.is_at_full_bandwidth {
            self.check_if_full_bw_reached(app_limited);
        }

        self.maybe_exit_startup_or_drain(now, in_flight);

        self.maybe_enter_or_exit_probe_rtt(now, is_round_start, in_flight, app_limited);

        // After the model is updated, recalculate the pacing rate and congestion window.
        self.calculate_pacing_rate();
        self.calculate_cwnd(bytes_acked, excess_acked);
        self.calculate_recovery_window(bytes_acked, self.loss_state.lost_bytes, in_flight);

        self.prev_in_flight_count = in_flight;
        self.loss_state.reset();
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.loss_state.lost_bytes += lost_bytes;
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.current_mtu = new_mtu as u64;
        self.min_cwnd = calculate_min_window(self.current_mtu);
        self.init_cwnd = self.config.initial_window.max(self.min_cwnd);
        self.cwnd = self.cwnd.max(self.min_cwnd);
    }

    fn window(&self) -> u64 {
        if self.mode == Mode::ProbeRtt {
            return self.get_probe_rtt_cwnd();
        } else if self.recovery_state.in_recovery() && self.mode != Mode::Startup {
            return self.cwnd.min(self.recovery_window);
        }
        self.cwnd
    }

    fn pacing_rate(&self) -> Option<std::num::NonZeroU64> {
        if self.config.delivery_rate_sampling {
            std::num::NonZeroU64::new(self.pacing_rate)
        } else {
            None
        }
    }

    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: Some(self.pacing_rate.saturating_mul(8)),
        }
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config.initial_window
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Configuration for the [`Bbr`] congestion controller
#[derive(Debug, Clone)]
pub struct BbrConfig {
    initial_window: u64,
    delivery_rate_sampling: bool,
}

impl BbrConfig {
    /// Opt in to delivery snapshots, bounded Startup growth and controller-rate pacing.
    /// Disabled by default to preserve upstream controller behavior.
    pub fn delivery_rate_sampling(&mut self, enabled: bool) -> &mut Self {
        self.delivery_rate_sampling = enabled;
        self
    }
    /// Default limit on the amount of outstanding data in bytes.
    ///
    /// Recommended value: `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_window = value;
        self
    }
}

impl Default for BbrConfig {
    fn default() -> Self {
        Self {
            initial_window: K_MAX_INITIAL_CONGESTION_WINDOW * BASE_DATAGRAM_SIZE,
            delivery_rate_sampling: false,
        }
    }
}

impl ControllerFactory for BbrConfig {
    fn build(self: Arc<Self>, _now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Bbr::new(self, current_mtu))
    }
}

#[derive(Debug, Default, Copy, Clone)]
struct AckAggregationState {
    max_ack_height: MinMax,
    aggregation_epoch_start_time: Option<Instant>,
    aggregation_epoch_bytes: u64,
}

impl AckAggregationState {
    fn update_ack_aggregation_bytes(
        &mut self,
        newly_acked_bytes: u64,
        now: Instant,
        round: u64,
        max_bandwidth: u64,
    ) -> u64 {
        // Compute how many bytes are expected to be delivered, assuming max
        // bandwidth is correct.
        let expected_bytes_acked = max_bandwidth
            * now
                .saturating_duration_since(self.aggregation_epoch_start_time.unwrap_or(now))
                .as_micros() as u64
            / 1_000_000;

        // Reset the current aggregation epoch as soon as the ack arrival rate is
        // less than or equal to the max bandwidth.
        if self.aggregation_epoch_bytes <= expected_bytes_acked {
            // Reset to start measuring a new aggregation epoch.
            self.aggregation_epoch_bytes = newly_acked_bytes;
            self.aggregation_epoch_start_time = Some(now);
            return 0;
        }

        // Compute how many extra bytes were delivered vs max bandwidth.
        // Include the bytes most recently acknowledged to account for stretch acks.
        self.aggregation_epoch_bytes += newly_acked_bytes;
        let diff = self.aggregation_epoch_bytes - expected_bytes_acked;
        self.max_ack_height.update_max(round, diff);
        diff
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Mode {
    // Startup phase of the connection.
    Startup,
    // After achieving the highest possible bandwidth during the startup, lower
    // the pacing rate in order to drain the queue.
    Drain,
    // Cruising mode.
    ProbeBw,
    // Temporarily slow down sending in order to empty the buffer and measure
    // the real minimum RTT.
    ProbeRtt,
}

// Indicates how the congestion control limits the amount of bytes in flight.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RecoveryState {
    // Do not limit.
    NotInRecovery,
    // Allow an extra outstanding byte for each byte acknowledged.
    Conservation,
    // Allow two extra outstanding bytes for each byte acknowledged (slow
    // start).
    Growth,
}

impl RecoveryState {
    pub(super) fn in_recovery(&self) -> bool {
        !matches!(self, Self::NotInRecovery)
    }
}

#[derive(Debug, Clone, Default)]
struct LossState {
    lost_bytes: u64,
}

impl LossState {
    pub(super) fn reset(&mut self) {
        self.lost_bytes = 0;
    }

    pub(super) fn has_losses(&self) -> bool {
        self.lost_bytes != 0
    }
}

fn calculate_min_window(current_mtu: u64) -> u64 {
    4 * current_mtu
}

// The gain used for the STARTUP, equal to 2/ln(2).
const K_DEFAULT_HIGH_GAIN: f32 = 2.885;
// The newly derived CWND gain for STARTUP, 2.
const K_DERIVED_HIGH_CWNDGAIN: f32 = 2.0;
// The cycle of gains used during the ProbeBw stage.
const K_PACING_GAIN: [f32; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];

const K_STARTUP_GROWTH_TARGET: f32 = 1.25;
const K_ROUND_TRIPS_WITHOUT_GROWTH_BEFORE_EXITING_STARTUP: u8 = 3;

// Do not allow initial congestion window to be greater than 200 packets.
const K_MAX_INITIAL_CONGESTION_WINDOW: u64 = 200;

const PROBE_RTT_BASED_ON_BDP: bool = true;
const DRAIN_TO_TARGET: bool = true;

#[cfg(test)]
mod delivery_model_tests {
    use super::*;
    // Break caught: Startup compares a dimensionless gain against bytes and
    // keeps increasing an application-limited window beyond its model target.
    #[test]
    fn startup_window_stops_growing_after_its_bandwidth_target() {
        let mut config = BbrConfig::default();
        config.delivery_rate_sampling(true);
        let mut bbr = Bbr::new(Arc::new(config), 1200);
        bbr.min_rtt = Duration::from_millis(200);
        bbr.max_bandwidth.on_sent(Instant::now(), 1000);
        bbr.cwnd = 1_000_000;
        let before = bbr.cwnd;
        bbr.acked_bytes = bbr.init_cwnd;
        bbr.calculate_cwnd(1200, 0);
        assert_eq!(bbr.cwnd, before);
    }
    // Break caught: minRTT*bandwidth overflows before scaling to bytes.
    #[test]
    fn target_window_uses_wide_arithmetic() {
        let mut config = BbrConfig::default();
        config.delivery_rate_sampling(true);
        let mut bbr = Bbr::new(Arc::new(config), 1200);
        bbr.min_rtt = Duration::from_secs(2);
        // One-byte ACK pair with1ns spacing yields1GB/s in legacy sampling;
        // seed an otherwise valid very high model without private mutation.
        let t = Instant::now();
        bbr.max_bandwidth.on_sent(t, 1_000_000_000);
        bbr.max_bandwidth.on_ack(t, t, 1_000_000_000, 0, false);
        bbr.max_bandwidth
            .on_sent(t + Duration::from_nanos(1), 1_000_000_000);
        bbr.max_bandwidth
            .on_ack(t + Duration::from_nanos(1), t, 1_000_000_000, 0, false);
        assert_eq!(bbr.get_target_cwnd(1.0), 2_000_000_000_000_000_000);
    }
    // Break caught: the FXLink opt-in never exports its measured byte rate,
    // exports bits/s, enables old/default BBR, or exports a zero rate.
    #[test]
    fn only_opted_in_bbr_exports_a_nonzero_byte_rate() {
        let mut bbr = corrected();
        assert_eq!(bbr.pacing_rate(), None);
        bbr.pacing_rate = 64_000;
        assert_eq!(bbr.pacing_rate().map(|v| v.get()), Some(64_000));
        let mut legacy = Bbr::new(Arc::new(BbrConfig::default()), 1200);
        legacy.pacing_rate = 64_000;
        assert_eq!(legacy.pacing_rate(), None);
    }
    // Break caught: exporting a large byte rate overflows qlog's bits/s metric.
    #[test]
    fn pacing_metric_conversion_saturates_at_bits_per_second_limit() {
        let mut bbr = corrected();
        bbr.pacing_rate = u64::MAX;
        assert_eq!(bbr.metrics().pacing_rate, Some(u64::MAX));
    }
    fn corrected() -> Bbr {
        let mut config = BbrConfig::default();
        config.initial_window(12_000).delivery_rate_sampling(true);
        Bbr::new(Arc::new(config), 1200)
    }
    // Break caught: inferred/snapshotless ACKs credit bytes without delivery.
    #[test]
    fn missing_snapshot_cannot_credit_an_inferred_ack() {
        let mut bbr = corrected();
        let t = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(100));
        bbr.on_packet_sent(t, 0).unwrap();
        bbr.on_ack_with_snapshot(t + Duration::from_millis(100), t, 1200, false, &rtt, None);
        bbr.on_end_acks(t + Duration::from_millis(100), 0, false, Some(999));
        assert_eq!(bbr.acked_bytes, 0);
        assert_eq!(bbr.max_bandwidth.get_estimate(), 0);
        assert_eq!(bbr.round_count, 0);
    }
    // Break caught: a new packet-number space rewinds the round fence, or a
    // same-time packet batch loses its latest delivered-byte snapshot.
    #[test]
    fn delivery_rounds_ignore_packet_number_space_and_handle_equal_send_times() {
        let mut bbr = corrected();
        let t = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(100));
        let first = bbr.on_packet_sent(t, 0).unwrap();
        bbr.on_sent(t, 1200, u64::MAX);
        bbr.on_ack_with_snapshot(
            t + Duration::from_millis(100),
            t,
            1200,
            false,
            &rtt,
            Some(first),
        );
        bbr.on_end_acks(t + Duration::from_millis(100), 0, false, Some(u64::MAX));
        assert_eq!(bbr.round_count, 1);
        assert_eq!(bbr.max_bandwidth.get_estimate(), 12_000);
        let at = t + Duration::from_millis(100);
        let a = bbr.on_packet_sent(at, 0).unwrap();
        let b = bbr.on_packet_sent(at, 1200).unwrap();
        bbr.on_sent(at, 2400, 0);
        bbr.on_ack_with_snapshot(
            at + Duration::from_millis(100),
            at,
            1200,
            false,
            &rtt,
            Some(a),
        );
        bbr.on_ack_with_snapshot(
            at + Duration::from_millis(100),
            at,
            1200,
            false,
            &rtt,
            Some(b),
        );
        bbr.on_end_acks(at + Duration::from_millis(100), 0, false, Some(0));
        assert_eq!(bbr.round_count, 2);
        assert_eq!(bbr.max_bandwidth.get_estimate(), 24_000);
    }
    // Break caught: a NAT-rebound controller clone carries a partial old ACK
    // batch or application-limited marker into a new generation's flight.
    #[test]
    fn cloned_path_resets_transient_sampling_and_its_sequence_fence() {
        let mut bbr = corrected();
        let t = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(100));
        let packet = bbr.on_packet_sent(t, 0).unwrap();
        bbr.on_application_limited(1200);
        bbr.on_ack_with_snapshot(
            t + Duration::from_millis(100),
            t,
            1200,
            false,
            &rtt,
            Some(packet),
        );
        let mut rebound = bbr.clone();
        rebound.on_path_change();
        rebound.on_end_acks(t + Duration::from_millis(101), 0, false, Some(0));
        assert_eq!(rebound.max_bandwidth.get_estimate(), 0);
        assert_eq!(rebound.max_sent_packet_number, packet.sequence);
        let at = t + Duration::from_secs(1);
        let p = rebound.on_packet_sent(at, 0).unwrap();
        rebound.on_ack_with_snapshot(
            at + Duration::from_millis(100),
            at,
            1200,
            false,
            &rtt,
            Some(p),
        );
        rebound.on_end_acks(at + Duration::from_millis(100), 0, false, Some(0));
        assert_eq!(rebound.max_bandwidth.get_estimate(), 12_000);
        assert_eq!(rebound.round_count, 1);
        assert!(!rebound.max_bandwidth.last_sample.unwrap().limited);
    }
    // Break caught: a recovering clone resets send sequence while retaining a
    // much larger old recovery fence, keeping the new path in recovery.
    #[test]
    fn recovering_clone_can_exit_recovery_on_its_first_new_flight() {
        let mut bbr = corrected();
        let t = Instant::now();
        for _ in 0..1000 {
            bbr.on_packet_sent(t, 1200).unwrap();
        }
        bbr.recovery_state = RecoveryState::Growth;
        bbr.end_recovery_at_packet_number = bbr.max_sent_packet_number;
        bbr.current_round_trip_end_packet_number = bbr.max_sent_packet_number;
        let mut rebound = bbr.clone();
        rebound.on_path_change();
        let at = t + Duration::from_secs(1);
        let packet = rebound.on_packet_sent(at, 0).unwrap();
        rebound.on_ack_with_snapshot(
            at + Duration::from_millis(100),
            at,
            1200,
            false,
            &RttEstimator::new(Duration::from_millis(100)),
            Some(packet),
        );
        rebound.on_end_acks(at + Duration::from_millis(100), 0, false, Some(0));
        assert_eq!(rebound.recovery_state, RecoveryState::NotInRecovery);
    }
}

#[cfg(test)]
mod demand_tests;

#[cfg(test)]
mod startup_bootstrap_tests {
    use super::*;

    fn controller(sampled: bool) -> Bbr {
        let mut config = BbrConfig::default();
        config
            .initial_window(12_000)
            .delivery_rate_sampling(sampled);
        Bbr::new(Arc::new(config), 1200)
    }

    fn ack_flight(bbr: &mut Bbr, sent: Instant, packets: u64) {
        let rtt = RttEstimator::new(Duration::from_millis(200));
        let snapshots: Vec<_> = (0..packets)
            .map(|index| {
                let snapshot = bbr.on_packet_sent(sent, index * 1200);
                bbr.on_sent(sent, 1200, index);
                snapshot
            })
            .collect();
        let acked = sent + Duration::from_millis(200);
        for snapshot in snapshots {
            bbr.on_ack_with_snapshot(acked, sent, 1200, false, &rtt, snapshot);
        }
        bbr.on_end_acks(acked, 0, false, Some(packets - 1));
    }

    // Break caught: a small application-limited handshake ACK initializes the
    // opted-in pacer at unit gain instead of probing with the startup gain.
    #[test]
    fn first_handshake_ack_uses_startup_gain_without_inflating_initial_window() {
        let mut bbr = controller(true);
        assert_eq!(bbr.initial_window(), 12_000);
        assert_eq!(bbr.pacing_rate(), None);
        bbr.on_application_limited(0);
        ack_flight(&mut bbr, Instant::now(), 1);
        assert_eq!(bbr.max_bandwidth.get_estimate(), 6000);
        // floor(12,000 / 0.2 * the existing f32 gain 2.885), in bytes/s.
        assert_eq!(bbr.pacing_rate().unwrap().get(), 173_099);
        assert_eq!(bbr.initial_window(), 12_000);
        assert_eq!(bbr.acked_bytes, 1200);
    }

    // Break caught: the first limited handshake observation leaves minRTT at
    // zero, bypassing the initial-window bootstrap on the media transition.
    #[test]
    fn tracked_limited_handshake_initializes_rtt_and_its_age() {
        let mut bbr = controller(true);
        let sent = Instant::now();
        bbr.on_application_limited(0);
        ack_flight(&mut bbr, sent, 1);
        assert_eq!(bbr.min_rtt, Duration::from_millis(200));
        assert_eq!(
            bbr.probe_rtt_last_started_at,
            Some(sent + Duration::from_millis(200))
        );
        assert_eq!(bbr.mode, Mode::Startup);
    }

    // Break caught: treating the uninitialized RTT-age timestamp as already
    // expired forces a cold connection into ProbeRtt instead of Startup.
    #[test]
    fn first_full_flight_does_not_probe_an_unaged_rtt() {
        let mut bbr = controller(true);
        ack_flight(&mut bbr, Instant::now(), 10);
        assert_eq!(bbr.mode, Mode::Startup);
        assert_eq!(bbr.window(), 24_000);
    }

    // Break caught: fixing first-use initialization accidentally disables the
    // existing ten-second aging rule on a later ordinary tracked ACK.
    #[test]
    fn initialized_rtt_still_enters_probe_after_its_age_expires() {
        let mut bbr = controller(true);
        let sent = Instant::now();
        bbr.on_application_limited(0);
        ack_flight(&mut bbr, sent, 1);
        ack_flight(&mut bbr, sent + Duration::from_secs(11), 1);
        assert_eq!(bbr.mode, Mode::ProbeRtt);
    }

    // Break caught: a later application-limited ACK overwrites startup credit
    // with its smaller delivery estimate, delaying the media transition again.
    #[test]
    fn lower_application_limited_sample_cannot_reduce_bootstrapped_rate() {
        let mut bbr = controller(true);
        let sent = Instant::now();
        bbr.on_application_limited(0);
        ack_flight(&mut bbr, sent, 1);
        let before = bbr.pacing_rate().unwrap().get();
        bbr.on_application_limited(0);
        ack_flight(&mut bbr, sent + Duration::from_millis(200), 1);
        assert!(bbr.pacing_rate().unwrap().get() >= before);
        assert_eq!(bbr.acked_bytes, 2400);
    }

    // Break caught: treating the initial-window estimate as a permanent cap
    // prevents ordinary growth after a fully used initial and doubled flight.
    #[test]
    fn measured_media_flight_can_raise_rate_above_initial_bootstrap() {
        let mut bbr = controller(true);
        let sent = Instant::now();
        ack_flight(&mut bbr, sent, 10);
        assert_eq!(bbr.window(), 24_000);
        ack_flight(&mut bbr, sent + Duration::from_millis(200), 20);
        assert_eq!(bbr.max_bandwidth.get_estimate(), 120_000);
        assert_eq!(bbr.pacing_rate().unwrap().get(), 346_199);
        assert_eq!(bbr.initial_window(), 12_000);
        assert_eq!(bbr.acked_bytes, 36_000);
    }

    // Break caught: changing default/legacy BBR while correcting the explicit
    // controller-rate variant, including exposing a byte rate to its pacer.
    #[test]
    fn legacy_bootstrap_retains_unit_gain_and_window_derived_pacing() {
        let mut bbr = controller(false);
        let sent = Instant::now();
        ack_flight(&mut bbr, sent, 1);
        ack_flight(&mut bbr, sent + Duration::from_millis(200), 1);
        assert_eq!(bbr.pacing_rate, 60_000);
        assert_eq!(bbr.pacing_rate(), None);
        assert_eq!(bbr.initial_window(), 12_000);
    }
}
