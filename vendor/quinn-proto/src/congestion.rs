//! Logic for controlling the rate at which data is sent

use crate::Instant;
use crate::connection::RttEstimator;
use std::any::Any;
use std::num::NonZeroU64;
use std::sync::Arc;

mod bbr;
mod cubic;
mod demand;
mod new_reno;

pub use bbr::{Bbr, BbrConfig, DeliverySnapshot};
pub use cubic::{Cubic, CubicConfig};
pub use demand::{ApplicationData, ApplicationDemand, DemandRegistration, ReadyData};
pub use new_reno::{NewReno, NewRenoConfig};

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// Snapshot delivery history for one tracked ack-eliciting packet.
    fn on_packet_sent(&mut self, _now: Instant, _in_flight: u64) -> Option<DeliverySnapshot> {
        None
    }
    /// Optional connection-local upstream original demand; None preserves the
    /// controller's existing idle classification.
    fn application_demand(&self) -> Option<ApplicationDemand> {
        None
    }
    /// Application queues actually drained without transport-side blocking.
    fn on_application_limited(&mut self, _in_flight: u64) {}
    /// Reset transient packet sampling after cloning a path's controller.
    fn on_path_change(&mut self) {}
    /// Explicit, current-path ACK with its original delivery history, if present.
    fn on_ack_with_snapshot(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
        _packet: Option<DeliverySnapshot>,
    ) {
        self.on_ack(now, sent, bytes, app_limited, rtt);
    }
    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}

    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    );

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Optional transmit pacing rate in bytes per second, independent of the
    /// congestion window and telemetry's bits/s. `None` retains window-derived
    /// pacing. Congestion-window admission remains mandatory for paced data.
    fn pacing_rate(&self) -> Option<NonZeroU64> {
        None
    }

    /// Retrieve implementation-specific metrics used to populate `qlog` traces when they are enabled
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
        }
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Common congestion controller metrics
#[derive(Default)]
#[non_exhaustive]
pub struct ControllerMetrics {
    /// Congestion window (bytes)
    pub congestion_window: u64,
    /// Slow start threshold (bytes)
    pub ssthresh: Option<u64>,
    /// Pacing rate (bits/s)
    pub pacing_rate: Option<u64>,
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;
