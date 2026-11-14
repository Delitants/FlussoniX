//! Real QUIC packet-path idle classification; simulated transport, no OS UDP.
use super::*;
use crate::congestion::{Controller, ControllerFactory};
use std::{
    num::NonZeroU64,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
#[derive(Clone)]
struct Probe {
    window: u64,
    rate: u64,
    marks: Arc<Mutex<Vec<u64>>>,
    sent: Arc<AtomicU64>,
    opted_in: bool,
}
impl Controller for Probe {
    fn application_demand(&self) -> Option<crate::congestion::ApplicationDemand> {
        self.opted_in
            .then(crate::congestion::ApplicationDemand::default)
    }
    fn on_application_limited(&mut self, flight: u64) {
        self.marks.lock().unwrap().push(flight);
    }
    fn on_sent(&mut self, _: Instant, bytes: u64, _: u64) {
        self.sent.fetch_add(bytes, Ordering::Relaxed);
    }
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        self.window
    }
    fn initial_window(&self) -> u64 {
        self.window
    }
    fn pacing_rate(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.rate)
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
impl ControllerFactory for Probe {
    fn build(self: Arc<Self>, _: Instant, _: u16) -> Box<dyn Controller> {
        Box::new((*self).clone())
    }
}
fn probe(label: &str, window: u64, rate: u64, queued: usize, opted_in: bool) {
    let marks = Arc::new(Mutex::new(Vec::new()));
    let sent = Arc::new(AtomicU64::new(0));
    let mut transport = TransportConfig::default();
    transport
        .congestion_controller_factory(Arc::new(Probe {
            window,
            opted_in,
            rate,
            marks: marks.clone(),
            sent: sent.clone(),
        }))
        .mtu_discovery_config(None)
        .datagram_send_buffer_size(128 * 1024);
    let mut config = client_config();
    config.transport_config(Arc::new(transport));
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(100);
    let (client, _) = pair.connect_with(config);
    marks.lock().unwrap().clear();
    sent.store(0, Ordering::Relaxed);
    for _ in 0..queued {
        pair.client_datagrams(client)
            .send(Bytes::from(vec![0; 1000]), false)
            .unwrap();
    }
    pair.drive_client();
    let calls = marks.lock().unwrap().clone();
    let before = sent.load(Ordering::Relaxed);
    assert!(before > 0);
    pair.client_datagrams(client)
        .send(Bytes::from(vec![0; 1000]), false)
        .unwrap();
    pair.drive_client();
    let after = sent.load(Ordering::Relaxed);
    if label == "available-idle" {
        assert!(!calls.is_empty());
        assert!(after > before);
    } else {
        assert_eq!(after, before);
    }
    if label.starts_with("backlogged-") || (opted_in && label.starts_with("finite-")) {
        assert!(
            calls.is_empty(),
            "QUIC FIFO drained while the next normal packet is blocked: {calls:?}"
        );
    } else {
        assert!(
            !calls.is_empty(),
            "true idle or unchanged default controller"
        );
    }
}
#[test]
fn finite_cwnd_boundary() {
    probe("finite-cwnd", 4800, 0, 4, true);
}
#[test]
fn finite_pacing_boundary() {
    probe("finite-pacing", 512000, 64000, 11, true);
}
#[test]
fn genuine_idle_with_available_credit() {
    probe("available-idle", 512000, u64::MAX, 1, true);
}
#[test]
fn continuously_queued_cwnd_control() {
    probe("backlogged-cwnd", 4800, 0, 64, true);
}
#[test]
fn continuously_queued_pacing_control() {
    probe("backlogged-pacing", 512000, 64000, 64, true);
}

#[test]
fn default_controller_cwnd_boundary_is_unchanged() {
    probe("finite-cwnd", 4800, 0, 4, false);
}
#[test]
fn default_controller_pacing_boundary_is_unchanged() {
    probe("finite-pacing", 512000, 64000, 11, false);
}
