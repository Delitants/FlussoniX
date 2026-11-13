//! Encoded QUIC transmission in Pair's simulated protocol path, not OS UDP.
use super::*;
use crate::congestion::{Controller, ControllerFactory, ControllerMetrics};
use std::{
    num::NonZeroU64,
    sync::atomic::{AtomicU64, Ordering},
};
#[derive(Clone)]
struct Fixed {
    rate: Arc<AtomicU64>,
    window: u64,
}
impl Controller for Fixed {
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        self.window
    }
    fn initial_window(&self) -> u64 {
        self.window
    }
    fn pacing_rate(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.rate.load(Ordering::Relaxed))
    }
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window,
            ssthresh: None,
            pacing_rate: Some(64_000 * 8),
        }
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
impl ControllerFactory for Fixed {
    fn build(self: Arc<Self>, _: Instant, _: u16) -> Box<dyn Controller> {
        Box::new((*self).clone())
    }
}
fn flight(rate: u64, window: u64, change: Option<u64>) -> Duration {
    let speed = Arc::new(AtomicU64::new(rate));
    let mut t = TransportConfig::default();
    t.congestion_controller_factory(Arc::new(Fixed {
        rate: speed.clone(),
        window,
    }));
    t.mtu_discovery_config(None);
    let mut client = client_config();
    client.transport_config(Arc::new(t));
    let mut p = Pair::default();
    p.latency = Duration::from_millis(10);
    let (client, server) = p.connect_with(client);
    for index in 0..64u8 {
        p.client_datagrams(client)
            .send(Bytes::from(vec![index; 1000]), false)
            .unwrap();
    }
    let start = p.time;
    if let Some(rate) = change {
        p.drive_client();
        p.time += Duration::from_millis(1);
        speed.store(rate, Ordering::Relaxed);
        p.drive_client();
    }
    let mut received = 0;
    for _ in 0..20_000 {
        assert!(p.step(), "queued flight unexpectedly idle");
        while let Some(packet) = p.server_datagrams(server).recv() {
            assert_eq!(packet, Bytes::from(vec![received as u8; 1000]));
            received += 1;
        }
        if received == 64 {
            return p.time - start;
        }
    }
    panic!("flight did not complete")
}
// Break caught: Controller's explicit pacing input never reaches the scheduler,
// even though metrics report the correct value and cwnd permits the whole flight.
#[test]
fn explicit_controller_rate_limits_encoded_datagram_emission() {
    let elapsed = flight(64_000, 512_000, None);
    println!("controlled_us={}", elapsed.as_micros());
    assert!(elapsed >= Duration::from_millis(700));
    assert!(elapsed < Duration::from_millis(1600));
}
// Break caught: telemetry is accidentally used as an opt-in rate, changing
// controllers that deliberately keep the existing window-derived pacer.
#[test]
fn telemetry_alone_preserves_window_derived_pacing() {
    assert!(flight(0, 512_000, None) < Duration::from_millis(100));
}
// Break caught: using the pacing input as an admission window bypasses cwnd.
#[test]
fn high_rate_cannot_bypass_the_congestion_window() {
    assert!(flight(u64::MAX, 4800, None) >= Duration::from_millis(200));
}
// Break caught: a previously scheduled wait or cached controller rate prevents
// an I/O-triggered poll from applying a faster rate before the old deadline.
#[test]
fn rate_increase_replaces_an_outstanding_wait() {
    assert!(flight(64_000, 512_000, Some(6_400_000)) < Duration::from_millis(150));
}
// Break caught: a rate decrease leaves the old fast schedule/credit active.
#[test]
fn rate_decrease_slows_an_already_queued_flight() {
    assert!(flight(6_400_000, 512_000, Some(64_000)) >= Duration::from_millis(600));
}
