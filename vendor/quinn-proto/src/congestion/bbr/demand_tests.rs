use super::*;
use crate::congestion::ApplicationData;
struct Queued;
impl ApplicationData for Queued {
    fn pending_records(&self) -> Option<bool> {
        Some(true)
    }
}

fn bbr() -> Bbr {
    let mut c = BbrConfig::default();
    c.delivery_rate_sampling(true);
    Bbr::new(Arc::new(c), 1200)
}
#[test]
fn admission_blocked_originals_do_not_start_an_application_limited_bubble() {
    let mut bbr = bbr();
    let sub = bbr
        .application_demand()
        .unwrap()
        .register(Arc::new(Queued))
        .unwrap();
    sub.set_admission_paused(true);
    bbr.on_application_limited(1200);
    assert!(!bbr.on_packet_sent(Instant::now(), 1200).unwrap().limited);
    sub.set_admission_paused(false);
    bbr.on_application_limited(1200);
    assert!(
        bbr.on_packet_sent(Instant::now(), 1200).unwrap().limited,
        "CPU/producer wait remains idle"
    );
}
#[test]
fn demand_preserves_preexisting_limited_history_until_delivery_flushes_it() {
    let mut bbr = bbr();
    let t = Instant::now();
    let rtt = RttEstimator::new(Duration::from_millis(100));
    bbr.on_application_limited(0);
    let sub = bbr
        .application_demand()
        .unwrap()
        .register(Arc::new(Queued))
        .unwrap();
    let ready = sub.ready_data();
    bbr.on_application_limited(0);
    let old = bbr.on_packet_sent(t, 0).unwrap();
    assert!(old.limited);
    bbr.on_ack_with_snapshot(
        t + Duration::from_millis(100),
        t,
        1200,
        false,
        &rtt,
        Some(old),
    );
    bbr.on_end_acks(t + Duration::from_millis(100), 0, false, Some(0));
    assert!(
        bbr.max_bandwidth.last_sample.unwrap().limited,
        "never relabel an old packet"
    );
    bbr.on_application_limited(0);
    assert!(
        !bbr.on_packet_sent(t + Duration::from_millis(101), 0)
            .unwrap()
            .limited,
        "known ready data must not reopen bubble"
    );
    drop(ready);
    bbr.on_application_limited(0);
    assert!(
        bbr.on_packet_sent(t + Duration::from_millis(102), 0)
            .unwrap()
            .limited
    );
}
#[test]
fn path_clones_share_live_demand_but_new_connections_and_default_bbr_do_not() {
    let bbr = bbr();
    let sub = bbr
        .application_demand()
        .unwrap()
        .register(Arc::new(Queued))
        .unwrap();
    sub.set_admission_paused(true);
    let mut path = bbr.clone();
    path.on_path_change();
    path.on_application_limited(0);
    assert!(!path.on_packet_sent(Instant::now(), 0).unwrap().limited);
    let mut fresh = Bbr::new(bbr.config.clone(), 1200);
    fresh.on_application_limited(0);
    assert!(fresh.on_packet_sent(Instant::now(), 0).unwrap().limited);
    drop(sub);
    path.on_application_limited(0);
    assert!(path.on_packet_sent(Instant::now(), 0).unwrap().limited);
    assert!(
        Bbr::new(Arc::new(BbrConfig::default()), 1200)
            .application_demand()
            .is_none()
    );
}

#[test]
fn invalid_source_snapshot_fences_even_a_held_encoded_guard() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Source(Arc<AtomicBool>);
    impl ApplicationData for Source {
        fn pending_records(&self) -> Option<bool> {
            self.0.load(Ordering::Acquire).then_some(false)
        }
    }
    let mut bbr = bbr();
    let active = Arc::new(AtomicBool::new(true));
    let sub = bbr
        .application_demand()
        .unwrap()
        .register(Arc::new(Source(active.clone())))
        .unwrap();
    let _ready = sub.ready_data();
    bbr.on_application_limited(1200);
    assert!(!bbr.on_packet_sent(Instant::now(), 1200).unwrap().limited);
    active.store(false, Ordering::Release);
    bbr.on_application_limited(1200);
    assert!(
        bbr.on_packet_sent(Instant::now(), 1200).unwrap().limited,
        "invalid source cannot keep the controller busy"
    );
}
