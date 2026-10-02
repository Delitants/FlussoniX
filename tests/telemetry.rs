use flussonix::telemetry::Sampler;
use std::{
    path::Path,
    time::{Duration, Instant},
};
fn fixture() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(d.path().join("proc/net")).unwrap();
    std::fs::create_dir_all(d.path().join("sys/class/net/wan0/statistics")).unwrap();
    std::fs::write(d.path().join("proc/net/route"),"Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nwan0 00000000 0100000A 0003 0 0 10 00000000 0 0 0\n").unwrap();
    write(d.path(), 1000, 100, 80);
    std::fs::write(
        d.path().join("proc/meminfo"),
        "MemTotal: 1000 kB\nMemAvailable: 750 kB\n",
    )
    .unwrap();
    d
}
fn write(d: &Path, bytes: u64, total: u64, idle: u64) {
    std::fs::write(
        d.join("sys/class/net/wan0/statistics/tx_bytes"),
        bytes.to_string(),
    )
    .unwrap();
    std::fs::write(
        d.join("proc/stat"),
        format!("cpu {} 0 0 {idle} 0 0 0 0\n", total - idle),
    )
    .unwrap();
}
#[test]
fn interface_counts_other_process_traffic_separately_from_http_egress() {
    let d = fixture();
    let sampler = Sampler::with_roots("auto", d.path().join("proc"), d.path().join("sys")).unwrap();
    let t = Instant::now();
    sampler.sample_at(t, 10);
    assert!(sampler.snapshot_at(t, 1000.0)["uplink"].is_null());
    write(d.path(), 1_001_000, 200, 150);
    sampler.sample_at(t + Duration::from_secs(1), 100_010);
    let v = sampler.snapshot_at(t + Duration::from_secs(1), 1000.0);
    assert_eq!(v["uplink_interface"], "wan0");
    assert_eq!(v["uplink_source"], "interface");
    assert_eq!(v["egress_mbps"], 8.0);
    assert_eq!(v["http_egress_mbps"], 0.8);
    assert_eq!(v["ram"], 0.25);
    assert!((v["cpu"].as_f64().unwrap() - 0.3).abs() < 1e-9);
}
#[test]
fn reset_missing_and_stale_interface_are_unknown_until_a_new_interval() {
    let d = fixture();
    let sampler = Sampler::with_roots("wan0", d.path().join("proc"), d.path().join("sys")).unwrap();
    let t = Instant::now();
    sampler.sample_at(t, 10);
    write(d.path(), 2000, 200, 150);
    sampler.sample_at(t + Duration::from_secs(1), 20);
    assert!(sampler.snapshot_at(t + Duration::from_secs(1), 1000.0)["uplink"].is_number());
    assert!(sampler.snapshot_at(t + Duration::from_secs(6), 1000.0)["uplink"].is_null());
    write(d.path(), 10, 300, 220);
    sampler.sample_at(t + Duration::from_secs(2), 30);
    assert!(sampler.snapshot_at(t + Duration::from_secs(2), 1000.0)["uplink"].is_null());
    std::fs::remove_file(d.path().join("sys/class/net/wan0/statistics/tx_bytes")).unwrap();
    sampler.sample_at(t + Duration::from_secs(3), 40);
    assert!(sampler.snapshot_at(t + Duration::from_secs(3), 1000.0)["uplink"].is_null());
    write(d.path(), 1000, 400, 300);
    sampler.sample_at(t + Duration::from_secs(4), 50);
    assert!(sampler.snapshot_at(t + Duration::from_secs(4), 1000.0)["uplink"].is_null());
    write(d.path(), 2000, 500, 370);
    sampler.sample_at(t + Duration::from_secs(5), 60);
    assert!(sampler.snapshot_at(t + Duration::from_secs(5), 1000.0)["uplink"].is_number());
}
#[test]
fn explicit_interface_is_validated_and_process_mode_is_labelled() {
    let d = fixture();
    for invalid in ["../wan0", "/etc/passwd", "missing", "", "wan0/../wan0"] {
        assert!(Sampler::with_roots(invalid, d.path().join("proc"), d.path().join("sys")).is_err());
    }
    let sampler =
        Sampler::with_roots("process", d.path().join("proc"), d.path().join("sys")).unwrap();
    let t = Instant::now();
    sampler.sample_at(t, 0);
    write(d.path(), 1_000_000, 200, 150);
    sampler.sample_at(t + Duration::from_secs(1), 100_000);
    let v = sampler.snapshot_at(t + Duration::from_secs(1), 1000.0);
    assert_eq!(v["uplink_source"], "process");
    assert!(v["uplink_interface"].is_null());
    assert_eq!(v["egress_mbps"], 0.8);
}
#[test]
fn rtsp_output_is_separate_and_included_in_process_capacity() {
    let d = fixture();
    let sampler =
        Sampler::with_roots("process", d.path().join("proc"), d.path().join("sys")).unwrap();
    let t = Instant::now();
    sampler.sample_media_at(t, 100, 200);
    sampler.sample_media_at(t + Duration::from_secs(1), 100100, 250200);
    let v = sampler.snapshot_at(t + Duration::from_secs(1), 1000.0);
    assert_eq!(v["http_egress_mbps"], 0.8);
    assert_eq!(v["rtsp_egress_mbps"], 2.0);
    assert_eq!(v["egress_mbps"], 2.8);
    assert_eq!(v["bytes_out"], 350300);
}
#[test]
fn reset_of_either_media_counter_invalidates_process_capacity() {
    let d = fixture();
    let sampler =
        Sampler::with_roots("process", d.path().join("proc"), d.path().join("sys")).unwrap();
    let t = Instant::now();
    sampler.sample_media_at(t, 100000, 0);
    sampler.sample_media_at(t + Duration::from_secs(1), 0, 200000);
    let v = sampler.snapshot_at(t + Duration::from_secs(1), 1000.0);
    assert!(v["egress_mbps"].is_null());
    assert!(v["uplink"].is_null());
    assert_eq!(v["rtsp_egress_mbps"], 1.6);
}
