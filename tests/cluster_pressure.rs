use flussonix::cluster::{NodeLoad, select};

fn node(name: &str, uplink: f64, cpu: f64, ram: f64, ready: bool) -> NodeLoad {
    NodeLoad {
        name: name.into(),
        uplink,
        cpu,
        ram,
        ready,
        drain: false,
        age_ms: 0,
        active: 0,
        limit: 100,
    }
}

// A weighted average can conceal a nearly exhausted CPU or RAM budget.
#[test]
fn busiest_resource_decides_placement_instead_of_weighted_average() {
    for busy in [
        node("cpu", 0.1, 0.85, 0.1, true),
        node("ram", 0.1, 0.1, 0.9, true),
    ] {
        let balanced = node("balanced", 0.4, 0.4, 0.4, true);
        assert_eq!(select(&[busy, balanced], 0.0).as_deref(), Some("balanced"));
    }
}

// A ready stream must not receive a large fixed advantage over available headroom.
#[test]
fn ready_preference_applies_only_within_pressure_margin() {
    let cold = node("cold", 0.1, 0.1, 0.1, false);
    assert_eq!(
        select(&[node("ready", 0.3, 0.1, 0.1, true), cold.clone()], 0.0).as_deref(),
        Some("cold")
    );
    assert_eq!(
        select(&[cold, node("ready", 0.14, 0.1, 0.1, true)], 0.0).as_deref(),
        Some("ready")
    );
}

// Asynchronous telemetry completion order must not determine equivalent placement.
#[test]
fn equal_pressure_selection_is_stable_across_candidate_order() {
    let a = node("alpha", 0.2, 0.2, 0.2, true);
    let z = node("zeta", 0.2, 0.2, 0.2, true);
    for nodes in [[z.clone(), a.clone()], [a, z]] {
        assert_eq!(select(&nodes, 0.0).as_deref(), Some("alpha"));
    }
}

// Invalid negative metrics must not manufacture spare capacity.
#[test]
fn invalid_metrics_never_authorize_selection() {
    for metric in 0..3 {
        for bad in [-0.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.1] {
            let mut n = node("invalid", 0.1, 0.1, 0.1, true);
            match metric {
                0 => n.uplink = bad,
                1 => n.cpu = bad,
                _ => n.ram = bad,
            }
            assert_eq!(select(&[n], 0.0), None, "metric {metric}");
        }
    }
}

// A negative or nonfinite estimate cannot reduce projected delivery pressure.
#[test]
fn invalid_viewer_cost_never_reduces_load() {
    for expected in [-0.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(select(&[node("edge", 0.1, 0.1, 0.1, true)], expected), None);
    }
}

// Ranking never overrides the existing strict resource, drain or freshness gates.
#[test]
fn pressure_ranking_retains_hard_admission_boundaries() {
    assert_eq!(
        select(&[node("edge", 0.89, 0.89, 0.94, true)], 0.0).as_deref(),
        Some("edge")
    );
    for n in [
        node("uplink", 0.9, 0.1, 0.1, true),
        node("cpu", 0.1, 0.9, 0.1, true),
        node("ram", 0.1, 0.1, 0.95, true),
    ] {
        assert_eq!(select(&[n], 0.0), None);
    }
    assert_eq!(select(&[node("edge", 0.89, 0.1, 0.1, true)], 0.02), None);
    let mut n = node("stale", 0.1, 0.1, 0.1, true);
    n.age_ms = 10001;
    assert_eq!(select(std::slice::from_ref(&n), 0.0), None);
    n.age_ms = 0;
    n.drain = true;
    assert_eq!(select(std::slice::from_ref(&n), 0.0), None);
    n.drain = false;
    n.active = 100;
    assert_eq!(select(&[n], 0.0), None);
}
