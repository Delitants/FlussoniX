use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLoad {
    pub name: String,
    pub uplink: f64,
    pub cpu: f64,
    pub ram: f64,
    pub ready: bool,
    pub drain: bool,
    pub age_ms: u64,
    pub active: u64,
    pub limit: u64,
}
pub fn select(nodes: &[NodeLoad], expected_uplink: f64) -> Option<String> {
    nodes
        .iter()
        .filter(|n| {
            n.age_ms <= 10000
                && !n.drain
                && n.active < n.limit
                && n.uplink.is_finite()
                && n.cpu.is_finite()
                && n.ram.is_finite()
                && n.uplink + expected_uplink < 0.9
                && n.cpu < 0.9
                && n.ram < 0.95
        })
        .min_by(|a, b| score(a, expected_uplink).total_cmp(&score(b, expected_uplink)))
        .map(|n| n.name.clone())
}
fn score(n: &NodeLoad, expected: f64) -> f64 {
    (n.uplink + expected) * 0.65 + n.cpu * 0.25 + n.ram * 0.1 + if n.ready { 0.0 } else { 0.2 }
}
