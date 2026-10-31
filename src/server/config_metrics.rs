//! Bounded OpenMetrics projection for Config GET.

use serde_json::Value;
use std::fmt::Write;

pub(super) fn accepts_openmetrics(accept: &str) -> bool {
    accept.split(',').any(|entry| {
        let mut parts = entry.split(';');
        if !parts.next().is_some_and(|media| {
            media
                .trim()
                .eq_ignore_ascii_case("application/openmetrics-text")
        }) {
            return false;
        }
        for parameter in parts {
            if let Some((name, value)) = parameter.split_once('=') {
                if name.trim().eq_ignore_ascii_case("q") {
                    return value
                        .trim()
                        .parse::<f32>()
                        .is_ok_and(|quality| quality > 0.0 && quality <= 1.0);
                }
            }
        }
        true
    })
}

pub(super) fn render(node_id: uuid::Uuid, stats: &Value) -> String {
    let mut body = String::with_capacity(512);
    for (field, metric) in [
        ("cpu_usage", "server_cpu_usage"),
        ("memory_usage", "server_memory_usage"),
        ("total_clients", "server_total_clients"),
        ("output_kbit", "server_output_bitrate"),
    ] {
        if let Some(value) = stats[field].as_u64() {
            writeln!(body, "# TYPE {metric} gauge").unwrap();
            writeln!(body, "{metric}{{server_id=\"{node_id}\"}} {value}").unwrap();
        }
    }
    body.push_str("# EOF\n");
    body
}
