//! Bounded OpenMetrics projection for Config GET.

use serde_json::Value;
use std::fmt::Write;

pub(super) fn accepts_openmetrics(accept: &str) -> bool {
    for entry in accept.split(',') {
        let media = entry.split(';').next().unwrap_or("").trim();
        if media.eq_ignore_ascii_case("application/openmetrics-text") {
            return true;
        }
        if media.eq_ignore_ascii_case("application/json")
            || media.eq_ignore_ascii_case("text/plain")
            || media == "*/*"
        {
            return false;
        }
    }
    false
}

pub(super) fn render(node_id: uuid::Uuid, stats: &Value) -> String {
    let mut body = String::with_capacity(512);
    for (field, metric) in [
        ("started_at", "server_started_at"),
        ("cpu_usage", "server_cpu_usage"),
        ("memory_usage", "server_memory_usage"),
        ("total_clients", "server_total_clients"),
        ("online_streams", "server_online_streams"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renderer_emits_measured_gauges_and_omits_unknown_telemetry() {
        let id = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let body = render(
            id,
            &json!({
                "started_at":1_700_000_000_u64,
                "online_streams":2,
                "cpu_usage":47,
                "memory_usage":null,
                "total_clients":3,
                "output_kbit":12_500,
                "input_kbit":null,
                "scheduler_load":null
            }),
        );
        assert_eq!(
            body,
            concat!(
                "# TYPE server_started_at gauge\n",
                "server_started_at{server_id=\"123e4567-e89b-12d3-a456-426614174000\"} 1700000000\n",
                "# TYPE server_cpu_usage gauge\n",
                "server_cpu_usage{server_id=\"123e4567-e89b-12d3-a456-426614174000\"} 47\n",
                "# TYPE server_total_clients gauge\n",
                "server_total_clients{server_id=\"123e4567-e89b-12d3-a456-426614174000\"} 3\n",
                "# TYPE server_online_streams gauge\n",
                "server_online_streams{server_id=\"123e4567-e89b-12d3-a456-426614174000\"} 2\n",
                "# TYPE server_output_bitrate gauge\n",
                "server_output_bitrate{server_id=\"123e4567-e89b-12d3-a456-426614174000\"} 12500\n",
                "# EOF\n"
            )
        );
    }
}
