//! Bounded OpenMetrics projection for Config GET.

use serde_json::Value;
use std::fmt::Write;

pub(super) fn accepts_openmetrics(accept: &str) -> bool {
    fn recognized_format(entry: &str) -> Option<bool> {
        let media = entry.split(';').next().unwrap_or("").trim();
        if media.eq_ignore_ascii_case("application/openmetrics-text") {
            Some(true)
        } else if media.eq_ignore_ascii_case("application/json")
            || media.eq_ignore_ascii_case("text/plain")
            || media == "*/*"
        {
            Some(false)
        } else {
            None
        }
    }

    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in accept.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            ',' if !quoted => {
                if let Some(selected) = recognized_format(&accept[start..index]) {
                    return selected;
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    if quoted {
        return false;
    }
    recognized_format(&accept[start..]).unwrap_or(false)
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

    #[test]
    fn accept_media_ranges_do_not_split_quoted_commas() {
        assert!(!accepts_openmetrics(
            "application/xml; profile=\"x,application/openmetrics-text;v=y\""
        ));
        assert!(accepts_openmetrics(
            "application/xml; profile=\"x,application/json\", application/openmetrics-text"
        ));
        assert!(!accepts_openmetrics(
            "application/xml; profile=\"x,\\\"application/openmetrics-text\", application/json"
        ));
    }

    #[test]
    fn renderer_output_follows_strict_bounded_openmetrics_grammar() {
        let id = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let body = render(
            id,
            &json!({
                "started_at": 1_700_000_000_u64,
                "cpu_usage": 47,
                "memory_usage": null,
                "total_clients": 3,
                "online_streams": 2,
                "output_kbit": 12_500
            }),
        );
        let mut lines = body.lines();
        let mut seen = std::collections::HashSet::new();
        loop {
            let Some(type_line) = lines.next() else {
                panic!("OpenMetrics body must end with EOF");
            };
            if type_line == "# EOF" {
                assert!(lines.next().is_none(), "data after EOF");
                break;
            }
            let Some(name) = type_line
                .strip_prefix("# TYPE ")
                .and_then(|s| s.strip_suffix(" gauge"))
            else {
                panic!("invalid metric type line: {type_line}");
            };
            assert!(
                [
                    "server_started_at",
                    "server_cpu_usage",
                    "server_memory_usage",
                    "server_total_clients",
                    "server_online_streams",
                    "server_output_bitrate"
                ]
                .contains(&name),
                "unapproved metric family"
            );
            assert!(seen.insert(name), "duplicate metric family");
            let sample = lines.next().expect("sample after type");
            let prefix = format!("{name}{{server_id=\"{id}\"}} ");
            let value = sample
                .strip_prefix(&prefix)
                .expect("canonical sample label");
            assert!(value.parse::<u64>().is_ok(), "invalid gauge value");
        }
        assert_eq!(seen.len(), 5, "unknown measurements must be omitted");
    }
}
