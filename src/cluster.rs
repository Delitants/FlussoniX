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
impl NodeLoad {
    /// Native telemetry is advisory. Include pending egress and this viewer's cost;
    /// the selected CDN still owns the final admission reservation.
    pub fn from_telemetry(
        name: &str,
        node: &serde_json::Value,
        ready: bool,
        peer_drain: bool,
        elapsed_ms: u64,
        expected_mbps: f64,
    ) -> Option<Self> {
        if !matches!(node["role"].as_str(), Some("cdn" | "standalone"))
            || !expected_mbps.is_finite()
            || expected_mbps < 0.0
        {
            return None;
        }
        let capacity = node["uplink_mbps"]
            .as_f64()
            .filter(|v| v.is_finite() && *v > 0.0)?;
        let metric = |key: &str| {
            node[key]
                .as_f64()
                .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
        };
        let reserved = node["reserved_mbps"]
            .as_f64()
            .filter(|v| v.is_finite() && *v >= 0.0)?;
        Some(Self {
            name: name.into(),
            uplink: metric("uplink")? + (reserved + expected_mbps) / capacity,
            cpu: metric("cpu")?,
            ram: metric("ram")?,
            ready,
            drain: node["drain"].as_bool().unwrap_or(true) || peer_drain,
            age_ms: node["age_ms"].as_u64()?.checked_add(elapsed_ms)?,
            active: node["active"]
                .as_u64()?
                .checked_add(node["reserved"].as_u64()?)?,
            limit: node["limit"].as_u64()?,
        })
    }
}

pub fn select(nodes: &[NodeLoad], expected_uplink: f64) -> Option<String> {
    if !expected_uplink.is_finite() || expected_uplink < 0.0 {
        return None;
    }
    let eligible = nodes.iter().filter(|n| {
        n.age_ms <= 10000
            && !n.drain
            && n.active < n.limit
            && n.uplink.is_finite()
            && n.uplink >= 0.0
            && n.cpu.is_finite()
            && n.cpu >= 0.0
            && n.ram.is_finite()
            && n.ram >= 0.0
            && n.uplink + expected_uplink < 0.9
            && n.cpu < 0.9
            && n.ram < 0.95
    });
    let best = eligible
        .clone()
        .map(|n| pressure(n, expected_uplink))
        .min_by(f64::total_cmp)?;
    eligible
        .filter(|n| pressure(n, expected_uplink) <= best + 0.05)
        .min_by(|a, b| {
            b.ready
                .cmp(&a.ready)
                .then_with(|| pressure(a, expected_uplink).total_cmp(&pressure(b, expected_uplink)))
                .then_with(|| a.name.cmp(&b.name))
        })
        .map(|n| n.name.clone())
}
fn pressure(n: &NodeLoad, expected: f64) -> f64 {
    ((n.uplink + expected) / 0.9)
        .max(n.cpu / 0.9)
        .max(n.ram / 0.95)
}

/// Unknown rates retain the migration fallback. Callers validate the enclosing
/// native node separately before using an observation for other candidates.
pub(crate) const FALLBACK_MBPS: f64 = 2.0;
pub(crate) const MAX_HINT_MBPS: f64 = 1_000_000.0;
pub(crate) fn observed_bitrate(
    node: &serde_json::Value,
    name: &str,
    elapsed_ms: u64,
) -> Option<f64> {
    let observation = &node["stream_bitrates"][name];
    let mbps = observation["mbps"]
        .as_f64()
        .filter(|v| v.is_finite() && *v > 0.0)?;
    let age = observation["age_ms"].as_u64()?.checked_add(elapsed_ms)?;
    if age > crate::media_rate::MAX_AGE_MS {
        return None;
    }
    // Preserve overflow as an unusable cost, never as a smaller fallback.
    Some((mbps * 1.25).max(FALLBACK_MBPS))
}

/// Keep LAN endpoint prefix/query and stream path separate from the selected media scheme.
pub fn source_input_url(endpoint: &str, name: &str, transport: &str) -> Result<String, String> {
    crate::config::valid_name(name)?;
    let mut url = url::Url::parse(endpoint).map_err(|_| "invalid private endpoint")?;
    let secure = match url.scheme() {
        "http" => false,
        "https" => true,
        _ => return Err("private endpoint must use HTTP(S)".into()),
    };
    if url.host_str().is_none() {
        return Err("private endpoint requires a host".into());
    }
    let scheme = match (transport, secure) {
        ("hls", false) => "hls",
        ("hls", true) => "hlss",
        ("m4s", false) => "m4s",
        ("m4s", true) => "m4ss",
        ("m4f", false) => "m4f",
        ("m4f", true) => "m4fs",
        ("mpegts", false) => "tshttp",
        ("mpegts", true) => "tshttps",
        _ => return Err("unsupported source transport".into()),
    };
    let name = name
        .split('/')
        .map(|s| {
            percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
        })
        .collect::<Vec<_>>()
        .join("/");
    let suffix = match transport {
        "hls" => "/index.m3u8",
        "mpegts" => "/mpegts",
        _ => "",
    };
    url.set_path(&format!(
        "{}/{name}{suffix}",
        url.path().trim_end_matches('/')
    ));
    url.set_fragment(None);
    Ok(format!(
        "{scheme}://{}",
        url.as_str().split_once("://").unwrap().1
    ))
}

#[cfg(test)]
mod bitrate_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn fresh_output_has_headroom_and_a_floor_with_a_strict_age_boundary() {
        let node = json!({"stream_bitrates":{"owned":{"mbps":20,"age_ms":2999}}});
        assert_eq!(observed_bitrate(&node, "owned", 1), Some(25.0));
        assert_eq!(observed_bitrate(&node, "owned", 2), None);
        let node = json!({"stream_bitrates":{"owned":{"mbps":0.1,"age_ms":0}}});
        assert_eq!(observed_bitrate(&node, "owned", 0), Some(2.0));
        assert_eq!(observed_bitrate(&node, "other", 0), None);
    }
    #[test]
    fn malformed_and_overflowing_ages_are_unknown_but_rate_overflow_fails_closed() {
        for rate in [json!(null), json!("20"), json!(-1), json!(0), json!([])] {
            assert_eq!(
                observed_bitrate(
                    &json!({"stream_bitrates":{"owned":{"mbps":rate,"age_ms":0}}}),
                    "owned",
                    0
                ),
                None
            );
        }
        for age in [json!(null), json!("0"), json!(-1), json!(u64::MAX)] {
            assert_eq!(
                observed_bitrate(
                    &json!({"stream_bitrates":{"owned":{"mbps":20,"age_ms":age}}}),
                    "owned",
                    1
                ),
                None
            );
        }
        let cost = observed_bitrate(
            &json!({"stream_bitrates":{"owned":{"mbps":f64::MAX,"age_ms":0}}}),
            "owned",
            0,
        )
        .unwrap();
        assert!(
            !cost.is_finite(),
            "overflow must exclude placement rather than under-reserve using fallback"
        );
    }
}
