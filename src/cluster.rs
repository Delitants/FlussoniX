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
    select_rotating(nodes, expected_uplink, 0)
}

fn tied_candidates(nodes: &[NodeLoad], expected_uplink: f64) -> Vec<&NodeLoad> {
    if !expected_uplink.is_finite() || expected_uplink < 0.0 {
        return Vec::new();
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
    let Some(best) = eligible
        .clone()
        .map(|n| pressure(n, expected_uplink))
        .min_by(f64::total_cmp)
    else {
        return Vec::new();
    };
    let mut shortlist = eligible
        .filter(|n| pressure(n, expected_uplink) <= best + 0.05)
        .collect::<Vec<_>>();
    let preferred = shortlist
        .iter()
        .min_by(|a, b| {
            b.ready
                .cmp(&a.ready)
                .then_with(|| pressure(a, expected_uplink).total_cmp(&pressure(b, expected_uplink)))
        })
        .unwrap();
    let ready = preferred.ready;
    let preferred_pressure = pressure(preferred, expected_uplink);
    shortlist.retain(|n| n.ready == ready && pressure(n, expected_uplink) == preferred_pressure);
    shortlist.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    shortlist
}
fn pressure(n: &NodeLoad, expected: f64) -> f64 {
    ((n.uplink + expected) / 0.9)
        .max(n.cpu / 0.9)
        .max(n.ram / 0.95)
}

fn select_rotating(nodes: &[NodeLoad], expected_uplink: f64, turn: u64) -> Option<String> {
    let ties = tied_candidates(nodes, expected_uplink);
    let count = ties.len();
    if count == 0 {
        return None;
    }
    Some(ties[(turn % count as u64) as usize].name.clone())
}

/// Per-LB progress for exact ties. Hash keys keep retained memory independent of
/// configured name length and peer count. No lock survives an admission RPC.
#[derive(Default)]
pub(crate) struct TieRotation {
    groups: std::sync::Mutex<std::collections::HashMap<[u8; 32], (u64, std::time::Instant)>>,
}
impl TieRotation {
    pub(crate) fn select(
        &self,
        nodes: &[NodeLoad],
        expected_uplink: f64,
        turn: &mut Option<u64>,
    ) -> Option<String> {
        use sha2::{Digest, Sha256};
        let ties = tied_candidates(nodes, expected_uplink);
        if ties.is_empty() {
            return None;
        }
        if ties.len() == 1 {
            return Some(ties[0].name.clone());
        }
        let turn = *turn.get_or_insert_with(|| {
            let mut digest = Sha256::new();
            for node in &ties {
                digest.update((node.name.len() as u64).to_be_bytes());
                digest.update(node.name.as_bytes());
            }
            let key: [u8; 32] = digest.finalize().into();
            let mut groups = self.groups.lock().unwrap();
            let now = std::time::Instant::now();
            if !groups.contains_key(&key) && groups.len() >= 64 {
                let oldest = groups
                    .iter()
                    .min_by_key(|(key, (_, last))| (*last, **key))
                    .map(|(key, _)| *key)
                    .unwrap();
                groups.remove(&oldest);
            }
            let entry = groups.entry(key).or_insert((0, now));
            let turn = entry.0;
            entry.0 = turn.wrapping_add(1);
            entry.1 = now;
            turn
        });
        Some(ties[(turn % ties.len() as u64) as usize].name.clone())
    }
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

#[cfg(test)]
mod rotation_tests {
    use super::*;
    fn node(name: &str, uplink: f64, ready: bool) -> NodeLoad {
        NodeLoad {
            name: name.into(),
            uplink,
            cpu: 0.1,
            ram: 0.1,
            ready,
            drain: false,
            age_ms: 0,
            active: 0,
            limit: 100,
        }
    }
    // Hostname-only selection or indexing unsorted probe results concentrates ties.
    #[test]
    fn turns_visit_all_exact_ties_independent_of_probe_order() {
        let nodes = [
            node("zeta", 0.2, true),
            node("alpha", 0.2, true),
            node("mu", 0.2, true),
        ];
        for input in [
            nodes.clone(),
            [nodes[1].clone(), nodes[2].clone(), nodes[0].clone()],
        ] {
            for (turn, want) in ["alpha", "mu", "zeta", "alpha", "mu", "zeta"]
                .iter()
                .enumerate()
            {
                assert_eq!(
                    select_rotating(&input, 0.0, turn as u64).as_deref(),
                    Some(*want)
                );
            }
        }
    }
    // Round-robin across the whole readiness band would override load ranking.
    #[test]
    fn rotation_never_expands_exact_ties_to_nearby_pressure() {
        let nodes = [node("alpha", 0.2, true), node("beta", 0.201, true)];
        for turn in 0..8 {
            assert_eq!(select_rotating(&nodes, 0.0, turn).as_deref(), Some("alpha"));
        }
    }
    // Rotating a cold peer alongside a ready peer loses the existing locality policy.
    #[test]
    fn exact_ties_keep_readiness_priority() {
        let nodes = [
            node("cold", 0.2, false),
            node("ready-a", 0.2, true),
            node("ready-b", 0.2, true),
        ];
        for (turn, want) in ["ready-a", "ready-b", "ready-a", "ready-b"]
            .iter()
            .enumerate()
        {
            assert_eq!(
                select_rotating(&nodes, 0.0, turn as u64).as_deref(),
                Some(*want)
            );
        }
    }
    // Locality still applies within the band, and cannot reach outside it.
    #[test]
    fn rotation_retains_the_bounded_ready_shortlist() {
        for turn in 0..8 {
            assert_eq!(
                select_rotating(
                    &[node("cold", 0.1, false), node("ready", 0.14, true)],
                    0.0,
                    turn
                )
                .as_deref(),
                Some("ready")
            );
            assert_eq!(
                select_rotating(
                    &[node("cold", 0.1, false), node("ready", 0.3, true)],
                    0.0,
                    turn
                )
                .as_deref(),
                Some("cold")
            );
        }
    }
    // Rotating before eligibility filtering could admit any of these peers.
    #[test]
    fn every_turn_retains_hard_resource_and_freshness_gates() {
        let mut nodes = vec![node("healthy", 0.2, true)];
        for (name, field) in [
            ("drain", 0),
            ("stale", 1),
            ("full", 2),
            ("cpu", 3),
            ("ram", 4),
            ("uplink", 5),
            ("invalid", 6),
        ] {
            let mut bad = node(name, 0.0, true);
            match field {
                0 => bad.drain = true,
                1 => bad.age_ms = 10001,
                2 => bad.active = 100,
                3 => bad.cpu = 0.9,
                4 => bad.ram = 0.95,
                5 => bad.uplink = 0.9,
                _ => bad.cpu = f64::NAN,
            }
            nodes.push(bad);
        }
        for turn in 0..16 {
            assert_eq!(
                select_rotating(&nodes, 0.0, turn).as_deref(),
                Some("healthy")
            );
            assert_eq!(select_rotating(&nodes[1..], 0.0, turn), None);
            assert_eq!(select_rotating(&nodes, -0.1, turn), None);
        }
    }
    // Full-width turns are reduced before indexing, including counter wrap.
    #[test]
    fn large_and_wrapping_turns_keep_selection_in_bounds() {
        let nodes = [
            node("alpha", 0.2, true),
            node("mu", 0.2, true),
            node("zeta", 0.2, true),
        ];
        for (turn, want) in [
            (u64::MAX - 1, "zeta"),
            (u64::MAX, "alpha"),
            (0, "alpha"),
            (1, "mu"),
        ] {
            assert_eq!(select_rotating(&nodes, 0.0, turn).as_deref(), Some(want));
        }
        assert_eq!(select_rotating(&[], 0.0, u64::MAX), None);
    }

    // One global cursor aliases alternating subsets even when each is an exact tie.
    #[test]
    fn different_tied_sets_have_independent_progress() {
        let rotation = TieRotation::default();
        let ab = [node("alpha", 0.2, true), node("beta", 0.2, true)];
        let bc = [node("beta", 0.2, true), node("gamma", 0.2, true)];
        for (want_ab, want_bc) in [
            ("alpha", "beta"),
            ("beta", "gamma"),
            ("alpha", "beta"),
            ("beta", "gamma"),
        ] {
            assert_eq!(
                rotation.select(&ab, 0.0, &mut None).as_deref(),
                Some(want_ab)
            );
            assert_eq!(
                rotation.select(&bc, 0.0, &mut None).as_deref(),
                Some(want_bc)
            );
        }
    }
    // Retried selections reuse their turn rather than moving another group's cursor.
    #[test]
    fn request_turn_survives_retries_without_consuming_another_group() {
        let rotation = TieRotation::default();
        let ab = [node("alpha", 0.2, true), node("beta", 0.2, true)];
        let bc = [node("beta", 0.2, true), node("gamma", 0.2, true)];
        let mut request_turn = None;
        assert_eq!(
            rotation.select(&ab, 0.0, &mut request_turn).as_deref(),
            Some("alpha")
        );
        assert_eq!(
            rotation.select(&bc, 0.0, &mut request_turn).as_deref(),
            Some("beta")
        );
        assert_eq!(
            rotation.select(&ab, 0.0, &mut None).as_deref(),
            Some("beta")
        );
        assert_eq!(
            rotation.select(&bc, 0.0, &mut None).as_deref(),
            Some("beta")
        );
    }
    // Without bounded eviction an inactive group's cursor never resets; FIFO loses active groups.
    #[test]
    fn group_history_is_bounded_and_recent_groups_survive_eviction() {
        let rotation = TieRotation::default();
        let main = [
            node("alpha", 0.2, true),
            node("mu", 0.2, true),
            node("zeta", 0.2, true),
        ];
        let insert = |i| {
            let pair = [
                node(&format!("g{i}-a"), 0.2, true),
                node(&format!("g{i}-b"), 0.2, true),
            ];
            rotation.select(&pair, 0.0, &mut None);
        };
        assert_eq!(
            rotation.select(&main, 0.0, &mut None).as_deref(),
            Some("alpha")
        );
        for i in 1..=63 {
            insert(i);
        }
        assert_eq!(
            rotation.select(&main, 0.0, &mut None).as_deref(),
            Some("mu")
        );
        insert(64);
        assert_eq!(
            rotation.select(&main, 0.0, &mut None).as_deref(),
            Some("zeta")
        );
        assert_eq!(
            rotation.select(&main, 0.0, &mut None).as_deref(),
            Some("alpha")
        );
        for i in 65..=128 {
            insert(i);
        }
        assert_eq!(
            rotation.select(&main, 0.0, &mut None).as_deref(),
            Some("alpha")
        );
    }
}
