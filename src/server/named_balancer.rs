//! Pure compatibility ranking for configured named HTTP balancers.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    io::Read,
    net::IpAddr,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_GEOIP_BYTES: u64 = 128 * 1024 * 1024;

pub(super) struct GeoIp {
    reader: maxminddb::Reader<Vec<u8>>,
    revision: String,
}
impl GeoIp {
    pub fn open(path: &Path) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("GeoIP database: {e}"))?;
        let meta = file
            .metadata()
            .map_err(|e| format!("GeoIP database: {e}"))?;
        if !meta.is_file() || meta.len() == 0 || meta.len() > MAX_GEOIP_BYTES {
            return Err(
                "GeoIP database must be a nonempty regular file no larger than 128 MiB".into(),
            );
        }
        let mut bytes = Vec::with_capacity(meta.len() as usize);
        file.take(MAX_GEOIP_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| format!("GeoIP database: {e}"))?;
        if bytes.len() as u64 != meta.len() {
            return Err("GeoIP database changed while loading".into());
        }
        let revision = format!("{:x}", Sha256::digest(&bytes));
        let reader = maxminddb::Reader::from_source(bytes)
            .map_err(|e| format!("invalid GeoIP database: {e}"))?;
        let kind = reader.metadata.database_type.as_str();
        if !kind.ends_with("-Country") && !kind.ends_with("-City") {
            return Err("GeoIP database must contain country or city records".into());
        }
        Ok(Self {
            reader,
            revision: revision[..16].to_owned(),
        })
    }
    pub fn country(&self, client: IpAddr) -> Option<String> {
        let country: Option<String> = self
            .reader
            .lookup(client)
            .ok()?
            .decode_path(&[
                maxminddb::PathElement::Key("country"),
                maxminddb::PathElement::Key("iso_code"),
            ])
            .ok()?;
        country.filter(|code| code.len() == 2 && code.bytes().all(|c| c.is_ascii_uppercase()))
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
}

pub(super) fn requires_geoip(root_or_patch: &Value) -> bool {
    root_or_patch["balancers"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|balancer| {
            balancer["servers"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|server| {
                    server["countries"]
                        .as_array()
                        .is_some_and(|countries| !countries.is_empty())
                })
        })
}

pub(super) fn row_requires_geoip(row: &Value) -> bool {
    row["servers"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|server| {
            server["countries"]
                .as_array()
                .is_some_and(|countries| !countries.is_empty())
        })
}

pub(super) struct HttpPath {
    pub balancer: String,
    pub media_path: String,
}

fn valid_percent_encoding(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            if at + 2 >= bytes.len()
                || !bytes[at + 1].is_ascii_hexdigit()
                || !bytes[at + 2].is_ascii_hexdigit()
            {
                return false;
            }
            at += 3;
        } else {
            at += 1;
        }
    }
    true
}

/// Retain the exact post-prefix wire path while validating its decoded meaning.
pub(super) fn http_path(root: &Value, raw_path: &str) -> Result<Option<HttpPath>, &'static str> {
    let Some(raw) = raw_path.strip_prefix('/') else {
        return Ok(None);
    };
    let (first, tail) = raw
        .split_once('/')
        .map_or((raw, None), |(first, tail)| (first, Some(tail)));
    let Some(balancers) = root["balancers"].as_array() else {
        return Ok(None);
    };
    let decoded_first = percent_encoding::percent_decode_str(first)
        .decode_utf8()
        .ok();
    let matched = balancers.iter().find_map(|row| {
        let name = row["name"].as_str()?;
        (decoded_first.as_deref() == Some(name)).then_some(name)
    });
    let Some(name) = matched else {
        if balancers
            .iter()
            .filter_map(|row| row["name"].as_str())
            .any(|name| {
                decoded_first
                    .as_deref()
                    .is_some_and(|first| first.starts_with(&format!("{name}/")))
            })
        {
            return Err("invalid named balancer prefix");
        }
        return Ok(None);
    };
    let Some(tail) = tail else {
        return Err("named balancer media path required");
    };
    if tail.is_empty() || raw_path.len() > 4096 || !valid_percent_encoding(raw) {
        return Err("invalid named balancer media path");
    }
    let decoded = percent_encoding::percent_decode_str(tail)
        .decode_utf8()
        .map_err(|_| "invalid named balancer media path")?;
    if decoded.split('/').count() != tail.split('/').count()
        || decoded
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || decoded
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '\\' | '?' | '#'))
    {
        return Err("invalid named balancer media path");
    }
    Ok(Some(HttpPath {
        balancer: name.to_owned(),
        media_path: format!("/{tail}"),
    }))
}

pub(super) fn valid_admission_query(raw: Option<&str>) -> bool {
    let mut ticket = false;
    let mut handoff = false;
    for (key, _) in url::form_urlencoded::parse(raw.unwrap_or("").as_bytes()) {
        match key.as_ref() {
            "flussonix_ticket" => {
                if ticket {
                    return false;
                }
                ticket = true;
            }
            "flussonix_handoff" => {
                if handoff {
                    return false;
                }
                handoff = true;
            }
            _ => {}
        }
    }
    !(ticket && handoff)
}

#[derive(Clone, Debug)]
pub(super) struct ObservedPeer<'a> {
    pub name: &'a str,
    pub output_kbit: Option<u64>,
    pub bandwidth_usage: Option<f64>,
    pub clients: Option<u64>,
    pub total_streams_count: Option<u64>,
    pub age_ms: u64,
}

pub(super) fn rank(
    config: &Value,
    candidates: &[ObservedPeer<'_>],
    client: IpAddr,
    country: Option<&str>,
    revision: u64,
    stream: &str,
    affinity: &str,
) -> Vec<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    rank_at(
        config,
        candidates,
        client,
        country,
        revision,
        (stream, affinity),
        now,
    )
}

#[derive(Clone, Copy)]
enum Score {
    Count(u64),
    Percent(f64),
}
impl Score {
    fn compare(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::Count(left), Self::Count(right)) => left.cmp(&right),
            (Self::Percent(left), Self::Percent(right)) => left.total_cmp(&right),
            _ => Ordering::Equal,
        }
    }
}

fn rank_at(
    config: &Value,
    candidates: &[ObservedPeer<'_>],
    client: IpAddr,
    country: Option<&str>,
    revision: u64,
    session: (&str, &str),
    now_seconds: u64,
) -> Vec<String> {
    let (stream, affinity) = session;
    let Some(servers) = config["servers"].as_array() else {
        return Vec::new();
    };
    let scoped = servers.iter().any(|server| {
        server["countries"]
            .as_array()
            .is_some_and(|codes| !codes.is_empty())
    });
    let matching = if scoped {
        servers
            .iter()
            .filter(|server| {
                country.is_some_and(|country| {
                    server["countries"].as_array().is_some_and(|codes| {
                        codes.iter().any(|code| code.as_str() == Some(country))
                    })
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let selected = if !scoped {
        servers.iter().collect::<Vec<_>>()
    } else if !matching.is_empty() {
        matching
    } else {
        servers
            .iter()
            .filter(|server| server["countries_default"] == true)
            .collect::<Vec<_>>()
    };
    let mode = config["mode"].as_str().unwrap_or("bitrate");
    let mut eligible = Vec::new();
    for server in selected {
        let Some(name) = server["name"].as_str() else {
            continue;
        };
        let Some(observed) = candidates
            .iter()
            .find(|candidate| candidate.name == name && candidate.age_ms <= 10_000)
        else {
            continue;
        };
        if let Some(max) = server["max_bitrate"].as_u64() {
            if !observed
                .output_kbit
                .and_then(|rate| rate.checked_mul(1024))
                .is_some_and(|rate| rate <= max)
            {
                continue;
            }
        }
        let score = match mode {
            "bitrate" => observed.output_kbit.map(Score::Count),
            "usage" => observed
                .bandwidth_usage
                .filter(|n| n.is_finite() && (0.0..=10_000.0).contains(n))
                .map(Score::Percent),
            "clients" => observed.clients.map(Score::Count),
            "streams" => observed.total_streams_count.map(Score::Count),
            _ => None,
        };
        if let Some(score) = score {
            eligible.push((name, score));
        }
    }
    let mut membership = eligible.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    membership.sort_unstable();
    let mut scope = Sha256::new();
    for part in [
        config["name"].as_str().unwrap_or(""),
        stream,
        &client.to_string(),
        affinity,
    ] {
        scope.update((part.len() as u64).to_be_bytes());
        scope.update(part.as_bytes());
    }
    scope.update(revision.to_be_bytes());
    scope.update((now_seconds / 600).to_be_bytes());
    for name in membership {
        scope.update((name.len() as u64).to_be_bytes());
        scope.update(name.as_bytes());
    }
    let scope = scope.finalize();
    let mut ranked = eligible
        .into_iter()
        .map(|(name, score)| {
            let mut digest = Sha256::new();
            digest.update(scope);
            digest.update(name.as_bytes());
            (name.to_owned(), score, digest.finalize())
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.1
            .compare(right.1)
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.0.cmp(&right.0))
    });
    ranked.into_iter().map(|(name, _, _)| name).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn peer(
        name: &'static str,
        output_kbit: u64,
        usage: f64,
        clients: u64,
        streams: u64,
    ) -> ObservedPeer<'static> {
        ObservedPeer {
            name,
            output_kbit: Some(output_kbit),
            bandwidth_usage: Some(usage),
            clients: Some(clients),
            total_streams_count: Some(streams),
            age_ms: 0,
        }
    }
    fn rank_names(
        config: Value,
        candidates: &[ObservedPeer<'_>],
        country: Option<&str>,
    ) -> Vec<String> {
        rank(
            &config,
            candidates,
            "198.51.100.8".parse().unwrap(),
            country,
            7,
            "channel",
            "viewer-1",
        )
    }

    #[test]
    fn four_modes_use_distinct_fresh_metrics_and_total_stream_count() {
        let peers = [peer("a", 400, 70.0, 5, 1), peer("b", 100, 90.0, 2, 9)];
        for (mode, first) in [
            ("bitrate", "b"),
            ("usage", "a"),
            ("clients", "b"),
            ("streams", "a"),
        ] {
            let config = json!({"name":"lb0","mode":mode,"servers":[{"name":"a"},{"name":"b"}]});
            assert_eq!(
                rank_names(config, &peers, None).first().map(String::as_str),
                Some(first),
                "mode {mode}"
            );
        }
        let config = json!({"name":"lb0","servers":[{"name":"a"},{"name":"b"}]});
        assert_eq!(rank_names(config, &peers, None)[0], "b");
    }

    #[test]
    fn ceiling_is_optional_or_exact_in_bits_per_second_in_every_mode() {
        let peers = [peer("a", 40_960, 1.0, 1, 1), peer("b", 10, 2.0, 2, 2)];
        for mode in ["bitrate", "usage", "clients", "streams"] {
            let exact = json!({"name":"lb0","mode":mode,"servers":[{"name":"a","max_bitrate":41_943_040},{"name":"b"}]});
            assert_eq!(rank_names(exact, &peers, None).len(), 2, "mode {mode}");
            let below = json!({"name":"lb0","mode":mode,"servers":[{"name":"a","max_bitrate":41_943_039},{"name":"b"}]});
            assert_eq!(rank_names(below, &peers, None), ["b"], "mode {mode}");
            let zero = json!({"name":"lb0","mode":mode,"servers":[{"name":"a","max_bitrate":0},{"name":"b"}]});
            assert_eq!(rank_names(zero, &peers, None), ["b"], "mode {mode}");
        }
    }

    #[test]
    fn country_matches_win_and_misses_use_only_explicit_defaults() {
        let peers = [
            peer("us", 1, 1.0, 1, 1),
            peer("default", 2, 2.0, 2, 2),
            peer("gb", 3, 3.0, 3, 3),
        ];
        let config = json!({"name":"lb0","servers":[{"name":"us","countries":["US"]},{"name":"default","countries_default":true},{"name":"gb","countries":["GB"]}]});
        assert_eq!(rank_names(config.clone(), &peers, Some("US")), ["us"]);
        assert_eq!(rank_names(config.clone(), &peers, Some("FR")), ["default"]);
        assert_eq!(rank_names(config, &peers, None), ["default"]);
        let no_default = json!({"name":"lb0","servers":[{"name":"us","countries":["US"]}]});
        assert!(rank_names(no_default, &peers, None).is_empty());
    }

    #[test]
    fn stale_or_missing_selected_metric_never_becomes_zero_load() {
        let mut peers = [peer("a", 1, 1.0, 1, 1), peer("b", 2, 2.0, 2, 2)];
        peers[0].age_ms = 10_001;
        for mode in ["bitrate", "usage", "clients", "streams"] {
            let config = json!({"name":"lb0","mode":mode,"servers":[{"name":"a"},{"name":"b"}]});
            assert_eq!(rank_names(config, &peers, None), ["b"]);
        }
        peers[0].age_ms = 0;
        peers[0].bandwidth_usage = None;
        let config = json!({"name":"lb0","mode":"usage","servers":[{"name":"a"},{"name":"b"}]});
        assert_eq!(rank_names(config, &peers, None), ["b"]);
    }

    #[test]
    fn geoip_reader_uses_local_country_data_and_rejects_bad_files() {
        // Test data from maxmind/MaxMind-DB, Apache-2.0 or MIT, Git blob
        // 530ef08f867510e22054609c638c87754d113be4.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("country.mmdb");
        std::fs::write(
            &path,
            include_bytes!("../../tests/fixtures/GeoIP2-Country-Test.mmdb"),
        )
        .unwrap();
        let geoip = GeoIp::open(&path).unwrap();
        assert_eq!(
            geoip.country("89.160.20.128".parse().unwrap()).as_deref(),
            Some("SE")
        );
        assert_eq!(geoip.country("192.0.2.1".parse().unwrap()), None);
        assert_eq!(geoip.revision().len(), 16);
        std::fs::write(&path, b"not a database").unwrap();
        assert!(GeoIp::open(&path).is_err());
        let oversized = dir.path().join("oversized.mmdb");
        std::fs::File::create(&oversized)
            .unwrap()
            .set_len(MAX_GEOIP_BYTES + 1)
            .unwrap();
        assert!(GeoIp::open(&oversized).is_err());
    }
}
