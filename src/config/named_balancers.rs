//! Validated named HTTP balancer configuration. Runtime placement is separate.
use serde_json::Value;
use std::collections::HashSet;

pub(super) fn validate(root: &Value) -> Result<(), String> {
    let rows = root["balancers"]
        .as_array()
        .ok_or("balancers must be an array")?;
    if rows.len() > 64 {
        return Err("at most 64 named balancers are supported".into());
    }
    let peers: HashSet<&str> = root["peers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|peer| peer["hostname"].as_str())
        .collect();
    let streams: Vec<&str> = root["streams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|stream| stream["name"].as_str())
        .collect();
    for row in rows {
        let name = row["name"].as_str().ok_or("balancer name required")?;
        super::valid_identity(&row["name"])?;
        if matches!(name, "admin" | "streamer" | "flussonix" | "tv") {
            return Err("balancer name overlaps a reserved HTTP path".into());
        }
        if streams.iter().any(|stream| {
            *stream == name
                || stream
                    .strip_prefix(name)
                    .is_some_and(|tail| tail.starts_with('/'))
        }) {
            return Err("balancer name overlaps a stream path".into());
        }
        if root["sources"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|source| source["prefix"].as_str())
            .any(|prefix| {
                prefix == name
                    || prefix
                        .strip_prefix(name)
                        .is_some_and(|tail| tail.starts_with('/'))
            })
        {
            return Err("balancer name overlaps a source prefix".into());
        }
        if let Some(mode) = row.get("mode") {
            if !matches!(
                mode.as_str(),
                Some("bitrate" | "usage" | "clients" | "streams")
            ) {
                return Err("balancer mode must be bitrate, usage, clients or streams".into());
            }
        }
        let servers = row["servers"]
            .as_array()
            .filter(|servers| !servers.is_empty() && servers.len() <= 64)
            .ok_or("balancer requires 1 to 64 servers")?;
        let mut seen = HashSet::new();
        for server in servers {
            let fields = server
                .as_object()
                .ok_or("balancer server must be an object")?;
            if fields.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "name" | "max_bitrate" | "countries" | "countries_default"
                )
            }) {
                return Err("unsupported balancer server option".into());
            }
            let peer = server["name"]
                .as_str()
                .ok_or("balancer server name required")?;
            if !seen.insert(peer) {
                return Err("duplicate balancer server".into());
            }
            if !peers.contains(peer) {
                return Err("balancer server must name a registered peer".into());
            }
            let public = root["peers"]
                .as_array()
                .and_then(|items| items.iter().find(|item| item["hostname"] == peer))
                .and_then(|item| item["public_payload_url"].as_str())
                .and_then(|value| url::Url::parse(value).ok())
                .filter(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.port() != Some(0)
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.query().is_none()
                        && url.fragment().is_none()
                });
            if public.is_none() {
                return Err("balancer peer requires a public HTTP(S) delivery URL".into());
            }
            if server
                .get("max_bitrate")
                .is_some_and(|value| value.as_u64().is_none())
            {
                return Err(
                    "balancer maximum bitrate must be a nonnegative integer in bit/s".into(),
                );
            }
            if let Some(countries) = server.get("countries") {
                let countries = countries
                    .as_array()
                    .filter(|countries| countries.len() <= 256)
                    .ok_or("balancer countries must be an array of at most 256 codes")?;
                let mut unique = HashSet::new();
                for country in countries {
                    let code = country.as_str().ok_or("balancer country must be a code")?;
                    if code.len() != 2
                        || !code.bytes().all(|byte| byte.is_ascii_uppercase())
                        || !unique.insert(code)
                    {
                        return Err(
                            "balancer countries require unique uppercase two-letter codes".into(),
                        );
                    }
                }
            }
            if server
                .get("countries_default")
                .is_some_and(|value| !value.is_boolean())
            {
                return Err("balancer countries_default must be boolean".into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn named_balancer_rejects_owned_source_prefix() {
        for prefix in ["lb0", "lb0/sub"] {
            let root = json!({
                "balancers":[{"name":"lb0","servers":[{"name":"cdn-a"}]}],
                "peers":[{"hostname":"cdn-a","public_payload_url":"https://cdn.example"}],
                "streams":[],
                "sources":[{"prefix":prefix}]
            });
            assert!(
                validate(&root).is_err(),
                "accepted overlapping source prefix {prefix}"
            );
        }
    }

    #[test]
    fn named_balancer_rejects_paths_owned_by_administration_and_iptv() {
        for name in ["admin", "streamer", "flussonix", "tv"] {
            let root = json!({
                "balancers":[{"name":name,"servers":[{"name":"cdn-a"}]}],
                "peers":[{"hostname":"cdn-a","public_payload_url":"https://cdn.example"}],
                "streams":[],
                "sources":[]
            });
            assert!(
                validate(&root).is_err(),
                "accepted reserved HTTP path {name}"
            );
        }
    }
}
