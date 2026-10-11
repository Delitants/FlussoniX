//! Bounded native source metadata lookup. App supplies its no-redirect HTTP(S) client.
use serde_json::Value;
use std::time::Duration;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupFailure {
    Unavailable,
    Absent,
    Invalid,
}
/// Select a source namespace without renaming the upstream stream. Exclusions
/// are relative to that namespace; without a prefix they use full stream names.
/// A subtree pattern does not exclude its parent name or a similar sibling prefix.
pub(crate) fn allows(source: &Value, name: &str) -> bool {
    let name = if let Some(prefix) = source.get("prefix") {
        let Some(relative) = prefix
            .as_str()
            .and_then(|p| name.strip_prefix(p))
            .and_then(|tail| tail.strip_prefix('/'))
            .filter(|tail| !tail.is_empty())
        else {
            return false;
        };
        relative
    } else {
        name
    };
    !source["except"].as_array().is_some_and(|patterns| {
        patterns.iter().filter_map(Value::as_str).any(|pattern| {
            pattern == name
                || pattern
                    .strip_suffix('*')
                    .filter(|p| p.ends_with('/'))
                    .is_some_and(|prefix| name.starts_with(prefix) && name.len() > prefix.len())
        })
    })
}

pub async fn query(
    client: &reqwest::Client,
    source: &Value,
    name: &str,
    default_key: &str,
) -> Result<Value, LookupFailure> {
    tokio::time::timeout(Duration::from_millis(750), async {
        crate::config::valid_name(name).map_err(|_| LookupFailure::Invalid)?;
        if !allows(source, name) {
            return Err(LookupFailure::Absent);
        }
        let mut url = url::Url::parse(source["api_url"].as_str().ok_or(LookupFailure::Invalid)?)
            .map_err(|_| LookupFailure::Invalid)?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(LookupFailure::Invalid);
        }
        let name = name
            .split('/')
            .map(|p| {
                percent_encoding::utf8_percent_encode(p, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("/");
        url.set_path(&format!(
            "{}/flussonix/api/v1/stream/{name}",
            url.path().trim_end_matches('/')
        ));
        url.set_fragment(None);
        let key = source["cluster_key"].as_str().unwrap_or(default_key);
        let mut response = client
            .get(url)
            .header("X-Flussonix-Peer", key)
            .send()
            .await
            .map_err(|_| LookupFailure::Unavailable)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(LookupFailure::Absent);
        }
        if !response.status().is_success() {
            return Err(LookupFailure::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|len| len > 1024 * 1024)
        {
            return Err(LookupFailure::Invalid);
        }
        let mut data = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| LookupFailure::Unavailable)?
        {
            if data.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(LookupFailure::Invalid);
            }
            data.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&data).map_err(|_| LookupFailure::Invalid)?;
        if !value.is_object() {
            return Err(LookupFailure::Invalid);
        }
        Ok(value)
    })
    .await
    .unwrap_or(Err(LookupFailure::Unavailable))
}

/// Static activation is independent of visibility: only never filters query().
pub(crate) fn selected_static(source: &Value, name: &str) -> bool {
    if !allows(source, name) {
        return false;
    }
    let relative = source["prefix"]
        .as_str()
        .and_then(|p| name.strip_prefix(p))
        .and_then(|tail| tail.strip_prefix('/'))
        .unwrap_or(name);
    source["only"].as_array().is_some_and(|patterns| {
        patterns.iter().filter_map(Value::as_str).any(|p| {
            p == relative
                || p.strip_suffix("/*").is_some_and(|p| {
                    relative
                        .strip_prefix(p)
                        .and_then(|tail| tail.strip_prefix('/'))
                        .is_some_and(|tail| !tail.is_empty())
                })
        })
    })
}

pub(crate) async fn inventory(
    client: &reqwest::Client,
    source: &Value,
    default_key: &str,
) -> Result<Vec<String>, LookupFailure> {
    tokio::time::timeout(Duration::from_millis(750), async {
        let mut url = url::Url::parse(source["api_url"].as_str().ok_or(LookupFailure::Invalid)?)
            .map_err(|_| LookupFailure::Invalid)?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(LookupFailure::Invalid);
        }
        url.set_path(&format!(
            "{}/flussonix/api/v1/source-streams",
            url.path().trim_end_matches('/')
        ));
        url.set_fragment(None);
        let mut response = client
            .get(url)
            .header(
                "X-Flussonix-Peer",
                source["cluster_key"].as_str().unwrap_or(default_key),
            )
            .send()
            .await
            .map_err(|_| LookupFailure::Unavailable)?;
        if !response.status().is_success() {
            return Err(LookupFailure::Unavailable);
        }
        if response.content_length().is_some_and(|n| n > 1024 * 1024) {
            return Err(LookupFailure::Invalid);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| LookupFailure::Unavailable)?
        {
            if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(LookupFailure::Invalid);
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| LookupFailure::Invalid)?;
        let rows = value["streams"]
            .as_array()
            .filter(|rows| rows.len() <= 10000)
            .ok_or(LookupFailure::Invalid)?;
        let mut seen = std::collections::HashSet::new();
        let mut names = Vec::new();
        for row in rows {
            let name = row["name"].as_str().ok_or(LookupFailure::Invalid)?;
            crate::config::valid_name(name).map_err(|_| LookupFailure::Invalid)?;
            if !seen.insert(name) {
                return Err(LookupFailure::Invalid);
            }
            let static_ = row["static"].as_bool().ok_or(LookupFailure::Invalid)?;
            let disabled = row["disabled"].as_bool().ok_or(LookupFailure::Invalid)?;
            if static_ && !disabled && selected_static(source, name) {
                names.push(name.to_owned());
            }
        }
        Ok(names)
    })
    .await
    .unwrap_or(Err(LookupFailure::Unavailable))
}
