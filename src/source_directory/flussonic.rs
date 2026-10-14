//! Explicit Flussonic HTTP inventory transport, independent of vendor components.
//!
//! This client returns discovery facts, not viewer policy or media routes. It is
//! deliberately not selected by the native source resolver. Integrating a vendor
//! source requires separate policy, provenance and private-media qualification.
mod identity;
pub mod modern;
pub(crate) mod routing;
use super::LookupFailure;
use reqwest::header::HeaderValue;
use serde_json::Value;
use std::{
    collections::HashSet,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;

const MAX_BYTES: usize = 1024 * 1024;
const MAX_ROWS: usize = 10000;
const MAX_PAGES: usize = 100;
const MAX_CURSOR_BYTES: usize = 4096;
const MAX_GROUPS_PER_ROW: usize = 64;
const MAX_MEMBERSHIPS: usize = 10000;
const DEADLINE: Duration = Duration::from_millis(750);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamProvenance {
    Config,
    User,
    Remote,
    External,
}
/// Minimal inventory facts. Missing provenance and groups remain unknown.
/// Unknown fields, including remote inputs/authentication, are not route grants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryStream {
    pub name: String,
    pub is_static: bool,
    pub disabled: bool,
    pub named_by: Option<StreamProvenance>,
    /// Omission is unknown; `Some(vec![])` is explicitly ungrouped.
    pub groups: Option<Vec<String>>,
}

/// Metadata-only candidate classification, not an activation or routing grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateKind {
    Static,
    OnDemand,
}
/// Generic diagnostics contain no name, group, URL, policy or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EligibilityFailure {
    Invalid,
    Disabled,
    UnknownProvenance,
    Relayed,
    External,
    UnknownGroups,
}
impl InventoryStream {
    /// Conservatively accept enabled config/user claims with explicit groups.
    /// Revalidate public facts that callers may construct or mutate. A passing
    /// result does not verify provenance, input topology, local configuration,
    /// viewer authority, revisions or leases. The daemon does not use this gate.
    pub fn local_origin_candidate(&self) -> Result<CandidateKind, EligibilityFailure> {
        crate::config::valid_name(&self.name).map_err(|_| EligibilityFailure::Invalid)?;
        if let Some(groups) = &self.groups {
            validate_group_names(groups).map_err(|_| EligibilityFailure::Invalid)?;
        }
        if self.disabled {
            return Err(EligibilityFailure::Disabled);
        }
        match self.named_by {
            Some(StreamProvenance::Config | StreamProvenance::User) => {}
            Some(StreamProvenance::Remote) => return Err(EligibilityFailure::Relayed),
            Some(StreamProvenance::External) => return Err(EligibilityFailure::External),
            None => return Err(EligibilityFailure::UnknownProvenance),
        }
        self.groups
            .as_ref()
            .ok_or(EligibilityFailure::UnknownGroups)?;
        Ok(if self.is_static {
            CandidateKind::Static
        } else {
            CandidateKind::OnDemand
        })
    }
}

/// Owns a verified, no-proxy, no-redirect HTTP client and a privileged key.
/// Credentials are never embedded in URLs, returned in errors or sent using the
/// native peer protocol. Intentionally has no Debug/Serialize implementation.
pub struct InventoryClient {
    endpoint: Url,
    key: Box<str>,
    client: reqwest::Client,
}
impl InventoryClient {
    /// `base` is an HTTP(S) management root, optionally with a proxy path prefix.
    /// A supplied CA replaces public roots and requires HTTPS. Keys are UTF-8,
    /// nonempty, and at most 4096 bytes; native key requirements are unchanged.
    pub fn new(base: &str, key: &str, ca: Option<&Path>) -> Result<Self, LookupFailure> {
        Self::prepare(base, key, ca, DEADLINE)
    }

    fn prepare(
        base: &str,
        key: &str,
        ca: Option<&Path>,
        request_deadline: Duration,
    ) -> Result<Self, LookupFailure> {
        if base.len() > 4096
            || base.chars().any(|c| c.is_control() || c.is_whitespace())
            || base.contains('\\')
            || key.is_empty()
            || key.len() > 4096
        {
            return Err(LookupFailure::Invalid);
        }
        let mut endpoint = Url::parse(base).map_err(|_| LookupFailure::Invalid)?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || endpoint.port() == Some(0)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || ca.is_some() && endpoint.scheme() != "https"
        {
            return Err(LookupFailure::Invalid);
        }
        endpoint.set_path(&format!(
            "{}/-/streams",
            endpoint.path().trim_end_matches('/')
        ));
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(request_deadline)
            .use_preconfigured_tls(
                (*crate::tls_input::client(ca).map_err(|_| LookupFailure::Invalid)?).clone(),
            )
            .build()
            .map_err(|_| LookupFailure::Invalid)?;
        Ok(Self {
            endpoint,
            key: key.into(),
            client,
        })
    }

    /// Publish a result only after the complete cursor chain validates. All pages
    /// share one deadline and byte/row/page budget. Dropping this future cancels
    /// its current fetch; no background pagination or partial result is retained.
    pub async fn inventory(&self) -> Result<Vec<InventoryStream>, LookupFailure> {
        let mut remaining = MAX_BYTES;
        tokio::time::timeout(DEADLINE, self.fetch(&mut remaining))
            .await
            .unwrap_or(Err(LookupFailure::Unavailable))
    }

    async fn fetch(&self, remaining: &mut usize) -> Result<Vec<InventoryStream>, LookupFailure> {
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        let mut names = HashSet::new();
        let mut streams = Vec::new();
        let mut remaining_memberships = MAX_MEMBERSHIPS;
        for _ in 0..MAX_PAGES {
            let mut url = self.endpoint.clone();
            if let Some(cursor) = &cursor {
                // An opaque cursor is a query value, never a navigation target.
                url.query_pairs_mut().append_pair("cursor", cursor);
            }
            let value = self.json(url, remaining).await?;
            let rows = value
                .get("streams")
                .and_then(Value::as_array)
                .ok_or(LookupFailure::Invalid)?;
            if rows.len() > MAX_ROWS - streams.len() {
                return Err(LookupFailure::Invalid);
            }
            for row in rows {
                let row = parse_row(row)?;
                if !names.insert(row.name.clone()) {
                    return Err(LookupFailure::Invalid);
                }
                remaining_memberships = remaining_memberships
                    .checked_sub(row.groups.as_ref().map_or(0, Vec::len))
                    .ok_or(LookupFailure::Invalid)?;
                streams.push(row);
            }
            match value.get("next") {
                None | Some(Value::Null) => return Ok(streams),
                Some(Value::String(next))
                    if !next.is_empty()
                        && next.len() <= MAX_CURSOR_BYTES
                        && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next.clone());
                }
                Some(_) => return Err(LookupFailure::Invalid),
            }
        }
        Err(LookupFailure::Invalid)
    }
    /// Fixed-origin authenticated JSON request sharing a whole-operation byte budget.
    async fn json(&self, url: Url, remaining: &mut usize) -> Result<Value, LookupFailure> {
        self.json_status(url, remaining, false).await
    }
    async fn modern_json(&self, url: Url, remaining: &mut usize) -> Result<Value, LookupFailure> {
        self.json_status(url, remaining, true).await
    }
    async fn json_status(
        &self,
        url: Url,
        remaining: &mut usize,
        modern: bool,
    ) -> Result<Value, LookupFailure> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LookupFailure::Unavailable)?
            .as_secs();
        let mut response = self
            .client
            .get(url)
            .header(
                "Authorization",
                inventory_authorization(&self.key, timestamp),
            )
            .send()
            .await
            .map_err(|_| LookupFailure::Unavailable)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(if modern && !response.status().is_server_error() {
                LookupFailure::Invalid
            } else {
                LookupFailure::Unavailable
            });
        }
        if response
            .content_length()
            .is_some_and(|n| n > *remaining as u64)
        {
            return Err(LookupFailure::Invalid);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| LookupFailure::Unavailable)?
        {
            *remaining = remaining
                .checked_sub(chunk.len())
                .ok_or(LookupFailure::Invalid)?;
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| LookupFailure::Invalid)
    }
}

fn parse_row(row: &Value) -> Result<InventoryStream, LookupFailure> {
    let name = row
        .get("name")
        .and_then(Value::as_str)
        .ok_or(LookupFailure::Invalid)?;
    crate::config::valid_name(name).map_err(|_| LookupFailure::Invalid)?;
    let named_by = match row.get("named_by") {
        None => None,
        Some(Value::String(s)) => Some(match s.as_str() {
            "config" => StreamProvenance::Config,
            "user" => StreamProvenance::User,
            "remote" => StreamProvenance::Remote,
            "external" => StreamProvenance::External,
            _ => return Err(LookupFailure::Invalid),
        }),
        Some(_) => return Err(LookupFailure::Invalid),
    };
    Ok(InventoryStream {
        name: name.into(),
        is_static: optional_bool(row, "static", true)?,
        disabled: optional_bool(row, "disabled", false)?,
        named_by,
        groups: row.get("groups").map(parse_groups).transpose()?,
    })
}

fn parse_groups(value: &Value) -> Result<Vec<String>, LookupFailure> {
    let array = value.as_array().ok_or(LookupFailure::Invalid)?;
    if array.len() > MAX_GROUPS_PER_ROW {
        return Err(LookupFailure::Invalid);
    }
    let groups = array
        .iter()
        .map(|group| {
            group
                .as_str()
                .map(str::to_owned)
                .ok_or(LookupFailure::Invalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_group_names(&groups)?;
    Ok(groups)
}

fn validate_group_names(groups: &[String]) -> Result<(), LookupFailure> {
    if groups.len() > MAX_GROUPS_PER_ROW {
        return Err(LookupFailure::Invalid);
    }
    let mut seen = HashSet::new();
    for group in groups {
        crate::config::valid_name(group).map_err(|_| LookupFailure::Invalid)?;
        if !seen.insert(group) {
            return Err(LookupFailure::Invalid);
        }
    }
    Ok(())
}

fn optional_bool(row: &Value, field: &str, default: bool) -> Result<bool, LookupFailure> {
    match row.get(field) {
        None => Ok(default),
        Some(value) => value.as_bool().ok_or(LookupFailure::Invalid),
    }
}

fn inventory_authorization(key: &str, timestamp: u64) -> HeaderValue {
    // SHA-1 is required by this legacy wire protocol only; native authentication
    // and viewer-token algorithms are not changed. Hash without copying the key
    // into a formatted request or diagnostic string.
    let timestamp = timestamp.to_string();
    let mut context = ring::digest::Context::new(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY);
    context.update(timestamp.as_bytes());
    context.update(b":");
    context.update(key.as_bytes());
    let digest = context.finish();
    let hex = digest
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let mut header = HeaderValue::from_str(&format!("Cluster {timestamp}:{hex}"))
        .expect("decimal time and lowercase digest form an ASCII HTTP header");
    header.set_sensitive(true);
    header
}

#[cfg(test)]
mod tests {
    use super::*;
    // Break caught: using minutes/milliseconds, uppercase/HMAC/SHA256, or native
    // raw credentials instead of the reference-version inventory digest.
    #[test]
    fn inventory_authentication_matches_independent_literal_vectors() {
        for (timestamp, key, expected) in [
            (
                0,
                "synthetic-cluster-key",
                "Cluster 0:065264cc202b040ca9b98e5877ba9a1d8d4b5f72",
            ),
            (
                1791700000,
                "synthetic-cluster-key",
                "Cluster 1791700000:f680a19577adeadfd53faeab5b858173fdae82a2",
            ),
            (
                1791700001,
                "synthetic:other-key",
                "Cluster 1791700001:d97466574296d7db8e237ec71924260b30f52b74",
            ),
            (
                1791700000,
                "synthetic-ключ",
                "Cluster 1791700000:7bfc354ee7e0f8f23d180535d3615f0f2f21245e",
            ),
        ] {
            let header = inventory_authorization(key, timestamp);
            assert_eq!(header, expected);
            assert!(header.is_sensitive());
        }
    }
}
