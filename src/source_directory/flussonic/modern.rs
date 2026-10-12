//! Explicit modern management discovery. Observations never authorize routing.
mod identity;
use super::{
    InventoryClient, InventoryStream, LookupFailure, MAX_BYTES, MAX_CURSOR_BYTES, MAX_MEMBERSHIPS,
    MAX_PAGES, MAX_ROWS, parse_row,
};
pub use identity::ObservedServerId;
use serde_json::Value;
use std::{collections::HashSet, path::Path, time::Duration};
use url::Url;

const MAX_REFRESH: Duration = Duration::from_secs(30);
const REQUEST_DEADLINE: Duration = Duration::from_secs(5);
const SELECT: &str = "name,named_by,groups,static,disabled";

/// Complete bounded wire observations, not an authority-bound candidate, lease,
/// viewer grant, topology proof or private-media endpoint attestation.
/// Intentionally no Debug/serialization: remote identities are untrusted values.
pub struct ObservedInventory {
    server_id: ObservedServerId,
    rows: Vec<InventoryStream>,
}
impl ObservedInventory {
    pub fn server_id(&self) -> &ObservedServerId {
        &self.server_id
    }
    /// Missing groups/provenance remain unknown. Remote policy/inputs are omitted.
    pub fn rows(&self) -> &[InventoryStream] {
        &self.rows
    }
}

/// Opt-in modern endpoint and refresh budget. No legacy endpoint fallback,
/// proxies, redirects, background refresh or daemon configuration integration.
/// The privileged key and prepared TLS client remain private and nonserializable.
pub struct ModernInventoryClient {
    transport: InventoryClient,
    config: Url,
    endpoint: Url,
    budget: Duration,
}
impl ModernInventoryClient {
    /// HTTP(S) management root with optional trusted proxy prefix. Custom CA
    /// replaces public roots and requires HTTPS; credentials never enter a URL.
    /// The complete config/pages/config refresh budget must be in (0,30s].
    /// Every request, including connection and body, is bounded by min(5s,budget).
    pub fn new(
        base: &str,
        key: &str,
        ca: Option<&Path>,
        budget: Duration,
    ) -> Result<Self, LookupFailure> {
        if budget.is_zero() || budget > MAX_REFRESH {
            return Err(LookupFailure::Invalid);
        }
        let transport = InventoryClient::prepare(base, key, ca, budget.min(REQUEST_DEADLINE))?;
        let prefix = transport
            .endpoint
            .path()
            .strip_suffix("/-/streams")
            .ok_or(LookupFailure::Invalid)?;
        let mut config = transport.endpoint.clone();
        config.set_path(&format!("{prefix}/streamer/api/v3/config"));
        let mut endpoint = transport.endpoint.clone();
        endpoint.set_path(&format!("{prefix}/streamer/api/v3/streams"));
        Ok(Self {
            transport,
            config,
            endpoint,
            budget,
        })
    }

    /// Caller supplies its authoritative identity. UUID aliases compare as UUIDs;
    /// other configured identities compare exactly. Reject direct self claims and
    /// observed changes in every page and the final config probe. These are not
    /// atomic remote snapshots or verified transitive topology. One byte budget
    /// covers all response bodies; no partial inventory escapes any failure.
    /// Dropping this future cancels its direct pending request.
    pub async fn inventory(
        &self,
        local_id: &ObservedServerId,
    ) -> Result<ObservedInventory, LookupFailure> {
        let deadline = tokio::time::Instant::now() + self.budget;
        tokio::time::timeout_at(deadline, async {
            let mut remaining = MAX_BYTES;
            let before = self.server_id(&mut remaining).await?;
            if &before == local_id {
                return Err(LookupFailure::Invalid);
            }
            let rows = self.fetch(&before, &mut remaining).await?;
            let after = self.server_id(&mut remaining).await?;
            if before != after || &after == local_id {
                return Err(LookupFailure::Invalid);
            }
            // The timeout cannot preempt synchronous bounded JSON/row parsing.
            // Recheck before returning successful observations as well.
            if tokio::time::Instant::now() >= deadline {
                return Err(LookupFailure::Unavailable);
            }
            Ok(ObservedInventory {
                server_id: before,
                rows,
            })
        })
        .await
        .unwrap_or(Err(LookupFailure::Unavailable))
    }

    async fn server_id(&self, remaining: &mut usize) -> Result<ObservedServerId, LookupFailure> {
        let value = self.transport.json(self.config.clone(), remaining).await?;
        let id = value
            .get("stats")
            .and_then(|s| s.get("id"))
            .and_then(Value::as_str)
            .ok_or(LookupFailure::Invalid)?;
        ObservedServerId::parse(id)
    }

    async fn fetch(
        &self,
        expected: &ObservedServerId,
        remaining: &mut usize,
    ) -> Result<Vec<InventoryStream>, LookupFailure> {
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        let mut names = HashSet::new();
        let mut streams = Vec::new();
        let mut memberships = MAX_MEMBERSHIPS;
        for _ in 0..MAX_PAGES {
            let mut url = self.endpoint.clone();
            {
                let mut query = url.query_pairs_mut();
                query
                    .append_pair("select", SELECT)
                    .append_pair("limit", "100");
                if let Some(cursor) = &cursor {
                    query.append_pair("cursor", cursor);
                }
            }
            let value = self.transport.json(url, remaining).await?;
            let page_id = value
                .get("server_id")
                .and_then(Value::as_str)
                .ok_or(LookupFailure::Invalid)?;
            if &ObservedServerId::parse(page_id)? != expected {
                return Err(LookupFailure::Invalid);
            }
            let rows = value
                .get("streams")
                .and_then(Value::as_array)
                .ok_or(LookupFailure::Invalid)?;
            if rows.len() > MAX_ROWS - streams.len() {
                return Err(LookupFailure::Invalid);
            }
            for value in rows {
                let row = parse_row(value)?;
                if !names.insert(row.name.clone()) {
                    return Err(LookupFailure::Invalid);
                }
                memberships = memberships
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
                    cursor = Some(next.clone())
                }
                _ => return Err(LookupFailure::Invalid),
            }
        }
        Err(LookupFailure::Invalid)
    }
}
