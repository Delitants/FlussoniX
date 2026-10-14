//! HTTP native routing observations. Advisory snapshots never grant admission.
use super::*;
use std::collections::HashSet;
use tokio::sync::Semaphore;

pub(super) struct Snapshot {
    pub when: Instant,
    pub value: Value,
    pub ready: HashSet<String>,
    pub vendor_id: Option<crate::source_directory::flussonic::modern::ObservedServerId>,
}
type Slot = Arc<Mutex<Option<Arc<Snapshot>>>>;
type Holds = (u64, HashMap<String, Vec<(Instant, f64)>>);
pub(super) struct Registry {
    slots: Mutex<(u64, HashMap<String, Slot>)>,
    network: Semaphore,
    holds: std::sync::Mutex<Holds>,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            slots: Mutex::new((0, HashMap::new())),
            network: Semaphore::new(8),
            holds: std::sync::Mutex::new((0, HashMap::new())),
        }
    }
}
impl App {
    pub(super) async fn http_snapshot(
        &self,
        peer: &Value,
        name: &str,
        revision: u64,
    ) -> Option<Arc<Snapshot>> {
        let vendor = crate::config::vendor_peers::is_vendor(peer);
        let id = peer["hostname"].as_str()?;
        let remote = if vendor {
            Some(crate::config::vendor_peers::remote_name(peer, name)?)
        } else {
            None
        };
        let slot_id = if vendor {
            format!("{id}\0{name}")
        } else {
            id.to_owned()
        };
        let slot = {
            let mut slots = self.http_routes.slots.lock().await;
            if self.config.revision() != revision {
                return None;
            }
            if slots.0 != revision {
                slots.0 = revision;
                slots.1.clear();
            }
            slots
                .1
                .entry(slot_id)
                .or_insert_with(|| Arc::new(Mutex::new(None)))
                .clone()
        };
        let mut cache = slot.lock().await;
        if self.config.revision() != revision {
            return None;
        }
        if let Some(snapshot) = cache
            .as_ref()
            .filter(|s| s.when.elapsed() < Duration::from_secs(1))
        {
            return Some(snapshot.clone());
        }

        let _network = self.http_routes.network.acquire().await.ok()?;
        if self.config.revision() != revision {
            return None;
        }
        if let Some(remote) = remote {
            use crate::source_directory::flussonic::{modern::ObservedServerId, routing};
            let mut local_ids = vec![ObservedServerId::parse(&self.options.node_name).ok()?];
            if let Some(id) = self
                .config
                .read(|root| root["server_id"].as_str().map(str::to_owned))
            {
                local_ids.push(ObservedServerId::parse(&id).ok()?);
            }
            let when = Instant::now();
            let mut observation = routing::observe(
                peer["api_url"].as_str()?,
                peer["cluster_key"].as_str()?,
                peer["flussonix_tls_ca"].as_str().map(Path::new),
                remote,
                &local_ids,
            )
            .await
            .ok()?;
            if cache
                .as_ref()
                .and_then(|s| s.vendor_id.as_ref())
                .is_some_and(|id| *id != observation.id)
                || self.config.revision() != revision
            {
                return None;
            }
            if let Some(rate) = observation.value["stream_bitrates"].get(remote).cloned() {
                observation.value["stream_bitrates"] = json!({name:rate});
            }
            let ready = if observation.ready {
                HashSet::from([name.to_owned()])
            } else {
                HashSet::new()
            };
            let snapshot = Arc::new(Snapshot {
                when,
                value: observation.value,
                ready,
                vendor_id: Some(observation.id),
            });
            *cache = Some(snapshot.clone());
            return Some(snapshot);
        }
        let client = self.cluster_client(peer).ok()?;
        let api = peer["api_url"].as_str()?;
        let key = peer["cluster_key"]
            .as_str()
            .unwrap_or(&self.options.peer_key);
        // Response latency consumes the original observation's freshness.
        let when = Instant::now();
        let mut response = client
            .get(format!(
                "{}/flussonix/api/v1/node",
                api.trim_end_matches('/')
            ))
            .header("X-Flussonix-Peer", key)
            .timeout(Duration::from_millis(500))
            .send()
            .await
            .ok()?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|n| n > 2 * 1024 * 1024)
        {
            return None;
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if bytes.len().checked_add(chunk.len())? > 2 * 1024 * 1024 {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut value: Value = serde_json::from_slice(&bytes).ok()?;
        let ready = value["streams"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| s["ready"] == true)
            .filter_map(|s| s["name"].as_str().map(str::to_owned))
            .collect();
        // Retain only routing inputs, not complete statistics or unrelated metadata.
        value.as_object_mut()?.retain(|key, _| {
            matches!(
                key.as_str(),
                "role"
                    | "uplink"
                    | "uplink_mbps"
                    | "cpu"
                    | "ram"
                    | "drain"
                    | "age_ms"
                    | "active"
                    | "reserved"
                    | "reserved_mbps"
                    | "limit"
                    | "stream_bitrates"
            )
        });
        if self.config.revision() != revision {
            return None;
        }
        let snapshot = Arc::new(Snapshot {
            when,
            value,
            ready,
            vendor_id: None,
        });
        *cache = Some(snapshot.clone());
        Some(snapshot)
    }
}

/// Preserve raw media suffix and query bytes; only the configured stream name changes.
pub(super) fn vendor_location(peer: &Value, name: &str, path: &str, query: &str) -> Option<String> {
    let remote = crate::config::vendor_peers::remote_name(peer, name)?;
    let prefix = format!("/{name}");
    let boundary = path.char_indices().find_map(|(i, c)| {
        (i > 0
            && c == '/'
            && percent_encoding::percent_decode_str(&path[..i])
                .decode_utf8()
                .ok()
                .as_deref()
                == Some(prefix.as_str()))
        .then_some(i)
    })?;
    let remote = remote
        .split('/')
        .map(|s| {
            percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
        })
        .collect::<Vec<_>>()
        .join("/");
    let public = peer["public_payload_url"].as_str()?;
    let clean = App::http_viewer_query(query);
    Some(format!(
        "{}/{remote}{}{}{}",
        public.trim_end_matches('/'),
        &path[boundary..],
        if clean.is_empty() { "" } else { "?" },
        clean
    ))
}

impl App {
    pub(super) fn http_load(
        &self,
        peer: &Value,
        snapshot: &Snapshot,
        ready: bool,
        elapsed: u64,
        cost: f64,
        revision: u64,
    ) -> Option<NodeLoad> {
        let id = peer["hostname"].as_str()?;
        if !crate::config::vendor_peers::is_vendor(peer) {
            return NodeLoad::from_telemetry(
                id,
                &snapshot.value,
                ready,
                peer["drain"] == true,
                elapsed,
                cost,
            );
        }
        let mut holds = self.http_routes.holds.lock().ok()?;
        if self.config.revision() != revision {
            return None;
        }
        if holds.0 != revision {
            *holds = (revision, HashMap::new());
        }
        let pending = holds.1.entry(id.into()).or_default();
        pending.retain(|(until, _)| *until > Instant::now());
        let mut value = snapshot.value.clone();
        value["reserved_mbps"] = json!(pending.iter().map(|(_, rate)| rate).sum::<f64>());
        value["reserved"] = json!(pending.len());
        NodeLoad::from_telemetry(id, &value, ready, peer["drain"] == true, elapsed, cost)
    }
    pub(super) fn hold_vendor_http(
        &self,
        peer: &Value,
        snapshot: &Snapshot,
        cost: f64,
        revision: u64,
    ) -> bool {
        let Some(id) = peer["hostname"].as_str() else {
            return false;
        };
        let Ok(mut holds) = self.http_routes.holds.lock() else {
            return false;
        };
        if self.config.revision() != revision {
            return false;
        }
        if holds.0 != revision {
            *holds = (revision, HashMap::new());
        }
        let now = Instant::now();
        let pending = holds.1.entry(id.into()).or_default();
        pending.retain(|(until, _)| *until > now);
        if pending.len() >= 10000 {
            return false;
        }
        let mut value = snapshot.value.clone();
        value["reserved_mbps"] = json!(pending.iter().map(|(_, rate)| rate).sum::<f64>());
        value["reserved"] = json!(pending.len());
        let elapsed = now
            .duration_since(snapshot.when)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let Some(load) =
            NodeLoad::from_telemetry(id, &value, false, peer["drain"] == true, elapsed, cost)
        else {
            return false;
        };
        if crate::cluster::select(&[load], 0.0).is_none() {
            return false;
        }
        pending.push((now + Duration::from_secs(5), cost));
        true
    }
}
