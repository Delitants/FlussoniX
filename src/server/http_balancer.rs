//! HTTP native routing observations. Advisory snapshots never grant admission.
use super::*;
use std::collections::HashSet;
use tokio::sync::Semaphore;

pub(super) struct Snapshot {
    pub when: Instant,
    pub value: Value,
    pub ready: HashSet<String>,
}
type Slot = Arc<Mutex<Option<Arc<Snapshot>>>>;
pub(super) struct Registry {
    slots: Mutex<(u64, HashMap<String, Slot>)>,
    network: Semaphore,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            slots: Mutex::new((0, HashMap::new())),
            network: Semaphore::new(8),
        }
    }
}
impl App {
    pub(super) async fn http_snapshot(&self, peer: &Value, revision: u64) -> Option<Arc<Snapshot>> {
        let id = peer["hostname"].as_str()?;
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
                .entry(id.into())
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
        *cache = None;
        let _network = self.http_routes.network.acquire().await.ok()?;
        if self.config.revision() != revision {
            return None;
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
        let snapshot = Arc::new(Snapshot { when, value, ready });
        *cache = Some(snapshot.clone());
        Some(snapshot)
    }
}
