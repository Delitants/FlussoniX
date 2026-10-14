//! HTTP native routing observations. Advisory snapshots never grant admission.
use super::*;
use std::collections::HashSet;
use tokio::sync::Semaphore;

pub(super) struct Snapshot {
    pub when: Instant,
    pub value: Value,
    pub ready: HashSet<String>,
    pub vendor: Option<super::vendor_http_state::Stamp>,
}
type Slot = Arc<Mutex<Option<Arc<Snapshot>>>>;
pub(super) struct Registry {
    slots: Mutex<(u64, HashMap<String, Slot>)>,
    network: Semaphore,
    vendor_network: Semaphore,
    pub(super) vendor: std::sync::Mutex<super::vendor_http_state::Book>,
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            slots: Mutex::new((0, HashMap::new())),
            network: Semaphore::new(8),
            vendor_network: Semaphore::new(64),
            vendor: std::sync::Mutex::new(Default::default()),
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

        let network = if vendor {
            &self.http_routes.vendor_network
        } else {
            &self.http_routes.network
        };
        let _network = network.acquire().await.ok()?;
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
            let stamp = self.begin_vendor_http(peer, remote, revision)?;
            let when = Instant::now();
            let observation = routing::observe(
                peer["api_url"].as_str()?,
                peer["cluster_key"].as_str()?,
                peer["flussonix_tls_ca"].as_str().map(Path::new),
                remote,
                &local_ids,
            )
            .await;
            let (stamp, mut observation) =
                self.record_vendor_http(stamp, when, observation, revision)?;
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
                vendor: Some(stamp),
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
            vendor: None,
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

#[cfg(test)]
mod current_sample_tests {
    use super::*;
    #[tokio::test]
    async fn final_vendor_hold_rechecks_newer_telemetry_before_using_held_snapshot() {
        use axum::routing::get;
        let cpu = Arc::new(AtomicU64::new(10));
        let current = cpu.clone();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", tcp.local_addr().unwrap());
        let service=Router::new().route("/streamer/api/v3/config",get(move||{let cpu=current.clone();async move{json_response(json!({"stats":{"id":"owned-vendor-cdn","now":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()as u64,"cpu_usage":cpu.load(Ordering::SeqCst),"memory_usage":20,"output_kbit":10000,"total_bandwidth":100000000,"total_clients":0,"streamer_status":"running"}}))}})).route("/streamer/api/v3/streams/{name}",get(||async{json_response(json!({"name":"owned","named_by":"remote","stats":{"running":true}}))}));
        let cdn = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("c.json"),
            dir.path().join("media"),
            Options {
                role: "lb".into(),
                admin_password: "owned-admin".into(),
                peer_key: "owned-native-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let peer = json!({"hostname":"edge","api_url":api,"public_payload_url":"https://edge.invalid","cluster_key":"owned-vendor-cluster-key","flussonix_peer_kind":"flussonic","flussonix_stream_map":[{"name":"owned","remote_name":"owned"}]});
        app.config.put("peers", "edge", peer.clone()).unwrap();
        let rev = app.config.revision();
        let held = app.http_snapshot(&peer, "owned", rev).await.unwrap();
        assert!(app.http_load(&peer, &held, true, 0, 2.0, rev).is_some());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        cpu.store(95, Ordering::SeqCst);
        let _new = app.http_snapshot(&peer, "owned", rev).await.unwrap();
        assert!(
            !app.hold_vendor_http(&peer, &held, 2.0, rev),
            "pending placement used superseded healthy CPU sample"
        );
        cdn.abort();
        let _ = cdn.await;
    }
}
