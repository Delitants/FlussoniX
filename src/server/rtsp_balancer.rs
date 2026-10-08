//! Native RTSP placement. Cached telemetry is advisory; the CDN owns admission.
use super::*;
use futures_util::StreamExt;
use std::collections::HashSet;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(super) enum Kind {
    Http,
    Rtsp { secure: bool, token_hash: String },
}
pub(super) fn kind(body: &Value) -> Result<Kind, String> {
    match body.get("protocol") {
        None => Ok(Kind::Http),
        Some(Value::String(p)) if p == "http" => Ok(Kind::Http),
        Some(Value::String(p)) if matches!(p.as_str(), "rtsp" | "rtsps") => {
            let hash = body["token_hash"]
                .as_str()
                .filter(|h| h.len() == 64 && h.bytes().all(|c| c.is_ascii_hexdigit()))
                .ok_or("RTSP admission requires token_hash SHA256 hex")?;
            Ok(Kind::Rtsp {
                secure: p == "rtsps",
                token_hash: hash.to_ascii_lowercase(),
            })
        }
        _ => Err("unknown admission protocol".into()),
    }
}
fn token_hash(viewer: &ViewerRequest) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(viewer.token.as_bytes()))
}
pub(super) fn strip_ticket(qs: &str) -> (Option<String>, String) {
    let mut ticket = None;
    let clean = qs
        .split('&')
        .filter(|part| {
            if let Some((key, value)) = url::form_urlencoded::parse(part.as_bytes()).next() {
                if key == "flussonix_ticket" {
                    ticket = Some(value.into_owned());
                    return false;
                }
            }
            true
        })
        .collect::<Vec<_>>()
        .join("&");
    (ticket, clean)
}
struct Snapshot {
    when: Instant,
    value: Value,
    ready: HashSet<String>,
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
    pub(super) async fn rtsp_routing_node(&self) -> Value {
        let mut load = self.load_node().await;
        load["ready"] = json!(self.media.rtsp_ready_names().await);
        load
    }
    async fn rtsp_snapshot(&self, peer: &Value, revision: u64) -> Option<Arc<Snapshot>> {
        let id = peer["hostname"].as_str()?;
        let slot = {
            let mut slots = self.rtsp_routes.slots.lock().await;
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
        *cache = None; // Expired observations never survive a failed refresh.
        let _network = self.rtsp_routes.network.acquire().await.ok()?;
        if self.config.revision() != revision {
            return None;
        }
        let client = self.cluster_client(peer).ok()?;
        let api = peer["api_url"].as_str()?;
        let key = peer["cluster_key"]
            .as_str()
            .unwrap_or(&self.options.peer_key);
        // Start the observation clock before the request. Response latency must
        // consume freshness rather than granting an old output rate a new age.
        let when = Instant::now();
        let mut response = client
            .get(format!(
                "{}/flussonix/api/v1/rtsp-routing",
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
        let value: Value = serde_json::from_slice(&bytes).ok()?;
        let ready = value["ready"]
            .as_array()?
            .iter()
            .map(|v| v.as_str().map(str::to_owned))
            .collect::<Option<HashSet<_>>>()?;
        if self.config.revision() != revision {
            return None;
        }
        let snapshot = Arc::new(Snapshot { when, value, ready });
        *cache = Some(snapshot.clone());
        Some(snapshot)
    }
    pub(super) async fn rtsp_consume(
        &self,
        ticket: &str,
        viewer: &ViewerRequest,
        secure: bool,
    ) -> Result<(), u16> {
        let mut ledger = self.reservations.lock().await;
        ledger.retain(|_, r| r.expires > Instant::now());
        let valid = ledger.get(ticket).is_some_and(|r| r.stream == viewer.name && matches!(&r.kind,Kind::Rtsp{secure:s,token_hash:h} if *s==secure && *h==token_hash(viewer)));
        if !valid {
            return Err(503);
        }
        ledger.remove(ticket);
        Ok(())
    }
    pub(super) async fn rtsp_place(
        &self,
        viewer: &ViewerRequest,
        source: &url::Url,
        secure: bool,
        grant: &Grant,
        revision: u64,
    ) -> Result<String, u16> {
        let work = self.rtsp_place_inner(viewer, source, secure, grant, revision);
        tokio::select! { biased; _=grant.cancelled()=>Err(403), result=tokio::time::timeout(Duration::from_secs(8),work)=>result.map_err(|_|503u16)? }
    }
    async fn rtsp_place_inner(
        &self,
        viewer: &ViewerRequest,
        source: &url::Url,
        secure: bool,
        grant: &Grant,
        revision: u64,
    ) -> Result<String, u16> {
        let root = self.config.snapshot();
        let peers = root["peers"].as_array().cloned().unwrap_or_default();
        if peers.len() > 64 || self.config.revision() != revision {
            return Err(503);
        }
        let calls = peers.into_iter().map(|peer| async move {
            let public = if secure {
                peer["flussonix_rtsps_url"].as_str()
            } else {
                peer["flussonix_rtsp_url"]
                    .as_str()
                    .or(peer["flussonix_rtsps_url"].as_str())
            }?;
            let mut target = crate::rtsp::redirect::destination(public)?;
            let encrypted = target.scheme() == "rtsps";
            if secure && !encrypted {
                return None;
            }
            let port = target.port().unwrap_or(if encrypted { 322 } else { 554 });
            let source_port = source.port().unwrap_or(if secure { 322 } else { 554 });
            if encrypted == secure
                && target.host_str()?.eq_ignore_ascii_case(source.host_str()?)
                && port == source_port
            {
                return None;
            }
            let name = viewer
                .name
                .split('/')
                .map(|s| {
                    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC)
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join("/");
            target.set_path(&format!("/{name}"));
            target.set_query((!viewer.qs.is_empty()).then_some(viewer.qs.as_str()));
            let probe = append_ticket(target.as_str(), "00000000-0000-0000-0000-000000000000");
            crate::rtsp::redirect::destination(&probe)?;
            let snapshot = self.rtsp_snapshot(&peer, revision).await?;
            let n = &snapshot.value;
            if !matches!(n["role"].as_str(), Some("cdn" | "standalone"))
                || !n["rtsp_publication"][if encrypted { "rtsps" } else { "rtsp" }].is_string()
            {
                return None;
            }
            let load = NodeLoad::from_telemetry(
                peer["hostname"].as_str()?,
                n,
                snapshot.ready.contains(&viewer.name),
                peer["drain"] == true,
                snapshot.when.elapsed().as_millis().try_into().ok()?,
                2.0,
            )?;
            (load.age_ms <= 10000).then_some((snapshot, peer, target, encrypted))
        });
        let mut calls = futures_util::stream::iter(calls).buffer_unordered(8);
        let snapshot_deadline = tokio::time::sleep(Duration::from_millis(4500));
        tokio::pin!(snapshot_deadline);
        let mut observations = Vec::new();
        loop {
            let result = tokio::select! { biased; _=&mut snapshot_deadline=>break, result=calls.next()=>result };
            let Some(result) = result else { break };
            if let Some(observation) = result {
                observations.push(observation);
            }
        }
        drop(calls); // Cancel unfinished probes; preserve time for admission.
        let mut attempted = HashSet::new();
        let mut turn = None;
        loop {
            let observed_at = Instant::now();
            let elapsed = |snapshot: &Snapshot| {
                observed_at
                    .duration_since(snapshot.when)
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX)
            };
            let valid = observations
                .iter()
                .filter(|(snapshot, peer, _, _)| {
                    peer["hostname"]
                        .as_str()
                        .and_then(|id| {
                            NodeLoad::from_telemetry(
                                id,
                                &snapshot.value,
                                snapshot.ready.contains(&viewer.name),
                                peer["drain"] == true,
                                elapsed(snapshot),
                                crate::cluster::FALLBACK_MBPS,
                            )
                        })
                        .is_some_and(|load| load.age_ms <= 10000)
                })
                .collect::<Vec<_>>();
            let bitrate_mbps = valid
                .iter()
                .filter_map(|(snapshot, _, _, _)| {
                    crate::cluster::observed_bitrate(
                        &snapshot.value,
                        &viewer.name,
                        elapsed(snapshot),
                    )
                })
                .fold(crate::cluster::FALLBACK_MBPS, f64::max);
            if !bitrate_mbps.is_finite() || bitrate_mbps > crate::cluster::MAX_HINT_MBPS {
                return Err(503);
            }
            let mut nodes = Vec::new();
            for (snapshot, peer, _, _) in valid {
                if peer["hostname"]
                    .as_str()
                    .is_some_and(|id| attempted.contains(id))
                {
                    continue;
                }
                if let Some(load) = NodeLoad::from_telemetry(
                    peer["hostname"].as_str().unwrap(),
                    &snapshot.value,
                    snapshot.ready.contains(&viewer.name),
                    peer["drain"] == true,
                    elapsed(snapshot),
                    bitrate_mbps,
                ) {
                    nodes.push(load);
                }
            }
            let Some(id) = self.routing_rotation.select(&nodes, 0.0, &mut turn) else {
                break;
            };
            attempted.insert(id.clone());
            if grant.is_cancelled() {
                return Err(403);
            }
            if self.config.revision() != revision {
                return Err(503);
            }
            let (_, peer, target, encrypted) = observations
                .iter()
                .find(|(_, peer, _, _)| peer["hostname"] == id)
                .unwrap();
            let api = peer["api_url"].as_str().unwrap();
            let key = peer["cluster_key"]
                .as_str()
                .unwrap_or(&self.options.peer_key);
            if let Ok(client) = self.cluster_client(peer) {
                let response=client.post(format!("{}/flussonix/api/v1/admit",api.trim_end_matches('/'))).header("X-Flussonix-Peer",key).json(&json!({"name":viewer.name,"protocol":if *encrypted {"rtsps"} else {"rtsp"},"token_hash":token_hash(viewer),"bitrate_mbps":bitrate_mbps})).send().await;
                if let Ok(response) = response {
                    if response.status().is_success()
                        && response.content_length().is_none_or(|n| n <= 16384)
                    {
                        // Bound streamed admission replies as well as announced lengths.
                        let body = bounded_admission(response).await;
                        if let Some(ticket) = body
                            .as_ref()
                            .and_then(|b| b["ticket"].as_str())
                            .filter(|s| uuid::Uuid::parse_str(s).is_ok())
                        {
                            if grant.is_cancelled() {
                                return Err(403);
                            }
                            if self.config.revision() != revision {
                                return Err(503);
                            }
                            let location = append_ticket(target.as_str(), ticket);
                            if crate::rtsp::redirect::destination(&location).is_some() {
                                return Ok(location);
                            }
                        }
                    }
                }
            }
        }
        Err(503)
    }
}
async fn bounded_admission(mut response: reqwest::Response) -> Option<Value> {
    let mut bytes = vec![];
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len().checked_add(chunk.len())? > 16384 {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}
fn append_ticket(target: &str, ticket: &str) -> String {
    format!(
        "{target}{}flussonix_ticket={ticket}",
        if target.contains('?') { "&" } else { "?" }
    )
}
