//! App-owned direct vendor playback. No implicit native/vendor failover.
mod response;
use crate::{
    media::{SharedHlsOptions, SharedHlsSessions},
    server::App,
    source_directory::{
        allows,
        modern_coordinator::{
            CoordinatedCandidate, CoordinatedInventory, ModernSourceCoordinator, SharedInputs,
            SourceHandle,
        },
    },
};
pub(crate) use response::HttpRequest;
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

pub(crate) enum Selection {
    Native,
    Vendor { key: String, revision: u64 },
    Excluded,
    Ambiguous,
}
/// Namespaces reserve exclusions too: no weaker native route on vendor failure.
pub(crate) fn select(root: &Value, name: &str, revision: u64) -> Selection {
    if root["streams"]
        .as_array()
        .is_some_and(|streams| streams.iter().any(|stream| stream["name"] == name))
    {
        return Selection::Native;
    }
    let mut selected = None;
    for source in root["sources"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| s["flussonix_source_kind"] == "flussonic")
    {
        let namespace = source["prefix"].as_str().is_none_or(|prefix| {
            name.strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('/') && tail.len() > 1)
        });
        if !namespace {
            continue;
        }
        if selected.replace(source).is_some() {
            return Selection::Ambiguous;
        }
    }
    match selected {
        None => Selection::Native,
        Some(source) if !allows(source, name) => Selection::Excluded,
        Some(source) => Selection::Vendor {
            key: crate::config::item_key("sources", source)
                .expect("validated source")
                .to_owned(),
            revision,
        },
    }
}
pub(crate) fn reserved(root: &Value, name: &str) -> bool {
    !matches!(select(root, name, 0), Selection::Native)
}
#[derive(Debug, Clone, Copy)]
enum Failure {
    Closed,
    Unavailable,
    Absent,
}
#[derive(Default)]
struct Cache {
    handle: Option<SourceHandle>,
    inventory: Option<CoordinatedInventory>,
    next_probe: Option<Instant>,
}
#[derive(Default)]
struct Caches {
    revision: u64,
    sources: HashMap<String, Arc<Mutex<Cache>>>,
}
pub(crate) struct Runtime {
    owner: Arc<ModernSourceCoordinator>,
    inputs: Arc<SharedInputs>,
    sessions: Arc<SharedHlsSessions>,
    caches: Mutex<Caches>,
    queries: Semaphore,
    closed: CancellationToken,
    claimed: AtomicBool,
}
struct RunGuard<'a>(&'a Runtime);
impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.0.closed.cancel();
        self.0.sessions.shutdown();
        self.0.inputs.shutdown();
    }
}
impl Runtime {
    pub(crate) fn new(owner: Arc<ModernSourceCoordinator>) -> Self {
        let inputs = Arc::new(SharedInputs::new(owner.clone()));
        Self {
            owner,
            sessions: Arc::new(
                SharedHlsSessions::new(inputs.clone(), SharedHlsOptions::default())
                    .expect("valid default session options"),
            ),
            inputs,
            caches: Mutex::new(Caches::default()),
            queries: Semaphore::new(4),
            closed: CancellationToken::new(),
            claimed: AtomicBool::new(false),
        }
    }
    pub(crate) async fn run(&self, cancel: CancellationToken) -> Result<(), String> {
        if self.claimed.swap(true, Ordering::AcqRel) {
            return Err("vendor playback owner already started".into());
        }
        let _guard = RunGuard(self);
        self.sessions
            .clone()
            .run(cancel)
            .await
            .map_err(|_| "vendor playback owner unavailable".into())
    }
    fn running(&self) -> bool {
        !self.closed.is_cancelled() && self.sessions.running()
    }
    async fn candidate(
        &self,
        app: &App,
        key: &str,
        revision: u64,
        name: &str,
    ) -> Result<CoordinatedCandidate, Failure> {
        if !self.running() {
            return Err(Failure::Closed);
        }
        // Managed routes carry the latest atomically renewed source lease.
        if let Ok(candidate) = self.inputs.current_candidate(name)
            && candidate.source_key() == key
            && candidate.config_revision() == revision
        {
            return Ok(candidate);
        }
        let work = async {
            let slot = {
                let mut caches = self.caches.lock().await;
                if app.config.revision() != revision {
                    return Err(Failure::Unavailable);
                }
                if caches.revision != revision {
                    caches.sources.clear();
                    caches.revision = revision;
                }
                if caches.sources.len() >= 64 && !caches.sources.contains_key(key) {
                    return Err(Failure::Unavailable);
                }
                caches.sources.entry(key.to_owned()).or_default().clone()
            };
            let mut cache = slot.lock().await;
            // Recheck after a concurrent cold request, without repeated discovery.
            if let Ok(candidate) = self.inputs.current_candidate(name)
                && candidate.source_key() == key
                && candidate.config_revision() == revision
            {
                return Ok(candidate);
            }
            if let Some(rows) = &cache.inventory
                && rows.expires_at() > Instant::now()
            {
                match rows.candidate(name) {
                    Ok(candidate) if self.owner.publish(&candidate, |_, _| ()).is_ok() => {
                        return Ok(candidate);
                    }
                    Err(_) => {
                        return Err(if rows.names().any(|n| n == name) {
                            Failure::Unavailable
                        } else {
                            Failure::Absent
                        });
                    }
                    _ => {}
                }
            }
            if cache.next_probe.is_some_and(|next| next > Instant::now()) {
                return Err(Failure::Unavailable);
            }
            cache.next_probe = Some(Instant::now() + Duration::from_millis(500));
            let _permit = self.queries.acquire().await.map_err(|_| Failure::Closed)?;
            let rows = if let Some(handle) = &cache.handle
                && self.inputs.source_in_use(handle)
            {
                // Ordinary inventory replaces tickets and would revoke siblings.
                self.inputs
                    .renew(handle, Duration::from_secs(30))
                    .await
                    .map_err(|_| Failure::Unavailable)?
                    .inventory
            } else {
                let captured = self.owner.capture(key).map_err(|_| Failure::Unavailable)?;
                if captured.config_revision() != revision {
                    return Err(Failure::Unavailable);
                }
                let prepared = captured
                    .prepare(Duration::from_secs(3))
                    .map_err(|_| Failure::Unavailable)?;
                // Serialize install with attachment, closing the check/install race.
                let handle = self
                    .inputs
                    .install_idle(prepared)
                    .map_err(|_| Failure::Unavailable)?;
                cache.handle = Some(handle.clone());
                self.owner
                    .inventory(&handle, Duration::from_secs(30))
                    .await
                    .map_err(|_| Failure::Unavailable)?
            };
            let candidate = rows.candidate(name).map_err(|_| {
                if rows.names().any(|n| n == name) {
                    Failure::Unavailable
                } else {
                    Failure::Absent
                }
            });
            cache.inventory = Some(rows);
            candidate
        };
        tokio::select! {biased;_=self.closed.cancelled()=>Err(Failure::Closed),result=tokio::time::timeout(Duration::from_secs(4),work)=>result.unwrap_or(Err(Failure::Unavailable))}
    }
}
