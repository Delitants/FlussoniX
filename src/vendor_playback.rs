//! App-owned direct vendor playback. No implicit native/vendor failover.
mod activation;
#[cfg(test)]
mod catalog_consumption_tests;
mod recovery;
mod replicas;
mod response;
mod rtsp;
mod srt;
mod srt_lookup;
use crate::{
    media::{SharedHlsOptions, SharedHlsSessions},
    server::App,
    source_directory::{
        allows,
        modern_coordinator::{
            CoordinatedCandidate, CoordinatedInventory, CoordinationFailure,
            ModernSourceCoordinator, SharedInputs, SourceHandle,
        },
    },
};
pub(crate) use response::HttpRequest;
pub(crate) use rtsp::RtspRequest;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

// Leave bounded WAN headroom for config/catalog/config, and one extra second
// for serialized selection. The underlying byte, page and lease bounds remain.
const DISCOVERY_BUDGET: Duration = Duration::from_secs(5);
const SOURCE_SELECTION_BUDGET: Duration = Duration::from_secs(6);
// Selected source plus two alternative waves under the four-query semaphore.
// Contention may still exhaust this bound; no lease or authority is extended.
const GROUP_SELECTION_BUDGET: Duration = Duration::from_secs(16);
const RECOVERY_JOB_BUDGET: Duration = Duration::from_secs(20);
pub(crate) const SRT_ADMISSION_BUDGET: Duration = Duration::from_secs(20);

// Live M4F windows are short: a long exponential pause after the origin comes
// back can lose segments that were still available to a direct viewer. Spread
// repeat probes across names so a recovering catalog does not reconnect at once.
fn private_retry_delay(source: &str, name: &str, streak: u32) -> Duration {
    let delay = crate::recovery::retry_delay(streak);
    if !source.starts_with("m4f://") && !source.starts_with("m4fs://") {
        return delay;
    }
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hash);
    delay.min(Duration::from_secs(2)) + Duration::from_millis(hash.finish() % 500)
}

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
    let mut selected: Option<&Value> = None;
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
        if let Some(first) = selected {
            if first["flussonix_source_group"].as_str().is_none()
                || first["flussonix_source_group"] != source["flussonix_source_group"]
                || first["flussonix_content_id"] != source["flussonix_content_id"]
            {
                return Selection::Ambiguous;
            }
        } else {
            selected = Some(source);
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
    revision: u64,
    handle: Option<SourceHandle>,
    inventory: Option<Result<Arc<CoordinatedInventory>, CoordinationFailure>>,
    next_probe: Option<Instant>,
}
impl Cache {
    fn at_revision(&mut self, revision: u64) {
        if self.revision != revision {
            self.revision = revision;
            self.inventory = None;
            self.next_probe = None;
        }
    }
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
    outputs: std::sync::Mutex<(u64, HashMap<String, Value>)>,
    groups: std::sync::Mutex<replicas::Groups>,
    recovery: std::sync::Mutex<recovery::State>,
    activation: std::sync::Mutex<activation::State>,
    queries: Semaphore,
    closed: CancellationToken,
    claimed: AtomicBool,
}
struct RunGuard<'a>(&'a Runtime);
impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.0.closed.cancel();
        self.0.sessions.shutdown();
        for group in self.0.groups.lock().unwrap().groups.values() {
            group.selector.shutdown();
        }
        self.0.recovery.lock().unwrap().entries.clear();
        self.0.activation.lock().unwrap().demands.clear();
        self.0.inputs.shutdown();
    }
}
impl Runtime {
    async fn source_cache_slot(
        &self,
        app: &App,
        key: &str,
        revision: u64,
    ) -> Result<Arc<Mutex<Cache>>, Failure> {
        let needs_revision = self.caches.lock().await.revision != revision;
        let configured = if needs_revision {
            Some(
                app.config
                    .at_revision(revision, |root| {
                        root["sources"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|source| crate::config::item_key("sources", source))
                            .map(str::to_owned)
                            .collect::<HashSet<_>>()
                    })
                    .ok_or(Failure::Unavailable)?,
            )
        } else {
            None
        };
        let mut caches = self.caches.lock().await;
        if app.config.revision() != revision {
            return Err(Failure::Unavailable);
        }
        if caches.revision != revision {
            let configured = configured.ok_or(Failure::Unavailable)?;
            caches
                .sources
                .retain(|source, _| configured.contains(source));
            caches.revision = revision;
        }
        if caches.sources.len() >= 64 && !caches.sources.contains_key(key) {
            return Err(Failure::Unavailable);
        }
        Ok(caches.sources.entry(key.to_owned()).or_default().clone())
    }
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
            outputs: std::sync::Mutex::new((0, HashMap::new())),
            groups: std::sync::Mutex::new(replicas::Groups::default()),
            recovery: std::sync::Mutex::new(recovery::State::default()),
            activation: std::sync::Mutex::new(activation::State::default()),
            queries: Semaphore::new(4),
            closed: CancellationToken::new(),
            claimed: AtomicBool::new(false),
        }
    }
    pub(crate) async fn run(&self, app: &App, cancel: CancellationToken) -> Result<(), String> {
        if self.claimed.swap(true, Ordering::AcqRel) {
            return Err("vendor playback owner already started".into());
        }
        let _guard = RunGuard(self);
        let cancel = cancel.child_token();
        let sessions = self.sessions.clone().run(cancel.clone());
        let recovery = self.maintain_replicas(app, cancel.clone());
        let activation = self.poll_static_catalogs(app, cancel.clone());
        tokio::pin!(sessions, recovery, activation);
        tokio::select! {
            result=&mut sessions=>{cancel.cancel();tokio::join!(recovery,activation);result.map_err(|_|"vendor playback owner unavailable".into())},
            _=&mut recovery=>{cancel.cancel();let(result,_)=tokio::join!(sessions,activation);result.map_err(|_|"vendor playback owner unavailable".into())},
            _=&mut activation=>{cancel.cancel();let(result,_)=tokio::join!(sessions,recovery);result.map_err(|_|"vendor playback owner unavailable".into())},
        }
    }
    fn running(&self) -> bool {
        !self.closed.is_cancelled() && self.sessions.running()
    }
    fn output_settings(&self, app: &App, key: &str, revision: u64) -> Result<Value, Failure> {
        // Config -> immutable profile cache; never hold either lock across I/O.
        app.config
            .at_revision(revision, |root| {
                let mut cache = self.outputs.lock().map_err(|_| Failure::Unavailable)?;
                if cache.0 != revision {
                    cache.0 = revision;
                    cache.1.clear();
                }
                if let Some(settings) = cache.1.get(key) {
                    return Ok(settings.clone());
                }
                if cache.1.len() >= 64 {
                    return Err(Failure::Unavailable);
                }
                let source = root["sources"]
                    .as_array()
                    .and_then(|rows| {
                        rows.iter()
                            .find(|row| crate::config::item_key("sources", row) == Some(key))
                    })
                    .ok_or(Failure::Unavailable)?;
                let settings = crate::vendor_output::resolve(root, source)
                    .map_err(|_| Failure::Unavailable)?;
                cache.1.insert(key.to_owned(), settings.clone());
                Ok(settings)
            })
            .ok_or(Failure::Unavailable)?
    }
    /// Publish an already current managed/CDN or active LB catalog policy.
    /// This never discovers a source, attaches media, or creates a viewer grant.
    pub(crate) fn publish_current_policy(&self, app: &App, name: &str) -> bool {
        if !self.running() || app.modern_source_coordinator().is_err() {
            return false;
        }
        let revision = app.config.revision();
        let Selection::Vendor { key, revision } = select(&app.config.snapshot(), name, revision)
        else {
            return false;
        };
        let group = self.cached_group(app, &key, revision);
        let actual_key = match &group {
            Some(group) => match group.selector.status(name) {
                Ok(status) if group.keys.contains(&status.selected_source) => {
                    status.selected_source
                }
                _ => return false,
            },
            None => key.clone(),
        };
        let candidate = if app.options.role == "lb" {
            if !app
                .playback_auth
                .snapshots()
                .iter()
                .any(|row| row["name"] == name)
            {
                return false;
            }
            self.catalog_candidate(&actual_key, revision, name)
        } else {
            self.inputs.current_candidate(name).ok()
        };
        let Some(candidate) = candidate else {
            return false;
        };
        if candidate.source_key() != actual_key || candidate.config_revision() != revision {
            return false;
        }
        if let Some(group) = group {
            return group
                .selector
                .choice_for(candidate, name)
                .is_ok_and(|choice| choice.publish_policy(&app.playback_auth));
        }
        self.owner
            .publish(&candidate, |view, _| {
                app.playback_auth
                    .publish(view.name, Some(view.policy.clone()))
                    .is_some()
            })
            .unwrap_or(false)
    }
    fn catalog_candidate(
        &self,
        key: &str,
        revision: u64,
        name: &str,
    ) -> Option<CoordinatedCandidate> {
        // Admin reauth is read-only: never wait for or start a catalog refresh.
        // Release cache locks before the coordinator's publication fence.
        let slot = {
            let caches = self.caches.try_lock().ok()?;
            if caches.revision != revision {
                return None;
            }
            caches.sources.get(key)?.clone()
        };
        let cache = slot.try_lock().ok()?;
        if cache.revision != revision {
            return None;
        }
        cache
            .inventory
            .as_ref()?
            .as_ref()
            .ok()?
            .candidate(name)
            .ok()
    }
    async fn direct_candidate(
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
            let slot = self.source_cache_slot(app, key, revision).await?;
            let mut cache = slot.lock().await;
            cache.at_revision(revision);
            // Recheck after a concurrent cold request, without repeated discovery.
            if let Ok(candidate) = self.inputs.current_candidate(name)
                && candidate.source_key() == key
                && candidate.config_revision() == revision
            {
                return Ok(candidate);
            }
            if let Some(Ok(rows)) = &cache.inventory
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
            cache.inventory = None;
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
                    .prepare(DISCOVERY_BUDGET)
                    .map_err(|_| Failure::Unavailable)?;
                // Serialize install with attachment, closing the check/install race.
                let handle = self
                    .inputs
                    .install_idle(prepared, cache.handle.as_ref())
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
            cache.inventory = Some(Ok(Arc::new(rows)));
            candidate
        };
        tokio::select! {biased;_=self.closed.cancelled()=>Err(Failure::Closed),result=tokio::time::timeout(SOURCE_SELECTION_BUDGET,work)=>result.unwrap_or(Err(Failure::Unavailable))}
    }
}
