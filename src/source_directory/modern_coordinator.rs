//! Opt-in modern source lifecycle. Native routing never selects these handles.
//! Lock order: ConfigStore -> coordinator -> SourceAuthority; no lock across I/O.
mod admission;
mod replicas;
pub(crate) use admission::SharedFeed;
pub use replicas::{
    ReplicaAdmission, ReplicaAdmissionOutcome, ReplicaChoice, ReplicaFailure, ReplicaLookup,
    ReplicaMember, ReplicaPullFailure, ReplicaSelector, ReplicaStatus,
};
mod relay;
use super::{
    flussonic::modern::ObservedServerId,
    flussonic_config::{
        CandidateView, LeaseFailure, ModernLeasedCandidate, ModernLeasedInventory,
        ProjectionFailure, SourceAuthority,
    },
};
use crate::{config::ConfigStore, m4_ingest::flussonic::MediaInput};
pub use admission::{
    AdmissionFailure, AdmissionOutcome, AdmittedCandidate, PrivatePullFailure, PrivatePullOptions,
    SharedFailure, SharedInput, SharedInputAttachment, SharedInputOptions, SharedInputs,
    SharedRenewal, SharedSubscription, SharedViewer,
};
use relay::RelayRoutes;
pub use relay::{RelayCandidate, RelayFailure, RelayHop, RelayRoute};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

const MAX_ALIASES: usize = 16;
const MAX_SOURCES: usize = 64;

/// Generic diagnostics contain no configured identity, endpoint or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinationFailure {
    Invalid,
    Unsupported,
    Unavailable,
    Stale,
    Lease(LeaseFailure),
}
fn projection(error: ProjectionFailure) -> CoordinationFailure {
    match error {
        ProjectionFailure::Unsupported => CoordinationFailure::Unsupported,
        ProjectionFailure::Stale => CoordinationFailure::Stale,
        ProjectionFailure::Removed | ProjectionFailure::Unavailable => {
            CoordinationFailure::Unavailable
        }
        _ => CoordinationFailure::Invalid,
    }
}
struct LocalIdentity {
    ids: Vec<ObservedServerId>,
}
struct State {
    retired: bool,
    routes: Arc<RelayRoutes>,
    identity: Arc<LocalIdentity>,
    revision: u64,
    preparations: HashMap<String, Arc<()>>,
    sources: HashMap<String, Arc<SourceAuthority>>,
}
/// Owns the exact configuration store and startup local identity. No Debug or
/// serialization: internal preparations retain privileged source credentials.
/// This does not enable automatic vendor routing or worker reuse. Explicit
/// direct-source viewer admission is opt-in through admit/publish_admitted.
pub struct ModernSourceCoordinator {
    config: Arc<ConfigStore>,
    owner: Arc<()>,
    primary: ObservedServerId,
    state: Mutex<State>,
}
#[derive(Clone)]
struct Stamp {
    owner: Arc<()>,
    identity: Arc<LocalIdentity>,
    revision: u64,
    key: String,
    turn: Arc<()>,
}
/// Opaque snapshot. Client preparation is outside all publication locks.
pub struct SourcePreparation {
    stamp: Stamp,
    source: Value,
    backends: Value,
}
/// Fully prepared clients, still requiring ordered installation by their owner.
pub struct PreparedSource {
    stamp: Stamp,
    authority: Arc<SourceAuthority>,
}
/// Installed source generation. Cloning never renews a lease or reinstalls it.
#[derive(Clone)]
pub struct SourceHandle {
    stamp: Stamp,
    authority: Arc<SourceAuthority>,
}
/// Complete discovery bound to global config, identity and installed generation.
pub struct CoordinatedInventory {
    handle: SourceHandle,
    inventory: ModernLeasedInventory,
}
/// Opaque candidate; only its coordinator can enter a current publication fence.
pub struct CoordinatedCandidate {
    handle: SourceHandle,
    candidate: ModernLeasedCandidate,
}
impl SourceHandle {
    // Process-local grouping only; retained handles prevent pointer reuse.
    pub(crate) fn generation_key(&self) -> usize {
        Arc::as_ptr(&self.authority) as usize
    }
}
impl ModernSourceCoordinator {
    /// The owner supplies its actual startup identity, not a per-request claim.
    /// Primary identity changes require a new owner; aliases are explicit only.
    pub fn new(config: Arc<ConfigStore>, primary: &str) -> Result<Self, CoordinationFailure> {
        let primary = ObservedServerId::parse(primary).map_err(|_| CoordinationFailure::Invalid)?;
        Ok(Self {
            state: Mutex::new(State {
                retired: false,
                routes: Arc::new(RelayRoutes::default()),
                identity: Arc::new(LocalIdentity {
                    ids: vec![primary.clone()],
                }),
                revision: config.revision(),
                preparations: HashMap::new(),
                sources: HashMap::new(),
            }),
            primary,
            config,
            owner: Arc::new(()),
        })
    }
    fn state(&self) -> Result<MutexGuard<'_, State>, CoordinationFailure> {
        let state = self
            .state
            .lock()
            .map_err(|_| CoordinationFailure::Unavailable)?;
        if state.retired {
            return Err(CoordinationFailure::Stale);
        }
        Ok(state)
    }
    /// App's public config handle is retained for compatibility. Detect store
    /// substitution at its accessor and permanently retire this coordinator.
    /// No configuration lock is acquired here; this never reverses lock order.
    pub(crate) fn require_config(
        &self,
        config: &Arc<ConfigStore>,
    ) -> Result<(), CoordinationFailure> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CoordinationFailure::Unavailable)?;
        if !Arc::ptr_eq(&self.config, config) {
            state.retired = true;
            state.sources.clear();
            state.preparations.clear();
        }
        if state.retired {
            return Err(CoordinationFailure::Stale);
        }
        Ok(())
    }
    /// Validate atomically; duplicates (including primary/UUID equivalents) fail.
    /// Every accepted edit, even identical, invalidates all outstanding objects.
    pub fn replace_aliases(&self, aliases: &[&str]) -> Result<(), CoordinationFailure> {
        if aliases.len() > MAX_ALIASES {
            return Err(CoordinationFailure::Invalid);
        }
        let mut ids = vec![self.primary.clone()];
        for alias in aliases {
            let id = ObservedServerId::parse(alias).map_err(|_| CoordinationFailure::Invalid)?;
            if ids.contains(&id) {
                return Err(CoordinationFailure::Invalid);
            }
            ids.push(id);
        }
        let mut state = self.state()?;
        state.identity = Arc::new(LocalIdentity { ids });
        state.sources.clear();
        state.preparations.clear();
        Ok(())
    }
    /// Capture source/backends and revision atomically. A newer capture supersedes
    /// an older preparation for that key; installed handles survive until install.
    /// At most 64 distinct keys may have preparations in one configuration epoch.
    pub fn capture(&self, key: &str) -> Result<SourcePreparation, CoordinationFailure> {
        self.config.read(|root| {
            let source = root["sources"]
                .as_array()
                .and_then(|rows| {
                    rows.iter()
                        .find(|row| crate::config::item_key("sources", row) == Some(key))
                })
                .ok_or(CoordinationFailure::Unavailable)?
                .clone();
            let revision = self.config.revision();
            let mut state = self.state()?;
            if state.revision != revision {
                state.sources.clear();
                state.preparations.clear();
                state.revision = revision;
            }
            if !state.preparations.contains_key(key) && state.preparations.len() >= MAX_SOURCES {
                return Err(CoordinationFailure::Invalid);
            }
            let turn = Arc::new(());
            state.preparations.insert(key.to_owned(), turn.clone());
            Ok(SourcePreparation {
                stamp: Stamp {
                    owner: self.owner.clone(),
                    identity: state.identity.clone(),
                    revision,
                    key: key.to_owned(),
                    turn,
                },
                source,
                backends: root
                    .get("auth_backends")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([])),
            })
        })
    }
    fn check(&self, stamp: &Stamp, state: &State) -> Result<(), CoordinationFailure> {
        if !Arc::ptr_eq(&stamp.owner, &self.owner)
            || !Arc::ptr_eq(&stamp.identity, &state.identity)
            || stamp.revision != state.revision
        {
            return Err(CoordinationFailure::Stale);
        }
        Ok(())
    }
    /// Install only the latest capture from this exact owner/config/identity.
    /// Successful installation always replaces the source generation, even equal.
    pub fn install(&self, prepared: PreparedSource) -> Result<SourceHandle, CoordinationFailure> {
        self.config
            .at_revision(prepared.stamp.revision, |_| {
                let mut state = self.state()?;
                self.check(&prepared.stamp, &state)?;
                if !state
                    .preparations
                    .get(&prepared.stamp.key)
                    .is_some_and(|turn| Arc::ptr_eq(turn, &prepared.stamp.turn))
                {
                    return Err(CoordinationFailure::Stale);
                }
                state
                    .sources
                    .insert(prepared.stamp.key.clone(), prepared.authority.clone());
                Ok(SourceHandle {
                    stamp: prepared.stamp,
                    authority: prepared.authority,
                })
            })
            .ok_or(CoordinationFailure::Stale)?
    }
    fn at_handle<T>(
        &self,
        handle: &SourceHandle,
        read: impl FnOnce() -> Result<T, CoordinationFailure>,
    ) -> Result<T, CoordinationFailure> {
        self.at_handle_root(handle, |_| read())
    }
    fn at_handle_root<T>(
        &self,
        handle: &SourceHandle,
        read: impl FnOnce(&Value) -> Result<T, CoordinationFailure>,
    ) -> Result<T, CoordinationFailure> {
        self.config
            .at_revision(handle.stamp.revision, |root| {
                let state = self.state()?;
                self.check(&handle.stamp, &state)?;
                if !state
                    .sources
                    .get(&handle.stamp.key)
                    .is_some_and(|source| Arc::ptr_eq(source, &handle.authority))
                {
                    return Err(CoordinationFailure::Stale);
                }
                let result = read(root);
                drop(state);
                result
            })
            .ok_or(CoordinationFailure::Stale)?
    }
    /// Direct pending I/O is cancellable. Recheck global config/identity/source
    /// after both success and failure. Existing lease timing/latest-turn rules hold.
    pub async fn inventory(
        &self,
        handle: &SourceHandle,
        lifetime: Duration,
    ) -> Result<CoordinatedInventory, CoordinationFailure> {
        self.at_handle(handle, || Ok(()))?;
        let result = handle
            .authority
            .leased_modern_inventory_with_ids(&handle.stamp.identity.ids, lifetime)
            .await;
        self.at_handle(handle, || {
            Ok(CoordinatedInventory {
                handle: handle.clone(),
                inventory: result.map_err(CoordinationFailure::Lease)?,
            })
        })
    }
    /// Hold config -> coordinator -> source through synchronous closure entry and
    /// return. Never await/reenter any owner in the closure. Async work must return
    /// through a fresh fence; exported inputs/workers/sessions are not revoked.
    pub fn publish<T>(
        &self,
        candidate: &CoordinatedCandidate,
        commit: impl FnOnce(CandidateView<'_>, MediaInput) -> T,
    ) -> Result<T, CoordinationFailure> {
        self.at_handle(&candidate.handle, || {
            candidate
                .handle
                .authority
                .at_modern_leased_current(&candidate.candidate, commit)
                .map_err(CoordinationFailure::Lease)
        })
    }
}
impl SourcePreparation {
    pub(crate) fn config_revision(&self) -> u64 {
        self.stamp.revision
    }
    /// Explicit local policy-only templates, validated with captured backends.
    /// Materialize inheritance before strict complete-source projection. These
    /// inputs are frozen into this preparation, not imported from discovery or
    /// implicitly projected from the daemon's general-purpose stream templates.
    pub fn with_policy_templates(
        self,
        source: &Value,
        groups: &Value,
        templates: &Value,
    ) -> Result<Self, CoordinationFailure> {
        let policies = super::flussonic_policy::PolicySet::with_templates(
            source,
            groups,
            templates,
            &self.backends,
        )
        .map_err(|error| match error {
            super::flussonic_policy::PolicyFailure::Unsupported => CoordinationFailure::Unsupported,
            _ => CoordinationFailure::Invalid,
        })?;
        let (source, groups) = policies.materialized_inputs();
        self.with_policy(&source, &groups)
    }
    /// Explicit local policy only, frozen with captured backend definitions.
    /// Missing policy stays unresolved; remote discovery never supplies it.
    pub fn with_policy(
        mut self,
        source: &Value,
        groups: &Value,
    ) -> Result<Self, CoordinationFailure> {
        super::flussonic_policy::PolicySet::new(source, groups, &self.backends).map_err(
            |error| match error {
                super::flussonic_policy::PolicyFailure::Unsupported => {
                    CoordinationFailure::Unsupported
                }
                _ => CoordinationFailure::Invalid,
            },
        )?;
        let object = self
            .source
            .as_object_mut()
            .ok_or(CoordinationFailure::Invalid)?;
        for field in ["on_play", "flussonix_token_sha256"] {
            object.remove(field);
            if let Some(value) = source.get(field) {
                object.insert(field.to_owned(), value.clone());
            }
        }
        object.insert("group_config".to_owned(), groups.clone());
        Ok(self)
    }

    /// Prepare complete immutable clients with independent per-leg TLS trust.
    /// Validated daemon selection, processing reference and derived transport
    /// metadata are removed; complete-profile validation rejects unsupported options.
    pub fn prepare(self, budget: Duration) -> Result<PreparedSource, CoordinationFailure> {
        let source =
            super::flussonic_config::normalized_modern_source(&self.source).map_err(projection)?;
        let authority =
            SourceAuthority::new_modern(&source, &self.backends, budget).map_err(projection)?;
        Ok(PreparedSource {
            stamp: self.stamp,
            authority: Arc::new(authority),
        })
    }
}
impl CoordinatedInventory {
    /// Literal facts remain inspectable after invalidation; they grant no route.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.inventory.names()
    }
    pub fn server_id(&self) -> &ObservedServerId {
        self.inventory.server_id()
    }
    pub fn expires_at(&self) -> Instant {
        self.inventory.expires_at()
    }
    pub fn candidate(&self, name: &str) -> Result<CoordinatedCandidate, CoordinationFailure> {
        self.inventory
            .candidate(name)
            .map(|candidate| CoordinatedCandidate {
                handle: self.handle.clone(),
                candidate,
            })
            .map_err(CoordinationFailure::Lease)
    }
}
impl CoordinatedCandidate {
    pub(crate) fn config_revision(&self) -> u64 {
        self.handle.stamp.revision
    }
    pub(crate) fn source_key(&self) -> &str {
        &self.handle.stamp.key
    }
    pub fn server_id(&self) -> &ObservedServerId {
        self.candidate.server_id()
    }
    pub fn expires_at(&self) -> Instant {
        self.candidate.expires_at()
    }
}
