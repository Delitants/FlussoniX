//! Complete local source projection and metadata-candidate publication fencing.
//! Shared with daemon source validation; no automatic routes or vendor components.
mod binding;
mod modern;
use super::flussonic::InventoryClient;
use super::flussonic::modern::ModernInventoryClient;
use super::{
    flussonic::{CandidateKind, EligibilityFailure, InventoryStream},
    flussonic_policy::{PolicyFailure, PolicySet},
};
use crate::{
    m4_ingest::flussonic::{MediaOrigin, Transport},
    playback_auth::Policy,
};
pub use binding::{
    BindingFailure, BoundCandidate, BoundInventory, IdentityCandidate, IdentityInventory,
    LeaseFailure, LeasedIdentityCandidate, LeasedIdentityInventory,
};
pub use modern::{
    ModernBoundCandidate, ModernBoundInventory, ModernLeasedCandidate, ModernLeasedInventory,
};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

const MAX_PROFILE_BYTES: usize = 1024 * 1024;
const MAX_GROUP_BYTES: usize = 16 * 1024;
const MAX_GROUPS: usize = 128;
const MAX_PIN_MEMBERSHIPS: usize = 64;
const MAX_PATTERNS: usize = 1024;

/// Remove validated local selection/processing metadata and derived transport.
/// Complete-profile validation still rejects every unsupported native field.
/// Unmarked rows remain available to explicit library callers; a marked native
/// row must never be projected as a vendor source by mistake.
pub(crate) fn normalized_modern_source(source: &Value) -> Result<Value, ProjectionFailure> {
    let mut profile = source.clone();
    let object = profile.as_object_mut().ok_or(ProjectionFailure::Invalid)?;
    if let Some(kind) = object.remove("flussonix_source_kind")
        && kind.as_str() != Some("flussonic")
    {
        return Err(ProjectionFailure::Unsupported);
    }
    let group = object.remove("flussonix_source_group");
    let content = object.remove("flussonix_content_id");
    if group.is_some() != content.is_some() {
        return Err(ProjectionFailure::Invalid);
    }
    for identity in [group, content].into_iter().flatten() {
        crate::config::valid_identity(&identity).map_err(|_| ProjectionFailure::Invalid)?;
    }
    if let Some(reference) = object.remove("flussonix_output_template") {
        crate::config::valid_name(reference.as_str().ok_or(ProjectionFailure::Invalid)?)
            .map_err(|_| ProjectionFailure::Invalid)?;
    }
    if let Some(transport) = object.remove("flussonix_transport") {
        let (_, expected) = crate::config::source_address(
            object
                .get("url")
                .and_then(Value::as_str)
                .ok_or(ProjectionFailure::Unsupported)?,
        )
        .map_err(|_| ProjectionFailure::Invalid)?;
        if transport.as_str() != Some(expected) {
            return Err(ProjectionFailure::Unsupported);
        }
    }
    Ok(profile)
}

/// Diagnostics deliberately contain no endpoint, name, policy or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionFailure {
    Invalid,
    Unavailable,
    Unsupported,
    Limit,
    Excluded,
    Removed,
    Stale,
    MissingGroupPin,
    GroupPinMismatch,
    Ineligible(EligibilityFailure),
    Policy(PolicyFailure),
}

#[derive(Clone, Copy)]
enum DiscoveryMode {
    Legacy,
    Modern(Duration),
}
enum Inventory {
    Legacy(InventoryClient),
    Modern(Box<ModernInventoryClient>),
}
struct Profile {
    source: Value,
    policies: PolicySet,
    group_pins: HashMap<String, Vec<String>>,
    uniform: std::sync::OnceLock<Result<Option<Policy>, PolicyFailure>>,
    inventory: Inventory,
    media: MediaOrigin,
}
impl Profile {
    fn parse(
        source: &Value,
        backends: &Value,
        mode: DiscoveryMode,
    ) -> Result<Self, ProjectionFailure> {
        let object = source.as_object().ok_or(ProjectionFailure::Invalid)?;
        bounded(
            &json!({"source":source,"auth_backends":backends}),
            MAX_PROFILE_BYTES,
        )?;
        supported(
            object,
            &[
                "url",
                "cluster_key",
                "prefix",
                "only",
                "except",
                "on_play",
                "flussonix_token_sha256",
                "group_config",
                "flussonix_group_pins",
                "api_url",
                "private_payload_url",
                "flussonix_tls_ca",
                "flussonix_media_tls_ca",
            ],
        )?;
        let url = source["url"].as_str().ok_or(ProjectionFailure::Invalid)?;
        let (default_root, protocol) =
            crate::config::source_address(url).map_err(|_| ProjectionFailure::Invalid)?;
        source["cluster_key"]
            .as_str()
            .filter(|key| !key.is_empty() && key.len() <= 4096)
            .ok_or(ProjectionFailure::Invalid)?;
        if let Some(prefix) = source.get("prefix") {
            let prefix = prefix.as_str().ok_or(ProjectionFailure::Invalid)?;
            if prefix.contains('*') {
                return Err(ProjectionFailure::Invalid);
            }
            crate::config::valid_name(prefix).map_err(|_| ProjectionFailure::Invalid)?;
        }
        for field in ["only", "except"] {
            if let Some(value) = source.get(field) {
                let patterns = value.as_array().ok_or(ProjectionFailure::Invalid)?;
                if patterns.len() > MAX_PATTERNS {
                    return Err(ProjectionFailure::Limit);
                }
                for pattern in patterns {
                    let pattern = pattern.as_str().ok_or(ProjectionFailure::Invalid)?;
                    let name = pattern.strip_suffix("/*").unwrap_or(pattern);
                    if pattern.len() > 256 || name.contains('*') {
                        return Err(ProjectionFailure::Invalid);
                    }
                    crate::config::valid_name(name).map_err(|_| ProjectionFailure::Invalid)?;
                }
            }
        }
        let mut groups = Map::new();
        if let Some(value) = source.get("group_config") {
            let configured = value.as_object().ok_or(ProjectionFailure::Invalid)?;
            if configured.len() > MAX_GROUPS {
                return Err(ProjectionFailure::Limit);
            }
            for (name, value) in configured {
                crate::config::valid_name(name).map_err(|_| ProjectionFailure::Invalid)?;
                bounded(value, MAX_GROUP_BYTES)?;
                let group = value.as_object().ok_or(ProjectionFailure::Invalid)?;
                supported(group, &["name", "on_play", "flussonix_token_sha256"])?;
                if let Some(redundant) = group.get("name")
                    && redundant.as_str() != Some(name)
                {
                    return Err(ProjectionFailure::Invalid);
                }
                groups.insert(name.clone(), project_policy(group));
            }
        }
        let mut group_pins = HashMap::new();
        if let Some(value) = source.get("flussonix_group_pins") {
            let configured = value.as_object().ok_or(ProjectionFailure::Invalid)?;
            if configured.len() > MAX_GROUPS {
                return Err(ProjectionFailure::Limit);
            }
            for (stream, value) in configured {
                if stream.contains('*') || crate::config::valid_name(stream).is_err() {
                    return Err(ProjectionFailure::Invalid);
                }
                let names = value.as_array().ok_or(ProjectionFailure::Invalid)?;
                if names.is_empty() || names.len() > MAX_PIN_MEMBERSHIPS {
                    return Err(ProjectionFailure::Limit);
                }
                let mut seen = HashSet::new();
                let mut pin = Vec::with_capacity(names.len());
                for name in names {
                    let name = name.as_str().ok_or(ProjectionFailure::Invalid)?;
                    if name.contains('*')
                        || crate::config::valid_name(name).is_err()
                        || !groups.contains_key(name)
                    {
                        return Err(ProjectionFailure::Invalid);
                    }
                    if !seen.insert(name) {
                        return Err(ProjectionFailure::Invalid);
                    }
                    pin.push(name.to_owned());
                }
                group_pins.insert(stream.clone(), pin);
            }
        }
        let api = optional_text(source, "api_url")?.unwrap_or(&default_root);
        let private = optional_text(source, "private_payload_url")?.unwrap_or(api);
        let key = source["cluster_key"]
            .as_str()
            .expect("validated privileged key");
        let ca = optional_text(source, "flussonix_tls_ca")?.map(Path::new);
        let inventory = match mode {
            DiscoveryMode::Legacy => InventoryClient::new(api, key, ca).map(Inventory::Legacy),
            DiscoveryMode::Modern(budget) => ModernInventoryClient::new(api, key, ca, budget)
                .map(|client| Inventory::Modern(Box::new(client))),
        }
        .map_err(|_| ProjectionFailure::Invalid)?;
        let media = MediaOrigin::new(
            private,
            if protocol == "m4f" {
                Transport::M4f
            } else {
                Transport::M4s
            },
            key,
            optional_text(source, "flussonix_media_tls_ca")?.map(Path::new),
        )
        .map_err(|_| ProjectionFailure::Invalid)?;
        let policies = PolicySet::new(&project_policy(object), &Value::Object(groups), backends)
            .map_err(ProjectionFailure::Policy)?;
        Ok(Self {
            source: source.clone(),
            policies,
            group_pins,
            uniform: std::sync::OnceLock::new(),
            inventory,
            media,
        })
    }
    fn legacy_inventory(&self) -> Result<&InventoryClient, ProjectionFailure> {
        match &self.inventory {
            Inventory::Legacy(client) => Ok(client),
            Inventory::Modern(_) => Err(ProjectionFailure::Unsupported),
        }
    }
    fn modern_inventory(&self) -> Result<&ModernInventoryClient, ProjectionFailure> {
        match &self.inventory {
            Inventory::Modern(client) => Ok(client),
            Inventory::Legacy(_) => Err(ProjectionFailure::Unsupported),
        }
    }
}

#[derive(Clone)]
pub(crate) enum ModernFact {
    Eligible {
        observation: Arc<()>,
        names: Arc<[String]>,
    },
    Rejected {
        observation: Arc<()>,
    },
}

struct State {
    revision: u64,
    profile: Option<Arc<Profile>>,
    lease_turn: Option<Arc<()>>,
    renewal_turn: Option<Arc<()>>,
    // Compact facts minted only by accepted modern discovery/renewal. A
    // caller's copied LeaseFailure is never source-wide rejection proof.
    modern_fact: Option<ModernFact>,
}

/// Owns one independent local source authority. Not the daemon's configuration
/// store. No Debug/serialization: source settings contain privileged credentials.
pub struct SourceAuthority {
    mode: DiscoveryMode,
    state: Mutex<State>,
}
impl SourceAuthority {
    pub(crate) fn uniform_policy(&self) -> Result<Option<Policy>, ProjectionFailure> {
        let state = self.state()?;
        let profile = state.profile.as_ref().ok_or(ProjectionFailure::Removed)?;
        profile
            .uniform
            .get_or_init(|| profile.policies.uniform())
            .clone()
            .map_err(ProjectionFailure::Policy)
    }
    pub fn new(source: &Value, backends: &Value) -> Result<Self, ProjectionFailure> {
        Self::with_mode(source, backends, DiscoveryMode::Legacy)
    }
    fn with_mode(
        source: &Value,
        backends: &Value,
        mode: DiscoveryMode,
    ) -> Result<Self, ProjectionFailure> {
        let profile = Arc::new(Profile::parse(source, backends, mode)?);
        Ok(Self {
            mode,
            state: Mutex::new(State {
                revision: 0,
                profile: Some(profile),
                lease_turn: None,
                renewal_turn: None,
                modern_fact: None,
            }),
        })
    }
    fn state(&self) -> Result<MutexGuard<'_, State>, ProjectionFailure> {
        self.state
            .lock()
            .map_err(|_| ProjectionFailure::Unavailable)
    }
    fn current(
        &self,
        revision: u64,
        profile: &Arc<Profile>,
    ) -> Result<MutexGuard<'_, State>, ProjectionFailure> {
        let state = self.state()?;
        if state.revision != revision
            || !state
                .profile
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(p, profile))
        {
            return Err(ProjectionFailure::Stale);
        }
        Ok(state)
    }
    pub fn snapshot(&self) -> Result<SourceSnapshot, ProjectionFailure> {
        let state = self.state()?;
        Ok(SourceSnapshot {
            revision: state.revision,
            profile: state.profile.clone().ok_or(ProjectionFailure::Removed)?,
        })
    }
    /// Validate outside the publication lock; a failed edit changes nothing.
    /// Every successful unconditional replacement creates a new profile identity.
    /// Callers must order/fence asynchronous configuration loads themselves.
    pub fn replace(&self, source: &Value, backends: &Value) -> Result<(), ProjectionFailure> {
        let profile = Arc::new(Profile::parse(source, backends, self.mode)?);
        let mut state = self.state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or(ProjectionFailure::Limit)?;
        state.profile = Some(profile);
        state.revision = revision;
        state.lease_turn = None;
        state.renewal_turn = None;
        state.modern_fact = None;
        Ok(())
    }
    pub fn remove(&self) -> Result<(), ProjectionFailure> {
        let mut state = self.state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or(ProjectionFailure::Limit)?;
        state.profile = None;
        state.revision = revision;
        state.lease_turn = None;
        state.renewal_turn = None;
        state.modern_fact = None;
        Ok(())
    }
    /// Hold the authority lock while committing a current candidate. Never await
    /// or re-enter this authority in the closure. This is not a daemon/global
    /// fence, inventory lease, verified topology check or viewer authorization.
    pub fn at_current<T>(
        &self,
        candidate: &Candidate,
        commit: impl FnOnce(CandidateView<'_>) -> T,
    ) -> Result<T, ProjectionFailure> {
        let state = self.current(candidate.revision, &candidate.profile)?;
        // Keep state alive through commit: replacement/removal cannot interleave.
        let result = commit(CandidateView {
            source: candidate.profile.source["url"]
                .as_str()
                .expect("validated source identity"),
            name: &candidate.name,
            policy: &candidate.policy,
            kind: candidate.kind,
            selected_static: candidate.selected_static,
        });
        drop(state);
        Ok(result)
    }
}

/// Immutable local configuration snapshot; may outlive edits and async lookups.
/// It can still construct old candidates, but they cannot pass at_current().
pub struct SourceSnapshot {
    revision: u64,
    profile: Arc<Profile>,
}
impl SourceSnapshot {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn candidate(&self, row: &InventoryStream) -> Result<Candidate, ProjectionFailure> {
        let kind = match row.local_origin_candidate() {
            Ok(kind) => kind,
            // Modern management responses can omit memberships. Preserve that
            // uncertainty; selected_candidate must prove policy independence.
            Err(super::flussonic::EligibilityFailure::UnknownGroups)
                if matches!(&self.profile.inventory, Inventory::Modern(_)) =>
            {
                if row.is_static {
                    CandidateKind::Static
                } else {
                    CandidateKind::OnDemand
                }
            }
            Err(error) => return Err(ProjectionFailure::Ineligible(error)),
        };
        self.selected_candidate(row, kind)
    }
    // Only the explicit all-hop coordinator can use remote rows. Preserve the
    // existing public local-origin gate and revalidate all other row facts.
    pub(super) fn relay_candidate(
        &self,
        row: &InventoryStream,
    ) -> Result<Candidate, ProjectionFailure> {
        match row.local_origin_candidate() {
            Err(super::flussonic::EligibilityFailure::Relayed) => {}
            Err(error) => return Err(ProjectionFailure::Ineligible(error)),
            Ok(_) => return Err(ProjectionFailure::Invalid),
        }
        if row.groups.is_none() && !self.profile.group_pins.contains_key(&row.name) {
            return Err(ProjectionFailure::Ineligible(
                super::flussonic::EligibilityFailure::UnknownGroups,
            ));
        }
        self.selected_candidate(
            row,
            if row.is_static {
                CandidateKind::Static
            } else {
                CandidateKind::OnDemand
            },
        )
    }
    fn selected_candidate(
        &self,
        row: &InventoryStream,
        kind: CandidateKind,
    ) -> Result<Candidate, ProjectionFailure> {
        if !super::allows(&self.profile.source, &row.name) {
            return Err(ProjectionFailure::Excluded);
        }
        let policy = if let Some(pin) = self.profile.group_pins.get(&row.name) {
            if let Some(groups) = &row.groups {
                let remote: HashSet<_> = groups.iter().collect();
                let approved: HashSet<_> = pin.iter().collect();
                if remote.len() != groups.len() || remote != approved {
                    return Err(ProjectionFailure::GroupPinMismatch);
                }
            }
            self.profile
                .policies
                .for_groups(&json!(pin))
                .map_err(ProjectionFailure::Policy)?
        } else {
            // Every possible subset of configured groups must produce the
            // same explicit local policy. Unknown memberships never become [].
            let uniform = self
                .profile
                .uniform
                .get_or_init(|| self.profile.policies.uniform())
                .clone();
            match uniform {
                Ok(Some(policy)) => policy,
                Ok(None) => return Err(ProjectionFailure::MissingGroupPin),
                Err(PolicyFailure::Unresolved)
                    if self.profile.source["group_config"]
                        .as_object()
                        .is_some_and(|groups| !groups.is_empty()) =>
                {
                    return Err(ProjectionFailure::MissingGroupPin);
                }
                Err(error) => return Err(ProjectionFailure::Policy(error)),
            }
        };
        let selected_static = kind == CandidateKind::Static
            && super::selected_static(&self.profile.source, &row.name);
        Ok(Candidate {
            revision: self.revision,
            profile: self.profile.clone(),
            name: row.name.clone(),
            policy,
            kind,
            selected_static,
        })
    }
}

/// Opaque candidate: policy is exposed only inside a current-authority closure.
/// No Debug/serialization and no privileged media or viewer access is granted.
pub struct Candidate {
    revision: u64,
    profile: Arc<Profile>,
    name: String,
    policy: Policy,
    kind: CandidateKind,
    selected_static: bool,
}
/// Borrowed local facts for a synchronous current-authority publication.
/// Copying a policy does not authorize later publication without a fresh fence.
pub struct CandidateView<'a> {
    pub source: &'a str,
    pub name: &'a str,
    pub policy: &'a Policy,
    pub kind: CandidateKind,
    pub selected_static: bool,
}

fn supported(object: &Map<String, Value>, fields: &[&str]) -> Result<(), ProjectionFailure> {
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(ProjectionFailure::Unsupported);
    }
    Ok(())
}
fn project_policy(object: &Map<String, Value>) -> Value {
    Value::Object(
        object
            .iter()
            .filter(|(key, _)| ["on_play", "flussonix_token_sha256"].contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}
fn bounded(value: &Value, limit: usize) -> Result<(), ProjectionFailure> {
    if serde_json::to_vec(value)
        .map_err(|_| ProjectionFailure::Invalid)?
        .len()
        > limit
    {
        return Err(ProjectionFailure::Limit);
    }
    Ok(())
}

fn optional_text<'a>(source: &'a Value, field: &str) -> Result<Option<&'a str>, ProjectionFailure> {
    source
        .get(field)
        .map(|v| {
            v.as_str()
                .filter(|s| !s.is_empty() && s.len() <= 4096)
                .ok_or(ProjectionFailure::Invalid)
        })
        .transpose()
}

#[cfg(test)]
mod group_pin_tests {
    use super::*;
    use crate::source_directory::flussonic::StreamProvenance;

    #[test]
    fn relay_omitted_membership_requires_an_exact_local_pin() {
        let source = json!({
            "url":"m4s://origin.invalid",
            "cluster_key":"owned-cluster-key",
            "on_play":"true",
            "group_config":{"paid":{"flussonix_token_sha256":"a".repeat(64)}},
            "flussonix_group_pins":{"music":["paid"]}
        });
        let authority =
            SourceAuthority::new_modern(&source, &json!([]), Duration::from_secs(3)).unwrap();
        let row = InventoryStream {
            name: "music".into(),
            is_static: true,
            disabled: false,
            named_by: Some(StreamProvenance::Remote),
            groups: None,
        };
        let candidate = authority.snapshot().unwrap().relay_candidate(&row).unwrap();
        authority
            .at_current(&candidate, |view| {
                assert_eq!(
                    view.policy,
                    &Policy::from_config(
                        &json!({"flussonix_token_sha256":"a".repeat(64)}),
                        &json!({})
                    )
                    .unwrap()
                );
            })
            .unwrap();
        let mut mismatched = row;
        mismatched.groups = Some(vec![]);
        assert!(
            authority
                .snapshot()
                .unwrap()
                .relay_candidate(&mismatched)
                .is_err()
        );
    }
}
