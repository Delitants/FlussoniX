//! Explicit modern configured-source binding. No relay or daemon grant.
mod lease;
use super::{
    BindingFailure, BoundCandidate, BoundInventory, CandidateView, DiscoveryMode,
    ProjectionFailure, SourceAuthority,
};
use crate::{
    m4_ingest::flussonic::MediaInput,
    source_directory::flussonic::modern::{ObservedInventory, ObservedServerId},
};
pub use lease::{ModernLeasedCandidate, ModernLeasedInventory};
use serde_json::Value;
use std::time::Duration;

/// Complete modern discovery through this exact internally prepared profile.
/// No public importer, Debug or serialization. Facts alone grant no access.
pub struct ModernBoundInventory {
    inventory: BoundInventory,
    server_id: ObservedServerId,
}
/// Only opaque modern bound results construct this publication candidate.
/// Neither plain observations nor ordinary/legacy candidates can substitute.
pub struct ModernBoundCandidate {
    candidate: BoundCandidate,
    server_id: ObservedServerId,
}
impl ModernBoundInventory {
    /// Untrusted observed management claim, not an origin/topology attestation.
    pub fn server_id(&self) -> &ObservedServerId {
        &self.server_id
    }
    /// Sorted literal names remain inspectable even after source replacement.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.inventory.names()
    }
    /// Mandatory existing local-origin, membership, filter and local policy gates.
    /// Old facts can form candidates but cannot pass the current-profile fence.
    pub fn candidate(&self, name: &str) -> Result<ModernBoundCandidate, BindingFailure> {
        self.inventory
            .candidate(name)
            .map(|candidate| ModernBoundCandidate {
                candidate,
                server_id: self.server_id.clone(),
            })
    }
}
impl ModernBoundCandidate {
    pub fn server_id(&self) -> &ObservedServerId {
        &self.server_id
    }
}
impl SourceAuthority {
    /// Opt into modern config/pages/config discovery with an explicit (0,30s]
    /// whole-refresh budget. Prepare management and private-media clients from
    /// the same fully validated local profile, retaining independent TLS trust.
    /// Mode/budget are immutable across replace/remove/re-add; use a new authority
    /// to choose another mode. Legacy discovery/identity/lease methods reject
    /// this mode before I/O. Existing new() authorities remain strictly legacy.
    pub fn new_modern(
        source: &Value,
        backends: &Value,
        budget: Duration,
    ) -> Result<Self, ProjectionFailure> {
        Self::with_mode(source, backends, DiscoveryMode::Modern(budget))
    }
    /// Caller supplies its authoritative current local identity. Capture profile
    /// before I/O and bind only complete discovery after rechecking source/profile
    /// under the publication lock, including errors. No detached task or public
    /// observation importer. This does not reserve or grant a local freshness lease.
    pub async fn modern_inventory(
        &self,
        local_id: &ObservedServerId,
    ) -> Result<ModernBoundInventory, BindingFailure> {
        let snapshot = self.snapshot().map_err(BindingFailure::Projection)?;
        let result = snapshot
            .profile
            .modern_inventory()
            .map_err(BindingFailure::Projection)?
            .inventory(local_id)
            .await
            .map(ObservedInventory::into_parts);
        self.bind_inventory(snapshot, result)
            .map(|(server_id, inventory)| ModernBoundInventory {
                inventory,
                server_id,
            })
    }
    /// Construct private media under the current profile lock and invoke a
    /// synchronous closure. Never await/re-enter this authority in the closure.
    /// Viewer admission, async re-fencing, global ordering and worker/session
    /// revocation remain caller responsibilities. No lease/relay permission.
    pub fn at_modern_current<T>(
        &self,
        bound: &ModernBoundCandidate,
        commit: impl FnOnce(CandidateView<'_>, MediaInput) -> T,
    ) -> Result<T, ProjectionFailure> {
        self.at_bound_current(&bound.candidate, commit)
    }
}
