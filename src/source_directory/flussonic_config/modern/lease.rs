//! Opt-in modern freshness; wire timing and local lease lifetime are independent.
mod renewal;
use super::{ModernBoundCandidate, ModernBoundInventory};
use crate::{
    m4_ingest::flussonic::MediaInput,
    source_directory::{
        flussonic::modern::ObservedServerId,
        flussonic_config::{
            BindingFailure, BoundCandidate, BoundInventory, CandidateView, LeaseFailure,
            ProjectionFailure, SourceAuthority, SourceSnapshot,
            binding::{MAX_LIFETIME, Ticket},
        },
    },
};
use std::{
    sync::{Arc, MutexGuard},
    time::{Duration, Instant},
};

fn projection(error: ProjectionFailure) -> LeaseFailure {
    LeaseFailure::Binding(BindingFailure::Projection(error))
}
/// Complete configured modern discovery plus a process-local freshness ticket.
/// No importer, public fields, Debug or serialization; facts do not grant access.
pub struct ModernLeasedInventory {
    inventory: ModernBoundInventory,
    ticket: Ticket,
}
/// Only modern leased inventory constructs this publication candidate.
/// Unleased/legacy candidates cannot substitute at the leased publication gate.
pub struct ModernLeasedCandidate {
    candidate: ModernBoundCandidate,
    ticket: Ticket,
}
impl ModernLeasedInventory {
    pub(crate) fn path_provenance(
        &self,
        name: &str,
    ) -> Result<Option<crate::source_directory::flussonic::StreamProvenance>, LeaseFailure> {
        self.inventory
            .inventory
            .rows
            .get(name)
            .map(|row| row.named_by)
            .ok_or(LeaseFailure::Binding(BindingFailure::Missing))
    }
    pub(crate) fn relay_candidate(
        &self,
        name: &str,
    ) -> Result<ModernLeasedCandidate, LeaseFailure> {
        let inventory = &self.inventory.inventory;
        let row = inventory
            .rows
            .get(name)
            .ok_or(LeaseFailure::Binding(BindingFailure::Missing))?;
        let candidate = inventory
            .snapshot
            .relay_candidate(row)
            .map_err(projection)?;
        Ok(ModernLeasedCandidate {
            candidate: ModernBoundCandidate {
                candidate: BoundCandidate { candidate },
                server_id: self.server_id().clone(),
            },
            ticket: self.ticket.clone(),
        })
    }
    // Explicit relay assertions require known membership at the terminal too;
    // the direct-origin uniform-policy exception does not attest relay paths.
    pub(crate) fn relay_origin_candidate(
        &self,
        name: &str,
    ) -> Result<ModernLeasedCandidate, LeaseFailure> {
        let row = self
            .inventory
            .inventory
            .rows
            .get(name)
            .ok_or(LeaseFailure::Binding(BindingFailure::Missing))?;
        row.local_origin_candidate()
            .map_err(|error| projection(ProjectionFailure::Ineligible(error)))?;
        self.candidate(name)
    }
    /// Untrusted management claim, not private-endpoint identity or topology proof.
    pub fn server_id(&self) -> &ObservedServerId {
        self.inventory.server_id()
    }
    /// Monotonic local deadline; inspecting or copying it never renews the lease.
    pub fn expires_at(&self) -> Instant {
        self.ticket.expires_at
    }
    /// Literal names remain inspectable after expiry or source replacement.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.inventory.names()
    }
    /// Preserve mandatory local origin, membership, filter and viewer policy gates.
    /// Old facts can form old candidates, but publication must recheck freshness.
    pub fn candidate(&self, name: &str) -> Result<ModernLeasedCandidate, LeaseFailure> {
        self.inventory
            .candidate(name)
            .map(|candidate| ModernLeasedCandidate {
                candidate,
                ticket: self.ticket.clone(),
            })
            .map_err(LeaseFailure::Binding)
    }
}
impl ModernLeasedCandidate {
    pub(crate) fn shares_lease(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.ticket.turn, &other.ticket.turn)
            && self.ticket.expires_at == other.ticket.expires_at
    }

    pub fn server_id(&self) -> &ObservedServerId {
        self.candidate.server_id()
    }
    pub fn expires_at(&self) -> Instant {
        self.ticket.expires_at
    }
}
impl SourceAuthority {
    /// Fence negative facts as well as candidates. Keep this source lock through
    /// the synchronous mutation; a pre-check alone races a newer refresh.
    pub(crate) fn with_modern_inventory_current<T>(
        &self,
        inventory: &ModernLeasedInventory,
        commit: impl FnOnce() -> T,
    ) -> Result<T, LeaseFailure> {
        let snapshot = &inventory.inventory.inventory.snapshot;
        let state = self
            .current(snapshot.revision, &snapshot.profile)
            .map_err(projection)?;
        inventory.ticket.check(&state)?;
        let result = commit();
        inventory.ticket.check(&state)?;
        Ok(result)
    }
    pub(crate) fn guard_modern_lease<'a>(
        &'a self,
        bound: &'a ModernLeasedCandidate,
    ) -> Result<ModernLeaseGuard<'a>, LeaseFailure> {
        let candidate = &bound.candidate.candidate.candidate;
        let state = self
            .current(candidate.revision, &candidate.profile)
            .map_err(projection)?;
        bound.ticket.check(&state)?;
        Ok(ModernLeaseGuard { state, bound })
    }
    /// Lifetime must be in (0,30s], measured from reservation before I/O, separately
    /// from the constructor's modern wire budget. Invalid lifetime, removed source
    /// or wrong mode never reserve a turn or send requests. Unpolled futures do
    /// nothing. A valid polled attempt immediately supersedes prior leased results;
    /// failure or cancellation never restores them. Caller owns current local ID.
    pub async fn leased_modern_inventory(
        &self,
        local_id: &ObservedServerId,
        lifetime: Duration,
    ) -> Result<ModernLeasedInventory, LeaseFailure> {
        self.leased_modern_inventory_with_ids(std::slice::from_ref(local_id), lifetime)
            .await
    }

    pub(crate) async fn leased_modern_inventory_with_ids(
        &self,
        local_ids: &[ObservedServerId],
        lifetime: Duration,
    ) -> Result<ModernLeasedInventory, LeaseFailure> {
        if lifetime.is_zero() || lifetime > MAX_LIFETIME {
            return Err(LeaseFailure::InvalidLifetime);
        }
        let (snapshot, ticket) = {
            let mut state = self.state().map_err(projection)?;
            let snapshot = SourceSnapshot {
                revision: state.revision,
                profile: state
                    .profile
                    .clone()
                    .ok_or_else(|| projection(ProjectionFailure::Removed))?,
            };
            snapshot.profile.modern_inventory().map_err(projection)?;
            let ticket = Ticket {
                turn: Arc::new(()),
                expires_at: Instant::now() + lifetime,
            };
            state.lease_turn = Some(ticket.turn.clone());
            state.renewal_turn = None;
            (snapshot, ticket)
        };
        let result = snapshot
            .profile
            .modern_inventory()
            .map_err(projection)?
            .inventory_with_ids(local_ids)
            .await;
        // Same lock fences success AND error: exact profile, newest turn, expiry,
        // upstream result. No publication lock is held across network awaits.
        let state = self
            .current(snapshot.revision, &snapshot.profile)
            .map_err(projection)?;
        ticket.check(&state)?;
        let (server_id, rows) = result
            .map_err(|error| LeaseFailure::Binding(BindingFailure::Inventory(error)))?
            .into_parts();
        let inventory = ModernBoundInventory {
            inventory: BoundInventory {
                snapshot,
                rows: rows.into_iter().map(|r| (r.name.clone(), r)).collect(),
            },
            server_id,
        };
        ticket.check(&state)?;
        drop(state);
        Ok(ModernLeasedInventory { inventory, ticket })
    }
    /// Under one publication lock, check current profile, latest turn and expiry
    /// before media construction and again before synchronous closure entry.
    /// Never await/reenter this authority in the closure. Later expiry/replacement
    /// does not revoke an exported input, worker or admitted viewer; async work
    /// must return through a fresh fence. No relay or daemon routing permission.
    pub fn at_modern_leased_current<T>(
        &self,
        bound: &ModernLeasedCandidate,
        commit: impl FnOnce(CandidateView<'_>, MediaInput) -> T,
    ) -> Result<T, LeaseFailure> {
        let guard = self.guard_modern_lease(bound)?;
        let (view, input) = guard.publication()?;
        let result = commit(view, input);
        drop(guard);
        Ok(result)
    }
}
/// Crate-private guard: no standalone external relay candidate or lock export.
pub(crate) struct ModernLeaseGuard<'a> {
    state: MutexGuard<'a, super::super::State>,
    bound: &'a ModernLeasedCandidate,
}
impl ModernLeaseGuard<'_> {
    pub(crate) fn check(&self) -> Result<(), LeaseFailure> {
        self.bound.ticket.check(&self.state)
    }
    pub(crate) fn publication(&self) -> Result<(CandidateView<'_>, MediaInput), LeaseFailure> {
        self.check()?;
        let candidate = &self.bound.candidate.candidate.candidate;
        let input = candidate
            .profile
            .media
            .input(&candidate.name)
            .map_err(|_| projection(ProjectionFailure::Invalid))?;
        self.check()?;
        Ok((
            CandidateView {
                source: candidate.profile.source["url"]
                    .as_str()
                    .expect("validated source identity"),
                name: &candidate.name,
                policy: &candidate.policy,
                kind: candidate.kind,
                selected_static: candidate.selected_static,
            },
            input,
        ))
    }
}
