//! Opt-in modern freshness; wire timing and local lease lifetime are independent.
use super::{ModernBoundCandidate, ModernBoundInventory};
use crate::{
    m4_ingest::flussonic::MediaInput,
    source_directory::{
        flussonic::modern::ObservedServerId,
        flussonic_config::{
            BindingFailure, BoundInventory, CandidateView, LeaseFailure, ProjectionFailure,
            SourceAuthority, SourceSnapshot,
            binding::{MAX_LIFETIME, Ticket},
        },
    },
};
use std::{
    sync::Arc,
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
    pub fn server_id(&self) -> &ObservedServerId {
        self.candidate.server_id()
    }
    pub fn expires_at(&self) -> Instant {
        self.ticket.expires_at
    }
}
impl SourceAuthority {
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
        let candidate = &bound.candidate.candidate.candidate;
        let state = self
            .current(candidate.revision, &candidate.profile)
            .map_err(projection)?;
        bound.ticket.check(&state)?;
        let input = candidate
            .profile
            .media
            .input(&candidate.name)
            .map_err(|_| projection(ProjectionFailure::Invalid))?;
        bound.ticket.check(&state)?;
        let result = commit(
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
        );
        drop(state);
        Ok(result)
    }
}
