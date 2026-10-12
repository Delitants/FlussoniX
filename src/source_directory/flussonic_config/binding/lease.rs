//! Opt-in local inventory freshness; not a remote lease or worker revocation.
use super::super::{ProjectionFailure, SourceSnapshot, State};
use super::{
    BindingFailure, BoundInventory, CandidateView, IdentityCandidate, IdentityInventory,
    SourceAuthority,
};
use crate::{m4_ingest::flussonic::MediaInput, source_directory::LookupFailure};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub(in super::super) const MAX_LIFETIME: Duration = Duration::from_secs(30);

/// Generic diagnostics contain no endpoint, stream name, credential or body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseFailure {
    Binding(BindingFailure),
    InvalidLifetime,
    Expired,
    Superseded,
}
fn projection(error: ProjectionFailure) -> LeaseFailure {
    LeaseFailure::Binding(BindingFailure::Projection(error))
}
#[derive(Clone)]
pub(in super::super) struct Ticket {
    pub(in super::super) turn: Arc<()>,
    pub(in super::super) expires_at: Instant,
}
impl Ticket {
    /// Called only while holding this authority's source publication lock.
    pub(in super::super) fn check(&self, state: &State) -> Result<(), LeaseFailure> {
        if !state
            .lease_turn
            .as_ref()
            .is_some_and(|turn| Arc::ptr_eq(turn, &self.turn))
        {
            return Err(LeaseFailure::Superseded);
        }
        if Instant::now() >= self.expires_at {
            return Err(LeaseFailure::Expired);
        }
        Ok(())
    }
}
/// Complete identity-observed facts with a local monotonic freshness bound.
/// Private fields and no Debug/serialization prevent importing a lease grant.
pub struct LeasedIdentityInventory {
    inventory: IdentityInventory,
    ticket: Ticket,
}
/// Created only from leased discovery. Ordinary bound/identity candidates cannot
/// substitute for this type at the leased publication entry point.
pub struct LeasedIdentityCandidate {
    candidate: IdentityCandidate,
    ticket: Ticket,
}
impl LeasedIdentityInventory {
    /// An observed claim, not authenticated content or private-endpoint identity.
    pub fn server_id(&self) -> Uuid {
        self.inventory.server_id()
    }
    /// Process-local monotonic deadline; not a remote promise or serialized time.
    pub fn expires_at(&self) -> Instant {
        self.ticket.expires_at
    }
    /// Names remain inspectable after expiry; facts alone grant no permission.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.inventory.names()
    }
    /// Apply existing metadata/filter/policy checks. A retained result can form
    /// an old candidate, but that candidate must pass at_leased_current again.
    pub fn candidate(&self, name: &str) -> Result<LeasedIdentityCandidate, LeaseFailure> {
        self.inventory
            .candidate(name)
            .map(|candidate| LeasedIdentityCandidate {
                candidate,
                ticket: self.ticket.clone(),
            })
            .map_err(LeaseFailure::Binding)
    }
}
impl LeasedIdentityCandidate {
    pub fn server_id(&self) -> Uuid {
        self.candidate.server_id()
    }
    pub fn expires_at(&self) -> Instant {
        self.ticket.expires_at
    }
}
impl SourceAuthority {
    /// A valid, polled attempt supersedes previous leased results immediately.
    /// Failure/cancellation does not restore them. Invalid arguments do no I/O
    /// and leave the current turn intact. Lifetime must be >0 and <=30 seconds,
    /// measured from reservation before I/O, not from successful completion.
    /// The caller still supplies its authoritative local ID and viewer policy.
    pub async fn leased_identity_inventory(
        &self,
        local_id: Uuid,
        lifetime: Duration,
    ) -> Result<LeasedIdentityInventory, LeaseFailure> {
        if lifetime.is_zero() || lifetime > MAX_LIFETIME {
            return Err(LeaseFailure::InvalidLifetime);
        }
        if local_id.is_nil() {
            return Err(LeaseFailure::Binding(BindingFailure::Inventory(
                LookupFailure::Invalid,
            )));
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
            // Wrong discovery mode must not reserve a legacy refresh turn.
            snapshot.profile.legacy_inventory().map_err(projection)?;
            let ticket = Ticket {
                turn: Arc::new(()),
                expires_at: Instant::now() + lifetime,
            };
            state.lease_turn = Some(ticket.turn.clone());
            (snapshot, ticket)
        };
        let result = snapshot
            .profile
            .legacy_inventory()
            .map_err(projection)?
            .identity_inventory(local_id)
            .await;
        // Apply local source/turn/expiry fences to success AND error, in that
        // order, without a gap between the check and constructing bound facts.
        let state = self
            .current(snapshot.revision, &snapshot.profile)
            .map_err(projection)?;
        ticket.check(&state)?;
        let (server_id, rows) =
            result.map_err(|e| LeaseFailure::Binding(BindingFailure::Inventory(e)))?;
        let inventory = IdentityInventory {
            inventory: BoundInventory {
                snapshot,
                rows: rows.into_iter().map(|r| (r.name.clone(), r)).collect(),
            },
            server_id,
        };
        ticket.check(&state)?;
        drop(state);
        Ok(LeasedIdentityInventory { inventory, ticket })
    }
    /// Recheck the source profile, newest refresh turn and strict expiry under
    /// one publication lock, before media construction and closure entry. Never
    /// await or re-enter this authority in the synchronous closure. Expiry after
    /// closure entry does not interrupt it or revoke an exported input/worker.
    /// Async viewer authorization/activation must return through a fresh fence.
    pub fn at_leased_current<T>(
        &self,
        bound: &LeasedIdentityCandidate,
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
