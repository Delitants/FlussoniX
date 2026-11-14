//! Staged renewal preserves the published ticket until atomic route publication.
use super::*;
use crate::source_directory::flussonic::modern::ObservedInventory;

pub(crate) struct ModernRenewal {
    snapshot: SourceSnapshot,
    old: Ticket,
    next: Ticket,
}
impl ModernRenewal {
    pub(crate) async fn observe(
        &self,
        ids: &[ObservedServerId],
    ) -> Result<ObservedInventory, LeaseFailure> {
        let deadline = self.old.expires_at.min(self.next.expires_at);
        let client = self
            .snapshot
            .profile
            .modern_inventory()
            .map_err(projection)?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            client.inventory_with_ids(ids),
        )
        .await
        .map_err(|_| LeaseFailure::Expired)?
        .map_err(|e| LeaseFailure::Binding(BindingFailure::Inventory(e)))
    }
}
/// Acquire before reading a shared worker's mutable route, preserving lock order.
pub(crate) struct ModernStateGuard<'a> {
    state: MutexGuard<'a, super::super::super::State>,
}
impl ModernStateGuard<'_> {
    pub(crate) fn check(&self, bound: &ModernLeasedCandidate) -> Result<(), LeaseFailure> {
        current(&self.state, bound)
    }
}
fn current(
    state: &super::super::super::State,
    bound: &ModernLeasedCandidate,
) -> Result<(), LeaseFailure> {
    let candidate = &bound.candidate.candidate.candidate;
    if state.revision != candidate.revision
        || !state
            .profile
            .as_ref()
            .is_some_and(|p| Arc::ptr_eq(p, &candidate.profile))
    {
        return Err(projection(ProjectionFailure::Stale));
    }
    bound.ticket.check(state)
}
impl SourceAuthority {
    pub(crate) fn guard_modern_state(&self) -> Result<ModernStateGuard<'_>, LeaseFailure> {
        Ok(ModernStateGuard {
            state: self.state().map_err(projection)?,
        })
    }
    pub(crate) fn begin_modern_renewal(
        &self,
        bound: &ModernLeasedCandidate,
        lifetime: Duration,
    ) -> Result<ModernRenewal, LeaseFailure> {
        if lifetime.is_zero() || lifetime > MAX_LIFETIME {
            return Err(LeaseFailure::InvalidLifetime);
        }
        let candidate = &bound.candidate.candidate.candidate;
        let mut state = self
            .current(candidate.revision, &candidate.profile)
            .map_err(projection)?;
        bound.ticket.check(&state)?;
        candidate.profile.modern_inventory().map_err(projection)?;
        let next = Ticket {
            turn: Arc::new(()),
            expires_at: Instant::now() + lifetime,
        };
        state.renewal_turn = Some(next.turn.clone());
        Ok(ModernRenewal {
            snapshot: SourceSnapshot {
                revision: candidate.revision,
                profile: candidate.profile.clone(),
            },
            old: bound.ticket.clone(),
            next,
        })
    }
    /// Prepare decisions without mutation, re-fence, then publish ticket/routes
    /// together. Caller holds registry/config/coordinator locks; no closure awaits.
    pub(crate) fn commit_modern_renewal<T, F: FnOnce()>(
        &self,
        pending: ModernRenewal,
        observed: Result<ObservedInventory, LeaseFailure>,
        reject: impl FnOnce(LeaseFailure),
        prepare: impl FnOnce(&ModernLeasedInventory, &dyn Fn(&ModernLeasedCandidate) -> bool) -> (T, F),
    ) -> Result<(ModernLeasedInventory, T), LeaseFailure> {
        let mut state = self
            .current(pending.snapshot.revision, &pending.snapshot.profile)
            .map_err(projection)?;
        let check = |state: &super::super::super::State| -> Result<(), LeaseFailure> {
            pending.old.check(state)?;
            if !state
                .renewal_turn
                .as_ref()
                .is_some_and(|t| Arc::ptr_eq(t, &pending.next.turn))
            {
                return Err(LeaseFailure::Superseded);
            }
            if Instant::now() >= pending.next.expires_at {
                return Err(LeaseFailure::Expired);
            }
            Ok(())
        };
        check(&state)?;
        let observed = match observed {
            Ok(observed) => observed,
            Err(error) => {
                if matches!(
                    error,
                    LeaseFailure::Binding(BindingFailure::Inventory(
                        crate::source_directory::LookupFailure::Absent
                            | crate::source_directory::LookupFailure::Invalid
                    ))
                ) {
                    state.modern_fact = Some(super::super::super::ModernFact::Rejected {
                        observation: pending.next.turn.clone(),
                    });
                }
                reject(error);
                return Err(error);
            }
        };
        let (server_id, rows) = observed.into_parts();
        let inventory = ModernLeasedInventory {
            inventory: ModernBoundInventory {
                inventory: BoundInventory {
                    snapshot: SourceSnapshot {
                        revision: pending.snapshot.revision,
                        profile: pending.snapshot.profile.clone(),
                    },
                    rows: rows.into_iter().map(|r| (r.name.clone(), r)).collect(),
                },
                server_id,
            },
            ticket: pending.next.clone(),
            eligible: OnceLock::new(),
        };
        drop(state);
        let names = inventory.eligible_names();
        let mut state = self
            .current(pending.snapshot.revision, &pending.snapshot.profile)
            .map_err(projection)?;
        check(&state)?;
        let valid = |bound: &ModernLeasedCandidate| current(&state, bound).is_ok();
        let (result, apply) = prepare(&inventory, &valid);
        check(&state)?;
        state.lease_turn = Some(pending.next.turn.clone());
        state.renewal_turn = None;
        state.modern_fact = Some(super::super::super::ModernFact::Eligible {
            observation: pending.next.turn.clone(),
            names,
        });
        apply();
        drop(state);
        Ok((inventory, result))
    }
}
impl ModernLeasedCandidate {
    // Internal route copies retain facts only, never viewer grants.
    pub(crate) fn copy_for_worker(&self) -> Self {
        let c = &self.candidate.candidate.candidate;
        Self {
            candidate: ModernBoundCandidate {
                candidate: BoundCandidate {
                    candidate: super::super::super::Candidate {
                        revision: c.revision,
                        profile: c.profile.clone(),
                        name: c.name.clone(),
                        policy: c.policy.clone(),
                        kind: c.kind,
                        selected_static: c.selected_static,
                    },
                },
                server_id: self.server_id().clone(),
            },
            ticket: self.ticket.clone(),
        }
    }
    pub(crate) fn equivalent_route(&self, other: &Self) -> bool {
        let a = &self.candidate.candidate.candidate;
        let b = &other.candidate.candidate.candidate;
        a.revision == b.revision
            && Arc::ptr_eq(&a.profile, &b.profile)
            && a.name == b.name
            && a.policy == b.policy
            && a.kind == b.kind
            && a.selected_static == b.selected_static
            && self.server_id() == other.server_id()
    }
}
