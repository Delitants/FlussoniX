use super::*;
use crate::source_directory::flussonic_config::ProjectionFailure;

fn transient(error: CoordinationFailure) -> bool {
    matches!(
        error,
        CoordinationFailure::Lease(LeaseFailure::Binding(BindingFailure::Inventory(
            LookupFailure::Unavailable
        )))
    )
}
pub(super) fn authoritative(error: CoordinationFailure) -> bool {
    matches!(
        error,
        CoordinationFailure::Lease(LeaseFailure::Binding(
            BindingFailure::Missing
                | BindingFailure::Inventory(LookupFailure::Absent | LookupFailure::Invalid)
                | BindingFailure::Projection(
                    ProjectionFailure::Invalid
                        | ProjectionFailure::Unsupported
                        | ProjectionFailure::Limit
                        | ProjectionFailure::Excluded
                        | ProjectionFailure::Ineligible(_)
                        | ProjectionFailure::Policy(_)
                )
        ))
    )
}

enum Observation {
    Candidate(CoordinatedCandidate),
    Rejected(Result<Option<ReplicaChoice>, ReplicaFailure>),
    Failed(CoordinationFailure),
}

impl ReplicaLookup {
    // Caller holds either the current source handle fence (trusted error input)
    // or the full inventory lease fence (opaque positive/negative facts).
    fn reject_current(
        &self,
        selected: usize,
        authoritative: bool,
    ) -> Result<Option<ReplicaChoice>, ReplicaFailure> {
        let mut state = self.selector.state()?;
        let entry = state
            .entries
            .get_mut(&self.name)
            .ok_or(ReplicaFailure::Stale)?;
        if !Arc::ptr_eq(&entry.turn, &self.turn) {
            return Err(ReplicaFailure::Stale);
        }
        if selected == self.first {
            entry.available = false;
            entry.route.cancel.cancel();
            if authoritative {
                entry.blocked = true;
            }
            if entry.blocked {
                return Err(ReplicaFailure::Blocked);
            }
        }
        Ok(None)
    }
    fn finish_rejection(
        &mut self,
        outcome: Result<Option<ReplicaChoice>, ReplicaFailure>,
    ) -> Result<Option<ReplicaChoice>, ReplicaFailure> {
        self.offset += 1;
        if matches!(outcome, Err(ReplicaFailure::Blocked)) {
            self.finished = true;
        }
        outcome
    }
    fn reject(
        &mut self,
        handle: &SourceHandle,
        authoritative: bool,
    ) -> Result<Option<ReplicaChoice>, ReplicaFailure> {
        let selected = (self.first + self.offset) % self.selector.members.len();
        let outcome = self
            .selector
            .owner
            .at_handle(handle, || Ok(self.reject_current(selected, authoritative)))
            .map_err(ReplicaFailure::Coordination)?;
        self.finish_rejection(outcome)
    }
    pub fn next_source(&self) -> Result<Option<String>, ReplicaFailure> {
        self.selector.at_plan(|| {
            let state = self.selector.state()?;
            let entry = state.entries.get(&self.name).ok_or(ReplicaFailure::Stale)?;
            if !Arc::ptr_eq(&entry.turn, &self.turn) {
                return Err(ReplicaFailure::Stale);
            }
            if self.finished
                || self.offset >= self.selector.members.len()
                || ((entry.blocked || entry.private_rejected) && self.offset > 0)
            {
                return Ok(None);
            }
            Ok(Some(
                self.selector.members[(self.first + self.offset) % self.selector.members.len()]
                    .source_key
                    .clone(),
            ))
        })
    }
    /// Supply the original source handle and its discovery result together.
    /// Errors are trusted scheduling input; only current opaque candidates can
    /// produce a choice. This method performs no network I/O.
    pub fn observe(
        &mut self,
        handle: &SourceHandle,
        result: Result<CoordinatedInventory, CoordinationFailure>,
    ) -> Result<Option<ReplicaChoice>, ReplicaFailure> {
        self.observe_cached(handle, result.as_ref().map_err(|error| *error))
    }
    pub(crate) fn observe_cached(
        &mut self,
        handle: &SourceHandle,
        result: Result<&CoordinatedInventory, CoordinationFailure>,
    ) -> Result<Option<ReplicaChoice>, ReplicaFailure> {
        let expected = self.next_source()?.ok_or(ReplicaFailure::Invalid)?;
        if handle.stamp.key != expected
            || handle.stamp.revision != self.selector.revision
            || !Arc::ptr_eq(&handle.stamp.identity, &self.selector.identity)
        {
            return Err(ReplicaFailure::Stale);
        }
        let selected = (self.first + self.offset) % self.selector.members.len();
        let uniform = self
            .selector
            .owner
            .at_handle(handle, || {
                Ok(handle.authority.uniform_policy().ok().flatten()
                    == Some(self.selector.policy.clone()))
            })
            .map_err(ReplicaFailure::Coordination)?;
        if !uniform {
            return self.reject(handle, true);
        }
        let result = match result {
            Ok(inventory) => {
                if !Arc::ptr_eq(&handle.authority, &inventory.handle.authority) {
                    return Err(ReplicaFailure::Stale);
                }
                let observation = self
                    .selector
                    .owner
                    .at_inventory_current(inventory, || match inventory.candidate(&self.name) {
                        Ok(candidate) => Observation::Candidate(candidate),
                        Err(error) if transient(error) || authoritative(error) => {
                            Observation::Rejected(
                                self.reject_current(selected, authoritative(error)),
                            )
                        }
                        Err(error) => Observation::Failed(error),
                    })
                    .map_err(ReplicaFailure::Coordination)?;
                match observation {
                    Observation::Candidate(candidate) => Ok(candidate),
                    Observation::Rejected(outcome) => return self.finish_rejection(outcome),
                    Observation::Failed(error) => return Err(ReplicaFailure::Coordination(error)),
                }
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(candidate) => {
                let stamp = self
                    .selector
                    .owner
                    .publish(&candidate, |view, _| {
                        if view.policy != &self.selector.policy {
                            return Err(ReplicaFailure::PolicyMismatch);
                        }
                        let mut state = self.selector.state()?;
                        let entry = state
                            .entries
                            .get_mut(&self.name)
                            .ok_or(ReplicaFailure::Stale)?;
                        if !Arc::ptr_eq(&entry.turn, &self.turn) {
                            return Err(ReplicaFailure::Stale);
                        }
                        if !entry.available || entry.selected != selected || entry.blocked {
                            entry.route.cancel.cancel();
                            entry.route = Epoch::new();
                            if entry.selected != selected {
                                entry.switches = entry.switches.saturating_add(1);
                            }
                        }
                        entry.selected = selected;
                        entry.available = true;
                        entry.blocked = false;
                        entry.pending_failure = None;
                        Ok(RouteStamp {
                            selector: self.selector.clone(),
                            name: self.name.clone(),
                            selected,
                            route: entry.route.clone(),
                        })
                    })
                    .map_err(ReplicaFailure::Coordination)??;
                self.finished = true;
                Ok(Some(ReplicaChoice { candidate, stamp }))
            }
            Err(error) => {
                if !transient(error) && !authoritative(error) {
                    return Err(ReplicaFailure::Coordination(error));
                }
                self.reject(handle, authoritative(error))
            }
        }
    }
}
