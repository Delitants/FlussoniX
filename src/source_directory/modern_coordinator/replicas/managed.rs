//! Managed route fencing is independent of the source's renewable lease ticket.
use super::*;
impl ReplicaSelector {
    /// Preserve only trusted local diagnostics/tombstones for an unchanged
    /// relationship. No old input, lease, admission or route epoch is imported.
    pub(crate) fn inherit_unchanged(&self, old: &Self) -> bool {
        if self.group != old.group
            || self.policy != old.policy
            || !Arc::ptr_eq(&self.owner, &old.owner)
            || !Arc::ptr_eq(&self.identity, &old.identity)
            || self.members.len() != old.members.len()
            || self
                .members
                .iter()
                .zip(&old.members)
                .any(|(a, b)| a.source_key != b.source_key || a.content_id != b.content_id)
        {
            return false;
        }
        let Ok(previous) = old.state.lock() else {
            return false;
        };
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        state.entries = previous
            .entries
            .iter()
            .map(|(name, e)| {
                (
                    name.clone(),
                    Entry {
                        selected: e.selected,
                        available: false,
                        blocked: e.blocked,
                        private_rejected: e.private_rejected,
                        pending_failure: None,
                        switches: e.switches,
                        turn: Arc::new(()),
                        route: Epoch::new(),
                    },
                )
            })
            .collect();
        true
    }
    pub(crate) fn choice_for(
        self: &Arc<Self>,
        candidate: CoordinatedCandidate,
        name: &str,
    ) -> Result<ReplicaChoice, ReplicaFailure> {
        let stamp = self
            .owner
            .publish(&candidate, |view, _| {
                if view.name != name || view.policy != &self.policy {
                    return Err(ReplicaFailure::PolicyMismatch);
                }
                let state = self.state()?;
                let entry = state.entries.get(name).ok_or(ReplicaFailure::Stale)?;
                if !entry.available
                    || entry.blocked
                    || entry.route.cancel.is_cancelled()
                    || candidate.source_key() != self.members[entry.selected].source_key
                {
                    return Err(ReplicaFailure::Stale);
                }
                Ok(RouteStamp {
                    selector: self.clone(),
                    name: name.into(),
                    selected: entry.selected,
                    route: entry.route.clone(),
                })
            })
            .map_err(ReplicaFailure::Coordination)??;
        Ok(ReplicaChoice { candidate, stamp })
    }
    pub(crate) fn fail_current(
        &self,
        candidate: &CoordinatedCandidate,
        route: &ReplicaRoute,
        blocked: bool,
    ) -> Result<(), ReplicaFailure> {
        if !std::ptr::eq(self, Arc::as_ptr(&route.stamp.selector)) {
            return Err(ReplicaFailure::Stale);
        }
        self.owner
            .publish(candidate, |view, _| {
                if view.policy != &self.policy
                    || candidate.source_key() != self.members[route.stamp.selected].source_key
                {
                    return Err(ReplicaFailure::PolicyMismatch);
                }
                let mut state = self.state()?;
                let entry = state
                    .entries
                    .get_mut(&route.stamp.name)
                    .ok_or(ReplicaFailure::Stale)?;
                if !entry.available
                    || entry.selected != route.stamp.selected
                    || !Arc::ptr_eq(&entry.route, &route.stamp.route)
                    || entry.route.cancel.is_cancelled()
                {
                    return Err(ReplicaFailure::Stale);
                }
                entry.available = false;
                entry.blocked |= blocked;
                entry.route.cancel.cancel();
                entry.turn = Arc::new(());
                Ok(())
            })
            .map_err(ReplicaFailure::Coordination)?
    }
    pub(crate) fn begin_after_media(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<ReplicaLookup, ReplicaFailure> {
        let mut lookup = self.begin(name)?;
        lookup.offset = 1;
        Ok(lookup)
    }
}
impl ReplicaChoice {
    pub(crate) fn config_revision(&self) -> u64 {
        self.candidate.config_revision()
    }
}

impl ReplicaChoice {
    pub(crate) fn into_managed(self) -> (CoordinatedCandidate, ReplicaRoute) {
        (self.candidate, ReplicaRoute { stamp: self.stamp })
    }
    pub(crate) fn fail_current(&self, blocked: bool) -> Result<(), ReplicaFailure> {
        self.stamp.selector.fail_current(
            &self.candidate,
            &ReplicaRoute {
                stamp: self.stamp.clone(),
            },
            blocked,
        )
    }
}

impl ReplicaChoice {
    pub(crate) fn publish_policy(&self, auth: &PlaybackAuth) -> bool {
        self.stamp
            .selector
            .owner
            .publish(&self.candidate, |view, _| {
                self.stamp.selector.at_stamp(&self.stamp, || {
                    auth.publish(view.name, Some(view.policy.clone())).is_some()
                })
            })
            .is_ok_and(|result| result.unwrap_or(false))
    }
}
impl super::ReplicaRoute {
    pub(crate) fn activate(
        self,
        candidate: CoordinatedCandidate,
    ) -> Result<super::super::ActivatedCandidate, ReplicaFailure> {
        let owner = self.stamp.selector.owner.clone();
        owner
            .activate_scoped(candidate, Some(self))
            .map_err(ReplicaFailure::Admission)
    }
}
