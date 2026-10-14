//! Private rejection commits at the source fence, independently of scheduler delay.
use super::*;
use crate::m4_ingest::flussonic::InputFailure;
/// Constructed only while a managed input holds source/lease and route authority.
pub(crate) struct ReplicaInputFailure {
    selector: Arc<ReplicaSelector>,
    handle: SourceHandle,
    stamp: RouteStamp,
    turn: Arc<()>,
    pub(crate) kind: InputFailure,
}
fn matches(entry: &Entry, stamp: &RouteStamp) -> bool {
    entry.available
        && !entry.blocked
        && entry.selected == stamp.selected
        && Arc::ptr_eq(&entry.route, &stamp.route)
        && !stamp.route.cancel.is_cancelled()
}
impl ReplicaRoute {
    pub(crate) async fn cancelled(&self) {
        self.stamp.route.cancel.cancelled().await;
    }
    /// Successful renewal absence and authoritative renewal errors must latch
    /// under the source publication lock, before a later outage can hide them.
    pub(crate) fn reject_inventory(&self, error: LeaseFailure) -> bool {
        if !super::lookup::authoritative(CoordinationFailure::Lease(error)) {
            return false;
        }
        let Ok(mut state) = self.stamp.selector.state() else {
            return false;
        };
        let Some(entry) = state.entries.get_mut(&self.stamp.name) else {
            return false;
        };
        if !matches(entry, &self.stamp) {
            return false;
        }
        entry.available = false;
        entry.blocked = true;
        entry.route.cancel.cancel();
        entry.turn = Arc::new(());
        entry.pending_failure = None;
        true
    }
    /// Caller holds Config -> coordinator -> source -> managed route. No awaits.
    pub(crate) fn accept_private<T>(
        &self,
        commit: impl FnOnce() -> Result<T, super::super::AdmissionFailure>,
    ) -> Result<T, super::super::AdmissionFailure> {
        let mut state = self
            .stamp
            .selector
            .state()
            .map_err(|_| super::super::AdmissionFailure::Revoked)?;
        let entry = state
            .entries
            .get_mut(&self.stamp.name)
            .ok_or(super::super::AdmissionFailure::Revoked)?;
        if !matches(entry, &self.stamp) {
            return Err(super::super::AdmissionFailure::Revoked);
        }
        let result = commit()?;
        entry.private_rejected = false;
        Ok(result)
    }
    pub(crate) fn input_failed(
        &self,
        handle: &SourceHandle,
        kind: InputFailure,
    ) -> Result<ReplicaInputFailure, super::super::AdmissionFailure> {
        let mut state = self
            .stamp
            .selector
            .state()
            .map_err(|_| super::super::AdmissionFailure::Revoked)?;
        let entry = state
            .entries
            .get_mut(&self.stamp.name)
            .ok_or(super::super::AdmissionFailure::Revoked)?;
        if !matches(entry, &self.stamp) {
            return Err(super::super::AdmissionFailure::Revoked);
        }
        if matches!(kind, InputFailure::Rejected | InputFailure::Invalid) {
            entry.private_rejected = true;
            entry.blocked = true;
        }
        entry.available = false;
        entry.route.cancel.cancel();
        entry.turn = Arc::new(());
        let turn = Arc::new(());
        entry.pending_failure = Some(turn.clone());
        Ok(ReplicaInputFailure {
            selector: self.stamp.selector.clone(),
            handle: handle.clone(),
            stamp: self.stamp.clone(),
            turn,
            kind,
        })
    }
}
impl ReplicaSelector {
    pub(crate) fn private_rejected(&self, name: &str) -> Result<bool, ReplicaFailure> {
        self.at_plan(|| {
            Ok(self
                .state()?
                .entries
                .get(name)
                .is_some_and(|e| e.private_rejected))
        })
    }
    /// The input already checked its lease at observation. A later lease renewal
    /// or expiry must not discard a recorded denial. Keep the installed source,
    /// configuration, owner and exact failed route/notice generation fences.
    pub(crate) fn failure_current(&self, notice: &ReplicaInputFailure) -> bool {
        if !std::ptr::eq(self, Arc::as_ptr(&notice.selector)) {
            return false;
        }
        self.owner
            .at_handle(&notice.handle, || {
                Ok({
                    self.state().is_ok_and(|s| {
                        s.entries.get(&notice.stamp.name).is_some_and(|e| {
                            !e.available
                                && e.selected == notice.stamp.selected
                                && Arc::ptr_eq(&e.route, &notice.stamp.route)
                                && e.pending_failure
                                    .as_ref()
                                    .is_some_and(|turn| Arc::ptr_eq(turn, &notice.turn))
                        })
                    })
                })
            })
            .unwrap_or(false)
    }
}
