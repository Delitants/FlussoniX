//! Trusted static input demand, deliberately carrying no viewer permission.
use super::*;
use tokio_util::sync::CancellationToken;
/// Local selected-static ownership. Constructed only from a current leased
/// candidate, never from caller-provided flags or native viewer/peer grants.
/// Embeddings must independently authorize every network viewer.
pub struct ActivatedCandidate {
    pub(super) candidate: CoordinatedCandidate,
    pub(super) name: String,
    pub(super) selection: Option<super::super::ReplicaRoute>,
    cancel: CancellationToken,
}
/// Cancellation only; cloning this control does not retain input demand.
#[derive(Clone)]
pub struct ActivationControl {
    cancel: CancellationToken,
}
impl ActivationControl {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}
impl ActivatedCandidate {
    pub fn control(&self) -> ActivationControl {
        ActivationControl {
            cancel: self.cancel.clone(),
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled() || self.selection.as_ref().is_some_and(|s| s.is_cancelled())
    }
    pub async fn cancelled(&self) {
        match &self.selection {
            Some(scope) => tokio::select! {_=self.cancel.cancelled()=>{},_=scope.cancelled()=>{}},
            None => self.cancel.cancelled().await,
        }
    }
    pub(super) fn at_scope<T>(&self, commit: impl FnOnce() -> T) -> Result<T, AdmissionFailure> {
        match &self.selection {
            Some(scope) => scope
                .at_current(commit)
                .map_err(|_| AdmissionFailure::Revoked),
            None => Ok(commit()),
        }
    }
}
impl Drop for ActivatedCandidate {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl ModernSourceCoordinator {
    /// Grant locally selected static demand; no callback, viewer policy
    /// publication, session reservation or playback promotion is performed.
    pub fn activate(
        &self,
        candidate: CoordinatedCandidate,
    ) -> Result<ActivatedCandidate, AdmissionFailure> {
        self.activate_scoped(candidate, None)
    }
    pub(crate) fn activate_scoped(
        &self,
        candidate: CoordinatedCandidate,
        selection: Option<super::super::ReplicaRoute>,
    ) -> Result<ActivatedCandidate, AdmissionFailure> {
        let name = self
            .publish(&candidate, |view, _| view.name.to_owned())
            .map_err(AdmissionFailure::Coordination)?;
        self.at_admission_candidate(&candidate, &name, |view, _| {
            if !view.selected_static {
                return Err(AdmissionFailure::Invalid);
            }
            if let Some(scope) = &selection {
                scope
                    .at_current(|| ())
                    .map_err(|_| AdmissionFailure::Revoked)?;
            }
            Ok(())
        })?;
        Ok(ActivatedCandidate {
            candidate,
            name,
            selection,
            cancel: CancellationToken::new(),
        })
    }
    /// Initial candidate fence. Attached owners use the shared generation's
    /// current route so safe renewal can replace its original lease ticket.
    pub fn activated_current(
        &self,
        activated: &ActivatedCandidate,
    ) -> Result<(), AdmissionFailure> {
        self.at_admission_candidate(&activated.candidate, &activated.name, |view, _| {
            if !view.selected_static || activated.is_cancelled() {
                return Err(AdmissionFailure::Revoked);
            }
            activated.at_scope(|| ())
        })
    }
}
