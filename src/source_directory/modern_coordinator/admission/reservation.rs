//! Short-lived control routing authority. No viewer grant or input demand.
use super::*;

pub(crate) struct ReservedCandidate {
    candidate: CoordinatedCandidate,
    selection: Option<super::super::ReplicaRoute>,
}
impl ReservedCandidate {
    pub(crate) fn source_key(&self) -> &str {
        self.candidate.source_key()
    }
    pub(crate) fn config_revision(&self) -> u64 {
        self.candidate.config_revision()
    }
}
impl ModernSourceCoordinator {
    pub(crate) fn reserve_candidate(
        &self,
        candidate: CoordinatedCandidate,
        selection: Option<super::super::ReplicaRoute>,
        name: &str,
    ) -> Result<ReservedCandidate, AdmissionFailure> {
        self.at_admission_candidate(&candidate, name, |_, _| Ok(()))?;
        let reserved = ReservedCandidate {
            candidate,
            selection,
        };
        self.with_reserved_current(&reserved, || ())?;
        Ok(reserved)
    }
    /// Synchronous non-reentrant control commit, including post-commit expiry.
    pub(crate) fn with_reserved_current<T>(
        &self,
        reserved: &ReservedCandidate,
        commit: impl FnOnce() -> T,
    ) -> Result<T, AdmissionFailure> {
        self.at_current_candidate(&reserved.candidate, || match &reserved.selection {
            Some(scope) => scope
                .at_current(commit)
                .map_err(|_| AdmissionFailure::Revoked),
            None => Ok(commit()),
        })
    }
    /// Every redeemer receives a real independent native viewer decision against
    /// the exact reserved route. No discovery or alternate-origin selection.
    pub(crate) async fn admit_reserved(
        &self,
        reserved: &ReservedCandidate,
        auth: &PlaybackAuth,
        request: ViewerRequest,
    ) -> Result<AdmissionOutcome, AdmissionFailure> {
        let name = request.name.clone();
        let candidate = self.with_reserved_current(reserved, || CoordinatedCandidate {
            handle: reserved.candidate.handle.clone(),
            candidate: reserved.candidate.candidate.copy_for_worker(),
        })?;
        let mut outcome = self.admit(candidate, auth, request).await?;
        if let AdmissionOutcome::Allowed(admitted) = &mut outcome {
            self.at_admission_candidate(&reserved.candidate, &name, |_, _| {
                if let Some(scope) = &reserved.selection {
                    scope.commit().map_err(|_| AdmissionFailure::Revoked)?;
                }
                Ok(())
            })?;
            admitted.selection = reserved.selection.clone();
        } else {
            self.with_reserved_current(reserved, || ())?;
        }
        Ok(outcome)
    }
}
