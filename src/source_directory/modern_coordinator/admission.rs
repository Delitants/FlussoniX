//! Opt-in direct-source viewer admission. No automatic routing or worker owner.
mod pull;
mod shared;
use super::{CoordinatedCandidate, CoordinationFailure, ModernSourceCoordinator};
use crate::{
    m4_ingest::flussonic::MediaInput,
    playback_auth::{AuthOutcome, Grant, PlaybackAuth, ViewerRequest},
    source_directory::flussonic_config::CandidateView,
};
pub use pull::{PrivatePullFailure, PrivatePullOptions};
pub(crate) use shared::SharedFeed;
pub use shared::{
    SharedFailure, SharedInput, SharedInputAttachment, SharedInputOptions, SharedInputs,
    SharedSubscription, SharedViewer,
};

/// Generic diagnostics contain no names, callback URLs or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionFailure {
    Invalid,
    Unavailable,
    LocalStream,
    Revoked,
    Coordination(CoordinationFailure),
}
/// Native denial/redirect semantics; redirects never carry a media grant.
pub enum AdmissionOutcome {
    Allowed(Box<AdmittedCandidate>),
    Denied,
    Redirect(String),
}
/// Owns one real native control grant and its exact direct-source candidate.
/// Not Clone, Debug or serializable. It does not own or stop a shared worker.
pub struct AdmittedCandidate {
    candidate: CoordinatedCandidate,
    name: String,
    grant: Grant,
}
impl AdmittedCandidate {
    /// Grant revocation only; use admitted_current for source/config/lease checks.
    pub fn is_cancelled(&self) -> bool {
        self.grant.is_cancelled()
    }
    pub async fn cancelled(&self) {
        self.grant.cancelled().await
    }
}
impl ModernSourceCoordinator {
    fn at_admission_candidate<T>(
        &self,
        candidate: &CoordinatedCandidate,
        name: &str,
        commit: impl FnOnce(CandidateView<'_>, MediaInput) -> Result<T, AdmissionFailure>,
    ) -> Result<T, AdmissionFailure> {
        self.at_handle_root(&candidate.handle, |root| {
            candidate
                .handle
                .authority
                .at_modern_leased_current(&candidate.candidate, |view, input| {
                    if view.name != name {
                        return Err(AdmissionFailure::Invalid);
                    }
                    // Even disabled locally configured streams take precedence.
                    // Check under the same global lock, before policy publication.
                    if root["streams"]
                        .as_array()
                        .is_some_and(|streams| streams.iter().any(|stream| stream["name"] == name))
                    {
                        return Err(AdmissionFailure::LocalStream);
                    }
                    commit(view, input)
                })
                .map_err(CoordinationFailure::Lease)
        })
        .map_err(AdmissionFailure::Coordination)?
    }

    /// Publish the exact current local policy, authorize through the embedding
    /// owner's shared native authorizer, then re-fence every decision after await.
    /// No lock spans callback I/O; no private request starts here. Callers must
    /// select the source first and use App.playback_auth when embedded in App.
    pub async fn admit(
        &self,
        candidate: CoordinatedCandidate,
        auth: &PlaybackAuth,
        request: ViewerRequest,
    ) -> Result<AdmissionOutcome, AdmissionFailure> {
        let name = request.name.clone();
        let snapshot = self.at_admission_candidate(&candidate, &name, |view, _| {
            auth.publish(view.name, Some(view.policy.clone()))
                .ok_or(AdmissionFailure::Unavailable)
        })?;
        // Control admission holds native capacity without marking playback demand
        // until the consumer enters a current media handoff.
        let outcome = auth.authorize_control(snapshot, request).await;
        let outcome = self.at_admission_candidate(&candidate, &name, |_, _| {
            if matches!(&outcome, AuthOutcome::Allowed(grant) if grant.is_cancelled()) {
                return Err(AdmissionFailure::Revoked);
            }
            Ok(outcome)
        })?;
        Ok(match outcome {
            AuthOutcome::Allowed(grant) => AdmissionOutcome::Allowed(Box::new(AdmittedCandidate {
                candidate,
                name,
                grant,
            })),
            AuthOutcome::Denied => AdmissionOutcome::Denied,
            AuthOutcome::Redirect(target) => AdmissionOutcome::Redirect(target),
        })
    }

    /// Recheck the exact owner/config/source/refresh/lease and native revocation.
    /// This is an instant check, not a background revocation or lease renewal.
    pub fn admitted_current(&self, admitted: &AdmittedCandidate) -> Result<(), AdmissionFailure> {
        // Name and local precedence were established when this opaque admission
        // was created. The identical config revision preserves those facts.
        // Frequent frame/write checks need no media client/key allocation or
        // stream-list scan; retain every owner/profile/refresh/expiry fence.
        self.at_current_candidate(&admitted.candidate, || {
            if admitted.grant.is_cancelled() {
                Err(AdmissionFailure::Revoked)
            } else {
                Ok(())
            }
        })
    }

    // Shared inputs fence their source independently of any particular viewer.
    fn at_current_candidate<T>(
        &self,
        candidate: &CoordinatedCandidate,
        commit: impl FnOnce() -> Result<T, AdmissionFailure>,
    ) -> Result<T, AdmissionFailure> {
        self.at_handle(&candidate.handle, || {
            let guard = candidate
                .handle
                .authority
                .guard_modern_lease(&candidate.candidate)
                .map_err(CoordinationFailure::Lease)?;
            let result = commit();
            guard.check().map_err(CoordinationFailure::Lease)?;
            Ok(result)
        })
        .map_err(AdmissionFailure::Coordination)?
    }

    /// Re-fence immediately before synchronous media handoff and promote the
    /// native control grant to playback demand. Never await/reenter these owners
    /// in commit. Async consumers must retain the admission, watch cancelled(),
    /// and recheck admitted_current before private I/O and delivery. Exported
    /// clients are not automatically revoked and no worker is started/stopped.
    pub fn publish_admitted<T>(
        &self,
        admitted: &AdmittedCandidate,
        commit: impl FnOnce(CandidateView<'_>, MediaInput, &Grant) -> T,
    ) -> Result<T, AdmissionFailure> {
        self.at_admission_candidate(&admitted.candidate, &admitted.name, |view, input| {
            if admitted.grant.is_cancelled() {
                return Err(AdmissionFailure::Revoked);
            }
            admitted.grant.playback();
            Ok(commit(view, input, &admitted.grant))
        })
    }
}
