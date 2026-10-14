//! Separate input-owner and viewer permissions share transport, never authority.
use super::super::ActivatedCandidate;
use super::*;
#[derive(Clone)]
pub(super) enum Permission {
    Viewer(Arc<AdmittedCandidate>),
    Activation(Arc<ActivatedCandidate>),
}
impl Permission {
    pub fn candidate(&self) -> &super::super::super::CoordinatedCandidate {
        match self {
            Self::Viewer(a) => &a.candidate,
            Self::Activation(a) => &a.candidate,
        }
    }
    pub fn name(&self) -> &str {
        match self {
            Self::Viewer(a) => &a.name,
            Self::Activation(a) => &a.name,
        }
    }
    pub fn selection(&self) -> Option<super::super::super::ReplicaRoute> {
        match self {
            Self::Viewer(a) => a.selection.clone(),
            Self::Activation(a) => a.selection.clone(),
        }
    }
    pub fn counts_as_viewer(&self) -> bool {
        matches!(self, Self::Viewer(_))
    }
    pub fn at_scope<T>(&self, commit: impl FnOnce() -> T) -> Result<T, AdmissionFailure> {
        match self {
            Self::Viewer(a) => a.at_scope(commit),
            Self::Activation(a) => a.at_scope(commit),
        }
    }
    pub fn is_cancelled(&self) -> bool {
        match self {
            Self::Viewer(a) => a.is_cancelled(),
            Self::Activation(a) => a.is_cancelled(),
        }
    }
    pub async fn cancelled(&self) {
        match self {
            Self::Viewer(a) => a.cancelled().await,
            Self::Activation(a) => a.cancelled().await,
        }
    }
    pub fn playback(&self) {
        if let Self::Viewer(a) = self
            && !a.background
        {
            a.grant.playback();
        }
    }
    pub fn add_bytes(&self, n: usize) {
        if let Self::Viewer(a) = self
            && !a.background
        {
            a.grant.add_bytes(n);
        }
    }
}
