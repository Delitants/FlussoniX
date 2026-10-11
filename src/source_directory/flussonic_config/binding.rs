//! Opaque inventory results and media construction bound to a local profile.
//! This is not daemon routing, viewer admission, a lease or verified topology.
use super::{Candidate, CandidateView, ProjectionFailure, SourceAuthority, SourceSnapshot};
use crate::{
    m4_ingest::flussonic::MediaInput,
    source_directory::{LookupFailure, flussonic::InventoryStream},
};
use std::collections::BTreeMap;

/// Generic diagnostics; no endpoint, name, credentials or upstream body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingFailure {
    Projection(ProjectionFailure),
    Inventory(LookupFailure),
    Missing,
}
/// Only a complete fetch through the configured inventory client constructs this
/// type. Facts cannot be imported or mutated; no Debug/serialization is provided.
pub struct BoundInventory {
    snapshot: SourceSnapshot,
    rows: BTreeMap<String, InventoryStream>,
}
/// Constructed only from an opaque bound result. A plain/manual Candidate cannot
/// be substituted at the bound media construction/publication entry point.
pub struct BoundCandidate {
    candidate: Candidate,
}
impl SourceAuthority {
    /// Capture the profile before I/O, without holding the publication lock across
    /// await. Recheck identity after success OR error; stale work never becomes a
    /// current lookup result. Cancellation drops the direct client future.
    pub async fn inventory(&self) -> Result<BoundInventory, BindingFailure> {
        let snapshot = self.snapshot().map_err(BindingFailure::Projection)?;
        let result = snapshot.profile.inventory.inventory().await;
        let state = self
            .current(snapshot.revision, &snapshot.profile)
            .map_err(BindingFailure::Projection)?;
        let result = result
            .map(|rows| BoundInventory {
                snapshot,
                rows: rows.into_iter().map(|r| (r.name.clone(), r)).collect(),
            })
            .map_err(BindingFailure::Inventory);
        drop(state);
        result
    }
    /// Construct a media input for this bound candidate under the current-profile
    /// lock and invoke a synchronous publication closure. No network I/O occurs
    /// here. Never await or re-enter this authority inside the closure. The caller
    /// must authorize viewers and re-fence asynchronous work before private I/O.
    /// Exported inputs are NOT revoked when settings change; worker/session
    /// lifetime belongs to the future daemon adapter and its global fence.
    pub fn at_bound_current<T>(
        &self,
        bound: &BoundCandidate,
        commit: impl FnOnce(CandidateView<'_>, MediaInput) -> T,
    ) -> Result<T, ProjectionFailure> {
        self.at_current(&bound.candidate, |view| {
            let input = bound
                .candidate
                .profile
                .media
                .input(view.name)
                .map_err(|_| ProjectionFailure::Invalid)?;
            Ok(commit(view, input))
        })?
    }
}
impl BoundInventory {
    /// Literal names in sorted order. Discovery facts alone grant no permission.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.rows.keys().map(String::as_str)
    }
    /// Apply mandatory local-origin preflight, namespace/exclusions, selection and
    /// local policy to facts from this exact fetch. Old results may still create
    /// candidates, but those candidates cannot pass the current-profile fence.
    pub fn candidate(&self, name: &str) -> Result<BoundCandidate, BindingFailure> {
        let row = self.rows.get(name).ok_or(BindingFailure::Missing)?;
        self.snapshot
            .candidate(row)
            .map(|candidate| BoundCandidate { candidate })
            .map_err(BindingFailure::Projection)
    }
}
