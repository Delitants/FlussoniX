//! Explicit equivalent-origin selection. No daemon activation or background I/O.
mod failure;
mod lookup;
mod managed;
mod pull;
mod registration;
mod summary;
use super::{
    CoordinatedCandidate, CoordinatedInventory, CoordinationFailure, LocalIdentity,
    ModernSourceCoordinator, SourceHandle,
};
use crate::{
    m4_ingest::flussonic::MediaInput,
    playback_auth::{Grant, PlaybackAuth, Policy, ViewerRequest},
    source_directory::{
        LookupFailure,
        flussonic_config::{BindingFailure, CandidateView, LeaseFailure, SourceAuthority},
    },
};
pub(crate) use failure::ReplicaInputFailure;
pub use pull::ReplicaPullFailure;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use summary::NegativeSummary;
use tokio_util::sync::CancellationToken;

/// Trusted local intent. Matching names or observed server IDs do not create it.
#[derive(Clone)]
pub struct ReplicaMember {
    pub source_key: String,
    pub content_id: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaFailure {
    Invalid,
    Limit,
    Stale,
    Closed,
    Blocked,
    PolicyMismatch,
    Coordination(CoordinationFailure),
    Admission(super::AdmissionFailure),
}
/// Last selection observation; this never renews or establishes a media lease.
pub struct ReplicaStatus {
    pub group: String,
    pub selected_source: String,
    pub available: bool,
    pub blocked: bool,
    pub switches: u64,
}
pub struct ReplicaSelector {
    pub(crate) owner: Arc<ModernSourceCoordinator>,
    identity: Arc<LocalIdentity>,
    revision: u64,
    // Digest of ordered source definitions, backends and resolved processing;
    // credentials are compared without retaining another plaintext copy.
    relationship: [u8; 32],
    // Each successful edit of a selected source relationship gets a new token.
    // This detects delete/re-add even if the final JSON equals the old JSON.
    relationship_lineages: Vec<Arc<()>>,
    group: String,
    members: Vec<ReplicaMember>,
    // Multi-origin equivalence requires a uniform policy. Single-origin routes
    // use each catalog candidate policy; recovery still requires the original grant.
    policy: Option<Policy>,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    closed: bool,
    permanent_shutdown: bool,
    entries: HashMap<String, Entry>,
    summaries: Vec<Option<NegativeSummary>>,
}
thread_local! {
    static HELD_CLOSURES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    static DEFERRED_RELEASES: RefCell<Vec<(usize, String, Arc<AtomicBool>)>> = const { RefCell::new(Vec::new()) };
}

// A fenced closure can drop another provisional owner while holding this
// selector's lock. Drain those releases under the same lock before returning;
// the queue is scoped to the closure and never becomes a lazy tombstone list.
struct FencedClosure<'a> {
    selector_id: usize,
    state: &'a mut State,
}
impl<'a> FencedClosure<'a> {
    fn new(selector: &ReplicaSelector, state: &'a mut State) -> Self {
        let selector_id = selector as *const _ as usize;
        HELD_CLOSURES.with(|held| held.borrow_mut().push(selector_id));
        Self { selector_id, state }
    }
}
impl Drop for FencedClosure<'_> {
    fn drop(&mut self) {
        DEFERRED_RELEASES.with(|releases| {
            let mut releases = releases.borrow_mut();
            let mut index = 0;
            while index < releases.len() {
                if releases[index].0 == self.selector_id {
                    let (_, name, identity) = releases.swap_remove(index);
                    ReplicaSelector::release_from(self.state, &name, &identity);
                } else {
                    index += 1;
                }
            }
        });
        HELD_CLOSURES.with(|held| {
            let mut held = held.borrow_mut();
            let index = held
                .iter()
                .rposition(|id| *id == self.selector_id)
                .expect("fenced selector closure marker");
            held.remove(index);
        });
    }
}
struct Entry {
    identity: Arc<AtomicBool>,
    committed: bool,
    selected: usize,
    available: bool,
    blocked: bool,
    private_rejected: bool,
    pending_failure: Option<Arc<()>>,
    switches: u64,
    turn: Arc<()>,
    route: Arc<Epoch>,
}
struct Epoch {
    cancel: CancellationToken,
}
impl Epoch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancel: CancellationToken::new(),
        })
    }
}
#[derive(Clone)]
struct RouteStamp {
    selector: Arc<ReplicaSelector>,
    name: String,
    identity: Arc<AtomicBool>,
    selected: usize,
    route: Arc<Epoch>,
}
/// A reserved lookup turn; a later turn rejects this turn's late success/errors.
pub struct ReplicaLookup {
    selector: Arc<ReplicaSelector>,
    name: String,
    identity: Arc<AtomicBool>,
    turn: Arc<()>,
    first: usize,
    offset: usize,
    finished: bool,
}
/// Current source candidate plus selection generation. No viewer/media grant.
pub struct ReplicaChoice {
    candidate: CoordinatedCandidate,
    stamp: RouteStamp,
}
/// The real native viewer grant plus its selection generation; not cloneable.
pub struct ReplicaAdmission {
    inner: Box<super::AdmittedCandidate>,
    stamp: RouteStamp,
}
pub enum ReplicaAdmissionOutcome {
    Allowed(Box<ReplicaAdmission>),
    Denied,
    Redirect(String),
}

impl ReplicaSelector {
    fn accepts_policy(&self, policy: &Policy) -> bool {
        self.policy
            .as_ref()
            .is_none_or(|expected| expected == policy)
    }
    // Config -> coordinator -> source (where present) -> selector. Never await.
    fn at_plan<T>(
        &self,
        commit: impl FnOnce() -> Result<T, ReplicaFailure>,
    ) -> Result<T, ReplicaFailure> {
        self.owner
            .config
            .at_revision(self.revision, |_| {
                let owner = self.owner.state().map_err(ReplicaFailure::Coordination)?;
                if owner.revision != self.revision || !Arc::ptr_eq(&owner.identity, &self.identity)
                {
                    return Err(ReplicaFailure::Stale);
                }
                commit()
            })
            .ok_or(ReplicaFailure::Stale)?
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>, ReplicaFailure> {
        let state = self.state.lock().map_err(|_| ReplicaFailure::Closed)?;
        if state.closed {
            return Err(ReplicaFailure::Closed);
        }
        Ok(state)
    }
    pub fn begin(self: &Arc<Self>, name: &str) -> Result<ReplicaLookup, ReplicaFailure> {
        crate::config::valid_name(name).map_err(|_| ReplicaFailure::Invalid)?;
        self.at_plan(|| {
            let mut state = self.state()?;
            if state.entries.len() >= 20_000 && !state.entries.contains_key(name) {
                return Err(ReplicaFailure::Limit);
            }
            let entry = state
                .entries
                .entry(name.to_owned())
                .or_insert_with(|| Entry {
                    identity: Arc::new(AtomicBool::new(false)),
                    committed: false,
                    selected: 0,
                    available: false,
                    blocked: false,
                    private_rejected: false,
                    pending_failure: None,
                    switches: 0,
                    turn: Arc::new(()),
                    route: Epoch::new(),
                });
            entry.turn = Arc::new(());
            Ok(ReplicaLookup {
                selector: self.clone(),
                name: name.to_owned(),
                identity: entry.identity.clone(),
                turn: entry.turn.clone(),
                first: entry.selected,
                offset: 0,
                finished: false,
            })
        })
    }
    // The map owns one token and every live lookup/route stamp owns another.
    // Check both identity and refcount under the selector lock: an old owner
    // cannot retire a replacement or a route still used by another caller.
    fn release_provisional(&self, name: &str, identity: &Arc<AtomicBool>) {
        // Accepted controls and static demand retain history permanently. The
        // flag also avoids re-entering this mutex when a retained viewer is
        // dropped from inside a fenced selector publication closure.
        if identity.load(Ordering::Acquire) {
            return;
        }
        match self.state.try_lock() {
            Ok(mut state) => Self::release_from(&mut state, name, identity),
            Err(TryLockError::WouldBlock) => {
                let id = self as *const _ as usize;
                let held_here = HELD_CLOSURES.with(|held| held.borrow().contains(&id));
                if held_here {
                    DEFERRED_RELEASES.with(|releases| {
                        releases
                            .borrow_mut()
                            .push((id, name.to_owned(), identity.clone()));
                    });
                } else if let Ok(mut state) = self.state.lock() {
                    Self::release_from(&mut state, name, identity);
                }
            }
            Err(TryLockError::Poisoned(_)) => {}
        }
    }
    fn release_from(state: &mut State, name: &str, identity: &Arc<AtomicBool>) {
        if state.entries.get(name).is_some_and(|entry| {
            !entry.committed
                && Arc::ptr_eq(&entry.identity, identity)
                && Arc::strong_count(identity) == 2
        }) {
            state.entries.remove(name);
        }
    }
    fn commit_route(&self, stamp: &RouteStamp) -> Result<(), ReplicaFailure> {
        self.commit_route_if(stamp, || true).map(|_| ())
    }
    fn commit_route_if(
        &self,
        stamp: &RouteStamp,
        accept: impl FnOnce() -> bool,
    ) -> Result<bool, ReplicaFailure> {
        if !std::ptr::eq(self, Arc::as_ptr(&stamp.selector)) {
            return Err(ReplicaFailure::Stale);
        }
        let mut state = self.state()?;
        let entry = state
            .entries
            .get(&stamp.name)
            .ok_or(ReplicaFailure::Stale)?;
        if !entry.available
            || entry.blocked
            || entry.selected != stamp.selected
            || !Arc::ptr_eq(&entry.identity, &stamp.identity)
            || !Arc::ptr_eq(&entry.route, &stamp.route)
            || stamp.route.cancel.is_cancelled()
        {
            return Err(ReplicaFailure::Stale);
        }
        let accepted = {
            let _fenced = FencedClosure::new(self, &mut state);
            accept()
        };
        if accepted {
            let entry = state
                .entries
                .get_mut(&stamp.name)
                .ok_or(ReplicaFailure::Stale)?;
            entry.committed = true;
            entry.identity.store(true, Ordering::Release);
        }
        Ok(accepted)
    }
    fn check_stamp(&self, stamp: &RouteStamp) -> Result<(), ReplicaFailure> {
        self.at_stamp(stamp, || ())
    }
    fn at_stamp<T>(
        &self,
        stamp: &RouteStamp,
        commit: impl FnOnce() -> T,
    ) -> Result<T, ReplicaFailure> {
        if !std::ptr::eq(self, Arc::as_ptr(&stamp.selector)) {
            return Err(ReplicaFailure::Stale);
        }
        let mut state = self.state()?;
        let entry = state
            .entries
            .get(&stamp.name)
            .ok_or(ReplicaFailure::Stale)?;
        if !entry.available
            || entry.blocked
            || entry.selected != stamp.selected
            || !Arc::ptr_eq(&entry.identity, &stamp.identity)
            || !Arc::ptr_eq(&entry.route, &stamp.route)
            || stamp.route.cancel.is_cancelled()
        {
            return Err(ReplicaFailure::Stale);
        }
        let result = {
            let _fenced = FencedClosure::new(self, &mut state);
            commit()
        };
        drop(state);
        Ok(result)
    }
    pub fn current(&self, choice: &ReplicaChoice) -> Result<(), ReplicaFailure> {
        self.owner
            .publish(&choice.candidate, |view, _| {
                if !self.accepts_policy(view.policy) {
                    return Err(ReplicaFailure::PolicyMismatch);
                }
                self.check_stamp(&choice.stamp)
            })
            .map_err(ReplicaFailure::Coordination)?
    }
    pub fn status(&self, name: &str) -> Result<ReplicaStatus, ReplicaFailure> {
        self.at_plan(|| {
            let state = self.state()?;
            let entry = state.entries.get(name).ok_or(ReplicaFailure::Invalid)?;
            Ok(ReplicaStatus {
                group: self.group.clone(),
                selected_source: self.members[entry.selected].source_key.clone(),
                available: entry.available,
                blocked: entry.blocked,
                switches: entry.switches,
            })
        })
    }
    /// Permanent lifecycle stop; consumers still own and join their input tasks.
    pub fn shutdown(&self) {
        self.close(true);
    }
    /// A configuration revision closes old routes but permits exact-relationship
    /// diagnostic transfer into the new selector.
    pub(crate) fn retire_for_revision(&self) {
        self.close(false);
    }
    fn close(&self, permanent: bool) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.permanent_shutdown |= permanent;
            for entry in state.entries.values() {
                entry.route.cancel.cancel();
            }
        }
    }

    /// Real native control admission; no callback lock or private input ownership.
    pub async fn admit(
        &self,
        choice: ReplicaChoice,
        auth: &PlaybackAuth,
        request: ViewerRequest,
    ) -> Result<ReplicaAdmissionOutcome, ReplicaFailure> {
        self.current(&choice)?;
        let ReplicaChoice { candidate, stamp } = choice;
        let fence = CoordinatedCandidate {
            handle: candidate.handle.clone(),
            candidate: candidate.candidate.copy_for_worker(),
        };
        let outcome = self
            .owner
            .admit(candidate, auth, request)
            .await
            .map_err(ReplicaFailure::Admission)?;
        self.owner
            .publish(&fence, |_, _| {
                if matches!(&outcome, super::AdmissionOutcome::Allowed(_)) {
                    self.commit_route(&stamp)
                } else {
                    self.check_stamp(&stamp)
                }
            })
            .map_err(ReplicaFailure::Coordination)??;
        Ok(match outcome {
            super::AdmissionOutcome::Allowed(inner) => {
                ReplicaAdmissionOutcome::Allowed(Box::new(ReplicaAdmission { inner, stamp }))
            }
            super::AdmissionOutcome::Denied => ReplicaAdmissionOutcome::Denied,
            super::AdmissionOutcome::Redirect(url) => ReplicaAdmissionOutcome::Redirect(url),
        })
    }
    pub fn admitted_current(&self, admitted: &ReplicaAdmission) -> Result<(), ReplicaFailure> {
        self.owner
            .with_admitted_current(&admitted.inner, || self.check_stamp(&admitted.stamp))
            .map_err(ReplicaFailure::Admission)?
    }
    /// Synchronous non-reentrant handoff. The route lock stays held through commit.
    /// Guard exported clients with admitted_current before and during private I/O.
    pub fn publish_admitted<T>(
        &self,
        admitted: &ReplicaAdmission,
        commit: impl FnOnce(CandidateView<'_>, MediaInput, &Grant) -> T,
    ) -> Result<T, ReplicaFailure> {
        self.owner
            .at_admitted(&admitted.inner, |view, input, grant| {
                Ok(self.at_stamp(&admitted.stamp, || {
                    grant.playback();
                    commit(view, input, grant)
                }))
            })
            .map_err(ReplicaFailure::Admission)?
    }
}
impl ReplicaChoice {
    pub fn source_key(&self) -> &str {
        &self.stamp.selector.members[self.stamp.selected].source_key
    }
}
impl ReplicaAdmission {
    /// Route/native cancellation only; admitted_current checks source and lease.
    pub fn is_cancelled(&self) -> bool {
        self.stamp.route.cancel.is_cancelled() || self.inner.is_cancelled()
    }
    pub async fn cancelled(&self) {
        tokio::select! {_=self.stamp.route.cancel.cancelled()=>{},_=self.inner.cancelled()=>{}}
    }
}
impl Drop for ReplicaLookup {
    fn drop(&mut self) {
        self.selector
            .release_provisional(&self.name, &self.identity);
    }
}
impl Drop for RouteStamp {
    fn drop(&mut self) {
        self.selector
            .release_provisional(&self.name, &self.identity);
    }
}

/// Managed workers retain group generation independently of renewed source tickets.
#[derive(Clone)]
pub(crate) struct ReplicaRoute {
    stamp: RouteStamp,
}
impl ReplicaRoute {
    pub(crate) fn commit(&self) -> Result<(), ReplicaFailure> {
        self.stamp.selector.commit_route(&self.stamp)
    }
    pub(crate) fn commit_if(
        &self,
        selector: &ReplicaSelector,
        accept: impl FnOnce() -> bool,
    ) -> Result<bool, ReplicaFailure> {
        selector.commit_route_if(&self.stamp, accept)
    }
    pub(crate) fn at_current<T>(&self, commit: impl FnOnce() -> T) -> Result<T, ReplicaFailure> {
        self.stamp.selector.at_stamp(&self.stamp, commit)
    }
    pub(crate) fn is_cancelled(&self) -> bool {
        self.stamp.route.cancel.is_cancelled()
    }
}
impl ReplicaAdmission {
    pub(crate) fn into_native(self) -> Box<super::AdmittedCandidate> {
        let Self { mut inner, stamp } = self;
        inner.selection = Some(ReplicaRoute { stamp });
        inner
    }
}
