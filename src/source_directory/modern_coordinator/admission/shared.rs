//! One private input per generation; viewer grants remain independently owned.
mod feed;
mod permission;
mod renewal;
use super::{AdmissionFailure, AdmittedCandidate, ModernSourceCoordinator};
use crate::{
    m4_ingest::flussonic::MediaInput, m4s::Track, media_queue, playback_auth::Policy, wire::Hub,
};
use bytes::Bytes;
pub(crate) use feed::SharedFeed;
use permission::Permission;
pub use renewal::SharedRenewal;
use std::{
    collections::{HashMap, VecDeque},
    io,
    pin::Pin,
    sync::{Arc, Mutex, Weak, atomic::AtomicU64},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::AsyncWrite,
    sync::{Notify, broadcast::error::RecvError, oneshot},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct SharedInputOptions {
    pub preserve_subtitles: bool,
    pub conversion: Option<Arc<crate::caption_hls::State>>,
}
impl SharedInputOptions {
    fn matches(&self, other: &Self) -> bool {
        self.preserve_subtitles == other.preserve_subtitles
            && match (&self.conversion, &other.conversion) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedFailure {
    Authority(AdmissionFailure),
    Unavailable,
    Closed,
    Mismatch,
    Limit,
    Lagged,
    Input,
}
/// Keep one registry per embedding runtime; separate registries do not deduplicate.
pub struct SharedInputs {
    owner: Arc<ModernSourceCoordinator>,
    id: Arc<()>,
    failures: Mutex<HashMap<String, FailureSlot>>,
    workers: Mutex<Registry>,
}
struct Registry {
    workers: HashMap<String, Weak<Worker>>,
    epoch: Arc<()>,
}
struct Worker {
    id: Uuid,
    registry: Arc<()>,
    owner: Arc<ModernSourceCoordinator>,
    handle: super::super::SourceHandle,
    route: Mutex<super::super::CoordinatedCandidate>,
    scope: Option<super::super::ReplicaRoute>,
    failure: Option<FailureSlot>,
    name: String,
    policy: Policy,
    options: SharedInputOptions,
    members: Mutex<HashMap<Uuid, Weak<Viewer>>>,
    changed: Notify,
    cancel: CancellationToken,
    hub: Arc<Hub>,
    transport: crate::m4_ingest::flussonic::Transport,
    ts: media_queue::Channel,
    required: Mutex<Option<tokio::sync::mpsc::Sender<Bytes>>>,
    tracks: Mutex<Option<Vec<Track>>>,
    metadata_ready: Notify,
    detected: AtomicU64,
}
struct Viewer {
    id: Uuid,
    worker: Arc<Worker>,
    admission: Permission,
}
#[derive(Clone)]
pub struct SharedViewer {
    inner: Arc<Viewer>,
}
/// Unique, caller-owned startup/run handle. Drop closes only this generation.
pub struct SharedInput {
    worker: Arc<Worker>,
    input: MediaInput,
}
pub struct SharedSubscription {
    viewer: SharedViewer,
    bootstrap: VecDeque<Bytes>,
    receiver: media_queue::Receiver,
}
pub struct SharedInputAttachment {
    pub viewer: SharedViewer,
    pub input: Option<SharedInput>,
}
impl Worker {
    fn input_failed(
        &self,
        kind: crate::m4_ingest::flussonic::InputFailure,
        before_metadata: bool,
    ) -> Result<(), SharedFailure> {
        self.at_source_route(|_| {
            if self.cancel.is_cancelled() || self.active_viewer().is_none() {
                return Err(AdmissionFailure::Revoked);
            }
            // Packaging supervision can expire before the private client's timer.
            // Once metadata exists, a blocked local feed/packager is not a remote stall.
            if before_metadata && self.tracks.lock().unwrap().is_some() {
                return Ok(());
            }
            if let Some(scope) = &self.scope {
                let notice = scope.input_failed(&self.handle, kind)?;
                if let Some(slot) = &self.failure {
                    *slot.lock().unwrap() = Some(FailureObservation {
                        generation: self.id,
                        notice,
                    });
                }
            }
            Ok(())
        })
    }
    fn close(&self) {
        self.cancel.cancel();
        self.ts.close();
        self.hub.finish();
    }
    fn active_viewer(&self) -> Option<Arc<Viewer>> {
        // Common case is O(1): stop at the first authorized attachment. Defer
        // dropping upgraded handles until after members is unlocked, since a
        // concurrent final external drop can make this the destructor's owner.
        let members = self.members.lock().unwrap();
        let mut retired = Vec::new();
        let mut active = None;
        for viewer in members.values().filter_map(Weak::upgrade) {
            if !viewer.admission.is_cancelled() {
                active = Some(viewer);
                break;
            }
            retired.push(viewer);
        }
        drop(members);
        drop(retired);
        active
    }
    fn current(&self) -> Result<(), SharedFailure> {
        if self.cancel.is_cancelled() {
            return Err(SharedFailure::Closed);
        }
        if self.active_viewer().is_none() {
            return Err(SharedFailure::Closed);
        }
        self.at_route(|_| {
            if self.active_viewer().is_some() {
                Ok(())
            } else {
                Err(AdmissionFailure::Revoked)
            }
        })?;
        if self.cancel.is_cancelled() {
            return Err(SharedFailure::Closed);
        }
        Ok(())
    }
    fn compatible(
        &self,
        admitted: &Permission,
        policy: &Policy,
        options: &SharedInputOptions,
    ) -> bool {
        if &self.policy != policy || !self.options.matches(options) {
            return false;
        }
        let a = self.route.lock().unwrap();
        let b = admitted.candidate();
        same_source(&a.handle, &b.handle)
            && a.candidate.shares_lease(&b.candidate)
            && self.name == admitted.name()
    }
    fn at_route<T>(
        &self,
        check: impl FnOnce(&super::super::CoordinatedCandidate) -> Result<T, AdmissionFailure>,
    ) -> Result<T, SharedFailure> {
        self.at_source_route(|route| match &self.scope {
            Some(scope) => scope
                .at_current(|| check(route))
                .unwrap_or(Err(AdmissionFailure::Revoked)),
            None => check(route),
        })
    }
    fn at_source_route<T>(
        &self,
        check: impl FnOnce(&super::super::CoordinatedCandidate) -> Result<T, AdmissionFailure>,
    ) -> Result<T, SharedFailure> {
        self.owner
            .at_handle(&self.handle, || {
                let guard = self
                    .handle
                    .authority
                    .guard_modern_state()
                    .map_err(super::super::CoordinationFailure::Lease)?;
                let route = self.route.lock().unwrap();
                guard
                    .check(&route.candidate)
                    .map_err(super::super::CoordinationFailure::Lease)?;
                let result = check(&route);
                guard
                    .check(&route.candidate)
                    .map_err(super::super::CoordinationFailure::Lease)?;
                Ok(result)
            })
            .map_err(|e| SharedFailure::Authority(AdmissionFailure::Coordination(e)))?
            .map_err(SharedFailure::Authority)
    }

    fn attach(self: &Arc<Self>, admission: Permission) -> Result<SharedViewer, SharedFailure> {
        let mut members = self.members.lock().unwrap();
        members.retain(|_, v| v.strong_count() > 0);
        if members.len() >= 20_000 {
            return Err(SharedFailure::Limit);
        }
        let inner = Arc::new(Viewer {
            id: Uuid::new_v4(),
            worker: self.clone(),
            admission,
        });
        members.insert(inner.id, Arc::downgrade(&inner));
        drop(members);
        inner.admission.playback();
        self.changed.notify_one();
        Ok(SharedViewer { inner })
    }
}
impl Drop for Viewer {
    fn drop(&mut self) {
        self.worker.members.lock().unwrap().remove(&self.id);
        self.worker.changed.notify_one();
    }
}
impl SharedInputs {
    /// Aggregate authorized registry demand, without retaining viewer grants.
    pub(crate) fn renewal_sources(
        &self,
    ) -> Result<Vec<(usize, super::super::SourceHandle, std::time::Instant)>, SharedFailure> {
        let registry = self
            .workers
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        Ok(registry
            .workers
            .values()
            .filter_map(Weak::upgrade)
            .filter_map(|worker| {
                worker.current().ok()?;
                worker
                    .at_route(|route| {
                        if worker.cancel.is_cancelled() || worker.active_viewer().is_none() {
                            return Err(AdmissionFailure::Revoked);
                        }
                        Ok((
                            route.handle.generation_key(),
                            route.handle.clone(),
                            route.expires_at(),
                        ))
                    })
                    .ok()
            })
            .collect())
    }
    /// Current managed metadata for fresh native admission; no media permission.
    pub fn current_candidate(
        &self,
        name: &str,
    ) -> Result<super::super::CoordinatedCandidate, SharedFailure> {
        let registry = self
            .workers
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        let worker = registry
            .workers
            .get(name)
            .and_then(Weak::upgrade)
            .ok_or(SharedFailure::Unavailable)?;
        worker.current()?;
        worker.at_route(|route| {
            if worker.cancel.is_cancelled() || worker.active_viewer().is_none() {
                return Err(AdmissionFailure::Revoked);
            }
            Ok(copy_route(route))
        })
    }
    pub(crate) fn hls_session_identity(
        &self,
        admission: &AdmittedCandidate,
    ) -> Result<String, SharedFailure> {
        if admission.proto != "hls" {
            return Err(SharedFailure::Authority(AdmissionFailure::Invalid));
        }
        self.owner
            .admitted_current(admission)
            .map_err(SharedFailure::Authority)?;
        admission
            .grant
            .session_identity()
            .map(str::to_owned)
            .ok_or(SharedFailure::Authority(AdmissionFailure::Invalid))
    }
    pub(crate) fn source_in_use(&self, handle: &super::super::SourceHandle) -> bool {
        self.workers
            .lock()
            .unwrap()
            .workers
            .values()
            .filter_map(Weak::upgrade)
            .any(|w| same_source(&w.handle, handle) && w.current().is_ok())
    }
    pub(crate) fn install_idle(
        &self,
        prepared: super::super::PreparedSource,
    ) -> Result<super::super::SourceHandle, SharedFailure> {
        let registry = self.workers.lock().unwrap();
        if registry
            .workers
            .values()
            .filter_map(Weak::upgrade)
            .any(|w| w.handle.stamp.key == prepared.stamp.key && w.current().is_ok())
        {
            return Err(SharedFailure::Unavailable);
        }
        self.owner
            .install(prepared)
            .map_err(|e| SharedFailure::Authority(AdmissionFailure::Coordination(e)))
    }
    pub fn new(owner: Arc<ModernSourceCoordinator>) -> Self {
        Self {
            owner,
            id: Arc::new(()),
            failures: Mutex::new(HashMap::new()),
            workers: Mutex::new(Registry {
                workers: HashMap::new(),
                epoch: Arc::new(()),
            }),
        }
    }
    /// Consume opaque replica admission with its selected-route fence intact.
    pub fn attach_replica(
        &self,
        admission: super::super::ReplicaAdmission,
        options: SharedInputOptions,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        self.attach(admission.into_native(), options)
    }
    /// Attach locally selected static input ownership without a viewer session.
    /// Every network viewer must still enter through independent native admission.
    pub fn attach_activation(
        &self,
        activation: super::ActivatedCandidate,
        options: SharedInputOptions,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        self.attach_permission(Permission::Activation(Arc::new(activation)), |_| {
            Ok(options)
        })
    }
    /// Consume an independently authorized viewer. The first attachment owns
    /// startup; later attachments share only a current compatible generation.
    pub fn attach(
        &self,
        admission: Box<AdmittedCandidate>,
        options: SharedInputOptions,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        self.attach_with(admission, |_| Ok(options))
    }
    /// Resolve local processing options inside the registry's attachment fence.
    /// Mutable conversion state belongs to exactly one shared input generation.
    pub(crate) fn attach_configured(
        &self,
        admission: Box<AdmittedCandidate>,
        settings: &serde_json::Value,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        self.attach_with(admission, |reuse| {
            crate::vendor_output::validate(settings).map_err(|_| SharedFailure::Input)?;
            let preserve = crate::config::subtitle_tracks(settings)
                .map_err(|_| SharedFailure::Input)?
                == "preserve";
            let services =
                crate::captions::configuration(settings).map_err(|_| SharedFailure::Input)?;
            let native = services.iter().any(|s| s.native_track().is_some());
            if let Some(worker) = reuse {
                if worker.options.preserve_subtitles != preserve {
                    return Err(SharedFailure::Mismatch);
                }
                let matches = match (&worker.options.conversion, native) {
                    (None, false) => true,
                    (Some(state), true) => {
                        let decoder = state
                            .decoder
                            .lock()
                            .map_err(|_| SharedFailure::Unavailable)?;
                        serde_json::to_value(&decoder.services).map_err(|_| SharedFailure::Input)?
                            == serde_json::to_value(&services).map_err(|_| SharedFailure::Input)?
                    }
                    _ => false,
                };
                return if matches {
                    Ok(worker.options.clone())
                } else {
                    Err(SharedFailure::Mismatch)
                };
            }
            Ok(SharedInputOptions {
                preserve_subtitles: preserve,
                conversion: native.then(|| {
                    Arc::new(crate::caption_hls::State::new(
                        crate::captions::Decoder::new(services),
                        format!("vendor-{}", Uuid::new_v4()),
                        0,
                    ))
                }),
            })
        })
    }
    fn attach_with(
        &self,
        admission: Box<AdmittedCandidate>,
        options: impl FnOnce(Option<&Worker>) -> Result<SharedInputOptions, SharedFailure>,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        self.attach_permission(Permission::Viewer(admission.into()), options)
    }
    fn attach_permission(
        &self,
        admission: Permission,
        options: impl FnOnce(Option<&Worker>) -> Result<SharedInputOptions, SharedFailure>,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        let mut registry = self
            .workers
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        let workers = &mut registry.workers;
        workers.retain(|_, w| w.upgrade().is_some_and(|w| !w.cancel.is_cancelled()));
        let old = workers.get(admission.name()).and_then(Weak::upgrade);
        // Check outside the incoming publication fence to avoid lock reentry.
        let reuse = old.as_ref().filter(|w| w.current().is_ok());
        self.owner
            .at_admission_candidate(admission.candidate(), admission.name(), |view, input| {
                admission.at_scope(|| {
                    if admission.is_cancelled()
                        || (!admission.counts_as_viewer() && !view.selected_static)
                    {
                        return Err(AdmissionFailure::Revoked);
                    }
                    let options = match options(reuse.map(Arc::as_ref)) {
                        Ok(options) => options,
                        Err(error) => return Ok(Err(error)),
                    };
                    let result = if let Some(worker) = reuse {
                        if !worker.compatible(&admission, view.policy, &options) {
                            Err(SharedFailure::Mismatch)
                        } else {
                            worker
                                .attach(admission.clone())
                                .map(|viewer| SharedInputAttachment {
                                    viewer,
                                    input: None,
                                })
                        }
                    } else if old.is_none() && workers.len() >= 256 {
                        Err(SharedFailure::Limit)
                    } else {
                        if let Some(old) = &old {
                            old.close();
                        }
                        let worker = Arc::new(Worker {
                            id: Uuid::new_v4(),
                            registry: self.id.clone(),
                            owner: self.owner.clone(),
                            handle: admission.candidate().handle.clone(),
                            route: Mutex::new(copy_route(admission.candidate())),
                            scope: admission.selection(),
                            failure: self.failures.lock().unwrap().get(view.name).cloned(),
                            name: view.name.into(),
                            policy: view.policy.clone(),
                            options,
                            members: Mutex::new(HashMap::new()),
                            changed: Notify::new(),
                            cancel: CancellationToken::new(),
                            hub: Arc::new(Hub::new()),
                            transport: input.transport(),
                            ts: media_queue::Channel::new(128, 8 * 1024 * 1024),
                            required: Mutex::new(None),
                            tracks: Mutex::new(None),
                            metadata_ready: Notify::new(),
                            detected: AtomicU64::new(0),
                        });
                        let viewer = worker.attach(admission.clone());
                        workers.insert(worker.name.clone(), Arc::downgrade(&worker));
                        viewer.map(|viewer| SharedInputAttachment {
                            viewer,
                            input: Some(SharedInput { worker, input }),
                        })
                    };
                    Ok(result)
                })?
            })
            .map_err(SharedFailure::Authority)?
    }
    /// Retire only the exact currently registered generation named by this handle.
    pub fn retire(&self, viewer: &SharedViewer) -> bool {
        let worker = &viewer.inner.worker;
        if !Arc::ptr_eq(&self.id, &worker.registry) {
            return false;
        }
        let mut registry = self.workers.lock().unwrap();
        let workers = &mut registry.workers;
        if !workers
            .get(&worker.name)
            .and_then(Weak::upgrade)
            .is_some_and(|w| Arc::ptr_eq(&w, worker))
        {
            return false;
        }
        workers.remove(&worker.name);
        worker.close();
        true
    }
    /// Cancel registered generations. The caller still owns and joins run futures.
    /// Subsequent explicit attachment may start a new generation.
    pub fn shutdown(&self) {
        let mut registry = self.workers.lock().unwrap();
        let workers = &mut registry.workers;
        for worker in workers.values().filter_map(Weak::upgrade) {
            worker.close();
        }
        workers.clear();
        registry.epoch = Arc::new(());
        self.failures.lock().unwrap().clear();
    }
}
impl Drop for SharedInputs {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl SharedViewer {
    pub(crate) fn counts_as_viewer(&self) -> bool {
        self.inner.admission.counts_as_viewer()
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.worker.cancel.is_cancelled() || self.inner.admission.is_cancelled()
    }
    pub(crate) fn with_current<T>(&self, commit: impl FnOnce() -> T) -> Result<T, SharedFailure> {
        let worker = &self.inner.worker;
        worker.at_route(|_| {
            if worker.cancel.is_cancelled() || self.inner.admission.is_cancelled() {
                return Err(AdmissionFailure::Revoked);
            }
            Ok(commit())
        })
    }
    pub fn generation(&self) -> Uuid {
        self.inner.worker.id
    }
    pub fn current(&self) -> Result<(), SharedFailure> {
        let worker = &self.inner.worker;
        if worker.cancel.is_cancelled() {
            return Err(SharedFailure::Closed);
        }
        worker.at_route(|_| {
            if self.inner.admission.is_cancelled() {
                Err(AdmissionFailure::Revoked)
            } else {
                Ok(())
            }
        })?;
        if worker.cancel.is_cancelled() {
            return Err(SharedFailure::Closed);
        }
        Ok(())
    }
    fn subscription(
        &self,
        bootstrap: Vec<Bytes>,
        receiver: media_queue::Receiver,
    ) -> Result<SharedSubscription, SharedFailure> {
        self.current()?;
        Ok(SharedSubscription {
            viewer: self.clone(),
            bootstrap: bootstrap.into(),
            receiver,
        })
    }
    pub fn subscribe_ts(&self) -> Result<SharedSubscription, SharedFailure> {
        self.current()?;
        self.subscription(vec![], self.inner.worker.ts.subscribe())
    }
    pub fn subscribe_m4s(&self) -> Result<SharedSubscription, SharedFailure> {
        self.current()?;
        let (bootstrap, receiver) = self.inner.worker.hub.m4s_subscribe();
        self.subscription(bootstrap, receiver)
    }
    pub fn subscribe_m4f(&self) -> Result<SharedSubscription, SharedFailure> {
        self.current()?;
        let (bootstrap, receiver) = self.inner.worker.hub.signal_subscribe();
        self.subscription(bootstrap, receiver)
    }
    pub fn segment(&self, name: &str) -> Result<Option<Bytes>, SharedFailure> {
        self.current()?;
        let bytes = self.inner.worker.hub.segment(name);
        self.current()?;
        if let Some(bytes) = &bytes {
            self.add_bytes(bytes.len());
        }
        Ok(bytes)
    }
    pub fn tracks(&self) -> Result<Option<Vec<Track>>, SharedFailure> {
        self.current()?;
        let tracks = self.inner.worker.tracks.lock().unwrap().clone();
        self.current()?;
        Ok(tracks)
    }
}
struct QueueWriter<'a> {
    channel: &'a media_queue::Channel,
    required: Option<tokio_util::sync::PollSender<Bytes>>,
}
impl AsyncWrite for QueueWriter<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let n = bytes.len().min(64 * 1024);
        if n == 0 {
            return Poll::Ready(Ok(0));
        }
        if let Some(required) = &mut this.required {
            match required.poll_reserve(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
                Poll::Ready(Ok(())) => {}
            }
        }
        let bytes = Bytes::copy_from_slice(&bytes[..n]);
        if let Some(required) = &mut this.required {
            if required.send_item(bytes.clone()).is_err() {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
        }
        Poll::Ready(
            this.channel
                .send(bytes)
                .map(|_| n)
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe)),
        )
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl SharedInput {
    /// Run this unique input in the caller's task. Drop/abort closes the generation.
    pub async fn run(self) -> Result<(), SharedFailure> {
        let worker = &self.worker;
        worker.current()?;
        let authority = || worker.current().is_ok();
        let required = worker
            .required
            .lock()
            .unwrap()
            .clone()
            .map(tokio_util::sync::PollSender::new);
        let mut output = QueueWriter {
            channel: &worker.ts,
            required,
        };
        let (tx, mut metadata) = oneshot::channel();
        let pull = self.input.pull_guarded_classified(
            &mut output,
            Some(&worker.hub),
            tx,
            crate::m4_ingest::PullOptions {
                preserve: worker.options.preserve_subtitles,
                detected: Some(&worker.detected),
                conversion: worker.options.conversion.as_deref(),
                ..Default::default()
            },
            &authority,
        );
        tokio::pin!(pull);
        let mut metadata_pending = true;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased;
                _=worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
                _=worker.changed.notified()=>worker.current()?,
                _=tick.tick()=>worker.current()?,
                tracks = &mut metadata, if metadata_pending => {
                    metadata_pending = false;
                    if let Ok(tracks) = tracks {
                        worker.current()?;
                        worker.at_source_route(|_| {
                            let commit=|| {
                                if worker.active_viewer().is_none(){return Err(AdmissionFailure::Revoked);}
                                *worker.tracks.lock().unwrap()=Some(tracks);
                                worker.metadata_ready.notify_waiters();Ok(())
                            };
                            match &worker.scope {Some(scope)=>scope.accept_private(commit),None=>commit()}
                        })?;
                    }
                },
                result = &mut pull => {
                    worker.current()?;
                    if let Err(kind)=result {
                        worker.input_failed(kind,false)?;
                        return Err(SharedFailure::Input);
                    }
                    return Ok(());
                }
            }
        }
    }
}
impl Drop for SharedInput {
    fn drop(&mut self) {
        self.worker.close();
    }
}
impl SharedSubscription {
    pub async fn recv(&mut self) -> Result<Bytes, SharedFailure> {
        self.viewer.current()?;
        let bytes = if let Some(bytes) = self.bootstrap.pop_front() {
            bytes
        } else {
            tokio::select! {biased;
                _=self.viewer.inner.admission.cancelled()=>return Err(SharedFailure::Authority(AdmissionFailure::Revoked)),
                _=self.viewer.inner.worker.cancel.cancelled()=>return Err(SharedFailure::Closed),
                result = self.receiver.recv() => result.map_err(|e| match e {
                    RecvError::Lagged(_) => SharedFailure::Lagged,
                    RecvError::Closed => SharedFailure::Closed,
                })?,
            }
        };
        self.viewer.current()?;
        self.viewer.add_bytes(bytes.len());
        Ok(bytes)
    }
}

fn same_source(a: &super::super::SourceHandle, b: &super::super::SourceHandle) -> bool {
    Arc::ptr_eq(&a.authority, &b.authority)
        && a.stamp.revision == b.stamp.revision
        && Arc::ptr_eq(&a.stamp.identity, &b.stamp.identity)
        && Arc::ptr_eq(&a.stamp.owner, &b.stamp.owner)
        && Arc::ptr_eq(&a.stamp.turn, &b.stamp.turn)
        && a.stamp.key == b.stamp.key
}
fn copy_route(a: &super::super::CoordinatedCandidate) -> super::super::CoordinatedCandidate {
    super::super::CoordinatedCandidate {
        handle: a.handle.clone(),
        candidate: a.candidate.copy_for_worker(),
    }
}

pub(crate) struct FailureObservation {
    pub generation: Uuid,
    pub notice: super::super::ReplicaInputFailure,
}
// Reserve one mailbox with each recovery entry, rather than allocating on
// failure. A retired worker can write only its orphaned slot, never consume a
// new entry's capacity or replace its failure signal.
type FailureSlot = Arc<Mutex<Option<FailureObservation>>>;
impl SharedInputs {
    pub(crate) fn reserve_failure(&self, name: &str) -> Result<(), SharedFailure> {
        let mut slots = self
            .failures
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        if slots.len() >= 256 && !slots.contains_key(name) {
            return Err(SharedFailure::Limit);
        }
        slots
            .entry(name.into())
            .or_insert_with(|| Arc::new(Mutex::new(None)));
        Ok(())
    }
    pub(crate) fn retain_failures(&self, active: impl Fn(&str) -> bool) {
        self.failures.lock().unwrap().retain(|name, _| active(name));
    }
    pub(crate) fn take_failure(&self, name: &str) -> Option<FailureObservation> {
        let slot = self.failures.lock().ok()?.get(name)?.clone();
        slot.lock().ok()?.take()
    }
}
