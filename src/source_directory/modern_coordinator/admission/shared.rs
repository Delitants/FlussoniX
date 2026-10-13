//! One private input per generation; viewer grants remain independently owned.
use super::{AdmissionFailure, AdmittedCandidate, ModernSourceCoordinator};
use crate::{
    m4_ingest::flussonic::MediaInput, m4s::Track, media_queue, playback_auth::Policy, wire::Hub,
};
use bytes::Bytes;
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
    workers: Mutex<HashMap<String, Weak<Worker>>>,
}
struct Worker {
    id: Uuid,
    registry: Arc<()>,
    owner: Arc<ModernSourceCoordinator>,
    name: String,
    policy: Policy,
    options: SharedInputOptions,
    members: Mutex<HashMap<Uuid, Weak<Viewer>>>,
    changed: Notify,
    cancel: CancellationToken,
    hub: Hub,
    ts: media_queue::Channel,
    tracks: Mutex<Option<Vec<Track>>>,
    detected: AtomicU64,
}
struct Viewer {
    id: Uuid,
    worker: Arc<Worker>,
    admission: Arc<AdmittedCandidate>,
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
            if !viewer.admission.grant.is_cancelled() {
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
        let first = self.active_viewer().ok_or(SharedFailure::Closed)?;
        self.owner
            .at_current_candidate(&first.admission.candidate, || {
                if self.active_viewer().is_some() {
                    Ok(())
                } else {
                    Err(AdmissionFailure::Revoked)
                }
            })
            .map_err(SharedFailure::Authority)?;
        if self.cancel.is_cancelled() {
            return Err(SharedFailure::Closed);
        }
        Ok(())
    }
    fn compatible(
        &self,
        admitted: &AdmittedCandidate,
        policy: &Policy,
        options: &SharedInputOptions,
    ) -> bool {
        if &self.policy != policy || !self.options.matches(options) {
            return false;
        }
        self.active_viewer().is_some_and(|v| {
            let a = &v.admission.candidate;
            let b = &admitted.candidate;
            Arc::ptr_eq(&a.handle.authority, &b.handle.authority)
                && a.handle.stamp.revision == b.handle.stamp.revision
                && Arc::ptr_eq(&a.handle.stamp.identity, &b.handle.stamp.identity)
                && a.candidate.shares_lease(&b.candidate)
                && v.admission.name == admitted.name
        })
    }
    fn attach(
        self: &Arc<Self>,
        admission: Arc<AdmittedCandidate>,
    ) -> Result<SharedViewer, SharedFailure> {
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
        inner.admission.grant.playback();
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
    pub fn new(owner: Arc<ModernSourceCoordinator>) -> Self {
        Self {
            owner,
            id: Arc::new(()),
            workers: Mutex::new(HashMap::new()),
        }
    }
    /// Consume an independently authorized viewer. The first attachment owns
    /// startup; later attachments share only a current compatible generation.
    pub fn attach(
        &self,
        admission: Box<AdmittedCandidate>,
        options: SharedInputOptions,
    ) -> Result<SharedInputAttachment, SharedFailure> {
        let admission: Arc<AdmittedCandidate> = admission.into();
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| SharedFailure::Unavailable)?;
        workers.retain(|_, w| w.upgrade().is_some_and(|w| !w.cancel.is_cancelled()));
        let old = workers.get(&admission.name).and_then(Weak::upgrade);
        // Check outside the incoming publication fence to avoid lock reentry.
        let reuse = old.as_ref().filter(|w| w.current().is_ok());
        self.owner
            .at_admission_candidate(&admission.candidate, &admission.name, |view, input| {
                if admission.grant.is_cancelled() {
                    return Err(AdmissionFailure::Revoked);
                }
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
                        name: view.name.into(),
                        policy: view.policy.clone(),
                        options,
                        members: Mutex::new(HashMap::new()),
                        changed: Notify::new(),
                        cancel: CancellationToken::new(),
                        hub: Hub::new(),
                        ts: media_queue::Channel::new(128, 8 * 1024 * 1024),
                        tracks: Mutex::new(None),
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
            })
            .map_err(SharedFailure::Authority)?
    }
    /// Retire only the exact currently registered generation named by this handle.
    pub fn retire(&self, viewer: &SharedViewer) -> bool {
        let worker = &viewer.inner.worker;
        if !Arc::ptr_eq(&self.id, &worker.registry) {
            return false;
        }
        let mut workers = self.workers.lock().unwrap();
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
        let mut workers = self.workers.lock().unwrap();
        for worker in workers.values().filter_map(Weak::upgrade) {
            worker.close();
        }
        workers.clear();
    }
}
impl Drop for SharedInputs {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl SharedViewer {
    pub fn generation(&self) -> Uuid {
        self.inner.worker.id
    }
    pub fn current(&self) -> Result<(), SharedFailure> {
        let worker = &self.inner.worker;
        if worker.cancel.is_cancelled() {
            return Err(SharedFailure::Closed);
        }
        worker
            .owner
            .admitted_current(&self.inner.admission)
            .map_err(SharedFailure::Authority)?;
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
            self.inner.admission.grant.add_bytes(bytes.len());
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
struct QueueWriter<'a>(&'a media_queue::Channel);
impl AsyncWrite for QueueWriter<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = bytes.len().min(64 * 1024);
        if n == 0 {
            return Poll::Ready(Ok(0));
        }
        Poll::Ready(
            self.0
                .send(Bytes::copy_from_slice(&bytes[..n]))
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
        let mut output = QueueWriter(&worker.ts);
        let (tx, mut metadata) = oneshot::channel();
        let pull = self.input.pull_guarded(
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
                        *worker.tracks.lock().unwrap() = Some(tracks);
                    }
                },
                result = &mut pull => {
                    worker.current()?;
                    return result.map_err(|_| SharedFailure::Input);
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
        self.viewer.inner.admission.grant.add_bytes(bytes.len());
        Ok(bytes)
    }
}
