//! Opt-in packaging of an already admitted shared private input.
mod sessions;
use super::*;
use crate::source_directory::modern_coordinator::{
    SharedFailure, SharedFeed, SharedInput, SharedInputAttachment, SharedViewer,
};
pub use sessions::{HlsSessionFailure, SharedHlsOptions, SharedHlsSessions};
use std::collections::VecDeque;

#[derive(Default)]
pub(super) struct StartMode {
    pub publishing: Option<PublicationInput>,
    pub last_playback: Option<Instant>,
    pub shared: Option<SharedStart>,
}
pub(super) struct SharedStart {
    pub feed: SharedFeed,
    pub input: Option<SharedInput>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedPackagingFailure {
    Authority(SharedFailure),
    Invalid,
    Mismatch,
    Closed,
    Lagged,
    Unavailable,
}
struct Viewer {
    name: String,
    source: SharedViewer,
    worker: Arc<Worker>,
}
impl Drop for Viewer {
    fn drop(&mut self) {
        self.worker.viewers.fetch_sub(1, Ordering::Relaxed);
        self.worker.touch();
    }
}
#[derive(Clone)]
pub struct PackagedPlayback {
    inner: Arc<Viewer>,
}
pub struct PackagedSubscription {
    playback: PackagedPlayback,
    bootstrap: VecDeque<Bytes>,
    receiver: Receiver,
}
enum Receiver {
    Ts(broadcast::Receiver<Bytes>),
    Native(crate::media_queue::Receiver),
}
impl Engine {
    /// Native supervision may retire shared generations but never restart them
    /// through privileged native input configuration.
    pub(crate) async fn maintain_shared(&self, name: &str) -> bool {
        let worker = self
            .workers
            .lock()
            .await
            .get(name)
            .filter(|w| w.shared_generation.is_some())
            .cloned();
        let Some(worker) = worker else {
            return false;
        };
        if worker.is_closed()
            || !worker.alive.load(Ordering::Relaxed)
            || worker
                .shared_authority
                .as_ref()
                .is_none_or(|current| !current())
        {
            self.stop_if_current(name, &worker).await;
        }
        true
    }
    /// Package one already admitted shared generation. Settings describe local
    /// outputs only; no source URL, credentials or input override is accepted.
    /// The Engine owns and joins the unique source run future with this worker.
    pub async fn package_shared(
        &self,
        attachment: SharedInputAttachment,
        settings: &Value,
    ) -> Result<PackagedPlayback, SharedPackagingFailure> {
        let object = settings
            .as_object()
            .ok_or(SharedPackagingFailure::Invalid)?;
        if object.keys().any(|k| {
            !matches!(
                k.as_str(),
                "transcoder"
                    | "flussonix_input_timeout"
                    | "flussonix_subtitle_tracks"
                    | "flussonix_hls_subtitles"
                    | "flussonix_hls_captions"
            )
        }) {
            return Err(SharedPackagingFailure::Invalid);
        }
        if object
            .get("flussonix_input_timeout")
            .is_some_and(|v| !v.as_u64().is_some_and(|n| (1..=300).contains(&n)))
        {
            return Err(SharedPackagingFailure::Invalid);
        }
        let feed = attachment
            .viewer
            .feed(attachment.input.is_some())
            .map_err(SharedPackagingFailure::Authority)?;
        feed.validate(settings)
            .map_err(|_| SharedPackagingFailure::Invalid)?;
        let name = feed.name().to_owned();
        let mut cfg = settings.clone();
        cfg["inputs"] = json!([{"url":format!("{}://shared.invalid",feed.protocol())}]);
        let current = feed.authority();
        let worker = self
            .ensure_mode(
                &name,
                &cfg,
                true,
                async { current() },
                StartMode {
                    shared: Some(SharedStart {
                        feed,
                        input: attachment.input,
                    }),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                if e == "shared packaging mismatch" {
                    SharedPackagingFailure::Mismatch
                } else {
                    SharedPackagingFailure::Unavailable
                }
            })?;
        // A viewer revoked while startup waited must not close the shared worker
        // if another attachment still has valid demand. The run owner is separate.
        attachment
            .viewer
            .current()
            .map_err(SharedPackagingFailure::Authority)?;
        if worker.is_closed() {
            return Err(SharedPackagingFailure::Closed);
        }
        worker.viewers.fetch_add(1, Ordering::Relaxed);
        Ok(PackagedPlayback {
            inner: Arc::new(Viewer {
                name,
                source: attachment.viewer,
                worker,
            }),
        })
    }
    /// Recheck the individual viewer and exact installed output generation after
    /// asynchronous HLS/file reads. Cached data carries no viewer permission.
    pub async fn read_shared(
        &self,
        playback: &PackagedPlayback,
        file: &str,
    ) -> Result<Bytes, SharedPackagingFailure> {
        playback.current()?;
        if !self.is_shared_current(playback).await {
            return Err(SharedPackagingFailure::Closed);
        }
        let bytes = self
            .read(&playback.inner.name, file)
            .await
            .map_err(|_| SharedPackagingFailure::Unavailable)?;
        playback.current()?;
        if !self.is_shared_current(playback).await {
            return Err(SharedPackagingFailure::Closed);
        }
        playback.current()?;
        playback.inner.source.add_bytes(bytes.len());
        Ok(bytes)
    }
    async fn is_shared_current(&self, playback: &PackagedPlayback) -> bool {
        self.workers
            .lock()
            .await
            .get(&playback.inner.name)
            .is_some_and(|w| Arc::ptr_eq(w, &playback.inner.worker))
    }
}
impl PackagedPlayback {
    pub(crate) fn with_current_source<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, SharedPackagingFailure> {
        self.inner
            .source
            .with_current(|| {
                if self.inner.worker.is_closed() || !self.inner.worker.alive.load(Ordering::Relaxed)
                {
                    return Err(SharedPackagingFailure::Closed);
                }
                Ok(commit())
            })
            .map_err(SharedPackagingFailure::Authority)?
    }
    pub fn generation(&self) -> uuid::Uuid {
        self.inner.source.generation()
    }
    pub fn pid(&self) -> u32 {
        self.inner.worker.pid()
    }
    pub fn current(&self) -> Result<(), SharedPackagingFailure> {
        self.inner
            .source
            .current()
            .map_err(SharedPackagingFailure::Authority)?;
        if self.inner.worker.is_closed() || !self.inner.worker.alive.load(Ordering::Relaxed) {
            return Err(SharedPackagingFailure::Closed);
        }
        Ok(())
    }
    pub fn stats(&self) -> Value {
        self.inner.worker.stats()
    }
    fn subscription(
        &self,
        bootstrap: Vec<Bytes>,
        receiver: Receiver,
    ) -> Result<PackagedSubscription, SharedPackagingFailure> {
        self.current()?;
        self.inner.worker.touch();
        Ok(PackagedSubscription {
            playback: self.clone(),
            bootstrap: bootstrap.into(),
            receiver,
        })
    }
    pub fn subscribe_ts(&self) -> Result<PackagedSubscription, SharedPackagingFailure> {
        self.current()?;
        self.subscription(vec![], Receiver::Ts(self.inner.worker.subscribe()))
    }
    pub fn subscribe_m4s(&self) -> Result<PackagedSubscription, SharedPackagingFailure> {
        self.current()?;
        let (bootstrap, receiver) = self.inner.worker.wire.m4s_subscribe();
        self.subscription(bootstrap, Receiver::Native(receiver))
    }
    pub fn subscribe_m4f(&self) -> Result<PackagedSubscription, SharedPackagingFailure> {
        self.current()?;
        let (bootstrap, receiver) = self.inner.worker.wire.signal_subscribe();
        self.subscription(bootstrap, Receiver::Native(receiver))
    }
}
impl PackagedSubscription {
    pub async fn recv(&mut self) -> Result<Bytes, SharedPackagingFailure> {
        self.playback.current()?;
        let bytes = if let Some(bytes) = self.bootstrap.pop_front() {
            bytes
        } else {
            let receive = async {
                match &mut self.receiver {
                    Receiver::Ts(receiver) => receiver.recv().await,
                    Receiver::Native(receiver) => receiver.recv().await,
                }
            };
            tokio::select! {biased;
                _=self.playback.inner.source.closed()=>return Err(SharedPackagingFailure::Closed),
                _=self.playback.inner.worker.closed()=>return Err(SharedPackagingFailure::Closed),
                result=receive=>result.map_err(|e|match e {
                    broadcast::error::RecvError::Lagged(_)=>SharedPackagingFailure::Lagged,
                    broadcast::error::RecvError::Closed=>SharedPackagingFailure::Closed,
                })?,
            }
        };
        self.playback.current()?;
        self.playback.inner.source.add_bytes(bytes.len());
        self.playback.inner.worker.touch();
        Ok(bytes)
    }
}
