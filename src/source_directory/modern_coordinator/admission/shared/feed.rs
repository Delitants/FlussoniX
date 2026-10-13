//! Aggregate worker feed: deliberately owns no individual viewer grant.
use super::*;
use tokio::io::AsyncWriteExt;
pub(crate) struct SharedFeed {
    worker: Arc<Worker>,
    receiver: Option<tokio::sync::mpsc::Receiver<Bytes>>,
}
impl SharedViewer {
    pub(crate) fn feed(&self, startup: bool) -> Result<SharedFeed, SharedFailure> {
        self.inner.worker.current()?;
        let worker = self.inner.worker.clone();
        let receiver = if startup {
            let mut slot = worker.required.lock().unwrap();
            if slot.is_some() {
                return Err(SharedFailure::Mismatch);
            }
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            *slot = Some(tx);
            Some(rx)
        } else {
            None
        };
        Ok(SharedFeed { worker, receiver })
    }
    pub(crate) async fn closed(&self) {
        tokio::select! { biased;
            _=self.inner.admission.cancelled()=>{},
            _=self.inner.worker.cancel.cancelled()=>{},
        }
    }
    pub(crate) fn add_bytes(&self, n: usize) {
        self.inner.admission.grant.add_bytes(n);
    }
}
impl SharedFeed {
    pub(crate) fn name(&self) -> &str {
        &self.worker.name
    }
    pub(crate) fn generation(&self) -> Uuid {
        self.worker.id
    }
    pub(crate) fn current(&self) -> bool {
        self.worker.current().is_ok()
    }
    pub(crate) fn authority(&self) -> impl Fn() -> bool + Send + Sync + 'static {
        let worker = self.worker.clone();
        move || worker.current().is_ok()
    }
    pub(crate) fn protocol(&self) -> &'static str {
        match self.worker.transport {
            crate::m4_ingest::flussonic::Transport::M4s => "m4s",
            crate::m4_ingest::flussonic::Transport::M4f => "m4f",
        }
    }
    pub(crate) fn hub(&self) -> Arc<Hub> {
        self.worker.hub.clone()
    }
    pub(crate) fn conversion(&self) -> Option<Arc<crate::caption_hls::State>> {
        self.worker.options.conversion.clone()
    }
    pub(crate) fn validate(&self, cfg: &serde_json::Value) -> Result<(), String> {
        if self.worker.options.preserve_subtitles
            != (crate::config::subtitle_tracks(cfg)? == "preserve")
        {
            return Err("shared source subtitle options differ".into());
        }
        let services = crate::captions::configuration(cfg)?;
        let native = services.iter().any(|s| s.native_track().is_some());
        match (&self.worker.options.conversion, native) {
            (None, false) => Ok(()),
            (Some(state), true) => {
                let configured =
                    serde_json::to_value(&services).map_err(|_| "invalid caption settings")?;
                let shared = serde_json::to_value(&state.decoder.lock().unwrap().services)
                    .map_err(|_| "invalid caption settings")?;
                if configured == shared {
                    Ok(())
                } else {
                    Err("shared source conversion options differ".into())
                }
            }
            _ => Err("shared source conversion options differ".into()),
        }
    }
    pub(crate) async fn copy<W: AsyncWrite + Unpin>(
        &mut self,
        output: &mut W,
        metadata: oneshot::Sender<Vec<Track>>,
        full_copy: bool,
        detected: &AtomicU64,
    ) -> Result<(), String> {
        let worker = self.worker.clone();
        let receiver = self
            .receiver
            .as_mut()
            .ok_or("shared input startup owner required")?;
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let tracks = loop {
            if worker.current().is_err() {
                return Err("shared source closed".into());
            }
            let notified = self.worker.metadata_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let ready = self.worker.tracks.lock().unwrap().clone();
            if let Some(tracks) = ready {
                break tracks;
            }
            tokio::select! {biased;_=self.worker.cancel.cancelled()=>return Err("shared source closed".into()),_=tick.tick()=>{},_=notified=>{}}
        };
        detected.fetch_max(
            self.worker
                .detected
                .load(std::sync::atomic::Ordering::Relaxed),
            std::sync::atomic::Ordering::Relaxed,
        );
        if self.worker.options.preserve_subtitles
            && !full_copy
            && tracks.iter().any(|t| t.codec == "subtitle")
        {
            return Err("native_subtitle_transcode_unsupported".into());
        }
        if worker.current().is_err() {
            return Err("shared source closed".into());
        }
        metadata
            .send(tracks)
            .map_err(|_| "native startup cancelled")?;
        let unsupported = || {
            !full_copy
                && worker.options.preserve_subtitles
                && worker.detected.load(std::sync::atomic::Ordering::Relaxed) > 0
        };
        let failure = || {
            if unsupported() {
                "native_subtitle_transcode_unsupported"
            } else {
                "shared source closed"
            }
        };
        let authority = || worker.current().is_ok() && !unsupported();
        let mut output = crate::m4_ingest::authority::Writer {
            inner: output,
            authority: Some(&authority),
        };
        loop {
            if !authority() {
                return Err(failure().into());
            }
            detected.fetch_max(
                worker.detected.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            let bytes = tokio::select! {biased;
                _=worker.cancel.cancelled()=>return Err("shared source closed".into()),
                _=tick.tick()=>continue,
                result=receiver.recv()=>result.ok_or("shared source pipe closed")?,
            };
            if !authority() {
                return Err(failure().into());
            }
            let write = output.write_all(&bytes);
            tokio::pin!(write);
            loop {
                tokio::select! {biased;
                    _=worker.cancel.cancelled()=>return Err("shared source closed".into()),
                    _=tick.tick()=>{
                        if !authority(){return Err(failure().into());}
                    },
                    result=&mut write=>{
                        if unsupported(){return Err(failure().into());}
                        result.map_err(|_|"media pipe closed")?;break;
                    },
                }
            }
        }
    }
}
