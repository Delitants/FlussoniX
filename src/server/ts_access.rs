//! SRT TS playback uses viewer policy/source fences without RTP codec limits.
use super::*;
pub(crate) struct Playback {
    pub worker: Arc<Worker>,
    pub grant: Grant,
    pub name: String,
    attached: bool,
}
impl Playback {
    pub fn attach(&mut self) {
        if !self.attached {
            self.worker.viewers.fetch_add(1, Ordering::Relaxed);
            self.worker.touch();
            self.attached = true;
        }
    }
}
impl Drop for Playback {
    fn drop(&mut self) {
        if self.attached {
            self.worker.viewers.fetch_sub(1, Ordering::Relaxed);
            self.worker.touch();
        }
    }
}
impl App {
    pub(crate) async fn ts_admit(&self, viewer: ViewerRequest) -> Result<Playback, u16> {
        if self.options.role == "lb" || self.options.drain {
            return Err(503);
        }
        let name = viewer.name.clone();
        let resolved = self.resolve(&name).await.ok_or(404u16)?;
        let grant = match self.playback_auth.authorize(resolved.policy, viewer).await {
            AuthOutcome::Allowed(g) => g,
            _ => return Err(403),
        };
        if self.config.revision() != resolved.revision {
            self.resolve(&name).await.ok_or(404u16)?;
        }
        let (cfg, _) = self.media_config(&name).await.ok_or(404u16)?;
        let signature = crate::media::media_signature(&cfg);
        let check = async {
            !grant.is_cancelled()
                && self.media_config(&name).await.is_some_and(|(c, _)| {
                    c["disabled"] != true && crate::media::media_signature(&c) == signature
                })
        };
        let worker = self
            .media
            .ensure_guarded(&name, &cfg, true, check)
            .await
            .map_err(|_| 503u16)?;
        if grant.is_cancelled()
            || worker.is_closed()
            || !self.media_config(&name).await.is_some_and(|(c, _)| {
                c["disabled"] != true && crate::media::media_signature(&c) == worker.signature()
            })
        {
            self.media.stop_if_current(&name, &worker).await;
            return Err(503);
        }
        Ok(Playback {
            worker,
            grant,
            name,
            attached: false,
        })
    }
    pub(crate) async fn ts_current(&self, playback: &Playback) -> bool {
        !playback.grant.is_cancelled()
            && !playback.worker.is_closed()
            && self
                .media_config(&playback.name)
                .await
                .is_some_and(|(c, _)| {
                    c["disabled"] != true
                        && crate::media::media_signature(&c) == playback.worker.signature()
                })
    }
}
