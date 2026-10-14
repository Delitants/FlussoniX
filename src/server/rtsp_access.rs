//! RTSP uses exactly the same viewer policy and source/worker fences as HTTP.
use super::*;
pub(crate) enum Admission {
    Playback(Playback),
    Redirect(String),
}
pub(crate) struct Playback {
    pub worker: Arc<Worker>,
    authority: Authority,
    pub description: crate::rtp::Description,
    pub name: String,
    attached: bool,
}
enum Authority {
    Native(Grant),
    Vendor(crate::media::PackagedPlayback),
}
impl Playback {
    pub(crate) fn current(&self) -> bool {
        let authority = match &self.authority {
            Authority::Native(grant) => !grant.is_cancelled(),
            Authority::Vendor(playback) => playback.current().is_ok(),
        };
        authority
            && !self.worker.is_closed()
            && self
                .worker
                .wire
                .rtp
                .generation_is(self.description.generation)
    }
    pub(crate) async fn cancelled(&self) {
        match &self.authority {
            Authority::Native(grant) => grant.cancelled().await,
            Authority::Vendor(playback) => playback.closed().await,
        }
    }
    pub(crate) fn add_bytes(&self, bytes: usize) {
        match &self.authority {
            Authority::Native(grant) => grant.add_bytes(bytes),
            Authority::Vendor(playback) => playback.account_bytes(bytes),
        }
    }
    pub(crate) async fn vendor(
        name: String,
        playback: crate::media::PackagedPlayback,
    ) -> Result<Self, u16> {
        let worker = playback.worker();
        let description = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                playback.current().map_err(|_| 503u16)?;
                if let Some(description) = worker.wire.rtp.description() {
                    return description.map_err(|_| 415u16);
                }
                tokio::select! {biased;
                    _=playback.closed()=>return Err(503),
                    _=tokio::time::sleep(Duration::from_millis(25))=>{},
                }
            }
        })
        .await
        .map_err(|_| 503u16)??;
        let value = Self {
            worker,
            authority: Authority::Vendor(playback),
            description,
            name,
            attached: false,
        };
        if !value.current() {
            return Err(503);
        }
        Ok(value)
    }
    pub fn attach(&mut self) {
        if !self.attached {
            if matches!(self.authority, Authority::Native(_)) {
                self.worker.viewers.fetch_add(1, Ordering::Relaxed);
            }
            self.worker.touch();
            self.attached = true;
        }
    }
}
impl Drop for Playback {
    fn drop(&mut self) {
        if self.attached && matches!(self.authority, Authority::Native(_)) {
            self.worker.viewers.fetch_sub(1, Ordering::Relaxed);
            self.worker.touch();
        }
    }
}
impl App {
    pub(crate) async fn rtsp_admit(
        &self,
        mut viewer: ViewerRequest,
        source: &url::Url,
        secure: bool,
    ) -> Result<Admission, u16> {
        let (ticket, clean) = super::rtsp_balancer::strip_ticket(&viewer.qs);
        viewer.qs = clean;
        let selection = self.config.read(|root| {
            crate::vendor_playback::select(root, &viewer.name, self.config.revision())
        });
        match selection {
            crate::vendor_playback::Selection::Native => {}
            crate::vendor_playback::Selection::Excluded => return Err(404),
            crate::vendor_playback::Selection::Ambiguous => return Err(503),
            crate::vendor_playback::Selection::Vendor { key, revision } => {
                return match &self.vendor_playback {
                    Some(runtime) => {
                        runtime
                            .rtsp_admit(
                                self,
                                crate::vendor_playback::RtspRequest {
                                    viewer,
                                    key: &key,
                                    revision,
                                    ticket: ticket.as_deref(),
                                    source,
                                    secure,
                                },
                            )
                            .await
                    }
                    None => Err(503),
                };
            }
        }
        let route_viewer = viewer.clone();
        let name = viewer.name.clone();
        let resolved = self.resolve(&name).await.ok_or(404u16)?;
        let grant = match self
            .playback_auth
            .authorize_control(resolved.policy, viewer)
            .await
        {
            AuthOutcome::Allowed(g) => g,
            AuthOutcome::Denied => return Err(403),
            AuthOutcome::Redirect(target) => {
                // Redirects hold no media grant. Do not emit a decision from a
                // configuration changed while its callback was in flight.
                if self.config.revision() != resolved.revision {
                    return Err(503);
                }
                return Ok(Admission::Redirect(target));
            }
        };
        if self.config.revision() != resolved.revision {
            self.resolve(&name).await.ok_or(404u16)?;
        }
        if self.options.role == "lb" {
            if ticket.is_some() {
                return Err(503);
            }
            return self
                .rtsp_place(&route_viewer, source, secure, &grant, resolved.revision)
                .await
                .map(Admission::Redirect);
        }
        if let Some(ticket) = ticket {
            self.rtsp_consume(&ticket, &route_viewer, secure).await?;
        }
        grant.playback();
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
            || !self.media_config(&name).await.is_some_and(|(c, _)| {
                c["disabled"] != true && crate::media::media_signature(&c) == worker.signature()
            })
        {
            self.media.stop_if_current(&name, &worker).await;
            return Err(503);
        }
        let description=tokio::time::timeout(Duration::from_secs(8),async{loop{if let Some(description)=worker.wire.rtp.description(){return description.map_err(|_|415u16);}tokio::select!{biased;_=grant.cancelled()=>return Err(403),_=worker.closed()=>return Err(503),_=tokio::time::sleep(Duration::from_millis(25))=>{}}}}).await.map_err(|_|503u16)??;
        if grant.is_cancelled() || worker.is_closed() {
            return Err(403);
        }
        Ok(Admission::Playback(Playback {
            worker,
            authority: Authority::Native(grant),
            description,
            name,
            attached: false,
        }))
    }
    pub(crate) async fn rtsp_current(&self, playback: &Playback) -> bool {
        playback.current()
            && (matches!(playback.authority, Authority::Vendor(_))
                || self
                    .media_config(&playback.name)
                    .await
                    .is_some_and(|(c, _)| {
                        c["disabled"] != true
                            && crate::media::media_signature(&c) == playback.worker.signature()
                    }))
    }
}

#[cfg(test)]
mod tests;
