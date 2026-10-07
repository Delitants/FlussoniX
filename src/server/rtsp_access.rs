//! RTSP uses exactly the same viewer policy and source/worker fences as HTTP.
use super::*;
pub(crate) enum Admission {
    Playback(Playback),
    Redirect(String),
}
pub(crate) struct Playback {
    pub worker: Arc<Worker>,
    pub grant: Grant,
    pub description: crate::rtp::Description,
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
    pub(crate) async fn rtsp_admit(
        &self,
        mut viewer: ViewerRequest,
        source: &url::Url,
        secure: bool,
    ) -> Result<Admission, u16> {
        let (ticket, clean) = super::rtsp_balancer::strip_ticket(&viewer.qs);
        viewer.qs = clean;
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
            grant,
            description,
            name,
            attached: false,
        }))
    }
    pub(crate) async fn rtsp_current(&self, playback: &Playback) -> bool {
        !playback.grant.is_cancelled()
            && !playback.worker.is_closed()
            && self
                .media_config(&playback.name)
                .await
                .is_some_and(|(c, _)| {
                    c["disabled"] != true
                        && crate::media::media_signature(&c) == playback.worker.signature()
                })
            && playback
                .worker
                .wire
                .rtp
                .description()
                .is_some_and(|d| d.is_ok_and(|d| d == playback.description))
    }
}
