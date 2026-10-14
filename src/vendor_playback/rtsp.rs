//! RTSP delivery retains the same opaque shared permission as HTTP playback.
use super::*;
use crate::{
    playback_auth::ViewerRequest,
    server::rtsp_access::{Admission, Playback},
    source_directory::modern_coordinator::{AdmissionOutcome, ReservedCandidate},
};
pub(crate) struct RtspRequest<'a> {
    pub viewer: ViewerRequest,
    pub key: &'a str,
    pub revision: u64,
    pub ticket: Option<&'a str>,
    pub source: &'a url::Url,
    pub secure: bool,
}
impl Runtime {
    pub(crate) async fn rtsp_reserve(
        &self,
        app: &App,
        name: &str,
        key: &str,
        revision: u64,
    ) -> Result<ReservedCandidate, u16> {
        if !self.running() {
            return Err(503);
        }
        let candidate = self
            .candidate(app, key, revision, name)
            .await
            .map_err(|e| {
                if matches!(e, Failure::Absent) {
                    404u16
                } else {
                    503
                }
            })?;
        let (candidate, selection) = match candidate {
            super::replicas::Candidate::Direct(c) => (c, None),
            super::replicas::Candidate::Replica(_, c) => {
                let (c, s) = c.into_managed();
                (c, Some(s))
            }
        };
        self.owner
            .reserve_candidate(candidate, selection, name)
            .map_err(|_| 503u16)
    }
    pub(crate) async fn rtsp_admit(
        &self,
        app: &App,
        request: RtspRequest<'_>,
    ) -> Result<Admission, u16> {
        let RtspRequest {
            viewer,
            key,
            revision,
            ticket,
            source,
            secure,
        } = request;
        if !self.running() || app.modern_source_coordinator().is_err() {
            return Err(503);
        }
        let name = viewer.name.clone();
        if app.options.role == "lb" && ticket.is_some() {
            return Err(503);
        }
        let route_viewer = viewer.clone();
        let reserved = if let Some(ticket) = ticket {
            Some(
                app.vendor_rtsp_ticket(ticket, &viewer, secure)
                    .await
                    .ok_or(503u16)?,
            )
        } else {
            None
        };
        let prepare = async {
            if let Some(reserved) = &reserved {
                Ok::<_, u16>((
                    reserved.source_key().to_owned(),
                    reserved.config_revision(),
                    self.owner
                        .admit_reserved(reserved, &app.playback_auth, viewer)
                        .await,
                ))
            } else {
                let candidate =
                    self.candidate(app, key, revision, &name)
                        .await
                        .map_err(|e| match e {
                            Failure::Absent => 404u16,
                            _ => 503,
                        })?;
                let output_key = candidate.source_key().to_owned();
                let output_revision = candidate.config_revision();
                Ok((
                    output_key,
                    output_revision,
                    self.admit_selection(candidate, &app.playback_auth, viewer)
                        .await,
                ))
            }
        };
        let (output_key, output_revision, outcome) = tokio::select! {biased; _=self.closed.cancelled()=>return Err(503), result=prepare=>result?};
        let admission = match outcome {
            Ok(AdmissionOutcome::Allowed(admission)) => admission,
            Ok(AdmissionOutcome::Denied) => return Err(403),
            Ok(AdmissionOutcome::Redirect(target)) => return Ok(Admission::Redirect(target)),
            Err(_) => return Err(503),
        };
        if app.options.role == "lb" {
            return tokio::select! {biased;
                _=self.closed.cancelled()=>Err(503),
                result=app.balance_vendor_rtsp(&self.owner,&admission,&route_viewer,source,secure)=>result.map(Admission::Redirect),
            };
        }
        if let Some(ticket) = ticket {
            if !app
                .redeem_vendor_rtsp_ticket(
                    ticket,
                    &route_viewer,
                    secure,
                    &self.owner,
                    &admission,
                    reserved.as_ref().expect("checked ticket"),
                )
                .await
            {
                return Err(503);
            }
        }
        let settings = self
            .output_settings(app, &output_key, output_revision)
            .map_err(|_| 503u16)?;
        let _attempt = self
            .register_recovery(app, &name, key, revision, &admission, &settings)
            .map_err(|_| 503u16)?;
        let attachment = self
            .inputs
            .attach_configured(admission, &settings)
            .map_err(|_| 503u16)?;
        let playback = tokio::select! {biased;
            _=self.closed.cancelled()=>return Err(503),
            result=app.media.package_shared(attachment,&settings)=>result.map_err(|_|503u16)?,
        };
        self.remember_recovery(&name, &playback);
        Playback::vendor(name, playback)
            .await
            .map(Admission::Playback)
    }
}
