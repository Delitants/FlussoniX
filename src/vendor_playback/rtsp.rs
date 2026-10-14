//! RTSP delivery retains the same opaque shared permission as HTTP playback.
use super::*;
use crate::{
    playback_auth::ViewerRequest,
    server::rtsp_access::{Admission, Playback},
    source_directory::modern_coordinator::AdmissionOutcome,
};
impl Runtime {
    pub(crate) async fn rtsp_admit(
        &self,
        app: &App,
        viewer: ViewerRequest,
        key: &str,
        revision: u64,
        has_ticket: bool,
    ) -> Result<Admission, u16> {
        if !self.running() || app.modern_source_coordinator().is_err() || has_ticket {
            return Err(503);
        }
        let name = viewer.name.clone();
        let candidate = self
            .candidate(app, key, revision, &name)
            .await
            .map_err(|e| match e {
                Failure::Absent => 404u16,
                _ => 503,
            })?;
        let output_key = candidate.source_key().to_owned();
        let output_revision = candidate.config_revision();
        let admission = tokio::select! {biased;
            _=self.closed.cancelled()=>return Err(503),
            result=self.admit_selection(candidate,&app.playback_auth,viewer)=>match result {
                Ok(AdmissionOutcome::Allowed(admission))=>admission,
                Ok(AdmissionOutcome::Denied)=>return Err(403),
                Ok(AdmissionOutcome::Redirect(target))=>return Ok(Admission::Redirect(target)),
                Err(_)=>return Err(503),
            }
        };
        // Vendor RTSP placement needs its own source-fenced reservations. Never
        // start media on an LB or accept a native/HTTP ticket as vendor authority.
        if app.options.role == "lb" {
            return Err(503);
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
