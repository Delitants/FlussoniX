//! Vendor SRT keeps an individual permission on the shared TS worker.
use super::*;
use crate::{
    playback_auth::ViewerRequest, server::ts_access::Playback,
    source_directory::modern_coordinator::AdmissionOutcome,
};
impl Runtime {
    pub(crate) async fn ts_admit(
        &self,
        app: &App,
        viewer: ViewerRequest,
        key: &str,
        revision: u64,
        ticket: Option<crate::server::srt_tickets::Ticket<'_>>,
    ) -> Result<Playback, u16> {
        if !self.running() || app.modern_source_coordinator().is_err() {
            return Err(503);
        }
        let name = viewer.name.clone();
        let prepare = async {
            let route_viewer = viewer.clone();
            let reserved = if let Some(ticket) = ticket {
                Some(app.vendor_srt_ticket(ticket, &viewer).await.ok_or(503u16)?)
            } else {
                None
            };
            let (output_key, output_revision, outcome) = if let Some(reserved) = &reserved {
                (
                    reserved.source_key().to_owned(),
                    reserved.config_revision(),
                    self.owner
                        .admit_reserved(reserved, &app.playback_auth, viewer)
                        .await,
                )
            } else {
                let candidate =
                    self.candidate(app, key, revision, &name)
                        .await
                        .map_err(|e| match e {
                            Failure::Absent => 404u16,
                            _ => 503,
                        })?;
                (
                    candidate.source_key().to_owned(),
                    candidate.config_revision(),
                    self.admit_selection(candidate, &app.playback_auth, viewer)
                        .await,
                )
            };
            let admission = match outcome {
                Ok(AdmissionOutcome::Allowed(a)) => a,
                Ok(AdmissionOutcome::Denied | AdmissionOutcome::Redirect(_)) => return Err(403),
                Err(_) => return Err(503),
            };
            if let Some(ticket) = ticket
                && !app
                    .redeem_vendor_srt_ticket(
                        ticket,
                        &route_viewer,
                        &self.owner,
                        &admission,
                        reserved.as_ref().unwrap(),
                    )
                    .await
            {
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
            let playback = app
                .media
                .package_shared(attachment, &settings)
                .await
                .map_err(|_| 503u16)?;
            self.remember_recovery(&name, &playback);
            Playback::vendor(name.clone(), playback)
        };
        tokio::select! {biased;_=self.closed.cancelled()=>Err(503),result=prepare=>result}
    }
}
