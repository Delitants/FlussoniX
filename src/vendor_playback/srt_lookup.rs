//! Control-only SRT discovery retains source lease and replica authority.
use super::*;
use crate::{playback_auth::ViewerRequest, source_directory::modern_coordinator::AdmissionOutcome};
impl Runtime {
    pub(crate) async fn issue_local_srt_ticket(
        &self,
        app: &App,
        viewer: ViewerRequest,
        key: &str,
        revision: u64,
    ) -> Result<String, u16> {
        if !self.running() || app.modern_source_coordinator().is_err() {
            return Err(503);
        }
        let reserved = self.rtsp_reserve(app, &viewer.name, key, revision).await?;
        let admitted = match self
            .owner
            .admit_reserved(&reserved, &app.playback_auth, viewer.clone())
            .await
        {
            Ok(AdmissionOutcome::Allowed(admitted)) => admitted,
            Ok(AdmissionOutcome::Denied | AdmissionOutcome::Redirect(_)) => return Err(403),
            Err(_) => return Err(503),
        };
        self.owner.admitted_current(&admitted).map_err(|_| 503u16)?;
        let ticket = app
            .reserve_srt_vendor_credential(
                viewer,
                crate::cluster::FALLBACK_MBPS,
                Arc::new(reserved),
            )
            .await?;
        let (_, grant, _) = admitted.control_route();
        let current = self.owner.admitted_current(&admitted).is_ok();
        if !current
            || (app.iptv_authority_client.is_some()
                && !app.playback_auth.release_redirect_lease(grant).await)
        {
            app.discard_srt_credential(&ticket).await;
            return Err(503);
        }
        Ok(ticket)
    }
    pub(crate) async fn srt_lookup(
        &self,
        app: &App,
        viewer: ViewerRequest,
        key: &str,
        revision: u64,
        streamid: &str,
        mode: crate::server::SrtLookupMode,
    ) -> Result<Value, u16> {
        if !self.running() || app.modern_source_coordinator().is_err() {
            return Err(503);
        }
        let work = async {
            let candidate = self
                .candidate(app, key, revision, &viewer.name)
                .await
                .map_err(|e| match e {
                    Failure::Absent => 404u16,
                    _ => 503,
                })?;
            let admitted = match self
                .admit_selection(candidate, &app.playback_auth, viewer.clone())
                .await
            {
                Ok(AdmissionOutcome::Allowed(a)) => a,
                Ok(AdmissionOutcome::Denied | AdmissionOutcome::Redirect(_)) => return Err(403),
                Err(_) => return Err(503),
            };
            app.balance_vendor_srt(&self.owner, &admitted, &viewer, streamid, mode)
                .await
        };
        tokio::select! {biased; _=self.closed.cancelled()=>Err(503), result=work=>result}
    }
}
