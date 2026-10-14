//! Control-only SRT discovery retains source lease and replica authority.
use super::*;
use crate::{playback_auth::ViewerRequest, source_directory::modern_coordinator::AdmissionOutcome};
impl Runtime {
    pub(crate) async fn srt_lookup(
        &self,
        app: &App,
        viewer: ViewerRequest,
        key: &str,
        revision: u64,
        streamid: &str,
        encrypted: bool,
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
            app.balance_vendor_srt(&self.owner, &admitted, &viewer, streamid, encrypted)
                .await
        };
        tokio::select! {biased; _=self.closed.cancelled()=>Err(503), result=work=>result}
    }
}
