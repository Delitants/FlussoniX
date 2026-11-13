//! Source-only vendor reservations. Ticket possession never grants playback.
use super::*;
use crate::source_directory::modern_coordinator::{
    AdmittedCandidate, ModernSourceCoordinator, ReservedCandidate,
};

fn matches(
    reservation: &Reservation,
    viewer: &ViewerRequest,
    secure: bool,
    bind_token: bool,
) -> bool {
    reservation.expires > Instant::now()
        && reservation.stream == viewer.name
        && reservation.vendor.is_some()
        && matches!(&reservation.kind, rtsp_balancer::Kind::Rtsp{secure:s,token_hash:h}
            if *s == secure && (!bind_token || *h == rtsp_balancer::token_hash(viewer)))
}
impl App {
    /// Reject unknown, expired, HTTP/native, wrong-protocol
    /// tickets before source discovery. A mismatch never consumes the ticket.
    pub(crate) async fn vendor_rtsp_ticket(
        &self,
        ticket: &str,
        viewer: &ViewerRequest,
        secure: bool,
    ) -> Option<Arc<ReservedCandidate>> {
        self.reservations
            .lock()
            .await
            .get(ticket)
            .filter(|r| matches(r, viewer, secure, false))
            .and_then(|r| r.vendor.clone())
    }
    pub(crate) async fn redeem_vendor_rtsp_ticket(
        &self,
        ticket: &str,
        viewer: &ViewerRequest,
        secure: bool,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
        reserved: &Arc<ReservedCandidate>,
    ) -> bool {
        let mut ledger = self.reservations.lock().await;
        let mut removed = None;
        // admit_reserved produced this admission from the exact retained route.
        // No second source lock is acquired under the admitted-source fence.
        let result = owner.with_admitted_current(admitted, || {
            if !ledger.get(ticket).is_some_and(|r| {
                matches(r, viewer, secure, true)
                    && r.vendor.as_ref().is_some_and(|r| Arc::ptr_eq(r, reserved))
            }) {
                return false;
            }
            removed = ledger.remove(ticket);
            removed.is_some()
        });
        if result.is_err()
            && let Some(reservation) = removed
        {
            ledger.insert(ticket.into(), reservation);
        }
        result.unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::test_vendor_admission::admitted_source;
    #[tokio::test]
    async fn expired_source_only_reservation_never_enters_control_commit_or_viewer_admission() {
        let (_dir, app, admitted, origin, key) =
            admitted_source(Duration::from_secs(30), "cdn", None).await;
        drop(admitted);
        let owner = app.modern_sources.as_ref().unwrap();
        let handle = owner
            .install(
                owner
                    .capture(&key)
                    .unwrap()
                    .prepare(Duration::from_secs(1))
                    .unwrap(),
            )
            .unwrap();
        let rows = owner
            .inventory(&handle, Duration::from_secs(2))
            .await
            .unwrap();
        let reserved = owner
            .reserve_candidate(rows.candidate("owned").unwrap(), None, "owned")
            .unwrap();
        assert_eq!(app.playback_auth.active(), 0);
        tokio::time::sleep(Duration::from_millis(2100)).await;
        let mut committed = false;
        assert!(
            owner
                .with_reserved_current(&reserved, || {
                    committed = true;
                })
                .is_err()
        );
        assert!(!committed);
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            ip: "127.0.0.1".into(),
            token: String::new(),
            qs: String::new(),
            user_agent: String::new(),
            referer: String::new(),
            host: String::new(),
            finite_media: false,
        };
        assert!(
            owner
                .admit_reserved(&reserved, &app.playback_auth, viewer)
                .await
                .is_err()
        );
        assert_eq!(app.playback_auth.active(), 0);
        assert_eq!(app.media.count().await, 0);
        origin.abort();
        let _ = origin.await;
    }
    #[tokio::test]
    async fn replica_generation_shutdown_rejects_reservation_without_config_change() {
        use crate::source_directory::modern_coordinator::{ReplicaMember, ReplicaSelector};
        let (_dir, app, admitted, origin, key) =
            admitted_source(Duration::from_secs(30), "cdn", None).await;
        drop(admitted);
        let alias = key.replacen("127.0.0.1", "localhost", 1);
        let mut root = app.config.snapshot();
        let mut source = root["sources"][0].clone();
        source["flussonix_source_group"] = json!("owned-group");
        source["flussonix_content_id"] = json!("owned-content");
        root["sources"][0] = source.clone();
        source["url"] = json!(alias);
        root["sources"].as_array_mut().unwrap().push(source);
        app.config.replace(root).unwrap();
        let owner = app.modern_sources.as_ref().unwrap();
        let handle = owner
            .install(
                owner
                    .capture(&key)
                    .unwrap()
                    .prepare(Duration::from_secs(1))
                    .unwrap(),
            )
            .unwrap();
        let rows = owner
            .inventory(&handle, Duration::from_secs(30))
            .await
            .unwrap();
        let selector = Arc::new(
            ReplicaSelector::new(
                owner.clone(),
                "owned-group",
                [key, alias]
                    .into_iter()
                    .map(|key| ReplicaMember {
                        source_key: key,
                        content_id: "owned-content".into(),
                    })
                    .collect(),
            )
            .unwrap(),
        );
        let mut lookup = selector.begin("owned").unwrap();
        let choice = lookup.observe_cached(&handle, Ok(&rows)).unwrap().unwrap();
        let (candidate, scope) = choice.into_managed();
        let reserved = owner
            .reserve_candidate(candidate, Some(scope), "owned")
            .unwrap();
        let revision = app.config.revision();
        selector.shutdown();
        assert_eq!(app.config.revision(), revision);
        let mut committed = false;
        assert!(
            owner
                .with_reserved_current(&reserved, || {
                    committed = true;
                })
                .is_err()
        );
        assert!(!committed);
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            ip: "127.0.0.1".into(),
            token: String::new(),
            qs: String::new(),
            user_agent: String::new(),
            referer: String::new(),
            host: String::new(),
            finite_media: false,
        };
        assert!(
            owner
                .admit_reserved(&reserved, &app.playback_auth, viewer)
                .await
                .is_err()
        );
        assert_eq!(app.playback_auth.active(), 0);
        assert_eq!(app.media.count().await, 0);
        origin.abort();
        let _ = origin.await;
    }
}
