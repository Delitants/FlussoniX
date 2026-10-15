//! Native reservations bind playback to one listener generation and source route.
use super::*;
use crate::source_directory::modern_coordinator::{
    AdmittedCandidate, ModernSourceCoordinator, ReservedCandidate,
};
#[derive(Clone)]
pub(super) struct Binding {
    pub listener_id: String,
    pub revision: u64,
    pub signature: Option<String>,
}
#[derive(Clone, Copy)]
pub(crate) struct Ticket<'a> {
    pub id: &'a str,
    pub encrypted: bool,
    pub listener_id: &'a str,
}
impl App {
    pub(crate) fn srt_listener_id(&self) -> String {
        self.srt_playback.lock().unwrap()["listener_id"]
            .as_str()
            .unwrap_or("")
            .into()
    }

    fn srt_match(&self, r: &Reservation, viewer: &ViewerRequest, ticket: Ticket<'_>) -> bool {
        viewer.proto == "srt"
            && r.expires > Instant::now()
            && r.stream == viewer.name
            && matches!(&r.kind,rtsp_balancer::Kind::Srt{encrypted,token_hash} if *encrypted==ticket.encrypted && *token_hash==rtsp_balancer::token_hash(viewer))
            && r.srt.as_ref().is_some_and(|b| {
                b.listener_id == ticket.listener_id && b.revision == self.config.revision()
            })
    }
    fn with_srt_listener<T>(&self, ticket: Ticket<'_>, commit: impl FnOnce() -> T) -> Option<T> {
        let listener = self.srt_playback.lock().unwrap();
        if listener["enabled"] != true
            || listener["listener_id"] != ticket.listener_id
            || listener["encrypted"] != ticket.encrypted
        {
            return None;
        }
        Some(commit())
    }
    pub(crate) async fn srt_consume(
        &self,
        ticket: Ticket<'_>,
        viewer: &ViewerRequest,
        grant: &Grant,
        signature: &str,
    ) -> Result<(), u16> {
        let mut ledger = self.reservations.lock().await;
        let binding = ledger
            .get(ticket.id)
            .filter(|r| r.vendor.is_none() && self.srt_match(r, viewer, ticket))
            .and_then(|r| r.srt.as_ref())
            .ok_or(503u16)?
            .clone();
        self.config
            .at_revision(binding.revision, |_| {
                if grant.is_cancelled()
                    || self.with_srt_listener(ticket, || ()).is_none()
                    || binding.signature.as_deref() != Some(signature)
                {
                    return Err(503);
                }
                self.with_srt_listener(ticket, || {
                    ledger.remove(ticket.id);
                    grant.playback();
                    Ok(())
                })
                .ok_or(503u16)?
            })
            .ok_or(503u16)?
    }
    pub(crate) async fn vendor_srt_ticket(
        &self,
        ticket: Ticket<'_>,
        viewer: &ViewerRequest,
    ) -> Option<Arc<ReservedCandidate>> {
        self.with_srt_listener(ticket, || ())?;
        self.reservations
            .lock()
            .await
            .get(ticket.id)
            .filter(|r| self.srt_match(r, viewer, ticket))
            .and_then(|r| r.vendor.clone())
    }
    pub(crate) async fn redeem_vendor_srt_ticket(
        &self,
        ticket: Ticket<'_>,
        viewer: &ViewerRequest,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
        reserved: &Arc<ReservedCandidate>,
    ) -> bool {
        let mut ledger = self.reservations.lock().await;
        let mut removed = None;
        let result = owner.with_admitted_current(admitted, || {
            if self.with_srt_listener(ticket, || ()).is_none()
                || !ledger.get(ticket.id).is_some_and(|r| {
                    self.srt_match(r, viewer, ticket)
                        && r.vendor.as_ref().is_some_and(|r| Arc::ptr_eq(r, reserved))
                })
            {
                return false;
            }
            self.with_srt_listener(ticket, || {
                removed = ledger.remove(ticket.id);
                removed.is_some()
            })
            .unwrap_or(false)
        });
        if result.is_err()
            && let Some(reservation) = removed
        {
            ledger.insert(ticket.id.into(), reservation);
        }
        result.unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn native() -> (tempfile::TempDir, Arc<App>, ViewerRequest, Grant, String) {
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        *app.srt_playback.lock().unwrap() =
            json!({"enabled":true,"listener_id":"owned-listener","encrypted":true});
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            token: "opaque%2B+é".into(),
            ..Default::default()
        };
        let resolved = app.resolve("owned").await.unwrap();
        let signature = crate::media::media_signature(&resolved.config);
        let AuthOutcome::Allowed(grant) = app
            .playback_auth
            .authorize_control(resolved.policy, viewer.clone())
            .await
        else {
            panic!("owned grant denied")
        };
        app.reservations.lock().await.insert(
            "owned-ticket".into(),
            Reservation {
                http_slot: None,
                srt: Some(Binding {
                    listener_id: "owned-listener".into(),
                    revision: app.config.revision(),
                    signature: Some(signature.clone()),
                }),
                vendor: None,
                kind: rtsp_balancer::Kind::Srt {
                    encrypted: true,
                    token_hash: rtsp_balancer::token_hash(&viewer),
                },
                bitrate_mbps: 5.0,
                stream: "owned".into(),
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
        (d, app, viewer, grant, signature)
    }
    fn ticket() -> Ticket<'static> {
        Ticket {
            id: "owned-ticket",
            encrypted: true,
            listener_id: "owned-listener",
        }
    }
    #[tokio::test]
    async fn mismatch_does_not_consume_ticket_across_token_name_protocol_encryption_and_listener() {
        let (_d, app, viewer, grant, sig) = native().await;
        for field in ["name", "token", "proto"] {
            let mut wrong = viewer.clone();
            match field {
                "name" => wrong.name = "other".into(),
                "token" => wrong.token = "wrong".into(),
                _ => wrong.proto = "rtsp".into(),
            };
            assert!(
                app.srt_consume(ticket(), &wrong, &grant, &sig)
                    .await
                    .is_err(),
                "{field} mismatch consumed SRT reservation"
            );
        }
        for t in [
            Ticket {
                encrypted: false,
                ..ticket()
            },
            Ticket {
                listener_id: "other",
                ..ticket()
            },
            Ticket {
                id: "unknown",
                ..ticket()
            },
        ] {
            assert!(app.srt_consume(t, &viewer, &grant, &sig).await.is_err());
        }
        assert_eq!(app.reservations.lock().await.len(), 1);
        assert!(
            app.srt_consume(ticket(), &viewer, &grant, &sig)
                .await
                .is_ok()
        );
        assert!(
            app.srt_consume(ticket(), &viewer, &grant, &sig)
                .await
                .is_err()
        );
        assert_eq!(app.media.count().await, 0);
    }
    #[tokio::test]
    async fn expired_changed_route_and_restarted_listener_leave_ticket_unredeemed() {
        for reason in ["expiry", "config", "signature", "listener"] {
            let (_d, app, viewer, grant, sig) = native().await;
            match reason {
                "expiry" => {
                    app.reservations
                        .lock()
                        .await
                        .get_mut("owned-ticket")
                        .unwrap()
                        .expires = Instant::now() - Duration::from_millis(1)
                }
                "config" => {
                    app.config
                        .put("streams", "owned", json!({"title":"changed"}))
                        .unwrap();
                }
                "listener" => {
                    app.srt_playback.lock().unwrap()["listener_id"] = json!("restarted-listener")
                }
                _ => {}
            }
            assert!(
                app.srt_consume(
                    ticket(),
                    &viewer,
                    &grant,
                    if reason == "signature" {
                        "changed"
                    } else {
                        &sig
                    }
                )
                .await
                .is_err(),
                "{reason}"
            );
            assert_eq!(app.reservations.lock().await.len(), 1);
            assert_eq!(app.media.count().await, 0);
        }
    }
    #[tokio::test]
    async fn revoked_grant_while_waiting_for_ledger_cannot_consume_or_start_media() {
        use std::task::Poll;
        let (_d, app, viewer, grant, sig) = native().await;
        let held = app.reservations.lock().await;
        let mut consuming = Box::pin(app.srt_consume(ticket(), &viewer, &grant, &sig));
        assert!(matches!(
            futures_util::poll!(consuming.as_mut()),
            Poll::Pending
        ));
        let id = app.playback_auth.snapshots()[0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(app.playback_auth.revoke(&id));
        drop(held);
        assert!(consuming.await.is_err());
        assert_eq!(app.reservations.lock().await.len(), 1);
        assert_eq!(app.media.count().await, 0);
    }
    #[tokio::test]
    async fn expired_vendor_source_reservation_cannot_redeem_or_start_media() {
        use crate::source_directory::modern_coordinator::AdmissionOutcome;
        let (_d, app, admitted, origin, key) =
            crate::server::test_vendor_admission::admitted_source(
                Duration::from_secs(30),
                "cdn",
                None,
            )
            .await;
        drop(admitted);
        *app.srt_playback.lock().unwrap() =
            json!({"enabled":true,"listener_id":"owned-listener","encrypted":true});
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
        let reserved = Arc::new(
            owner
                .reserve_candidate(rows.candidate("owned").unwrap(), None, "owned")
                .unwrap(),
        );
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            token: "owned".into(),
            ..Default::default()
        };
        let AdmissionOutcome::Allowed(admitted) = owner
            .admit_reserved(&reserved, &app.playback_auth, viewer.clone())
            .await
            .unwrap()
        else {
            panic!("owned source denied")
        };
        app.reservations.lock().await.insert(
            "owned-ticket".into(),
            Reservation {
                http_slot: None,
                srt: Some(Binding {
                    listener_id: "owned-listener".into(),
                    revision: app.config.revision(),
                    signature: None,
                }),
                vendor: Some(reserved.clone()),
                kind: rtsp_balancer::Kind::Srt {
                    encrypted: true,
                    token_hash: rtsp_balancer::token_hash(&viewer),
                },
                bitrate_mbps: 5.0,
                stream: "owned".into(),
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert!(
            !app.redeem_vendor_srt_ticket(ticket(), &viewer, owner, &admitted, &reserved)
                .await
        );
        assert_eq!(app.reservations.lock().await.len(), 1);
        assert_eq!(app.media.count().await, 0);
        origin.abort();
        let _ = origin.await;
    }
}
