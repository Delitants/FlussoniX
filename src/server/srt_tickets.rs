//! Native reservations bind playback to one listener generation and source route.
use super::*;
use crate::source_directory::modern_coordinator::{
    AdmittedCandidate, ModernSourceCoordinator, ReservedCandidate,
};
use std::net::IpAddr;
#[derive(Clone)]
pub(super) struct Binding {
    pub listener_id: String,
    pub revision: u64,
    pub signature: Option<String>,
    pub viewer: Option<ViewerRequest>,
    pub peer_ip: Option<IpAddr>,
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
                b.listener_id == ticket.listener_id
                    && b.revision == self.config.revision()
                    && b.peer_ip
                        .is_none_or(|ip| viewer.ip.parse::<IpAddr>().ok() == Some(ip))
                    && b.viewer.as_ref().is_none_or(|saved| {
                        saved.name == viewer.name
                            && saved.proto == viewer.proto
                            && saved.ip == viewer.ip
                            && saved.token == viewer.token
                            && saved.qs == viewer.qs
                            && saved.user_agent == viewer.user_agent
                            && saved.referer == viewer.referer
                            && saved.host == viewer.host
                            && saved.finite_media == viewer.finite_media
                    })
            })
    }
    pub(crate) async fn srt_ticket_viewer(
        &self,
        ticket: Ticket<'_>,
        name: &str,
        peer_ip: IpAddr,
    ) -> Option<ViewerRequest> {
        self.with_srt_listener(ticket, || ())?;
        let ledger = self.reservations.lock().await;
        let reservation = ledger.get(ticket.id)?;
        let binding = reservation.srt.as_ref()?;
        if binding.peer_ip != Some(peer_ip)
            || reservation.stream != name
            || reservation.vendor.is_some()
        {
            return None;
        }
        let mut viewer = binding.viewer.clone()?;
        viewer.ip = peer_ip.to_string();
        self.srt_match(reservation, &viewer, ticket)
            .then_some(viewer)
    }
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "the HTTPS issuer wires this in Task 3")
    )]
    pub(crate) async fn reserve_srt_credential(
        &self,
        mut viewer: ViewerRequest,
        hint_mbps: f64,
    ) -> Result<String, u16> {
        if !matches!(self.options.role.as_str(), "cdn" | "standalone")
            || self.options.drain
            || viewer.proto != "srt"
            || viewer.token.is_empty()
            || viewer.finite_media
            || viewer.ip.parse::<IpAddr>().is_err()
            || crate::config::valid_name(&viewer.name).is_err()
            || !hint_mbps.is_finite()
            || !(0.0..=crate::cluster::MAX_HINT_MBPS).contains(&hint_mbps)
            || hint_mbps == 0.0
        {
            return Err(503);
        }
        let route = self.config.read(|root| {
            crate::vendor_playback::select(root, &viewer.name, self.config.revision())
        });
        if !matches!(route, crate::vendor_playback::Selection::Native) {
            return Err(503);
        }
        let resolved = self.resolve(&viewer.name).await.ok_or(404u16)?;
        if resolved.config["disabled"] == true {
            return Err(404);
        }
        let listener = self.srt_playback.lock().unwrap().clone();
        if listener["enabled"] != true || listener["encrypted"] != true {
            return Err(503);
        }
        let listener_id = listener["listener_id"].as_str().ok_or(503u16)?.to_owned();
        let peer_ip = viewer.ip.parse::<IpAddr>().map_err(|_| 503u16)?;
        viewer.ip = peer_ip.to_string();
        let binding = Binding {
            listener_id: listener_id.clone(),
            revision: resolved.revision,
            signature: Some(crate::media::media_signature(&resolved.config)),
            viewer: Some(viewer.clone()),
            peer_ip: Some(peer_ip),
        };
        let observations = json!({"stream_bitrates":self.media.output_bitrates().await});
        let observed_at = Instant::now();
        let mut reservations = self.reservations.lock().await;
        reservations.retain(|_, reservation| reservation.expires > Instant::now());
        let bitrate_mbps = hint_mbps
            .max(
                crate::cluster::observed_bitrate(
                    &observations,
                    &viewer.name,
                    observed_at
                        .elapsed()
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                )
                .unwrap_or(crate::cluster::FALLBACK_MBPS),
            )
            .max(crate::cluster::FALLBACK_MBPS);
        if !bitrate_mbps.is_finite()
            || bitrate_mbps > crate::cluster::MAX_HINT_MBPS
            || reservations
                .values()
                .filter(|r| r.srt.as_ref().is_some_and(|b| b.viewer.is_some()))
                .count()
                >= 4096
            || !self.reservation_capacity(&reservations, Some(&binding), bitrate_mbps)
        {
            return Err(503);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let ticket = Ticket {
            id: &id,
            encrypted: true,
            listener_id: &listener_id,
        };
        let committed = self
            .config
            .at_revision(binding.revision, |_| {
                self.with_srt_listener(ticket, || {
                    reservations.insert(
                        id.clone(),
                        Reservation {
                            http_slot: None,
                            srt: Some(binding),
                            vendor: None,
                            kind: rtsp_balancer::Kind::Srt {
                                encrypted: true,
                                token_hash: rtsp_balancer::token_hash(&viewer),
                            },
                            bitrate_mbps,
                            stream: viewer.name,
                            expires: Instant::now() + Duration::from_secs(5),
                        },
                    );
                })
            })
            .flatten();
        committed.ok_or(503u16)?;
        Ok(id)
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
    use std::net::IpAddr;
    #[tokio::test]
    async fn private_ticket_restores_original_callback_identity_only_for_observed_peer_ip() {
        let (_d, app, _legacy, _grant, _signature) = native().await;
        let original = ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            ip: "127.0.0.1".into(),
            token: "private-owned-token".into(),
            qs: "streamid=%23%21%3A%3Ar%3Downed%2Cu%3Dprivate-owned-token".into(),
            user_agent: "owned-client".into(),
            referer: "https://viewer.invalid/".into(),
            host: "playback.invalid".into(),
            finite_media: false,
        };
        let id = app
            .reserve_srt_credential(original.clone(), 5.0)
            .await
            .unwrap();
        let ticket = Ticket {
            id: &id,
            encrypted: true,
            listener_id: "owned-listener",
        };
        let wrong: IpAddr = "127.0.0.2".parse().unwrap();
        assert!(
            app.srt_ticket_viewer(ticket, "owned", wrong)
                .await
                .is_none()
        );
        assert!(
            app.srt_ticket_viewer(ticket, "other", "127.0.0.1".parse().unwrap())
                .await
                .is_none()
        );
        let restored = app
            .srt_ticket_viewer(ticket, "owned", "127.0.0.1".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(restored.token, original.token);
        assert_eq!(restored.qs, original.qs);
        assert_eq!(restored.user_agent, original.user_agent);
        assert_eq!(restored.referer, original.referer);
        assert_eq!(restored.host, original.host);
        assert_eq!(restored.ip, "127.0.0.1");
        assert!(
            app.srt_ticket_viewer(
                Ticket {
                    encrypted: false,
                    ..ticket
                },
                "owned",
                "127.0.0.1".parse().unwrap()
            )
            .await
            .is_none()
        );
        assert_eq!(
            app.reservations.lock().await.len(),
            2,
            "wrong claims must not consume the ticket"
        );
    }
    #[tokio::test]
    async fn private_ticket_accepts_the_same_ipv6_address_in_canonical_form() {
        let (_d, app, mut viewer, _grant, _signature) = native().await;
        viewer.ip = "0:0:0:0:0:0:0:1".into();
        let id = app.reserve_srt_credential(viewer, 5.0).await.unwrap();
        let ticket = Ticket {
            id: &id,
            encrypted: true,
            listener_id: "owned-listener",
        };
        let restored = app
            .srt_ticket_viewer(ticket, "owned", "::1".parse().unwrap())
            .await;
        assert_eq!(restored.unwrap().ip, "::1");
    }
    #[tokio::test]
    async fn private_ticket_expires_and_rejects_listener_or_config_changes_without_consumption() {
        for reason in ["expiry", "listener", "config"] {
            let (_d, app, mut viewer, _grant, _signature) = native().await;
            viewer.ip = "127.0.0.1".into();
            let id = app.reserve_srt_credential(viewer, 5.0).await.unwrap();
            let ticket = Ticket {
                id: &id,
                encrypted: true,
                listener_id: "owned-listener",
            };
            match reason {
                "expiry" => {
                    app.reservations.lock().await.get_mut(&id).unwrap().expires =
                        Instant::now() - Duration::from_millis(1)
                }
                "listener" => {
                    app.srt_playback.lock().unwrap()["listener_id"] = json!("restarted-listener")
                }
                _ => {
                    app.config
                        .put("streams", "owned", json!({"title":"changed"}))
                        .unwrap();
                }
            }
            assert!(
                app.srt_ticket_viewer(ticket, "owned", "127.0.0.1".parse().unwrap())
                    .await
                    .is_none(),
                "{reason}"
            );
            assert!(app.reservations.lock().await.contains_key(&id));
            assert_eq!(app.media.count().await, 0);
        }
    }
    #[tokio::test]
    async fn periodic_metrics_tick_erases_expired_private_credentials_without_new_admissions() {
        let (_d, app, mut viewer, _grant, _signature) = native().await;
        viewer.ip = "127.0.0.1".into();
        let id = app.reserve_srt_credential(viewer, 5.0).await.unwrap();
        app.reservations.lock().await.get_mut(&id).unwrap().expires =
            Instant::now() - Duration::from_millis(1);
        app.sample_metrics();
        assert!(!app.reservations.lock().await.contains_key(&id));
    }
    #[tokio::test]
    async fn private_ticket_redemption_is_single_use_when_same_ip_connections_race() {
        let (_d, app, mut viewer, _grant, _signature) = native().await;
        viewer.ip = "127.0.0.1".into();
        let id = app.reserve_srt_credential(viewer, 5.0).await.unwrap();
        let ticket = Ticket {
            id: &id,
            encrypted: true,
            listener_id: "owned-listener",
        };
        let restored = app
            .srt_ticket_viewer(ticket, "owned", "127.0.0.1".parse().unwrap())
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            app.ts_admit_ticket(restored.clone(), Some(ticket)),
            app.ts_admit_ticket(restored, Some(ticket)),
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert!(app.reservations.lock().await.get(&id).is_none());
        app.media.stop_all().await;
    }
    #[tokio::test]
    async fn redeemed_private_ticket_obeys_iptv_session_limit_and_subscriber_revocation() {
        let (_d, app, _viewer, _grant, _signature) = native().await;
        app.config
            .put(
                "streams",
                "owned",
                json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
            )
            .unwrap();
        app.iptv.set_enabled(true).unwrap();
        app.iptv
            .put_package("basic", json!({"channels":["owned"]}))
            .unwrap();
        app.iptv
            .put_subscriber(
                "alice",
                json!({"token":"private-subscriber-token","packages":["basic"],"max_sessions":1}),
            )
            .unwrap();
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            ip: "127.0.0.1".into(),
            token: "private-subscriber-token".into(),
            ..Default::default()
        };
        let first_id = app
            .reserve_srt_credential(viewer.clone(), 5.0)
            .await
            .unwrap();
        let first_ticket = Ticket {
            id: &first_id,
            encrypted: true,
            listener_id: "owned-listener",
        };
        let first_viewer = app
            .srt_ticket_viewer(first_ticket, "owned", "127.0.0.1".parse().unwrap())
            .await
            .unwrap();
        let first = app
            .ts_admit_ticket(first_viewer, Some(first_ticket))
            .await
            .unwrap();
        let second_id = app.reserve_srt_credential(viewer, 5.0).await.unwrap();
        let second_ticket = Ticket {
            id: &second_id,
            encrypted: true,
            listener_id: "owned-listener",
        };
        let second_viewer = app
            .srt_ticket_viewer(second_ticket, "owned", "127.0.0.1".parse().unwrap())
            .await
            .unwrap();
        let second = app
            .ts_admit_ticket(second_viewer, Some(second_ticket))
            .await;
        assert!(
            second.is_err(),
            "subscriber limit admitted a second SRT session"
        );
        assert!(first.current());
        app.iptv.delete_subscriber("alice").unwrap();
        tokio::time::timeout(Duration::from_millis(200), first.cancelled())
            .await
            .unwrap();
        assert!(!first.current());
        drop(first);
        app.media.stop_all().await;
    }
    #[tokio::test]
    async fn private_reservations_stop_at_4096_even_if_other_capacity_is_available() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-secret".into(),
                peer_key: "owned-cluster-key".into(),
                uplink_interface: "process".into(),
                uplink_mbps: 100_000.0,
                client_limit: 6000,
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put("streams", "owned", json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
        *app.srt_playback.lock().unwrap() = json!({"enabled":true,"listener_id":"owned-listener","encrypted":true,"client_limit":5000});
        tokio::time::sleep(Duration::from_millis(50)).await;
        app.sample_metrics();
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            ip: "127.0.0.1".into(),
            token: "owned-token".into(),
            ..Default::default()
        };
        let binding = Binding {
            listener_id: "owned-listener".into(),
            revision: app.config.revision(),
            signature: None,
            viewer: Some(viewer.clone()),
            peer_ip: Some("127.0.0.1".parse().unwrap()),
        };
        let mut ledger = app.reservations.lock().await;
        for index in 0..4096 {
            ledger.insert(
                format!("pending-{index}"),
                Reservation {
                    http_slot: None,
                    srt: Some(binding.clone()),
                    vendor: None,
                    kind: rtsp_balancer::Kind::Srt {
                        encrypted: true,
                        token_hash: rtsp_balancer::token_hash(&viewer),
                    },
                    bitrate_mbps: 0.0,
                    stream: "owned".into(),
                    expires: Instant::now() + Duration::from_secs(5),
                },
            );
        }
        drop(ledger);
        assert_eq!(
            app.reserve_srt_credential(viewer.clone(), 5.0).await,
            Err(503)
        );
        app.reservations.lock().await.remove("pending-0");
        assert!(app.reserve_srt_credential(viewer, 5.0).await.is_ok());
    }
    async fn native() -> (tempfile::TempDir, Arc<App>, ViewerRequest, Grant, String) {
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-key".into(),
                uplink_interface: "process".into(),
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
        *app.srt_playback.lock().unwrap() = json!({"enabled":true,"listener_id":"owned-listener","encrypted":true,"client_limit":4096});
        tokio::time::sleep(Duration::from_millis(50)).await;
        app.sample_metrics();
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
                    viewer: None,
                    peer_ip: None,
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
                    viewer: None,
                    peer_ip: None,
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
