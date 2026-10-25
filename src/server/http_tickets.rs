//! Native HTTP reservation redemption for a vendor-backed FlussoniX CDN.
use super::*;
use crate::source_directory::modern_coordinator::{AdmittedCandidate, ModernSourceCoordinator};

fn matches(reservation: &Reservation, name: &str) -> bool {
    reservation.stream == name
        && reservation.expires > Instant::now()
        && matches!(reservation.kind, rtsp_balancer::Kind::Http)
}

// Control commit kept synchronous so the source fence and ledger form one
// transaction. The checker may reject after commit when a lease expires.
fn consume_ticket(
    reservations: &mut HashMap<String, Reservation>,
    name: &str,
    ticket: &str,
    check: impl FnOnce(
        &mut dyn FnMut() -> bool,
    )
        -> Result<bool, crate::source_directory::modern_coordinator::AdmissionFailure>,
) -> bool {
    let mut removed = None;
    let outcome = check(&mut || {
        if !reservations
            .get(ticket)
            .is_some_and(|reservation| matches(reservation, name))
        {
            return false;
        }
        removed = reservations.remove(ticket);
        removed.is_some()
    });
    // The source fence includes a post-commit lease check. Roll back before
    // releasing the ledger so a failed redemption cannot destroy its retry.
    if outcome.is_err()
        && let Some(reservation) = removed
    {
        reservations.insert(ticket.to_owned(), reservation);
    }
    outcome.unwrap_or(false)
}

impl App {
    pub(crate) fn http_viewer_query(query: &str) -> String {
        query
            .split('&')
            .filter(|pair| {
                url::form_urlencoded::parse(pair.as_bytes())
                    .next()
                    .is_none_or(|(key, _)| key != "flussonix_ticket" && key != "flussonix_handoff")
            })
            .collect::<Vec<_>>()
            .join("&")
    }

    pub(crate) fn http_ticket_redirect(path: &str, query: &HashMap<String, String>) -> Response {
        let clean = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(
                query
                    .iter()
                    .filter(|(key, _)| key.as_str() != "flussonix_ticket"),
            )
            .finish();
        // A Location starting with // changes authority, even for a relative URI.
        let path = format!("/{}", path.trim_start_matches('/'));
        let location = if clean.is_empty() {
            path
        } else {
            format!("{path}?{clean}")
        };
        (StatusCode::FOUND, [("location", location)]).into_response()
    }

    pub(crate) fn native_http_handoff_redirect(
        path: &str,
        query: &HashMap<String, String>,
        ticket: &str,
    ) -> Response {
        let mut next = query.clone();
        next.insert("flussonix_handoff".into(), ticket.into());
        Self::http_ticket_redirect(path, &next)
    }

    /// Cheap rejection before vendor discovery; this does not consume authority.
    pub(crate) async fn vendor_http_ticket_current(&self, name: &str, ticket: &str) -> bool {
        self.reservations
            .lock()
            .await
            .get(ticket)
            .is_some_and(|reservation| matches(reservation, name))
    }

    /// The ledger stays locked through the exact admitted source fence. No lock
    /// spans network I/O, and consuming a ticket never starts a worker.
    pub(crate) async fn redeem_vendor_http_ticket(
        &self,
        name: &str,
        ticket: &str,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
    ) -> bool {
        let mut reservations = self.reservations.lock().await;
        consume_ticket(&mut reservations, name, ticket, |commit| {
            owner.with_admitted_current(admitted, commit)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::test_vendor_admission::admitted_source;
    #[tokio::test]
    async fn post_commit_lease_expiry_restores_still_valid_http_reservation() {
        let (_dir, app, admitted, server, _key) =
            admitted_source(Duration::from_secs(2), "standalone", None).await;
        let owner = app.modern_sources.as_ref().unwrap().clone();
        let mut ledger = app.reservations.lock().await;
        ledger.insert(
            "owned-ticket".into(),
            Reservation {
                http_slot: None,
                srt: None,
                vendor: None,
                kind: rtsp_balancer::Kind::Http,
                bitrate_mbps: 2.0,
                stream: "owned".into(),
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
        let mut removed = false;
        let result = consume_ticket(&mut ledger, "owned", "owned-ticket", |commit| {
            owner.with_admitted_current(&admitted, || {
                let result = commit();
                removed = result;
                // Simulate preemption across the real leased-current postcheck.
                std::thread::sleep(Duration::from_millis(2100));
                result
            })
        });
        assert!(removed, "test must enter the commit with a current source");
        assert!(!result, "expired source fence accepted commit");
        assert!(
            ledger
                .get("owned-ticket")
                .is_some_and(|r| matches(r, "owned")),
            "failed final fence consumed a still-valid ticket"
        );
        drop(admitted);
        assert_eq!(app.playback_auth.active(), 0);
        server.abort();
        let _ = server.await;
    }
    #[tokio::test]
    async fn admitted_source_change_while_ledger_waits_leaves_ticket_unconsumed() {
        let (_dir, app, admitted, server, key) =
            admitted_source(Duration::from_secs(30), "standalone", None).await;
        let owner = app.modern_sources.as_ref().unwrap().clone();
        let mut ledger = app.reservations.lock().await;
        ledger.insert(
            "owned-ticket".into(),
            Reservation {
                http_slot: None,
                srt: None,
                vendor: None,
                kind: rtsp_balancer::Kind::Http,
                bitrate_mbps: 2.0,
                stream: "owned".into(),
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
        let a = app.clone();
        let o = owner.clone();
        let pending = tokio::spawn(async move {
            a.redeem_vendor_http_ticket("owned", "owned-ticket", &o, &admitted)
                .await
        });
        tokio::task::yield_now().await;
        app.config
            .put("sources", &key, json!({"except":["owned"]}))
            .unwrap();
        drop(ledger);
        assert!(!pending.await.unwrap());
        assert!(app.reservations.lock().await.contains_key("owned-ticket"));
        assert_eq!(app.media.count().await, 0);
        server.abort();
        let _ = server.await;
    }
    #[tokio::test]
    async fn native_ticket_redirect_keeps_authority_with_repeated_leading_slashes() {
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-key".into(),
                peer_key: "owned-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config.put("streams","owned",json!({"static":false,"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))})).unwrap();
        app.reservations.lock().await.insert(
            "owned-ticket".into(),
            Reservation {
                http_slot: None,
                srt: None,
                vendor: None,
                kind: rtsp_balancer::Kind::Http,
                bitrate_mbps: 2.0,
                stream: "owned".into(),
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
        let response = router(app.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("//owned/m4s?flussonix_ticket=owned-ticket&token=viewer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(response.headers()["location"], "/owned/m4s?token=viewer");
        assert_eq!(app.media.count().await, 0);
    }
    #[tokio::test]
    async fn rtsp_reservation_never_passes_vendor_http_ticket_precheck() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-key".into(),
                peer_key: "owned-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        for secure in [false, true] {
            app.reservations.lock().await.insert(
                "owned-ticket".into(),
                Reservation {
                    http_slot: None,
                    srt: None,
                    vendor: None,
                    kind: rtsp_balancer::Kind::Rtsp {
                        secure,
                        token_hash: "a".repeat(64),
                    },
                    bitrate_mbps: 2.0,
                    stream: "owned".into(),
                    expires: Instant::now() + Duration::from_secs(5),
                },
            );
            assert!(
                !app.vendor_http_ticket_current("owned", "owned-ticket")
                    .await
            );
            assert!(app.reservations.lock().await.contains_key("owned-ticket"));
        }
    }
    #[tokio::test]
    async fn vendor_http_ticket_does_not_block_its_own_viewer_admission() {
        use crate::source_directory::modern_coordinator::AdmissionOutcome;
        use tower::ServiceExt;
        let (_dir, app, admitted, server, key) =
            super::super::test_vendor_admission::admitted_source_with_limit(
                Duration::from_secs(30),
                "cdn",
                None,
                1,
            )
            .await;
        drop(admitted);
        let owner = app.modern_sources.as_ref().unwrap();
        let handle = owner
            .install(
                owner
                    .capture(&key)
                    .unwrap()
                    .prepare(Duration::from_secs(3))
                    .unwrap(),
            )
            .unwrap();
        let inventory = owner
            .inventory(&handle, Duration::from_secs(30))
            .await
            .unwrap();
        app.sample_metrics();
        tokio::time::sleep(Duration::from_millis(100)).await;
        app.sample_metrics();
        let response = router(app.clone())
            .oneshot(
                axum::http::Request::post("/flussonix/api/v1/admit")
                    .header("X-Flussonix-Peer", "owned-peer-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"name":"owned"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 16384)
                .await
                .unwrap(),
        )
        .unwrap();
        let outcome = owner
            .admit(
                inventory.candidate("owned").unwrap(),
                &app.playback_auth,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let AdmissionOutcome::Allowed(admitted) = outcome else {
            panic!("vendor HTTP ticket blocked its own independent authorization");
        };
        assert!(
            app.redeem_vendor_http_ticket(
                "owned",
                value["ticket"].as_str().unwrap(),
                owner,
                &admitted
            )
            .await
        );
        assert_eq!(app.media.count().await, 0);
        server.abort();
        let _ = server.await;
    }
}
