//! Native HTTP reservation redemption for a vendor-backed FlussoniX CDN.
use super::*;
use crate::source_directory::modern_coordinator::{AdmittedCandidate, ModernSourceCoordinator};

fn matches(reservation: &Reservation, name: &str) -> bool {
    reservation.stream == name
        && reservation.expires > Instant::now()
        && matches!(reservation.kind, rtsp_balancer::Kind::Http)
}

impl App {
    pub(crate) fn http_viewer_query(query: &str) -> String {
        query
            .split('&')
            .filter(|pair| {
                url::form_urlencoded::parse(pair.as_bytes())
                    .next()
                    .is_none_or(|(key, _)| key != "flussonix_ticket")
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
        owner
            .with_admitted_current(admitted, || {
                if !reservations
                    .get(ticket)
                    .is_some_and(|reservation| matches(reservation, name))
                {
                    return false;
                }
                reservations.remove(ticket);
                true
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn admitted_source_change_while_ledger_waits_leaves_ticket_unconsumed() {
        use crate::source_directory::modern_coordinator::AdmissionOutcome;
        use axum::{extract::OriginalUri, routing::get};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener,Router::new().fallback(get(|OriginalUri(uri):OriginalUri,headers:HeaderMap| async move {
                assert!(headers.contains_key("authorization"));
                assert!(!headers.contains_key("x-flussonix-peer"));
                let value=if uri.path().ends_with("/config") {json!({"stats":{"id":"owned-origin"}})}
                    else {assert_eq!(uri.path(),"/streamer/api/v3/streams");json!({"server_id":"owned-origin","streams":[{"name":"owned","groups":[],"static":true,"named_by":"config"}]})};
                json_response(value)
            }))).await.unwrap()
        });
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
        app.config.put("sources",&key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-key","on_play":"true"})).unwrap();
        let owner = app.modern_sources.as_ref().unwrap().clone();
        let handle = owner
            .install(
                owner
                    .capture(&key)
                    .unwrap()
                    .prepare(Duration::from_secs(3))
                    .unwrap(),
            )
            .unwrap();
        let rows = owner
            .inventory(&handle, Duration::from_secs(30))
            .await
            .unwrap();
        let outcome = owner
            .admit(
                rows.candidate("owned").unwrap(),
                &app.playback_auth,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "m4s".into(),
                    ip: "127.0.0.1".into(),
                    token: String::new(),
                    qs: String::new(),
                    user_agent: String::new(),
                    referer: String::new(),
                    host: String::new(),
                },
            )
            .await
            .unwrap();
        let AdmissionOutcome::Allowed(admitted) = outcome else {
            panic!("owned explicit policy rejected")
        };
        assert!(owner.admitted_current(&admitted).is_ok());
        let mut ledger = app.reservations.lock().await;
        ledger.insert(
            "owned-ticket".into(),
            Reservation {
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
}
