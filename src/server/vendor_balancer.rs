//! Vendor catalog authority feeds native placement, never a media worker on LB.
use super::*;
use crate::source_directory::modern_coordinator::{AdmittedCandidate, ModernSourceCoordinator};

impl App {
    pub(crate) async fn balance_vendor(
        self: &Arc<Self>,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
        path: &str,
        query: &HashMap<String, String>,
        secure: bool,
    ) -> Response {
        if self.options.role != "lb" || owner.admitted_current(admitted).is_err() {
            return error(StatusCode::SERVICE_UNAVAILABLE, "vendor authority changed");
        }
        let (name, grant, revision) = admitted.control_route();
        let response = balance(self, name, path, query, secure, grant, revision).await;
        // Placement awaits advisory telemetry and a CDN-owned capacity ticket.
        // Recheck source identity/lease as well as config and viewer authority
        // before exposing a redirect. Unused remote tickets expire normally.
        match owner.with_admitted_current(admitted, || {
            if response.status() == StatusCode::FOUND {
                grant.playback();
            }
            response
        }) {
            Ok(response) => response,
            Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "vendor authority changed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::test_vendor_admission::admitted_source;
    #[tokio::test]
    async fn expired_vendor_lease_cannot_expose_a_cdn_ticket_after_placement_wait() {
        use axum::routing::{get, post};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let admitted = Arc::new(AtomicU64::new(0));
        let count = admitted.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener,Router::new()
                .route("/flussonix/api/v1/node",get(|headers:HeaderMap| async move {
                    assert_eq!(headers["x-flussonix-peer"],"owned-peer-key");
                    json_response(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000,"cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,"reserved":0,"reserved_mbps":0,"limit":1000,"streams":[],"stream_bitrates":{}}))
                }))
                .route("/flussonix/api/v1/admit",post(move |headers:HeaderMap| {let count=count.clone();async move {
                    assert_eq!(headers["x-flussonix-peer"],"owned-peer-key");
                    count.fetch_add(1,Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(2100)).await;
                    json_response(json!({"ticket":"owned-native-capacity-ticket","expires_in":5}))
                }}))
            ).await.unwrap()
        });
        let peer = json!({"hostname":"edge","api_url":url,"public_payload_url":url,"private_payload_url":url});
        let (_dir, app, viewer, origin, _key) =
            admitted_source(Duration::from_secs(2), "lb", Some(peer)).await;
        let owner = app.modern_sources.as_ref().unwrap().clone();
        let response = app
            .balance_vendor(&owner, &viewer, "/owned/m4s", &HashMap::new(), false)
            .await;
        let status = response.status();
        let location = response.headers().contains_key("location");
        let reservations = admitted.load(Ordering::SeqCst);
        assert_eq!(app.media.count().await, 0);
        drop(viewer);
        assert_eq!(app.playback_auth.active(), 0);
        server.abort();
        let _ = server.await;
        origin.abort();
        let _ = origin.await;
        assert_eq!(
            reservations, 1,
            "test must complete a real native admission wait"
        );
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!location, "expired vendor lease exposed a CDN ticket");
    }
}
