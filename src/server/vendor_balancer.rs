//! Vendor catalog authority feeds native placement, never a media worker on LB.
use super::*;
use crate::source_directory::modern_coordinator::{AdmittedCandidate, ModernSourceCoordinator};

impl App {
    pub(crate) async fn balance_vendor_srt(
        &self,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
        viewer: &ViewerRequest,
        streamid: &str,
        encrypted: bool,
    ) -> Result<Value, u16> {
        if self.options.role != "lb" || owner.admitted_current(admitted).is_err() {
            return Err(503);
        }
        let (_, grant, revision) = admitted.control_route();
        let result = self
            .srt_place(viewer, streamid, encrypted, grant, revision)
            .await;
        owner
            .with_admitted_current(admitted, || {
                if result.is_ok() {
                    grant.playback();
                }
                result
            })
            .map_err(|_| 503u16)?
    }

    pub(crate) async fn balance_vendor_rtsp(
        &self,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
        viewer: &ViewerRequest,
        source: &url::Url,
        secure: bool,
    ) -> Result<String, u16> {
        if self.options.role != "lb" || owner.admitted_current(admitted).is_err() {
            return Err(503);
        }
        let (_, grant, revision) = admitted.control_route();
        let result = self
            .rtsp_place(viewer, source, secure, grant, revision)
            .await;
        owner
            .with_admitted_current(admitted, || {
                if result.is_ok() {
                    grant.playback();
                }
                result
            })
            .map_err(|_| 503u16)?
    }
    pub(crate) async fn balance_vendor(
        self: &Arc<Self>,
        owner: &ModernSourceCoordinator,
        admitted: &AdmittedCandidate,
        path: &str,
        query: &str,
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
    async fn expired_vendor_lease_cannot_expose_rtsp_ticket_after_placement_wait() {
        use axum::routing::{get, post};
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", tcp.local_addr().unwrap());
        let count = Arc::new(AtomicU64::new(0));
        let observed = count.clone();
        let server = tokio::spawn(async move {
            axum::serve(tcp,Router::new()
            .route("/flussonix/api/v1/rtsp-routing",get(|headers:HeaderMap|async move {
                assert_eq!(headers["x-flussonix-peer"],"owned-peer-key");
                json_response(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000,"cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,"reserved":0,"reserved_mbps":0,"limit":1000,"streams":[],"stream_bitrates":{},"ready":[],"rtsp_publication":{"rtsp":"127.0.0.1:8554"}}))
            }))
            .route("/flussonix/api/v1/admit",post(move |headers:HeaderMap|{let observed=observed.clone();async move {
                assert_eq!(headers["x-flussonix-peer"],"owned-peer-key");observed.fetch_add(1,Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2100)).await;
                json_response(json!({"ticket":"00000000-0000-0000-0000-000000000001","expires_in":5}))
            }}))).await.unwrap()
        });
        let peer = json!({"hostname":"edge","api_url":api,"flussonix_rtsp_url":"rtsp://edge.invalid:8554"});
        let (_dir, app, admitted, origin, _key) =
            admitted_source(Duration::from_secs(2), "lb", Some(peer)).await;
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
        let result = app
            .balance_vendor_rtsp(
                app.modern_sources.as_ref().unwrap(),
                &admitted,
                &viewer,
                &url::Url::parse("rtsp://lb.invalid/owned").unwrap(),
                false,
            )
            .await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "must complete a real capacity reservation wait"
        );
        assert_eq!(result, Err(503), "expired source exposed remote ticket");
        assert_eq!(app.media.count().await, 0);
        drop(admitted);
        assert_eq!(app.playback_auth.active(), 0);
        server.abort();
        let _ = server.await;
        origin.abort();
        let _ = origin.await;
    }
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
            .balance_vendor(&owner, &viewer, "/owned/m4s", "", false)
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

#[cfg(test)]
mod vendor_destination_tests {
    use super::*;
    #[tokio::test]
    async fn vendor_source_lease_expiring_during_flussonic_telemetry_prevents_redirect() {
        use axum::{extract::OriginalUri, routing::get};
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", tcp.local_addr().unwrap());
        let calls = Arc::new(AtomicU64::new(0));
        let observed = calls.clone();
        let service=Router::new().route("/streamer/api/v3/{*rest}",get(move |OriginalUri(uri):OriginalUri,headers:HeaderMap|{let calls=observed.clone();async move {
   assert!(headers["authorization"].to_str().unwrap().starts_with("Cluster "));assert!(!headers.contains_key("x-flussonix-peer"));calls.fetch_add(1,Ordering::SeqCst);
   if uri.path().ends_with("/config") {tokio::time::sleep(Duration::from_millis(450)).await;json_response(json!({"stats":{"id":"owned-vendor-cdn","now":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()as u64,"cpu_usage":10,"memory_usage":20,"output_kbit":10000,"total_bandwidth":100000000,"total_clients":0,"streamer_status":"running"}}))}
   else{tokio::time::sleep(Duration::from_millis(300)).await;json_response(json!({"name":"owned","disabled":false,"named_by":"remote","stats":{"running":true}}))}
  }}));
        let cdn = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let peer = json!({"hostname":"edge","api_url":api,"public_payload_url":"https://edge.invalid","cluster_key":"owned-vendor-cluster-key","flussonix_peer_kind":"flussonic","flussonix_stream_map":[{"name":"owned","remote_name":"owned"}]});
        let (_dir, app, admitted, origin, _key) =
            crate::server::test_vendor_admission::admitted_source(
                Duration::from_secs(2),
                "lb",
                Some(peer),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let owner = app.modern_sources.as_ref().unwrap();
        let response = app
            .balance_vendor(owner, &admitted, "/owned/m4s", "", false)
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!response.headers().contains_key("location"));
        assert_eq!(app.media.count().await, 0);
        drop(admitted);
        assert_eq!(app.playback_auth.active(), 0);
        cdn.abort();
        let _ = cdn.await;
        origin.abort();
        let _ = origin.await;
    }
    #[tokio::test]
    async fn vendor_source_lease_expiring_during_flussonic_rtsp_telemetry_prevents_redirect() {
        use axum::{extract::OriginalUri, routing::get};
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", tcp.local_addr().unwrap());
        let calls = Arc::new(AtomicU64::new(0));
        let observed = calls.clone();
        let service=Router::new().route("/streamer/api/v3/{*rest}",get(move |OriginalUri(uri):OriginalUri,headers:HeaderMap|{let calls=observed.clone();async move {
   assert!(headers["authorization"].to_str().unwrap().starts_with("Cluster "));assert!(!headers.contains_key("x-flussonix-peer"));calls.fetch_add(1,Ordering::SeqCst);
   if uri.path().ends_with("/config") {tokio::time::sleep(Duration::from_millis(450)).await;json_response(json!({"stats":{"id":"owned-vendor-cdn","now":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()as u64,"cpu_usage":10,"memory_usage":20,"output_kbit":10000,"total_bandwidth":100000000,"total_clients":0,"streamer_status":"running"}}))}
   else{tokio::time::sleep(Duration::from_millis(300)).await;json_response(json!({"name":"owned","disabled":false,"named_by":"remote","stats":{"running":true}}))}
  }}));
        let cdn = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let peer = json!({"hostname":"edge","api_url":api,"public_payload_url":"https://edge.invalid","flussonix_rtsps_url":"rtsps://edge.invalid:8322","cluster_key":"owned-vendor-cluster-key","flussonix_peer_kind":"flussonic","flussonix_stream_map":[{"name":"owned","remote_name":"owned"}]});
        let (_dir, app, admitted, origin, _key) =
            crate::server::test_vendor_admission::admitted_source(
                Duration::from_secs(2),
                "lb",
                Some(peer),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let owner = app.modern_sources.as_ref().unwrap();
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
        let response = app
            .balance_vendor_rtsp(
                owner,
                &admitted,
                &viewer,
                &url::Url::parse("rtsps://owned-lb.invalid/owned").unwrap(),
                true,
            )
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(response, Err(503));
        assert_eq!(app.media.count().await, 0);
        drop(admitted);
        assert_eq!(app.playback_auth.active(), 0);
        cdn.abort();
        let _ = cdn.await;
        origin.abort();
        let _ = origin.await;
    }
    #[tokio::test]
    async fn vendor_source_lease_expiring_during_flussonic_srt_telemetry_prevents_lookup() {
        use axum::{extract::OriginalUri, routing::get};
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", tcp.local_addr().unwrap());
        let calls = Arc::new(AtomicU64::new(0));
        let observed = calls.clone();
        let service=Router::new().route("/streamer/api/v3/{*rest}",get(move |OriginalUri(uri):OriginalUri,headers:HeaderMap|{let calls=observed.clone();async move {
   assert!(headers["authorization"].to_str().unwrap().starts_with("Cluster "));assert!(!headers.contains_key("x-flussonix-peer"));calls.fetch_add(1,Ordering::SeqCst);
   if uri.path().ends_with("/config") {tokio::time::sleep(Duration::from_millis(450)).await;json_response(json!({"stats":{"id":"owned-vendor-cdn","now":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()as u64,"cpu_usage":10,"memory_usage":20,"output_kbit":10000,"total_bandwidth":100000000,"total_clients":0,"streamer_status":"running"}}))}
   else{tokio::time::sleep(Duration::from_millis(300)).await;json_response(json!({"name":"owned","disabled":false,"named_by":"remote","stats":{"running":true}}))}
  }}));
        let cdn = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let peer = json!({"hostname":"edge","api_url":api,"public_payload_url":"https://edge.invalid","flussonix_srt_url":"srt://edge.invalid:9998","flussonix_srt_encrypted":true,"cluster_key":"owned-vendor-cluster-key","flussonix_peer_kind":"flussonic","flussonix_stream_map":[{"name":"owned","remote_name":"owned"}]});
        let (_dir, app, admitted, origin, _key) =
            crate::server::test_vendor_admission::admitted_source(
                Duration::from_secs(2),
                "lb",
                Some(peer),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let owner = app.modern_sources.as_ref().unwrap();
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            ip: "127.0.0.1".into(),
            token: String::new(),
            qs: String::new(),
            user_agent: String::new(),
            referer: String::new(),
            host: String::new(),
            finite_media: false,
        };
        let response = app
            .balance_vendor_srt(owner, &admitted, &viewer, "#!::r=owned,m=request", true)
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(response, Err(503));
        assert_eq!(app.media.count().await, 0);
        drop(admitted);
        assert_eq!(app.playback_auth.active(), 0);
        cdn.abort();
        let _ = cdn.await;
        origin.abort();
        let _ = origin.await;
    }
}
