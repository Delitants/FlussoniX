//! Vendor discovery on the LB must never start a privileged media pull.
use super::*;

#[tokio::test]
async fn discovered_vendor_lb_routes_m4s_without_local_stream_rows() {
    super::placement::check_native_lb_vendor_cdn("m4s", true).await;
}
#[tokio::test]
async fn discovered_vendor_lb_routes_guarded_decodable_hls_without_local_stream_rows() {
    super::placement::check_native_lb_vendor_cdn("index.m3u8", true).await;
}
#[tokio::test]
async fn vendor_lb_denial_and_absence_do_not_start_media() {
    use sha2::{Digest, Sha256};
    let f = Fixture::variant(
        json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}),
        "m4s",
        "lb",
    )
    .await;
    let denied = f.get("/owned/m4s?token=wrong").await.status();
    let absent = f.get("/missing/m4s?token=viewer").await.status();
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.media.count().await, 0);
    f.stop().await;
    assert_eq!(denied, StatusCode::FORBIDDEN);
    assert_eq!(absent, StatusCode::NOT_FOUND);
}

struct Peer {
    url: String,
    calls: Arc<Mutex<Vec<String>>>,
    hold: Hold,
    server: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn new() -> Self {
        use axum::{extract::Json, routing::post};
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let node_calls = calls.clone();
        let admit_calls = calls.clone();
        let hold: Hold = Arc::new(Mutex::new(None));
        let held = hold.clone();
        let service=Router::new()
            .route("/flussonix/api/v1/node",get(move |headers:HeaderMap|{let calls=node_calls.clone();async move {
                assert_eq!(headers["x-flussonix-peer"],"owned-native-peer-key");
                assert!(!headers.contains_key("cluster-auth-key"));assert!(!headers.contains_key("authorization"));
                calls.lock().unwrap().push("node".into());
                axum::Json(json!({"role":"cdn","uplink":0.1,"uplink_mbps":1000,"cpu":0.1,"ram":0.1,"drain":false,"age_ms":0,"active":0,"reserved":0,"reserved_mbps":0,"limit":1000,"streams":[],"stream_bitrates":{}}))
            }}))
            .route("/flussonix/api/v1/admit",post(move |headers:HeaderMap,Json(body):Json<Value>| {let calls=admit_calls.clone();let held=held.clone();async move {
                assert_eq!(headers["x-flussonix-peer"],"owned-native-peer-key");
                assert!(!headers.contains_key("cluster-auth-key"));assert!(!headers.contains_key("authorization"));
                assert_eq!(body["name"],"owned");assert_eq!(body["bitrate_mbps"],2.0);
                calls.lock().unwrap().push("admit".into());
                let pending=held.lock().unwrap().take();
                if let Some((started,release))=pending {let _=started.send(());let _=release.await;}
                axum::Json(json!({"ticket":"owned-capacity-ticket","expires_in":5}))
            }}));
        let server = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        Self {
            url,
            calls,
            hold,
            server,
        }
    }
    fn configure(&self, f: &Fixture) {
        f.app.config.put("peers","edge",json!({"api_url":self.url,"public_payload_url":self.url,"private_payload_url":self.url})).unwrap();
    }
    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

#[tokio::test]
async fn vendor_lb_source_edit_during_cdn_admission_emits_no_redirect() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    let peer = Peer::new().await;
    peer.configure(&f);
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    *peer.hold.lock().unwrap() = Some((started, held));
    let service = router(f.app.clone());
    let pending = tokio::spawn(async move {
        service
            .oneshot(
                Request::builder()
                    .uri("/owned/m4s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    f.app
        .config
        .put("sources", &f.source, json!({"except":["owned"]}))
        .unwrap();
    let _ = release.send(());
    let response = pending.await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!response.headers().contains_key("location"));
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.media.count().await, 0);
    assert_eq!(f.app.playback_auth.active(), 0);
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_owner_stop_cancels_pending_cdn_admission() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    let peer = Peer::new().await;
    peer.configure(&f);
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    *peer.hold.lock().unwrap() = Some((started, held));
    let service = router(f.app.clone());
    let pending = tokio::spawn(async move {
        service
            .oneshot(
                Request::builder()
                    .uri("/owned/m4s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    f.cancel.cancel();
    let response = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!response.headers().contains_key("location"));
    let _ = release.send(());
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.playback_auth.active(), 0);
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_callback_redirect_precedes_peer_rpcs() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    let peer = Peer::new().await;
    peer.configure(&f);
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let callback = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(|| async {
                (
                    StatusCode::FOUND,
                    [("location", "https://owned-billing.invalid/permission")],
                )
            })),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put("sources", &f.source, json!({"on_play":url}))
        .unwrap();
    let response = f.get("/owned/m4s?token=viewer").await;
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        response.headers()["location"],
        "https://owned-billing.invalid/permission"
    );
    assert!(peer.calls.lock().unwrap().is_empty());
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.playback_auth.active(), 0);
    callback.abort();
    let _ = callback.await;
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_group_policy_and_peer_header_cannot_bypass_viewer_guard() {
    use sha2::{Digest, Sha256};
    let f=Fixture::variant(json!({"on_play":"true","group_config":{"paid":{"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"paid-viewer"))}},"flussonix_group_pins":{"owned":["paid"]}}),"m4s","lb").await;
    (*f.remote.lock().unwrap())[0]["groups"] = json!(["paid"]);
    let peer = Peer::new().await;
    peer.configure(&f);
    let response = router(f.app.clone())
        .oneshot(
            Request::builder()
                .uri("/owned/m4s?token=wrong")
                .header("X-Flussonix-Peer", "owned-native-peer-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(peer.calls.lock().unwrap().is_empty());
    assert_eq!(
        f.get("/owned/m4s?token=paid-viewer").await.status(),
        StatusCode::FOUND
    );
    assert_eq!(&*peer.calls.lock().unwrap(), &["node", "admit"]);
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.media.count().await, 0);
    assert_eq!(f.app.playback_auth.active(), 1);
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_rejects_forged_public_group_before_redirect() {
    use sha2::{Digest, Sha256};
    let f = Fixture::variant(
        json!({"on_play":"true","group_config":{"paid":{"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"paid-viewer"))}},"flussonix_group_pins":{"owned":["paid"]}}),
        "m4s",
        "lb",
    )
    .await;
    let peer = Peer::new().await;
    peer.configure(&f);
    let response = f.get("/owned/m4s?token=paid-viewer").await;
    assert_ne!(response.status(), StatusCode::FOUND);
    assert!(peer.calls.lock().unwrap().is_empty());
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.playback_auth.active(), 0);
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_pin_edit_rejects_stale_group_before_cdn_handoff() {
    use sha2::{Digest, Sha256};
    let f = Fixture::variant(
        json!({"on_play":"true","group_config":{"paid":{"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"paid-viewer"))},"public":{"on_play":"true"}},"flussonix_group_pins":{"owned":["paid"]}}),
        "m4s",
        "lb",
    )
    .await;
    (*f.remote.lock().unwrap())[0]["groups"] = json!(["paid"]);
    let peer = Peer::new().await;
    peer.configure(&f);
    assert_eq!(
        f.get("/owned/m4s?token=paid-viewer").await.status(),
        StatusCode::FOUND
    );
    let admitted = peer.calls.lock().unwrap().len();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_group_pins":{"owned":["public"]}}),
        )
        .unwrap();
    assert_ne!(
        f.get("/owned/m4s?token=paid-viewer").await.status(),
        StatusCode::FOUND
    );
    assert_eq!(peer.calls.lock().unwrap().len(), admitted);
    assert_eq!(f.media_calls(), 0);
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_exclusion_and_disabled_local_precedence_skip_discovery() {
    let f = Fixture::variant(json!({"on_play":"true","except":["owned"]}), "m4s", "lb").await;
    assert_eq!(f.get("/owned/m4s").await.status(), StatusCode::NOT_FOUND);
    assert!(f.calls.lock().unwrap().is_empty());
    f.app
        .config
        .put("sources", &f.source, json!({"except":[]}))
        .unwrap();
    f.app
        .config
        .put("streams", "owned", json!({"disabled":true}))
        .unwrap();
    assert_eq!(f.get("/owned/m4s").await.status(), StatusCode::NOT_FOUND);
    assert!(f.calls.lock().unwrap().is_empty());
    assert_eq!(f.app.media.count().await, 0);
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_https_request_rejects_http_destination_and_preserves_secure_redirects() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    let peer = Peer::new().await;
    peer.configure(&f);
    let cert = tls_fixture::Certificates::new();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", tcp.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let server = tokio::spawn(flussonix::http_tls::serve(
        tcp,
        cert.server(),
        f.app.clone(),
        cancel.clone(),
    ));
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&cert.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let response = client
        .get(format!("{url}/owned/m4s?token=viewer"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(peer.calls.lock().unwrap().is_empty());
    f.app
        .config
        .put(
            "peers",
            "edge",
            json!({"public_payload_url":"https://owned-cdn.invalid"}),
        )
        .unwrap();
    let response = client
        .get(format!("{url}/owned/m4s?token=viewer"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert!(
        response.headers()["location"]
            .to_str()
            .unwrap()
            .starts_with("https://owned-cdn.invalid/owned/m4s?")
    );
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.media.count().await, 0);
    drop(client);
    cancel.cancel();
    assert!(server.await.unwrap().is_ok());
    peer.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_successful_placement_retains_native_user_session_limits() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    let peer = Peer::new().await;
    peer.configure(&f);
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let callback = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(|| async {
                (StatusCode::OK, [("x-userid", "owned-viewer-account")])
            })),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"on_play":{"url":url,"session_keys":["name","proto","token"],"max_sessions":1}}),
        )
        .unwrap();
    let first = f.get("/owned/m4s?token=first").await.status();
    let second = f.get("/owned/m4s?token=second").await.status();
    let active = f.app.playback_auth.active();
    let admits = peer
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_str() == "admit")
        .count();
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.media.count().await, 0);
    callback.abort();
    let _ = callback.await;
    peer.stop().await;
    f.stop().await;
    assert_eq!(first, StatusCode::FOUND);
    assert_eq!(second, StatusCode::FORBIDDEN);
    assert_eq!(active, 1);
    assert_eq!(admits, 1);
}

#[tokio::test]
async fn vendor_lb_active_session_reauth_uses_current_catalog_without_discovery_or_media() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    let peer = Peer::new().await;
    peer.configure(&f);
    assert_eq!(reauth(&f).await.status(), StatusCode::NOT_FOUND);
    assert!(f.calls.lock().unwrap().is_empty());
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let allowed = Arc::new(AtomicBool::new(true));
    let flag = allowed.clone();
    let count = Arc::new(AtomicU64::new(0));
    let calls = count.clone();
    let callback = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(move || {
                let flag = flag.clone();
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if flag.load(Ordering::SeqCst) {
                        StatusCode::OK
                    } else {
                        StatusCode::FORBIDDEN
                    }
                }
            })),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put("sources", &f.source, json!({"on_play":url}))
        .unwrap();
    assert_eq!(
        f.get("/owned/m4s?token=viewer").await.status(),
        StatusCode::FOUND
    );
    let discovery = f.calls.lock().unwrap().len();
    let placements = peer.calls.lock().unwrap().len();
    allowed.store(false, Ordering::SeqCst);
    let response = reauth(&f).await;
    let status = response.status();
    let body = bytes(response).await;
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.calls.lock().unwrap().len(), discovery);
    assert_eq!(peer.calls.lock().unwrap().len(), placements);
    let active = f.app.playback_auth.active();
    let callbacks = count.load(Ordering::SeqCst);
    let inactive_status = reauth(&f).await.status();
    callback.abort();
    let _ = callback.await;
    peer.stop().await;
    f.stop().await;
    assert_eq!(status, StatusCode::OK);
    let result: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["estimated_count"], 1);
    assert_eq!(active, 0);
    assert_eq!(callbacks, 2);
    assert_eq!(inactive_status, StatusCode::NOT_FOUND);
}
