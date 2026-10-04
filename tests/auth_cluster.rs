use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::{
    auth::{Credentials, Role},
    cluster::{NodeLoad, select},
};
#[test]
fn edit_view_and_peer_credentials_are_separate() {
    let c = Credentials::new(
        "admin",
        "edit-secret",
        Some(("viewer", "view-secret")),
        "peer-secret",
    );
    let basic = |u: &str, p: &str| format!("Basic {}", STANDARD.encode(format!("{u}:{p}")));
    assert_eq!(
        c.authorize(Some(&basic("admin", "edit-secret"))),
        Some(Role::Edit)
    );
    assert_eq!(
        c.authorize(Some(&basic("viewer", "view-secret"))),
        Some(Role::View)
    );
    assert_eq!(
        c.authorize(Some(&format!(
            "Bearer {}",
            STANDARD.encode("admin:edit-secret")
        ))),
        Some(Role::Edit)
    );
    assert_eq!(c.authorize(Some(&basic("admin", "wrong"))), None);
    assert_eq!(c.authorize(Some("peer-secret")), None);
    assert!(!c.peer(Some("edit-secret")));
    assert!(c.peer(Some("peer-secret")));
}
fn node(name: &str, uplink: f64, ready: bool) -> NodeLoad {
    NodeLoad {
        name: name.into(),
        uplink,
        cpu: 0.1,
        ram: 0.2,
        ready,
        drain: false,
        age_ms: 0,
        active: 0,
        limit: 100,
    }
}
#[test]
fn balancer_excludes_stale_drained_full_and_saturated_nodes() {
    let mut stale = node("stale", 0.0, true);
    stale.age_ms = 15000;
    let mut drained = node("drain", 0.0, true);
    drained.drain = true;
    let mut full = node("full", 0.0, true);
    full.active = 100;
    let busy = node("busy", 0.99, true);
    let cached = node("cached", 0.3, true);
    let free = node("free", 0.1, false);
    assert_eq!(
        select(&[stale, drained, full, busy, cached, free], 0.02).unwrap(),
        "cached"
    );
    assert_eq!(select(&[node("saturated", 0.99, true)], 0.02), None);
}
#[tokio::test]
async fn admission_rejects_unknown_warmup_metrics() {
    use axum::{body::Body, http::Request};
    use flussonix::server::{App, Options, router};
    use serde_json::json;
    use tower::ServiceExt;
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("c.json"),
        d.path().join("media"),
        Options {
            admin_password: "test-admin".into(),
            peer_key: "test-peer-secret".into(),
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
    let response = router(app)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/flussonix/api/v1/admit")
                .header("X-Flussonix-Peer", "test-peer-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name":"owned","bitrate_mbps":2}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        503,
        "unsampled NIC and CPU cannot admit a viewer as zero load"
    );
}

#[test]
fn private_source_urls_preserve_prefix_secure_scheme_and_encoded_stream_names() {
    use flussonix::cluster::source_input_url;
    assert_eq!(
        source_input_url(
            "https://origin.example/media/?routing=owned",
            "region/news",
            "m4s"
        )
        .unwrap(),
        "m4ss://origin.example/media/region/news?routing=owned"
    );
    assert_eq!(
        source_input_url("http://origin.example/prefix", "live/café HD", "m4f").unwrap(),
        "m4f://origin.example/prefix/live/caf%C3%A9%20HD"
    );
    assert_eq!(
        source_input_url("https://origin.example", "one", "hls").unwrap(),
        "hlss://origin.example/one/index.m3u8"
    );
    assert!(source_input_url("file:///etc", "one", "m4f").is_err());
    assert!(source_input_url("http://origin.example", "../bad", "m4s").is_err());
}

#[test]
fn private_transport_stream_urls_preserve_prefix_query_and_secure_scheme() {
    use flussonix::cluster::source_input_url;
    assert_eq!(
        source_input_url(
            "http://origin.example/media/?routing=owned",
            "region/café HD",
            "mpegts"
        )
        .unwrap(),
        "tshttp://origin.example/media/region/caf%C3%A9%20HD/mpegts?routing=owned"
    );
    assert_eq!(
        source_input_url("https://origin.example", "one", "mpegts").unwrap(),
        "tshttps://origin.example/one/mpegts"
    );
}
