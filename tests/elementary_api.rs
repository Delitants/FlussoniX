use axum::{body::Body, http::Request};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;
async fn get(a: Arc<App>, auth: bool, query: &str) -> (u16, String) {
    let mut r = Request::builder().uri(format!("/flussonix/api/v1/rtp-sdp/owned{query}"));
    if auth {
        r = r.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("admin:owned")),
        );
    }
    let response = router(a)
        .oneshot(r.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}
#[tokio::test]
async fn sdp_is_management_authenticated_and_describes_only_running_elementary_destination() {
    qualify(false).await;
}
#[tokio::test]
async fn secure_sdp_is_authenticated_fenced_and_contains_no_key_or_key_path() {
    qualify(true).await;
}
async fn qualify(secure: bool) {
    let d = tempfile::tempdir().unwrap();
    let key = d.path().join("owned.key");
    std::fs::write(&key, STANDARD.encode([0x31; 30])).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let scheme = if secure { "srtp" } else { "rtp" };
    let transport = if secure { "RTP/SAVP" } else { "RTP/AVP" };
    let a = App::new(
        d.path().join("c.json"),
        d.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "owned".into(),
            peer_key: "owned-peer-key".into(),
            ffmpeg: "/usr/bin/ffmpeg".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let (port, _receivers) = loop {
        let first = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        if port > 65516 {
            continue;
        }
        let mut sockets = vec![first];
        for p in port + 1..port + 4 {
            if let Ok(s) = std::net::UdpSocket::bind(("127.0.0.1", p)) {
                sockets.push(s);
            } else {
                break;
            }
        }
        if sockets.len() == 4 {
            break (port, sockets);
        }
    };
    let mut options = json!({"profile":"elementary"});
    if secure {
        options["key_file"] = json!(key);
    }
    let cfg = json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("{scheme}://127.0.0.1:{port}"),"flussonix_rtp":options}]});
    a.config.put("streams", "owned", cfg.clone()).unwrap();
    assert_eq!(get(a.clone(), false, "").await.0, 401);
    assert_ne!(get(a.clone(), true, "").await.0, 200);
    assert_eq!(
        a.media.count().await,
        0,
        "SDP request must not start sources"
    );
    let w = a.media.ensure("owned", &cfg).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while w.stats()["flussonix_rtp_outputs"][0]["sdp_ready"] != true {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("SDP readiness: {}", w.stats()));
    let (status, text) = get(a.clone(), true, "?destination=0").await;
    // Reconciliation is asynchronous. Never return the previous destination's
    // SDP as if it described a newly saved target during that interval.
    a.config.put("streams", "owned",json!({"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{}",port+4),"flussonix_rtp":{"profile":"elementary"}}]})).unwrap();
    let changed = get(a.clone(), true, "").await.0;
    a.media.stop_all().await;
    assert_eq!(status, 200, "{text}");
    assert!(text.starts_with("v=0\r\n"));
    assert!(text.contains(&format!("m=video {port} {transport} 96")));
    assert!(text.contains(&format!("m=audio {} {transport} 97", port + 2)));
    assert!(text.contains("MPEG4-GENERIC/48000/2"));
    assert!(!text.contains("token="));
    assert!(!text.contains("crypto:"));
    assert!(!text.contains(key.to_str().unwrap()));
    assert!(!text.contains(&STANDARD.encode([0x31; 30])));
    assert_ne!(get(a.clone(), true, "").await.0, 200);
    assert_ne!(
        changed, 200,
        "A new saved target requires its own running worker SDP"
    );
}
