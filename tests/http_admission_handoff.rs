use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

async fn request(app: &Arc<App>, uri: &str, body: Option<Value>) -> axum::response::Response {
    let request = if let Some(body) = body {
        Request::post(uri)
            .header("X-Flussonix-Peer", "owned-peer-secret")
            .header("Content-Type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    } else {
        Request::get(uri).body(Body::empty()).unwrap()
    };
    router(app.clone()).oneshot(request).await.unwrap()
}
async fn setup(limit: u64, uplink: f64) -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            client_limit: limit,
            uplink_mbps: uplink,
            uplink_interface: "process".into(),
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put("streams", "owned", json!({"static":false}))
        .unwrap();
    app.sample_metrics();
    tokio::time::sleep(Duration::from_millis(100)).await;
    app.sample_metrics();
    (dir, app)
}
async fn reserve(app: &Arc<App>, cost: f64) -> axum::response::Response {
    request(
        app,
        "/flussonix/api/v1/admit",
        Some(json!({"name":"owned", "bitrate_mbps":cost})),
    )
    .await
}
async fn ticket(app: &Arc<App>, cost: f64) -> String {
    let response = reserve(app, cost).await;
    assert_eq!(response.status(), 200);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16384).await.unwrap()).unwrap();
    body["ticket"].as_str().unwrap().to_owned()
}

// A redirected ticket must retain its promised slot while the client follows Location.
#[tokio::test]
async fn clean_redirect_gap_does_not_release_promised_session_capacity() {
    let (_dir, app) = setup(1, 1000.0).await;
    let t = ticket(&app, 2.0).await;
    let response = request(
        &app,
        &format!("/owned/mpegts?token=viewer&flussonix_ticket={t}"),
        None,
    )
    .await;
    assert_eq!(response.status(), 302);
    assert_eq!(response.headers()["location"], "/owned/mpegts?token=viewer");
    assert_eq!(
        reserve(&app, 2.0).await.status(),
        503,
        "another ticket stole the redirected viewer's slot"
    );
    assert_eq!(
        app.media.count().await,
        0,
        "a control redirect must not start media"
    );
}

// Measured egress is still zero until the clean playback request starts.
#[tokio::test]
async fn clean_redirect_gap_does_not_release_promised_bandwidth() {
    let (_dir, app) = setup(100, 100.0).await;
    let t = ticket(&app, 50.0).await;
    assert_eq!(
        request(
            &app,
            &format!("/owned/mpegts?token=viewer&flussonix_ticket={t}"),
            None
        )
        .await
        .status(),
        302
    );
    assert_eq!(
        reserve(&app, 50.0).await.status(),
        503,
        "unmeasured redirected output lost its bandwidth credit"
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn direct_playback_cannot_steal_an_unredeemed_http_ticket_slot() {
    let (_dir, app) = setup(1, 1000.0).await;
    let _t = ticket(&app, 2.0).await;
    let response = request(&app, "/owned/mpegts?token=intruder", None).await;
    assert_eq!(
        response.status(),
        403,
        "pending admission must protect its promised slot before redemption"
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn promised_viewer_still_starts_after_competing_direct_playback_is_denied() {
    use futures_util::{FutureExt, StreamExt};
    let (_dir, app) = setup(1, 1000.0).await;
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let t = ticket(&app, 2.0).await;
        assert_eq!(
            request(&app, "/owned/mpegts?token=other", None)
                .await
                .status(),
            403
        );
        let redirect = request(
            &app,
            &format!("/owned/mpegts?token=viewer&flussonix_ticket={t}"),
            None,
        )
        .await;
        assert_eq!(redirect.status(), 302);
        assert_eq!(
            request(&app, "/owned/mpegts?token=other", None)
                .await
                .status(),
            403
        );
        let media = request(&app, redirect.headers()["location"].to_str().unwrap(), None).await;
        assert_eq!(media.status(), 200);
        let mut body = media.into_body().into_data_stream();
        let bytes = tokio::time::timeout(Duration::from_secs(10), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!bytes.is_empty(), "reserved viewer received no media");
        assert_eq!(app.playback_auth.active(), 1);
        assert_eq!(app.media.count().await, 1);
        drop(body);
    })
    .catch_unwind()
    .await;
    app.media.stop_all().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn abandoned_redirect_expires_without_management_requests() {
    let (_dir, app) = setup(1, 1000.0).await;
    let t = ticket(&app, 2.0).await;
    assert_eq!(
        request(
            &app,
            &format!("/owned/mpegts?token=viewer&flussonix_ticket={t}"),
            None
        )
        .await
        .status(),
        302
    );
    // Only the daemon's periodic sampler runs; no API read or new playback drives expiry.
    for _ in 0..22 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        app.sample_metrics();
    }
    assert_eq!(
        app.playback_auth.active(),
        0,
        "abandoned control admission survived its deadline"
    );
    assert_eq!(app.media.count().await, 0);
    assert_eq!(reserve(&app, 2.0).await.status(), 200);
}

#[tokio::test]
async fn unredeemed_ticket_expiry_does_not_block_direct_playback_authorization() {
    let (_dir, app) = setup(1, 1000.0).await;
    let _t = ticket(&app, 2.0).await;
    tokio::time::sleep(Duration::from_millis(5100)).await;
    // No sampler or admission endpoint prunes the ticket ledger before this request.
    // This stream intentionally has no input: passing authorization reaches startup (503).
    assert_eq!(
        request(&app, "/owned/mpegts?token=viewer", None)
            .await
            .status(),
        503
    );
    assert_eq!(app.playback_auth.active(), 1);
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn ticket_is_not_viewer_authority_and_failed_credentials_do_not_consume_it() {
    use sha2::{Digest, Sha256};
    let (_dir, app) = setup(1, 1000.0).await;
    app.config
        .put(
            "streams",
            "owned",
            json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}),
        )
        .unwrap();
    let t = ticket(&app, 2.0).await;
    assert_eq!(
        request(
            &app,
            &format!("/owned/mpegts?token=wrong&flussonix_ticket={t}"),
            None
        )
        .await
        .status(),
        403
    );
    assert_eq!(app.playback_auth.active(), 0);
    assert_eq!(
        request(
            &app,
            &format!("/owned/mpegts?token=viewer&flussonix_ticket={t}"),
            None
        )
        .await
        .status(),
        302
    );
    assert_eq!(
        request(
            &app,
            &format!("/owned/mpegts?token=viewer&flussonix_ticket={t}"),
            None
        )
        .await
        .status(),
        503,
        "redeemed ticket was replayable"
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn clean_hls_playlist_keeps_bandwidth_until_media_is_represented() {
    use futures_util::FutureExt;
    let (_dir, app) = setup(100, 100.0).await;
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let t = ticket(&app, 50.0).await;
        let redirect = request(
            &app,
            &format!("/owned/index.m3u8?token=viewer&flussonix_ticket={t}"),
            None,
        )
        .await;
        assert_eq!(redirect.status(), 302);
        let response = request(&app, redirect.headers()["location"].to_str().unwrap(), None).await;
        assert_eq!(response.status(), 200);
        let playlist = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(playlist.starts_with(b"#EXTM3U"));
        app.sample_metrics(); // A small manifest does not represent the admitted stream bitrate.
        assert_eq!(
            reserve(&app, 50.0).await.status(),
            503,
            "finite manifest response discarded the stream's bandwidth credit"
        );
    })
    .catch_unwind()
    .await;
    app.media.stop_all().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn m4f_signals_and_finite_media_share_measured_bandwidth_credit() {
    use futures_util::{FutureExt, StreamExt};
    let (_dir, app) = setup(100, 100.0).await;
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let t = ticket(&app, 50.0).await;
        let redirect = request(
            &app,
            &format!("/owned/m4f?token=viewer&flussonix_ticket={t}"),
            None,
        )
        .await;
        assert_eq!(redirect.status(), 302);
        let response = request(&app, redirect.headers()["location"].to_str().unwrap(), None).await;
        assert_eq!(response.status(), 200);
        let mut signal = response.into_body().into_data_stream();
        let line = tokio::time::timeout(Duration::from_secs(10), signal.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = std::str::from_utf8(&line).unwrap();
        let stamp = text
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        app.sample_metrics();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let response = request(&app, &format!("/owned/{stamp}.m4f?token=viewer"), None).await;
        assert_eq!(response.status(), 200);
        let media = to_bytes(response.into_body(), 10 * 1024 * 1024)
            .await
            .unwrap();
        assert!(media.len() > 10000);
        app.sample_metrics();
        let response = router(app.clone())
            .oneshot(
                Request::get("/flussonix/api/v1/node")
                    .header("X-Flussonix-Peer", "owned-peer-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let n: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                .unwrap();
        let measured = n["http_egress_mbps"].as_f64().unwrap();
        let reserved = n["reserved_mbps"].as_f64().unwrap();
        assert!(measured > 0.1);
        assert!(
            (reserved - (50.0 - measured).max(0.0)).abs() < 0.000001,
            "M4F media was charged twice: measured={measured} reserved={reserved}"
        );
        drop(signal);
    })
    .catch_unwind()
    .await;
    app.media.stop_all().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
