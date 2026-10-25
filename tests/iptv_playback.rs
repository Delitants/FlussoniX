use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::{
    playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest},
    server::{App, Options, router},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower::ServiceExt;

fn app() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "admin-secret-123".into(),
            peer_key: "peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for name in ["news", "sports"] {
        app.config
            .put(
                "streams",
                name,
                json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
            )
            .unwrap();
    }
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("basic", json!({"channels":["news","sports"]}))
        .unwrap();
    app.iptv
        .put_subscriber(
            "alice",
            json!({"token":"alice-private-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    (dir, app)
}

async fn media(app: &Arc<App>, path: &str) -> axum::response::Response {
    router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn hls_and_ts_require_current_subscriber_entitlement() {
    let (_dir, app) = app();
    for path in [
        "/news/index.m3u8",
        "/news/mpegts?token=wrong",
        "/sports/index.m3u8?token=wrong",
    ] {
        let response = media(&app, path).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
        let body = axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("alice-private-token"));
    }
    let hls = media(&app, "/news/index.m3u8?token=alice-private-token").await;
    assert_eq!(hls.status(), StatusCode::OK);
    drop(hls);
    app.media.stop_all().await;
}

#[tokio::test]
async fn cdn_never_accepts_its_local_subscriber_file_as_cluster_authority() {
    let (_dir, app) = app();
    // The fixture has a valid local subscriber, as a stale CDN copy might.
    // Changing its role must not turn that copy into a cluster grant.
    let dir = tempfile::tempdir().unwrap();
    let cdn = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "admin-secret-123".into(),
            peer_key: "peer-secret-1234".into(),
            role: "cdn".into(),
            iptv_data: Some(dir.path().join("iptv.json")),
            ..Default::default()
        },
    )
    .unwrap();
    cdn.config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    cdn.iptv.set_enabled(true).unwrap();
    cdn.iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    cdn.iptv
        .put_subscriber(
            "alice",
            json!({"token":"alice-private-token","packages":["basic"]}),
        )
        .unwrap();
    assert!(
        app.iptv
            .snapshot()
            .authorize("alice-private-token", "news", std::time::SystemTime::now())
            .is_some()
    );
    assert_eq!(
        media(&cdn, "/news/index.m3u8?token=alice-private-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    cdn.media.stop_all().await;
}

#[tokio::test]
async fn standalone_with_remote_authority_never_falls_back_to_local_subscriber_file() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "admin-secret-123".into(),
            peer_key: "peer-secret-1234".into(),
            iptv_authority: Some(url::Url::parse("https://authority.example/").unwrap()),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    app.iptv
        .put_subscriber(
            "alice",
            json!({"token":"alice-private-token","packages":["basic"]}),
        )
        .unwrap();
    assert_eq!(
        media(&app, "/news/index.m3u8?token=alice-private-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    app.media.stop_all().await;
}

#[tokio::test]
async fn subscriber_limits_span_streams_and_session_views_hide_token() {
    let (_dir, app) = app();
    let first = media(&app, "/news/mpegts?token=alice-private-token").await;
    assert_eq!(first.status(), StatusCode::OK);
    let second = media(&app, "/sports/mpegts?token=alice-private-token").await;
    assert_eq!(second.status(), StatusCode::FORBIDDEN);
    let sessions = serde_json::to_string(&app.playback_auth.snapshots()).unwrap();
    assert!(!sessions.contains("alice-private-token"));
    let node = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/node")
                .header("X-Flussonix-Peer", "peer-secret-1234")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(node.status(), StatusCode::OK);
    let metrics = axum::body::to_bytes(node.into_body(), 1_000_000)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&metrics).contains("alice-private-token"));
    drop(first);
    app.media.stop_all().await;
}

#[tokio::test]
async fn admin_basic_credentials_do_not_act_as_subscriber_token() {
    let (_dir, app) = app();
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/news/mpegts")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:admin-secret-123")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn disabled_expired_and_foreign_channel_are_denied() {
    let (_dir, app) = app();
    app.iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    assert_eq!(
        media(&app, "/sports/mpegts?token=alice-private-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    app.iptv
        .put_subscriber(
            "alice",
            json!({"flussonix_expires_at":"2000-01-01T00:00:00Z"}),
        )
        .unwrap();
    assert_eq!(
        media(&app, "/news/mpegts?token=alice-private-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
    app.iptv
        .put_subscriber(
            "alice",
            json!({"flussonix_expires_at":null,"packages":["missing"]}),
        )
        .unwrap_err();
    app.iptv.set_enabled(false).unwrap();
    assert_eq!(
        media(&app, "/news/mpegts?token=alice-private-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn exact_iptv_policy_and_additional_hash_guard() {
    for on_play in [json!("iptv://"), json!({"url":"iptv://"})] {
        assert!(Policy::from_config(&json!({"on_play":on_play}), &json!({})).is_ok());
    }
    for on_play in [
        json!("iptv://anything"),
        json!({"url":"iptv://x"}),
        json!({"url":"iptv://","session_keys":["name","proto"]}),
        json!({"url":"iptv://","max_sessions":2}),
    ] {
        assert!(Policy::from_config(&json!({"on_play":on_play}), &json!({})).is_err());
    }
    let (_dir, app) = app();
    let digest = format!("{:x}", Sha256::digest(b"different"));
    app.config
        .put("streams", "news", json!({"flussonix_token_sha256":digest}))
        .unwrap();
    assert_eq!(
        media(&app, "/news/mpegts?token=alice-private-token")
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn iptv_user_identity_is_stable_across_channels_without_raw_token() {
    let (_dir, app) = app();
    let auth = PlaybackAuth::with_iptv(10, app.iptv.clone());
    let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
    let news = auth.publish("news", Some(policy.clone())).unwrap();
    let sports = auth.publish("sports", Some(policy)).unwrap();
    let request = |name: &str| ViewerRequest {
        name: name.into(),
        proto: "mpegts".into(),
        token: "alice-private-token".into(),
        ..Default::default()
    };
    let first = auth.authorize(news, request("news")).await;
    assert!(matches!(first, AuthOutcome::Allowed(_)));
    let second = auth.authorize(sports, request("sports")).await;
    assert!(matches!(second, AuthOutcome::Denied));
    let sessions = serde_json::to_string(&auth.snapshots()).unwrap();
    assert!(!sessions.contains("alice-private-token"));
    assert!(!sessions.contains("alice"));
    drop(first);
}

#[tokio::test]
async fn peer_inventory_preserves_local_iptv_policy_sentinel() {
    let (_dir, app) = app();
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/stream/news")
                .header("X-Flussonix-Peer", "peer-secret-1234")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["on_play"], "iptv://");
}

#[tokio::test]
async fn every_native_http_media_path_checks_subscriber_token_before_delivery() {
    let (_dir, app) = app();
    for path in [
        "/news/index.m3u8",
        "/news/mpegts",
        "/news/m4s",
        "/news/m4f",
        "/news/fmp4/index.m3u8",
        "/news/segment-1.ts",
        "/news/key",
    ] {
        let denied = media(&app, path).await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN, "{path}");
    }
    assert_eq!(app.media.count().await, 0);
}
