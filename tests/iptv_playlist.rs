use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

fn app(origin: Option<&str>) -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "local-admin-secret".into(),
            peer_key: "local-peer-secret".into(),
            iptv_public_origin: origin.map(|value| url::Url::parse(value).unwrap()),
            ..Options::default()
        },
    )
    .unwrap();
    (dir, app)
}

async fn playlist(
    app: Arc<App>,
    path: &str,
    hostile_host: bool,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut request = Request::builder().uri(path);
    if hostile_host {
        request = request
            .header("Host", "evil.example")
            .header("X-Forwarded-Host", "evil.example")
            .header("Forwarded", "host=evil.example;proto=http");
    }
    let response = router(app)
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 128_000)
        .await
        .unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

fn configure(app: &Arc<App>) {
    app.config
        .put(
            "streams",
            "region/news",
            json!({"title":"Region News","inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    app.config
        .put(
            "streams",
            "music",
            json!({"title":"Music","inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    app.config
        .put(
            "streams",
            "off",
            json!({"disabled":true,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package(
            "regional",
            json!({"channels":["region/news","off","staged/unknown"]}),
        )
        .unwrap();
    app.iptv
        .put_package("favorites", json!({"channels":["music","region/news"]}))
        .unwrap();
    app.iptv
        .put_subscriber(
            "alice",
            json!({"token":"a+b#1","packages":["regional","favorites"]}),
        )
        .unwrap();
    app.iptv
        .put_subscriber(
            "bob",
            json!({"token":"bob-secret","packages":["favorites"]}),
        )
        .unwrap();
}

#[tokio::test]
async fn playlist_requires_the_named_subscribers_current_token() {
    let (_dir, app) = app(Some("https://streams.example"));
    configure(&app);
    for path in [
        "/tv/playlists/alice",
        "/tv/playlists/alice?token=wrong",
        "/tv/playlists/alice?token=bob-secret",
    ] {
        assert_eq!(
            playlist(app.clone(), path, false).await.0,
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        playlist(app.clone(), "/tv/playlists/missing?token=a%2Bb%231", false)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        playlist(app.clone(), "/tv/playlists/alice?token=a%2Bb%231", false)
            .await
            .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn expired_or_disabled_subscribers_cannot_read_playlists() {
    let (_dir, app) = app(Some("https://streams.example"));
    configure(&app);
    app.iptv.put_subscriber("expired", json!({"token":"old-token","packages":["regional"],"flussonix_expires_at":"2000-01-01T00:00:00Z"})).unwrap();
    assert_eq!(
        playlist(app.clone(), "/tv/playlists/expired?token=old-token", false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    app.iptv.set_enabled(false).unwrap();
    assert_eq!(
        playlist(app.clone(), "/tv/playlists/alice?token=a%2Bb%231", false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn playlist_uses_fixed_origin_and_only_sorted_playable_entitlements() {
    let (_dir, app) = app(Some("https://streams.example"));
    configure(&app);
    assert!(app.playback_auth.snapshots().is_empty());
    let (status, headers, body) =
        playlist(app.clone(), "/tv/playlists/alice?token=a%2Bb%231", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "application/vnd.apple.mpegurl");
    assert!(
        headers["cache-control"]
            .to_str()
            .unwrap()
            .contains("no-store")
    );
    assert_eq!(
        body,
        "#EXTM3U\n#EXTINF:-1,Music\nhttps://streams.example/music/index.m3u8?token=a%2Bb%231\n#EXTINF:-1,Region News\nhttps://streams.example/region%2Fnews/index.m3u8?token=a%2Bb%231\n"
    );
    assert!(!body.contains("evil.example"));
    assert!(!body.contains("staged/unknown"));
    assert!(!body.contains("/off/"));
    assert!(app.playback_auth.snapshots().is_empty());
}

#[tokio::test]
async fn hostile_stream_title_cannot_inject_an_extra_m3u_entry() {
    let (_dir, app) = app(Some("https://streams.example"));
    configure(&app);
    app.config
        .put(
            "streams",
            "music",
            json!({"title":"Music\r\n#EXTINF:-1,Injected"}),
        )
        .unwrap();
    let (status, _, body) =
        playlist(app.clone(), "/tv/playlists/alice?token=a%2Bb%231", false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.lines()
            .filter(|line| line.starts_with("#EXTINF:"))
            .count(),
        2
    );
    assert!(!body.contains("\n#EXTINF:-1,Injected"));
    assert!(!body.contains('\r'));
}

#[tokio::test]
async fn missing_public_origin_returns_clear_error_and_status_advertises_configured_origin() {
    let (_dir, missing_origin_app) = app(None);
    configure(&missing_origin_app);
    let (status, _, body) = playlist(
        missing_origin_app,
        "/tv/playlists/alice?token=a%2Bb%231",
        false,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("public origin"));
    let (_dir, app) = app(Some("https://streams.example"));
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config/stats")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:local-admin-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["iptv_public_origin"],
        "https://streams.example/"
    );
}

#[test]
fn public_origin_must_be_a_plain_http_or_https_origin() {
    for invalid in [
        "ftp://streams.example",
        "https://user:secret@streams.example",
        "https://streams.example/subpath",
        "https://streams.example/?token=secret",
        "https://streams.example/#fragment",
    ] {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            App::new(
                dir.path().join("config.json"),
                dir.path().join("media"),
                Options {
                    admin_password: "local-admin-secret".into(),
                    peer_key: "local-peer-secret".into(),
                    iptv_public_origin: Some(url::Url::parse(invalid).unwrap()),
                    ..Options::default()
                }
            )
            .is_err(),
            "{invalid}"
        );
    }
}
