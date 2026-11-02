use axum::{body::Body, http::Request};
use flussonix::server::{App, Options, router};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower::ServiceExt;

fn fixture() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Options::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({
                "static":false,
                "inputs":[{"url":"testsrc://"}],
                "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer-token"))
            }),
        )
        .unwrap();
    (dir, app)
}

async fn get(app: &Arc<App>, path: &str) -> axum::response::Response {
    router(app.clone())
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn exact_ts_and_fmp4_entry_urls_return_authorized_playable_masters() {
    let (_dir, app) = fixture();
    let mut results = Vec::new();
    for (alias, segment_extension) in [("index.ts.m3u8", ".ts"), ("index.fmp4.m3u8", ".m4s")] {
        let response = get(&app, &format!("/owned/{alias}?token=viewer-token")).await;
        let status = response.status();
        let content_type = response.headers().get("content-type").cloned();
        let cache_control = response.headers().get("cache-control").cloned();
        let master = String::from_utf8(
            axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let child = (status.is_success() && master.starts_with("#EXTM3U"))
            .then(|| {
                master
                    .lines()
                    .find(|line| !line.is_empty() && !line.starts_with('#'))
                    .map(str::to_owned)
            })
            .flatten();
        let (child_status, child_body) = if let Some(child) = &child {
            let response = get(&app, &format!("/owned/{child}")).await;
            let status = response.status();
            let body = String::from_utf8(
                axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            (Some(status), body)
        } else {
            (None, String::new())
        };
        results.push((
            alias,
            segment_extension,
            status,
            content_type,
            cache_control,
            master,
            child,
            child_status,
            child_body,
        ));
    }
    app.media.stop_all().await;
    for (
        alias,
        extension,
        status,
        content_type,
        cache_control,
        master,
        child,
        child_status,
        child_body,
    ) in results
    {
        assert_eq!(status, 200, "{alias}: {master}");
        assert_eq!(content_type.unwrap(), "application/vnd.apple.mpegurl");
        assert_eq!(cache_control.unwrap(), "no-store");
        assert!(master.contains("#EXT-X-STREAM-INF:BANDWIDTH="), "{master}");
        assert!(!master.contains("#EXTINF:"), "{master}");
        assert!(child.unwrap().contains("token=viewer-token"), "{master}");
        assert_eq!(child_status.unwrap(), 200, "{alias}: {child_body}");
        assert!(child_body.contains("#EXTINF:"), "{alias}: {child_body}");
        assert!(child_body.contains(extension), "{alias}: {child_body}");
    }
}

#[tokio::test]
async fn denied_and_disabled_alias_requests_do_not_expose_a_master() {
    let (_dir, app) = fixture();
    for alias in ["index.ts.m3u8", "index.fmp4.m3u8"] {
        assert_eq!(
            get(&app, &format!("/owned/{alias}?token=wrong"))
                .await
                .status(),
            403
        );
    }
    app.config
        .put("streams", "owned", json!({"disabled":true}))
        .unwrap();
    for alias in ["index.ts.m3u8", "index.fmp4.m3u8"] {
        assert_eq!(
            get(&app, &format!("/owned/{alias}?token=viewer-token"))
                .await
                .status(),
            404
        );
    }
    assert_eq!(app.media.count().await, 0);
}
