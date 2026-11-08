use axum::{body::Body, http::Request};
use flussonix::server::{App, Options, router};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower::ServiceExt;

async fn get(app: &Arc<App>, path: &str) -> (u16, Vec<u8>) {
    let response = router(app.clone())
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

fn uri(playlist: &str) -> &str {
    playlist
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .expect("HLS child or segment")
}

#[tokio::test]
async fn exact_track_urls_authorize_each_hop_and_decode_only_selected_media() {
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
            "group/owned",
            json!({
                "static":false,
                "inputs":[{"url":"testsrc://"}],
                "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer-token"))
            }),
        )
        .unwrap();
    let denied = [
        "/group/owned/video.m3u8?token=wrong",
        "/group/owned/tracks-v1a1/mono.m3u8?token=wrong",
        "/group/owned/tracks-v1a1/g1_1.ts?token=wrong",
    ];
    let mut denial_status = Vec::new();
    for path in denied {
        denial_status.push(get(&app, path).await.0);
    }
    let (master_status, master_body) =
        get(&app, "/group/owned/video.m3u8?token=viewer-token").await;
    let master = String::from_utf8_lossy(&master_body);
    let child = uri(&master).to_owned();
    let (child_status, child_body) = get(&app, &format!("/group/owned/{child}")).await;
    let playlist = String::from_utf8_lossy(&child_body);
    let segment = uri(&playlist).to_owned();
    let mut outputs = Vec::new();
    for selector in ["v1a1", "v1", "a1"] {
        let path = format!("/group/owned/tracks-{selector}/{segment}");
        let (status, bytes) = get(&app, &path).await;
        let file = dir.path().join(format!("selected-{selector}.ts"));
        std::fs::write(&file, bytes).unwrap();
        outputs.push((selector, status, file));
    }
    let invalid_selector = get(
        &app,
        "/group/owned/tracks-v9a1/mono.m3u8?token=viewer-token",
    )
    .await
    .0;
    let cached_repeat_status = get(&app, &format!("/group/owned/tracks-v1a1/{segment}"))
        .await
        .0;
    let worker_count = app.media.count().await;
    app.config
        .put("streams", "group/owned", json!({"disabled":true}))
        .unwrap();
    let stale_cached_segment = get(&app, &format!("/group/owned/tracks-v1a1/{segment}"))
        .await
        .0;
    app.media.stop_all().await;

    assert_eq!(denial_status, [403, 403, 403]);
    assert_eq!(master_status, 200, "{master}");
    assert!(
        child.starts_with("tracks-v1a1/mono.m3u8?token=viewer-token"),
        "{master}"
    );
    assert_eq!(child_status, 200, "{playlist}");
    assert!(segment.ends_with(".ts?token=viewer-token"), "{playlist}");
    assert_eq!(invalid_selector, 404);
    assert_eq!(cached_repeat_status, 200);
    assert_eq!(worker_count, 1, "track variants must share one worker");
    assert_eq!(
        stale_cached_segment, 404,
        "cached media must not bypass disabled stream"
    );
    for (selector, status, file) in outputs {
        assert_eq!(status, 200, "{selector}");
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::new("ffprobe")
                .kill_on_drop(true)
                .args([
                    "-v",
                    "error",
                    "-show_entries",
                    "stream=codec_type",
                    "-of",
                    "json",
                ])
                .arg(&file)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let probe: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let streams = probe["streams"].as_array().unwrap();
        let video = streams
            .iter()
            .filter(|s| s["codec_type"] == "video")
            .count();
        let audio = streams
            .iter()
            .filter(|s| s["codec_type"] == "audio")
            .count();
        assert_eq!(
            video,
            usize::from(selector.contains('v')),
            "{selector}: {probe}"
        );
        assert_eq!(
            audio,
            usize::from(selector.contains('a')),
            "{selector}: {probe}"
        );
    }
}
