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

#[tokio::test]
async fn ts_ready_does_not_advertise_an_absent_fmp4_child() {
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
    let cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer-token"))});
    app.config.put("streams", "owned", cfg.clone()).unwrap();
    let publisher = app
        .media
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let output = dir
        .path()
        .join("media")
        .join(format!("{:x}", Sha256::digest(b"owned")));
    std::fs::write(output.join("index.m3u8"), "#EXTM3U\n#EXTINF:2,\nowned.ts\n").unwrap();
    std::fs::write(output.join("owned.ts"), vec![0x47; 188]).unwrap();
    let ts = get(&app, "/owned/index.ts.m3u8?token=viewer-token").await;
    let ts_status = ts.status();
    let ts_body = String::from_utf8(
        axum::body::to_bytes(ts.into_body(), 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let fmp4 = get(&app, "/owned/index.fmp4.m3u8?token=viewer-token").await;
    let fmp4_status = fmp4.status();
    drop(publisher);
    app.media.stop_all().await;
    assert_eq!(ts_status, 200, "{ts_body}");
    assert!(ts_body.contains("index.m3u8?token=viewer-token"));
    assert_eq!(
        fmp4_status, 503,
        "missing fMP4 playlist must not be advertised"
    );
}

#[tokio::test]
async fn alias_children_decode_in_both_hls_containers_with_one_worker() {
    let (dir, app) = fixture();
    let mut files = Vec::new();
    for (alias, extension) in [("index.ts.m3u8", "ts"), ("index.fmp4.m3u8", "mp4")] {
        let response = get(&app, &format!("/owned/{alias}?token=viewer-token")).await;
        assert_eq!(response.status(), 200);
        let master = String::from_utf8(
            axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let child = master
            .lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        let response = get(&app, &format!("/owned/{child}")).await;
        assert_eq!(response.status(), 200);
        let playlist = String::from_utf8(
            axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let prefix = child.rsplit_once('/').map_or("", |(prefix, _)| prefix);
        let mut data = Vec::new();
        if let Some(init) = playlist.lines().find_map(|line| {
            line.strip_prefix("#EXT-X-MAP:URI=\"")
                .and_then(|tail| tail.split_once('"').map(|(uri, _)| uri))
        }) {
            let path = format!("/owned/{prefix}/{init}");
            let response = get(&app, &path).await;
            assert_eq!(response.status(), 200, "{path}");
            data.extend_from_slice(
                &axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap(),
            );
        }
        let segment = playlist
            .lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        let path = if prefix.is_empty() {
            format!("/owned/{segment}")
        } else {
            format!("/owned/{prefix}/{segment}")
        };
        let response = get(&app, &path).await;
        assert_eq!(response.status(), 200, "{path}");
        data.extend_from_slice(
            &axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap(),
        );
        let file = dir.path().join(format!("alias-{extension}.{extension}"));
        std::fs::write(&file, data).unwrap();
        files.push(file);
    }
    let workers = app.media.count().await;
    app.media.stop_all().await;
    assert_eq!(workers, 1, "aliases must not start additional workers");
    for file in files {
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::new("ffprobe")
                .kill_on_drop(true)
                .args([
                    "-v",
                    "error",
                    "-show_entries",
                    "stream=codec_name",
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
        let streams: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(
            streams["streams"]
                .as_array()
                .unwrap()
                .iter()
                .any(|stream| stream["codec_name"] == "h264"),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}
