use axum::{body::Body, http::Request};
use flussonix::server::{App, Options, router};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower::ServiceExt;

#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod caption_fixture;

#[tokio::test]
async fn hls_input_keeps_second_video_track_available_to_selector() {
    use axum::{Router, extract::Path, http::StatusCode, routing::get as route_get};
    use bytes::Bytes;
    use std::{collections::HashMap, time::Duration};

    let fixture = tempfile::tempdir().unwrap();
    let playlist = fixture.path().join("index.m3u8");
    let source_pattern = fixture.path().join("source%d.ts");
    let caption_file = fixture.path().join("captions.ts");
    std::fs::write(&caption_file, caption_fixture::transport()).unwrap();
    let generated = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("ffmpeg")
            .kill_on_drop(true)
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=64x64:rate=10",
                "-i",
            ])
            .arg(&caption_file)
            .args([
                "-t",
                "6",
                "-map",
                "0:v:0",
                "-map",
                "1:v:0",
                "-map",
                "1:a:0",
                "-c:v:0",
                "libx264",
                "-c:v:1",
                "copy",
                "-threads",
                "1",
                "-g:v:0",
                "10",
                "-sc_threshold",
                "0",
                "-c:a",
                "copy",
                "-f",
                "segment",
                "-segment_time",
                "1",
                "-segment_list",
            ])
            .arg(&playlist)
            .args(["-segment_list_type", "m3u8", "-segment_list_size", "0"])
            .arg(&source_pattern)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let source_segments: Vec<u8> = (0..6)
        .flat_map(|index| std::fs::read(fixture.path().join(format!("source{index}.ts"))).unwrap())
        .collect();
    let has_live = |ts: &[u8], video: usize| {
        let path = fixture.path().join("caption-probe.ts");
        std::fs::write(&path, ts).unwrap();
        let demux = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(&path)
            .args([
                "-map",
                &format!("0:v:{video}"),
                "-c:v",
                "copy",
                "-f",
                "mpegts",
                "pipe:1",
            ])
            .output()
            .unwrap();
        assert!(
            demux.status.success(),
            "{}",
            String::from_utf8_lossy(&demux.stderr)
        );
        let mut decoder = flussonix::captions::Decoder::new(vec![flussonix::captions::Service {
            channel: 1,
            language: "en".into(),
            name: "English".into(),
            ocr_language: None,
        }]);
        flussonix::caption_transport::Transport::default().push(&demux.stdout, &mut decoder);
        decoder
            .snapshot()
            .iter()
            .any(|cue| cue.text.contains("LIVE"))
    };
    assert!(
        has_live(&source_segments, 1),
        "source video 2 lost its fixture captions"
    );
    let mut files = HashMap::new();
    files.insert(
        "index.m3u8".to_owned(),
        Bytes::from(
            std::fs::read_to_string(&playlist)
                .unwrap()
                .replace("#EXT-X-ENDLIST\n", ""),
        ),
    );
    for index in 0..6 {
        let name = format!("source{index}.ts");
        files.insert(
            name.clone(),
            Bytes::from(std::fs::read(fixture.path().join(name)).unwrap()),
        );
    }
    let files = Arc::new(files);
    let router = Router::new().route(
        "/{file}",
        route_get(move |Path(file): Path<String>| {
            let files = files.clone();
            async move {
                files
                    .get(&file)
                    .cloned()
                    .map_or((StatusCode::NOT_FOUND, Bytes::new()), |body| {
                        (StatusCode::OK, body)
                    })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let input = format!("hls://{}/index.m3u8", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let app = App::new(
        fixture.path().join("config.json"),
        fixture.path().join("media"),
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
            json!({"inputs":[{"url":input}],"flussonix_hls_subtitles":"drop"}),
        )
        .unwrap();
    let selected = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (status, body) = get(&app, "/owned/tracks-v2a1/mono.m3u8").await;
            let playlist = String::from_utf8_lossy(&body);
            if status == 200 && playlist.matches("#EXTINF:").count() >= 1 {
                let mut selected = Vec::new();
                let mut ordinary = Vec::new();
                let mut raw = Vec::new();
                let mut complete = true;
                for segment in playlist.lines().filter(|line| line.ends_with(".ts")) {
                    let (selected_status, selected_bytes) =
                        get(&app, &format!("/owned/tracks-v2a1/{segment}")).await;
                    let (ordinary_status, ordinary_bytes) =
                        get(&app, &format!("/owned/{segment}")).await;
                    if selected_status != 200 || ordinary_status != 200 {
                        complete = false;
                        break;
                    }
                    selected.extend(selected_bytes);
                    ordinary.extend(ordinary_bytes);
                    raw.extend(
                        std::fs::read(
                            fixture
                                .path()
                                .join("media")
                                .join(format!("{:x}", Sha256::digest(b"owned")))
                                .join(segment),
                        )
                        .unwrap(),
                    );
                }
                if complete && has_live(&raw, 1) {
                    break (selected, ordinary);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    app.media.stop_all().await;
    server.abort();
    let (bytes, ordinary) = selected.expect("second HLS source video track was not selectable");
    assert!(
        !has_live(&bytes, 0),
        "selected video 2 leaked embedded captions"
    );
    assert!(
        !has_live(&ordinary, 1),
        "ordinary HLS leaked video 2 embedded captions"
    );
    let output = fixture.path().join("second-video.ts");
    std::fs::write(&output, bytes).unwrap();
    let probe = tokio::process::Command::new("ffprobe")
        .kill_on_drop(true)
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type,width,height",
            "-of",
            "json",
        ])
        .arg(&output)
        .output()
        .await
        .unwrap();
    assert!(
        probe.status.success(),
        "{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    let streams: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
    let video: Vec<_> = streams["streams"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["codec_type"] == "video")
        .collect();
    assert_eq!(video.len(), 1, "{streams}");
    assert_eq!(
        (video[0]["width"].as_i64(), video[0]["height"].as_i64()),
        (Some(64), Some(48)),
        "selected first video instead of second: {streams}"
    );
}

#[tokio::test]
async fn hls_input_keeps_second_audio_track_available_to_selector() {
    use axum::{Router, extract::Path, http::StatusCode, routing::get as route_get};
    use bytes::Bytes;
    use std::{collections::HashMap, time::Duration};

    let fixture = tempfile::tempdir().unwrap();
    let playlist = fixture.path().join("index.m3u8");
    let pattern = fixture.path().join("source%d.ts");
    let generated = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("ffmpeg")
            .kill_on_drop(true)
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=64x64:rate=10",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=48000",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=880:sample_rate=48000",
                "-t",
                "6",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                "-map",
                "2:a:0",
                "-c:v",
                "libx264",
                "-threads",
                "1",
                "-g",
                "10",
                "-sc_threshold",
                "0",
                "-c:a",
                "aac",
                "-f",
                "hls",
                "-hls_time",
                "1",
                "-hls_list_size",
                "0",
                "-hls_segment_filename",
            ])
            .arg(&pattern)
            .arg(&playlist)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let mut files = HashMap::new();
    files.insert(
        "index.m3u8".to_owned(),
        Bytes::from(
            std::fs::read_to_string(&playlist)
                .unwrap()
                .replace("#EXT-X-ENDLIST\n", ""),
        ),
    );
    for index in 0..6 {
        let name = format!("source{index}.ts");
        files.insert(
            name.clone(),
            Bytes::from(std::fs::read(fixture.path().join(name)).unwrap()),
        );
    }
    let source = Arc::new(files);
    let router = Router::new().route(
        "/{file}",
        route_get(move |Path(file): Path<String>| {
            let source = source.clone();
            async move {
                source
                    .get(&file)
                    .cloned()
                    .map_or((StatusCode::NOT_FOUND, Bytes::new()), |body| {
                        (StatusCode::OK, body)
                    })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let input = format!("hls://{}/index.m3u8", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let app = App::new(
        fixture.path().join("config.json"),
        fixture.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Options::default()
        },
    )
    .unwrap();
    app.config
        .put("streams", "owned", json!({"inputs":[{"url":input}]}))
        .unwrap();

    let selected = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let (status, body) = get(&app, "/owned/tracks-v1a2/mono.m3u8").await;
            if status == 200 {
                let segment = uri(&String::from_utf8_lossy(&body)).to_owned();
                let (segment_status, bytes) =
                    get(&app, &format!("/owned/tracks-v1a2/{segment}")).await;
                if segment_status == 200 {
                    break bytes;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    app.media.stop_all().await;
    server.abort();
    let bytes = selected.expect("second HLS source audio track was not selectable");
    let output = fixture.path().join("second-audio.ts");
    std::fs::write(&output, bytes).unwrap();
    let probe = tokio::process::Command::new("ffprobe")
        .kill_on_drop(true)
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type",
            "-of",
            "json",
        ])
        .arg(&output)
        .output()
        .await
        .unwrap();
    assert!(
        probe.status.success(),
        "{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    let streams: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
    assert_eq!(
        streams["streams"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["codec_type"] == "video")
            .count(),
        1
    );
    assert_eq!(
        streams["streams"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["codec_type"] == "audio")
            .count(),
        1
    );
    let decoded = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new("ffmpeg")
            .kill_on_drop(true)
            .args(["-v", "error", "-i"])
            .arg(&output)
            .args([
                "-map", "0:a:0", "-t", "1", "-ac", "1", "-ar", "48000", "-f", "s16le", "pipe:1",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        decoded.status.success(),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    let samples: Vec<_> = decoded
        .stdout
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]) as f64)
        .collect();
    assert!(
        samples.len() >= 24000,
        "selected segment has too little decoded audio"
    );
    let tone = |hz: f64| {
        let (sin, cos) = samples
            .iter()
            .enumerate()
            .fold((0.0, 0.0), |(sin, cos), (n, sample)| {
                let phase = 2.0 * std::f64::consts::PI * hz * n as f64 / 48000.0;
                (sin + sample * phase.sin(), cos + sample * phase.cos())
            });
        sin.hypot(cos)
    };
    assert!(
        tone(880.0) > 3.0 * tone(440.0),
        "selector returned the first HLS audio track"
    );
}

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
