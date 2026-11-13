//! A late HTTP TS subscriber must not receive undecodable pre-keyframe video.
use axum::{body::Body, http::Request};
use flussonix::{
    m4s,
    server::{App, Options, router},
};
use futures_util::{FutureExt, StreamExt};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

async fn capture(app: &Arc<App>, codec: &str) -> Result<Vec<u8>, String> {
    let cfg = app
        .config
        .effective("owned")
        .ok_or("missing owned stream")?;
    let worker = app.media.ensure("owned", &cfg).await?;
    let (initial, mut frames) = worker.m4s_subscribe().ok_or("no native subscription")?;
    let mut decoder = m4s::Decoder::default();
    let mut video = None;
    for record in initial {
        for event in decoder.push(&record)? {
            if let m4s::Event::Info { tracks, .. } = event {
                video = tracks.iter().find(|t| t.codec == codec).map(|t| t.id);
            }
        }
    }
    // Observe a fresh keyframe and then five predicted frames. The HTTP request
    // therefore joins a running worker between IDRs, rather than relying on sleep.
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut since_key = None;
        loop {
            let record = frames.recv().await.map_err(|e| e.to_string())?;
            for event in decoder.push(&record)? {
                match event {
                    m4s::Event::Info { tracks, .. } => {
                        video = tracks.iter().find(|t| t.codec == codec).map(|t| t.id);
                    }
                    m4s::Event::Frame { track_id, key, .. } if Some(track_id) == video => {
                        if key {
                            since_key = Some(0);
                        } else if let Some(count) = &mut since_key {
                            *count += 1;
                            if *count == 5 {
                                return Ok::<_, String>(());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    })
    .await
    .map_err(|_| "could not observe warm non-IDR phase".to_owned())??;
    drop(frames);
    let response = router(app.clone())
        .oneshot(Request::get("/owned/mpegts").body(Body::empty()).unwrap())
        .await
        .map_err(|e| e.to_string())?;
    if response.status() != 200 {
        return Err(format!("HTTP {}", response.status()));
    }
    let mut body = response.into_body().into_data_stream();
    let mut bytes = vec![];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, body.next()).await {
            Err(_) => break,
            Ok(Some(Ok(chunk))) => {
                bytes.extend_from_slice(&chunk);
                if bytes.len() > 8 * 1024 * 1024 {
                    return Err("capture bound exceeded".into());
                }
            }
            other => return Err(format!("TS body ended: {other:?}")),
        }
    }
    if bytes.len() < 188 * 10 {
        return Err("no usable TS output".into());
    }
    Ok(bytes)
}

async fn warm(encoder: &str, codec: &str) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            ffmpeg: "/usr/bin/ffmpeg".into(),
            admin_password: "owned-admin".into(),
            peer_key: "owned-ts-startup".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,
                "inputs":[{"url":"testsrc://"}],
                "transcoder":{"encoder":encoder,"vb":1000,"acodec":"aac","ab":96}
            }),
        )
        .unwrap();
    let captured = std::panic::AssertUnwindSafe(capture(&app, codec))
        .catch_unwind()
        .await;
    app.media.stop_all().await;
    let bytes = captured.expect("capture panicked").expect("capture failed");
    let path = dir.path().join("warm.ts");
    std::fs::write(&path, &bytes).unwrap();
    if let Some(destination) = std::env::var_os("FLUSSONIX_TS_STARTUP_ARTIFACT_DIR") {
        let destination = std::path::PathBuf::from(destination);
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::write(destination.join(format!("{encoder}-warm.ts")), &bytes).unwrap();
    }
    let mut command = tokio::process::Command::new("/usr/bin/ffmpeg");
    command
        .kill_on_drop(true)
        .args(["-v", "error", "-nostdin", "-i"])
        .arg(&path)
        .args(["-map", "0:v:0", "-t", "1", "-f", "null", "-"]);
    let output = tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .expect("bounded decoder deadline")
        .expect("start independent decoder");
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "late TS must decode without missing headers or frames: exit={:?}, stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn warm_http_ts_starts_with_decodable_video() {
    warm("libx264", "h264").await;
}

#[tokio::test]
async fn warm_hevc_http_ts_starts_with_decodable_video() {
    warm("libx265", "hevc").await;
}

#[tokio::test]
#[ignore = "requires a separately qualified NVIDIA H264 encoder"]
async fn warm_nvidia_http_ts_starts_with_decodable_video() {
    warm("h264_nvenc", "h264").await;
}
