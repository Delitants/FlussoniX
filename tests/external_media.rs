//! Opt-in qualification against an explicitly authorized source. URLs stay in environment variables.
use flussonix::media::Engine;
use serde_json::{Value, json};
use std::time::Duration;
#[tokio::test]
#[ignore = "requires an authorized FLUSSONIX_M4S_URL"]
async fn authorized_m4s_ingest_produces_decodable_hls_and_relay() {
    let url = std::env::var("FLUSSONIX_M4S_URL").expect("authorized source URL required");
    let d = tempfile::tempdir().unwrap();
    let engine = Engine::new(d.path(), "ffmpeg");
    let worker = engine
        .ensure("external", &json!({"inputs":[{"url":url}]}))
        .await
        .unwrap();
    for _ in 0..150 {
        if engine.read("external", "index.m3u8").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let manifest = engine.read("external", "index.m3u8").await.unwrap();
    let manifest = String::from_utf8_lossy(&manifest);
    let file = manifest
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .expect("HLS segment required");
    let bytes = engine.read("external", file).await.unwrap();
    let ts = d.path().join("verified.ts");
    tokio::fs::write(&ts, bytes).await.unwrap();
    let result = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
        ])
        .arg(&ts)
        .output()
        .await
        .unwrap();
    let parsed: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert!(
        parsed["streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["codec_name"] == "h264")
    );
    assert!(
        parsed["streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["codec_name"] == "aac")
    );
    let decode = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&ts)
        .args(["-t", "1", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(decode.status.success(), "media must decode");
    let (boot, _) = worker.m4s_subscribe().unwrap();
    let mut decoder = flussonix::m4s::Decoder::default();
    let mut frames = 0;
    for b in boot {
        for event in decoder.push(&b).unwrap() {
            if matches!(event, flussonix::m4s::Event::Frame { .. }) {
                frames += 1
            }
        }
    }
    assert!(frames > 0);
    engine.stop_all().await;
}
