use flussonix::media::Engine;
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio::process::Command;

async fn camera(codec: &str, scheme: &str, udp: bool, audio: Option<&str>, expected: &str) {
    let d = tempfile::tempdir().unwrap();
    let samples = d.path().join("samples.g711");
    let encoded = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=8000",
            "-t",
            "2",
            "-ac",
            "1",
            "-c:a",
        ])
        .arg(if codec == "PCMA" {
            "pcm_alaw"
        } else {
            "pcm_mulaw"
        })
        .args(["-f", if codec == "PCMA" { "alaw" } else { "mulaw" }])
        .arg(&samples)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    let ready = d.path().join("ready");
    let proof = d.path().join("proof.json");
    let mut source = Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/rtsp_g711_camera.py"
        ))
        .args(["--codec", codec, "--samples"])
        .arg(&samples)
        .arg("--ready")
        .arg(&ready)
        .arg("--proof")
        .arg(&proof)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let media = Engine::new(d.path().join("media"), "ffmpeg");
    let result = AssertUnwindSafe(async {
        let port = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(port) = std::fs::read_to_string(&ready) { break port; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        let mut input = json!({"url":format!("{scheme}://owned-user:owned-camera-secret@127.0.0.1:{port}/camera?token=owned%3Atoken")});
        if udp { input["rtp"] = json!("udp"); }
        let mut cfg = json!({"inputs":[input]});
        if let Some(codec) = audio { cfg["transcoder"] = json!({"acodec":codec}); }
        let worker = media.ensure("owned", &cfg).await.unwrap();
        assert!(Arc::ptr_eq(&worker, &media.ensure("owned", &cfg).await.unwrap()));
        assert_eq!(worker.stats()["input_protocol"], scheme);
        let segment = tokio::time::timeout(Duration::from_secs(18), async {
            loop {
                if let Ok(index) = media.read("owned", "index.m3u8").await {
                    if let Some(name) = String::from_utf8_lossy(&index).lines().find(|s| !s.is_empty() && !s.starts_with('#')) {
                        if let Ok(bytes) = media.read("owned", name).await { break bytes; }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.unwrap_or_else(|_| panic!("G.711 camera must produce HLS: {}", worker.stats()));
        let file = d.path().join("decode.ts");
        std::fs::write(&file, segment).unwrap();
        let probe = Command::new("ffprobe").args(["-v", "error", "-show_streams", "-of", "json"])
            .arg(&file).kill_on_drop(true).output().await.unwrap();
        assert!(probe.status.success(), "{}", String::from_utf8_lossy(&probe.stderr));
        let stream: Value = serde_json::from_slice(&probe.stdout).unwrap();
        assert_eq!(stream["streams"][0]["codec_name"], expected);
        assert_eq!(stream["streams"][0]["sample_rate"], "48000");
        assert_eq!(stream["streams"][0]["channels"], 2);
        let decode = Command::new("ffmpeg").args(["-nostdin", "-v", "error", "-xerror", "-i"])
            .arg(file).args(["-map", "0:a:0", "-threads", "1", "-f", "framemd5", "-"])
            .kill_on_drop(true).output().await.unwrap();
        assert!(decode.status.success() && decode.stderr.is_empty(), "{}", String::from_utf8_lossy(&decode.stderr));
        let frames = String::from_utf8(decode.stdout).unwrap();
        assert!(frames.lines().filter(|s| !s.starts_with('#') && !s.is_empty()).count() >= 60, "{frames}");
    }).catch_unwind().await;
    // Even assertion failures stop and reap the owned input worker and camera.
    media.stop_all().await;
    let status = tokio::time::timeout(Duration::from_secs(3), source.wait()).await;
    if status.is_err() {
        source.kill().await.unwrap();
        source.wait().await.unwrap();
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert!(status.unwrap().unwrap().success());
    let proof: Value = serde_json::from_slice(&std::fs::read(proof).unwrap()).unwrap();
    assert_eq!(proof["connections"], 1);
    assert_eq!(proof["transport"], if udp { "udp" } else { "tcp" });
    assert_eq!(proof["query_verified"], true);
    assert!(proof["authorized"].as_u64().unwrap() >= 3);
    assert!(proof["rtp_bytes"].as_u64().unwrap() > 16000);
    assert!(
        proof["versions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v == "RTSP/1.0")
    );
}
#[tokio::test]
async fn independent_g711_oracle_works_with_canonical_rtsp_and_explicit_aac() {
    camera("PCMA", "rtsp", false, Some("aac"), "aac").await;
}
#[tokio::test]
async fn rtsp2_defaults_pcma_and_pcmu_to_aac_on_tcp_and_udp() {
    for codec in ["PCMA", "PCMU"] {
        for udp in [false, true] {
            camera(codec, "rtsp2", udp, None, "aac").await;
        }
    }
}
#[tokio::test]
async fn rtsp2_respects_explicit_mp3_and_layer_two_audio_profiles() {
    camera("PCMA", "rtsp2", false, Some("mp3"), "mp3").await;
    camera("PCMU", "rtsp2", true, Some("mp2a"), "mp2").await;
}
