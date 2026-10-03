#[path = "support/caption_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/subtitle_fixture.rs"]
mod original;
use flussonix::media::Engine;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
async fn run(cpu: bool) {
    let d = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new(
        d.path(),
        &std::env::var("FLUSSONIX_CAPTION_TEST_FFMPEG").unwrap_or("ffmpeg".into()),
    ));
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]});
    if cpu {
        cfg["transcoder"] = json!({"encoder":"libx264","vb":300})
    }
    cfg["flussonix_subtitle_tracks"] = json!("preserve");
    let mut p = e
        .publish_guarded("group/owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let mut subscriber = p.worker.subscribe();
    let captured = tokio_util::sync::CancellationToken::new();
    let cancel = captured.clone();
    let capture = tokio::spawn(async move {
        let mut bytes = vec![];
        loop {
            tokio::select! {biased;_=cancel.cancelled()=>break,result=subscriber.recv()=>match result{Ok(chunk)=>{bytes.extend(chunk);assert!(bytes.len()<2*1024*1024);},Err(e)=>panic!("capture failed: {e}")}}
        }
        bytes
    });
    let input = original::inject(&fixture::transport());
    p.stdin.as_mut().unwrap().write_all(&input).await.unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if e.read("group/owned", "fmp4/av.m3u8").await.is_ok()
                && e.read("group/owned", "av.m3u8")
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5)
            {
                break;
            }
            assert!(
                p.worker.alive.load(std::sync::atomic::Ordering::Relaxed),
                "{}",
                p.worker.stats()
            );
            tokio::time::sleep(Duration::from_millis(30)).await
        }
    })
    .await
    .unwrap_or_else(|err| panic!("{err}: {}", p.worker.stats()));
    for prefix in ["", "fmp4/"] {
        let master = String::from_utf8(
            e.read("group/owned", &format!("{prefix}index.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(master.contains("#EXT-X-MEDIA:TYPE=SUBTITLES"), "{master}");
        assert!(master.contains("LANGUAGE=\"en\""));
        let av = String::from_utf8(
            e.read("group/owned", &format!("{prefix}av.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}cc1.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(av.contains("#EXTINF:"));
        let mut words = String::new();
        let mut empty = false;
        for segment in list.lines().filter(|s| s.ends_with(".vtt")) {
            let vtt = String::from_utf8(
                e.read("group/owned", &format!("{prefix}{segment}"))
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            assert!(vtt.starts_with("WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:"));
            if !vtt.contains("-->") {
                empty = true
            }
            words += &vtt;
        }
        assert!(words.contains("USA 608"), "{words}");
        assert!(words.contains("00:00:02.160 --> 00:00:03.000"), "{words}");
        assert!(empty, "silence must publish an empty segment");
    }
    captured.cancel();
    let ts = capture.await.unwrap();
    let descriptors = original::descriptors(&ts);
    for (desc, body) in [
        (original::DVB_DESC, original::DVB_BODY.to_vec()),
        (original::TTX_DESC, original::teletext_body()),
    ] {
        let (pid, _) = descriptors
            .iter()
            .find(|(_, v)| v.as_slice() == desc)
            .expect("original subtitle descriptor");
        let payloads = original::pes_bodies(&ts, *pid);
        assert!(!payloads.is_empty());
        assert!(payloads.iter().all(|p| p == &body));
    }
    assert_eq!(p.worker.stats()["hls_captions"]["status"], "running");
    assert!(p.worker.stats()["hls_captions"]["cues"].as_u64().unwrap() > 0);
    e.stop_all().await;
}
#[tokio::test]
async fn copy_delivers_timed_selectable_captions_in_both_hls_variants() {
    run(false).await
}
#[tokio::test]
async fn cpu_transcoding_extracts_captions_before_video_encode() {
    run(true).await
}
#[tokio::test]
async fn caption_resources_reuse_authorization_and_grouped_stream_paths() {
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;
    let d = tempfile::tempdir().unwrap();
    let app = flussonix::server::App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        flussonix::server::Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),"flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]});
    app.config
        .put("streams", "group/owned", cfg.clone())
        .unwrap();
    let mut p = app
        .media
        .publish_guarded("group/owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    p.stdin
        .as_mut()
        .unwrap()
        .write_all(&fixture::transport())
        .await
        .unwrap();
    let get = |path: String| {
        let app = app.clone();
        async move {
            flussonix::server::router(app)
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
        }
    };
    let master = get("/group/owned/index.m3u8?token=owned-viewer".into()).await;
    assert_eq!(master.status(), 200);
    let body = String::from_utf8(
        master
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("cc1.m3u8?token=owned-viewer"), "{body}");
    let list = get("/group/owned/cc1.m3u8?token=owned-viewer".into()).await;
    assert_eq!(list.status(), 200);
    let body = String::from_utf8(
        list.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let file = body.lines().find(|s| s.contains(".vtt")).unwrap();
    let path = format!("/group/owned/{file}");
    let vtt = get(path.clone()).await;
    assert_eq!(vtt.status(), 200);
    assert_eq!(vtt.headers()["content-type"], "text/vtt; charset=utf-8");
    assert_eq!(get("/group/owned/cc1.m3u8".into()).await.status(), 403);
    app.config
        .put(
            "streams",
            "group/owned",
            json!({"flussonix_token_sha256":"f".repeat(64)}),
        )
        .unwrap();
    assert_eq!(get(path).await.status(), 403);
    app.media.stop_all().await;
}
#[tokio::test]
async fn live_silence_keeps_av_progressing_and_generation_edit_removes_old_captions() {
    let d = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new(d.path(), "ffmpeg"));
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":"preserve","flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]});
    let mut p = e
        .publish_guarded("live", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let feed = tokio::spawn(fixture::paced(
        p.stdin.take().unwrap(),
        fixture::transport(),
    ));
    let worker = p.worker.clone();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if e.read("live", "cc1.m3u8").await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await
        }
    })
    .await
    .unwrap();
    let before = worker.stats()["bytes_in"].as_u64().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(worker.stats()["bytes_in"].as_u64().unwrap() > before);
    assert_eq!(worker.stats()["hls_captions"]["status"], "running");
    let old = String::from_utf8(e.read("live", "cc1.m3u8").await.unwrap().to_vec())
        .unwrap()
        .lines()
        .find(|s| s.ends_with(".vtt"))
        .unwrap()
        .to_owned();
    cfg["flussonix_hls_captions"] = json!([{"channel":2,"language":"es","name":"Spanish"}]);
    let replacement = e
        .publish_guarded("live", &cfg, std::future::ready(true))
        .await
        .unwrap();
    assert!(worker.is_closed());
    assert!(e.read("live", &old).await.is_err());
    assert_eq!(replacement.worker.stats()["subtitle_tracks"], "preserve");
    assert_eq!(
        replacement.worker.stats()["hls_captions"]["channels"][0]["channel"],
        2
    );
    feed.abort();
    let _ = feed.await;
    e.stop_all().await;
}
#[test]
fn owned_caption_timing_agrees_with_an_independent_decoder() {
    use std::process::Command;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("owned.ts");
    std::fs::write(&input, fixture::transport()).unwrap();
    let source = format!("movie={}[out0+subcc]", input.display());
    let output = Command::new("ffmpeg")
        .args([
            "-v", "error", "-f", "lavfi", "-i", &source, "-map", "0:s", "-c:s", "srt", "-f", "srt",
            "pipe:1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let srt = String::from_utf8_lossy(&output.stdout);
    assert!(srt.contains("USA 608"));
    assert!(srt.contains("00:00:01,160 --> 00:00:03,000"), "{srt}");
}
#[tokio::test]
async fn a_missing_video_track_fails_conversion_without_stopping_audio_delivery() {
    let fixture = std::process::Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "12",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            "pipe:1",
        ])
        .output()
        .unwrap();
    assert!(fixture.status.success());
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]});
    let mut publication = e
        .publish_guarded("audio", &cfg, std::future::ready(true))
        .await
        .unwrap();
    publication
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&fixture.stdout)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(bytes) = e.read("audio", "index.m3u8").await {
                if String::from_utf8_lossy(&bytes).contains("#EXTINF:") {
                    return bytes;
                }
            }
            if !publication
                .worker
                .alive
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                panic!(
                    "caption conversion must not stop audio: {}",
                    publication.worker.stats()
                )
            }
            tokio::time::sleep(Duration::from_millis(30)).await
        }
    })
    .await;
    let stats = publication.worker.stats();
    e.stop_all().await;
    let bytes = result.unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("TYPE=SUBTITLES"));
    assert_eq!(stats["hls_captions"]["status"], "failed");
}
