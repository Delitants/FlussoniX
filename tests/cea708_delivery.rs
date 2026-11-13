#[path = "support/caption_evidence.rs"]
mod evidence;
#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/subtitle_fixture.rs"]
mod original;
use flussonix::media::Engine;
use futures_util::FutureExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
async fn run(cpu: bool) {
    // Prepare the owned fixture before starting any long-lived media tasks.
    let input = original::inject(&fixture::digital_transport());
    let d = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new(
        d.path(),
        &std::env::var("FLUSSONIX_CAPTION_TEST_FFMPEG").unwrap_or("ffmpeg".into()),
    ));
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"service":1,"language":"en","name":"English"},{"service":2,"language":"es","name":"Spanish"}]});
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
    // A full sixteen-second burst can rotate the live window past the first
    // digital cue before both independently scheduled HLS variants are read.
    let progress = Arc::new(std::sync::Mutex::new(fixture::PacingProgress {
        phase: "starting",
        total_bytes: input.len(),
        ..Default::default()
    }));
    let samples = Arc::new(std::sync::Mutex::new(vec![]));
    let monitoring = tokio_util::sync::CancellationToken::new();
    let sample_task = {
        let samples = samples.clone();
        let progress = progress.clone();
        let worker = p.worker.clone();
        let root = d.path().to_path_buf();
        let cancel = monitoring.clone();
        tokio::spawn(async move {
            let clock = tokio::time::Instant::now();
            loop {
                let row = json!({"elapsed_ms":clock.elapsed().as_millis(),
                    "feed":progress.lock().unwrap().clone(),"worker":worker.stats(),
                    "process":evidence::process(worker.pid()),"playlists":evidence::playlists(&root)});
                if samples.lock().unwrap().len() < 256 {
                    samples.lock().unwrap().push(row);
                }
                tokio::select! {biased;_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(100))=>{}}
            }
        })
    };
    let feed = {
        let progress = progress.clone();
        let bytes = input.clone();
        let mut stdin = p.stdin.take().unwrap();
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            fixture::paced_observed(&mut stdin, bytes, Some(progress)).await;
        }))
    };
    let mut capture = Some(capture);
    let mut captured_ts = None;
    let run = std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(16), async {
            loop {
                // Variants publish independently. Both must reach the quiet tail
                // before either playlist can prove an empty subtitle segment.
                let mut ready = true;
                for prefix in ["", "fmp4/"] {
                    ready &= e
                        .read("group/owned", &format!("{prefix}av.m3u8"))
                        .await
                        .is_ok_and(|b| {
                            String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5
                        });
                }
                if ready {
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
            assert!(master.contains("URI=\"s1.m3u8\""), "{master}");
            assert!(master.contains("URI=\"s2.m3u8\""), "{master}");
            let av = String::from_utf8(
                e.read("group/owned", &format!("{prefix}av.m3u8"))
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            let list = String::from_utf8(
                e.read("group/owned", &format!("{prefix}s1.m3u8"))
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
            assert!(words.contains("USA708"), "{words}");
            assert!(words.lines().any(|l| l == "LIVE708"), "{words}");
            assert!(words.contains("00:00:02.160 --> 00:00:03.000"), "{words}");
            assert!(
                empty,
                "{prefix} silence must publish an empty segment: {list}"
            );
        }
        for prefix in ["", "fmp4/"] {
            let list = String::from_utf8(
                e.read("group/owned", &format!("{prefix}s2.m3u8"))
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            let mut words = String::new();
            for file in list.lines().filter(|f| f.ends_with(".vtt")) {
                words += std::str::from_utf8(
                    &e.read("group/owned", &format!("{prefix}{file}"))
                        .await
                        .unwrap(),
                )
                .unwrap();
            }
            assert!(words.contains("ESPAÑOL"), "{words}");
            assert!(!words.contains("USA708"));
            assert!(words.contains("s2-"));
        }
        captured.cancel();
        captured_ts = Some(capture.take().unwrap().await.unwrap());
        let ts = captured_ts.as_ref().unwrap();
        let descriptors = original::descriptors(ts);
        for (desc, body) in [
            (original::DVB_DESC, original::DVB_BODY.to_vec()),
            (original::TTX_DESC, original::teletext_body()),
        ] {
            let (pid, _) = descriptors
                .iter()
                .find(|(_, v)| v.as_slice() == desc)
                .expect("original subtitle descriptor");
            let payloads = original::pes_bodies(ts, *pid);
            assert!(!payloads.is_empty());
            assert!(payloads.iter().all(|p| p == &body));
        }
        assert_eq!(p.worker.stats()["hls_captions"]["status"], "running");
        assert!(p.worker.stats()["hls_captions"]["cues"].as_u64().unwrap() > 0);
    })
    .catch_unwind()
    .await;
    let final_feed = progress.lock().unwrap().clone();
    let final_worker = p.worker.stats();
    let snapshot = evidence::snapshot(&e, d.path()).await;
    captured.cancel();
    monitoring.cancel();
    feed.abort();
    let feed_result = feed.await;
    let monitor_result = sample_task.await;
    let mut capture_error = None;
    if let Some(capture) = capture {
        match capture.await {
            Ok(bytes) => captured_ts = Some(bytes),
            Err(error) => capture_error = Some(error.to_string()),
        }
    }
    e.stop_all().await;
    let report = json!({"passed":run.is_ok() && monitor_result.is_ok() && capture_error.is_none(),"cpu":cpu,"deadline_seconds":16,
        "feed":final_feed,"worker_before_stop":final_worker,"samples":*samples.lock().unwrap(),
        "cleanup":{"engine_workers":e.count().await,"feed_cancelled":feed_result.is_err_and(|e|e.is_cancelled()),
            "monitor_joined":monitor_result.is_ok(),"capture_error":capture_error}});
    let saved = evidence::save(
        snapshot,
        cpu,
        &input,
        captured_ts.as_deref().unwrap_or(&[]),
        &report,
    );
    if let Err(error) = run {
        eprintln!(
            "caption boundary evidence: {}",
            json!({"feed":report["feed"],"worker":report["worker_before_stop"],"cleanup":report["cleanup"]})
        );
        if let Err(error) = saved {
            eprintln!("owned caption evidence could not be retained: {error}");
        }
        std::panic::resume_unwind(error);
    }
    saved.unwrap();
    assert!(monitor_result.is_ok());
    assert_eq!(e.count().await, 0);
}
#[tokio::test]
async fn copy_delivers_two_digital_services_in_ts_and_fmp4() {
    run(false).await
}
#[tokio::test]
async fn cpu_extracts_digital_services_before_encoding() {
    run(true).await
}
#[tokio::test]
async fn digital_resources_share_auth_and_revoke_old_tokens() {
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
    let cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),"flussonix_hls_captions":[{"service":1,"language":"en","name":"English"},{"service":2,"language":"es","name":"Spanish"}]});
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
        .write_all(&fixture::digital_transport())
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
    assert!(body.contains("s1.m3u8?token=owned-viewer"), "{body}");
    let list = get("/group/owned/s1.m3u8?token=owned-viewer".into()).await;
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
    assert_eq!(get("/group/owned/s1.m3u8".into()).await.status(), 403);
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
