//! Real M4F bursts must leave usable paced output and preserve lifetime fences.
#[path = "support/paced_m4f.rs"]
mod support;
use flussonix::{
    media::Engine,
    server::{App, Options, router},
};
use futures_util::FutureExt;
use http_body_util::BodyExt;
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use support::Source;
use tower::ServiceExt;

async fn playlist(engine: &Engine) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(bytes) = engine.read("owned", "index.m3u8").await {
                if bytes.windows(3).any(|w| w == b".ts") {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("bounded paced HLS startup");
}

#[tokio::test]
async fn m4f_bursts_leave_media_for_future_reconnections_and_stop_promptly() {
    let source = Source::new(24).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "/usr/bin/ffmpeg");
    let result = std::panic::AssertUnwindSafe(async {
        let cfg = json!({"inputs":[{"url":source.url}]});
        let worker = engine.ensure("owned", &cfg).await.unwrap();
        playlist(&engine).await;
        // All 24 seconds were advertised at source startup. Check that the
        // packager has not drained them, then reconnect across distinct windows.
        tokio::time::sleep(Duration::from_secs(2).saturating_sub(source.elapsed())).await;
        for _ in 0..3 {
            let before = worker.bytes.load(Ordering::Relaxed);
            // This 64 kbit/s MP3 fixture uses less than 32 KiB/s of TS;
            // allow two seconds of probing/batching ahead of source wall time.
            let ceiling = ((source.elapsed().as_secs_f64() + 2.0) * 32768.0) as u64;
            assert!(
                before <= ceiling,
                "packager drained future media: {before} > {ceiling}"
            );
            let mut late = worker.subscribe();
            let bytes = tokio::time::timeout(Duration::from_secs(3), late.recv())
                .await
                .expect("M4F future reconnect waited for another source burst")
                .unwrap();
            assert!(!bytes.is_empty());
            assert!(worker.bytes.load(Ordering::Relaxed) > before);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let pid = worker.pid();
        let stopping = engine.stop("owned");
        tokio::pin!(stopping);
        let timely = tokio::time::timeout(Duration::from_secs(1), &mut stopping).await;
        // Keep ownership of cancellation until the child has been reaped even
        // if the latency assertion fails; dropping stop loses its removed worker.
        if timely.is_err() {
            stopping.await;
        }
        assert!(
            timely.is_ok(),
            "paced pipe backpressure delayed cancellation"
        );
        assert!(!worker.alive.load(Ordering::Relaxed));
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    engine.stop_all().await;
    source.stop().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn revoking_one_paced_m4f_viewer_preserves_the_other_viewer() {
    let source = Source::new(24).await;
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-pacing-admin".into(),
            peer_key: "owned-pacing-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":source.url}]}),
            )
            .unwrap();
        let get = |token: &str| {
            router(app.clone()).oneshot(
                axum::http::Request::builder()
                    .uri(format!("/owned/mpegts?token={token}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
        };
        let first = get("first").await.unwrap();
        let second = get("second").await.unwrap();
        assert_eq!(first.status(), 200);
        assert_eq!(second.status(), 200);
        let (mut first, mut second) = (first.into_body(), second.into_body());
        assert!(
            tokio::time::timeout(Duration::from_secs(4), first.frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_data()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(4), second.frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_data()
        );
        let pid = app.media.stats("owned").await["pid"].as_u64().unwrap();
        use sha2::{Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(b"first"));
        let session = app
            .playback_auth
            .snapshots()
            .into_iter()
            .find(|s| s["user_id"] == hash)
            .unwrap();
        assert!(app.playback_auth.revoke(session["id"].as_str().unwrap()));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), first.frame())
                .await
                .unwrap()
                .is_none(),
            "revoked body returned queued media"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(3), second.frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_data()
        );
        // A new subscription cannot read the survivor's queued pre-revocation
        // bytes. Require new production on the same live worker generation.
        let before = app.media.stats("owned").await["bytes_in"].as_u64().unwrap();
        let fresh = get("second").await.unwrap();
        assert_eq!(fresh.status(), 200);
        let mut fresh = fresh.into_body();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), fresh.frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .is_data()
        );
        let stats = app.media.stats("owned").await;
        assert_eq!(stats["pid"], pid);
        assert_eq!(stats["status"], "running");
        assert!(stats["bytes_in"].as_u64().unwrap() > before);
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    app.media.stop_all().await;
    source.stop().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn paced_m4f_source_eof_reaps_and_recovers_one_generation() {
    let source = Source::new(4).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "/usr/bin/ffmpeg");
    let result = std::panic::AssertUnwindSafe(async {
        let cfg = json!({"inputs":[{"url":source.url}]});
        let first = engine.ensure("owned", &cfg).await.unwrap();
        playlist(&engine).await;
        let old_pid = first.pid();
        source.disconnect();
        tokio::time::timeout(Duration::from_secs(8), first.closed())
            .await
            .expect("bounded M4F source EOF detection");
        let replacement = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match engine.recover("owned", &cfg).await {
                    Ok(worker) => break worker,
                    Err(e) => assert_eq!(e, "input retry backoff"),
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let same = engine.recover("owned", &cfg).await.unwrap();
        assert!(Arc::ptr_eq(&replacement, &same));
        assert!(!Arc::ptr_eq(&first, &replacement));
        assert!(!first.alive.load(Ordering::Relaxed));
        assert!(!std::path::Path::new(&format!("/proc/{old_pid}")).exists());
        playlist(&engine).await;
        assert_eq!(replacement.stats()["restart_count"], 1);
        assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    })
    .catch_unwind()
    .await;
    engine.stop_all().await;
    source.stop().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
