use super::*;
use http_body_util::BodyExt;

#[tokio::test]
async fn first_manifest_survives_one_missing_announcement_without_replacement_or_client_retry() {
    for no_dvr in [true, false] {
        let f = fixture::Fixture::m4f().await;
        f.origins[0].no_dvr_segment.store(no_dvr, Ordering::SeqCst);
        f.origins[0].startup_missing.store(true, Ordering::SeqCst);
        let began = std::time::Instant::now();
        let response = f.get("/owned/index.m3u8").await;
        let status = response.status();
        let text = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        let decoded = if status == StatusCode::OK {
            let segment = text
                .lines()
                .find(|s| !s.is_empty() && !s.starts_with('#'))
                .unwrap();
            let response = f.get(&format!("/owned/{segment}")).await;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("startup.ts");
            tokio::fs::write(&path, bytes).await.unwrap();
            Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio::process::Command::new("/usr/bin/ffmpeg")
                        .kill_on_drop(true)
                        .args([
                            "-nostdin",
                            "-v",
                            "error",
                            "-xerror",
                            "-err_detect",
                            "explode",
                            "-i",
                        ])
                        .arg(path)
                        .args(["-map", "0:a:0", "-f", "null", "-"])
                        .output(),
                )
                .await,
            )
        } else {
            None
        };
        let stats = f.app.media.stats("owned").await;
        let primary = f.origins[0].private_calls.load(Ordering::SeqCst);
        let backup = f.origins[1].private_calls.load(Ordering::SeqCst);
        let segments = f.origins[0].segment_calls.load(Ordering::SeqCst);
        let elapsed = began.elapsed();
        f.stop().await;
        assert_eq!(
            status,
            StatusCode::OK,
            "first manifest discarded next available media: {text}"
        );
        assert!(text.contains("#EXTINF:"));
        let decoded = decoded.unwrap().unwrap().unwrap();
        assert!(decoded.status.success() && decoded.stderr.is_empty());
        assert_eq!((primary, backup), (1, 0));
        assert!(segments >= 2);
        assert_eq!(stats["restart_count"], 0);
        assert!(stats["last_error"].is_null());
        assert!(elapsed < Duration::from_secs(4));
    }
}

#[tokio::test]
async fn skipped_announcement_does_not_restart_the_original_short_watchdog() {
    let f = fixture::Fixture::m4f().await;
    f.app
        .config
        .put("templates", "short", json!({"flussonix_input_timeout":1}))
        .unwrap();
    let mut root = f.app.config.snapshot();
    for source in root["sources"].as_array_mut().unwrap() {
        source["flussonix_output_template"] = json!("short");
    }
    f.app.config.replace(root).unwrap();
    f.origins[0].no_dvr_segment.store(true, Ordering::SeqCst);
    f.origins[0].startup_missing.store(true, Ordering::SeqCst);
    f.origins[0].next_signal_delay.store(4000, Ordering::SeqCst);
    let began = std::time::Instant::now();
    let status = f.get("/owned/index.m3u8").await.status();
    let elapsed = began.elapsed();
    let stats = f.app.media.stats("owned").await;
    let calls = f.origins[0].segment_calls.load(Ordering::SeqCst);
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(elapsed < Duration::from_secs(3));
    assert_eq!(stats["last_error"], "startup_timeout");
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn skipped_startup_announcement_remains_fenced_by_revocation_edit_and_shutdown() {
    for action in ["revoke", "edit", "stop"] {
        let f = fixture::Fixture::m4f().await;
        f.origins[0].no_dvr_segment.store(true, Ordering::SeqCst);
        f.origins[0].startup_missing.store(true, Ordering::SeqCst);
        f.origins[0].next_signal_delay.store(4000, Ordering::SeqCst);
        let request = f.get("/owned/index.m3u8");
        let cancel = async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while f.origins[0].segment_calls.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            match action {
                "revoke" => {
                    let rows = f.app.playback_auth.snapshots();
                    assert!(!rows.is_empty());
                    for row in rows {
                        assert!(f.app.playback_auth.revoke(row["id"].as_str().unwrap()));
                    }
                }
                "edit" => {
                    let mut root = f.app.config.snapshot();
                    for source in root["sources"].as_array_mut().unwrap() {
                        source["except"] = json!(["owned"]);
                    }
                    f.app.config.replace(root).unwrap();
                }
                _ => f.cancel.cancel(),
            }
            std::time::Instant::now()
        };
        let (response, cancelled) = tokio::join!(request, cancel);
        let status = response.status();
        let elapsed = cancelled.elapsed();
        let primary = f.origins[0].private_calls.load(Ordering::SeqCst);
        let backup = f.origins[1].private_calls.load(Ordering::SeqCst);
        let segments = f.origins[0].segment_calls.load(Ordering::SeqCst);
        f.stop().await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{action}");
        assert!(elapsed < Duration::from_secs(2));
        assert_eq!((primary, backup, segments), (1, 0, 1));
    }
}
