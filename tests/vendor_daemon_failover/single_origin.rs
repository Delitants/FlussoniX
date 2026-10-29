use super::*;

#[tokio::test]
async fn single_origin_warm_segment_loss_recovers_without_another_viewer_request() {
    for no_dvr in [true, false] {
        let f = fixture::Fixture::single_m4f().await;
        let _ = f.playlist().await;
        let old_pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
        let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
        f.origins[0].no_dvr_segment.store(no_dvr, Ordering::SeqCst);
        f.origins[0].startup_missing.store(true, Ordering::SeqCst);
        // Observe only process/byte counters; a viewer request must not trigger recovery.
        let recovered = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let stats = f.app.media.stats("owned").await;
                if f.origins[0].private_calls.load(Ordering::SeqCst) > calls
                    && stats["pid"].as_u64().is_some_and(|p| p > 0 && p != old_pid)
                    && stats["bytes_in"].as_u64().unwrap_or(0) > 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        let backup_calls = f.origins[1].private_calls.load(Ordering::SeqCst);
        let old_gone = !std::path::Path::new(&format!("/proc/{old_pid}")).exists();
        let decoded = if recovered.is_ok() {
            Some(decode_hls(&f).await)
        } else {
            None
        };
        f.stop().await;
        assert!(
            recovered.is_ok(),
            "single-origin warm loss never recovered autonomously"
        );
        assert_eq!(backup_calls, 0);
        assert!(old_gone);
        let decoded = decoded.unwrap();
        assert!(
            decoded.status.success() && decoded.stderr.is_empty(),
            "recovered media did not strictly decode"
        );
    }
}

async fn decode_hls(f: &fixture::Fixture) -> std::process::Output {
    use http_body_util::BodyExt;
    let playlist = f.playlist().await;
    let segment = playlist
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap();
    let response = f.get(&format!("/owned/{segment}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recovered.ts");
    tokio::fs::write(&path, bytes).await.unwrap();
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
            .stdin(std::process::Stdio::null())
            .output(),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn single_origin_m4s_recovers_and_background_does_not_extend_viewer_demand() {
    let f = fixture::Fixture::configured("standalone", json!({"only":[]}), None, false).await;
    let _ = f.playlist().await;
    let before = f.origins[0].private_calls.load(Ordering::SeqCst);
    f.origins[0].close_media();
    f.wait_private(0, before + 1).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
    f.app.reconcile().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let workers = f.app.media.count().await;
    let after = f.origins[0].private_calls.load(Ordering::SeqCst);
    f.stop().await;
    assert_eq!(
        workers, 0,
        "background recovery extended original viewer activity"
    );
    assert_eq!(calls, after);
}

#[tokio::test]
async fn single_origin_held_recovery_obeys_revocation_and_configuration_changes() {
    for action in ["revoke", "edit"] {
        let f = fixture::Fixture::single_m4f().await;
        let _ = f.playlist().await;
        let before = f.origins[0].private_calls.load(Ordering::SeqCst);
        let (started, wait) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        *f.origins[0].hold.lock().unwrap() = Some((started, resume));
        f.origins[0].close_media();
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap()
            .unwrap();
        if action == "revoke" {
            for row in f.app.playback_auth.snapshots() {
                assert!(f.app.playback_auth.revoke(row["id"].as_str().unwrap()));
            }
        } else {
            f.app
                .config
                .put("sources", &f.origins[0].key, json!({"except":["owned"]}))
                .unwrap();
        }
        let _ = release.send(());
        tokio::time::sleep(Duration::from_secs(2)).await;
        let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
        let workers = f.app.media.count().await;
        f.stop().await;
        assert_eq!(calls, before, "{action} allowed obsolete private recovery");
        assert_eq!(workers, 0);
    }
}

#[tokio::test]
async fn single_origin_authoritative_removal_blocks_background_reopening() {
    let f = fixture::Fixture::single_m4f().await;
    let _ = f.playlist().await;
    let before = f.origins[0].private_calls.load(Ordering::SeqCst);
    *f.origins[0].rows.lock().unwrap() = json!([]);
    f.origins[0].close_media();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
    let workers = f.app.media.count().await;
    f.stop().await;
    assert_eq!(calls, before);
    assert_eq!(workers, 0);
}

#[tokio::test]
async fn single_origin_group_specific_policy_is_preserved_during_recovery() {
    let f = fixture::Fixture::configured(
        "standalone",
        json!({"only":[],
        "group_config":{"public":{"on_play":"true"},
                        "paid":{"flussonix_token_sha256":"a".repeat(64)}},
        "flussonix_group_pins":{"owned":["public"]}}),
        Some(json!([{"name":"owned","named_by":"config","static":true,"groups":["public"]}])),
        false,
    )
    .await;
    let _ = f.playlist().await;
    let before = f.origins[0].private_calls.load(Ordering::SeqCst);
    // A fresh catalog now puts the same stream behind a different policy.
    *f.origins[0].rows.lock().unwrap() = json!([
        {"name":"owned","named_by":"config","static":true,"groups":["paid"]}]);
    f.origins[0].close_media();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
    let status = f.get("/owned/index.m3u8").await.status();
    f.stop().await;
    assert_eq!(
        calls, before,
        "a different catalog policy reused the original permission"
    );
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn single_origin_unacknowledged_missing_segment_cannot_publish_media() {
    let f = fixture::Fixture::single_m4f().await;
    let _ = f.playlist().await;
    f.origins[0].segment_ack.store(false, Ordering::SeqCst);
    f.origins[0].missing_segment.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3)).await;
    f.app
        .config
        .put("templates", "unrelated", json!({}))
        .unwrap();
    let status = f.get("/owned/index.m3u8").await.status();
    let workers = f.app.media.stats("owned").await;
    let backup = f.origins[1].private_calls.load(Ordering::SeqCst);
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(workers["bytes_in"].as_u64().unwrap_or(0) == 0);
    assert_eq!(backup, 0);
}
