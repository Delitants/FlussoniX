//! Owned daemon replica groups: no production services or vendor runtime.
use flussonix::config::ConfigStore;
use serde_json::{Value, json};

fn source(group: &str, content: &str) -> Value {
    json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-config-key","on_play":"true","flussonix_source_group":group,"flussonix_content_id":content})
}
#[test]
fn explicit_vendor_replica_group_can_be_staged_and_saved_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    for host in ["one", "two"] {
        store
            .put(
                "sources",
                &format!("m4s://{host}.invalid"),
                source("replicas", "music-v1"),
            )
            .unwrap();
    }
    let reopened = ConfigStore::open(dir.path().join("config.json")).unwrap();
    assert_eq!(reopened.snapshot()["sources"].as_array().unwrap().len(), 2);
    assert!(
        reopened.snapshot()["sources"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["flussonix_content_id"] == "music-v1")
    );
}
#[test]
fn vendor_replica_group_rejects_different_content_policy_namespace_or_processing() {
    for patch in [
        json!({"flussonix_content_id":"different"}),
        json!({"flussonix_token_sha256":"a".repeat(64)}),
        json!({"group_config":{"paid":{"flussonix_token_sha256":"b".repeat(64)}}}),
        json!({"prefix":"different"}),
        json!({"except":["news"]}),
        json!({"flussonix_output_template":"aac"}),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
        store
            .put(
                "templates",
                "aac",
                json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96}}),
            )
            .unwrap();
        store
            .put(
                "sources",
                "m4s://one.invalid",
                source("replicas", "music-v1"),
            )
            .unwrap();
        let before = store.snapshot();
        let mut other = source("replicas", "music-v1");
        for (key, value) in patch.as_object().unwrap() {
            other[key] = value.clone();
        }
        assert!(store.put("sources", "m4s://two.invalid", other).is_err());
        assert_eq!(store.snapshot(), before);
    }
}
#[test]
fn vendor_group_requires_paired_valid_identity_and_cannot_mix_with_native_sources() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    for bad in [
        json!({"flussonix_source_group":"replicas"}),
        json!({"flussonix_content_id":"content"}),
        source("bad group", "content"),
        source("replicas", ""),
    ] {
        let mut row = json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-config-key","on_play":"true"});
        for (key, value) in bad.as_object().unwrap() {
            row[key] = value.clone();
        }
        assert!(store.put("sources", "m4s://bad.invalid", row).is_err());
    }
    store
        .put(
            "sources",
            "m4s://one.invalid",
            source("replicas", "content"),
        )
        .unwrap();
    assert!(
        store
            .put(
                "sources",
                "native",
                json!({"api_url":"http://native.invalid","flussonix_source_group":"replicas"})
            )
            .is_err()
    );
}

#[path = "vendor_daemon_failover/fixture.rs"]
mod fixture;
use axum::http::StatusCode;
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test]
async fn management_outage_selects_backup_and_keeps_it_after_primary_recovers() {
    let f = fixture::Fixture::new().await;
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let _ = f.playlist().await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    *f.origins[0].api.lock().unwrap() = StatusCode::OK;
    let _ = f.playlist().await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}
#[tokio::test]
async fn private_media_loss_recovers_without_viewer_requests_while_management_stays_up() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    let old_pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    assert!(old_pid > 0);
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let stats = f.app.media.stats("owned").await;
            if stats["pid"]
                .as_u64()
                .is_some_and(|pid| pid > 0 && pid != old_pid)
                && stats["bytes_in"].as_u64().unwrap_or(0) > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("replacement packager did not start in the background");
    assert!(!std::path::Path::new(&format!("/proc/{old_pid}")).exists());
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    let playlist = f.playlist().await;
    assert!(playlist.contains("#EXT-X-DISCONTINUITY"));
    use http_body_util::BodyExt;
    let segment = playlist
        .lines()
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap();
    let response = f.get(&format!("/owned/{segment}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let ts = response.into_body().collect().await.unwrap().to_bytes();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recovered.ts");
    tokio::fs::write(&path, &ts).await.unwrap();
    let status = tokio::process::Command::new("/usr/bin/ffmpeg")
        .kill_on_drop(true)
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(&path)
        .args(["-t", "0.1", "-f", "null", "-"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .unwrap();
    assert!(
        status.success(),
        "independent decoder rejected recovered HLS"
    );
    f.stop().await;
}

#[tokio::test]
async fn acknowledged_m4f_segment_loss_recovers_in_background_without_latching_rejection() {
    missing_m4f_recovery(false, false).await;
}

#[tokio::test]
async fn acknowledged_m4f_no_dvr_segment_loss_recovers_in_background() {
    missing_m4f_recovery(true, false).await;
}

#[tokio::test]
async fn acknowledged_m4f_no_dvr_cold_start_recovers_without_another_viewer_request() {
    missing_m4f_recovery(true, true).await;
}

async fn missing_m4f_recovery(no_dvr: bool, cold: bool) {
    let f = fixture::Fixture::m4f().await;
    f.origins[0].no_dvr_segment.store(no_dvr, Ordering::SeqCst);
    let old_pid = if cold {
        f.origins[0].missing_segment.store(true, Ordering::SeqCst);
        assert_eq!(
            f.get("/owned/index.m3u8").await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        0
    } else {
        let _ = f.playlist().await;
        let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
        assert!(pid > 0);
        pid
    };
    if !cold {
        assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    }
    let before = f.origins[0].segment_calls.load(Ordering::SeqCst);
    f.origins[0].missing_segment.store(true, Ordering::SeqCst);
    let recovered = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let stats = f.app.media.stats("owned").await;
            if f.origins[1].private_calls.load(Ordering::SeqCst) > 0
                && stats["pid"]
                    .as_u64()
                    .is_some_and(|pid| pid > 0 && pid != old_pid)
                && stats["bytes_in"].as_u64().unwrap_or(0) > 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let missing_requests = f.origins[0].segment_calls.load(Ordering::SeqCst);
    let backup_segments = f.origins[1].segment_calls.load(Ordering::SeqCst);
    let old_gone = !std::path::Path::new(&format!("/proc/{old_pid}")).exists();
    let decode = if recovered.is_ok() {
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
        let path = dir.path().join("recovered-m4f.ts");
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
                    .stdin(std::process::Stdio::null())
                    .output(),
            )
            .await,
        )
    } else {
        None
    };
    f.stop().await;
    assert!(
        recovered.is_ok(),
        "announced missing segment permanently blocked background recovery (no_dvr={no_dvr})"
    );
    assert!(
        (cold || missing_requests > before)
            && missing_requests > 0
            && backup_segments > 0
            && old_gone
    );
    let decoded = decode.unwrap().unwrap().unwrap();
    assert!(
        decoded.status.success() && decoded.stderr.is_empty(),
        "recovered HLS audio did not strictly decode"
    );
}

#[tokio::test]
async fn unacknowledged_m4f_segment_404_still_blocks_backup() {
    unacknowledged_m4f_loss(false).await;
}

#[tokio::test]
async fn unacknowledged_m4f_no_dvr_segment_still_blocks_backup() {
    unacknowledged_m4f_loss(true).await;
}

async fn unacknowledged_m4f_loss(no_dvr: bool) {
    let f = fixture::Fixture::m4f().await;
    let _ = f.playlist().await;
    f.origins[0].segment_ack.store(false, Ordering::SeqCst);
    f.origins[0].no_dvr_segment.store(no_dvr, Ordering::SeqCst);
    let before = f.origins[0].segment_calls.load(Ordering::SeqCst);
    f.origins[0].missing_segment.store(true, Ordering::SeqCst);
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        while f.origins[0].segment_calls.load(Ordering::SeqCst) <= before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let backup_calls = f.origins[1].private_calls.load(Ordering::SeqCst);
    f.stop().await;
    assert!(observed.is_ok());
    assert_eq!(
        backup_calls, 0,
        "missing acknowledgment must fence backup recovery"
    );
}

#[tokio::test]
async fn private_rejection_blocks_backup_through_catalog_success_and_later_api_outage() {
    let f = fixture::Fixture::new().await;
    *f.origins[0].media.lock().unwrap() = StatusCode::FORBIDDEN;
    let _ = f.get("/owned/index.m3u8").await;
    f.wait_private(0, 1).await;
    tokio::time::sleep(Duration::from_millis(650)).await;
    let _ = f.get("/owned/index.m3u8").await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    assert!(f.origins[0].management_calls.load(Ordering::SeqCst) > 3);
    *f.origins[0].api.lock().unwrap() = StatusCode::OK;
    *f.origins[0].media.lock().unwrap() = StatusCode::OK;
    tokio::time::sleep(Duration::from_millis(650)).await;
    let _ = f.playlist().await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    f.stop().await;
}

#[tokio::test]
async fn authoritative_removal_during_private_loss_prevents_rotation_through_later_outages() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    *f.origins[0].rows.lock().unwrap() = json!([]);
    f.origins[0].close_media();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn recovered_background_worker_stops_after_original_viewer_demand_expires() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    tokio::time::sleep(Duration::from_secs(31)).await;
    let calls = f.origins[1].private_calls.load(Ordering::SeqCst);
    f.app.reconcile().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        f.app.media.count().await,
        0,
        "background packaging must not extend playback activity"
    );
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), calls);
    f.stop().await;
}

#[tokio::test]
async fn revoked_viewer_cannot_start_backup_after_held_recovery_discovery() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    let (started, wait) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *f.origins[1].hold.lock().unwrap() = Some((started, resume));
    f.origins[0].close_media();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap();
    for session in f.app.playback_auth.snapshots() {
        assert!(f.app.playback_auth.revoke(session["id"].as_str().unwrap()));
    }
    let _ = release.send(());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn configuration_edit_during_held_recovery_cannot_publish_or_pull_obsolete_backup() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    let (started, wait) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *f.origins[1].hold.lock().unwrap() = Some((started, resume));
    f.origins[0].close_media();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap();
    let mut root = f.app.config.snapshot();
    for row in root["sources"].as_array_mut().unwrap() {
        row["except"] = json!(["owned"]);
    }
    f.app.config.replace(root).unwrap();
    let _ = release.send(());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::NOT_FOUND
    );
    f.stop().await;
}

#[tokio::test]
async fn denied_viewer_and_lb_without_capacity_never_open_private_replica_media() {
    let f = fixture::Fixture::with_role("lb").await;
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.app.media.count().await, 0);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    f.stop().await;
    let f = fixture::Fixture::new().await;
    let mut root = f.app.config.snapshot();
    for row in root["sources"].as_array_mut().unwrap() {
        row["flussonix_token_sha256"] = json!("a".repeat(64));
    }
    f.app.config.replace(root).unwrap();
    assert_eq!(
        f.get("/owned/index.m3u8?token=wrong").await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(f.app.media.count().await, 0);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    f.stop().await;
}

#[tokio::test]
async fn renewed_live_body_closes_at_switch_and_background_backup_retains_valid_demand() {
    use http_body_util::BodyExt;
    let f = fixture::Fixture::new().await;
    let response = f.get("/owned/m4s").await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    assert!(
        tokio::time::timeout(Duration::from_secs(32), async {
            while body.frame().await.is_some() {}
        })
        .await
        .is_err()
    );
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert!(f.origins[0].management_calls.load(Ordering::SeqCst) >= 6);
    f.origins[0].close_media();
    tokio::time::timeout(Duration::from_secs(3), async {
        while body.frame().await.is_some() {}
    })
    .await
    .expect("obsolete continuous body did not close");
    drop(body);
    f.wait_private(1, 1).await;
    f.stop().await;
}

#[tokio::test]
async fn selected_backup_is_reported_and_admin_reauth_uses_its_current_group_authority() {
    use base64::Engine;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let f = fixture::Fixture::new().await;
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let _ = f.playlist().await;
    let before = f
        .origins
        .iter()
        .map(|o| o.management_calls.load(Ordering::SeqCst))
        .sum::<usize>();
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("admin:owned-admin")
    );
    let response = flussonix::server::router(f.app.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri("/flussonix/api/v1/node")
                .header("authorization", &auth)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let node: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let stats = &node["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "owned")
        .unwrap()["stats"];
    assert_eq!(stats["upstream_source"], f.origins[1].key);
    assert_eq!(stats["source_group"], "music-origins");
    assert_eq!(stats["source_switches"], 1);
    let response = flussonix::server::router(f.app.clone())
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/sessions/reauth?name=owned")
                .header("authorization", &auth)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        f.origins
            .iter()
            .map(|o| o.management_calls.load(Ordering::SeqCst))
            .sum::<usize>(),
        before
    );
    f.stop().await;
}

#[tokio::test]
async fn concurrent_group_viewers_share_one_discovery_handle_and_private_input() {
    let f = fixture::Fixture::new().await;
    let (a, b, c) = tokio::join!(
        f.get("/owned/index.m3u8"),
        f.get("/owned/index.m3u8"),
        f.get("/owned/index.m3u8")
    );
    for reply in [a, b, c] {
        assert_eq!(reply.status(), StatusCode::OK);
    }
    let _ = f.playlist().await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.origins[0].management_calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn unrelated_configuration_edit_cannot_erase_private_rejection_authority() {
    let f = fixture::Fixture::new().await;
    *f.origins[0].media.lock().unwrap() = StatusCode::FORBIDDEN;
    let _ = f.get("/owned/index.m3u8").await;
    tokio::time::sleep(Duration::from_millis(650)).await;
    let _ = f.get("/owned/index.m3u8").await;
    f.app
        .config
        .put("templates", "unrelated", json!({}))
        .unwrap();
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let _ = f.get("/owned/index.m3u8").await;
    assert_eq!(
        f.origins[1].private_calls.load(Ordering::SeqCst),
        0,
        "an unrelated save must not reset a denied source relationship"
    );
    f.stop().await;
}

#[tokio::test]
async fn private_rejection_is_latched_before_background_consumption_or_unrelated_save() {
    let f = fixture::Fixture::new().await;
    *f.origins[0].media.lock().unwrap() = StatusCode::FORBIDDEN;
    let _ = f.get("/owned/index.m3u8").await;
    f.app
        .config
        .put("templates", "immediate-unrelated", json!({}))
        .unwrap();
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let _ = f.get("/owned/index.m3u8").await;
    assert_eq!(
        f.origins[1].private_calls.load(Ordering::SeqCst),
        0,
        "private rejection must commit at the failure fence, not in a later scheduler pass"
    );
    f.stop().await;
}

#[tokio::test]
async fn consecutive_private_losses_rotate_to_third_origin_and_keep_healthy_fallback() {
    let mut f = fixture::Fixture::new().await;
    let third = fixture::Origin::start("owned-origin-c").await;
    f.app.config.put("sources",&third.key,json!({"flussonix_source_kind":"flussonic","cluster_key":third.secret,"on_play":"true","flussonix_source_group":"music-origins","flussonix_content_id":"explicit-music-v1"})).unwrap();
    f.origins.push(third);
    let _ = f.playlist().await;
    *f.origins[0].media.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    let _ = f.playlist().await;
    *f.origins[1].media.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    f.origins[1].close_media();
    f.wait_private(2, 1).await;
    let _ = f.playlist().await;
    *f.origins[0].media.lock().unwrap() = StatusCode::OK;
    let _ = f.playlist().await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.origins[2].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}

#[tokio::test]
async fn unrelated_save_keeps_selected_backup_without_importing_old_leases_or_workers() {
    let f = fixture::Fixture::new().await;
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let _ = f.playlist().await;
    *f.origins[0].api.lock().unwrap() = StatusCode::OK;
    f.app
        .config
        .put("templates", "unrelated", json!({}))
        .unwrap();
    let _ = f.playlist().await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 2);
    f.stop().await;
}

#[tokio::test]
async fn healthy_original_private_source_can_reopen_when_no_backup_catalog_is_available() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    *f.origins[1].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    f.origins[0].close_media();
    f.wait_private(0, 2).await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn eighth_replica_is_reachable_with_slow_unavailable_predecessors_in_lookup_budget() {
    let mut f = fixture::Fixture::new().await;
    for id in [
        "owned-origin-c",
        "owned-origin-d",
        "owned-origin-e",
        "owned-origin-f",
        "owned-origin-g",
        "owned-origin-h",
    ] {
        let origin = fixture::Origin::start(id).await;
        f.app.config.put("sources",&origin.key,json!({"flussonix_source_kind":"flussonic","cluster_key":origin.secret,"on_play":"true","flussonix_source_group":"music-origins","flussonix_content_id":"explicit-music-v1"})).unwrap();
        f.origins.push(origin);
    }
    let mut releases = Vec::new();
    for origin in f.origins.iter().take(7) {
        *origin.api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
        let (started, _wait) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        *origin.hold.lock().unwrap() = Some((started, resume));
        releases.push(release);
    }
    let response = f.get("/owned/index.m3u8").await;
    for release in releases {
        let _ = release.send(());
    }
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "bounded discovery must reach every declared replica"
    );
    assert!(
        f.origins
            .iter()
            .take(7)
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    assert_eq!(f.origins[7].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}

#[tokio::test]
async fn scheduled_catalog_removal_latches_before_api_outage_and_prevents_background_backup() {
    use http_body_util::BodyExt;
    let f = fixture::Fixture::new().await;
    let response = f.get("/owned/m4s").await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    assert!(body.frame().await.is_some());
    *f.origins[0].rows.lock().unwrap() = json!([]);
    tokio::time::timeout(Duration::from_secs(24), async {
        while body.frame().await.is_some() {}
    })
    .await
    .expect("authoritative renewal did not close the old body");
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        f.origins[1].private_calls.load(Ordering::SeqCst),
        0,
        "scheduled removal authority was lost before recovery"
    );
    drop(body);
    f.stop().await;
}

#[tokio::test]
async fn authenticated_private_startup_stall_rotates_with_short_and_default_deadlines() {
    for seconds in [1, 15] {
        let f = fixture::Fixture::new().await;
        f.app
            .config
            .put(
                "templates",
                "startup",
                json!({"flussonix_input_timeout":seconds}),
            )
            .unwrap();
        let mut root = f.app.config.snapshot();
        for source in root["sources"].as_array_mut().unwrap() {
            source["flussonix_output_template"] = json!("startup");
        }
        f.app.config.replace(root).unwrap();
        f.origins[0].stall_metadata.store(true, Ordering::SeqCst);
        // This viewer creates real demand but never gets metadata from A.
        // The manifest honors the configured worker startup deadline. Keep this
        // test's outer guard beyond that deadline, including the default 15s.
        let request = f
            .get_with_timeout("/owned/index.m3u8", Duration::from_secs(seconds + 8))
            .await;
        assert_eq!(request.status(), StatusCode::SERVICE_UNAVAILABLE);
        tokio::time::timeout(Duration::from_secs(seconds + 8), async {
            while f.origins[1].private_calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("authenticated startup stall never selected the healthy backup");
        assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
        let playlist = f.playlist().await;
        assert!(playlist.contains("#EXTINF:"));
        f.stop().await;
    }
}

#[path = "vendor_daemon_failover/discovery.rs"]
mod discovery;
