//! Slow owned catalogs exercise the real daemon and bounded replica selection.
use super::*;
use axum::{body::Body, http::Request};
use flussonix::server::router;
use tower::ServiceExt;

async fn wait_worker(f: &fixture::Fixture, seconds: u64, previous: u64) -> bool {
    tokio::time::timeout(Duration::from_secs(seconds), async {
        loop {
            let stats = f.app.media.stats("owned").await;
            if stats["pid"]
                .as_u64()
                .is_some_and(|p| p > 0 && p != previous)
                && stats["bytes_in"].as_u64().unwrap_or(0) > 0
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn discovery_coordination_slow_static_catalog_starts_without_viewers() {
    let f =
        fixture::Fixture::with_catalog_delay(json!({"only":["owned"]}), None, false, 4500).await;
    let ready = wait_worker(&f, 8, 0).await;
    let pulls = f.origins[0].private_calls.load(Ordering::SeqCst);
    let grants = f.app.playback_auth.live_grants();
    let pid = f.app.media.stats("owned").await["pid"]
        .as_u64()
        .unwrap_or(0);
    f.stop().await;
    assert!(
        ready,
        "static discovery must finish within the shared source allowance"
    );
    assert_eq!(pulls, 1);
    assert_eq!(grants, 0);
    assert!(pid > 0 && !std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn discovery_coordination_static_first_handle_serves_foreground_sibling_after_refresh() {
    let f = fixture::Fixture::configured(
        "standalone",
        json!({"only":["owned"]}),
        Some(json!([
            {"name":"owned","named_by":"config","static":true,"groups":[]},
            {"name":"owned-alt","named_by":"config","static":false,"groups":[]}
        ])),
        false,
    )
    .await;
    let initial = wait_worker(&f, 4, 0).await;
    let pid = f.app.media.stats("owned").await["pid"]
        .as_u64()
        .unwrap_or(0);
    f.origins[0].catalog_delay.store(1500, Ordering::SeqCst);
    let polled = tokio::time::timeout(Duration::from_secs(12), async {
        while f.origins[0].catalog_calls.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok();
    // Observe the bounded periodic refresh completing (or its old750ms failure).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let status = f.get("/owned-alt/m4s?token=sibling").await.status();
    let same = f.app.media.stats("owned").await["pid"].as_u64() == Some(pid);
    let pulls = f.origins[0].private_calls.load(Ordering::SeqCst);
    f.stop().await;
    assert!(
        initial && polled && same,
        "refresh must retain the original static worker"
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "static-created handle must not shorten foreground discovery"
    );
    assert_eq!(pulls, 2);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn discovery_coordination_primary_timeout_then_slow_backup_completes_first_request() {
    let f = fixture::Fixture::with_catalog_delay(json!({"only":[]}), None, true, 4500).await;
    f.origins[0].catalog_delay.store(7000, Ordering::SeqCst);
    let began = std::time::Instant::now();
    let reply = f.get("/owned/index.m3u8?token=slow-backup").await;
    let status = reply.status();
    drop(reply);
    let elapsed = began.elapsed();
    let pulls = [
        f.origins[0].private_calls.load(Ordering::SeqCst),
        f.origins[1].private_calls.load(Ordering::SeqCst),
    ];
    f.stop().await;
    assert_eq!(
        status,
        StatusCode::OK,
        "primary timeout must leave time for complete backup discovery"
    );
    assert!(elapsed >= Duration::from_secs(9) && elapsed < Duration::from_secs(12));
    assert_eq!(pulls, [0, 1]);
}

#[tokio::test]
async fn discovery_coordination_background_recovery_outlives_old_job_deadline() {
    let f = fixture::Fixture::new().await;
    let _ = f.playlist().await;
    let old = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    f.origins[0].catalog_delay.store(7000, Ordering::SeqCst);
    f.origins[1].catalog_delay.store(4500, Ordering::SeqCst);
    f.origins[0].close_media();
    let recovered = wait_worker(&f, 15, old).await;
    let pulls = [
        f.origins[0].private_calls.load(Ordering::SeqCst),
        f.origins[1].private_calls.load(Ordering::SeqCst),
    ];
    f.stop().await;
    assert!(
        recovered,
        "background owner must finish primary plus backup discovery without a new viewer"
    );
    assert_eq!(pulls, [1, 1]);
    assert!(!std::path::Path::new(&format!("/proc/{old}")).exists());
}

#[tokio::test]
async fn discovery_coordination_slow_authoritative_denial_does_not_probe_backup() {
    let f = fixture::Fixture::with_catalog_delay(json!({"only":[]}), None, true, 4500).await;
    *f.origins[0].rows.lock().unwrap() =
        json!([{"name":"owned","named_by":"config","static":true,"disabled":true,"groups":[]}]);
    let status = f.get("/owned/index.m3u8?token=denied").await.status();
    let backup_management = f.origins[1].management_calls.load(Ordering::SeqCst);
    let media = f
        .origins
        .iter()
        .map(|o| o.private_calls.load(Ordering::SeqCst))
        .sum::<usize>();
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        backup_management, 0,
        "complete selected denial must remain an alternative barrier"
    );
    assert_eq!(media, 0);
}

async fn eight_slow_origins() -> fixture::Fixture {
    let mut f = fixture::Fixture::with_catalog_delay(json!({"only":[]}), None, true, 4500).await;
    for id in [
        "owned-origin-c",
        "owned-origin-d",
        "owned-origin-e",
        "owned-origin-f",
        "owned-origin-g",
        "owned-origin-h",
    ] {
        let origin = fixture::Origin::start(id).await;
        origin.catalog_delay.store(4500, Ordering::SeqCst);
        f.app.config.put("sources",&origin.key,json!({"flussonix_source_kind":"flussonic","cluster_key":origin.secret,"on_play":"true","only":[],"flussonix_source_group":"music-origins","flussonix_content_id":"explicit-music-v1"})).unwrap();
        f.origins.push(origin);
    }
    f.origins[0].catalog_delay.store(7000, Ordering::SeqCst);
    f
}

#[tokio::test]
async fn discovery_coordination_eight_members_finish_two_alternative_waves_in_order() {
    let f = eight_slow_origins().await;
    let began = std::time::Instant::now();
    let reply = tokio::time::timeout(
        Duration::from_secs(19),
        router(f.app.clone()).oneshot(
            Request::builder()
                .uri("/owned/index.m3u8?token=waves")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let status = reply.status();
    drop(reply);
    let elapsed = began.elapsed();
    let calls = f
        .origins
        .iter()
        .map(|o| o.private_calls.load(Ordering::SeqCst))
        .collect::<Vec<_>>();
    f.stop().await;
    assert_eq!(
        status,
        StatusCode::OK,
        "two alternative waves must fit the group selection budget"
    );
    assert!(elapsed >= Duration::from_secs(13) && elapsed < Duration::from_secs(19));
    assert_eq!(
        calls,
        vec![0, 1, 0, 0, 0, 0, 0, 0],
        "network completion must not change declared rotation order"
    );
}

#[tokio::test]
async fn discovery_coordination_stalled_eight_member_group_is_bounded_without_media() {
    let f = eight_slow_origins().await;
    for origin in &f.origins {
        origin.catalog_delay.store(7000, Ordering::SeqCst);
    }
    let began = std::time::Instant::now();
    let reply = tokio::time::timeout(
        Duration::from_secs(18),
        router(f.app.clone()).oneshot(
            Request::builder()
                .uri("/owned/index.m3u8?token=stalled")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let status = reply.status();
    drop(reply);
    let elapsed = began.elapsed();
    let pulls = f
        .origins
        .iter()
        .map(|o| o.private_calls.load(Ordering::SeqCst))
        .sum::<usize>();
    let sessions = f.app.playback_auth.snapshots().len();
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(elapsed >= Duration::from_secs(14) && elapsed < Duration::from_secs(18));
    assert_eq!(pulls, 0);
    assert_eq!(sessions, 0);
}

#[tokio::test]
async fn discovery_coordination_srt_admission_survives_two_alternative_waves() {
    #[path = "../support/vendor_srt.rs"]
    #[allow(dead_code)]
    mod transport;
    let f = eight_slow_origins().await;
    let secret = "owned-coordinated-srt-key";
    let serving = transport::Serving::start(f.app.clone(), secret).await;
    let mut caller = tokio::process::Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-xerror",
            "-nostdin",
            "-threads",
            "1",
            "-passphrase",
            secret,
            "-srt_streamid",
            "#!::r=owned,m=request,u=slow-srt",
            "-analyzeduration",
            "500000",
            "-probesize",
            "131072",
            "-i",
        ])
        .arg(format!(
            "srt://{}?mode=caller&latency=120000&connect_timeout=1000&timeout=25000000",
            serving.address
        ))
        .args(["-map", "0:a:0", "-frames:a", "3", "-f", "null", "-"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = caller.id().unwrap();
    use tokio::io::AsyncReadExt;
    let mut stderr = caller.stderr.take().unwrap();
    let errors = tokio::spawn(async move {
        let mut v = Vec::new();
        stderr.read_to_end(&mut v).await.unwrap();
        v
    });
    let success = match tokio::time::timeout(Duration::from_secs(25), caller.wait()).await {
        Ok(status) => status.unwrap().success(),
        Err(_) => {
            caller.kill().await.unwrap();
            let _ = caller.wait().await;
            false
        }
    };
    let errors = errors.await.unwrap();
    let calls = f
        .origins
        .iter()
        .map(|o| o.private_calls.load(Ordering::SeqCst))
        .collect::<Vec<_>>();
    serving.stop().await;
    f.stop().await;
    assert!(
        success && errors.is_empty(),
        "independent encrypted SRT decode failed: {}",
        String::from_utf8_lossy(&errors)
    );
    assert_eq!(calls, vec![0, 1, 0, 0, 0, 0, 0, 0]);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}
