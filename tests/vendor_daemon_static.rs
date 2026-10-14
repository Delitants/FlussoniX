//! Owned automatic vendor activation; all fixtures and ports belong to this test.
#[path = "vendor_daemon_failover/fixture.rs"]
#[allow(dead_code)]
mod fixture;
use axum::http::StatusCode;
use fixture::Fixture;
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
async fn node(f: &Fixture) -> serde_json::Value {
    use base64::Engine;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let response = flussonix::server::router(f.app.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri("/flussonix/api/v1/node")
                .header(
                    "authorization",
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode("admin:owned-admin")
                    ),
                )
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}
async fn ready(f: &Fixture, name: &str) -> u64 {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stats = f.app.media.stats(name).await;
            if stats["bytes_in"].as_u64().unwrap_or(0) > 0 && stats["pid"].as_u64().unwrap_or(0) > 0
            {
                return stats["pid"].as_u64().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("selected static stream never started without viewer demand")
}
#[tokio::test]
async fn default_static_startup_creates_no_viewer_sessions_and_viewer_joins_one_pull() {
    let f = Fixture::configured("standalone", json!({}), None, false).await;
    let pid = ready(&f, "owned").await;
    assert!(f.app.playback_auth.snapshots().is_empty());
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    assert_eq!(f.app.media.stats("owned").await["online_clients"], 0);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    let _ = f.playlist().await;
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    for session in f.app.playback_auth.snapshots() {
        f.app.playback_auth.revoke(session["id"].as_str().unwrap());
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    f.stop().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}
#[tokio::test]
async fn selected_patterns_use_upstream_names_and_filter_exclusions_on_demand_and_disabled() {
    for (patch, row, want) in [
        (
            json!({"prefix":"remote","only":["owned"]}),
            json!({"name":"remote/owned","named_by":"config","static":true,"groups":[]}),
            true,
        ),
        (
            json!({"prefix":"remote","only":["sports/*"]}),
            json!({"name":"remote/sports/owned","named_by":"config","static":true,"groups":[]}),
            true,
        ),
        (
            json!({"only":[]}),
            json!({"name":"owned","named_by":"config","static":true,"groups":[]}),
            false,
        ),
        (
            json!({"only":["other"]}),
            json!({"name":"owned","named_by":"config","static":true,"groups":[]}),
            false,
        ),
        (
            json!({"except":["owned"]}),
            json!({"name":"owned","named_by":"config","static":true,"groups":[]}),
            false,
        ),
        (
            json!({}),
            json!({"name":"owned","named_by":"config","static":false,"groups":[]}),
            false,
        ),
        (
            json!({}),
            json!({"name":"owned","named_by":"config","static":true,"disabled":true,"groups":[]}),
            false,
        ),
    ] {
        let name = row["name"].as_str().unwrap().to_owned();
        let f = Fixture::configured("standalone", patch, Some(json!([row])), false).await;
        if want {
            ready(&f, &name).await;
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert_eq!(f.app.media.count().await, 0);
        }
        assert_eq!(
            f.origins[0].private_calls.load(Ordering::SeqCst),
            usize::from(want)
        );
        f.stop().await;
    }
}
#[tokio::test]
async fn on_demand_and_lb_do_no_eager_discovery_or_media_work() {
    for (role, patch) in [("standalone", json!({"only":[]})), ("lb", json!({}))] {
        let f = Fixture::configured(role, patch, None, true).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(f.app.media.count().await, 0);
        assert!(
            f.origins
                .iter()
                .all(|o| o.management_calls.load(Ordering::SeqCst) == 0
                    && o.private_calls.load(Ordering::SeqCst) == 0)
        );
        f.stop().await;
    }
}
#[tokio::test]
async fn static_demand_does_not_bypass_viewer_token_guard() {
    let f = Fixture::configured(
        "standalone",
        json!({"flussonix_token_sha256":"a".repeat(64)}),
        None,
        true,
    )
    .await;
    ready(&f, "owned").await;
    assert_eq!(
        f.get("/owned/index.m3u8?token=wrong").await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    assert!(f.app.playback_auth.snapshots().is_empty());
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn static_origin_eof_recovers_without_any_viewer_and_remains_sticky() {
    let f = Fixture::configured("standalone", json!({}), None, true).await;
    let old = ready(&f, "owned").await;
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let p = ready(&f, "owned").await;
            if p != old {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.app.playback_auth.snapshots().is_empty());
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    let _ = f.playlist().await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
    assert!(!std::path::Path::new(&format!("/proc/{old}")).exists());
}
#[tokio::test]
async fn static_source_catalog_renews_beyond_original_lease_without_restarting() {
    let f = Fixture::configured("standalone", json!({}), None, false).await;
    let pid = ready(&f, "owned").await;
    tokio::time::sleep(Duration::from_secs(32)).await;
    f.app.reconcile().await;
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert!(f.origins[0].management_calls.load(Ordering::SeqCst) >= 6);
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    assert!(f.app.playback_auth.snapshots().is_empty());
    f.stop().await;
}
#[tokio::test]
async fn static_demotion_stops_unviewed_media_without_rotating_to_backup() {
    let f = Fixture::configured("standalone", json!({}), None, true).await;
    let pid = ready(&f, "owned").await;
    f.origins[0].rows.lock().unwrap()[0]["static"] = json!(false);
    tokio::time::timeout(Duration::from_secs(13), async {
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("demoted static owner kept its packager");
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    let _ = f.playlist().await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}
#[tokio::test]
async fn config_exclusion_during_held_static_discovery_never_pulls_obsolete_media() {
    // Config is updated before starting a held poll; the stale completion must not create demand.
    let f = Fixture::new().await;
    let (started, wait) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *f.origins[0].hold.lock().unwrap() = Some((started, resume));
    let mut root = f.app.config.snapshot();
    for s in root["sources"].as_array_mut().unwrap() {
        s.as_object_mut().unwrap().remove("only");
    }
    f.app.config.replace(root).unwrap();
    tokio::time::timeout(Duration::from_secs(4), wait)
        .await
        .unwrap()
        .unwrap();
    let mut root = f.app.config.snapshot();
    for s in root["sources"].as_array_mut().unwrap() {
        s["except"] = json!(["owned"]);
    }
    f.app.config.replace(root).unwrap();
    let _ = release.send(());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    f.stop().await;
}

#[tokio::test]
async fn static_warmup_never_calls_viewer_backend_and_denied_viewer_stays_denied() {
    use axum::{Router, routing::get};
    use std::sync::{Arc, atomic::AtomicUsize};
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/check", tcp.local_addr().unwrap());
    let callback = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().route(
                "/check",
                get(move || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        StatusCode::FORBIDDEN
                    }
                }),
            ),
        )
        .await
        .unwrap()
    });
    let f = Fixture::configured(
        "standalone",
        json!({"on_play":{"url":url,"max_sessions":1}}),
        None,
        false,
    )
    .await;
    ready(&f, "owned").await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
    callback.abort();
    let _ = callback.await;
}
#[tokio::test]
async fn static_private_denial_cannot_be_erased_by_catalog_success_or_api_outage() {
    let f = Fixture::new().await;
    *f.origins[0].media.lock().unwrap() = StatusCode::FORBIDDEN;
    let mut root = f.app.config.snapshot();
    for source in root["sources"].as_array_mut().unwrap() {
        source.as_object_mut().unwrap().remove("only");
    }
    f.app.config.replace(root).unwrap();
    f.wait_private(0, 1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    *f.origins[0].api.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    assert!(f.app.playback_auth.snapshots().is_empty());
    f.stop().await;
}
#[tokio::test]
async fn static_output_template_converts_audio_and_remains_shared_with_viewer() {
    use http_body_util::BodyExt;
    let f = Fixture::configured(
        "cdn",
        json!({"flussonix_output_template":"owned-aac"}),
        None,
        true,
    )
    .await;
    let pid = ready(&f, "owned").await;
    let response = f.get("/owned/m4s").await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let mut decoder = flussonix::m4s::Decoder::default();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = body.frame().await.unwrap().unwrap();
            if let Ok(bytes) = frame.into_data() {
                for event in decoder.push(&bytes).unwrap() {
                    if let flussonix::m4s::Event::Info { tracks, .. } = event {
                        assert_eq!(tracks[0].codec, "aac");
                        return;
                    }
                }
            }
        }
    })
    .await
    .unwrap();
    drop(body);
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}
#[tokio::test]
async fn configured_disabled_local_stream_wins_before_static_discovery() {
    let f = Fixture::new().await;
    let mut root = f.app.config.snapshot();
    for s in root["sources"].as_array_mut().unwrap() {
        s.as_object_mut().unwrap().remove("only");
    }
    root["streams"] = json!([{"name":"owned","disabled":true}]);
    f.app.config.replace(root).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(f.app.media.count().await, 0);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    f.stop().await;
}
#[tokio::test]
async fn malformed_complete_catalog_does_not_start_an_earlier_valid_row() {
    let f=Fixture::configured("standalone",json!({}),Some(json!([{"name":"owned","named_by":"config","static":true,"groups":[]},{"name":"bad","named_by":"config","static":"yes","groups":[]}])),false).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(f.app.media.count().await, 0);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    f.stop().await;
}

#[tokio::test]
async fn viewer_recovery_continues_when_only_backup_selects_static_activation() {
    let mut f = Fixture::new().await;
    let third = fixture::Origin::start("owned-origin-c").await;
    f.app.config.put("sources",&third.key,json!({"flussonix_source_kind":"flussonic","cluster_key":third.secret,"on_play":"true","flussonix_source_group":"music-origins","flussonix_content_id":"explicit-music-v1"})).unwrap();
    f.origins.push(third);
    let _ = f.playlist().await;
    tokio::time::timeout(Duration::from_secs(4), async {
        while node(&f).await["vendor_source_activation"]["wanted_streams"] != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("static intent not published");
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    let _ = f.playlist().await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}

#[tokio::test]
async fn viewer_recovery_deadline_wins_over_ineligible_static_backoff() {
    let mut f = Fixture::new().await;
    let third = fixture::Origin::start("owned-origin-c").await;
    f.app.config.put("sources",&third.key,json!({"flussonix_source_kind":"flussonic","cluster_key":third.secret,"on_play":"true","flussonix_source_group":"music-origins","flussonix_content_id":"explicit-music-v1"})).unwrap();
    f.origins.push(third);
    let _ = f.playlist().await;
    tokio::time::timeout(Duration::from_secs(4), async {
        while node(&f).await["vendor_source_activation"]["wanted_streams"] != 1 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("static intent not published");
    // Healthy retries occur near 0, 1, 3, 7 and 15 seconds. The next static
    // attempt is around31s; viewer recovery must follow its own earlier deadline.
    tokio::time::sleep(Duration::from_secs(19)).await;
    f.origins[0].close_media();
    f.wait_private(1, 1).await;
    let _ = f.playlist().await;
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    f.stop().await;
}
