use super::*;

#[tokio::test]
async fn protected_static_media_never_authorizes_a_denied_rtsp_viewer() {
    let mut patch = policy();
    patch.as_object_mut().unwrap().remove("only");
    let f = Fixture::configured("cdn", patch, None, false).await;
    f.wait_private(0, 1).await;
    let (url, cancel, task) = listener(&f).await;
    let mut denied = connect(&url).await;
    assert_eq!(
        request(&mut denied, "DESCRIBE", &format!("{url}?token=wrong"), "")
            .await
            .0,
        403
    );
    assert!(
        f.app
            .playback_auth
            .snapshots()
            .iter()
            .all(|r| r["is_open"] != true)
    );
    let mut viewer = connect(&url).await;
    play(&mut viewer, &url).await;
    let _ = packet(&mut viewer).await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    drop(viewer);
    drop(denied);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn rtsp_uses_saved_cpu_aac_template_and_shares_http_worker() {
    let f = Fixture::configured(
        "cdn",
        json!({"only":[],"flussonix_output_template":"owned-aac"}),
        None,
        false,
    )
    .await;
    let (url, cancel, task) = listener(&f).await;
    let mut viewer = connect(&url).await;
    let (code, _, body) = request(
        &mut viewer,
        "DESCRIBE",
        &format!("{url}?token=owned-viewer"),
        "",
    )
    .await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    assert!(sdp.contains("MPEG4-GENERIC/"), "{sdp}");
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    setup_play(&mut viewer, &url, id).await;
    let body = packet(&mut viewer).await;
    assert_eq!(body[0] >> 6, 2);
    assert!((96..=127).contains(&(body[1] & 0x7f)));
    let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    assert_eq!(f.get("/owned/index.m3u8?token=http").await.status(), 200);
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    drop(viewer);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn source_edit_during_metadata_startup_cannot_publish_stale_sdp() {
    let f = Fixture::configured("standalone", policy(), None, false).await;
    f.origins[0].stall_metadata.store(true, Ordering::SeqCst);
    let (url, cancel, task) = listener(&f).await;
    let request_url = url.clone();
    let pending = tokio::spawn(async move {
        let mut viewer = connect(&request_url).await;
        request(
            &mut viewer,
            "DESCRIBE",
            &format!("{request_url}?token=owned-viewer"),
            "",
        )
        .await
        .0
    });
    f.wait_private(0, 1).await;
    let key = &f.origins[0].key;
    f.app
        .config
        .put("sources", key, json!({"except":["owned"]}))
        .unwrap();
    let code = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code, 503);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn revoking_one_rtsp_session_keeps_another_viewer_and_static_owner() {
    let f = Fixture::configured("cdn", json!({"on_play":"true"}), None, false).await;
    f.wait_private(0, 1).await;
    let (url, cancel, task) = listener(&f).await;
    let mut a = connect(&url).await;
    play_token(&mut a, &url, "a").await;
    let _ = packet(&mut a).await;
    let first = f.app.playback_auth.snapshots()[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut b = connect(&url).await;
    play_token(&mut b, &url, "b").await;
    let _ = packet(&mut b).await;
    let rows = f.app.playback_auth.snapshots();
    assert_eq!(rows.len(), 2);
    assert!(f.app.playback_auth.revoke(&first));
    tokio::time::sleep(Duration::from_millis(350)).await;
    let live = f
        .app
        .playback_auth
        .snapshots()
        .into_iter()
        .filter(|row| row["is_open"] == true)
        .count();
    assert_eq!(live, 1);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.app.media.stats("owned").await["online_clients"], 1);
    closed(&mut a).await;
    let result = packet(&mut b).await;
    assert!(result.len() > 16);
    drop(a);
    drop(b);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn source_exclusion_closes_existing_rtsp_and_never_falls_back_to_native() {
    let f = Fixture::configured("standalone", policy(), None, true).await;
    let (url, cancel, task) = listener(&f).await;
    let mut viewer = connect(&url).await;
    play(&mut viewer, &url).await;
    let _ = packet(&mut viewer).await;
    let mut root = f.app.config.snapshot();
    for source in root["sources"].as_array_mut().unwrap() {
        source["except"] = json!(["owned"]);
    }
    f.app.config.replace(root).unwrap();
    closed(&mut viewer).await;
    let mut next = connect(&url).await;
    assert_eq!(
        request(
            &mut next,
            "DESCRIBE",
            &format!("{url}?token=owned-viewer"),
            ""
        )
        .await
        .0,
        404
    );
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    drop(viewer);
    drop(next);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn private_eof_retires_rtsp_and_fresh_viewer_uses_background_recovered_replica() {
    let f = Fixture::configured("standalone", policy(), None, true).await;
    let (url, cancel, task) = listener(&f).await;
    let mut viewer = connect(&url).await;
    play(&mut viewer, &url).await;
    let _ = packet(&mut viewer).await;
    f.origins[0].close_media();
    closed(&mut viewer).await;
    f.wait_private(1, 1).await;
    let mut next = connect(&url).await;
    play(&mut next, &url).await;
    let _ = packet(&mut next).await;
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    drop(viewer);
    drop(next);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn lb_and_foreign_native_tickets_never_start_vendor_media() {
    for role in ["lb", "standalone"] {
        let f = Fixture::configured(role, policy(), None, false).await;
        let (url, cancel, task) = listener(&f).await;
        let mut viewer = connect(&url).await;
        let suffix = if role == "lb" {
            ""
        } else {
            "&flussonix_ticket=00000000-0000-0000-0000-000000000000"
        };
        assert_eq!(
            request(
                &mut viewer,
                "DESCRIBE",
                &format!("{url}?token=owned-viewer{suffix}"),
                ""
            )
            .await
            .0,
            503
        );
        assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.app.media.count().await, 0);
        drop(viewer);
        cancel.cancel();
        task.await.unwrap().unwrap();
        f.stop().await;
    }
}

#[tokio::test]
async fn disabled_local_stream_takes_precedence_over_vendor_namespace() {
    let f = Fixture::configured("standalone", policy(), None, false).await;
    f.app
        .config
        .put(
            "streams",
            "owned",
            json!({"disabled":true,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let (url, cancel, task) = listener(&f).await;
    let mut viewer = connect(&url).await;
    assert_eq!(
        request(
            &mut viewer,
            "DESCRIBE",
            &format!("{url}?token=owned-viewer"),
            ""
        )
        .await
        .0,
        404
    );
    assert_eq!(f.origins[0].management_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    drop(viewer);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}
