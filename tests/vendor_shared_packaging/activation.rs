//! Static input ownership is separate from every native viewer permission.
use super::*;
use flussonix::source_directory::modern_coordinator::AdmissionFailure;
fn configure(f: &mut Fixture, patch: serde_json::Value) {
    let key = f.config.snapshot()["sources"][0]["url"]
        .as_str()
        .unwrap()
        .to_owned();
    f.config.put("sources", &key, patch).unwrap();
    f.handle = f
        .owner
        .install(
            f.owner
                .capture(&key)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap();
}
#[tokio::test]
async fn selected_static_authority_has_no_native_viewer_permission_or_session_side_effects() {
    let f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    f.owner.activated_current(&activated).unwrap();
    let control = activated.control();
    assert_eq!(f.auth.live_grants(), 0);
    assert!(f.auth.snapshots().is_empty());
    assert_eq!(f.media_calls(), 0);
    control.cancel();
    assert!(activated.is_cancelled());
    assert!(f.owner.activated_current(&activated).is_err());
}
#[tokio::test]
async fn only_selects_static_activation_without_hiding_on_demand_viewer_candidates() {
    for only in [json!([]), json!(["other"])] {
        let mut f = Fixture::start(false).await;
        configure(
            &mut f,
            json!({"only":only,"on_play":"true","flussonix_source_kind":"flussonic"}),
        );
        let rows = f.inventory(Duration::from_secs(30)).await;
        assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
        let viewer = f.admitted(&rows, "owned").await;
        assert!(!viewer.is_cancelled());
        drop(viewer);
        assert_eq!(f.auth.live_grants(), 0);
        assert_eq!(f.media_calls(), 0);
    }
}
#[tokio::test]
async fn remote_on_demand_and_disabled_local_streams_cannot_create_static_demand() {
    let f = Fixture::start(false).await;
    f.remote.lock().unwrap().streams[0]["static"] = json!(false);
    let rows = f.inventory(Duration::from_secs(30)).await;
    assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
    let mut f = Fixture::start(false).await;
    f.config
        .put("streams", NAME, json!({"disabled":true}))
        .unwrap();
    f.handle = install(
        &f.owner,
        f.config.snapshot()["sources"][0]["url"].as_str().unwrap(),
    );
    let rows = f.inventory(Duration::from_secs(30)).await;
    assert!(matches!(
        f.owner.activate(rows.candidate(NAME).unwrap()),
        Err(AdmissionFailure::LocalStream)
    ));
    assert_eq!(f.media_calls(), 0);
}
#[tokio::test]
async fn expired_foreign_and_reconfigured_static_candidates_are_rejected() {
    let f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_millis(100)).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let other = ModernSourceCoordinator::new(f.config.clone(), "owned-other-runtime").unwrap();
    assert!(other.activate(rows.candidate(NAME).unwrap()).is_err());
    let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    f.config.put("templates", "unrelated", json!({})).unwrap();
    assert!(f.owner.activated_current(&activated).is_err());
    assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
    assert_eq!(f.media_calls(), 0);
}

#[tokio::test]
async fn static_packager_starts_without_viewers_and_decodes_real_media() {
    for m4f in [false, true] {
        let mut f = Fixture::start(m4f).await;
        let rows = f.inventory(Duration::from_secs(30)).await;
        let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
        let control = activated.control();
        let registry = SharedInputs::new(f.owner.clone());
        let attachment = registry
            .attach_activation(activated, SharedInputOptions::default())
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let playback = engine.package_shared(attachment, &options()).await.unwrap();
        let conn = f.connection().await;
        let feeder = if m4f {
            conn.feed
                .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
                .unwrap();
            let segment = f.connection().await;
            let frames = (0..150)
                .map(|i| {
                    let mut f = frame();
                    f.dts += i * 2351;
                    f
                })
                .collect::<Vec<_>>();
            segment
                .feed
                .send(Ok(Bytes::from(
                    m4f::pack(&[audio()], &frames, 360000).unwrap(),
                )))
                .unwrap();
            drop(segment.feed);
            None
        } else {
            Some(tokio::spawn(feed_audio(conn.feed.clone())))
        };
        let manifest = ready(&engine, &playback).await;
        let pid = playback.pid();
        assert!(pid > 0);
        assert_eq!(f.auth.live_grants(), 0);
        assert!(f.auth.snapshots().is_empty());
        assert_eq!(
            playback.stats()["online_clients"],
            0,
            "static owner counted as a viewer"
        );
        let text = String::from_utf8(manifest.to_vec()).unwrap();
        let file = text
            .lines()
            .find(|l| !l.starts_with('#') && !l.is_empty())
            .unwrap();
        let media = engine.read_shared(&playback, file).await.unwrap();
        let decoded = dir.path().join("activated.ts");
        std::fs::write(&decoded, media).unwrap();
        decode(&decoded).await;
        control.cancel();
        assert!(playback.current().is_err());
        engine.stop_all().await;
        if let Some(feeder) = feeder {
            feeder.abort();
            let _ = feeder.await;
        }
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}
#[tokio::test]
async fn static_and_native_viewer_owners_share_one_input_and_cancel_independently() {
    for (activation_first, static_first) in
        [(true, false), (true, true), (false, false), (false, true)]
    {
        let mut f = Fixture::start(false).await;
        let rows = f.inventory(Duration::from_secs(30)).await;
        let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
        let control = activated.control();
        let registry = SharedInputs::new(f.owner.clone());
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let (active, viewer) = if activation_first {
            let active = engine
                .package_shared(
                    registry
                        .attach_activation(activated, SharedInputOptions::default())
                        .unwrap(),
                    &options(),
                )
                .await
                .unwrap();
            let viewer = engine
                .package_shared(
                    attach(&f, f.admitted(&rows, "viewer").await, &registry),
                    &options(),
                )
                .await
                .unwrap();
            (active, viewer)
        } else {
            let viewer = engine
                .package_shared(
                    attach(&f, f.admitted(&rows, "viewer").await, &registry),
                    &options(),
                )
                .await
                .unwrap();
            let active = engine
                .package_shared(
                    registry
                        .attach_activation(activated, SharedInputOptions::default())
                        .unwrap(),
                    &options(),
                )
                .await
                .unwrap();
            (active, viewer)
        };
        let conn = f.connection().await;
        let feed = conn.feed.clone();
        let feeder = tokio::spawn(feed_audio(feed));
        ready(&engine, &viewer).await;
        let pid = viewer.pid();
        assert_eq!(active.pid(), pid);
        assert_eq!(f.media_calls(), 1);
        assert_eq!(viewer.stats()["online_clients"], 1);
        let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
        if static_first {
            control.cancel();
            assert!(active.current().is_err());
            viewer.current().unwrap();
            let _ = ready(&engine, &viewer).await;
            f.auth.revoke(&id);
        } else {
            f.auth.revoke(&id);
            assert!(viewer.current().is_err());
            active.current().unwrap();
            let _ = ready(&engine, &active).await;
            control.cancel();
        }
        assert!(active.current().is_err());
        assert!(viewer.current().is_err());
        drop((active, viewer));
        tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
            .await
            .unwrap();
        engine.stop_all().await;
        feeder.abort();
        let _ = feeder.await;
        assert_eq!(f.auth.live_grants(), 0);
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}

#[tokio::test]
async fn static_owner_survives_safe_renewal_but_not_remote_demotion_or_removal() {
    for update in [
        json!([]),
        json!([{"name":NAME,"groups":[],"named_by":"config","static":false}]),
        json!([{"name":NAME,"groups":[],"named_by":"config","static":true,"disabled":true}]),
    ] {
        let f = Fixture::start(false).await;
        let rows = f.inventory(Duration::from_secs(30)).await;
        let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
        let registry = SharedInputs::new(f.owner.clone());
        let attachment = registry
            .attach_activation(activated, SharedInputOptions::default())
            .unwrap();
        let id = attachment.viewer.generation();
        assert_eq!(
            registry
                .renew(&f.handle, Duration::from_secs(30))
                .await
                .unwrap()
                .renewed,
            1
        );
        attachment.viewer.current().unwrap();
        assert_eq!(attachment.viewer.generation(), id);
        f.remote.lock().unwrap().streams = update;
        let renewed = registry
            .renew(&f.handle, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(renewed.retired, 1);
        assert!(attachment.viewer.current().is_err());
        drop(attachment);
        assert_eq!(f.media_calls(), 0);
        assert_eq!(f.auth.live_grants(), 0);
    }
}
#[tokio::test]
async fn cancelled_and_expired_static_owners_cannot_attach_or_retain_private_delivery() {
    let f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    activated.control().cancel();
    let registry = SharedInputs::new(f.owner.clone());
    assert!(
        registry
            .attach_activation(activated, SharedInputOptions::default())
            .is_err()
    );
    assert_eq!(f.media_calls(), 0);
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_millis(350)).await;
    let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    let registry = SharedInputs::new(f.owner.clone());
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let playback = engine
        .package_shared(
            registry
                .attach_activation(activated, SharedInputOptions::default())
                .unwrap(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
        .await
        .unwrap();
    assert!(playback.current().is_err());
    drop(playback);
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert_eq!(f.auth.live_grants(), 0);
}

#[tokio::test]
async fn static_warmup_never_calls_viewer_callback_and_does_not_consume_viewer_limit() {
    use axum::extract::Query;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let callback =
        Router::new().fallback(get(move |Query(query): Query<HashMap<String, String>>| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let status = if query.get("token").map(String::as_str) == Some("deny") {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::OK
                };
                Response::builder()
                    .status(status)
                    .header("X-UserId", "owned-account")
                    .body(Body::empty())
                    .unwrap()
            }
        }));
    let task = tokio::spawn(async move { axum::serve(listener, callback).await.unwrap() });
    let _abort = Abort(task.abort_handle());
    let mut f = Fixture::start(false).await;
    configure(
        &mut f,
        json!({"flussonix_source_kind":"flussonic","on_play":{"url":url,"max_sessions":1}}),
    );
    let rows = f.inventory(Duration::from_secs(30)).await;
    let activation = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    let control = activation.control();
    let registry = SharedInputs::new(f.owner.clone());
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let active = engine
        .package_shared(
            registry
                .attach_activation(activation, SharedInputOptions::default())
                .unwrap(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &active).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.auth.live_grants(), 0);
    assert!(f.auth.snapshots().is_empty());
    let deny = ViewerRequest {
        name: NAME.into(),
        proto: "hls".into(),
        ip: "127.0.0.1".into(),
        token: "deny".into(),
        ..Default::default()
    };
    assert!(matches!(
        f.owner
            .admit(rows.candidate(NAME).unwrap(), &f.auth, deny)
            .await
            .unwrap(),
        AdmissionOutcome::Denied
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.auth.live_grants(), 0);
    active.current().unwrap();
    let viewer = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "allow").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    assert_eq!(f.auth.live_grants(), 1);
    assert_eq!(viewer.stats()["online_clients"], 1);
    assert_eq!(f.media_calls(), 1);
    drop(viewer);
    control.cancel();
    drop(active);
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    task.abort();
    let _ = task.await;
    assert_eq!(f.auth.live_grants(), 0);
}
struct Abort(tokio::task::AbortHandle);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn dropping_static_owner_stops_input_even_when_cancellation_controls_are_retained() {
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let activation = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    let control = activation.control();
    let _retained_control = control.clone();
    let registry = SharedInputs::new(f.owner.clone());
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let active = engine
        .package_shared(
            registry
                .attach_activation(activation, SharedInputOptions::default())
                .unwrap(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &active).await;
    let pid = active.pid();
    drop(active);
    tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
        .await
        .expect("cancellation-only controls retained static demand");
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert_eq!(f.auth.live_grants(), 0);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}
