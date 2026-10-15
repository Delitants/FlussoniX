use super::*;
use crate::{
    media::Engine,
    playback_auth::ViewerRequest,
    server::{App, Options, router},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tower::ServiceExt;

async fn candidate(controller: &Controller) -> (Instant, Ticket) {
    drop(controller.activity().await);
    let now = Instant::now();
    controller.inner.state.lock().unwrap().idle_since = Some(now);
    let when = now + Duration::from_secs(5);
    (
        when,
        controller.reserve(when, CancellationToken::new()).unwrap(),
    )
}
fn engine(dir: &std::path::Path, controller: &Controller, ffmpeg: &str) -> Engine {
    let mut engine = Engine::new(dir, ffmpeg);
    engine.idle_memory = controller.clone();
    engine
}

#[tokio::test]
async fn distinct_engines_share_the_process_wide_media_fence() {
    let dir = tempfile::tempdir().unwrap();
    let a = Engine::new(dir.path().join("a"), "ffmpeg");
    let b = Engine::new(dir.path().join("b"), "ffmpeg");
    let activity = a.idle_memory.activity().await;
    assert!(
        b.idle_memory.snapshot()["active_operations"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert!(
        b.idle_memory.inner.gate.try_write().is_err(),
        "another engine cannot trim a live engine's heap"
    );
    drop(activity);
}

#[tokio::test]
async fn worker_reference_still_blocks_cleanup_after_map_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let controller = test_support::controller();
    let engine = engine(dir.path(), &controller, "ffmpeg");
    let worker = engine
        .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
        .await
        .unwrap();
    let active = controller.snapshot()["active_operations"].as_u64().unwrap();
    engine.stop("owned").await;
    assert_eq!(engine.count().await, 0);
    let retired_blocked = controller
        .reserve(
            Instant::now() + Duration::from_secs(600),
            CancellationToken::new(),
        )
        .is_none();
    drop(worker);
    assert_eq!(controller.snapshot()["active_operations"], 0);
    assert!(
        controller
            .reserve(
                Instant::now() + Duration::from_secs(6),
                CancellationToken::new()
            )
            .is_some()
    );
    assert!(active > 0);
    assert!(
        retired_blocked,
        "retired worker still has an external owner"
    );
}

#[tokio::test]
async fn unpublished_native_start_invalidates_idle_release_before_currentness_wait() {
    let dir = tempfile::tempdir().unwrap();
    let controller = test_support::controller();
    let engine = engine(dir.path(), &controller, "ffmpeg");
    let (when, ticket) = candidate(&controller).await;
    let config = json!({"inputs":[{"url":"testsrc://"}]});
    let mut start = Box::pin(engine.ensure_guarded("owned", &config, true, std::future::pending()));
    assert!(futures_util::poll!(&mut start).is_pending());
    assert_eq!(controller.snapshot()["active_operations"], 1);
    assert!(!ticket.execute(when, || false));
    drop(start);
    assert_eq!(controller.snapshot()["active_operations"], 0);
    assert_eq!(engine.count().await, 0);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn gpu_preparation_is_tracked_before_the_worker_map_is_locked() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let controller = test_support::controller();
    let marker = dir.path().join("probe.pid");
    let probe = dir.path().join("owned-memory-probe");
    std::fs::write(&probe, format!("#!/usr/bin/python3\nimport os,time,pathlib\npathlib.Path({:?}).write_text(str(os.getpid()))\nwhile True: time.sleep(.01)\n", marker.to_str().unwrap())).unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = engine(
        &dir.path().join("media"),
        &controller,
        probe.to_str().unwrap(),
    );
    let (when, ticket) = candidate(&controller).await;
    let config = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"h264_nvenc"}});
    let mut start = Box::pin(engine.ensure("owned", &config));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                _ = &mut start => panic!("owned probe unexpectedly completed"),
                _ = tokio::time::sleep(Duration::from_millis(5)) => if marker.exists() { break; },
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        engine.count().await,
        0,
        "probe runs before a worker is installed"
    );
    let active = controller.snapshot()["active_operations"].clone();
    let prevented = !ticket.execute(when, || false);
    let pid: u32 = std::fs::read_to_string(marker).unwrap().parse().unwrap();
    drop(start);
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(controller.snapshot()["active_operations"], 0);
    assert_eq!(active, 1);
    assert!(prevented);
}

async fn pending_authorization(protocol: &str) {
    let dir = tempfile::tempdir().unwrap();
    let controller = test_support::controller();
    let mut app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-idle-admin".into(),
            peer_key: "owned-idle-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    Arc::get_mut(&mut app).unwrap().media.idle_memory = controller.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback = format!("http://{}/auth", listener.local_addr().unwrap());
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":callback}),
        )
        .unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0; 1024];
        while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        let _ = entered_tx.send(());
        let _ = finish_rx.await;
        let _ = socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
    }));
    let (when, ticket) = candidate(&controller).await;
    let mut admission: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>> =
        match protocol {
            "http" => Box::pin(async {
                let _ = router(app.clone())
                    .oneshot(
                        axum::http::Request::builder()
                            .uri("/owned/mpegts")
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await;
            }),
            "rtsp" => Box::pin(async {
                let viewer = ViewerRequest {
                    name: "owned".into(),
                    proto: "rtsp".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                };
                let _ = app
                    .rtsp_admit(
                        viewer,
                        &url::Url::parse("rtsp://127.0.0.1/owned").unwrap(),
                        false,
                    )
                    .await;
            }),
            "srt" => Box::pin(async {
                let viewer = ViewerRequest {
                    name: "owned".into(),
                    proto: "srt".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                };
                let _ = app.ts_admit(viewer).await;
            }),
            _ => unreachable!(),
        };
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! { _=&mut admission=>panic!("admission finished before held auth callback"), result=entered_rx=>result.unwrap() }
    }).await.unwrap();
    assert_eq!(app.media.count().await, 0);
    let active = controller.snapshot()["active_operations"].as_u64().unwrap();
    let prevented = !ticket.execute(when, || false);
    drop(admission);
    let _ = finish_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(controller.snapshot()["active_operations"], 0);
    assert!(
        active > 0,
        "{protocol} authorization must inhibit cleanup before any worker exists"
    );
    assert!(prevented, "held {protocol} admission cannot be compacted");
}
#[tokio::test]
async fn held_http_authorization_blocks_idle_cleanup() {
    pending_authorization("http").await;
}
#[tokio::test]
async fn held_rtsp_authorization_blocks_idle_cleanup() {
    pending_authorization("rtsp").await;
}
#[tokio::test]
async fn held_srt_authorization_blocks_idle_cleanup() {
    pending_authorization("srt").await;
}
