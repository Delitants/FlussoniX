use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flussonix::{
    iptv::store::IptvStore,
    media::Engine,
    playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest},
    rtsp,
    server::{App, Options, router},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn setup() -> (tempfile::TempDir, Arc<IptvStore>, Arc<PlaybackAuth>) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("state")).unwrap();
    let store = IptvStore::open(dir.path().join("state/iptv.json")).unwrap();
    store.set_enabled(true).unwrap();
    store
        .put_package("basic", json!({"channels":["news","sports"]}))
        .unwrap();
    store
        .put_subscriber(
            "alice",
            json!({"token":"old-token","packages":["basic"],"max_sessions":2}),
        )
        .unwrap();
    let auth = Arc::new(PlaybackAuth::with_iptv(10, store.clone()));
    for name in ["news", "sports"] {
        auth.publish(
            name,
            Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
        );
    }
    let weak = Arc::downgrade(&auth);
    store.set_on_change(Box::new(move || {
        if let Some(auth) = weak.upgrade() {
            auth.invalidate_iptv();
        }
    }));
    (dir, store, auth)
}

async fn grant(auth: &PlaybackAuth, name: &str, token: &str) -> AuthOutcome {
    let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
    let snapshot = auth.publish(name, Some(policy)).unwrap();
    auth.authorize(
        snapshot,
        ViewerRequest {
            name: name.into(),
            proto: "mpegts".into(),
            token: token.into(),
            ..Default::default()
        },
    )
    .await
}

#[tokio::test]
async fn token_rotation_cancels_old_grant_and_requires_current_token() {
    let (_dir, store, auth) = setup();
    let AuthOutcome::Allowed(first) = grant(&auth, "news", "old-token").await else {
        panic!("initial grant");
    };
    store
        .put_subscriber("alice", json!({"token":"new-token"}))
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), first.cancelled())
        .await
        .expect("old viewer cancellation");
    assert!(matches!(
        grant(&auth, "news", "old-token").await,
        AuthOutcome::Denied
    ));
    assert!(matches!(
        grant(&auth, "news", "new-token").await,
        AuthOutcome::Allowed(_)
    ));
}

#[tokio::test]
async fn package_and_subscriber_deletion_cancel_live_grants() {
    let (_dir, store, auth) = setup();
    let AuthOutcome::Allowed(first) = grant(&auth, "news", "old-token").await else {
        panic!("initial grant");
    };
    store.delete_package("basic").unwrap();
    tokio::time::timeout(Duration::from_millis(100), first.cancelled())
        .await
        .expect("package revocation");
    assert!(matches!(
        grant(&auth, "news", "old-token").await,
        AuthOutcome::Denied
    ));
    store
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    store
        .put_subscriber("alice", json!({"packages":["basic"]}))
        .unwrap();
    let AuthOutcome::Allowed(second) = grant(&auth, "news", "old-token").await else {
        panic!("restored grant");
    };
    store.delete_subscriber("alice").unwrap();
    tokio::time::timeout(Duration::from_millis(100), second.cancelled())
        .await
        .expect("subscriber revocation");
}

#[tokio::test]
async fn lower_session_limit_and_disable_cancel_stale_allowances() {
    let (_dir, store, auth) = setup();
    let AuthOutcome::Allowed(first) = grant(&auth, "news", "old-token").await else {
        panic!("first grant");
    };
    let AuthOutcome::Allowed(second) = grant(&auth, "sports", "old-token").await else {
        panic!("second grant");
    };
    store
        .put_subscriber("alice", json!({"max_sessions":1}))
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), first.cancelled())
        .await
        .expect("limit edit cancellation");
    tokio::time::timeout(Duration::from_millis(100), second.cancelled())
        .await
        .expect("limit edit cancellation");
    drop(first);
    drop(second);
    assert!(matches!(
        grant(&auth, "news", "old-token").await,
        AuthOutcome::Allowed(_)
    ));
    store.set_enabled(false).unwrap();
    assert!(matches!(
        grant(&auth, "sports", "old-token").await,
        AuthOutcome::Denied
    ));
}

#[tokio::test]
async fn failed_edit_leaves_existing_grant_usable() {
    let (_dir, store, auth) = setup();
    let AuthOutcome::Allowed(first) = grant(&auth, "news", "old-token").await else {
        panic!("initial grant");
    };
    let before = store.revision();
    assert!(
        store
            .put_subscriber("alice", json!({"packages":["missing"]}))
            .is_err()
    );
    assert_eq!(store.revision(), before);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), first.cancelled())
            .await
            .is_err()
    );
    assert!(matches!(
        grant(&auth, "news", "old-token").await,
        AuthOutcome::Allowed(_)
    ));
}

#[tokio::test]
async fn failed_disk_save_does_not_cancel_existing_grant_or_advance_revision() {
    let (dir, store, auth) = setup();
    let AuthOutcome::Allowed(first) = grant(&auth, "news", "old-token").await else {
        panic!("initial grant");
    };
    let before = store.revision();
    let state = dir.path().join("state");
    let moved = dir.path().join("state-moved");
    std::fs::rename(&state, &moved).unwrap();
    std::fs::write(&state, b"blocks directory creation").unwrap();
    let result = store.put_subscriber("alice", json!({"token":"replacement"}));
    std::fs::remove_file(&state).unwrap();
    std::fs::rename(&moved, &state).unwrap();
    assert!(result.is_err());
    assert_eq!(store.revision(), before);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), first.cancelled())
            .await
            .is_err()
    );
    assert!(matches!(
        grant(&auth, "news", "old-token").await,
        AuthOutcome::Allowed(_)
    ));
}

#[test]
fn stale_revision_cannot_publish_after_subscriber_edit() {
    let (_dir, store, _auth) = setup();
    let old = store.revision();
    store
        .put_subscriber("alice", json!({"token":"new-token"}))
        .unwrap();
    assert!(store.at_current(old, || true).is_none());
    assert_eq!(store.at_current(store.revision(), || true), Some(true));
}

#[test]
fn successful_edit_waits_for_an_inflight_revision_guard() {
    let (_dir, store, _auth) = setup();
    let revision = store.revision();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let reader_store = store.clone();
    let reader = std::thread::spawn(move || {
        reader_store.at_current(revision, || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            true
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let writer_store = store.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let result = writer_store.put_subscriber("alice", json!({"token":"new-token"}));
        done_tx.send(result).unwrap();
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
    release_tx.send(()).unwrap();
    assert_eq!(reader.join().unwrap(), Some(true));
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok()
    );
    writer.join().unwrap();
    assert!(store.at_current(revision, || true).is_none());
}

#[tokio::test]
async fn expiry_boundary_denies_and_renewal_cancels_live_grant() {
    let (_dir, store, auth) = setup();
    let expiry = (chrono::Utc::now() + chrono::Duration::milliseconds(350))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    store
        .put_subscriber("alice", json!({"flussonix_expires_at":expiry}))
        .unwrap();
    let AuthOutcome::Allowed(first) = grant(&auth, "news", "old-token").await else {
        panic!("initial grant");
    };
    tokio::time::sleep(Duration::from_millis(450)).await;
    auth.renew_due().await;
    tokio::time::timeout(Duration::from_millis(100), first.cancelled())
        .await
        .expect("expired playback cancellation");
    assert!(matches!(
        grant(&auth, "news", "old-token").await,
        AuthOutcome::Denied
    ));
}

#[tokio::test]
async fn app_store_edit_closes_live_http_viewer_but_preserves_public_viewer() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("c.json"),
        dir.path().join("media"),
        Options {
            admin_password: "admin-secret-123".into(),
            peer_key: "peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for (name, protected) in [("news", true), ("free", false)] {
        let mut cfg = json!({"inputs":[{"url":"testsrc://"}]});
        if protected {
            cfg["on_play"] = json!("iptv://");
        }
        app.config.put("streams", name, cfg).unwrap();
    }
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    app.iptv
        .put_subscriber("alice", json!({"token":"old-token","packages":["basic"]}))
        .unwrap();
    let request = |path: &str| Request::builder().uri(path).body(Body::empty()).unwrap();
    let protected = router(app.clone())
        .oneshot(request("/news/mpegts?token=old-token"))
        .await
        .unwrap();
    let public = router(app.clone())
        .oneshot(request("/free/mpegts"))
        .await
        .unwrap();
    assert_eq!(protected.status(), StatusCode::OK);
    assert_eq!(public.status(), StatusCode::OK);
    app.iptv
        .put_subscriber("alice", json!({"token":"new-token"}))
        .unwrap();
    let mut body = protected.into_body().into_data_stream();
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        while body.next().await.is_some() {}
    })
    .await
    .is_ok();
    assert!(ended, "old HTTP playback body must close");
    assert!(
        app.playback_auth
            .snapshots()
            .iter()
            .any(|row| row["name"] == "free" && row["is_open"] == true)
    );
    drop(public);
    app.media.stop_all().await;
}

#[tokio::test]
async fn tokenless_push_and_rtp_outputs_cannot_start_on_iptv_stream() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    for extra in [
        json!({"flussonix_rtp_outputs":[{"url":"rtp://127.0.0.1:39000"}]}),
        json!({"pushes":[{"url":"srt://127.0.0.1:39001"}]}),
    ] {
        let mut cfg = json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"});
        for (key, value) in extra.as_object().unwrap() {
            cfg[key] = value.clone();
        }
        let result = engine.ensure("protected", &cfg).await;
        engine.stop_all().await;
        let error = match result {
            Ok(_) => panic!("tokenless output started on IPTV stream"),
            Err(error) => error,
        };
        assert!(error.contains("subscriber token"), "{error}");
    }
    assert_eq!(engine.count().await, 0);
    engine.stop_all().await;
}

#[tokio::test]
async fn live_rtsp_describe_session_closes_when_subscriber_token_rotates() {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("c.json"),
        dir.path().join("media"),
        Options {
            admin_password: "admin-secret-123".into(),
            peer_key: "peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    app.iptv
        .put_subscriber("alice", json!({"token":"old-token","packages":["basic"]}))
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(rtsp::serve(listener, app.clone(), stop.clone()));
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("DESCRIBE rtsp://{address}/news?token=old-token RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n").as_bytes()).await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !reply.ends_with(b"\r\n\r\n") {
            reply.push(socket.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    let headers = String::from_utf8(reply).unwrap();
    assert!(headers.starts_with("RTSP/1.0 200"), "{headers}");
    let length: usize = headers
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse()
        .unwrap();
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await.unwrap();
    app.iptv
        .put_subscriber("alice", json!({"token":"new-token"}))
        .unwrap();
    let mut byte = [0u8];
    let closed = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed, 0, "revocation must close the RTSP socket");
    stop.cancel();
    task.await.unwrap().unwrap();
    app.media.stop_all().await;
}
