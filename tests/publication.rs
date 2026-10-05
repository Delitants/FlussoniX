use flussonix::{media::Engine, publish::Policy};
use serde_json::json;
use std::{sync::Arc, time::Duration};
fn config() -> serde_json::Value {
    json!({"inputs":[{"url":"publish://"}]})
}
#[test]
fn publisher_policy_is_separate_and_resolves_named_backend() {
    let root = json!({"auth_backends":[{"name":"billing","url":"https://auth.example/publish"}]});
    let p=Policy::from_config(&json!({"password":"owned-password","on_publish":"auth://billing","on_play":"https://viewer.example/auth"}), &root).unwrap();
    assert!(p.accepts_password("owned-password"));
    assert!(!p.accepts_password(""));
    assert!(!p.accepts_password("owned-password "));
    assert_eq!(p.url.as_deref(), Some("https://auth.example/publish"));
}
#[tokio::test]
async fn publication_is_fenced_exclusive_owned_and_reconnects_immediately() {
    let d = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(d.path(), "ffmpeg"));
    let cfg = config();
    assert!(engine.ensure("owned", &cfg).await.is_err());
    assert_eq!(
        engine.count().await,
        0,
        "a viewer cannot start an empty publication"
    );
    assert!(
        engine
            .publish_guarded("owned", &cfg, std::future::ready(false))
            .await
            .is_err()
    );
    assert_eq!(engine.count().await, 0);
    let p = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .expect("publisher starts worker");
    let old = p.worker.clone();
    assert!(p.stdin.is_some());
    assert_eq!(engine.ensure("owned", &cfg).await.unwrap().pid(), old.pid());
    assert!(
        engine
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .is_err(),
        "second publisher must not steal stdin"
    );
    drop(p);
    tokio::time::timeout(Duration::from_secs(3), old.closed())
        .await
        .unwrap();
    let replacement = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .expect("reconnect without pull backoff");
    assert_ne!(replacement.worker.pid(), old.pid());
    engine.stop_if_current("owned", &old).await;
    assert!(
        !replacement.worker.is_closed(),
        "late cleanup cannot stop replacement"
    );
    engine.stop_all().await;
    assert!(replacement.worker.is_closed());
}
use axum::{body::Body, http::Request};
use flussonix::server::{App, Options, router};
use tower::ServiceExt;
fn app(d: &std::path::Path) -> Arc<App> {
    App::new(
        d.join("config.json"),
        d.join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap()
}
fn post(uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .body(body)
        .unwrap()
}
async fn until(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("condition deadline");
}
#[tokio::test]
async fn denied_publications_never_poll_body_or_start_workers() {
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config
        .put(
            "streams",
            "owned",
            json!({"inputs":[{"url":"publish://"}],"password":"owned-publisher"}),
        )
        .unwrap();
    a.config
        .put(
            "streams",
            "pull",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    for (uri, code) in [
        ("/owned/mpegts", 403),
        ("/owned/mpegts?password=wrong", 403),
        ("/owned/mpegts?password=owned-publisher&password=wrong", 400),
        ("/missing/mpegts", 404),
        ("/pull/mpegts", 403),
    ] {
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = polled.clone();
        let body = Body::from_stream(futures_util::stream::once(async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"ignored"))
        }));
        let mut r = post(uri, body);
        r.headers_mut()
            .insert("x-flussonix-peer", "owned-peer-secret".parse().unwrap());
        let response = router(a.clone()).oneshot(r).await.unwrap();
        assert_eq!(response.status(), code, "{uri}");
        assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(a.media.count().await, 0);
    }
}
fn transport_packet() -> bytes::Bytes {
    let mut b = vec![0xff; 188];
    b[0] = 0x47;
    b[1] = 0x1f;
    b[2] = 0xff;
    b[3] = 0x10;
    bytes::Bytes::from(b)
}
async fn upload(
    a: Arc<App>,
    uri: &str,
) -> (
    tokio::sync::mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    tokio::task::JoinHandle<axum::response::Response>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let r = post(
        uri,
        Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)),
    );
    let t = tokio::spawn(async move { router(a).oneshot(r).await.unwrap() });
    (tx, t)
}
#[tokio::test]
async fn publisher_conflict_policy_edit_and_request_drop_cancel_exact_worker() {
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config
        .put(
            "streams",
            "owned",
            json!({"inputs":[{"url":"publish://"}],"password":"owned-publisher"}),
        )
        .unwrap();
    let (tx, t) = upload(a.clone(), "/owned/mpegts?password=owned-publisher").await;
    tx.send(Ok(transport_packet())).await.unwrap();
    until(async || a.media.count().await == 1).await;
    let pid = a.media.stats("owned").await["pid"].clone();
    assert_eq!(
        router(a.clone())
            .oneshot(post(
                "/owned/mpegts?password=owned-publisher",
                Body::empty()
            ))
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(a.media.stats("owned").await["pid"], pid);
    a.config
        .put("streams", "owned", json!({"title":"Unrelated"}))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t.is_finished());
    a.config
        .put("streams", "owned", json!({"password":"new-password"}))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), t)
            .await
            .unwrap()
            .unwrap()
            .status(),
        403
    );
    drop(tx);
    until(async || a.media.count().await == 0).await;
    let (tx, t) = upload(a.clone(), "/owned/mpegts?password=new-password").await;
    tx.send(Ok(transport_packet())).await.unwrap();
    until(async || a.media.count().await == 1).await;
    t.abort();
    let _ = t.await;
    drop(tx);
    until(async || a.media.count().await == 0).await;
    a.media.stop_all().await;
}
#[tokio::test]
async fn malformed_truncated_and_stalled_publications_are_bounded() {
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config
        .put(
            "streams",
            "owned",
            json!({"inputs":[{"url":"publish://"}],"flussonix_input_timeout":1}),
        )
        .unwrap();
    for b in [vec![0; 188], vec![0x47; 187], Vec::new()] {
        assert_eq!(
            router(a.clone())
                .oneshot(post("/owned/mpegts", Body::from(b)))
                .await
                .unwrap()
                .status(),
            400
        );
        until(async || a.media.count().await == 0).await;
    }
    let (tx, t) = upload(a.clone(), "/owned/mpegts").await;
    let response = tokio::time::timeout(Duration::from_secs(3), t)
        .await
        .unwrap()
        .unwrap();
    assert!(response.status() == 408 || response.status() == 503);
    drop(tx);
    until(async || a.media.count().await == 0).await;
    let mut r = post("/owned/mpegts", Body::empty());
    r.headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    assert_eq!(router(a.clone()).oneshot(r).await.unwrap().status(), 415);
    a.media.stop_all().await;
}
async fn auth_backend(handler: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let t = tokio::spawn(async move {
        axum::serve(l, handler).await.unwrap();
    });
    (url, t)
}
#[tokio::test]
async fn publisher_callback_uses_post_metadata_and_renews_same_session_before_denying() {
    use axum::{Json, extract::State};
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    let s = seen.clone();
    let (url, backend) = auth_backend(
        axum::Router::new()
            .route(
                "/publish",
                axum::routing::post(
                    async |State(seen): State<Arc<tokio::sync::Mutex<Vec<serde_json::Value>>>>,
                           Json(v): Json<serde_json::Value>| {
                        let mut seen = seen.lock().await;
                        seen.push(v);
                        if seen.len() == 1 {
                            (axum::http::StatusCode::OK, [("x-authduration", "1")])
                        } else {
                            (axum::http::StatusCode::FORBIDDEN, [("x-authduration", "1")])
                        }
                    },
                ),
            )
            .with_state(s),
    )
    .await;
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config.put("streams","owned",json!({"inputs":[{"url":"publish://"}],"password":"publisher","on_publish":format!("{url}/publish")})).unwrap();
    let (tx, t) = upload(
        a.clone(),
        "/owned/mpegts?password=publisher&token=owned%2Btoken&extra=one%20two",
    )
    .await;
    tx.send(Ok(transport_packet())).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), t)
            .await
            .unwrap()
            .unwrap()
            .status(),
        403
    );
    drop(tx);
    let log = seen.lock().await;
    assert_eq!(log.len(), 2);
    assert_eq!(log[0]["name"], "owned");
    assert_eq!(log[0]["proto"], "mpegts");
    assert_eq!(log[0]["ip"], "127.0.0.1");
    assert_eq!(log[0]["token"], "owned+token");
    assert_eq!(log[0]["request_type"], "new_session");
    assert_eq!(log[0]["request_number"], 0);
    assert_eq!(log[1]["request_number"], 1);
    assert_eq!(log[1]["request_type"], "update_session");
    assert_eq!(log[0]["session_id"], log[1]["session_id"]);
    assert_eq!(log[1]["bytes"], 188);
    assert!(log[0]["qs"].as_str().unwrap().contains("extra=one%20two"));
    drop(log);
    a.media.stop_all().await;
    backend.abort();
}
#[tokio::test]
async fn callback_denial_redirect_outage_and_stale_decision_never_admit_worker() {
    use axum::{Json, extract::State};
    let gate = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let state = (gate.clone(), entered.clone());
    let (url, backend) = auth_backend(
        axum::Router::new()
            .route(
                "/deny",
                axum::routing::post(|| async { axum::http::StatusCode::FORBIDDEN }),
            )
            .route(
                "/redirect",
                axum::routing::post(|| async {
                    (axum::http::StatusCode::FOUND, [("location", "/deny")])
                }),
            )
            .route(
                "/wait",
                axum::routing::post(
                    async |State((gate, entered)): State<(
                        Arc<tokio::sync::Notify>,
                        Arc<tokio::sync::Notify>,
                    )>,
                           Json(_): Json<serde_json::Value>| {
                        entered.notify_one();
                        gate.notified().await;
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(state),
    )
    .await;
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    for endpoint in [
        format!("{url}/deny"),
        format!("{url}/redirect"),
        "http://127.0.0.1:1/publish".into(),
    ] {
        a.config
            .put(
                "streams",
                "owned",
                json!({"inputs":[{"url":"publish://"}],"on_publish":endpoint}),
            )
            .unwrap();
        assert_eq!(
            router(a.clone())
                .oneshot(post("/owned/mpegts", Body::empty()))
                .await
                .unwrap()
                .status(),
            403
        );
        assert_eq!(a.media.count().await, 0);
    }
    a.config
        .put(
            "streams",
            "owned",
            json!({"on_publish":format!("{url}/wait")}),
        )
        .unwrap();
    let pending = tokio::spawn({
        let a = a.clone();
        async move {
            router(a)
                .oneshot(post("/owned/mpegts", Body::empty()))
                .await
                .unwrap()
        }
    });
    entered.notified().await;
    a.config
        .put("streams", "owned", json!({"password":"new-password"}))
        .unwrap();
    gate.notify_one();
    assert_eq!(pending.await.unwrap().status(), 403);
    assert_eq!(a.media.count().await, 0);
    backend.abort();
}
fn synthetic_transport() -> &'static bytes::Bytes {
    static DATA: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    DATA.get_or_init(|| {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("owned.ts");
        let status = std::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=320x180:rate=25",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=48000",
                "-t",
                "12",
                "-c:v",
                "libx264",
                "-threads",
                "2",
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-g",
                "25",
                "-c:a",
                "aac",
                "-f",
                "mpegts",
                "-muxrate",
                "4000000",
            ])
            .arg(&p)
            .status()
            .unwrap();
        assert!(status.success());
        bytes::Bytes::from(std::fs::read(p).unwrap())
    })
}
#[tokio::test]
async fn large_chunked_publication_feeds_hls_native_wire_and_cpu_transcoding() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    for encoder in ["copy", "libx264"] {
        let d = tempfile::tempdir().unwrap();
        let a = app(d.path());
        a.config
            .put(
                "streams",
                "owned",
                json!({"inputs":[{"url":"publish://"}],"transcoder":{"encoder":encoder,"vb":600}}),
            )
            .unwrap();
        let (tx, t) = upload(a.clone(), "/owned/mpegts").await;
        let data = synthetic_transport();
        assert!(data.len() > 2 * 1024 * 1024);
        for chunk in data.chunks(65537) {
            tx.send(Ok(bytes::Bytes::copy_from_slice(chunk)))
                .await
                .unwrap();
        }
        until(async || a.media.ready("owned").await).await;
        let w = a
            .media
            .ensure("owned", &a.config.effective("owned").unwrap())
            .await
            .unwrap();
        until(async || w.wire.has_info()).await;
        let rtp = w.wire.rtp.play_snapshot().unwrap();
        assert!(
            !rtp.packets.is_empty(),
            "publication feeds the RTSP packetizer"
        );
        let fmp4_manifest = String::from_utf8(
            a.media
                .read("owned", "fmp4/index.m3u8")
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let init = fmp4_manifest
            .lines()
            .find(|l| l.starts_with("#EXT-X-MAP:"))
            .unwrap()
            .split('"')
            .nth(1)
            .unwrap();
        let fragment = fmp4_manifest
            .lines()
            .find(|l| !l.starts_with('#') && !l.is_empty())
            .unwrap();
        let mut mp4 = a
            .media
            .read("owned", &format!("fmp4/{init}"))
            .await
            .unwrap()
            .to_vec();
        mp4.extend_from_slice(
            &a.media
                .read("owned", &format!("fmp4/{fragment}"))
                .await
                .unwrap(),
        );
        let mp4_path = d.path().join("decode.mp4");
        std::fs::write(&mp4_path, mp4).unwrap();
        let decoded = tokio::process::Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-i"])
            .arg(&mp4_path)
            .args([
                "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "framemd5", "-",
            ])
            .output()
            .await
            .unwrap();
        assert!(
            decoded.status.success(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        let frames = String::from_utf8(decoded.stdout).unwrap();
        assert!(
            frames.lines().filter(|l| l.starts_with("0,")).count() >= 25,
            "encoder={encoder}, videos={}, manifest={fmp4_manifest}, stats={}",
            frames.lines().filter(|l| l.starts_with("0,")).count(),
            w.stats()
        );
        assert!(frames.lines().filter(|l| l.starts_with("1,")).count() >= 50);
        let hls = a.media.read("owned", "index.m3u8").await.unwrap();
        let text = String::from_utf8(hls.to_vec()).unwrap();
        let segment = text
            .lines()
            .find(|l| !l.starts_with('#') && !l.is_empty())
            .unwrap();
        let ts = a.media.read("owned", segment).await.unwrap();
        let path = d.path().join("decode.ts");
        std::fs::write(&path, ts).unwrap();
        let output = tokio::process::Command::new("ffprobe")
            .args(["-v", "error", "-show_streams", "-of", "json"])
            .arg(&path)
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        let probe: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let streams = probe["streams"].as_array().unwrap();
        assert!(streams.iter().any(|s| s["codec_name"] == "h264"));
        assert!(streams.iter().any(|s| s["codec_name"] == "aac"));
        let decoded = tokio::process::Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-i"])
            .arg(&path)
            .args([
                "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "framemd5", "-",
            ])
            .output()
            .await
            .unwrap();
        assert!(
            decoded.status.success(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        let frames = String::from_utf8(decoded.stdout).unwrap();
        assert!(frames.lines().filter(|l| l.starts_with("0,")).count() >= 25);
        assert!(frames.lines().filter(|l| l.starts_with("1,")).count() >= 50);

        drop(tx);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), t)
                .await
                .unwrap()
                .unwrap()
                .status(),
            204
        );
        until(async || a.media.count().await == 0).await;
        a.media.stop_all().await;
    }
}

#[tokio::test]
async fn deletion_worker_stop_and_lb_rejection_release_publications() {
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config.put("streams", "owned", config()).unwrap();
    let (tx, t) = upload(a.clone(), "/owned/mpegts").await;
    tx.send(Ok(transport_packet())).await.unwrap();
    until(async || a.media.count().await == 1).await;
    a.config.delete("streams", "owned").unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), t)
            .await
            .unwrap()
            .unwrap()
            .status(),
        403
    );
    drop(tx);
    until(async || a.media.count().await == 0).await;
    a.config.put("streams", "owned", config()).unwrap();
    let (tx, t) = upload(a.clone(), "/owned/mpegts").await;
    tx.send(Ok(transport_packet())).await.unwrap();
    until(async || a.media.count().await == 1).await;
    tokio::time::timeout(Duration::from_secs(2), a.media.stop_all())
        .await
        .unwrap();
    assert_eq!(t.await.unwrap().status(), 503);
    drop(tx);
    let lb = App::new(
        d.path().join("lb.json"),
        d.path().join("lb-media"),
        Options {
            role: "lb".into(),
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    lb.config.put("streams", "owned", config()).unwrap();
    assert_eq!(
        router(lb.clone())
            .oneshot(post("/owned/mpegts", Body::empty()))
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(lb.media.count().await, 0);
}

#[tokio::test]
async fn active_policy_changes_interrupt_a_pending_renewal() {
    use axum::{Json, extract::State};
    let renew = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Notify::new());
    let state = (renew.clone(), gate.clone());
    let (url, backend) = auth_backend(
        axum::Router::new()
            .route(
                "/publish",
                axum::routing::post(
                    async |State((renew, gate)): State<(
                        Arc<tokio::sync::Notify>,
                        Arc<tokio::sync::Notify>,
                    )>,
                           Json(v): Json<serde_json::Value>| {
                        if v["request_number"] == 1 {
                            renew.notify_one();
                            gate.notified().await;
                        }
                        (axum::http::StatusCode::OK, [("x-authduration", "1")])
                    },
                ),
            )
            .with_state(state),
    )
    .await;
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config
        .put(
            "streams",
            "owned",
            json!({"inputs":[{"url":"publish://"}],"on_publish":format!("{url}/publish")}),
        )
        .unwrap();
    let (tx, t) = upload(a.clone(), "/owned/mpegts").await;
    tx.send(Ok(transport_packet())).await.unwrap();
    renew.notified().await;
    a.config
        .put("streams", "owned", json!({"password":"changed"}))
        .unwrap();
    let response = tokio::time::timeout(Duration::from_millis(800), t)
        .await
        .expect("a callback wait must not delay policy revocation")
        .unwrap();
    assert_eq!(response.status(), 403);
    drop(tx);
    gate.notify_one();
    backend.abort();
    a.media.stop_all().await;
}

#[tokio::test]
async fn publisher_callback_admission_is_bounded_before_worker_creation() {
    use axum::{Json, extract::State};
    let (sent, mut received) = tokio::sync::mpsc::channel::<()>(64);
    let (url, backend) = auth_backend(
        axum::Router::new()
            .route(
                "/publish",
                axum::routing::post(
                    async |State(sent): State<tokio::sync::mpsc::Sender<()>>,
                           Json(_): Json<serde_json::Value>| {
                        sent.send(()).await.unwrap();
                        std::future::pending::<axum::http::StatusCode>().await
                    },
                ),
            )
            .with_state(sent),
    )
    .await;
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config
        .put(
            "streams",
            "owned",
            json!({"inputs":[{"url":"publish://"}],"on_publish":format!("{url}/publish")}),
        )
        .unwrap();
    let mut requests = Vec::new();
    for _ in 0..64 {
        let a = a.clone();
        requests.push(tokio::spawn(async move {
            router(a)
                .oneshot(post("/owned/mpegts", Body::empty()))
                .await
                .unwrap()
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..64 {
            received.recv().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(a.media.count().await, 0);
    assert_eq!(
        router(a.clone())
            .oneshot(post("/owned/mpegts", Body::empty()))
            .await
            .unwrap()
            .status(),
        503
    );
    for r in requests {
        r.abort();
        let _ = r.await;
    }
    backend.abort();
    a.config
        .put("streams", "owned", json!({"on_publish":null}))
        .unwrap();
    assert_eq!(
        router(a.clone())
            .oneshot(post("/owned/mpegts", Body::empty()))
            .await
            .unwrap()
            .status(),
        400
    );
    a.media.stop_all().await;
}

#[tokio::test]
async fn publication_is_an_authenticated_private_native_cluster_source() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    use sha2::{Digest, Sha256};
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config.put("streams","owned",json!({"inputs":[{"url":"publish://"}],"password":"publisher","flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))})).unwrap();
    let (source, server) = auth_backend(router(a.clone())).await;
    let (tx, t) = upload(a.clone(), "/owned/mpegts?password=publisher").await;
    let sending = tx.clone();
    let producer = tokio::spawn(async move {
        for piece in synthetic_transport().chunks(65537) {
            if sending
                .send(Ok(bytes::Bytes::copy_from_slice(piece)))
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    until(async || a.media.ready("owned").await).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{source}/owned/index.m3u8?password=publisher"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    for transport in ["m4s", "m4f"] {
        let cdn = Engine::new(d.path().join(transport), "ffmpeg");
        let cfg = json!({"inputs":[{"url":format!("{transport}://{}/owned",source.strip_prefix("http://").unwrap())}],"flussonix_peer_key":"owned-peer-secret"});
        let worker = cdn.ensure("relay", &cfg).await.unwrap();
        let ready = tokio::time::timeout(Duration::from_secs(20), async {
            while !cdn.ready("relay").await {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            ready.is_ok(),
            "{transport} startup: {}; source: {}; native_info: {}",
            cdn.stats("relay").await,
            a.media.stats("owned").await,
            worker.wire.has_info()
        );
        assert!(worker.wire.has_info());
        assert!(
            cdn.read("relay", "index.m3u8")
                .await
                .unwrap()
                .starts_with(b"#EXTM3U")
        );
        cdn.stop_all().await;
    }
    producer.await.unwrap();
    drop(tx);
    assert_eq!(t.await.unwrap().status(), 204);
    a.media.stop_all().await;
    server.abort();
}

#[tokio::test]
async fn peer_discovery_never_discloses_explicit_or_inherited_publisher_secrets() {
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path());
    a.config
        .put(
            "auth_backends",
            "viewers",
            json!({"url":"https://viewer.example/check"}),
        )
        .unwrap();
    let publisher = json!({"inputs":[{"url":"publish://"}], "password":"owned-private-publisher", "on_publish":"https://publisher.example/check?secret=owned-private-callback", "on_play":"auth://viewers", "flussonix_content_id":"owned-content", "flussonix_token_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"});
    a.config
        .put("templates", "receive", publisher.clone())
        .unwrap();
    a.config.put("streams", "explicit", publisher).unwrap();
    a.config
        .put("streams", "inherited", json!({"template":"receive"}))
        .unwrap();
    for name in ["explicit", "inherited"] {
        let request = Request::builder()
            .uri(format!("/flussonix/api/v1/stream/{name}"))
            .header("x-flussonix-peer", "owned-peer-secret")
            .body(Body::empty())
            .unwrap();
        let response = router(a.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), 200);
        let bytes = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let discovery: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            discovery.get("password").is_none(),
            "peer cannot learn publisher password"
        );
        assert!(
            discovery.get("on_publish").is_none(),
            "publisher callback is private"
        );
        assert!(
            discovery.get("config_on_disk").is_none(),
            "raw saved configuration is private"
        );
        assert!(!String::from_utf8_lossy(&bytes).contains("owned-private"));
        assert_eq!(discovery["name"], name);
        assert_eq!(discovery["flussonix_content_id"], "owned-content");
        assert_eq!(discovery["on_play"], "https://viewer.example/check");
        assert_eq!(discovery["flussonix_token_sha256"], "a".repeat(64));
        let response = router(a.clone())
            .oneshot(post(&format!("/{name}/mpegts"), Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
        assert_eq!(
            a.config.effective(name).unwrap()["password"],
            "owned-private-publisher",
            "management config retains policy"
        );
    }
    assert_eq!(a.media.count().await, 0);
}
