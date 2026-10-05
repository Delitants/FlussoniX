//! Independent localhost callers; every listener/process/stream belongs to this test.
use flussonix::{
    server::{App, Options, router},
    srt_playback::{self, Listener, Settings},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    process::{Child, Command},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
static MEDIA: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct Lab {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    address: SocketAddr,
    url: String,
    cancel: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
    http: JoinHandle<()>,
    renew: JoinHandle<()>,
}
impl Lab {
    async fn start(config: Value, role: &str, secret: &str, limit: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: role.into(),
                admin_password: "owned-admin-secret".into(),
                peer_key: "owned-cluster-peer-secret".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config.put("streams", "owned", config).unwrap();
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            Settings::new(120, limit, secret.into()).unwrap(),
        )
        .unwrap();
        let address = listener.address();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(srt_playback::serve(listener, app.clone(), cancel.clone()));
        let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", http_listener.local_addr().unwrap());
        let a = app.clone();
        let c = cancel.clone();
        let http = tokio::spawn(async move {
            axum::serve(
                http_listener,
                router(a).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(c.cancelled_owned())
            .await
            .unwrap();
        });
        let a = app.clone();
        let c = cancel.clone();
        let renew = tokio::spawn(async move {
            loop {
                tokio::select! {_=c.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(100))=>a.playback_auth.renew_due().await}
            }
        });
        Self {
            _dir: dir,
            app,
            address,
            url,
            cancel,
            task,
            http,
            renew,
        }
    }
    async fn stop(self) {
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.app.media.stop_all().await;
        self.renew.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.http)
            .await
            .unwrap()
            .unwrap();
    }
}
fn config() -> Value {
    json!({"inputs":[{"url":"testsrc://"}],"static":false})
}
fn receiver(lab: &Lab, token: &str, path: &Path, duration: &str) -> Child {
    let mut cmd = Command::new("ffmpeg");
    cmd.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-threads",
        "1",
        "-analyzeduration",
        "1000000",
        "-probesize",
        "524288",
        "-srt_streamid",
        &format!("#!::r=owned,m=request,u={token}"),
    ]);
    if lab.address.port() == 0 {
        panic!("Unbound owned listener");
    }
    cmd.args([
        "-i",
        &format!(
            "srt://{}?mode=caller&latency=120000&connect_timeout=1000&timeout=10000000",
            lab.address
        ),
        "-t",
        duration,
        "-map",
        "0",
        "-c",
        "copy",
        "-f",
        "mpegts",
    ])
    .arg(path)
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .kill_on_drop(true)
    .spawn()
    .unwrap()
}
fn encrypted_receiver(lab: &Lab, token: &str, secret: &str, path: &Path) -> Child {
    Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-threads",
            "1",
            "-analyzeduration",
            "1000000",
            "-probesize",
            "524288",
            "-srt_streamid",
            &format!("#!::r=owned,m=request,u={token}"),
            "-passphrase",
            secret,
            "-i",
            &format!(
                "srt://{}?mode=caller&latency=120000&connect_timeout=1000&timeout=10000000",
                lab.address
            ),
            "-t",
            "1",
            "-map",
            "0",
            "-c",
            "copy",
            "-f",
            "mpegts",
        ])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}
async fn decoded(child: &mut Child, path: &Path, video: &str, audio: &str) {
    assert!(
        tokio::time::timeout(Duration::from_secs(20), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success(),
        "Owned caller failed"
    );
    let probe = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .await
        .unwrap();
    assert!(probe.status.success());
    let data: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let codecs: Vec<_> = data["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["codec_name"].as_str().unwrap())
        .collect();
    assert!(
        codecs.contains(&video) && codecs.contains(&audio),
        "Missing expected owned codecs"
    );
    let result = Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(path)
        .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    if !result.status.success() {
        std::fs::copy(
            path,
            Path::new(".runtime/qualification").join(path.file_name().unwrap()),
        )
        .unwrap();
    }
    assert!(
        result.status.success(),
        "Owned received video/audio failed decode: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}
async fn viewers(app: &App, count: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if app.media.stats("owned").await["online_clients"] == count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("Owned viewer count did not settle");
}
async fn bytes(app: &App) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while app.srt_egress.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("No authorized SRT output");
}
#[tokio::test]
async fn shared_listener_delivers_six_encrypted_codec_profiles() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    for (video, encoder) in [("h264", "libx264"), ("hevc", "libx265")] {
        for (audio, acodec, bitrate) in [
            ("aac", "aac", 96),
            ("mp2", "mp2a", 192),
            ("mp3", "mp3", 128),
        ] {
            let mut cfg = config();
            cfg["transcoder"] = json!({"encoder":encoder,"acodec":acodec,"ab":bitrate});
            cfg["flussonix_token_sha256"] = json!(format!("{:x}", Sha256::digest(b"owned-token")));
            let lab = Lab::start(cfg, "standalone", "owned-secure-secret", 4).await;
            let path = dir.path().join(format!("{video}-{audio}.ts"));
            let mut child = encrypted_receiver(&lab, "owned-token", "owned-secure-secret", &path);
            decoded(&mut child, &path, video, audio).await;
            assert_eq!(lab.app.media.count().await, 1);
            assert!(lab.app.srt_egress.load(Ordering::Relaxed) > 0);
            let metadata = serde_json::to_string(&lab.app.playback_auth.snapshots()).unwrap();
            assert!(!metadata.contains("owned-token") && !metadata.contains("owned-secure-secret"));
            lab.stop().await;
        }
    }
}
#[tokio::test]
async fn denied_token_and_lb_role_never_start_workers_or_send_media() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    for (role, token) in [("standalone", "bad-token"), ("lb", "owned-token")] {
        let mut cfg = config();
        cfg["flussonix_token_sha256"] = json!(format!("{:x}", Sha256::digest(b"owned-token")));
        let lab = Lab::start(cfg, role, "", 4).await;
        let path = dir.path().join(format!("{role}.ts"));
        let mut child = receiver(&lab, token, &path, "1");
        assert!(
            !tokio::time::timeout(Duration::from_secs(8), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert_eq!(lab.app.media.count().await, 0);
        assert_eq!(lab.app.srt_egress.load(Ordering::Relaxed), 0);
        assert!(!path.exists() || path.metadata().unwrap().len() == 0);
        lab.stop().await;
    }
}
#[derive(Clone)]
struct Backend {
    allow: Arc<AtomicBool>,
    seen: Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>,
}
async fn callback() -> (Backend, String, JoinHandle<()>) {
    let state = Backend {
        allow: Arc::new(AtomicBool::new(true)),
        seen: Arc::new(tokio::sync::Mutex::new(vec![])),
    };
    let app=axum::Router::new().route("/auth",axum::routing::get(|axum::extract::State(s):axum::extract::State<Backend>,axum::extract::Query(q):axum::extract::Query<HashMap<String,String>>|async move {
        s.seen.lock().await.push(q.clone());let token=q.get("token").map(String::as_str).unwrap_or("");if token=="stalled" {tokio::time::sleep(Duration::from_secs(5)).await;}
        let code=if token=="redirect" {axum::http::StatusCode::FOUND}else if token=="denied"||!s.allow.load(Ordering::Relaxed) {axum::http::StatusCode::FORBIDDEN}else{axum::http::StatusCode::OK};
        (code,[("X-AuthDuration","1"),("Location","http://owned.invalid/never-follow")],"")
    })).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (state, url, task)
}
#[tokio::test]
async fn denied_redirect_outage_and_stalled_authorization_emit_no_media() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (_state, url, backend) = callback().await;
    for token in ["denied", "redirect", "stalled"] {
        let mut cfg = config();
        cfg["on_play"] = json!(url);
        let lab = Lab::start(cfg, "standalone", "", 2).await;
        let path = dir.path().join(format!("{token}.ts"));
        let mut child = receiver(&lab, token, &path, "1");
        assert!(
            !tokio::time::timeout(Duration::from_secs(8), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert_eq!(lab.app.media.count().await, 0);
        assert_eq!(lab.app.srt_egress.load(Ordering::Relaxed), 0);
        lab.stop().await;
    }
    backend.abort();
    backend.await.ok();
    let mut cfg = config();
    cfg["on_play"] = json!(url);
    let lab = Lab::start(cfg, "standalone", "", 2).await;
    let mut child = receiver(&lab, "outage", &dir.path().join("outage.ts"), "1");
    assert!(
        !tokio::time::timeout(Duration::from_secs(8), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert_eq!(lab.app.media.count().await, 0);
    lab.stop().await;
}
#[tokio::test]
async fn callback_uses_actual_peer_and_renewal_revokes_existing_delivery() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (state, url, backend) = callback().await;
    let mut cfg = config();
    cfg["on_play"] = json!(url);
    let lab = Lab::start(cfg, "standalone", "", 4).await;
    let mut child = receiver(
        &lab,
        "owned-renewal-token",
        &dir.path().join("renew.ts"),
        "60",
    );
    bytes(&lab.app).await;
    assert_eq!(lab.app.media.count().await, 1);
    let records = state.seen.lock().await.clone();
    assert!(
        records
            .iter()
            .any(|r| r.get("proto").map(String::as_str) == Some("srt")
                && r.get("ip").map(String::as_str) == Some("127.0.0.1")
                && r.get("token").map(String::as_str) == Some("owned-renewal-token"))
    );
    state.allow.store(false, Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    viewers(&lab.app, 0).await;
    assert!(state.seen.lock().await.len() >= 2);
    lab.stop().await;
    backend.abort();
    backend.await.ok();
}
#[tokio::test]
async fn session_deletion_closes_only_that_viewer_and_preserves_shared_worker() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let lab = Lab::start(config(), "standalone", "", 2).await;
    let mut first = receiver(
        &lab,
        "first-owned-token",
        &dir.path().join("first.ts"),
        "60",
    );
    let mut second = receiver(
        &lab,
        "second-owned-token",
        &dir.path().join("second.ts"),
        "60",
    );
    viewers(&lab.app, 2).await;
    bytes(&lab.app).await;
    assert_eq!(lab.app.media.count().await, 1);
    let pid = lab.app.media.stats("owned").await["pid"].clone();
    let sessions = lab.app.playback_auth.snapshots();
    assert_eq!(sessions.len(), 2);
    let id = sessions[0]["id"].as_str().unwrap();
    let result = reqwest::Client::new()
        .delete(format!("{}/streamer/api/v3/sessions/{id}", lab.url))
        .basic_auth("admin", Some("owned-admin-secret"))
        .send()
        .await
        .unwrap();
    assert_eq!(result.status(), 204);
    viewers(&lab.app, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(first.try_wait().unwrap().is_some() || second.try_wait().unwrap().is_some());
    assert_eq!(lab.app.media.stats("owned").await["pid"], pid);
    let before = lab.app.srt_egress.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(lab.app.srt_egress.load(Ordering::Relaxed) > before);
    for child in [&mut first, &mut second] {
        if child.try_wait().unwrap().is_none() {
            child.kill().await.unwrap();
            child.wait().await.unwrap();
        }
    }
    viewers(&lab.app, 0).await;
    lab.stop().await;
}
#[tokio::test]
async fn replacement_disable_and_shutdown_release_connections_promptly() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let lab = Lab::start(config(), "standalone", "", 2).await;
    let mut old = receiver(&lab, "owned-old", &dir.path().join("old.ts"), "60");
    bytes(&lab.app).await;
    let pid = lab.app.media.stats("owned").await["pid"].clone();
    let mut cfg = config();
    cfg["transcoder"] = json!({"encoder":"libx265"});
    lab.app.config.put("streams", "owned", cfg.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), old.wait())
        .await
        .unwrap()
        .unwrap();
    viewers(&lab.app, 0).await;
    let path = dir.path().join("new.ts");
    let mut next = receiver(&lab, "owned-next", &path, "1");
    decoded(&mut next, &path, "hevc", "aac").await;
    assert_ne!(lab.app.media.stats("owned").await["pid"], pid);
    let mut waiting = receiver(&lab, "owned-disable", &dir.path().join("disable.ts"), "60");
    viewers(&lab.app, 1).await;
    cfg["disabled"] = json!(true);
    lab.app.config.put("streams", "owned", cfg).unwrap();
    tokio::time::timeout(Duration::from_secs(3), waiting.wait())
        .await
        .unwrap()
        .unwrap();
    viewers(&lab.app, 0).await;
    lab.stop().await;
    let lab = Lab::start(config(), "standalone", "", 2).await;
    let mut child = receiver(&lab, "shutdown", &dir.path().join("shutdown.ts"), "60");
    bytes(&lab.app).await;
    lab.stop().await;
    tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn stopped_receiver_cannot_block_healthy_viewer_and_releases_its_slot() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let lab = Lab::start(config(), "standalone", "", 2).await;
    let mut stopped = receiver(&lab, "owned-stopped", &dir.path().join("stopped.ts"), "60");
    viewers(&lab.app, 1).await;
    bytes(&lab.app).await;
    let owned_pid = stopped.id().unwrap();
    // SAFETY: Pause only the exact live child created above by this test.
    assert_eq!(unsafe { libc::kill(owned_pid as i32, libc::SIGSTOP) }, 0);
    let path = dir.path().join("healthy.ts");
    let mut healthy = receiver(&lab, "healthy", &path, "3");
    decoded(&mut healthy, &path, "h264", "aac").await;
    viewers(&lab.app, 0).await;
    assert_eq!(lab.app.media.count().await, 1);
    let path = dir.path().join("reused.ts");
    let mut reused = receiver(&lab, "reused-slot", &path, "3");
    decoded(&mut reused, &path, "h264", "aac").await;
    stopped.kill().await.unwrap();
    stopped.wait().await.unwrap();
    lab.stop().await;
}
#[tokio::test]
async fn native_cdn_srt_output_uses_authorized_private_mpegts_pull() {
    let _lock = MEDIA.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg["flussonix_token_sha256"] = json!(format!("{:x}", Sha256::digest(b"owned-cdn-token")));
    let source = Lab::start(cfg, "source", "", 4).await;
    let edge = Lab::start(config(), "cdn", "", 4).await;
    edge.app.config.delete("streams", "owned").unwrap();
    edge.app.config.put("sources","owned-source",json!({"api_url":source.url,"private_payload_url":source.url,"flussonix_transport":"mpegts"})).unwrap();
    let path = dir.path().join("edge.ts");
    let mut child = receiver(&edge, "owned-cdn-token", &path, "1");
    decoded(&mut child, &path, "h264", "aac").await;
    assert_eq!(source.app.media.count().await, 1);
    assert_eq!(edge.app.media.count().await, 1);
    assert_eq!(source.app.playback_auth.active(), 0);
    edge.stop().await;
    source.stop().await;
}
