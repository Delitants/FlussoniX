use flussonix::{
    http_tls,
    server::{App, Options, router},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
};
use tokio_util::sync::CancellationToken;
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;

struct Fixture {
    _dir: tempfile::TempDir,
    cert: Certificates,
    app: Arc<App>,
    url: String,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
async fn fixture(role: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let cert = Certificates::new();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            role: role.into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-token"))})).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    app.set_http_delivery(None, Some(address));
    let cancel = CancellationToken::new();
    let task = tokio::spawn(http_tls::serve(
        listener,
        cert.server(),
        app.clone(),
        cancel.clone(),
    ));
    Fixture {
        _dir: dir,
        cert,
        app,
        url: format!("https://{address}"),
        cancel,
        task,
    }
}
fn client(cert: &Certificates) -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&cert.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap()
}
async fn stop(f: Fixture) {
    f.cancel.cancel();
    f.app.media.stop_all().await;
    tokio::time::timeout(Duration::from_secs(5), f.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn https_uses_real_peer_and_same_authorization_and_rejects_downgrade() {
    let f = fixture("standalone").await;
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
    let captured = seen.clone();
    let callback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback_url = format!("http://{}/auth", callback.local_addr().unwrap());
    let handler = axum::Router::new().route(
        "/auth",
        axum::routing::get(
            move |axum::extract::Query(query): axum::extract::Query<Value>| {
                let seen = captured.clone();
                async move {
                    seen.lock().await.push(query);
                    (
                        axum::http::StatusCode::FOUND,
                        [(
                            "location",
                            "http://plaintext.example/owned?token=owned-token",
                        )],
                    )
                }
            },
        ),
    );
    let callback_task = tokio::spawn(async move { axum::serve(callback, handler).await.unwrap() });
    f.app
        .config
        .put("streams", "owned", json!({"on_play":callback_url}))
        .unwrap();
    let c = client(&f.cert);
    assert_eq!(
        c.get(format!("{}/flussonix/api/v1/node", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let status: Value = c
        .get(format!("{}/flussonix/api/v1/node", f.url))
        .basic_auth("admin", Some("owned-admin"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["http_delivery"]["https_only"], true);
    assert!(status["http_delivery"]["http"].is_null());
    assert!(
        status["http_delivery"]["https"]
            .as_str()
            .unwrap()
            .contains(':')
    );
    for path in ["index.m3u8", "mpegts", "m4s", "m4f"] {
        assert_eq!(
            c.get(format!("{}/owned/{path}", f.url))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    let result = c
        .get(format!("{}/owned/index.m3u8?token=owned-token", f.url))
        .header("x-forwarded-for", "203.0.113.9")
        .header("x-forwarded-proto", "http")
        .send()
        .await
        .unwrap();
    assert_eq!(
        result.status(),
        403,
        "HTTPS must reject backend plaintext redirects"
    );
    assert_eq!(seen.lock().await[0]["ip"], "127.0.0.1");
    assert_eq!(f.app.media.count().await, 0);
    // The same callback redirect remains valid on the plaintext router.
    use tower::ServiceExt;
    let result = router(f.app.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri("/owned/index.m3u8?token=owned-token")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.status(), 302);
    callback_task.abort();
    stop(f).await;
}

#[tokio::test]
async fn slow_or_plaintext_handshakes_do_not_block_verified_clients_and_cancel_closes_sockets() {
    let f = fixture("standalone").await;
    let addr = url::Url::parse(&f.url).unwrap();
    let mut idle = Vec::new();
    for _ in 0..16 {
        idle.push(
            TcpStream::connect(("127.0.0.1", addr.port().unwrap()))
                .await
                .unwrap(),
        );
    }
    let mut invalid = TcpStream::connect(("127.0.0.1", addr.port().unwrap()))
        .await
        .unwrap();
    invalid
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let healthy = tokio::time::timeout(
        Duration::from_secs(2),
        client(&f.cert).get(format!("{}/health", f.url)).send(),
    )
    .await
    .expect("idle handshakes cannot serialize healthy clients")
    .unwrap();
    assert!(healthy.status().is_success());
    assert!(
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("{}/health", f.url))
            .send()
            .await
            .is_err(),
        "untrusted CA must fail"
    );
    let wrong_name = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&f.cert.ca).unwrap()).unwrap(),
        )
        .resolve(
            "wrong.example",
            format!("127.0.0.1:{}", addr.port().unwrap())
                .parse()
                .unwrap(),
        )
        .build()
        .unwrap();
    assert!(
        wrong_name
            .get(format!(
                "https://wrong.example:{}/health",
                addr.port().unwrap()
            ))
            .send()
            .await
            .is_err(),
        "wrong identity on the reachable TLS server must fail"
    );
    let mut data = [0; 128];
    let n = tokio::time::timeout(Duration::from_secs(2), invalid.read(&mut data))
        .await
        .unwrap()
        .unwrap_or(0);
    assert!(
        !data[..n].starts_with(b"HTTP/"),
        "plaintext cannot reach app"
    );
    stop(f).await;
    for mut socket in idle {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut data))
                .await
                .unwrap()
                .unwrap_or(0),
            0
        );
    }
}

#[tokio::test]
async fn handshake_deadline_releases_idle_connections() {
    let f = fixture("standalone").await;
    let address = url::Url::parse(&f.url).unwrap();
    let mut socket = TcpStream::connect(("127.0.0.1", address.port().unwrap()))
        .await
        .unwrap();
    let mut byte = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(7), socket.read(&mut byte))
            .await
            .expect("idle TLS socket must expire")
            .unwrap_or(0),
        0
    );
    stop(f).await;
}

#[tokio::test]
async fn https_lb_excludes_plaintext_peers_before_admission() {
    let f = fixture("lb").await;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = calls.clone();
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer.local_addr().unwrap());
    let routes=axum::Router::new().route("/flussonix/api/v1/node",axum::routing::get(||async{axum::Json(json!({"role":"cdn","streams":[],"limit":1000,"active":0,"reserved":0,"reserved_mbps":0,"uplink_mbps":1000,"uplink":0.1,"cpu":0.1,"ram":0.1,"drain":false,"age_ms":0}))})).route("/flussonix/api/v1/admit",axum::routing::post(move||{let count=count.clone();async move {count.fetch_add(1,std::sync::atomic::Ordering::SeqCst);axum::Json(json!({"ticket":"owned-ticket"}))}}));
    let task = tokio::spawn(async move { axum::serve(peer, routes).await.unwrap() });
    f.app
        .config
        .put(
            "peers",
            "owned",
            json!({"api_url":peer_url,"public_payload_url":"http://plain.example"}),
        )
        .unwrap();
    let c = client(&f.cert);
    assert_eq!(
        c.get(format!("{}/owned/index.m3u8?token=owned-token", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    f.app
        .config
        .put(
            "peers",
            "owned",
            json!({"public_payload_url":"https://secure.example"}),
        )
        .unwrap();
    let r = c
        .get(format!("{}/owned/index.m3u8?token=owned-token", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 302);
    assert!(
        r.headers()["location"]
            .to_str()
            .unwrap()
            .starts_with("https://secure.example/owned/index.m3u8?")
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    task.abort();
    stop(f).await;
}

#[tokio::test]
async fn https_hls_and_native_outputs_share_media_and_decode_independently() {
    let f = fixture("standalone").await;
    let c = client(&f.cert);
    let playlist = c
        .get(format!("{}/owned/index.m3u8?token=owned-token", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(playlist.status(), 200);
    let body = playlist.text().await.unwrap();
    assert!(body.contains("#EXTINF"));
    let segment = body
        .lines()
        .find(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap();
    assert!(!segment.starts_with("http://"));
    assert!(segment.contains("token=owned-token"));
    let ts = c
        .get(format!("{}/owned/{segment}", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(ts.status(), 200);
    assert!(ts.bytes().await.unwrap().len() > 188);
    let mut wire = c
        .get(format!("{}/owned/m4s?token=owned-token", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(wire.status(), 200);
    let mut parser = flussonix::m4s::Decoder::default();
    let mut tracks = false;
    let mut frames = false;
    for _ in 0..30 {
        for event in parser.push(&wire.chunk().await.unwrap().unwrap()).unwrap() {
            match event {
                flussonix::m4s::Event::Info { .. } => tracks = true,
                flussonix::m4s::Event::Frame { .. } | flussonix::m4s::Event::Gop { .. } => {
                    frames = true
                }
                _ => {}
            }
        }
        if tracks && frames {
            break;
        }
    }
    assert!(tracks && frames);
    drop(wire);
    let mut signals = c
        .get(format!("{}/owned/m4f?token=owned-token", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(signals.status(), 200);
    let mut notifications = flussonix::m4_ingest::Signals::default();
    let note = loop {
        let chunk = signals.chunk().await.unwrap().unwrap();
        if let Some(note) = notifications.push(&chunk).unwrap().into_iter().next() {
            break note;
        }
    };
    let payload = c
        .get(format!("{}/owned/{}?token=owned-token", f.url, note.name))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        payload,
        f.app.media.read("owned", &note.name).await.unwrap(),
        "TLS segment delivery preserves original wire bytes"
    );
    let (tracks, frames) = flussonix::m4f::unpack(&payload).unwrap();
    assert_eq!(tracks.len(), 2);
    assert!(!frames.is_empty());
    drop(signals);
    let output = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-tls_verify",
            "1",
            "-ca_file",
            f.cert.ca.to_str().unwrap(),
            "-i",
            &format!("{}/owned/fmp4/index.m3u8?token=owned-token", f.url),
            "-t",
            "2",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-f",
            "null",
            "-",
        ])
        .output();
    let result = tokio::time::timeout(Duration::from_secs(20), output)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    f.app
        .config
        .put(
            "streams",
            "owned",
            json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"replacement-token"))}),
        )
        .unwrap();
    assert_eq!(
        c.get(format!("{}/owned/index.m3u8?token=owned-token", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    stop(f).await;
}

#[tokio::test]
async fn https_publication_password_callback_renewal_decode_and_drop_are_preserved() {
    let f = fixture("standalone").await;
    let c = client(&f.cert);
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
    let captured = seen.clone();
    let callback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback_url = format!("http://{}/publish", callback.local_addr().unwrap());
    let routes = axum::Router::new().route(
        "/publish",
        axum::routing::post(move |axum::Json(metadata): axum::Json<Value>| {
            let seen = captured.clone();
            async move {
                seen.lock().await.push(metadata);
                [("X-AuthDuration", "1")]
            }
        }),
    );
    let callback_task = tokio::spawn(async move { axum::serve(callback, routes).await.unwrap() });
    f.app.config.put("streams","received",json!({"inputs":[{"url":"publish://"}],"password":"owned-password","on_publish":callback_url,"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})).unwrap();
    assert_eq!(
        c.post(format!("{}/received/mpegts?password=wrong", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert!(seen.lock().await.is_empty());
    assert_eq!(f.app.media.count().await, 0);
    let transport = f._dir.path().join("publication.ts");
    let generated = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=sample_rate=48000",
            "-t",
            "14",
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
        ])
        .arg(&transport)
        .output()
        .await
        .unwrap();
    assert!(generated.status.success());
    let data = bytes::Bytes::from(std::fs::read(&transport).unwrap());
    let body = futures_util::stream::unfold((data, 0), |(data, offset)| async move {
        if offset == data.len() {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let end = (offset + 4096).min(data.len());
        let chunk = data.slice(offset..end);
        Some((Ok::<_, std::io::Error>(chunk), (data, end)))
    });
    let publisher = tokio::spawn(
        c.post(format!(
            "{}/received/mpegts?password=owned-password&token=owned-publisher",
            f.url
        ))
        .header("Content-Type", "video/mp2t")
        .header("x-forwarded-for", "203.0.113.9")
        .body(reqwest::Body::wrap_stream(body))
        .send(),
    );
    for _ in 0..80 {
        if f.app.media.count().await > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(f.app.media.count().await, 1);
    assert_eq!(
        c.get(format!("{}/received/index.m3u8", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let playlist = c
        .get(format!("{}/received/index.m3u8?token=owned-viewer", f.url))
        .send()
        .await
        .unwrap();
    assert_eq!(playlist.status(), 200);
    let output = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-tls_verify",
            "1",
            "-ca_file",
            f.cert.ca.to_str().unwrap(),
            "-i",
            &format!("{}/received/fmp4/index.m3u8?token=owned-viewer", f.url),
            "-t",
            "2",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-f",
            "null",
            "-",
        ])
        .output();
    let decoded = tokio::time::timeout(Duration::from_secs(20), output)
        .await
        .unwrap()
        .unwrap();
    assert!(
        decoded.status.success(),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    let seen = seen.lock().await;
    assert!(seen.len() >= 2, "TLS publisher callback renews");
    assert_eq!(seen[0]["ip"], "127.0.0.1");
    assert_eq!(seen[0]["token"], "owned-publisher");
    assert!(uuid::Uuid::parse_str(seen[0]["session_id"].as_str().unwrap()).is_ok());
    assert_eq!(seen[0]["session_id"], seen[1]["session_id"]);
    assert_eq!(seen[0]["request_number"], 0);
    assert_eq!(seen[1]["request_number"], 1);
    drop(seen);
    publisher.abort();
    let _ = publisher.await;
    for _ in 0..80 {
        if f.app.media.count().await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        f.app.media.count().await,
        0,
        "TLS publisher drop releases fenced worker"
    );
    callback_task.abort();
    stop(f).await;
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}
fn daemon(dir: &std::path::Path, cert: &Certificates) -> Command {
    daemon_with(dir, cert, "127.0.0.1:0", &cert.key)
}
fn daemon_with(
    dir: &std::path::Path,
    cert: &Certificates,
    address: &str,
    key: &std::path::Path,
) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_flussonix"));
    c.args([
        "--https-listen",
        address,
        "--https-cert",
        cert.cert.to_str().unwrap(),
        "--https-key",
        key.to_str().unwrap(),
        "--https-only",
        "--config",
        dir.join("config.json").to_str().unwrap(),
        "--media-dir",
        dir.join("media").to_str().unwrap(),
    ])
    .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
    .env("FLUSSONIX_PEER_KEY", "owned-peer-secret")
    .env("RUST_LOG", "error")
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .kill_on_drop(true);
    c
}
#[tokio::test]
async fn https_only_daemon_does_not_bind_plaintext_and_shutdown_releases_tls_port() {
    let d = tempfile::tempdir().unwrap();
    let cert = Certificates::new();
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = daemon(d.path(), &cert);
    c.args(["--listen", &occupied.local_addr().unwrap().to_string()]);
    let mut process = Process(c.spawn().unwrap());
    let mut reader = BufReader::new(process.0.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(4), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let info: Value = serde_json::from_str(&line)
        .expect("HTTPS-only must start even when plaintext port is occupied");
    assert!(info["listen"].is_null());
    let addr = info["https_listen"].as_str().unwrap();
    assert_eq!(
        client(&cert)
            .get(format!("https://{addr}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let pid = process.0.id().unwrap();
    Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(7), process.0.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(TcpListener::bind(addr).await.is_ok());
}
#[tokio::test]
async fn bad_tls_material_and_occupied_port_fail_before_static_worker_startup() {
    let other = Certificates::new();
    let busy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    for case in 0..3 {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join("config.json"),
            json!({"streams":[{"name":"static","inputs":[{"url":"testsrc://"}]}]}).to_string(),
        )
        .unwrap();
        let cert = Certificates::new();
        if case == 0 {
            std::fs::write(&cert.cert, "bad PEM").unwrap();
        }
        let busy_address = busy.local_addr().unwrap().to_string();
        let mut c = daemon_with(
            d.path(),
            &cert,
            if case == 2 {
                &busy_address
            } else {
                "127.0.0.1:0"
            },
            if case == 1 { &other.key } else { &cert.key },
        );
        let result = tokio::time::timeout(Duration::from_secs(5), c.output())
            .await
            .unwrap()
            .unwrap();
        assert!(!result.status.success());
        assert!(
            String::from_utf8_lossy(&result.stdout).trim().is_empty(),
            "startup must not advertise a running server"
        );
        assert!(
            !d.path().join("media/static").exists(),
            "invalid startup cannot start a static worker"
        );
    }
}
