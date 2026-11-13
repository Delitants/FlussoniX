//! The ticket issuer is exercised through the public router with owned state.
use axum::{
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{Method, Request, StatusCode},
};
use flussonix::{
    server::{App, Options, router},
    srt_playback::{Listener, Settings},
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;
#[path = "support/tls.rs"]
mod tls_fixture;

struct Lab {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    _listener: Option<Listener>,
}
impl Lab {
    fn new(encrypted: bool, public_url: bool) -> Self {
        Self::with_direct_tokens(encrypted, public_url, false)
    }
    fn with_listener_limit(limit: usize) -> Self {
        Self::with_settings(true, true, false, limit)
    }
    fn with_direct_tokens(encrypted: bool, public_url: bool, direct_tokens: bool) -> Self {
        Self::with_settings(encrypted, public_url, direct_tokens, 8)
    }
    fn with_settings(encrypted: bool, public_url: bool, direct_tokens: bool, limit: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            Settings::new(
                120,
                limit,
                if encrypted {
                    "owned-test-passphrase"
                } else {
                    ""
                }
                .into(),
            )
            .unwrap()
            .with_direct_tokens(direct_tokens),
        )
        .unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-secret".into(),
                peer_key: "owned-cluster-secret".into(),
                uplink_interface: "process".into(),
                srt_play_public_url: public_url
                    .then(|| url::Url::parse(&format!("srt://{}", listener.address())).unwrap()),
                ..Options::default()
            },
        )
        .unwrap();
        app.config
            .put("streams", "owned", json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
        app.set_srt_playback(Some(&listener));
        std::thread::sleep(Duration::from_millis(20));
        app.sample_metrics();
        Self {
            _dir: dir,
            app,
            _listener: Some(listener),
        }
    }
    async fn request(
        &self,
        body: &str,
        peer: Option<SocketAddr>,
        uri: &str,
    ) -> (StatusCode, Value, String) {
        let response = self.response(body, peer, uri).await;
        let status = response.status();
        let cache = response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value, cache)
    }
    async fn response(
        &self,
        body: &str,
        peer: Option<SocketAddr>,
        uri: &str,
    ) -> axum::response::Response {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .header("x-forwarded-for", "203.0.113.71")
            .body(Body::from(body.to_owned()))
            .unwrap();
        if let Some(peer) = peer {
            request.extensions_mut().insert(ConnectInfo(peer));
        }
        router(self.app.clone()).oneshot(request).await.unwrap()
    }
}
const BODY: &str =
    r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","encryption":"required"}"##;
fn loopback() -> SocketAddr {
    "127.0.0.1:40111".parse().unwrap()
}
fn token_body(token: &str) -> String {
    format!(r##"{{"streamid":"#!::r=owned,m=request,u={token}","encryption":"required"}}"##)
}

#[tokio::test]
async fn one_token_cannot_fill_the_public_srt_listener() {
    let lab = Lab::with_listener_limit(128);
    let uri = "/flussonix/api/v1/srt-ticket";
    let body = token_body("shared-secret");
    for _ in 0..4 {
        let (status, value, _) = lab.request(&body, Some(loopback()), uri).await;
        assert_eq!(status, StatusCode::OK, "{value}");
    }
    let response = lab.response(&body, Some(loopback()), uri).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(response.headers()["connection"], "close");
    let denied = to_bytes(response.into_body(), 8192).await.unwrap();
    assert!(
        !denied
            .windows(b"shared-secret".len())
            .any(|part| part == b"shared-secret")
    );
    let (status, value, _) = lab
        .request(
            &token_body("other-secret"),
            Some("127.0.0.2:40111".parse().unwrap()),
            uri,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

#[tokio::test]
async fn one_peer_cannot_fill_the_public_srt_listener_with_rotated_tokens() {
    let lab = Lab::with_listener_limit(128);
    let uri = "/flussonix/api/v1/srt-ticket";
    for index in 0..16 {
        let (status, value, _) = lab
            .request(
                &token_body(&format!("token-{index}")),
                Some(loopback()),
                uri,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{index}: {value}");
    }
    let response = lab
        .response(&token_body("token-16"), Some(loopback()), uri)
        .await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(response.headers()["connection"], "close");
    let (status, value, _) = lab
        .request(
            &token_body("token-16"),
            Some("127.0.0.2:40111".parse().unwrap()),
            uri,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{value}");
}

#[tokio::test]
async fn concurrent_issuance_cannot_exceed_one_tokens_four_pending_tickets() {
    let lab = Lab::with_listener_limit(128);
    let body = token_body("parallel-secret");
    let responses = futures_util::future::join_all(
        (0..12).map(|_| lab.response(&body, Some(loopback()), "/flussonix/api/v1/srt-ticket")),
    )
    .await;
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.status() == StatusCode::OK)
            .count(),
        4
    );
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.status() == StatusCode::TOO_MANY_REQUESTS)
            .count(),
        8
    );
}

#[tokio::test]
async fn loopback_issue_returns_only_tokenless_five_second_ticket() {
    let lab = Lab::new(true, true);
    let (status, value, cache) = lab
        .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(cache, "private, no-store");
    assert_eq!(
        value["address"],
        format!("srt://{}", lab._listener.as_ref().unwrap().address())
    );
    assert_eq!(value["encryption_required"], true);
    assert_eq!(value["expires_in"], 5);
    let id = value["streamid"].as_str().unwrap();
    assert!(id.starts_with("#!::r=owned,m=request,flussonix_ticket="));
    assert!(!id.contains("owned-secret"));
    assert!(!value.to_string().contains("owned-secret"));
}

#[tokio::test]
async fn request_is_strict_bounded_and_never_echoes_credentials() {
    let lab = Lab::new(true, true);
    let invalid = [
        r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","encryption":"any"}"##.to_owned(),
        r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","unknown":1}"##.to_owned(),
        r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","streamid":"#!::r=owned"}"##
            .to_owned(),
        r##"{"streamid":"#!::r=owned,m=publish,u=owned-secret"}"##.to_owned(),
        "{".to_owned(),
        format!("{{\"streamid\":\"{}\"}}", "x".repeat(4100)),
    ];
    for body in invalid {
        let (status, value, cache) = lab
            .request(&body, Some(loopback()), "/flussonix/api/v1/srt-ticket")
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {value}");
        assert_eq!(cache, "private, no-store");
        assert!(!value.to_string().contains("owned-secret"));
    }
}

#[tokio::test]
async fn unfinished_ticket_body_expires_and_closes_the_connection() {
    let lab = Lab::new(true, true);
    let body = Body::from_stream(
        stream::iter([Ok::<bytes::Bytes, std::io::Error>(
            bytes::Bytes::from_static(b"{"),
        )])
        .chain(stream::pending()),
    );
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/flussonix/api/v1/srt-ticket")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(loopback()));
    let response = tokio::time::timeout(
        Duration::from_secs(8),
        router(lab.app.clone()).oneshot(request),
    )
    .await
    .expect("unfinished ticket body held a connection beyond the deadline")
    .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(response.headers()["connection"], "close");
}

#[tokio::test]
async fn one_peer_cannot_fill_ticket_body_read_capacity() {
    let lab = Lab::new(true, true);
    let mut held = Vec::new();
    let mut entered = Vec::new();
    for _ in 0..8 {
        let (signal, ready) = tokio::sync::oneshot::channel();
        entered.push(ready);
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/flussonix/api/v1/srt-ticket")
            .header("content-type", "application/json")
            .body(Body::from_stream(stream::once(async move {
                let _ = signal.send(());
                std::future::pending::<Result<bytes::Bytes, std::io::Error>>().await
            })))
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(loopback()));
        let app = lab.app.clone();
        held.push(tokio::spawn(
            async move { router(app).oneshot(request).await },
        ));
    }
    for ready in entered {
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .expect("held body did not enter the ticket reader")
            .unwrap();
    }
    let mut ninth = Request::builder()
        .method(Method::POST)
        .uri("/flussonix/api/v1/srt-ticket")
        .header("content-type", "application/json")
        .body(Body::from_stream(stream::pending::<
            Result<bytes::Bytes, std::io::Error>,
        >()))
        .unwrap();
    ninth.extensions_mut().insert(ConnectInfo(loopback()));
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        router(lab.app.clone()).oneshot(ninth),
    )
    .await;
    for task in held {
        task.abort();
        let _ = task.await;
    }
    let response = response
        .expect("ninth unfinished body was admitted from one peer")
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    assert_eq!(response.headers()["connection"], "close");
    let (status, _, _) = lab
        .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::OK, "body-read slots were not released");
}

#[tokio::test]
async fn issuer_requires_actual_peer_and_encrypted_public_listener() {
    let lab = Lab::new(true, true);
    let (status, _, cache) = lab
        .request(BODY, None, "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
    assert_eq!(cache, "private, no-store");
    let remote: SocketAddr = "198.51.100.7:41111".parse().unwrap();
    let (status, _, _) = lab
        .request(BODY, Some(remote), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
    let (status, _, _) = lab
        .request(
            BODY,
            Some(loopback()),
            "/flussonix/api/v1/srt-ticket?token=owned-secret",
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for lab in [Lab::new(false, true), Lab::new(true, false)] {
        let (status, _, cache) = lab
            .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
            .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(cache, "private, no-store");
    }
}

#[tokio::test]
async fn owned_https_listener_issues_without_exposing_the_original_token() {
    let lab = Lab::new(true, true);
    let certs = tls_fixture::Certificates::new();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let app = lab.app.clone();
    let stop = cancel.clone();
    let tls = certs.server();
    let task = tokio::spawn(async move { flussonix::http_tls::serve(tcp, tls, app, stop).await });
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&certs.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let response = client
        .post(format!(
            "https://localhost:{}/flussonix/api/v1/srt-ticket",
            address.port()
        ))
        .header("content-type", "application/json")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    let text = response.text().await.unwrap();
    assert!(!text.contains("owned-secret"));
    assert!(text.contains("flussonix_ticket="));
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn https_srt_ticket_rejects_expired_callback_after_503() {
    let lab = Lab::new(true, true);
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback_url = format!("http://{}/auth", backend.local_addr().unwrap());
    let callback_task = tokio::spawn(async move {
        axum::serve(
            backend,
            axum::Router::new().route(
                "/auth",
                axum::routing::get(move || {
                    let calls = callback_calls.clone();
                    async move {
                        let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                        (
                            if first {
                                StatusCode::OK
                            } else {
                                StatusCode::SERVICE_UNAVAILABLE
                            },
                            [("X-AuthDuration", "1")],
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    lab.app
        .config
        .put(
            "streams",
            "owned",
            json!({
                "inputs":[{"url":"testsrc://"}], "on_play":callback_url
            }),
        )
        .unwrap();
    let certs = tls_fixture::Certificates::new();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tcp.local_addr().unwrap().port();
    let cancel = tokio_util::sync::CancellationToken::new();
    let app = lab.app.clone();
    let stop = cancel.clone();
    let tls = certs.server();
    let task = tokio::spawn(async move { flussonix::http_tls::serve(tcp, tls, app, stop).await });
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&certs.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let endpoint = format!("https://localhost:{port}/flussonix/api/v1/srt-ticket");
    let initial = client
        .post(&endpoint)
        .header("content-type", "application/json")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(initial.status(), StatusCode::OK);
    let initial_body = initial.text().await.unwrap();
    assert!(initial_body.contains("flussonix_ticket="));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    lab.app.playback_auth.renew_due().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "503 callback was not exercised"
    );
    let expired = client
        .post(&endpoint)
        .header("content-type", "application/json")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(expired.status(), StatusCode::FORBIDDEN);
    assert_eq!(expired.headers()["cache-control"], "private, no-store");
    let expired_body = expired.text().await.unwrap();
    assert!(!expired_body.contains("flussonix_ticket="));
    assert!(!expired_body.contains("owned-secret"));
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    callback_task.abort();
    let _ = callback_task.await;
}

#[tokio::test]
async fn failed_viewer_authorization_leaves_no_public_ticket() {
    let lab = Lab::new(true, true);
    lab.app
        .config
        .put(
            "streams",
            "owned",
            json!({
                "inputs":[{"url":"testsrc://"}],
                "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"different-token"))
            }),
        )
        .unwrap();
    let (status, value, cache) = lab
        .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(cache, "private, no-store");
    assert!(!value.to_string().contains("owned-secret"));
    assert!(!value.to_string().contains("flussonix_ticket"));
}

async fn relay_and_capture(
    target: SocketAddr,
    packets: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    stop: tokio_util::sync::CancellationToken,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let facing = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let upstream = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = facing.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut from_client = [0u8; 4096];
        let mut from_server = [0u8; 4096];
        let mut client = None;
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                result = facing.recv_from(&mut from_client) => {
                    let Ok((size, peer)) = result else {break};
                    if client.is_some_and(|known| known != peer) {continue}
                    client = Some(peer);
                    let mut found = packets.lock().await;
                    if found.len() < 128 {found.push(from_client[..size].to_vec());}
                    drop(found);
                    if upstream.send_to(&from_client[..size], target).await.is_err() {break}
                },
                result = upstream.recv_from(&mut from_server) => {
                    let Ok((size, source)) = result else {break};
                    if source != target {continue}
                    if let Some(peer) = client {
                        if facing.send_to(&from_server[..size], peer).await.is_err() {break}
                    }
                }
            }
        }
    });
    (address, task)
}

fn capture_contains(packets: &[Vec<u8>], needle: &[u8]) -> bool {
    packets.iter().any(|packet| {
        if packet.windows(needle.len()).any(|part| part == needle) {
            return true;
        }
        // SRT carries Stream ID extension bytes in network-order 32-bit words.
        // Try each possible alignment so the assertion sees both wire order
        // and the application-level Stream ID without assuming a fixed header.
        (0..4).any(|offset| {
            let mut decoded = packet.clone();
            for word in decoded[offset..].chunks_exact_mut(4) {
                word.reverse();
            }
            decoded.windows(needle.len()).any(|part| part == needle)
        })
    })
}

async fn receive_srt(address: SocketAddr, streamid: &str, path: &std::path::Path) -> bool {
    receive_srt_with_passphrase(address, streamid, "owned-test-passphrase", path).await
}

async fn receive_srt_with_passphrase(
    address: SocketAddr,
    streamid: &str,
    passphrase: &str,
    path: &std::path::Path,
) -> bool {
    let mut client = tokio::process::Command::new("ffmpeg");
    client.args([
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
        streamid,
        "-passphrase",
        passphrase,
        "-i",
        &format!("srt://{address}?mode=caller&latency=120000&connect_timeout=1000&timeout=5000000"),
        "-t",
        "3",
        "-map",
        "0",
        "-c",
        "copy",
        "-f",
        "mpegts",
    ]);
    client
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = client.kill_on_drop(true).spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success()
}

async fn assert_decoded(path: &std::path::Path, video: &str, audio: &str) {
    let probe = tokio::process::Command::new("ffprobe")
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
    let codecs: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let streams = codecs["streams"].as_array().unwrap();
    assert!(streams.iter().any(|stream| stream["codec_name"] == video));
    assert!(streams.iter().any(|stream| stream["codec_name"] == audio));
    let decoded = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(path)
        .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success(),
        "owned SRT media failed strict decode: {}",
        String::from_utf8_lossy(&decoded.stderr)
    );
}

#[tokio::test]
async fn independent_srt_handshake_capture_omits_reusable_token_but_detects_legacy_control() {
    let mut lab = Lab::with_direct_tokens(true, true, true);
    lab.app
        .config
        .put(
            "streams",
            "owned",
            json!({
                "inputs":[{"url":"testsrc://"}], "static":false,
                "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-secret"))
            }),
        )
        .unwrap();
    lab.app.sample_metrics();
    let destination = lab._listener.as_ref().unwrap().address();
    let cancel = tokio_util::sync::CancellationToken::new();
    let playback = tokio::spawn(flussonix::srt_playback::serve(
        lab._listener.take().unwrap(),
        lab.app.clone(),
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    lab.app.sample_metrics();
    let credential_body = r##"{"streamid":"#!::r=owned,m=request,u=owned-secret,s=owned-secret,a=owned-secret","encryption":"required"}"##;
    let (status, ticket, _) = lab
        .request(
            credential_body,
            Some(loopback()),
            "/flussonix/api/v1/srt-ticket",
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{ticket}");
    let ticket_id = ticket["streamid"].as_str().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wrong_key = dir.path().join("wrong-key.ts");
    assert!(
        !receive_srt_with_passphrase(destination, ticket_id, "owned-wrong-passphrase", &wrong_key)
            .await
    );
    assert!(!wrong_key.exists() || wrong_key.metadata().unwrap().len() == 0);
    let recorded = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let relay_stop = tokio_util::sync::CancellationToken::new();
    let (relay, task) = relay_and_capture(destination, recorded.clone(), relay_stop.clone()).await;
    let ticket_media = dir.path().join("ticket.ts");
    assert!(receive_srt(relay, ticket_id, &ticket_media).await);
    relay_stop.cancel();
    task.await.unwrap();
    let ticket_packets = recorded.lock().await.clone();
    assert!(!ticket_packets.is_empty());
    assert!(!capture_contains(&ticket_packets, b"owned-secret"));
    assert!(!capture_contains(&ticket_packets, b"u=owned-secret"));
    assert_decoded(&ticket_media, "h264", "aac").await;

    let replay = dir.path().join("replay.ts");
    assert!(!receive_srt(destination, ticket_id, &replay).await);
    assert!(!replay.exists() || replay.metadata().unwrap().len() == 0);

    let legacy = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let relay_stop = tokio_util::sync::CancellationToken::new();
    let (relay, task) = relay_and_capture(destination, legacy.clone(), relay_stop.clone()).await;
    let legacy_media = dir.path().join("legacy.ts");
    assert!(receive_srt(relay, "#!::r=owned,m=request,u=owned-secret", &legacy_media).await);
    relay_stop.cancel();
    task.await.unwrap();
    assert!(
        capture_contains(&legacy.lock().await, b"u=owned-secret"),
        "legacy control must prove that the capture detects handshake tokens"
    );
    assert_decoded(&legacy_media, "h264", "aac").await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), playback)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    lab.app.media.stop_all().await;
}
