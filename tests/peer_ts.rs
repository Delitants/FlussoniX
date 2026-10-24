use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use flussonix::peer_hls::PeerHls;
use futures_util::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[path = "support/tls.rs"]
mod tls;

#[tokio::test]
async fn continuous_ts_never_forwards_peer_credentials_on_redirect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let foreign = format!("{url}/foreign/mpegts");
    let app = Router::new()
        .route(
            "/owned/mpegts",
            get(move || {
                let location = foreign.clone();
                async move {
                    Response::builder()
                        .status(302)
                        .header("location", location)
                        .body(Body::empty())
                        .unwrap()
                }
            }),
        )
        .route(
            "/foreign/mpegts",
            get(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    "foreign"
                }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let bridge = PeerHls::start(&format!("{url}/owned/mpegts"), "owned-peer-secret")
        .await
        .unwrap();
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&bridge.url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    drop(bridge);
    server.abort();
}

#[tokio::test]
async fn secure_peer_ts_rejects_an_untrusted_certificate_before_media_startup() {
    use flussonix::{
        http_tls,
        server::{App, Options},
    };
    let dir = tempfile::tempdir().unwrap();
    let certificates = tls::Certificates::new();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            serde_json::json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}/owned/mpegts", listener.local_addr().unwrap());
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(http_tls::serve(
        listener,
        certificates.server(),
        app.clone(),
        cancel.clone(),
    ));
    let bridge = PeerHls::start(&endpoint, "owned-peer-secret")
        .await
        .unwrap();
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&bridge.url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    assert_eq!(app.media.count().await, 0);
    drop(bridge);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn progressing_peer_ts_can_run_longer_than_finite_hls_request_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/owned/mpegts", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/owned/mpegts",
        get(|| async {
            let stream = futures_util::stream::unfold(0u8, |n| async move {
                if n == 120 {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                Some((Ok::<_, std::io::Error>(Bytes::from(vec![n; 188])), n + 1))
            });
            Response::builder().body(Body::from_stream(stream)).unwrap()
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let bridge = PeerHls::start(&url, "owned-peer-secret").await.unwrap();
    let bytes = tokio::time::timeout(Duration::from_secs(16), async {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(&bridge.url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(bytes.len(), 120 * 188);
    assert_eq!(&bytes[119 * 188..], &[119; 188]);
    drop(bridge);
    task.abort();
}

async fn reject_metadata(oversized: bool) {
    use flussonix::media::Engine;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("tshttp://{}/owned/mpegts", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/owned/mpegts",
        get(move || async move {
            if oversized {
                Response::builder()
                    .body(Body::from(vec![0x47; 2 * 1024 * 1024]))
                    .unwrap()
            } else {
                let stream = futures_util::stream::unfold((), |()| async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(&[0x47])), ()))
                });
                Response::builder().body(Body::from_stream(stream)).unwrap()
            }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let worker=engine.ensure("owned",&serde_json::json!({"inputs":[{"url":endpoint}],"flussonix_peer_key":"owned-peer-secret"})).await.unwrap();
    tokio::time::timeout(Duration::from_secs(7), worker.closed())
        .await
        .unwrap();
    assert_eq!(worker.pid(), 0, "invalid metadata spawned a packager");
    assert_eq!(
        worker.stats()["last_error"],
        if oversized {
            "metadata_limit"
        } else {
            "startup_timeout"
        }
    );
    engine.stop_all().await;
    task.abort();
}
#[tokio::test]
async fn oversized_ts_metadata_is_rejected_before_packager_start() {
    reject_metadata(true).await;
}
#[tokio::test]
async fn progressing_without_pmt_has_a_total_startup_deadline() {
    reject_metadata(false).await;
}

#[tokio::test]
async fn stalled_peer_metadata_does_not_block_other_stream_startups() {
    use flussonix::media::Engine;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("tshttp://{}/owned/mpegts", listener.local_addr().unwrap());
    let connected = Arc::new(AtomicUsize::new(0));
    let seen = connected.clone();
    let app = Router::new().route(
        "/owned/mpegts",
        get(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            async {
                let stream = futures_util::stream::unfold((), |()| async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(&[0x47])), ()))
                });
                Response::builder().body(Body::from_stream(stream)).unwrap()
            }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let e = engine.clone();
    let stalled = tokio::spawn(async move {
        e.ensure("stalled",&serde_json::json!({"inputs":[{"url":endpoint}],"flussonix_peer_key":"owned-peer-secret"})).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while connected.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    let healthy = tokio::time::timeout(
        Duration::from_secs(1),
        engine.ensure(
            "healthy",
            &serde_json::json!({"inputs":[{"url":"testsrc://"}]}),
        ),
    )
    .await;
    stalled.await.unwrap().ok();
    engine.stop_all().await;
    task.abort();
    assert!(
        healthy.is_ok_and(|result| result.is_ok()),
        "one bad source serialized another stream's startup"
    );
}

#[tokio::test]
async fn fixed_peer_ts_stream_delivers_before_eof_and_cancels_its_upstream() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!(
        "http://{}/owned/mpegts?routing=owned",
        listener.local_addr().unwrap()
    );
    let active = Arc::new(AtomicUsize::new(0));
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let count = active.clone();
    let app = Router::new().route(
        "/owned/mpegts",
        get(move |h: HeaderMap| {
            let count = count.clone();
            async move {
                if h.get("x-flussonix-peer").and_then(|v| v.to_str().ok())
                    != Some("owned-peer-secret")
                {
                    return Response::builder().status(403).body(Body::empty()).unwrap();
                }
                count.fetch_add(1, Ordering::SeqCst);
                let guard = Guard(count);
                let stream =
                    futures_util::stream::unfold((true, guard), |(first, guard)| async move {
                        if first {
                            Some((
                                Ok::<_, std::io::Error>(Bytes::from(vec![0x47; 188])),
                                (false, guard),
                            ))
                        } else {
                            std::future::pending().await
                        }
                    });
                Response::builder()
                    .header("content-type", "video/mp2t")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let bridge = PeerHls::start(&upstream, "owned-peer-secret")
        .await
        .expect("continuous peer TS must be supported");
    assert!(!bridge.url.contains("owned-peer-secret"));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut parts = bridge.url.rsplitn(3, '/');
    let _file = parts.next().unwrap();
    let _resource = parts.next().unwrap();
    let prefix = parts.next().unwrap();
    let forged = format!(
        "{prefix}/{}/resource.bin",
        URL_SAFE_NO_PAD.encode(upstream.replace("/owned/mpegts", "/other/mpegts"))
    );
    assert_eq!(
        client.get(forged).send().await.unwrap().status(),
        StatusCode::BAD_GATEWAY
    );
    let response = tokio::time::timeout(Duration::from_secs(2), client.get(&bridge.url).send())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .as_ref(),
        &[0x47; 188]
    );
    assert_eq!(active.load(Ordering::SeqCst), 1);
    // Hold the permit through the body, preventing a second upstream subscription.
    assert_eq!(
        client.get(&bridge.url).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(bridge);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .is_none()
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn aac_only_peer_ts_retains_native_bootstrap_and_actual_media() {
    let fixture = tokio::process::Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "12",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            "pipe:1",
        ])
        .output()
        .await
        .unwrap();
    assert!(fixture.status.success());
    let bytes = Bytes::from(fixture.stdout);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("tshttp://{}/radio/mpegts", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/radio/mpegts",
        get(move |headers: HeaderMap| {
            let bytes = bytes.clone();
            async move {
                assert_eq!(headers["x-flussonix-peer"], "owned-radio-peer");
                let body =
                    futures_util::stream::once(std::future::ready(Ok::<_, std::io::Error>(bytes)))
                        .chain(futures_util::stream::pending());
                Body::from_stream(body)
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path(), "ffmpeg");
    let worker = engine
        .ensure(
            "radio",
            &serde_json::json!({"inputs":[{"url":url}],"flussonix_peer_key":"owned-radio-peer"}),
        )
        .await
        .unwrap();
    let ready = tokio::time::timeout(Duration::from_secs(8), async {
        while worker.wire.signal_subscribe().0.is_empty() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        ready.is_ok(),
        "AAC-only native output missing: {}",
        worker.stats()
    );
    let mut decoder = flussonix::m4s::Decoder::default();
    let mut tracks = Vec::new();
    let mut frames = 0;
    for record in worker.wire.m4s_subscribe().0 {
        for event in decoder.push(&record).unwrap() {
            match event {
                flussonix::m4s::Event::Info { tracks: t, .. } => tracks = t,
                flussonix::m4s::Event::Frame { body, .. } => {
                    assert!(!body.is_empty());
                    frames += 1;
                }
                _ => {}
            }
        }
    }
    assert_eq!(tracks.len(), 1);
    assert_eq!(tracks[0].codec, "aac");
    assert!(frames > 0, "AAC-only M4S has metadata but no media");
    for signal in worker.wire.signal_subscribe().0 {
        let text = std::str::from_utf8(&signal).unwrap();
        let stamp = text
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        let segment = worker.wire.segment(&format!("{stamp}.m4f")).unwrap();
        let (_, decoded) = flussonix::m4f::unpack(&segment).unwrap();
        assert!(!decoded.is_empty(), "AAC-only M4F has no media");
    }
    engine.stop_all().await;
    server.abort();
}

// A peer's first media can follow publication probing. Its startup deadline
// must cover that wait without extending the budget for malformed PMT data.
async fn inspected_peer_startup_case(case: &str) {
    use flussonix::media::Engine;
    use futures_util::FutureExt;
    let delayed = case == "delayed";
    let input = if delayed {
        let mut command = tokio::process::Command::new("ffmpeg");
        command.kill_on_drop(true).args([
            "-nostdin",
            "-v",
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
            "8",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "50",
            "-b:v",
            "300k",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            "pipe:1",
        ]);
        let output = tokio::time::timeout(Duration::from_secs(10), command.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.stdout.is_empty() && output.stdout.len() < 1024 * 1024);
        Bytes::from(output.stdout)
    } else {
        Bytes::new()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("tshttp://{}/owned/mpegts", listener.local_addr().unwrap());
    let active = Arc::new(AtomicUsize::new(0));
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let count = active.clone();
    let app = Router::new().route(
        "/owned/mpegts",
        get(move |headers: HeaderMap| {
            let active = count.clone();
            let input = input.clone();
            async move {
                assert_eq!(headers["X-Flussonix-Peer"], "owned-peer-secret");
                active.fetch_add(1, Ordering::SeqCst);
                let stream = futures_util::stream::unfold(
                    (true, input, Guard(active)),
                    |(first, input, guard)| async move {
                        if first && !input.is_empty() {
                            tokio::time::sleep(Duration::from_millis(5500)).await;
                            Some((
                                Ok::<_, std::io::Error>(input.clone()),
                                (false, input, guard),
                            ))
                        } else {
                            std::future::pending::<()>().await;
                            Some((Ok(Bytes::new()), (false, input, guard)))
                        }
                    },
                );
                Response::builder().body(Body::from_stream(stream)).unwrap()
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path().join("media"), "ffmpeg");
    let outcome = std::panic::AssertUnwindSafe(async {
        let worker = engine
            .ensure(
                "owned",
                &serde_json::json!({
                    "inputs":[{"url":endpoint}], "flussonix_peer_key":"owned-peer-secret",
                    "flussonix_input_timeout": if case == "deadline" { 1 } else { 15 }
                }),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned peer did not attach");
        match case {
            "delayed" => {
                let playlist = tokio::time::timeout(Duration::from_secs(9), async {
                    loop {
                        assert!(
                            worker.alive.load(Ordering::Relaxed),
                            "healthy delayed source failed before first media: {}",
                            worker.stats()
                        );
                        if let Ok(bytes) = engine.read("owned", "fmp4/index.m3u8").await {
                            let text = String::from_utf8(bytes.to_vec()).unwrap();
                            if text.contains("#EXTINF:") {
                                break text;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .expect("delayed peer must produce real HLS media");
                let init = playlist
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("#EXT-X-MAP:URI=\"")
                            .and_then(|value| value.strip_suffix('"'))
                    })
                    .unwrap();
                let segment = playlist
                    .lines()
                    .find(|line| line.ends_with(".m4s"))
                    .unwrap();
                let mut bytes = engine
                    .read("owned", &format!("fmp4/{init}"))
                    .await
                    .unwrap()
                    .to_vec();
                bytes.extend(
                    engine
                        .read("owned", &format!("fmp4/{segment}"))
                        .await
                        .unwrap(),
                );
                let path = dir.path().join("delayed.mp4");
                std::fs::write(&path, bytes).unwrap();
                let output = tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio::process::Command::new("ffmpeg")
                        .kill_on_drop(true)
                        .args([
                            "-nostdin",
                            "-v",
                            "error",
                            "-xerror",
                            "-err_detect",
                            "explode",
                            "-i",
                        ])
                        .arg(path)
                        .args([
                            "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "framemd5",
                            "-",
                        ])
                        .output(),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(
                    output.status.success() && output.stderr.is_empty(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let frames = String::from_utf8(output.stdout).unwrap();
                assert!(
                    frames.lines().filter(|line| line.starts_with("0,")).count() >= 25,
                    "delayed peer delivered too few decoded video frames"
                );
                assert!(
                    frames.lines().filter(|line| line.starts_with("1,")).count() >= 50,
                    "delayed peer delivered too few decoded audio frames"
                );
                assert!(worker.pid() > 0 && worker.stats()["bytes_in"].as_u64().unwrap() > 0);
            }
            "deadline" => {
                tokio::time::timeout(Duration::from_secs(2), worker.closed())
                    .await
                    .expect(
                        "configured one-second startup deadline ignored before first body bytes",
                    );
                assert_eq!(worker.stats()["last_error"], "startup_timeout");
                assert_eq!(worker.pid(), 0, "empty source must not spawn a packager");
            }
            "cancel" => {
                tokio::time::timeout(Duration::from_secs(1), engine.stop_all())
                    .await
                    .expect("cancellation waited for peer startup deadline");
                assert_eq!(worker.pid(), 0);
                assert!(worker.is_closed());
            }
            _ => unreachable!(),
        }
    })
    .catch_unwind()
    .await;
    engine.stop_all().await;
    let released = tokio::time::timeout(Duration::from_secs(1), async {
        while active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    server.abort();
    let _ = server.await;
    assert_eq!(engine.count().await, 0);
    assert!(
        released.is_ok(),
        "peer startup retained its upstream body after cleanup"
    );
    if let Err(error) = outcome {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn inspected_peer_allows_source_startup_before_its_pmt_budget() {
    inspected_peer_startup_case("delayed").await;
}

#[tokio::test]
async fn inspected_peer_honors_configured_deadline_before_first_body_bytes() {
    inspected_peer_startup_case("deadline").await;
}

#[tokio::test]
async fn inspected_peer_cancellation_releases_an_upstream_with_no_body_bytes() {
    inspected_peer_startup_case("cancel").await;
}
