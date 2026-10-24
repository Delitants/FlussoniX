//! Exercise diagnostics through real routed response bodies, not a logging helper.
use super::*;
use crate::server::{App, Options, router};
use axum::{body::Body, http::Request};
use futures_util::FutureExt;
use http_body_util::BodyExt;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};
use tower::ServiceExt;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt};

type Rows = Arc<std::sync::Mutex<Vec<BTreeMap<String, Value>>>>;
struct Capture(Rows);
struct Fields(BTreeMap<String, Value>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), json!(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name().into(), json!(value));
    }
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Fields(BTreeMap::new());
        event.record(&mut fields);
        if fields.0.get("event") == Some(&json!("native_ts_delivery_end")) {
            self.0.lock().unwrap().push(fields.0);
        }
    }
}
fn capture() -> (Rows, tracing::subscriber::DefaultGuard) {
    let rows = Arc::new(std::sync::Mutex::new(vec![]));
    let subscriber = tracing_subscriber::registry().with(Capture(rows.clone()));
    (rows, tracing::subscriber::set_default(subscriber))
}
fn setup() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    // A real idle worker keeps lifecycle/cancellation ownership intact while
    // tests inject exact chunks into its production TS queue. No codec mocking.
    let idle = dir.path().join("owned-idle-ffmpeg");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            ffmpeg: idle.to_str().unwrap().into(),
            admin_password: "private-admin-password".into(),
            peer_key: "private-peer-password".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,
        "inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":30}),
        )
        .unwrap();
    (dir, app)
}
async fn checked(app: &Arc<App>, work: impl std::future::Future<Output = ()>) {
    let result = std::panic::AssertUnwindSafe(work).catch_unwind().await;
    app.media.stop_all().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
async fn body(app: &Arc<App>, token: &str) -> Body {
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/owned/mpegts?token={token}"))
                .header("User-Agent", "private-user-agent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "video/mp2t");
    response.into_body()
}
async fn worker(app: &App) -> Arc<Worker> {
    app.media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap()
}
fn send(worker: &Worker, bytes: Bytes) {
    worker
        .bytes
        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
    worker.tx.send(bytes).unwrap();
}
async fn next(body: &mut Body) -> Option<Bytes> {
    tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .map(|frame| frame.unwrap().into_data().unwrap())
}
fn bootstrap() -> Bytes {
    // An owned, independently generated MP3 sample provides real current PSI
    // and a fresh PES. Marker bytes in these lifecycle tests follow startup;
    // they must not bypass the production codec gate to initialize a viewer.
    let tracks = [crate::m4s::Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }];
    let mut muxer = crate::worker_ts::Muxer::new(&tracks).unwrap();
    let bytes = muxer
        .frame(&crate::m4f::Frame {
            track_id: 1,
            dts: 90_000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("../../tests/fixtures/codecs/mp3.bin").to_vec(),
        })
        .unwrap();
    assert!(
        bytes.len() < 2048,
        "bootstrap must fit before the controlled socket stall"
    );
    bytes.into()
}
async fn started(body: &mut Body, bootstrap: &Bytes) {
    let output = next(body)
        .await
        .expect("real TS bootstrap must start delivery");
    assert_eq!(output.len(), bootstrap.len());
    // Startup may repacketize the two PSI tables without changing their size.
    // The encoded PES and all following marker records must remain byte-exact.
    assert_eq!(&output[376..], &bootstrap[376..]);
}
fn one(rows: &Rows, reason: &str) -> BTreeMap<String, Value> {
    let rows = rows.lock().unwrap();
    assert_eq!(
        rows.len(),
        1,
        "a terminated body must emit exactly one diagnostic"
    );
    assert_eq!(rows[0]["reason"], reason);
    assert!(rows[0]["delivery_id"].as_u64().is_some_and(|id| id > 0));
    let text = serde_json::to_string(&*rows).unwrap();
    for secret in [
        "private-viewer-token",
        "private-admin-password",
        "private-peer-password",
        "private-user-agent",
        "testsrc://",
    ] {
        assert!(!text.contains(secret), "diagnostics exposed {secret}");
    }
    rows[0].clone()
}

#[tokio::test]
async fn lag_is_identified_without_stopping_another_ts_viewer() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut slow = body(&app, "private-viewer-token").await;
        let mut fast = body(&app, "another-private-viewer-token").await;
        let worker = worker(&app).await;
        let prefix = bootstrap();
        send(&worker, prefix.clone());
        started(&mut slow, &prefix).await;
        started(&mut fast, &prefix).await;
        for _ in 0..513 {
            let bytes = Bytes::from(vec![0x47; 8192]);
            send(&worker, bytes.clone());
            assert_eq!(next(&mut fast).await.unwrap(), bytes);
        }
        assert!(next(&mut slow).await.is_none());
        drop(slow);
        let row = one(&rows, "queue_lagged");
        assert_eq!(row["missed_records"], 1);
        assert_eq!(row["bytes_yielded"], prefix.len());
        assert_eq!(row["worker_bytes_since_start"], prefix.len() + 513 * 8192);
        assert_eq!(row["backlog_bytes"], 4_194_304);
        assert_eq!(row["backlog_records"], 512);
        assert_eq!(row["worker_pid"], worker.pid());
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 1);
        assert_eq!(worker.tx.retained(), (0, 0));
        send(&worker, Bytes::from_static(b"tail"));
        assert_eq!(next(&mut fast).await.unwrap(), "tail");
        drop(fast);
    })
    .await;
}

#[tokio::test]
async fn revocation_wins_over_shutdown_and_queued_ts() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"must-not-escape"));
        let sessions = app.playback_auth.snapshots();
        assert_eq!(sessions.len(), 1);
        assert!(
            app.playback_auth
                .revoke(sessions[0]["id"].as_str().unwrap())
        );
        worker.cancel.cancel();
        assert!(next(&mut body).await.is_none());
        drop(body);
        let row = one(&rows, "authorization_revoked");
        assert_eq!(row["bytes_yielded"], 0);
        assert_eq!(row["authorization_cancelled"], true);
        assert_eq!(row["worker_closed"], true);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    })
    .await;
}

#[tokio::test]
async fn stopped_worker_is_identified_before_queued_data_can_escape() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"must-not-escape"));
        app.media.stop("owned").await;
        assert!(next(&mut body).await.is_none());
        drop(body);
        let row = one(&rows, "worker_stopped");
        assert_eq!(row["bytes_yielded"], 0);
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], true);
        assert_eq!(row["worker_alive"], false);
    })
    .await;
}

#[tokio::test]
async fn unpolled_body_drop_is_recorded_and_releases_viewer_and_backlog() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        send(&worker, Bytes::from_static(b"pending"));
        drop(body);
        let row = one(&rows, "body_dropped");
        assert_eq!(row["bytes_yielded"], 0);
        assert_eq!(row["backlog_bytes"], 7);
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert_eq!(worker.tx.retained(), (0, 0));
    })
    .await;
}

#[tokio::test]
async fn pending_body_drop_records_only_yielded_bytes_and_releases_unread_backlog() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        let prefix = bootstrap();
        send(&worker, prefix.clone());
        started(&mut body, &prefix).await;
        assert!(body.frame().now_or_never().is_none());
        send(&worker, Bytes::from_static(b"pending"));
        drop(body);
        let row = one(&rows, "body_dropped");
        assert_eq!(row["bytes_yielded"], prefix.len());
        assert_eq!(row["worker_bytes_since_start"], prefix.len() + 7);
        assert_eq!(row["backlog_bytes"], 7);
        assert_eq!(row["authorization_cancelled"], false);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        assert_eq!(worker.tx.retained(), (0, 0));
    })
    .await;
}

#[tokio::test]
async fn producer_queue_close_is_distinct_and_drains_remaining_ts() {
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    checked(&app, async {
        let mut body = body(&app, "private-viewer-token").await;
        let worker = worker(&app).await;
        let prefix = bootstrap();
        send(&worker, prefix.clone());
        send(&worker, Bytes::from_static(b"last"));
        worker.tx.close();
        started(&mut body, &prefix).await;
        assert_eq!(next(&mut body).await.unwrap(), "last");
        assert!(next(&mut body).await.is_none());
        drop(body);
        let row = one(&rows, "queue_closed");
        assert_eq!(row["bytes_yielded"], prefix.len() + 4);
        assert_eq!(row["missed_records"], 0);
        assert_eq!(row["worker_closed"], false);
        assert_eq!(row["backlog_bytes"], 0);
    })
    .await;
}

use crate::transport_tls_fixture;
trait ClientIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> ClientIo for T {}

async fn cancelled_blocked_socket(tls: bool, revoke: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (rows, _capture) = capture();
    let (_dir, app) = setup();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let certificates = tls.then(transport_tls_fixture::Certificates::new);
    let server = if let Some(cert) = &certificates {
        tokio::spawn(crate::http_tls::serve(
            listener,
            cert.server(),
            app.clone(),
            CancellationToken::new(),
        ))
    } else {
        tokio::spawn(crate::http_transport::serve(
            listener,
            app.clone(),
            CancellationToken::new(),
        ))
    };
    let result = std::panic::AssertUnwindSafe(async {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.bind("127.81.0.1:0".parse().unwrap()).unwrap();
        let tcp = socket.connect(address).await.unwrap();
        let mut client: Box<dyn ClientIo> = if let Some(cert) = &certificates {
            Box::new(
                tokio_rustls::TlsConnector::from(cert.client())
                    .connect("localhost".try_into().unwrap(), tcp)
                    .await
                    .unwrap(),
            )
        } else {
            Box::new(tcp)
        };
        client
            .write_all(
                b"GET /owned/mpegts?token=private-viewer-token HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .unwrap();
        let worker = worker(&app).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.viewers.load(Ordering::Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let session = app
            .playback_auth
            .snapshots()
            .into_iter()
            .find(|s| s["ip"] == "127.81.0.1")
            .expect("listener must retain real peer identity");
        let mut healthy = if revoke {
            Some(body(&app, "another-private-viewer-token").await)
        } else {
            None
        };
        let prefix = bootstrap();
        let startup_egress = prefix.len() as u64 * if revoke { 2 } else { 1 };
        send(&worker, prefix.clone());
        if let Some(healthy) = &mut healthy {
            started(healthy, &prefix).await;
        }
        let mut header = vec![0; 1024];
        let n = tokio::time::timeout(Duration::from_secs(2), client.read(&mut header))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&header[..n]).starts_with("HTTP/1.1 200"));
        tokio::time::timeout(Duration::from_secs(2), async {
            while app.egress.load(Ordering::Relaxed) < startup_egress {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // One legal queue record exceeds socket capacity. Pause only this
        // receiver; host network policy remains unchanged.
        send(&worker, Bytes::from(vec![0x47; 4 * 1024 * 1024]));
        tokio::time::timeout(Duration::from_secs(2), async {
            while app.egress.load(Ordering::Relaxed) < startup_egress + 4 * 1024 * 1024 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if revoke {
            assert!(app.playback_auth.revoke(session["id"].as_str().unwrap()));
        } else {
            app.media.stop("owned").await;
            assert!(worker.is_closed());
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while worker.viewers.load(Ordering::Relaxed) != if revoke { 1 } else { 0 } {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled delivery retained viewer while transport blocked");
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut bytes = [0; 4096];
            loop {
                match client.read(&mut bytes).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
        .await
        .expect("old media socket still drains after cancellation");
        let row = one(
            &rows,
            if revoke {
                "authorization_revoked"
            } else {
                "worker_stopped"
            },
        );
        assert_eq!(row["transport_abort_requested"], true);
        if let Some(healthy) = &mut healthy {
            assert!(!worker.is_closed());
            assert_eq!(next(healthy).await.unwrap().len(), 4 * 1024 * 1024);
            send(&worker, Bytes::from_static(b"healthy-tail"));
            assert_eq!(next(healthy).await.unwrap(), "healthy-tail");
        }
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(3));
        if let Some(cert) = &certificates {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&std::fs::read(&cert.ca).unwrap()).unwrap(),
            );
        }
        let scheme = if tls { "https" } else { "http" };
        let response = builder
            .build()
            .unwrap()
            .get(format!(
                "{scheme}://localhost:{}/flussonix/api/v1/node",
                address.port()
            ))
            .header("X-Flussonix-Peer", "private-peer-password")
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            200,
            "unaffected connection must keep serving"
        );
    })
    .catch_unwind()
    .await;
    server.abort();
    let _ = server.await;
    app.media.stop_all().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
#[tokio::test]
async fn stopped_worker_releases_viewer_while_http_socket_write_is_blocked() {
    cancelled_blocked_socket(false, false).await;
}
#[tokio::test]
async fn stopped_worker_releases_viewer_while_https_socket_write_is_blocked() {
    cancelled_blocked_socket(true, false).await;
}
#[tokio::test]
async fn revoked_http_viewer_releases_blocked_transport_and_preserves_another_viewer() {
    cancelled_blocked_socket(false, true).await;
}
#[tokio::test]
async fn revoked_https_viewer_releases_blocked_transport_and_preserves_another_viewer() {
    cancelled_blocked_socket(true, true).await;
}

#[tokio::test]
async fn unpolled_transport_cancellation_prioritizes_authorization_and_excludes_http2() {
    for version in [axum::http::Version::HTTP_11, axum::http::Version::HTTP_2] {
        let (rows, _capture) = capture();
        let (_dir, app) = setup();
        checked(&app, async {
            let control = crate::http_transport::Control::default();
            let mut request = Request::builder()
                .uri("/owned/mpegts?token=private-viewer-token")
                .version(version)
                .body(Body::empty())
                .unwrap();
            request.extensions_mut().insert(control.clone());
            let response = router(app.clone()).oneshot(request).await.unwrap();
            assert_eq!(response.status(), 200);
            let worker = worker(&app).await;
            let id = app.playback_auth.snapshots()[0]["id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(app.playback_auth.revoke(&id));
            worker.cancellation_token().cancel();
            if version == axum::http::Version::HTTP_11 {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !control.is_aborted() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            let mut body = response.into_body();
            assert!(next(&mut body).await.is_none());
            drop(body);
            assert_eq!(
                control.is_aborted(),
                version == axum::http::Version::HTTP_11
            );
            let row = one(&rows, "authorization_revoked");
            assert_eq!(
                row["transport_abort_requested"],
                version == axum::http::Version::HTTP_11
            );
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        })
        .await;
    }
}
