//! Real RTSP/TLS listener tests using an independently authenticated vendor API fixture.
use super::{Vendor, app, request, tls_fixture};
use flussonix::{rtsp, server::App};
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
struct Listener {
    plain: String,
    secure: String,
    cert: tls_fixture::Certificates,
    stop: CancellationToken,
    tasks: Vec<AbortOnDropHandle<()>>,
}
impl Listener {
    async fn new(app: Arc<App>) -> Self {
        let cert = tls_fixture::Certificates::new();
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let plain = format!("rtsp://{}", tcp.local_addr().unwrap());
        let secure = format!("rtsps://{}", tls.local_addr().unwrap());
        let stop = CancellationToken::new();
        let a = app.clone();
        let cancel = stop.clone();
        let first = AbortOnDropHandle::new(tokio::spawn(async move {
            rtsp::serve(tcp, a, cancel).await.unwrap()
        }));
        let cancel = stop.clone();
        let config = cert.server();
        let second = AbortOnDropHandle::new(tokio::spawn(async move {
            rtsp::serve_tls(tls, app, cancel, config).await.unwrap()
        }));
        Self {
            plain,
            secure,
            cert,
            stop,
            tasks: vec![first, second],
        }
    }
    async fn describe(&self, secure: bool, name: &str, query: &str) -> (u16, String) {
        let base = if secure { &self.secure } else { &self.plain };
        let stream = TcpStream::connect(base.split_once("://").unwrap().1)
            .await
            .unwrap();
        let socket: Box<dyn Io> = if secure {
            let connector = tokio_rustls::TlsConnector::from(self.cert.client());
            Box::new(
                connector
                    .connect(
                        tokio_rustls::rustls::pki_types::ServerName::try_from("127.0.0.1")
                            .unwrap()
                            .to_owned(),
                        stream,
                    )
                    .await
                    .unwrap(),
            )
        } else {
            Box::new(stream)
        };
        let mut socket = BufReader::new(socket);
        let uri = format!(
            "{base}/{name}{}{}",
            if query.is_empty() { "" } else { "?" },
            query
        );
        socket
            .get_mut()
            .write_all(
                format!("DESCRIBE {uri} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            let code = line.split_whitespace().nth(1).unwrap().parse().unwrap();
            let mut location = String::new();
            let mut length = 0;
            loop {
                line.clear();
                assert!(socket.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some((k, v)) = line.split_once(':') {
                    if k.eq_ignore_ascii_case("location") {
                        location = v.trim().to_owned()
                    }
                    if k.eq_ignore_ascii_case("content-length") {
                        length = v.trim().parse::<usize>().unwrap()
                    }
                }
            }
            assert!(length <= 16384);
            socket.read_exact(&mut vec![0; length]).await.unwrap();
            (code, location)
        })
        .await
        .unwrap()
    }
    async fn stop(mut self) {
        self.stop.cancel();
        for task in self.tasks.drain(..) {
            tokio::time::timeout(Duration::from_secs(6), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
fn peer(v: &Vendor) -> serde_json::Value {
    let mut p = v.peer();
    p["flussonix_rtsp_url"] = json!("rtsp://owned-edge.invalid:8554");
    p["flussonix_rtsps_url"] = json!("rtsps://owned-edge.invalid:8322");
    p
}
#[tokio::test]
async fn mapped_vendor_rtsp_and_verified_rtsps_preserve_query_without_native_ticket() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let lb = Listener::new(a.clone()).await;
    let query = "token=a%2Bb&x=1&y=2&flag&empty=";
    for secure in [false, true] {
        let (code, location) = lb.describe(secure, "local/news", query).await;
        assert_eq!(code, 302);
        assert_eq!(
            location,
            format!(
                "{}://owned-edge.invalid:{}/remote/news?token=a%2Bb&x=1&y=2&flag&empty=",
                if secure { "rtsps" } else { "rtsp" },
                if secure { 8322 } else { 8554 }
            )
        );
        assert!(!location.contains("owned-vendor-cluster-key"));
    }
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 3);
    assert_eq!(a.media.count().await, 0);
    assert_eq!(a.playback_auth.live_grants(), 0);
    lb.stop().await;
}
#[tokio::test]
async fn secure_vendor_viewer_cannot_use_clear_destination_and_unmapped_names_do_not_probe() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    let mut p = peer(&v);
    p.as_object_mut().unwrap().remove("flussonix_rtsps_url");
    a.config.put("peers", "edge", p).unwrap();
    a.config
        .put(
            "streams",
            "unmapped",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let lb = Listener::new(a.clone()).await;
    assert_eq!(lb.describe(true, "local/news", "").await.0, 503);
    assert_eq!(lb.describe(false, "unmapped", "").await.0, 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    assert_eq!(a.media.count().await, 0);
    lb.stop().await;
}
#[tokio::test]
async fn vendor_http_and_rtsp_share_one_pending_egress_budget_in_both_orders() {
    for rtsp_first in [false, true] {
        let v = Vendor::new().await;
        v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
        let (_dir, a) = app();
        a.config.put("peers", "edge", peer(&v)).unwrap();
        let lb = Listener::new(a.clone()).await;
        if rtsp_first {
            assert_eq!(lb.describe(false, "local/news", "").await.0, 302);
            assert_eq!(request(&a, "/local/news/m4s").await.status(), 503)
        } else {
            assert_eq!(request(&a, "/local/news/m4s").await.status(), 302);
            assert_eq!(lb.describe(true, "local/news", "").await.0, 503)
        }
        assert_eq!(a.media.count().await, 0);
        lb.stop().await;
    }
}
#[tokio::test]
async fn vendor_tls_only_idle_route_encodes_mapped_name_for_clear_viewer() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    let mut p = peer(&v);
    p.as_object_mut().unwrap().remove("flussonix_rtsp_url");
    p["flussonix_stream_map"][0]["remote_name"] = json!("remote/Café+HD");
    v.state.stream.lock().unwrap()["name"] = json!("remote/Café+HD");
    v.state.stream.lock().unwrap()["stats"]["running"] = json!(false);
    a.config.put("peers", "edge", p).unwrap();
    let lb = Listener::new(a.clone()).await;
    let (code, location) = lb
        .describe(false, "local/news", "token=one%2btwo&flag")
        .await;
    assert_eq!(code, 302);
    assert_eq!(
        location,
        "rtsps://owned-edge.invalid:8322/remote/Caf%C3%A9%2BHD?token=one%2btwo&flag"
    );
    assert_eq!(a.media.count().await, 0);
    lb.stop().await;
}
#[tokio::test]
async fn overloaded_missing_and_malformed_vendor_rtsp_metadata_expose_no_redirect() {
    for case in [
        "cpu",
        "ram",
        "uplink",
        "missing",
        "malformed",
        "identity",
        "drain",
    ] {
        let v = Vendor::new().await;
        match case {
            "cpu" => v.state.config.lock().unwrap()["stats"]["cpu_usage"] = json!(95),
            "ram" => v.state.config.lock().unwrap()["stats"]["memory_usage"] = json!(95),
            "uplink" => v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(95000),
            "missing" => v.state.stream_status.store(404, Ordering::SeqCst),
            "malformed" => v.state.stream.lock().unwrap()["stats"] = json!({}),
            "identity" => v.state.flip.store(true, Ordering::SeqCst),
            "drain" => {}
            _ => unreachable!(),
        }
        let (_dir, a) = app();
        let mut profile = peer(&v);
        if case == "drain" {
            profile["drain"] = json!(true);
        }
        a.config.put("peers", "edge", profile).unwrap();
        let lb = Listener::new(a.clone()).await;
        let (code, location) = lb.describe(true, "local/news", "").await;
        assert_eq!(code, 503, "{case}");
        assert!(location.is_empty());
        assert_eq!(a.playback_auth.live_grants(), 0);
        assert_eq!(a.media.count().await, 0);
        lb.stop().await;
    }
}
#[tokio::test]
async fn concurrent_vendor_http_rtsp_and_rtsps_placements_share_one_capacity_hold() {
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let lb = Arc::new(Listener::new(a.clone()).await);
    let mut tasks = Vec::new();
    for index in 0..12 {
        let lb = lb.clone();
        let app = a.clone();
        tasks.push(tokio::spawn(async move {
            if index % 3 == 0 {
                request(&app, "/local/news/m4s?token=owned")
                    .await
                    .status()
                    .as_u16()
            } else {
                lb.describe(index % 2 == 0, "local/news", "token=owned")
                    .await
                    .0
            }
        }));
    }
    let mut found = 0;
    for task in tasks {
        let status = task.await.unwrap();
        if status == 302 {
            found += 1
        } else {
            assert_eq!(status, 503)
        }
    }
    assert_eq!(found, 1, "one 5Mbps viewer fits below 90% uplink");
    assert_eq!(a.media.count().await, 0);
    Arc::try_unwrap(lb).ok().unwrap().stop().await;
}
#[tokio::test]
async fn denied_forged_and_duplicate_query_rtsp_viewers_do_not_discover_vendor() {
    use sha2::{Digest, Sha256};
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    a.config
        .put(
            "streams",
            "local/news",
            json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"allowed"))}),
        )
        .unwrap();
    let lb = Listener::new(a.clone()).await;
    for (query, code) in [
        ("token=wrong", 403),
        ("token=allowed&flussonix_ticket=forged", 503),
        ("token=allowed&token=wrong", 400),
    ] {
        let (status, location) = lb.describe(false, "local/news", query).await;
        assert_eq!(status, code);
        assert!(location.is_empty())
    }
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    assert_eq!(a.media.count().await, 0);
    lb.stop().await;
}
#[tokio::test]
async fn changed_config_during_vendor_rtsp_observation_blocks_redirect() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let lb = Arc::new(Listener::new(a.clone()).await);
    let (entered, seen) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    *v.state.hold.lock().unwrap() = Some((entered, held));
    let copy = lb.clone();
    let task = tokio::spawn(async move { copy.describe(true, "local/news", "").await });
    tokio::time::timeout(Duration::from_secs(2), seen)
        .await
        .unwrap()
        .unwrap();
    a.config
        .put("streams", "local/news", json!({"disabled":true}))
        .unwrap();
    let _ = release.send(());
    let (code, location) = task.await.unwrap();
    assert!([403, 503].contains(&code));
    assert!(location.is_empty());
    assert_eq!(a.media.count().await, 0);
    assert_eq!(a.playback_auth.live_grants(), 0);
    Arc::try_unwrap(lb).ok().unwrap().stop().await;
}
#[tokio::test]
async fn vendor_rtsp_self_destination_is_excluded_before_observation() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    let lb = Listener::new(a.clone()).await;
    let mut p = peer(&v);
    p["flussonix_rtsp_url"] = json!(lb.plain);
    p["flussonix_rtsps_url"] = json!(lb.secure);
    a.config.put("peers", "edge", p).unwrap();
    for secure in [false, true] {
        assert_eq!(lb.describe(secure, "local/news", "").await.0, 503)
    }
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    lb.stop().await;
}
#[tokio::test]
async fn changed_vendor_rtsp_identity_blocks_http_placement_too() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let lb = Listener::new(a.clone()).await;
    assert_eq!(lb.describe(false, "local/news", "").await.0, 302);
    v.state.config.lock().unwrap()["stats"]["id"] = json!("replacement-vendor");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(lb.describe(true, "local/news", "").await.0, 503);
    assert_eq!(request(&a, "/local/news/m4s").await.status(), 503);
    assert_eq!(a.media.count().await, 0);
    lb.stop().await;
}
#[tokio::test]
async fn native_rtsp_admission_failure_can_retry_a_vendor_destination() {
    use axum::{
        Router,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let calls = count.clone();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", tcp.local_addr().unwrap());
    let service=Router::new().route("/flussonix/api/v1/rtsp-routing",get(|headers:HeaderMap|async move{assert_eq!(headers["x-flussonix-peer"],"owned-native-peer-key");axum::Json(json!({"role":"cdn","uplink":0.01,"uplink_mbps":100,"cpu":0.01,"ram":0.01,"drain":false,"age_ms":0,"active":0,"reserved":0,"reserved_mbps":0,"limit":100,"ready":[],"stream_bitrates":{},"rtsp_publication":{"rtsp":"owned-native.invalid:8554"}}))})).route("/flussonix/api/v1/admit",post(move|headers:HeaderMap|{let calls=calls.clone();async move{assert_eq!(headers["x-flussonix-peer"],"owned-native-peer-key");assert!(!headers.contains_key("authorization"));calls.fetch_add(1,Ordering::SeqCst);StatusCode::SERVICE_UNAVAILABLE}}));
    let native = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(tcp, service).await.unwrap()
    }));
    a.config
        .put(
            "peers",
            "native",
            json!({"api_url":api,"flussonix_rtsp_url":"rtsp://owned-native.invalid:8554"}),
        )
        .unwrap();
    let lb = Listener::new(a.clone()).await;
    let (code, location) = lb.describe(false, "local/news", "token=owned").await;
    assert_eq!(code, 302);
    assert!(location.starts_with("rtsp://owned-edge.invalid:8554/remote/news?"));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(a.media.count().await, 0);
    lb.stop().await;
    native.abort();
    let _ = native.await;
}
#[tokio::test]
async fn independent_decoder_follows_vendor_redirect_and_destination_checks_viewer_policy() {
    use futures_util::FutureExt;
    use sha2::{Digest, Sha256};
    let v = Vendor::new().await;
    let (_dir, a) = app();
    let delivery_dir = tempfile::tempdir().unwrap();
    let delivery = App::new(
        delivery_dir.path().join("config.json"),
        delivery_dir.path().join("media"),
        flussonix::server::Options {
            role: "standalone".into(),
            node_name: "owned-delivery".into(),
            admin_password: "owned-delivery-admin".into(),
            peer_key: "owned-delivery-peer-key".into(),
            ..Default::default()
        },
    )
    .unwrap();
    delivery.config.put("streams","remote/news",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"allowed-on-cdn"))})).unwrap();
    let cdn = Listener::new(delivery.clone()).await;
    let mut p = peer(&v);
    p["flussonix_rtsp_url"] = json!(cdn.plain);
    p["flussonix_rtsps_url"] = json!(cdn.secure);
    a.config.put("peers", "edge", p).unwrap();
    let lb = Listener::new(a.clone()).await;
    let result = std::panic::AssertUnwindSafe(async {
        assert_eq!(
            lb.describe(false, "local/news", "token=denied-on-cdn")
                .await
                .0,
            302
        );
        assert_eq!(
            cdn.describe(false, "remote/news", "token=denied-on-cdn")
                .await
                .0,
            403
        );
        assert_eq!(delivery.media.count().await, 0);
        let trust = delivery_dir.path().join("viewer-ca.pem");
        let mut roots = std::fs::read(&lb.cert.ca).unwrap();
        roots.extend(std::fs::read(&cdn.cert.ca).unwrap());
        std::fs::write(&trust, roots).unwrap();
        // Trusting the LB alone cannot grant trust to the redirected destination.
        let rejected = flussonix::tls_input::Bridge::prepare(
            &format!("{}/local/news?token=allowed-on-cdn", lb.secure),
            Some(&lb.cert.ca),
        )
        .await
        .unwrap();
        let failed = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("ffmpeg")
                .args([
                    "-nostdin",
                    "-v",
                    "error",
                    "-rtsp_transport",
                    "tcp",
                    "-i",
                    rejected.local_url(),
                    "-t",
                    "1",
                    "-f",
                    "null",
                    "-",
                ])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        rejected.close().await;
        assert!(!failed.status.success());
        assert_eq!(delivery.media.count().await, 0);
        for secure in [false, true] {
            let uri = format!(
                "{}/local/news?token=allowed-on-cdn",
                if secure { &lb.secure } else { &lb.plain }
            );
            // Older FFmpeg RTSP demuxers cannot pass TLS verification options.
            // The production bridge verifies every TLS hop and owns all loopback relays.
            let bridge = if secure {
                Some(
                    flussonix::tls_input::Bridge::prepare(&uri, Some(&trust))
                        .await
                        .unwrap(),
                )
            } else {
                None
            };
            let input = bridge
                .as_ref()
                .map(|b| b.local_url())
                .unwrap_or(&uri)
                .to_owned();
            let mut command = tokio::process::Command::new("ffmpeg");
            command.args(["-nostdin", "-v", "error", "-rtsp_transport", "tcp"]);
            let output = tokio::time::timeout(
                Duration::from_secs(30),
                command
                    .args([
                        "-i", &input, "-t", "2", "-map", "0:v:0", "-map", "0:a:0", "-threads", "1",
                        "-f", "framemd5", "-",
                    ])
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .unwrap()
            .unwrap();
            if let Some(bridge) = bridge {
                bridge.close().await;
            }
            assert!(
                output.status.success() && output.stderr.is_empty(),
                "independent {} decode: {}",
                if secure { "RTSPS" } else { "RTSP" },
                String::from_utf8_lossy(&output.stderr)
            );
            let frames = String::from_utf8_lossy(&output.stdout);
            assert!(
                frames.contains("#media_type 0: video") && frames.contains("#media_type 1: audio")
            );
            assert!(frames.lines().filter(|l| l.starts_with("0,")).count() >= 20);
            assert!(frames.lines().filter(|l| l.starts_with("1,")).count() >= 40);
            assert_eq!(a.media.count().await, 0);
            assert_eq!(delivery.media.count().await, 1);
        }
    })
    .catch_unwind()
    .await;
    delivery.media.stop_all().await;
    lb.stop().await;
    cdn.stop().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error)
    }
}
