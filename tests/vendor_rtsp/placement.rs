//! Real native LB/CDN control plane and vendor-private media plane on loopback.
use super::*;
use axum::{body::Body, http::Request};
use flussonix::server::router;
use http_body_util::BodyExt;
use tower::ServiceExt;

async fn reserve(f: &Fixture, name: &str, protocol: &str, token: &str) -> (u16, String) {
    let response = router(f.app.clone()).oneshot(Request::builder()
        .method("POST").uri("/flussonix/api/v1/admit")
        .header("X-Flussonix-Peer", "owned-native-peer")
        .header("content-type", "application/json")
        .body(Body::from(json!({"name":name,"protocol":protocol,"token_hash":format!("{:x}",Sha256::digest(token.as_bytes()))}).to_string())).unwrap()).await.unwrap();
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, value["ticket"].as_str().unwrap_or_default().into())
}
async fn measured(f: &Fixture) {
    f.app.sample_metrics();
    tokio::time::sleep(Duration::from_millis(150)).await;
    f.app.sample_metrics();
}
fn location(headers: &str) -> String {
    headers
        .lines()
        .find_map(|l| l.strip_prefix("Location: "))
        .unwrap()
        .into()
}

#[tokio::test]
async fn vendor_rtsp_capacity_ticket_is_viewer_bound_once_only_and_no_media_until_describe() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    let (url, cancel, task) = listener(&f).await;
    let addr = url::Url::parse(&url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    f.app.set_rtsp_publication(Some(addr), None);
    measured(&f).await;
    let (status, ticket) = reserve(&f, "owned", "rtsp", "owned-viewer").await;
    if status != 200 {
        cancel.cancel();
        task.await.unwrap().unwrap();
        f.stop().await;
        assert_eq!(
            status, 200,
            "vendor source should be reservable without a native stream row"
        );
        return;
    }
    assert_eq!(
        f.app.playback_auth.active(),
        0,
        "capacity created synthetic viewer permission"
    );
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    let uri = format!("{url}?token=owned-viewer&flussonix_ticket={ticket}");
    let mut denied = connect(&url).await;
    assert_eq!(
        request(
            &mut denied,
            "DESCRIBE",
            &uri.replace("token=owned-viewer", "token=wrong"),
            ""
        )
        .await
        .0,
        403
    );
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    let mut viewer = connect(&url).await;
    let (code, _, body) = request(&mut viewer, "DESCRIBE", &uri, "").await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    setup_play(&mut viewer, &url, id).await;
    assert_eq!(packet(&mut viewer).await[1] & 0x7f, 14);
    let mut duplicate = connect(&url).await;
    assert_eq!(request(&mut duplicate, "DESCRIBE", &uri, "").await.0, 503);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    drop((denied, viewer, duplicate));
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn vendor_lb_redirects_to_cdn_and_shares_private_pull_with_http() {
    let cdn = Fixture::configured("cdn", policy(), None, true).await;
    let (cdn_url, cdn_cancel, cdn_task) = listener(&cdn).await;
    let addr = url::Url::parse(&cdn_url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    cdn.app.set_rtsp_publication(Some(addr), None);
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", http.local_addr().unwrap());
    let app = cdn.app.clone();
    let http_task = tokio::spawn(async move { axum::serve(http, router(app)).await.unwrap() });
    let lb = Fixture::configured("lb", policy(), None, false).await;
    let mut config = cdn.app.config.snapshot();
    config["peers"] = json!([{"hostname":"owned-edge","api_url":api,"flussonix_rtsp_url":cdn_url.trim_end_matches("/owned"),"cluster_key":"owned-native-peer"}]);
    lb.app.config.replace(config).unwrap();
    let (lb_url, lb_cancel, lb_task) = listener(&lb).await;
    measured(&cdn).await;
    let mut client = connect(&lb_url).await;
    let (code, headers, _) = request(
        &mut client,
        "DESCRIBE",
        &format!("{lb_url}?token=owned-viewer&extra=a%2Bb"),
        "",
    )
    .await;
    if code != 302 {
        drop(client);
        lb_cancel.cancel();
        lb_task.await.unwrap().unwrap();
        lb.stop().await;
        http_task.abort();
        let _ = http_task.await;
        cdn_cancel.cancel();
        cdn_task.await.unwrap().unwrap();
        cdn.stop().await;
        assert_eq!(code, 302, "vendor LB placement unavailable");
        return;
    }
    let destination = location(&headers);
    assert!(destination.starts_with(&cdn_url));
    assert!(destination.contains("extra=a%2Bb"));
    assert!(destination.contains("flussonix_ticket="));
    assert_eq!(lb.app.media.count().await, 0);
    assert_eq!(cdn.origins[0].private_calls.load(Ordering::SeqCst), 0);
    let mut viewer = connect(&destination).await;
    let (code, _, body) = request(&mut viewer, "DESCRIBE", &destination, "").await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    setup_play(&mut viewer, &cdn_url, id).await;
    let _ = packet(&mut viewer).await;
    let response = cdn.get("/owned/m4s?token=owned-viewer").await;
    assert_eq!(response.status().as_u16(), 200);
    let mut body = response.into_body();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    // Independent client follows the actual LB redirect and strictly decodes
    // media from the same private input already used by RTSP and HTTP viewers.
    let decoded = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("ffmpeg")
            .kill_on_drop(true)
            .args([
                "-v",
                "error",
                "-xerror",
                "-nostdin",
                "-rtsp_transport",
                "tcp",
                "-i",
                &format!("{lb_url}?token=owned-viewer"),
                "-map",
                "0:a:0",
                "-frames:a",
                "3",
                "-f",
                "null",
                "-",
            ])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        decoded.status.success() && decoded.stderr.is_empty(),
        "independent LB decode: {}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    assert_eq!(cdn.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(cdn.origins[1].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(lb.app.media.count().await, 0);
    drop((body, viewer, client));
    lb_cancel.cancel();
    lb_task.await.unwrap().unwrap();
    lb.stop().await;
    http_task.abort();
    let _ = http_task.await;
    cdn_cancel.cancel();
    cdn_task.await.unwrap().unwrap();
    cdn.stop().await;
}

#[tokio::test]
async fn authorized_wrong_token_cannot_consume_another_viewers_reservation() {
    let f = Fixture::configured("cdn", json!({"only":[]}), None, false).await;
    let (url, cancel, task) = listener(&f).await;
    let addr = url::Url::parse(&url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    f.app.set_rtsp_publication(Some(addr), None);
    measured(&f).await;
    let (code, ticket) = reserve(&f, "owned", "rtsp", "owned-viewer").await;
    assert_eq!(code, 200);
    let mut other = connect(&url).await;
    assert_eq!(
        request(
            &mut other,
            "DESCRIBE",
            &format!("{url}?token=another-allowed-viewer&flussonix_ticket={ticket}"),
            ""
        )
        .await
        .0,
        503
    );
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    let mut viewer = connect(&url).await;
    assert_eq!(
        request(
            &mut viewer,
            "DESCRIBE",
            &format!("{url}?token=owned-viewer&flussonix_ticket={ticket}"),
            ""
        )
        .await
        .0,
        200
    );
    drop((other, viewer));
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn concurrent_vendor_ticket_redeemers_have_exactly_one_winner() {
    let f = Fixture::configured("cdn", policy(), None, true).await;
    let (url, cancel, task) = listener(&f).await;
    let addr = url::Url::parse(&url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    f.app.set_rtsp_publication(Some(addr), None);
    measured(&f).await;
    let (code, ticket) = reserve(&f, "owned", "rtsp", "owned-viewer").await;
    assert_eq!(code, 200);
    let uri = format!("{url}?token=owned-viewer&flussonix_ticket={ticket}");
    let mut one = connect(&url).await;
    let mut two = connect(&url).await;
    let (one_reply, two_reply) = tokio::join!(
        request(&mut one, "DESCRIBE", &uri, ""),
        request(&mut two, "DESCRIBE", &uri, "")
    );
    let mut codes = [one_reply.0, two_reply.0];
    codes.sort();
    assert_eq!(codes, [200, 503]);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    drop((one, two));
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn source_edit_or_native_takeover_cannot_reuse_vendor_reservation() {
    for native in [false, true] {
        let f = Fixture::configured("cdn", policy(), None, true).await;
        let (url, cancel, task) = listener(&f).await;
        let addr = url::Url::parse(&url)
            .unwrap()
            .socket_addrs(|| None)
            .unwrap()[0];
        f.app.set_rtsp_publication(Some(addr), None);
        measured(&f).await;
        let (code, ticket) = reserve(&f, "owned", "rtsp", "owned-viewer").await;
        assert_eq!(code, 200);
        let calls = f.origins[0].management_calls.load(Ordering::SeqCst);
        if native {
            f.app
                .config
                .put(
                    "streams",
                    "owned",
                    json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                )
                .unwrap();
        } else {
            let mut root = f.app.config.snapshot();
            for source in root["sources"].as_array_mut().unwrap() {
                source["except"] = json!(["unrelated"]);
            }
            f.app.config.replace(root).unwrap();
        }
        let mut viewer = connect(&url).await;
        assert_eq!(
            request(
                &mut viewer,
                "DESCRIBE",
                &format!("{url}?token=owned-viewer&flussonix_ticket={ticket}"),
                ""
            )
            .await
            .0,
            503
        );
        assert_eq!(
            f.origins[0].management_calls.load(Ordering::SeqCst),
            calls,
            "obsolete ticket triggered discovery"
        );
        assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f.app.media.count().await, 0);
        drop(viewer);
        cancel.cancel();
        task.await.unwrap().unwrap();
        f.stop().await;
    }
}

#[tokio::test]
async fn http_native_wrong_stream_and_expired_tickets_fail_before_vendor_discovery() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    let (url, cancel, task) = listener(&f).await;
    let addr = url::Url::parse(&url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    f.app.set_rtsp_publication(Some(addr), Some(addr));
    measured(&f).await;
    // HTTP reservations deliberately require no vendor discovery.
    let (code, http) = reserve(&f, "owned", "http", "owned-viewer").await;
    assert_eq!(code, 200);
    assert_eq!(f.origins[0].management_calls.load(Ordering::SeqCst), 0);
    let mut viewer = connect(&url).await;
    assert_eq!(
        request(
            &mut viewer,
            "DESCRIBE",
            &format!("{url}?token=owned-viewer&flussonix_ticket={http}"),
            ""
        )
        .await
        .0,
        503
    );
    assert_eq!(f.origins[0].management_calls.load(Ordering::SeqCst), 0);
    let (code, secure) = reserve(&f, "owned", "rtsps", "owned-viewer").await;
    assert_eq!(code, 200);
    let calls = f.origins[0].management_calls.load(Ordering::SeqCst);
    assert_eq!(
        request(
            &mut viewer,
            "DESCRIBE",
            &format!("{url}?token=owned-viewer&flussonix_ticket={secure}"),
            ""
        )
        .await
        .0,
        503
    );
    let (code, ticket) = reserve(&f, "owned", "rtsp", "owned-viewer").await;
    assert_eq!(code, 200);
    assert_eq!(
        request(
            &mut viewer,
            "DESCRIBE",
            &format!(
                "{}?token=owned-viewer&flussonix_ticket={ticket}",
                url.replace("/owned", "/other")
            ),
            ""
        )
        .await
        .0,
        503
    );
    tokio::time::sleep(Duration::from_millis(5100)).await;
    assert_eq!(
        request(
            &mut viewer,
            "DESCRIBE",
            &format!("{url}?token=owned-viewer&flussonix_ticket={ticket}"),
            ""
        )
        .await
        .0,
        503
    );
    assert_eq!(f.origins[0].management_calls.load(Ordering::SeqCst), calls);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    drop(viewer);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

async fn tls_connect(
    url: &str,
    cert: &tls_fixture::Certificates,
) -> BufReader<tokio_rustls::client::TlsStream<TcpStream>> {
    let u = url::Url::parse(url).unwrap();
    let socket = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    let tls = tokio_rustls::TlsConnector::from(cert.client())
        .connect(
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    BufReader::new(tls)
}

#[tokio::test]
async fn secure_vendor_lb_never_downgrades_and_delivers_through_verified_cdn_tls() {
    let cert = tls_fixture::Certificates::new();
    let cdn = Fixture::configured("cdn", policy(), None, false).await;
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let cdn_url = format!("rtsps://{addr}/owned");
    let cdn_cancel = CancellationToken::new();
    let cdn_task = tokio::spawn(rtsp::serve_tls(
        tcp,
        cdn.app.clone(),
        cdn_cancel.clone(),
        cert.server(),
    ));
    cdn.app.set_rtsp_publication(None, Some(addr));
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", http.local_addr().unwrap());
    let app = cdn.app.clone();
    let http_task = tokio::spawn(async move { axum::serve(http, router(app)).await.unwrap() });
    let lb = Fixture::configured("lb", policy(), None, false).await;
    let mut config = cdn.app.config.snapshot();
    config["peers"] = json!([{"hostname":"owned-edge","api_url":api,"flussonix_rtsp_url":format!("rtsp://{addr}"),"cluster_key":"owned-native-peer"}]);
    lb.app.config.replace(config).unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_url = format!("rtsps://{}/owned", tcp.local_addr().unwrap());
    let lb_cancel = CancellationToken::new();
    let lb_task = tokio::spawn(rtsp::serve_tls(
        tcp,
        lb.app.clone(),
        lb_cancel.clone(),
        cert.server(),
    ));
    measured(&cdn).await;
    let mut client = tls_connect(&lb_url, &cert).await;
    assert_eq!(
        request(
            &mut client,
            "DESCRIBE",
            &format!("{lb_url}?token=owned-viewer"),
            ""
        )
        .await
        .0,
        503,
        "secure LB used plain RTSP peer"
    );
    assert_eq!(cdn.origins[0].private_calls.load(Ordering::SeqCst), 0);
    lb.app
        .config
        .put(
            "peers",
            "owned-edge",
            json!({"flussonix_rtsps_url":cdn_url.trim_end_matches("/owned")}),
        )
        .unwrap();
    let (code, headers, _) = request(
        &mut client,
        "DESCRIBE",
        &format!("{lb_url}?token=owned-viewer"),
        "",
    )
    .await;
    assert_eq!(code, 302);
    let destination = location(&headers);
    assert!(destination.starts_with(&cdn_url));
    let mut viewer = tls_connect(&destination, &cert).await;
    let (code, _, body) = request(&mut viewer, "DESCRIBE", &destination, "").await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    setup_play(&mut viewer, &cdn_url, id).await;
    assert_eq!(packet(&mut viewer).await[1] & 0x7f, 14);
    assert_eq!(cdn.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(lb.app.media.count().await, 0);
    drop((client, viewer));
    lb_cancel.cancel();
    lb_task.await.unwrap().unwrap();
    lb.stop().await;
    http_task.abort();
    let _ = http_task.await;
    cdn_cancel.cancel();
    cdn_task.await.unwrap().unwrap();
    cdn.stop().await;
}

#[tokio::test]
async fn source_changes_during_viewer_callback_cannot_redeem_ticket_or_pull_media() {
    use std::sync::Arc;
    use tokio::sync::Notify;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback = format!("http://{}/auth", http.local_addr().unwrap());
    let (started, unblock) = (entered.clone(), release.clone());
    let auth = tokio::spawn(async move {
        axum::serve(
            http,
            axum::Router::new().route(
                "/auth",
                axum::routing::get(move || {
                    let (started, unblock) = (started.clone(), unblock.clone());
                    async move {
                        started.notify_one();
                        unblock.notified().await;
                        axum::http::StatusCode::OK
                    }
                }),
            ),
        )
        .await
        .unwrap()
    });
    let f = Fixture::configured("cdn", json!({"on_play":callback,"only":[]}), None, true).await;
    let (url, cancel, task) = listener(&f).await;
    let addr = url::Url::parse(&url)
        .unwrap()
        .socket_addrs(|| None)
        .unwrap()[0];
    f.app.set_rtsp_publication(Some(addr), None);
    measured(&f).await;
    let (code, ticket) = reserve(&f, "owned", "rtsp", "owned-viewer").await;
    assert_eq!(code, 200);
    assert_eq!(f.app.playback_auth.active(), 0, "reservation ran callback");
    let uri = format!("{url}?token=owned-viewer&flussonix_ticket={ticket}");
    let mut viewer = connect(&url).await;
    let pending = tokio::spawn(async move { request(&mut viewer, "DESCRIBE", &uri, "").await.0 });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let mut root = f.app.config.snapshot();
    for source in root["sources"].as_array_mut().unwrap() {
        source["except"] = json!(["unrelated"]);
    }
    f.app.config.replace(root).unwrap();
    release.notify_one();
    assert_eq!(pending.await.unwrap(), 503);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.app.media.count().await, 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
    auth.abort();
    let _ = auth.await;
}
