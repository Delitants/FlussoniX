//! Native HTTP placement into vendor-backed FlussoniX CDNs.
use super::*;

async fn http_reservation(f: &Fixture, name: &str) -> String {
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        f.app.sample_metrics();
        let response = router(f.app.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/flussonix/api/v1/admit")
                    .header("X-Flussonix-Peer", "owned-native-peer-key")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"name":name}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            let value: Value = serde_json::from_slice(&bytes(response).await).unwrap();
            return value["ticket"].as_str().unwrap().to_owned();
        }
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    panic!("owned CDN never acquired measured admission capacity")
}

#[tokio::test]
async fn vendor_http_placement_redeems_once_without_pulling_until_clean_playback() {
    use sha2::{Digest, Sha256};
    let f = Fixture::variant(
        json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}),
        "m4s",
        "cdn",
    )
    .await;
    let ticket = http_reservation(&f, "owned").await;
    let path = format!("/owned/m4s?token=viewer&other=a%2Bb&flussonix_ticket={ticket}");
    let response = f.get(&path).await;
    let status = response.status();
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    // Cleanup precedes the assertion while this is a RED regression.
    if status != StatusCode::FOUND {
        f.stop().await;
        assert_eq!(status, StatusCode::FOUND);
        return;
    }
    let location = location.unwrap();
    assert!(location.starts_with("/owned/m4s?"));
    let q: std::collections::HashMap<_, _> =
        url::form_urlencoded::parse(location.split_once('?').unwrap().1.as_bytes())
            .into_owned()
            .collect();
    assert_eq!(q.get("token").map(String::as_str), Some("viewer"));
    assert_eq!(q.get("other").map(String::as_str), Some("a+b"));
    assert!(!q.contains_key("flussonix_ticket"));
    assert_eq!(f.media_calls(), 0, "redemption started a private pull");
    assert_eq!(
        f.app.playback_auth.active(),
        0,
        "control-only redemption left playback occupancy"
    );
    assert_eq!(f.get(&path).await.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = f.get(&location).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    assert!(
        native_info(&mut body)
            .await
            .iter()
            .any(|t| t.codec == "mp3")
    );
    assert_eq!(f.media_calls(), 1);
    drop(body);
    f.stop().await;
}

#[tokio::test]
async fn vendor_http_placement_denied_viewer_cannot_consume_valid_ticket() {
    use sha2::{Digest, Sha256};
    let f =
        Fixture::new(json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}))
            .await;
    let ticket = http_reservation(&f, "owned").await;
    let denied = f
        .get(&format!("/owned/m4s?token=wrong&flussonix_ticket={ticket}"))
        .await
        .status();
    if denied != StatusCode::FORBIDDEN {
        f.stop().await;
        assert_eq!(denied, StatusCode::FORBIDDEN);
        return;
    }
    assert_eq!(f.media_calls(), 0);
    assert_eq!(
        f.get(&format!(
            "/owned/m4s?token=viewer&flussonix_ticket={ticket}"
        ))
        .await
        .status(),
        StatusCode::FOUND
    );
    assert_eq!(f.media_calls(), 0);
    f.stop().await;
}

#[tokio::test]
async fn vendor_http_placement_rejects_wrong_stream_and_expired_ticket_before_discovery() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let ticket = http_reservation(&f, "other").await;
    assert_eq!(
        f.get(&format!("/owned/m4s?flussonix_ticket={ticket}"))
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.calls.lock().unwrap().is_empty());
    let ticket = http_reservation(&f, "owned").await;
    tokio::time::sleep(Duration::from_millis(5100)).await;
    assert_eq!(
        f.get(&format!("/owned/m4s?flussonix_ticket={ticket}"))
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.calls.lock().unwrap().is_empty());
    f.stop().await;
}

#[tokio::test]
async fn vendor_http_placement_concurrent_redemption_has_exactly_one_winner() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let ticket = http_reservation(&f, "owned").await;
    let path = format!("/owned/m4s?flussonix_ticket={ticket}");
    let (a, b) = tokio::join!(f.get(&path), f.get(&path));
    let mut statuses = [a.status().as_u16(), b.status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [302, 503]);
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.playback_auth.active(), 0);
    f.stop().await;
}

#[tokio::test]
async fn vendor_http_placement_pending_policy_edit_cannot_redeem_or_pull() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let s = started.clone();
    let r = release.clone();
    let callback = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(get(move || {
                let s = s.clone();
                let r = r.clone();
                async move {
                    s.notify_one();
                    r.notified().await;
                    StatusCode::OK
                }
            })),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put("sources", &f.source, json!({"on_play":url}))
        .unwrap();
    let ticket = http_reservation(&f, "owned").await;
    let service = router(f.app.clone());
    let path = format!("/owned/m4s?flussonix_ticket={ticket}");
    let pending = tokio::spawn(async move {
        service
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"on_play":"true","flussonix_token_sha256":"a".repeat(64)}),
        )
        .unwrap();
    release.notify_one();
    assert_eq!(
        pending.await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.media_calls(), 0);
    // Replace the guard: failed stale authorization did not consume the ticket.
    use sha2::{Digest, Sha256};
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}),
        )
        .unwrap();
    assert_eq!(
        f.get(&format!(
            "/owned/m4s?token=viewer&flussonix_ticket={ticket}"
        ))
        .await
        .status(),
        StatusCode::FOUND
    );
    assert_eq!(f.app.playback_auth.active(), 0);
    callback.abort();
    let _ = callback.await;
    f.stop().await;
}

pub(super) async fn check_native_lb_vendor_cdn(file: &str, discovery: bool) {
    use sha2::{Digest, Sha256};
    let guard = format!("{:x}", Sha256::digest(b"viewer"));
    let f = Fixture::variant(json!({"flussonix_token_sha256":guard}), "m4s", "cdn").await;
    let cdn_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cdn_url = format!("http://{}", cdn_listener.local_addr().unwrap());
    let cdn = f.app.clone();
    let cdn_server =
        tokio::spawn(async move { axum::serve(cdn_listener, router(cdn)).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let lb = App::new(
        dir.path().join("lb.json"),
        dir.path().join("lb-media"),
        Options {
            role: "lb".into(),
            admin_password: "owned-lb-admin-key".into(),
            peer_key: "owned-native-peer-key".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    if discovery {
        let source = f.app.config.snapshot()["sources"][0].clone();
        lb.config.put("sources", &f.source, source).unwrap();
        assert!(
            lb.config.snapshot()["streams"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    } else {
        lb.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"flussonix_token_sha256":guard}),
            )
            .unwrap();
    }
    let lb_cancel = CancellationToken::new();
    let lb_owner = if discovery {
        let a = lb.clone();
        let c = lb_cancel.clone();
        Some(tokio::spawn(async move { a.run_vendor_playback(c).await }))
    } else {
        None
    };
    tokio::task::yield_now().await;
    lb.config
        .put(
            "peers",
            "edge",
            json!({"api_url":cdn_url,"public_payload_url":cdn_url,"private_payload_url":cdn_url}),
        )
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(12))
        .build()
        .unwrap();
    let mut eligible = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        f.app.sample_metrics();
        let node: Value = client
            .get(format!("{cdn_url}/flussonix/api/v1/node"))
            .header("X-Flussonix-Peer", "owned-native-peer-key")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if node["cpu"].as_f64().is_some_and(|v| v < 0.9)
            && node["ram"].as_f64().is_some_and(|v| v < 0.95)
            && node["uplink"].as_f64().is_some_and(|v| v < 0.8)
        {
            eligible = true;
            break;
        }
    }
    assert!(eligible);
    let service = router(lb.clone());
    let denied = service
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/owned/{file}?token=wrong"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let denied_status = denied.status();
    if denied_status != StatusCode::FORBIDDEN {
        lb_cancel.cancel();
        if let Some(task) = lb_owner {
            assert!(task.await.unwrap().is_ok());
        }
        cdn_server.abort();
        let _ = cdn_server.await;
        lb.media.stop_all().await;
        f.stop().await;
        assert_eq!(denied_status, StatusCode::FORBIDDEN);
        return;
    }
    assert_eq!(f.media_calls(), 0);
    if !discovery {
        assert!(f.calls.lock().unwrap().is_empty());
    }
    let discovery_calls = f.calls.lock().unwrap().len();
    let response = service
        .oneshot(
            Request::builder()
                .uri(format!("/owned/{file}?token=viewer"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    let destination = response.headers()["location"].to_str().unwrap().to_owned();
    assert!(destination.starts_with(&cdn_url));
    assert!(destination.contains("flussonix_ticket="));
    assert!(!destination.contains("owned-private-cluster-key"));
    assert!(!destination.contains("owned-native-peer-key"));
    assert_eq!(f.media_calls(), 0, "LB pulled vendor media");
    assert_eq!(
        f.calls.lock().unwrap().len(),
        discovery_calls,
        "LB repeated cached source discovery"
    );
    if discovery {
        assert!(discovery_calls > 0);
    }
    let redeemed = client.get(&destination).send().await.unwrap();
    assert_eq!(redeemed.status(), StatusCode::FOUND);
    let clean = redeemed.headers()["location"].to_str().unwrap().to_owned();
    assert!(!clean.contains("flussonix_ticket"));
    assert_eq!(f.media_calls(), 0);
    assert_eq!(
        client.get(&destination).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = client
        .get(format!("{cdn_url}{clean}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut media = None;
    if file.ends_with(".m3u8") {
        let mut manifest = response.text().await.unwrap();
        for _ in 0..40 {
            if manifest.contains("#EXTINF:") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            manifest = client
                .get(format!("{cdn_url}{clean}"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
        }
        let segment = manifest
            .lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        assert!(segment.contains("token=viewer"));
        let clean_url = url::Url::parse(&format!("{cdn_url}{clean}")).unwrap();
        let segment_url = clean_url.join(segment).unwrap();
        let denied = client
            .get(segment_url.as_str().split('?').next().unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let response = client.get(segment_url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        media = Some(response.bytes().await.unwrap());
    } else {
        let mut body = Body::from_stream(response.bytes_stream());
        assert!(
            native_info(&mut body)
                .await
                .iter()
                .any(|t| t.codec == "mp3")
        );
        drop(body);
    }
    assert_eq!(f.media_calls(), 1);
    if discovery {
        lb.reconcile().await;
    }
    assert_eq!(lb.media.count().await, 0);
    lb_cancel.cancel();
    if let Some(task) = lb_owner {
        assert!(task.await.unwrap().is_ok());
    }
    cdn_server.abort();
    let _ = cdn_server.await;
    lb.media.stop_all().await;
    f.stop().await;
    if let Some(media) = media {
        let path = dir.path().join("owned-lb-output.ts");
        std::fs::write(&path, media).unwrap();
        let mut child = tokio::process::Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-i"])
            .arg(path)
            .args(["-t", "0.1", "-f", "null", "-"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let status = match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
            Ok(status) => status.unwrap(),
            Err(_) => {
                child.kill().await.unwrap();
                let _ = child.wait().await;
                panic!("owned LB segment decoder timed out")
            }
        };
        assert!(status.success());
    }
}

#[tokio::test]
async fn native_lb_delivers_vendor_backed_cdn_without_origin_credentials_or_lb_worker() {
    check_native_lb_vendor_cdn("m4s", false).await;
}
#[tokio::test]
async fn native_lb_delivers_vendor_backed_hls_with_guarded_decodable_segments() {
    check_native_lb_vendor_cdn("index.m3u8", false).await;
}

#[tokio::test]
async fn vendor_http_placement_clean_redirect_cannot_be_a_network_path_reference() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let ticket = http_reservation(&f, "owned").await;
    let response = f
        .get(&format!("//owned/m4s?flussonix_ticket={ticket}"))
        .await;
    let status = response.status();
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    f.stop().await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(location.as_deref(), Some("/owned/m4s"));
}

#[tokio::test]
async fn vendor_http_placement_hides_internal_ticket_from_callback_and_preserves_raw_viewer_query()
{
    use axum::extract::Query;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let queries = Arc::new(Mutex::new(Vec::new()));
    let observed = queries.clone();
    let callback = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(get(
                move |Query(query): Query<std::collections::HashMap<String, String>>| {
                    let observed = observed.clone();
                    async move {
                        observed
                            .lock()
                            .unwrap()
                            .push(query.get("qs").cloned().unwrap());
                        StatusCode::OK
                    }
                },
            )),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put("sources", &f.source, json!({"on_play":url}))
        .unwrap();
    let ticket = http_reservation(&f, "owned").await;
    let response = f
        .get(&format!(
            "/owned/m4s?token=viewer&flussonix%5Fticket={ticket}&sig=a%2Bb&sig=again"
        ))
        .await;
    let status = response.status();
    let observed = queries.lock().unwrap().clone();
    callback.abort();
    let _ = callback.await;
    f.stop().await;
    assert_eq!(status, StatusCode::FOUND);
    assert_eq!(observed, ["token=viewer&sig=a%2Bb&sig=again"]);
}
