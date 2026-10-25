use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flussonix::rtsp;
use flussonix::{
    playback_auth::{AuthOutcome, Policy, ViewerRequest},
    server::{App, Options, router},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn node(dir: &tempfile::TempDir, name: &str, authority: Option<url::Url>) -> Arc<App> {
    App::new(
        dir.path().join(format!("{name}-config.json")),
        dir.path().join(format!("{name}-media")),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "cdn".into(),
            node_name: name.into(),
            iptv_authority: authority,
            iptv_authority_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn two_cdn_nodes_cannot_exceed_one_authority_subscriber_session() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn_a = node(&dir, "cdn-a", Some(url.clone()));
    let cdn_b = node(&dir, "cdn-b", Some(url));
    let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
    let first = cdn_a
        .playback_auth
        .publish("news", Some(policy.clone()))
        .unwrap();
    let second = cdn_b.playback_auth.publish("news", Some(policy)).unwrap();
    let request = ViewerRequest {
        name: "news".into(),
        proto: "hls".into(),
        token: "private-viewer-token".into(),
        ip: "192.0.2.8".into(),
        ..Default::default()
    };
    // The authority intentionally quarantines its fresh in-memory ledger
    // longer than the maximum lease from an earlier process instance.
    tokio::time::sleep(Duration::from_secs(11)).await;
    let (left, right) = tokio::join!(
        cdn_a
            .playback_auth
            .authorize(first.clone(), request.clone()),
        cdn_b
            .playback_auth
            .authorize(second.clone(), request.clone()),
    );
    let (winning_auth, winning_policy, grant) = match (left, right) {
        (AuthOutcome::Allowed(grant), AuthOutcome::Denied) => (&cdn_a.playback_auth, first, grant),
        (AuthOutcome::Denied, AuthOutcome::Allowed(grant)) => (&cdn_b.playback_auth, second, grant),
        _ => panic!("authority did not admit exactly one viewer"),
    };
    assert_eq!(authority.iptv_leases.active_leases(), 1);
    let same_hls_session = winning_auth.authorize(winning_policy, request).await;
    assert!(
        matches!(same_hls_session, AuthOutcome::Allowed(_)),
        "matching HLS segments share one lease unit"
    );
    assert_eq!(authority.iptv_leases.active_leases(), 1);
    drop((grant, same_hls_session));
    server.abort();
}

#[tokio::test]
async fn two_continuous_outputs_use_two_units_and_outage_cancels_only_iptv() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":2}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = node(&dir, "cdn", Some(url));
    let iptv = cdn
        .playback_auth
        .publish(
            "news",
            Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
        )
        .unwrap();
    let public = cdn
        .playback_auth
        .publish(
            "public",
            Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
        )
        .unwrap();
    let viewer = ViewerRequest {
        name: "news".into(),
        proto: "mpegts".into(),
        token: "private-viewer-token".into(),
        ..Default::default()
    };
    tokio::time::sleep(Duration::from_secs(11)).await;
    let AuthOutcome::Allowed(first) = cdn
        .playback_auth
        .authorize(iptv.clone(), viewer.clone())
        .await
    else {
        panic!("first continuous grant");
    };
    let AuthOutcome::Allowed(second) = cdn
        .playback_auth
        .authorize(iptv.clone(), viewer.clone())
        .await
    else {
        panic!("second continuous grant");
    };
    assert_eq!(
        authority.iptv_leases.active_leases(),
        1,
        "one local session has two units"
    );
    assert!(matches!(
        cdn.playback_auth.authorize(iptv, viewer).await,
        AuthOutcome::Denied
    ));
    let AuthOutcome::Allowed(public_grant) = cdn
        .playback_auth
        .authorize(
            public,
            ViewerRequest {
                name: "public".into(),
                proto: "mpegts".into(),
                ..Default::default()
            },
        )
        .await
    else {
        panic!("unrelated public grant");
    };
    server.abort();
    tokio::time::timeout(Duration::from_secs(9), async {
        while !first.is_cancelled() || !second.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("IPTV grants exceeded authority deadline after outage");
    assert!(
        !public_grant.is_cancelled(),
        "unrelated playback must continue"
    );
}

#[tokio::test]
async fn a_new_admission_waits_for_the_previous_session_release() {
    use axum::{
        extract::Request,
        middleware::{self, Next},
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let release_entered = Arc::new(tokio::sync::Notify::new());
    let release_continue = Arc::new(tokio::sync::Notify::new());
    let hold_first = Arc::new(AtomicBool::new(true));
    let gate = {
        let entered = release_entered.clone();
        let proceed = release_continue.clone();
        let first = hold_first.clone();
        middleware::from_fn(move |request: Request, next: Next| {
            let entered = entered.clone();
            let proceed = proceed.clone();
            let first = first.clone();
            async move {
                if request.uri().path().ends_with("/leases/release")
                    && first.swap(false, Ordering::SeqCst)
                {
                    entered.notify_one();
                    proceed.notified().await;
                }
                next.run(request).await
            }
        })
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone())
                .layer(gate)
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = node(&dir, "cdn", Some(url));
    let policy = cdn
        .playback_auth
        .publish(
            "news",
            Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
        )
        .unwrap();
    let viewer = ViewerRequest {
        name: "news".into(),
        proto: "hls".into(),
        token: "private-viewer-token".into(),
        ..Default::default()
    };
    tokio::time::sleep(Duration::from_secs(11)).await;
    let AuthOutcome::Allowed(old) = cdn
        .playback_auth
        .authorize(policy.clone(), viewer.clone())
        .await
    else {
        panic!("old grant");
    };
    cdn.playback_auth.invalidate_iptv();
    tokio::time::timeout(Duration::from_secs(7), release_entered.notified())
        .await
        .expect("old lease release did not start");
    let auth = cdn.playback_auth.clone();
    let pending = tokio::spawn(async move { auth.authorize(policy, viewer).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !pending.is_finished(),
        "replacement raced ahead of the old release"
    );
    release_continue.notify_one();
    assert!(matches!(pending.await.unwrap(), AuthOutcome::Allowed(_)));
    assert!(old.is_cancelled());
    assert_eq!(authority.iptv_leases.active_leases(), 1);
    server.abort();
}

#[tokio::test]
async fn local_occupancy_change_during_reservation_rolls_back_extra_units() {
    use axum::{
        extract::Request,
        middleware::{self, Next},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":2}),
        )
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let proceed = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let gate = {
        let entered = entered.clone();
        let proceed = proceed.clone();
        let count = count.clone();
        middleware::from_fn(move |request: Request, next: Next| {
            let entered = entered.clone();
            let proceed = proceed.clone();
            let count = count.clone();
            async move {
                if request.uri().path().ends_with("/leases/acquire")
                    && count.fetch_add(1, Ordering::SeqCst) == 1
                {
                    entered.notify_one();
                    proceed.notified().await;
                }
                next.run(request).await
            }
        })
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone())
                .layer(gate)
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = node(&dir, "cdn", Some(url.clone()));
    let other = node(&dir, "other", Some(url));
    let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
    let p = cdn
        .playback_auth
        .publish("news", Some(policy.clone()))
        .unwrap();
    let q = other.playback_auth.publish("news", Some(policy)).unwrap();
    let viewer = ViewerRequest {
        name: "news".into(),
        proto: "mpegts".into(),
        token: "private-viewer-token".into(),
        ..Default::default()
    };
    tokio::time::sleep(Duration::from_secs(11)).await;
    let AuthOutcome::Allowed(first) = cdn.playback_auth.authorize(p.clone(), viewer.clone()).await
    else {
        panic!("first grant");
    };
    let auth = cdn.playback_auth.clone();
    let request = viewer.clone();
    let second = tokio::spawn(async move { auth.authorize(p, request).await });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("replacement reservation did not start");
    drop(first);
    proceed.notify_one();
    assert!(
        matches!(second.await.unwrap(), AuthOutcome::Denied),
        "final admission must recheck exact units"
    );
    assert!(
        matches!(
            other.playback_auth.authorize(q, viewer).await,
            AuthOutcome::Allowed(_)
        ),
        "rolled-back extra unit must be available to another CDN"
    );
    server.abort();
}

#[tokio::test]
async fn authority_outage_closes_live_http_and_rtsp_but_preserves_public_http() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":3}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let authority_task = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = node(&dir, "cdn", Some(url));
    cdn.config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    cdn.config
        .put("streams", "free", json!({"inputs":[{"url":"testsrc://"}]}))
        .unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    let protected = router(cdn.clone())
        .oneshot(
            Request::builder()
                .uri("/news/mpegts?token=private-viewer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let public = router(cdn.clone())
        .oneshot(
            Request::builder()
                .uri("/free/mpegts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(protected.status(), StatusCode::OK);
    assert_eq!(public.status(), StatusCode::OK);
    let rtsp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rtsp_address = rtsp_listener.local_addr().unwrap();
    let rtsp_stop = CancellationToken::new();
    let rtsp_task = tokio::spawn(rtsp::serve(rtsp_listener, cdn.clone(), rtsp_stop.clone()));
    let mut socket = TcpStream::connect(rtsp_address).await.unwrap();
    socket
        .write_all(
            format!("DESCRIBE rtsp://{rtsp_address}/news?token=private-viewer-token RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut headers = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(socket.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    let headers = String::from_utf8(headers).unwrap();
    assert!(headers.starts_with("RTSP/1.0 200"), "{headers}");
    let length: usize = headers
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: "))
        .unwrap()
        .parse()
        .unwrap();
    let mut sdp = vec![0; length];
    socket.read_exact(&mut sdp).await.unwrap();
    authority_task.abort();
    let mut protected_body = protected.into_body().into_data_stream();
    let protected_closed = tokio::time::timeout(Duration::from_secs(9), async {
        while protected_body.next().await.is_some() {}
    });
    let rtsp_closed = tokio::time::timeout(Duration::from_secs(9), async {
        let mut byte = [0u8];
        socket.read(&mut byte).await.unwrap()
    });
    let (protected_closed, rtsp_closed) = tokio::join!(protected_closed, rtsp_closed);
    assert!(
        protected_closed.is_ok(),
        "IPTV HTTP body stayed open after authority loss"
    );
    assert_eq!(rtsp_closed.unwrap(), 0, "IPTV RTSP socket stayed open");
    let mut public_body = public.into_body().into_data_stream();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), public_body.next())
            .await
            .unwrap()
            .is_some(),
        "unrelated public HTTP playback stopped"
    );
    rtsp_stop.cancel();
    rtsp_task.await.unwrap().unwrap();
    drop(public_body);
    cdn.media.stop_all().await;
}

#[tokio::test]
async fn failed_commit_rolls_back_reservation_without_leaking_a_subscriber_slot() {
    use axum::{
        extract::Request,
        middleware::{self, Next},
        response::Response,
    };
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let fail_commit = middleware::from_fn(|request: Request, next: Next| async move {
        if request.uri().path().ends_with("/leases/commit") {
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Body::empty())
                .unwrap()
        } else {
            next.run(request).await
        }
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone())
                .layer(fail_commit)
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = App::new(
        dir.path().join("cdn-config.json"),
        dir.path().join("cdn-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "cdn".into(),
            client_limit: 1,
            iptv_authority: Some(url),
            iptv_authority_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    let policy = cdn
        .playback_auth
        .publish(
            "news",
            Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert!(matches!(
        cdn.playback_auth
            .authorize(
                policy,
                ViewerRequest {
                    name: "news".into(),
                    proto: "hls".into(),
                    token: "private-viewer-token".into(),
                    ..Default::default()
                },
            )
            .await,
        AuthOutcome::Denied
    ));
    assert_eq!(authority.iptv_leases.active_leases(), 0);
    let public = cdn
        .playback_auth
        .publish(
            "free",
            Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
        )
        .unwrap();
    assert!(
        matches!(
            cdn.playback_auth
                .authorize(
                    public,
                    ViewerRequest {
                        name: "free".into(),
                        proto: "mpegts".into(),
                        ..Default::default()
                    },
                )
                .await,
            AuthOutcome::Allowed(_)
        ),
        "failed commit must not reserve local viewer capacity"
    );
    server.abort();
}

#[tokio::test]
async fn a_late_renewal_response_cannot_revive_an_expired_viewer_grant() {
    use axum::{
        body::to_bytes,
        extract::Request,
        http::header::CONTENT_LENGTH,
        middleware::{self, Next},
        response::Response,
    };
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let renewal_started = Arc::new(tokio::sync::Notify::new());
    let renewal_returning = Arc::new(tokio::sync::Notify::new());
    let gate = {
        let started = renewal_started.clone();
        let returning = renewal_returning.clone();
        middleware::from_fn(move |request: Request, next: Next| {
            let started = started.clone();
            let returning = returning.clone();
            async move {
                let path = request.uri().path().to_owned();
                let response = next.run(request).await;
                if path.ends_with("/leases/commit") && response.status() == StatusCode::OK {
                    let (mut parts, body) = response.into_parts();
                    let body = to_bytes(body, 8192).await.unwrap();
                    let mut grant: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    // Only the test wire reply has a short deadline. The
                    // authority itself still commits a normal ten-second lease.
                    grant["ttl_ms"] = json!(750);
                    parts.headers.remove(CONTENT_LENGTH);
                    Response::from_parts(parts, Body::from(grant.to_string()))
                } else if path.ends_with("/leases/renew") {
                    started.notify_one();
                    tokio::time::sleep(Duration::from_millis(1300)).await;
                    returning.notify_one();
                    response
                } else {
                    response
                }
            }
        })
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone())
                .layer(gate)
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = node(&dir, "cdn", Some(url));
    let policy = cdn
        .playback_auth
        .publish(
            "news",
            Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    let AuthOutcome::Allowed(grant) = cdn
        .playback_auth
        .authorize(
            policy,
            ViewerRequest {
                name: "news".into(),
                proto: "mpegts".into(),
                token: "private-viewer-token".into(),
                ..Default::default()
            },
        )
        .await
    else {
        panic!("initial short viewer lease");
    };
    tokio::time::timeout(Duration::from_secs(2), renewal_started.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(grant.is_cancelled(), "old local deadline was not enforced");
    tokio::time::timeout(Duration::from_secs(2), renewal_returning.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        grant.is_cancelled(),
        "late renewal response revived media after an expired lease"
    );
    server.abort();
}
