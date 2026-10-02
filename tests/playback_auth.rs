use axum::{
    body::Body,
    extract::{Query, State},
    http::{Request, StatusCode},
    routing::get,
};
use flussonix::server::{App, Options, router};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;
#[derive(Clone)]
struct Backend {
    calls: Arc<AtomicUsize>,
    queries: Arc<Mutex<Vec<HashMap<String, String>>>>,
    mode: Arc<AtomicUsize>,
}
async fn callback(
    State(b): State<Backend>,
    Query(q): Query<HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    b.calls.fetch_add(1, Ordering::SeqCst);
    b.queries.lock().unwrap().push(q);
    let mode = b.mode.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let status = match mode {
        1 => StatusCode::FORBIDDEN,
        2 => StatusCode::INTERNAL_SERVER_ERROR,
        3 => StatusCode::FOUND,
        _ => StatusCode::OK,
    };
    let mut headers = axum::http::HeaderMap::new();
    for (key, value) in [
        ("X-AuthDuration", "1"),
        ("X-UserId", "account"),
        ("X-Max-Sessions", "1"),
        ("Location", "https://player.example/alternate"),
    ] {
        headers.insert(
            axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    if mode == 4 {
        headers.remove("x-authduration");
    }
    (status, headers)
}
async fn setup() -> (
    tempfile::TempDir,
    Arc<App>,
    Backend,
    tokio::task::JoinHandle<()>,
    String,
) {
    let d = tempfile::tempdir().unwrap();
    let b = Backend {
        calls: Arc::new(AtomicUsize::new(0)),
        queries: Arc::new(Mutex::new(vec![])),
        mode: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", listener.local_addr().unwrap());
    let backend = b.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/auth", get(callback))
                .with_state(backend),
        )
        .await
        .unwrap()
    });
    let app = App::new(
        d.path().join("c.json"),
        d.path().join("media"),
        Options {
            admin_password: "edit-secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("view-secret".into()),
            peer_key: "peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for name in ["owned", "other"] {
        app.config
            .put(
                "streams",
                name,
                json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":url}),
            )
            .unwrap();
    }
    (d, app, b, task, url)
}
async fn media(app: &Arc<App>, path: &str) -> axum::response::Response {
    router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}
#[tokio::test]
async fn callback_has_required_fields_uuid_and_actual_protocol() {
    let (_d, app, b, task, _) = setup().await;
    let response = media(&app, "/owned/mpegts?token=one&customer=demo").await;
    assert_eq!(response.status(), 200);
    let q = b.queries.lock().unwrap().last().unwrap().clone();
    app.media.stop_all().await;
    task.abort();
    assert_eq!(q.get("proto").map(String::as_str), Some("mpegts"));
    assert_eq!(
        q.get("request_type").map(String::as_str),
        Some("new_session")
    );
    assert_eq!(q.get("request_number").map(String::as_str), Some("0"));
    assert!(uuid::Uuid::parse_str(q.get("session_id").expect("session UUID")).is_ok());
    assert_eq!(
        q.get("qs").map(String::as_str),
        Some("token=one&customer=demo")
    );
    for field in ["stream_clients", "total_clients", "duration", "bytes"] {
        assert!(q.contains_key(field), "missing {field}");
    }
}
#[tokio::test]
async fn concurrent_initial_requests_share_one_callback() {
    let (_d, app, b, task, _) = setup().await;
    let responses =
        futures_util::future::join_all((0..8).map(|_| media(&app, "/owned/mpegts?token=shared")))
            .await;
    let calls = b.calls.load(Ordering::SeqCst);
    app.media.stop_all().await;
    task.abort();
    assert!(responses.iter().all(|r| r.status() == 200));
    assert_eq!(
        calls, 1,
        "a first viewer triggers only one backend decision"
    );
}
#[tokio::test]
async fn standard_max_sessions_header_limits_one_user_across_streams() {
    let (_d, app, _b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=one").await;
    let second = media(&app, "/other/mpegts?token=two").await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(first.status(), 200);
    assert_eq!(second.status(), 403);
}
#[tokio::test]
async fn callback_redirect_is_cached_and_never_starts_a_worker() {
    let (_d, app, b, task, _) = setup().await;
    b.mode.store(3, Ordering::SeqCst);
    for _ in 0..2 {
        let r = media(&app, "/owned/index.m3u8?token=redirect").await;
        assert_eq!(r.status(), 302);
        assert_eq!(r.headers()["location"], "https://player.example/alternate");
    }
    assert_eq!(app.media.count().await, 0);
    assert_eq!(b.calls.load(Ordering::SeqCst), 1);
    task.abort();
}
async fn api(
    app: &Arc<App>,
    method: &str,
    path: &str,
    body: serde_json::Value,
    viewer: bool,
) -> axum::response::Response {
    use base64::Engine;
    let cred = if viewer {
        "viewer:view-secret"
    } else {
        "admin:edit-secret"
    };
    router(app.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/streamer/api/v3/{path}"))
                .header(
                    "Authorization",
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode(cred)
                    ),
                )
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}
async fn value(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100000)
            .await
            .unwrap(),
    )
    .unwrap()
}
async fn session_id(app: &Arc<App>) -> String {
    value(api(app, "GET", "sessions", json!(null), true).await).await["sessions"][0]["id"]
        .as_str()
        .unwrap()
        .into()
}
async fn closed(response: axum::response::Response) -> bool {
    use futures_util::StreamExt;
    let mut body = response.into_body().into_data_stream();
    tokio::time::timeout(Duration::from_secs(2), async {
        while body.next().await.is_some() {}
    })
    .await
    .is_ok()
}
#[tokio::test]
async fn delete_session_enforces_edit_role_closes_body_and_caches_revoke() {
    let (_d, app, b, task, _) = setup().await;
    let live = media(&app, "/owned/mpegts?token=private-token&secret=hidden").await;
    let id = session_id(&app).await;
    let snapshot =
        value(api(&app, "GET", &format!("sessions/{id}"), json!(null), true).await).await;
    assert_eq!(snapshot["id"], id);
    assert!(!snapshot.to_string().contains("private-token"));
    assert!(!snapshot.to_string().contains("hidden"));
    assert_eq!(
        api(&app, "DELETE", &format!("sessions/{id}"), json!(null), true)
            .await
            .status(),
        403
    );
    let deletion = api(
        &app,
        "DELETE",
        &format!("sessions/{id}"),
        json!(null),
        false,
    )
    .await
    .status();
    let denied = media(&app, "/owned/mpegts?token=private-token&secret=hidden")
        .await
        .status();
    let ended = closed(live).await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(deletion, 204);
    assert_eq!(denied, 403);
    assert!(ended, "revocation closes a live body");
    assert_eq!(b.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn force_reauth_preserves_uuid_counts_and_revokes_explicit_deny() {
    let (_d, app, b, task, _) = setup().await;
    let live = media(&app, "/owned/mpegts?token=renew").await;
    let id = session_id(&app).await;
    b.mode.store(1, Ordering::SeqCst);
    let response = api(
        &app,
        "POST",
        "sessions/reauth?name=owned",
        json!(null),
        false,
    )
    .await;
    assert_eq!(response.status(), 200);
    assert_eq!(value(response).await["estimated_count"], 1);
    let ended = closed(live).await;
    let queries = b.queries.lock().unwrap().clone();
    app.media.stop_all().await;
    task.abort();
    assert!(ended);
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[1]["session_id"], id);
    assert_eq!(queries[1]["request_type"], "update_session");
    assert_eq!(queries[1]["request_number"], "1");
}
#[tokio::test]
async fn backend_outage_retains_previous_allow_but_new_viewer_fails_closed() {
    let (_d, app, b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=existing").await;
    b.mode.store(2, Ordering::SeqCst);
    assert_eq!(
        api(
            &app,
            "POST",
            "sessions/reauth?name=owned",
            json!(null),
            false
        )
        .await
        .status(),
        200
    );
    let same = media(&app, "/owned/mpegts?token=existing").await;
    let new = media(&app, "/other/mpegts?token=new").await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(same.status(), 200);
    assert_eq!(new.status(), 403);
    drop(first);
}
#[tokio::test]
async fn in_flight_approval_cannot_restore_manual_revoke() {
    let (_d, app, b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=race").await;
    let id = session_id(&app).await;
    let other = app.clone();
    let renewing = tokio::spawn(async move {
        api(
            &other,
            "POST",
            "sessions/reauth?name=owned",
            json!(null),
            false,
        )
        .await
    });
    for _ in 0..100 {
        if b.calls.load(Ordering::SeqCst) > 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let removed = api(
        &app,
        "DELETE",
        &format!("sessions/{id}"),
        json!(null),
        false,
    )
    .await
    .status();
    let _ = renewing.await.unwrap();
    let denied = media(&app, "/owned/mpegts?token=race").await.status();
    let ended = closed(first).await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(removed, 204);
    assert_eq!(denied, 403);
    assert!(ended);
}
#[tokio::test]
async fn metadata_edit_keeps_identity_but_changed_policy_revokes_live_grant() {
    let (_d, app, b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=metadata").await;
    let id = session_id(&app).await;
    assert_eq!(
        api(
            &app,
            "PUT",
            "streams/owned",
            json!({"title":"renamed"}),
            false
        )
        .await
        .status(),
        200
    );
    let same = media(&app, "/owned/mpegts?token=metadata").await;
    assert_eq!(same.status(), 200);
    assert_eq!(session_id(&app).await, id);
    assert_eq!(b.calls.load(Ordering::SeqCst), 1);
    let changed = api(
        &app,
        "PUT",
        "streams/owned",
        json!({"flussonix_token_sha256":"f".repeat(64)}),
        false,
    )
    .await
    .status();
    let denied = media(&app, "/owned/mpegts?token=metadata").await.status();
    let ended = closed(first).await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(changed, 200);
    assert_eq!(denied, 403);
    assert!(ended);
}
#[tokio::test]
async fn reauth_requires_stream_name_and_reports_missing_stream() {
    let (_d, app, _b, task, _) = setup().await;
    assert_eq!(
        api(&app, "POST", "sessions/reauth", json!(null), false)
            .await
            .status(),
        400
    );
    assert_eq!(
        api(
            &app,
            "POST",
            "sessions/reauth?name=missing",
            json!(null),
            false
        )
        .await
        .status(),
        404
    );
    task.abort();
}
#[tokio::test]
async fn default_duration_does_not_recheck_after_one_second() {
    let (_d, app, b, task, _) = setup().await;
    b.mode.store(4, Ordering::SeqCst);
    let first = media(&app, "/owned/mpegts?token=default").await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    app.playback_auth.renew_due().await;
    let calls = b.calls.load(Ordering::SeqCst);
    app.media.stop_all().await;
    task.abort();
    drop(first);
    assert_eq!(calls, 1, "missing duration defaults to 180 seconds");
}
#[tokio::test]
async fn changed_policy_fences_late_backend_approval() {
    let (_d, app, b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=policy-race").await;
    let other = app.clone();
    let renewing = tokio::spawn(async move {
        api(
            &other,
            "POST",
            "sessions/reauth?name=owned",
            json!(null),
            false,
        )
        .await
    });
    for _ in 0..100 {
        if b.calls.load(Ordering::SeqCst) > 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(b.calls.load(Ordering::SeqCst) > 1);
    let changed = api(
        &app,
        "PUT",
        "streams/owned",
        json!({"flussonix_token_sha256":"f".repeat(64)}),
        false,
    )
    .await
    .status();
    let _ = renewing.await.unwrap();
    let denied = media(&app, "/owned/mpegts?token=policy-race")
        .await
        .status();
    let ended = closed(first).await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(changed, 200);
    assert_eq!(denied, 403);
    assert!(ended);
}
#[tokio::test]
async fn running_daemon_renews_open_ts_without_another_viewer_request() {
    use std::process::Stdio;
    let (d, app, b, task, _) = setup().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .args(["--listen", &address.to_string(), "--config"])
        .arg(d.path().join("c.json"))
        .arg("--media-dir")
        .arg(d.path().join("daemon-media"))
        .env("FLUSSONIX_ADMIN_PASSWORD", "daemon-test-admin")
        .env("FLUSSONIX_PEER_KEY", "daemon-peer-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for _ in 0..100 {
        if client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let live = client
        .get(format!("http://{address}/owned/mpegts?token=scheduler"))
        .send()
        .await
        .unwrap();
    b.mode.store(1, Ordering::SeqCst);
    use futures_util::StreamExt;
    let mut body = live.bytes_stream();
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while body.next().await.is_some() {}
    })
    .await
    .is_ok();
    std::process::Command::new("kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(8), child.wait())
        .await
        .unwrap()
        .unwrap();
    let queries = b.queries.lock().unwrap().clone();
    app.media.stop_all().await;
    task.abort();
    assert!(exit.success());
    assert!(ended, "scheduled denial stops an existing TS body");
    assert!(queries.len() >= 2);
    assert_eq!(queries[1]["request_type"], "update_session");
    assert_eq!(queries[1]["session_id"], queries[0]["session_id"]);
}

#[tokio::test]
async fn review_reauth_must_not_shorten_manual_revoke() {
    let (_d, app, b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=review-revoke").await;
    let id = session_id(&app).await;
    assert_eq!(
        api(
            &app,
            "DELETE",
            &format!("sessions/{id}"),
            json!(null),
            false
        )
        .await
        .status(),
        204
    );
    let r = value(
        api(
            &app,
            "POST",
            "sessions/reauth?name=owned",
            json!(null),
            false,
        )
        .await,
    )
    .await;
    let second = media(&app, "/owned/mpegts?token=review-revoke").await;
    let actual = second.status();
    assert_eq!(r["estimated_count"], 0);
    assert_eq!(b.calls.load(Ordering::SeqCst), 1);
    println!(
        "reauth={} callback_count={} second_status={}",
        r,
        b.calls.load(Ordering::SeqCst),
        actual
    );
    drop(first);
    drop(second);
    app.media.stop_all().await;
    task.abort();
    assert_eq!(
        actual, 403,
        "manual denial must remain cached after reauth races body closure"
    );
}
#[tokio::test]
async fn review_removed_source_must_not_be_restored_by_late_discovery() {
    let (_d, app, _b, task, _) = setup().await;
    app.config.delete("streams", "owned").unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let e = entered.clone();
    let r = release.clone();
    let source = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/flussonix/api/v1/stream/owned",
                get(move || {
                    let e = e.clone();
                    let r = r.clone();
                    async move {
                        e.notify_one();
                        r.notified().await;
                        axum::Json(json!({"name":"owned","inputs":[{"url":"testsrc://"}]}))
                    }
                }),
            ),
        )
        .await
        .unwrap()
    });
    app.config
        .put(
            "sources",
            "origin",
            json!({"api_url":url,"private_payload_url":url}),
        )
        .unwrap();
    let a = app.clone();
    let playback = tokio::spawn(async move { media(&a, "/owned/mpegts?token=discovery").await });
    entered.notified().await;
    assert_eq!(
        api(&app, "DELETE", "cluster/sources/origin", json!(null), false)
            .await
            .status(),
        204
    );
    release.notify_one();
    let response = playback.await.unwrap();
    let actual = response.status();
    let second = media(&app, "/owned/mpegts?token=discovery").await.status();
    println!(
        "late_discovery={} subsequent_playback={} active={}",
        actual,
        second,
        app.playback_auth.active()
    );
    drop(response);
    app.media.stop_all().await;
    source.abort();
    task.abort();
    assert_eq!(
        actual, 404,
        "removed source must not install a late discovery result"
    );
    assert_eq!(second, 404);
}

#[tokio::test]
async fn review_discovered_policy_must_revoke_existing_grants() {
    let (_d, app, _b, task, _) = setup().await;
    app.config.delete("streams", "owned").unwrap();
    let policy = Arc::new(Mutex::new(
        json!({"name":"owned","inputs":[{"url":"testsrc://"}]}),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let config = policy.clone();
    let source = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/flussonix/api/v1/stream/owned",
                get(move || {
                    let config = config.clone();
                    async move { axum::Json(config.lock().unwrap().clone()) }
                }),
            ),
        )
        .await
        .unwrap()
    });
    app.config
        .put(
            "sources",
            "origin",
            json!({"api_url":url,"private_payload_url":url}),
        )
        .unwrap();
    let first = media(&app, "/owned/mpegts?token=discovered").await;
    assert_eq!(first.status(), 200);
    let id = session_id(&app).await;
    policy.lock().unwrap()["flussonix_token_sha256"] = json!("f".repeat(64));
    let another = media(&app, "/owned/mpegts?token=another-viewer").await;
    assert_eq!(another.status(), 200);
    tokio::time::sleep(Duration::from_millis(10100)).await;
    app.reconcile().await;
    assert!(
        app.playback_auth
            .snapshots()
            .iter()
            .all(|s| s["is_open"] == false),
        "background source refresh invalidates every existing viewer without a new media request"
    );
    drop(another);
    let second = media(&app, "/owned/mpegts?token=discovered").await;
    assert_eq!(second.status(), 403);
    let snapshot = app.playback_auth.snapshot(&id).unwrap();
    println!("discovered stricter policy rejected new request but existing session={snapshot}");
    drop(first);
    drop(second);
    app.media.stop_all().await;
    source.abort();
    task.abort();
    assert_eq!(
        snapshot["is_open"], false,
        "discovered policy must invalidate every old grant for stream"
    );
}
#[tokio::test]
async fn review_stale_policy_snapshot_must_not_restore_old_approval() {
    use flussonix::playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest};
    let auth = PlaybackAuth::new(10);
    let old = Policy::from_config(&json!({}), &json!({})).unwrap();
    let newer = Policy::from_config(
        &json!({"flussonix_token_sha256":"f".repeat(64)}),
        &json!({}),
    )
    .unwrap();
    let request = ViewerRequest {
        name: "owned".into(),
        proto: "mpegts".into(),
        token: "old".into(),
        ..Default::default()
    };
    let old = auth.publish("owned", Some(old)).unwrap();
    let AuthOutcome::Allowed(first) = auth.authorize(old.clone(), request.clone()).await else {
        panic!("grant")
    };
    auth.invalidate(|_| Some(newer.clone()));
    assert!(first.is_cancelled());
    let restored = matches!(auth.authorize(old, request).await, AuthOutcome::Allowed(_));
    println!("stale policy captured before save restored approval={restored}");
    assert!(
        !restored,
        "an older request snapshot must not replace authoritative newer policy"
    );
}

#[tokio::test]
async fn review_noop_config_save_must_preserve_discovered_session() {
    let (_d, app, _b, task, _) = setup().await;
    app.config.delete("streams", "owned").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let source = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/flussonix/api/v1/stream/owned",
                get(|| async {
                    axum::Json(json!({"name":"owned","inputs":[{"url":"testsrc://"}]}))
                }),
            ),
        )
        .await
        .unwrap()
    });
    app.config
        .put(
            "sources",
            "origin",
            json!({"api_url":url,"private_payload_url":url}),
        )
        .unwrap();
    let first = media(&app, "/owned/mpegts?token=noop").await;
    assert_eq!(first.status(), 200);
    assert_eq!(
        api(&app, "PUT", "config", json!({}), false).await.status(),
        200
    );
    let second = media(&app, "/owned/mpegts?token=noop").await;
    let actual = second.status();
    println!("unchanged config save caused subsequent playback={actual}");
    drop(first);
    drop(second);
    app.media.stop_all().await;
    source.abort();
    task.abort();
    assert_eq!(
        actual, 200,
        "no-op save must preserve discovered session policy and identity"
    );
}

#[tokio::test]
async fn review_concurrent_different_streams_preserve_user_limit() {
    let (_d, app, b, task, _) = setup().await;
    let results = futures_util::future::join_all((0..12).map(|i| {
        let app = &app;
        async move {
            media(
                app,
                &format!(
                    "/{}/mpegts?token={i}",
                    if i % 2 == 0 { "owned" } else { "other" }
                ),
            )
            .await
        }
    }))
    .await;
    let allowed = results.iter().filter(|r| r.status() == 200).count();
    println!(
        "concurrent callbacks={}, allowed={allowed}",
        b.calls.load(Ordering::SeqCst)
    );
    drop(results);
    app.media.stop_all().await;
    task.abort();
    assert_eq!(allowed, 1);
}
#[tokio::test]
async fn review_silent_m4_bodies_close_on_revocation() {
    use futures_util::StreamExt;
    for protocol in ["m4s", "m4f"] {
        let (_d, app, _b, task, _) = setup().await;
        let response = media(&app, &format!("/owned/{protocol}?token=silent")).await;
        assert_eq!(response.status(), 200);
        let id = session_id(&app).await;
        let pid = app.media.stats("owned").await["pid"].as_u64().unwrap();
        std::process::Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .unwrap();
        let mut stream = response.into_body().into_data_stream();
        let mut stopped_early = false;
        loop {
            match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    stopped_early = true;
                    break;
                }
                Err(_) => break,
            }
        }
        assert_eq!(
            api(
                &app,
                "DELETE",
                &format!("sessions/{id}"),
                json!(null),
                false
            )
            .await
            .status(),
            204
        );
        let closes = matches!(
            tokio::time::timeout(Duration::from_millis(500), stream.next()).await,
            Ok(None)
        );
        std::process::Command::new("kill")
            .args(["-CONT", &pid.to_string()])
            .status()
            .unwrap();
        drop(stream);
        app.media.stop_all().await;
        task.abort();
        println!("{protocol}: stopped_early={stopped_early}, closes_after_revoke={closes}");
        assert!(!stopped_early);
        assert!(closes);
    }
}

#[tokio::test]
async fn stale_policy_is_rejected_before_any_session_entry_exists() {
    use flussonix::playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest};
    let auth = PlaybackAuth::new(10);
    let old = auth
        .publish(
            "owned",
            Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
        )
        .unwrap();
    let newer = Policy::from_config(
        &json!({"flussonix_token_sha256":"f".repeat(64)}),
        &json!({}),
    )
    .unwrap();
    auth.invalidate(|_| Some(newer.clone()));
    let request = ViewerRequest {
        name: "owned".into(),
        proto: "mpegts".into(),
        token: "stale".into(),
        ..Default::default()
    };
    assert!(matches!(
        auth.authorize(old, request).await,
        AuthOutcome::Denied
    ));
    assert_eq!(auth.active(), 0);
}
