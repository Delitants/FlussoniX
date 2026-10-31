use axum::{
    body::Body,
    extract::{Query, State},
    http::{Request, StatusCode},
    routing::get,
};
use flussonix::playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest};
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
    b.queries.lock().unwrap().push(q.clone());
    let mode = b.mode.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let status = match mode {
        1 => StatusCode::FORBIDDEN,
        2 => StatusCode::INTERNAL_SERVER_ERROR,
        3 => StatusCode::FOUND,
        5 if ["qs", "user_agent", "referer", "host"]
            .iter()
            .any(|key| q.get(*key).is_some_and(|value| value.contains("denied")))
            || q.get("ip").is_some_and(|ip| ip == "192.0.2.2") =>
        {
            StatusCode::FORBIDDEN
        }
        8 if q.get("qs").is_some_and(|qs| qs.contains("lane=first")) => StatusCode::FORBIDDEN,
        9 if q.get("qs").is_some_and(|qs| qs.contains("lane=redirect")) => StatusCode::FOUND,
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
    if mode == 7 || mode == 12 {
        let user = if mode == 12 || q.get("qs").is_some_and(|qs| qs.contains("profile=two")) {
            "account-two"
        } else {
            "account-one"
        };
        headers.insert("x-userid", user.parse().unwrap());
    }
    if mode == 13 {
        if q.get("qs").is_some_and(|qs| qs.contains("profile=two")) {
            headers.insert("x-userid", "account-two".parse().unwrap());
        } else if q.get("request_number").is_some_and(|number| number == "0") {
            headers.insert("x-userid", "account-one".parse().unwrap());
        } else {
            headers.remove("x-userid");
        }
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
async fn token_free_callback_config_is_rejected_before_persistence() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let (_d, app, _backend, task, _) = setup().await;
    let previous = app.config.effective("owned").unwrap();
    let payload =
        json!({"on_play":{"url":"https://auth.example/check","session_keys":["name","proto"]}});
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/streamer/api/v3/streams/owned")
                .header(
                    "authorization",
                    format!("Basic {}", STANDARD.encode("admin:edit-secret")),
                )
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(app.config.effective("owned").unwrap(), previous);
    task.abort();
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
async fn changed_callback_visible_field_gets_its_own_decision_in_one_session() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(5, Ordering::SeqCst);
    for field in ["qs", "user_agent", "referer", "host", "ip"] {
        let auth = PlaybackAuth::new(100);
        let policy = Policy::from_config(
            &json!({"on_play":{"url":url,"session_keys":["name","proto","token"]}}),
            &json!({}),
        )
        .unwrap();
        let snapshot = auth.publish("direct", Some(policy)).unwrap();
        let mut first = ViewerRequest {
            name: "direct".into(),
            proto: "mpegts".into(),
            ip: "192.0.2.1".into(),
            token: "same-token".into(),
            ..Default::default()
        };
        match field {
            "qs" => first.qs = "plan=allowed".into(),
            "user_agent" => first.user_agent = "allowed-device".into(),
            "referer" => first.referer = "https://allowed.example/".into(),
            "host" => first.host = "allowed.example".into(),
            "ip" => {}
            _ => unreachable!(),
        }
        let before = backend.calls.load(Ordering::SeqCst);
        let grant = match auth.authorize(snapshot.clone(), first.clone()).await {
            AuthOutcome::Allowed(grant) => grant,
            _ => panic!("first {field} request should be allowed"),
        };
        let mut changed = first;
        match field {
            "qs" => changed.qs = "plan=denied".into(),
            "user_agent" => changed.user_agent = "denied-device".into(),
            "referer" => changed.referer = "https://denied.example/".into(),
            "host" => changed.host = "denied.example".into(),
            "ip" => changed.ip = "192.0.2.2".into(),
            _ => unreachable!(),
        }
        assert!(
            matches!(auth.authorize(snapshot, changed).await, AuthOutcome::Denied),
            "changed {field} must be denied by a fresh callback"
        );
        assert!(
            !grant.is_cancelled(),
            "denial of changed {field} must not revoke the first grant"
        );
        let queries = backend.queries.lock().unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst) - before, 2, "{field}");
        assert_eq!(
            queries[before]["session_id"],
            queries[before + 1]["session_id"]
        );
        assert_eq!(queries[before + 1]["request_type"], "update_session");
        assert_eq!(queries[before + 1]["request_number"], "1");
    }
    task.abort();
}
#[tokio::test]
async fn changed_query_denial_does_not_close_an_existing_mpegts_viewer() {
    let (_d, app, backend, task, _) = setup().await;
    backend.mode.store(5, Ordering::SeqCst);
    let live = media(&app, "/owned/mpegts?token=same&plan=allowed").await;
    assert_eq!(live.status(), 200);
    assert_eq!(
        media(&app, "/owned/mpegts?token=same&plan=denied")
            .await
            .status(),
        403
    );
    assert!(
        !closed(live).await,
        "another fingerprint's denial closed the viewer"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    app.media.stop_all().await;
    task.abort();
}
#[tokio::test]
async fn real_http_socket_denies_changed_query_without_closing_the_first_viewer() {
    use futures_util::StreamExt;

    let (_d, app, backend, callback_task, _) = setup().await;
    backend.mode.store(5, Ordering::SeqCst);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let served_app = app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(served_app)).await.unwrap();
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let base = format!("http://{address}/owned/mpegts?token=same&plan=");

    let cold_denied = client
        .get(format!(
            "http://{address}/other/mpegts?token=cold&plan=denied"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(cold_denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        app.media.count().await,
        0,
        "denial must precede worker start"
    );

    let first = client.get(format!("{base}allowed")).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let denied = client.get(format!("{base}denied")).send().await.unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let same = client.get(format!("{base}allowed")).send().await.unwrap();
    assert_eq!(same.status(), StatusCode::OK);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);

    let mut body = first.bytes_stream();
    let ended = tokio::time::timeout(Duration::from_millis(500), async {
        while let Some(chunk) = body.next().await {
            chunk.unwrap();
        }
    })
    .await
    .is_ok();
    assert!(
        !ended,
        "changed-query denial closed the authorized HTTP socket"
    );

    drop(same);
    app.media.stop_all().await;
    server.abort();
    callback_task.abort();
}
#[tokio::test]
async fn changed_callback_policy_discards_old_decisions_before_new_admission() {
    let (_d, _app, backend, task, url) = setup().await;
    let auth = PlaybackAuth::new(10);
    let request = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        ..Default::default()
    };
    let first_policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let first = auth.publish("direct", Some(first_policy)).unwrap();
    let first_grant = match auth.authorize(first.clone(), request.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("initial callback should allow"),
    };
    let other_request = ViewerRequest {
        qs: "lane=other".into(),
        ..request.clone()
    };
    let other_grant = match auth.authorize(first, other_request).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("second lane should allow"),
    };
    let changed_url = format!("{url}?revision=two");
    let second_policy = Policy::from_config(&json!({"on_play":changed_url}), &json!({})).unwrap();
    let second = auth.publish("direct", Some(second_policy)).unwrap();
    assert!(first_grant.is_cancelled());
    assert!(other_grant.is_cancelled());
    let second_grant = match auth.authorize(second, request).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("updated callback should allow"),
    };
    assert!(
        !second_grant.is_cancelled(),
        "a new policy must not inherit a canceled lane"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    task.abort();
}
#[tokio::test]
async fn one_configurable_session_cannot_merge_different_callback_user_ids() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(7, Ordering::SeqCst);
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let first = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "profile=one".into(),
        ..Default::default()
    };
    let first_grant = match auth.authorize(snapshot.clone(), first.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("first account should allow"),
    };
    let second = ViewerRequest {
        qs: "profile=two".into(),
        ..first
    };
    assert!(matches!(
        auth.authorize(snapshot, second).await,
        AuthOutcome::Denied
    ));
    assert!(!first_grant.is_cancelled());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    task.abort();
}
#[tokio::test]
async fn account_conflict_rechecks_after_old_grant_ends() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(7, Ordering::SeqCst);
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let first = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "profile=one".into(),
        ..Default::default()
    };
    let grant = match auth.authorize(snapshot.clone(), first.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("first account should allow"),
    };
    let second = ViewerRequest {
        qs: "profile=two".into(),
        ..first
    };
    assert!(matches!(
        auth.authorize(snapshot.clone(), second.clone()).await,
        AuthOutcome::Denied
    ));
    drop(grant);
    let next = auth.authorize(snapshot, second).await;
    assert!(
        matches!(next, AuthOutcome::Allowed(_)),
        "new account needs a fresh check after old grants end"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    task.abort();
}
#[tokio::test]
async fn cached_lane_cannot_rejoin_a_session_owned_by_another_account() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(7, Ordering::SeqCst);
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let first = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "profile=one".into(),
        ..Default::default()
    };
    let first_grant = match auth.authorize(snapshot.clone(), first.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("first account should allow"),
    };
    drop(first_grant);
    let second = ViewerRequest {
        qs: "profile=two".into(),
        ..first.clone()
    };
    let second_grant = match auth.authorize(snapshot.clone(), second).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("second account should allow after first leaves"),
    };
    assert!(
        matches!(auth.authorize(snapshot, first).await, AuthOutcome::Denied),
        "cached first-account approval joined a live second-account session"
    );
    assert!(!second_grant.is_cancelled());
    task.abort();
}
#[tokio::test]
async fn missing_user_id_on_renewal_keeps_the_lanes_original_account() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(13, Ordering::SeqCst);
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let first = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "profile=one".into(),
        ..Default::default()
    };
    let AuthOutcome::Allowed(first_grant) = auth.authorize(snapshot.clone(), first.clone()).await
    else {
        panic!("first account should allow")
    };
    drop(first_grant);
    let second = ViewerRequest {
        qs: "profile=two".into(),
        ..first.clone()
    };
    let AuthOutcome::Allowed(second_grant) = auth.authorize(snapshot.clone(), second).await else {
        panic!("second account should allow after first leaves")
    };
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        matches!(auth.authorize(snapshot, first).await, AuthOutcome::Denied),
        "missing X-UserId must not silently transfer the old lane to the live second account"
    );
    assert!(!second_grant.is_cancelled());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    task.abort();
}
#[tokio::test]
async fn renewal_account_change_cancels_old_grant_and_rechecks_after_release() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(7, Ordering::SeqCst);
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let request = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "profile=one".into(),
        ..Default::default()
    };
    let grant = match auth.authorize(snapshot.clone(), request.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("first account should allow"),
    };
    backend.mode.store(12, Ordering::SeqCst);
    assert_eq!(auth.force_reauth("direct"), 1);
    auth.renew_due().await;
    assert!(
        grant.is_cancelled(),
        "account-changing renewal must revoke old playback"
    );
    drop(grant);
    assert!(
        matches!(
            auth.authorize(snapshot, request).await,
            AuthOutcome::Allowed(_)
        ),
        "replacement account needs a fresh decision"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    task.abort();
}
#[tokio::test]
async fn callback_outage_retains_only_the_same_fingerprint() {
    let (_d, _app, backend, task, url) = setup().await;
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let request = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "lane=existing".into(),
        ..Default::default()
    };
    let grant = match auth.authorize(snapshot.clone(), request.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("initial callback should allow"),
    };
    backend.mode.store(2, Ordering::SeqCst);
    assert_eq!(auth.force_reauth("direct"), 1);
    auth.renew_due().await;
    assert!(matches!(
        auth.authorize(snapshot.clone(), request.clone()).await,
        AuthOutcome::Allowed(_)
    ));
    let changed = ViewerRequest {
        qs: "lane=new".into(),
        ..request
    };
    assert!(matches!(
        auth.authorize(snapshot, changed).await,
        AuthOutcome::Denied
    ));
    assert!(!grant.is_cancelled());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    task.abort();
}
#[tokio::test]
async fn callback_lane_cache_caps_live_contexts_and_reclaims_an_idle_one() {
    let (_d, _app, backend, task, url) = setup().await;
    let auth = PlaybackAuth::new(100);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let mut grants = Vec::new();
    for index in 0..16 {
        let request = ViewerRequest {
            name: "direct".into(),
            proto: "mpegts".into(),
            ip: "192.0.2.1".into(),
            token: "same".into(),
            qs: format!("context={index}"),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(snapshot.clone(), request).await else {
            panic!("context {index} should be allowed")
        };
        grants.push(grant);
    }
    let overflow = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "context=overflow".into(),
        ..Default::default()
    };
    assert!(
        matches!(
            auth.authorize(snapshot.clone(), overflow.clone()).await,
            AuthOutcome::Denied
        ),
        "all protected lanes must cause fail-closed overflow"
    );
    assert_eq!(
        backend.calls.load(Ordering::SeqCst),
        16,
        "overflow must not call the backend"
    );
    drop(grants.remove(0));
    assert!(
        matches!(
            auth.authorize(snapshot, overflow).await,
            AuthOutcome::Allowed(_)
        ),
        "an idle lane should be reclaimed for a fresh callback"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 17);
    assert!(grants.iter().all(|grant| !grant.is_cancelled()));
    task.abort();
}
#[tokio::test]
async fn renewal_denial_cancels_only_its_original_mpegts_viewer() {
    let (_d, app, backend, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=same&lane=first").await;
    let second = media(&app, "/owned/mpegts?token=same&lane=second").await;
    assert_eq!(first.status(), 200);
    assert_eq!(second.status(), 200);
    backend.mode.store(8, Ordering::SeqCst);
    assert_eq!(app.playback_auth.force_reauth("owned"), 1);
    app.playback_auth.renew_due().await;
    assert!(closed(first).await, "the denied lane stayed live");
    assert!(
        !closed(second).await,
        "another lane was cancelled by the denial"
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 4);
    app.media.stop_all().await;
    task.abort();
}
#[tokio::test]
async fn redirected_lane_does_not_cancel_an_allowed_lane() {
    let (_d, _app, backend, task, url) = setup().await;
    backend.mode.store(9, Ordering::SeqCst);
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let request = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "lane=allowed".into(),
        ..Default::default()
    };
    let grant = match auth.authorize(snapshot.clone(), request.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("allowed lane"),
    };
    let redirected = ViewerRequest {
        qs: "lane=redirect".into(),
        ..request
    };
    assert!(matches!(
        auth.authorize(snapshot, redirected).await,
        AuthOutcome::Redirect(_)
    ));
    assert!(!grant.is_cancelled());
    task.abort();
}
#[tokio::test]
async fn operator_revoke_cancels_all_lanes_in_one_session() {
    let (_d, _app, _backend, task, url) = setup().await;
    let auth = PlaybackAuth::new(10);
    let policy = Policy::from_config(&json!({"on_play":url}), &json!({})).unwrap();
    let snapshot = auth.publish("direct", Some(policy)).unwrap();
    let request = ViewerRequest {
        name: "direct".into(),
        proto: "mpegts".into(),
        ip: "192.0.2.1".into(),
        token: "same".into(),
        qs: "lane=one".into(),
        ..Default::default()
    };
    let first = match auth.authorize(snapshot.clone(), request.clone()).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("first lane"),
    };
    let second = ViewerRequest {
        qs: "lane=two".into(),
        ..request
    };
    let second = match auth.authorize(snapshot, second).await {
        AuthOutcome::Allowed(grant) => grant,
        _ => panic!("second lane"),
    };
    let session = auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    assert!(auth.revoke(&session));
    assert!(first.is_cancelled());
    assert!(second.is_cancelled());
    task.abort();
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
#[tokio::test]
async fn unrelated_metadata_save_during_auth_does_not_deny_viewer() {
    let (_d, app, b, task, _) = setup().await;
    let other = app.clone();
    let pending =
        tokio::spawn(async move { media(&other, "/owned/mpegts?token=metadata-in-flight").await });
    for _ in 0..100 {
        if b.calls.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(b.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        api(
            &app,
            "PUT",
            "streams/other",
            json!({"title":"unrelated edit"}),
            false
        )
        .await
        .status(),
        200
    );
    let response = pending.await.unwrap();
    let status = response.status();
    drop(response);
    app.media.stop_all().await;
    task.abort();
    assert_eq!(
        status, 200,
        "a metadata save on another stream does not invalidate this viewer policy"
    );
}
