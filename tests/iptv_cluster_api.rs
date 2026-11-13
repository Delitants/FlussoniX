use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Method, Request, StatusCode},
};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tower::ServiceExt;
use uuid::Uuid;

fn app() -> (tempfile::TempDir, Arc<App>) {
    app_with_authority(true, true)
}

fn app_with_authority(
    authority: bool,
    allow_plain_loopback: bool,
) -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "edit-password".into(),
            peer_key: "owned-iptv-peer-key".into(),
            iptv_lease_authority: authority,
            iptv_peer_allow_insecure_loopback: allow_plain_loopback,
            ..Options::default()
        },
    )
    .unwrap();
    (dir, app)
}

#[tokio::test]
async fn ordinary_node_never_serves_the_private_lease_authority() {
    let (_dir, app) = app_with_authority(false, false);
    let (status, _) = call(app, Some("owned-iptv-peer-key"), Body::from("{}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

async fn call(app: Arc<App>, key: Option<&str>, body: Body) -> (StatusCode, Value) {
    call_at(app, key, body, "127.0.0.1:12345".parse().unwrap()).await
}

async fn call_at(
    app: Arc<App>,
    key: Option<&str>,
    body: Body,
    peer: SocketAddr,
) -> (StatusCode, Value) {
    call_path_at(app, key, body, peer, "acquire").await
}

async fn call_path_at(
    app: Arc<App>,
    key: Option<&str>,
    body: Body,
    peer: SocketAddr,
    action: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/flussonix/peer/v1/iptv/leases/{action}"));
    if let Some(key) = key {
        request = request.header("X-Flussonix-Peer", key);
    }
    let mut request = request.body(body).unwrap();
    request.extensions_mut().insert(ConnectInfo(peer));
    let response = router(app).oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 8192)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn call_action(app: Arc<App>, action: &str, body: Value) -> (StatusCode, Value) {
    call_path_at(
        app,
        Some("owned-iptv-peer-key"),
        Body::from(body.to_string()),
        "127.0.0.1:12345".parse().unwrap(),
        action,
    )
    .await
}

#[tokio::test]
async fn authority_registers_bounded_lease_lifecycle_actions() {
    let (_dir, app) = app();
    for action in ["commit", "renew", "release", "rollback"] {
        let (status, _) = call_action(app.clone(), action, json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "action={action}");
    }
}

#[tokio::test]
async fn lease_commit_renew_release_and_rollback_preserve_exact_capacity() {
    let (_dir, app) = app();
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("sports", json!({"channels":["news"]}))
        .unwrap();
    app.iptv
        .put_subscriber(
            "alice",
            json!({"token":"token-alice","packages":["sports"],"max_sessions":1}),
        )
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    let session = Uuid::new_v4();
    let operation = Uuid::new_v4();
    let mut first = acquire("token-alice", "news", session, operation);
    let (status, pending) = call_action(app.clone(), "acquire", first.clone()).await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    let lease_id = pending["lease_id"].as_str().unwrap();
    let subscriber_id = pending["subscriber_id"].as_str().unwrap().to_owned();
    first["lease_id"] = json!(lease_id);
    let (status, committed) = call_action(app.clone(), "commit", first.clone()).await;
    assert_eq!(status, StatusCode::OK, "{committed}");
    assert_eq!(committed["lease_id"], lease_id);
    assert!(committed["ttl_ms"].as_u64().is_some_and(|ttl| ttl > 5_000));
    let (status, renewed) = call_action(app.clone(), "renew", first.clone()).await;
    assert_eq!(status, StatusCode::OK, "{renewed}");
    assert_eq!(renewed["lease_id"], lease_id);
    let release = json!({
        "subscriber_id":subscriber_id,
        "boot":Uuid::nil().to_string(),
        "session":session.to_string(),
        "lease_id":lease_id,
        "operation":operation.to_string()
    });
    let mut wrong = release.clone();
    wrong["lease_id"] = json!(Uuid::new_v4().to_string());
    assert_eq!(
        call_action(app.clone(), "release", wrong).await.0,
        StatusCode::NOT_FOUND
    );
    let second = acquire("token-alice", "news", Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(
        call_action(app.clone(), "acquire", second).await.0,
        StatusCode::CONFLICT
    );
    app.iptv
        .put_subscriber("alice", json!({"token":"rotated-token"}))
        .unwrap();
    assert_eq!(
        call_action(app.clone(), "renew", first).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call_action(app.clone(), "release", release).await.0,
        StatusCode::NO_CONTENT
    );
    let later = acquire("rotated-token", "news", Uuid::new_v4(), Uuid::new_v4());
    let (status, pending) = call_action(app.clone(), "acquire", later.clone()).await;
    assert_eq!(status, StatusCode::OK, "{pending}");
    let rollback = json!({
        "subscriber_id":subscriber_id,
        "boot":Uuid::nil().to_string(),
        "session":later["session"],
        "lease_id":pending["lease_id"],
        "operation":later["operation"]
    });
    assert_eq!(
        call_action(app.clone(), "rollback", rollback).await.0,
        StatusCode::NO_CONTENT
    );
    let fresh = acquire("rotated-token", "news", Uuid::new_v4(), Uuid::new_v4());
    assert_eq!(call_action(app, "acquire", fresh).await.0, StatusCode::OK);
}

#[tokio::test]
async fn plain_peer_rpc_requires_explicit_loopback_exception_and_actual_loopback_socket() {
    let (_dir, no_exception) = app_with_authority(true, false);
    let (status, _) = call(no_exception, Some("owned-iptv-peer-key"), Body::from("{}")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_dir, authority) = app();
    let (status, _) = call_at(
        authority,
        Some("owned-iptv-peer-key"),
        Body::from("{}"),
        "192.0.2.5:12345".parse().unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

fn acquire(token: &str, channel: &str, session: Uuid, operation: Uuid) -> Value {
    json!({
        "token":token,
        "channel":channel,
        "boot":Uuid::nil().to_string(),
        "session":session.to_string(),
        "operation":operation.to_string(),
        "units":1
    })
}

#[tokio::test]
async fn private_endpoint_requires_peer_key_and_rejects_large_bodies() {
    let (_dir, app) = app();
    let body =
        Body::from(acquire("token-alice", "news", Uuid::new_v4(), Uuid::new_v4()).to_string());
    let (status, _) = call(app.clone(), None, body).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(app.clone(), Some("incorrect-peer-key"), Body::empty()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, result) = call(
        app,
        Some("owned-iptv-peer-key"),
        Body::from("x".repeat(8193)),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{result}");
}

#[tokio::test]
async fn authority_validates_current_subscriber_and_idempotently_reserves_one_slot() {
    let (_dir, app) = app();
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("sports", json!({"channels":["news"]}))
        .unwrap();
    app.iptv
        .put_subscriber(
            "alice",
            json!({"token":"token-alice","packages":["sports"],"max_sessions":1,"flussonix_expires_at":"2099-01-01T00:00:00Z"}),
        )
        .unwrap();
    let session = Uuid::new_v4();
    let operation = Uuid::new_v4();
    let first = acquire("token-alice", "news", session, operation);
    let (status, value) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(first.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{value}"); // restart quarantine
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    let (status, value) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(first.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let lease_id = value["lease_id"].as_str().unwrap().to_owned();
    assert_eq!(value["units"], 1);
    assert_eq!(value["subscriber_expires_at_unix_ms"], 4_070_908_800_000u64);
    assert_eq!(value["subscriber_id"].as_str().unwrap().len(), 64);
    assert!(value["ttl_ms"].as_u64().is_some_and(|ttl| ttl <= 2000));
    let (status, retry) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(first.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["lease_id"], lease_id);
    let second = acquire("token-alice", "news", Uuid::new_v4(), Uuid::new_v4());
    let (status, denied) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(second.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{denied}");
    for (token, channel) in [("wrong-token", "news"), ("token-alice", "foreign")] {
        let payload = acquire(token, channel, Uuid::new_v4(), Uuid::new_v4());
        let (status, denied) = call(
            app.clone(),
            Some("owned-iptv-peer-key"),
            Body::from(payload.to_string()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(!denied.to_string().contains(token));
    }
    app.iptv
        .put_subscriber("alice", json!({"token":"new-token"}))
        .unwrap();
    let (status, denied) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(first.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    let changed = acquire("new-token", "news", Uuid::new_v4(), Uuid::new_v4());
    let (status, denied) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(changed.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{denied}");
    app.iptv.set_enabled(false).unwrap();
    let (status, denied) = call(
        app,
        Some("owned-iptv-peer-key"),
        Body::from(first.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
}

#[tokio::test]
async fn expired_subscriber_and_malformed_reservations_fail_without_echoing_tokens() {
    let (_dir, app) = app();
    app.iptv.set_enabled(true).unwrap();
    app.iptv
        .put_package("sports", json!({"channels":["news"]}))
        .unwrap();
    app.iptv
        .put_subscriber(
            "alice",
            json!({
                "token":"expired-secret-token",
                "packages":["sports"],
                "flussonix_expires_at":"2000-01-01T00:00:00Z"
            }),
        )
        .unwrap();
    let expired = acquire(
        "expired-secret-token",
        "news",
        Uuid::new_v4(),
        Uuid::new_v4(),
    );
    let (status, denied) = call(
        app.clone(),
        Some("owned-iptv-peer-key"),
        Body::from(expired.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    assert!(!denied.to_string().contains("expired-secret-token"));
    let malformed = json!({"token":"expired-secret-token","channel":"news","boot":"not-a-uuid","session":"no","operation":"no","units":0});
    let (status, denied) = call(
        app,
        Some("owned-iptv-peer-key"),
        Body::from(malformed.to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{denied}");
    assert!(!denied.to_string().contains("expired-secret-token"));
}

#[tokio::test]
async fn stalled_peer_cannot_consume_another_peer_address_budget() {
    use bytes::Bytes;
    use futures_util::stream;
    let (_dir, app) = app();
    let entered = Arc::new(AtomicUsize::new(0));
    let (gate, open) = tokio::sync::watch::channel(false);
    let mut workers = Vec::new();
    for _ in 0..8 {
        let app = app.clone();
        let entered = entered.clone();
        let mut open = open.clone();
        workers.push(tokio::spawn(async move {
            let body = Body::from_stream(stream::once(async move {
                entered.fetch_add(1, Ordering::Release);
                while !*open.borrow() {
                    open.changed().await.unwrap();
                }
                Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{}"))
            }));
            call(app, Some("owned-iptv-peer-key"), body).await
        }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while entered.load(Ordering::Acquire) != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (status, response) = call(app.clone(), Some("owned-iptv-peer-key"), Body::from("{}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{response}");
    let (status, response) = call_at(
        app,
        Some("owned-iptv-peer-key"),
        Body::from("{}"),
        "127.0.0.2:12345".parse().unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    gate.send(true).unwrap();
    for worker in workers {
        assert_eq!(worker.await.unwrap().0, StatusCode::BAD_REQUEST);
    }
}
