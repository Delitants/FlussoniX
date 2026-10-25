use axum::{
    Json, Router,
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use flussonix::iptv::{
    client::{AuthorityClient, Error, LeaseInput},
    lease::SessionKey,
};
use flussonix::{
    http_tls,
    server::{App, Options},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
#[path = "support/tls.rs"]
mod tls_fixture;

fn input() -> LeaseInput {
    LeaseInput {
        token: "private-viewer-token".into(),
        channel: "news".into(),
        session: SessionKey {
            boot: Uuid::new_v4(),
            local: Uuid::new_v4(),
        },
        operation: Uuid::new_v4(),
        units: 1,
        lease_id: None,
    }
}

#[tokio::test]
async fn peer_client_sends_key_and_retries_same_acquire_operation() {
    let requests = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let count = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let attempts = count.clone();
    let app =
        Router::new().route(
            "/flussonix/peer/v1/iptv/leases/acquire",
            post(move |headers: HeaderMap, body: Bytes| {
                let seen = seen.clone();
                let attempts = attempts.clone();
                async move {
                    seen.lock().unwrap().push((
                        headers
                            .get("x-flussonix-peer")
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .into(),
                        serde_json::from_slice(&body).unwrap(),
                    ));
                    if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({"error":"busy"})),
                        )
                            .into_response()
                    } else {
                        (StatusCode::OK, Json(json!({
                    "subscriber_id": "a".repeat(64), "lease_id": Uuid::new_v4().to_string(),
                    "revision": 4, "units": 1, "ttl_ms": 1500,
                    "subscriber_expires_at_unix_ms": null
                }))).into_response()
                    }
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = AuthorityClient::new(
        url::Url::parse(&format!("http://{address}/")).unwrap(),
        "private-peer-key-123".into(),
        None,
        true,
    )
    .unwrap();
    let grant = client.acquire(&input()).await.unwrap();
    assert_eq!(grant.revision, 4);
    assert_eq!(grant.units, 1);
    assert!(grant.deadline > std::time::Instant::now());
    let observed = requests.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0], observed[1]);
    assert_eq!(observed[0].0, "private-peer-key-123");
    server.abort();
}

#[tokio::test]
async fn outage_fails_closed_with_no_token_in_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let client = AuthorityClient::new(
        url::Url::parse(&format!("http://{address}/")).unwrap(),
        "private-peer-key-123".into(),
        None,
        true,
    )
    .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), client.acquire(&input()))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error, Error::Unavailable);
    assert!(!format!("{error:?}").contains("private-viewer-token"));
    assert!(!client.probe().await);
    assert!(!client.recently_ready());
}

#[tokio::test]
async fn readiness_probe_clears_prior_success_when_authority_disappears() {
    let app = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/status",
        post(|| async {
            (
                [(axum::http::header::CONNECTION, "close")],
                Json(json!({"ready": true})),
            )
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = AuthorityClient::new(
        url::Url::parse(&format!("http://{address}/")).unwrap(),
        "private-peer-key-123".into(),
        None,
        true,
    )
    .unwrap();
    assert!(client.probe().await);
    assert!(client.recently_ready());
    server.abort();
    let _ = server.await;
    assert!(!client.probe().await);
    assert!(!client.recently_ready());
}

#[tokio::test]
async fn untrusted_authority_certificate_fails_closed_but_explicit_ca_reaches_peer() {
    let dir = tempfile::tempdir().unwrap();
    let cert = tls_fixture::Certificates::new();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "private-peer-key-123".into(),
            iptv_lease_authority: true,
            ..Options::default()
        },
    )
    .unwrap();
    app.iptv.set_enabled(true).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("https://{}/", listener.local_addr().unwrap())).unwrap();
    let cancel = CancellationToken::new();
    let server = tokio::spawn(http_tls::serve(
        listener,
        cert.server(),
        app,
        cancel.clone(),
    ));
    let untrusted =
        AuthorityClient::new(url.clone(), "private-peer-key-123".into(), None, false).unwrap();
    assert!(!untrusted.probe().await);
    assert!(!untrusted.recently_ready());
    assert_eq!(
        untrusted.acquire(&input()).await.unwrap_err(),
        Error::Unavailable
    );
    let trusted =
        AuthorityClient::new(url, "private-peer-key-123".into(), Some(&cert.ca), false).unwrap();
    assert!(!trusted.probe().await);
    assert!(trusted.recently_reachable());
    assert_eq!(trusted.acquire(&input()).await.unwrap_err(), Error::Denied);
    tokio::time::sleep(Duration::from_millis(11_050)).await;
    assert!(trusted.probe().await);
    assert!(trusted.recently_ready());
    assert!(!untrusted.probe().await);
    assert!(!untrusted.recently_ready());
    cancel.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn response_delay_never_extends_the_authoritys_lease_deadline() {
    let app = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/acquire",
        post(|| async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            Json(json!({
                "subscriber_id":"a".repeat(64), "lease_id":Uuid::new_v4().to_string(),
                "revision":1, "units":1, "ttl_ms":250,
                "subscriber_expires_at_unix_ms":null
            }))
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = AuthorityClient::new(
        url::Url::parse(&format!("http://{address}/")).unwrap(),
        "private-peer-key-123".into(),
        None,
        true,
    )
    .unwrap();
    let before = std::time::Instant::now();
    let grant = client.acquire(&input()).await.unwrap();
    assert!(grant.deadline <= before + Duration::from_millis(300));
    assert!(grant.deadline > std::time::Instant::now());
    server.abort();
}
