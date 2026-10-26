#![cfg(unix)]

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::{
    iptv::ha::{
        catalog::{CatalogEdit, HaCatalog},
        connection::{HaOptions, HaPool},
        schema,
    },
    server::{App, Options, router},
};
use std::{fs, net::SocketAddr, os::unix::fs::PermissionsExt, sync::Arc};
use tower::ServiceExt;
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

fn options(certs: &tls_fixture::Certificates, port: u16) -> HaOptions {
    let path = certs.dir.path().join("authority-db.json");
    fs::write(
        &path,
        serde_json::json!({
            "cluster_id":CLUSTER,"host":"127.0.0.1","port":port,"database":"postgres",
            "user":"ha_test","password":"owned-test-secret","ca_pem":certs.ca
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    HaOptions::load(&path).unwrap()
}

fn authority(certs: &tls_fixture::Certificates, ha: HaOptions) -> Arc<App> {
    App::new(
        certs.dir.path().join("authority-config.json"),
        certs.dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            view_user: Some("viewer".into()),
            view_password: Some("owned-view".into()),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            iptv_public_origin: Some(url::Url::parse("http://localhost:18210/").unwrap()),
            iptv_ha: Some(ha),
            ..Options::default()
        },
    )
    .unwrap()
}

async fn management(
    app: Arc<App>,
    method: Method,
    path: &str,
    if_match: Option<&str>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(format!("/streamer/api/v3/{path}"))
        .header("Host", "localhost:18210")
        .header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("admin:owned-admin")),
        );
    if let Some(tag) = if_match {
        request = request.header("If-Match", tag);
    }
    router(app)
        .oneshot(request.body(Body::from("{}")).unwrap())
        .await
        .unwrap()
}

async fn status(app: Arc<App>) -> axum::response::Response {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/flussonix/peer/v1/iptv/leases/status")
        .header("x-flussonix-peer", "owned-cluster-peer-123")
        .body(Body::from("{}"))
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 41777))));
    router(app).oneshot(request).await.unwrap()
}

async fn peer(app: Arc<App>, action: &str, body: serde_json::Value) -> axum::response::Response {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/flussonix/peer/v1/iptv/leases/{action}"))
        .header("x-flussonix-peer", "owned-cluster-peer-123")
        .body(Body::from(body.to_string()))
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 41777))));
    router(app).oneshot(request).await.unwrap()
}

#[tokio::test]
async fn ha_status_and_management_etag_follow_canonical_database() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = options(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    let app = authority(&certs, ha);
    let read = management(app.clone(), Method::GET, "iptv", None).await;
    assert_eq!(read.status(), StatusCode::OK);
    assert_eq!(read.headers()["etag"], "\"1\"");
    let missing = management(app.clone(), Method::PUT, "iptv", None).await;
    assert_eq!(missing.status(), StatusCode::PRECONDITION_REQUIRED);
    let edited = management(app.clone(), Method::PUT, "iptv", Some("\"1\"")).await;
    assert_eq!(edited.status(), StatusCode::OK);
    assert_eq!(edited.headers()["etag"], "\"2\"");
    let response = status(app.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-flussonix-iptv-cluster-id"], CLUSTER);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"ready":true})
    );
    drop(db);
    let body = axum::body::to_bytes(status(app.clone()).await.into_body(), 4096)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({"ready":false})
    );
    let playlist = router(app)
        .oneshot(
            Request::builder()
                .uri("/tv/playlists/alice?token=unused")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(playlist.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(playlist.headers()["cache-control"], "private, no-store");
}

#[tokio::test]
async fn ha_peer_wire_playlists_and_view_role_use_shared_catalog_and_ledger() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = options(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    catalog
        .edit(
            1,
            CatalogEdit::PutPackage("basic".into(), serde_json::json!({"channels":["news"]})),
        )
        .await
        .unwrap();
    catalog.edit(2, CatalogEdit::PutSubscriber("alice".into(),serde_json::json!({"token":"owned-private-token","packages":["basic"],"max_sessions":1}))).await.unwrap();
    let a = authority(&certs, ha.clone());
    let b = authority(&certs, ha);
    let body = serde_json::json!({
        "token":"owned-private-token", "channel":"news", "boot":uuid::Uuid::new_v4().to_string(),
        "session":uuid::Uuid::new_v4().to_string(), "operation":uuid::Uuid::new_v4().to_string(), "units":1
    });
    let acquired = peer(a.clone(), "acquire", body.clone()).await;
    assert_eq!(acquired.status(), StatusCode::OK);
    assert_eq!(acquired.headers()["cache-control"], "no-store");
    let first: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(acquired.into_body(), 4096)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first["revision"], 3);
    assert!(
        first["ttl_ms"]
            .as_u64()
            .is_some_and(|ttl| ttl > 0 && ttl <= 2_000)
    );
    assert_eq!(first["units"], 1);
    assert_eq!(
        first["subscriber_expires_at_unix_ms"],
        serde_json::Value::Null
    );
    assert_eq!(
        peer(b.clone(), "acquire", body.clone()).await.status(),
        StatusCode::OK
    );
    let mut second = body.clone();
    second["session"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    second["operation"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    let denied = peer(b.clone(), "acquire", second).await;
    assert_eq!(denied.status(), StatusCode::CONFLICT);
    assert_eq!(denied.headers()["cache-control"], "no-store");
    let mut with_id = body.clone();
    with_id["lease_id"] = first["lease_id"].clone();
    assert_eq!(
        peer(b.clone(), "commit", with_id.clone()).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        peer(a.clone(), "renew", with_id).await.status(),
        StatusCode::OK
    );
    let release = serde_json::json!({"subscriber_id":first["subscriber_id"],"boot":body["boot"],"session":body["session"],"lease_id":first["lease_id"],"operation":body["operation"]});
    assert_eq!(
        peer(b.clone(), "release", release.clone()).await.status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        peer(a.clone(), "release", release).await.status(),
        StatusCode::NO_CONTENT
    );
    let playlist = router(a.clone())
        .oneshot(
            Request::builder()
                .uri("/tv/playlists/alice?token=owned-private-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(playlist.status(), StatusCode::OK);
    let view = Request::builder()
        .method(Method::GET)
        .uri("/streamer/api/v3/iptv/subscribers/alice")
        .header("Host", "localhost:18210")
        .header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("viewer:owned-view")),
        )
        .body(Body::empty())
        .unwrap();
    let response = router(b).oneshot(view).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("owned-private-token"));
}

#[tokio::test]
async fn catalog_changes_immediately_revoke_old_tokens_and_playlist_access_on_other_authority() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = options(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    catalog
        .edit(
            1,
            CatalogEdit::PutPackage("basic".into(), serde_json::json!({"channels":["news"]})),
        )
        .await
        .unwrap();
    catalog
        .edit(
            2,
            CatalogEdit::PutSubscriber(
                "alice".into(),
                serde_json::json!({"token":"old-token","packages":["basic"],"max_sessions":1}),
            ),
        )
        .await
        .unwrap();
    let a = authority(&certs, ha.clone());
    let b = authority(&certs, ha);
    let mut request = serde_json::json!({
        "token":"old-token", "channel":"news", "boot":uuid::Uuid::new_v4().to_string(),
        "session":uuid::Uuid::new_v4().to_string(), "operation":uuid::Uuid::new_v4().to_string(), "units":1
    });
    assert_eq!(
        peer(a, "acquire", request.clone()).await.status(),
        StatusCode::OK
    );
    catalog
        .edit(
            3,
            CatalogEdit::PutSubscriber(
                "alice".into(),
                serde_json::json!({"token":"new-token","packages":["basic"],"max_sessions":1}),
            ),
        )
        .await
        .unwrap();
    request["session"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    request["operation"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    let denied = peer(b.clone(), "acquire", request.clone()).await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(denied.headers()["cache-control"], "no-store");
    let playlist = router(b.clone())
        .oneshot(
            Request::builder()
                .uri("/tv/playlists/alice?token=old-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(playlist.status(), StatusCode::FORBIDDEN);
    request["token"] = serde_json::json!("new-token");
    assert_eq!(
        peer(b.clone(), "acquire", request.clone()).await.status(),
        StatusCode::CONFLICT
    );
    catalog
        .edit(4, CatalogEdit::DeletePackage("basic".into()))
        .await
        .unwrap();
    request["operation"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    assert_eq!(
        peer(b, "acquire", request).await.status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn ha_management_requires_exact_revision_on_root_and_item_edits() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = options(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let app_a = authority(&certs, ha.clone());
    let app_b = authority(&certs, ha);
    let root = management(app_a.clone(), Method::GET, "iptv", None).await;
    assert_eq!(root.headers()["etag"], "\"0\"");
    assert_eq!(
        management(app_a.clone(), Method::PUT, "iptv", None)
            .await
            .status(),
        StatusCode::PRECONDITION_REQUIRED
    );
    assert_eq!(
        management(app_a.clone(), Method::PUT, "iptv", Some("W/\"0\""))
            .await
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        management(app_a.clone(), Method::PUT, "iptv", Some("\"0\",\"0\""))
            .await
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        management(app_b.clone(), Method::PUT, "iptv", Some("\"0\""))
            .await
            .status(),
        StatusCode::OK
    );
    let changed = management(app_a.clone(), Method::PUT, "iptv", Some("\"0\"")).await;
    assert_eq!(changed.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(changed.headers()["etag"], "\"1\"");
    assert_eq!(
        management(app_a.clone(), Method::DELETE, "iptv", None)
            .await
            .status(),
        StatusCode::PRECONDITION_REQUIRED
    );
    assert_eq!(
        management(app_a.clone(), Method::DELETE, "iptv", Some("\"1\""))
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        management(app_b.clone(), Method::PUT, "iptv/packages/news", None)
            .await
            .status(),
        StatusCode::PRECONDITION_REQUIRED
    );
    assert_eq!(
        management(
            app_b.clone(),
            Method::PUT,
            "iptv/packages/news",
            Some("\"1\"")
        )
        .await
        .status(),
        StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        management(
            app_b.clone(),
            Method::PUT,
            "iptv/packages/news",
            Some("\"2\"")
        )
        .await
        .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn two_ha_admins_racing_the_same_revision_apply_only_one_edit() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = options(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let a = authority(&certs, ha.clone());
    let b = authority(&certs, ha);
    let (left, right) = tokio::join!(
        management(a.clone(), Method::PUT, "iptv", Some("\"0\"")),
        management(b.clone(), Method::PUT, "iptv", Some("\"0\"")),
    );
    let mut statuses = [left.status(), right.status()];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::OK, StatusCode::PRECONDITION_FAILED]);
    let loser = if left.status() == StatusCode::PRECONDITION_FAILED {
        &left
    } else {
        &right
    };
    assert_eq!(loser.headers()["etag"], "\"1\"");
    let current = management(a, Method::GET, "iptv", None).await;
    assert_eq!(current.headers()["etag"], "\"1\"");
    assert_eq!(current.headers()["cache-control"], "no-store");
}
