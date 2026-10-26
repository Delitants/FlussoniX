use axum::{
    Json, Router,
    body::Bytes,
    extract::ConnectInfo,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    serve::ListenerExt,
};
use flussonix::{
    http_tls,
    iptv::{
        client::{AuthorityClient, Error, LeaseInput},
        ha::{
            catalog::{CatalogEdit, HaCatalog},
            connection::{HaOptions, HaPool},
            schema,
        },
        lease::SessionKey,
    },
    server::{App, Options, router},
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/iptv_legacy_client.rs"]
mod legacy_client;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

fn input() -> LeaseInput {
    LeaseInput {
        token: "owned-viewer-token".into(),
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

async fn serve(certs: &tls_fixture::Certificates, app: Router) -> (url::Url, JoinHandle<()>) {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = tcp.local_addr().unwrap();
    let listener = http_tls::Listener::new(tcp, certs.server()).tap_io(|_| {});
    let task = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    (
        url::Url::parse(&format!("https://{address}/")).unwrap(),
        task,
    )
}

fn marker(value: &'static str) -> [(axum::http::HeaderName, &'static str); 1] {
    [(
        axum::http::HeaderName::from_static("x-flussonix-iptv-cluster-id"),
        value,
    )]
}

#[tokio::test]
async fn ha_client_retries_same_acquire_body_at_secondary_after_primary_503() {
    let certs = tls_fixture::Certificates::new();
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let first_seen = seen.clone();
    let first = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/acquire",
        post(move |body: Bytes| {
            let seen = first_seen.clone();
            async move {
                seen.lock()
                    .unwrap()
                    .push(serde_json::from_slice(&body).unwrap());
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    marker(CLUSTER),
                    Json(json!({"errors":[]})),
                )
                    .into_response()
            }
        }),
    );
    let second_seen = seen.clone();
    let second = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/acquire",
        post(move |headers: HeaderMap, body: Bytes| {
            let seen = second_seen.clone();
            async move {
                assert_eq!(headers["x-flussonix-peer"], "owned-cluster-key-123");
                seen.lock()
                    .unwrap()
                    .push(serde_json::from_slice(&body).unwrap());
                (
                    marker(CLUSTER),
                    Json(json!({
                        "subscriber_id":"a".repeat(64), "lease_id": Uuid::new_v4().to_string(),
                        "revision":3, "units":1, "ttl_ms":1500,
                        "subscriber_expires_at_unix_ms":null
                    })),
                )
                    .into_response()
            }
        }),
    );
    let (primary, primary_task) = serve(&certs, first).await;
    let (secondary, secondary_task) = serve(&certs, second).await;
    let client = AuthorityClient::new_with_secondary(
        primary,
        secondary,
        Uuid::parse_str(CLUSTER).unwrap(),
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    let grant = client.acquire(&input()).await.unwrap();
    assert_eq!(grant.revision, 3);
    let observed = seen.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0], observed[1]);
    primary_task.abort();
    secondary_task.abort();
}

#[tokio::test]
async fn explicit_denial_and_wrong_cluster_marker_never_fail_over() {
    let certs = tls_fixture::Certificates::new();
    let second_count = Arc::new(AtomicUsize::new(0));
    let count = second_count.clone();
    let second = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/acquire",
        post(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                (marker(CLUSTER), Json(json!({})))
            }
        }),
    );
    let (secondary, secondary_task) = serve(&certs, second).await;
    for (status, cluster, expected) in [
        (StatusCode::FORBIDDEN, CLUSTER, Error::Denied),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "a3192376-6df5-4b76-a15c-43942fd3d467",
            Error::Unavailable,
        ),
        (StatusCode::SERVICE_UNAVAILABLE, "", Error::Unavailable),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            CLUSTER,
            Error::Unavailable,
        ),
        (StatusCode::OK, CLUSTER, Error::Unavailable),
    ] {
        let first = Router::new().route(
            "/flussonix/peer/v1/iptv/leases/acquire",
            post(move || async move { (status, marker(cluster), Json(json!({"errors":[]}))) }),
        );
        let (primary, task) = serve(&certs, first).await;
        let client = AuthorityClient::new_with_secondary(
            primary,
            secondary.clone(),
            Uuid::parse_str(CLUSTER).unwrap(),
            "owned-cluster-key-123".into(),
            Some(&certs.ca),
            false,
        )
        .unwrap();
        assert_eq!(client.acquire(&input()).await.unwrap_err(), expected);
        task.abort();
    }
    assert_eq!(second_count.load(Ordering::SeqCst), 0);
    secondary_task.abort();
}

#[tokio::test]
async fn transport_failure_uses_secondary_and_both_down_fail_closed() {
    let certs = tls_fixture::Certificates::new();
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary = url::Url::parse(&format!("https://{}/", unused.local_addr().unwrap())).unwrap();
    drop(unused);
    let second = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/acquire",
        post(|| async {
            (
                marker(CLUSTER),
                Json(json!({
                    "subscriber_id":"a".repeat(64), "lease_id":Uuid::new_v4().to_string(),
                    "revision":2,"units":1,"ttl_ms":1500,"subscriber_expires_at_unix_ms":null
                })),
            )
        }),
    );
    let (secondary, server) = serve(&certs, second).await;
    let cluster = Uuid::parse_str(CLUSTER).unwrap();
    let client = AuthorityClient::new_with_secondary(
        primary.clone(),
        secondary.clone(),
        cluster,
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    assert_eq!(client.acquire(&input()).await.unwrap().revision, 2);
    server.abort();
    let _ = server.await;
    let both_down = AuthorityClient::new_with_secondary(
        primary,
        secondary,
        cluster,
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    assert_eq!(
        both_down.acquire(&input()).await.unwrap_err(),
        Error::Unavailable
    );
}

#[tokio::test]
async fn probes_keep_each_ha_origin_ready_state_separate() {
    let certs = tls_fixture::Certificates::new();
    let first = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/status",
        post(|| async { (marker(CLUSTER), Json(json!({"ready":false}))) }),
    );
    let second = Router::new().route(
        "/flussonix/peer/v1/iptv/leases/status",
        post(|| async { (marker(CLUSTER), Json(json!({"ready":true}))) }),
    );
    let (primary, first_task) = serve(&certs, first).await;
    let (secondary, second_task) = serve(&certs, second).await;
    let client = AuthorityClient::new_with_secondary(
        primary,
        secondary,
        Uuid::parse_str(CLUSTER).unwrap(),
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    assert!(client.probe().await);
    let health = client.origin_health();
    assert_eq!(health.len(), 2);
    assert!(health[0].reachable && health[0].refused && !health[0].ready);
    assert!(health[1].reachable && health[1].ready && !health[1].refused);
    first_task.abort();
    second_task.abort();
}

fn database(certs: &tls_fixture::Certificates, port: u16) -> HaOptions {
    let path = certs.dir.path().join("authority-db.json");
    fs::write(
        &path,
        json!({
            "cluster_id":CLUSTER,"host":"127.0.0.1","port":port,"database":"postgres",
            "user":"ha_test","password":"owned-test-secret","ca_pem":certs.ca
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    HaOptions::load(&path).unwrap()
}

fn authority(certs: &tls_fixture::Certificates, db: HaOptions, name: &str) -> Arc<App> {
    App::new(
        certs.dir.path().join(format!("{name}-config.json")),
        certs.dir.path().join(format!("{name}-media")),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-key-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            iptv_ha: Some(db),
            ..Options::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn pre_ha_single_origin_client_uses_unchanged_lease_wire() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = database(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    catalog
        .edit(
            1,
            CatalogEdit::PutPackage("basic".into(), json!({"channels":["news"]})),
        )
        .await
        .unwrap();
    catalog
        .edit(
            2,
            CatalogEdit::PutSubscriber(
                "alice".into(),
                json!({"token":"owned-viewer-token","packages":["basic"],"max_sessions":1}),
            ),
        )
        .await
        .unwrap();
    let (first_url, first_task) =
        serve(&certs, router(authority(&certs, ha.clone(), "first"))).await;
    let (second_url, second_task) = serve(&certs, router(authority(&certs, ha, "second"))).await;

    let old = legacy_client::AuthorityClient::new(
        first_url,
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    let mut lease = legacy_client::LeaseInput {
        token: "owned-viewer-token".into(),
        channel: "news".into(),
        session: SessionKey {
            boot: Uuid::new_v4(),
            local: Uuid::new_v4(),
        },
        operation: Uuid::new_v4(),
        units: 1,
        lease_id: None,
    };
    let grant = old.acquire(&lease).await.unwrap();
    assert_eq!(
        grant.subscriber.as_hex(),
        flussonix::iptv::lease::SubscriberId::from_name("alice").as_hex()
    );
    assert_eq!(grant.revision, 3);
    lease.lease_id = Some(grant.lease_id);
    assert_eq!(old.commit(&lease).await.unwrap().lease_id, grant.lease_id);
    assert_eq!(old.renew(&lease).await.unwrap().lease_id, grant.lease_id);

    let other = AuthorityClient::new(
        second_url,
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    assert_eq!(other.acquire(&input()).await.unwrap_err(), Error::Limit);
    old.release(&lease, &grant).await.unwrap();
    assert!(other.acquire(&input()).await.is_ok());
    first_task.abort();
    second_task.abort();
}

#[tokio::test]
async fn ambiguous_acquire_commit_renew_and_release_use_one_database_lease() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let ha = database(&certs, db.port());
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    catalog
        .edit(
            1,
            CatalogEdit::PutPackage("basic".into(), json!({"channels":["news"]})),
        )
        .await
        .unwrap();
    catalog
        .edit(
            2,
            CatalogEdit::PutSubscriber(
                "alice".into(),
                json!({"token":"owned-viewer-token","packages":["basic"],"max_sessions":1}),
            ),
        )
        .await
        .unwrap();
    let primary_app = authority(&certs, ha.clone(), "first");
    let secondary_app = authority(&certs, ha, "second");
    let failed = Arc::new(Mutex::new(HashSet::<String>::new()));
    let failed_once = failed.clone();
    let proxy = Router::new().fallback(move |mut request: axum::extract::Request| {
        let app = primary_app.clone();
        let failed = failed_once.clone();
        async move {
            let action = request
                .uri()
                .path()
                .rsplit('/')
                .next()
                .unwrap_or("")
                .to_owned();
            request
                .extensions_mut()
                .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 40111))));
            let response = router(app).oneshot(request).await.unwrap();
            if response.status().is_success()
                && matches!(action.as_str(), "acquire" | "commit" | "renew" | "release")
                && failed.lock().unwrap().insert(action)
            {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    marker(CLUSTER),
                    Json(json!({"errors":[]})),
                )
                    .into_response()
            } else {
                response
            }
        }
    });
    let (primary, primary_task) = serve(&certs, proxy).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let secondary =
        url::Url::parse(&format!("https://{}/", listener.local_addr().unwrap())).unwrap();
    let cancel = CancellationToken::new();
    let server = tokio::spawn(http_tls::serve(
        listener,
        certs.server(),
        secondary_app,
        cancel.clone(),
    ));
    let client = AuthorityClient::new_with_secondary(
        primary,
        secondary.clone(),
        Uuid::parse_str(CLUSTER).unwrap(),
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    let mut lease = input();
    let grant = client.acquire(&lease).await.unwrap();
    lease.lease_id = Some(grant.lease_id);
    let committed = client.commit(&lease).await.unwrap();
    assert_eq!(committed.lease_id, grant.lease_id);
    let renewed = client.renew(&lease).await.unwrap();
    assert_eq!(renewed.lease_id, grant.lease_id);
    client.release(&lease, &renewed).await.unwrap();
    assert_eq!(failed.lock().unwrap().len(), 4);
    let old = AuthorityClient::new(
        secondary,
        "owned-cluster-key-123".into(),
        Some(&certs.ca),
        false,
    )
    .unwrap();
    assert!(old.probe().await);
    let next = old.acquire(&input()).await.unwrap();
    assert_eq!(next.revision, 3);
    primary_task.abort();
    cancel.cancel();
    server.await.unwrap().unwrap();
}
