//! A separate, owned PostgreSQL-backed authority for the opt-in browser gate.
#![cfg(unix)]
use flussonix::{
    iptv::ha::{
        connection::{HaOptions, HaPool},
        schema,
    },
    server::{App, Options, router},
};
use serde_json::json;
use std::{
    fs,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::net::TcpListener;
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

#[tokio::test]
#[ignore = "started explicitly by the CI browser gate with ready and stop files"]
async fn launch_owned_ha_ui_server() {
    let ready =
        PathBuf::from(std::env::var("FLUSSONIX_HA_UI_READY_FILE").expect("ready file required"));
    let stop =
        PathBuf::from(std::env::var("FLUSSONIX_HA_UI_STOP_FILE").expect("stop file required"));
    let admin_password =
        std::env::var("FLUSSONIX_ADMIN_PASSWORD").expect("test admin password required");
    let view_password =
        std::env::var("FLUSSONIX_VIEW_PASSWORD").expect("test view password required");
    let certs = tls_fixture::Certificates::new();
    let database = iptv_postgres::PgServer::start(&certs)
        .expect("native PostgreSQL is required for the HA browser gate");
    let path = certs.dir.path().join("authority-db.json");
    fs::write(
        &path,
        json!({
            "cluster_id":CLUSTER,"host":"127.0.0.1","port":database.port(),"database":"postgres",
            "user":"ha_test","password":"owned-test-secret","ca_pem":certs.ca
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let ha = HaOptions::load(&path).unwrap();
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    pool.verify_startup().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let origin = url::Url::parse(&format!("http://{address}/")).unwrap();
    let app = App::new(
        certs.dir.path().join("authority-config.json"),
        certs.dir.path().join("media"),
        Options {
            admin_password,
            view_user: Some("viewer".into()),
            view_password: Some(view_password),
            peer_key: "owned-ha-ui-peer-key-123".into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            iptv_public_origin: Some(origin.clone()),
            iptv_ha: Some(ha),
            web_dir: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("web/dist"),
            ..Options::default()
        },
    )
    .unwrap();
    app.iptv_ha
        .as_ref()
        .unwrap()
        .catalog
        .load_current()
        .await
        .unwrap();
    assert!(app.iptv_ha.as_ref().unwrap().ledger.ready().await.unwrap());
    app.iptv_ha
        .as_ref()
        .unwrap()
        .catalog
        .retry_export()
        .await
        .unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(app).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    fs::write(&ready, origin.as_str()).unwrap();
    fs::set_permissions(&ready, fs::Permissions::from_mode(0o600)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(300);
    while !stop.exists() && Instant::now() < deadline {
        assert!(
            !server.is_finished(),
            "owned IPTV HA UI server exited early"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server.abort();
    let _ = server.await;
    let _ = fs::remove_file(ready);
    let _ = fs::remove_file(stop);
    assert!(
        Instant::now() < deadline,
        "owned IPTV HA browser fixture timed out"
    );
}
