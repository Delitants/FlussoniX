#![cfg(unix)]

use flussonix::iptv::ha::connection::{HaError, HaOptions, HaPool};
use flussonix::server::{App, Options};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER_ID: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

fn write_config(
    root: &Path,
    name: &str,
    mode: u32,
    cluster: &str,
    ca: &Path,
) -> std::path::PathBuf {
    let path = root.join(name);
    let content = serde_json::json!({
        "cluster_id": cluster,
        "host": "db.example.test",
        "port": 5432,
        "database": "flussonix_iptv",
        "user": "flussonix_iptv",
        "password": "owned-test-secret",
        "ca_pem": ca,
    });
    fs::write(&path, serde_json::to_vec(&content).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    path
}

#[test]
fn ha_credentials_must_be_owner_only_and_have_valid_cluster_id() {
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("ca.pem");
    fs::write(&ca, "not-a-certificate").unwrap();

    let private = write_config(dir.path(), "private.json", 0o600, CLUSTER_ID, &ca);
    assert!(HaOptions::load(&private).is_ok());

    let public = write_config(dir.path(), "public.json", 0o644, CLUSTER_ID, &ca);
    assert!(HaOptions::load(&public).is_err());

    let unreadable = write_config(dir.path(), "unreadable.json", 0o000, CLUSTER_ID, &ca);
    assert!(HaOptions::load(&unreadable).is_err());

    let malformed = write_config(dir.path(), "invalid.json", 0o600, "not-a-uuid", &ca);
    assert!(HaOptions::load(&malformed).is_err());

    let link = dir.path().join("link.json");
    std::os::unix::fs::symlink(&private, &link).unwrap();
    assert!(HaOptions::load(&link).is_err());

    let oversized = dir.path().join("oversized.json");
    fs::write(&oversized, vec![b' '; 16 * 1024 + 1]).unwrap();
    fs::set_permissions(&oversized, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(HaOptions::load(&oversized).is_err());
}

#[test]
fn ha_credentials_reject_relative_or_missing_ca_and_unusable_database_host() {
    let dir = tempfile::tempdir().unwrap();
    let missing_ca = dir.path().join("missing.pem");
    let missing = write_config(dir.path(), "missing.json", 0o600, CLUSTER_ID, &missing_ca);
    assert!(HaOptions::load(&missing).is_err());

    let relative = write_config(
        dir.path(),
        "relative.json",
        0o600,
        CLUSTER_ID,
        Path::new("ca.pem"),
    );
    assert!(HaOptions::load(&relative).is_err());

    let ca = dir.path().join("ca.pem");
    fs::write(&ca, "not-a-certificate").unwrap();
    let no_host = dir.path().join("no-host.json");
    fs::write(
        &no_host,
        serde_json::to_vec(&serde_json::json!({
            "cluster_id":CLUSTER_ID,
            "host":"",
            "port":5432,
            "database":"flussonix_iptv",
            "user":"flussonix_iptv",
            "password":"owned-test-secret",
            "ca_pem":ca,
        }))
        .unwrap(),
    )
    .unwrap();
    fs::set_permissions(&no_host, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(HaOptions::load(&no_host).is_err());
}

#[tokio::test]
async fn ha_database_tls_rejects_trusted_certificate_for_wrong_host() {
    let certs = tls_fixture::Certificates::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.2:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(certs.server());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8];
        socket.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
        socket.write_all(b"S").await.unwrap();
        acceptor.accept(socket).await.is_ok()
    });

    let path = write_config(
        certs.dir.path(),
        "database.json",
        0o600,
        CLUSTER_ID,
        &certs.ca,
    );
    let mut document: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    document["host"] = "127.0.0.2".into();
    document["port"] = port.into();
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let options = HaOptions::load(&path).unwrap();
    let pool = HaPool::new(&options).unwrap();
    assert_eq!(
        pool.verify_startup().await.unwrap_err(),
        HaError::Unavailable
    );
    assert!(
        !tokio::time::timeout(std::time::Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap()
    );
}

#[tokio::test]
async fn ha_database_tls_accepts_matching_host_before_rejecting_incomplete_server() {
    let certs = tls_fixture::Certificates::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio_rustls::TlsAcceptor::from(certs.server());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8];
        socket.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [0, 0, 0, 8, 4, 210, 22, 47]);
        socket.write_all(b"S").await.unwrap();
        let mut secured = acceptor.accept(socket).await.unwrap();
        secured.read_exact(&mut request).await.unwrap();
        request
    });

    let path = write_config(
        certs.dir.path(),
        "database.json",
        0o600,
        CLUSTER_ID,
        &certs.ca,
    );
    let mut document: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    document["host"] = "127.0.0.1".into();
    document["port"] = port.into();
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let pool = HaPool::new(&HaOptions::load(&path).unwrap()).unwrap();
    assert_eq!(
        pool.verify_startup().await.unwrap_err(),
        HaError::Unavailable
    );
    let startup = tokio::time::timeout(std::time::Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    assert!(u32::from_be_bytes(startup[..4].try_into().unwrap()) >= 8);
}

#[test]
fn ha_authority_does_not_load_or_serve_the_standalone_catalog() {
    let certs = tls_fixture::Certificates::new();
    let path = write_config(
        certs.dir.path(),
        "database.json",
        0o600,
        CLUSTER_ID,
        &certs.ca,
    );
    let legacy = certs.dir.path().join("legacy-iptv.json");
    fs::write(&legacy, "invalid legacy data").unwrap();
    let options = Options {
        admin_password: "owned-admin-password".into(),
        peer_key: "owned-peer-key-123".into(),
        role: "lb".into(),
        iptv_lease_authority: true,
        iptv_ha: Some(HaOptions::load(&path).unwrap()),
        iptv_data: Some(legacy),
        ..Options::default()
    };
    let app = App::new(
        certs.dir.path().join("config.json"),
        certs.dir.path().join("media"),
        options,
    )
    .unwrap();
    assert!(app.iptv_ha.is_some());
    assert!(!app.iptv.snapshot().enabled());
}

#[tokio::test]
async fn ha_database_rejects_wrong_cluster_and_replayed_primary_epoch() {
    let certs = tls_fixture::Certificates::new();
    let Some(postgres) = iptv_postgres::PgServer::start(&certs) else {
        eprintln!("native PostgreSQL test binaries unavailable; set FLUSSONIX_TEST_PG_BINDIR");
        return;
    };
    let client = postgres.local_client().await;
    client.batch_execute(
        "CREATE TABLE fx_iptv_meta (cluster_id TEXT NOT NULL, schema_version INTEGER NOT NULL, epoch BIGINT NOT NULL); \
         INSERT INTO fx_iptv_meta VALUES ('19b3774a-3879-461d-a89f-a6de5461abb8', 1, 1);"
    ).await.unwrap();
    let path = write_config(
        certs.dir.path(),
        "real-db.json",
        0o600,
        CLUSTER_ID,
        &certs.ca,
    );
    let mut document: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    document["host"] = "127.0.0.1".into();
    document["port"] = postgres.port().into();
    document["database"] = "postgres".into();
    document["user"] = "ha_test".into();
    document["password"] = "owned-test-secret".into();
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let pool = HaPool::new(&HaOptions::load(&path).unwrap()).unwrap();
    assert_eq!(
        pool.verify_startup().await.unwrap_err(),
        HaError::WrongCluster
    );
    client
        .execute(
            "UPDATE fx_iptv_meta SET cluster_id = $1, epoch = 2",
            &[&CLUSTER_ID],
        )
        .await
        .unwrap();
    pool.verify_startup().await.unwrap();
    client
        .execute("UPDATE fx_iptv_meta SET epoch = 1", &[])
        .await
        .unwrap();
    assert_eq!(
        pool.verify_startup().await.unwrap_err(),
        HaError::StaleEpoch
    );
}
