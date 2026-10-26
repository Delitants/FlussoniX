#![cfg(unix)]

use flussonix::iptv::ha::{
    catalog::{CatalogEdit, HaCatalog},
    connection::{HaError, HaOptions, HaPool},
    schema,
};
use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

fn pool(certs: &tls_fixture::Certificates, port: u16) -> Arc<HaPool> {
    let path = certs.dir.path().join("db.json");
    fs::write(
        &path,
        serde_json::json!({
            "cluster_id": CLUSTER,
            "host": "127.0.0.1",
            "port": port,
            "database": "postgres",
            "user": "ha_test",
            "password": "owned-test-secret",
            "ca_pem": certs.ca,
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    Arc::new(HaPool::new(&HaOptions::load(&path).unwrap()).unwrap())
}

#[tokio::test]
async fn explicit_empty_schema_and_revision_cas_across_two_catalogs() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    assert!(pool.verify_startup().await.is_err());
    schema::initialize(&pool, None).await.unwrap();
    pool.verify_startup().await.unwrap();
    assert_eq!(
        schema::initialize(&pool, None).await,
        Err(HaError::InvalidConfig)
    );

    let first = HaCatalog::new(pool.clone(), certs.dir.path().join("export.json"));
    let second = HaCatalog::new(pool, certs.dir.path().join("export2.json"));
    assert_eq!(first.load_current().await.unwrap().revision(), 0);
    assert!(!first.load_current().await.unwrap().enabled());
    assert_eq!(second.load_current().await.unwrap().revision(), 0);
    assert_eq!(
        first
            .edit(0, CatalogEdit::Enable(true))
            .await
            .unwrap()
            .revision,
        1
    );
    assert_eq!(
        second
            .edit(0, CatalogEdit::Enable(false))
            .await
            .unwrap_err(),
        HaError::PreconditionFailed
    );
    assert!(second.load_current().await.unwrap().enabled());
    assert_eq!(second.load_current().await.unwrap().revision(), 1);
}

#[tokio::test]
async fn invalid_edits_do_not_advance_revision_and_exports_are_owner_only() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let export = certs.dir.path().join("iptv-export.json");
    let catalog = HaCatalog::new(pool, export.clone());
    assert_eq!(
        catalog
            .edit(
                0,
                CatalogEdit::PutPackage(
                    "news".into(),
                    serde_json::json!({"channels":["bad//name"]})
                )
            )
            .await
            .unwrap_err(),
        HaError::Invalid
    );
    assert_eq!(catalog.load_current().await.unwrap().revision(), 0);
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    assert_eq!(
        fs::metadata(&export).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(catalog.export_healthy());
}

#[tokio::test]
async fn regular_startup_does_not_import_a_local_iptv_file() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    let local = certs.dir.path().join("iptv.json");
    fs::write(
        &local,
        r#"{"version":1,"enabled":true,"packages":[],"subscribers":[]}"#,
    )
    .unwrap();
    fs::set_permissions(&local, fs::Permissions::from_mode(0o600)).unwrap();
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, local);
    assert!(!catalog.load_current().await.unwrap().enabled());
}

#[tokio::test]
async fn explicit_import_requires_private_valid_data_and_only_empty_schema() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    let input = certs.dir.path().join("import.json");
    fs::write(
        &input,
        r#"{"version":1,"enabled":true,"packages":[],"subscribers":[]}"#,
    )
    .unwrap();
    fs::set_permissions(&input, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        schema::initialize(&pool, Some(&input)).await,
        Err(HaError::InvalidConfig)
    );
    fs::set_permissions(&input, fs::Permissions::from_mode(0o600)).unwrap();
    schema::initialize(&pool, Some(&input)).await.unwrap();
    let catalog = HaCatalog::new(pool.clone(), certs.dir.path().join("export.json"));
    assert!(catalog.load_current().await.unwrap().enabled());
    assert_eq!(
        schema::initialize(&pool, Some(&input)).await,
        Err(HaError::InvalidConfig)
    );
}

#[tokio::test]
async fn simultaneous_edits_conflict_and_stale_revision_fails_closed() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let first = HaCatalog::new(pool.clone(), certs.dir.path().join("a.json"));
    let second = HaCatalog::new(pool, certs.dir.path().join("b.json"));
    let (a, b) = tokio::join!(
        first.edit(0, CatalogEdit::Enable(true)),
        second.edit(0, CatalogEdit::Enable(false))
    );
    assert_eq!(a.is_ok() as u8 + b.is_ok() as u8, 1);
    assert_eq!(first.load_current().await.unwrap().revision(), 1);
    let client = db.local_client().await;
    client
        .execute("UPDATE fx_iptv_catalog SET revision = 0", &[])
        .await
        .unwrap();
    assert!(matches!(
        first.load_current().await,
        Err(HaError::StaleRevision)
    ));
}

#[tokio::test]
async fn catalog_rejects_wrong_cluster_future_schema_and_oversized_document() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("export.json"));
    let client = db.local_client().await;
    client
        .execute("UPDATE fx_iptv_meta SET schema_version = 2", &[])
        .await
        .unwrap();
    assert!(matches!(
        catalog.load_current().await,
        Err(HaError::UnsupportedSchema)
    ));
    client
        .execute(
            "UPDATE fx_iptv_meta SET schema_version = 1, cluster_id = 'other'",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        catalog.load_current().await,
        Err(HaError::WrongCluster)
    ));
    client
        .execute("UPDATE fx_iptv_meta SET cluster_id = $1", &[&CLUSTER])
        .await
        .unwrap();
    client
        .execute(
            "UPDATE fx_iptv_catalog SET document = repeat('x', 67108865)",
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(
        catalog.load_current().await,
        Err(HaError::UnsupportedSchema)
    ));
}

#[tokio::test]
async fn failed_export_does_not_undo_commit_and_can_be_retried() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let parent = certs.dir.path().join("missing");
    let export = parent.join("export.json");
    let catalog = HaCatalog::new(pool, export.clone());
    assert_eq!(
        catalog
            .edit(0, CatalogEdit::Enable(true))
            .await
            .unwrap()
            .revision,
        1
    );
    assert!(!catalog.export_healthy());
    assert!(catalog.load_current().await.unwrap().enabled());
    fs::create_dir(&parent).unwrap();
    catalog.retry_export().await.unwrap();
    assert!(catalog.export_healthy());
    assert_eq!(
        fs::metadata(export).unwrap().permissions().mode() & 0o777,
        0o600
    );
}
