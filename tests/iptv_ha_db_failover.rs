#![cfg(unix)]

use flussonix::iptv::{
    ha::{
        catalog::{CatalogEdit, HaCatalog},
        connection::{HaError, HaOptions, HaPool},
        lease::HaLedger,
        schema,
    },
    lease::{LeaseRequest, SessionKey, SubscriberId},
};
use std::{fs, os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use uuid::Uuid;
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

fn pool(certs: &tls_fixture::Certificates, port: u16) -> Arc<HaPool> {
    let path = certs.dir.path().join(format!("failover-{port}.json"));
    fs::write(
        &path,
        serde_json::json!({
            "cluster_id": CLUSTER, "host": "127.0.0.1", "port": port,
            "database": "postgres", "user": "ha_test", "password": "owned-test-secret",
            "ca_pem": certs.ca,
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    Arc::new(HaPool::new(&HaOptions::load(&path).unwrap()).unwrap())
}

fn request() -> LeaseRequest {
    LeaseRequest {
        subscriber: SubscriberId::from_name("alice"),
        session: SessionKey {
            boot: Uuid::new_v4(),
            local: Uuid::new_v4(),
        },
        operation: Uuid::new_v4(),
        units: 1,
        limit: Some(1),
        revision: 1,
        subscriber_expires_at: None,
    }
}

#[tokio::test]
async fn acknowledged_catalog_and_lease_survive_fenced_synchronous_promotion() {
    let certs = tls_fixture::Certificates::new();
    let Some(mut primary) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let primary_pool = pool(&certs, primary.port());
    schema::initialize(&primary_pool, None).await.unwrap();
    let catalog = HaCatalog::new(primary_pool.clone(), certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();

    let standby = iptv_postgres::PgReplica::start(&primary, &certs);
    assert!(
        standby.confirmed_replay(&primary).await.is_none(),
        "asynchronous replay is not promotion evidence"
    );
    let primary_sql = primary.local_client().await;
    primary_sql
        .batch_execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (ha_standby)'")
        .await
        .unwrap();
    primary_sql
        .batch_execute("ALTER SYSTEM SET synchronous_commit = 'remote_apply'")
        .await
        .unwrap();
    primary_sql
        .query_one("SELECT pg_reload_conf()", &[])
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let count: i64 = primary_sql
            .query_one(
                "SELECT count(*) FROM pg_stat_replication WHERE application_name = 'ha_standby' AND sync_state = 'sync'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if count == 1 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let rows = primary_sql
                .query(
                    "SELECT application_name, state, sync_state FROM pg_stat_replication",
                    &[],
                )
                .await
                .unwrap();
            let observed: Vec<(String, String, String)> = rows
                .iter()
                .map(|row| (row.get(0), row.get(1), row.get(2)))
                .collect();
            panic!(
                "standby did not become synchronous: {observed:?}; standby log: {}",
                standby.diagnostic_log()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let req = request();
    let grant = HaLedger::new(primary_pool.clone())
        .acquire(&req, None)
        .await
        .unwrap();
    assert_eq!(
        catalog
            .edit(1, CatalogEdit::Enable(false))
            .await
            .unwrap()
            .revision,
        2
    );
    let standby_sql = standby.local_client().await;
    assert!(
        standby_sql
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let standby_row = standby_sql
        .query_one(
            "SELECT (SELECT revision FROM fx_iptv_catalog), (SELECT count(*) FROM fx_iptv_sessions)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(standby_row.get::<_, i64>(0), 2);
    assert_eq!(standby_row.get::<_, i64>(1), 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let replay = loop {
        if let Some(evidence) = standby.confirmed_replay(&primary).await {
            break evidence;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "acknowledged WAL was not confirmed on the standby"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let evidence = primary
        .fence()
        .expect("owned primary must be stopped before promotion");
    assert!(primary_pool.verify_startup().await.is_err());
    assert!(standby.promote(&evidence, &replay).await);
    standby_sql
        .batch_execute("ALTER SYSTEM RESET synchronous_standby_names")
        .await
        .unwrap();
    standby_sql
        .query_one("SELECT pg_reload_conf()", &[])
        .await
        .unwrap();
    assert!(
        !standby_sql
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );

    let promoted_pool = pool(&certs, standby.port());
    promoted_pool.verify_startup().await.unwrap();
    let promoted_catalog = HaCatalog::new(
        promoted_pool.clone(),
        certs.dir.path().join("promoted-export.json"),
    );
    assert_eq!(promoted_catalog.load_current().await.unwrap().revision(), 2);
    assert!(!promoted_catalog.load_current().await.unwrap().enabled());
    let replicated_grant: Uuid = standby_sql
        .query_one("SELECT grant_id FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(replicated_grant, grant.id);
    assert_eq!(
        HaLedger::new(promoted_pool.clone())
            .acquire(&req, None)
            .await
            .err(),
        Some(HaError::StaleRevision)
    );
    standby_sql
        .execute("UPDATE fx_iptv_meta SET epoch = 2", &[])
        .await
        .unwrap();
    promoted_pool.verify_startup().await.unwrap();
    standby_sql
        .execute("UPDATE fx_iptv_meta SET epoch = 1", &[])
        .await
        .unwrap();
    assert_eq!(
        promoted_pool.verify_startup().await,
        Err(HaError::StaleEpoch)
    );
}

#[tokio::test]
async fn lost_primary_without_acknowledged_standby_denies_new_admission() {
    let certs = tls_fixture::Certificates::new();
    let Some(mut primary) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, primary.port());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(
        pool.clone(),
        certs.dir.path().join("lost-primary-export.json"),
    );
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    assert!(primary.fence().is_some());
    assert_eq!(
        HaLedger::new(pool.clone())
            .acquire(&request(), None)
            .await
            .err(),
        Some(HaError::Unavailable)
    );
    assert_eq!(
        catalog.load_current().await.err(),
        Some(HaError::Unavailable)
    );
    assert_eq!(pool.verify_startup().await, Err(HaError::Unavailable));
}

#[tokio::test]
async fn owner_only_backup_restores_supported_schema_and_catalog_after_failed_upgrade() {
    let certs = tls_fixture::Certificates::new();
    let Some(primary) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let db_pool = pool(&certs, primary.port());
    schema::initialize(&db_pool, None).await.unwrap();
    let catalog = HaCatalog::new(db_pool.clone(), certs.dir.path().join("backup-export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    let backup = primary.backup(&certs);
    assert_eq!(
        fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let sql = primary.local_client().await;
    sql.execute("UPDATE fx_iptv_meta SET schema_version = 2", &[])
        .await
        .unwrap();
    assert_eq!(
        db_pool.verify_startup().await,
        Err(HaError::UnsupportedSchema)
    );
    drop(sql);
    primary.restore(&certs, &backup);
    let restored = pool(&certs, primary.port());
    restored.verify_startup().await.unwrap();
    let catalog = HaCatalog::new(restored, certs.dir.path().join("restored-export.json"));
    assert_eq!(catalog.load_current().await.unwrap().revision(), 1);
    assert!(catalog.load_current().await.unwrap().enabled());
}

#[tokio::test]
async fn primary_loss_during_unacknowledged_acquire_does_not_create_a_grant() {
    let certs = tls_fixture::Certificates::new();
    let Some(mut primary) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, primary.port());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool.clone(), certs.dir.path().join("uncertain-export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    let standby = iptv_postgres::PgReplica::start(&primary, &certs);
    let sql = primary.local_client().await;
    sql.batch_execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (ha_standby)'")
        .await
        .unwrap();
    sql.batch_execute("ALTER SYSTEM SET synchronous_commit = 'remote_apply'")
        .await
        .unwrap();
    sql.query_one("SELECT pg_reload_conf()", &[]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if standby.confirmed_replay(&primary).await.is_some() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "synchronous replay did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sql.batch_execute(
        "CREATE FUNCTION fx_test_delay_operation() RETURNS trigger LANGUAGE plpgsql AS $$
           BEGIN PERFORM pg_sleep(5); RETURN NEW; END $$;
         CREATE TRIGGER fx_test_delay_operation BEFORE INSERT ON fx_iptv_operations
           FOR EACH ROW EXECUTE FUNCTION fx_test_delay_operation()",
    )
    .await
    .unwrap();
    let replay = loop {
        if let Some(replay) = standby.confirmed_replay(&primary).await {
            break replay;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "trigger definition was not acknowledged on standby"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let pending = tokio::spawn({
        let ledger = HaLedger::new(pool.clone());
        async move { ledger.acquire(&request(), None).await }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let sleeping: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'flussonix-iptv-ha' AND wait_event = 'PgSleep'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if sleeping > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "acquire did not reach the pre-commit delay"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let fence = primary.fence().expect("owned process group must be fenced");
    assert_eq!(pending.await.unwrap().err(), Some(HaError::Unavailable));
    assert!(standby.promote(&fence, &replay).await);
    let promoted = standby.local_client().await;
    let counts = promoted
        .query_one(
            "SELECT (SELECT count(*) FROM fx_iptv_sessions), (SELECT count(*) FROM fx_iptv_operations)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(counts.get::<_, i64>(0), 0);
    assert_eq!(counts.get::<_, i64>(1), 0);
}
