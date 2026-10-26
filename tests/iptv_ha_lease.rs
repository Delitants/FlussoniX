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
use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};
use uuid::Uuid;
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";

fn pool(certs: &tls_fixture::Certificates, port: u16) -> Arc<HaPool> {
    let path = certs.dir.path().join("lease-db.json");
    fs::write(&path, serde_json::json!({
        "cluster_id": CLUSTER, "host":"127.0.0.1", "port":port,
        "database":"postgres", "user":"ha_test", "password":"owned-test-secret", "ca_pem":certs.ca
    }).to_string()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    Arc::new(HaPool::new(&HaOptions::load(&path).unwrap()).unwrap())
}

fn request(name: &str, local: Uuid, operation: Uuid, limit: u32, revision: u64) -> LeaseRequest {
    LeaseRequest {
        subscriber: SubscriberId::from_name(name),
        session: SessionKey {
            boot: Uuid::nil(),
            local,
        },
        operation,
        units: 1,
        limit: Some(limit),
        revision,
        subscriber_expires_at: None,
    }
}

#[tokio::test]
async fn two_authorities_racing_for_last_slot_issue_only_one_grant() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool.clone(), certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    let first = HaLedger::new(pool.clone());
    let second = HaLedger::new(pool);
    let a = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let b = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    let (left, right) = tokio::join!(first.acquire(&a, None), second.acquire(&b, None));
    assert_eq!(left.is_ok() as u8 + right.is_ok() as u8, 1);
    assert_eq!(left.err().or_else(|| right.err()), Some(HaError::Limit));
    let client = db.local_client().await;
    let active: i64 = client
        .query_one("SELECT count(*) FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(active, 1);
}

#[tokio::test]
async fn ambiguous_acquire_retry_and_release_tombstone_cannot_create_extra_slot() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let grant = ledger.acquire(&req, None).await.unwrap();
    let retry = ledger.acquire(&req, None).await.unwrap();
    assert_eq!(grant.id, retry.id);
    assert!(
        ledger
            .release(req.subscriber, req.session, grant.id, Uuid::new_v4())
            .await
            .unwrap()
    );
    assert!(matches!(
        ledger.acquire(&req, None).await,
        Err(HaError::Missing)
    ));
    let client = db.local_client().await;
    let active: i64 = client
        .query_one("SELECT count(*) FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(active, 0);
    let counts = client.query_one("SELECT (SELECT count(*) FROM fx_iptv_operations), operation_count FROM fx_iptv_capacity", &[]).await.unwrap();
    assert_eq!(counts.get::<_, i64>(0), i64::from(counts.get::<_, i32>(1)));
}

#[tokio::test]
async fn catalog_revision_change_blocks_old_admission() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool.clone(), certs.dir.path().join("export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    let ledger = HaLedger::new(pool);
    let old = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 1);
    catalog.edit(1, CatalogEdit::Enable(false)).await.unwrap();
    assert!(matches!(
        ledger.acquire(&old, None).await,
        Err(HaError::StaleRevision)
    ));
}

#[tokio::test]
async fn commit_renew_and_same_operation_release_are_retry_safe() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let pending = ledger.acquire(&req, None).await.unwrap();
    let committed = ledger.commit(&req, Some(pending.id)).await.unwrap();
    assert_eq!(committed.id, pending.id);
    assert!(committed.deadline > pending.deadline);
    let client = db.local_client().await;
    let before: chrono::DateTime<chrono::Utc> = client
        .query_one("SELECT expires_at FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    let commit_retry = ledger.commit(&req, Some(pending.id)).await.unwrap();
    assert_eq!(commit_retry.id, pending.id);
    let after: chrono::DateTime<chrono::Utc> = client
        .query_one("SELECT expires_at FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    let renewed = ledger.renew(&req, Some(pending.id)).await.unwrap();
    assert_eq!(renewed.id, pending.id);
    assert!(
        ledger
            .release(req.subscriber, req.session, pending.id, req.operation)
            .await
            .unwrap()
    );
    assert!(
        ledger
            .release(req.subscriber, req.session, pending.id, req.operation)
            .await
            .unwrap()
    );
    assert!(matches!(
        ledger.acquire(&req, None).await,
        Err(HaError::Missing)
    ));
}

#[tokio::test]
async fn pending_replacement_reserves_larger_units_then_rollback_restores_prior_grant() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let base = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 0);
    let first = ledger.acquire(&base, None).await.unwrap();
    ledger.commit(&base, Some(first.id)).await.unwrap();
    let replacement = LeaseRequest {
        operation: Uuid::new_v4(),
        units: 2,
        ..base.clone()
    };
    let pending = ledger.acquire(&replacement, None).await.unwrap();
    assert_ne!(pending.id, first.id);
    let second = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 0);
    assert!(matches!(
        ledger.acquire(&second, None).await,
        Err(HaError::Limit)
    ));
    assert!(ledger.rollback(&replacement, pending.id).await.unwrap());
    let restored = ledger.renew(&base, Some(first.id)).await.unwrap();
    assert_eq!(restored.id, first.id);
    assert!(ledger.acquire(&second, None).await.is_ok());
}

#[tokio::test]
async fn expired_target_is_reclaimed_even_beyond_bounded_global_sweep() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let client = db.local_client().await;
    client.execute("INSERT INTO fx_iptv_sessions(session_key,subscriber,grant_id,state,units,revision,operation_id,expires_at) SELECT 'dummy-'||n,'dummy',$1,'pending',1,0,$2,clock_timestamp()+interval '1 second' FROM generate_series(1,129) n", &[&Uuid::new_v4(),&Uuid::new_v4()]).await.unwrap();
    client
        .execute("UPDATE fx_iptv_capacity SET active_sessions=129", &[])
        .await
        .unwrap();
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let old = ledger.acquire(&req, None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let replacement = LeaseRequest {
        operation: Uuid::new_v4(),
        ..req
    };
    let fresh = ledger.acquire(&replacement, None).await.unwrap();
    assert_ne!(fresh.id, old.id);
    let counted: i32 = client
        .query_one("SELECT active_sessions FROM fx_iptv_capacity", &[])
        .await
        .unwrap()
        .get(0);
    let actual: i64 = client
        .query_one("SELECT count(*) FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(counted as i64, actual);
}

#[tokio::test]
async fn bounded_sweep_preserves_and_counts_all_live_prior_grants() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let client = db.local_client().await;
    let subscriber = SubscriberId::from_name("alice").as_hex();
    client.execute("INSERT INTO fx_iptv_sessions(session_key,subscriber,grant_id,state,units,revision,operation_id,expires_at,prior_grant_id,prior_units,prior_revision,prior_expires_at) SELECT 'prior-'||n,$1,$2,'pending',1,0,$3,clock_timestamp()-interval '1 second',$4,1,0,clock_timestamp()+interval '30 seconds' FROM generate_series(1,300) n", &[&subscriber,&Uuid::new_v4(),&Uuid::new_v4(),&Uuid::new_v4()]).await.unwrap();
    client
        .execute("UPDATE fx_iptv_capacity SET active_sessions=300", &[])
        .await
        .unwrap();

    // One admission restores only the first batch; the remaining prior grants
    // must still occupy their subscriber slots and the global capacity.
    let unrelated = request("bob", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    ledger.acquire(&unrelated, None).await.unwrap();
    let rows = client.query_one("SELECT count(*), count(*) FILTER (WHERE state='pending') FROM fx_iptv_sessions WHERE subscriber=$1", &[&subscriber]).await.unwrap();
    assert_eq!(rows.get::<_, i64>(0), 300);
    assert!(rows.get::<_, i64>(1) > 0);
    let count: i32 = client
        .query_one("SELECT active_sessions FROM fx_iptv_capacity", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 301);

    let extra = request("alice", Uuid::new_v4(), Uuid::new_v4(), 300, 0);
    assert_eq!(
        ledger.acquire(&extra, None).await.err(),
        Some(HaError::Limit)
    );
    let rows: i64 = client
        .query_one(
            "SELECT count(*) FROM fx_iptv_sessions WHERE subscriber=$1",
            &[&subscriber],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 300);
}

#[tokio::test]
async fn sweep_rechecks_prior_renewed_while_delete_waited_for_row_lock() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let mut owner = db.local_client().await;
    owner.execute("INSERT INTO fx_iptv_sessions(session_key,subscriber,grant_id,state,units,revision,operation_id,expires_at,prior_grant_id,prior_units,prior_revision,prior_expires_at) VALUES('renewed-prior','alice',$1,'pending',1,0,$2,clock_timestamp()-interval '1 second',$3,1,0,clock_timestamp()+interval '1 second')", &[&Uuid::new_v4(),&Uuid::new_v4(),&Uuid::new_v4()]).await.unwrap();
    owner
        .execute("UPDATE fx_iptv_capacity SET active_sessions=1", &[])
        .await
        .unwrap();
    let tx = owner.transaction().await.unwrap();
    tx.execute("UPDATE fx_iptv_sessions SET prior_expires_at=clock_timestamp()+interval '30 seconds' WHERE session_key='renewed-prior'", &[]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    let unrelated = request("bob", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let admission = tokio::spawn(async move { ledger.acquire(&unrelated, None).await });
    let monitor = db.local_client().await;
    let mut blocked_on_delete = false;
    for _ in 0..40 {
        let blocked: i64 = monitor.query_one("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'DELETE FROM fx_iptv_sessions WHERE expires_at%'", &[]).await.unwrap().get(0);
        if blocked > 0 {
            blocked_on_delete = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    tx.commit().await.unwrap();
    assert!(blocked_on_delete, "sweep did not wait on the renewing row");
    admission.await.unwrap().unwrap();
    let row = monitor.query_one("SELECT count(*), max(prior_expires_at) > clock_timestamp() FROM fx_iptv_sessions WHERE session_key='renewed-prior'", &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, Option<bool>>(1), Some(true));
    let count: i32 = monitor
        .query_one("SELECT active_sessions FROM fx_iptv_capacity", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
}

#[tokio::test]
async fn database_capacity_and_subscriber_expiry_fail_closed() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let client = db.local_client().await;
    let expired = LeaseRequest {
        subscriber_expires_at: Some(std::time::SystemTime::UNIX_EPOCH),
        ..request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0)
    };
    assert!(matches!(
        ledger.acquire(&expired, None).await,
        Err(HaError::Expired)
    ));
    client
        .execute("UPDATE fx_iptv_capacity SET active_sessions=100000", &[])
        .await
        .unwrap();
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    assert!(matches!(
        ledger.acquire(&req, None).await,
        Err(HaError::Full)
    ));
}

#[tokio::test]
async fn one_subscriber_lock_does_not_block_another_subscriber() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = Arc::new(HaLedger::new(pool));
    let mut client = db.local_client().await;
    let tx = client.transaction().await.unwrap();
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&SubscriberId::from_name("alice").as_hex()],
    )
    .await
    .unwrap();
    let alice = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let waiting = tokio::spawn({
        let ledger = ledger.clone();
        async move { ledger.acquire(&alice, None).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    let bob = request("bob", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            ledger.acquire(&bob, None)
        )
        .await
        .unwrap()
        .is_ok()
    );
    tx.rollback().await.unwrap();
    assert!(waiting.await.unwrap().is_ok());
}

#[tokio::test]
async fn admission_waiting_for_catalog_edit_rechecks_committed_revision() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = Arc::new(HaLedger::new(pool));
    let mut client = db.local_client().await;
    let tx = client.transaction().await.unwrap();
    tx.execute(
        "UPDATE fx_iptv_catalog SET revision=1 WHERE singleton=TRUE",
        &[],
    )
    .await
    .unwrap();
    let old = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let waiting = tokio::spawn({
        let ledger = ledger.clone();
        async move { ledger.acquire(&old, None).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    tx.commit().await.unwrap();
    assert!(matches!(
        waiting.await.unwrap(),
        Err(HaError::StaleRevision)
    ));
}

#[tokio::test]
async fn operation_table_saturation_rejects_new_grants_but_existing_release_still_works() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let existing = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let grant = ledger.acquire(&existing, None).await.unwrap();
    let client = db.local_client().await;
    client.execute("INSERT INTO fx_iptv_operations(operation_id,kind,session_key,subscriber,status,result,expires_at) SELECT md5(n::text)::uuid,'release','owned-'||n,'dummy','terminal',FALSE,clock_timestamp()+interval '1 hour' FROM generate_series(1,199999) n", &[]).await.unwrap();
    client
        .execute("UPDATE fx_iptv_capacity SET operation_count=200000", &[])
        .await
        .unwrap();
    let actual: i64 = client
        .query_one("SELECT count(*) FROM fx_iptv_operations", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(actual, 200000);
    let new_request = request("bob", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    assert!(matches!(
        ledger.acquire(&new_request, None).await,
        Err(HaError::Full)
    ));
    assert!(
        ledger
            .release(
                existing.subscriber,
                existing.session,
                grant.id,
                existing.operation
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn second_operation_for_same_grant_cannot_recreate_it_after_release() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let first = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    let grant = ledger.acquire(&first, None).await.unwrap();
    ledger.commit(&first, Some(grant.id)).await.unwrap();
    let second = LeaseRequest {
        operation: Uuid::new_v4(),
        ..first.clone()
    };
    assert_eq!(ledger.acquire(&second, None).await.unwrap().id, grant.id);
    assert!(
        ledger
            .release(first.subscriber, first.session, grant.id, first.operation)
            .await
            .unwrap()
    );
    assert!(matches!(
        ledger.acquire(&second, None).await,
        Err(HaError::Missing)
    ));
}

#[tokio::test]
async fn expired_pending_replacement_restores_the_prior_grant() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let ledger = HaLedger::new(pool);
    let base = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 0);
    let first = ledger.acquire(&base, None).await.unwrap();
    ledger.commit(&base, Some(first.id)).await.unwrap();
    let pending_req = LeaseRequest {
        operation: Uuid::new_v4(),
        units: 2,
        ..base.clone()
    };
    ledger.acquire(&pending_req, None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let other = request("alice", Uuid::new_v4(), Uuid::new_v4(), 2, 0);
    assert!(ledger.acquire(&other, None).await.is_ok());
    let renewed = ledger.renew(&base, Some(first.id)).await.unwrap();
    assert_eq!(renewed.id, first.id);
}

#[tokio::test]
async fn real_hundred_thousand_session_rows_enforce_global_cap() {
    let certs = tls_fixture::Certificates::new();
    let Some(db) = iptv_postgres::PgServer::start(&certs) else {
        return;
    };
    let pool = pool(&certs, db.port());
    schema::initialize(&pool, None).await.unwrap();
    let client = db.local_client().await;
    client.execute("INSERT INTO fx_iptv_sessions(session_key,subscriber,grant_id,state,units,revision,operation_id,expires_at) SELECT 'owned-'||n,'dummy',$1,'committed',1,0,$2,clock_timestamp()+interval '1 hour' FROM generate_series(1,100000) n", &[&Uuid::new_v4(),&Uuid::new_v4()]).await.unwrap();
    client
        .execute("UPDATE fx_iptv_capacity SET active_sessions=100000", &[])
        .await
        .unwrap();
    let ledger = HaLedger::new(pool);
    let req = request("alice", Uuid::new_v4(), Uuid::new_v4(), 1, 0);
    assert!(matches!(
        ledger.acquire(&req, None).await,
        Err(HaError::Full)
    ));
    let actual: i64 = client
        .query_one("SELECT count(*) FROM fx_iptv_sessions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(actual, 100000);
}
