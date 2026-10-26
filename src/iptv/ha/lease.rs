//! PostgreSQL-backed lease admission for opt-in IPTV HA authorities.
use super::connection::{DATABASE_TIMEOUT, HaError, HaPool};
use crate::iptv::lease::{LeaseGrant, LeaseRequest, SessionKey, SubscriberId};
use chrono::{DateTime, Utc};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

const MAX_SESSIONS: i32 = 100_000;
const MAX_OPERATIONS: i32 = 200_000;

pub struct HaLedger {
    pool: Arc<HaPool>,
}

impl HaLedger {
    pub fn new(pool: Arc<HaPool>) -> Self {
        Self { pool }
    }

    pub async fn ready(&self) -> Result<bool, HaError> {
        let connection = self.pool.acquire().await?;
        let row = tokio::time::timeout(
            DATABASE_TIMEOUT,
            connection.query_one(
                "SELECT active_sessions,operation_count FROM fx_iptv_capacity WHERE singleton=TRUE",
                &[],
            ),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
        .map_err(|_| HaError::Unavailable)?;
        Ok(row.get::<_, i32>(0) < MAX_SESSIONS && row.get::<_, i32>(1) < MAX_OPERATIONS)
    }

    pub async fn acquire(
        &self,
        req: &LeaseRequest,
        lease_id: Option<Uuid>,
    ) -> Result<LeaseGrant, HaError> {
        tokio::time::timeout(
            DATABASE_TIMEOUT,
            retry_transient(|| self.acquire_once(req, lease_id)),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
    }

    async fn acquire_once(
        &self,
        req: &LeaseRequest,
        lease_id: Option<Uuid>,
    ) -> Result<LeaseGrant, HaError> {
        if lease_id.is_some() || req.units == 0 || req.units > 1024 || req.limit == Some(0) {
            return Err(HaError::Invalid);
        }
        let mut connection = self.pool.acquire().await?;
        tokio::time::timeout(DATABASE_TIMEOUT, async {
            let tx = connection.transaction().await.map_err(|_| HaError::Unavailable)?;
            lock_catalog(&tx, &self.pool, Some(req.revision)).await?;
            let subscriber = req.subscriber.as_hex();
            let session = session_key(req.session);
            lock_subscriber(&tx, &subscriber).await?;
            lock_capacity_and_sweep(&tx).await?;
            let restored = tx.execute("UPDATE fx_iptv_sessions SET grant_id=prior_grant_id,state='committed',units=prior_units,revision=prior_revision,expires_at=prior_expires_at,prior_grant_id=NULL,prior_units=NULL,prior_revision=NULL,prior_expires_at=NULL WHERE session_key=$1 AND state='pending' AND expires_at<=clock_timestamp() AND prior_expires_at>clock_timestamp()", &[&session]).await.map_err(|_| HaError::Unavailable)?;
            let _ = restored;
            let expired = tx.execute("DELETE FROM fx_iptv_sessions WHERE session_key=$1 AND expires_at<=clock_timestamp()", &[&session]).await.map_err(|_| HaError::Unavailable)?;
            if expired > 0 { tx.execute("UPDATE fx_iptv_capacity SET active_sessions=active_sessions-$1 WHERE singleton=TRUE", &[&(expired as i32)]).await.map_err(|_| HaError::Unavailable)?; }
            let prior_op = tx.query_opt("SELECT kind,session_key,subscriber,grant_id,units,revision,grant_expires_at,status FROM fx_iptv_operations WHERE operation_id=$1 FOR UPDATE", &[&req.operation]).await.map_err(|_| HaError::Unavailable)?;
            if let Some(row) = prior_op {
                let kind: String = row.get(0);
                let recorded_session: String = row.get(1);
                let recorded_subscriber: String = row.get(2);
                let units: Option<i32> = row.get(4);
                let revision: Option<i64> = row.get(5);
                if kind != "acquire" || recorded_session != session || recorded_subscriber != subscriber || units != Some(req.units as i32) || revision != Some(req.revision as i64) {
                    return Err(HaError::Invalid);
                }
                if row.get::<_,String>(7) != "active" { return Err(HaError::Missing); }
                let grant_id: Uuid = row.get::<_,Option<Uuid>>(3).ok_or(HaError::Missing)?;
                let grant_expires: DateTime<Utc> = row.get::<_,Option<DateTime<Utc>>>(6).ok_or(HaError::Missing)?;
                let still_active = tx.query_opt("SELECT 1 FROM fx_iptv_sessions WHERE session_key=$1 AND grant_id=$2 AND expires_at>clock_timestamp()", &[&session,&grant_id]).await.map_err(|_| HaError::Unavailable)?.is_some();
                if !still_active { return Err(HaError::Missing); }
                let grant = grant_from_db(&tx, grant_id, grant_expires, req.units, req.revision).await?;
                tx.commit().await.map_err(|_| HaError::Unavailable)?;
                return Ok(grant);
            }
            let existing = tx.query_opt("SELECT grant_id,state,units,revision,expires_at,operation_id,prior_grant_id,prior_units,prior_revision,prior_expires_at FROM fx_iptv_sessions WHERE session_key=$1 FOR UPDATE", &[&session]).await.map_err(|_| HaError::Unavailable)?;
            if let Some(row) = &existing {
                let owner: String = tx.query_one("SELECT subscriber FROM fx_iptv_sessions WHERE session_key=$1", &[&session]).await.map_err(|_| HaError::Unavailable)?.get(0);
                if owner != subscriber { return Err(HaError::Invalid); }
                let state: String = row.get(1);
                let old_id: Uuid = row.get(0);
                let old_units: i32 = row.get(2);
                let old_revision: i64 = row.get(3);
                let old_expiry: DateTime<Utc> = row.get(4);
                if state == "committed" && old_units == req.units as i32 && old_revision == req.revision as i64 {
                    let count: i32 = tx.query_one("SELECT operation_count FROM fx_iptv_capacity WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?.get(0);
                    if count >= MAX_OPERATIONS { return Err(HaError::Full); }
                    let grant = grant_from_db(&tx, old_id, old_expiry, req.units, req.revision).await?;
                    tx.execute("INSERT INTO fx_iptv_operations(operation_id,kind,session_key,subscriber,grant_id,units,revision,grant_expires_at,status,result,expires_at) VALUES($1,'acquire',$2,$3,$4,$5,$6,$7,'active',TRUE,clock_timestamp()+interval '30 seconds')", &[&req.operation,&session,&subscriber,&old_id,&(req.units as i32),&(req.revision as i64),&old_expiry]).await.map_err(|_| HaError::Unavailable)?;
                    tx.execute("UPDATE fx_iptv_capacity SET operation_count=operation_count+1 WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
                    tx.commit().await.map_err(|_| HaError::Unavailable)?;
                    return Ok(grant);
                }
                if state != "committed" { return Err(HaError::Invalid); }
            }
            let row = tx.query_one("SELECT clock_timestamp(), LEAST(clock_timestamp() + interval '2 seconds', COALESCE($1::timestamptz, 'infinity'::timestamptz))", &[&req.subscriber_expires_at.map(DateTime::<Utc>::from)]).await.map_err(|_| HaError::Unavailable)?;
            let db_now: DateTime<Utc> = row.get(0);
            let expires_at: DateTime<Utc> = row.get(1);
            if expires_at <= db_now { return Err(HaError::Expired); }
            let occupied: i64 = tx.query_one("SELECT COALESCE(sum(GREATEST(units,COALESCE(prior_units,0))),0)::bigint FROM fx_iptv_sessions WHERE subscriber=$1 AND expires_at>clock_timestamp()", &[&subscriber]).await.map_err(|_| HaError::Unavailable)?.get(0);
            let old_occupied = existing.as_ref().map(|row| row.get::<_,i32>(2).max(row.get::<_,Option<i32>>(7).unwrap_or(0)) as i64).unwrap_or(0);
            let requested = (req.units as i64).max(old_occupied);
            if req.limit.is_some_and(|limit| occupied - old_occupied + requested > limit as i64) { return Err(HaError::Limit); }
            let capacity = tx.query_one("SELECT active_sessions,operation_count FROM fx_iptv_capacity WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
            if capacity.get::<_,i32>(1) >= MAX_OPERATIONS { return Err(HaError::Full); }
            if existing.is_none() && capacity.get::<_,i32>(0) >= MAX_SESSIONS { return Err(HaError::Full); }
            let id = Uuid::new_v4();
            if let Some(row) = existing {
                let old_id: Uuid = row.get(0);
                let old_units: i32 = row.get(2);
                let old_revision: i64 = row.get(3);
                let old_expiry: DateTime<Utc> = row.get(4);
                tx.execute("UPDATE fx_iptv_sessions SET grant_id=$2,state='pending',units=$3,revision=$4,operation_id=$5,expires_at=$6,prior_grant_id=$7,prior_units=$8,prior_revision=$9,prior_expires_at=$10 WHERE session_key=$1", &[&session,&id,&(req.units as i32),&(req.revision as i64),&req.operation,&expires_at,&old_id,&old_units,&old_revision,&old_expiry]).await.map_err(|_| HaError::Unavailable)?;
            } else {
                tx.execute("INSERT INTO fx_iptv_sessions(session_key,subscriber,grant_id,state,units,revision,operation_id,expires_at) VALUES($1,$2,$3,'pending',$4,$5,$6,$7)", &[&session,&subscriber,&id,&(req.units as i32),&(req.revision as i64),&req.operation,&expires_at]).await.map_err(|_| HaError::Unavailable)?;
                tx.execute("UPDATE fx_iptv_capacity SET active_sessions=active_sessions+1 WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
            }
            tx.execute("INSERT INTO fx_iptv_operations(operation_id,kind,session_key,subscriber,grant_id,units,revision,grant_expires_at,status,result,expires_at) VALUES($1,'acquire',$2,$3,$4,$5,$6,$7,'active',TRUE,clock_timestamp()+interval '30 seconds')", &[&req.operation,&session,&subscriber,&id,&(req.units as i32),&(req.revision as i64),&expires_at]).await.map_err(|_| HaError::Unavailable)?;
            tx.execute("UPDATE fx_iptv_capacity SET operation_count=operation_count+1 WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
            let grant = grant_from_db(&tx, id, expires_at, req.units, req.revision).await?;
            tx.commit().await.map_err(|_| HaError::Unavailable)?;
            Ok(grant)
        }).await.map_err(|_| HaError::Unavailable)?
    }

    pub async fn release(
        &self,
        subscriber: SubscriberId,
        session: SessionKey,
        lease_id: Uuid,
        operation: Uuid,
    ) -> Result<bool, HaError> {
        tokio::time::timeout(
            DATABASE_TIMEOUT,
            retry_transient(|| self.release_once(subscriber, session, lease_id, operation)),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
    }

    async fn release_once(
        &self,
        subscriber: SubscriberId,
        session: SessionKey,
        lease_id: Uuid,
        operation: Uuid,
    ) -> Result<bool, HaError> {
        let mut connection = self.pool.acquire().await?;
        tokio::time::timeout(DATABASE_TIMEOUT, async {
            let tx = connection.transaction().await.map_err(|_| HaError::Unavailable)?;
            lock_catalog(&tx, &self.pool, None).await?;
            let subscriber = subscriber.as_hex();
            let session = session_key(session);
            lock_subscriber(&tx, &subscriber).await?;
            lock_capacity_and_sweep(&tx).await?;
            let recorded = tx.query_opt("SELECT kind,session_key,subscriber,grant_id,result,status FROM fx_iptv_operations WHERE operation_id=$1 FOR UPDATE", &[&operation]).await.map_err(|_| HaError::Unavailable)?;
            if let Some(row) = &recorded {
                if !matches!(row.get::<_,String>(0).as_str(), "acquire" | "release") || row.get::<_,String>(1)!=session || row.get::<_,String>(2)!=subscriber || row.get::<_,Option<Uuid>>(3)!=Some(lease_id) { return Err(HaError::Invalid); }
                if row.get::<_,String>(5)=="terminal" {
                    let result: bool = row.get(4);
                    tx.commit().await.map_err(|_| HaError::Unavailable)?;
                    return Ok(result);
                }
            }
            let removed = tx.execute("DELETE FROM fx_iptv_sessions WHERE session_key=$1 AND subscriber=$2 AND grant_id=$3", &[&session,&subscriber,&lease_id]).await.map_err(|_| HaError::Unavailable)? > 0;
            if removed {
                tx.execute("UPDATE fx_iptv_capacity SET active_sessions=active_sessions-1 WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
                tx.execute("UPDATE fx_iptv_operations SET status='terminal',expires_at=GREATEST(expires_at,clock_timestamp()+interval '30 seconds') WHERE session_key=$1 AND grant_id=$2", &[&session,&lease_id]).await.map_err(|_| HaError::Unavailable)?;
            }
            if recorded.is_none() {
                let count: i32 = tx.query_one("SELECT operation_count FROM fx_iptv_capacity WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?.get(0);
                if count >= MAX_OPERATIONS { return Err(HaError::Full); }
                tx.execute("INSERT INTO fx_iptv_operations(operation_id,kind,session_key,subscriber,grant_id,status,result,expires_at) VALUES($1,'release',$2,$3,$4,'terminal',$5,clock_timestamp()+interval '30 seconds')", &[&operation,&session,&subscriber,&lease_id,&removed]).await.map_err(|_| HaError::Unavailable)?;
                tx.execute("UPDATE fx_iptv_capacity SET operation_count=operation_count+1 WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
            }
            tx.commit().await.map_err(|_| HaError::Unavailable)?;
            Ok(removed)
        }).await.map_err(|_| HaError::Unavailable)?
    }

    pub async fn rollback(&self, req: &LeaseRequest, lease_id: Uuid) -> Result<bool, HaError> {
        tokio::time::timeout(
            DATABASE_TIMEOUT,
            retry_transient(|| self.rollback_once(req, lease_id)),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
    }

    async fn rollback_once(&self, req: &LeaseRequest, lease_id: Uuid) -> Result<bool, HaError> {
        let mut connection = self.pool.acquire().await?;
        tokio::time::timeout(DATABASE_TIMEOUT, async {
            let tx = connection.transaction().await.map_err(|_| HaError::Unavailable)?;
            lock_catalog(&tx, &self.pool, None).await?;
            let subscriber = req.subscriber.as_hex();
            let session = session_key(req.session);
            lock_subscriber(&tx, &subscriber).await?;
            lock_capacity_and_sweep(&tx).await?;
            let row = tx.query_opt("SELECT grant_id,state,operation_id,prior_grant_id,prior_units,prior_revision,prior_expires_at FROM fx_iptv_sessions WHERE session_key=$1 AND subscriber=$2 FOR UPDATE", &[&session,&subscriber]).await.map_err(|_| HaError::Unavailable)?;
            let Some(row) = row else { return Ok(false); };
            if row.get::<_,Uuid>(0) != lease_id || row.get::<_,String>(1) != "pending" || row.get::<_,Uuid>(2) != req.operation { return Ok(false); }
            let previous: Option<Uuid> = row.get(3);
            if let Some(old_id) = previous {
                let old_units: i32 = row.get::<_,Option<i32>>(4).ok_or(HaError::UnsupportedSchema)?;
                let old_revision: i64 = row.get::<_,Option<i64>>(5).ok_or(HaError::UnsupportedSchema)?;
                let old_expiry: DateTime<Utc> = row.get::<_,Option<DateTime<Utc>>>(6).ok_or(HaError::UnsupportedSchema)?;
                tx.execute("UPDATE fx_iptv_sessions SET grant_id=$2,state='committed',units=$3,revision=$4,expires_at=$5,prior_grant_id=NULL,prior_units=NULL,prior_revision=NULL,prior_expires_at=NULL WHERE session_key=$1", &[&session,&old_id,&old_units,&old_revision,&old_expiry]).await.map_err(|_| HaError::Unavailable)?;
            } else {
                tx.execute("DELETE FROM fx_iptv_sessions WHERE session_key=$1", &[&session]).await.map_err(|_| HaError::Unavailable)?;
                tx.execute("UPDATE fx_iptv_capacity SET active_sessions=active_sessions-1 WHERE singleton=TRUE", &[]).await.map_err(|_| HaError::Unavailable)?;
            }
            tx.execute("UPDATE fx_iptv_operations SET status='terminal',expires_at=GREATEST(expires_at,clock_timestamp()+interval '30 seconds') WHERE operation_id=$1 AND grant_id=$2", &[&req.operation,&lease_id]).await.map_err(|_| HaError::Unavailable)?;
            tx.commit().await.map_err(|_| HaError::Unavailable)?;
            Ok(true)
        }).await.map_err(|_| HaError::Unavailable)?
    }

    pub async fn commit(
        &self,
        req: &LeaseRequest,
        lease_id: Option<Uuid>,
    ) -> Result<LeaseGrant, HaError> {
        self.transition(req, lease_id.ok_or(HaError::Invalid)?, true)
            .await
    }

    pub async fn renew(
        &self,
        req: &LeaseRequest,
        lease_id: Option<Uuid>,
    ) -> Result<LeaseGrant, HaError> {
        self.transition(req, lease_id.ok_or(HaError::Invalid)?, false)
            .await
    }

    async fn transition(
        &self,
        req: &LeaseRequest,
        lease_id: Uuid,
        commit: bool,
    ) -> Result<LeaseGrant, HaError> {
        tokio::time::timeout(
            DATABASE_TIMEOUT,
            retry_transient(|| self.transition_once(req, lease_id, commit)),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
    }

    async fn transition_once(
        &self,
        req: &LeaseRequest,
        lease_id: Uuid,
        commit: bool,
    ) -> Result<LeaseGrant, HaError> {
        let mut connection = self.pool.acquire().await?;
        tokio::time::timeout(DATABASE_TIMEOUT, async {
            let tx = connection.transaction().await.map_err(|_| HaError::Unavailable)?;
            lock_catalog(&tx, &self.pool, Some(req.revision)).await?;
            let subscriber = req.subscriber.as_hex();
            let session = session_key(req.session);
            lock_subscriber(&tx, &subscriber).await?;
            let row = tx.query_opt("SELECT subscriber,grant_id,state,units,revision,operation_id,expires_at,prior_grant_id,prior_units,prior_revision,prior_expires_at FROM fx_iptv_sessions WHERE session_key=$1 FOR UPDATE", &[&session]).await.map_err(|_| HaError::Unavailable)?.ok_or(HaError::Missing)?;
            if row.get::<_,String>(0) != subscriber { return Err(HaError::Invalid); }
            let current_id: Uuid = row.get(1);
            let state: String = row.get(2);
            let units: i32 = row.get(3);
            let revision: i64 = row.get(4);
            let operation: Uuid = row.get(5);
            let current_expiry: DateTime<Utc> = row.get(6);
            let prior_id: Option<Uuid> = row.get(7);
            let prior_expiry: Option<DateTime<Utc>> = row.get(10);
            let db_now: DateTime<Utc> = tx.query_one("SELECT clock_timestamp()", &[]).await.map_err(|_| HaError::Unavailable)?.get(0);
            if revision != req.revision as i64 { return Err(HaError::StaleRevision); }
            if !commit && state == "pending" && prior_id == Some(lease_id) {
                if row.get::<_,Option<i32>>(8) != Some(req.units as i32) || row.get::<_,Option<i64>>(9) != Some(req.revision as i64) || prior_expiry.is_none_or(|expiry| expiry <= db_now) { return Err(HaError::Missing); }
            } else if current_id != lease_id || current_expiry <= db_now {
                return Err(HaError::Missing);
            }
            let expires_at = if commit && state == "committed" {
                if req.units > units as u32 { return Err(HaError::Invalid); }
                current_expiry
            } else {
                let record = tx.query_one("SELECT LEAST(clock_timestamp()+interval '10 seconds',COALESCE($1::timestamptz,'infinity'::timestamptz))", &[&req.subscriber_expires_at.map(DateTime::<Utc>::from)]).await.map_err(|_| HaError::Unavailable)?;
                let expiry: DateTime<Utc> = record.get(0);
                if expiry <= db_now { return Err(HaError::Expired); }
                expiry
            };
            if commit {
                if state == "pending" {
                    if operation != req.operation || units != req.units as i32 { return Err(HaError::Invalid); }
                    tx.execute("UPDATE fx_iptv_sessions SET state='committed',expires_at=$2,prior_grant_id=NULL,prior_units=NULL,prior_revision=NULL,prior_expires_at=NULL WHERE session_key=$1", &[&session,&expires_at]).await.map_err(|_| HaError::Unavailable)?;
                    tx.execute("UPDATE fx_iptv_operations SET grant_expires_at=$2 WHERE operation_id=$1", &[&req.operation,&expires_at]).await.map_err(|_| HaError::Unavailable)?;
                }
            } else if state == "pending" && prior_id == Some(lease_id) {
                tx.execute("UPDATE fx_iptv_sessions SET prior_expires_at=$2 WHERE session_key=$1", &[&session,&expires_at]).await.map_err(|_| HaError::Unavailable)?;
            } else if state == "committed" && units == req.units as i32 {
                tx.execute("UPDATE fx_iptv_sessions SET expires_at=$2 WHERE session_key=$1", &[&session,&expires_at]).await.map_err(|_| HaError::Unavailable)?;
                tx.execute("UPDATE fx_iptv_operations SET grant_expires_at=$2 WHERE session_key=$1 AND grant_id=$3", &[&session,&expires_at,&lease_id]).await.map_err(|_| HaError::Unavailable)?;
            } else { return Err(HaError::Invalid); }
            let grant = grant_from_db(&tx, lease_id, expires_at, req.units, req.revision).await?;
            tx.commit().await.map_err(|_| HaError::Unavailable)?;
            Ok(grant)
        }).await.map_err(|_| HaError::Unavailable)?
    }
}

async fn retry_transient<T, F, Fut>(mut operation: F) -> Result<T, HaError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, HaError>>,
{
    for attempt in 0..=2 {
        match operation().await {
            Err(HaError::Unavailable) if attempt < 2 => {
                let jitter = u64::from(Uuid::new_v4().as_bytes()[0] % 7);
                tokio::time::sleep(Duration::from_millis(10 * (attempt + 1) + jitter)).await;
            }
            result => return result,
        }
    }
    Err(HaError::Unavailable)
}

fn session_key(session: SessionKey) -> String {
    format!("{}:{}", session.boot, session.local)
}

async fn lock_catalog(
    tx: &Transaction<'_>,
    pool: &HaPool,
    expected: Option<u64>,
) -> Result<(), HaError> {
    let row = tx.query_opt("SELECT c.revision,m.cluster_id,m.schema_version,m.epoch FROM fx_iptv_catalog c CROSS JOIN fx_iptv_meta m WHERE c.singleton=TRUE FOR SHARE OF c", &[]).await.map_err(|_| HaError::Unavailable)?.ok_or(HaError::UnsupportedSchema)?;
    let revision: i64 = row.get(0);
    pool.validate_metadata(&row.get::<_, String>(1), row.get(2), row.get(3))?;
    if expected.is_some_and(|value| i64::try_from(value).ok() != Some(revision)) {
        return Err(HaError::StaleRevision);
    }
    Ok(())
}

async fn lock_subscriber(tx: &Transaction<'_>, subscriber: &str) -> Result<(), HaError> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&subscriber],
    )
    .await
    .map_err(|_| HaError::Unavailable)?;
    Ok(())
}

async fn lock_capacity_and_sweep(tx: &Transaction<'_>) -> Result<(), HaError> {
    tx.query_one(
        "SELECT active_sessions FROM fx_iptv_capacity WHERE singleton=TRUE FOR UPDATE",
        &[],
    )
    .await
    .map_err(|_| HaError::Unavailable)?;
    let restored = tx.execute("UPDATE fx_iptv_sessions SET grant_id=prior_grant_id,state='committed',units=prior_units,revision=prior_revision,expires_at=prior_expires_at,prior_grant_id=NULL,prior_units=NULL,prior_revision=NULL,prior_expires_at=NULL WHERE session_key IN (SELECT session_key FROM fx_iptv_sessions WHERE state='pending' AND expires_at<=clock_timestamp() AND prior_expires_at>clock_timestamp() LIMIT 128)", &[]).await.map_err(|_| HaError::Unavailable)?;
    let _ = restored;
    let expired = tx.execute("DELETE FROM fx_iptv_sessions WHERE session_key IN (SELECT session_key FROM fx_iptv_sessions WHERE expires_at<=clock_timestamp() LIMIT 128)", &[]).await.map_err(|_| HaError::Unavailable)?;
    if expired > 0 {
        tx.execute(
            "UPDATE fx_iptv_capacity SET active_sessions=active_sessions-$1 WHERE singleton=TRUE",
            &[&(expired as i32)],
        )
        .await
        .map_err(|_| HaError::Unavailable)?;
    }
    let removed = tx.execute("DELETE FROM fx_iptv_operations WHERE operation_id IN (SELECT operation_id FROM fx_iptv_operations WHERE expires_at<=clock_timestamp() LIMIT 128)", &[]).await.map_err(|_| HaError::Unavailable)?;
    if removed > 0 {
        tx.execute(
            "UPDATE fx_iptv_capacity SET operation_count=operation_count-$1 WHERE singleton=TRUE",
            &[&(removed as i32)],
        )
        .await
        .map_err(|_| HaError::Unavailable)?;
    }
    Ok(())
}

async fn grant_from_db(
    tx: &Transaction<'_>,
    id: Uuid,
    expires: DateTime<Utc>,
    units: u32,
    revision: u64,
) -> Result<LeaseGrant, HaError> {
    let anchor = Instant::now();
    let row: Row = tx
        .query_one("SELECT clock_timestamp()", &[])
        .await
        .map_err(|_| HaError::Unavailable)?;
    let now: DateTime<Utc> = row.get(0);
    let remaining = (expires - now).to_std().map_err(|_| HaError::Missing)?;
    if remaining.is_zero() {
        return Err(HaError::Missing);
    }
    Ok(LeaseGrant {
        id,
        deadline: anchor + remaining,
        units,
        revision,
    })
}
