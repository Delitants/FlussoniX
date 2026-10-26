//! Explicit, one-time creation of the IPTV HA schema.
use super::connection::{DATABASE_TIMEOUT, HaError, HaPool};
use crate::iptv::store::{IptvDocument, index_document, read_document};
use std::path::Path;

/// Creates schema v1 only in a database with no IPTV HA tables. A standalone
/// `iptv.json` is never read unless its path is explicitly supplied here.
pub async fn initialize(pool: &HaPool, import: Option<&Path>) -> Result<(), HaError> {
    let document = match import {
        Some(path) => read_document(path)
            .map_err(|_| HaError::InvalidConfig)?
            .ok_or(HaError::InvalidConfig)?,
        None => IptvDocument::default(),
    };
    index_document(document.clone(), 0).map_err(|_| HaError::Invalid)?;
    let encoded = serde_json::to_string(&document).map_err(|_| HaError::Invalid)?;
    if encoded.len() > 64 * 1024 * 1024 {
        return Err(HaError::Invalid);
    }
    let mut connection = pool.acquire().await?;
    tokio::time::timeout(DATABASE_TIMEOUT, async {
        let transaction = connection.transaction().await.map_err(|_| HaError::Unavailable)?;
        transaction
            .query_one("SELECT pg_advisory_xact_lock(1196988277)", &[])
            .await
            .map_err(|_| HaError::Unavailable)?;
        let row = transaction
            .query_one(
                "SELECT to_regclass('fx_iptv_meta')::text, to_regclass('fx_iptv_catalog')::text, to_regclass('fx_iptv_capacity')::text",
                &[],
            )
            .await
            .map_err(|_| HaError::Unavailable)?;
        if (0..3).any(|column| row.get::<_, Option<String>>(column).is_some()) {
            return Err(HaError::InvalidConfig);
        }
        transaction
            .batch_execute(
                "CREATE TABLE fx_iptv_meta (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), cluster_id TEXT NOT NULL, schema_version INTEGER NOT NULL, epoch BIGINT NOT NULL CHECK (epoch > 0));
                 CREATE TABLE fx_iptv_catalog (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), revision BIGINT NOT NULL CHECK (revision >= 0), document TEXT NOT NULL);
                 CREATE TABLE fx_iptv_capacity (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton), active_sessions INTEGER NOT NULL DEFAULT 0 CHECK (active_sessions BETWEEN 0 AND 100000), operation_count INTEGER NOT NULL DEFAULT 0 CHECK (operation_count >= 0));
                 CREATE TABLE fx_iptv_sessions (session_key TEXT PRIMARY KEY, subscriber TEXT NOT NULL, grant_id UUID NOT NULL, state TEXT NOT NULL CHECK (state IN ('pending','committed')), units INTEGER NOT NULL CHECK (units BETWEEN 1 AND 1024), revision BIGINT NOT NULL, operation_id UUID NOT NULL, expires_at TIMESTAMPTZ NOT NULL, prior_grant_id UUID, prior_units INTEGER, prior_revision BIGINT, prior_expires_at TIMESTAMPTZ);
                 CREATE INDEX fx_iptv_sessions_subscriber ON fx_iptv_sessions(subscriber);
                 CREATE INDEX fx_iptv_sessions_expiry ON fx_iptv_sessions(expires_at);
                 CREATE TABLE fx_iptv_operations (operation_id UUID PRIMARY KEY, kind TEXT NOT NULL, session_key TEXT NOT NULL, subscriber TEXT NOT NULL, grant_id UUID, units INTEGER, revision BIGINT, grant_expires_at TIMESTAMPTZ, status TEXT NOT NULL CHECK (status IN ('active','terminal')), result BOOLEAN NOT NULL, expires_at TIMESTAMPTZ NOT NULL);
                 CREATE INDEX fx_iptv_operations_expiry ON fx_iptv_operations(expires_at);",
            )
            .await
            .map_err(|_| HaError::Unavailable)?;
        transaction
            .execute(
                "INSERT INTO fx_iptv_meta (cluster_id, schema_version, epoch) VALUES ($1, 1, 1)",
                &[&pool.cluster_id().to_string()],
            )
            .await
            .map_err(|_| HaError::Unavailable)?;
        transaction
            .execute(
                "INSERT INTO fx_iptv_catalog (singleton, revision, document) VALUES (TRUE, 0, $1)",
                &[&encoded],
            )
            .await
            .map_err(|_| HaError::Unavailable)?;
        transaction
            .batch_execute("INSERT INTO fx_iptv_capacity (singleton) VALUES (TRUE)")
            .await
            .map_err(|_| HaError::Unavailable)?;
        transaction.commit().await.map_err(|_| HaError::Unavailable)
    })
    .await
    .map_err(|_| HaError::Unavailable)?
}
