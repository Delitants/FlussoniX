//! Canonical, revisioned IPTV catalog shared by HA authorities.
use super::connection::{DATABASE_TIMEOUT, HaError, HaPool};
use crate::{
    config::valid_name,
    iptv::store::{
        IptvDocument, IptvPackage, IptvSnapshot, IptvSubscriber, index_document, save_document,
        validate_patch,
    },
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

const MAX_DOCUMENT_BYTES: i32 = 64 * 1024 * 1024;

pub enum CatalogEdit {
    Enable(bool),
    PutPackage(String, Value),
    DeletePackage(String),
    PutSubscriber(String, Value),
    DeleteSubscriber(String),
}

#[derive(Debug)]
pub struct EditResult {
    pub revision: u64,
    pub item: Option<Value>,
}

pub struct HaCatalog {
    pool: Arc<HaPool>,
    export_path: PathBuf,
    active: RwLock<Option<Arc<IptvSnapshot>>>,
    export_healthy: AtomicBool,
    export_lock: tokio::sync::Mutex<()>,
}

impl HaCatalog {
    pub fn new(pool: Arc<HaPool>, export_path: PathBuf) -> Self {
        Self {
            pool,
            export_path,
            active: RwLock::new(None),
            export_healthy: AtomicBool::new(false),
            export_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn export_healthy(&self) -> bool {
        self.export_healthy.load(Ordering::Acquire)
    }

    pub async fn load_current(&self) -> Result<Arc<IptvSnapshot>, HaError> {
        let cached_revision = self
            .active
            .read()
            .unwrap()
            .as_ref()
            .and_then(|snapshot| i64::try_from(snapshot.revision()).ok())
            .unwrap_or(-1);
        let connection = self.pool.acquire().await?;
        let row = tokio::time::timeout(
            DATABASE_TIMEOUT,
            connection.query_opt(
                "SELECT m.cluster_id,m.schema_version,m.epoch,c.revision,octet_length(c.document),CASE WHEN c.revision = $1 THEN NULL WHEN octet_length(c.document) <= $2 THEN c.document ELSE NULL END FROM fx_iptv_meta m CROSS JOIN fx_iptv_catalog c WHERE c.singleton = TRUE LIMIT 1",
                &[&cached_revision, &MAX_DOCUMENT_BYTES],
            ),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
        .map_err(schema_or_unavailable)?
        .ok_or(HaError::UnsupportedSchema)?;
        let cluster: String = row.try_get(0).map_err(|_| HaError::UnsupportedSchema)?;
        let version: i32 = row.try_get(1).map_err(|_| HaError::UnsupportedSchema)?;
        let epoch: i64 = row.try_get(2).map_err(|_| HaError::UnsupportedSchema)?;
        self.pool.validate_metadata(&cluster, version, epoch)?;
        let revision: i64 = row.try_get(3).map_err(|_| HaError::UnsupportedSchema)?;
        let bytes: i32 = row.try_get(4).map_err(|_| HaError::UnsupportedSchema)?;
        if revision < 0 || !(0..=MAX_DOCUMENT_BYTES).contains(&bytes) {
            return Err(HaError::UnsupportedSchema);
        }
        if let Some(cached) = self.active.read().unwrap().as_ref() {
            if cached.revision() > revision as u64 {
                return Err(HaError::StaleRevision);
            }
            if cached.revision() == revision as u64 {
                return Ok(cached.clone());
            }
        }
        let encoded: String = row
            .try_get::<_, Option<String>>(5)
            .map_err(|_| HaError::UnsupportedSchema)?
            .ok_or(HaError::UnsupportedSchema)?;
        let document: IptvDocument =
            serde_json::from_str(&encoded).map_err(|_| HaError::UnsupportedSchema)?;
        let snapshot = Arc::new(
            index_document(document, revision as u64).map_err(|_| HaError::UnsupportedSchema)?,
        );
        install_loaded_snapshot(&self.active, snapshot)
    }

    pub async fn edit(
        &self,
        expected_revision: u64,
        edit: CatalogEdit,
    ) -> Result<EditResult, HaError> {
        let expected = i64::try_from(expected_revision).map_err(|_| HaError::PreconditionFailed)?;
        let mut connection = self.pool.acquire().await?;
        let (snapshot, item) = tokio::time::timeout(DATABASE_TIMEOUT, async {
            let transaction = connection.transaction().await.map_err(|_| HaError::Unavailable)?;
            let row = transaction.query_opt(
                "SELECT revision,octet_length(document),CASE WHEN octet_length(document) <= $1 THEN document ELSE NULL END FROM fx_iptv_catalog WHERE singleton = TRUE FOR UPDATE",
                &[&MAX_DOCUMENT_BYTES],
            ).await.map_err(schema_or_unavailable)?.ok_or(HaError::UnsupportedSchema)?;
            let revision: i64 = row.try_get(0).map_err(|_| HaError::UnsupportedSchema)?;
            if revision != expected { return Err(HaError::PreconditionFailed); }
            let bytes: i32 = row.try_get(1).map_err(|_| HaError::UnsupportedSchema)?;
            if !(0..=MAX_DOCUMENT_BYTES).contains(&bytes) { return Err(HaError::UnsupportedSchema); }
            let encoded: String = row.try_get::<_,Option<String>>(2).map_err(|_| HaError::UnsupportedSchema)?.ok_or(HaError::UnsupportedSchema)?;
            let metadata = transaction.query_one("SELECT cluster_id,schema_version,epoch FROM fx_iptv_meta LIMIT 1", &[]).await.map_err(schema_or_unavailable)?;
            let cluster: String = metadata.try_get(0).map_err(|_| HaError::UnsupportedSchema)?;
            let version: i32 = metadata.try_get(1).map_err(|_| HaError::UnsupportedSchema)?;
            let epoch: i64 = metadata.try_get(2).map_err(|_| HaError::UnsupportedSchema)?;
            self.pool.validate_metadata(&cluster, version, epoch)?;
            let mut document: IptvDocument = serde_json::from_str(&encoded).map_err(|_| HaError::UnsupportedSchema)?;
            let item = apply_edit(&mut document, edit)?;
            let next_revision = expected.checked_add(1).ok_or(HaError::Invalid)?;
            let snapshot = Arc::new(index_document(document, next_revision as u64).map_err(|_| HaError::Invalid)?);
            let encoded = serde_json::to_string(snapshot.document()).map_err(|_| HaError::Invalid)?;
            if encoded.len() > MAX_DOCUMENT_BYTES as usize { return Err(HaError::Invalid); }
            transaction.execute("UPDATE fx_iptv_catalog SET revision = $1, document = $2 WHERE singleton = TRUE", &[&next_revision,&encoded]).await.map_err(|_| HaError::Unavailable)?;
            transaction.commit().await.map_err(|_| HaError::Unavailable)?;
            Ok::<_, HaError>((snapshot,item))
        }).await.map_err(|_| HaError::Unavailable)??;
        {
            let mut active = self.active.write().unwrap();
            if active
                .as_ref()
                .is_none_or(|old| old.revision() <= snapshot.revision())
            {
                *active = Some(snapshot.clone());
            }
        }
        self.export_snapshot(snapshot.clone()).await;
        Ok(EditResult {
            revision: snapshot.revision(),
            item,
        })
    }

    pub async fn retry_export(&self) -> Result<(), HaError> {
        let snapshot = self.load_current().await?;
        self.export_snapshot(snapshot).await;
        if self.export_healthy() {
            Ok(())
        } else {
            Err(HaError::Unavailable)
        }
    }

    async fn export_snapshot(&self, snapshot: Arc<IptvSnapshot>) {
        let _guard = self.export_lock.lock().await;
        let snapshot = match self.load_current().await {
            Ok(current) if current.revision() >= snapshot.revision() => current,
            Ok(_) | Err(_) => {
                self.export_healthy.store(false, Ordering::Release);
                return;
            }
        };
        let path = self.export_path.clone();
        let result = tokio::task::spawn_blocking(move || {
            let bytes =
                serde_json::to_vec_pretty(snapshot.document()).map_err(|_| "invalid IPTV data")?;
            save_document(&path, &bytes)
        })
        .await;
        self.export_healthy
            .store(matches!(result, Ok(Ok(()))), Ordering::Release);
    }
}

fn install_loaded_snapshot(
    active: &RwLock<Option<Arc<IptvSnapshot>>>,
    snapshot: Arc<IptvSnapshot>,
) -> Result<Arc<IptvSnapshot>, HaError> {
    let mut active = active.write().unwrap();
    if let Some(previous) = active.as_ref() {
        if previous.revision() > snapshot.revision() {
            return Err(HaError::StaleRevision);
        }
        if previous.revision() == snapshot.revision() {
            return Ok(previous.clone());
        }
    }
    *active = Some(snapshot.clone());
    Ok(snapshot)
}

fn schema_or_unavailable(error: tokio_postgres::Error) -> HaError {
    if error
        .code()
        .is_some_and(|code| matches!(code.code(), "42P01" | "42703"))
    {
        HaError::UnsupportedSchema
    } else {
        HaError::Unavailable
    }
}

fn apply_edit(document: &mut IptvDocument, edit: CatalogEdit) -> Result<Option<Value>, HaError> {
    match edit {
        CatalogEdit::Enable(value) => {
            document.enabled = value;
            Ok(None)
        }
        CatalogEdit::PutPackage(name, patch) => {
            valid_name(&name).map_err(|_| HaError::Invalid)?;
            validate_patch(&patch, &name, &["name", "channels"]).map_err(|_| HaError::Invalid)?;
            let existing = document.packages.iter().position(|item| item.name == name);
            let base = existing
                .map(|i| serde_json::to_value(&document.packages[i]).unwrap())
                .unwrap_or_else(|| json!({"name":name,"channels":[]}));
            let merged = crate::config::merge(&base, &patch);
            let package: IptvPackage =
                serde_json::from_value(merged).map_err(|_| HaError::Invalid)?;
            if package.name != name {
                return Err(HaError::Invalid);
            }
            let result = serde_json::to_value(&package).map_err(|_| HaError::Invalid)?;
            match existing {
                Some(i) => document.packages[i] = package,
                None => document.packages.push(package),
            }
            Ok(Some(result))
        }
        CatalogEdit::DeletePackage(name) => {
            let before = document.packages.len();
            document.packages.retain(|item| item.name != name);
            if before == document.packages.len() {
                return Err(HaError::NotFound);
            }
            for subscriber in &mut document.subscribers {
                subscriber.packages.retain(|item| item != &name);
            }
            Ok(None)
        }
        CatalogEdit::PutSubscriber(name, patch) => {
            valid_name(&name).map_err(|_| HaError::Invalid)?;
            validate_patch(
                &patch,
                &name,
                &[
                    "name",
                    "token",
                    "packages",
                    "max_sessions",
                    "flussonix_expires_at",
                ],
            )
            .map_err(|_| HaError::Invalid)?;
            let existing = document
                .subscribers
                .iter()
                .position(|item| item.name == name);
            let base = existing
                .map(|i| serde_json::to_value(&document.subscribers[i]).unwrap())
                .unwrap_or_else(|| json!({"name":name,"packages":[]}));
            let mut merged = crate::config::merge(&base, &patch);
            if merged.get("token").is_none_or(Value::is_null) {
                merged["token"] = Value::String(format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    uuid::Uuid::new_v4().simple()
                ));
            }
            let subscriber: IptvSubscriber =
                serde_json::from_value(merged).map_err(|_| HaError::Invalid)?;
            if subscriber.name != name {
                return Err(HaError::Invalid);
            }
            let result = serde_json::to_value(&subscriber).map_err(|_| HaError::Invalid)?;
            match existing {
                Some(i) => document.subscribers[i] = subscriber,
                None => document.subscribers.push(subscriber),
            }
            Ok(Some(result))
        }
        CatalogEdit::DeleteSubscriber(name) => {
            let before = document.subscribers.len();
            document.subscribers.retain(|item| item.name != name);
            if before == document.subscribers.len() {
                return Err(HaError::NotFound);
            }
            Ok(None)
        }
    }
}

#[cfg(test)]
mod snapshot_install_tests {
    use super::*;
    use std::{sync::mpsc, thread};

    fn snapshot(revision: u64) -> Arc<IptvSnapshot> {
        Arc::new(index_document(IptvDocument::default(), revision).unwrap())
    }

    #[test]
    fn parsed_older_revision_cannot_install_after_newer_concurrent_refresh() {
        let active = Arc::new(RwLock::new(Some(snapshot(0))));
        let (parsed_tx, parsed_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let older_active = active.clone();
        let older = thread::spawn(move || {
            let parsed_older = snapshot(1);
            parsed_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            install_loaded_snapshot(&older_active, parsed_older)
        });

        parsed_rx.recv().unwrap();
        let newer = snapshot(2);
        let installed = install_loaded_snapshot(&active, newer.clone()).unwrap();
        assert!(Arc::ptr_eq(&installed, &newer));
        resume_tx.send(()).unwrap();
        assert!(matches!(older.join().unwrap(), Err(HaError::StaleRevision)));
        assert!(Arc::ptr_eq(
            active.read().unwrap().as_ref().unwrap(),
            &newer
        ));
    }
}
