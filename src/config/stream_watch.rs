//! Irreversible local-stream authority fences, evaluated only at publication.
use super::{ConfigStore, effective};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio_util::sync::CancellationToken;

// Matches the listener's maximum 64 peers x 16 admitted subscriptions.
const MAX_WATCHES: usize = 1024;
#[derive(Default)]
pub(super) struct Watches(Mutex<Vec<Weak<StreamWatch>>>);
pub(crate) struct StreamWatch {
    name: String,
    expected: [u8; 32],
    cancel: CancellationToken,
}

impl ConfigStore {
    /// Register against the same locked snapshot that admitted local playback.
    /// Only the digest is retained; configuration and credentials are not copied
    /// into each admission. Dropping the owner leaves a prunable weak entry.
    pub(crate) fn watch_stream(
        &self,
        name: &str,
        revision: u64,
        cancel: CancellationToken,
    ) -> Option<Arc<StreamWatch>> {
        let root = self.data.lock().unwrap();
        if self.revision() != revision || cancel.is_cancelled() {
            return None;
        }
        let expected = fingerprint(&root, name)?;
        let mut watches = self.stream_watches.0.lock().unwrap();
        watches.retain(|w| w.upgrade().is_some_and(|w| !w.cancel.is_cancelled()));
        if watches.len() >= MAX_WATCHES {
            return None;
        }
        let watch = Arc::new(StreamWatch {
            name: name.into(),
            expected,
            cancel,
        });
        watches.push(Arc::downgrade(&watch));
        Some(watch)
    }

    /// The caller holds config data and has completed persistence successfully.
    /// Cancel before installing the new root/revision, so a visible publication
    /// cannot leave an old local authority live. No observer acquires other locks.
    pub(super) fn retire_stream_watches(&self, next: &Value) {
        let mut fingerprints = HashMap::<String, Option<[u8; 32]>>::new();
        self.stream_watches.0.lock().unwrap().retain(|weak| {
            let Some(watch) = weak.upgrade() else {
                return false;
            };
            if watch.cancel.is_cancelled() {
                return false;
            }
            let actual = fingerprints
                .entry(watch.name.clone())
                .or_insert_with(|| fingerprint(next, &watch.name));
            if *actual != Some(watch.expected) {
                watch.cancel.cancel();
                return false;
            }
            true
        });
    }
}

fn fingerprint(root: &Value, name: &str) -> Option<[u8; 32]> {
    let cfg = effective(root, name)?;
    if cfg["disabled"] == true {
        return None;
    }
    // Policy::from_config supports both the URL string and object forms.
    let raw = cfg["on_play"]
        .as_str()
        .or_else(|| cfg["on_play"]["url"].as_str());
    let backend = match raw.and_then(|raw| raw.strip_prefix("auth://")) {
        Some(name) => Some(
            root["auth_backends"]
                .as_array()?
                .iter()
                .find(|backend| backend["name"] == name)?,
        ),
        None => None,
    };
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    serde_json::to_writer(&mut writer, &cfg).expect("JSON value serialization");
    serde_json::to_writer(&mut writer, &backend).expect("JSON value serialization");
    Some(writer.0.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn setup() -> (tempfile::TempDir, ConfigStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::open(dir.path().join("state/config.json")).unwrap();
        store
            .replace(json!({
                "auth_backends":[{"name":"selected","url":"http://127.0.0.1:1/allow"},
                    {"name":"unused","url":"http://127.0.0.1:1/unused"}],
                "templates":[{"name":"live","static":false}],
                "streams":[{"name":"owned","template":"live","on_play":"auth://selected",
                    "inputs":[{"url":"testsrc://"}]}]
            }))
            .unwrap();
        (dir, store)
    }
    fn watch(store: &ConfigStore) -> (Arc<StreamWatch>, CancellationToken) {
        let cancel = CancellationToken::new();
        (
            store
                .watch_stream("owned", store.revision(), cancel.clone())
                .unwrap(),
            cancel,
        )
    }
    #[test]
    fn unrelated_edits_and_shadowed_template_changes_preserve_the_watch() {
        let (_dir, store) = setup();
        store
            .put("streams", "owned", json!({"static":false}))
            .unwrap();
        let (_watch, cancel) = watch(&store);
        store
            .put("streams", "other", json!({"static":false}))
            .unwrap();
        store
            .put("templates", "unused", json!({"disabled":true}))
            .unwrap();
        store
            .put(
                "auth_backends",
                "unused",
                json!({"url":"http://127.0.0.1:2/unused"}),
            )
            .unwrap();
        store
            .put("templates", "live", json!({"static":true}))
            .unwrap();
        store.delete("streams", "other").unwrap();
        store
            .replace(json!({"templates":[{"name":"live","static":true}]}))
            .unwrap();
        assert!(!cancel.is_cancelled());
    }
    #[test]
    fn relevant_puts_cancel_irreversibly_even_when_restored_before_observation() {
        for (kind, name, change, restore) in [
            (
                "streams",
                "owned",
                json!({"disabled":true}),
                json!({"disabled":null}),
            ),
            (
                "templates",
                "live",
                json!({"static":true}),
                json!({"static":false}),
            ),
            (
                "auth_backends",
                "selected",
                json!({"url":"http://127.0.0.1:2/deny"}),
                json!({"url":"http://127.0.0.1:1/allow"}),
            ),
        ] {
            let (_dir, store) = setup();
            let (_watch, cancel) = watch(&store);
            let original = store.snapshot();
            store.put(kind, name, change).unwrap();
            store.put(kind, name, restore).unwrap();
            assert_eq!(store.snapshot(), original, "restore must be exact");
            assert!(
                cancel.is_cancelled(),
                "{kind}/{name} restored retired authority"
            );
        }
    }
    #[test]
    fn object_policy_backend_and_stream_delete_or_replace_retire() {
        for operation in 0..3 {
            let (_dir, store) = setup();
            store
                .put(
                    "streams",
                    "owned",
                    json!({"on_play":{"url":"auth://selected", "max_sessions":10}}),
                )
                .unwrap();
            let (_watch, cancel) = watch(&store);
            match operation {
                0 => {
                    store
                        .put(
                            "auth_backends",
                            "selected",
                            json!({"url":"http://127.0.0.1:2/deny"}),
                        )
                        .unwrap();
                }
                1 => {
                    store.delete("streams", "owned").unwrap();
                }
                _ => {
                    store.replace(json!({"streams":[]})).unwrap();
                }
            }
            assert!(
                cancel.is_cancelled(),
                "operation {operation} left authority live"
            );
        }
    }
    #[test]
    fn failed_validation_or_persistence_does_not_retire_authority() {
        let (dir, store) = setup();
        let (_watch, cancel) = watch(&store);
        let revision = store.revision();
        assert!(
            store
                .put("streams", "owned", json!({"disabled":"invalid"}))
                .is_err()
        );
        let state = dir.path().join("state");
        let moved = dir.path().join("moved");
        std::fs::rename(&state, &moved).unwrap();
        std::fs::write(&state, b"owned persistence obstruction").unwrap();
        assert!(
            store
                .put("streams", "owned", json!({"disabled":true}))
                .is_err()
        );
        assert!(!cancel.is_cancelled());
        assert_eq!(store.revision(), revision);
        assert_ne!(store.effective("owned").unwrap()["disabled"], true);
    }
    #[test]
    fn stale_or_unavailable_registration_is_rejected() {
        let (_dir, store) = setup();
        let revision = store.revision();
        store
            .put("templates", "unused", json!({"static":false}))
            .unwrap();
        assert!(
            store
                .watch_stream("owned", revision, CancellationToken::new())
                .is_none()
        );
        assert!(
            store
                .watch_stream("absent", store.revision(), CancellationToken::new())
                .is_none()
        );
        store
            .put("streams", "owned", json!({"disabled":true}))
            .unwrap();
        assert!(
            store
                .watch_stream("owned", store.revision(), CancellationToken::new())
                .is_none()
        );
    }
    #[test]
    fn full_registry_rejects_then_reclaims_dropped_or_cancelled_owners() {
        let (_dir, store) = setup();
        let mut owners: Vec<_> = (0..1024).map(|_| watch(&store)).collect();
        assert!(
            store
                .watch_stream("owned", store.revision(), CancellationToken::new())
                .is_none()
        );
        owners.pop();
        let (_replacement, cancel) = watch(&store);
        cancel.cancel();
        let (_replacement_again, _) = watch(&store);
        drop(owners);
        let (_after_disconnect, cancel) = watch(&store);
        store
            .put("streams", "owned", json!({"disabled":true}))
            .unwrap();
        assert!(cancel.is_cancelled());
    }
}
