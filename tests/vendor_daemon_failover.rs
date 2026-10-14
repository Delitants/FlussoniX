//! Owned daemon replica groups: no production services or vendor runtime.
use flussonix::config::ConfigStore;
use serde_json::{Value, json};

fn source(group: &str, content: &str) -> Value {
    json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-config-key","on_play":"true","flussonix_source_group":group,"flussonix_content_id":content})
}
#[test]
fn explicit_vendor_replica_group_can_be_staged_and_saved_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    for host in ["one", "two"] {
        store
            .put(
                "sources",
                &format!("m4s://{host}.invalid"),
                source("replicas", "music-v1"),
            )
            .unwrap();
    }
    let reopened = ConfigStore::open(dir.path().join("config.json")).unwrap();
    assert_eq!(reopened.snapshot()["sources"].as_array().unwrap().len(), 2);
    assert!(
        reopened.snapshot()["sources"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["flussonix_content_id"] == "music-v1")
    );
}
#[test]
fn vendor_replica_group_rejects_different_content_policy_namespace_or_processing() {
    for patch in [
        json!({"flussonix_content_id":"different"}),
        json!({"flussonix_token_sha256":"a".repeat(64)}),
        json!({"group_config":{"paid":{"flussonix_token_sha256":"b".repeat(64)}}}),
        json!({"prefix":"different"}),
        json!({"except":["news"]}),
        json!({"flussonix_output_template":"aac"}),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
        store
            .put(
                "templates",
                "aac",
                json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96}}),
            )
            .unwrap();
        store
            .put(
                "sources",
                "m4s://one.invalid",
                source("replicas", "music-v1"),
            )
            .unwrap();
        let before = store.snapshot();
        let mut other = source("replicas", "music-v1");
        for (key, value) in patch.as_object().unwrap() {
            other[key] = value.clone();
        }
        assert!(store.put("sources", "m4s://two.invalid", other).is_err());
        assert_eq!(store.snapshot(), before);
    }
}
#[test]
fn vendor_group_requires_paired_valid_identity_and_cannot_mix_with_native_sources() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    for bad in [
        json!({"flussonix_source_group":"replicas"}),
        json!({"flussonix_content_id":"content"}),
        source("bad group", "content"),
        source("replicas", ""),
    ] {
        let mut row = json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-config-key","on_play":"true"});
        for (key, value) in bad.as_object().unwrap() {
            row[key] = value.clone();
        }
        assert!(store.put("sources", "m4s://bad.invalid", row).is_err());
    }
    store
        .put(
            "sources",
            "m4s://one.invalid",
            source("replicas", "content"),
        )
        .unwrap();
    assert!(
        store
            .put(
                "sources",
                "native",
                json!({"api_url":"http://native.invalid","flussonix_source_group":"replicas"})
            )
            .is_err()
    );
}
