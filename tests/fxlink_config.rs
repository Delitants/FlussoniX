use flussonix::config::ConfigStore;
use serde_json::json;

#[test]
fn explicit_fxlink_input_and_normal_controls_persist() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    store
        .put(
            "streams",
            "news",
            json!({
                "flussonix_peer_key": "owned-native-peer-test-key",
                "inputs": [{"url":"fxlink://localhost:55000/group/news",
                    "flussonix_fxlink_token":"owned-viewer-token",
                    "flussonix_fxlink":{"latency_ms":1000,"data_shards":20,"parity_shards":4}}]
            }),
        )
        .unwrap();
    assert_eq!(
        store.effective("news").unwrap()["inputs"][0]["flussonix_fxlink"]["latency_ms"],
        1000
    );
}

#[test]
fn malformed_or_misplaced_fxlink_controls_fail_without_saving() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    let before = store.snapshot();
    for row in [
        json!({"url":"fxlink://localhost/news"}),
        json!({"url":"fxlink://localhost:55000/news?token=secret"}),
        json!({"url":"fxlink://user:password@localhost:55000/news"}),
        json!({"url":"fxlink://localhost:55000/news","flussonix_fxlink":{"latency_ms":499}}),
        json!({"url":"fxlink://localhost:55000/news","flussonix_fxlink":{"latency_ms":1501}}),
        json!({"url":"fxlink://localhost:55000/news","flussonix_fxlink":{"parity_shards":0}}),
        json!({"url":"fxlink://localhost:55000/news","flussonix_fxlink":{"data_shards":3,"parity_shards":1}}),
        json!({"url":"fxlink://localhost:55000/news","flussonix_fxlink":{"unknown":true}}),
        json!({"url":"m4s://localhost/news","flussonix_fxlink":{"latency_ms":1000}}),
        json!({"url":"m4ss://localhost/news","flussonix_fxlink_token":"secret"}),
    ] {
        assert!(
            store
                .put(
                    "streams",
                    "bad",
                    json!({"flussonix_peer_key":"owned-native-peer-test-key","inputs":[row]})
                )
                .is_err()
        );
        assert_eq!(store.snapshot(), before);
    }
}

#[test]
fn m4s_and_m4ss_keep_legacy_urls_and_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    for (index, url) in [
        "m4s://origin.example/news?token=legacy",
        "m4ss://origin.example/news?token=legacy",
    ]
    .iter()
    .enumerate()
    {
        let name = format!("legacy{index}");
        store
            .put("streams", &name, json!({"inputs":[{"url":url}]}))
            .unwrap();
        assert_eq!(store.effective(&name).unwrap()["inputs"][0]["url"], *url);
    }
}

#[test]
fn native_cluster_source_keeps_management_and_udp_identity_separate() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    store
        .put(
            "sources",
            "owned-origin",
            json!({"api_url":"http://origin.example:8080",
        "flussonix_transport":"fxlink","flussonix_fxlink_url":"fxlink://origin.example:55000",
        "flussonix_fxlink_token":"source-viewer-token","flussonix_fxlink":{"latency_ms":1000}}),
        )
        .unwrap();
    let saved = store.snapshot();
    assert_eq!(saved["sources"][0]["api_url"], "http://origin.example:8080");
    assert_eq!(
        saved["sources"][0]["flussonix_fxlink_url"],
        "fxlink://origin.example:55000"
    );
    for bad in [
        json!({"api_url":"http://origin.example:8080","flussonix_transport":"fxlink"}),
        json!({"api_url":"http://origin.example:8080","flussonix_transport":"m4s","flussonix_fxlink_url":"fxlink://origin.example:55000"}),
        json!({"api_url":"http://origin.example:8080","flussonix_transport":"fxlink","flussonix_fxlink_url":"fxlink://origin.example:55000/stream"}),
        json!({"flussonix_transport":"fxlink","flussonix_fxlink_url":"fxlink://origin.example:55000"}),
        json!({"url":"m4s://origin.example:8080","flussonix_source_kind":"flussonic","flussonix_transport":"fxlink","flussonix_fxlink_url":"fxlink://origin.example:55000"}),
    ] {
        assert!(store.put("sources", "bad-origin", bad).is_err());
        assert_eq!(store.snapshot(), saved);
    }
}

#[test]
fn effective_fxlink_stream_requires_a_peer_key_and_can_inherit_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    assert!(
        store
            .put(
                "streams",
                "no-key",
                json!({"inputs":[{"url":"fxlink://origin.example:55000/news"}]})
            )
            .is_err()
    );
    store.put("templates","native",json!({"flussonix_peer_key":"owned-template-peer-key","inputs":[{"url":"fxlink://origin.example:55000/news"}]})).unwrap();
    store
        .put("streams", "inherited", json!({"template":"native"}))
        .unwrap();
    assert_eq!(
        store.effective("inherited").unwrap()["flussonix_peer_key"],
        "owned-template-peer-key"
    );
}
