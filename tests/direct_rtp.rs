use flussonix::config::ConfigStore;
use serde_json::json;
#[test]
fn direct_rtp_configuration_inherits_and_rejects_ambiguous_endpoints() {
    let d = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(d.path().join("config.json")).unwrap();
    let output = json!([{"url":"rtp://127.0.0.1:39000","max_mbps":20}]);
    store.put("templates","direct",json!({"inputs":[{"url":"rtp://127.0.0.1:39002","flussonix_rtp":{"jitter_ms":25,"source_ip":"127.0.0.1"}}],"flussonix_rtp_outputs":output})).unwrap();
    store
        .put("streams", "owned", json!({"template":"direct"}))
        .unwrap();
    assert_eq!(
        store.effective("owned").unwrap()["flussonix_rtp_outputs"],
        output
    );
    store
        .put("streams", "owned", json!({"flussonix_rtp_outputs":[]}))
        .unwrap();
    assert_eq!(
        store.effective("owned").unwrap()["flussonix_rtp_outputs"],
        json!([])
    );
    let before = store.snapshot();
    for url in [
        "rtp://127.0.0.1:65535",
        "rtp://127.0.0.1:80",
        "rtp://example.org:39000",
        "rtp://127.0.0.1:39000?secret=abc",
        "rtp://127.0.0.1:39000/path",
        "rtp://user@127.0.0.1:39000",
        "rtp://239.1.2.3:39000",
    ] {
        assert!(
            store
                .put("streams", "bad", json!({"inputs":[{"url":url}]}))
                .is_err(),
            "{url}"
        );
        assert!(
            store
                .put(
                    "streams",
                    "bad",
                    json!({"flussonix_rtp_outputs":[{"url":url}]})
                )
                .is_err(),
            "{url}"
        );
    }
    assert!(store.put("streams","bad",json!({"inputs":[{"url":"rtp://127.0.0.1:39000","flussonix_rtp":{"jitter_ms":1001}}]})).is_err());
    assert!(
        store
            .put(
                "streams",
                "bad",
                json!({"flussonix_rtp_outputs":[{"url":"rtp://127.0.0.1:39000","max_mbps":0}]})
            )
            .is_err()
    );
    assert_eq!(store.snapshot(), before);
    store.put("streams","multicast",json!({"inputs":[{"url":"rtp://239.1.2.3:39000","flussonix_rtp":{"interface":"127.0.0.1","ttl":1}}]})).unwrap();
}
