use flussonix::config::ConfigStore;
use serde_json::json;
#[test]
fn public_rtsp_endpoints_validate_and_persist_without_mutating_failed_edits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let store = ConfigStore::open(&path).unwrap();
    store.put("peers", "edge", json!({"api_url":"http://127.0.0.1:9","flussonix_rtsp_url":"rtsp://cdn.example:8554","flussonix_rtsps_url":"rtsps://[::1]:8322/"})).unwrap();
    assert_eq!(
        ConfigStore::open(&path).unwrap().snapshot(),
        store.snapshot()
    );
    let before = store.snapshot();
    for field in ["flussonix_rtsp_url", "flussonix_rtsps_url"] {
        for invalid in [
            "http://cdn.example",
            "rtsp://user@cdn.example",
            "rtsp://@cdn.example",
            "rtsp://cdn.example/path",
            "rtsp://cdn.example?token=x",
            "rtsp://cdn.example#fragment",
            "rtsp://cdn.example:0",
            "rtsp://cdn.example:65536",
            "rtsp://cdn.example/percent%",
            "rtsp://cdn.example/raw\"quote",
            "rtsp://",
            "",
        ] {
            assert!(
                store.put("peers", "edge", json!({field:invalid})).is_err(),
                "{field}: {invalid}"
            );
            assert_eq!(store.snapshot(), before);
        }
    }
    assert!(
        store
            .put(
                "peers",
                "edge",
                json!({"flussonix_rtsp_url":"rtsps://cdn.example"})
            )
            .is_err()
    );
    assert!(
        store
            .put(
                "peers",
                "edge",
                json!({"flussonix_rtsps_url":"rtsp://cdn.example"})
            )
            .is_err()
    );
    assert!(store.put("sources","origin",json!({"api_url":"http://source.example","flussonix_rtsp_url":"rtsp://source.example"})).is_err());
    store
        .put("peers", "edge", json!({"flussonix_rtsp_url":null}))
        .unwrap();
    assert!(
        store.snapshot()["peers"][0]
            .get("flussonix_rtsp_url")
            .is_none()
    );
    assert_eq!(
        ConfigStore::open(&path).unwrap().snapshot(),
        store.snapshot()
    );
}
