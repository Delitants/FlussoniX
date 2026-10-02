use flussonix::config::{ConfigStore, merge};
use serde_json::json;
#[test]
fn partial_update_keeps_omitted_values_and_null_removes_feature() {
    let old = json!({"title":"News","inputs":[{"url":"hls://example.net/live.m3u8"}],"transcoder":{"vb":800,"ab":64}});
    assert_eq!(
        merge(&old, &json!({"transcoder":{"vb":1000},"title":null})),
        json!({"inputs":[{"url":"hls://example.net/live.m3u8"}],"transcoder":{"vb":1000,"ab":64}})
    );
    assert_eq!(
        merge(&old, &json!({"$reset":true,"title":"New"})),
        json!({"title":"New"})
    );
}
#[test]
fn explicit_overrides_survive_restart_and_template_edit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let store = ConfigStore::open(&path).unwrap();
    store.put("templates","broadcast",json!({"title":"Default","static":false,"inputs":[{"url":"hls://example.net/live.m3u8"}]})).unwrap();
    store
        .put(
            "streams",
            "region/news",
            json!({"template":"broadcast","title":"Region"}),
        )
        .unwrap();
    let effective = store.effective("region/news").unwrap();
    assert_eq!(effective["title"], "Region");
    assert_eq!(effective["static"], false);
    assert_eq!(
        effective["config_on_disk"]["inputs"],
        serde_json::Value::Null
    );
    drop(store);
    let store = ConfigStore::open(&path).unwrap();
    store
        .put("templates", "broadcast", json!({"static":true}))
        .unwrap();
    assert_eq!(store.effective("region/news").unwrap()["static"], true);
    assert_eq!(store.effective("region/news").unwrap()["title"], "Region");
}
#[test]
fn invalid_protocol_does_not_change_saved_config() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("c.json")).unwrap();
    store
        .put("streams", "ok", json!({"inputs":[{"url":"testsrc://"}]}))
        .unwrap();
    let before = store.snapshot();
    assert!(
        store
            .put(
                "streams",
                "bad",
                json!({"inputs":[{"url":"rtsps://example.net/news"}]})
            )
            .is_err()
    );
    assert_eq!(store.snapshot(), before);
    assert!(
        store
            .validate(json!({"streams":[{"name":"x","inputs":[{"url":"testsrc://"}]}]}))
            .is_ok()
    );
    assert_eq!(store.snapshot(), before);
}
#[test]
fn m4s_ingest_can_be_saved_after_wire_adapter_is_available() {
    let d = tempfile::tempdir().unwrap();
    let s = ConfigStore::open(d.path().join("c.json")).unwrap();
    assert!(
        s.put(
            "streams",
            "relay",
            json!({"static":false,"inputs":[{"url":"m4s://example.net/live"}]})
        )
        .is_ok()
    );
}
#[test]
fn unsupported_options_and_failed_disk_write_do_not_report_success() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("sub/c.json");
    let s = ConfigStore::open(&p).unwrap();
    assert!(
        s.put(
            "streams",
            "ignored",
            json!({"inputs":[{"url":"testsrc://","password":"unused"}]})
        )
        .is_err()
    );
    let before = s.snapshot();
    std::fs::write(d.path().join("sub"), b"blocking parent").unwrap();
    assert!(
        s.put("streams", "x", json!({"inputs":[{"url":"testsrc://"}]}))
            .is_err()
    );
    assert_eq!(s.snapshot(), before);
}
#[test]
fn m4f_input_can_be_saved_after_sample_table_adapter_is_available() {
    let d = tempfile::tempdir().unwrap();
    let s = ConfigStore::open(d.path().join("c.json")).unwrap();
    assert!(
        s.put(
            "streams",
            "relay",
            json!({"static":false,"inputs":[{"url":"m4f://example.net/live"}]})
        )
        .is_ok()
    );
}
