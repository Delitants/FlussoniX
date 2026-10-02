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

#[test]
fn invalid_auth_settings_never_save_or_disable_inherited_protection() {
    let d = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(d.path().join("c.json")).unwrap();
    for kind in ["streams", "templates"] {
        for policy in [
            json!({"on_play":[]}),
            json!({"on_play":"file:///auth"}),
            json!({"on_play":"auth://missing"}),
            json!({"flussonix_token_sha256":true}),
            json!({"flussonix_token_sha256":"not-a-digest"}),
        ] {
            let before = store.snapshot();
            assert!(store.put(kind, "bad", policy).is_err());
            assert_eq!(store.snapshot(), before);
        }
    }
}

#[test]
fn structured_auth_policy_validates_keys_and_preserves_template_options() {
    let d = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(d.path().join("c.json")).unwrap();
    store
        .put(
            "auth_backends",
            "billing",
            json!({"url":"https://middleware.example/auth"}),
        )
        .unwrap();
    assert!(store.put("templates","secure",json!({"on_play":{"url":"auth://billing","session_keys":["name","proto","token"],"max_sessions":2}})).is_ok());
    store
        .put(
            "streams",
            "owned",
            json!({"template":"secure","static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    assert_eq!(
        store.effective("owned").unwrap()["on_play"]["session_keys"],
        json!(["name", "proto", "token"])
    );
    for policy in [
        json!({"url":"file:///auth"}),
        json!({"url":"auth://missing"}),
        json!({"url":"https://middleware.example","domains":["example.org"]}),
        json!({"url":"https://middleware.example","session_keys":["token"]}),
        json!({"url":"https://middleware.example","max_sessions":-1}),
    ] {
        assert!(
            store
                .put("streams", "bad", json!({"on_play":policy}))
                .is_err()
        );
    }
}
