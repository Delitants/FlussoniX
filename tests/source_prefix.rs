//! Source prefix configuration must select a namespace without renaming streams.
use flussonix::config::ConfigStore;
use serde_json::json;

// Break caught: prefix cannot be configured/persisted or removed by normal merge semantics.
#[test]
fn source_prefix_survives_import_restart_and_can_be_removed() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    for key in ["native", "m4ss://origin.invalid:8443"] {
        let row = s.put("sources", key, json!({"api_url":"http://origin.invalid", "prefix":"region/eu", "except":["news","sports/*"]})).unwrap();
        assert_eq!(row["prefix"], "region/eu");
        assert_eq!(row["except"], json!(["news", "sports/*"]));
    }
    let saved = s.snapshot();
    assert_eq!(ConfigStore::open(&p).unwrap().snapshot(), saved);
    let disk = std::fs::read(&p).unwrap();
    let validated = s
        .validate(json!({"sources":[{"url":"m4s://import.invalid","prefix":"日本語/地域"}]}))
        .unwrap();
    assert_eq!(validated["sources"][0]["prefix"], "日本語/地域");
    assert_eq!(s.snapshot(), saved);
    assert_eq!(std::fs::read(&p).unwrap(), disk);
    s.replace(validated).unwrap();
    assert_eq!(
        ConfigStore::open(&p).unwrap().snapshot()["sources"][0]["prefix"],
        "日本語/地域"
    );
    assert!(
        s.put("sources", "m4s://import.invalid", json!({"prefix":null}))
            .unwrap()
            .get("prefix")
            .is_none()
    );
    s.put(
        "sources",
        "m4s://import.invalid",
        json!({"prefix":"region"}),
    )
    .unwrap();
    assert!(
        s.put("sources", "m4s://import.invalid", json!({"$reset":true}))
            .unwrap()
            .get("prefix")
            .is_none()
    );
}

// Break caught: malformed prefixes save or advance revision; non-source collections accept prefix.
#[test]
fn source_prefix_validation_rejects_invalid_updates_atomically() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    s.put(
        "sources",
        "native",
        json!({"api_url":"http://origin.invalid","prefix":"region"}),
    )
    .unwrap();
    let before = s.snapshot();
    let disk = std::fs::read(&p).unwrap();
    let revision = s.revision();
    for prefix in [
        json!(""),
        json!("/region"),
        json!("region/"),
        json!("region//eu"),
        json!("region/.."),
        json!("region/*"),
        json!("region*"),
        json!("a?b"),
        json!("a#b"),
        json!("a\\b"),
        json!("a\nb"),
        json!("x".repeat(257)),
        json!(42),
        json!(false),
        json!([]),
        json!({}),
    ] {
        assert!(
            s.put("sources", "native", json!({"prefix":prefix}))
                .is_err(),
            "{prefix}"
        );
        assert_eq!(s.snapshot(), before);
        assert_eq!(s.revision(), revision);
        assert_eq!(std::fs::read(&p).unwrap(), disk);
    }
    assert!(s.validate(json!({"sources":[{"hostname":"native","api_url":"http://origin.invalid","prefix":null}]})).is_err());
    for (kind, base) in [
        ("streams", json!({"inputs":[{"url":"testsrc://"}]})),
        ("templates", json!({"inputs":[{"url":"testsrc://"}]})),
        ("peers", json!({"api_url":"http://origin.invalid"})),
        ("auth_backends", json!({"url":"http://auth.invalid"})),
    ] {
        s.put(kind, "other", base).unwrap();
        assert!(s.put(kind, "other", json!({"prefix":"region"})).is_err());
    }
}
