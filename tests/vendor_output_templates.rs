use flussonix::{
    config::ConfigStore, source_directory::modern_coordinator::ModernSourceCoordinator,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
const KEY: &str = "m4s://owned-output.invalid:8080";
fn source() -> Value {
    json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-output-private-key","on_play":"true","flussonix_output_template":"processing"})
}
fn store() -> (tempfile::TempDir, Arc<ConfigStore>) {
    let d = tempfile::tempdir().unwrap();
    let store = Arc::new(ConfigStore::open(d.path().join("config.json")).unwrap());
    store.put("templates", "processing", json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96},"flussonix_hls_subtitles":"drop","inputs":[{"url":"http://owned-user:owned-template-secret@unused.invalid"}],"on_play":"https://unused-auth.invalid/check","static":true})).unwrap();
    (d, store)
}
#[test]
fn processing_reference_persists_reopens_and_prepares_independent_clients() {
    let (d, s) = store();
    let saved = s
        .put("sources", KEY, source())
        .expect("processing reference must save");
    assert_eq!(saved["flussonix_output_template"], "processing");
    assert_eq!(
        ConfigStore::open(d.path().join("config.json"))
            .unwrap()
            .snapshot(),
        s.snapshot()
    );
    let owner = ModernSourceCoordinator::new(s, "owned-output-node").unwrap();
    owner
        .install(
            owner
                .capture(KEY)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .expect("local processing metadata must not enter source clients"),
        )
        .unwrap();
}
#[test]
fn invalid_references_and_referenced_template_edits_are_atomic() {
    let (d, s) = store();
    s.put("sources", KEY, source()).unwrap();
    let before = s.snapshot();
    let revision = s.revision();
    let disk = std::fs::read(d.path().join("config.json")).unwrap();
    for reference in [
        json!(false),
        json!(""),
        json!("missing-owned-template-secret"),
        json!("../bad"),
    ] {
        let patch = json!({"flussonix_output_template":reference});
        let error = s.put("sources", KEY, patch.clone()).unwrap_err();
        assert!(!error.contains("owned-template-secret"));
        let row = flussonix::config::merge(&before["sources"][0], &patch);
        assert!(s.validate(json!({"sources":[row.clone()]})).is_err());
        assert!(s.replace(json!({"sources":[row]})).is_err());
    }
    assert!(s.delete("templates", "processing").is_err());
    assert!(
        s.put(
            "templates",
            "processing",
            json!({"flussonix_hls_subtitles":"convert"})
        )
        .is_err()
    );
    assert_eq!(s.snapshot(), before);
    assert_eq!(s.revision(), revision);
    assert_eq!(std::fs::read(d.path().join("config.json")).unwrap(), disk);
}
#[test]
fn output_reference_is_vendor_source_only_and_not_a_group_policy() {
    let (_d, s) = store();
    for kind in ["sources", "peers", "streams", "templates", "auth_backends"] {
        let row = json!({"api_url":"http://owned-native.invalid","flussonix_output_template":"processing"});
        assert!(s.put(kind, "owned", row).is_err());
    }
    let mut row = source();
    row["group_config"] = json!({"free":{"flussonix_output_template":"processing"}});
    assert!(s.put("sources", KEY, row).is_err());
    s.put("sources", KEY, source()).unwrap();
    assert!(
        s.put("sources", KEY, json!({"flussonix_source_kind":"native"}))
            .is_err()
    );
    s.put(
        "sources",
        KEY,
        json!({"flussonix_source_kind":"native","on_play":null,"flussonix_output_template":null}),
    )
    .unwrap();
}
#[test]
fn valid_processing_edit_changes_revision_and_unused_templates_remain_independent() {
    let (_d, s) = store();
    s.put("sources", KEY, source()).unwrap();
    let revision = s.revision();
    s.put("templates", "processing", json!({"transcoder":{"ab":128}}))
        .unwrap();
    assert!(s.revision() > revision);
    s.put(
        "templates",
        "unused",
        json!({"flussonix_hls_subtitles":"convert"}),
    )
    .unwrap();
    s.delete("templates", "unused").unwrap();
    assert_eq!(s.snapshot()["templates"][0]["transcoder"]["ab"], 128);
}
#[test]
fn processing_projection_accepts_existing_cpu_gpu_and_broadcast_selector_profiles() {
    let (_d, s) = store();
    s.put("sources", KEY, source()).unwrap();
    for encoder in [
        "libx264",
        "libx265",
        "h264_nvenc",
        "hevc_nvenc",
        "h264_vaapi",
        "hevc_vaapi",
    ] {
        let mut profile = json!({"encoder":encoder});
        if !encoder.ends_with("vaapi") {
            profile["vb"] = json!(300);
        }
        s.put(
            "templates",
            "processing",
            json!({"$reset":true,"transcoder":profile,"flussonix_hls_subtitles":"drop"}),
        )
        .unwrap();
    }
    for selector in [
        json!({"channel":1}),
        json!({"service":1}),
        json!({"teletext_page":888}),
        json!({"dvb_page":1,"ocr_language":"eng"}),
        json!({"native_track":7}),
    ] {
        let row = flussonix::config::merge(&json!({"language":"en","name":"English"}), &selector);
        s.put("templates","processing",json!({"$reset":true,"flussonix_hls_subtitles":"convert","flussonix_hls_captions":[row]})).unwrap();
    }
}
#[test]
fn known_unsupported_gpu_caption_combination_rejects_atomically() {
    let (_d, s) = store();
    s.put("sources", KEY, source()).unwrap();
    let before = s.snapshot();
    let revision = s.revision();
    assert!(s.put("templates","processing",json!({"transcoder":{"encoder":"h264_nvenc","vb":300},"flussonix_hls_subtitles":"convert","flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"}]})).is_err());
    assert_eq!(s.snapshot(), before);
    assert_eq!(s.revision(), revision);
}
