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
                json!({"inputs":[{"url":"unsupported://example.net/news"}]})
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
#[test]
fn portable_policy_rejects_malformed_native_token_guards() {
    use flussonix::playback_auth::Policy;
    for invalid in [
        json!(false),
        json!({}),
        json!("not-a-hash"),
        json!("f".repeat(63)),
    ] {
        assert!(
            Policy::from_config(&json!({"flussonix_token_sha256":invalid}), &json!({})).is_err(),
            "a discovered native guard cannot silently become unprotected"
        );
    }
}

#[test]
fn explicit_copy_encoder_overrides_template_transcoding() {
    let d = tempfile::tempdir().unwrap();
    let s = flussonix::config::ConfigStore::open(d.path().join("config.json")).unwrap();
    s.put(
        "templates",
        "cpu",
        serde_json::json!({"transcoder":{"encoder":"libx264","vb":1200}}),
    )
    .unwrap();
    s.put("streams","copy",serde_json::json!({"template":"cpu","transcoder":{"encoder":"copy"},"inputs":[{"url":"testsrc://"}]})).expect("copy must be accepted as an explicit override");
    assert_eq!(
        s.effective("copy").unwrap()["transcoder"]["encoder"],
        "copy"
    );
}

#[test]
fn native_source_transport_is_explicit_source_only_and_validated() {
    let d = tempfile::tempdir().unwrap();
    let s = flussonix::config::ConfigStore::open(d.path().join("config.json")).unwrap();
    for transport in ["hls", "m4s", "m4f", "mpegts"] {
        s.put("sources","origin",serde_json::json!({"api_url":"http://origin.example/control","private_payload_url":"https://origin.example/media","flussonix_transport":transport})).expect("source transport must save");
    }
    for transport in [serde_json::json!("rtsp"), serde_json::json!(true)] {
        assert!(s.put("sources","bad",serde_json::json!({"api_url":"http://origin.example","flussonix_transport":transport})).is_err());
    }
    assert!(
        s.put(
            "peers",
            "bad",
            serde_json::json!({"api_url":"http://origin.example","flussonix_transport":"m4s"})
        )
        .is_err()
    );
}

#[test]
fn media_stall_timeout_is_native_validated_and_inherited() {
    let d = tempfile::tempdir().unwrap();
    let c = flussonix::config::ConfigStore::open(d.path().join("config.json")).unwrap();
    c.put(
        "templates",
        "protected",
        serde_json::json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":30}),
    )
    .unwrap();
    c.put(
        "streams",
        "owned",
        serde_json::json!({"template":"protected"}),
    )
    .unwrap();
    assert_eq!(c.effective("owned").unwrap()["flussonix_input_timeout"], 30);
    for invalid in [
        serde_json::json!(0),
        serde_json::json!(301),
        serde_json::json!(1.5),
        serde_json::json!("15"),
    ] {
        assert!(
            c.put(
                "streams",
                "owned",
                serde_json::json!({"flussonix_input_timeout":invalid})
            )
            .is_err()
        );
    }
    c.put(
        "streams",
        "owned",
        serde_json::json!({"flussonix_input_timeout":1}),
    )
    .unwrap();
    assert_eq!(c.effective("owned").unwrap()["flussonix_input_timeout"], 1);
    c.put(
        "streams",
        "owned",
        serde_json::json!({"flussonix_input_timeout":null}),
    )
    .unwrap();
    assert_eq!(c.effective("owned").unwrap()["flussonix_input_timeout"], 30);
}

#[test]
fn native_content_identity_and_replica_groups_are_validated_and_inherited() {
    use serde_json::json;
    let d = tempfile::tempdir().unwrap();
    let c = flussonix::config::ConfigStore::open(d.path().join("config.json")).unwrap();
    c.put(
        "templates",
        "replica",
        json!({"inputs":[{"url":"testsrc://"}],"flussonix_content_id":"owned-news-v1"}),
    )
    .unwrap();
    c.put("streams", "news", json!({"template":"replica"}))
        .unwrap();
    assert_eq!(
        c.effective("news").unwrap()["flussonix_content_id"],
        "owned-news-v1"
    );
    for bad in [
        json!(""),
        json!("a/b"),
        json!("a b"),
        json!("x".repeat(129)),
        json!(42),
    ] {
        assert!(
            c.put("streams", "news", json!({"flussonix_content_id":bad}))
                .is_err()
        );
        assert!(
            c.put(
                "sources",
                "bad",
                json!({"api_url":"http://127.0.0.1","flussonix_source_group":bad})
            )
            .is_err()
        );
    }
    for i in 0..8 {
        c.put(
            "sources",
            &format!("origin{i}"),
            json!({"api_url":"http://127.0.0.1","flussonix_source_group":"replicas"}),
        )
        .unwrap();
    }
    assert!(
        c.put(
            "sources",
            "overflow",
            json!({"api_url":"http://127.0.0.1","flussonix_source_group":"replicas"})
        )
        .is_err()
    );
    assert!(
        c.put(
            "peers",
            "edge",
            json!({"api_url":"http://127.0.0.1","flussonix_source_group":"replicas"})
        )
        .is_err()
    );
    c.put(
        "streams",
        "news",
        json!({"flussonix_content_id":"override"}),
    )
    .unwrap();
    c.put("streams", "news", json!({"flussonix_content_id":null}))
        .unwrap();
    assert_eq!(
        c.effective("news").unwrap()["flussonix_content_id"],
        "owned-news-v1"
    );
}

#[test]
fn rtsp_udp_input_option_is_validated_inherited_and_persisted() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("c.json");
    let store = ConfigStore::open(&path).unwrap();
    store
        .put(
            "templates",
            "camera",
            json!({"inputs":[{"url":"rtsp://camera/live","rtp":"udp"}]}),
        )
        .unwrap();
    store
        .put(
            "streams",
            "owned",
            json!({"template":"camera","static":false}),
        )
        .unwrap();
    assert_eq!(store.effective("owned").unwrap()["inputs"][0]["rtp"], "udp");
    for input in [
        json!({"url":"hls://example/live","rtp":"udp"}),
        json!({"url":"rtsp://camera/live","rtp":"quic"}),
        json!({"url":"rtsp://camera/live","rtp":true}),
    ] {
        assert!(
            store
                .put("streams", "owned", json!({"inputs":[input]}))
                .is_err()
        );
    }
    drop(store);
    let store = ConfigStore::open(path).unwrap();
    assert_eq!(store.effective("owned").unwrap()["inputs"][0]["rtp"], "udp");
}

#[test]
fn rtsps_ca_input_is_persisted_inherited_and_rejects_unsafe_options() {
    let d = tempfile::tempdir().unwrap();
    let c = tls_fixture::Certificates::new();
    let path = d.path().join("c.json");
    let store = ConfigStore::open(&path).unwrap();
    store
        .put(
            "templates",
            "secure",
            json!({"inputs":[{"url":"rtsps://localhost:322/owned","flussonix_tls_ca":c.ca}]}),
        )
        .unwrap();
    store
        .put(
            "streams",
            "owned",
            json!({"template":"secure","static":false}),
        )
        .unwrap();
    let saved = store.snapshot();
    drop(store);
    let store = ConfigStore::open(&path).unwrap();
    assert_eq!(
        store.effective("owned").unwrap()["inputs"][0]["flussonix_tls_ca"],
        json!(c.ca)
    );
    for input in [
        json!({"url":"rtsps://localhost/owned","rtp":"udp"}),
        json!({"url":"rtsp://localhost/owned","flussonix_tls_ca":c.ca}),
        json!({"url":"rtsps://localhost/owned","flussonix_tls_ca":"relative.pem"}),
        json!({"url":"rtsps://localhost/owned","flussonix_tls_ca":true}),
        json!({"url":"rtsps://localhost/owned","flussonix_tls_ca":"/no-owned-ca.pem"}),
    ] {
        assert!(
            store
                .put("streams", "owned", json!({"inputs":[input]}))
                .is_err()
        );
        assert_eq!(store.snapshot(), saved);
    }
}
#[path = "support/tls.rs"]
mod tls_fixture;

#[test]
fn publication_inputs_credentials_and_callbacks_persist_and_inherit() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("c.json");
    let store = ConfigStore::open(&p).unwrap();
    store
        .put(
            "auth_backends",
            "publisher",
            json!({"url":"https://auth.example/publish"}),
        )
        .unwrap();
    store.put("templates", "published", json!({"inputs":[{"url":"publish://"}],"password":"owned-publisher","on_publish":"auth://publisher"})).expect("publication template must be supported");
    store
        .put("streams", "owned", json!({"template":"published"}))
        .unwrap();
    let effective = store.effective("owned").unwrap();
    assert_eq!(effective["inputs"][0]["url"], "publish://");
    assert_eq!(effective["password"], "owned-publisher");
    assert_eq!(effective["on_publish"], "auth://publisher");
    assert!(effective["config_on_disk"].get("password").is_none());
    drop(store);
    let store = ConfigStore::open(p).unwrap();
    assert_eq!(
        store.effective("owned").unwrap()["password"],
        "owned-publisher"
    );
    for patch in [
        json!({"inputs":[{"url":"publish://extra"}]}),
        json!({"inputs":[{"url":"publish://"},{"url":"testsrc://"}]}),
        json!({"password":12}),
        json!({"password":""}),
        json!({"on_publish":{ "url":"http://example/publish" }}),
        json!({"on_publish":"auth://missing"}),
        json!({"on_publish":"file:///publish"}),
    ] {
        let before = store.snapshot();
        assert!(store.put("streams", "invalid", patch).is_err());
        assert_eq!(store.snapshot(), before);
    }
}

#[test]
fn native_tls_ca_is_persisted_inherited_and_rejects_plain_or_invalid_trust() {
    let d = tempfile::tempdir().unwrap();
    let c = tls_fixture::Certificates::new();
    let path = d.path().join("c.json");
    let store = ConfigStore::open(&path).unwrap();
    for scheme in ["m4fs", "m4ss"] {
        store.put("templates","secure",json!({"inputs":[{"url":format!("{scheme}://localhost/owned"),"flussonix_tls_ca":c.ca}]})).unwrap();
        store
            .put(
                "streams",
                "owned",
                json!({"template":"secure","static":false}),
            )
            .unwrap();
        assert_eq!(
            store.effective("owned").unwrap()["inputs"][0]["flussonix_tls_ca"],
            json!(c.ca)
        );
    }
    let saved = store.snapshot();
    drop(store);
    let store = ConfigStore::open(&path).unwrap();
    assert_eq!(store.snapshot(), saved);
    for input in [
        json!({"url":"m4f://localhost/owned","flussonix_tls_ca":c.ca}),
        json!({"url":"m4s://localhost/owned","flussonix_tls_ca":c.ca}),
        json!({"url":"m4ss://localhost/owned","flussonix_tls_ca":"relative.pem"}),
        json!({"url":"m4fs://localhost/owned","flussonix_tls_ca":true}),
    ] {
        assert!(
            store
                .put("streams", "owned", json!({"inputs":[input]}))
                .is_err()
        );
        assert_eq!(store.snapshot(), saved);
    }
}

#[test]
fn cluster_ca_paths_persist_and_invalid_or_plaintext_trust_is_not_saved() {
    let d = tempfile::tempdir().unwrap();
    let cert = tls_fixture::Certificates::new();
    let path = d.path().join("config.json");
    let store = ConfigStore::open(&path).unwrap();
    for kind in ["sources", "peers"] {
        store
            .put(
                kind,
                "trusted",
                json!({"api_url":"https://localhost:19876", "flussonix_tls_ca":cert.ca}),
            )
            .unwrap();
    }
    store.put("sources", "trusted", json!({"private_payload_url":"https://localhost:19877", "flussonix_media_tls_ca":cert.ca})).unwrap();
    let saved = store.snapshot();
    drop(store);
    let store = ConfigStore::open(&path).unwrap();
    assert_eq!(store.snapshot(), saved);
    for (kind, patch) in [
        ("sources", json!({"api_url":"http://localhost:19876"})),
        (
            "sources",
            json!({"private_payload_url":"http://localhost:19877"}),
        ),
        ("sources", json!({"flussonix_tls_ca":"relative.pem"})),
        (
            "sources",
            json!({"flussonix_media_tls_ca":"/missing-owned-ca.pem"}),
        ),
        ("sources", json!({"flussonix_media_tls_ca":true})),
        ("peers", json!({"flussonix_media_tls_ca":cert.ca})),
        ("peers", json!({"flussonix_tls_ca":12})),
    ] {
        assert!(store.put(kind, "trusted", patch).is_err());
        assert_eq!(store.snapshot(), saved);
    }
    // Management can be encrypted while private delivery uses a configured LAN HTTP endpoint.
    store
        .put(
            "sources",
            "trusted",
            json!({"flussonix_media_tls_ca":null,"private_payload_url":"http://localhost:19877"}),
        )
        .unwrap();
}

#[test]
fn https_input_ca_settings_inherit_persist_and_reject_plain_or_invalid_trust() {
    let d = tempfile::tempdir().unwrap();
    let cert = tls_fixture::Certificates::new();
    let path = d.path().join("owned.json");
    let store = ConfigStore::open(&path).unwrap();
    for scheme in ["hlss", "tshttps", "https"] {
        store.put("templates", "secure", json!({"inputs":[{"url":format!("{scheme}://localhost:19876/transport"),"flussonix_tls_ca":cert.ca}]})).expect("secure HTTP inputs must accept explicit trust");
        store
            .put(
                "streams",
                "owned",
                json!({"template":"secure","static":false}),
            )
            .unwrap();
        assert_eq!(
            store.effective("owned").unwrap()["inputs"][0]["flussonix_tls_ca"],
            json!(cert.ca)
        );
    }
    let saved = store.snapshot();
    drop(store);
    let store = ConfigStore::open(&path).unwrap();
    assert_eq!(store.snapshot(), saved);
    for input in [
        json!({"url":"hls://localhost/owned","flussonix_tls_ca":cert.ca}),
        json!({"url":"tshttp://localhost/owned","flussonix_tls_ca":cert.ca}),
        json!({"url":"hlss://localhost/owned","flussonix_tls_ca":"relative.pem"}),
        json!({"url":"tshttps://localhost/owned","flussonix_tls_ca":false}),
        json!({"url":"hlss://localhost/owned","flussonix_tls_ca":"/no-owned-ca.pem"}),
        json!({"url":"hlss://localhost/owned#fragment","flussonix_tls_ca":cert.ca}),
        json!({"url":"tshttps://user:password@localhost/owned","flussonix_tls_ca":cert.ca}),
        json!({"url":"hlss://","flussonix_tls_ca":cert.ca}),
    ] {
        assert!(
            store
                .put("streams", "owned", json!({"inputs":[input]}))
                .is_err()
        );
        assert_eq!(store.snapshot(), saved);
    }
}

#[test]
fn cpu_codec_profiles_save_and_inherit_independently() {
    let d = tempfile::tempdir().unwrap();
    let s = ConfigStore::open(d.path().join("c.json")).unwrap();
    s.put(
        "templates",
        "codec",
        json!({"transcoder":{"encoder":"libx265","vb":900,"acodec":"mp2a","ab":192}}),
    )
    .unwrap();
    s.put(
        "streams",
        "owned",
        json!({"template":"codec","transcoder":{"encoder":"copy"}}),
    )
    .unwrap();
    assert_eq!(
        s.effective("owned").unwrap()["transcoder"],
        json!({"encoder":"copy","vb":900,"acodec":"mp2a","ab":192})
    );
    s.put(
        "streams",
        "owned",
        json!({"transcoder":{"acodec":"mp3","ab":128}}),
    )
    .unwrap();
    s.put("templates", "codec", json!({"transcoder":{"vb":1200}}))
        .unwrap();
    assert_eq!(
        s.effective("owned").unwrap()["transcoder"],
        json!({"encoder":"copy","vb":1200,"acodec":"mp3","ab":128})
    );
    s.put(
        "streams",
        "owned",
        json!({"transcoder":{"acodec":null,"ab":null}}),
    )
    .unwrap();
    assert_eq!(
        s.effective("owned").unwrap()["transcoder"]["acodec"],
        "mp2a"
    );
    drop(s);
    let s = ConfigStore::open(d.path().join("c.json")).unwrap();
    assert_eq!(
        s.effective("owned").unwrap()["transcoder"]["encoder"],
        "copy"
    );
}
#[test]
fn invalid_audio_profiles_and_merged_template_rates_are_atomic() {
    let d = tempfile::tempdir().unwrap();
    let s = ConfigStore::open(d.path().join("c.json")).unwrap();
    s.put(
        "templates",
        "codec",
        json!({"transcoder":{"encoder":"libx265","acodec":"aac","ab":100}}),
    )
    .unwrap();
    let before = s.snapshot();
    for t in [
        json!({"acodec":"mp2a","ab":100}),
        json!({"acodec":"mp3","ab":384}),
        json!({"acodec":"aac","ab":31}),
        json!({"acodec":"aac","ab":512.5}),
        json!({"acodec":"opus"}),
        json!({"encoder":"libx265","ab":"128k"}),
    ] {
        assert!(s.put("streams", "bad", json!({"transcoder":t})).is_err());
        assert_eq!(s.snapshot(), before);
    }
    assert!(
        s.put(
            "streams",
            "bad",
            json!({"template":"codec","transcoder":{"acodec":"mp2a"}})
        )
        .is_err()
    );
    assert_eq!(s.snapshot(), before);
    s.put(
        "streams",
        "owned",
        json!({"template":"codec","transcoder":{"acodec":"mp2a","ab":192}}),
    )
    .unwrap();
    s.put("streams", "owned", json!({"transcoder":{"ab":null}}))
        .expect_err("reset must validate inherited bitrate");
}

#[test]
fn legacy_template_video_default_survives_audio_only_overrides_and_future_edits() {
    let d = tempfile::tempdir().unwrap();
    let s = ConfigStore::open(d.path().join("c.json")).unwrap();
    for t in [json!({}), json!({"vb":1800})] {
        s.put("templates", "legacy", json!({"$reset":true,"transcoder":t}))
            .unwrap();
        s.put(
            "streams",
            "owned",
            json!({"$reset":true,"template":"legacy","transcoder":{"acodec":"mp3","ab":128}}),
        )
        .unwrap();
        let effective = s.effective("owned").unwrap();
        assert_eq!(effective["transcoder"]["encoder"], "libx264");
        assert!(
            effective["config_on_disk"]["transcoder"]
                .get("encoder")
                .is_none()
        );
        s.put(
            "templates",
            "legacy",
            json!({"transcoder":{"encoder":"libx265"}}),
        )
        .unwrap();
        assert_eq!(
            s.effective("owned").unwrap()["transcoder"]["encoder"],
            "libx265"
        );
    }
}
