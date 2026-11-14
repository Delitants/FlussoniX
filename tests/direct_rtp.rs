use flussonix::config::ConfigStore;
use serde_json::json;
#[test]
fn receiver_key_reuse_override_is_explicit_secure_input_only() {
    use flussonix::direct_rtp::config::{Settings, outputs};
    let base =
        json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{"key_file":"/tmp/owned.key"}});
    assert!(
        !Settings::input(&base)
            .unwrap()
            .unwrap()
            .allow_legacy_reused_key
    );
    let mut legacy = base.clone();
    legacy["flussonix_rtp"]["allow_legacy_reused_key"] = json!(true);
    assert!(
        Settings::input(&legacy)
            .unwrap()
            .unwrap()
            .allow_legacy_reused_key
    );
    for invalid in [
        json!({"url":"rtp://127.0.0.1:40000","flussonix_rtp":{"allow_legacy_reused_key":true}}),
        json!({"url":"rtp://127.0.0.1:40000","flussonix_rtp":{"allow_legacy_reused_key":false}}),
        json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{"key_file":"/tmp/owned.key","allow_legacy_reused_key":"true"}}),
    ] {
        assert!(Settings::input(&invalid).is_err(), "accepted {invalid}");
    }
    assert!(outputs(&json!({"flussonix_rtp_outputs":[legacy]})).is_err());
    assert!(
        outputs(&json!({"flussonix_rtp_outputs":[{
            "url":"srtp://127.0.0.1:40000",
            "flussonix_rtp":{"key_file":"/tmp/owned.key","allow_legacy_reused_key":false}
        }]}))
        .is_err()
    );
}
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
        "rtp://127.0.0.1:39000/a/../",
        "rtp://127.0.0.1:39000\n",
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
#[test]
fn input_interface_must_match_bind_address_or_restrict_wildcard() {
    use flussonix::direct_rtp::config::Settings;
    assert!(
        Settings::input(
            &json!({"url":"rtp://127.0.0.1:39000","flussonix_rtp":{"interface":"127.0.0.2"}})
        )
        .is_err()
    );
    for interface in ["0.0.0.0", "239.1.2.3", "255.255.255.255"] {
        assert!(
            Settings::parse(
                &json!({"url":"rtp://239.1.2.3:39000","flussonix_rtp":{"interface":interface}})
            )
            .is_err()
        );
    }
}

#[test]
fn receive_buffer_request_persists_for_input_and_rejects_output_or_invalid_size() {
    let owned = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(owned.path().join("config.json")).unwrap();
    store
        .put(
            "templates",
            "buffered",
            json!({"inputs":[{"url":"rtp://127.0.0.1:39002","flussonix_rtp":{"receive_buffer_bytes":1048576}}]}),
        )
        .unwrap();
    store
        .put("streams", "inherited", json!({"template":"buffered"}))
        .unwrap();
    assert_eq!(
        store.effective("inherited").unwrap()["inputs"][0]["flussonix_rtp"]["receive_buffer_bytes"],
        1048576
    );
    let before = store.snapshot();
    for size in [
        json!(0),
        json!(65535),
        json!(33554433),
        json!(-1),
        json!("1048576"),
    ] {
        assert!(
            store
                .put(
                    "streams",
                    "invalid",
                    json!({"inputs":[{"url":"rtp://127.0.0.1:39002","flussonix_rtp":{"receive_buffer_bytes":size}}]}),
                )
                .is_err(),
            "accepted invalid receive buffer {size}"
        );
    }
    assert!(
        store
            .put(
                "streams",
                "invalid",
                json!({"flussonix_rtp_outputs":[{"url":"rtp://127.0.0.1:39002","flussonix_rtp":{"receive_buffer_bytes":1048576}}]}),
            )
            .is_err()
    );
    assert_eq!(store.snapshot(), before);
}

#[test]
fn elementary_srtp_accepts_explicit_receive_track_protection_modes() {
    use flussonix::direct_rtp::config::Settings;

    let shared = json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{
        "profile":"elementary","sdp_file":"/tmp/owned.sdp",
        "key_file":"/tmp/shared.key","expected_ssrcs":[101,202]
    }});
    let separate = json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{
        "profile":"elementary","sdp_file":"/tmp/owned.sdp",
        "track_key_files":["/tmp/video.key","/tmp/audio.key"]
    }});
    assert!(
        Settings::input(&shared).is_ok(),
        "shared key plus bound SSRCs"
    );
    assert!(
        Settings::input(&separate).is_ok(),
        "one distinct key path per track"
    );
    assert!(
        Settings::input(&json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{
            "profile":"elementary","sdp_file":"/tmp/owned.sdp","key_file":"/tmp/one.key"
        }}))
        .is_ok(),
        "single-track compatibility remains available until SDP count is known"
    );
}

#[test]
fn elementary_srtp_rejects_conflicting_or_unbounded_receive_bindings() {
    use flussonix::direct_rtp::config::Settings;

    let base = json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{
        "profile":"elementary","sdp_file":"/tmp/owned.sdp","key_file":"/tmp/shared.key"
    }});
    let invalid = [
        json!({"expected_ssrcs":[]}),
        json!({"expected_ssrcs":[0]}),
        json!({"expected_ssrcs":[101,101]}),
        json!({"expected_ssrcs":[-1]}),
        json!({"expected_ssrcs":[4294967296u64]}),
        json!({"expected_ssrcs":["101"]}),
        json!({"expected_ssrcs":[1,2,3,4,5,6,7,8,9]}),
        json!({"track_key_files":["/tmp/video.key"]}),
        json!({"track_key_files":[]}),
    ];
    for fields in invalid {
        let mut row = base.clone();
        for (key, value) in fields.as_object().unwrap() {
            row["flussonix_rtp"][key] = value.clone();
        }
        assert!(Settings::input(&row).is_err(), "accepted {row}");
    }
    for keys in [
        json!(["relative.key"]),
        json!(["/tmp/a.key", "relative.key"]),
        json!(["/tmp/a.key", "/tmp/a.key"]),
        json!(["/tmp/a.key\n"]),
        json!([1]),
        json!(vec!["/tmp/a.key"; 9]),
    ] {
        let row = json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{
            "profile":"elementary","sdp_file":"/tmp/owned.sdp","track_key_files":keys
        }});
        assert!(Settings::input(&row).is_err(), "accepted {row}");
    }
    for url in ["rtp://127.0.0.1:40000", "srtp://127.0.0.1:40000"] {
        let row = json!({"url":url,"flussonix_rtp":{
            "profile":"mp2t","key_file":"/tmp/shared.key","expected_ssrcs":[101]
        }});
        assert!(Settings::input(&row).is_err(), "accepted {row}");
    }
}

#[test]
fn elementary_srtp_output_track_keys_require_exact_typed_mapping() {
    use flussonix::direct_rtp::config::{Settings, outputs};

    let entries = json!([
        {"track_id":7,"media":"video","encoding":"H264/90000","key_file":"/tmp/video.key"},
        {"track_id":9,"media":"audio","encoding":"MPEG4-GENERIC/48000/2","key_file":"/tmp/audio.key"}
    ]);
    let row = json!({"url":"srtp://127.0.0.1:40000","flussonix_rtp":{
        "profile":"elementary","track_keys":entries
    }});
    assert!(outputs(&json!({"flussonix_rtp_outputs":[row.clone()]})).is_ok());
    let mut input_row = row.clone();
    input_row["flussonix_rtp"]["sdp_file"] = json!("/tmp/owned.sdp");
    assert!(
        Settings::input(&input_row).is_err(),
        "output mapping must not be accepted as input"
    );
    let mut bad = row.clone();
    for replacement in [
        json!([]),
        json!([{"track_id":0,"media":"video","encoding":"H264/90000","key_file":"/tmp/v.key"}]),
        json!([{"track_id":7,"media":"video","encoding":"H264/90000","key_file":"/tmp/v.key"},{"track_id":7,"media":"audio","encoding":"MPA/90000","key_file":"/tmp/a.key"}]),
        json!([{"track_id":7,"media":"text","encoding":"H264/90000","key_file":"/tmp/v.key"}]),
        json!([{"track_id":7,"media":"video","encoding":"H264/90000","key_file":"relative.key"}]),
        json!([{"track_id":7,"media":"video","encoding":"H264/90000","key_file":"/tmp/v.key","unknown":true}]),
        json!("not-an-array"),
    ] {
        bad["flussonix_rtp"]["track_keys"] = replacement;
        assert!(
            outputs(&json!({"flussonix_rtp_outputs":[bad.clone()]})).is_err(),
            "accepted {bad}"
        );
    }
    for url in ["rtp://127.0.0.1:40000", "srtp://127.0.0.1:40000"] {
        let row = json!({"url":url,"flussonix_rtp":{"profile":"mp2t","track_keys":entries}});
        assert!(outputs(&json!({"flussonix_rtp_outputs":[row]})).is_err());
    }
}
