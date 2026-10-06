use flussonix::config::ConfigStore;
use serde_json::json;
#[test]
fn vaapi_modes_and_template_switches_are_safe() {
    let d = tempfile::tempdir().unwrap();
    let c = ConfigStore::open(d.path().join("config.json")).unwrap();
    c.put("templates","gpu",json!({"transcoder":{"encoder":"h264_vaapi","vaapi_device":"/dev/dri/renderD128","low_power":true,"vaapi_rc":"cqp","qp":26}})).unwrap();
    c.put(
        "streams",
        "owned",
        json!({"$reset":true,"template":"gpu","inputs":[{"url":"testsrc://"}],"transcoder":{"qp":28}}),
    )
    .unwrap();
    assert_eq!(c.effective("owned").unwrap()["transcoder"]["qp"], 28);
    c.put("streams","owned",json!({"$reset":true,"template":"gpu","inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":1200}})).unwrap();
    let t = c.effective("owned").unwrap()["transcoder"].clone();
    for key in ["vaapi_device", "low_power", "vaapi_rc", "qp"] {
        assert!(t.get(key).is_none(), "{t}");
    }
    c.put("streams","owned",json!({"$reset":true,"template":"gpu","inputs":[{"url":"testsrc://"}],"transcoder":{"vaapi_rc":"cbr","vb":1000}})).unwrap();
    assert!(
        c.effective("owned").unwrap()["transcoder"]
            .get("qp")
            .is_none()
    );
}
#[test]
fn invalid_vaapi_settings_fail_before_worker_start() {
    let d = tempfile::tempdir().unwrap();
    let c = ConfigStore::open(d.path().join("config.json")).unwrap();
    for t in [
        json!({"encoder":"h264_vaapi","vaapi_device":"/tmp/private"}),
        json!({"encoder":"h264_vaapi","vaapi_device":"/dev/dri/renderD128,foo"}),
        json!({"encoder":"h264_vaapi","qp":52}),
        json!({"encoder":"h264_vaapi","vb":900}),
        json!({"encoder":"h264_vaapi","vaapi_rc":"cbr","qp":24}),
        json!({"encoder":"libx264","vaapi_device":"/dev/dri/renderD128"}),
        json!({"encoder":"h264_vaapi","low_power":1}),
    ] {
        assert!(
            c.put(
                "streams",
                "bad",
                json!({"$reset":true,"inputs":[{"url":"testsrc://"}],"transcoder":t})
            )
            .is_err(),
            "{t}"
        );
    }
    for t in [
        json!({"encoder":"h264_vaapi"}),
        json!({"encoder":"hevc_vaapi","vaapi_rc":"cbr","vb":1500}),
        json!({"encoder":"h264_vaapi","qp":0,"low_power":false}),
    ] {
        c.put(
            "streams",
            "good",
            json!({"$reset":true,"inputs":[{"url":"testsrc://"}],"transcoder":t}),
        )
        .unwrap();
    }
}
#[test]
fn unrelated_streams_without_transcoding_retain_absent_profile() {
    let d = tempfile::tempdir().unwrap();
    let c = ConfigStore::open(d.path().join("c.json")).unwrap();
    c.put(
        "streams",
        "plain",
        json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
    )
    .unwrap();
    assert!(c.effective("plain").unwrap().get("transcoder").is_none());
}
