use flussonix::{config::ConfigStore, direct_rtp::config::Settings};
use serde_json::json;
#[test]
fn elementary_configuration_requires_sdp_input_and_never_downgrades_srtp() {
    let d = tempfile::tempdir().unwrap();
    let c = ConfigStore::open(d.path().join("c.json")).unwrap();
    c.put("streams","input",json!({"inputs":[{"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary","sdp_file":"/tmp/owned.sdp"}}]})).unwrap();
    c.put("streams","output",json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary"}}]})).unwrap();
    for row in [
        json!({"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary"}}),
        json!({"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary","sdp_file":"relative.sdp"}}),
        json!({"url":"srtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary","sdp_file":"/tmp/owned.sdp","key_file":"/tmp/key"}}),
        json!({"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"sdp_file":"/tmp/owned.sdp"}}),
    ] {
        assert!(Settings::input(&row).is_err(), "{row}");
    }
    assert!(c.put("streams","bad",json!({"flussonix_rtp_outputs":[{"url":"rtp://127.0.0.1:65530","flussonix_rtp":{"profile":"elementary"}}]})).is_err());
    assert!(c.put("streams","bad",json!({"flussonix_rtp_outputs":[{"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary","sdp_file":"/tmp/input.sdp"}}]})).is_err());
}
fn settings() -> Settings {
    Settings::input(&json!({"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary","sdp_file":"/tmp/owned.sdp"}})).unwrap().unwrap()
}
fn source() -> String {
    "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=Owned fixture\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video 20000 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\nm=audio 20002 RTP/AVP 97\r\na=rtpmap:97 MPEG4-GENERIC/48000/2\r\na=fmtp:97 streamtype=5;profile-level-id=1;mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\r\n".into()
}
#[test]
fn static_sdp_accepts_supported_media_and_rejects_indirection_and_conflicting_ports() {
    use flussonix::direct_rtp::elementary::sdp::Session;
    let cfg = settings();
    let valid = source();
    assert!(Session::parse(valid.as_bytes(), &cfg).is_ok());
    for invalid in [
        valid.replace("127.0.0.1\r\nt=", "example.net\r\nt="),
        valid.replace("20002 RTP/AVP", "20001 RTP/AVP"),
        valid.replace("20002 RTP/AVP", "20000 RTP/AVP"),
        valid.replace("RTP/AVP", "RTP/SAVP"),
        valid.replace("config=1190", "config=1210"),
        valid.replace("H264/90000", "H264/48000"),
        valid.clone() + "a=control:rtsp://127.0.0.1:9/secret\r\n",
        valid.clone() + "a=crypto:1 AES_CM_128_HMAC_SHA1_80 inline:private\r\n",
        valid.clone() + "a=fmtp:97 config=1190\r\n",
        valid.replace("packetization-mode=1", "packetization-mode=2"),
    ] {
        assert!(
            Session::parse(invalid.as_bytes(), &cfg).is_err(),
            "{invalid}"
        );
    }
    assert!(Session::parse(&vec![b'x'; 16385], &cfg).is_err());
}
#[test]
fn sdp_file_checks_open_descriptor_and_normalizes_decoder_addresses() {
    use flussonix::direct_rtp::elementary::sdp::Session;
    use std::os::unix::fs::{PermissionsExt, symlink};
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("owned.sdp");
    std::fs::write(&file, source()).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    let session = Session::read(&file, &settings()).unwrap();
    assert_eq!(session.tracks.len(), 2);
    let decoder = session.decoder_sdp(&[30000, 30002]);
    assert!(decoder.contains("m=video 30000 RTP/AVP 96"));
    assert!(!decoder.contains("20000"));
    assert!(decoder.contains("c=IN IP4 127.0.0.1"));
    let link = d.path().join("link.sdp");
    symlink(&file, &link).unwrap();
    assert!(Session::read(&link, &settings()).is_err());
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(Session::read(&file, &settings()).is_err());
    assert!(Session::read(d.path(), &settings()).is_err());
    std::fs::write(&file, vec![b'x'; 16385]).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(Session::read(&file, &settings()).is_err());
}
#[test]
fn ffmpeg_aac_sdp_without_streamtype_is_normalized_after_codec_validation() {
    use flussonix::direct_rtp::elementary::sdp::Session;
    let text = source()
        .replace("streamtype=5;", "")
        .replace("config=1190", "config=119056E500")
        + "\r\n";
    let session = Session::parse(text.as_bytes(), &settings()).unwrap();
    assert!(
        session
            .decoder_sdp(&[30000, 30002])
            .contains("streamtype=5")
    );
    assert!(
        Session::parse(
            text.replace("mode=AAC-hbr", "mode=generic").as_bytes(),
            &settings()
        )
        .is_err()
    );
}
