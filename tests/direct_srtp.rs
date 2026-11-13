use flussonix::direct_rtp::{
    config::Settings,
    crypto::{self, Session},
    packet,
};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
fn ts() -> Vec<u8> {
    let mut b = vec![0xff; 188];
    b[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    b
}
fn owned_key(dir: &std::path::Path, name: &str, value: u8) -> std::path::PathBuf {
    let path = dir.join(name);
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [value; 30]);
    std::fs::write(&path, encoded).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}
fn elementary_settings(protection: serde_json::Value) -> Settings {
    let mut options = protection;
    options["profile"] = json!("elementary");
    options["sdp_file"] = json!("/tmp/owned.sdp");
    Settings::input(&json!({
        "url":"srtp://127.0.0.1:20000",
        "flussonix_rtp":options
    }))
    .unwrap()
    .unwrap()
}
#[test]
fn shared_key_receive_lanes_pin_expected_ssrc_before_first_media_or_control() {
    let dir = tempfile::tempdir().unwrap();
    let key = owned_key(dir.path(), "shared", 0x31);
    let settings = elementary_settings(json!({"key_file":key,"expected_ssrcs":[101,202]}));
    let mut lanes = crypto::receive_track_sessions(&settings, &[Some(101), Some(202)]).unwrap();
    assert_eq!(lanes.sessions.len(), 2);
    assert_eq!(lanes.fingerprints.len(), 1);
    let mut a_tx = Session::new([0x31; 30], Some(101)).unwrap();
    let mut b_tx = Session::new([0x31; 30], Some(202)).unwrap();
    let mut a = packet::packet(1, 0, 101, &ts());
    let mut b = packet::packet(1, 0, 202, &ts());
    a_tx.protect(&mut a, false).unwrap();
    b_tx.protect(&mut b, false).unwrap();
    assert!(
        lanes.sessions[1]
            .0
            .unprotect(&mut a.clone(), false)
            .is_err()
    );
    let mut a_valid = a.clone();
    lanes.sessions[0].0.unprotect(&mut a_valid, false).unwrap();
    assert_eq!(a_valid, packet::packet(1, 0, 101, &ts()));
    lanes.sessions[1].0.unprotect(&mut b, false).unwrap();
    let mut report = packet::receiver_report(101, 101, &packet::Reception::default());
    a_tx.protect(&mut report, true).unwrap();
    assert!(
        lanes.sessions[1]
            .0
            .unprotect(&mut report.clone(), true)
            .is_err()
    );
    lanes.sessions[0].0.unprotect(&mut report, true).unwrap();
    let mut feedback = packet::receiver_report(303, 101, &packet::Reception::default());
    lanes.sessions[0].1.protect(&mut feedback, true).unwrap();
    Session::new([0x31; 30], None)
        .unwrap()
        .unprotect(&mut feedback, true)
        .unwrap();
}
#[test]
fn distinct_track_key_snapshots_reject_cross_lane_ciphertext_and_duplicate_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let a_path = owned_key(dir.path(), "track-a", 0x31);
    let b_path = owned_key(dir.path(), "track-b", 0x32);
    let settings = elementary_settings(json!({"track_key_files":[a_path,b_path]}));
    let mut lanes = crypto::receive_track_sessions(&settings, &[None, None]).unwrap();
    assert_eq!(lanes.fingerprints.len(), 2);
    assert_ne!(lanes.fingerprints[0], lanes.fingerprints[1]);
    let mut a_tx = Session::new([0x31; 30], Some(101)).unwrap();
    let mut b_tx = Session::new([0x32; 30], Some(202)).unwrap();
    let mut a = packet::packet(1, 0, 101, &ts());
    let mut b = packet::packet(1, 0, 202, &ts());
    a_tx.protect(&mut a, false).unwrap();
    b_tx.protect(&mut b, false).unwrap();
    assert!(
        lanes.sessions[1]
            .0
            .unprotect(&mut a.clone(), false)
            .is_err()
    );
    assert!(
        lanes.sessions[0]
            .0
            .unprotect(&mut b.clone(), false)
            .is_err()
    );
    lanes.sessions[0].0.unprotect(&mut a, false).unwrap();
    lanes.sessions[1].0.unprotect(&mut b, false).unwrap();
    let duplicate = owned_key(dir.path(), "duplicate", 0x31);
    let settings =
        elementary_settings(json!({"track_key_files":[dir.path().join("track-a"),duplicate]}));
    assert!(crypto::receive_track_sessions(&settings, &[None, None]).is_err());
}
#[test]
fn receive_track_sessions_fail_closed_for_count_or_key_errors_without_secret_paths() {
    let dir = tempfile::tempdir().unwrap();
    let key = owned_key(dir.path(), "single", 0x31);
    let shared = elementary_settings(json!({"key_file":key,"expected_ssrcs":[101,202]}));
    assert!(crypto::receive_track_sessions(&shared, &[Some(101)]).is_err());
    assert!(crypto::receive_track_sessions(&shared, &[Some(101), Some(203)]).is_err());
    let single = elementary_settings(json!({"key_file":key}));
    assert_eq!(
        crypto::receive_track_sessions(&single, &[None])
            .unwrap()
            .sessions
            .len(),
        1
    );
    assert!(crypto::receive_track_sessions(&single, &[None, None]).is_err());
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
    let error = crypto::receive_track_sessions(&single, &[None])
        .err()
        .unwrap();
    assert!(!error.contains(key.to_str().unwrap()));
    assert!(!error.contains("MTExMTEx"));
    let missing = elementary_settings(
        json!({"track_key_files":[dir.path().join("none"),dir.path().join("other")]}),
    );
    assert!(crypto::receive_track_sessions(&missing, &[None, None]).is_err());
}
#[test]
fn confidential_authenticated_rtp_replay_tamper_plaintext_and_wrap() {
    assert!(crypto::availability());
    let key = [0x71; 30];
    let mut tx = Session::new(key, Some(42)).unwrap();
    let mut rx = Session::new(key, None).unwrap();
    let mut wrong = Session::new([0x72; 30], None).unwrap();
    for sequence in [65534, 65535, 0, 1] {
        let clear = packet::packet(sequence, 123, 42, &ts());
        let mut cipher = clear.clone();
        tx.protect(&mut cipher, false).unwrap();
        assert_eq!(cipher.len(), clear.len() + 10);
        assert_ne!(&cipher[12..clear.len()], &clear[12..]);
        assert!(wrong.unprotect(&mut cipher.clone(), false).is_err());
        let mut corrupt = cipher.clone();
        corrupt[30] ^= 1;
        assert!(rx.unprotect(&mut corrupt, false).is_err());
        assert!(rx.unprotect(&mut clear.clone(), false).is_err());
        if sequence == 0 {
            assert!(
                Session::new(key, None)
                    .unwrap()
                    .unprotect(&mut cipher.clone(), false)
                    .is_err(),
                "a fresh receiver does not know an existing nonzero ROC"
            );
        }
        let mut valid = cipher.clone();
        rx.unprotect(&mut valid, false).unwrap();
        assert_eq!(valid, clear);
        assert!(rx.unprotect(&mut cipher, false).is_err());
        assert!(tx.protect(&mut clear.clone(), false).is_err());
    }
    let mut foreign = Session::new(key, Some(43)).unwrap();
    let mut cipher = packet::packet(2, 0, 43, &ts());
    foreign.protect(&mut cipher, false).unwrap();
    assert!(rx.unprotect(&mut cipher, false).is_err());
}
#[test]
fn srtcp_has_independent_replay_state_and_confidential_reports() {
    let key = [0x55; 30];
    let mut tx = Session::new(key, Some(42)).unwrap();
    let mut rx = Session::new(key, None).unwrap();
    let clear = packet::receiver_report(
        42,
        99,
        &packet::Reception {
            highest: 123,
            lost: 2,
            fraction: 4,
            jitter: 3,
            last_sr: 0,
            delay_sr: 0,
        },
    );
    let mut cipher = clear.clone();
    tx.protect(&mut cipher, true).unwrap();
    assert_eq!(cipher.len(), clear.len() + 14);
    assert_ne!(&clear[8..], &cipher[8..clear.len()]);
    let mut valid = cipher.clone();
    rx.unprotect(&mut valid, true).unwrap();
    assert_eq!(valid, clear);
    assert!(rx.unprotect(&mut cipher, true).is_err());
    assert!(rx.unprotect(&mut clear.clone(), true).is_err());
    let mut next = clear.clone();
    tx.protect(&mut next, true).unwrap();
    rx.unprotect(&mut next, true).unwrap();
}
#[test]
fn key_file_is_bounded_owner_only_regular_absolute_and_not_a_symlink() {
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("key");
    let expected = [0x33; 30];
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, expected);
    std::fs::write(&file, &encoded).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(crypto::read_key(&file).unwrap(), expected);
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(crypto::read_key(&file).is_err());
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let link = d.path().join("link");
    std::os::unix::fs::symlink(&file, &link).unwrap();
    assert!(crypto::read_key(&link).is_err());
    assert!(crypto::read_key(d.path()).is_err());
    assert!(crypto::read_key(std::path::Path::new("relative.key")).is_err());
    for malformed in [
        "bad".to_owned(),
        "A".repeat(129),
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 29]),
    ] {
        std::fs::write(&file, malformed).unwrap();
        let err = crypto::read_key(&file).unwrap_err();
        assert!(!err.contains("key".repeat(10).as_str()));
        assert!(!err.contains(file.to_str().unwrap()));
    }
    let fifo = d.path().join("fifo");
    let path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    assert!(crypto::read_key(&fifo).is_err());
}
