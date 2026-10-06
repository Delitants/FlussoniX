use flussonix::direct_rtp::{
    crypto::{self, Session},
    packet,
};
use std::os::unix::fs::PermissionsExt;
fn ts() -> Vec<u8> {
    let mut b = vec![0xff; 188];
    b[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    b
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
