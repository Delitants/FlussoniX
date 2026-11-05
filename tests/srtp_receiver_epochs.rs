use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::direct_rtp::{
    config::Settings, crypto::Session, input::Input, key_ledger::Ledger, packet,
};
use serde_json::json;
use std::{fs, net::UdpSocket, os::unix::fs::PermissionsExt, path::Path, sync::Arc};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

fn pair() -> (u16, Vec<UdpSocket>) {
    for _ in 0..64 {
        let first = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        if port < 65535 {
            if let Ok(second) = UdpSocket::bind(("127.0.0.1", port + 1)) {
                return (port, vec![first, second]);
            }
        }
    }
    panic!("no isolated UDP pair available");
}
fn four_ports() -> (u16, Vec<UdpSocket>) {
    for _ in 0..128 {
        let first = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        if port > 65520 {
            continue;
        }
        let mut sockets = vec![first];
        for offset in 1..4 {
            if let Ok(socket) = UdpSocket::bind(("127.0.0.1", port + offset)) {
                sockets.push(socket);
            }
        }
        if sockets.len() == 4 {
            return (port, sockets);
        }
    }
    panic!("no isolated four-port UDP range available");
}
fn two_track_sdp(port: u16, ids: [u32; 2]) -> String {
    format!(
        "v=0\no=- 0 0 IN IP4 127.0.0.1\ns=Owned SRTP input\nc=IN IP4 127.0.0.1\nt=0 0\nm=audio {port} RTP/SAVP 97\na=rtpmap:97 MPEG4-GENERIC/48000/2\na=fmtp:97 mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\na=ssrc:{} cname:lane-a\nm=audio {} RTP/SAVP 97\na=rtpmap:97 MPEG4-GENERIC/48000/2\na=fmtp:97 mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\na=ssrc:{} cname:lane-b\n",
        ids[0],
        port + 2,
        ids[1]
    )
}
fn two_track_settings(port: u16, sdp: &Path, keys: [&Path; 2]) -> Settings {
    Settings::input(&json!({
        "url":format!("srtp://127.0.0.1:{port}"),
        "flussonix_rtp":{"profile":"elementary","sdp_file":sdp,"track_key_files":keys}
    }))
    .unwrap()
    .unwrap()
}
fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
fn key(path: &Path, value: u8) {
    fs::write(path, STANDARD.encode([value; 30])).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn settings(port: u16, path: &Path, legacy: bool) -> Settings {
    Settings::input(&json!({
        "url":format!("srtp://127.0.0.1:{port}"),
        "flussonix_rtp":{"key_file":path,"allow_legacy_reused_key":legacy,"jitter_ms":0}
    }))
    .unwrap()
    .unwrap()
}
fn ts() -> Vec<u8> {
    let mut body = vec![0xff; 188];
    body[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    body
}

#[tokio::test]
async fn mp2t_receiver_claims_before_admission_and_rejects_reused_key_after_restart() {
    let dir = private_dir();
    let history = dir.path().join("history");
    Ledger::initialize(&history).unwrap();
    let ledger = Arc::new(Ledger::new(history));
    let file = dir.path().join("key");
    key(&file, 0x31);
    let (port, reserved) = pair();
    drop(reserved);
    let cfg = settings(port, &file, false);
    assert!(
        Input::bind(&cfg).await.is_err(),
        "secure default needs a ledger"
    );
    for candidate in port..=port + 1 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }
    let first = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    let (writer, mut reader) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(first.run(writer, cancel.clone()));
    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut old = packet::packet(1, 0, 101, &ts());
    Session::new([0x31; 30], Some(101))
        .unwrap()
        .protect(&mut old, false)
        .unwrap();
    sender.send_to(&old, cfg.address).await.unwrap();
    let mut body = vec![0; 188];
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.read_exact(&mut body),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(body, ts());
    cancel.cancel();
    task.await.unwrap().unwrap();
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    for candidate in port..=port + 1 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }
    key(&file, 0x32);
    let rotated = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    let (writer, mut reader) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rotated.run(writer, cancel.clone()));
    sender.send_to(&old, cfg.address).await.unwrap();
    let mut fresh = packet::packet(1, 0, 102, &ts());
    Session::new([0x32; 30], Some(102))
        .unwrap()
        .protect(&mut fresh, false)
        .unwrap();
    sender.send_to(&fresh, cfg.address).await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.read_exact(&mut body),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(body, ts());
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn elementary_receiver_without_media_consumes_track_keys_before_retry() {
    let dir = private_dir();
    let history = dir.path().join("history");
    Ledger::initialize(&history).unwrap();
    let ledger = Arc::new(Ledger::new(history));
    let first_key = dir.path().join("video.key");
    let second_key = dir.path().join("audio.key");
    let sdp = dir.path().join("source.sdp");
    key(&first_key, 0x61);
    key(&second_key, 0x62);
    let (port, reserved) = four_ports();
    fs::write(&sdp, two_track_sdp(port, [301, 302])).unwrap();
    drop(reserved);
    let cfg = two_track_settings(port, &sdp, [&first_key, &second_key]);

    // No sender is started: binding itself must durably claim both keys.
    let first = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    drop(first);
    let refused = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .err()
        .expect("an unobserved first epoch must still block a same-key retry");
    assert_eq!(
        refused,
        "SRTP receiver key already used; rotate key and restart sender"
    );
    for candidate in port..port + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }

    key(&first_key, 0x63);
    key(&second_key, 0x64);
    let rotated = Input::bind_with_ledger(&cfg, Some(ledger)).await.unwrap();
    drop(rotated);
}

#[tokio::test]
async fn missing_history_or_occupied_control_port_does_not_burn_new_key() {
    let dir = private_dir();
    let history = dir.path().join("history");
    let ledger = Arc::new(Ledger::new(history.clone()));
    let file = dir.path().join("key");
    key(&file, 0x34);
    let (port, reserved) = pair();
    drop(reserved);
    let cfg = settings(port, &file, false);
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    Ledger::initialize(&history).unwrap();
    let occupied = UdpSocket::bind(("127.0.0.1", port + 1)).unwrap();
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    drop(occupied);
    let first = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    drop(first);
    assert!(Input::bind_with_ledger(&cfg, Some(ledger)).await.is_err());
}

#[tokio::test]
async fn legacy_override_allows_static_key_only_for_its_input() {
    let dir = private_dir();
    let file = dir.path().join("key");
    key(&file, 0x35);
    let (port, reserved) = pair();
    drop(reserved);
    let cfg = settings(port, &file, true);
    let first = Input::bind(&cfg).await.unwrap();
    drop(first);
    let second = Input::bind(&cfg).await.unwrap();
    drop(second);
}

#[tokio::test]
async fn elementary_two_track_claim_is_atomic_and_a_later_occupied_port_does_not_burn_keys() {
    let dir = private_dir();
    let history = dir.path().join("history");
    Ledger::initialize(&history).unwrap();
    let ledger = Arc::new(Ledger::new(history));
    let a = dir.path().join("a.key");
    let b = dir.path().join("b.key");
    let sdp = dir.path().join("source.sdp");
    key(&a, 0x41);
    key(&b, 0x42);
    let (port, reserved) = four_ports();
    fs::write(&sdp, two_track_sdp(port, [101, 202])).unwrap();
    drop(reserved);
    let cfg = two_track_settings(port, &sdp, [&a, &b]);

    // The final public RTCP port must be checked before either fingerprint is
    // consumed. Once released, exactly the same key pair can start.
    let occupied = UdpSocket::bind(("127.0.0.1", port + 3)).unwrap();
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    drop(occupied);
    let first = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    drop(first);
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    for candidate in port..port + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }

    // B has already been claimed by the first generation. A fresh A with
    // stale B must fail as a batch; a second try with fresh B must still use A.
    key(&a, 0x43);
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    for candidate in port..port + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }
    key(&b, 0x44);
    let rotated = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    drop(rotated);
}

#[tokio::test]
async fn shared_key_sdp_identity_mismatch_does_not_claim_or_open_lanes() {
    let dir = private_dir();
    let history = dir.path().join("history");
    Ledger::initialize(&history).unwrap();
    let ledger = Arc::new(Ledger::new(history));
    let file = dir.path().join("shared.key");
    let sdp = dir.path().join("source.sdp");
    key(&file, 0x51);
    let (port, reserved) = four_ports();
    drop(reserved);
    let cfg = Settings::input(&json!({
        "url":format!("srtp://127.0.0.1:{port}"),
        "flussonix_rtp":{"profile":"elementary","sdp_file":sdp,"key_file":file,"expected_ssrcs":[101,202]}
    }))
    .unwrap()
    .unwrap();
    fs::write(&sdp, two_track_sdp(port, [101, 303])).unwrap();
    assert!(
        Input::bind_with_ledger(&cfg, Some(ledger.clone()))
            .await
            .is_err()
    );
    for candidate in port..port + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }
    fs::write(&sdp, two_track_sdp(port, [101, 202])).unwrap();
    let first = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    drop(first);
    assert!(Input::bind_with_ledger(&cfg, Some(ledger)).await.is_err());
}
