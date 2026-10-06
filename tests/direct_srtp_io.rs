use flussonix::direct_rtp::{
    config::{Settings, outputs},
    crypto::Session,
    input::Input,
    output::State,
    packet,
};
use serde_json::json;
use std::{os::unix::fs::PermissionsExt, time::Duration};
use tokio::{io::AsyncReadExt, net::UdpSocket};
use tokio_util::sync::CancellationToken;
fn port() -> u16 {
    for _ in 0..64 {
        let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = a.local_addr().unwrap().port();
        if p < 65535 && std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p + 1)).is_ok() {
            return p;
        }
    }
    panic!("no pair")
}
fn ts() -> Vec<u8> {
    let mut b = vec![0xff; 188];
    b[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    b
}
fn key(path: &std::path::Path, value: u8) {
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [value; 30]);
    std::fs::write(path, encoded).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
#[tokio::test]
async fn receive_authenticates_before_peer_pin_and_rejects_plaintext_tamper_replay() {
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("key");
    key(&file, 0x33);
    let p = port();
    let cfg = Settings::parse(
        &json!({"url":format!("srtp://127.0.0.1:{p}"),"flussonix_rtp":{"key_file":file}}),
    )
    .unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let stats = input.stats.clone();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { input.run(write, c).await });
    let hostile = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut wrong = Session::new([0x34; 30], Some(99)).unwrap();
    let mut bad = packet::packet(1, 0, 99, &ts());
    wrong.protect(&mut bad, false).unwrap();
    hostile.send_to(&bad, cfg.address).await.unwrap();
    let mut malformed = packet::packet(2, 0, 99, &ts());
    malformed[1] = 96;
    Session::new([0x33; 30], Some(99))
        .unwrap()
        .protect(&mut malformed, false)
        .unwrap();
    hostile.send_to(&malformed, cfg.address).await.unwrap();
    sender
        .send_to(&packet::packet(65534, 0, 42, &ts()), cfg.address)
        .await
        .unwrap();
    let mut crypto = Session::new([0x33; 30], Some(42)).unwrap();
    let clear = packet::packet(65534, 0, 42, &ts());
    let mut valid = clear;
    crypto.protect(&mut valid, false).unwrap();
    let mut tamper = valid.clone();
    tamper[15] ^= 1;
    sender.send_to(&tamper, cfg.address).await.unwrap();
    sender.send_to(&valid, cfg.address).await.unwrap();
    sender.send_to(&valid, cfg.address).await.unwrap();
    let mut body = vec![0; 188];
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(body, ts());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(stats.snapshot()["packets"], 1);
    assert!(stats.snapshot()["auth_failures"].as_u64().unwrap() >= 4);
    let mut extra = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), read.read_exact(&mut extra))
            .await
            .is_err()
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
    assert!(UdpSocket::bind(cfg.address).await.is_ok());
    assert!(UdpSocket::bind((cfg.address.ip(), p + 1)).await.is_ok());
    let serialized = stats.snapshot().to_string();
    assert!(!serialized.contains(file.to_str().unwrap()));
    assert!(!serialized.contains(&base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        [0x33u8; 30]
    )));
}
#[tokio::test]
async fn srtcp_feedback_is_encrypted_and_plaintext_control_cannot_count() {
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("key");
    key(&file, 0x33);
    let p = port();
    let cfg = Settings::parse(
        &json!({"url":format!("srtp://127.0.0.1:{p}"),"flussonix_rtp":{"key_file":file}}),
    )
    .unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let stats = input.stats.clone();
    let (write, mut read) = tokio::io::duplex(4096);
    let c = CancellationToken::new();
    let stop = c.clone();
    let task = tokio::spawn(async move { input.run(write, stop).await });
    let source = port();
    let tx = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, source))
        .await
        .unwrap();
    let rtcp = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, source + 1))
        .await
        .unwrap();
    let mut cipher = Session::new([0x33; 30], Some(42)).unwrap();
    let mut body = packet::packet(10, 0, 42, &ts());
    cipher.protect(&mut body, false).unwrap();
    tx.send_to(&body, cfg.address).await.unwrap();
    let mut payload = [0; 188];
    read.read_exact(&mut payload).await.unwrap();
    let clear = packet::receiver_report(
        42,
        99,
        &packet::Reception {
            highest: 10,
            lost: 0,
            fraction: 0,
            jitter: 0,
            last_sr: 0,
            delay_sr: 0,
        },
    );
    rtcp.send_to(&clear, (cfg.address.ip(), p + 1))
        .await
        .unwrap();
    let mut body = clear.clone();
    cipher.protect(&mut body, true).unwrap();
    rtcp.send_to(&body, (cfg.address.ip(), p + 1))
        .await
        .unwrap();
    rtcp.send_to(&body, (cfg.address.ip(), p + 1))
        .await
        .unwrap();
    let mut feedback = [0; 2048];
    let n = tokio::time::timeout(Duration::from_secs(2), rtcp.recv(&mut feedback))
        .await
        .unwrap()
        .unwrap();
    assert!(!packet::valid_rtcp(&feedback[..n]));
    let mut receive = Session::new([0x33; 30], None).unwrap();
    let mut plain = feedback[..n].to_vec();
    receive.unprotect(&mut plain, true).unwrap();
    assert!(packet::valid_rtcp(&plain));
    assert_eq!(&plain[8..12], &42u32.to_be_bytes());
    tokio::time::timeout(Duration::from_secs(2), async {
        while stats.snapshot()["rtcp_packets"] != 1
            || stats.snapshot()["auth_failures"].as_u64().unwrap() < 2
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.snapshot()["rtcp_packets"], 1);
    assert!(stats.snapshot()["auth_failures"].as_u64().unwrap() >= 2);
    c.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn encrypted_destination_never_sends_plaintext_and_releases_local_pair() {
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("key");
    key(&file, 0x33);
    let p = port();
    let rx = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p))
        .await
        .unwrap();
    let control = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p + 1))
        .await
        .unwrap();
    let state=State::new(outputs(&json!({"flussonix_rtp_outputs":[{"url":format!("srtp://127.0.0.1:{p}"),"flussonix_rtp":{"key_file":file}}]})).unwrap().remove(0),0);
    let (tx, receiver) = tokio::sync::broadcast::channel(4);
    let c = CancellationToken::new();
    let stop = c.clone();
    let s = state.clone();
    let task = tokio::spawn(async move { s.run(receiver, stop).await });
    tx.send(bytes::Bytes::from(ts().repeat(7))).unwrap();
    let mut bytes = [0; 1600];
    let (n, source) = tokio::time::timeout(Duration::from_secs(2), rx.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 12 + 7 * 188 + 10);
    assert!(packet::parse(&bytes[..n]).is_err());
    let mut crypto = Session::new([0x33; 30], None).unwrap();
    let mut body = bytes[..n].to_vec();
    crypto.unprotect(&mut body, false).unwrap();
    assert_eq!(packet::parse(&body).unwrap().payload, ts().repeat(7));
    let report = packet::receiver_report(
        44,
        42,
        &packet::Reception {
            highest: 10,
            lost: 0,
            fraction: 0,
            jitter: 0,
            last_sr: 0,
            delay_sr: 0,
        },
    );
    control
        .send_to(&report, (source.ip(), source.port() + 1))
        .await
        .unwrap();
    let mut malformed = packet::receiver_report(
        45,
        42,
        &packet::Reception {
            highest: 10,
            lost: 0,
            fraction: 0,
            jitter: 0,
            last_sr: 0,
            delay_sr: 0,
        },
    );
    malformed.extend([0, 0, 0, 0]);
    Session::new([0x33; 30], Some(45))
        .unwrap()
        .protect(&mut malformed, true)
        .unwrap();
    control
        .send_to(&malformed, (source.ip(), source.port() + 1))
        .await
        .unwrap();
    let mut cipher = Session::new([0x33; 30], Some(44)).unwrap();
    let mut encrypted = report.clone();
    cipher.protect(&mut encrypted, true).unwrap();
    control
        .send_to(&encrypted, (source.ip(), source.port() + 1))
        .await
        .unwrap();
    control
        .send_to(&encrypted, (source.ip(), source.port() + 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(state.stats()["rtcp_packets"], 1);
    assert!(state.stats()["auth_failures"].as_u64().unwrap() >= 2);
    c.cancel();
    task.await.unwrap();
    assert!(UdpSocket::bind(source).await.is_ok());
    assert!(
        UdpSocket::bind((source.ip(), source.port() + 1))
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn key_reference_change_replaces_worker_and_reopens_input_pair() {
    let d = tempfile::tempdir().unwrap();
    let first = d.path().join("first.key");
    let second = d.path().join("second.key");
    key(&first, 0x33);
    key(&second, 0x34);
    let p = port();
    let engine = flussonix::media::Engine::new(d.path().join("media"), "ffmpeg");
    let mut cfg = json!({"inputs":[{"url":format!("srtp://127.0.0.1:{p}"),"flussonix_rtp":{"key_file":first}}]});
    let one = engine
        .ensure_guarded("owned", &cfg, true, std::future::ready(true))
        .await
        .unwrap();
    let same = engine
        .ensure_guarded("owned", &cfg, true, std::future::ready(true))
        .await
        .unwrap();
    assert_eq!(one.pid(), same.pid());
    cfg["inputs"][0]["flussonix_rtp"]["key_file"] = json!(second);
    let two = engine
        .ensure_guarded("owned", &cfg, true, std::future::ready(true))
        .await
        .unwrap();
    assert_ne!(one.pid(), two.pid());
    assert_eq!(engine.count().await, 1);
    engine.stop_all().await;
    assert!(
        UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p))
            .await
            .is_ok()
    );
    assert!(
        UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p + 1))
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn unavailable_or_unsafe_key_fails_closed_without_plaintext_or_input_socket() {
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("missing.key");
    let p = port();
    let cfg = Settings::parse(
        &json!({"url":format!("srtp://127.0.0.1:{p}"),"flussonix_rtp":{"key_file":file}}),
    )
    .unwrap();
    assert!(Input::bind(&cfg).await.is_err());
    assert!(UdpSocket::bind(cfg.address).await.is_ok());
    assert!(UdpSocket::bind((cfg.address.ip(), p + 1)).await.is_ok());
    let listener = UdpSocket::bind(cfg.address).await.unwrap();
    let state=State::new(outputs(&json!({"flussonix_rtp_outputs":[{"url":cfg.endpoint(),"flussonix_rtp":{"key_file":file}}]})).unwrap().remove(0),0);
    let (tx, rx) = tokio::sync::broadcast::channel(4);
    tx.send(bytes::Bytes::from(ts().repeat(7))).unwrap();
    state.clone().run(rx, CancellationToken::new()).await;
    assert_eq!(state.stats()["status"], "failed");
    let mut body = [0; 1600];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.recv(&mut body))
            .await
            .is_err()
    );
    assert!(!state.stats().to_string().contains(file.to_str().unwrap()));
}
