use flussonix::direct_rtp::{
    config::Settings,
    crypto,
    elementary::{input::Input, sdp::Session},
    key_ledger::Ledger,
    packet,
};
use serde_json::json;
use std::{
    net::UdpSocket,
    os::unix::fs::PermissionsExt,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::UdpSocket as AsyncUdp};
use tokio_util::sync::CancellationToken;
fn ports(count: u16) -> (u16, Vec<UdpSocket>) {
    for _ in 0..64 {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = s.local_addr().unwrap().port();
        if p > 65520 {
            continue;
        }
        let mut sockets = vec![s];
        for n in p + 1..p + count {
            if let Ok(s) = UdpSocket::bind(("127.0.0.1", n)) {
                sockets.push(s)
            } else {
                break;
            }
        }
        if sockets.len() == usize::from(count) {
            return (p, sockets);
        }
    }
    panic!("owned ports unavailable")
}
fn description(port: u16, identities: [u32; 2]) -> String {
    format!(
        "v=0\no=- 0 0 IN IP4 127.0.0.1\ns=Owned secure input\nc=IN IP4 127.0.0.1\nt=0 0\nm=audio {port} RTP/SAVP 97\na=rtpmap:97 MPEG4-GENERIC/48000/2\na=fmtp:97 mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\na=ssrc:{} cname:fx-audio-a\nm=audio {} RTP/SAVP 97\na=rtpmap:97 MPEG4-GENERIC/48000/2\na=fmtp:97 mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\na=ssrc:{} cname:fx-audio-b\n",
        identities[0],
        port + 2,
        identities[1]
    )
}
fn config(
    port: u16,
    sdp: &std::path::Path,
    key: &std::path::Path,
    identities: [u32; 2],
) -> Settings {
    Settings::input(&json!({"url":format!("srtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary","sdp_file":sdp,"key_file":key,"expected_ssrcs":identities,"allow_legacy_reused_key":true,"jitter_ms":0,"source_ip":"127.0.0.1"}})).expect("secure elementary configuration accepted").unwrap()
}
fn key_file(dir: &std::path::Path) -> std::path::PathBuf {
    use base64::Engine;
    let p = dir.join("owned.key");
    std::fs::write(
        &p,
        base64::engine::general_purpose::STANDARD.encode([0x31; 30]),
    )
    .unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    p
}
fn media(seq: u16, ssrc: u32, size: usize) -> Vec<u8> {
    let mut payload = vec![42; size + 4];
    payload[..2].copy_from_slice(&16u16.to_be_bytes());
    payload[2..4].copy_from_slice(&((size as u16) << 3).to_be_bytes());
    let mut b = packet::packet(seq, 0, ssrc, &payload);
    b[1] = 97;
    b
}
fn sr(ssrc: u32) -> Vec<u8> {
    let mut b = vec![0x80, 200, 0, 6];
    for n in [ssrc, 0, 0, 0, 0, 0] {
        b.extend(n.to_be_bytes());
    }
    b.extend(packet::sdes(ssrc));
    b
}
#[test]
fn secure_sdp_requires_matching_transport_and_rejects_inline_keys() {
    let d = tempfile::tempdir().unwrap();
    let k = key_file(d.path());
    let s = d.path().join("owned.sdp");
    let cfg = config(40000, &s, &k, [100, 101]);
    let text = description(40000, [100, 101]);
    assert!(Session::parse(text.as_bytes(), &cfg).is_ok());
    assert!(Session::parse(description(40000, [100, 102]).as_bytes(), &cfg).is_err());
    assert!(Session::parse(text.replace("RTP/SAVP", "RTP/AVP").as_bytes(), &cfg).is_err());
    assert!(Session::parse(format!("{text}a=crypto:1 AES_CM_128_HMAC_SHA1_80 inline:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n").as_bytes(),&cfg).is_err());
    let plain=Settings::input(&json!({"url":"rtp://127.0.0.1:40000","flussonix_rtp":{"profile":"elementary","sdp_file":s}})).unwrap().unwrap();
    assert!(Session::parse(text.as_bytes(), &plain).is_err());
}
#[tokio::test]
async fn unsafe_key_fails_before_any_elementary_public_socket_opens() {
    let d = tempfile::tempdir().unwrap();
    let k = key_file(d.path());
    std::fs::set_permissions(&k, std::fs::Permissions::from_mode(0o644)).unwrap();
    let (p, res) = ports(4);
    let s = d.path().join("owned.sdp");
    std::fs::write(&s, description(p, [100, 101])).unwrap();
    drop(res);
    let c = config(p, &s, &k, [100, 101]);
    assert!(Input::bind(&c).await.is_err());
    for n in p..p + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}
#[tokio::test]
async fn separate_track_key_errors_leave_every_public_port_unbound() {
    use base64::Engine as _;
    let d = tempfile::tempdir().unwrap();
    let a = key_file(d.path());
    let b = d.path().join("track-b.key");
    let (port, reserved) = ports(4);
    let sdp = d.path().join("owned.sdp");
    std::fs::write(&sdp, description(port, [101, 202])).unwrap();
    drop(reserved);
    for bad_second_key in [None, Some(0x31), Some(0x32)] {
        match bad_second_key {
            None => {
                let _ = std::fs::remove_file(&b);
            }
            Some(value) => {
                std::fs::write(
                    &b,
                    base64::engine::general_purpose::STANDARD.encode([value; 30]),
                )
                .unwrap();
                std::fs::set_permissions(
                    &b,
                    std::fs::Permissions::from_mode(if value == 0x32 { 0o644 } else { 0o600 }),
                )
                .unwrap();
            }
        }
        let settings = Settings::input(&json!({
            "url":format!("srtp://127.0.0.1:{port}"),
            "flussonix_rtp":{"profile":"elementary","sdp_file":sdp,"track_key_files":[a,b],"allow_legacy_reused_key":true}
        }))
        .unwrap()
        .unwrap();
        let error = Input::bind(&settings).await.err().unwrap();
        assert!(!error.contains(a.to_str().unwrap()));
        assert!(!error.contains(b.to_str().unwrap()));
        for candidate in port..port + 4 {
            assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
        }
    }
}

#[tokio::test]
async fn distinct_keys_reject_captured_cross_lane_packet_before_first_media() {
    use base64::Engine as _;
    let d = tempfile::tempdir().unwrap();
    std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let ledger_path = d.path().join("receiver-epochs");
    Ledger::initialize(&ledger_path).unwrap();
    let ledger = Arc::new(Ledger::new(ledger_path));
    let a = d.path().join("lane-a.key");
    let b = d.path().join("lane-b.key");
    for (path, value) in [(&a, 0x41), (&b, 0x42)] {
        std::fs::write(
            path,
            base64::engine::general_purpose::STANDARD.encode([value; 30]),
        )
        .unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let (port, reserved) = ports(4);
    let sdp = d.path().join("source.sdp");
    std::fs::write(&sdp, description(port, [101, 202])).unwrap();
    drop(reserved);
    let cfg = Settings::input(&json!({
        "url":format!("srtp://127.0.0.1:{port}"),
        "flussonix_rtp":{"profile":"elementary","sdp_file":sdp,"track_key_files":[a,b],"jitter_ms":0}
    }))
    .unwrap()
    .unwrap();
    let input = Input::bind_with_ledger(&cfg, Some(ledger.clone()))
        .await
        .unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let task = tokio::spawn(input.run(writer, cancel.clone()));
    let mut text = Vec::new();
    reader.read_to_end(&mut text).await.unwrap();
    let private_ports: Vec<u16> = String::from_utf8(text)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("m=audio "))
        .map(|line| line.split_whitespace().nth(1).unwrap().parse().unwrap())
        .collect();
    assert_eq!(private_ports.len(), 2);
    let (_, sender_pair) = ports(2);
    let mut sender_pair = sender_pair.into_iter().map(|socket| {
        socket.set_nonblocking(true).unwrap();
        AsyncUdp::from_std(socket).unwrap()
    });
    let sender = sender_pair.next().unwrap();
    let sender_rtcp = sender_pair.next().unwrap();
    let mut a_tx = crypto::Session::new([0x41; 30], Some(101)).unwrap();
    let mut b_tx = crypto::Session::new([0x42; 30], Some(202)).unwrap();
    let a_plain = media(1, 101, 4);
    let b_plain = media(1, 202, 4);
    let mut a_cipher = a_plain.clone();
    a_tx.protect(&mut a_cipher, false).unwrap();
    sender
        .send_to(&a_cipher, ("127.0.0.1", port))
        .await
        .unwrap();
    sender
        .send_to(&a_cipher, ("127.0.0.1", port + 2))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.auth_failed.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.packets.load(Ordering::Relaxed), 1);
    let mut b_cipher = b_plain.clone();
    b_tx.protect(&mut b_cipher, false).unwrap();
    sender
        .send_to(&b_cipher, ("127.0.0.1", port + 2))
        .await
        .unwrap();
    for (id, tx, offset) in [(101, &mut a_tx, 1), (202, &mut b_tx, 3)] {
        let mut report = sr(id);
        tx.protect(&mut report, true).unwrap();
        sender_rtcp
            .send_to(&report, ("127.0.0.1", port + offset))
            .await
            .unwrap();
    }
    let mut private_rtp = Vec::new();
    let mut private_rtcp = Vec::new();
    for private_port in private_ports {
        private_rtp.push(AsyncUdp::bind(("127.0.0.1", private_port)).await.unwrap());
        private_rtcp.push(
            AsyncUdp::bind(("127.0.0.1", private_port + 1))
                .await
                .unwrap(),
        );
    }
    let mut received = [0; 1600];
    for (index, expected) in [a_plain, b_plain].iter().enumerate() {
        let size = tokio::time::timeout(
            Duration::from_secs(2),
            private_rtp[index].recv(&mut received),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&received[..size], expected);
        let size = tokio::time::timeout(
            Duration::from_secs(2),
            private_rtcp[index].recv(&mut received),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&received[..size], sr(if index == 0 { 101 } else { 202 }));
    }
    assert_eq!(stats.packets.load(Ordering::Relaxed), 2);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for candidate in port..port + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", candidate)).is_ok());
    }
    assert!(Input::bind_with_ledger(&cfg, Some(ledger)).await.is_err());
}
#[tokio::test]
async fn authenticated_tracks_reject_bad_media_preserve_rollover_and_encrypt_feedback() {
    let d = tempfile::tempdir().unwrap();
    let k = key_file(d.path());
    let (p, res) = ports(4);
    let s = d.path().join("owned.sdp");
    std::fs::write(&s, description(p, [100, 101])).unwrap();
    drop(res);
    let c = config(p, &s, &k, [100, 101]);
    let input = Input::bind(&c).await.unwrap();
    // Replacing a configured file affects the next generation only. All current
    // lanes and feedback must retain the same original snapshot.
    use base64::Engine as _;
    std::fs::write(
        &k,
        base64::engine::general_purpose::STANDARD.encode([0x32; 30]),
    )
    .unwrap();

    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let task = tokio::spawn(input.run(writer, cancel.clone()));
    let mut text = vec![];
    reader.read_to_end(&mut text).await.unwrap();
    let text = String::from_utf8(text).unwrap();
    assert!(
        text.contains("RTP/AVP")
            && !text.contains("SAVP")
            && !text.contains("crypto")
            && !text.contains("owned.key")
    );
    let mut decoders = vec![];
    let mut control = vec![];
    for line in text.lines().filter(|l| l.starts_with("m=audio ")) {
        let port = line
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        decoders.push(AsyncUdp::bind(("127.0.0.1", port)).await.unwrap());
        control.push(AsyncUdp::bind(("127.0.0.1", port + 1)).await.unwrap());
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while *stats.status.lock().unwrap() != "bound" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut b = [0; 2049];
    let foreign = AsyncUdp::bind("127.0.0.2:0").await.unwrap();
    let mut foreign_packet = media(1, 999, 1);
    crypto::Session::new([0x31; 30], Some(999))
        .unwrap()
        .protect(&mut foreign_packet, false)
        .unwrap();
    foreign
        .send_to(&foreign_packet, ("127.0.0.1", p))
        .await
        .unwrap();

    for (i, decoder) in decoders.iter().enumerate() {
        let (_source, reserved) = ports(2);
        let mut sender = reserved.into_iter().map(|s| {
            s.set_nonblocking(true).unwrap();
            AsyncUdp::from_std(s).unwrap()
        });
        let socket = sender.next().unwrap();
        let feedback = sender.next().unwrap();
        let target = ("127.0.0.1", p + 2 * i as u16);
        let id = 100 + i as u32;
        let mut tx = crypto::Session::new([0x31; 30], Some(id)).unwrap();
        socket.send_to(&media(65534, id, 1), target).await.unwrap();
        let mut wrong = media(65534, id, 1);
        crypto::Session::new([0x32; 30], Some(id))
            .unwrap()
            .protect(&mut wrong, false)
            .unwrap();
        socket.send_to(&wrong, target).await.unwrap();
        let mut malformed = media(65535, id, 1);
        malformed[1] = 96;
        tx.protect(&mut malformed, false).unwrap();
        socket.send_to(&malformed, target).await.unwrap();
        socket.send_to(&malformed, target).await.unwrap();
        let mut alternate = malformed.clone();
        alternate[8..12].copy_from_slice(&(id + 100).to_be_bytes());
        socket.send_to(&alternate, target).await.unwrap();
        let plain = media(0, id, 1584);
        assert_eq!(plain.len(), 1600);
        let mut cipher = plain.clone();
        tx.protect(&mut cipher, false).unwrap();
        let mut tampered = cipher.clone();
        *tampered.last_mut().unwrap() ^= 1;
        socket.send_to(&tampered, target).await.unwrap();
        socket.send_to(&cipher, target).await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), decoder.recv(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&b[..n], plain);
        socket.send_to(&cipher, target).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), decoder.recv(&mut b))
                .await
                .is_err()
        );
        let mut report = sr(id);
        tx.protect(&mut report, true).unwrap();
        feedback
            .send_to(&report, ("127.0.0.1", p + 2 * i as u16 + 1))
            .await
            .unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), control[i].recv_from(&mut b))
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(&b[..n], sr(id));
        let bridge = b[..n].to_vec();
        feedback
            .send_to(&bridge, ("127.0.0.1", p + 2 * i as u16 + 1))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), control[i].recv_from(&mut b))
                .await
                .is_err()
        );
        let rr = packet::receiver_report(500 + i as u32, id, &packet::Reception::default());
        // Reply to the private relay endpoint learned from its authenticated SR.
        feedback
            .send_to(&report, ("127.0.0.1", p + 2 * i as u16 + 1))
            .await
            .unwrap(); // rejected replay
        let mut next = sr(id);
        tx.protect(&mut next, true).unwrap();
        feedback
            .send_to(&next, ("127.0.0.1", p + 2 * i as u16 + 1))
            .await
            .unwrap();
        let (n, addr) = tokio::time::timeout(Duration::from_secs(2), control[i].recv_from(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert!(packet::valid_rtcp(&b[..n]));
        control[i].send_to(&rr, addr).await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(2), feedback.recv_from(&mut b))
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_ne!(&b[..n], rr);
        let mut received = b[..n].to_vec();
        crypto::Session::new([0x31; 30], None)
            .unwrap()
            .unprotect(&mut received, true)
            .unwrap();
        assert_eq!(received, rr);
    }
    assert_eq!(stats.packets.load(Ordering::Relaxed), 2);
    assert!(stats.foreign.load(Ordering::Relaxed) >= 1);
    assert!(stats.auth_failed.load(Ordering::Relaxed) >= 10);
    assert_eq!(stats.invalid.load(Ordering::Relaxed), 2);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for n in p..p + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}

#[tokio::test]
async fn secure_cancellation_before_decoder_bind_releases_public_lanes() {
    let d = tempfile::tempdir().unwrap();
    let k = key_file(d.path());
    let (p, res) = ports(4);
    let s = d.path().join("owned.sdp");
    std::fs::write(&s, description(p, [222, 223])).unwrap();
    drop(res);
    let input = Input::bind(&config(p, &s, &k, [222, 223])).await.unwrap();
    let stats = input.stats.clone();
    let c = CancellationToken::new();
    let (writer, _reader) = tokio::io::duplex(1);
    let task = tokio::spawn(input.run(writer, c.clone()));
    let sender = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    let mut encrypted = media(1, 222, 4);
    crypto::Session::new([0x31; 30], Some(222))
        .unwrap()
        .protect(&mut encrypted, false)
        .unwrap();
    sender.send_to(&encrypted, ("127.0.0.1", p)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.packets.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("packet must be queued while SDP writer is blocked");
    c.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for n in p..p + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}

#[tokio::test]
async fn secure_packets_wait_for_all_owned_decoder_ports_without_filling_public_socket() {
    let d = tempfile::tempdir().unwrap();
    let key = key_file(d.path());
    let (port, reserved) = ports(4);
    let sdp = d.path().join("owned.sdp");
    std::fs::write(&sdp, description(port, [777, 888])).unwrap();
    drop(reserved);
    let input = Input::bind(&config(port, &sdp, &key, [777, 888]))
        .await
        .unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let mut task = tokio::spawn(input.run(writer, cancel.clone()));
    let mut text = Vec::new();
    reader.read_to_end(&mut text).await.unwrap();
    let private_ports: Vec<u16> = String::from_utf8(text)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("m=audio "))
        .map(|line| line.split_whitespace().nth(1).unwrap().parse().unwrap())
        .collect();
    assert_eq!(private_ports.len(), 2);

    let (_, sender_pair) = ports(2);
    let mut sender_pair = sender_pair.into_iter().map(|socket| {
        socket.set_nonblocking(true).unwrap();
        AsyncUdp::from_std(socket).unwrap()
    });
    let sender = sender_pair.next().unwrap();
    let sender_rtcp = sender_pair.next().unwrap();
    let foreign = AsyncUdp::bind("127.0.0.2:0").await.unwrap();
    let wrong_key_sender = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    let malformed_sender = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    assert_ne!(
        wrong_key_sender.local_addr().unwrap(),
        sender.local_addr().unwrap()
    );
    assert_ne!(
        malformed_sender.local_addr().unwrap(),
        sender.local_addr().unwrap()
    );
    foreign
        .send_to(&media(8, 777, 4), ("127.0.0.1", port))
        .await
        .unwrap();
    let mut wrong_key = media(9, 901, 4);
    crypto::Session::new([0x32; 30], Some(901))
        .unwrap()
        .protect(&mut wrong_key, false)
        .unwrap();
    wrong_key_sender
        .send_to(&wrong_key, ("127.0.0.1", port))
        .await
        .unwrap();
    let mut malformed = media(9, 777, 4);
    malformed[1] = 96;
    crypto::Session::new([0x31; 30], Some(777))
        .unwrap()
        .protect(&mut malformed, false)
        .unwrap();
    let mut source_crypto = crypto::Session::new([0x31; 30], Some(777)).unwrap();
    malformed_sender
        .send_to(&malformed, ("127.0.0.1", port))
        .await
        .unwrap();
    let original = media(10, 777, 4);
    let mut encrypted = original.clone();
    source_crypto.protect(&mut encrypted, false).unwrap();
    sender
        .send_to(&encrypted, ("127.0.0.1", port))
        .await
        .unwrap();
    // A captured, correctly authenticated packet from the first lane must not
    // become the second lane's initial peer or enter its startup queue.
    sender
        .send_to(&encrypted, ("127.0.0.1", port + 2))
        .await
        .unwrap();
    let second_original = media(11, 888, 4);
    let mut second_encrypted = second_original.clone();
    crypto::Session::new([0x31; 30], Some(888))
        .unwrap()
        .protect(&mut second_encrypted, false)
        .unwrap();
    sender
        .send_to(&second_encrypted, ("127.0.0.1", port + 2))
        .await
        .unwrap();
    let admitted = tokio::time::timeout(Duration::from_secs(1), async {
        while stats.packets.load(Ordering::Relaxed) < 2
            || stats.auth_failed.load(Ordering::Relaxed) < 2
            || stats.invalid.load(Ordering::Relaxed) == 0
            || stats.foreign.load(Ordering::Relaxed) == 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    if !admitted {
        cancel.cancel();
        (&mut task).await.unwrap().unwrap();
    }
    assert!(
        admitted,
        "valid public SRTP must be read during decoder startup"
    );
    assert_eq!(stats.packets.load(Ordering::Relaxed), 2);

    // RTCP is accepted only after RTP has established the legitimate peer.
    let mut report = sr(777);
    source_crypto.protect(&mut report, true).unwrap();
    sender_rtcp
        .send_to(&report, ("127.0.0.1", port + 1))
        .await
        .unwrap();

    let first_rtp = AsyncUdp::bind(("127.0.0.1", private_ports[0]))
        .await
        .unwrap();
    let first_rtcp = AsyncUdp::bind(("127.0.0.1", private_ports[0] + 1))
        .await
        .unwrap();
    let mut received = [0; 1600];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), first_rtp.recv(&mut received))
            .await
            .is_err(),
        "one bound decoder track must not release packets"
    );
    let second_rtp = AsyncUdp::bind(("127.0.0.1", private_ports[1]))
        .await
        .unwrap();
    let _second_rtcp = AsyncUdp::bind(("127.0.0.1", private_ports[1] + 1))
        .await
        .unwrap();
    let n = tokio::time::timeout(Duration::from_secs(2), first_rtp.recv(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received[..n], original);
    let n = tokio::time::timeout(Duration::from_secs(2), second_rtp.recv(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received[..n], second_original);
    let n = tokio::time::timeout(Duration::from_secs(2), first_rtcp.recv(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&received[..n], sr(777));
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), &mut task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn secure_startup_overflow_drops_oldest_without_kernel_loss() {
    let d = tempfile::tempdir().unwrap();
    let key = key_file(d.path());
    let (port, reserved) = ports(4);
    let sdp = d.path().join("owned.sdp");
    std::fs::write(&sdp, description(port, [222, 333])).unwrap();
    drop(reserved);
    let input = Input::bind(&config(port, &sdp, &key, [222, 333]))
        .await
        .unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let task = tokio::spawn(input.run(writer, cancel.clone()));
    let mut text = Vec::new();
    reader.read_to_end(&mut text).await.unwrap();
    let private_ports: Vec<u16> = String::from_utf8(text)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("m=audio "))
        .map(|line| line.split_whitespace().nth(1).unwrap().parse().unwrap())
        .collect();
    assert_eq!(private_ports.len(), 2);
    let sender = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    let mut crypto = crypto::Session::new([0x31; 30], Some(222)).unwrap();
    for seq in 0..=1024u16 {
        let mut packet = media(seq, 222, 4);
        crypto.protect(&mut packet, false).unwrap();
        sender.send_to(&packet, ("127.0.0.1", port)).await.unwrap();
        if seq % 32 == 31 || seq == 1024 {
            tokio::time::timeout(Duration::from_secs(1), async {
                while stats.packets.load(Ordering::Relaxed) < u64::from(seq) + 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the public reader must keep up with paced startup packets");
        }
    }
    let second_original = media(200, 333, 4);
    let mut second_encrypted = second_original.clone();
    crypto::Session::new([0x31; 30], Some(333))
        .unwrap()
        .protect(&mut second_encrypted, false)
        .unwrap();
    sender
        .send_to(&second_encrypted, ("127.0.0.1", port + 2))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.packets.load(Ordering::Relaxed) < 1026 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.packets.load(Ordering::Relaxed), 1026);
    assert_eq!(stats.startup_dropped_packets.load(Ordering::Relaxed), 1);
    let decoder = AsyncUdp::bind(("127.0.0.1", private_ports[0]))
        .await
        .unwrap();
    let _control = AsyncUdp::bind(("127.0.0.1", private_ports[0] + 1))
        .await
        .unwrap();
    let second_decoder = AsyncUdp::bind(("127.0.0.1", private_ports[1]))
        .await
        .unwrap();
    let _second_control = AsyncUdp::bind(("127.0.0.1", private_ports[1] + 1))
        .await
        .unwrap();
    let mut plain = [0; 1600];
    let n = tokio::time::timeout(Duration::from_secs(2), decoder.recv(&mut plain))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, media(1, 222, 4).len());
    assert_eq!(u16::from_be_bytes([plain[2], plain[3]]), 1);
    let n = tokio::time::timeout(Duration::from_secs(2), second_decoder.recv(&mut plain))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&plain[..n], second_original);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn secure_startup_rtcp_overflow_has_its_own_bound() {
    let d = tempfile::tempdir().unwrap();
    let key = key_file(d.path());
    let (port, reserved) = ports(4);
    let sdp = d.path().join("owned.sdp");
    std::fs::write(&sdp, description(port, [555, 556])).unwrap();
    drop(reserved);
    let input = Input::bind(&config(port, &sdp, &key, [555, 556]))
        .await
        .unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let task = tokio::spawn(input.run(writer, cancel.clone()));
    let mut text = Vec::new();
    reader.read_to_end(&mut text).await.unwrap();
    let private_ports: Vec<u16> = String::from_utf8(text)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("m=audio "))
        .map(|line| line.split_whitespace().nth(1).unwrap().parse().unwrap())
        .collect();
    let (_, sockets) = ports(2);
    let mut sockets = sockets.into_iter().map(|socket| {
        socket.set_nonblocking(true).unwrap();
        AsyncUdp::from_std(socket).unwrap()
    });
    let sender = sockets.next().unwrap();
    let sender_rtcp = sockets.next().unwrap();
    let mut crypto = crypto::Session::new([0x31; 30], Some(555)).unwrap();
    let mut media = media(1, 555, 4);
    crypto.protect(&mut media, false).unwrap();
    sender.send_to(&media, ("127.0.0.1", port)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.packets.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    for marker in 1..=65u32 {
        let mut report = sr(555);
        report[8..12].copy_from_slice(&marker.to_be_bytes());
        crypto.protect(&mut report, true).unwrap();
        sender_rtcp
            .send_to(&report, ("127.0.0.1", port + 1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.startup_dropped_rtcp_packets.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        stats.startup_dropped_rtcp_packets.load(Ordering::Relaxed),
        1
    );
    assert_eq!(stats.startup_dropped_packets.load(Ordering::Relaxed), 0);
    let _first_rtp = AsyncUdp::bind(("127.0.0.1", private_ports[0]))
        .await
        .unwrap();
    let first_rtcp = AsyncUdp::bind(("127.0.0.1", private_ports[0] + 1))
        .await
        .unwrap();
    let _second_rtp = AsyncUdp::bind(("127.0.0.1", private_ports[1]))
        .await
        .unwrap();
    let _second_rtcp = AsyncUdp::bind(("127.0.0.1", private_ports[1] + 1))
        .await
        .unwrap();
    let mut received = [0; 2048];
    let n = tokio::time::timeout(Duration::from_secs(2), first_rtcp.recv(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(n >= 12);
    assert_eq!(u32::from_be_bytes(received[8..12].try_into().unwrap()), 2);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn secure_startup_drain_keeps_order_with_new_arrivals() {
    let d = tempfile::tempdir().unwrap();
    let key = key_file(d.path());
    let (port, reserved) = ports(4);
    let sdp = d.path().join("owned.sdp");
    std::fs::write(&sdp, description(port, [666, 667])).unwrap();
    drop(reserved);
    let input = Input::bind(&config(port, &sdp, &key, [666, 667]))
        .await
        .unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let task = tokio::spawn(input.run(writer, cancel.clone()));
    let mut text = Vec::new();
    reader.read_to_end(&mut text).await.unwrap();
    let private_ports: Vec<u16> = String::from_utf8(text)
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("m=audio "))
        .map(|line| line.split_whitespace().nth(1).unwrap().parse().unwrap())
        .collect();
    let sender = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    let mut crypto = crypto::Session::new([0x31; 30], Some(666)).unwrap();
    for sequence in 0..64u16 {
        let mut packet = media(sequence, 666, 4);
        crypto.protect(&mut packet, false).unwrap();
        sender.send_to(&packet, ("127.0.0.1", port)).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.packets.load(Ordering::Relaxed) < 64 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let decoder = AsyncUdp::bind(("127.0.0.1", private_ports[0]))
        .await
        .unwrap();
    let _control = AsyncUdp::bind(("127.0.0.1", private_ports[0] + 1))
        .await
        .unwrap();
    let _second_decoder = AsyncUdp::bind(("127.0.0.1", private_ports[1]))
        .await
        .unwrap();
    let _second_control = AsyncUdp::bind(("127.0.0.1", private_ports[1] + 1))
        .await
        .unwrap();
    let receiver = tokio::spawn(async move {
        let mut buffer = [0; 1600];
        let mut seen = Vec::new();
        while seen.len() < 128 {
            let n = decoder.recv(&mut buffer).await.unwrap();
            assert_eq!(n, media(0, 666, 4).len());
            seen.push(u16::from_be_bytes([buffer[2], buffer[3]]));
        }
        seen
    });
    for sequence in 64..128u16 {
        let mut packet = media(sequence, 666, 4);
        crypto.protect(&mut packet, false).unwrap();
        sender.send_to(&packet, ("127.0.0.1", port)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let seen = tokio::time::timeout(Duration::from_secs(3), receiver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen, (0..128).collect::<Vec<u16>>());
    assert_eq!(stats.startup_dropped_packets.load(Ordering::Relaxed), 0);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
