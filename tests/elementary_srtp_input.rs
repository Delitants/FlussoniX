use flussonix::direct_rtp::{
    config::Settings,
    crypto,
    elementary::{input::Input, sdp::Session},
    packet,
};
use serde_json::json;
use std::{net::UdpSocket, os::unix::fs::PermissionsExt, sync::atomic::Ordering, time::Duration};
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
fn description(port: u16) -> String {
    format!(
        "v=0\no=- 0 0 IN IP4 127.0.0.1\ns=Owned secure input\nc=IN IP4 127.0.0.1\nt=0 0\nm=audio {port} RTP/SAVP 97\na=rtpmap:97 MPEG4-GENERIC/48000/2\na=fmtp:97 mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\nm=audio {} RTP/SAVP 97\na=rtpmap:97 MPEG4-GENERIC/48000/2\na=fmtp:97 mode=AAC-hbr;config=1190;sizeLength=13;indexLength=3;indexDeltaLength=3\n",
        port + 2
    )
}
fn config(port: u16, sdp: &std::path::Path, key: &std::path::Path) -> Settings {
    Settings::input(&json!({"url":format!("srtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary","sdp_file":sdp,"key_file":key,"jitter_ms":0,"source_ip":"127.0.0.1"}})).expect("secure elementary configuration accepted").unwrap()
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
    let cfg = config(40000, &s, &k);
    let text = description(40000);
    assert!(Session::parse(text.as_bytes(), &cfg).is_ok());
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
    std::fs::write(&s, description(p)).unwrap();
    drop(res);
    let c = config(p, &s, &k);
    assert!(Input::bind(&c).await.is_err());
    for n in p..p + 4 {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}
#[tokio::test]
async fn authenticated_tracks_reject_bad_media_preserve_rollover_and_encrypt_feedback() {
    let d = tempfile::tempdir().unwrap();
    let k = key_file(d.path());
    let (p, res) = ports(4);
    let s = d.path().join("owned.sdp");
    std::fs::write(&s, description(p)).unwrap();
    drop(res);
    let c = config(p, &s, &k);
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
    std::fs::write(&s, description(p)).unwrap();
    drop(res);
    let input = Input::bind(&config(p, &s, &k)).await.unwrap();
    let c = CancellationToken::new();
    let (writer, _reader) = tokio::io::duplex(1);
    let task = tokio::spawn(input.run(writer, c.clone()));
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
