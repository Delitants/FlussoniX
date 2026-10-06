use flussonix::direct_rtp::{config::Settings, input::Input, packet::packet};
use serde_json::json;
use std::time::Duration;
use tokio::{io::AsyncReadExt, net::UdpSocket};
use tokio_util::sync::CancellationToken;
fn ports() -> u16 {
    for _ in 0..64 {
        let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = a.local_addr().unwrap().port();
        if p < 65535 && std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p + 1)).is_ok() {
            return p;
        }
    }
    panic!("no owned pair");
}
fn ts() -> Vec<u8> {
    let mut b = vec![0xff; 188];
    b[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    b
}
#[tokio::test]
async fn input_pins_media_peer_reorders_and_releases_both_ports_on_cancel() {
    let port = ports();
    let settings = Settings::parse(
        &json!({"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"jitter_ms":30}}),
    )
    .unwrap();
    let input = Input::bind(&settings).await.unwrap();
    let stats = input.stats.clone();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { input.run(write, c).await });
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let hostile = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut body = ts();
    sender
        .send_to(&packet(65534, 0, 42, &body), settings.address)
        .await
        .unwrap();
    let mut received = vec![0; 188];
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received, body);
    hostile
        .send_to(&packet(65535, 1, 42, &body), settings.address)
        .await
        .unwrap();
    sender
        .send_to(&packet(65535, 1, 43, &body), settings.address)
        .await
        .unwrap();
    body[4] = 3;
    sender
        .send_to(&packet(0, 2, 42, &body), settings.address)
        .await
        .unwrap();
    body[4] = 2;
    sender
        .send_to(&packet(65535, 1, 42, &body), settings.address)
        .await
        .unwrap();
    let mut rest = vec![0; 376];
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut rest))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rest[4], 2);
    assert_eq!(rest[192], 3);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(UdpSocket::bind(settings.address).await.is_ok());
    assert!(
        UdpSocket::bind((settings.address.ip(), port + 1))
            .await
            .is_ok()
    );
    assert_eq!(stats.snapshot()["packets"], 3);
    assert_eq!(stats.snapshot()["foreign_packets"], 2);
}
#[tokio::test]
async fn occupied_rtcp_port_rejects_startup_without_leaking_rtp_socket() {
    let port = ports();
    let held = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port + 1))
        .await
        .unwrap();
    let cfg = Settings::parse(&json!({"url":format!("rtp://127.0.0.1:{port}")})).unwrap();
    assert!(Input::bind(&cfg).await.is_err());
    assert!(UdpSocket::bind(cfg.address).await.is_ok());
    drop(held);
}
#[tokio::test]
async fn output_preserves_ts_in_mtu_packets_and_stops_without_a_remux_child() {
    use flussonix::direct_rtp::{config::outputs, output::State};
    let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let defs = outputs(
        &json!({"flussonix_rtp_outputs":[{"url":format!("rtp://{address}"),"max_mbps":10}]}),
    )
    .unwrap();
    let state = State::new(defs.into_iter().next().unwrap(), 0);
    let (tx, rx) = tokio::sync::broadcast::channel(8);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let s = state.clone();
    let task = tokio::spawn(async move {
        s.run(rx, c).await;
    });
    let body = ts().repeat(14);
    tx.send(bytes::Bytes::copy_from_slice(&body[..100]))
        .unwrap();
    tx.send(bytes::Bytes::copy_from_slice(&body[100..]))
        .unwrap();
    let mut received: Vec<u8> = Vec::new();
    let mut seq = None;
    let mut id = None;
    for _ in 0..2 {
        let mut buffer = vec![0; 1600];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), listener.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 1328);
        assert_eq!(&buffer[..2], &[0x80, 33]);
        let next = u16::from_be_bytes(buffer[2..4].try_into().unwrap());
        if let Some(prior) = seq {
            assert_eq!(next, prior + 1)
        }
        seq = Some(next);
        let ssrc = u32::from_be_bytes(buffer[8..12].try_into().unwrap());
        if let Some(prior) = id {
            assert_eq!(ssrc, prior)
        }
        id = Some(ssrc);
        received.extend_from_slice(&buffer[12..n]);
    }
    assert_eq!(received, body);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.stats()["packets"], 2);
    assert_eq!(state.stats()["status"], "stopped");
}
#[tokio::test]
async fn sparse_output_flushes_complete_ts_packets_without_waiting_for_seven() {
    use flussonix::direct_rtp::{config::outputs, output::State};
    let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let def = outputs(&json!({"flussonix_rtp_outputs":[{"url":format!("rtp://{address}")}]}))
        .unwrap()
        .remove(0);
    let state = State::new(def, 0);
    let (tx, rx) = tokio::sync::broadcast::channel(8);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { state.run(rx, c).await });
    tx.send(bytes::Bytes::from(ts().repeat(3))).unwrap();
    let mut b = [0; 1600];
    let (n, _) = tokio::time::timeout(Duration::from_secs(1), listener.recv_from(&mut b))
        .await
        .expect("sparse complete TS must progress")
        .unwrap();
    assert_eq!(n, 12 + 3 * 188);
    cancel.cancel();
    task.await.unwrap();
}
#[tokio::test]
async fn input_sends_receiver_report_only_to_its_pinned_media_source() {
    let port = ports();
    let source_port = ports();
    assert_ne!(port, source_port);
    let settings = Settings::parse(&json!({"url":format!("rtp://127.0.0.1:{port}")})).unwrap();
    let input = Input::bind(&settings).await.unwrap();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { input.run(write, c).await });
    let source = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, source_port))
        .await
        .unwrap();
    let reports = UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, source_port + 1))
        .await
        .unwrap();
    source
        .send_to(&packet(100, 90, 42, &ts()), settings.address)
        .await
        .unwrap();
    let mut tsbytes = [0; 188];
    read.read_exact(&mut tsbytes).await.unwrap();
    let mut report = [0; 2048];
    let (n, peer) = tokio::time::timeout(Duration::from_secs(2), reports.recv_from(&mut report))
        .await
        .expect("RTCP receiver report must arrive")
        .unwrap();
    assert_eq!(peer.port(), port + 1);
    assert_eq!(&report[..2], &[0x81, 201]);
    assert_eq!(u32::from_be_bytes(report[8..12].try_into().unwrap()), 42);
    assert!(flussonix::direct_rtp::packet::valid_rtcp(&report[..n]));
    cancel.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn ipv4_multicast_uses_explicit_loopback_interface_and_releases_pair() {
    use flussonix::direct_rtp::{config::outputs, output::State};
    let port = ports();
    let url = format!("rtp://239.255.19.42:{port}");
    let opts = json!({"interface":"127.0.0.1","ttl":1});
    let cfg = Settings::parse(&json!({"url":url,"flussonix_rtp":opts})).unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { input.run(write, c).await });
    let definition = outputs(&json!({"flussonix_rtp_outputs":[{"url":url,"flussonix_rtp":opts}]}))
        .unwrap()
        .remove(0);
    let state = State::new(definition, 0);
    let output_stats = state.clone();
    let (tx, rx) = tokio::sync::broadcast::channel(4);
    let c = cancel.clone();
    let sender = tokio::spawn(async move { state.run(rx, c).await });
    tx.send(bytes::Bytes::from(ts().repeat(7))).unwrap();
    let mut body = vec![0; 7 * 188];
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(body, ts().repeat(7));
    let feedback = tokio::time::timeout(Duration::from_secs(2), async {
        while output_stats.stats()["rtcp_packets"].as_u64().unwrap() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    cancel.cancel();
    task.await.unwrap().unwrap();
    sender.await.unwrap();
    assert!(
        feedback.is_ok(),
        "multicast receiver feedback must reach sender"
    );
    assert!(
        UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, port))
            .await
            .is_ok()
    );
    assert!(
        UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, port + 1))
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn one_broadcast_fans_out_to_independent_destinations() {
    use flussonix::direct_rtp::{config::outputs, output::State};
    let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let defs=outputs(&json!({"flussonix_rtp_outputs":[{"url":format!("rtp://{}",first.local_addr().unwrap())},{"url":format!("rtp://{}",second.local_addr().unwrap())}]})).unwrap();
    let (tx, _) = tokio::sync::broadcast::channel(8);
    let cancel = CancellationToken::new();
    let mut tasks = vec![];
    for (i, def) in defs.into_iter().enumerate() {
        let state = State::new(def, i);
        let rx = tx.subscribe();
        let c = cancel.clone();
        tasks.push(tokio::spawn(async move { state.run(rx, c).await }));
    }
    let expected = ts().repeat(7);
    tx.send(bytes::Bytes::copy_from_slice(&expected)).unwrap();
    let mut ids = vec![];
    for rx in [first, second] {
        let mut b = [0; 1600];
        let n = tokio::time::timeout(Duration::from_secs(2), rx.recv(&mut b))
            .await
            .unwrap()
            .unwrap();
        let p = flussonix::direct_rtp::packet::parse(&b[..n]).unwrap();
        assert_eq!(p.payload, expected);
        ids.push(p.ssrc);
    }
    assert_ne!(ids[0], ids[1]);
    cancel.cancel();
    for task in tasks {
        task.await.unwrap();
    }
}
#[tokio::test]
async fn ipv6_loopback_receives_and_transmits_on_owned_consecutive_ports() {
    use flussonix::direct_rtp::{config::outputs, output::State};
    let p = loop {
        let socket = std::net::UdpSocket::bind((std::net::Ipv6Addr::LOCALHOST, 0)).unwrap();
        let p = socket.local_addr().unwrap().port();
        if p < 65535 && std::net::UdpSocket::bind((std::net::Ipv6Addr::LOCALHOST, p + 1)).is_ok() {
            break p;
        }
    };
    let url = format!("rtp://[::1]:{p}");
    let cfg = Settings::parse(&json!({"url":url,"flussonix_rtp":{"source_ip":"::1"}})).unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let receiving = tokio::spawn(async move { input.run(write, stop).await });
    let output = State::new(
        outputs(&json!({"flussonix_rtp_outputs":[{"url":url}]}))
            .unwrap()
            .remove(0),
        0,
    );
    let (tx, rx) = tokio::sync::broadcast::channel(4);
    let stop = cancel.clone();
    let sending = tokio::spawn(async move { output.run(rx, stop).await });
    tx.send(bytes::Bytes::from(ts().repeat(7))).unwrap();
    let mut received = [0; 7 * 188];
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.to_vec(), ts().repeat(7));
    cancel.cancel();
    receiving.await.unwrap().unwrap();
    sending.await.unwrap();
    assert!(
        UdpSocket::bind((std::net::Ipv6Addr::LOCALHOST, p))
            .await
            .is_ok()
    );
    assert!(
        UdpSocket::bind((std::net::Ipv6Addr::LOCALHOST, p + 1))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn malformed_ts_does_not_pin_or_reach_the_decoder() {
    let p = ports();
    let cfg = Settings::parse(&json!({"url":format!("rtp://127.0.0.1:{p}")})).unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let stats = input.stats.clone();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { input.run(write, c).await });
    let hostile = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for (afc, len, flags) in [(0x20, 0, 0), (0x30, 183, 0), (0x30, 1, 0x10)] {
        let mut malformed = ts();
        malformed[3] = afc;
        malformed[4] = len;
        malformed[5] = flags;
        hostile
            .send_to(&packet(1, 0, 99, &malformed), cfg.address)
            .await
            .unwrap();
    }
    sender
        .send_to(&packet(1, 0, 42, &ts()), cfg.address)
        .await
        .unwrap();
    let mut received = [0; 188];
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.to_vec(), ts());
    assert_eq!(stats.snapshot()["packets"], 1);
    assert_eq!(stats.snapshot()["invalid_packets"], 3);
    cancel.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn multicast_ignores_unrelated_unicast_before_source_pin() {
    let p = ports();
    let cfg=Settings::parse(&json!({"url":format!("rtp://239.255.19.43:{p}"),"flussonix_rtp":{"interface":"127.0.0.1"}})).unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let (write, mut read) = tokio::io::duplex(4096);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { input.run(write, c).await });
    let hostile = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    hostile
        .send_to(&packet(1, 0, 99, &ts()), (std::net::Ipv4Addr::LOCALHOST, p))
        .await
        .unwrap();
    let mut received = [0; 188];
    assert!(
        tokio::time::timeout(Duration::from_millis(80), read.read_exact(&mut received))
            .await
            .is_err(),
        "unicast must not enter configured multicast input"
    );
    let source = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    source.set_multicast_loop_v4(true).unwrap();
    use std::os::fd::AsRawFd;
    let interface = libc::in_addr {
        s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
    };
    assert_eq!(
        unsafe {
            libc::setsockopt(
                source.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_MULTICAST_IF,
                (&interface as *const libc::in_addr).cast(),
                std::mem::size_of_val(&interface) as libc::socklen_t,
            )
        },
        0
    );
    source
        .send_to(&packet(1, 0, 42, &ts()), cfg.address)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.to_vec(), ts());
    cancel.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn distinct_multicast_groups_share_ports_without_cross_delivery_and_release() {
    use flussonix::direct_rtp::{config::outputs, output::State};
    let p = ports();
    let opts = json!({"interface":"127.0.0.1","ttl":1});
    let cancel = CancellationToken::new();
    let mut readers = vec![];
    let mut tasks = vec![];
    let mut txs = vec![];
    for (i, group) in ["239.255.19.44", "239.255.19.45"].iter().enumerate() {
        let url = format!("rtp://{group}:{p}");
        let cfg = Settings::parse(&json!({"url":url,"flussonix_rtp":opts})).unwrap();
        let input = Input::bind(&cfg)
            .await
            .expect("different multicast groups may share RTP/RTCP ports");
        let (write, read) = tokio::io::duplex(4096);
        readers.push(read);
        let c = cancel.clone();
        tasks.push(tokio::spawn(
            async move { input.run(write, c).await.unwrap() },
        ));
        let def = outputs(&json!({"flussonix_rtp_outputs":[{"url":url,"flussonix_rtp":opts}]}))
            .unwrap()
            .remove(0);
        let state = State::new(def, i);
        let (tx, rx) = tokio::sync::broadcast::channel(4);
        txs.push(tx);
        let c = cancel.clone();
        tasks.push(tokio::spawn(async move { state.run(rx, c).await }));
    }
    for (i, tx) in txs.iter().enumerate() {
        let mut body = ts();
        body[4] = i as u8;
        tx.send(bytes::Bytes::from(body)).unwrap();
    }
    for (i, read) in readers.iter_mut().enumerate() {
        let mut b = [0; 188];
        tokio::time::timeout(Duration::from_secs(2), read.read_exact(&mut b))
            .await
            .unwrap()
            .unwrap();
        let mut expected = ts();
        expected[4] = i as u8;
        assert_eq!(b.to_vec(), expected);
        assert!(
            tokio::time::timeout(Duration::from_millis(80), read.read_exact(&mut b))
                .await
                .is_err()
        );
    }
    cancel.cancel();
    for task in tasks {
        task.await.unwrap();
    }
    assert!(
        UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, p))
            .await
            .is_ok()
    );
    assert!(
        UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, p + 1))
            .await
            .is_ok()
    );
}
