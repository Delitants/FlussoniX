use flussonix::direct_rtp::{config::Settings, elementary::input::Input, packet};
use serde_json::json;
use std::{
    net::{SocketAddr, UdpSocket},
    sync::atomic::Ordering,
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::UdpSocket as AsyncUdp};
use tokio_util::sync::CancellationToken;
fn port() -> u16 {
    loop {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = s.local_addr().unwrap().port();
        if p < 65520 && UdpSocket::bind(("127.0.0.1", p + 1)).is_ok() {
            return p;
        }
    }
}
fn rtp(seq: u16, ssrc: u32, payload: &[u8]) -> Vec<u8> {
    let mut b = packet::packet(seq, 0, ssrc, payload);
    b[1] = 96;
    b
}
#[tokio::test]
async fn sdp_pipe_failure_retains_the_specific_static_input_diagnostic() {
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let file = d.path().join("owned.sdp");
    std::fs::write(&file,format!("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=Owned\nc=IN IP4 127.0.0.1\nt=0 0\nm=video {p} RTP/AVP 96\na=rtpmap:96 H264/90000\na=fmtp:96 packetization-mode=1\n")).unwrap();
    let cfg=Settings::input(&json!({"url":format!("rtp://127.0.0.1:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file}})).unwrap().unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let stats = input.stats.clone();
    let (writer, reader) = tokio::io::duplex(32768);
    drop(reader);
    let error = input
        .run(writer, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error, "Elementary decoder SDP write failed");
    assert_eq!(
        stats.snapshot()["last_error"],
        "Elementary decoder SDP write failed"
    );
    assert_eq!(stats.snapshot()["status"], "failed");
    for n in [p, p + 1] {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}
#[tokio::test]
async fn engine_does_not_admit_media_to_sockets_owned_by_another_process() {
    use flussonix::media::Engine;
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let file = d.path().join("owned.sdp");
    let normalized = d.path().join("decoder.sdp");
    std::fs::write(&file,format!("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=Owned\nc=IN IP4 127.0.0.1\nt=0 0\nm=video {p} RTP/AVP 96\na=rtpmap:96 H264/90000\na=fmtp:96 packetization-mode=1\n")).unwrap();
    // This owned decoder process consumes its real SDP pipe but opens no UDP
    // sockets. The test process claims those ports, simulating a foreign bind
    // during the reservation handoff. The engine must never deliver to it.
    let decoder = d.path().join("decoder.py");
    std::fs::write(&decoder, format!("#!/usr/bin/python3\nimport sys,time,pathlib\npathlib.Path({:?}).write_text(sys.stdin.read())\ntime.sleep(20)\n", normalized.to_str().unwrap())).unwrap();
    std::fs::set_permissions(&decoder, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(d.path().join("media"), decoder.to_str().unwrap());
    let worker = engine.ensure("owned", &json!({"inputs":[{"url":format!("rtp://127.0.0.1:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file,"jitter_ms":0}}],"transcoder":{"encoder":"copy","acodec":"copy"},"flussonix_input_timeout":15})).await.unwrap();
    let result = async {
        let text = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(s) = std::fs::read_to_string(&normalized) {
                    if s.ends_with('\n') {
                        break s;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "owned decoder did not read SDP".to_owned())?;
        let local: u16 = text
            .lines()
            .find(|l| l.starts_with("m=video "))
            .ok_or("decoder SDP has no video track")?
            .split_whitespace()
            .nth(1)
            .ok_or("decoder SDP has no video port")?
            .parse::<u16>()
            .map_err(|e| e.to_string())?;
        let foreign = AsyncUdp::bind(("127.0.0.1", local))
            .await
            .map_err(|e| e.to_string())?;
        let _control = AsyncUdp::bind(("127.0.0.1", local + 1))
            .await
            .map_err(|e| e.to_string())?;
        let sender = AsyncUdp::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        sender
            .send_to(&rtp(1, 123, &[0x65, 1]), ("127.0.0.1", p))
            .await
            .map_err(|e| e.to_string())?;
        let mut bytes = [0; 1600];
        let leaked = tokio::time::timeout(Duration::from_millis(300), foreign.recv(&mut bytes))
            .await
            .is_ok();
        Ok::<_, String>((leaked, worker.stats()))
    }
    .await;
    engine.stop_all().await;
    let (leaked, stats) = result.unwrap();
    assert!(
        !leaked,
        "port occupancy admitted media to a foreign decoder"
    );
    assert_eq!(stats["direct_rtp_input"]["packets"], 0);
    assert_eq!(stats["direct_rtp_input"]["status"], "starting");
    for n in [p, p + 1] {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}
#[tokio::test]
async fn cancellation_before_private_decoder_binding_releases_every_public_pair() {
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let file = d.path().join("owned.sdp");
    std::fs::write(&file,format!("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=Owned\nc=IN IP4 127.0.0.1\nt=0 0\nm=video {p} RTP/AVP 96\na=rtpmap:96 H264/90000\na=fmtp:96 packetization-mode=1\n")).unwrap();
    let cfg=Settings::input(&json!({"url":format!("rtp://127.0.0.1:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file}})).unwrap().unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let cancel = CancellationToken::new();
    let (writer, _reader) = tokio::io::duplex(1);
    let c = cancel.clone();
    let task = tokio::spawn(input.run(writer, c));
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for n in [p, p + 1] {
        assert!(UdpSocket::bind(("127.0.0.1", n)).is_ok());
    }
}
#[tokio::test]
async fn elementary_multicast_groups_are_isolated_and_source_filter_precedes_pin() {
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let cancel = CancellationToken::new();
    let mut tasks = vec![];
    let mut decoders = vec![];
    let mut controls = vec![];
    let mut states = vec![];
    for (i, group) in ["239.255.24.70", "239.255.24.71"].into_iter().enumerate() {
        let file = d.path().join(format!("owned-{i}.sdp"));
        std::fs::write(&file,format!("v=0\no=- 0 0 IN IP4 {group}\ns=Owned\nc=IN IP4 {group}\nt=0 0\nm=video {p} RTP/AVP 96\na=rtpmap:96 H264/90000\na=fmtp:96 packetization-mode=1\n")).unwrap();
        let cfg=Settings::input(&json!({"url":format!("rtp://{group}:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file,"interface":"127.0.0.1","source_ip":"127.0.0.1","ttl":1}})).unwrap().unwrap();
        let input = Input::bind(&cfg).await.unwrap();
        states.push(input.stats.clone());
        let (writer, mut reader) = tokio::io::duplex(32768);
        tasks.push(tokio::spawn(input.run(writer, cancel.clone())));
        let mut b = vec![];
        reader.read_to_end(&mut b).await.unwrap();
        let s = String::from_utf8(b).unwrap();
        let local = s
            .lines()
            .find(|l| l.starts_with("m=video "))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        decoders.push(AsyncUdp::bind(("127.0.0.1", local)).await.unwrap());
        controls.push(AsyncUdp::bind(("127.0.0.1", local + 1)).await.unwrap());
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while states.iter().any(|s| *s.status.lock().unwrap() != "bound") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fn sender(ip: &str) -> UdpSocket {
        use std::os::fd::AsRawFd;
        let socket = UdpSocket::bind((ip, 0)).unwrap();
        let interface = libc::in_addr {
            s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
        };
        // Initialized in_addr and its exact size are passed to this owned socket.
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::IPPROTO_IP,
                    libc::IP_MULTICAST_IF,
                    (&interface as *const libc::in_addr).cast(),
                    std::mem::size_of_val(&interface) as libc::socklen_t,
                )
            },
            0
        );
        socket
    }
    let foreign = sender("127.0.0.2");
    let good = sender("127.0.0.1");
    for group in ["239.255.24.70", "239.255.24.71"] {
        foreign
            .send_to(&rtp(1, 999, &[0x65, 99]), (group, p))
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while states
            .iter()
            .any(|s| s.foreign.load(Ordering::Relaxed) == 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for (i, group) in ["239.255.24.70", "239.255.24.71"].into_iter().enumerate() {
        good.send_to(&rtp(1, 100 + i as u32, &[0x65, i as u8]), (group, p))
            .unwrap();
    }
    for (i, decoder) in decoders.iter().enumerate() {
        let mut b = [0; 1601];
        let n = tokio::time::timeout(Duration::from_secs(2), decoder.recv(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&b[..n], &rtp(1, 100 + i as u32, &[0x65, i as u8]));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), decoder.recv(&mut b))
                .await
                .is_err()
        );
    }
    cancel.cancel();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    drop(decoders);
    drop(controls);
    for n in [p, p + 1] {
        assert!(UdpSocket::bind(("0.0.0.0", n)).is_ok());
    }
}
#[tokio::test]
async fn elementary_input_validates_before_pin_reorders_and_releases_ports() {
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let file = d.path().join("owned.sdp");
    std::fs::write(&file,format!("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=Owned\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video {p} RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\n")).unwrap();
    let cfg=Settings::input(&json!({"url":format!("rtp://127.0.0.1:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file,"jitter_ms":50}})).unwrap().unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let c = cancel.clone();
    let task = tokio::spawn(input.run(writer, c));
    let mut normalized = vec![];
    tokio::time::timeout(Duration::from_secs(2), reader.read_to_end(&mut normalized))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8(normalized).unwrap();
    let local = text
        .lines()
        .find(|l| l.starts_with("m=video "))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let decoder = AsyncUdp::bind(("127.0.0.1", local)).await.unwrap();
    let _control = AsyncUdp::bind(("127.0.0.1", local + 1)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while *stats.status.lock().unwrap() != "bound" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let bad = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    let good = AsyncUdp::bind("127.0.0.1:0").await.unwrap();
    let destination = SocketAddr::from(([127, 0, 0, 1], p));
    bad.send_to(&rtp(1, 999, &[0]), destination).await.unwrap();
    let first = rtp(1, 123, &[0x65, 1]);
    good.send_to(&first, destination).await.unwrap();
    let mut buffer = [0; 1601];
    let n = tokio::time::timeout(Duration::from_secs(2), decoder.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..n], &first);
    bad.send_to(&rtp(2, 999, &[0x65, 1]), destination)
        .await
        .unwrap();
    good.send_to(&rtp(2, 999, &[0x65, 1]), destination)
        .await
        .unwrap();
    good.send_to(&rtp(3, 123, &[0x65, 3]), destination)
        .await
        .unwrap();
    good.send_to(&rtp(2, 123, &[0x65, 2]), destination)
        .await
        .unwrap();
    good.send_to(&rtp(2, 123, &[0x65, 2]), destination)
        .await
        .unwrap();
    for seq in [2u16, 3] {
        let n = tokio::time::timeout(Duration::from_secs(2), decoder.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u16::from_be_bytes([buffer[2], buffer[3]]), seq);
        assert_eq!(n, 14);
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while stats.duplicates.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stats.invalid.load(Ordering::Relaxed), 1);
    assert_eq!(stats.foreign.load(Ordering::Relaxed), 2);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(UdpSocket::bind(("127.0.0.1", p)).is_ok());
    assert!(UdpSocket::bind(("127.0.0.1", p + 1)).is_ok());
}
#[tokio::test]
async fn elementary_rtcp_is_not_forwarded_before_pin_and_receiver_reports_return_to_owned_peer() {
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let file = d.path().join("owned.sdp");
    std::fs::write(&file,format!("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=Owned\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video {p} RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\n")).unwrap();
    let cfg=Settings::input(&json!({"url":format!("rtp://127.0.0.1:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file}})).unwrap().unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let stats = input.stats.clone();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let (writer, mut reader) = tokio::io::duplex(32768);
    let task = tokio::spawn(input.run(writer, c));
    let mut text = vec![];
    reader.read_to_end(&mut text).await.unwrap();
    let text = String::from_utf8(text).unwrap();
    let local = text
        .lines()
        .find(|l| l.starts_with("m=video "))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let decoder = AsyncUdp::bind(("127.0.0.1", local)).await.unwrap();
    let decoder_control = AsyncUdp::bind(("127.0.0.1", local + 1)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while *stats.status.lock().unwrap() != "bound" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let src = port();
    let sender = AsyncUdp::bind(("127.0.0.1", src)).await.unwrap();
    let sender_control = AsyncUdp::bind(("127.0.0.1", src + 1)).await.unwrap();
    let destination = SocketAddr::from(([127, 0, 0, 1], p));
    let target = SocketAddr::from(([127, 0, 0, 1], p + 1));
    let mut sr = vec![0x80, 200, 0, 6];
    for n in [123u32, 0, 0, 0, 1, 2] {
        sr.extend(n.to_be_bytes());
    }
    sr.extend(packet::sdes(123));
    let mut b = [0; 2049];
    sender_control.send_to(&sr, target).await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            decoder_control.recv_from(&mut b)
        )
        .await
        .is_err()
    );
    assert_eq!(stats.rtcp.load(Ordering::Relaxed), 0);
    sender
        .send_to(&rtp(1, 123, &[0x65, 1]), destination)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), decoder.recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    sender_control.send_to(&sr, target).await.unwrap();
    let (n, forwarder) =
        tokio::time::timeout(Duration::from_secs(2), decoder_control.recv_from(&mut b))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&b[..n], &sr);
    let rr = packet::receiver_report(
        555,
        123,
        &packet::Reception {
            highest: 1,
            lost: 0,
            fraction: 0,
            jitter: 0,
            last_sr: 0,
            delay_sr: 0,
        },
    );
    decoder_control.send_to(&rr, forwarder).await.unwrap();
    let (n, source) =
        tokio::time::timeout(Duration::from_secs(2), sender_control.recv_from(&mut b))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&b[..n], &rr);
    assert_eq!(source, target);
    let mut wrong = sr.clone();
    wrong[4..8].copy_from_slice(&999u32.to_be_bytes());
    sender_control.send_to(&wrong, target).await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            decoder_control.recv_from(&mut b)
        )
        .await
        .is_err()
    );
    assert_eq!(stats.invalid.load(Ordering::Relaxed), 1);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stats.rtcp.load(Ordering::Relaxed), 2);
}
#[tokio::test]
async fn elementary_sdp_closes_real_child_stdin_before_waiting_for_decoder() {
    let d = tempfile::tempdir().unwrap();
    let p = port();
    let file = d.path().join("owned.sdp");
    std::fs::write(&file,format!("v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=Owned\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video {p} RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\n")).unwrap();
    let cfg=Settings::input(&json!({"url":format!("rtp://127.0.0.1:{p}"),"flussonix_rtp":{"profile":"elementary","sdp_file":file}})).unwrap().unwrap();
    let input = Input::bind(&cfg).await.unwrap();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let mut child = tokio::process::Command::new("/bin/cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let task = tokio::spawn(input.run(stdin, c));
    let mut output = vec![];
    let eof = tokio::time::timeout(Duration::from_secs(1), stdout.read_to_end(&mut output)).await;
    cancel.cancel();
    task.await.unwrap().unwrap();
    if eof.is_err() {
        let _ = child.kill().await;
    }
    let _ = child.wait().await;
    assert!(
        eof.is_ok(),
        "SDP reader must receive EOF before the relay waits for UDP decoder ports"
    );
    assert!(
        String::from_utf8(output)
            .unwrap()
            .contains("FlussoniX validated decoder")
    );
}
