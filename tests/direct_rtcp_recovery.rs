#![cfg(target_os = "linux")]

use bytes::Bytes;
use flussonix::{
    direct_rtp::{config::outputs, crypto::Session, output::State, packet},
    media_queue::Channel,
};
use serde_json::json;
use std::{
    net::SocketAddr,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::fs::PermissionsExt,
    time::{Duration, Instant},
};
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;

struct Owner {
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

async fn pair() -> (UdpSocket, UdpSocket, SocketAddr) {
    for _ in 0..64 {
        let media = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = media.local_addr().unwrap();
        if address.port() < 65535 {
            if let Ok(control) = UdpSocket::bind((address.ip(), address.port() + 1)).await {
                return (media, control, address);
            }
        }
    }
    panic!("no owned UDP pair");
}
fn ts(marker: u8, packets: usize) -> Vec<u8> {
    let mut data = vec![marker; 188 * 7 * packets];
    for p in data.chunks_exact_mut(188) {
        p[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    }
    data
}
async fn wire(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut b = [0; 2048];
    let (n, peer) = tokio::time::timeout(Duration::from_secs(7), socket.recv_from(&mut b))
        .await
        .expect("owned receive deadline")
        .unwrap();
    (b[..n].to_vec(), peer)
}
struct Media {
    cipher: Option<Session>,
    sequence: Option<u16>,
    ssrc: Option<u32>,
    source: Option<SocketAddr>,
    packets: u64,
}
impl Media {
    async fn receive(&mut self, socket: &UdpSocket, marker: u8) {
        let (mut body, peer) = wire(socket).await;
        if let Some(c) = &mut self.cipher {
            c.unprotect(&mut body, false).unwrap();
        }
        let p = packet::parse(&body).unwrap();
        assert_eq!(p.payload, ts(marker, 1));
        if let Some(s) = self.ssrc {
            assert_eq!(s, p.ssrc);
        } else {
            self.ssrc = Some(p.ssrc);
        }
        if let Some(s) = self.source {
            assert_eq!(s, peer);
        } else {
            self.source = Some(peer);
        }
        if let Some(previous) = self.sequence {
            assert_eq!(
                p.sequence.wrapping_sub(previous),
                1,
                "healthy RTP sequence gap"
            );
        }
        self.sequence = Some(p.sequence);
        self.packets += 1;
    }
    fn control(&mut self, wire: &[u8]) -> (Vec<u8>, Option<u32>) {
        let mut b = wire.to_vec();
        let index = if let Some(c) = &mut self.cipher {
            let mut bad = b.clone();
            *bad.last_mut().unwrap() ^= 1;
            assert!(
                c.unprotect(&mut bad, true).is_err(),
                "fresh tamper accepted"
            );
            assert!(wire.len() >= 14);
            let at = wire.len() - 14;
            let index = u32::from_be_bytes(wire[at..at + 4].try_into().unwrap());
            assert_ne!(index & 0x80000000, 0);
            c.unprotect(&mut b, true).unwrap();
            assert!(
                c.unprotect(&mut wire.to_vec(), true).is_err(),
                "control replay accepted"
            );
            Some(index & 0x7fffffff)
        } else {
            None
        };
        assert!(packet::valid_rtcp(&b));
        assert_eq!(b[1], 200);
        assert_eq!(
            u32::from_be_bytes(b[4..8].try_into().unwrap()),
            self.ssrc.unwrap()
        );
        (b, index)
    }
}
fn owned_control(source: SocketAddr, destination: SocketAddr) -> std::net::UdpSocket {
    // Duplicate only a socket in THIS driver, then validate both endpoint tuples
    // and its datagram type. Never inspect or alter another process descriptor.
    for entry in std::fs::read_dir("/proc/self/fd").unwrap().flatten() {
        let Ok(number) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        let fd = unsafe { libc::dup(number) };
        if fd < 0 {
            continue;
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let mut kind = 0;
        let mut length = std::mem::size_of::<i32>() as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TYPE,
                (&mut kind as *mut i32).cast(),
                &mut length,
            )
        } != 0
            || kind != libc::SOCK_DGRAM
        {
            continue;
        }
        let socket = std::net::UdpSocket::from(fd);
        if socket.local_addr().ok() == Some(source) && socket.peer_addr().ok() == Some(destination)
        {
            return socket;
        }
    }
    panic!("owned connected RTCP sender descriptor not found");
}
async fn case(secure: bool) -> serde_json::Value {
    let clock = Instant::now();
    let key = tempfile::Builder::new()
        .prefix("flussonix-rtcp-qual-")
        .tempdir()
        .unwrap();
    let path = key.path().join("key");
    std::fs::write(&path, "UlJS".repeat(10)).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let (media, control, address) = pair().await;
    let definition=outputs(&json!({"flussonix_rtp_outputs":[{"url":format!("{}://{address}",if secure{"srtp"}else{"rtp"}),"max_mbps":1,"flussonix_rtp":if secure{json!({"key_file":path})}else{json!({})}}]})).unwrap().remove(0);
    let state = State::new(definition, 0);
    let queue = Channel::new(64, 4 * 1024 * 1024);
    let stop = CancellationToken::new();
    let s = state.clone();
    let c = stop.clone();
    let rx = queue.subscribe();
    let mut owner = Owner {
        stop,
        task: tokio::spawn(async move { s.run(rx, c).await }),
    };
    let mut receiver = Media {
        cipher: secure.then(|| Session::new([0x52; 30], None).unwrap()),
        sequence: None,
        ssrc: None,
        source: None,
        packets: 0,
    };
    queue.send(Bytes::from(ts(0x21, 1))).unwrap();
    receiver.receive(&media, 0x21).await;
    let (first_report, control_source) = wire(&control).await;
    let (_, first_index) = receiver.control(&first_report);
    let source = receiver.source.unwrap();
    assert_eq!(
        control_source,
        SocketAddr::new(source.ip(), source.port() + 1)
    );
    assert_eq!(state.stats()["unreachable_packets"], 0);
    drop(control); // Media socket remains bound and actively drained throughout.
    let mut attempts = vec![];
    for attempt in 0..8 {
        let marker = 0x30 + attempt;
        // A large valid TS record keeps the sender in its paced media loop.
        // Leave a real ICMP error pending on RTCP until its next report tick.
        queue.send(Bytes::from(ts(marker, 600))).unwrap();
        receiver.receive(&media, marker).await;
        let descriptor = owned_control(
            control_source,
            SocketAddr::new(address.ip(), address.port() + 1),
        );
        let mut prior_errors = 0;
        loop {
            match descriptor.send(&first_report) {
                Ok(n) => {
                    assert_eq!(n, first_report.len());
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused && prior_errors < 3 => {
                    prior_errors += 1
                }
                Err(e) => panic!("owned ICMP probe failed: {e}"),
            }
        }
        let mut poll = libc::pollfd {
            fd: descriptor.as_raw_fd(),
            events: libc::POLLERR,
            revents: 0,
        };
        assert_eq!(unsafe { libc::poll(&mut poll, 1, 100) }, 1);
        assert_ne!(poll.revents & libc::POLLERR, 0);
        drop(descriptor);
        for _ in 1..600 {
            receiver.receive(&media, marker).await;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let stats = state.stats();
        assert_ne!(stats["status"], "failed");
        assert!(stats["last_error"].is_null());
        attempts.push(json!({"attempt":attempt,"pending_icmp_confirmed":true,"prior_errors_drained":prior_errors,"stats":stats}));
        if state.stats()["unreachable_packets"].as_u64().unwrap() > 0 {
            break;
        }
    }
    let refused = state.stats()["unreachable_packets"].as_u64().unwrap();
    assert!(
        refused > 0,
        "RTCP send-refusal branch not observed within finite profile"
    );
    let control = UdpSocket::bind((address.ip(), address.port() + 1))
        .await
        .unwrap();
    queue.send(Bytes::from(ts(0x77, 1))).unwrap();
    receiver.receive(&media, 0x77).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(state.stats()["status"], "sending");
    let (fresh_report, peer) = wire(&control).await;
    assert_eq!(peer, control_source);
    let (_, fresh_index) = receiver.control(&fresh_report);
    if let (Some(first), Some(fresh)) = (first_index, fresh_index) {
        // The retained context accepts an increasing index across the outage.
        // A gap alone cannot identify which particular protected send was refused.
        assert!(fresh > first, "SRTCP index did not advance");
        assert!(
            receiver
                .cipher
                .as_mut()
                .unwrap()
                .unprotect(&mut first_report.clone(), true)
                .is_err()
        );
    }
    let final_stats = state.stats();
    owner.stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), &mut owner.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.stats()["status"], "stopped");
    assert!(UdpSocket::bind(source).await.is_ok());
    assert!(UdpSocket::bind(control_source).await.is_ok());
    json!({"secure":secure,"passed":true,"elapsed_seconds":clock.elapsed().as_secs_f64(),"healthy_rtp_packets":receiver.packets,"healthy_sequence_gaps":0,"sender_ssrc":receiver.ssrc,"media_source":source,"control_source":control_source,"rtcp_refused_sends":refused,"first_srtcp_index":first_index,"recovered_srtcp_index":fresh_index,"attempts":attempts,"final_stats":final_stats,"cancelled_and_sender_sockets_released":true})
}
// Linux connected UDP exposes real ICMP refusal through POLLERR. The probe
// duplicates only this test process's own validated RTCP sender descriptor.
// State's report and error-receive branches race, so counted send refusal is not
// guaranteed within the finite attempts. Run this qualification explicitly;
// exhausting attempts fails rather than silently claiming branch coverage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit Linux ICMP scheduling qualification on owned loopback sockets"]
async fn rtcp_refusal_does_not_interrupt_healthy_mp2t_media() {
    let result = tokio::time::timeout(Duration::from_secs(90), case(false))
        .await
        .expect("90-second total RTCP case deadline");
    assert_eq!(result["healthy_sequence_gaps"], 0);
    eprintln!("RTCP recovery qualification: {result}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit Linux ICMP scheduling qualification on owned loopback sockets"]
async fn srtcp_refusal_preserves_media_and_authenticated_control_context() {
    let result = tokio::time::timeout(Duration::from_secs(90), case(true))
        .await
        .expect("90-second total SRTCP case deadline");
    assert_eq!(result["healthy_sequence_gaps"], 0);
    eprintln!("RTCP recovery qualification: {result}");
}
