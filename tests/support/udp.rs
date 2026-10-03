use flussonix::rtsp::udp::PortRange;
use std::sync::atomic::{AtomicU32, Ordering};

// Disjoint ranges survive the reserve/drop/Pool::bind handoff. Stay below
// Linux's default ephemeral range so concurrent FFmpeg clients cannot steal them.
static NEXT_PORT: AtomicU32 = AtomicU32::new(20000);
fn ip() -> std::net::IpAddr {
    "127.0.0.1".parse().unwrap()
}
pub fn reserved(count: u16) -> (PortRange, Vec<std::net::UdpSocket>) {
    assert!((2..=256).contains(&count) && count % 2 == 0);
    for _ in 0..200 {
        let next = NEXT_PORT.fetch_add(count as u32, Ordering::Relaxed);
        assert!(next + count as u32 <= 30000, "test UDP range exhausted");
        let base = next as u16;
        let mut sockets = Vec::new();
        for p in base..base + count {
            if let Ok(s) = std::net::UdpSocket::bind((ip(), p)) {
                sockets.push(s)
            } else {
                break;
            }
        }
        if sockets.len() == count as usize {
            return (
                format!("{base}-{}", base + count - 1).parse().unwrap(),
                sockets,
            );
        }
    }
    panic!("no owned UDP range available")
}
