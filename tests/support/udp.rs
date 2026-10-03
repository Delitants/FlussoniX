use flussonix::rtsp::udp::PortRange;
fn ip() -> std::net::IpAddr {
    "127.0.0.1".parse().unwrap()
}
pub fn reserved(count: u16) -> (PortRange, Vec<std::net::UdpSocket>) {
    for _ in 0..200 {
        let first = std::net::UdpSocket::bind((ip(), 0)).unwrap();
        let base = first.local_addr().unwrap().port() & !1;
        drop(first);
        if base < 1024 || base as u32 + count as u32 > 65536 {
            continue;
        }
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
