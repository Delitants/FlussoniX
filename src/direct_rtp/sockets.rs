use super::config::Settings;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::AsRawFd;
use tokio::net::UdpSocket;
pub struct Pair {
    pub rtp: UdpSocket,
    pub rtcp: UdpSocket,
}
fn socket(address: SocketAddr, cfg: &Settings, receive: bool) -> std::io::Result<UdpSocket> {
    let s = std::net::UdpSocket::bind(address)?;
    s.set_nonblocking(true)?;
    if cfg.address.ip().is_multicast() {
        let IpAddr::V4(group) = cfg.address.ip() else {
            return Err(std::io::Error::other("IPv6 multicast is not implemented"));
        };
        if receive {
            s.join_multicast_v4(&group, &cfg.interface.unwrap())?;
        }
        s.set_multicast_ttl_v4(cfg.ttl)?;
        s.set_multicast_loop_v4(true)?;
        let interface = libc::in_addr {
            s_addr: u32::from_ne_bytes(cfg.interface.unwrap().octets()),
        };
        // The pointer and size describe one initialized in_addr for this syscall.
        if unsafe {
            libc::setsockopt(
                s.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_MULTICAST_IF,
                (&interface as *const libc::in_addr).cast(),
                std::mem::size_of_val(&interface) as libc::socklen_t,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    UdpSocket::from_std(s)
}
fn request_receive_buffer(socket: &UdpSocket, bytes: u32) -> Result<(), &'static str> {
    let requested = bytes as libc::c_int;
    // The option reads one initialized integer from this stack slot.
    if unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&requested as *const libc::c_int).cast(),
            std::mem::size_of_val(&requested) as libc::socklen_t,
        )
    } != 0
    {
        return Err("RTP input receive buffer unavailable");
    }
    let mut observed: libc::c_int = 0;
    let mut len = std::mem::size_of_val(&observed) as libc::socklen_t;
    // The kernel writes one integer into the owned local slot.
    if unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&mut observed as *mut libc::c_int).cast(),
            &mut len,
        )
    } != 0
        || len as usize != std::mem::size_of_val(&observed)
        || observed <= 0
    {
        return Err("RTP input receive buffer unavailable");
    }
    let effective = if cfg!(target_os = "linux") {
        observed as u64 / 2
    } else {
        observed as u64
    };
    if effective < u64::from(bytes) {
        return Err("RTP input receive buffer below requested size");
    }
    Ok(())
}
impl Pair {
    pub fn receive(cfg: &Settings) -> Result<Self, String> {
        // Binding the configured group isolates destination addresses and permits
        // distinct IPTV groups on the same port without socket reuse. Wildcard
        // membership also receives unrelated unicast traffic on Linux.
        let ip = if cfg.address.ip().is_multicast() {
            cfg.address.ip()
        } else {
            cfg.interface.map(IpAddr::V4).unwrap_or(cfg.address.ip())
        };
        let rtp = socket(SocketAddr::new(ip, cfg.address.port()), cfg, true)
            .map_err(|_| "RTP input port unavailable")?;
        if let Some(bytes) = cfg.receive_buffer_bytes {
            request_receive_buffer(&rtp, bytes).map_err(str::to_owned)?;
        }
        let rtcp = socket(SocketAddr::new(ip, cfg.address.port() + 1), cfg, true)
            .map_err(|_| "RTCP input port unavailable")?;
        Ok(Self { rtp, rtcp })
    }
    pub async fn send(cfg: &Settings) -> Result<Self, String> {
        let ip = if cfg.address.is_ipv4() {
            IpAddr::V4(cfg.interface.unwrap_or(Ipv4Addr::UNSPECIFIED))
        } else {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        };
        for _ in 0..64 {
            let rtp =
                socket(SocketAddr::new(ip, 0), cfg, false).map_err(|_| "RTP output bind failed")?;
            let port = rtp
                .local_addr()
                .map_err(|_| "RTP output bind failed")?
                .port();
            if port == 65535 {
                continue;
            }
            let Ok(rtcp) = socket(SocketAddr::new(ip, port + 1), cfg, false) else {
                continue;
            };
            rtp.connect(cfg.address)
                .await
                .map_err(|_| "RTP destination unavailable")?;
            // Multicast reports go to the group, but receiver feedback is unicast
            // to this source port. Connecting to the group would filter it out.
            if !cfg.address.ip().is_multicast() {
                rtcp.connect(SocketAddr::new(cfg.address.ip(), cfg.address.port() + 1))
                    .await
                    .map_err(|_| "RTCP destination unavailable")?;
            }
            return Ok(Self { rtp, rtcp });
        }
        Err("RTP output pair unavailable".into())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod receive_buffer_tests {
    use super::*;
    use serde_json::json;
    use std::os::fd::AsRawFd;

    fn available_pair() -> u16 {
        for _ in 0..64 {
            let first = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let port = first.local_addr().unwrap().port();
            if port < 65535 && std::net::UdpSocket::bind(("127.0.0.1", port + 1)).is_ok() {
                return port;
            }
        }
        panic!("no owned RTP/RTCP pair");
    }

    fn raw_receive_buffer(socket: &UdpSocket) -> libc::c_int {
        let mut size = 0;
        let mut len = std::mem::size_of_val(&size) as libc::socklen_t;
        // The kernel writes one initialized integer into the owned local slot.
        assert_eq!(
            unsafe {
                libc::getsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    (&mut size as *mut libc::c_int).cast(),
                    &mut len,
                )
            },
            0
        );
        assert_eq!(len as usize, std::mem::size_of_val(&size));
        size
    }

    #[tokio::test]
    async fn requested_buffer_changes_public_rtp_socket_capacity() {
        let default_port = available_pair();
        let default_cfg = Settings::parse(&json!({
            "url": format!("rtp://127.0.0.1:{default_port}")
        }))
        .unwrap();
        let default_pair = Pair::receive(&default_cfg).unwrap();
        let default_capacity = raw_receive_buffer(&default_pair.rtp);
        assert_eq!(raw_receive_buffer(&default_pair.rtcp), default_capacity);

        let port = available_pair();
        let cfg = Settings::parse(&json!({
            "url": format!("rtp://127.0.0.1:{port}"),
            "flussonix_rtp": {"receive_buffer_bytes": 65536}
        }))
        .unwrap();
        let pair = Pair::receive(&cfg).unwrap();
        assert_eq!(raw_receive_buffer(&pair.rtp), 131072);
        assert_eq!(raw_receive_buffer(&pair.rtcp), default_capacity);
    }

    #[tokio::test]
    async fn kernel_capped_request_fails_and_releases_public_ports() {
        let host_limit: u64 = std::fs::read_to_string("/proc/sys/net/core/rmem_max")
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        if host_limit >= 33554432 {
            // This host can actually satisfy the maximum allowed request.
            return;
        }
        let port = available_pair();
        let cfg = Settings::parse(&json!({
            "url": format!("srtp://127.0.0.1:{port}"),
            "flussonix_rtp": {"receive_buffer_bytes": 33554432, "key_file": "/tmp/owned.key"}
        }))
        .unwrap();
        let error = match Pair::receive(&cfg) {
            Ok(_) => panic!("accepted kernel-capped RTP buffer"),
            Err(error) => error,
        };
        assert!(error.contains("receive buffer"), "{error}");
        let rtp = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
        let rtcp = std::net::UdpSocket::bind(("127.0.0.1", port + 1)).unwrap();
        drop((rtp, rtcp));
    }
}
