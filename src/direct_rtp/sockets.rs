use super::config::Settings;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
        use std::os::fd::AsRawFd;
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
