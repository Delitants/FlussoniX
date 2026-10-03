//! Prebound UDP pairs are owned by the RTSP daemon and leased to one authorized track.
use super::protocol::ClientPorts;
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::net::UdpSocket;
#[derive(Clone, Copy, Debug)]
pub struct PortRange {
    first: u16,
    last: u16,
}
impl FromStr for PortRange {
    type Err = String;
    fn from_str(v: &str) -> Result<Self, String> {
        let error = || {
            "UDP ports require an inclusive even/odd range of 2..256 ports, all >=1024".to_string()
        };
        let (a, b) = v.split_once('-').ok_or_else(error)?;
        if a.is_empty() || b.is_empty() || !a.bytes().chain(b.bytes()).all(|c| c.is_ascii_digit()) {
            return Err(error());
        }
        let first: u16 = a.parse().map_err(|_| error())?;
        let last: u16 = b.parse().map_err(|_| error())?;
        if first < 1024
            || first % 2 != 0
            || last % 2 != 1
            || last < first
            || last as u32 - first as u32 + 1 > 256
        {
            return Err(error());
        }
        Ok(Self { first, last })
    }
}
impl fmt::Display for PortRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.first, self.last)
    }
}
struct Pair {
    rtp: std::net::UdpSocket,
    rtcp: std::net::UdpSocket,
}
struct ActivePair {
    rtp: UdpSocket,
    rtcp: UdpSocket,
}
pub struct Pool {
    idle: Mutex<Vec<Pair>>,
}
pub struct Lease {
    pair: Option<Pair>,
    active: Option<ActivePair>,
    pool: Arc<Pool>,
    peer_rtcp: SocketAddr,
}
impl Pool {
    /// Keep every successfully bound socket; failure drops all preceding bindings.
    pub async fn bind(ip: IpAddr, range: PortRange) -> std::io::Result<Arc<Self>> {
        let mut pairs = Vec::new();
        for port in (range.first..=range.last).step_by(2) {
            let rtp = std::net::UdpSocket::bind((ip, port))?;
            rtp.set_nonblocking(true)?;
            let rtcp = std::net::UdpSocket::bind((ip, port + 1))?;
            rtcp.set_nonblocking(true)?;
            pairs.push(Pair { rtp, rtcp });
        }
        Ok(Arc::new(Self {
            idle: Mutex::new(pairs),
        }))
    }
    pub async fn lease(
        self: &Arc<Self>,
        peer: IpAddr,
        ports: ClientPorts,
    ) -> std::io::Result<Lease> {
        if !ports.valid()
            || peer.is_unspecified()
            || peer.is_multicast()
            || peer == IpAddr::V4(std::net::Ipv4Addr::BROADCAST)
        {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let pair = self
            .idle
            .lock()
            .unwrap()
            .pop()
            .ok_or(std::io::ErrorKind::WouldBlock)?;
        let peer_rtcp = SocketAddr::new(peer, ports.rtcp);
        let setup = || -> std::io::Result<ActivePair> {
            pair.rtp.connect(SocketAddr::new(peer, ports.rtp))?;
            pair.rtcp.connect(peer_rtcp)?;
            let mut buffer = [0; 8193];
            for socket in [&pair.rtp, &pair.rtcp] {
                let _ = socket.take_error()?;
                // Raw nonblocking reads observe queued kernel bytes without waiting for reactor readiness.
                for _ in 0..64 {
                    if socket.recv_from(&mut buffer).is_err() {
                        break;
                    }
                }
            }
            Ok(ActivePair {
                rtp: UdpSocket::from_std(pair.rtp.try_clone()?)?,
                rtcp: UdpSocket::from_std(pair.rtcp.try_clone()?)?,
            })
        };
        match setup() {
            Ok(active) => Ok(Lease {
                pair: Some(pair),
                active: Some(active),
                pool: self.clone(),
                peer_rtcp,
            }),
            Err(error) => {
                self.idle.lock().unwrap().push(pair);
                Err(error)
            }
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.active.take());
        if let Some(pair) = self.pair.take() {
            self.pool.idle.lock().unwrap().push(pair);
        }
    }
}
impl Lease {
    pub fn server_ports(&self) -> (u16, u16) {
        let p = self.pair.as_ref().unwrap();
        (
            p.rtp.local_addr().unwrap().port(),
            p.rtcp.local_addr().unwrap().port(),
        )
    }
    pub fn source_ip(&self) -> IpAddr {
        self.pair.as_ref().unwrap().rtp.local_addr().unwrap().ip()
    }
    pub async fn send_rtp(&self, body: &[u8]) -> std::io::Result<usize> {
        self.active.as_ref().unwrap().rtp.send(body).await
    }
    pub async fn send_rtcp(&self, body: &[u8]) -> std::io::Result<usize> {
        self.active.as_ref().unwrap().rtcp.send(body).await
    }
    pub async fn recv_rtcp(&self, body: &mut [u8]) -> std::io::Result<Option<usize>> {
        let (n, source) = self.active.as_ref().unwrap().rtcp.recv_from(body).await?;
        Ok((source == self.peer_rtcp && n <= 8192).then_some(n))
    }
}
/// Receiver reports may extend keepalive; they never change the destination or grant.
pub fn valid_receiver_report(body: &[u8], ssrc: u32) -> bool {
    if body.len() > 8192 || body.len() < 8 || body[1] != 201 {
        return false;
    }
    let mut cursor = 0;
    let mut rr = false;
    let mut cname = false;
    let sender = u32::from_be_bytes(body[4..8].try_into().unwrap());
    while cursor < body.len() {
        let Some(h) = body.get(cursor..cursor + 4) else {
            return false;
        };
        if h[0] >> 6 != 2 || !(192..=223).contains(&h[1]) {
            return false;
        }
        let len = (u16::from_be_bytes([h[2], h[3]]) as usize + 1) * 4;
        let Some(packet) = body.get(cursor..cursor + len) else {
            return false;
        };
        let mut payload = packet.len();
        if h[0] & 32 != 0 {
            let pad = *packet.last().unwrap() as usize;
            if cursor + len != body.len() || pad == 0 || pad > payload - 4 {
                return false;
            }
            payload -= pad;
        }
        if h[1] == 201 {
            let count = (h[0] & 31) as usize;
            if payload != 8 + 24 * count {
                return false;
            }
            if count > 0
                && !(0..count).any(|i| {
                    u32::from_be_bytes(packet[8 + i * 24..12 + i * 24].try_into().unwrap()) == ssrc
                })
            {
                return false;
            }
            rr = true;
        } else if h[1] == 202 {
            let mut at = 4;
            for _ in 0..(h[0] & 31) {
                let Some(id) = packet.get(at..at + 4).filter(|_| at + 4 <= payload) else {
                    return false;
                };
                let id = u32::from_be_bytes(id.try_into().unwrap());
                at += 4;
                loop {
                    let Some(&kind) = packet.get(at).filter(|_| at < payload) else {
                        return false;
                    };
                    at += 1;
                    if kind == 0 {
                        break;
                    }
                    let Some(&n) = packet.get(at).filter(|_| at < payload) else {
                        return false;
                    };
                    at += 1;
                    if at + n as usize > payload {
                        return false;
                    }
                    if kind == 1 && n > 0 && id == sender {
                        cname = true;
                    }
                    at += n as usize;
                }
                while at % 4 != 0 {
                    if packet.get(at) != Some(&0) || at >= payload {
                        return false;
                    }
                    at += 1;
                }
            }
            if at != payload {
                return false;
            }
        } else {
            return false;
        }
        cursor += len;
    }
    rr && cname
}

/// DTS spacing plus a token bucket bounds bootstrap/FU-A bursts without sleeping per packet.
pub struct Pacer {
    rate: f64,
    tokens: f64,
    refill: Option<tokio::time::Instant>,
    origin: Option<(u64, tokio::time::Instant)>,
}
impl Pacer {
    pub fn new(mbps: f64) -> Result<Self, String> {
        if !mbps.is_finite() || !(1.0..=10000.0).contains(&mbps) {
            return Err("UDP rate must be finite 1..10000 Mbps".into());
        }
        Ok(Self {
            rate: mbps * 125000.0,
            tokens: 32768.0,
            refill: None,
            origin: None,
        })
    }
    fn refill(&mut self, now: tokio::time::Instant) {
        if let Some(at) = self.refill {
            self.tokens = (self.tokens
                + now.saturating_duration_since(at).as_secs_f64() * self.rate)
                .min(32768.0);
        }
        self.refill = Some(now);
    }
    pub fn ready_at(
        &mut self,
        dts: u64,
        bytes: usize,
        now: tokio::time::Instant,
    ) -> std::io::Result<tokio::time::Instant> {
        if bytes == 0 || bytes > 32768 {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        self.refill(now);
        let (origin, at) = *self.origin.get_or_insert((dts, now));
        let delta = dts.saturating_sub(origin);
        let spacing = std::time::Duration::new(
            delta / 90000,
            ((delta % 90000) * 1_000_000_000 / 90000) as u32,
        );
        let media = at
            .checked_add(spacing)
            .ok_or(std::io::ErrorKind::InvalidData)?;
        let wait = ((bytes as f64 - self.tokens) / self.rate).max(0.0);
        let bucket = now
            .checked_add(std::time::Duration::from_secs_f64(wait))
            .ok_or(std::io::ErrorKind::InvalidData)?;
        Ok(media.max(bucket).max(now))
    }
    pub fn sent(&mut self, bytes: usize, now: tokio::time::Instant) {
        self.refill(now);
        self.tokens = (self.tokens - bytes as f64).max(0.0);
    }
}
