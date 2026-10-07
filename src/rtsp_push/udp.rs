//! Per-attempt endpoint-bound UDP pairs. No server-directed address changes.
use crate::rtsp::protocol::ClientPorts;
use std::{
    collections::HashSet,
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tokio::net::UdpSocket;
fn bad() -> io::Error {
    io::Error::other("invalid RTSP UDP transport")
}
fn ports(value: &str) -> io::Result<ClientPorts> {
    let (a, b) = value.split_once('-').ok_or_else(bad)?;
    if a.is_empty() || b.is_empty() || !a.bytes().chain(b.bytes()).all(|c| c.is_ascii_digit()) {
        return Err(bad());
    }
    let p = ClientPorts {
        rtp: a.parse().map_err(|_| bad())?,
        rtcp: b.parse().map_err(|_| bad())?,
    };
    if !p.valid() {
        return Err(bad());
    }
    Ok(p)
}
pub(super) fn response(value: &str, client: ClientPorts, peer: IpAddr) -> io::Result<ClientPorts> {
    let mut parts = value.split(';');
    if !parts.next().is_some_and(|v| {
        ["RTP/AVP", "RTP/AVP/UDP"]
            .iter()
            .any(|p| v.trim().eq_ignore_ascii_case(p))
    }) {
        return Err(bad());
    }
    let mut seen = HashSet::new();
    let (mut unicast, mut echoed, mut server) = (false, None, None);
    for part in parts {
        let (key, value) = part.trim().split_once('=').unwrap_or((part.trim(), ""));
        let key = key.to_ascii_lowercase();
        if !seen.insert(key.clone()) {
            return Err(bad());
        }
        match key.as_str() {
            "unicast" if value.is_empty() => unicast = true,
            "mode"
                if ["record", "receive"].iter().any(|m| {
                    value.eq_ignore_ascii_case(m) || value.eq_ignore_ascii_case(&format!("\"{m}\""))
                }) => {}
            "client_port" => echoed = Some(ports(value)?),
            "server_port" => server = Some(ports(value)?),
            "source" | "destination" if value.parse::<IpAddr>().ok() == Some(peer) => {}
            "ssrc" if value.len() == 8 && value.bytes().all(|b| b.is_ascii_hexdigit()) => {}
            _ => return Err(bad()),
        }
    }
    if !unicast || echoed != Some(client) {
        return Err(bad());
    }
    server.ok_or_else(bad)
}
pub(super) struct Pair {
    rtp: UdpSocket,
    pub rtcp: Arc<UdpSocket>,
    pub remote: Option<ClientPorts>,
    ports: ClientPorts,
    egress: Arc<std::sync::atomic::AtomicU64>,
    drain_rtp: std::net::UdpSocket,
    drain_rtcp: std::net::UdpSocket,
}
impl Pair {
    pub async fn bind(
        local: IpAddr,
        egress: Arc<std::sync::atomic::AtomicU64>,
    ) -> io::Result<Self> {
        for _ in 0..128 {
            let rtp = UdpSocket::bind(SocketAddr::new(local, 0)).await?;
            let port = rtp.local_addr()?.port();
            let ports = ClientPorts {
                rtp: port,
                rtcp: port.saturating_add(1),
            };
            if !ports.valid() {
                continue;
            }
            if let Ok(rtcp) = UdpSocket::bind(SocketAddr::new(local, ports.rtcp)).await {
                let raw = rtp.into_std()?;
                let drain_rtp = raw.try_clone()?;
                let rtp = UdpSocket::from_std(raw)?;
                let raw = rtcp.into_std()?;
                let drain_rtcp = raw.try_clone()?;
                let rtcp = UdpSocket::from_std(raw)?;
                return Ok(Self {
                    egress,
                    drain_rtp,
                    drain_rtcp,
                    rtp,
                    rtcp: Arc::new(rtcp),
                    remote: None,
                    ports,
                });
            }
        }
        Err(io::ErrorKind::AddrInUse.into())
    }
    pub fn ports(&self) -> ClientPorts {
        self.ports
    }
    pub async fn connect(&mut self, peer: IpAddr, ports: ClientPorts) -> io::Result<()> {
        self.rtp.connect(SocketAddr::new(peer, ports.rtp)).await?;
        self.rtcp.connect(SocketAddr::new(peer, ports.rtcp)).await?;
        self.remote = Some(ports);
        Ok(())
    }
    pub fn drain(&self) -> io::Result<()> {
        for socket in [&self.drain_rtp, &self.drain_rtcp] {
            let mut data = [0; 2049];
            for i in 0..=64 {
                match socket.recv(&mut data) {
                    Ok(_) if i < 64 => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    _ => return Err(bad()),
                }
            }
        }
        Ok(())
    }
    pub async fn send(&self, rtcp: bool, body: &[u8]) -> io::Result<()> {
        if body.is_empty() || body.len() > 8192 {
            return Err(bad());
        }
        let socket = if rtcp { self.rtcp.as_ref() } else { &self.rtp };
        if socket.send(body).await? != body.len() {
            return Err(bad());
        }
        self.egress
            .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receiver_transport_binds_echoed_ports_and_control_peer() {
        let local = ClientPorts {
            rtp: 4000,
            rtcp: 4001,
        };
        let peer = "127.0.0.1".parse().unwrap();
        for value in [
            "RTP/AVP;unicast;client_port=4000-4001;server_port=6000-6001",
            "RTP/AVP/UDP;unicast;client_port=4000-4001;server_port=6000-6001;source=127.0.0.1;mode=\"receive\";ssrc=abcd0001",
        ] {
            assert_eq!(
                response(value, local, peer).unwrap(),
                ClientPorts {
                    rtp: 6000,
                    rtcp: 6001
                }
            );
        }
        for tail in [
            "",
            ";server_port=6000-6001;server_port=6002-6003",
            ";server_port=6001-6002",
            ";server_port=80-81",
            ";server_port=6000-6001;destination=127.0.0.2",
            ";server_port=6000-6001;source=example.org",
            ";server_port=6000-6001;mode=play",
            ";server_port=6000-6001;interleaved=0-1",
            ";server_port=6000-6001;multicast",
            ";server_port=6000-6001;ssrc=1",
        ] {
            assert!(
                response(
                    &format!("RTP/AVP;unicast;client_port=4000-4001{tail}"),
                    local,
                    peer
                )
                .is_err(),
                "{tail}"
            );
        }
        assert!(
            response(
                "RTP/AVP;unicast;client_port=4002-4003;server_port=6000-6001",
                local,
                peer
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn pair_filters_foreign_feedback_drains_real_queue_and_releases_both_ports() {
        let egress = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut pair = Pair::bind("127.0.0.1".parse().unwrap(), egress.clone())
            .await
            .unwrap();
        let ports = pair.ports();
        let receiver = Pair::bind(
            "127.0.0.1".parse().unwrap(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        )
        .await
        .unwrap();
        pair.connect("127.0.0.1".parse().unwrap(), receiver.ports())
            .await
            .unwrap();
        let foreign = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        foreign
            .send_to(b"foreign", ("127.0.0.1", ports.rtcp))
            .await
            .unwrap();
        receiver
            .rtcp
            .send_to(b"stale", ("127.0.0.1", ports.rtcp))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        pair.drain().unwrap();
        receiver
            .rtcp
            .send_to(b"fresh", ("127.0.0.1", ports.rtcp))
            .await
            .unwrap();
        let mut data = [0; 32];
        let n = tokio::time::timeout(std::time::Duration::from_secs(1), pair.rtcp.recv(&mut data))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&data[..n], b"fresh");
        pair.send(false, b"owned").await.unwrap();
        let (n, source) = receiver.rtp.recv_from(&mut data).await.unwrap();
        assert_eq!(&data[..n], b"owned");
        assert_eq!(source.port(), ports.rtp);
        assert_eq!(egress.load(std::sync::atomic::Ordering::Relaxed), 5);
        let report = [0x80, 201, 0, 1, 0, 0, 0, 1];
        pair.send(true, &report).await.unwrap();
        let (n, source) = receiver.rtcp.recv_from(&mut data).await.unwrap();
        assert_eq!(&data[..n], &report);
        assert_eq!(source.port(), ports.rtcp);
        assert_eq!(egress.load(std::sync::atomic::Ordering::Relaxed), 13);
        drop(pair);
        let a = UdpSocket::bind(("127.0.0.1", ports.rtp)).await.unwrap();
        let b = UdpSocket::bind(("127.0.0.1", ports.rtcp)).await.unwrap();
        drop((a, b));
    }
}
