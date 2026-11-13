//! Bounded concurrent HTTPS handshakes; the application receives actual socket peers.
use crate::server::{App, router};
use axum::serve::ListenerExt;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, rustls, server::TlsStream};
use tokio_util::sync::CancellationToken;

/// This extension is installed only by the daemon's TLS listener, never from headers.
#[derive(Clone, Copy)]
pub(crate) struct SecureHttp;

type Handshake =
    Pin<Box<dyn Future<Output = io::Result<(TlsStream<TcpStream>, SocketAddr)>> + Send>>;
const MAX_PRETLS_PER_IP: usize = 16;
struct HandshakeGate {
    by_ip: Mutex<HashMap<IpAddr, usize>>,
}
impl HandshakeGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            by_ip: Mutex::new(HashMap::new()),
        })
    }
    fn acquire(self: &Arc<Self>, ip: IpAddr) -> Option<HandshakeLease> {
        let ip = match ip {
            IpAddr::V6(address) => address.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
            _ => ip,
        };
        let mut by_ip = self.by_ip.lock().unwrap_or_else(|e| e.into_inner());
        let count = by_ip.entry(ip).or_default();
        if *count >= MAX_PRETLS_PER_IP {
            return None;
        }
        *count += 1;
        Some(HandshakeLease {
            gate: self.clone(),
            ip,
        })
    }
}
struct HandshakeLease {
    gate: Arc<HandshakeGate>,
    ip: IpAddr,
}
impl Drop for HandshakeLease {
    fn drop(&mut self) {
        let mut by_ip = self.gate.by_ip.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = by_ip.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                by_ip.remove(&self.ip);
            }
        }
    }
}
pub struct Listener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
    pending: FuturesUnordered<Handshake>,
    gate: Arc<HandshakeGate>,
}
impl Listener {
    pub fn new(tcp: TcpListener, config: Arc<rustls::ServerConfig>) -> Self {
        let mut config = (*config).clone();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self {
            tcp,
            acceptor: TlsAcceptor::from(Arc::new(config)),
            pending: FuturesUnordered::new(),
            gate: HandshakeGate::new(),
        }
    }
}
impl axum::serve::Listener for Listener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, SocketAddr) {
        loop {
            tokio::select! {
                result = self.pending.next(), if !self.pending.is_empty() => {
                    if let Some(Ok(connection)) = result { return connection; }
                }
                result = self.tcp.accept(), if self.pending.len() < 128 => {
                    match result {
                        Ok((socket, address)) => {
                            let Some(lease) = self.gate.acquire(address.ip()) else {
                                continue;
                            };
                            let acceptor = self.acceptor.clone();
                            self.pending.push(Box::pin(async move {
                                let _lease = lease;
                                let stream = tokio::time::timeout(Duration::from_secs(5), acceptor.accept(socket)).await
                                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake expired"))??;
                                Ok((stream, address))
                            }));
                        }
                        Err(error) => {
                            tracing::warn!(kind=?error.kind(), "HTTPS accept failed");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }
}
pub async fn serve(
    tcp: TcpListener,
    config: Arc<rustls::ServerConfig>,
    app: Arc<App>,
    cancel: CancellationToken,
) -> io::Result<()> {
    // Axum's TapIo Connected implementation retains the listener's SocketAddr.
    let listener = Listener::new(tcp, config).tap_io(|stream| {
        let _ = stream.get_ref().0.set_nodelay(true);
    });
    let policy = crate::http_transport::TransportPolicy {
        secure: true,
        allow_insecure_remote_management: app.options.allow_insecure_remote_management,
    };
    crate::http_transport::serve_listener(
        listener,
        router(app).layer(axum::Extension(SecureHttp)),
        cancel,
        policy,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::serve::Listener as _;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::{io::AsyncReadExt, net::TcpSocket};

    #[test]
    fn pre_tls_gate_limits_canonical_peer_and_releases_every_slot() {
        let gate = HandshakeGate::new();
        let first = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mapped: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        let second: IpAddr = "127.0.0.2".parse().unwrap();
        let mut held = (0..MAX_PRETLS_PER_IP)
            .map(|_| gate.acquire(first).unwrap())
            .collect::<Vec<_>>();
        assert!(gate.acquire(mapped).is_none());
        let other = gate.acquire(second).expect("another peer has capacity");
        drop(held.pop());
        assert!(gate.acquire(mapped).is_some());
        drop(held);
        drop(other);
        assert!(gate.by_ip.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stalled_peer_cannot_fill_every_https_handshake_slot() {
        let cert = crate::transport_tls_fixture::Certificates::new();
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let mut listener = Listener::new(tcp, cert.server());
        let gate = listener.gate.clone();
        let accepted = tokio::spawn(async move { listener.accept().await });
        let mut stalled = Vec::new();
        for _ in 0..MAX_PRETLS_PER_IP {
            stalled.push(TcpStream::connect(address).await.unwrap());
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if gate
                    .by_ip
                    .lock()
                    .unwrap()
                    .get(&IpAddr::V4(Ipv4Addr::LOCALHOST))
                    == Some(&MAX_PRETLS_PER_IP)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stalled handshakes did not enter admission");
        let mut excess = TcpStream::connect(address).await.unwrap();
        let mut byte = [0];
        let n = tokio::time::timeout(Duration::from_secs(2), excess.read(&mut byte))
            .await
            .expect("excess handshake stayed open");
        assert!(matches!(n, Ok(0) | Err(_)));

        let second = TcpSocket::new_v4().unwrap();
        second.bind("127.0.0.2:0".parse().unwrap()).unwrap();
        let socket = second.connect(address).await.unwrap();
        let client = tokio_rustls::TlsConnector::from(cert.client());
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let peer = tokio::time::timeout(Duration::from_secs(2), client.connect(name, socket))
            .await
            .expect("second peer waited for stalled handshakes")
            .unwrap();
        let (server, remote) = accepted.await.unwrap();
        assert_eq!(remote.ip(), "127.0.0.2".parse::<IpAddr>().unwrap());
        drop(peer);
        drop(server);
        drop(stalled);
        assert!(gate.by_ip.lock().unwrap().is_empty());
    }
}
