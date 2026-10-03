//! Bounded concurrent HTTPS handshakes; the application receives actual socket peers.
use crate::server::{App, router};
use axum::serve::ListenerExt;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, rustls, server::TlsStream};
use tokio_util::sync::CancellationToken;

/// This extension is installed only by the daemon's TLS listener, never from headers.
#[derive(Clone, Copy)]
pub(crate) struct SecureHttp;

type Handshake =
    Pin<Box<dyn Future<Output = io::Result<(TlsStream<TcpStream>, SocketAddr)>> + Send>>;
pub struct Listener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
    pending: FuturesUnordered<Handshake>,
}
impl Listener {
    pub fn new(tcp: TcpListener, config: Arc<rustls::ServerConfig>) -> Self {
        let mut config = (*config).clone();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self {
            tcp,
            acceptor: TlsAcceptor::from(Arc::new(config)),
            pending: FuturesUnordered::new(),
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
                            let acceptor = self.acceptor.clone();
                            self.pending.push(Box::pin(async move {
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
    axum::serve(
        listener,
        router(app)
            .layer(axum::Extension(SecureHttp))
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(cancel.cancelled_owned())
    .await
}
