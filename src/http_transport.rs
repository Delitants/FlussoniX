//! Connection-scoped interruption of cancelled native HTTP/1 TS delivery.
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request},
    middleware::{Next, from_fn},
    response::Response,
    serve::Listener,
};
use hyper::{body::Incoming, server::conn::http1::Builder, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tower::Service;

#[cfg(test)]
const HEADER_READ_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(not(test))]
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ESTABLISHED_CONNECTIONS: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reason {
    Authorization = 1,
    Worker = 2,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct Control {
    abort: CancellationToken,
    active: Arc<Mutex<Option<Arc<()>>>>,
}
pub(crate) struct Watch {
    control: Control,
    identity: Arc<()>,
    reason: Arc<AtomicU8>,
    task: JoinHandle<()>,
    registered: bool,
}
impl Control {
    #[cfg(test)]
    pub(crate) fn is_aborted(&self) -> bool {
        self.abort.is_cancelled()
    }
    fn abort_if_current(&self, identity: &Arc<()>, selected: &AtomicU8, reason: Reason) {
        // Eligibility and cancellation are atomic with unregister. A dropped
        // body cannot later reset a reused connection through a stale watcher.
        let active = self.active.lock().unwrap();
        if active.as_ref().is_some_and(|id| Arc::ptr_eq(id, identity)) {
            let _ = selected.compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire);
            self.abort.cancel();
        }
    }
    pub(crate) fn watch(&self, stopped: impl Future<Output = Reason> + Send + 'static) -> Watch {
        let identity = Arc::new(());
        *self.active.lock().unwrap() = Some(identity.clone());
        let reason = Arc::new(AtomicU8::new(0));
        let (control, expected, selected) = (self.clone(), identity.clone(), reason.clone());
        let task = tokio::spawn(async move {
            let reason = stopped.await;
            control.abort_if_current(&expected, &selected, reason);
        });
        Watch {
            control: self.clone(),
            identity,
            reason,
            task,
            registered: true,
        }
    }
}
impl Watch {
    pub(crate) fn abort(&self, reason: Reason) {
        self.control
            .abort_if_current(&self.identity, &self.reason, reason);
    }
    pub(crate) fn reason(&self) -> Option<Reason> {
        match self.reason.load(Ordering::Acquire) {
            1 => Some(Reason::Authorization),
            2 => Some(Reason::Worker),
            _ => None,
        }
    }
}
impl Watch {
    pub(crate) fn unregister(&mut self) {
        if !self.registered {
            return;
        }
        {
            let mut active = self.control.active.lock().unwrap();
            if active
                .as_ref()
                .is_some_and(|id| Arc::ptr_eq(id, &self.identity))
            {
                *active = None;
            }
        }
        self.registered = false;
        self.task.abort();
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        self.unregister();
    }
}
#[derive(Clone, Debug)]
struct Peer {
    address: SocketAddr,
    control: Control,
}
pub(crate) trait ResetSocket {
    fn reset_on_drop(&self) -> io::Result<()>;
}
impl ResetSocket for TcpStream {
    fn reset_on_drop(&self) -> io::Result<()> {
        self.set_zero_linger()
    }
}
impl ResetSocket for TlsStream<TcpStream> {
    fn reset_on_drop(&self) -> io::Result<()> {
        self.get_ref().0.set_zero_linger()
    }
}
struct Io<T> {
    inner: T,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    reset: bool,
}
impl<T: ResetSocket + Unpin> Io<T> {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            if !self.reset {
                self.inner.reset_on_drop()?;
                self.reset = true;
            }
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "media delivery cancelled",
            ));
        }
        Ok(())
    }
}
impl<T: AsyncRead + ResetSocket + Unpin> AsyncRead for Io<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + ResetSocket + Unpin> AsyncWrite for Io<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
struct Guarded<L>(L);
impl<L: Listener<Addr = SocketAddr>> Listener for Guarded<L>
where
    L::Io: ResetSocket,
{
    type Io = Io<L::Io>;
    type Addr = Peer;
    async fn accept(&mut self) -> (Self::Io, Peer) {
        let (inner, address) = self.0.accept().await;
        let control = Control::default();
        let cancelled = Box::pin(control.abort.clone().cancelled_owned());
        (
            Io {
                inner,
                cancelled,
                reset: false,
            },
            Peer { address, control },
        )
    }
    fn local_addr(&self) -> io::Result<Peer> {
        Ok(Peer {
            address: self.0.local_addr()?,
            control: Control::default(),
        })
    }
}
async fn context(
    ConnectInfo(peer): ConnectInfo<Peer>,
    mut request: Request,
    next: Next,
) -> Response {
    request.extensions_mut().insert(ConnectInfo(peer.address));
    request.extensions_mut().insert(peer.control);
    next.run(request).await
}
pub(crate) async fn serve_listener<L: Listener<Addr = SocketAddr>>(
    listener: L,
    router: Router,
    cancel: CancellationToken,
) -> io::Result<()>
where
    L::Io: ResetSocket,
{
    serve_listener_bounded(listener, router, cancel, MAX_ESTABLISHED_CONNECTIONS).await
}
async fn serve_listener_bounded<L: Listener<Addr = SocketAddr>>(
    listener: L,
    router: Router,
    cancel: CancellationToken,
    limit: usize,
) -> io::Result<()>
where
    L::Io: ResetSocket,
{
    assert!(limit > 0);
    let mut listener = Guarded(listener);
    let router = router.layer(from_fn(context));
    let mut tasks = JoinSet::new();
    let slots = Arc::new(Semaphore::new(limit));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    tracing::warn!(%error, "HTTP connection task failed");
                }
            }
            (io, peer) = listener.accept() => {
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    tracing::debug!(address=%peer.address, "HTTP connection limit reached");
                    drop(io);
                    continue;
                };
                let router = router.clone();
                let shutdown = cancel.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let service = service_fn(move |mut request: axum::http::Request<Incoming>| {
                        let mut router = router.clone();
                        let peer = peer.clone();
                        async move {
                            request.extensions_mut().insert(ConnectInfo(peer));
                            router.call(request.map(Body::new)).await
                        }
                    });
                    let mut builder = Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(HEADER_READ_TIMEOUT);
                    let mut connection = Box::pin(builder.serve_connection(TokioIo::new(io), service).with_upgrades());
                    let mut draining = false;
                    loop {
                        tokio::select! {
                            result = &mut connection => {
                                if let Err(error) = result {
                                    tracing::debug!(%error, "HTTP connection ended");
                                }
                                break;
                            }
                            _ = shutdown.cancelled(), if !draining => {
                                connection.as_mut().graceful_shutdown();
                                draining = true;
                            }
                        }
                    }
                });
            }
        }
    }
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            tracing::warn!(%error, "HTTP connection task failed");
        }
    }
    Ok(())
}
pub async fn serve(
    tcp: TcpListener,
    app: Arc<crate::server::App>,
    cancel: CancellationToken,
) -> io::Result<()> {
    serve_listener(tcp, crate::server::router(app), cancel).await
}

#[cfg(test)]
#[path = "http_transport_tests.rs"]
mod tests;
