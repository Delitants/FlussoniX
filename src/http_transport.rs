//! Connection-scoped interruption of cancelled native HTTP/1 TS delivery.
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    serve::Listener,
};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
    time::Sleep,
};
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reason {
    Authorization = 1,
    Worker = 2,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct Control {
    abort: CancellationToken,
    active: Arc<Mutex<Option<Arc<()>>>>,
    first_request: Arc<AtomicBool>,
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
    first_request: Arc<AtomicBool>,
    header_deadline: Option<Pin<Box<Sleep>>>,
    _reservation: Option<Reservation>,
}
impl<T: ResetSocket + Unpin> Io<T> {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if !self.first_request.load(Ordering::Acquire)
            && self
                .header_deadline
                .as_mut()
                .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
        {
            self.inner.reset_on_drop()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP request headers expired",
            ));
        }
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
struct Admission {
    slots: Arc<Semaphore>,
    by_ip: Arc<Mutex<HashMap<IpAddr, usize>>>,
    per_ip: usize,
}
struct Reservation {
    _slot: OwnedSemaphorePermit,
    by_ip: Arc<Mutex<HashMap<IpAddr, usize>>>,
    ip: IpAddr,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut by_ip = self.by_ip.lock().unwrap();
        if let Some(count) = by_ip.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                by_ip.remove(&self.ip);
            }
        }
    }
}
impl Admission {
    fn new(total: usize, per_ip: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(total)),
            by_ip: Arc::new(Mutex::new(HashMap::new())),
            per_ip,
        }
    }
    fn for_process() -> Arc<Self> {
        static SHARED: OnceLock<Arc<Admission>> = OnceLock::new();
        SHARED
            .get_or_init(|| {
                let mut limits = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                let soft_limit =
                    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } == 0 {
                        limits.rlim_cur.min(65_536) as usize
                    } else {
                        1024
                    };
                let total = (soft_limit / 2).clamp(32, 32_768);
                Arc::new(Self::new(total, (total * 3 / 4).max(1)))
            })
            .clone()
    }
    fn try_reserve(&self, ip: IpAddr) -> Option<Reservation> {
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let mut by_ip = self.by_ip.lock().unwrap();
        let count = by_ip.entry(ip).or_default();
        if *count >= self.per_ip {
            return None;
        }
        *count += 1;
        Some(Reservation {
            _slot: slot,
            by_ip: self.by_ip.clone(),
            ip,
        })
    }
}
struct Guarded<L>(L, Arc<Admission>);
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TransportPolicy {
    pub secure: bool,
    pub allow_insecure_remote_management: bool,
}

fn remote_management_request(request: &Request) -> bool {
    let path = request.uri().path();
    path == "/admin"
        || path.starts_with("/admin/")
        || path == "/streamer/api/v3"
        || path.starts_with("/streamer/api/v3/")
        || ((path == "/flussonix/api/v1" || path.starts_with("/flussonix/api/v1/"))
            && request.headers().contains_key(header::AUTHORIZATION))
}
impl<L: Listener<Addr = SocketAddr>> Listener for Guarded<L>
where
    L::Io: ResetSocket,
{
    type Io = Io<L::Io>;
    type Addr = Peer;
    async fn accept(&mut self) -> (Self::Io, Peer) {
        let (inner, address, reservation) = loop {
            let (inner, address) = self.0.accept().await;
            if let Some(reservation) = self.1.try_reserve(address.ip()) {
                break (inner, address, reservation);
            }
            drop(inner);
        };
        let control = Control::default();
        let cancelled = Box::pin(control.abort.clone().cancelled_owned());
        (
            Io {
                inner,
                cancelled,
                reset: false,
                first_request: control.first_request.clone(),
                header_deadline: Some(Box::pin(tokio::time::sleep(Duration::from_secs(5)))),
                _reservation: Some(reservation),
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
    State(policy): State<TransportPolicy>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    mut request: Request,
    next: Next,
) -> Response {
    peer.control.first_request.store(true, Ordering::Release);
    if !policy.secure
        && !policy.allow_insecure_remote_management
        && !peer.address.ip().is_loopback()
        && remote_management_request(&request)
    {
        return (
            StatusCode::FORBIDDEN,
            [(header::CACHE_CONTROL, "no-store")],
            "Remote management requires HTTPS or a local tunnel",
        )
            .into_response();
    }
    request.extensions_mut().insert(ConnectInfo(peer.address));
    request.extensions_mut().insert(peer.control);
    next.run(request).await
}
// Existing public TLS Listener and its handshake bounds remain unchanged.
pub(crate) async fn serve_listener<L: Listener<Addr = SocketAddr>>(
    listener: L,
    router: Router,
    cancel: CancellationToken,
    policy: TransportPolicy,
) -> io::Result<()>
where
    L::Io: ResetSocket,
{
    let mut listener = Guarded(listener, Admission::for_process());
    let router = router.layer(from_fn_with_state(policy, context));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = connections.join_next(), if !connections.is_empty() => {},
            (io, peer) = listener.accept() => {
                let router = router.clone();
                let shutdown = cancel.clone();
                connections.spawn(async move {
                    let service = service_fn(move |mut request: hyper::Request<Incoming>| {
                        request.extensions_mut().insert(ConnectInfo(peer.clone()));
                        let router = router.clone();
                        async move { router.oneshot(request.map(Body::new)).await }
                    });
                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new());
                    builder.header_read_timeout(Duration::from_secs(5));
                    let connection = builder.serve_connection(TokioIo::new(io), service).with_upgrades();
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            connection.as_mut().graceful_shutdown();
                            let _ = connection.await;
                        }
                        _ = &mut connection => {},
                    }
                });
            }
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}
pub async fn serve(
    tcp: TcpListener,
    app: Arc<crate::server::App>,
    cancel: CancellationToken,
) -> io::Result<()> {
    let policy = TransportPolicy {
        secure: false,
        allow_insecure_remote_management: app.options.allow_insecure_remote_management,
    };
    serve_listener(tcp, crate::server::router(app), cancel, policy).await
}

#[cfg(test)]
#[path = "http_transport_tests.rs"]
mod tests;
