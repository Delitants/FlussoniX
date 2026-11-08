//! Client-pool lock contention around retired connections.
use super::*;
use crate::fxlink::{Listener, Provider, Request, Source};
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Live;
impl Provider for Live {
    fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async {
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::pending()),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
struct AdmissionGate {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: CancellationToken,
}
impl Provider for AdmissionGate {
    fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if call == 1 {
                self.entered.notify_one();
                self.release.cancelled().await;
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::pending()),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}

struct Server {
    url: String,
    stop: CancellationToken,
    task: JoinHandle<Result<(), String>>,
}
impl Server {
    fn new(cert: &test_tls::Certificates) -> Self {
        Self::new_on(cert, "127.0.0.1".parse().unwrap())
    }
    fn new_on(cert: &test_tls::Certificates, ip: std::net::IpAddr) -> Self {
        Self::with_provider(cert, ip, Arc::new(Live))
    }
    fn with_provider(
        cert: &test_tls::Certificates,
        ip: std::net::IpAddr,
        provider: Arc<dyn Provider>,
    ) -> Self {
        let listener = Listener::bind(
            SocketAddr::new(ip, 0),
            &cert.cert,
            &cert.key,
            Settings::default(),
        )
        .unwrap();
        let url = format!("fxlink://{}/live", listener.address());
        let stop = CancellationToken::new();
        let task = tokio::spawn(listener.serve(provider, stop.clone()));
        Self { url, stop, task }
    }
    async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn admission_keeps_four_mib_charge_until_ack() {
    let _serial = super::super::transport::test_lock().lock().await;
    let cert = test_tls::Certificates::new();
    let gate = Arc::new(AdmissionGate {
        calls: AtomicUsize::new(0),
        entered: tokio::sync::Notify::new(),
        release: CancellationToken::new(),
    });
    let server = Server::with_provider(&cert, "127.0.0.1".parse().unwrap(), gate.clone());
    let peer = peer_budget("127.0.0.1".parse().unwrap()).unwrap();
    let baseline = peer.used();
    let pool = Arc::new(ClientPool::default());
    let first = subscribe(&pool, &server.url, &cert.ca).await;
    let connected = peer.used();
    let cancel = CancellationToken::new();
    let second = {
        let pool = pool.clone();
        let url = server.url.clone();
        let ca = cert.ca.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pool.subscribe(
                &url,
                "peer",
                "token",
                Some(&ca),
                Settings::default(),
                cancel,
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let charged = peer.used().saturating_sub(connected);
    cancel.cancel();
    gate.release.cancel();
    assert!(second.await.unwrap().is_err());
    drop(first);
    pool.close().await;
    server.shutdown().await;
    assert!(
        charged >= 4 * 1024 * 1024,
        "admission lost its setup reservation"
    );
    assert_eq!(peer.used(), baseline);
}

#[tokio::test]
async fn cancelled_reservation_failure_cannot_strand_pending_origin() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let held = global().reserve(global().limit() - baseline).unwrap();
    let cert = test_tls::Certificates::new();
    let url = "fxlink://127.0.0.1:19457/live";
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, url);
    *pool.inner.reserve_gate.lock().unwrap() = Some(gate.clone());
    let cancel = CancellationToken::new();
    let attempt = {
        let pool = pool.clone();
        let ca = cert.ca.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pool.subscribe(url, "peer", "token", Some(&ca), Settings::default(), cancel)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let state_guard = pool.inner.state.lock().await;
    gate.release.cancel();
    tokio::task::yield_now().await;
    cancel.cancel();
    assert!(attempt.await.unwrap().is_err());
    drop(state_guard);
    let cleaned = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if pool.inner.state.lock().await.pending.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    pool.close().await;
    drop(held);
    assert!(
        cleaned.is_ok(),
        "cancelled reserve failure left a permanent slot"
    );
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn immediate_retry_waits_for_cancelled_attempt_then_reconnects() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let setup = gate(&pool, &server.url);
    let retired = Arc::new(SetupGate {
        host: setup.host.clone(),
        port: setup.port,
        entered: tokio::sync::Notify::new(),
        release: CancellationToken::new(),
        starts: AtomicUsize::new(0),
    });
    *pool.inner.retirement_gate.lock().unwrap() = Some(retired.clone());
    let cancel = CancellationToken::new();
    let first = {
        let pool = pool.clone();
        let url = server.url.clone();
        let ca = cert.ca.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pool.subscribe(
                &url,
                "peer",
                "token",
                Some(&ca),
                Settings::default(),
                cancel,
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), setup.entered.notified())
        .await
        .unwrap();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), retired.entered.notified())
        .await
        .unwrap();
    // A is cancelled and its worker is held just before removing the pending entry.
    // A fresh caller must wait for retirement, not inherit A's error.
    let retry = spawn_subscription(&pool, &server.url, &cert.ca, "peer", Settings::default());
    tokio::time::sleep(Duration::from_millis(30)).await;
    setup.release.cancel();
    retired.release.cancel();
    assert!(first.await.unwrap().is_err());
    let second = tokio::time::timeout(Duration::from_secs(3), retry)
        .await
        .unwrap()
        .unwrap();
    assert!(second.is_ok(), "new caller inherited cancelled setup");
    drop(second);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

async fn subscribe(pool: &ClientPool, url: &str, ca: &Path) -> Subscription {
    pool.subscribe(
        url,
        "peer",
        "token",
        Some(ca),
        Settings::default(),
        CancellationToken::new(),
    )
    .await
    .unwrap()
}

async fn install_slow_retirement(pool: &ClientPool) -> oneshot::Sender<()> {
    let (release, stalled) = oneshot::channel();
    let mut state = pool.inner.state.lock().await;
    let peer = state.peers.last_mut().unwrap();
    peer.cancel.cancel();
    peer.connection.close(0u32.into(), b"test retirement");
    let old = peer.task.replace(tokio::spawn(async move {
        let _ = stalled.await;
    }));
    drop(state);
    old.unwrap().await.unwrap();
    release
}

fn gate(pool: &ClientPool, url: &str) -> Arc<SetupGate> {
    let address = Address::parse(url, true).unwrap();
    let gate = Arc::new(SetupGate {
        host: address.host,
        port: address.port,
        entered: tokio::sync::Notify::new(),
        release: CancellationToken::new(),
        starts: AtomicUsize::new(0),
    });
    *pool.inner.setup_gate.lock().unwrap() = Some(gate.clone());
    gate
}
fn spawn_subscription(
    pool: &Arc<ClientPool>,
    url: &str,
    ca: &Path,
    peer_key: &'static str,
    settings: Settings,
) -> JoinHandle<Result<Subscription, String>> {
    let pool = pool.clone();
    let url = url.to_owned();
    let ca = ca.to_owned();
    tokio::spawn(async move {
        pool.subscribe(
            &url,
            peer_key,
            "token",
            Some(&ca),
            settings,
            CancellationToken::new(),
        )
        .await
    })
}

#[tokio::test]
async fn same_origin_callers_share_one_inflight_setup() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &server.url);
    let entered = gate.entered.notified();
    let calls: Vec<_> = (0..4)
        .map(|_| spawn_subscription(&pool, &server.url, &cert.ca, "peer", Settings::default()))
        .collect();
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.release.cancel();
    let mut subs = Vec::new();
    for call in calls {
        subs.push(call.await.unwrap().unwrap());
    }
    assert_eq!(gate.starts.load(Ordering::SeqCst), 1);
    assert!(
        subs.iter()
            .all(|sub| sub.connection_id() == subs[0].connection_id())
    );
    drop(subs);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn same_origin_distinct_full_keys_do_not_share_connection() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &server.url);
    let entered = gate.entered.notified();
    let first = spawn_subscription(&pool, &server.url, &cert.ca, "peer-a", Settings::default());
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap();
    let second = spawn_subscription(
        &pool,
        &server.url,
        &cert.ca,
        "peer-b",
        Settings {
            latency_ms: 1500,
            ..Settings::default()
        },
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(gate.starts.load(Ordering::SeqCst), 1);
    gate.release.cancel();
    let a = first.await.unwrap().unwrap();
    let b = second.await.unwrap().unwrap();
    assert_ne!(a.connection_id(), b.connection_id());
    assert_eq!(gate.starts.load(Ordering::SeqCst), 2);
    drop((a, b));
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn pending_and_live_share_sixteen_slots() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let san = (1..=18)
        .map(|n| format!("IP:127.0.0.{n}"))
        .collect::<Vec<_>>()
        .join(",");
    std::fs::write(
        cert.dir.path().join("extensions"),
        format!("subjectAltName={san}\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"),
    )
    .unwrap();
    let signed = std::process::Command::new("openssl")
        .current_dir(cert.dir.path())
        .args([
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            "1",
            "-extfile",
            "extensions",
        ])
        .output()
        .unwrap();
    assert!(
        signed.status.success(),
        "{}",
        String::from_utf8_lossy(&signed.stderr)
    );
    let pool = Arc::new(ClientPool::default());
    let mut servers = Vec::new();
    let mut subs = Vec::new();
    for host in 2..17 {
        let server = Server::new_on(&cert, format!("127.0.0.{host}").parse().unwrap());
        subs.push(subscribe(&pool, &server.url, &cert.ca).await);
        servers.push(server);
    }
    let pending_server = Server::new_on(&cert, "127.0.0.17".parse().unwrap());
    let rejected_server = Server::new_on(&cert, "127.0.0.18".parse().unwrap());
    let gate = gate(&pool, &pending_server.url);
    let entered = gate.entered.notified();
    let pending = spawn_subscription(
        &pool,
        &pending_server.url,
        &cert.ca,
        "peer",
        Settings::default(),
    );
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap();
    let seventeenth = tokio::time::timeout(
        Duration::from_secs(1),
        pool.subscribe(
            &rejected_server.url,
            "peer",
            "token",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        ),
    )
    .await;
    gate.release.cancel();
    let pending_sub = pending.await.unwrap().unwrap();
    drop(pending_sub);
    let correct_rejection =
        matches!(seventeenth, Ok(Err(ref error)) if error == "FXLink peer limit");
    drop(subs);
    pool.close().await;
    for server in servers {
        server.shutdown().await;
    }
    pending_server.shutdown().await;
    rejected_server.shutdown().await;
    assert!(
        correct_rejection,
        "17th origin was not rejected while one setup was pending"
    );
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn cancel_one_waiter_keeps_sibling_setup() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &server.url);
    let first_cancel = CancellationToken::new();
    let first = {
        let pool = pool.clone();
        let url = server.url.clone();
        let ca = cert.ca.clone();
        let cancel = first_cancel.clone();
        tokio::spawn(async move {
            pool.subscribe(
                &url,
                "peer",
                "token",
                Some(&ca),
                Settings::default(),
                cancel,
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let second = spawn_subscription(&pool, &server.url, &cert.ca, "peer", Settings::default());
    tokio::time::sleep(Duration::from_millis(30)).await;
    first_cancel.cancel();
    assert!(first.await.unwrap().is_err());
    assert!(
        !second.is_finished(),
        "sibling setup retired with first waiter"
    );
    gate.release.cancel();
    let sibling = second.await.unwrap().unwrap();
    assert!(sibling.current());
    assert_eq!(gate.starts.load(Ordering::SeqCst), 1);
    drop(sibling);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn cancel_last_waiter_closes_endpoint_and_releases_budgets() {
    let _serial = super::super::transport::test_lock().lock().await;
    let cert = test_tls::Certificates::new();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let peer = peer_budget("127.0.0.1".parse().unwrap()).unwrap();
    let baseline = peer.used();
    let pool = Arc::new(ClientPool::default());
    let cancel = CancellationToken::new();
    let attempt = {
        let pool = pool.clone();
        let url = url.clone();
        let ca = cert.ca.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pool.subscribe(
                &url,
                "peer",
                "token",
                Some(&ca),
                Settings::default(),
                cancel,
            )
            .await
        })
    };
    let mut packet = [0; 1500];
    tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer.used() - baseline, 8 * 1024 * 1024);
    cancel.cancel();
    assert!(attempt.await.unwrap().is_err());
    let cleaned = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if pool.inner.state.lock().await.pending.is_empty() && peer.used() == baseline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    pool.close().await;
    assert!(
        cleaned.is_ok(),
        "last cancelled waiter retained setup and budget"
    );
    assert_eq!(peer.used(), baseline);
}

#[tokio::test]
async fn timed_out_setup_clears_pending_for_retry() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &server.url);
    let attempt = spawn_subscription(&pool, &server.url, &cert.ca, "peer", Settings::default());
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(16), attempt)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.err().as_deref(), Some("FXLink setup timeout"));
    let cleaned = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if pool.inner.state.lock().await.pending.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    gate.release.cancel();
    assert!(cleaned.is_ok(), "timed-out setup retained its pending slot");
    let retry = subscribe(&pool, &server.url, &cert.ca).await;
    assert!(retry.current());
    drop(retry);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn dns_and_tls_failures_allow_retry() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let wrong = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = ClientPool::default();
    let missing = pool
        .subscribe(
            "fxlink://does-not-exist.invalid:19455/live",
            "peer",
            "token",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await;
    assert!(missing.is_err());
    assert!(pool.inner.state.lock().await.pending.is_empty());
    let bad_tls = pool
        .subscribe(
            &server.url,
            "peer",
            "token",
            Some(&wrong.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await;
    assert!(bad_tls.is_err());
    assert!(pool.inner.state.lock().await.pending.is_empty());
    let good = subscribe(&pool, &server.url, &cert.ca).await;
    assert!(good.current());
    drop(good);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn close_race_rejects_old_publication_but_allows_new_epoch() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &server.url);
    let first = spawn_subscription(&pool, &server.url, &cert.ca, "peer", Settings::default());
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    pool.close().await;
    assert!(first.await.unwrap().is_err());
    assert!(pool.inner.state.lock().await.pending.is_empty());
    assert!(pool.inner.state.lock().await.peers.is_empty());
    gate.release.cancel();
    let next = subscribe(&pool, &server.url, &cert.ca).await;
    assert!(next.current());
    drop(next);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn aborted_setup_task_releases_pending_for_retry() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let server = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &server.url);
    let first = spawn_subscription(&pool, &server.url, &cert.ca, "peer", Settings::default());
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let pending = pool.inner.state.lock().await.pending[0].clone();
    pending.task.lock().unwrap().as_ref().unwrap().abort();
    let cleared = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if pool.inner.state.lock().await.pending.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    gate.release.cancel();
    assert!(
        cleared.is_ok(),
        "aborted setup left a permanent pending origin"
    );
    assert!(first.await.unwrap().is_err());
    let retry = subscribe(&pool, &server.url, &cert.ca).await;
    drop(retry);
    pool.close().await;
    server.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test(flavor = "current_thread")]
async fn abort_before_first_worker_poll_clears_pending() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let pool = ClientPool::default();
    let mut attempt = Box::pin(pool.subscribe(
        "fxlink://127.0.0.1:19458/live",
        "peer",
        "token",
        Some(&cert.ca),
        Settings::default(),
        CancellationToken::new(),
    ));
    let pending_poll = std::future::poll_fn(|cx| {
        use std::future::Future;
        std::task::Poll::Ready(attempt.as_mut().poll(cx).is_pending())
    })
    .await;
    assert!(pending_poll);
    let pending = pool.inner.state.lock().await.pending[0].clone();
    pending.task.lock().unwrap().as_ref().unwrap().abort();
    drop(attempt);
    let cleaned = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if pool.inner.state.lock().await.pending.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    pool.close().await;
    assert!(
        cleaned.is_ok(),
        "unpolled aborted worker stranded pending slot"
    );
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn stalled_origin_does_not_delay_other_origin() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let a = Server::new(&cert);
    let b = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &a.url);
    let entered = gate.entered.notified();
    let first = {
        let pool = pool.clone();
        let url = a.url.clone();
        let ca = cert.ca.clone();
        tokio::spawn(async move { subscribe(&pool, &url, &ca).await })
    };
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap();
    let second = {
        let pool = pool.clone();
        let url = b.url.clone();
        let ca = cert.ca.clone();
        tokio::spawn(async move { subscribe(&pool, &url, &ca).await })
    };
    let mut second = second;
    let early = tokio::time::timeout(Duration::from_secs(1), &mut second).await;
    let b_completed_early = early.is_ok();
    gate.release.cancel();
    let a_sub = first.await.unwrap();
    let b_sub = match early {
        Ok(result) => result.unwrap(),
        Err(_) => second.await.unwrap(),
    };
    drop((a_sub, b_sub));
    pool.close().await;
    a.shutdown().await;
    b.shutdown().await;
    assert!(b_completed_early, "origin B waited for A's setup gate");
    assert_eq!(gate.starts.load(Ordering::SeqCst), 1);
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn cancelling_stalled_origin_keeps_other_origin_playing() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let a = Server::new(&cert);
    let b = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let gate = gate(&pool, &a.url);
    let cancel = CancellationToken::new();
    let first = {
        let pool = pool.clone();
        let url = a.url.clone();
        let ca = cert.ca.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pool.subscribe(
                &url,
                "peer",
                "token",
                Some(&ca),
                Settings::default(),
                cancel,
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    let b_sub = tokio::time::timeout(Duration::from_secs(1), subscribe(&pool, &b.url, &cert.ca))
        .await
        .expect("origin B must complete within one second while A is gated");
    cancel.cancel();
    assert!(first.await.unwrap().is_err());
    assert!(b_sub.current(), "cancelling A retired B's subscription");
    gate.release.cancel();
    drop(b_sub);
    pool.close().await;
    a.shutdown().await;
    b.shutdown().await;
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn retired_router_join_does_not_block_other_origin() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let a = Server::new(&cert);
    let b = Server::new(&cert);
    let c = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let old = subscribe(&pool, &a.url, &cert.ca).await;
    let release = install_slow_retirement(&pool).await;
    let second = {
        let pool = pool.clone();
        let url = b.url.clone();
        let ca = cert.ca.clone();
        tokio::spawn(async move { subscribe(&pool, &url, &ca).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let third = {
        let pool = pool.clone();
        let url = c.url.clone();
        let ca = cert.ca.clone();
        tokio::spawn(async move { subscribe(&pool, &url, &ca).await })
    };
    let mut third = third;
    let early = tokio::time::timeout(Duration::from_millis(500), &mut third).await;
    let completed_without_release = early.is_ok();
    let _ = release.send(());
    let new = second.await.unwrap();
    let another = match early {
        Ok(result) => result.unwrap(),
        Err(_) => third.await.unwrap(),
    };
    drop((old, new, another));
    pool.close().await;
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
    assert!(
        completed_without_release,
        "retired router join held the pool lock"
    );
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn close_join_does_not_block_new_epoch_subscription() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let a = Server::new(&cert);
    let b = Server::new(&cert);
    let pool = Arc::new(ClientPool::default());
    let old = subscribe(&pool, &a.url, &cert.ca).await;
    let release = install_slow_retirement(&pool).await;
    let closing = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.close().await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let second = {
        let pool = pool.clone();
        let url = b.url.clone();
        let ca = cert.ca.clone();
        tokio::spawn(async move { subscribe(&pool, &url, &ca).await })
    };
    let mut second = second;
    let early = tokio::time::timeout(Duration::from_millis(500), &mut second).await;
    let completed_without_release = early.is_ok();
    let _ = release.send(());
    closing.await.unwrap();
    let new = match early {
        Ok(result) => result.unwrap(),
        Err(_) => second.await.unwrap(),
    };
    drop((old, new));
    pool.close().await;
    a.shutdown().await;
    b.shutdown().await;
    assert!(
        completed_without_release,
        "close joined router under pool lock"
    );
    assert_eq!(global().used(), baseline);
}
