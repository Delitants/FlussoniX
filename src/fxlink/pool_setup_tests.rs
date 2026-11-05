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
        let listener = Listener::bind(
            SocketAddr::new(ip, 0),
            &cert.cert,
            &cert.key,
            Settings::default(),
        )
        .unwrap();
        let url = format!("fxlink://{}/live", listener.address());
        let stop = CancellationToken::new();
        let task = tokio::spawn(listener.serve(Arc::new(Live), stop.clone()));
        Self { url, stop, task }
    }
    async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.unwrap().unwrap();
    }
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
