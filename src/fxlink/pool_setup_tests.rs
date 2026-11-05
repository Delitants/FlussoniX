//! Client-pool lock contention around retired connections.
use super::*;
use crate::fxlink::{Listener, Provider, Request, Source};
use futures_util::future::BoxFuture;

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
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
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
