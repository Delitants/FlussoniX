use bytes::Bytes;
use flussonix::fxlink::{ClientPool, Listener, Provider, Request, Settings, Source};
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[path = "support/tls.rs"]
mod tls;
// Independent fixtures all use loopback, which is one production peer identity.
// Isolate their shared 128 MiB allowance while preserving concurrency inside each test.
async fn peer_budget_fixture() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}
struct Feed {
    alive: Arc<AtomicBool>,
}
fn record() -> Bytes {
    let mut b = vec![7; 8000];
    b[..4].copy_from_slice(&7996u32.to_be_bytes());
    Bytes::from(b)
}
impl Provider for Feed {
    fn subscribe(&self, r: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let alive = self.alive.clone();
        Box::pin(async move {
            if r.peer_key != "peer-secret" {
                return Err(401);
            }
            if r.token
                != if r.name == "second" {
                    "other-stream-secret"
                } else {
                    "stream-secret"
                }
            {
                return Err(403);
            }
            let current = if r.name == "revocable" {
                alive
            } else {
                Arc::new(AtomicBool::new(true))
            };
            let records: flussonix::fxlink::RecordStream = if r.name == "paced" {
                Box::pin(
                    futures_util::stream::unfold(0usize, |n| async move {
                        if n >= 250 {
                            return None;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        Some((Ok(record()), n + 1))
                    })
                    .chain(futures_util::stream::pending()),
                )
            } else if r.name == "continuous" {
                Box::pin(futures_util::stream::unfold((), |()| async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Some((Ok(record()), ()))
                }))
            } else if r.name == "overload" {
                Box::pin(
                    futures_util::stream::iter((0..100).map(|_| Ok(record())))
                        .chain(futures_util::stream::pending()),
                )
            } else {
                Box::pin(
                    futures_util::stream::once(async { Ok(record()) })
                        .chain(futures_util::stream::pending()),
                )
            };
            Ok(Source {
                pending_records: None,
                records,
                current: Arc::new(move || current.load(Ordering::Acquire)),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
#[tokio::test]
async fn tls_auth_multiplex_revocation_cancel_and_shutdown() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let alive = Arc::new(AtomicBool::new(true));
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let base = format!("fxlink://{}", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: alive.clone(),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    assert!(
        pool.subscribe(
            &format!("{base}/live"),
            "peer-secret",
            "stream-secret",
            None,
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(
        pool.subscribe(
            &format!("{base}/live"),
            "wrong",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(
        pool.subscribe(
            &format!("{base}/live"),
            "peer-secret",
            "wrong",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let mut a = pool
        .subscribe(
            &format!("{base}/revocable"),
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let mut b = pool
        .subscribe(
            &format!("{base}/second"),
            "peer-secret",
            "other-stream-secret",
            Some(&cert.ca),
            Settings::default(),
            cancel.clone(),
        )
        .await
        .unwrap();
    assert_eq!(a.connection_id(), b.connection_id());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), a.recv())
            .await
            .unwrap()
            .unwrap(),
        record()
    );
    assert_eq!(b.recv().await.unwrap(), record());
    alive.store(false, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!a.current());
    assert!(a.recv().await.is_err());
    assert!(b.current());
    cancel.cancel();
    assert!(!b.current());
    assert!(b.recv().await.is_err());
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn replacement_ca_content_cannot_reuse_trusted_connection() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let other = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/live", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let first = pool
        .subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    std::fs::copy(&other.ca, &cert.ca).unwrap();
    assert!(
        pool.subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(first.current());
    drop(first);
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn subscriptions_are_bounded_and_drop_reclaims_capacity() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/live", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let mut subscriptions = Vec::new();
    for _ in 0..16 {
        subscriptions.push(
            pool.subscribe(
                &url,
                "peer-secret",
                "stream-secret",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        );
    }
    assert!(
        pool.subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let telemetry = subscriptions[0].telemetry();
    subscriptions.clear();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    let mut fresh = pool
        .subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(fresh.recv().await.unwrap(), record());
    drop(fresh);
    pool.close().await;
    let mut reused = pool
        .subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(reused.recv().await.unwrap(), record());
    drop(reused);
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn trusted_certificate_still_requires_matching_server_identity() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.2:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/live", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    assert!(
        pool.subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn overloaded_subscription_retires_without_blocking_healthy_stream() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let base = format!("fxlink://{}", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let mut healthy = pool
        .subscribe(
            &format!("{base}/continuous"),
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut overloaded = pool
        .subscribe(
            &format!("{base}/overload"),
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(healthy.connection_id(), overloaded.connection_id());
    // A healthy sibling must actively consume: leaving its output unread
    // through the overloaded stream's original TTL would expire both fairly.
    let receiving = tokio::time::timeout(Duration::from_secs(3), async {
        let mut received = 0usize;
        while overloaded.current() {
            tokio::select! {
                packet=healthy.recv()=>{
                    if packet? != record() {return Err("healthy record corrupted".to_string());}
                    received += 1;
                },
                _=tokio::time::sleep(Duration::from_millis(10))=>{},
            }
        }
        Ok::<_, String>(received)
    })
    .await;
    let retired = !overloaded.current();
    let rejected = tokio::time::timeout(Duration::from_secs(1), overloaded.recv()).await;
    let after = tokio::time::timeout(Duration::from_secs(1), healthy.recv()).await;
    let active = healthy.current();
    drop(overloaded);
    drop(healthy);
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
    assert!(
        receiving
            .expect("bounded overload retirement")
            .expect("active sibling stopped receiving")
            > 1
    );
    assert!(retired);
    assert!(rejected.expect("bounded rejected read").is_err());
    assert_eq!(
        after
            .expect("bounded post-retirement progress")
            .expect("healthy sibling retired"),
        record()
    );
    assert!(active);
}

#[tokio::test]
async fn sustained_native_record_rate_does_not_use_receiver_incomplete_record_cap() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/paced", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..250 {
            assert_eq!(sub.recv().await.unwrap(), record());
        }
    })
    .await
    .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(1000));
    assert!(sub.current());
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn listener_negotiates_operator_profiles_with_resource_ceilings() {
    let _peer_budget = peer_budget_fixture().await;
    let cert = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/live", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    for settings in [
        Settings {
            latency_ms: 1200,
            parity_shards: 6,
            ..Settings::default()
        },
        Settings {
            latency_ms: 500,
            data_shards: 1,
            parity_shards: 1,
            ..Settings::default()
        },
    ] {
        let mut sub = pool
            .subscribe(
                &url,
                "peer-secret",
                "stream-secret",
                Some(&cert.ca),
                settings,
                CancellationToken::new(),
            )
            .await
            .expect("supported operator profile must negotiate");
        assert_eq!(sub.recv().await.unwrap(), record());
    }
    assert!(
        pool.subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings {
                parity_shards: 5,
                ..Settings::default()
            },
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
    let ceiling = Settings {
        max_record_len: 16384,
        ..Settings::default()
    };
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        ceiling.clone(),
    )
    .unwrap();
    let url = format!("fxlink://{}/live", listener.address());
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.serve(
        Arc::new(Feed {
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    assert!(
        pool.subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            Settings {
                max_record_len: 32768,
                ..ceiling.clone()
            },
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let mut allowed = pool
        .subscribe(
            &url,
            "peer-secret",
            "stream-secret",
            Some(&cert.ca),
            ceiling,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(allowed.recv().await.unwrap(), record());
    pool.close().await;
    stop.cancel();
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn cancelled_delayed_admissions_release_stream_slots_promptly() {
    let _peer_budget = peer_budget_fixture().await;
    use std::sync::atomic::AtomicUsize;
    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct Pending {
        started: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }
    impl Provider for Pending {
        fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
            let started = self.started.clone();
            let dropped = self.dropped.clone();
            Box::pin(async move {
                if request.name == "pending" {
                    let _probe = Probe(dropped);
                    started.fetch_add(1, Ordering::AcqRel);
                    futures_util::future::pending::<()>().await;
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
    let cert = tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let base = format!("fxlink://{}", listener.address());
    let stop = CancellationToken::new();
    let started = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(listener.serve(
        Arc::new(Pending {
            started: started.clone(),
            dropped: dropped.clone(),
        }),
        stop.clone(),
    ));
    let pool = Arc::new(ClientPool::default());
    let live = pool
        .subscribe(
            &format!("{base}/live"),
            "peer",
            "token",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    for round in 0..2 {
        let mut attempts = Vec::new();
        for _ in 0..15 {
            let cancel = CancellationToken::new();
            let p = pool.clone();
            let url = format!("{base}/pending");
            let ca = cert.ca.clone();
            let c = cancel.clone();
            attempts.push((
                cancel,
                tokio::spawn(async move {
                    p.subscribe(&url, "peer", "token", Some(&ca), Settings::default(), c)
                        .await
                }),
            ));
        }
        let admitted = tokio::time::timeout(Duration::from_secs(2), async {
            while started.load(Ordering::Acquire) < (round + 1) * 15 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        if admitted.is_err() {
            let mut failures = Vec::new();
            for (cancel, attempt) in attempts {
                let finished = attempt.is_finished();
                cancel.cancel();
                if let Err(error) = attempt.await.unwrap() {
                    failures.push((finished, error));
                }
            }
            panic!(
                "provider start barrier: round {round}, started {}, dropped {}, attempts {failures:?}",
                started.load(Ordering::Acquire),
                dropped.load(Ordering::Acquire)
            );
        }
        for (cancel, _) in &attempts {
            cancel.cancel();
        }
        for (_, attempt) in attempts {
            assert!(attempt.await.unwrap().is_err());
        }
        tokio::time::timeout(Duration::from_millis(350), async {
            while dropped.load(Ordering::Acquire) < (round + 1) * 15 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("cancelled Provider futures must drop promptly");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let fresh = pool
        .subscribe(
            &format!("{base}/live"),
            "peer",
            "token",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(fresh.connection_id(), live.connection_id());
    drop(fresh);
    tokio::time::sleep(Duration::from_millis(30)).await;
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
}
