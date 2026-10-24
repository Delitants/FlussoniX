//! Encrypted startup bursts, bounded consumer credit and record ordering.
use super::review_tests::{DecodeCadence, Feed, GateGuard, record, until};
use super::*;
use crate::fxlink::{Encoder, Listener};
use std::sync::{Mutex as StdMutex, atomic::AtomicBool};

// Observe the fixed fixture's original identities, including queue discards.
// Repairs may already be in flight when the decoder gate closes; duplicate
// observations must not complete the original-burst barrier early.
struct OriginalBurst {
    state: std::sync::Weak<State>,
    seen: [std::sync::atomic::AtomicU32; 14],
}
static ORIGINAL_BURST: std::sync::OnceLock<StdMutex<Option<Arc<OriginalBurst>>>> =
    std::sync::OnceLock::new();
struct BurstGuard(Arc<OriginalBurst>);
impl BurstGuard {
    fn new(state: &Arc<State>) -> Self {
        let burst = Arc::new(OriginalBurst {
            state: Arc::downgrade(state),
            seen: std::array::from_fn(|_| std::sync::atomic::AtomicU32::new(0)),
        });
        *ORIGINAL_BURST
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap() = Some(burst.clone());
        Self(burst)
    }
    fn observed(&self) -> u64 {
        self.0
            .seen
            .iter()
            .map(|bits| u64::from(bits.load(Ordering::Acquire).count_ones()))
            .sum()
    }
}
impl Drop for BurstGuard {
    fn drop(&mut self) {
        *ORIGINAL_BURST.get().unwrap().lock().unwrap() = None;
    }
}
pub(super) fn observe_packet(state: &Arc<State>, header: &Header) {
    let burst = ORIGINAL_BURST
        .get_or_init(|| StdMutex::new(None))
        .lock()
        .unwrap()
        .clone();
    let Some(burst) = burst else {
        return;
    };
    if burst.state.as_ptr() != Arc::as_ptr(state) || header.record_id != 0 || header.block_id >= 14
    {
        return;
    }
    let block = header.block_id as usize;
    let width = if block == 13 { 7 } else { 24 };
    if header.shard_index < width {
        burst.seen[block].fetch_or(1 << header.shard_index, Ordering::Release);
    }
}

#[tokio::test]
async fn paused_consumer_uses_record_credit_then_resumes_without_overload() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let settings = Settings {
        latency_ms: 1500,
        ..Settings::default()
    };
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let url = format!("fxlink://{}/owned-consumer", listener.address());
    let (tx, rx) = mpsc::channel(160);
    let expected: Vec<_> = (0..160u8)
        .map(|n| {
            let mut b = record().to_vec();
            b[12] = n;
            Bytes::from(b)
        })
        .collect();
    for record in &expected {
        tx.send(Ok(record.clone())).await.unwrap();
    }
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(
        Arc::new(Feed {
            records: StdMutex::new(Some(rx)),
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "token",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    until(|| sub.state.queue.lock().unwrap().len() == 64 || !sub.current()).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let paused_active = sub.current();
    let paused_records = sub.state.queue.lock().unwrap().len();
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        let mut records = Vec::new();
        for _ in 0..expected.len() {
            records.push(sub.recv().await?);
        }
        Ok::<_, String>(records)
    })
    .await;
    let active = sub.current();
    let telemetry = sub.telemetry();
    drop(sub);
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    drop(tx);
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    assert_eq!(global().used(), baseline);
    assert!(
        paused_active,
        "bounded consumer pause caused premature output overload"
    );
    assert_eq!(
        paused_records, 64,
        "record credit must bound decoded output while paused"
    );
    assert_eq!(
        outcome
            .expect("bounded drain")
            .expect("consumer could not resume"),
        expected
    );
    assert!(active);
}

#[tokio::test]
async fn recoverable_full_packet_route_keeps_bounds_and_repairs_the_record() {
    full_route_case(false, 4).await;
}

#[tokio::test]
async fn full_packet_route_with_unavailable_decoder_keeps_original_expiry() {
    full_route_case(true, 4).await;
}

#[tokio::test]
async fn default_repair_batch_recovers_full_packet_route_without_spending_credit_on_queued_originals()
 {
    full_route_case(false, 64).await;
}

async fn full_route_case(expire: bool, repair_batch: usize) {
    // 266 data + 53 parity, each a 64-byte header + 1024-byte payload.
    const ORIGINAL_SHARDS: u64 = 319;
    const DATAGRAM_BYTES: u64 = 1088;
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let settings = Settings {
        latency_ms: if expire { 500 } else { 1500 },
        max_repair_shards: repair_batch,
        ..Settings::default()
    };
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let egress = listener.egress();
    let url = format!("fxlink://{}/owned-route", listener.address());
    let (tx, rx) = mpsc::channel(1);
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(
        Arc::new(Feed {
            records: StdMutex::new(Some(rx)),
            alive: Arc::new(AtomicBool::new(true)),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "token",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut bytes = vec![83; 260 * 1024];
    let len = bytes.len();
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    let expected = Bytes::from(bytes);
    let gate = GateGuard::new();
    let burst = BurstGuard::new(&sub.state);
    let started = Instant::now();
    tx.send(Ok(expected.clone())).await.unwrap();
    until(|| {
        // Keep the decoder gated through the complete original burst. Opening
        // at the first overflow lets later originals arrive after draining;
        // FEC alone can then recover the loss, without any retransmission.
        !sub.current() || burst.observed() == ORIGINAL_SHARDS
    })
    .await;
    let survived_full = sub.current();
    let burst_observed = burst.observed();
    drop(burst);
    let burst_routed = sub.state.routed_packets.load(Ordering::Acquire);
    let burst_dropped = sub.state.telemetry.queue_dropped.load(Ordering::Acquire);
    let held = if expire {
        Some(fec_slots().acquire_many_owned(4).await.unwrap())
    } else {
        None
    };
    drop(gate);
    let outcome = tokio::time::timeout(Duration::from_secs(3), sub.recv()).await;
    let elapsed = started.elapsed();
    drop(held);
    let active = sub.current();
    let snapshot = sub.telemetry().snapshot();
    let telemetry = sub.telemetry();
    drop(sub);
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    drop(tx);
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    assert_eq!(global().used(), baseline);
    assert!(
        survived_full,
        "recoverable bounded packet overflow retired the subscription"
    );
    assert_eq!(burst_observed, ORIGINAL_SHARDS);
    // At most 256 queued packets plus one held before decode admission survive,
    // below the 266 data shards needed even with FEC, regardless of arrival order.
    assert!(
        burst_routed <= 257,
        "decoder ran before the original burst finished"
    );
    assert!(
        burst_dropped >= 62,
        "original burst did not overflow the held route"
    );
    let outcome = outcome.expect("bounded recovery or expiry");
    if expire {
        assert!(
            outcome.is_err(),
            "unavailable decoder must not publish old media"
        );
        assert!(!active);
        assert!(
            elapsed >= Duration::from_millis(450) && elapsed < Duration::from_millis(950),
            "original 500 ms age was renewed: {elapsed:?}"
        );
        assert!(snapshot["queue_dropped_shards"].as_u64().unwrap() > 0);
        return;
    }
    assert_eq!(outcome.expect("lost tail could not be repaired"), expected);
    // The real listener must repair within its original 25% allowance.
    let original_bytes = ORIGINAL_SHARDS * DATAGRAM_BYTES;
    let sent_bytes = egress.load(Ordering::Acquire);
    assert!(
        sent_bytes > original_bytes,
        "no repair datagram was enqueued"
    );
    assert!(
        sent_bytes - original_bytes <= original_bytes / 4,
        "bounded overflow exceeded its original repair allowance: {sent_bytes}"
    );
    assert!(snapshot["queue_dropped_shards"].as_u64().unwrap() > 0);
    assert!(snapshot["repair_requested_shards"].as_u64().unwrap() > 0);
    assert_eq!(snapshot["expired_records"], 0);
    assert!(active);
}

#[tokio::test]
async fn paced_large_record_survives_decoder_handoff_contention() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = control::protocols();
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(super::super::transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/owned-burst", endpoint.local_addr().unwrap());
    let settings = Settings {
        latency_ms: 1500,
        ..Settings::default()
    };
    let mut bytes = vec![73; 400 * 1024];
    let len = bytes.len();
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    let expected = Bytes::from(bytes);
    let start = CancellationToken::new();
    let stop = CancellationToken::new();
    let server = {
        let endpoint = endpoint.clone();
        let start = start.clone();
        let stop = stop.clone();
        let settings = settings.clone();
        let record = expected.clone();
        tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let Control::Subscribe { id, .. } = control::read(&mut recv).await.unwrap() else {
                panic!("subscription")
            };
            control::write(&mut send, &Control::Accepted).await.unwrap();
            start.cancelled().await;
            control::write(
                &mut send,
                &Control::Announce {
                    record: 0,
                    len: record.len(),
                },
            )
            .await
            .unwrap();
            let mut encoder = Encoder::new(settings, id, 1, Budget::new(4 * 1024 * 1024)).unwrap();
            let encoded = encoder.encode(record, now()).unwrap();
            assert!(encoded.datagrams.len() > 256);
            for packet in encoded.datagrams {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    result = connection.send_datagram_wait(packet) => if result.is_err() { return; },
                }
                // Sustained arrival exceeds one blocking job per repair-poll
                // interval, but fits the existing bounded packet route.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            stop.cancelled().await;
        })
    };
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    // Reuse the bounded contention fixture: each decoder handoff resumes at
    // a repair poll for at most 650 ms, without changing network deadlines.
    let cadence = DecodeCadence::new();
    start.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(3), sub.recv()).await;
    let active = sub.current();
    let telemetry = sub.telemetry();
    drop(cadence);
    drop(sub);
    pool.close().await;
    stop.cancel();
    endpoint.close(0u32.into(), b"done");
    server.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    assert_eq!(global().used(), baseline);
    assert_eq!(
        outcome
            .expect("bounded record delivery")
            .expect("timely record retired during decoder handoff contention"),
        expected
    );
    assert!(
        active,
        "bounded decoder contention must not retire a recoverable stream"
    );
}

#[tokio::test]
async fn queued_records_preserve_one_record_window_admission_order() {
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(super::super::transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/owned-window", endpoint.local_addr().unwrap());
    let settings = Settings {
        latency_ms: 500,
        max_records: 1,
        ..Settings::default()
    };
    let first = record();
    let mut next = first.to_vec();
    next[12] ^= 1;
    let expected = vec![first, Bytes::from(next)];
    let start = CancellationToken::new();
    let stop = CancellationToken::new();
    let server = {
        let endpoint = endpoint.clone();
        let start = start.clone();
        let stop = stop.clone();
        let records = expected.clone();
        let settings = settings.clone();
        tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let Control::Subscribe { id, .. } = control::read(&mut recv).await.unwrap() else {
                panic!("subscription")
            };
            control::write(&mut send, &Control::Accepted).await.unwrap();
            start.cancelled().await;
            let mut encoder = Encoder::new(settings, id, 1, Budget::new(1024 * 1024)).unwrap();
            // V1 has no record credit. Both complete records can be queued
            // before the decoder's first permit becomes available.
            for record in records {
                let encoded = encoder.encode(record, now()).unwrap();
                assert_eq!(encoded.datagrams.len(), 1);
                connection
                    .send_datagram_wait(encoded.datagrams[0].clone())
                    .await
                    .unwrap();
            }
            stop.cancelled().await;
        })
    };
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "token",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let held = fec_slots().acquire_many_owned(4).await.unwrap();
    start.cancel();
    until(|| sub.state.routed_packets.load(Ordering::Acquire) == 2).await;
    drop(held);
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        Ok::<_, String>(vec![sub.recv().await?, sub.recv().await?])
    })
    .await;
    let active = sub.current();
    let telemetry = sub.telemetry();
    drop(sub);
    pool.close().await;
    stop.cancel();
    endpoint.close(0u32.into(), b"done");
    server.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    assert_eq!(global().used(), baseline);
    assert_eq!(
        outcome
            .expect("bounded delivery")
            .expect("queued successor was discarded before head publication"),
        expected
    );
    assert!(active);
}

#[test]
fn dropped_record_age_survives_repeated_observations_without_renewal() {
    let state = State::new(CancellationToken::new());
    let mut fence = DiscardFence::default();
    state.observe_queue_loss(7, 100);
    let (record, age) = state.take_queue_loss().unwrap();
    fence.observe(record, age);
    state.observe_queue_loss(7, 200);
    let (record, age) = state.take_queue_loss().unwrap();
    fence.observe(record, age);
    assert!(!fence.expired(99));
    assert!(fence.expired(100));
    assert_eq!(fence.deadline(&Default::default(), 7, 300), 100);
    // A packet/announcement admitted before this witness was imported may
    // already have a later deadline. Publication must still use the old age.
    let deadlines = [(7, 250)].into_iter().collect();
    assert_eq!(fence.deadline(&deadlines, 7, 300), 100);
    state.publish(8);
    state.observe_queue_loss(7, 1);
    assert!(state.take_queue_loss().is_none());
    fence.advance(8);
    assert!(!fence.expired(300));
}

#[test]
fn completed_record_cannot_reinsert_a_stale_queue_loss_during_publication() {
    for _ in 0..32 {
        let state = State::new(CancellationToken::new());
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                state.observe_queue_loss(7, 1);
            });
            scope.spawn(|| {
                barrier.wait();
                state.publish(8);
            });
        });
        assert!(state.take_queue_loss().is_none());
        state.observe_queue_loss(8, 200);
        assert_eq!(state.take_queue_loss(), Some((8, 200)));
    }
}

#[tokio::test]
async fn cold_source_admission_can_finish_after_transport_handshake_deadline() {
    source_admission_case(false).await;
}

#[tokio::test]
async fn stalled_source_admission_keeps_a_fixed_bound_and_releases_ownership() {
    source_admission_case(true).await;
}

async fn source_admission_case(stalled: bool) {
    use crate::fxlink::{Provider, Request, Source};
    use futures_util::{StreamExt, future::BoxFuture};
    struct ColdSource(bool);
    impl Provider for ColdSource {
        fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
            Box::pin(async move {
                if self.0 {
                    futures_util::future::pending::<()>().await;
                }
                tokio::time::sleep(Duration::from_millis(5500)).await;
                Ok(Source {
                    pending_records: None,
                    records: Box::pin(
                        futures_util::stream::once(async { Ok(record()) })
                            .chain(futures_util::stream::pending()),
                    ),
                    current: Arc::new(|| true),
                    cancelled: CancellationToken::new(),
                })
            })
        }
    }
    let _serial = super::super::transport::test_lock().lock().await;
    let baseline = global().used();
    let cert = test_tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/cold", listener.address());
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(Arc::new(ColdSource(stalled)), stop.clone()));
    let pool = ClientPool::default();
    let started = Instant::now();
    let result = pool
        .subscribe(
            &url,
            "peer",
            "token",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await;
    let elapsed = started.elapsed();
    let admitted = result.is_ok();
    let delivered = match result {
        Ok(mut sub) => tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .ok()
            .and_then(Result::ok),
        Err(_) => None,
    };
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    assert_eq!(global().used(), baseline);
    if stalled {
        assert!(!admitted);
        assert!(
            elapsed >= Duration::from_millis(9500) && elapsed < Duration::from_secs(12),
            "admission wait lost its fixed ten-second bound: {elapsed:?}"
        );
        assert!(delivered.is_none());
        return;
    }
    assert!(
        admitted,
        "transport retired before the provider's bounded cold-start interval"
    );
    assert_eq!(delivered, Some(record()));
}
