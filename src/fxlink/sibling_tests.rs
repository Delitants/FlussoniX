//! A full viewer queue must not stall another stream on the same connection.
use super::*;
use crate::fxlink::{Listener, Provider, Request, Source};
use futures_util::future::BoxFuture;
use std::sync::{Mutex as StdMutex, atomic::AtomicUsize};

const RECORDS: usize = 160;
// Stay below the existing admission cadence in debug builds; the producer
// keeps an independent clock and fails instead of applying backpressure.
const PERIOD: Duration = Duration::from_millis(12);
const RECORD_LEN: usize = 4500;

fn record(stream: u8, sequence: usize) -> Bytes {
    // Five data shards produce real parity with the default 20+4 profile.
    let mut bytes = vec![stream; RECORD_LEN];
    bytes[..4].copy_from_slice(&((RECORD_LEN - 4) as u32).to_be_bytes());
    bytes[4..12].copy_from_slice(&(sequence as u64).to_be_bytes());
    Bytes::from(bytes)
}

struct Feed {
    streams: StdMutex<HashMap<String, mpsc::Receiver<Result<Bytes, String>>>>,
    stalled_polled: Arc<AtomicUsize>,
}
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            let rx = self
                .streams
                .lock()
                .unwrap()
                .remove(&request.name)
                .ok_or(404u16)?;
            let polled = (request.name == "stalled").then(|| self.stalled_polled.clone());
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::unfold(
                    (rx, polled),
                    |(mut rx, polled)| async move {
                        let record = rx.recv().await?;
                        if let Some(count) = &polled {
                            count.fetch_add(1, Ordering::Release);
                        }
                        Some((record, (rx, polled)))
                    },
                )),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}

async fn until(
    label: &str,
    within: Duration,
    mut condition: impl FnMut() -> bool,
) -> Result<(), String> {
    tokio::time::timeout(within, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| label.to_owned())
}

#[tokio::test]
async fn full_consumer_cancellation_preserves_active_sibling_delivery() {
    isolation(false).await;
}

#[tokio::test]
async fn full_consumer_expiry_preserves_active_sibling_delivery() {
    isolation(true).await;
}

async fn isolation(expire: bool) {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let permits = fec_slots().available_permits();
    let settings = Settings {
        latency_ms: 500,
        // A paced 64-record burst can outlast the first queued record's TTL.
        // Negotiate a smaller window so full credit and retirement coexist.
        max_records: 16,
        ..Settings::default()
    };
    let cert = test_tls::Certificates::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let address = listener.address();
    let (stalled_tx, stalled_rx) = mpsc::channel(RECORDS);
    let (live_tx, live_rx) = mpsc::channel(16);
    let polled = Arc::new(AtomicUsize::new(0));
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(
        Arc::new(Feed {
            streams: StdMutex::new(HashMap::from([
                ("stalled".into(), stalled_rx),
                ("live".into(), live_rx),
            ])),
            stalled_polled: polled.clone(),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let cancel = CancellationToken::new();
    let stalled = pool
        .subscribe(
            &format!("fxlink://{address}/stalled"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings.clone(),
            cancel.clone(),
        )
        .await
        .unwrap();
    let mut live = pool
        .subscribe(
            &format!("fxlink://{address}/live"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(stalled.connection_id(), live.connection_id());
    let live_state = live.state.clone();
    let stalled_stats = stalled.telemetry();
    let live_stats = live.telemetry();
    let received = Arc::new(AtomicUsize::new(0));
    let count = received.clone();
    let work_stop = CancellationToken::new();
    let producer_stop = work_stop.clone();
    let producer = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        for sequence in 0..RECORDS {
            tokio::select! { biased;
                _ = producer_stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep_until(start + PERIOD * sequence as u32) => {}
            }
            live_tx.try_send(Ok(record(0x71, sequence))).map_err(|e| {
                format!("independent live source lost its clock at record {sequence}: {e}")
            })?;
        }
        producer_stop.cancelled().await;
        Ok::<_, String>(())
    });
    let consumer_stop = work_stop.clone();
    let consumer = tokio::spawn(async move {
        for sequence in 0..RECORDS {
            let next = tokio::select! { biased;
                _ = consumer_stop.cancelled() => return Err("fixture stopped before delivery".into()),
                next = live.recv() => next?,
            };
            if next != record(0x71, sequence) {
                return Err(format!(
                    "live record {sequence} changed or arrived out of order"
                ));
            }
            count.store(sequence + 1, Ordering::Release);
        }
        // Retain the healthy subscription until the parent checks cleanup.
        Ok::<_, String>(live)
    });
    let outcome = async {
        for sequence in 0..RECORDS {
            stalled_tx
                .try_send(Ok(record(0xa3, sequence)))
                .map_err(|e| e.to_string())?;
        }
        until(
            "stalled viewer never filled its bounded queue",
            Duration::from_millis(350),
            || stalled.state.queue.lock().unwrap().len() == settings.max_records,
        )
        .await?;
        let first_deadline = stalled
            .state
            .queue
            .lock()
            .unwrap()
            .front()
            .unwrap()
            .deadline_ms;
        if !stalled.current() || polled.load(Ordering::Acquire) != settings.max_records {
            return Err("stalled source did not stop at the negotiated record credit".into());
        }
        let before = received.load(Ordering::Acquire);
        until(
            "full sibling queue stalled healthy delivery",
            Duration::from_millis(250),
            || received.load(Ordering::Acquire) >= before + 16,
        )
        .await?;
        if !stalled.current() || polled.load(Ordering::Acquire) != settings.max_records {
            return Err("stalled credit changed before the isolation check".into());
        }
        if !expire {
            cancel.cancel();
        }
        until(
            "stalled subscription did not process retirement",
            Duration::from_millis(first_deadline.saturating_sub(now()) + 100),
            || {
                !stalled_stats.active.load(Ordering::Acquire)
                    && stalled.state.queue.lock().unwrap().is_empty()
            },
        )
        .await?;
        let retired = now();
        if expire && retired < first_deadline {
            return Err("queued media retired before its original deadline".into());
        }
        if expire && retired > first_deadline + 100 {
            return Err("queued media expiry exceeded its original deadline tolerance".into());
        }
        if !expire && retired >= first_deadline {
            return Err("cancellation did not retire before queued media expiry".into());
        }
        if !stalled.state.queue.lock().unwrap().is_empty() {
            return Err("retired viewer retained queued output".into());
        }
        until(
            "stalled subscription retained memory while sibling was active",
            Duration::from_millis(350),
            || stalled_stats.snapshot()["state_bytes"] == 0,
        )
        .await?;
        let after = received.load(Ordering::Acquire);
        if !live_state.current() || after >= RECORDS || stalled.connection.close_reason().is_some()
        {
            return Err("healthy sibling did not remain live during retirement cleanup".into());
        }
        until(
            "healthy stream did not finish",
            Duration::from_secs(3),
            || received.load(Ordering::Acquire) == RECORDS,
        )
        .await?;
        if !live_state.current() {
            return Err("healthy subscription retired after complete delivery".into());
        }
        Ok::<_, String>((
            before,
            after,
            first_deadline.saturating_sub(retired),
            retired.saturating_sub(first_deadline),
        ))
    }
    .await;
    if let Err(error) = &outcome {
        eprintln!(
            "expiry={expire} failure={error} polled={} queued={} received={} stalled={} healthy={}",
            polled.load(Ordering::Acquire),
            stalled.state.queue.lock().unwrap().len(),
            received.load(Ordering::Acquire),
            stalled_stats.snapshot(),
            live_stats.snapshot()
        );
    }
    work_stop.cancel();
    let production = producer.await;
    let delivery = consumer.await;
    drop(stalled);
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    drop(stalled_tx);
    let delivered = delivery.unwrap().map(drop);
    if outcome.is_err() {
        eprintln!("expiry={expire} producer={production:?} consumer={delivered:?}");
    }
    assert_eq!(stalled_stats.snapshot()["state_bytes"], 0);
    assert_eq!(live_stats.snapshot()["state_bytes"], 0);
    assert_eq!(fec_slots().available_permits(), permits);
    assert_eq!(
        global().used(),
        baseline,
        "sibling fixture leaked transport ownership"
    );
    let phases = outcome.expect("shared connection isolated stalled consumer");
    production.unwrap().unwrap();
    delivered.expect("healthy stream delivered every record");
    assert_eq!(received.load(Ordering::Acquire), RECORDS);
    let snapshot = live_stats.snapshot();
    assert_eq!(snapshot["expired_records"], 0);
    assert_eq!(snapshot["queue_dropped_shards"], 0);
    assert!(snapshot["received_parity_bytes"].as_u64().unwrap() > 0);
    eprintln!("expiry={expire} phases={phases:?} healthy={snapshot}");
}
