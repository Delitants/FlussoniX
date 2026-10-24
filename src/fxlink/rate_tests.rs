//! A drained v5 byte queue must not quantize a ready source to housekeeping.
use super::*;
use crate::fxlink::{Listener, Provider, Request, Source};
use futures_util::future::{BoxFuture, join_all};
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicU64, AtomicUsize},
};

#[path = "rate_history.rs"]
mod history;
#[derive(Clone, Copy)]
struct Attempt {
    stream: usize,
    sequence: usize,
    scheduled_ns: u128,
    wake_ns: u128,
    attempt_ns: u128,
    capacity: usize,
    admitted: [usize; 2],
    polled: [usize; 2],
    received: [usize; 2],
    accepted: bool,
}
impl std::fmt::Debug for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt")
            .field("stream", &self.stream)
            .field("sequence", &self.sequence)
            .field("scheduled_ns", &self.scheduled_ns)
            .field("wake_ns", &self.wake_ns)
            .field(
                "wake_late_ns",
                &self.wake_ns.saturating_sub(self.scheduled_ns),
            )
            .field("attempt_ns", &self.attempt_ns)
            .field("capacity_before", &self.capacity)
            .field("admitted_before", &self.admitted)
            .field("polled_before", &self.polled)
            .field("received_before", &self.received)
            .field("accepted", &self.accepted)
            .finish()
    }
}
struct Evidence {
    attempts: history::History<Attempt, 64>,
    worst: Option<Attempt>,
}
struct DumpOnFailure(Arc<StdMutex<Evidence>>);
impl Drop for DumpOnFailure {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let history = self.0.lock().unwrap_or_else(|p| p.into_inner());
            eprintln!("fixed_rate_worst_clock_attempt={:?}", history.worst);
            for attempt in history.attempts.iter() {
                eprintln!("fixed_rate_source_history={attempt:?}");
            }
        }
    }
}
const RECORDS: usize = 400;
const PERIOD: Duration = Duration::from_millis(8);
fn record(stream: usize, sequence: usize) -> Bytes {
    let mut bytes = vec![stream as u8 + 0x40; 4500];
    bytes[..4].copy_from_slice(&4496u32.to_be_bytes());
    bytes[4..12].copy_from_slice(&(sequence as u64).to_be_bytes());
    Bytes::from(bytes)
}
struct Feed {
    streams: StdMutex<HashMap<String, mpsc::Receiver<Result<Bytes, String>>>>,
    polled: Vec<Arc<AtomicUsize>>,
}
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            let index = request.name.parse::<usize>().map_err(|_| 404u16)?;
            let rx = self
                .streams
                .lock()
                .unwrap()
                .remove(&request.name)
                .ok_or(404u16)?;
            let count = self.polled[index].clone();
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::unfold(
                    (rx, count),
                    |(mut rx, count)| async move {
                        let value = rx.recv().await?;
                        count.fetch_add(1, Ordering::Release);
                        Some((value, (rx, count)))
                    },
                )),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
#[tokio::test]
async fn fixed_clock_multishard_source_keeps_125_records_per_second() {
    paced(1).await;
}
#[tokio::test]
async fn shared_connection_keeps_two_independent_125_record_sources() {
    paced(2).await;
}

async fn paced(streams: usize) {
    assert!((1..=2).contains(&streams));
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let permits = fec_slots().available_permits();
    let settings = Settings {
        latency_ms: 500,
        ..Default::default()
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
    let mut channels = HashMap::new();
    let mut senders = Vec::new();
    let polled: Vec<_> = (0..streams)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    for index in 0..streams {
        let (tx, rx) = mpsc::channel(16);
        channels.insert(index.to_string(), rx);
        senders.push(tx);
    }
    let stop = CancellationToken::new();
    let server = tokio::spawn(listener.serve(
        Arc::new(Feed {
            streams: StdMutex::new(channels),
            polled: polled.clone(),
        }),
        stop.clone(),
    ));
    let pool = ClientPool::default();
    let mut subscriptions = Vec::new();
    for index in 0..streams {
        subscriptions.push(
            pool.subscribe(
                &format!("fxlink://{address}/{index}"),
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                settings.clone(),
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        );
    }
    assert!(
        subscriptions
            .iter()
            .all(|s| s.connection_id() == subscriptions[0].connection_id())
    );
    let stats: Vec<_> = subscriptions.iter().map(Subscription::telemetry).collect();
    let states: Vec<_> = subscriptions.iter().map(|s| s.state.clone()).collect();
    let received: Vec<_> = (0..streams)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let work_stop = CancellationToken::new();
    let tasks: Vec<_> = subscriptions
        .into_iter()
        .enumerate()
        .map(|(index, mut sub)| {
            let count = received[index].clone();
            let stop = work_stop.clone();
            tokio::spawn(async move {
                for sequence in 0..RECORDS {
                    let value = tokio::select! { biased;
                        _ = stop.cancelled() => return Err("fixture stopped".to_owned()),
                        value = sub.recv() => value?,
                    };
                    if value != record(index, sequence) {
                        return Err(format!(
                            "stream {index} record {sequence} changed or reordered"
                        ));
                    }
                    count.store(sequence + 1, Ordering::Release);
                }
                Ok(sub)
            })
        })
        .collect();
    let (report_tx, report_rx) = tokio::sync::oneshot::channel();
    let producer_stop = work_stop.clone();
    let max_late = Arc::new(AtomicU64::new(0));
    let late = max_late.clone();
    let history = Arc::new(StdMutex::new(Evidence {
        attempts: history::History::new(),
        worst: None,
    }));
    // Lives through joins, cleanup and every final assertion, including telemetry failures.
    let _dump_on_failure = DumpOnFailure(history.clone());
    let source_polled = polled.clone();
    let source_received = received.clone();
    let producer = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let mut admitted = [0usize; 2];
        let outcome = async {
            for sequence in 0..RECORDS {
                let scheduled = start + PERIOD * sequence as u32;
                tokio::select! { biased;
                    _ = producer_stop.cancelled() => return Err("source fixture stopped".to_owned()),
                    _ = tokio::time::sleep_until(scheduled) => {},
                }
                let wake=tokio::time::Instant::now();
                late.fetch_max(wake.saturating_duration_since(scheduled).as_millis() as u64, Ordering::Relaxed);
                for (index, tx) in senders.iter().enumerate() {
                    let capacity=tx.capacity();
                    let mut polled_before=[0;2];let mut received_before=[0;2];
                    for stream in 0..streams {
                        polled_before[stream]=source_polled[stream].load(Ordering::Acquire);
                        received_before[stream]=source_received[stream].load(Ordering::Acquire);
                    }
                    let attempt_ns=start.elapsed().as_nanos();
                    let result=tx.try_send(Ok(record(index, sequence)));
                    let attempt=Attempt {stream:index,sequence,scheduled_ns:scheduled.duration_since(start).as_nanos(),wake_ns:wake.duration_since(start).as_nanos(),attempt_ns,capacity,admitted,polled:polled_before,received:received_before,accepted:result.is_ok()};
                    {
                        let mut evidence=history.lock().unwrap();
                        if evidence.worst.is_none_or(|old|attempt.wake_ns.saturating_sub(attempt.scheduled_ns)>old.wake_ns.saturating_sub(old.scheduled_ns)) {evidence.worst=Some(attempt);}
                        evidence.attempts.push(attempt);
                    }
                    result.map_err(|e| format!("stream {index} source queue overflow at record {sequence}: {e}"))?;
                    admitted[index]+=1;
                }
            }
            Ok(())
        }.await;
        let _ = report_tx.send(outcome);
        // Keep source ownership live until every receiver has finished.
        producer_stop.cancelled().await;
        drop(senders);
    });
    let outcome = async {
        tokio::time::timeout(Duration::from_secs(5), report_rx)
            .await
            .map_err(|_| "fixed-clock source timeout".to_owned())?
            .map_err(|_| "source report dropped".to_owned())??;
        tokio::time::timeout(Duration::from_secs(2), async {
            while received
                .iter()
                .any(|n| n.load(Ordering::Acquire) != RECORDS)
            {
                if states.iter().any(|s| !s.current()) {
                    return Err("active source retired before complete delivery".to_owned());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(())
        })
        .await
        .map_err(|_| "receiver completion timeout".to_owned())??;
        if states.iter().any(|s| !s.current()) {
            return Err("completed source unexpectedly retired".to_owned());
        }
        Ok::<_, String>(())
    }
    .await;
    eprintln!(
        "streams={streams} result={outcome:?} source_clock_max_late_ms={} polled={:?} received={:?}",
        max_late.load(Ordering::Relaxed),
        polled
            .iter()
            .map(|n| n.load(Ordering::Acquire))
            .collect::<Vec<_>>(),
        received
            .iter()
            .map(|n| n.load(Ordering::Acquire))
            .collect::<Vec<_>>()
    );
    work_stop.cancel();
    let production = producer.await;
    let deliveries = join_all(tasks).await;
    let mut delivery_errors = Vec::new();
    for delivery in deliveries {
        match delivery {
            Ok(Ok(sub)) => drop(sub),
            Ok(Err(error)) => delivery_errors.push(error),
            Err(error) => delivery_errors.push(error.to_string()),
        }
    }
    pool.close().await;
    stop.cancel();
    server.await.unwrap().unwrap();
    for telemetry in &stats {
        assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    }
    assert_eq!(global().used(), baseline);
    assert_eq!(fec_slots().available_permits(), permits);
    assert!(
        delivery_errors.is_empty() || outcome.is_err(),
        "{delivery_errors:?}"
    );
    production.expect("owned producer must shut down");
    outcome.expect("fixed-rate source must fit bounded input queues");
    for telemetry in stats {
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot["expired_records"], 0);
        assert_eq!(snapshot["queue_dropped_shards"], 0);
        assert!(snapshot["received_parity_bytes"].as_u64().unwrap() > 0);
    }
}

#[test]
fn failure_history_survives_late_assertion() {
    const CHILD: &str = "FLUSSONIX_OWNED_RATE_HISTORY_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("owned") {
        let h = Arc::new(StdMutex::new(Evidence {
            attempts: history::History::new(),
            worst: None,
        }));
        let _guard = DumpOnFailure(h.clone());
        for sequence in 0..70 {
            h.lock().unwrap().attempts.push(Attempt {
                stream: 0,
                sequence,
                scheduled_ns: 0,
                wake_ns: 0,
                attempt_ns: 0,
                capacity: 0,
                admitted: [sequence, 0],
                polled: [0, 0],
                received: [0, 0],
                accepted: sequence != 69,
            });
        }
        assert_eq!(1, 2, "owned late resource assertion");
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "fxlink::client::rate_tests::failure_history_survives_late_assertion",
            "--nocapture",
        ])
        .env(CHILD, "owned")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: Vec<_> = logs
        .lines()
        .filter(|l| l.starts_with("fixed_rate_source_history="))
        .collect();
    assert_eq!(rows.len(), 64, "{logs}");
    assert!(rows[0].contains("sequence: 6,"), "{logs}");
    assert!(
        rows[63].contains("sequence: 69,") && rows[63].contains("accepted: false"),
        "{logs}"
    );
}

#[tokio::test]
async fn enabled_service_trace_partitions_real_subscription_and_finishes() {
    use tracing_subscriber::{Layer, layer::Context, prelude::*};
    #[derive(Clone)]
    struct Capture(Arc<StdMutex<Vec<serde_json::Map<String, serde_json::Value>>>>);
    struct Visitor(serde_json::Map<String, serde_json::Value>);
    impl tracing::field::Visit for Visitor {
        fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
            self.0.insert(f.name().into(), format!("{v:?}").into());
        }
        fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
            self.0.insert(f.name().into(), v.into());
        }
        fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
            self.0.insert(f.name().into(), v.into());
        }
    }
    impl<S: tracing::Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            if event.metadata().target() != "flussonix::fxlink::service_timing" {
                return;
            }
            let mut visitor = Visitor(serde_json::Map::new());
            event.record(&mut visitor);
            let mut rows = self.0.lock().unwrap();
            assert!(rows.len() < 128, "unbounded diagnostic output");
            rows.push(visitor.0);
        }
    }
    let rows = Arc::new(StdMutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture(rows.clone()).with_filter(
        tracing_subscriber::filter::filter_fn(|m| {
            m.target() == "flussonix::fxlink::service_timing"
        }),
    ));
    let _guard = tracing::subscriber::set_default(subscriber);
    paced(1).await;
    let rows = rows.lock().unwrap();
    let mut previous = 0;
    let mut finals = 0;
    assert!(!rows.is_empty(), "no listener timing emitted");
    for row in rows.iter() {
        let elapsed = row["elapsed_us"].as_u64().unwrap();
        let phases: Vec<u64> = serde_json::from_str(row["phase_us"].as_str().unwrap()).unwrap();
        assert_eq!(phases.iter().sum::<u64>(), elapsed);
        let waits: Vec<Vec<u64>> = serde_json::from_str(row["wait_us"].as_str().unwrap()).unwrap();
        assert_eq!(
            waits.iter().flatten().sum::<u64>(),
            phases[1..8].iter().sum::<u64>()
        );
        assert!(!row.contains_key("token") && !row.contains_key("peer_key"));
        if row["final_report"] == true {
            finals += 1;
            let selected: Vec<u64> =
                serde_json::from_str(row["selected"].as_str().unwrap()).unwrap();
            assert!(
                selected[5] >= 400,
                "record selection missing from accounting"
            );
            assert!(phases[6] > 0, "absent callback must remain unknown");
        } else {
            assert!(elapsed >= previous + 100_000);
            previous = elapsed;
        }
    }
    assert_eq!(finals, 1, "source lifetime has no final summary");
}
