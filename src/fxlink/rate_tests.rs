//! A drained v5 byte queue must not quantize a ready source to housekeeping.
use super::*;
use crate::fxlink::{Listener, Provider, Request, Source};
use futures_util::future::{BoxFuture, join_all};
use std::collections::BTreeMap;
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
const SHAPED_BPS: u64 = 15_500_000;
const OVERLOADED_BPS: u64 = 12_500_000;
const ONE_WAY_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug, Default)]
struct LinkReport {
    received: [usize; 2],
    forwarded: [usize; 2],
    bytes: [usize; 2],
    peak_queue_bytes: usize,
    peak_downlink_backlog: Duration,
}

async fn shaped_link(
    socket: tokio::net::UdpSocket,
    remote: SocketAddr,
    bits_per_second: u64,
    stop: CancellationToken,
) -> LinkReport {
    let mut report = LinkReport::default();
    let mut pending = BTreeMap::<(tokio::time::Instant, u64), (Bytes, SocketAddr, usize)>::new();
    let mut client = None;
    let mut serial = 0u64;
    let mut queued_bytes = 0usize;
    let mut downlink_end = tokio::time::Instant::now();
    let mut draining = false;
    let mut quiet_at = tokio::time::Instant::now();
    let mut drain_deadline = tokio::time::Instant::now();
    let mut buffer = [0u8; 65536];
    loop {
        let due = pending
            .first_key_value()
            .map(|(key, _)| key.0)
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(1));
        tokio::select! {
            _ = stop.cancelled(), if !draining => {
                draining = true;
                quiet_at = tokio::time::Instant::now() + Duration::from_millis(150);
                drain_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            }
            _ = tokio::time::sleep_until(quiet_at), if draining && pending.is_empty() => break,
            _ = tokio::time::sleep_until(drain_deadline), if draining => panic!("owned shaped link did not drain"),
            _ = tokio::time::sleep_until(due), if !pending.is_empty() => {
                let (_, (packet, destination, direction)) = pending.pop_first().unwrap();
                queued_bytes -= packet.len();
                socket.send_to(&packet, destination).await.unwrap();
                report.forwarded[direction] += 1;
            }
            packet = socket.recv_from(&mut buffer) => {
                let (len, from) = packet.unwrap();
                let (destination, direction) = if from == remote {
                    (client.expect("origin replied before a client request"), 1)
                } else {
                    assert!(client.is_none_or(|known| known == from), "unexpected second link client");
                    client = Some(from);
                    (remote, 0)
                };
                report.received[direction] += 1;
                report.bytes[direction] += len;
                let now = tokio::time::Instant::now();
                let due = if direction == 1 {
                    let nanos = (len as u128 * 8 * 1_000_000_000).div_ceil(bits_per_second as u128);
                    downlink_end = downlink_end.max(now) + Duration::from_nanos(nanos as u64);
                    report.peak_downlink_backlog = report.peak_downlink_backlog.max(downlink_end - now);
                    downlink_end + ONE_WAY_DELAY
                } else {
                    now + ONE_WAY_DELAY
                };
                serial += 1;
                queued_bytes += len;
                report.peak_queue_bytes = report.peak_queue_bytes.max(queued_bytes);
                assert!(pending.len() < 4096 && queued_bytes <= 8 << 20, "owned shaped link exceeded its queue bound");
                pending.insert((due, serial), (Bytes::copy_from_slice(&buffer[..len]), destination, direction));
                if draining {
                    quiet_at = now + Duration::from_millis(150);
                }
            }
        }
    }
    assert_eq!(
        report.received, report.forwarded,
        "shaped link silently discarded datagrams"
    );
    report
}
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
    paced(1, None, RECORDS).await;
}
#[tokio::test]
async fn shared_connection_keeps_two_independent_125_record_sources() {
    paced(2, None, RECORDS).await;
}
#[tokio::test]
async fn healthy_v9_source_releases_geometry_state() {
    let _serial = test_lock().lock().await;
    paced_locked(1, None, RECORDS, crate::fxlink::Features {
        trimmed_records: true,
        repair_parity: true,
        bootstrap_repair: true,
        geometry_hints: true,
    }).await;
}

#[tokio::test]
async fn two_sources_share_a_constrained_100ms_link_without_starvation() {
    paced(2, Some(SHAPED_BPS), RECORDS).await;
}

#[tokio::test]
async fn two_sources_share_a_short_overloaded_100ms_link_without_starvation() {
    paced(2, Some(OVERLOADED_BPS), 200).await;
}

async fn paced(streams: usize, shaped_bps: Option<u64>, records: usize) {
    assert!((1..=2).contains(&streams));
    let _serial = test_lock().lock().await;
    paced_locked(streams, shaped_bps, records, crate::fxlink::Features::default()).await;
}

async fn paced_locked(streams: usize, shaped_bps: Option<u64>, records: usize, features: crate::fxlink::Features) {
    let baseline = global().used();
    let permits = fec_slots().available_permits();
    let settings = Settings {
        latency_ms: 500,
        ..Default::default()
    };
    let cert = test_tls::Certificates::new();
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
        features,
    )
    .unwrap();
    let remote = listener.address();
    let (address, link) = if let Some(bits_per_second) = shaped_bps {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(shaped_link(socket, remote, bits_per_second, cancel.clone()));
        (address, Some((cancel, task)))
    } else {
        (remote, None)
    };
    let mut channels = HashMap::new();
    let mut senders = Vec::new();
    let polled: Vec<_> = (0..streams)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    for index in 0..streams {
        // The shaped 100 ms RTT needs startup flight room. Stay below the
        // production native fan-out's 256-record bound.
        let (tx, rx) = mpsc::channel(if shaped_bps.is_some() { 64 } else { 16 });
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
    let pool = ClientPool::with_features(features);
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
                for sequence in 0..records {
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
    let max_received_gap = Arc::new(AtomicUsize::new(0));
    let observed_gap = max_received_gap.clone();
    let producer = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let mut admitted = [0usize; 2];
        let outcome = async {
            for sequence in 0..records {
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
                    if streams == 2 {
                        observed_gap.fetch_max(received_before[0].abs_diff(received_before[1]), Ordering::Relaxed);
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
                .any(|n| n.load(Ordering::Acquire) != records)
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
        "streams={streams} result={outcome:?} source_clock_max_late_ms={} max_received_gap={} polled={:?} received={:?}",
        max_late.load(Ordering::Relaxed),
        max_received_gap.load(Ordering::Relaxed),
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
    let link_report = if let Some((cancel, task)) = link {
        cancel.cancel();
        Some(task.await.unwrap())
    } else {
        None
    };
    if let Some(report) = &link_report {
        eprintln!("constrained_link={report:?}");
    }
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
    if let Some(report) = link_report {
        // An oversubscribed burst can accumulate one link-buffer interval of
        // skew; keep both sources within the 500 ms media deadline. The
        // sustainable case has enough capacity for a tighter progress bound.
        let max_gap = if shaped_bps == Some(OVERLOADED_BPS) {
            50
        } else {
            32
        };
        assert!(
            max_received_gap.load(Ordering::Relaxed) <= max_gap,
            "one stream fell too far behind its peer on the shared link"
        );
        assert!(
            report.bytes[1] > 2_000_000,
            "link did not carry both media streams"
        );
        assert!(
            report.peak_downlink_backlog >= Duration::from_millis(50),
            "link never experienced meaningful shared queueing"
        );
    }
    for telemetry in stats {
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot["expired_records"], 0);
        assert_eq!(snapshot["queue_dropped_shards"], 0);
        assert!(snapshot["received_parity_bytes"].as_u64().unwrap() > 0);
        if features.geometry_hints {
            assert_eq!(snapshot["protocol_version"], 9);
            assert!(snapshot["geometry_hints"]["received_frames"].as_u64().unwrap() > 0);
        }
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
    let _serial = test_lock().lock().await;
    let _guard = tracing::subscriber::set_default(subscriber);
    paced_locked(1, None, RECORDS, crate::fxlink::Features::default()).await;
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
