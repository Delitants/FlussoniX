//! Reproduce the cached startup mix at the real transport boundary.
use super::super::{ClientPool, Listener, Provider, Request, Settings, Source, transport};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::UdpSocket, time::Instant};
use tokio_util::sync::CancellationToken;

// Literal lengths from the retained first, delay-only failed subscription.
const LENGTHS: [usize; 11] = [191, 9479, 350, 274, 3962, 293, 315, 4423, 325, 297, 4553];
fn record(index: usize) -> Bytes {
    let length = if index < LENGTHS.len() {
        LENGTHS[index]
    } else if index % 3 == 1 {
        4500
    } else {
        350
    };
    let mut bytes = vec![index as u8; length];
    bytes[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
const RECORDS: usize = 64;
struct Cached {
    connection: quinn::Connection,
    empty_space: usize,
    observations: Arc<Mutex<Vec<(usize, usize)>>>,
    demand_queries: Arc<std::sync::atomic::AtomicUsize>,
}
impl Provider for Cached {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let connection = self.connection.clone();
        let empty = self.empty_space;
        let observations = self.observations.clone();
        let demand_queries = self.demand_queries.clone();
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: Some(Arc::new(move || {
                    demand_queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Some(true)
                })),
                records: Box::pin(
                    futures_util::stream::iter(0..RECORDS)
                        .map(move |i| {
                            observations.lock().unwrap().push((
                                i,
                                empty.saturating_sub(connection.datagram_send_buffer_space()),
                            ));
                            Ok(record(i))
                        })
                        .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}

// Break caught: a ready source is polled behind a large local media FIFO.
// This admission contract does not reproduce the separate daemon restart.
#[tokio::test]
async fn cached_compact_mix_survives_cold_190ms_rtt_without_extending_500ms_records() {
    if std::env::var_os("FLUSSONIX_ADMISSION_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn,flussonix::fxlink=trace")
            .with_test_writer()
            .try_init();
    }
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let remote = listener.address();
    let stop = CancellationToken::new();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let seen = observations.clone();
    let demand_queries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let queried = demand_queries.clone();
    let halt = stop.clone();
    let serving = tokio::spawn(async move {
        let incoming = listener.endpoint.accept().await.unwrap();
        let budget = transport::peer_budget(incoming.remote_address().ip()).unwrap();
        let _queues = budget.reserve(transport::QUEUE_BYTES).unwrap();
        let connection = incoming.await.unwrap();
        let empty_space = connection.datagram_send_buffer_space();
        let provider = Arc::new(Cached {
            connection: connection.clone(),
            empty_space,
            observations: seen,
            demand_queries: queried,
        });
        super::serve_peer(
            connection,
            provider,
            listener.settings.clone(),
            budget,
            halt,
            listener.egress.clone(),
            None,
        )
        .await;
        listener.endpoint.close(0u32.into(), b"done");
        listener.endpoint.wait_idle().await;
    });
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let relay_stop = CancellationToken::new();
    let cancel = relay_stop.clone();
    let relay = tokio::spawn(async move {
        type Packet = (Instant, u64, Vec<u8>, SocketAddr);
        let mut pending: BinaryHeap<Reverse<Packet>> = BinaryHeap::new();
        let mut client = None;
        let mut serial = 0u64;
        let mut queued = 0usize;
        let mut peak = 0;
        let mut buffer = [0u8; 65536];
        let mut forwarded = [0u64; 2];
        loop {
            let due = pending
                .peek()
                .map(|Reverse((due, _, _, _))| *due)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
            tokio::select! {
                _=cancel.cancelled()=>break,
                _=tokio::time::sleep_until(due),if !pending.is_empty()=>{
                    let Reverse((_,_,bytes,to))=pending.pop().unwrap();queued-=bytes.len();
                    forwarded[usize::from(to!=remote)]+=1;
                    socket.send_to(&bytes,to).await.unwrap();
                },
                result=socket.recv_from(&mut buffer)=>{
                    let (length,from)=result.unwrap();
                    let to=if from==remote {client.expect("client first")} else {
                        assert!(client.is_none_or(|known|known==from));client=Some(from);remote
                    };
                    queued+=length;peak=peak.max(queued);assert!(pending.len()<8192 && queued<=4*1024*1024);
                    serial+=1;pending.push(Reverse((Instant::now()+Duration::from_millis(95),serial,buffer[..length].to_vec(),to)));
                }
            }
        }
        (forwarded, peak)
    });
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &format!("fxlink://{address}/owned-cache"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let began = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        let mut received = Vec::new();
        for _ in 0..RECORDS {
            received.push(sub.recv().await?);
        }
        Ok::<_, String>(received)
    })
    .await;
    let snapshot = sub.telemetry().snapshot();
    let current = sub.current();
    drop(sub);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap();
    drop(listener._incoming);
    relay_stop.cancel();
    let proxy = relay.await.unwrap();
    assert_eq!(transport::global().used(), baseline);
    eprintln!(
        "cold compact cache: elapsed={:?}, proxy={proxy:?}, outcome={:?}, telemetry={snapshot}",
        began.elapsed(),
        outcome.as_ref().map(|r| r.as_ref().map(Vec::len))
    );
    assert!(
        demand_queries.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "admission-limited originals must reach the live BBR controller"
    );
    let observations = observations.lock().unwrap();
    let largest = observations
        .iter()
        .map(|(_, queued)| *queued)
        .max()
        .unwrap();
    assert!(
        largest <= 1216,
        "new source record started behind excessive queued media: max={largest}, observations={observations:?}"
    );
    assert_eq!(
        outcome
            .expect("bounded cache receive")
            .expect("cached startup media must not retire"),
        (0..RECORDS).map(record).collect::<Vec<_>>()
    );
    assert!(
        current,
        "subscription must remain healthy after cache burst"
    );
    assert_eq!(snapshot["expired_records"], 0);
    assert_eq!(snapshot["queue_dropped_shards"], 0);
    assert!(proxy.0.iter().all(|n| *n > 10));
}

fn tiny(index: usize) -> Bytes {
    let mut bytes = vec![73; 350];
    bytes[..4].copy_from_slice(&346u32.to_be_bytes());
    bytes[4..12].copy_from_slice(&(index as u64).to_be_bytes());
    Bytes::from(bytes)
}
struct Rapid;
impl Provider for Rapid {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            let records: super::super::RecordStream = if request.name == "idle" {
                Box::pin(futures_util::stream::pending())
            } else {
                let began = Instant::now();
                Box::pin(
                    futures_util::stream::unfold(0usize, move |index| async move {
                        if index == 512 {
                            return None;
                        }
                        tokio::time::sleep_until(began + Duration::from_millis(index as u64 * 2))
                            .await;
                        Some((Ok(tiny(index)), index + 1))
                    })
                    .chain(futures_util::stream::pending()),
                )
            };
            Ok(Source {
                pending_records: None,
                records,
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}

// Break caught: requiring every tiny record to wait for a 10 ms queue poll,
// or cancelling another subscription when the idle sibling is dropped.
#[tokio::test]
async fn tiny_live_rate_and_idle_sibling_cancel_avoid_a_per_record_tick_limit() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let serving = tokio::spawn(listener.serve(Arc::new(Rapid), stop.clone()));
    let pool = ClientPool::default();
    let mut idle = Some(
        pool.subscribe(
            &format!("fxlink://{address}/idle"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap(),
    );
    let mut sub = pool
        .subscribe(
            &format!("fxlink://{address}/rapid"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(idle.as_ref().unwrap().connection_id(), sub.connection_id());
    let began = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        for index in 0..512 {
            let bytes = sub.recv().await?;
            if bytes != tiny(index) {
                return Err("tiny record sequence or payload changed".to_string());
            }
            if index == 256 {
                assert!(idle.as_ref().unwrap().current());
                drop(idle.take());
            }
        }
        Ok::<_, String>(())
    })
    .await;
    let elapsed = began.elapsed();
    let current = sub.current();
    let snapshot = sub.telemetry().snapshot();
    drop(sub);
    drop(idle);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    assert_eq!(transport::global().used(), baseline);
    assert_eq!(outcome, Ok(Ok(())));
    assert!(current);
    assert_eq!(snapshot["expired_records"], 0);
    assert_eq!(snapshot["queue_dropped_shards"], 0);
    eprintln!("512 tiny records with idle sibling: elapsed={elapsed:?}, state={snapshot}");
    assert!(
        elapsed >= Duration::from_millis(800),
        "paced source was not exercised"
    );
    assert!(
        elapsed < Duration::from_millis(1600),
        "tiny records must not wait for individual queue ticks"
    );
}
