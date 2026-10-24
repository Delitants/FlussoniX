//! Controlled application-frame faults within real encrypted FXLink endpoints.
//! This does not claim feedback loss at the encrypted UDP wire layer.
use super::{ClientPool, Settings, transport};
use bytes::Bytes;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

static ACTIVE: Mutex<Option<Arc<Mutex<State>>>> = Mutex::new(None);
struct State {
    remaining: usize,
    frames: Vec<(Vec<u8>, bool)>,
}
struct Faults(Arc<Mutex<State>>);
impl Faults {
    fn new(remaining: usize) -> Self {
        let state = Arc::new(Mutex::new(State {
            remaining,
            frames: Vec::new(),
        }));
        assert!(ACTIVE.lock().unwrap().replace(state.clone()).is_none());
        Self(state)
    }
    fn finish(self) -> Vec<(Vec<u8>, bool)> {
        self.0.lock().unwrap().frames.clone()
    }
}
impl Drop for Faults {
    fn drop(&mut self) {
        *ACTIVE.lock().unwrap() = None;
    }
}
pub(super) fn discard(bytes: &[u8]) -> bool {
    // This fixture admits stream0. Unrelated pure route tests may run in
    // parallel without the real-endpoint test mutex and use other identities.
    if bytes.get(8..16) != Some(0u64.to_be_bytes().as_slice()) {
        return false;
    }
    let Some(state) = ACTIVE.lock().unwrap().clone() else {
        return false;
    };
    let mut state = state.lock().unwrap();
    let dropped = state.remaining > 0;
    state.remaining = state.remaining.saturating_sub(1);
    assert!(state.frames.len() < 64, "feedback fixture frame cap");
    state.frames.push((bytes.to_vec(), dropped));
    dropped
}
#[tokio::test]
async fn two_consecutive_feedback_losses_recover_with_original_500ms_deadline() {
    feedback_case(2, false).await;
}
#[tokio::test]
async fn delivered_feedback_copy_spends_only_one_repair() {
    feedback_case(0, false).await;
}
#[tokio::test]
async fn v4_copies_recover_while_progress_writer_is_stalled() {
    feedback_case(2, true).await;
}
fn record(sequence: u8, length: usize) -> Bytes {
    let mut bytes = vec![sequence; length];
    bytes[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
async fn feedback_case(drop_count: usize, block_progress: bool) {
    if std::env::var_os("FLUSSONIX_GAP_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn,flussonix::fxlink=trace")
            .with_test_writer()
            .try_init();
    }
    use futures_util::{StreamExt, future::BoxFuture};
    use std::{
        cmp::Reverse,
        collections::BinaryHeap,
        net::SocketAddr,
        sync::atomic::{AtomicBool, Ordering},
    };
    use tokio::{net::UdpSocket, time::Instant};

    struct Feed(CancellationToken, CancellationToken);
    impl super::Provider for Feed {
        fn subscribe(
            &self,
            r: super::Request,
            _: SocketAddr,
        ) -> BoxFuture<'_, Result<super::Source, u16>> {
            let start = self.0.clone();
            let rest = self.1.clone();
            Box::pin(async move {
                if r.peer_key != "owned-peer" || r.token != "owned-viewer" {
                    return Err(403);
                }
                let first = futures_util::stream::once(async move {
                    start.cancelled().await;
                    Ok(record(0, 1000))
                });
                let second = futures_util::stream::once(async move {
                    rest.cancelled().await;
                    Ok(record(1, 1000))
                });
                Ok(super::Source {
                    pending_records: None,
                    records: Box::pin(
                        first
                            .chain(second)
                            .chain(futures_util::stream::iter(
                                (2..16).map(|n| Ok(record(n, 1000))),
                            ))
                            .chain(futures_util::stream::pending()),
                    ),
                    current: Arc::new(|| true),
                    cancelled: CancellationToken::new(),
                })
            })
        }
    }

    let _serial = transport::test_lock().lock().await;
    let before = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let listener = super::Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let remote = listener.address();
    let source_egress = listener.egress();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let rest = CancellationToken::new();
    let serving =
        tokio::spawn(listener.serve(Arc::new(Feed(start.clone(), rest.clone())), stop.clone()));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let armed = Arc::new(AtomicBool::new(false));
    let drop_media = armed.clone();
    let relay_stop = CancellationToken::new();
    let relay_cancel = relay_stop.clone();
    let relay = tokio::spawn(async move {
        let mut client = None;
        let mut sequence = 0u64;
        let mut pending = BinaryHeap::new();
        let mut buffer = [0u8; 65536];
        let mut dropped = Vec::new();
        loop {
            let due = pending
                .peek()
                .map(|Reverse((due, _, _, _))| *due)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
            tokio::select! {
                _=relay_cancel.cancelled()=>break,
                _=tokio::time::sleep_until(due), if !pending.is_empty()=>{
                    let Reverse((_, _, bytes, to)): Reverse<(Instant,u64,Vec<u8>,SocketAddr)> = pending.pop().unwrap();
                    socket.send_to(&bytes,to).await.unwrap();
                },
                packet=socket.recv_from(&mut buffer)=>{
                    let (length,from)=packet.unwrap();
                    let to=if from==remote {client.expect("client before response")} else {
                        assert!(client.is_none_or(|known|known==from));
                        client=Some(from);remote
                    };
                    // Media shards are about 1.1 KiB. Exclude >=1200-byte PMTU
                    // probes and arm only after the TLS handshake settles.
                    if from==remote && (1050..1200).contains(&length) && drop_media.swap(false,Ordering::AcqRel) {
                        dropped.push(length);
                        continue;
                    }
                    assert!(pending.len()<8192,"owned relay exceeded its cap");
                    sequence+=1;
                    pending.push(Reverse((Instant::now()+Duration::from_millis(95),sequence,buffer[..length].to_vec(),to)));
                }
            }
        }
        dropped
    });
    let faults = Faults::new(drop_count);
    let progress = block_progress.then(super::client::review_tests::ProgressGuard::new);
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
    tokio::time::sleep(Duration::from_millis(350)).await;
    let rtt = sub.telemetry().snapshot()["rtt_ms"].as_u64().unwrap();
    armed.store(false, Ordering::Release);
    start.cancel();
    if block_progress {
        // Hold consumption of record0 until media1 is already in flight. Its
        // eventual Progress write then has a full independent liveness window
        // across the ~540ms recovery, without extending either media deadline.
        tokio::time::timeout(Duration::from_secs(2), async {
            while sub.telemetry().snapshot()["received_shards"]
                .as_u64()
                .unwrap()
                == 0
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("warm record received");
    } else {
        assert_eq!(sub.recv().await.unwrap(), record(0, 1000));
    }
    armed.store(true, Ordering::Release);
    let began = std::time::Instant::now();
    rest.cancel();
    if let Some(progress) = &progress {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(sub.recv().await.unwrap(), record(0, 1000));
        tokio::time::timeout(Duration::from_secs(2), async {
            while !progress.entered() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("progress write entered its stall");
        assert_eq!(
            sub.telemetry().snapshot()["repair_requested_shards"],
            0,
            "writer must stall before first repair attempt"
        );
    }
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        let mut records = vec![record(0, 1000)];
        rest.cancel();
        while records.len() < 16 {
            records.push(sub.recv().await?);
        }
        Ok::<_, String>(records)
    })
    .await;
    let elapsed = began.elapsed();
    let telemetry = sub.telemetry().snapshot();
    drop(progress);
    let faults = faults.finish();
    drop(sub);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    relay_stop.cancel();
    let dropped = relay.await.unwrap();
    assert_eq!(
        transport::global().used(),
        before,
        "real-listener repair leaked state"
    );
    eprintln!(
        "feedback copy repair: RTT={rtt}, dropped={dropped:?}, elapsed={elapsed:?}, outcome={:?}, telemetry={telemetry}",
        outcome.as_ref().map(|r| r.as_ref().map(Vec::len))
    );
    assert!(
        (180..=240).contains(&rtt),
        "RTT profile not exercised: {rtt}"
    );
    assert_eq!(dropped.len(), 1, "one original media loss required");
    assert_eq!(
        faults.iter().filter(|(_, dropped)| *dropped).count(),
        drop_count,
        "application feedback fault count"
    );
    eprintln!("application feedback frames: {faults:?}");
    assert!(
        faults.len() >= if drop_count == 2 { 3 } else { 2 },
        "primary and copy were not both observed"
    );
    assert_eq!(
        faults[0].0, faults[1].0,
        "copy changed frame identity/payload"
    );
    assert_eq!(&faults[0].0[..8], b"FXRQ\x02\x00\x00\x20");
    if drop_count == 2 {
        assert_ne!(
            &faults[0].0[24..32],
            &faults[2].0[24..32],
            "RTT retry reused the lost sequence"
        );
    }
    assert_eq!(
        outcome
            .expect("bounded real repair")
            .expect("sender rejected useful repair"),
        (0..16).map(|n| record(n, 1000)).collect::<Vec<_>>()
    );
    assert_eq!(
        source_egress.load(Ordering::Relaxed),
        18496,
        "sixteen originals plus exactly one repair required"
    );
    assert!(
        elapsed < Duration::from_millis(650),
        "original receive gap deadline exceeded"
    );
}
