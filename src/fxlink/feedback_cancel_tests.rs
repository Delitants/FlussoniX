//! Cancellation with a real pending copy and a blocked, started decode job.
use super::{review_tests::PauseGuard, *};
use std::sync::{Mutex as StdMutex, atomic::AtomicBool};

struct Trigger {
    state: Arc<State>,
    pause: Arc<PauseGuard>,
    cancel: CancellationToken,
    fired: AtomicBool,
    remaining_ms: std::sync::atomic::AtomicU64,
}
static ACTIVE: StdMutex<Option<Arc<Trigger>>> = StdMutex::new(None);
struct TriggerGuard(Arc<Trigger>);
impl TriggerGuard {
    fn new(state: Arc<State>, pause: Arc<PauseGuard>, cancel: CancellationToken) -> Self {
        let trigger = Arc::new(Trigger {
            state,
            pause,
            cancel,
            fired: AtomicBool::new(false),
            remaining_ms: std::sync::atomic::AtomicU64::new(0),
        });
        assert!(ACTIVE.lock().unwrap().replace(trigger.clone()).is_none());
        Self(trigger)
    }
}
impl Drop for TriggerGuard {
    fn drop(&mut self) {
        *ACTIVE.lock().unwrap() = None;
    }
}
// Observe the receive loop's real ownership, without changing copy pacing,
// deadlines or decode work. Fire ordinary caller cancellation at coexistence.
pub(super) fn observe(state: &Arc<State>, copy_due: Option<u64>, decoding: bool, at: u64) {
    let Some(trigger) = ACTIVE.lock().unwrap().clone() else {
        return;
    };
    if let Some(due) = copy_due
        && Arc::ptr_eq(state, &trigger.state)
        && decoding
        && trigger.pause.entered()
        && !trigger.fired.swap(true, Ordering::AcqRel)
    {
        trigger
            .remaining_ms
            .store(due.saturating_sub(at), Ordering::Release);
        trigger.cancel.cancel();
    }
}
fn record(sequence: u8) -> Bytes {
    let mut bytes = vec![sequence; 1000];
    bytes[..4].copy_from_slice(&996u32.to_be_bytes());
    Bytes::from(bytes)
}
async fn until(label: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_millis(350), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect(label);
}
#[tokio::test]
async fn v4_cancellation_discards_armed_copy_before_joining_blocked_decoder() {
    cancellation_case(4).await;
}
#[tokio::test]
async fn v5_cancellation_discards_armed_copy_before_joining_blocked_decoder() {
    cancellation_case(5).await;
}
#[tokio::test]
async fn v7_cancellation_discards_armed_copy_before_joining_blocked_decoder() {
    cancellation_case(7).await;
}
#[tokio::test]
async fn v8_cancellation_discards_armed_copy_before_joining_blocked_decoder() {
    cancellation_case(8).await;
}
async fn cancellation_case(version: u8) {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let slots = fec_slots().available_permits();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(super::super::transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/live", endpoint.local_addr().unwrap());
    let remote = endpoint.clone();
    let settings = Settings {
        latency_ms: 500,
        parity_shards: 0,
        ..Settings::default()
    };
    let encoding = settings.clone();
    let frames = Arc::new(StdMutex::new(Vec::<Bytes>::new()));
    let seen = frames.clone();
    let primary = CancellationToken::new();
    let first_feedback = primary.clone();
    let warm = CancellationToken::new();
    let warm_ready = warm.clone();
    let reset = CancellationToken::new();
    let reset_seen = reset.clone();
    let done = CancellationToken::new();
    let stop = done.clone();
    let server = tokio::spawn(async move {
        let connection = remote.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            version: negotiated,
            id,
            generation,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("subscription required")
        };
        assert_eq!(negotiated, version);
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let mut encoder =
            super::super::Encoder::new(encoding, id, generation, Budget::new(1 << 20)).unwrap();
        // Warm up with data only. Announce-before-original can legitimately
        // request record0 on a slow runner, before this cancellation case starts.
        let encoded = encoder.encode(record(0), now()).unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        for packet in encoded.datagrams {
            connection.send_datagram_wait(packet).await.unwrap();
        }
        warm_ready.cancelled().await;
        // Only missing record1 is exposed after the unread warm record is queued.
        // No future announcement may add another request to the golden primary.
        let _missing = encoder.encode(record(1), now()).unwrap();
        control::write(
            &mut send,
            &Control::Announce {
                record: 1,
                len: 1000,
            },
        )
        .await
        .unwrap();
        let frame = tokio::time::timeout(Duration::from_millis(350), connection.read_datagram())
            .await
            .unwrap()
            .unwrap();
        seen.lock().unwrap().push(frame);
        first_feedback.cancel();
        // The pause/trigger already exist. Data-only record2 starts the real
        // blocked job without an intervening client gate or speculative query.
        let encoded = encoder.encode(record(2), now()).unwrap();
        for packet in encoded.datagrams {
            connection.send_datagram_wait(packet).await.unwrap();
        }
        let mut reset_observed = false;
        loop {
            tokio::select! {
                _=stop.cancelled()=>break,
                message=control::read(&mut recv), if !reset_observed=>{
                    assert!(message.is_err(), "unexpected reliable feedback");
                    reset_observed=true;
                    reset_seen.cancel();
                },
                frame=connection.read_datagram()=>{
                    let frame=frame.unwrap();
                    if frame.starts_with(b"FXRQ") {
                        let mut frames=seen.lock().unwrap();
                        assert!(frames.len()<64,"feedback observation cap exceeded");
                        frames.push(frame);
                    }
                }
            }
        }
        assert!(
            connection.close_reason().is_none(),
            "subscription cancellation closed the peer"
        );
    });
    let pool = ClientPool::with_features(super::super::Features {
        geometry_hints: false,
        trimmed_records: version >= 6,
        repair_parity: version >= 7,
        bootstrap_repair: version == 8,
    });
    let cancel = CancellationToken::new();
    let mut sub = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            cancel.clone(),
        )
        .await
        .unwrap();
    until("warm decoded record must be queued", || {
        sub.state.queue.lock().unwrap().len() == 1
    })
    .await;
    assert!(
        frames.lock().unwrap().is_empty(),
        "warm-up sent repair feedback"
    );
    let pause = Arc::new(PauseGuard::new());
    let trigger = TriggerGuard::new(sub.state.clone(), pause.clone(), cancel);
    warm.cancel();
    tokio::time::timeout(Duration::from_millis(350), primary.cancelled())
        .await
        .unwrap();
    let primary_frame = frames.lock().unwrap()[0].clone();
    // Independent literal identity: stream0/generation1/sequence1, missing
    // record1's first block (2048), one request for data shard0.
    let mut expected = b"FXRQ\x02\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x08\x00\x00\x01\x00\x00".to_vec();
    if version >= 7 {
        expected[4] = 3;
    }
    if version == 8 {
        expected[4] = 4;
        expected.splice(40..40, [0, 0]); // mode0 plus reserved byte in format4.
    }
    assert_eq!(primary_frame.as_ref(), expected.as_slice());
    until(
        "armed copy and blocked decoder overlap was not reached",
        || trigger.0.fired.load(Ordering::Acquire),
    )
    .await;
    assert!(pause.entered(), "decode job did not enter the barrier");
    assert!(
        trigger.0.remaining_ms.load(Ordering::Acquire) > 0,
        "copy was already due at cancellation"
    );
    // Break caught: joining a blocked job before retirement leaves active
    // telemetry and stale queued output, despite token-based current()==false.
    until(
        "cancellation must retire active state before decoder release",
        || !sub.telemetry().active.load(Ordering::Acquire),
    )
    .await;
    assert!(!sub.current());
    assert!(
        sub.state.queue.lock().unwrap().is_empty(),
        "retirement retained queued media"
    );
    assert!(
        sub.recv().await.is_err(),
        "cancelled subscription published media"
    );
    tokio::time::timeout(Duration::from_millis(350), reset.cancelled())
        .await
        .expect("stream must reset before decode release");
    assert!(sub.connection.close_reason().is_none());
    assert_eq!(
        fec_slots().available_permits(),
        slots - 1,
        "started job was detached or released its permit early"
    );
    assert!(
        sub.telemetry().snapshot()["state_bytes"].as_u64().unwrap() > 0,
        "job lost ownership of charged work"
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        frames.lock().unwrap().as_slice(),
        &[primary_frame],
        "armed feedback copy escaped after cancellation"
    );
    eprintln!(
        "v{version}: cancellation with {}ms before copy due; active=false, queued=0, stream reset, FXRQ frames=1, one decode permit and {} charged bytes retained until release",
        trigger.0.remaining_ms.load(Ordering::Acquire),
        sub.telemetry().snapshot()["state_bytes"]
    );
    let telemetry = sub.telemetry();
    drop(trigger);
    drop(pause);
    until("decode join must release all subscription charges", || {
        telemetry.snapshot()["state_bytes"] == 0
    })
    .await;
    assert!(
        sub.state.queue.lock().unwrap().is_empty(),
        "late decode published stale media"
    );
    assert_eq!(fec_slots().available_permits(), slots);
    done.cancel();
    server.await.unwrap();
    drop(sub);
    pool.close().await;
    endpoint.close(0u32.into(), b"done");
    endpoint.wait_idle().await;
    assert_eq!(
        global().used(),
        baseline,
        "cancellation leaked hierarchical reservations"
    );
}
