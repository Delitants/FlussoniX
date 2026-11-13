//! Actual v5 listener/client, changing feedback delay and one encrypted UDP loss.
use super::{transport, *};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::UdpSocket, time::Instant};
use tokio_util::sync::CancellationToken;
fn native(sequence: u64) -> Bytes {
    let mut b = vec![83; 350];
    b[..4].copy_from_slice(&346u32.to_be_bytes());
    b[4..12].copy_from_slice(&sequence.to_be_bytes());
    Bytes::from(b)
}
struct Feed;
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::unfold(0u64, |sequence| async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some((Ok(native(sequence)), sequence + 1))
                })),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
// Break caught: muting falls back to padded legacy data, fails to suppress or
// restore copies, or blocks genuine requested-data repair while copies are muted.
#[tokio::test]
async fn muted_compact_originals_remain_unpadded_and_repair_real_udp_loss() {
    let _serial = transport::test_lock().lock().await;
    let before = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings::default();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let remote = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let serving = tokio::spawn(listener.serve(Arc::new(Feed), stop.clone()));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let feedback_delay = Arc::new(AtomicU64::new(2));
    let delayed = feedback_delay.clone();
    let drop_one = Arc::new(AtomicBool::new(false));
    let armed = drop_one.clone();
    let relay_stop = CancellationToken::new();
    let halt = relay_stop.clone();
    let relay = tokio::spawn(async move {
        let mut client = None;
        let mut heap = BinaryHeap::new();
        let mut serial = 0u64;
        let mut buffer = [0u8; 65536];
        let mut dropped = 0;
        loop {
            let due = heap
                .peek()
                .map(|Reverse((at, _, _, _))| *at)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
            tokio::select! {
                _=halt.cancelled()=>break,
                _=tokio::time::sleep_until(due),if !heap.is_empty()=>{
                    let Reverse((_,_,body,to)):Reverse<(Instant,u64,Vec<u8>,SocketAddr)>=heap.pop().unwrap();
                    socket.send_to(&body,to).await.unwrap();
                },
                result=socket.recv_from(&mut buffer)=>{
                    let (length,from)=result.unwrap();
                    let (to,delay)=if from==remote {(client.unwrap(),2)} else {
                        assert!(client.is_none_or(|known|known==from),"client opened a second connection");client=Some(from);(remote,delayed.load(Ordering::Relaxed))
                    };
                    if from==remote && (400..1200).contains(&length) && armed.swap(false,Ordering::AcqRel) {dropped+=1;continue;}
                    assert!(heap.len()<8192);serial+=1;
                    heap.push(Reverse((Instant::now()+Duration::from_millis(delay),serial,buffer[..length].to_vec(),to)));
                }
            }
        }
        dropped
    });
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &format!("fxlink://{address}/owned-parity"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let telemetry = sub.telemetry();
    let mut baseline = None;
    let mut muted = None;
    let mut muted_end = None;
    let mut restored = None;
    let outcome = tokio::time::timeout(Duration::from_secs(25), async {
        let mut sequence = 0u64;
        let mut previous_parity = 0;
        let mut unchanged_since = Instant::now();
        let mut mute_at = None;
        let mut recover_at = None;
        loop {
            let received = sub.recv().await?;
            if received != native(sequence) {
                return Err("native bytes or sequence changed".to_string());
            }
            sequence += 1;
            let stats = telemetry.snapshot();
            let parity = stats["received_parity_bytes"].as_u64().unwrap();
            if parity != previous_parity {
                previous_parity = parity;
                unchanged_since = Instant::now();
            }
            if sequence == 20 {
                baseline = Some(stats.clone());
                feedback_delay.store(350, Ordering::Relaxed);
            }
            if sequence >= 60
                && mute_at.is_none()
                && unchanged_since.elapsed() >= Duration::from_millis(500)
            {
                mute_at = Some(sequence);
                muted = Some(stats.clone());
                drop_one.store(true, Ordering::Release);
            }
            if mute_at.is_some_and(|at| sequence >= at + 40) && recover_at.is_none() {
                muted_end = Some(stats.clone());
                recover_at = Some(sequence);
                feedback_delay.store(2, Ordering::Relaxed);
            }
            if recover_at.is_some_and(|at| sequence >= at + 80) {
                restored = Some(stats);
                return Ok::<_, String>(());
            }
        }
    })
    .await;
    drop(sub);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    relay_stop.cancel();
    let dropped = relay.await.unwrap();
    let final_stats = telemetry.snapshot();
    let total_egress = egress.load(Ordering::Relaxed);
    assert_eq!(
        transport::global().used(),
        before,
        "parity fixture leaked ownership"
    );
    assert_eq!(final_stats["state_bytes"], 0);
    outcome
        .expect("protection did not mute and recover within bounded time")
        .expect("muted transport lost native media");
    let baseline = baseline.unwrap();
    let muted = muted.unwrap();
    let muted_end = muted_end.unwrap();
    let restored = restored.unwrap();
    assert!(
        baseline["received_parity_bytes"].as_u64().unwrap() >= 18 * 434,
        "initial protection absent"
    );
    assert_eq!(
        muted_end["received_parity_bytes"], muted["received_parity_bytes"],
        "muted copies were queued or replayed"
    );
    let data = muted_end["received_data_bytes"].as_u64().unwrap()
        - muted["received_data_bytes"].as_u64().unwrap();
    assert!((32 * 434..=56 * 434).contains(&data));
    assert_eq!(data % 434, 0, "muting changed compact systematic width");
    assert!(
        restored["received_parity_bytes"].as_u64().unwrap()
            - muted_end["received_parity_bytes"].as_u64().unwrap()
            >= 20 * 434,
        "healthy-path protection did not resume"
    );
    assert_eq!(dropped, 1, "encrypted UDP loss was not exercised");
    assert!(
        restored["repair_requested_shards"].as_u64().unwrap() >= 1,
        "missing-data repair was not exercised while muted"
    );
    assert_eq!(
        restored["fec_recovered_shards"], 0,
        "a proactive copy masked the muted loss"
    );
    assert_eq!(restored["expired_records"], 0);
    assert_eq!(restored["queue_dropped_shards"], 0);
    assert_eq!(
        total_egress % 434,
        0,
        "listener emitted padded systematic bytes"
    );
}
