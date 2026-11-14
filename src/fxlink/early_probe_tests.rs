//! Real encrypted ClientPool selection and wire checks. The source is a
//! controlled peer; actual Listener age/credit fences have separate regressions.
use super::*;
use crate::fxlink::{Encoder, RepairRequest, feedback, packet::Header};
use std::sync::{Mutex as StdMutex, atomic::AtomicBool};
#[derive(Clone, Debug)]
pub(super) struct Seen {
    pub(super) record: u64,
    pub(super) known: bool,
    pub(super) rtt: u64,
    pub(super) grace: u64,
    pub(super) at: u64,
    pub(super) first: u64,
    pub(super) allowed: bool,
    pub(super) wake: Option<u64>,
}
struct Watch {
    state: Arc<State>,
    samples: StdMutex<Vec<Seen>>,
    cancel: Option<CancellationToken>,
    fired: AtomicBool,
}
static ACTIVE: StdMutex<Option<Arc<Watch>>> = StdMutex::new(None);
struct Guard(Arc<Watch>);
impl Guard {
    fn new(state: Arc<State>, cancel: Option<CancellationToken>) -> Self {
        let watch = Arc::new(Watch {
            state,
            samples: StdMutex::new(Vec::new()),
            cancel,
            fired: AtomicBool::new(false),
        });
        assert!(ACTIVE.lock().unwrap().replace(watch.clone()).is_none());
        Self(watch)
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        *ACTIVE.lock().unwrap() = None;
    }
}
// Bounded passive observation. The cancellation control invokes ordinary retirement
// cancellation at selection, without changing policy, timers or ownership.
pub(super) fn observe(state: &Arc<State>, sample: Seen) {
    let Some(watch) = ACTIVE.lock().unwrap().clone() else {
        return;
    };
    if !Arc::ptr_eq(state, &watch.state) || sample.record > 1 {
        return;
    }
    let selected = sample.allowed;
    let mut samples = watch.samples.lock().unwrap();
    if samples.len() < 128 {
        samples.push(sample);
    }
    drop(samples);
    if selected
        && let Some(cancel) = &watch.cancel
        && !watch.fired.swap(true, Ordering::AcqRel)
    {
        cancel.cancel();
    }
}
fn record(id: u8, len: usize) -> Bytes {
    let mut v = vec![id; len];
    v[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(v)
}
async fn relay(socket: tokio::net::UdpSocket, remote: SocketAddr, stop: CancellationToken) {
    use std::{cmp::Reverse, collections::BinaryHeap};
    let mut pending = BinaryHeap::new();
    let mut client = None;
    let mut serial = 0u64;
    let mut buf = [0u8; 65536];
    loop {
        let due = pending
            .peek()
            .map(|Reverse((at, _, _, _))| *at)
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(1));
        tokio::select! {
            _=stop.cancelled()=>break,
            _=tokio::time::sleep_until(due),if !pending.is_empty()=>{
                let Reverse((_,_,bytes,to)):Reverse<(tokio::time::Instant,u64,Vec<u8>,SocketAddr)>=pending.pop().unwrap();socket.send_to(&bytes,to).await.unwrap();
            },
            packet=socket.recv_from(&mut buf)=>{
                let(n,from)=packet.unwrap();let to=if from==remote{client.unwrap()}else{assert!(client.is_none_or(|old|old==from));client=Some(from);remote};
                assert!(pending.len()<8192);serial+=1;pending.push(Reverse((tokio::time::Instant::now()+Duration::from_millis(95),serial,buf[..n].to_vec(),to)));
            }
        }
    }
}
#[tokio::test]
async fn early_unknown_probe_v7_encrypted_selects20ms() {
    encrypted(7, false, false).await;
}
#[tokio::test]
async fn early_unknown_probe_v7_known_keeps_normal_grace() {
    encrypted(7, true, false).await;
}
#[tokio::test]
async fn early_unknown_probe_v6_fallback_keeps_normal_grace() {
    encrypted(6, false, false).await;
}
#[tokio::test]
async fn early_unknown_probe_v5_fallback_keeps_normal_grace() {
    encrypted(5, false, false).await;
}
#[tokio::test]
async fn early_unknown_probe_cancellation_fences_selected_batch() {
    encrypted(7, false, true).await;
}
#[tokio::test]
async fn early_unknown_probe_observer_waits_for_allowed_selection() {
    let _serial = test_lock().lock().await;
    let cancel = CancellationToken::new();
    let state = Arc::new(State::new(cancel.clone()));
    let guard = Guard::new(state.clone(), Some(cancel.clone()));
    let mut sample = Seen {
        record: 0,
        known: false,
        rtt: 194,
        grace: 20,
        at: 0,
        first: 0,
        allowed: false,
        wake: Some(20),
    };
    observe(&state, sample.clone());
    assert!(
        !cancel.is_cancelled(),
        "must not cancel a grace-rejected empty batch"
    );
    sample.at = 20;
    sample.allowed = true;
    sample.wake = None;
    observe(&state, sample);
    assert!(cancel.is_cancelled());
    assert!(guard.0.fired.load(Ordering::Acquire));
}
async fn encrypted(version: u8, known: bool, cancel_at_selection: bool) {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(super::super::transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let remote = endpoint.local_addr().unwrap();
    let peer = endpoint.clone();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/owned-probe", socket.local_addr().unwrap());
    let relay_stop = CancellationToken::new();
    let relaying = tokio::spawn(relay(socket, remote, relay_stop.clone()));
    let settings = Settings {
        latency_ms: 500,
        ..Default::default()
    };
    let encoding = settings.clone();
    let expected = [record(0, 4835), record(1, 388), record(2, 405)];
    let source_records = expected.clone();
    let begin = CancellationToken::new();
    let source_begin = begin.clone();
    let done = CancellationToken::new();
    let halt = done.clone();
    let server = tokio::spawn(async move {
        let connection = peer.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            version: negotiated,
            id,
            generation,
            peer_key,
            token,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("subscription")
        };
        assert_eq!(negotiated, version);
        assert_eq!(
            (peer_key.as_str(), token.as_str()),
            ("owned-peer", "owned-viewer")
        );
        let budget = Budget::new(4 << 20);
        let routes = feedback::Routes::default();
        let mut route = routes.admit(id, version, &budget).unwrap();
        let mut encoder = Encoder::new(encoding.clone(), id, generation, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::for_version(version));
        let frames = source_records
            .iter()
            .map(|r| encoder.encode(r.clone(), 0).unwrap())
            .collect::<Vec<_>>();
        let mut allowance = super::super::repair::Allowance::new(0, 500);
        // Declared funded cohort, including contemporaneous originals omitted
        // from this three-record fixture; not a wire overhead measurement.
        assert!(allowance.original(0, 0, 96585));
        control::write(&mut send, &Control::Accepted).await.unwrap();
        source_begin.cancelled().await;
        let created = Instant::now();
        if known {
            for (i, r) in source_records.iter().enumerate() {
                control::write(
                    &mut send,
                    &Control::Announce {
                        record: i as u64,
                        len: r.len(),
                    },
                )
                .await
                .unwrap();
            }
        }
        for p in &frames[2].datagrams {
            connection.send_datagram_wait(p.clone()).await.unwrap();
        }
        let mut replies = 0;
        let mut seen = Vec::new();
        loop {
            tokio::select! {
                _=halt.cancelled()=>break,
                packet=connection.read_datagram()=>{let Ok(packet)=packet else{break};assert_eq!(packet[4],if version==7{3}else{2});routes.receive(packet,created.elapsed().as_millis()as u64);},
                request=route.next(&encoding)=>{
                    let crate::fxlink::feedback::DatagramRequest::Ordinary(crate::fxlink::RepairRequest {block_id:block,shard_indices:shards})=request else{panic!("repair")};let index=(block/super::super::MAX_BLOCKS_PER_RECORD as u64)as usize;assert!(index<2);
                    if !known&&index==1{assert_eq!(shards,[0],"unknown middle prefix must remain one");}
                    seen.push(RepairRequest{block_id:block,shard_indices:shards.clone()});
                    let at=created.elapsed().as_millis()as u64;assert!(at+5<500,"fixture source TTL exceeded");
                    let mut parity=false;
                    for p in &frames[index].datagrams{let h=Header::parse(p).unwrap();if shards.contains(&h.shard_index)&&(h.shard_index<h.data_count||(version==7&&!parity)){
                        if h.shard_index>=h.data_count{parity=true;}
                        assert!(allowance.repair(0,at,p.len()));connection.send_datagram_wait(p.clone()).await.unwrap();replies+=1;
                    }}
                }
            }
        }
        connection.close(0u32.into(), b"done");
        drop(route);
        drop(frames);
        drop(encoder);
        assert_eq!(budget.used(), 0);
        (seen, replies)
    });
    let pool = ClientPool::with_features(Features {
        geometry_hints: false,
        trimmed_records: true,
        repair_parity: true,
        bootstrap_repair: false,
    });
    let caller = CancellationToken::new();
    let mut sub = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings,
            caller.clone(),
        )
        .await
        .unwrap();
    assert_eq!(control::version(&sub.connection).unwrap(), version);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let rtt = sub.connection.rtt().as_millis() as u64;
    let normal = super::super::repair::reordering_grace(500, rtt);
    assert!(
        normal > 20,
        "fixture must distinguish cap from normal grace: RTT{rtt},grace{normal}"
    );
    let guard = Guard::new(
        sub.state.clone(),
        cancel_at_selection.then(|| sub.retirement_token()),
    );
    begin.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        let mut out = Vec::new();
        for _ in 0..3 {
            out.push(sub.recv().await?);
        }
        Ok::<_, String>(out)
    })
    .await;
    let samples = guard.0.samples.lock().unwrap().clone();
    let fired = guard.0.fired.load(Ordering::Acquire);
    drop(guard);
    drop(sub);
    done.cancel();
    pool.close().await;
    let peer_result = server.await;
    endpoint.close(0u32.into(), b"done");
    relay_stop.cancel();
    relaying.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(global().used(), baseline);
    let (seen, replies) = peer_result.unwrap();
    assert!(!samples.is_empty(), "actual selection not observed");
    for s in &samples {
        let normal = super::super::repair::reordering_grace(500, s.rtt);
        assert_eq!(
            s.grace,
            if version == 7 && !s.known {
                normal.min(20)
            } else {
                normal
            },
            "actual encrypted-client record policy {s:?}"
        );
        if s.allowed {
            assert!(s.at >= s.first + s.grace);
        } else if let Some(wake) = s.wake {
            assert!(wake <= s.first + s.grace);
        }
    }
    if cancel_at_selection {
        assert!(
            samples.iter().any(|sample| sample.allowed),
            "cancellation must follow allowed selection"
        );
        assert!(fired);
        assert!(outcome.unwrap().is_err());
        assert!(seen.is_empty());
        assert_eq!(replies, 0);
    } else {
        assert_eq!(outcome.unwrap().unwrap(), expected);
        let first_middle = samples.iter().find(|s| s.allowed && s.record == 1).unwrap();
        assert_eq!(first_middle.known, known, "initial middle-record geometry");
        assert!(!seen.is_empty());
        assert!(replies > 0);
    }
}
