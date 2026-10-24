//! Real encrypted source-loop checks with a test-only ALPN8 endpoint.
//! Production advertisement is separately opt-in.
use super::*;
use crate::fxlink::{
    RepairRequest,
    feedback::{self, DatagramRequest},
    transport,
};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::sync::{Mutex, atomic::AtomicBool};
use tokio::sync::mpsc;

fn record(len: usize) -> Bytes {
    let mut bytes = vec![17; len];
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
struct Feed {
    live: Mutex<Option<mpsc::Receiver<Bytes>>>,
    valid: Arc<AtomicBool>,
}
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            let live = self.live.lock().unwrap().take().ok_or(409u16)?;
            let valid = self.valid.clone();
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::iter(
                        [record(9781), record(20000), record(20000), record(20000)]
                            .into_iter()
                            .map(Ok),
                    )
                    .chain(tokio_stream::wrappers::ReceiverStream::new(live).map(Ok)),
                ),
                current: Arc::new(move || valid.load(Ordering::Acquire)),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
struct Peer {
    version: u8,
    endpoint: quinn::Endpoint,
    listener: quinn::Endpoint,
    connection: quinn::Connection,
    serving: Option<tokio::task::JoinHandle<()>>,
    stop: CancellationToken,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    settings: Settings,
    next: u64,
    originals: Vec<Bytes>,
    input: mpsc::Sender<Bytes>,
    valid: Arc<AtomicBool>,
    egress: Arc<AtomicU64>,
}
impl Peer {
    async fn start() -> Self {
        Self::start_for_version(8).await
    }
    async fn start_for_version(version: u8) -> Self {
        let cert = crate::transport_tls_fixture::Certificates::new();
        let settings = Settings {
            latency_ms: 500,
            max_records: 4,
            ..Settings::default()
        };
        let mut tls = (*cert.server()).clone();
        tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server.transport_config(transport::config());
        let listener = quinn::Endpoint::server(server, "127.0.0.1:0".parse().unwrap()).unwrap();
        let address = listener.local_addr().unwrap();
        let listening = listener.clone();
        let (input, live) = mpsc::channel(8);
        let valid = Arc::new(AtomicBool::new(true));
        let provider = Arc::new(Feed {
            live: Mutex::new(Some(live)),
            valid: valid.clone(),
        });
        let egress = Arc::new(AtomicU64::new(0));
        let server_egress = egress.clone();
        let stop = CancellationToken::new();
        let halted = stop.clone();
        let source_settings = settings.clone();
        let serving = tokio::spawn(async move {
            let connection = tokio::select! {_=halted.cancelled()=>return,c=listening.accept()=>c.unwrap().await.unwrap()};
            let budget = transport::peer_budget(connection.remote_address().ip()).unwrap();
            let _queues = budget.reserve(transport::QUEUE_BYTES).unwrap();
            serve_peer(
                connection,
                provider,
                source_settings,
                budget,
                halted,
                server_egress,
            )
            .await;
        });
        let mut tls = (*cert.client()).clone();
        tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
        let mut config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
        ));
        config.transport_config(transport::config());
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let connection = endpoint
            .connect(address, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        let id = send.id().index();
        control::write(
            &mut send,
            &Control::Subscribe {
                version,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "owned-bootstrap".into(),
                peer_key: "owned-peer".into(),
                token: "owned-viewer".into(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            control::read(&mut recv).await.unwrap(),
            Control::Accepted
        ));
        let mut peer = Self {
            version,
            endpoint,
            listener,
            connection,
            serving: Some(serving),
            stop,
            send,
            recv,
            settings,
            next: 1,
            originals: Vec::new(),
            input,
            valid,
            egress,
        };
        peer.originals = peer.responses(84).await;
        assert_eq!(peer.originals.len(), 84);
        peer
    }
    fn request(&mut self, requests: &[DatagramRequest]) {
        for frame in feedback::encode_bootstrap(
            self.send.id().index(),
            requests,
            &self.settings,
            &mut self.next,
        )
        .unwrap()
        {
            self.connection.send_datagram(frame).unwrap();
        }
    }
    async fn responses(&self, count: usize) -> Vec<Bytes> {
        let mut output = Vec::new();
        let _ = tokio::time::timeout(Duration::from_millis(350), async {
            while output.len() < count {
                let packet = self.connection.read_datagram().await.unwrap();
                if self.version == 9 && packet.starts_with(b"FXGH") {
                    let frame = crate::fxlink::geometry::wire::decode(&packet, &self.settings)
                        .expect("valid negotiated geometry hint");
                    assert_eq!(
                        (frame.subscription, frame.generation),
                        (self.send.id().index(), 1)
                    );
                    continue;
                }
                Header::parse(&packet).expect("valid media datagram");
                output.push(packet);
            }
        })
        .await;
        output
    }
    fn original(&self, record: u64, index: u16) -> Bytes {
        self.originals
            .iter()
            .find(|packet| {
                let h = Header::parse(packet).unwrap();
                h.record_id == record && h.shard_index == index
            })
            .unwrap()
            .clone()
    }
    async fn finish(mut self) {
        self.stop.cancel();
        self.connection.close(0u32.into(), b"owned test finished");
        self.listener.close(0u32.into(), b"owned test finished");
        self.endpoint.close(0u32.into(), b"owned test finished");
        if let Some(task) = self.serving.take() {
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
        }
        self.endpoint.wait_idle().await;
        self.listener.wait_idle().await;
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.cancel();
        self.connection.close(0u32.into(), b"owned test dropped");
        self.endpoint.close(0u32.into(), b"owned test dropped");
        self.listener.close(0u32.into(), b"owned test dropped");
        if let Some(task) = self.serving.take() {
            task.abort();
        }
    }
}
fn ordinary(record: u64) -> DatagramRequest {
    DatagramRequest::Ordinary(RepairRequest {
        block_id: record * 2048,
        shard_indices: vec![0],
    })
}
#[tokio::test]
async fn bootstrap_source_cached_replies_are_bounded_and_ordinary_zero_stays_exact() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let mut peer = Peer::start().await;
    peer.request(&[DatagramRequest::Bootstrap {
        block_id: 0,
        reply_limit: 20,
    }]);
    let replies = peer.responses(11).await;
    if replies.len() != 11 {
        peer.finish().await;
        assert_eq!(replies.len(), 11, "missing bounded bootstrap reply");
        return;
    }
    for (index, packet) in replies.iter().enumerate() {
        assert_eq!(packet, &peer.original(0, index as u16));
    }
    peer.request(&[ordinary(0)]);
    let ordinary_reply = peer.responses(1).await;
    assert_eq!(ordinary_reply, [peer.original(0, 0)]);
    tokio::time::sleep(Duration::from_millis(21)).await;
    peer.request(&[DatagramRequest::Bootstrap {
        block_id: 2048,
        reply_limit: 2,
    }]);
    assert_eq!(
        peer.responses(2).await,
        [peer.original(1, 0), peer.original(1, 1)]
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(10), peer.connection.read_datagram())
            .await
            .is_err()
    );
    assert_eq!(
        peer.egress.load(Ordering::Relaxed),
        (84 + 11 + 1 + 2) * 1088
    );
    peer.finish().await;
    assert_eq!(transport::global().used(), baseline);
}
#[tokio::test]
async fn bootstrap_source_fences_consumed_records_and_cache_misses() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let mut peer = Peer::start().await;
    peer.input.send(record(20000)).await.unwrap();
    control::write(&mut peer.send, &Control::Progress { next: 1 })
        .await
        .unwrap();
    let fifth = peer.responses(24).await;
    assert_eq!(fifth.len(), 24);
    assert!(
        fifth
            .iter()
            .all(|packet| Header::parse(packet).unwrap().record_id == 4)
    );
    peer.request(&[
        DatagramRequest::Bootstrap {
            block_id: 0,
            reply_limit: 2,
        },
        ordinary(1),
    ]);
    assert_eq!(peer.responses(1).await, [peer.original(1, 0)]);
    tokio::time::sleep(Duration::from_millis(21)).await;
    peer.request(&[
        DatagramRequest::Bootstrap {
            block_id: 40 * 2048,
            reply_limit: 2,
        },
        ordinary(2),
    ]);
    assert_eq!(peer.responses(1).await, [peer.original(2, 0)]);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), peer.connection.read_datagram())
            .await
            .is_err()
    );
    assert_eq!(peer.egress.load(Ordering::Relaxed), (84 + 24 + 2) * 1088);
    peer.finish().await;
    assert_eq!(transport::global().used(), baseline);
}
#[tokio::test]
async fn bootstrap_source_fences_blocked_enqueue_at_original_expiry() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let mut peer = Peer::start().await;
    struct ResetDelay;
    impl Drop for ResetDelay {
        fn drop(&mut self) {
            tests::REPAIR_QUEUE_DELAY.store(0, Ordering::Release);
        }
    }
    let _reset = ResetDelay;
    tests::REPAIR_QUEUE_DELAY.store(520, Ordering::Release);
    peer.request(&[DatagramRequest::Bootstrap {
        block_id: 0,
        reply_limit: 2,
    }]);
    tokio::time::timeout(Duration::from_secs(1), async {
        while tests::REPAIR_QUEUE_DELAY.load(Ordering::Acquire) != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("repair must enter the held enqueue before Progress");
    peer.input.send(record(20000)).await.unwrap();
    control::write(&mut peer.send, &Control::Progress { next: 4 })
        .await
        .unwrap();
    let fresh = tokio::time::timeout(Duration::from_secs(2), peer.connection.read_datagram())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        Header::parse(&fresh).unwrap().record_id,
        4,
        "expired old media entered the source queue"
    );
    // Progress must not overtake the held repair and make this test vacuous.
    assert_eq!(
        tests::REPAIR_QUEUE_DELAY.load(Ordering::Acquire),
        0,
        "repair never reached the blocked enqueue"
    );
    tests::REPAIR_QUEUE_DELAY.store(0, Ordering::Release);
    peer.finish().await;
    assert_eq!(transport::global().used(), baseline);
}
#[tokio::test]
async fn bootstrap_source_fences_retirement_before_optional_send() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let mut peer = Peer::start().await;
    let initial = peer.egress.load(Ordering::Relaxed);
    peer.valid.store(false, Ordering::Release);
    peer.request(&[DatagramRequest::Bootstrap {
        block_id: 0,
        reply_limit: 2,
    }]);
    tokio::time::timeout(Duration::from_secs(2), async {
        while control::read(&mut peer.recv).await.is_ok() {}
    })
    .await
    .unwrap();
    assert_eq!(peer.egress.load(Ordering::Relaxed), initial);
    peer.finish().await;
    assert_eq!(transport::global().used(), baseline);
}

#[tokio::test]
async fn bootstrap_source_invalid_feedback_does_not_claim_sequence_or_spend_credit() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let peer = Peer::start().await;
    let id = peer.send.id().index();
    let mut sequence = 100;
    let valid = feedback::encode_bootstrap(id, &[ordinary(0)], &peer.settings, &mut sequence)
        .unwrap()
        .remove(0);
    for offset in [8usize, 16] {
        let mut wrong = valid.to_vec();
        wrong[offset + 7] ^= 1;
        peer.connection.send_datagram(Bytes::from(wrong)).unwrap();
    }
    let mut wrong = valid.to_vec();
    wrong[4] = 3;
    peer.connection.send_datagram(Bytes::from(wrong)).unwrap();
    let mut invalid = valid.to_vec();
    invalid.extend_from_slice(&[0; 12]);
    peer.connection.send_datagram(Bytes::from(invalid)).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), peer.connection.read_datagram())
            .await
            .is_err()
    );
    assert_eq!(peer.egress.load(Ordering::Acquire), 84 * 1088);
    peer.connection.send_datagram(valid.clone()).unwrap();
    assert_eq!(peer.responses(1).await, [peer.original(0, 0)]);
    peer.connection.send_datagram(valid).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), peer.connection.read_datagram())
            .await
            .is_err()
    );
    assert_eq!(peer.egress.load(Ordering::Acquire), 85 * 1088);
    for (version, generation, offset, token) in [
        (8, 1, 0, "bad-viewer"),
        (8, 2, 0, "owned-viewer"),
        (7, 1, 0, "owned-viewer"),
        (8, 1, 1, "owned-viewer"),
    ] {
        let (mut send, mut recv) = peer.connection.open_bi().await.unwrap();
        let id = send.id().index() + offset;
        control::write(
            &mut send,
            &Control::Subscribe {
                version,
                id,
                generation,
                profile: control::profile(&peer.settings),
                name: "owned-denied".into(),
                peer_key: "owned-peer".into(),
                token: token.into(),
            },
        )
        .await
        .unwrap();
        let denied = tokio::time::timeout(Duration::from_millis(100), control::read(&mut recv))
            .await
            .unwrap();
        assert!(denied.is_err() || matches!(denied, Ok(Control::Denied { .. })));
        assert!(peer.connection.close_reason().is_none());
    }
    assert_eq!(peer.egress.load(Ordering::Acquire), 85 * 1088);
    peer.finish().await;
    assert_eq!(transport::global().used(), baseline);
}

#[tokio::test]
async fn ordinary_empty_reliable_has_zero_packet_work_and_keeps_valid_repair() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let mut peer = Peer::start().await;
    let observed = super::ordinary_work_probe::arm();
    let expected = peer.original(0, 0);
    control::write(
        &mut peer.send,
        &Control::Repair {
            block: 0,
            shards: vec![],
        },
    )
    .await
    .unwrap();
    super::ordinary_work_probe::finished(1).await;
    let empty_work = super::ordinary_work_probe::inspected();
    control::write(
        &mut peer.send,
        &Control::Repair {
            block: 0,
            shards: vec![0],
        },
    )
    .await
    .unwrap();
    let reply = peer.responses(1).await;
    super::ordinary_work_probe::finished(2).await;
    let valid_work = super::ordinary_work_probe::inspected() - empty_work;
    drop(observed);
    peer.finish().await;
    assert_eq!(transport::global().used(), baseline);
    assert_eq!(
        reply,
        [expected],
        "legitimate reliable repair remains byte-exact"
    );
    println!(
        "empty request packet inspections={empty_work}; valid repair inspections={valid_work}"
    );
    assert_eq!(
        empty_work, 0,
        "ordinary empty request must not inspect cached packets"
    );
}

async fn large_ordinary_peer(version: u8) -> (Peer, Vec<Bytes>) {
    let mut peer = Peer::start_for_version(version).await;
    if version >= 2 {
        control::write(&mut peer.send, &Control::Progress { next: 4 })
            .await
            .unwrap();
    }
    peer.input.send(record(250000)).await.unwrap();
    // ceil(250000/1004)=250 fragments:12 full24-packet blocks +12 terminal packets.
    let originals = peer.responses(300).await;
    assert_eq!(originals.len(), 300);
    assert!(
        originals
            .iter()
            .all(|p| Header::parse(p).unwrap().record_id == 4)
    );
    (peer, originals)
}
fn original_block(packets: &[Bytes], block: u64, shard: u16) -> Bytes {
    packets
        .iter()
        .find(|packet| {
            let h = Header::parse(packet).unwrap();
            h.block_id == block && h.shard_index == shard
        })
        .unwrap()
        .clone()
}
#[tokio::test]
async fn ordinary_reliable_fallback_has_bounded_work_and_exact_order_in_versions_one_through_nine()
{
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    for version in 1..=9 {
        let (mut peer, originals) = large_ordinary_peer(version).await;
        let observed = super::ordinary_work_probe::arm();
        let base = 4 * 2048;
        let middle = base + 3;
        let terminal = base + 12;
        for (count, block, shards) in [
            (1, base, vec![]),
            (2, base + 100, vec![0]),
            (3, terminal, vec![20, 65535]),
        ] {
            control::write(&mut peer.send, &Control::Repair { block, shards })
                .await
                .unwrap();
            super::ordinary_work_probe::finished(count).await;
            assert_eq!(super::ordinary_work_probe::inspected(), 0);
        }
        control::write(
            &mut peer.send,
            &Control::Repair {
                block: middle,
                shards: vec![3, 0, 3, 65535],
            },
        )
        .await
        .unwrap();
        let replies = peer.responses(2).await;
        super::ordinary_work_probe::finished(4).await;
        let work = super::ordinary_work_probe::inspected();
        let before = peer.egress.load(Ordering::Relaxed);
        control::write(
            &mut peer.send,
            &Control::Repair {
                block: middle,
                shards: vec![20, 21],
            },
        )
        .await
        .unwrap();
        super::ordinary_work_probe::finished(5).await;
        let parity_egress = peer.egress.load(Ordering::Relaxed);
        // The raw limit must precede deduplication even for identical indices.
        control::write(
            &mut peer.send,
            &Control::Repair {
                block: middle,
                shards: vec![0; peer.settings.max_repair_shards + 1],
            },
        )
        .await
        .unwrap();
        super::ordinary_work_probe::finished(6).await;
        let reset = tokio::time::timeout(Duration::from_secs(1), async {
            while control::read(&mut peer.recv).await.is_ok() {}
        })
        .await
        .is_ok();
        drop(observed);
        peer.finish().await;
        assert_eq!(
            work, 24,
            "version{version} scanned beyond one block in300-packet record"
        );
        assert_eq!(
            replies,
            [
                original_block(&originals, middle, 0),
                original_block(&originals, middle, 3)
            ]
        );
        assert_eq!(
            parity_egress, before,
            "reliable fallback must remain systematic-only in every version"
        );
        assert!(
            reset,
            "over-limit duplicate request must still reset its stream"
        );
        assert_eq!(transport::global().used(), baseline);
        println!(
            "version{version}:300 cached packets,24 ordinary inspections,0 empty/missing/out-of-width inspections"
        );
    }
}
#[tokio::test]
async fn ordinary_datagram_exhausted_credit_still_has_only_block_local_work() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let (mut peer, originals) = large_ordinary_peer(8).await;
    let observed = super::ordinary_work_probe::arm();
    let block = 4 * 2048 + 3;
    let original_egress = peer.egress.load(Ordering::Relaxed);
    let expected = (0..20)
        .map(|i| original_block(&originals, block, i))
        .collect::<Vec<_>>();
    //84 initial +300 new fixed original datagrams (including parity) fund exactly96 repairs.
    for count in 1..=4 {
        if count > 1 {
            tokio::time::sleep(Duration::from_millis(21)).await;
        }
        peer.request(&[DatagramRequest::Ordinary(RepairRequest {
            block_id: block,
            shard_indices: (0..20).collect(),
        })]);
        assert_eq!(peer.responses(20).await, expected);
        super::ordinary_work_probe::finished(count).await;
    }
    tokio::time::sleep(Duration::from_millis(21)).await;
    peer.request(&[DatagramRequest::Ordinary(RepairRequest {
        block_id: block,
        shard_indices: (0..16).collect(),
    })]);
    assert_eq!(peer.responses(16).await, expected[..16]);
    super::ordinary_work_probe::finished(5).await;
    let funded = peer.egress.load(Ordering::Relaxed);
    assert_eq!(funded - original_egress, 96 * 1088);
    let before = super::ordinary_work_probe::inspected();
    tokio::time::sleep(Duration::from_millis(21)).await;
    peer.request(&[DatagramRequest::Ordinary(RepairRequest {
        block_id: block,
        shard_indices: vec![0],
    })]);
    super::ordinary_work_probe::finished(6).await;
    let work = super::ordinary_work_probe::inspected() - before;
    let after = peer.egress.load(Ordering::Relaxed);
    drop(observed);
    peer.finish().await;
    assert_eq!(
        work, 24,
        "exhausted allowance must not cause a record-wide scan"
    );
    assert_eq!(
        after, funded,
        "exhausted cohort must not gain or spend more repair credit"
    );
    assert_eq!(transport::global().used(), baseline);
    println!("exhausted ordinary datagram:24 inspections in300-packet record,0 output bytes");
}
