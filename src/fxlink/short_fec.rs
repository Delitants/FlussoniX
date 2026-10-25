//! Bounded deferred parity. Queue-empty observation separates encrypted packets.
use super::{Budget, Reservation, Source, packet::Header, repair::Allowance, transport::now};
use bytes::Bytes;
use futures_util::FutureExt;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio_util::sync::CancellationToken;
const LIMIT: usize = 64;
const METADATA: usize = 8192;
pub(super) struct Slot {
    _guard: Reservation,
    length: usize,
}
struct Entry {
    packet: Bytes,
    created: u64,
    deadline: u64,
    _slot: Slot,
}
pub(super) struct Pending {
    entries: VecDeque<Entry>,
    _metadata: Reservation,
}
pub(super) struct Send<'a> {
    pub connection: &'a quinn::Connection,
    /// Captured before the peer starts any media writers, not raw config bytes.
    pub empty_space: usize,
    pub source: &'a Source,
    pub cancel: &'a CancellationToken,
    pub allowance: &'a mut Allowance,
    pub egress: &'a AtomicU64,
}
impl Pending {
    pub(super) fn new(budget: &Budget) -> Result<Self, String> {
        let metadata = budget.reserve(METADATA).map_err(|e| e.to_string())?;
        Ok(Self {
            entries: VecDeque::with_capacity(LIMIT),
            _metadata: metadata,
        })
    }
    pub(super) fn reserve(&self, length: usize, budget: &Budget) -> Option<Slot> {
        if self.entries.len() >= LIMIT {
            return None;
        }
        Some(Slot {
            _guard: budget.reserve(length.checked_add(128)?).ok()?,
            length,
        })
    }
    pub(super) fn push(
        &mut self,
        slot: Slot,
        packet: Bytes,
        created: u64,
        deadline: u64,
    ) -> Result<(), String> {
        let header = Header::parse(&packet).map_err(|e| e.to_string())?;
        if self.entries.len() >= LIMIT
            || packet.len() != slot.length
            || header.layout != super::Layout::Compact
            || header.shard_index != 1
        {
            return Err("FXLink pending compact parity bounds".into());
        }
        self.entries.push_back(Entry {
            packet,
            created,
            deadline,
            _slot: slot,
        });
        Ok(())
    }
    /// Release only unsent optional copies. This cannot renew age, charge
    /// egress/repair allowance, or discard a queued systematic shard.
    pub(super) fn discard(&mut self) -> u64 {
        let bytes = self
            .entries
            .iter()
            .map(|entry| entry.packet.len() as u64)
            .sum();
        self.entries.clear();
        bytes
    }
    fn prune(&mut self, at: u64) {
        self.entries.retain(|entry| at < entry.deadline);
    }
    pub(super) fn flush(&mut self, context: Send<'_>) -> Result<u64, String> {
        self.prune(now());
        // The same driver lock protects the accessor and packet construction.
        // Every original already queued has been finalized before this reads
        // empty, including an immutable transmit blocked at the UDP socket.
        if context.connection.datagram_send_buffer_space() != context.empty_space {
            return Ok(0);
        }
        let mut bytes = 0u64;
        // This synchronous prefix cannot add entries under a stale observation.
        while let Some(entry) = self.entries.front() {
            super::listener::check_source(context.source, context.cancel)?;
            if now() >= entry.deadline {
                self.entries.pop_front();
                continue;
            }
            match context
                .connection
                .send_datagram_wait(entry.packet.clone())
                .now_or_never()
            {
                Some(Ok(())) => {}
                Some(Err(_)) => return Err("FXLink datagram send failed".into()),
                None => break,
            }
            let entry = self.entries.pop_front().unwrap();
            let length = entry.packet.len() as u64;
            bytes = bytes.saturating_add(length);
            context.egress.fetch_add(length, Ordering::Relaxed);
            // Credit is granted only for admitted bytes. Clock crossing after
            // admission can conservatively omit credit, never renew the cohort.
            let _ = context
                .allowance
                .original(entry.created, now(), length as usize);
            if tracing::enabled!(tracing::Level::TRACE) {
                let header = Header::parse(&entry.packet).unwrap();
                tracing::trace!(
                    subscription = header.subscription,
                    record = header.record_id,
                    created_ms = entry.created,
                    queued_at_ms = now(),
                    deadline_ms = entry.deadline,
                    "FXLink compact parity queued after original packetization"
                );
            }
            super::listener::check_source(context.source, context.cancel)?;
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fxlink::{Encoder, Settings, control, transport};
    use std::sync::Arc;
    fn encoded(budget: &Budget) -> super::super::EncodedRecord {
        let mut bytes = vec![83; 350];
        bytes[..4].copy_from_slice(&346u32.to_be_bytes());
        Encoder::new(Settings::default(), 0, 1, budget.clone())
            .unwrap()
            .with_short_record_fec(true)
            .encode(Bytes::from(bytes), now())
            .unwrap()
    }
    fn source() -> Source {
        Source {
            pending_records: None,
            records: Box::pin(futures_util::stream::pending()),
            current: Arc::new(|| true),
            cancelled: CancellationToken::new(),
        }
    }
    async fn pair() -> (
        quinn::Endpoint,
        quinn::Endpoint,
        quinn::Connection,
        quinn::Connection,
    ) {
        pair_socket(None).await
    }
    // Break caught: FXLink is left on latest-callback rate sampling, or its
    // per-packet history hooks are missing despite enabling the new controller.
    #[tokio::test]
    async fn fxlink_delivery_model_is_live_and_bounds_a_paced_local_flight() {
        let (client, server, sender, receiver) = pair().await;
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let send = async {
                let mut tick = tokio::time::interval(std::time::Duration::from_millis(1));
                let mut max_window = 0;
                for sequence in 0..512u32 {
                    tick.tick().await;
                    let mut packet = vec![71; 1000];
                    packet[..4].copy_from_slice(&sequence.to_be_bytes());
                    sender
                        .send_datagram_wait(Bytes::from(packet))
                        .await
                        .map_err(|e| e.to_string())?;
                    max_window = max_window.max(sender.stats().path.cwnd);
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                Ok::<_, String>((max_window, sender.congestion_state().metrics().pacing_rate))
            };
            let receive = async {
                for sequence in 0..512u32 {
                    let packet = receiver.read_datagram().await.map_err(|e| e.to_string())?;
                    if packet.len() != 1000
                        || packet[..4] != sequence.to_be_bytes()
                        || packet[4..].iter().any(|b| *b != 71)
                    {
                        return Err("paced packet mismatch".to_string());
                    }
                }
                Ok::<_, String>(())
            };
            tokio::join!(send, receive)
        })
        .await;
        sender.close(0u32.into(), b"done");
        receiver.close(0u32.into(), b"done");
        client.wait_idle().await;
        server.wait_idle().await;
        let (send, receive) = outcome.expect("bounded paced flight");
        receive.unwrap();
        let (window, rate) = send.unwrap();
        assert!(
            rate.is_some_and(|rate| rate > 0),
            "delivery history was not sampled"
        );
        assert!(
            window <= 128 * 1024,
            "paced flight retained an unbounded Startup window: {window}"
        );
    }
    async fn read(connection: &quinn::Connection) -> Bytes {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            connection.read_datagram(),
        )
        .await
        .expect("bounded pending fixture receive")
        .unwrap()
    }
    async fn pair_socket(
        socket: Option<Arc<dyn quinn::AsyncUdpSocket>>,
    ) -> (
        quinn::Endpoint,
        quinn::Endpoint,
        quinn::Connection,
        quinn::Connection,
    ) {
        let cert = crate::transport_tls_fixture::Certificates::new();
        let mut tls = (*cert.server()).clone();
        tls.alpn_protocols = control::protocols();
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server_config.transport_config(transport::config());
        let server = if let Some(socket) = socket {
            quinn::Endpoint::new_with_abstract_socket(
                quinn::EndpointConfig::default(),
                Some(server_config),
                socket,
                Arc::new(quinn::TokioRuntime),
            )
            .unwrap()
        } else {
            quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap()
        };
        let mut tls = (*cert.client()).clone();
        tls.alpn_protocols = control::protocols();
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        config.transport_config(transport::config());
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(config);
        let (outbound, inbound) = tokio::join!(
            client
                .connect(server.local_addr().unwrap(), "localhost")
                .unwrap(),
            async { server.accept().await.unwrap().await }
        );
        (client, server, outbound.unwrap(), inbound.unwrap())
    }
    #[test]
    fn compact_pending_capacity_budget_expiry_and_drop_are_bounded() {
        assert!(std::mem::size_of::<Entry>() * LIMIT <= METADATA);
        let packet_store = Budget::new(8192);
        let frame = encoded(&packet_store);
        let packet = &frame.datagrams[1];
        let budget = Budget::new(1 << 20);
        let mut pending = Pending::new(&budget).unwrap();
        let failed = Budget::new(10);
        assert!(pending.reserve(packet.len(), &failed).is_none());
        assert_eq!(failed.used(), 0);
        for _ in 0..LIMIT {
            let slot = pending.reserve(packet.len(), &budget).unwrap();
            pending.push(slot, packet.clone(), 0, 1000).unwrap();
        }
        assert!(pending.reserve(packet.len(), &budget).is_none());
        assert_eq!(budget.used(), METADATA + LIMIT * (packet.len() + 128));
        pending.prune(999);
        assert_eq!(pending.entries.len(), LIMIT);
        pending.prune(1000);
        assert!(pending.entries.is_empty());
        assert_eq!(budget.used(), METADATA);
        let held = pending.reserve(packet.len(), &budget).unwrap();
        drop(pending);
        assert_eq!(budget.used(), packet.len() + 128);
        drop(held);
        assert_eq!(budget.used(), 0);
        drop(frame);
        assert_eq!(packet_store.used(), 0);
    }
    // Break caught: suppressed duplicates retain their reservations or replay
    // after originals have resumed, despite no application egress being charged.
    #[tokio::test]
    async fn suppressed_compact_parity_is_freed_and_cannot_replay() {
        let (client, server, sender, receiver) = pair().await;
        let empty = sender.datagram_send_buffer_space();
        let budget = Budget::new(1 << 20);
        let frame = encoded(&budget);
        let mut pending = Pending::new(&budget).unwrap();
        let baseline = budget.used();
        let slot = pending.reserve(frame.datagrams[1].len(), &budget).unwrap();
        pending
            .push(
                slot,
                frame.datagrams[1].clone(),
                frame.created_ms,
                frame.created_ms + 1000,
            )
            .unwrap();
        let discarded = pending.discard();
        let retained = pending.entries.len();
        let used = budget.used();
        let source = source();
        let stop = CancellationToken::new();
        let egress = AtomicU64::new(0);
        let mut allowance = Allowance::new(frame.created_ms, 1000);
        sender
            .send_datagram_wait(Bytes::from_static(b"fresh systematic original"))
            .await
            .unwrap();
        let original = read(&receiver).await;
        let flushed = pending
            .flush(Send {
                connection: &sender,
                empty_space: empty,
                source: &source,
                cancel: &stop,
                allowance: &mut allowance,
                egress: &egress,
            })
            .unwrap();
        let replay = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            receiver.read_datagram(),
        )
        .await;
        let charged = egress.load(Ordering::Relaxed);
        drop(pending);
        drop(frame);
        let after = budget.used();
        sender.close(0u32.into(), b"done");
        receiver.close(0u32.into(), b"done");
        client.wait_idle().await;
        server.wait_idle().await;
        assert_eq!(discarded, 434);
        assert_eq!(retained, 0);
        assert_eq!(used, baseline);
        assert_eq!(original, b"fresh systematic original".as_slice());
        assert_eq!(flushed, 0);
        assert!(replay.is_err(), "suppressed parity reappeared on the wire");
        assert_eq!(charged, 0);
        assert_eq!(after, 0);
    }
    #[tokio::test]
    async fn compact_pending_never_joins_or_evicts_unpacketized_originals() {
        original_barrier_case(false).await;
    }
    #[tokio::test]
    async fn v6_trimmed_original_preserves_shared_compact_copy_barrier() {
        original_barrier_case(true).await;
    }
    async fn original_barrier_case(mixed: bool) {
        let (client, server, sender, receiver) = pair().await;
        let empty = sender.datagram_send_buffer_space();
        let budget = Budget::new(1 << 20);
        let frame = encoded(&budget);
        let mut pending = Pending::new(&budget).unwrap();
        let slot = pending.reserve(frame.datagrams[1].len(), &budget).unwrap();
        pending
            .push(
                slot,
                frame.datagrams[1].clone(),
                frame.created_ms,
                frame.created_ms + 1000,
            )
            .unwrap();
        let mut originals = vec![frame.datagrams[0].clone()];
        assert!(matches!(
            sender
                .send_datagram_wait(originals[0].clone())
                .now_or_never(),
            Some(Ok(()))
        ));
        let trimmed = if mixed {
            let mut body = vec![17; 401];
            body[..4].copy_from_slice(&397u32.to_be_bytes());
            let record = Encoder::new(Settings::default(), 99, 1, budget.clone())
                .unwrap()
                .with_capabilities(crate::fxlink::Capabilities::V6)
                .encode(Bytes::from(body), now())
                .unwrap();
            assert_eq!(record.datagrams[0][5], 2);
            assert!(matches!(
                sender
                    .send_datagram_wait(record.datagrams[0].clone())
                    .now_or_never(),
                Some(Ok(()))
            ));
            originals.push(record.datagrams[0].clone());
            Some(record)
        } else {
            None
        };
        for sequence in 0u8..64 {
            let packet = Bytes::from(vec![sequence; 1000]);
            match sender.send_datagram_wait(packet.clone()).now_or_never() {
                Some(Ok(())) => originals.push(packet),
                None => break,
                Some(Err(error)) => panic!("{error}"),
            }
        }
        assert!(originals.len() > 1 && originals.len() < 65);
        let free = sender.datagram_send_buffer_space();
        assert!(free < empty);
        let source = source();
        let stop = CancellationToken::new();
        let egress = AtomicU64::new(0);
        let mut allowance = Allowance::new(frame.created_ms, 1000);
        assert!(allowance.original(frame.created_ms, now(), frame.datagrams[0].len()));
        assert_eq!(
            pending
                .flush(Send {
                    connection: &sender,
                    empty_space: empty,
                    source: &source,
                    cancel: &stop,
                    allowance: &mut allowance,
                    egress: &egress
                })
                .unwrap(),
            0
        );
        assert_eq!(sender.datagram_send_buffer_space(), free);
        assert_eq!(pending.entries.len(), 1);
        assert_eq!(egress.load(Ordering::Relaxed), 0);
        for original in &originals {
            assert_eq!(read(&receiver).await, original);
        }
        assert_eq!(sender.datagram_send_buffer_space(), empty);
        assert_eq!(
            pending
                .flush(Send {
                    connection: &sender,
                    empty_space: empty,
                    source: &source,
                    cancel: &stop,
                    allowance: &mut allowance,
                    egress: &egress
                })
                .unwrap(),
            434
        );
        assert_eq!(read(&receiver).await, frame.datagrams[1]);
        assert_eq!(egress.load(Ordering::Relaxed), 434);
        assert!(pending.entries.is_empty());
        assert_eq!(allowance.counters(frame.created_ms, now()), (868, 0));
        drop(pending);
        drop(frame);
        drop(trimmed);
        assert_eq!(budget.used(), 0);
        sender.close(0u32.into(), b"done");
        receiver.close(0u32.into(), b"done");
        client.wait_idle().await;
        server.wait_idle().await;
    }
    #[tokio::test]
    async fn compact_pending_expiry_and_cancellation_cannot_emit_old_parity() {
        let (client, server, sender, receiver) = pair().await;
        let empty = sender.datagram_send_buffer_space();
        let budget = Budget::new(1 << 20);
        let frame = encoded(&budget);
        let mut pending = Pending::new(&budget).unwrap();
        let slot = pending.reserve(434, &budget).unwrap();
        pending
            .push(slot, frame.datagrams[1].clone(), frame.created_ms, now())
            .unwrap();
        let source = source();
        let stop = CancellationToken::new();
        let egress = AtomicU64::new(0);
        let mut allowance = Allowance::new(frame.created_ms, 1000);
        assert_eq!(
            pending
                .flush(Send {
                    connection: &sender,
                    empty_space: empty,
                    source: &source,
                    cancel: &stop,
                    allowance: &mut allowance,
                    egress: &egress
                })
                .unwrap(),
            0
        );
        assert!(pending.entries.is_empty());
        assert_eq!(egress.load(Ordering::Relaxed), 0);
        assert_eq!(allowance.counters(frame.created_ms, now()), (0, 0));
        sender
            .send_datagram_wait(Bytes::from_static(b"later healthy original"))
            .await
            .unwrap();
        assert_eq!(read(&receiver).await, b"later healthy original".as_slice());
        let slot = pending.reserve(434, &budget).unwrap();
        pending
            .push(
                slot,
                frame.datagrams[1].clone(),
                frame.created_ms,
                frame.created_ms + 1000,
            )
            .unwrap();
        source.cancelled.cancel();
        assert!(
            pending
                .flush(Send {
                    connection: &sender,
                    empty_space: empty,
                    source: &source,
                    cancel: &stop,
                    allowance: &mut allowance,
                    egress: &egress
                })
                .is_err()
        );
        assert_eq!(egress.load(Ordering::Relaxed), 0);
        drop(pending);
        drop(frame);
        assert_eq!(budget.used(), 0);
        sender.close(0u32.into(), b"done");
        receiver.close(0u32.into(), b"done");
        client.wait_idle().await;
        server.wait_idle().await;
    }
    use std::sync::{Mutex, atomic::AtomicBool};
    use std::{
        io::{self, IoSliceMut},
        pin::Pin,
        task::{Context, Poll},
    };
    #[derive(Debug, Default)]
    struct SocketGate {
        held: AtomicBool,
        blocked: Mutex<Option<Vec<u8>>>,
        first_after: Mutex<Option<Vec<u8>>>,
        wake: futures_util::task::AtomicWaker,
    }
    #[derive(Debug)]
    struct HeldSocket {
        inner: Arc<dyn quinn::AsyncUdpSocket>,
        gate: Arc<SocketGate>,
    }
    #[derive(Debug)]
    struct HeldPoller {
        inner: Pin<Box<dyn quinn::UdpPoller>>,
        gate: Arc<SocketGate>,
    }
    impl quinn::UdpPoller for HeldPoller {
        fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.gate.held.load(Ordering::Acquire) && self.gate.blocked.lock().unwrap().is_some()
            {
                self.gate.wake.register(cx.waker());
                if self.gate.held.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
            }
            self.inner.as_mut().poll_writable(cx)
        }
    }
    impl quinn::AsyncUdpSocket for HeldSocket {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
            Box::pin(HeldPoller {
                inner: self.inner.clone().create_io_poller(),
                gate: self.gate.clone(),
            })
        }
        fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
            if self.gate.held.load(Ordering::Acquire) {
                let mut blocked = self.gate.blocked.lock().unwrap();
                if blocked.is_none() {
                    assert!(transmit.contents.len() <= 65536);
                    *blocked = Some(transmit.contents.to_vec());
                }
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let result = self.inner.try_send(transmit);
            if result.is_ok() && self.gate.blocked.lock().unwrap().is_some() {
                let mut first = self.gate.first_after.lock().unwrap();
                if first.is_none() {
                    *first = Some(transmit.contents.to_vec());
                }
            }
            result
        }
        fn poll_recv(
            &self,
            cx: &mut Context<'_>,
            buffers: &mut [IoSliceMut<'_>],
            meta: &mut [quinn::udp::RecvMeta],
        ) -> Poll<io::Result<usize>> {
            self.inner.poll_recv(cx, buffers, meta)
        }
        fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
            self.inner.local_addr()
        }
        fn max_receive_segments(&self) -> usize {
            self.inner.max_receive_segments()
        }
        fn may_fragment(&self) -> bool {
            self.inner.may_fragment()
        }
    }
    #[tokio::test]
    async fn compact_barrier_covers_a_finalized_transmit_blocked_at_the_socket() {
        use quinn::Runtime;
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        udp.set_nonblocking(true).unwrap();
        let gate = Arc::new(SocketGate::default());
        let socket = Arc::new(HeldSocket {
            inner: quinn::TokioRuntime.wrap_udp_socket(udp).unwrap(),
            gate: gate.clone(),
        });
        let (client, server, receiver, sender) = pair_socket(Some(socket)).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let empty = sender.datagram_send_buffer_space();
        let budget = Budget::new(1 << 20);
        let frame = encoded(&budget);
        let mut pending = Pending::new(&budget).unwrap();
        let slot = pending.reserve(434, &budget).unwrap();
        pending
            .push(
                slot,
                frame.datagrams[1].clone(),
                frame.created_ms,
                frame.created_ms + 1000,
            )
            .unwrap();
        gate.held.store(true, Ordering::Release);
        sender
            .send_datagram_wait(frame.datagrams[0].clone())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_millis(500), async {
            while gate.blocked.lock().unwrap().is_none()
                || sender.datagram_send_buffer_space() != empty
            {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("original must be finalized but socket-blocked");
        let original = gate.blocked.lock().unwrap().clone().unwrap();
        assert!(original.len() > 434);
        let source = source();
        let stop = CancellationToken::new();
        let egress = AtomicU64::new(0);
        let mut allowance = Allowance::new(frame.created_ms, 1000);
        assert_eq!(
            pending
                .flush(Send {
                    connection: &sender,
                    empty_space: empty,
                    source: &source,
                    cancel: &stop,
                    allowance: &mut allowance,
                    egress: &egress
                })
                .unwrap(),
            434
        );
        assert!(
            sender.datagram_send_buffer_space() < empty,
            "parity is queued behind finalized original"
        );
        gate.held.store(false, Ordering::Release);
        gate.wake.wake();
        assert_eq!(read(&receiver).await, frame.datagrams[0]);
        assert_eq!(read(&receiver).await, frame.datagrams[1]);
        assert_eq!(
            gate.first_after.lock().unwrap().as_ref().unwrap(),
            &original,
            "queued parity mutated the finalized UDP packet"
        );
        drop(pending);
        drop(frame);
        assert_eq!(budget.used(), 0);
        sender.close(0u32.into(), b"done");
        receiver.close(0u32.into(), b"done");
        client.wait_idle().await;
        server.wait_idle().await;
    }
}
