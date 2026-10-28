//! Shared bounded ownership, queue accounting and TLS configuration.
use super::{Budget, Error, Reservation, Stats, budget::WeakBudget};
use bytes::Bytes;
use futures_util::{Stream, future::BoxFuture};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Bytes, String>> + Send>>;
pub struct Request {
    pub name: String,
    pub token: String,
    pub peer_key: String,
}
pub struct Source {
    pub records: RecordStream,
    /// Nonblocking native validity/unread snapshot: callback None means retired;
    /// Some(false) active/idle, Some(true) known unread originals. An absent
    /// callback means unknown. Must not retain payloads or call into QUIC.
    pub pending_records: Option<Arc<dyn Fn() -> Option<bool> + Send + Sync>>,
    pub current: Arc<dyn Fn() -> bool + Send + Sync>,
    pub cancelled: CancellationToken,
}
pub trait Provider: Send + Sync {
    fn subscribe(&self, request: Request, peer: SocketAddr) -> BoxFuture<'_, Result<Source, u16>>;
}
pub(crate) const SUB_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const PEER_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const QUEUE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const SETUP: Duration = Duration::from_secs(5);
// Native providers can spend eight seconds preparing a cold source. Keep
// this admission wait separate from framing/TLS and from any media lifetime.
pub(crate) const ADMISSION: Duration = Duration::from_secs(10);
pub(crate) fn global() -> Budget {
    static B: OnceLock<Budget> = OnceLock::new();
    B.get_or_init(|| Budget::new(512 * 1024 * 1024)).clone()
}
// Independent of connection pooling/authentication identity. Weak counters retain
// no peer state; both live and expired entries also have a fixed registry ceiling.
const MAX_PEER_BUDGETS: usize = 1024;
#[derive(Default)]
struct PeerBudgets {
    entries: HashMap<IpAddr, WeakBudget>,
}
impl PeerBudgets {
    fn get(&mut self, ip: IpAddr) -> Result<Budget, Error> {
        let ip = match ip {
            IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
            ip => ip,
        };
        self.entries.retain(|_, weak| weak.upgrade().is_some());
        if let Some(budget) = self.entries.get(&ip).and_then(WeakBudget::upgrade) {
            return Ok(budget);
        }
        self.entries.remove(&ip);
        if self.entries.len() >= MAX_PEER_BUDGETS {
            return Err(Error::BudgetExceeded);
        }
        let budget = global().child(PEER_BYTES);
        self.entries.insert(ip, budget.downgrade());
        Ok(budget)
    }
}
pub(crate) fn peer_budget(ip: IpAddr) -> Result<Budget, Error> {
    static PEERS: OnceLock<Mutex<PeerBudgets>> = OnceLock::new();
    PEERS
        .get_or_init(|| Mutex::new(PeerBudgets::default()))
        .lock()
        .unwrap()
        .get(ip)
}
pub(crate) fn fec_slots() -> Arc<Semaphore> {
    static S: OnceLock<Arc<Semaphore>> = OnceLock::new();
    S.get_or_init(|| Arc::new(Semaphore::new(4))).clone()
}
pub(crate) fn now() -> u64 {
    static E: OnceLock<Instant> = OnceLock::new();
    E.get_or_init(Instant::now).elapsed().as_millis() as u64
}
pub(crate) fn config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    // Use a bandwidth/RTT model for the opt-in FEC media path. Keep the
    // previous initial flight size instead of BBR's much larger default.
    let mut congestion = quinn::congestion::BbrConfig::default();
    congestion
        .initial_window(12_000)
        .delivery_rate_sampling(true);
    // Reordered FEC datagrams can overtake many packet numbers without loss.
    // Use Quinn's unchanged RTT-based loss timer and PTO instead of immediate
    // packet-gap declarations; genuine loss still enters congestion recovery.
    t.packet_threshold(u32::MAX);
    t.congestion_controller_factory(Arc::new(congestion))
        .max_concurrent_bidi_streams(16u32.into())
        .max_concurrent_uni_streams(0u32.into())
        .stream_receive_window(32768u32.into())
        .receive_window((512 * 1024u32).into())
        .send_window(512 * 1024)
        .datagram_receive_buffer_size(Some(512 * 1024))
        // Bound untransmitted originals close to one initial congestion flight.
        // send_datagram_wait then backpressures before later records acquire
        // deadlines behind a whole cached GOP in the QUIC FIFO.
        .datagram_send_buffer_size(16 * 1024)
        .max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()))
        .keep_alive_interval(Some(Duration::from_secs(2)));
    Arc::new(t)
}
pub(crate) struct Charged {
    pub bytes: Bytes,
    pub deadline_ms: u64,
    pub _guard: Reservation,
}
impl Charged {
    pub fn new(bytes: Bytes, budget: &Budget) -> Result<Self, String> {
        let guard = budget
            .reserve(bytes.len() + 128)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            bytes,
            deadline_ms: u64::MAX,
            _guard: guard,
        })
    }
}
#[derive(Default)]
struct Ingress {
    published: u64,
    loss: Option<(u64, u64)>,
}
pub(crate) struct State {
    #[cfg(test)]
    pub routed_packets: AtomicU64,
    pub consumed: AtomicU64,
    pub admitted: AtomicBool,
    pub geometry: Mutex<Option<Arc<super::geometry::receive::Ingress>>>,
    ingress: Mutex<Ingress>,
    pub live: AtomicBool,
    pub cancel: CancellationToken,
    pub telemetry: Arc<Telemetry>,
    pub queue: Mutex<std::collections::VecDeque<Charged>>,
    pub notify: tokio::sync::Notify,
}
impl State {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            #[cfg(test)]
            routed_packets: AtomicU64::new(0),
            consumed: AtomicU64::new(0),
            admitted: AtomicBool::new(false),
            geometry: Mutex::new(None),
            ingress: Mutex::new(Ingress::default()),
            live: AtomicBool::new(true),
            cancel,
            telemetry: Arc::new(Telemetry::default()),
            queue: Mutex::new(std::collections::VecDeque::new()),
            notify: tokio::sync::Notify::new(),
        }
    }
    pub fn current(&self) -> bool {
        self.live.load(Ordering::Acquire) && !self.cancel.is_cancelled()
    }
    pub(crate) fn published(&self) -> u64 {
        self.ingress.lock().unwrap().published
    }
    // One conservative age witness covers dropped records without retaining
    // packets or growing a record history. Publication shares the same lock:
    // a late router observation cannot reinsert an already completed record.
    pub fn observe_queue_loss(&self, record: u64, deadline: u64) {
        let mut ingress = self.ingress.lock().unwrap();
        if record < ingress.published {
            return;
        }
        ingress.loss = Some(ingress.loss.map_or((record, deadline), |(through, age)| {
            (through.max(record), age.min(deadline))
        }));
    }
    pub fn publish(&self, next: u64) {
        let mut ingress = self.ingress.lock().unwrap();
        ingress.published = ingress.published.max(next);
        if ingress
            .loss
            .is_some_and(|(through, _)| through < ingress.published)
        {
            ingress.loss = None;
        }
        drop(ingress);
        let geometry = self.geometry.lock().unwrap().clone();
        if let Some(geometry) = geometry {
            geometry.publish(next);
        }
    }
    pub fn take_queue_loss(&self) -> Option<(u64, u64)> {
        self.ingress.lock().unwrap().loss.take()
    }
    pub fn retire(&self) {
        self.admitted.store(false, Ordering::Release);
        self.live.store(false, Ordering::Release);
        self.cancel.cancel();
        let geometry = self.geometry.lock().unwrap().take();
        if let Some(geometry) = geometry {
            geometry.disable();
        }
        self.telemetry.finalize_geometry();
        self.telemetry.active.store(false, Ordering::Release);
        self.queue.lock().unwrap().clear();
        self.notify.notify_waiters();
    }
}
pub(crate) struct Retire(pub Arc<State>);
impl Drop for Retire {
    fn drop(&mut self) {
        self.0.retire();
    }
}

pub(crate) enum GeometryTelemetry {
    Live(Arc<super::geometry::receive::Ingress>),
    Final(serde_json::Value),
}

#[derive(Default)]
pub struct Telemetry {
    pub(crate) geometry: Mutex<Option<GeometryTelemetry>>,
    pub(crate) stats: Mutex<Stats>,
    pub(crate) budget: Mutex<Option<Budget>>,
    pub(crate) active: AtomicBool,
    pub(crate) protocol_version: AtomicU64,
    pub(crate) rtt: AtomicU64,
    pub(crate) data: AtomicU64,
    pub(crate) parity: AtomicU64,
    pub(crate) sent_data: AtomicU64,
    pub(crate) sent_parity: AtomicU64,
    pub(crate) sent_repair: AtomicU64,
    pub(crate) requested: AtomicU64,
    pub(crate) queue_dropped: AtomicU64,
}
impl Telemetry {
    pub(crate) fn finalize_geometry(&self) {
        let mut geometry = self.geometry.lock().unwrap();
        if let Some(GeometryTelemetry::Live(ingress)) = geometry.as_ref() {
            *geometry = Some(GeometryTelemetry::Final(ingress.diagnostics()));
        }
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let s = self.stats.lock().unwrap();
        let read = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let geometry_hints =
            self.geometry
                .lock()
                .unwrap()
                .as_ref()
                .map(|geometry| match geometry {
                    GeometryTelemetry::Live(ingress) => ingress.diagnostics(),
                    GeometryTelemetry::Final(final_counts) => final_counts.clone(),
                });
        serde_json::json!({"geometry_hints":geometry_hints,"active":self.active.load(Ordering::Acquire),"rtt_ms":read(&self.rtt),"state_bytes":self.budget.lock().unwrap().as_ref().map_or(0,Budget::used),"protocol_version":read(&self.protocol_version),"trimmed_shards":s.trimmed_shards,"received_shards":s.received_shards,"duplicate_shards":s.duplicate_shards,"original_lost_shards":s.original_lost_shards,"fec_recovered_shards":s.fec_recovered_shards,"expired_records":s.expired_records,"late_shards":s.late_shards,"unprotected_blocks":s.unprotected_blocks,"received_data_bytes":read(&self.data),"received_parity_bytes":read(&self.parity),"sender_original_data_bytes":read(&self.sent_data),"sender_parity_bytes":read(&self.sent_parity),"sender_repair_bytes":read(&self.sent_repair),"repair_requested_shards":read(&self.requested),"queue_dropped_shards":read(&self.queue_dropped)})
    }
}
#[cfg(test)]
pub(crate) fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
mod peer_budget_tests {
    use super::*;

    #[tokio::test]
    async fn mapped_ip_and_reservation_only_owners_preserve_peer_identity() {
        let _serial = test_lock().lock().await;
        let baseline = global().used();
        let mut registry = PeerBudgets::default();
        let peer = registry.get("192.0.2.44".parse().unwrap()).unwrap();
        let weak = peer.downgrade();
        let child = peer.child(SUB_BYTES);
        let held = child.reserve(SUB_BYTES).unwrap();
        drop(child);
        drop(peer);
        let mapped = registry.get("::ffff:192.0.2.44".parse().unwrap()).unwrap();
        assert_eq!(mapped.used(), 32 * 1024 * 1024);
        assert!(mapped.reserve(97 * 1024 * 1024).is_err());
        assert_eq!(registry.entries.len(), 1);
        drop(mapped);
        assert!(
            weak.upgrade().is_some(),
            "reservation must retain the peer identity"
        );
        drop(held);
        assert!(
            weak.upgrade().is_none(),
            "registry must not strongly retain idle peers"
        );
        let next = registry.get("192.0.2.45".parse().unwrap()).unwrap();
        assert_eq!(
            registry.entries.len(),
            1,
            "expired identities must be pruned"
        );
        drop(next);
        assert_eq!(global().used(), baseline);
    }

    #[tokio::test]
    async fn peer_registry_bounds_live_entries_without_evicting_charged_identity() {
        let _serial = test_lock().lock().await;
        let mut registry = PeerBudgets::default();
        let owners: Vec<_> = (0..1024u32)
            .map(|n| registry.get(IpAddr::V4((0x0a000000 + n).into())).unwrap())
            .collect();
        let charge = owners[0].reserve(1).unwrap();
        let extra = "198.51.100.2".parse().unwrap();
        assert!(matches!(registry.get(extra), Err(Error::BudgetExceeded)));
        assert_eq!(registry.get("10.0.0.0".parse().unwrap()).unwrap().used(), 1);
        drop(owners);
        let next = registry.get(extra).unwrap();
        assert_eq!(
            registry.entries.len(),
            2,
            "charged identity must survive pruning at the cap"
        );
        assert_eq!(registry.get("10.0.0.0".parse().unwrap()).unwrap().used(), 1);
        drop(charge);
        drop(next);
        let fresh = registry.get(extra).unwrap();
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(fresh.used(), 0);
    }
}
