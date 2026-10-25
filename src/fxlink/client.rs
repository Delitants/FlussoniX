use super::{
    Address, Budget, Capabilities, Features, Receiver, RecordStream, Settings, Stats,
    control::{self, Control},
    packet::Header,
    transport::*,
};
use bytes::Bytes;
use futures_util::FutureExt;
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
struct Key {
    host: String,
    port: u16,
    peer_key: String,
    trust: [u8; 32],
    settings: Settings,
    features: Features,
}
impl PartialEq for Key {
    fn eq(&self, b: &Self) -> bool {
        self.host == b.host
            && self.port == b.port
            && self.peer_key == b.peer_key
            && self.trust == b.trust
            && self.settings == b.settings
            && self.features == b.features
    }
}
struct Peer {
    key: Key,
    connection: quinn::Connection,
    commands: mpsc::Sender<Start>,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.connection.close(0u32.into(), b"closed");
    }
}
#[derive(Default)]
pub struct ClientPool {
    peers: Mutex<Vec<Peer>>,
    features: Features,
}
struct Start {
    name: String,
    peer_key: String,
    token: String,
    settings: Settings,
    state: Arc<State>,
    ready: oneshot::Sender<Result<(), String>>,
}
pub struct Subscription {
    state: Arc<State>,
    connection: quinn::Connection,
}
#[cfg(test)]
pub(super) fn negotiated_version_for_test(subscription: &Subscription) -> u8 {
    control::version(&subscription.connection).unwrap()
}
impl Subscription {
    pub fn current(&self) -> bool {
        self.state.current() && self.connection.close_reason().is_none()
    }
    pub fn stats(&self) -> Stats {
        self.state.telemetry.stats.lock().unwrap().clone()
    }
    pub fn telemetry(&self) -> Arc<Telemetry> {
        self.state.telemetry.clone()
    }
    /// Retain before into_stream to interrupt downstream work on retirement.
    pub fn retirement_token(&self) -> CancellationToken {
        self.state.cancel.clone()
    }
    pub fn connection_id(&self) -> usize {
        self.connection.stable_id()
    }
    pub async fn recv(&mut self) -> Result<Bytes, String> {
        if !self.current() {
            return Err("FXLink subscription retired".into());
        }
        let record = loop {
            let notified = self.state.notify.notified();
            if let Some(record) = self.state.queue.lock().unwrap().pop_front() {
                break record;
            }
            tokio::select! {biased;_=self.state.cancel.cancelled()=>return Err("FXLink subscription retired".into()),_=notified=>{}}
        };
        if !self.current() || now() >= record.deadline_ms {
            self.state.retire();
            return Err("FXLink subscription retired or expired".into());
        }
        self.state.consumed.fetch_add(1, Ordering::Release);
        Ok(record.bytes)
    }
    pub fn into_stream(self) -> RecordStream {
        Box::pin(futures_util::stream::unfold(
            Some(self),
            |state| async move {
                let mut sub = state?;
                match sub.recv().await {
                    Ok(bytes) => Some((Ok(bytes), Some(sub))),
                    Err(e) => Some((Err(e), None)),
                }
            },
        ))
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.state.retire();
    }
}
impl ClientPool {
    pub fn with_features(features: Features) -> Self {
        Self {
            peers: Mutex::new(Vec::new()),
            features,
        }
    }
    pub async fn subscribe(
        &self,
        url: &str,
        peer_key: &str,
        token: &str,
        ca: Option<&Path>,
        settings: Settings,
        cancel: CancellationToken,
    ) -> Result<Subscription, String> {
        tokio::select! {biased;_=cancel.cancelled()=>Err("FXLink cancelled".into()),r=tokio::time::timeout(SETUP+ADMISSION,self.subscribe_inner(url,peer_key,token,ca,settings,cancel.clone()))=>r.map_err(|_|"FXLink setup timeout")?}
    }
    async fn subscribe_inner(
        &self,
        url: &str,
        peer_key: &str,
        token: &str,
        ca: Option<&Path>,
        settings: Settings,
        cancel: CancellationToken,
    ) -> Result<Subscription, String> {
        settings.validate().map_err(|e| e.to_string())?;
        let address = Address::parse(url, true).map_err(|e| e.to_string())?;
        if peer_key.len() > 4096
            || token.len() > 4096
            || address.stream.as_ref().unwrap().len() > 1024
        {
            return Err("FXLink credential/name limit".into());
        }
        let mut peers = self.peers.lock().await;
        let mut setup = global()
            .reserve(4 * 1024 * 1024)
            .map_err(|e| e.to_string())?;
        // Hash the same parsed CA bytes used for this connection; no path-only trust reuse or TOCTOU reload.
        let (tls, trust) = client_tls(ca, self.features)?;
        let key = Key {
            host: address.host.clone(),
            port: address.port,
            peer_key: peer_key.into(),
            trust,
            settings: settings.clone(),
            features: self.features,
        };
        let mut i = 0;
        while i < peers.len() {
            if peers[i].task.as_ref().unwrap().is_finished()
                || peers[i].connection.close_reason().is_some()
            {
                let p = peers.remove(i);
                p.cancel.cancel();
                let Taken { task } = take_peer(p);
                let _ = task.await;
            } else {
                i += 1
            }
        }
        let index = if let Some(index) = peers.iter().position(|p| p.key == key) {
            let peer_setup = peer_budget(peers[index].connection.remote_address().ip())
                .and_then(|budget| budget.reserve(4 * 1024 * 1024))
                .map_err(|e| e.to_string())?;
            // Reserve under the peer before releasing pre-identity scratch.
            drop(std::mem::replace(&mut setup, peer_setup));
            index
        } else {
            if peers.len() >= 16 {
                return Err("FXLink peer limit".into());
            }
            let remote = tokio::time::timeout(
                SETUP,
                tokio::net::lookup_host((address.host.as_str(), address.port)),
            )
            .await
            .map_err(|_| "FXLink DNS timeout")?
            .map_err(|_| "FXLink DNS failed")?
            .next()
            .ok_or("FXLink DNS empty")?;
            let local: SocketAddr = if remote.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            }
            .parse()
            .unwrap();
            let mut endpoint = quinn::Endpoint::client(local).map_err(|_| "FXLink client bind")?;
            let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
                .map_err(|_| "FXLink TLS 1.3 required")?;
            let mut config = quinn::ClientConfig::new(Arc::new(crypto));
            config.transport_config(super::transport::config());
            endpoint.set_default_client_config(config);
            let budget = peer_budget(remote.ip()).map_err(|e| e.to_string())?;
            let peer_setup = budget.reserve(4 * 1024 * 1024).map_err(|e| e.to_string())?;
            // Keep continuous global accounting while transferring known-IP setup.
            drop(std::mem::replace(&mut setup, peer_setup));
            let queues = budget.reserve(QUEUE_BYTES).map_err(|e| e.to_string())?;
            let connecting = endpoint
                .connect(remote, &address.host)
                .map_err(|_| "FXLink connect failed")?;
            let connection = tokio::time::timeout(SETUP, connecting)
                .await
                .map_err(|_| "FXLink TLS connection timeout")?
                .map_err(|_| "FXLink TLS connection failed")?;
            if connection
                .max_datagram_size()
                .is_none_or(|n| n < super::packet::HEADER_LEN + settings.shard_size)
            {
                return Err("FXLink datagram MTU too small".into());
            }
            let (tx, rx) = mpsc::channel(16);
            let stop = CancellationToken::new();
            let task = tokio::spawn(router(
                endpoint,
                connection.clone(),
                rx,
                budget,
                queues,
                stop.clone(),
            ));
            peers.push(Peer {
                key,
                connection,
                commands: tx,
                cancel: stop,
                task: Some(task),
            });
            peers.len() - 1
        };
        let peer = &peers[index];
        let state = Arc::new(State::new(cancel.child_token()));
        let guard = state.cancel.clone().drop_guard();
        let (ready, ack) = oneshot::channel();
        peer.commands
            .try_send(Start {
                name: address.stream.unwrap(),
                peer_key: peer_key.into(),
                token: token.into(),
                settings,
                state: state.clone(),
                ready,
            })
            .map_err(|_| "FXLink subscription limit")?;
        let connection = peer.connection.clone();
        drop(peers);
        ack.await.map_err(|_| "FXLink setup closed")??;
        // Transfer synchronous retirement ownership to the returned Subscription.
        guard.disarm();
        Ok(Subscription { state, connection })
    }
    pub async fn close(&self) {
        let peers = std::mem::take(&mut *self.peers.lock().await);
        for peer in &peers {
            peer.cancel.cancel();
            peer.connection.close(0u32.into(), b"shutdown");
        }
        for peer in peers {
            let Taken { task } = take_peer(peer);
            let _ = task.await;
        }
    }
}
// Moving a JoinHandle out of a Drop type uses a small owned wrapper instead of detaching it.
struct Taken {
    task: JoinHandle<()>,
}
fn take_peer(mut p: Peer) -> Taken {
    p.cancel.cancel();
    let task = p.task.take().unwrap();
    Taken { task }
}
fn client_tls(
    ca: Option<&Path>,
    features: Features,
) -> Result<(tokio_rustls::rustls::ClientConfig, [u8; 32]), String> {
    use tokio_rustls::rustls::{
        self,
        pki_types::{CertificateDer, pem::PemObject},
    };
    let mut roots = rustls::RootCertStore::empty();
    let bytes = if let Some(path) = ca {
        if !path.is_absolute() || !path.is_file() {
            return Err("FXLink CA path invalid".into());
        }
        let bytes = crate::rtsp::tls::pem(path).map_err(|_| "FXLink CA read failed")?;
        let certs = CertificateDer::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "FXLink CA invalid")?;
        if certs.is_empty() || certs.len() > 128 {
            return Err("FXLink CA count".into());
        }
        for cert in certs {
            roots.add(cert).map_err(|_| "FXLink CA invalid")?;
        }
        bytes
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        b"public-roots".to_vec()
    };
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|_| "FXLink TLS configuration")?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = control::protocols_with_features(features);
    tls.enable_early_data = false;
    Ok((tls, Sha256::digest(&bytes).into()))
}
struct Route {
    tx: super::packet_queue::Sender,
    state: Arc<State>,
    budget: Budget,
    settings: Settings,
}
async fn router(
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    mut commands: mpsc::Receiver<Start>,
    budget: Budget,
    _queues: super::Reservation,
    cancel: CancellationToken,
) {
    let mut routes: HashMap<u64, Route> = HashMap::new();
    let version = control::version(&connection).unwrap_or(0);
    let capabilities = Capabilities::for_version(version);
    let geometry = if version == 9 {
        super::geometry::receive::ConnectionIngress::new(&budget)
    } else {
        None
    };
    let mut tasks = JoinSet::new();
    let mut idle = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                routes.retain(|_, route| route.state.current());
                if routes.is_empty() && tasks.is_empty() && commands.is_empty() && idle.elapsed() > Duration::from_secs(1) {
                    break;
                }
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Ok(id) = result { routes.remove(&id); }
                if routes.is_empty() { idle = Instant::now(); }
            },
            request = commands.recv() => {
                let Some(start) = request else { break; };
                if tasks.len() >= 16 {
                    let _ = start.ready.send(Err("FXLink subscription limit".into()));
                    start.state.retire();
                    continue;
                }
                // Never let exhausted reliable-stream credit block DATAGRAM demultiplexing.
                let Some(Ok((mut send, recv))) = connection.open_bi().now_or_never() else {
                    let _ = start.ready.send(Err("FXLink stream setup failed".into()));
                    start.state.retire();
                    continue;
                };
                let id = send.id().index();
                let child = budget.child(SUB_BYTES);
                let child=if let Some(geometry)=&geometry {geometry.scoped(&child)} else {child};
                let (tx, rx) = match super::packet_queue::channel(&child) {
                    Ok(channel) => channel,
                    Err(error) => {
                        let _ = start.ready.send(Err(error));
                        start.state.retire();
                        let _ = send.reset(1u32.into());
                        continue;
                    },
                };
                routes.insert(id, Route { tx, state:start.state.clone(), budget:child.clone(), settings:start.settings.clone() });
                let connection = connection.clone();
                let stop = cancel.child_token();
                let geometry=geometry.clone();
                tasks.spawn(async move {
                    receive_subscription(connection, id, start, child, rx, (send, recv), stop,geometry).await;
                    id
                });
            },
            packet = connection.read_datagram() => {
                let Ok(packet) = packet else { break; };
                let observed=now();
                #[cfg(test)]
                if let Some((id,_))=super::geometry::wire::peek_route(&packet)
                    && let Some(route)=routes.get(&id) {
                    super::listener::geometry_endpoint_tests::observe_hint(&route.state,false);
                }
                if version==9 && packet.starts_with(b"FXGH") {
                    if let Some((id,generation))=super::geometry::wire::peek_route(&packet)
                        && generation==1 && let Some(route)=routes.get(&id)
                        && route.state.current() && route.state.admitted.load(Ordering::Acquire) {
                        let ingress=route.state.geometry.lock().unwrap().clone();
                        if let (Some(connection),Some(ingress))=(&geometry,ingress) {
                            ingress.receive(connection,&packet,observed,route.state.published());
                        }
                        #[cfg(test)] super::listener::geometry_endpoint_tests::observe_hint(&route.state,true);
                    }
                    continue;
                }
                let Ok(header) = Header::parse(&packet) else { continue; };
                if header.generation != 1 { continue; }
                tracing::trace!(subscription=header.subscription,record=header.record_id,block=header.block_id,shard=header.shard_index,observed_ms=observed,"FXLink media datagram received");
                if let Some(route) = routes.get(&header.subscription) {
                    if !route.state.current() { continue; }
                    if validate_packet_profile(&header,&route.settings,capabilities).is_err() {
                        route.state.retire();
                        continue;
                    }
                    if let Some(ingress)=route.state.geometry.lock().unwrap().as_ref() {
                        ingress.authoritative(header.record_id,header.record_len as usize);
                    }
                    match Charged::new(packet, &route.budget) {
                        Ok(packet) => {
                            let sent=route.tx.try_send(packet, &header);
                            #[cfg(test)]
                            if sent.is_ok() {route.state.routed_packets.fetch_add(1,Ordering::Release);}
                            match sent {
                                Ok(())=>{},
                                Err(super::packet_queue::SendError::Full)=>{
                                    observe_geometry_queue_loss(&route.state,&header,observed+route.settings.latency_ms);
                                    tracing::debug!(subscription=header.subscription,record=header.record_id,"FXLink datagram dropped at packet queue");
                                },
                                Err(super::packet_queue::SendError::Closed | super::packet_queue::SendError::Limit)=>route.state.retire(),
                            }
                            #[cfg(test)]
                            startup_tests::observe_packet(&route.state,&header);
                        },
                        Err(error) => {
                            tracing::debug!(subscription=header.subscription,%error,used=route.budget.used(),limit=route.budget.limit(), "FXLink datagram route retired at packet budget");
                            route.state.retire();
                        },
                    }
                }
            },
        }
    }
    cancel.cancel();
    for route in routes.values() {
        route.state.retire();
    }
    routes.clear();
    connection.close(0u32.into(), b"closed");
    while tasks.join_next().await.is_some() {}
    endpoint.close(0u32.into(), b"closed");
    endpoint.wait_idle().await;
}
type DecodeJob = JoinHandle<(Receiver, Result<Vec<super::ReceivedRecord>, super::Error>)>;

fn activate_geometry(
    state: &State,
    connection: &super::geometry::receive::ConnectionIngress,
    budget: &Budget,
    settings: &Settings,
) {
    let mut ingress = state.geometry.lock().unwrap();
    // A full pre-Accepted queue retained age but not each discarded length.
    // Keep v8 repair fallback when those exact authorities cannot be rebuilt.
    // This lock also orders activation against the late Full-path hook below.
    if state.telemetry.queue_dropped.load(Ordering::Acquire) == 0 {
        *ingress = connection.admit(budget, settings);
    }
}

fn observe_geometry_queue_loss(state: &State, header: &Header, deadline: u64) {
    state.observe_queue_loss(header.record_id, deadline);
    state
        .telemetry
        .queue_dropped
        .fetch_add(1, Ordering::Release);
    // Activation can occur after the router's pre-queue authority hook. If it
    // did, preserve this exact length before any later hint is demultiplexed.
    if let Some(ingress) = state.geometry.lock().unwrap().as_ref() {
        ingress.authoritative(header.record_id, header.record_len as usize);
    }
}

// Amortize blocking-pool handoffs without growing packet routes or their byte
// budgets. Each held datagram keeps its existing hierarchical reservation.
const DECODE_BATCH_PACKETS: usize = 32;
// A partially written length-prefixed control frame must be reset on retirement,
// never abandoned and followed by another frame on the same stream.
struct ResetSend(quinn::SendStream);
impl Drop for ResetSend {
    fn drop(&mut self) {
        let _ = self.0.reset(1u32.into());
    }
}
type FeedbackWrite = futures_util::future::BoxFuture<
    'static,
    (ResetSend, Option<super::repair::Pacing>, Result<(), String>),
>;

#[allow(clippy::too_many_arguments)]
async fn receive_subscription(
    connection: quinn::Connection,
    id: u64,
    start: Start,
    budget: Budget,
    ingress: super::packet_queue::Ingress,
    (send, mut recv): (quinn::SendStream, quinn::RecvStream),
    cancel: CancellationToken,
    geometry: Option<Arc<super::geometry::receive::ConnectionIngress>>,
) {
    let super::packet_queue::Ingress {
        mut packets,
        pending: queued,
    } = ingress;
    let Start {
        name,
        peer_key,
        token,
        settings,
        state,
        ready,
    } = start;
    let _retire = Retire(state.clone());
    let mut ready = Some(ready);
    let mut send = Some(ResetSend(send));
    let mut feedback_write: Option<FeedbackWrite> = None;
    let mut feedback_repairs = false;
    let mut repair_pacing = None;
    // Keep started work outside the fallible receive loop so every exit can retire first and then join.
    let mut job: Option<DecodeJob> = None;
    let result: Result<(), String> = async {
        state.telemetry.active.store(true, Ordering::Release);
        *state.telemetry.budget.lock().unwrap() = Some(budget.clone());
        let _metadata = budget.reserve(65536).map_err(|e| e.to_string())?;
        repair_pacing=Some(super::repair::Pacing::new(budget.clone(),settings.max_blocks)?);
        let version=control::version(&connection)?;
        let mut receiver = Some(Receiver::new(settings.clone(), id, 1, budget.clone()).map_err(|e| e.to_string())?.with_capabilities(Capabilities::for_version(version)));
        let setup = async {
            tokio::time::timeout(SETUP, control::write(&mut send.as_mut().unwrap().0, &Control::Subscribe {
                version, id, generation: 1, profile: control::profile(&settings), name, peer_key, token,
            })).await.map_err(|_| "FXLink request timeout")??;
            match tokio::time::timeout(ADMISSION,control::read(&mut recv)).await.map_err(|_| "FXLink admission timeout")?? {
                Control::Accepted => Ok(()),
                Control::Denied { status } => Err(format!("FXLink admission denied ({status})")),
                _ => Err("FXLink invalid acceptance".into()),
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => return Err("FXLink cancelled".into()),
            _ = state.cancel.cancelled() => return Err("FXLink cancelled".into()),
            result = setup => result?,
        }
        if !state.current() { return Err("FXLink retired".into()); }
        if let Some(geometry)=&geometry {
            activate_geometry(&state,geometry,&budget,&settings);
        }
        let hint_ingress=state.geometry.lock().unwrap().clone();
        *state.telemetry.geometry.lock().unwrap()=hint_ingress.clone();
        state.admitted.store(true,Ordering::Release);
        state.telemetry.protocol_version.store(u64::from(version),Ordering::Relaxed);
        let _ = ready.take().unwrap().send(Ok(()));
        let mut read = control::Reader::new(recv);
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        let mut repair_at = now() + super::repair::POLL_MS;
        let mut repair_not_before = 0;
        let mut first_repair_at = None;
        let mut next_feedback_sequence = 1;
        let mut replica: Option<super::feedback_copy::Replica> = None;
        let mut bootstrap_slot=super::bootstrap::Slot::default();
        let mut deadlines: std::collections::BTreeMap<u64,u64> = std::collections::BTreeMap::new();
        let mut deferred = std::collections::BTreeMap::new();
        let mut delivered = 0u64;
        let mut reported = 0u64;
        let mut progress_at = now();
        let mut discarded = DiscardFence::default();
        let mut packet:Option<super::packet_queue::Packet> = None;
        // A different record waits unprepared until this record has published
        // and freed its deadline/window entries. It keeps its reservation.
        let mut pending:Option<super::packet_queue::Packet> = None;
        let mut acquire:Option<futures_util::future::BoxFuture<'static,Result<tokio::sync::OwnedSemaphorePermit,tokio::sync::AcquireError>>> = None;
        loop {
            if !state.current() || cancel.is_cancelled() { return Err("FXLink retired".into()); }
            bootstrap_slot.prune(now(),delivered);
            if let Some((through,deadline))=state.take_queue_loss() {
                if through>=delivered {discarded.observe(through,deadline);}
            }
            discarded.advance(delivered);
            // Consume an old copy before a primary batch can replace it. This
            // never changes primary pacing, Receiver ownership or grace hints.
            if replica.as_ref().is_some_and(|copy|now()>=copy.due) {
                let copied = replica.take().unwrap().attempt(&connection,now(),connection.rtt().as_millis() as u64,delivered);
                tracing::trace!(copied,"FXLink feedback copy attempt");
            }
            // Service an overdue query before another packet takes Receiver.
            // A busy decoder must not advance the poll clock without a query.
            let consumed=delivered.min(state.consumed.load(Ordering::Acquire));
            let progress=(version>=2 && consumed>reported && now()>=progress_at && feedback_write.is_none()).then_some(consumed);
            let first_at=first_repair_at.into_iter().chain(bootstrap_slot.wake_at()).min();
            let retry_at=retry_wake_at(repair_pacing.as_ref(),first_at,receiver.is_some(),version>=3 || feedback_write.is_none(),repair_not_before);
            let query_due=receiver.is_some() && now()>=repair_not_before
                && (now()>=repair_at || retry_at.is_some_and(|at|now()>=at));
            if (version>=3 || feedback_write.is_none()) && (query_due || progress.is_some()) {
                // One accounted, serialized batch: progress coalesces in two
                // counters while a previous write is pending, never a queue.
                let metadata=if matches!(version,8|9) {super::bootstrap::reserve_batch(&mut bootstrap_slot,&budget)}else{budget.reserve(65536)}.map_err(|e|e.to_string())?;
                let rtt=connection.rtt().as_millis() as u64;
                let at=now();
                let mut bootstrap_batch=Vec::new();
                let requests=if query_due {
                    repair_at=at+super::repair::POLL_MS;
                    // Rebuild even when Receiver returns no requests. A due
                    // hint is consumed once, and progress-only writes keep it.
                    first_repair_at=None;
                    let pacing=repair_pacing.as_mut().unwrap();
                    pacing.prune(at,delivered);
                    let queued = queued.lock();
                    if matches!(version,8|9) {
                        let view=super::bootstrap::View {
                            receiver:receiver.as_ref().unwrap(),pacing,settings:&settings,budget:&budget,at,rtt,
                            deadline:&|record|discarded.deadline(&deadlines,record,at.saturating_add(settings.latency_ms)),
                            authoritative_shape:&|record|deadlines.contains_key(&record)||deferred.contains_key(&record),
                            geometry_known:&|record|deadlines.contains_key(&record) || deferred.contains_key(&record)
                                || discarded.0.is_some_and(|(through,_)|record<=through),
                            pending_shard:&|block,shard|queued.contains(block,shard),
                            pending_record:&|record|queued.contains_record(record),
                        };
                        let snapshot=hint_ingress.as_ref().and_then(|ingress|ingress.snapshot(at,delivered));
                        bootstrap_batch=if let Some(snapshot)=snapshot {
                            let proof=deadlines.keys().copied().chain(deferred.keys().copied()).chain(discarded.0.map(|(through,_)|through)).max();
                            super::geometry::plan(view,&snapshot,proof,&mut bootstrap_slot,&mut first_repair_at)
                        } else {super::bootstrap::plan(view,&mut bootstrap_slot,&mut first_repair_at)};
                        tracing::trace!(head=delivered,request_blocks=bootstrap_batch.len(),rtt,"FXLink bootstrap repair poll");
                        Vec::new()
                    } else {
                    let mut eligible=pacing.batch_eligible(at);
                    let requests=receiver.as_ref().unwrap().repair_requests_avoiding(at,rtt,5,|block,shard| {
                        let record=block/super::MAX_BLOCKS_PER_RECORD as u64;
                        let deadline=discarded.deadline(&deadlines,record,at+settings.latency_ms);
                        let first=deadline.saturating_sub(settings.latency_ms);
                        let geometry_known=deadlines.contains_key(&record);
                        let grace=first_repair_grace(version,settings.latency_ms,rtt,geometry_known);
                        let allowed=super::repair::after_reordering(at,first,grace,&mut first_repair_at) && eligible(block,shard);
                        #[cfg(test)] early_probe_tests::observe(&state,early_probe_tests::Seen{record,known:geometry_known,rtt,grace,at,first,allowed,wake:first_repair_at});
                        allowed
                    }, |block,shard| queued.contains(block,shard));
                    tracing::trace!(head=delivered,pending_records=deadlines.len(),queued_packets=packets.len(),request_blocks=requests.len(),rtt,"FXLink repair poll");
                    requests.into_iter().map(|request| {
                        let record=request.block_id/super::MAX_BLOCKS_PER_RECORD as u64;
                        let deadline=discarded.deadline(&deadlines,record,at+settings.latency_ms);
                        (request,deadline)
                    }).collect::<Vec<_>>()
                    }
                } else {Vec::new()};
                let requests=if matches!(version,8|9) {
                    use super::feedback::DatagramRequest;
                    let selected_bootstrap=bootstrap_batch.iter().any(|entry|matches!(entry.request,DatagramRequest::Bootstrap{..}));
                    bootstrap_batch.retain(|entry|now().saturating_add(rtt).saturating_add(5)<entry.deadline);
                    if selected_bootstrap && !bootstrap_batch.iter().any(|entry|matches!(entry.request,DatagramRequest::Bootstrap{..})) {
                        bootstrap_slot.abandon();
                    }
                    let prepared=super::bootstrap::prepare_history(&mut bootstrap_batch,&mut bootstrap_slot,repair_pacing.as_ref().unwrap())?;
                    let copy_deadline=bootstrap_batch.iter().map(|entry|entry.deadline).min();
                    let last_record=bootstrap_batch.iter().map(|entry|entry.request.block_id()/super::MAX_BLOCKS_PER_RECORD as u64).max();
                    let requests:Vec<_>=bootstrap_batch.into_iter().map(|entry|entry.request).collect();
                    if !state.current() || cancel.is_cancelled() {return Err("FXLink retired".into());}
                    let frames=super::feedback::encode_bootstrap(id,&requests,&settings,&mut next_feedback_sequence)?;
                    if !frames.is_empty() {
                        super::feedback_copy::Replica::take_attempt(&mut replica,&connection,now(),connection.rtt().as_millis() as u64,delivered);
                    }
                    for frame in &frames {connection.send_datagram(frame.clone()).map_err(|_|"FXLink feedback datagram send failed")?;}
                    let queued_at=now();
                    if let (Some(deadline),Some(last_record))=(copy_deadline,last_record) {
                        replica=super::feedback_copy::Replica::new(frames,queued_at,deadline,last_record,&budget);
                    }
                    if !requests.is_empty() {repair_not_before=queued_at.saturating_add(super::repair::POLL_MS);}
                    for (request,prepared) in requests.into_iter().zip(prepared) {
                        let block=request.block_id();let units=request.units();
                        let is_bootstrap=matches!(request,DatagramRequest::Bootstrap{..});
                        match request {
                            DatagramRequest::Ordinary(request)=>repair_pacing.as_mut().unwrap().sent_prepared(prepared,&request.shard_indices,queued_at,rtt)?,
                            DatagramRequest::Bootstrap{reply_limit,..}=>{
                                let prefix:[u16;128]=std::array::from_fn(|index|index as u16);
                                repair_pacing.as_mut().unwrap().sent_prepared(prepared,&prefix[..usize::from(reply_limit)],queued_at,rtt)?;
                                bootstrap_slot.admitted(block,reply_limit,queued_at,rtt)?;
                            }
                        }
                        state.telemetry.requested.fetch_add(units as u64,Ordering::Relaxed);
                        if tracing::enabled!(tracing::Level::TRACE) {
                            let record=block/super::MAX_BLOCKS_PER_RECORD as u64;
                            let deadline=discarded.deadline(&deadlines,record,at.saturating_add(settings.latency_ms));
                            let known=deadlines.contains_key(&record);
                            let grace=if is_bootstrap {super::repair::reordering_grace(settings.latency_ms,rtt)}else{first_repair_grace(7,settings.latency_ms,rtt,known)};
                            tracing::trace!(block,count=units,bootstrap=is_bootstrap,rtt,queued_at_ms=queued_at,first_seen_ms=deadline.saturating_sub(settings.latency_ms),remaining_ms=deadline.saturating_sub(queued_at),geometry_known=known,reordering_grace_ms=grace,"FXLink unordered repair queued");
                        }
                    }
                    Vec::new()
                } else if version>=3 {
                    // Datagram repair pacing remains available while a reliable
                    // progress write is blocked. Only one bounded poll batch.
                    let requests:Vec<_>=requests.into_iter().filter(|(_,deadline)|now().saturating_add(rtt).saturating_add(5)<*deadline).collect();
                    let copy_deadline=requests.iter().map(|(_,deadline)|*deadline).min();
                    let last_record=requests.iter().map(|(request,_)|request.block_id/super::MAX_BLOCKS_PER_RECORD as u64).max();
                    let requests:Vec<_>=requests.into_iter().map(|(request,_)|request).collect();
                    if !state.current() || cancel.is_cancelled() {return Err("FXLink retired".into());}
                    let frames=if version==7 {super::feedback::encode_parity(id,&requests,&settings,&mut next_feedback_sequence)?} else if version>=4 {super::feedback::encode_sequenced(id,&requests,&settings,&mut next_feedback_sequence)?} else {super::feedback::encode(id,&requests,&settings)?};
                    if version>=4 && !frames.is_empty() {
                        let copied=super::feedback_copy::Replica::take_attempt(&mut replica,&connection,now(),connection.rtt().as_millis() as u64,delivered);
                        tracing::trace!(copied,"FXLink feedback copy before next primary");
                    }
                    for frame in &frames {
                        connection.send_datagram(frame.clone()).map_err(|_|"FXLink feedback datagram send failed")?;
                    }
                    let queued_at=now();
                    if version>=4 && let (Some(deadline),Some(last_record))=(copy_deadline,last_record) {
                        replica=super::feedback_copy::Replica::new(frames,queued_at,deadline,last_record,&budget);
                    }
                    // Only an actual repair batch consumes the global spacing;
                    // an empty poll must not postpone an eligible retry.
                    if !requests.is_empty() {repair_not_before=queued_at+super::repair::POLL_MS;}
                    for request in requests {
                        repair_pacing.as_mut().unwrap().sent(request.block_id,&request.shard_indices,queued_at,rtt)?;
                        state.telemetry.requested.fetch_add(request.shard_indices.len() as u64,Ordering::Relaxed);
                        if tracing::enabled!(tracing::Level::TRACE) {
                            let record=request.block_id/super::MAX_BLOCKS_PER_RECORD as u64;
                            let deadline=discarded.deadline(&deadlines,record,at+settings.latency_ms);
                            // Only actual Announce/Header observations insert an ID.
                            // An inherited successor age does not establish geometry;
                            // prepared headers count even before a FEC permit is ready.
                            tracing::trace!(block=request.block_id,count=request.shard_indices.len(),rtt,queued_at_ms=queued_at,first_seen_ms=deadline.saturating_sub(settings.latency_ms),remaining_ms=deadline.saturating_sub(queued_at),geometry_known=deadlines.contains_key(&record),reordering_grace_ms=first_repair_grace(version,settings.latency_ms,rtt,deadlines.contains_key(&record)),"FXLink unordered repair queued");
                        }
                    }
                    Vec::new()
                } else {requests};
                if !requests.is_empty() || progress.is_some() {
                    feedback_repairs = !requests.is_empty();
                    if let Some(next)=progress {reported=next;progress_at=at+20;}
                    let feedback=Feedback {progress,requests,rtt,until_ms:(version>=2).then_some(at+settings.latency_ms)};
                    let pacing=if version>=3 {None} else {repair_pacing.take()};
                    feedback_write=Some(write_feedback(send.take().unwrap(),pacing,feedback,metadata,state.clone(),cancel.clone()).boxed());
                }
            }
            // Recompute after pruning and sending: a consumed conservative
            // hint must never leave a permanently ready timer in the select.
            let first_at=first_repair_at.into_iter().chain(bootstrap_slot.wake_at()).min();
            let retry_at=retry_wake_at(repair_pacing.as_ref(),first_at,receiver.is_some(),version>=3 || feedback_write.is_none(),repair_not_before);
            let copy_at=replica.as_ref().map(|copy|copy.due);
            #[cfg(test)] feedback_cancel_tests::observe(&state,copy_at,job.is_some(),now());
            tokio::select! {
                _ = cancel.cancelled() => return Err("FXLink cancelled".into()),
                _ = state.cancel.cancelled() => return Err("FXLink retired".into()),
                _ = async {hint_ingress.as_ref().unwrap().changed.notified().await}, if hint_ingress.is_some() => {},
                _ = async {tokio::time::sleep(Duration::from_millis(copy_at.unwrap().saturating_sub(now()))).await}, if copy_at.is_some() => {},
                _ = async {tokio::time::sleep(Duration::from_millis(retry_at.unwrap().saturating_sub(now()))).await}, if retry_at.is_some() => {
                    tracing::trace!(deadline=retry_at.unwrap(),at=now(),first_eligibility=?first_repair_at,"FXLink repair eligibility timer wake");
                },
                _ = tick.tick() => {
                    #[cfg(test)]
                    if now()>=repair_at && feedback_write.is_none() {review_tests::repair_poll(receiver.is_some());}
                    if now()>=repair_at {
                        tracing::trace!(head=delivered,pending_records=deadlines.len(),receiver_ready=receiver.is_some(),decode_pending=job.is_some(),permit_pending=acquire.is_some(),write_pending=feedback_write.is_some(),"FXLink repair poll waiting");
                    }
                    state.telemetry.rtt.store(connection.rtt().as_millis() as u64, Ordering::Relaxed);
                    if let Some(receiver)=receiver.as_mut() {
                        let result=receiver.tick(now());
                        *state.telemetry.stats.lock().unwrap()=receiver.stats().clone();
                        result.map_err(|e|e.to_string())?;
                    }
                    // These original deadlines stay readable while the decoder owns Receiver.
                    if discarded.expired(now()) || deadlines.values().any(|deadline|now()>=*deadline)
                        || state.queue.lock().unwrap().front().is_some_and(|record|now()>=record.deadline_ms) {
                        return Err("FXLink record deadline".into());
                    }
                },
                completed = async {feedback_write.as_mut().unwrap().await}, if feedback_write.is_some() => {
                    feedback_write=None;
                    let (returned,pacing,result)=completed;
                    send=Some(returned);
                    if pacing.is_some() {repair_pacing=pacing;}
                    result?;
                    if feedback_repairs {
                        repair_at=now()+super::repair::POLL_MS;
                        repair_not_before=repair_at;
                    }
                    feedback_repairs=false;
                },
                // Backpressure reliable announcements at the existing record
                // ceiling while queued UDP/decode work catches up. Fair select
                // polling prevents a ready control stream starving that work.
                message = read.next(deadlines.len() >= settings.max_records) => {
                    match message? {
                        Control::Announce { record, len } => {
                            let observed=now();
                            let deadline=discarded.deadline(&deadlines,record,observed+settings.latency_ms);
                            tracing::trace!(record,len,head=delivered,observed_ms=observed,first_seen_ms=deadline.saturating_sub(settings.latency_ms),"FXLink record announcement received");
                            remember_deadline(&mut deadlines, record, delivered, settings.max_records, deadline)?;
                            if let Some(receiver)=receiver.as_mut() {
                                apply_announcement(receiver,&state.telemetry,record,len,observed)?;
                            } else if record>=delivered {
                                if let Some((previous,_))=deferred.get(&record) {
                                    if *previous!=len {return Err("FXLink conflicting announcement".into());}
                                } else {
                                    if deferred.len()>=settings.max_records {return Err("FXLink announcement queue limit".into());}
                                    deferred.insert(record,(len,observed));
                                }
                            }
                            if let Some(ingress)=&hint_ingress {ingress.authoritative(record,len);}
                        },
                        Control::Counters { data, parity, repair } => {
                            state.telemetry.sent_data.store(data, Ordering::Relaxed);
                            state.telemetry.sent_parity.store(parity, Ordering::Relaxed);
                            state.telemetry.sent_repair.store(repair, Ordering::Relaxed);
                        },
                        _ => return Err("FXLink invalid media control".into()),
                    }
                },
                completed = async {job.as_mut().unwrap().await}, if job.is_some() => {
                    job=None;
                    let (mut returned,output)=completed.map_err(|_|"FXLink decode failed")?;
                    // A failed decode may already have retired Receiver. Keep
                    // that first error before follow-up work can return Retired.
                    let output=output.and_then(|output| {
                        for (record,(len,observed)) in std::mem::take(&mut deferred) {
                            returned.announce(record,len,observed)?;
                        }
                        returned.tick(now())?;
                        Ok(output)
                    });
                    // Capture final counters even when decoding or deferred
                    // validation failed; retirement still fences all output.
                    *state.telemetry.stats.lock().unwrap()=returned.stats().clone();
                    let output=output.map_err(|e|e.to_string())?;
                    receiver=Some(returned);
                    // Include drops observed while the decoder owned Receiver,
                    // before publication can clear their conservative age.
                    if let Some((through,deadline))=state.take_queue_loss() {
                        if through>=delivered {discarded.observe(through,deadline);}
                    }
                    for record in output {
                        let deadline=discarded.deadline(&deadlines,delivered,record.deadline_ms)
                            .min(deadlines.remove(&delivered).ok_or("FXLink missing record deadline")?);
                        delivered=delivered.checked_add(1).ok_or("FXLink record sequence exhausted")?;
                        if !state.current() || now()>=deadline {return Err("FXLink retired or expired".into());}
                        let mut queue=state.queue.lock().unwrap();
                        if !state.current(){return Err("FXLink retired".into());}
                        if queue.len()>=64{return Err("FXLink output overloaded".into());}
                        queue.push_back(Charged{bytes:record.bytes,deadline_ms:deadline,_guard:record.reservation});
                        state.notify.notify_one();
                    }
                    state.publish(delivered);
                    discarded.advance(delivered);
                },
                permit = async {acquire.as_mut().unwrap().await}, if acquire.is_some() => {
                    acquire=None;
                    let permit=permit.map_err(|_|"FXLink decode closed")?;
                    let mut batch=Vec::with_capacity(DECODE_BATCH_PACKETS);
                    batch.push(packet.take().unwrap());
                    let record_id=Header::parse(&batch[0].bytes).map_err(|e|e.to_string())?.record_id;
                    // Bound attempts as well as accepted packets: a stream of
                    // capacity-discarded future packets must not monopolize us.
                    for _ in 1..DECODE_BATCH_PACKETS {
                        let Ok(next)=packets.try_recv() else {break};
                        if Header::parse(&next.bytes).map_err(|e|e.to_string())?.record_id!=record_id {
                            pending=Some(next);
                            break;
                        }
                        if let Some(next)=prepare_decode_packet(next,&settings,&state,delivered,&mut deadlines,&mut discarded,Capabilities::for_version(version),(matches!(version,8|9)).then_some(&mut bootstrap_slot))? {
                            batch.push(next);
                        }
                    }
                    let mut decoder=receiver.take().unwrap();
                    let queued=Instant::now();
                    let state=state.clone();
                    let stop=cancel.clone();
                    job=Some(tokio::task::spawn_blocking(move || {
                        let _permit=permit;
                        let began=Instant::now();
                        #[cfg(test)] review_tests::pause_decode();
                        let count=batch.len();
                        let record=Header::parse(&batch[0].bytes).ok().map(|header|header.record_id);
                        let output=batch.into_iter().try_fold(Vec::new(),|mut records,packet| {
                            if !state.current() || stop.is_cancelled() {return Err(super::Error::Retired);}
                            records.extend(decoder.ingest_charged(&packet.bytes,now())?);
                            Ok(records)
                        });
                        if tracing::enabled!(tracing::Level::TRACE) {
                            tracing::trace!(record,packets=count,queue_wait_ms=began.duration_since(queued).as_millis() as u64,work_ms=began.elapsed().as_millis() as u64,error=?output.as_ref().err(),"FXLink decode job completed");
                        }
                        (decoder,output)
                    }));
                },
                next = async {if let Some(next)=pending.take(){Some(next)}else{packets.recv().await}}, if job.is_none() && acquire.is_none() => {
                    let Some(next)=next else{return Err("FXLink route closed".into());};
                    let Some(next)=prepare_decode_packet(next,&settings,&state,delivered,&mut deadlines,&mut discarded,Capabilities::for_version(version),(matches!(version,8|9)).then_some(&mut bootstrap_slot))? else {continue};
                    #[cfg(test)] review_tests::before_acquire().await;
                    packet=Some(next);
                    acquire=Some(fec_slots().acquire_owned().boxed());
                },
            }
        }
    }.await;
    // Remote reset, deadline and local cancellation fence queued output before joining a started job.
    if let Err(error) = &result {
        tracing::debug!(%error, subscription = id, "FXLink receive subscription ended");
    }
    tracing::trace!(quic=?connection.stats(),"FXLink receiver final QUIC state");
    state.retire();
    drop(feedback_write);
    drop(repair_pacing);
    drop(send);
    if let Some(job) = job.take() {
        let _ = job.await;
    }
    if let Some(ready) = ready {
        let _ = ready.send(result);
    }
}

fn apply_announcement(
    receiver: &mut Receiver,
    telemetry: &Telemetry,
    record: u64,
    len: usize,
    observed: u64,
) -> Result<(), String> {
    let result = receiver.announce(record, len, observed);
    if result.is_err() {
        // An announcement can expire and retire Receiver before the next tick.
        // Publish its final counters before propagating that original error.
        *telemetry.stats.lock().unwrap() = receiver.stats().clone();
    }
    result.map_err(|error| error.to_string())
}

// Metadata can trail a wholly absent record while its source cache is already
// aging. V7 spends at most one poll on unknown geometry; known records and
// older peers retain the full reordering grace. All retry/credit fences remain.
fn first_repair_grace(version: u8, latency: u64, rtt: u64, geometry_known: bool) -> u64 {
    let grace = super::repair::reordering_grace(latency, rtt);
    if version == 7 && !geometry_known {
        grace.min(super::repair::POLL_MS)
    } else {
        grace
    }
}

fn retry_wake_at(
    pacing: Option<&super::repair::Pacing>,
    first_at: Option<u64>,
    receiver_ready: bool,
    feedback_ready: bool,
    not_before: u64,
) -> Option<u64> {
    if !receiver_ready || !feedback_ready {
        return None;
    }
    pacing?
        .wake_at()
        .into_iter()
        .chain(first_at)
        .min()
        .map(|at| at.max(not_before))
}

#[allow(clippy::too_many_arguments)]
fn prepare_decode_packet(
    next: super::packet_queue::Packet,
    settings: &Settings,
    state: &State,
    delivered: u64,
    deadlines: &mut std::collections::BTreeMap<u64, u64>,
    discarded: &mut DiscardFence,
    capabilities: Capabilities,
    bootstrap: Option<&mut super::bootstrap::Slot>,
) -> Result<Option<super::packet_queue::Packet>, String> {
    let header = Header::parse(&next.bytes).map_err(|e| e.to_string())?;
    validate_packet_profile(&header, settings, capabilities)?;
    let counter = if header.shard_index < header.data_count {
        &state.telemetry.data
    } else {
        &state.telemetry.parity
    };
    counter.fetch_add(next.bytes.len() as u64, Ordering::Relaxed);
    let observed = now();
    if let Some(slot) = bootstrap {
        slot.observe_header(header.record_id, observed);
    }
    let future = header.record_id > delivered;
    let reserve = usize::from(future && !deadlines.contains_key(&delivered));
    if future
        && !deadlines.contains_key(&header.record_id)
        && deadlines.len() >= settings.max_records.saturating_sub(reserve)
    {
        // Retain the unknown-head slot and conservative age fence in batches.
        discarded.observe(header.record_id, observed + settings.latency_ms);
        tracing::trace!(
            record = header.record_id,
            delivered,
            "FXLink future packet discarded at record ceiling"
        );
        return Ok(None);
    }
    let deadline = discarded.deadline(deadlines, header.record_id, observed + settings.latency_ms);
    remember_deadline(
        deadlines,
        header.record_id,
        delivered,
        settings.max_records,
        deadline,
    )?;
    Ok(Some(next))
}

fn validate_packet_profile(
    header: &Header,
    settings: &Settings,
    capabilities: Capabilities,
) -> Result<(), String> {
    header
        .validate_profile(settings, capabilities)
        .map_err(|_| "FXLink unnegotiated packet profile or bounds".to_owned())
}

/// A constant-size conservative deadline for capacity-discarded observations.
/// It survives decode jobs and record drains; readmission never renews an age.
#[derive(Default)]
struct DiscardFence(Option<(u64, u64)>);
impl DiscardFence {
    fn observe(&mut self, record: u64, deadline: u64) {
        self.0 = Some(self.0.map_or((record, deadline), |(through, old)| {
            (through.max(record), old.min(deadline))
        }));
    }
    fn expired(&self, now: u64) -> bool {
        self.0.is_some_and(|(_, deadline)| now >= deadline)
    }
    fn advance(&mut self, delivered: u64) {
        if self.0.is_some_and(|(through, _)| delivered > through) {
            self.0 = None;
        }
    }
    fn deadline(
        &self,
        deadlines: &std::collections::BTreeMap<u64, u64>,
        record: u64,
        mut deadline: u64,
    ) -> u64 {
        // Seeing a successor also observes this gap, even while an older head
        // blocks delivery. Late metadata must not restart its reordering grace.
        // Older predecessors alone do not prove a newer record existed yet.
        if let Some(gap) = deadlines
            .range(record..)
            .map(|(_, deadline)| deadline)
            .min()
        {
            deadline = deadline.min(*gap);
        }
        if let Some((through, old)) = self.0 {
            if record <= through {
                deadline = deadline.min(old);
            }
        }
        deadline
    }
}

struct Feedback {
    progress: Option<u64>,
    requests: Vec<(super::RepairRequest, u64)>,
    rtt: u64,
    until_ms: Option<u64>,
}
async fn write_feedback(
    mut send: ResetSend,
    mut pacing: Option<super::repair::Pacing>,
    feedback: Feedback,
    _metadata: super::Reservation,
    state: Arc<State>,
    cancel: CancellationToken,
) -> (ResetSend, Option<super::repair::Pacing>, Result<(), String>) {
    let until_ms = feedback.until_ms;
    let write = async {
        if !state.current()
            || cancel.is_cancelled()
            || until_ms.is_some_and(|deadline| now() >= deadline)
        {
            return Err("FXLink feedback retired or expired".into());
        }
        if let Some(next) = feedback.progress {
            #[cfg(test)]
            review_tests::hold_progress().await;
            control::write(&mut send.0, &Control::Progress { next }).await?;
        }
        for (request, deadline) in feedback.requests {
            if !state.current() || cancel.is_cancelled() {
                return Err("FXLink retired".into());
            }
            if now().saturating_add(feedback.rtt).saturating_add(5) >= deadline {
                continue;
            }
            let count = request.shard_indices.len() as u64;
            let message = Control::Repair {
                block: request.block_id,
                shards: request.shard_indices.clone(),
            };
            let write_at = now();
            tracing::trace!(
                block = request.block_id,
                count,
                remaining_ms = deadline.saturating_sub(write_at),
                rtt = feedback.rtt,
                "FXLink repair write started"
            );
            control::write(&mut send.0, &message).await?;
            tracing::trace!(
                block = request.block_id,
                write_ms = now().saturating_sub(write_at),
                "FXLink repair write completed"
            );
            pacing
                .as_mut()
                .ok_or("FXLink reliable repair pacing missing")?
                .sent(
                    request.block_id,
                    &request.shard_indices,
                    now(),
                    feedback.rtt,
                )?;
            state
                .telemetry
                .requested
                .fetch_add(count, Ordering::Relaxed);
        }
        Ok(())
    };
    // V1 keeps its established repair-write scheduling. Only negotiated v2
    // progress has this independent liveness bound; media ages never change.
    let result = if let Some(deadline) = until_ms {
        tokio::time::timeout(Duration::from_millis(deadline.saturating_sub(now())), write)
            .await
            .unwrap_or_else(|_| Err("FXLink feedback write deadline".into()))
    } else {
        write.await
    };
    (send, pacing, result)
}

fn remember_deadline(
    deadlines: &mut std::collections::BTreeMap<u64, u64>,
    record: u64,
    delivered: u64,
    limit: usize,
    deadline: u64,
) -> Result<(), String> {
    if record < delivered || deadlines.contains_key(&record) {
        return Ok(());
    }
    if deadlines.len() >= limit {
        return Err("FXLink deadline metadata limit".into());
    }
    deadlines.insert(record, deadline);
    Ok(())
}
#[cfg(test)]
use crate::transport_tls_fixture as test_tls;
#[cfg(test)]
pub(super) mod review_tests {
    use super::*;
    use crate::fxlink::{Encoder, Listener, Provider, Request, Source};
    use futures_util::future::BoxFuture;
    use std::sync::{Condvar, Mutex as StdMutex, OnceLock, atomic::AtomicBool};
    #[test]
    fn gap_age_uses_earliest_successor_without_borrowing_predecessor_age() {
        let fence = DiscardFence::default();
        let mut deadlines = std::collections::BTreeMap::from([(0, 1000), (3, 1400), (4, 1300)]);
        assert_eq!(fence.deadline(&deadlines, 1, 1600), 1300);
        assert_eq!(fence.deadline(&deadlines, 5, 1600), 1600);
        remember_deadline(&mut deadlines, 1, 0, 64, 1300).unwrap();
        remember_deadline(&mut deadlines, 1, 0, 64, 1700).unwrap();
        assert_eq!(deadlines[&1], 1300, "duplicate metadata renewed the age");
        let mut discarded = DiscardFence::default();
        discarded.observe(7, 1200);
        assert_eq!(discarded.deadline(&deadlines, 5, 1600), 1200);
        assert_eq!(discarded.deadline(&deadlines, 8, 1600), 1600);
    }
    pub(super) fn record() -> Bytes {
        let mut b = vec![7; 1000];
        b[..4].copy_from_slice(&996u32.to_be_bytes());
        Bytes::from(b)
    }
    #[tokio::test]
    async fn completed_blocking_batch_retains_all_hierarchical_reservations() {
        let settings = Settings {
            parity_shards: 0,
            ..Settings::default()
        };
        let global = Budget::new(1024 * 1024);
        let peer = global.child(512 * 1024);
        let budget = peer.child(256 * 1024);
        let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1024 * 1024)).unwrap();
        let encoded: Vec<_> = (0..3)
            .map(|_| encoder.encode(record(), 0).unwrap())
            .collect();
        let mut receiver = Receiver::new(settings, 0, 1, budget.clone()).unwrap();
        assert!(
            receiver
                .ingest(&encoded[1].datagrams[0], 0)
                .unwrap()
                .is_empty()
        );
        assert!(
            receiver
                .ingest(&encoded[2].datagrams[0], 0)
                .unwrap()
                .is_empty()
        );
        let packet = encoded[0].datagrams[0].clone();
        let job = tokio::task::spawn_blocking(move || receiver.ingest_charged(&packet, 0));
        while !job.is_finished() {
            tokio::task::yield_now().await;
        }
        for level in [&global, &peer, &budget] {
            assert!(
                level.used() >= 3000,
                "completed bytes lost their reservation before task-result consumption"
            );
            assert!(level.reserve(level.limit() - 2999).is_err());
        }
        let completed = job.await.unwrap().unwrap();
        assert_eq!(completed.len(), 3);
        let state = State::new(CancellationToken::new());
        for record in completed {
            state.queue.lock().unwrap().push_back(Charged {
                bytes: record.bytes,
                deadline_ms: record.deadline_ms,
                _guard: record.reservation,
            });
        }
        for level in [&global, &peer, &budget] {
            assert!(level.used() >= 3000);
        }
        state.retire();
        assert_eq!(budget.used(), 0);
        assert_eq!(peer.used(), 0);
        assert_eq!(global.used(), 0);
    }
    // Catches either production constructor resetting the allowance by connection,
    // profile, pool, listener port or direction. Reservations allocate no payload.
    #[tokio::test]
    async fn production_peer_budget_spans_profiles_pools_listeners_and_reconnect() {
        let _serial = super::super::transport::test_lock().lock().await;
        struct Pending;
        impl Provider for Pending {
            fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
                Box::pin(async {
                    Ok(Source {
                        pending_records: None,
                        records: Box::pin(futures_util::stream::pending()),
                        current: Arc::new(|| true),
                        cancelled: CancellationToken::new(),
                    })
                })
            }
        }
        let baseline = global().used();
        let cert = test_tls::Certificates::new();
        let mut servers = Vec::new();
        let mut urls = Vec::new();
        for _ in 0..2 {
            let listener = Listener::bind(
                "127.0.0.1:0".parse().unwrap(),
                &cert.cert,
                &cert.key,
                Settings::default(),
            )
            .unwrap();
            urls.push(format!("fxlink://{}/live", listener.address()));
            let stop = CancellationToken::new();
            servers.push((
                stop.clone(),
                tokio::spawn(listener.serve(Arc::new(Pending), stop)),
            ));
        }
        let pools = [ClientPool::default(), ClientPool::default()];
        let mut subs = Vec::new();
        for (pool, listener, alternate) in
            [(0, 0, false), (0, 0, true), (1, 0, false), (1, 1, false)]
        {
            let settings = if alternate {
                Settings {
                    latency_ms: 1500,
                    parity_shards: 2,
                    ..Settings::default()
                }
            } else {
                Settings::default()
            };
            subs.push(
                pools[pool]
                    .subscribe(
                        &urls[listener],
                        if pool == 0 { "peer" } else { "alias" },
                        "token",
                        Some(&cert.ca),
                        settings,
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap(),
            );
        }
        assert_ne!(subs[0].connection_id(), subs[1].connection_id());
        let budgets: Vec<_> = subs
            .iter()
            .map(|s| {
                s.state
                    .telemetry
                    .budget
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .clone()
            })
            .collect();
        let mib = 1024 * 1024;
        let mut held: Vec<_> = budgets[..3]
            .iter()
            .map(|b| b.reserve(24 * mib).unwrap())
            .collect();
        // Four connections in each direction cost 32 MiB in queues, plus bounded
        // metadata. The fourth 24 MiB charge must fail at the shared 128 MiB cap.
        let over_cap = budgets[3].reserve(24 * mib);
        let rejected = over_cap.is_err();
        drop(over_cap);
        // Leave room for the charged pending-shard index on each subscription.
        held.push(budgets[3].reserve(22 * mib).unwrap());
        drop(budgets);
        drop(subs);
        for pool in &pools {
            pool.close().await;
        }
        for (stop, task) in servers {
            stop.cancel();
            task.await.unwrap().unwrap();
        }
        assert_eq!(global().used(), baseline + 94 * mib);
        assert!(
            rejected,
            "production connections reset the aggregate 128 MiB peer allowance"
        );

        // All old connection owners have joined, but their outstanding charges
        // alone must keep the same peer ancestor alive during replacement.
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &cert.cert,
            &cert.key,
            Settings::default(),
        )
        .unwrap();
        let url = format!("fxlink://{}/live", listener.address());
        let stop = CancellationToken::new();
        let server = tokio::spawn(listener.serve(Arc::new(Pending), stop.clone()));
        let pool = ClientPool::default();
        let sub = pool
            .subscribe(
                &url,
                "new-alias",
                "token",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let budget = sub
            .state
            .telemetry
            .budget
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        assert!(
            // 94 MiB retained + 8 MiB connection queues + 26 MiB already
            // reaches the peer ceiling before any subscription metadata.
            budget.reserve(26 * mib).is_err(),
            "reconnect forgot still-live charges"
        );
        let unrelated = peer_budget("127.0.0.2".parse().unwrap()).unwrap();
        let independent = unrelated.reserve(128 * mib).unwrap();
        let pressure = budget.reserve(24 * mib).unwrap();
        let before_failed_setup = global().used();
        let reused = pool
            .subscribe(
                &url,
                "new-alias",
                "token",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await;
        let reused_rejected = reused.is_err();
        drop(reused);
        let replacement = ClientPool::default();
        assert!(
            replacement
                .subscribe(
                    &url,
                    "another-alias",
                    "token",
                    Some(&cert.ca),
                    Settings::default(),
                    CancellationToken::new(),
                )
                .await
                .is_err(),
            "new connections must not bypass a full peer budget"
        );
        if reused_rejected {
            assert_eq!(
                global().used(),
                before_failed_setup,
                "failed setup leaked charges"
            );
        }
        drop(pressure);
        let overlapping = replacement
            .subscribe(
                &url,
                "another-alias",
                "token",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        drop(overlapping);
        replacement.close().await;
        drop(independent);
        drop(unrelated);
        drop(held);
        assert!(budget.reserve(25 * mib).is_ok());
        drop(budget);
        drop(sub);
        pool.close().await;
        stop.cancel();
        server.await.unwrap().unwrap();
        assert_eq!(global().used(), baseline);
        assert!(
            reused_rejected,
            "pooled setup scratch bypassed the peer allowance"
        );
    }
    #[tokio::test]
    async fn peer_budget_charges_known_ip_during_pending_tls_setup() {
        let _serial = super::super::transport::test_lock().lock().await;
        let cert = test_tls::Certificates::new();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
        let peer = peer_budget("127.0.0.1".parse().unwrap()).unwrap();
        let baseline = peer.used();
        let pool = Arc::new(ClientPool::default());
        let cancel = CancellationToken::new();
        let attempt = {
            let pool = pool.clone();
            let cancel = cancel.clone();
            let ca = cert.ca.clone();
            tokio::spawn(async move {
                pool.subscribe(
                    &url,
                    "peer",
                    "token",
                    Some(&ca),
                    Settings::default(),
                    cancel,
                )
                .await
            })
        };
        let mut packet = [0; 1500];
        tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut packet))
            .await
            .unwrap()
            .unwrap();
        let during_tls = peer.used();
        cancel.cancel();
        assert!(attempt.await.unwrap().is_err());
        pool.close().await;
        assert_eq!(
            peer.used(),
            baseline,
            "cancelled TLS setup leaked peer charges"
        );
        assert_eq!(
            during_tls - baseline,
            8 * 1024 * 1024,
            "known-IP TLS scratch escaped the peer ancestor"
        );
    }
    struct Gate {
        entered: AtomicBool,
        allowed: AtomicBool,
        wake: tokio::sync::Notify,
    }
    static GATE: OnceLock<StdMutex<Option<Arc<Gate>>>> = OnceLock::new();
    pub(super) struct GateGuard(Arc<Gate>);
    impl GateGuard {
        pub(super) fn new() -> Self {
            let gate = Arc::new(Gate {
                entered: AtomicBool::new(false),
                allowed: AtomicBool::new(false),
                wake: tokio::sync::Notify::new(),
            });
            *GATE.get_or_init(|| StdMutex::new(None)).lock().unwrap() = Some(gate.clone());
            Self(gate)
        }
    }
    impl Drop for GateGuard {
        fn drop(&mut self) {
            *GATE.get().unwrap().lock().unwrap() = None;
            self.0.allowed.store(true, Ordering::Release);
            self.0.wake.notify_waiters();
        }
    }
    pub(super) async fn before_acquire() {
        let gate = GATE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap()
            .clone();
        if let Some(gate) = gate {
            gate.entered.store(true, Ordering::Release);
            loop {
                let notified = gate.wake.notified();
                if gate.allowed.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        }
    }
    // A bounded CPU-contention fixture releases each short job at a due
    // repair poll. It stops independently after 650 ms, even if polling fails.
    struct Cadence {
        generation: StdMutex<(u64, Option<Instant>)>,
        wake: Condvar,
        skipped: std::sync::atomic::AtomicUsize,
    }
    impl Cadence {
        fn pause(&self) {
            let mut state = self.generation.lock().unwrap();
            let cutoff = *state
                .1
                .get_or_insert_with(|| Instant::now() + Duration::from_millis(650));
            let before = state.0;
            while state.0 == before && Instant::now() < cutoff {
                state = self
                    .wake
                    .wait_timeout(state, cutoff.saturating_duration_since(Instant::now()))
                    .unwrap()
                    .0;
            }
        }
    }
    static CADENCE: OnceLock<StdMutex<Option<Arc<Cadence>>>> = OnceLock::new();
    pub(in crate::fxlink) struct DecodeCadence(Arc<Cadence>);
    impl DecodeCadence {
        pub(in crate::fxlink) fn new() -> Self {
            let cadence = Arc::new(Cadence {
                generation: StdMutex::new((0, None)),
                wake: Condvar::new(),
                skipped: std::sync::atomic::AtomicUsize::new(0),
            });
            *CADENCE.get_or_init(|| StdMutex::new(None)).lock().unwrap() = Some(cadence.clone());
            Self(cadence)
        }
        pub(in crate::fxlink) fn skipped_polls(&self) -> usize {
            self.0.skipped.load(Ordering::Relaxed)
        }
    }
    impl Drop for DecodeCadence {
        fn drop(&mut self) {
            *CADENCE.get().unwrap().lock().unwrap() = None;
            let mut state = self.0.generation.lock().unwrap();
            state.0 += 1;
            state.1 = Some(Instant::now());
            self.0.wake.notify_all();
        }
    }
    pub(super) fn repair_poll(available: bool) {
        if available {
            return;
        }
        let cadence = CADENCE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap()
            .clone();
        if let Some(cadence) = cadence {
            cadence.skipped.fetch_add(1, Ordering::Relaxed);
            cadence.generation.lock().unwrap().0 += 1;
            cadence.wake.notify_all();
        }
    }
    pub(super) struct Pause {
        entered: AtomicBool,
        released: StdMutex<bool>,
        wake: Condvar,
    }
    static PROGRESS_HELD: AtomicBool = AtomicBool::new(false);
    static PROGRESS_ENTERED: AtomicBool = AtomicBool::new(false);
    pub(in crate::fxlink) struct ProgressGuard;
    impl ProgressGuard {
        pub(in crate::fxlink) fn new() -> Self {
            PROGRESS_ENTERED.store(false, Ordering::Release);
            PROGRESS_HELD.store(true, Ordering::Release);
            Self
        }
        pub(in crate::fxlink) fn entered(&self) -> bool {
            PROGRESS_ENTERED.load(Ordering::Acquire)
        }
    }
    impl Drop for ProgressGuard {
        fn drop(&mut self) {
            PROGRESS_HELD.store(false, Ordering::Release);
        }
    }
    pub(super) async fn hold_progress() {
        if PROGRESS_HELD.load(Ordering::Acquire) {
            PROGRESS_ENTERED.store(true, Ordering::Release);
            while PROGRESS_HELD.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
    static PAUSE: OnceLock<StdMutex<Option<Arc<Pause>>>> = OnceLock::new();
    pub(in crate::fxlink) struct PauseGuard(Arc<Pause>);
    impl PauseGuard {
        pub(in crate::fxlink) fn new() -> Self {
            let p = Arc::new(Pause {
                entered: AtomicBool::new(false),
                released: StdMutex::new(false),
                wake: Condvar::new(),
            });
            *PAUSE.get_or_init(|| StdMutex::new(None)).lock().unwrap() = Some(p.clone());
            Self(p)
        }
        pub(in crate::fxlink) fn entered(&self) -> bool {
            self.0.entered.load(Ordering::Acquire)
        }
    }
    impl Drop for PauseGuard {
        fn drop(&mut self) {
            *PAUSE.get().unwrap().lock().unwrap() = None;
            *self.0.released.lock().unwrap() = true;
            self.0.wake.notify_all();
        }
    }
    pub(super) fn pause_decode() {
        let cadence = CADENCE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap()
            .clone();
        if let Some(cadence) = cadence {
            cadence.pause();
        }
        let pause = PAUSE
            .get_or_init(|| StdMutex::new(None))
            .lock()
            .unwrap()
            .clone();
        if let Some(p) = pause {
            p.entered.store(true, Ordering::Release);
            let mut ready = p.released.lock().unwrap();
            while !*ready {
                ready = p.wake.wait(ready).unwrap();
            }
        }
    }
    pub(super) struct Feed {
        pub(super) records: StdMutex<Option<mpsc::Receiver<Result<Bytes, String>>>>,
        pub(super) alive: Arc<AtomicBool>,
    }
    impl Provider for Feed {
        fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
            let rx = self.records.lock().unwrap().take().unwrap();
            let alive = self.alive.clone();
            Box::pin(async move {
                Ok(Source {
                    pending_records: None,
                    records: Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
                        rx.recv().await.map(|record| (record, rx))
                    })),
                    current: Arc::new(move || alive.load(Ordering::Acquire)),
                    cancelled: CancellationToken::new(),
                })
            })
        }
    }
    pub(super) async fn until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_millis(350), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("state change must occur before releasing decode barrier");
    }
    #[tokio::test]
    async fn remote_retirement_interrupts_decode_permit_wait_and_started_job() {
        retirement_during_decode(5).await;
    }
    #[tokio::test]
    async fn v8_remote_retirement_interrupts_decode_permit_wait_and_started_job() {
        retirement_during_decode(8).await;
    }
    async fn retirement_during_decode(version: u8) {
        let _serial = super::super::transport::test_lock().lock().await;
        let cert = test_tls::Certificates::new();
        for (started_job, revoke) in [(true, true), (false, true), (true, false), (false, false)] {
            let settings = Settings {
                latency_ms: if revoke { 1500 } else { 500 },
                ..Settings::default()
            };
            let features = Features {
                geometry_hints: false,
                trimmed_records: version == 8,
                repair_parity: version == 8,
                bootstrap_repair: version == 8,
            };
            let listener = Listener::bind_with_features(
                "127.0.0.1:0".parse().unwrap(),
                &cert.cert,
                &cert.key,
                settings.clone(),
                features,
            )
            .unwrap();
            let url = format!("fxlink://{}/live", listener.address());
            let (tx, rx) = mpsc::channel(2);
            let alive = Arc::new(AtomicBool::new(true));
            let stop = CancellationToken::new();
            let server = tokio::spawn(listener.serve(
                Arc::new(Feed {
                    records: StdMutex::new(Some(rx)),
                    alive: alive.clone(),
                }),
                stop.clone(),
            ));
            let pool = ClientPool::with_features(features);
            let mut sub = pool
                .subscribe(
                    &url,
                    "peer",
                    "token",
                    Some(&cert.ca),
                    settings,
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            tx.send(Ok(record())).await.unwrap();
            until(|| sub.state.queue.lock().unwrap().len() == 1).await;
            let initial = sub.state.telemetry.data.load(Ordering::Relaxed);
            let mut held = None;
            let gate = if started_job {
                None
            } else {
                Some(GateGuard::new())
            };
            let pause = if started_job {
                Some(PauseGuard::new())
            } else {
                None
            };
            tx.send(Ok(record())).await.unwrap();
            if let Some(p) = &pause {
                until(|| p.0.entered.load(Ordering::Acquire)).await;
            } else {
                until(|| gate.as_ref().unwrap().0.entered.load(Ordering::Acquire)).await;
                held = Some(fec_slots().acquire_many_owned(4).await.unwrap());
                drop(gate);
                until(|| sub.state.telemetry.data.load(Ordering::Relaxed) > initial).await;
            }
            if revoke {
                alive.store(false, Ordering::Release);
                until(|| !sub.current()).await;
            } else {
                tokio::time::timeout(Duration::from_millis(900), async {
                    while sub.current() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .expect("deadline must retire before releasing decode barrier");
            }
            assert!(sub.connection.close_reason().is_none());
            assert!(sub.recv().await.is_err());
            assert!(sub.state.queue.lock().unwrap().is_empty());
            let telemetry = sub.telemetry();
            drop(pause);
            drop(held);
            drop(sub);
            pool.close().await;
            stop.cancel();
            server.await.unwrap().unwrap();
            assert_eq!(telemetry.snapshot()["state_bytes"], 0);
        }
    }
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod startup_tests;

#[cfg(test)]
#[path = "first_error_tests.rs"]
mod first_error_tests;

#[cfg(test)]
#[path = "announcement_counter_tests.rs"]
mod announcement_counter_tests;

#[cfg(test)]
#[path = "sibling_tests.rs"]
mod sibling_tests;

#[cfg(test)]
#[path = "rate_tests.rs"]
mod rate_tests;

#[cfg(test)]
mod retry_wake_tests {
    use super::*;

    // Break caught: cooldown wakes work, but the initial reordering grace
    // still waits for a coarse poll and spends the source-cache retry window.
    #[test]
    fn first_repair_wake_preserves_a_retry_after_feedback_loss() {
        use crate::fxlink::repair::{Allowance, Pacing, after_reordering, reordering_grace};
        use crate::fxlink::{Encoder, Receiver};
        fn record(id: u8) -> Bytes {
            let mut b = vec![id; 1000];
            b[..4].copy_from_slice(&996u32.to_be_bytes());
            Bytes::from(b)
        }
        let settings = Settings {
            latency_ms: 500,
            ..Settings::default()
        };
        let budget = Budget::new(1 << 20);
        let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
        let encoded = (0..12)
            .map(|id| {
                encoder
                    .encode(record(id), if id == 0 { 0 } else { 43 })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut receiver = Receiver::new(settings, 0, 1, budget.clone()).unwrap();
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        let mut allowance = Allowance::new(0, 500);
        for frame in &encoded {
            assert!(allowance.original(
                frame.created_ms,
                frame.created_ms,
                frame.datagrams[0].len()
            ));
        }
        // Leave head metadata absent until after the first feedback is lost.
        for frame in &encoded[1..] {
            assert!(
                receiver
                    .ingest(&frame.datagrams[0], 138)
                    .unwrap()
                    .is_empty()
            );
        }
        let grace = reordering_grace(500, 191);
        assert_eq!(grace, 56);
        let mut first_at = None;
        assert!(
            receiver
                .repair_requests_if(158, 191, 5, |b, s| after_reordering(
                    158,
                    138,
                    grace,
                    &mut first_at
                ) && pacing.eligible(b, s, 158))
                .is_empty()
        );
        let request_at = retry_wake_at(Some(&pacing), first_at, true, true, 0)
            .unwrap_or(208)
            .min(208);
        let first = receiver
            .repair_requests_if(request_at, 191, 5, |b, s| pacing.eligible(b, s, request_at));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].block_id, 0);
        pacing
            .sent(0, &first[0].shard_indices, request_at, 191)
            .unwrap();
        // Feedback never reaches source; later metadata must not renew age.
        receiver.announce(0, 1000, 329).unwrap();
        let retry_at = pacing.wake_at().unwrap();
        pacing.prune(retry_at, 0);
        let retry =
            receiver.repair_requests_if(retry_at, 191, 5, |b, s| pacing.eligible(b, s, retry_at));
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].shard_indices, [0]);
        let source_at = retry_at + 95;
        let output = if source_at + 5 < 500
            && allowance.repair(0, source_at, encoded[0].datagrams[0].len())
        {
            receiver
                .ingest(&encoded[0].datagrams[0], source_at + 95)
                .unwrap()
        } else {
            Vec::new()
        };
        assert_eq!(
            output.len(),
            12,
            "coarse first request left no source-cache retry window"
        );
        assert_eq!(first_at, Some(194));
        assert_eq!(request_at, 194);
        assert_eq!(retry_at, 390);
        assert_eq!(source_at, 485);
        assert_eq!(output, (0..12).map(record).collect::<Vec<_>>());
        assert_eq!(receiver.stats().expired_records, 0);
        drop(receiver);
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn first_repair_wake_combines_grace_cooldown_and_batch_floor() {
        use crate::fxlink::repair::{Pacing, after_reordering};
        let budget = Budget::new(1 << 20);
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        let mut first_at = None;
        assert!(!after_reordering(150, 138, 56, &mut first_at));
        assert!(!after_reordering(150, 145, 56, &mut first_at));
        assert_eq!(first_at, Some(194));
        pacing.sent(0, &[0], 100, 191).unwrap();
        assert_eq!(
            retry_wake_at(Some(&pacing), first_at, true, true, 0),
            Some(194)
        );
        assert_eq!(
            retry_wake_at(Some(&pacing), Some(400), true, true, 0),
            Some(296)
        );
        assert_eq!(
            retry_wake_at(Some(&pacing), first_at, true, true, 210),
            Some(210)
        );
        assert_eq!(retry_wake_at(Some(&pacing), first_at, false, true, 0), None);
        assert_eq!(retry_wake_at(Some(&pacing), first_at, true, false, 0), None);
        first_at = None;
        assert!(after_reordering(194, 138, 56, &mut first_at));
        assert_eq!(first_at, None);
        assert!(after_reordering(138, 138, 0, &mut first_at));
        assert_eq!(first_at, None);
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }

    // Break caught: a blocked receiver/writer spins on a past retry deadline,
    // or an urgent wake bypasses the existing actual-feedback spacing.
    #[test]
    fn retry_wake_respects_receiver_feedback_and_batch_spacing() {
        let budget = Budget::new(1 << 20);
        let mut pacing = super::super::repair::Pacing::new(budget.clone(), 64).unwrap();
        assert_eq!(retry_wake_at(Some(&pacing), None, true, true, 0), None);
        pacing.sent(0, &[0], 100, 191).unwrap();
        assert_eq!(retry_wake_at(Some(&pacing), None, true, true, 0), Some(296));
        assert_eq!(retry_wake_at(Some(&pacing), None, false, true, 0), None);
        assert_eq!(retry_wake_at(Some(&pacing), None, true, false, 0), None);
        assert_eq!(retry_wake_at(None, None, true, true, 0), None);
        assert_eq!(
            retry_wake_at(Some(&pacing), None, true, true, 310),
            Some(310)
        );
        // Ownership returns after the due time: allow immediate service,
        // rather than renewing either the cooldown or ordinary poll clock.
        pacing.prune(295, 0);
        assert_eq!(
            retry_wake_at(Some(&pacing), None, true, true, 280),
            Some(296)
        );
        pacing.prune(300, 1);
        assert_eq!(retry_wake_at(Some(&pacing), None, true, true, 0), None);
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
#[path = "source_age_tests.rs"]
mod source_age_tests;

#[cfg(test)]
#[path = "feedback_cancel_tests.rs"]
mod feedback_cancel_tests;

#[cfg(test)]
#[path = "early_probe_tests.rs"]
mod early_probe_tests;

#[cfg(test)]
#[path = "early_probe_policy_tests.rs"]
mod early_probe_policy_tests;

#[cfg(test)]
#[path = "blackout_geometry_tests.rs"]
mod blackout_geometry_tests;

#[cfg(test)]
#[path = "bootstrap_client_tests.rs"]
mod bootstrap_client_tests;

#[cfg(test)]
pub(super) use review_tests::ProgressGuard as BootstrapProgressGuard;

#[cfg(test)]
#[path = "bootstrap_geometry_tests.rs"]
mod bootstrap_geometry_tests;
#[cfg(test)]
#[path = "geometry/clock_tests.rs"]
mod geometry_clock_tests;
