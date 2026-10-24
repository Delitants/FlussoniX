use super::service_timing::{Phase, Selected};
use super::{
    EncodedRecord, Encoder, Provider, Request, Settings, Source,
    control::{self, Control},
    packet::{HEADER_LEN, Header},
    transport::*,
};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::VecDeque, net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
// Independent of Receiver::max_records: completed audio/video records remain repairable through their TTL.
const MAX_CACHED_RECORDS: usize = 1024;
enum Input {
    Reliable(Control),
    Datagram(super::feedback::DatagramRequest),
}
pub struct Listener {
    endpoint: quinn::Endpoint,
    settings: Settings,
    _incoming: super::Reservation,
    egress: Arc<AtomicU64>,
}
impl Listener {
    pub fn bind(
        address: SocketAddr,
        cert: &Path,
        key: &Path,
        settings: Settings,
    ) -> Result<Self, String> {
        Self::bind_with_features(address, cert, key, settings, super::Features::default())
    }
    pub fn bind_with_features(
        address: SocketAddr,
        cert: &Path,
        key: &Path,
        settings: Settings,
        features: super::Features,
    ) -> Result<Self, String> {
        settings.validate().map_err(|e| e.to_string())?;
        let mut tls =
            (*crate::rtsp::tls::server(cert, key).map_err(|_| "FXLink TLS material")?).clone();
        tls.alpn_protocols = control::protocols_with_features(features);
        tls.max_early_data_size = 0;
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
            .map_err(|_| "FXLink TLS 1.3 required")?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(transport_config());
        config
            .max_incoming(64)
            .incoming_buffer_size(64 * 1024)
            .incoming_buffer_size_total(1024 * 1024);
        let incoming = global()
            .reserve(2 * 1024 * 1024)
            .map_err(|e| e.to_string())?;
        let endpoint =
            quinn::Endpoint::server(config, address).map_err(|_| "FXLink listener bind")?;
        Ok(Self {
            endpoint,
            settings,
            _incoming: incoming,
            egress: Arc::new(AtomicU64::new(0)),
        })
    }
    /// Application datagram bytes accepted by QUIC, including parity and repairs.
    pub fn egress(&self) -> Arc<AtomicU64> {
        self.egress.clone()
    }
    pub fn address(&self) -> SocketAddr {
        self.endpoint.local_addr().expect("bound endpoint")
    }
    pub async fn serve(
        self,
        provider: Arc<dyn Provider>,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let slots = Arc::new(Semaphore::new(64));
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {biased;
            _=cancel.cancelled()=>break,
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            incoming=self.endpoint.accept()=>{let Some(incoming)=incoming else{break};let Ok(permit)=slots.clone().try_acquire_owned() else{incoming.refuse();continue};
            let settings=self.settings.clone();let provider=provider.clone();let cancel=cancel.child_token();let egress=self.egress.clone();
            tasks.spawn(async move {let _permit=permit;let Ok(budget)=peer_budget(incoming.remote_address().ip()) else{incoming.refuse();return};let Ok(_queues)=budget.reserve(QUEUE_BYTES) else{incoming.refuse();return};
            let connection=tokio::select!{_=cancel.cancelled()=>return,r=tokio::time::timeout(SETUP,incoming)=>match r{Ok(Ok(c))=>c,_=>return}};
            serve_peer(connection,provider,settings,budget,cancel,egress).await;
            });}
            }
        }
        self.endpoint.close(0u32.into(), b"shutdown");
        while tasks.join_next().await.is_some() {}
        self.endpoint.wait_idle().await;
        Ok(())
    }
}
fn transport_config() -> Arc<quinn::TransportConfig> {
    config()
}
async fn serve_peer(
    connection: quinn::Connection,
    provider: Arc<dyn Provider>,
    settings: Settings,
    budget: super::Budget,
    cancel: CancellationToken,
    egress: Arc<AtomicU64>,
) {
    let mut tasks = JoinSet::new();
    let routes = super::feedback::Routes::default();
    // V9 remains unadvertised until the explicit feature/negotiation wiring.
    let geometry = if control::version(&connection).ok() == Some(9) {
        super::geometry::send::Hub::new(&budget)
    } else {
        None
    };
    let budget = if let Some(geometry) = &geometry {
        geometry.scoped(&budget)
    } else {
        budget
    };
    // No media writer exists yet; control/TLS frames use separate queues.
    let empty_datagram_space = connection.datagram_send_buffer_space();
    loop {
        let hint_wake = geometry.as_ref().and_then(|hub| hub.wake_at(now()));
        tokio::select! {
        _=cancel.cancelled()=>break,
        _=async {geometry.as_ref().unwrap().changed.notified().await},if geometry.is_some()=>{
            dispatch_geometry(geometry.as_ref().unwrap(),&connection,&egress);
        },
        _=async {tokio::time::sleep(Duration::from_millis(hint_wake.unwrap().saturating_sub(now()))).await},if hint_wake.is_some()=>{
            dispatch_geometry(geometry.as_ref().unwrap(),&connection,&egress);
        },
        Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
        packet=connection.read_datagram()=>{let Ok(packet)=packet else{break};routes.receive(packet,now());},
        streams=connection.accept_bi()=>{let Ok((send,recv))=streams else{break};if tasks.len()>=16{drop(send);drop(recv);continue}
        // Stream IDs are unique for this connection, so admitted IDs cannot alias concurrent subscriptions.
        let stream_id=send.id().index();
        let c=connection.clone();let p=provider.clone();let s=settings.clone();let b=budget.child(SUB_BYTES);let stop=cancel.child_token();let egress=egress.clone();
        let routes=routes.clone();
        let geometry=geometry.clone();
        tasks.spawn(async move{let _=serve_subscription(c,p,s,b,stop,(send,recv),egress,routes,empty_datagram_space,geometry).await;stream_id});
        }
        }
    }
    cancel.cancel();
    if let Some(geometry) = &geometry {
        geometry.disable();
    }
    connection.close(0u32.into(), b"closed");
    while tasks.join_next().await.is_some() {}
}
fn dispatch_geometry(
    hub: &super::geometry::send::Hub,
    connection: &quinn::Connection,
    egress: &AtomicU64,
) {
    let result = hub.attempt(connection, now());
    tracing::trace!(attempted_modeled_bytes=result.attempted_modeled_bytes,hints=?hub.diagnostics(),"FXLink local source hint diagnostics");
    if result.sent {
        egress.fetch_add(result.application_bytes as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "geometry/endpoint_tests.rs"]
pub(super) mod geometry_endpoint_tests;
#[allow(clippy::too_many_arguments)]
async fn serve_subscription(
    connection: quinn::Connection,
    provider: Arc<dyn Provider>,
    settings: Settings,
    budget: super::Budget,
    cancel: CancellationToken,
    (mut send, mut recv): (quinn::SendStream, quinn::RecvStream),
    egress: Arc<AtomicU64>,
    routes: super::feedback::Routes,
    empty_datagram_space: usize,
    hub: Option<Arc<super::geometry::send::Hub>>,
) -> Result<(), String> {
    let result = async {
        let _metadata = budget.reserve(65536).map_err(|e| e.to_string())?;
        let request = tokio::select! {
            _ = cancel.cancelled() => return Err("FXLink cancelled".into()),
            result = tokio::time::timeout(SETUP, control::read(&mut recv)) => result.map_err(|_| "FXLink setup timeout")??,
        };
        let Control::Subscribe { version, id, generation, profile, name, peer_key, token } = request else {
            return Err("FXLink expected subscription".into());
        };
        if version != control::version(&connection)? || id != send.id().index()
            || generation != 1 || name.is_empty() || name.len() > 1024 || peer_key.len() > 4096 || token.len() > 4096 {
            return Err("FXLink incompatible subscription".into());
        }
        let settings=control::negotiate(profile,&settings)?;
        let mut premature=[0u8;1];
        let source = tokio::select! {
            biased;
            _ = send.stopped() => return Err("FXLink admission requester stopped".into()),
            _ = recv.read(&mut premature) => return Err("FXLink admission request stream closed".into()),
            _ = cancel.cancelled() => return Err("FXLink cancelled".into()),
            result = tokio::time::timeout(ADMISSION, provider.subscribe(Request { name, peer_key, token }, connection.remote_address())) => {
                result.map_err(|_| "FXLink admission timeout")?
            },
        };
        let mut source = match source {
            Ok(source) => source,
            Err(status) => {
                let _ = tokio::time::timeout(SETUP, control::write(&mut send, &Control::Denied { status })).await;
                return Err("FXLink admission denied".into());
            }
        };
        let mut feedback = if version>=3 {Some(routes.admit(id,version,&budget)?)} else {None};
        let mut pending=if version>=5 && settings.compact_width(12).is_some() {super::short_fec::Pending::new(&budget).ok()} else {None};
        check_source(&source, &cancel)?;
        mtu(&connection, &settings)?;
        let demand=super::demand::register(&connection,&source,&cancel)?;
        source_write(&mut send, &Control::Accepted, &mut source, &cancel, now()+settings.latency_ms).await?;
        let mut probe=super::service_trace::Probe::new(connection.stable_id(),id,generation,&source);
        // The handle is declared after cache, so it retires before cached media
        // drops. A separate per-record guard fences an early in-flight exit.
        let mut cache: VecDeque<super::repair_cache::CachedRecord> = VecDeque::new();
        let geometry=hub.as_ref().and_then(|hub| {
            let current=source.current.clone();let source_cancel=source.cancelled.clone();
            hub.register(id,generation,settings.clone(),&budget,Arc::new(move||!source_cancel.is_cancelled()&&current()),cancel.clone())
        });
        let mut encoder = Encoder::new(settings.clone(), id, generation, budget.clone()).map_err(|e| e.to_string())?.with_capabilities(super::Capabilities::for_version(version));
        let mut allowance = super::repair::Allowance::new(now(), settings.latency_ms);
        let mut bootstrap_gate = super::bootstrap_source::Gate::default();
        let (mut data_bytes, mut parity_bytes, mut repair_bytes) = (0u64, 0u64, 0u64);
        let mut report_at = now();
        let mut parity_sample_at = now();
        let mut parity_gate = super::parity::Gate::default();
        let mut parity_acks = None;
        let mut parity_allowed = true;
        let mut skipped_parity_bytes = 0u64;
        let mut credit = super::flow::Window::default();
        let mut drain_wake = super::drain_wake::DrainWake::default();
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        let read = control::next(recv);
        tokio::pin!(read);
        loop {
            check_source(&source, &cancel)?;
            let time = now();
            if let Some(probe)=&mut probe {probe.observe(time);}
            let credit_allows=version==1 || credit.can_admit(encoder.next_record_id(),settings.max_records,time,settings.latency_ms)?;
            while cache.front().is_some_and(|record| time.saturating_sub(record.created_ms) >= settings.latency_ms) {
                cache.pop_front();
            }
            if version>=5 && time>=parity_sample_at {
                parity_sample_at=time.saturating_add(100);
                let stats=connection.stats();
                let path=stats.path;
                // Preserve known sub-millisecond baselines; zero means unknown.
                let millis=|duration:Duration| if duration.is_zero() {0} else {duration.as_millis().max(1) as u64};
                let rtt_ms=millis(path.rtt);
                let min_rtt_ms=millis(path.min_rtt);
                let fresh=parity_acks.is_none_or(|acks|stats.frame_rx.acks>acks);
                parity_acks=Some(stats.frame_rx.acks);
                // No newly observed ACK traffic is an unknown sample, not
                // fresh evidence that a cached RTT persisted between ticks.
                let allowed=if fresh {
                    parity_gate.observe(time,rtt_ms,min_rtt_ms,settings.latency_ms)
                } else {
                    parity_gate.observe(time,0,0,settings.latency_ms)
                };
                if allowed!=parity_allowed {
                    tracing::debug!(subscription=id,generation,parity_allowed=allowed,rtt_ms,min_rtt_ms,"FXLink compact parity protection changed");
                }
                parity_allowed=allowed;
            }
            if let Some(pending)=pending.as_mut() {
                if parity_allowed {
                    mtu(&connection,&settings)?;
                    let admitted=pending.flush(super::short_fec::Send {connection:&connection,empty_space:empty_datagram_space,source:&source,cancel:&cancel,allowance:&mut allowance,egress:&egress})?;
                    if let Some(geometry)=&geometry {geometry.original_admitted(admitted as usize);}
                    parity_bytes=parity_bytes.saturating_add(admitted);
                } else {
                    skipped_parity_bytes=skipped_parity_bytes.saturating_add(pending.discard());
                }
            }
            // V5 compact records can fit many ages behind the byte FIFO.
            // Sample after optional parity: start another source record only
            // near empty, without holding a record or renewing an existing age.
            // This is advisory across siblings, not an in-flight reservation.
            let queued_media=empty_datagram_space.saturating_sub(connection.datagram_send_buffer_space());
            let queue_allows=version<5 || queued_media<=HEADER_LEN+settings.shard_size+128;
            let retry_at=drain_wake.observe(credit_allows && !queue_allows,now());
            let can_admit=credit_allows && queue_allows;
            if let Some(demand)=&demand {demand.set_admission_paused(!can_admit);}
            // Fair polling keeps ready originals and record credit moving
            // during a continuously ready, bounded repair batch. The loop and
            // every send still fence source validity and cancellation.
            if let Some(probe)=&mut probe {probe.wait(credit_allows,queue_allows,&source,retry_at);}
            tokio::select! {
                _ = cancel.cancelled() => {if let Some(probe)=&mut probe {probe.wake(Selected::Cancel);}return Err("FXLink cancelled".into())},
                _ = source.cancelled.cancelled() => {if let Some(probe)=&mut probe {probe.wake(Selected::SourceCancel);}return Err("FXLink source retired".into())},
                _ = async { tokio::time::sleep(Duration::from_millis(retry_at.unwrap().saturating_sub(now()))).await }, if retry_at.is_some() => {if let Some(probe)=&mut probe {probe.wake(Selected::DrainRetry);}},
                _ = tick.tick() => {
                    if let Some(probe)=&mut probe {probe.wake(Selected::Tick);}
                    if now() >= report_at {
                        report_at = now()+100;
                        if tracing::enabled!(tracing::Level::TRACE) {
                            let congestion=connection.congestion_state().into_any();
                            let bbr=congestion.downcast_ref::<quinn::congestion::Bbr>();
                            tracing::trace!(subscription=id,generation,quic=?connection.stats(),?bbr,sent_records=encoder.next_record_id(),?credit,credit_allows,queue_allows,can_admit,queued_media,data_bytes,parity_bytes,repair_bytes,parity_allowed,skipped_parity_bytes,send_buffer_free=connection.datagram_send_buffer_space(),cached_records=cache.len(),"FXLink sender QUIC state");
                        }
                        source_write(&mut send, &Control::Counters { data:data_bytes, parity:parity_bytes, repair:repair_bytes }, &mut source, &cancel, now()+100).await?;
                    }
                },
                message = async {
                    tokio::select! {
                        message=&mut read => {let (returned,message)=message;(Some(returned),message.map(Input::Reliable))},
                        message=async {feedback.as_mut().unwrap().next(&settings).await}, if feedback.is_some() => (None,Ok(Input::Datagram(message))),
                    }
                } => {
                    if let Some(probe)=&mut probe {probe.wake(Selected::Feedback);}
                    let (returned, message) = message;
                    let parity_repairs = matches!(version, 7..=9) && returned.is_none();
                    if let Some(returned)=returned {read.set(control::next(returned));}
                    let message = message?;
                    if let Input::Reliable(Control::Progress {next}) = message {
                        if version<2 {return Err("FXLink unnegotiated progress".into());}
                        credit.progress(next,encoder.next_record_id())?;
                        if let Some(geometry)=&geometry {geometry.consumed(next);}
                        continue;
                    }
                    if let Some(probe)=&mut probe {probe.mark(Phase::Repair);}
                    let (block, shards, bootstrap_limit) = match message {
                        Input::Reliable(Control::Repair {block, shards}) => (block, shards, None),
                        Input::Datagram(super::feedback::DatagramRequest::Ordinary(request)) => (request.block_id, request.shard_indices, None),
                        Input::Datagram(super::feedback::DatagramRequest::Bootstrap {block_id, reply_limit}) => {
                            if !matches!(version,8|9) || !bootstrap_gate.admit(now()) {continue;}
                            #[cfg(test)] bootstrap_endpoint_tests::request(block_id,reply_limit);
                            (block_id, Vec::new(), Some(reply_limit))
                        }
                        _ => return Err("FXLink invalid repair".into()),
                    };
                    #[cfg(test)] let _work_observation = ordinary_work_probe::request(bootstrap_limit.is_none());
                    let requested = bootstrap_limit.map_or(shards.len(), usize::from);
                    if shards.len() > settings.max_repair_shards { return Err("FXLink repair limit".into()); }
                    let request_mask = super::repair_cache::Mask::new(&shards);
                    if bootstrap_limit.is_none() && request_mask.is_empty() {continue;}
                    let record_id = block/super::MAX_BLOCKS_PER_RECORD as u64;
                    // Progress may overtake old feedback. Never charge a repair
                    // for a record the subscriber has already consumed. The
                    // frontier itself remains repairable, including record0/v1.
                    if credit.has_consumed(record_id) {
                        tracing::trace!(record=record_id,block,"FXLink consumed record repair skipped");
                        continue;
                    }
                    // The deterministic block ID selects one retained record, avoiding full-cache scans.
                    if let Some(record) = cache.iter().find(|record| record.record_id == record_id) {
                        let at=now();
                        let age_ms=at.saturating_sub(record.created_ms);
                        // The request's receiver deadline already budgets the
                        // return trip. Another full RTT here can reject useful
                        // asymmetric/delayed-feedback repairs. Keep the original
                        // cache expiry and enqueue deadline; this margin is not
                        // a guarantee of queue admission or network arrival.
                        let expired=if bootstrap_limit.is_some() {
                            !super::bootstrap_source::age_valid(record.created_ms,at,settings.latency_ms)
                        } else {age_ms.saturating_add(5) >= settings.latency_ms};
                        if expired {
                            tracing::trace!(block,age_ms,requested,"FXLink repair rejected by sender age");
                            continue;
                        }
                        let mut sent=0usize;
                        let mut limited=0usize;
                        let mut parity_selected=false;
                        let mut bootstrap_packets=bootstrap_limit.map(|limit|super::bootstrap_source::packets(record,block,limit));
                        let mut ordinary_packets=record.ordinary_packets(block,&request_mask).iter();
                        while let Some(packet) = if let Some(packets)=&mut bootstrap_packets {
                            packets.next().transpose().map_err(|e|e.to_string())?
                        } else {ordinary_packets.next()} {
                            #[cfg(test)] if bootstrap_limit.is_none() { ordinary_work_probe::packet(); }
                            let header = Header::parse(packet).map_err(|e| e.to_string())?;
                            if bootstrap_limit.is_some() || (header.block_id == block && request_mask.contains(header.shard_index) && (header.shard_index < header.data_count || (parity_repairs && !parity_selected))) {
                                if header.shard_index >= header.data_count {parity_selected=true;}
                                if allowance.repair(record.created_ms, now(), packet.len()) {
                                    #[cfg(test)] tests::wait_repair_queue().await;
                                    if enqueue(&connection, packet, &mut source, &cancel, record.created_ms+settings.latency_ms, &settings, &egress, None).await? == Admission::Expired {
                                        check_source(&source,&cancel)?;
                                        if connection.close_reason().is_some() { return Err("FXLink datagram send failed".into()); }
                                        tracing::trace!(block,"FXLink expired optional repair skipped");
                                        break;
                                    }
                                    tracing::trace!(subscription=header.subscription,record=header.record_id,block,shard=header.shard_index,created_ms=record.created_ms,queued_at_ms=now(),deadline_ms=record.created_ms+settings.latency_ms,"FXLink repair packet queued");
                                    #[cfg(test)] if bootstrap_limit.is_some() { bootstrap_endpoint_tests::reply(block); }
                                    repair_bytes = repair_bytes.saturating_add(packet.len() as u64);
                                    sent+=1;
                                } else {limited+=1;if bootstrap_limit.is_some() {break;}}
                            }
                        }
                        let (original,repaired)=allowance.counters(record.created_ms,now());
                        tracing::trace!(block,age_ms,requested,sent,limited,original,repaired,send_buffer_free=connection.datagram_send_buffer_space(),"FXLink repair sender decision");
                    } else {
                        tracing::trace!(block,requested,"FXLink repair cache miss");
                    }
                },
                record = source.records.next(), if can_admit => {
                    if let Some(probe)=&mut probe {probe.wake(Selected::Record);}
                    let Some(record) = record else { return Err("FXLink source ended".into()); };
                    let record = record?;
                    if cache.len() >= MAX_CACHED_RECORDS { return Err("FXLink repair cache limit".into()); }
                    let len = record.len();
                    // Reserve the optional parity before assigning media age.
                    // Local capacity/budget pressure selects exact legacy bytes.
                    let mut compact_slot=pending.as_ref().and_then(|pending|settings.compact_width(len).and_then(|width|pending.reserve(HEADER_LEN+width,&budget)));
                    encoder=encoder.with_short_record_fec(compact_slot.is_some());
                    if let Some(probe)=&mut probe {probe.mark(Phase::Encode);}
                    let (returned, encoded) = encode_record(encoder, record, &budget, &settings, &mut source, &cancel, &mut send).await?;
                    encoder = returned;
                    check_source(&source, &cancel)?;
                    let publication=geometry.as_ref().map(super::geometry::send::SourceHandle::in_flight);
                    if let Some(geometry)=&geometry {geometry.encoded(encoded.record_id,len,encoded.created_ms);}
                    // CPU/FEC wait ended. Only already encoded originals
                    // count while their announcement/QUIC admission waits.
                    let mut ready=demand.as_ref().map(|demand|demand.ready_data());
                    let mut originals=settings.fragment_count(len);
                    let deadline = encoded.created_ms+settings.latency_ms;
                    tracing::trace!(subscription=id,generation,record=encoded.record_id,len,created_ms=encoded.created_ms,"FXLink record announcement queued");
                    if let Some(probe)=&mut probe {probe.mark(Phase::Announce);}
                    #[cfg(test)] let omit=bootstrap_endpoint_tests::omit_announcement(encoded.record_id);
                    #[cfg(not(test))] let omit=false;
                    if !omit { source_write(&mut send, &Control::Announce { record:encoded.record_id, len }, &mut source, &cancel, deadline).await?; }
                    let mut retaining=super::repair_cache::PendingRecord::new(encoded,&settings,id,generation,super::Capabilities::for_version(version)).map_err(|e|e.to_string())?;
                    let (encoded,validation)=retaining.parts();
                    for packet in &encoded.datagrams {
                        let header=Header::parse(packet).map_err(|e|e.to_string())?;
                        validation.observe(&header).map_err(|e|e.to_string())?;
                        if header.layout == super::Layout::Compact && header.shard_index==1 {
                            // Preserve compact systematic bytes even when the
                            // optional copy is omitted. Falling back to padded
                            // legacy packets would increase congestion.
                            if parity_allowed {
                                pending.as_mut().unwrap().push(compact_slot.take().ok_or("FXLink missing compact parity reservation")?,packet.clone(),encoded.created_ms,deadline)?;
                            } else {
                                skipped_parity_bytes=skipped_parity_bytes.saturating_add(packet.len() as u64);
                            }
                            continue;
                        }
                        if let Some(probe)=&mut probe {probe.mark(if header.shard_index<header.data_count {Phase::Data}else{Phase::Parity});}
                        if enqueue(&connection, packet, &mut source, &cancel, deadline, &settings, &egress, geometry.as_ref()).await? == Admission::Expired {
                            return Err("FXLink send deadline".into());
                        }
                        if !allowance.original(encoded.created_ms,now(),packet.len()) { return Err("FXLink original allowance expired".into()); }
                        tracing::trace!(subscription=header.subscription,record=header.record_id,block=header.block_id,shard=header.shard_index,created_ms=encoded.created_ms,queued_at_ms=now(),"FXLink original packet queued");
                        if header.shard_index < header.data_count {
                            data_bytes = data_bytes.saturating_add(packet.len() as u64);
                            originals-=1;
                            // Tail parity alone is not pending original data.
                            if originals==0 {drop(ready.take());}
                        }
                        else { parity_bytes = parity_bytes.saturating_add(packet.len() as u64); }
                    }
                    cache.push_back(retaining.finish().map_err(|e|e.to_string())?);
                    if let Some(publication)=publication {publication.commit();}
                },
            }
        }
    }.await;
    if let Err(error) = &result {
        tracing::debug!(%error, "FXLink send subscription ended");
    }
    let _ = send.reset(1u32.into());
    result
}
async fn encode_record(
    mut encoder: Encoder,
    record: bytes::Bytes,
    budget: &super::Budget,
    settings: &Settings,
    source: &mut Source,
    cancel: &CancellationToken,
    send: &mut quinn::SendStream,
) -> Result<(Encoder, EncodedRecord), String> {
    check_source(source, cancel)?;
    if record.len() > settings.max_record_len {
        return Err("FXLink record limit".into());
    }
    let created = now();
    let _input = budget
        .reserve(record.len() + 128)
        .map_err(|e| e.to_string())?;
    let acquire = fec_slots().acquire_owned();
    tokio::pin!(acquire);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    let permit = loop {
        check_source(source, cancel)?;
        if now() >= created + settings.latency_ms {
            return Err("FXLink encoding deadline".into());
        }
        tokio::select! {
            biased;
            _=cancel.cancelled()=>return Err("FXLink cancelled".into()),
            _=source.cancelled.cancelled()=>return Err("FXLink source retired".into()),
            _=tick.tick()=>{},
            permit=&mut acquire=>break permit.map_err(|_|"FXLink encoding closed")?,
        }
    };
    check_source(source, cancel)?;
    let mut job = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let result = encoder.encode(record, created);
        (encoder, result)
    });
    // Revoke promptly even during blocking computation, then join before releasing its owner.
    let (returned, result) = loop {
        if check_source(source, cancel).is_err() || now() >= created + settings.latency_ms {
            let _ = send.reset(1u32.into());
            let _ = job.await;
            return Err("FXLink encoding retired".into());
        }
        tokio::select! {
            biased;
            _=tick.tick()=>{},
            result=&mut job=>break result.map_err(|_|"FXLink encoding failed")?,
        }
    };
    Ok((returned, result.map_err(|e| e.to_string())?))
}
pub(super) fn check_source(source: &Source, cancel: &CancellationToken) -> Result<(), String> {
    if cancel.is_cancelled() || source.cancelled.is_cancelled() || !(source.current)() {
        Err("FXLink source retired".into())
    } else {
        Ok(())
    }
}
fn mtu(connection: &quinn::Connection, settings: &Settings) -> Result<(), String> {
    if connection
        .max_datagram_size()
        .is_none_or(|n| n < HEADER_LEN + settings.shard_size)
    {
        Err("FXLink datagram MTU too small".into())
    } else {
        Ok(())
    }
}
async fn source_write(
    send: &mut quinn::SendStream,
    message: &Control,
    source: &mut Source,
    cancel: &CancellationToken,
    deadline: u64,
) -> Result<(), String> {
    let future = control::write(send, message);
    tokio::pin!(future);
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        check_source(source, cancel)?;
        if now() >= deadline {
            return Err("FXLink send deadline".into());
        }
        tokio::select! {r=&mut future=>{r?;check_source(source,cancel)?;return Ok(())},_=tick.tick()=>{},_=cancel.cancelled()=>return Err("FXLink cancelled".into()),_=source.cancelled.cancelled()=>return Err("FXLink source retired".into())}
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admission {
    Queued,
    Expired,
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn enqueue(
    connection: &quinn::Connection,
    packet: &bytes::Bytes,
    source: &mut Source,
    cancel: &CancellationToken,
    deadline: u64,
    settings: &Settings,
    egress: &AtomicU64,
    geometry: Option<&super::geometry::send::SourceHandle>,
) -> Result<Admission, String> {
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        check_source(source, cancel)?;
        mtu(connection, settings)?;
        if now() >= deadline {
            return Ok(Admission::Expired);
        }
        #[cfg(test)]
        if drop_test_packet(packet) {
            return Ok(Admission::Queued);
        }
        // A fresh wait future is polled only after the source fence, including after backpressure.
        tokio::select! {biased;_=cancel.cancelled()=>return Err("FXLink cancelled".into()),_=source.cancelled.cancelled()=>return Err("FXLink source retired".into()),_=tick.tick()=>{},r=send_before(connection.send_datagram_wait(packet.clone()),deadline)=>{
            let admission=r?;
            if admission==Admission::Expired {return Ok(admission);}
            egress.fetch_add(packet.len() as u64,Ordering::Relaxed);
            if let Some(geometry)=geometry {geometry.original_admitted(packet.len());}
            check_source(source,cancel)?;
            return Ok(admission)
        }}
    }
}
// Kept separate so a blocked queue's deadline boundary can be exercised without
// relying on QUIC-driver scheduling at the exact clock edge.
async fn send_before<F>(future: F, deadline: u64) -> Result<Admission, String>
where
    F: std::future::Future<Output = Result<(), quinn::SendDatagramError>>,
{
    tokio::pin!(future);
    let guarded = futures_util::future::poll_fn(|cx| {
        if now() >= deadline {
            return std::task::Poll::Ready(Ok(Admission::Expired));
        }
        future.as_mut().poll(cx).map(|result| {
            result
                .map(|_| Admission::Queued)
                .map_err(|_| "FXLink datagram send failed".into())
        })
    });
    tokio::time::timeout(
        Duration::from_millis(deadline.saturating_sub(now())),
        guarded,
    )
    .await
    .unwrap_or(Ok(Admission::Expired))
}

#[cfg(test)]
fn drop_test_packet(packet: &bytes::Bytes) -> bool {
    {
        let h = Header::parse(packet).unwrap();
        if bootstrap_endpoint_tests::omit_packet(&h) {
            return true;
        }
        match TEST_DROP.load(std::sync::atomic::Ordering::Relaxed) {
            1 => h.shard_index == 0,
            2 => true,
            3 => h.shard_index == 0 && !TEST_FIRST.swap(true, std::sync::atomic::Ordering::Relaxed),
            _ => false,
        }
    }
}
#[cfg(test)]
pub(super) static TEST_DROP: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
static TEST_FIRST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
use crate::transport_tls_fixture as test_tls;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::fxlink::ClientPool;
    use bytes::Bytes;
    use futures_util::future::BoxFuture;
    struct Feed;
    fn record() -> Bytes {
        let mut b = vec![19; 12000];
        b[..4].copy_from_slice(&11996u32.to_be_bytes());
        Bytes::from(b)
    }
    impl Provider for Feed {
        fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
            Box::pin(async {
                Ok(Source {
                    pending_records: None,
                    records: Box::pin(
                        futures_util::stream::once(async { Ok(record()) })
                            .chain(futures_util::stream::pending()),
                    ),
                    current: Arc::new(|| true),
                    cancelled: CancellationToken::new(),
                })
            })
        }
    }
    pub(super) static REPAIR_QUEUE_DELAY: AtomicU64 = AtomicU64::new(0);
    pub(super) async fn wait_repair_queue() {
        let delay = REPAIR_QUEUE_DELAY.swap(0, Ordering::AcqRel);
        if delay != 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }

    #[tokio::test]
    async fn expired_optional_repair_keeps_later_originals_in_the_same_generation() {
        let _serial = super::super::transport::test_lock().lock().await;
        let before = global().used();
        struct ChannelFeed(
            std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<Result<Bytes, String>>>>,
        );
        impl Provider for ChannelFeed {
            fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
                let rx = self.0.lock().unwrap().take();
                Box::pin(async move {
                    let rx = rx.ok_or(409u16)?;
                    Ok(Source {
                        pending_records: None,
                        records: Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
                            rx.recv().await.map(|r| (r, rx))
                        })),
                        current: Arc::new(|| true),
                        cancelled: CancellationToken::new(),
                    })
                })
            }
        }
        let small = |n| {
            let mut b = vec![n; 1000];
            b[..4].copy_from_slice(&996u32.to_be_bytes());
            Bytes::from(b)
        };
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        for n in 0..4 {
            tx.send(Ok(small(n))).await.unwrap();
        }
        let cert = test_tls::Certificates::new();
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
        let egress = listener.egress();
        let stop = CancellationToken::new();
        let serving = tokio::spawn(listener.serve(
            Arc::new(ChannelFeed(std::sync::Mutex::new(Some(rx)))),
            stop.clone(),
        ));
        let mut tls = (*cert.client()).clone();
        tls.alpn_protocols = vec![b"fxlink/2".to_vec()];
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        config.transport_config(super::super::transport::config());
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let connection = endpoint
            .connect(remote, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        control::write(
            &mut send,
            &Control::Subscribe {
                version: 2,
                id: 0,
                generation: 1,
                profile: control::profile(&settings),
                name: "live".into(),
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
        for expected in 0..4 {
            let packet = connection.read_datagram().await.unwrap();
            assert_eq!(Header::parse(&packet).unwrap().record_id, expected);
        }
        // Simulate credit blocked past this cached record's original TTL.
        // This hook is test-only; real admission still uses send_before.
        REPAIR_QUEUE_DELAY.store(520, Ordering::Release);
        control::write(
            &mut send,
            &Control::Repair {
                block: 0,
                shards: vec![0],
            },
        )
        .await
        .unwrap();
        let entered = tokio::time::timeout(Duration::from_secs(1), async {
            while REPAIR_QUEUE_DELAY.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        tx.send(Ok(small(4))).await.unwrap();
        let outcome =
            tokio::time::timeout(Duration::from_secs(2), connection.read_datagram()).await;
        let queued = egress.load(Ordering::Relaxed);
        REPAIR_QUEUE_DELAY.store(0, Ordering::Release);
        connection.close(0u32.into(), b"test complete");
        drop(send);
        drop(recv);
        endpoint.wait_idle().await;
        stop.cancel();
        serving.await.unwrap().unwrap();
        assert_eq!(global().used(), before, "optional-repair test leaked state");
        entered.expect("repair must reach the blocked queue");
        let packet = outcome
            .expect("bounded source progress")
            .expect("expired optional repair retired healthy source");
        assert_eq!(Header::parse(&packet).unwrap().record_id, 4);
        assert_eq!(
            queued, 5440,
            "expired repair must not count as queued media"
        );
    }

    #[tokio::test]
    async fn blocked_datagram_cannot_commit_when_queue_credit_returns_after_expiry() {
        use std::sync::atomic::AtomicBool;
        let polled = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(AtomicBool::new(false));
        let watched = polled.clone();
        let sent = committed.clone();
        let (release, mut gate) = tokio::sync::oneshot::channel::<()>();
        let future = futures_util::future::poll_fn(move |cx| {
            watched.store(true, Ordering::Release);
            std::future::Future::poll(std::pin::Pin::new(&mut gate), cx).map(|_| {
                sent.store(true, Ordering::Release);
                Ok::<_, quinn::SendDatagramError>(())
            })
        });
        let deadline = now() + 50;
        let pending = tokio::spawn(send_before(future, deadline));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !polled.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = release.send(());
        let result = pending.await.unwrap();
        assert!(
            result == Ok(Admission::Expired),
            "late queue credit admitted an expired datagram"
        );
        assert!(
            !committed.load(Ordering::Acquire),
            "expired send future was polled to commit"
        );
        // Timeout wrappers may poll a ready future before checking their own
        // timer. The admission fence must also reject an already-expired send.
        let ready = async {
            committed.store(true, Ordering::Release);
            Ok::<_, quinn::SendDatagramError>(())
        };
        assert_eq!(
            send_before(ready, now().saturating_sub(1)).await,
            Ok(Admission::Expired)
        );
        assert!(!committed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn live_quic_datagram_erasure_reconstructs_exact_native_record() {
        let _serial = super::super::transport::test_lock().lock().await;
        let cert = test_tls::Certificates::new();
        for mode in [1, 2, 3] {
            let settings = Settings {
                parity_shards: if mode == 3 { 0 } else { 4 },
                ..Settings::default()
            };
            let listener = Listener::bind(
                "127.0.0.1:0".parse().unwrap(),
                &cert.cert,
                &cert.key,
                settings.clone(),
            )
            .unwrap();
            let egress = listener.egress();
            let url = format!("fxlink://{}/live", listener.address());
            let stop = CancellationToken::new();
            let task = tokio::spawn(listener.serve(Arc::new(Feed), stop.clone()));
            TEST_FIRST.store(false, std::sync::atomic::Ordering::Release);
            TEST_DROP.store(mode, std::sync::atomic::Ordering::Release);
            let pool = ClientPool::default();
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
            let result = tokio::time::timeout(Duration::from_secs(3), sub.recv())
                .await
                .unwrap();
            let telemetry = sub.telemetry();
            if mode == 2 {
                assert!(result.is_err());
                assert!(!sub.current());
                assert_eq!(sub.stats().expired_records, 1);
            } else {
                assert_eq!(result.unwrap(), record());
                if mode == 1 {
                    assert!(sub.stats().fec_recovered_shards >= 1);
                    assert!(sub.stats().original_lost_shards >= 1);
                }
                if mode == 3 {
                    assert_eq!(sub.stats().fec_recovered_shards, 0);
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    assert!(
                        telemetry.snapshot()["sender_repair_bytes"]
                            .as_u64()
                            .unwrap()
                            > 0
                    );
                    assert!(
                        telemetry.snapshot()["repair_requested_shards"]
                            .as_u64()
                            .unwrap()
                            > 0
                    );
                }
            }
            drop(sub);
            pool.close().await;
            stop.cancel();
            task.await.unwrap().unwrap();
            assert!(!telemetry.snapshot()["active"].as_bool().unwrap());
            assert_eq!(telemetry.snapshot()["state_bytes"], 0);
            if mode == 2 {
                assert_eq!(egress.load(Ordering::Relaxed), 0);
            } else {
                assert!(egress.load(Ordering::Relaxed) > 0);
            }
        }
        TEST_DROP.store(0, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;

#[cfg(test)]
#[path = "consumed_repair_tests.rs"]
mod consumed_repair_tests;

#[cfg(test)]
#[path = "bootstrap_transport_tests.rs"]
mod bootstrap_transport_tests;

#[cfg(test)]
#[path = "bootstrap_endpoint_tests.rs"]
mod bootstrap_endpoint_tests;

#[cfg(test)]
mod ordinary_work_probe {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    static ACTIVE: AtomicBool = AtomicBool::new(false);
    static INSPECTED: AtomicUsize = AtomicUsize::new(0);
    static FINISHED: AtomicUsize = AtomicUsize::new(0);
    pub(super) struct Guard;
    pub(super) fn arm() -> Guard {
        INSPECTED.store(0, Ordering::SeqCst);
        FINISHED.store(0, Ordering::SeqCst);
        assert!(!ACTIVE.swap(true, Ordering::SeqCst));
        Guard
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            ACTIVE.store(false, Ordering::SeqCst);
        }
    }
    pub(super) struct Request(bool);
    pub(super) fn request(ordinary: bool) -> Request {
        Request(ordinary && ACTIVE.load(Ordering::SeqCst))
    }
    impl Drop for Request {
        fn drop(&mut self) {
            if self.0 {
                FINISHED.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    pub(super) fn packet() {
        if ACTIVE.load(Ordering::SeqCst) {
            INSPECTED.fetch_add(1, Ordering::SeqCst);
        }
    }
    pub(super) fn inspected() -> usize {
        INSPECTED.load(Ordering::SeqCst)
    }
    pub(super) async fn finished(count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while FINISHED.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("repair request completed");
    }
}
