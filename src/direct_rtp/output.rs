use super::{config::Output, crypto, packet, sockets::Pair, stats::Stats};
use bytes::Bytes;
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
pub struct State {
    pub(crate) definition: Output,
    index: usize,
    pub(super) stats: Stats,
    pub(super) egress: Arc<AtomicU64>,
    pub(super) sdp: std::sync::Mutex<Option<(u64, String)>>,
}
impl State {
    pub fn new(definition: Output, index: usize) -> Arc<Self> {
        Self::with_egress(definition, index, Arc::new(AtomicU64::new(0)))
    }
    pub fn with_egress(definition: Output, index: usize, egress: Arc<AtomicU64>) -> Arc<Self> {
        let stats = Stats::default();
        if definition.settings.elementary {
            *stats.profile.lock().unwrap() = "elementary";
        }
        Arc::new(Self {
            sdp: std::sync::Mutex::new(None),
            definition,
            index,
            stats,
            egress,
        })
    }
    pub fn stats(&self) -> Value {
        let mut v = self.stats.snapshot();
        v["index"] = self.index.into();
        v["endpoint"] = self.definition.settings.endpoint().into();
        v["encrypted"] = self.definition.settings.secure.into();
        if self.definition.settings.elementary {
            let saved = self.sdp.lock().unwrap();
            v["sdp_ready"] = saved.is_some().into();
            v["sdp_generation"] =
                serde_json::json!(saved.as_ref().map(|(generation, _)| generation));
        }
        v
    }
    pub(crate) fn sdp_for_generation(&self, generation: u64) -> Option<String> {
        self.sdp
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(g, _)| *g == generation)
            .map(|(_, s)| s.clone())
    }
    pub async fn run_worker(
        self: Arc<Self>,
        worker: Arc<crate::media::Worker>,
        cancel: CancellationToken,
    ) {
        if self.definition.settings.elementary {
            super::elementary::output::run(self, worker, cancel).await;
        } else {
            self.run(worker.subscribe(), cancel).await;
        }
    }
    pub async fn run(
        self: Arc<Self>,
        mut receiver: broadcast::Receiver<Bytes>,
        cancel: CancellationToken,
    ) {
        if self.definition.settings.elementary {
            *self.stats.status.lock().unwrap() = "failed";
            *self.stats.error.lock().unwrap() = Some("Elementary RTP requires a native worker");
            return;
        }
        if self.definition.disabled {
            *self.stats.status.lock().unwrap() = "disabled";
            return;
        }
        *self.stats.status.lock().unwrap() = "starting";
        let result = self.send(&mut receiver, &cancel).await;
        *self.stats.status.lock().unwrap() = if result.is_err() { "failed" } else { "stopped" };
        if let Err(reason) = result {
            *self.stats.error.lock().unwrap() = Some(reason);
        }
    }
    async fn send(
        &self,
        receiver: &mut broadcast::Receiver<Bytes>,
        cancel: &CancellationToken,
    ) -> Result<(), &'static str> {
        let pair = Pair::send(&self.definition.settings)
            .await
            .map_err(|_| "RTP output socket failed")?;
        let seed = *uuid::Uuid::new_v4().as_bytes();
        let ssrc = u32::from_be_bytes(seed[..4].try_into().unwrap());
        let mut sequence = u16::from_be_bytes(seed[4..6].try_into().unwrap());
        let (mut feedback, mut crypto) = if self.definition.settings.secure {
            let (rx, tx) = crypto::sessions(
                self.definition
                    .settings
                    .key_file
                    .as_deref()
                    .ok_or("SRTP key file required")?,
                ssrc,
            )?;
            (Some(rx), Some(tx))
        } else {
            (None, None)
        };
        let origin = Instant::now();
        let stamp = u32::from_be_bytes(seed[8..12].try_into().unwrap());
        let mut reports = tokio::time::interval(Duration::from_secs(5));
        reports.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        reports.tick().await;
        let mut control = [0; 2049];
        let mut control_peer = None;
        let mut pending = Vec::with_capacity(7 * 188);
        let mut pacer = crate::rtsp::udp::Pacer::new(self.definition.max_mbps as f64)
            .map_err(|_| "RTP rate invalid")?;
        let mut flush = tokio::time::interval(Duration::from_millis(10));
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let mut drain = false;
            let mut closed = false;
            let bytes = tokio::select! {_=cancel.cancelled()=>return Ok(()),
                result=receiver.recv()=>match result {Ok(b)=>b,Err(broadcast::error::RecvError::Closed)=>{drain=true;closed=true;Bytes::new()},Err(_)=>return Err("RTP output queue lagged")},
                _=reports.tick()=>{
                    let time=stamp.wrapping_add((origin.elapsed().as_micros()*90/1000) as u32);
                    let mut body=report(ssrc,time,self.stats.packets.load(Ordering::Relaxed) as u32,self.stats.bytes.load(Ordering::Relaxed) as u32);
                    if let Some(crypto)=&mut crypto {crypto.protect(&mut body,true)?;}
                    tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),result=tokio::time::timeout(Duration::from_secs(2),pair.rtcp.send_to(&body,std::net::SocketAddr::new(self.definition.settings.address.ip(),self.definition.settings.address.port()+1)))=>{result.map_err(|_| "RTCP send stalled")?.map_err(|_| "RTCP send failed")?;}}self.egress.fetch_add((body.len()+if self.definition.settings.address.is_ipv4(){28}else{48}) as u64,Ordering::Relaxed);continue;
                },
                _=flush.tick()=>{drain=true;Bytes::new()},
                result=pair.rtcp.recv_from(&mut control)=>{if let Ok((n,addr))=result {
                    let destination=self.definition.settings.address;
                    if addr.port()!=destination.port()+1 || addr.ip().is_multicast() || addr.ip().is_unspecified() || (!destination.ip().is_multicast()&&addr.ip()!=destination.ip()) || control_peer.is_some_and(|p|p!=addr){self.stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
                    let mut plain=control[..n].to_vec();if let Some(cipher)=&mut feedback {if cipher.unprotect(&mut plain,true).is_err(){self.stats.auth_failed.fetch_add(1,Ordering::Relaxed);continue;}}
                    if packet::valid_rtcp(&plain){control_peer.get_or_insert(addr);self.stats.rtcp.fetch_add(1,Ordering::Relaxed);}else{self.stats.invalid.fetch_add(1,Ordering::Relaxed);if control_peer.is_none(){if let Some(cipher)=&mut feedback{cipher.discard_candidate()?;}}}
                }continue;},
            };
            // Incoming chunks can split TS packets. Retain at most one MTU of data.
            let mut offset = 0;
            loop {
                let n = (7 * 188 - pending.len()).min(bytes.len() - offset);
                pending.extend_from_slice(&bytes[offset..offset + n]);
                offset += n;
                let count = if pending.len() >= 7 * 188 {
                    7 * 188
                } else if drain {
                    pending.len() / 188 * 188
                } else {
                    0
                };
                if count > 0 {
                    let payload = &pending[..count];
                    if payload.chunks_exact(188).any(|p| p[0] != 0x47) {
                        return Err("RTP output TS framing invalid");
                    }
                    let ticks = (origin.elapsed().as_micros() * 90 / 1000) as u64;
                    let now = tokio::time::Instant::now();
                    let due = pacer
                        .ready_at(
                            ticks,
                            count + 12 + if crypto.is_some() { 10 } else { 0 },
                            now,
                        )
                        .map_err(|_| "RTP output pacing failed")?;
                    tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep_until(due)=>{}}
                    let timestamp =
                        stamp.wrapping_add((origin.elapsed().as_micros() * 90 / 1000) as u32);
                    let mut body = packet::packet(sequence, timestamp, ssrc, payload);
                    if let Some(crypto) = &mut crypto {
                        crypto.protect(&mut body, false)?;
                    }
                    tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=tokio::time::timeout(Duration::from_secs(2),pair.rtp.send(&body))=>{let n=result.map_err(|_| "RTP send stalled")?.map_err(|_| "RTP send failed")?;if n!=body.len(){return Err("RTP datagram truncated");}}}
                    self.egress.fetch_add(
                        (body.len()
                            + if self.definition.settings.address.is_ipv4() {
                                28
                            } else {
                                48
                            }) as u64,
                        Ordering::Relaxed,
                    );
                    pacer.sent(body.len(), tokio::time::Instant::now());
                    self.stats.packets.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .bytes
                        .fetch_add(payload.len() as u64, Ordering::Relaxed);
                    *self.stats.status.lock().unwrap() = "sending";
                    sequence = sequence.wrapping_add(1);
                    pending.drain(..count);
                }
                if offset == bytes.len() {
                    break;
                }
            }
            if closed {
                if !pending.is_empty() {
                    return Err("RTP output incomplete TS tail");
                }
                return Ok(());
            }
        }
    }
}
pub(super) fn report(ssrc: u32, timestamp: u32, packets: u32, octets: u32) -> Vec<u8> {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut out = vec![0x80, 200, 0, 6];
    for n in [
        ssrc,
        (time.as_secs() + 2_208_988_800) as u32,
        ((u64::from(time.subsec_nanos()) << 32) / 1_000_000_000) as u32,
        timestamp,
        packets,
        octets,
    ] {
        out.extend(n.to_be_bytes());
    }
    out.extend(packet::sdes(ssrc));
    out
}
