pub use super::stats::Stats as Statistics;
use super::{
    config::Settings,
    crypto::{self, Session},
    packet::{self, Reorder},
    sockets::Pair,
    stats::Stats,
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
struct Mp2t {
    pair: Pair,
    settings: Settings,
    pub stats: Arc<Stats>,
    crypto: Option<Session>,
    feedback: Option<Session>,
    receiver_id: u32,
}
impl Mp2t {
    pub async fn bind(settings: &Settings) -> Result<Self, String> {
        let receiver_id =
            u32::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..4].try_into().unwrap());
        let (receive, feedback) = if settings.secure {
            let (rx, tx) = crypto::sessions(
                settings
                    .key_file
                    .as_deref()
                    .ok_or("SRTP key file required")?,
                receiver_id,
            )?;
            (Some(rx), Some(tx))
        } else {
            (None, None)
        };
        let pair = Pair::receive(settings)?;
        let stats = Arc::new(Stats::default());
        *stats.status.lock().unwrap() = "bound";
        Ok(Self {
            pair,
            settings: settings.clone(),
            stats,
            crypto: receive,
            feedback,
            receiver_id,
        })
    }
    pub async fn run<W: AsyncWrite + Unpin>(
        mut self,
        mut writer: W,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let result = self.receive(&mut writer, &cancel).await;
        *self.stats.status.lock().unwrap() = "stopped";
        if result.is_err() {
            *self.stats.error.lock().unwrap() = Some("RTP input closed");
        }
        result
    }
    async fn receive<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        let mut buffer = [0; packet::MAX_PACKET + 1];
        let mut control = [0; 2049];
        let mut peer: Option<(SocketAddr, u32)> = None;
        let mut reorder = Reorder::new(self.settings.jitter);
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let receiver_id = self.receiver_id;
        let mut reports = tokio::time::interval(Duration::from_secs(1));
        reports.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_highest: Option<u32> = None;
        let mut last_loss = 0;
        let mut transit: Option<i32> = None;
        let mut jitter = 0.0f64;
        let start = Instant::now();
        let mut last_sr = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let ready = tokio::select! {_=cancel.cancelled()=>return Ok(()),
                result=self.pair.rtp.recv_from(&mut buffer)=>{
                    let (n,addr)=result.map_err(|_| "RTP input socket failed")?;
                    if self.settings.source_ip.is_some_and(|ip| ip!=addr.ip()) || peer.is_some_and(|(p,_)| p!=addr) {self.stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
                    let mut plain=buffer[..n].to_vec();
                    if let Some(crypto)=&mut self.crypto {if crypto.unprotect(&mut plain,false).is_err(){self.stats.auth_failed.fetch_add(1,Ordering::Relaxed);continue;}}
                    let p=match packet::parse(&plain){Ok(p)=>p,Err(_)=>{self.stats.invalid.fetch_add(1,Ordering::Relaxed);if peer.is_none(){if let Some(crypto)=&mut self.crypto {crypto.discard_candidate()?;}}continue;}};
                    if peer.is_some_and(|(_,ssrc)| p.ssrc!=ssrc) {self.stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
                    peer.get_or_insert((addr,p.ssrc));*self.stats.status.lock().unwrap()="receiving";
                    let current=((start.elapsed().as_micros()*90/1000) as u32).wrapping_sub(p.timestamp) as i32;
                    if let Some(prior)=transit {jitter+=((i64::from(current.wrapping_sub(prior))).unsigned_abs() as f64-jitter)/16.0;}transit=Some(current);
                    self.stats.packets.fetch_add(1,Ordering::Relaxed);self.stats.bytes.fetch_add(p.payload.len() as u64,Ordering::Relaxed);
                    reorder.push(p.sequence,p.payload.to_vec(),Instant::now())
                },
                result=self.pair.rtcp.recv_from(&mut control)=>{
                    let (n,addr)=result.map_err(|_| "RTCP input socket failed")?;
                    if !peer.is_some_and(|(p,_)| p.ip()==addr.ip() && p.port().checked_add(1)==Some(addr.port())){continue;}
                    let mut plain=control[..n].to_vec();if let Some(crypto)=&mut self.crypto {if crypto.unprotect(&mut plain,true).is_err(){self.stats.auth_failed.fetch_add(1,Ordering::Relaxed);continue;}}
                    let control=&plain;let n=control.len();
                    if packet::valid_rtcp(control) {
                        self.stats.rtcp.fetch_add(1,Ordering::Relaxed);
                        if n>=28 && control[1]==200 && peer.is_some_and(|(_,ssrc)| u32::from_be_bytes(control[4..8].try_into().unwrap())==ssrc) {
                            last_sr=Some((u32::from_be_bytes(control[10..14].try_into().unwrap()),Instant::now()));
                        }
                    }continue;
                },
                _=tick.tick()=>reorder.flush(Instant::now()),
                _=reports.tick()=>{
                    if let Some((p,source))=peer {
                        if let Some(port)=p.port().checked_add(1) {
                            let highest=reorder.highest();let expected=last_highest.map_or(self.stats.packets.load(Ordering::Relaxed).saturating_sub(reorder.duplicates)+reorder.lost,|prev| u64::from(highest.wrapping_sub(prev)));
                            let fraction=(reorder.lost.saturating_sub(last_loss).saturating_mul(256)).checked_div(expected).unwrap_or(0).min(255) as u8;last_loss=reorder.lost;last_highest=Some(highest);
                            let (sr,delay)=last_sr.map_or((0,0),|(sr,at)| (sr,(at.elapsed().as_micros()*65536/1_000_000).min(u128::from(u32::MAX)) as u32));
                            let mut report=packet::receiver_report(receiver_id,source,&packet::Reception{highest,lost:reorder.lost,fraction,jitter:jitter.min(f64::from(u32::MAX)) as u32,last_sr:sr,delay_sr:delay});
                            if let Some(crypto)=&mut self.feedback {crypto.protect(&mut report,true)?;}
                            tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),result=tokio::time::timeout(Duration::from_secs(2),self.pair.rtcp.send_to(&report,SocketAddr::new(p.ip(),port)))=>{result.map_err(|_| "RTCP feedback stalled")?.map_err(|_| "RTCP feedback failed")?;}}
                        }
                    }continue;
                },
            };
            self.stats
                .duplicates
                .store(reorder.duplicates, Ordering::Relaxed);
            self.stats.lost.store(reorder.lost, Ordering::Relaxed);
            for data in ready {
                tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=tokio::time::timeout(Duration::from_secs(2),writer.write_all(&data))=>{result.map_err(|_| "RTP decoder stalled")?.map_err(|_| "RTP decoder closed")?;}}
            }
        }
    }
}

enum Implementation {
    Mp2t(Mp2t),
    Elementary(super::elementary::input::Input),
}
pub struct Input {
    inner: Implementation,
    pub stats: Arc<Stats>,
}
impl Input {
    pub async fn bind(settings: &Settings) -> Result<Self, String> {
        if settings.elementary {
            let input = super::elementary::input::Input::bind(settings).await?;
            Ok(Self {
                stats: input.stats.clone(),
                inner: Implementation::Elementary(input),
            })
        } else {
            let input = Mp2t::bind(settings).await?;
            Ok(Self {
                stats: input.stats.clone(),
                inner: Implementation::Mp2t(input),
            })
        }
    }
    pub async fn run<W: AsyncWrite + Unpin>(
        self,
        writer: W,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        match self.inner {
            Implementation::Mp2t(input) => input.run(writer, cancel).await,
            Implementation::Elementary(input) => input.run(writer, cancel).await,
        }
    }
    pub(crate) async fn run_owned<W: AsyncWrite + Unpin>(
        self,
        writer: W,
        cancel: CancellationToken,
        decoder_pid: u32,
    ) -> Result<(), String> {
        match self.inner {
            Implementation::Mp2t(input) => input.run(writer, cancel).await,
            Implementation::Elementary(input) => input.run_owned(writer, cancel, decoder_pid).await,
        }
    }
}
