//! Public native admission/reorder with a private, validated SDP decoder relay.
use super::{
    packet,
    sdp::{Session, Track},
};
use crate::direct_rtp::{config::Settings, crypto, packet as control, sockets::Pair, stats::Stats};
use std::{
    net::{SocketAddr, UdpSocket},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
struct Lane {
    public: Pair,
    local: Pair,
    target: SocketAddr,
    track: Track,
    settings: Settings,
    crypto: Option<crypto::Session>,
    feedback: Option<crypto::Session>,
}
pub struct Input {
    lanes: Vec<Lane>,
    reservations: Vec<(UdpSocket, UdpSocket)>,
    session: Session,
    pub stats: Arc<Stats>,
}
fn reserve() -> Result<(UdpSocket, UdpSocket), String> {
    for _ in 0..64 {
        let a = UdpSocket::bind("127.0.0.1:0").map_err(|_| "Private RTP reservation failed")?;
        let port = a
            .local_addr()
            .map_err(|_| "Private RTP reservation failed")?
            .port();
        if port == 65535 {
            continue;
        }
        if let Ok(b) = UdpSocket::bind(("127.0.0.1", port + 1)) {
            return Ok((a, b));
        }
    }
    Err("Private RTP decoder ports unavailable".into())
}
impl Input {
    pub async fn bind(settings: &Settings) -> Result<Self, String> {
        let session = Session::read(
            settings
                .sdp_file
                .as_deref()
                .ok_or("Elementary RTP input requires an SDP file")?,
            settings,
        )?;
        let mut contexts = if settings.secure {
            crypto::track_sessions(
                settings
                    .key_file
                    .as_deref()
                    .ok_or("SRTP key file required")?,
                &vec![None; session.tracks.len()],
            )?
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>()
            .into_iter()
        } else {
            (0..session.tracks.len())
                .map(|_| None)
                .collect::<Vec<_>>()
                .into_iter()
        };
        let stats = Arc::new(Stats::default());
        *stats.profile.lock().unwrap() = "elementary";
        *stats.status.lock().unwrap() = "starting";
        let mut lanes = vec![];
        let mut reservations = vec![];
        for track in &session.tracks {
            let mut public = settings.clone();
            public.address.set_port(track.port);
            let pair = Pair::receive(&public)?;
            let reserved = reserve()?;
            let target = reserved
                .0
                .local_addr()
                .map_err(|_| "Private RTP reservation failed")?;
            let mut local = settings.clone();
            local.address = target;
            local.interface = None;
            local.source_ip = None;
            let forwarder = Pair::send(&local).await?;
            lanes.push(Lane {
                public: pair,
                local: forwarder,
                target,
                track: track.clone(),
                settings: public,
                crypto: None,
                feedback: None,
            });
            if let Some((receive, transmit)) = contexts.next().flatten() {
                let lane = lanes.last_mut().unwrap();
                lane.crypto = Some(receive);
                lane.feedback = Some(transmit);
            }
            reservations.push(reserved);
        }
        Ok(Self {
            lanes,
            reservations,
            session,
            stats,
        })
    }
    pub async fn run<W: AsyncWrite + Unpin>(
        mut self,
        mut writer: W,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let stats = self.stats.clone();
        let ports: Vec<_> = self.lanes.iter().map(|lane| lane.target.port()).collect();
        let sdp = self.session.decoder_sdp(&ports);
        self.reservations.clear();
        let initialized: Result<bool, String> = tokio::select! {biased;_=cancel.cancelled()=>Ok(false),result=tokio::time::timeout(Duration::from_secs(2),async{writer.write_all(sdp.as_bytes()).await?;writer.shutdown().await})=>match result{Ok(Ok(()))=>Ok(true),Ok(Err(_))=>Err("Elementary decoder SDP write failed".into()),Err(_)=>Err("Elementary decoder SDP write stalled".into())}};
        // Unix ChildStdin::shutdown is a no-op. Drop the owned pipe to deliver
        // EOF before FFmpeg can parse the full SDP and bind its decoder sockets.
        drop(writer);
        let result = match initialized {
            Ok(true) => self.receive(&cancel).await,
            Ok(false) => Ok(()),
            Err(e) => Err(e),
        };
        *stats.status.lock().unwrap() = if result.is_err() { "failed" } else { "stopped" };
        if result.is_err() {
            *stats.error.lock().unwrap() = Some("Elementary RTP input closed");
        }
        result
    }
    async fn receive(&mut self, cancel: &CancellationToken) -> Result<(), String> {
        // Do not admit public datagrams until the owned decoder has opened every
        // private pair. The shared loopback trust boundary matches other worker bridges.
        let decoder_ports: Vec<_> = self
            .lanes
            .iter()
            .flat_map(|lane| [lane.target.port(), lane.target.port() + 1])
            .collect();
        let ready: Result<(), &'static str> = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if super::readiness::bound(&decoder_ports)? {return Ok(());}
                tokio::select! { _=cancel.cancelled()=>return Ok(()), _=tokio::time::sleep(Duration::from_millis(10))=>{} }
            }
        }).await.map_err(|_| "Elementary RTP decoder did not bind")?;
        ready?;
        if cancel.is_cancelled() {
            return Ok(());
        }
        *self.stats.status.lock().unwrap() = "bound";
        let child = cancel.child_token();
        let mut tasks = tokio::task::JoinSet::new();
        for lane in self.lanes.drain(..) {
            let c = child.clone();
            let stats = self.stats.clone();
            tasks.spawn(relay(lane, stats, c));
        }
        let mut error = None;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error.get_or_insert(e);
                    child.cancel();
                }
                Err(_) => {
                    error.get_or_insert("Elementary RTP relay task failed".into());
                    child.cancel();
                }
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
async fn send(
    socket: &tokio::net::UdpSocket,
    bytes: &[u8],
    to: Option<SocketAddr>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    tokio::select! {biased;_=cancel.cancelled()=>Ok(()),result=tokio::time::timeout(Duration::from_secs(2),async{match to{Some(to)=>socket.send_to(bytes,to).await,None=>socket.send(bytes).await}})=>{let n=result.map_err(|_|"Elementary RTP relay write stalled")?.map_err(|_|"Elementary RTP relay write failed")?;if n!=bytes.len(){return Err("Elementary RTP relay datagram truncated".into());}Ok(())}}
}
async fn relay(mut lane: Lane, stats: Arc<Stats>, cancel: CancellationToken) -> Result<(), String> {
    let mut buffer = [0; control::MAX_PACKET + 11];
    let mut rtcp = [0; 2049];
    let mut feedback = [0; 2049];
    let mut peer: Option<(SocketAddr, u32)> = None;
    let mut reorder = control::Reorder::new(lane.settings.jitter);
    let mut duplicates = 0;
    let mut lost = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let ready = tokio::select! {_=cancel.cancelled()=>return Ok(()),
         result=lane.public.rtp.recv_from(&mut buffer)=>{
          let(n,addr)=result.map_err(|_|"Elementary RTP public socket failed")?;
          if lane.settings.source_ip.is_some_and(|ip|ip!=addr.ip()) || peer.is_some_and(|(p,_)|p!=addr){stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
          let mut plain=buffer[..n].to_vec();if let Some(crypto)=&mut lane.crypto {if crypto.unprotect(&mut plain,false).is_err(){stats.auth_failed.fetch_add(1,Ordering::Relaxed);continue;}}
          let parsed=match packet::parse(&plain,&lane.track){Ok(p)=>p,Err(_)=>{stats.invalid.fetch_add(1,Ordering::Relaxed);if peer.is_none(){if let Some(crypto)=&mut lane.crypto {crypto.discard_candidate()?;}}continue;}};
          if peer.is_some_and(|(_,ssrc)|ssrc!=parsed.ssrc){stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
          peer.get_or_insert((addr,parsed.ssrc));*stats.status.lock().unwrap()="receiving";stats.packets.fetch_add(1,Ordering::Relaxed);stats.bytes.fetch_add(parsed.payload.len() as u64,Ordering::Relaxed);
          reorder.push(parsed.sequence,plain,Instant::now())
         },
         _=tick.tick()=>reorder.flush(Instant::now()),
         result=lane.public.rtcp.recv_from(&mut rtcp)=>{
          let(n,addr)=result.map_err(|_|"Elementary RTCP public socket failed")?;
          let Some((p,ssrc))=peer else{continue;};if addr.ip()!=p.ip() || p.port().checked_add(1)!=Some(addr.port()){stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
          let mut plain=rtcp[..n].to_vec();if let Some(crypto)=&mut lane.crypto {if crypto.unprotect(&mut plain,true).is_err(){stats.auth_failed.fetch_add(1,Ordering::Relaxed);continue;}}
          if !control::valid_rtcp(&plain) || plain[1]!=200 || u32::from_be_bytes(plain[4..8].try_into().unwrap())!=ssrc{stats.invalid.fetch_add(1,Ordering::Relaxed);continue;}
          send(&lane.local.rtcp,&plain,None,&cancel).await?;stats.rtcp.fetch_add(1,Ordering::Relaxed);continue;
         },
         result=lane.local.rtcp.recv(&mut feedback)=>{
          let n=result.map_err(|_|"Elementary RTCP private socket failed")?;let Some((p,_))=peer else{continue;};let Some(port)=p.port().checked_add(1)else{continue;};
          if !control::valid_rtcp(&feedback[..n]){continue;}let mut body=feedback[..n].to_vec();if let Some(crypto)=&mut lane.feedback {crypto.protect(&mut body,true)?;}send(&lane.public.rtcp,&body,Some(SocketAddr::new(p.ip(),port)),&cancel).await?;stats.rtcp.fetch_add(1,Ordering::Relaxed);continue;
         },
        };
        stats.duplicates.fetch_add(
            reorder.duplicates.saturating_sub(duplicates),
            Ordering::Relaxed,
        );
        duplicates = reorder.duplicates;
        stats
            .lost
            .fetch_add(reorder.lost.saturating_sub(lost), Ordering::Relaxed);
        lost = reorder.lost;
        for bytes in ready {
            send(&lane.local.rtp, &bytes, None, &cancel).await?;
        }
    }
}
