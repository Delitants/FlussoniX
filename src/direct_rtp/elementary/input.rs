//! Public native admission/reorder with a private, validated SDP decoder relay.
use super::{
    packet,
    sdp::{Session, Track},
    startup::StartupQueue,
};
use crate::direct_rtp::key_ledger::Ledger;
use crate::direct_rtp::{config::Settings, crypto, packet as control, sockets::Pair, stats::Stats};
use std::{
    net::{SocketAddr, UdpSocket},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
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
    reservation_inodes: Vec<u64>,
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
        Self::bind_with_ledger(settings, None).await
    }
    pub async fn bind_with_ledger(
        settings: &Settings,
        ledger: Option<Arc<Ledger>>,
    ) -> Result<Self, String> {
        let session = Session::read(
            settings
                .sdp_file
                .as_deref()
                .ok_or("Elementary RTP input requires an SDP file")?,
            settings,
        )?;
        let mut fingerprints = Vec::new();
        let mut contexts = if settings.secure {
            let received = crypto::receive_track_sessions(
                settings,
                &session
                    .tracks
                    .iter()
                    .map(|track| track.ssrc)
                    .collect::<Vec<_>>(),
            )?;
            fingerprints = received.fingerprints;
            received
                .sessions
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
        let mut reservation_inodes = vec![];
        // Reserve every configured public pair before choosing ephemeral decoder
        // or forwarding ports, which could otherwise occupy a later track's pair.
        let mut public_pairs = Vec::with_capacity(session.tracks.len());
        for track in &session.tracks {
            let mut public = settings.clone();
            public.address.set_port(track.port);
            public_pairs.push(Pair::receive(&public)?);
        }
        for (track, pair) in session.tracks.iter().zip(public_pairs) {
            let mut public = settings.clone();
            public.address.set_port(track.port);
            let reserved = reserve()?;
            use std::os::fd::AsRawFd;
            for socket in [&reserved.0, &reserved.1] {
                reservation_inodes.push(super::readiness::socket_inode(socket.as_raw_fd())?);
            }
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
        if settings.secure && !settings.allow_legacy_reused_key {
            let ledger = ledger.ok_or("SRTP receiver ledger required")?;
            tokio::task::spawn_blocking(move || ledger.claim_many(&fingerprints))
                .await
                .map_err(|_| "SRTP receiver ledger task failed")??;
        }
        Ok(Self {
            lanes,
            reservations,
            reservation_inodes,
            session,
            stats,
        })
    }
    pub async fn run<W: AsyncWrite + Unpin>(
        self,
        writer: W,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        self.run_decoder(writer, cancel, None).await
    }
    pub(crate) async fn run_owned<W: AsyncWrite + Unpin>(
        self,
        writer: W,
        cancel: CancellationToken,
        decoder_pid: u32,
    ) -> Result<(), String> {
        self.run_decoder(writer, cancel, Some(decoder_pid)).await
    }
    async fn run_decoder<W: AsyncWrite + Unpin>(
        mut self,
        mut writer: W,
        cancel: CancellationToken,
        decoder_pid: Option<u32>,
    ) -> Result<(), String> {
        let stats = self.stats.clone();
        let ports: Vec<_> = self.lanes.iter().map(|lane| lane.target.port()).collect();
        let decoder_ports: Vec<_> = ports.iter().flat_map(|port| [*port, *port + 1]).collect();
        let sdp = self.session.decoder_sdp(&ports);
        self.reservations.clear();
        let child = cancel.child_token();
        let (ready_tx, ready_rx) = watch::channel(false);
        let mut tasks = tokio::task::JoinSet::new();
        for lane in self.lanes.drain(..) {
            tasks.spawn(relay(lane, stats.clone(), child.clone(), ready_rx.clone()));
        }
        let initialized: Result<bool, &'static str> = tokio::select! {biased;_=cancel.cancelled()=>Ok(false),result=tokio::time::timeout(Duration::from_secs(2),async{writer.write_all(sdp.as_bytes()).await?;writer.shutdown().await})=>match result{Ok(Ok(()))=>Ok(true),Ok(Err(_))=>Err("Elementary decoder SDP write failed"),Err(_)=>Err("Elementary decoder SDP write stalled")}};
        // Unix ChildStdin::shutdown is a no-op. Drop the owned pipe to deliver
        // EOF before FFmpeg can parse the full SDP and bind its decoder sockets.
        drop(writer);
        let startup = match initialized {
            Ok(true) => self.receive(&cancel, decoder_pid, &decoder_ports).await,
            Ok(false) => Ok(()),
            Err(e) => Err(e),
        };
        if startup.is_ok() && initialized == Ok(true) && !cancel.is_cancelled() {
            *stats.status.lock().unwrap() = if stats.packets.load(Ordering::Relaxed) > 0 {
                "receiving"
            } else {
                "bound"
            };
            ready_tx.send_replace(true);
        } else {
            child.cancel();
        }
        let mut task_error = None;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    task_error.get_or_insert(e);
                    child.cancel();
                }
                Err(_) => {
                    task_error.get_or_insert("Elementary RTP relay task failed");
                    child.cancel();
                }
            }
        }
        let result = startup.and_then(|()| task_error.map_or(Ok(()), Err));
        *stats.status.lock().unwrap() = if result.is_err() { "failed" } else { "stopped" };
        *stats.error.lock().unwrap() = result.as_ref().err().copied();
        result.map_err(str::to_owned)
    }
    async fn receive(
        &mut self,
        cancel: &CancellationToken,
        decoder_pid: Option<u32>,
        decoder_ports: &[u16],
    ) -> Result<(), &'static str> {
        // Public admission runs in the bounded reader tasks. Private delivery
        // remains gated on ownership of every decoder RTP/RTCP socket.
        let ready: Result<(), &'static str> = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let ready = match decoder_pid {
                    Some(pid) => super::readiness::owned(decoder_ports, pid, &self.reservation_inodes)?,
                    None => super::readiness::bound(decoder_ports)?,
                };
                if ready {return Ok(());}
                tokio::select! { _=cancel.cancelled()=>return Ok(()), _=tokio::time::sleep(Duration::from_millis(10))=>{} }
            }
        }).await.map_err(|_| "Elementary RTP decoder did not bind")?;
        ready?;
        if cancel.is_cancelled() {
            return Ok(());
        }
        Ok(())
    }
}
async fn send(
    socket: &tokio::net::UdpSocket,
    bytes: &[u8],
    to: Option<SocketAddr>,
    cancel: &CancellationToken,
) -> Result<(), &'static str> {
    tokio::select! {biased;_=cancel.cancelled()=>Ok(()),result=tokio::time::timeout(Duration::from_secs(2),async{match to{Some(to)=>socket.send_to(bytes,to).await,None=>socket.send(bytes).await}})=>{let n=result.map_err(|_|"Elementary RTP relay write stalled")?.map_err(|_|"Elementary RTP relay write failed")?;if n!=bytes.len(){return Err("Elementary RTP relay datagram truncated");}Ok(())}}
}
fn admit_rtp(
    lane: &mut Lane,
    stats: &Stats,
    peer: &mut Option<(SocketAddr, u32)>,
    bytes: &[u8],
    addr: SocketAddr,
) -> Result<Option<(Vec<u8>, u16)>, &'static str> {
    if lane.settings.source_ip.is_some_and(|ip| ip != addr.ip())
        || peer.is_some_and(|(p, _)| p != addr)
    {
        stats.foreign.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    }
    let mut plain = bytes.to_vec();
    if let Some(crypto) = &mut lane.crypto {
        if crypto.unprotect(&mut plain, false).is_err() {
            stats.auth_failed.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
    }
    let parsed = match packet::parse(&plain, &lane.track) {
        Ok(parsed) => parsed,
        Err(_) => {
            stats.invalid.fetch_add(1, Ordering::Relaxed);
            if peer.is_none() {
                if let Some(crypto) = &mut lane.crypto {
                    crypto.discard_candidate()?;
                }
            }
            return Ok(None);
        }
    };
    if peer.is_some_and(|(_, ssrc)| ssrc != parsed.ssrc) {
        stats.foreign.fetch_add(1, Ordering::Relaxed);
        return Ok(None);
    }
    peer.get_or_insert((addr, parsed.ssrc));
    stats.packets.fetch_add(1, Ordering::Relaxed);
    stats
        .bytes
        .fetch_add(parsed.payload.len() as u64, Ordering::Relaxed);
    let sequence = parsed.sequence;
    Ok(Some((plain, sequence)))
}

fn admit_rtcp(
    lane: &mut Lane,
    stats: &Stats,
    peer: Option<(SocketAddr, u32)>,
    bytes: &[u8],
    addr: SocketAddr,
) -> Option<Vec<u8>> {
    let (p, ssrc) = peer?;
    if addr.ip() != p.ip() || p.port().checked_add(1) != Some(addr.port()) {
        stats.foreign.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let mut plain = bytes.to_vec();
    if let Some(crypto) = &mut lane.crypto {
        if crypto.unprotect(&mut plain, true).is_err() {
            stats.auth_failed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    }
    if !control::valid_rtcp(&plain)
        || plain[1] != 200
        || u32::from_be_bytes(plain[4..8].try_into().unwrap()) != ssrc
    {
        stats.invalid.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    Some(plain)
}
async fn relay(
    mut lane: Lane,
    stats: Arc<Stats>,
    cancel: CancellationToken,
    mut ready_signal: watch::Receiver<bool>,
) -> Result<(), &'static str> {
    let mut buffer = [0; control::MAX_PACKET + 11];
    let mut rtcp = [0; 2049];
    let mut feedback = [0; 2049];
    let mut peer: Option<(SocketAddr, u32)> = None;
    let mut queue = StartupQueue::new(1024, 2 * 1024 * 1024, 64, 128 * 1024);
    let mut decoder_owned = *ready_signal.borrow();
    let mut reorder = control::Reorder::new(lane.settings.jitter);
    let mut duplicates = 0;
    let mut lost = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let ready = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            changed = ready_signal.changed(), if !decoder_owned => {
                changed.map_err(|_| "Elementary RTP decoder readiness lost")?;
                decoder_owned = *ready_signal.borrow_and_update();
                continue;
            },
            result = lane.public.rtp.recv_from(&mut buffer) => {
                let (n, addr) = result.map_err(|_| "Elementary RTP public socket failed")?;
                let Some((plain, sequence)) = admit_rtp(&mut lane, &stats, &mut peer, &buffer[..n], addr)? else { continue; };
                if decoder_owned {
                    *stats.status.lock().unwrap() = "receiving";
                }
                if !decoder_owned || queue.has_rtp() {
                    let dropped = queue.push_rtp(plain);
                    stats.startup_dropped_packets.fetch_add(dropped, Ordering::Relaxed);
                    continue;
                }
                reorder.push(sequence, plain, Instant::now())
            },
            result = lane.public.rtcp.recv_from(&mut rtcp) => {
                let (n, addr) = result.map_err(|_| "Elementary RTCP public socket failed")?;
                let Some(plain) = admit_rtcp(&mut lane, &stats, peer, &rtcp[..n], addr) else { continue; };
                if !decoder_owned || queue.has_rtcp() {
                    let dropped = queue.push_rtcp(plain);
                    stats.startup_dropped_rtcp_packets.fetch_add(dropped, Ordering::Relaxed);
                } else {
                    send(&lane.local.rtcp, &plain, None, &cancel).await?;
                    stats.rtcp.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            },
            _ = std::future::ready(()), if decoder_owned && queue.has_rtp() => {
                let plain = queue.pop_rtp().expect("queued RTP packet");
                let sequence = u16::from_be_bytes([plain[2], plain[3]]);
                reorder.push(sequence, plain, Instant::now())
            },
            _ = std::future::ready(()), if decoder_owned && queue.has_rtcp() => {
                let plain = queue.pop_rtcp().expect("queued RTCP packet");
                send(&lane.local.rtcp, &plain, None, &cancel).await?;
                stats.rtcp.fetch_add(1, Ordering::Relaxed);
                continue;
            },
            _ = tick.tick(), if decoder_owned => reorder.flush(Instant::now()),
            result = lane.local.rtcp.recv(&mut feedback), if decoder_owned => {
                let n = result.map_err(|_| "Elementary RTCP private socket failed")?;
                let Some((p, _)) = peer else { continue; };
                let Some(port) = p.port().checked_add(1) else { continue; };
                if !control::valid_rtcp(&feedback[..n]) { continue; }
                let mut body = feedback[..n].to_vec();
                if let Some(crypto) = &mut lane.feedback { crypto.protect(&mut body, true)?; }
                send(&lane.public.rtcp, &body, Some(SocketAddr::new(p.ip(), port)), &cancel).await?;
                stats.rtcp.fetch_add(1, Ordering::Relaxed);
                continue;
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
