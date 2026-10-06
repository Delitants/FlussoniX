//! Shared native RTP packetizer output, one RTP/RTCP pair per actual track.
use crate::{
    direct_rtp::{
        output::{State, report_at},
        packet,
        sockets::Pair,
    },
    media::Worker,
    rtp::{Description, MediaTrack},
};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;
struct Lane {
    pair: Arc<Pair>,
    track: MediaTrack,
    packets: u32,
    octets: u32,
    stamp_origin: u32,
    target: SocketAddr,
}
enum Sent {
    Packet,
    Unavailable,
    Cancelled,
}
fn sdp(description: &Description, address: SocketAddr) -> String {
    let family = if address.is_ipv4() { "IP4" } else { "IP6" };
    let ip = address.ip();
    let mut out = format!(
        "v=0\r\no=- 0 {} IN {family} {ip}\r\ns=FlussoniX elementary output\r\nc=IN {family} {ip}\r\nt=0 0\r\n",
        description.generation
    );
    for (i, t) in description.tracks.iter().enumerate() {
        out.push_str(&format!(
            "m={} {} RTP/AVP {}\r\na=rtpmap:{} {}\r\n",
            if t.video { "video" } else { "audio" },
            address.port() + 2 * i as u16,
            t.payload,
            t.payload,
            t.encoding
        ));
        if !t.fmtp.is_empty() {
            out.push_str(&format!("a=fmtp:{} {}\r\n", t.payload, t.fmtp));
        }
    }
    out
}
pub async fn run(state: Arc<State>, worker: Arc<Worker>, cancel: CancellationToken) {
    if state.definition.disabled {
        *state.stats.status.lock().unwrap() = "disabled";
        return;
    }
    *state.stats.status.lock().unwrap() = "starting";
    let child = cancel.child_token();
    let mut feedback = tokio::task::JoinSet::new();
    let result = send(&state, &worker, &child, &mut feedback).await;
    child.cancel();
    while feedback.join_next().await.is_some() {}
    *state.sdp.lock().unwrap() = None;
    *state.stats.status.lock().unwrap() = if result.is_err() { "failed" } else { "stopped" };
    if let Err(error) = result {
        *state.stats.error.lock().unwrap() = Some(error);
    }
}
fn current(worker: &Worker, description: &Description) -> bool {
    !worker.is_closed() && worker.wire.rtp.generation_is(description.generation)
}
async fn write(
    socket: &tokio::net::UdpSocket,
    body: &[u8],
    to: Option<SocketAddr>,
    cancel: &CancellationToken,
    worker: &Worker,
    description: &Description,
) -> Result<Sent, &'static str> {
    tokio::select! {biased;_=cancel.cancelled()=>Ok(Sent::Cancelled),result=tokio::time::timeout(Duration::from_secs(2),async {
     loop {
      socket.writable().await.map_err(|_|"Elementary RTP send failed")?;
      if cancel.is_cancelled(){return Ok(Sent::Cancelled);}
      if !current(worker,description){return Err("Elementary RTP generation changed");}
      let sent=match to{Some(addr)=>socket.try_send_to(body,addr),None=>socket.try_send(body)};
      match sent{Ok(n)if n==body.len()=>return Ok(Sent::Packet),Ok(_)=>return Err("Elementary RTP datagram truncated"),Err(e)if e.kind()==std::io::ErrorKind::WouldBlock=>continue,Err(e)if e.kind()==std::io::ErrorKind::ConnectionRefused=>return Ok(Sent::Unavailable),Err(_)=>return Err("Elementary RTP send failed")}
     }
    })=>result.map_err(|_|"Elementary RTP send stalled")?}
}
async fn send(
    state: &Arc<State>,
    worker: &Arc<Worker>,
    cancel: &CancellationToken,
    feedback: &mut tokio::task::JoinSet<()>,
) -> Result<(), &'static str> {
    let snapshot=tokio::time::timeout(Duration::from_secs(15),async{loop{if let Ok(snapshot)=worker.wire.rtp.play_snapshot(){return Some(snapshot);}tokio::select!{_=cancel.cancelled()=>return None,_=worker.closed()=>return None,_=tokio::time::sleep(Duration::from_millis(10))=>{}}}}).await.map_err(|_|"Elementary RTP media not ready")?;
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    let description = snapshot.description;
    if description.tracks.is_empty() || description.tracks.len() > 8 {
        return Err("Elementary RTP requires 1..8 supported tracks");
    }
    let mut receiver = snapshot.receiver;
    let mut initial: VecDeque<_> = snapshot
        .packets
        .into_iter()
        .zip(snapshot.decode_times)
        .collect();
    let mut lanes = vec![];
    for (i, track) in description.tracks.iter().enumerate() {
        let mut settings = state.definition.settings.clone();
        settings
            .address
            .set_port(settings.address.port() + 2 * i as u16);
        let target = SocketAddr::new(settings.address.ip(), settings.address.port() + 1);
        let pair = Arc::new(
            Pair::send(&settings)
                .await
                .map_err(|_| "Elementary RTP output socket failed")?,
        );
        let c = cancel.clone();
        let tx = pair.clone();
        let state = state.clone();
        let destination = settings.address;
        feedback.spawn(async move{let mut b=[0;2049];let mut peer=None;loop{let result=tokio::select!{biased;_=c.cancelled()=>return,result=tx.rtcp.recv_from(&mut b)=>result};let (n,addr)=match result{Ok(v)=>v,Err(e)if e.kind()==std::io::ErrorKind::ConnectionRefused=>{state.stats.unreachable.fetch_add(1,Ordering::Relaxed);continue;},Err(_)=>return};if addr.port()!=destination.port()+1 || addr.ip().is_multicast() || addr.ip().is_unspecified() || (!destination.ip().is_multicast() && addr.ip()!=destination.ip()) || peer.is_some_and(|p|p!=addr){state.stats.foreign.fetch_add(1,Ordering::Relaxed);continue;}
        if packet::valid_rtcp(&b[..n]){peer.get_or_insert(addr);state.stats.rtcp.fetch_add(1,Ordering::Relaxed);}else{state.stats.invalid.fetch_add(1,Ordering::Relaxed);}}});
        lanes.push(Lane {
            pair,
            track: track.clone(),
            packets: 0,
            octets: 0,
            // RTP origins belong to the transport session, not the media epoch.
            // A zero first origin also resets FFmpeg's RTP clock initialization.
            stamp_origin: u32::from_be_bytes(
                uuid::Uuid::new_v4().as_bytes()[..4].try_into().unwrap(),
            )
            .max(1),
            target,
        });
    }
    if !current(worker, &description) {
        return Err("Elementary RTP generation changed");
    }
    *state.sdp.lock().unwrap() = Some((
        description.generation,
        sdp(&description, state.definition.settings.address),
    ));
    let mut pacer = crate::rtsp::udp::Pacer::new(state.definition.max_mbps as f64)
        .map_err(|_| "Elementary RTP rate invalid")?;
    let overhead = if state.definition.settings.address.is_ipv4() {
        28
    } else {
        48
    };
    // All related media share one stable decode-time/wall-time mapping.
    // Packet PTS offsets and pacing delays must never re-anchor this mapping.
    let mut epoch: Option<(u64, Instant)> = None;
    let cname = format!("fx-{}", uuid::Uuid::new_v4());
    let mut reports = tokio::time::interval(Duration::from_secs(5));
    reports.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    reports.tick().await;
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        if !current(worker, &description) {
            return Err("Elementary RTP generation changed");
        }
        let item = if let Some(item) = initial.pop_front() {
            item
        } else {
            tokio::select! {_=cancel.cancelled()=>return Ok(()),_=worker.closed()=>return Ok(()),_=feedback.join_next()=>return Err("Elementary RTCP feedback closed"),item=receiver.recv_timed()=>{let item=item.map_err(|_|"Elementary RTP output queue closed or lagged")?;(item.bytes,item.dts)},_=reports.tick()=>{
             let observed = Instant::now(); let wall = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default(); let Some((dts, at)) = epoch else {continue;}; for lane in &lanes{if !current(worker,&description){return Err("Elementary RTP generation changed");}let clock = u128::from(lane.track.clock); let stamp = ((u128::from(dts)*clock/90000) + observed.duration_since(at).as_nanos()*clock/1_000_000_000) as u32; let body=report_at(lane.track.ssrc,stamp.wrapping_add(lane.stamp_origin),lane.packets,lane.octets,wall,&cname);match write(&lane.pair.rtcp,&body,Some(lane.target),cancel,worker,&description).await?{Sent::Cancelled=>return Ok(()),Sent::Unavailable=>{state.stats.unreachable.fetch_add(1,Ordering::Relaxed);},Sent::Packet=>{state.egress.fetch_add((body.len()+overhead) as u64,Ordering::Relaxed);}}}continue;
            }}
        };
        let (bytes, dts) = item;
        if bytes.len() < 16 {
            return Err("Elementary RTP packet framing invalid");
        }
        let id = u32::from_be_bytes(bytes[..4].try_into().unwrap());
        let lane = lanes
            .iter_mut()
            .find(|l| l.track.id == id)
            .ok_or("Elementary RTP track changed")?;
        let original = &bytes[4..];
        if original.len() > 1600 {
            return Err("Elementary RTP packet exceeds datagram bound");
        }
        // Payloads still come from the shared packetizer. Only the small, bounded
        // transport datagram changes its session clock; no per-output encoding.
        let mut datagram = [0; 1600];
        let body = &mut datagram[..original.len()];
        body.copy_from_slice(original);
        let stamp =
            u32::from_be_bytes(body[4..8].try_into().unwrap()).wrapping_add(lane.stamp_origin);
        body[4..8].copy_from_slice(&stamp.to_be_bytes());
        if body[1] & 127 != lane.track.payload
            || u32::from_be_bytes(body[8..12].try_into().unwrap()) != lane.track.ssrc
        {
            return Err("Elementary RTP packet generation changed");
        }
        let due = pacer
            .ready_at(dts, body.len() + overhead, tokio::time::Instant::now())
            .map_err(|_| "Elementary RTP output pacing failed")?;
        tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),_=worker.closed()=>return Ok(()),_=tokio::time::sleep_until(due)=>{}}
        if receiver.is_lagged() {
            return Err("Elementary RTP output queue lagged");
        }
        if !current(worker, &description) {
            return Err("Elementary RTP generation changed");
        }
        epoch.get_or_insert((dts, Instant::now()));
        match write(&lane.pair.rtp, body, None, cancel, worker, &description).await? {
            Sent::Cancelled => return Ok(()),
            Sent::Unavailable => {
                state.stats.unreachable.fetch_add(1, Ordering::Relaxed);
                *state.stats.status.lock().unwrap() = "receiver_unavailable";
                continue;
            }
            Sent::Packet => {}
        }
        if cancel.is_cancelled() {
            return Ok(());
        }
        state
            .egress
            .fetch_add((body.len() + overhead) as u64, Ordering::Relaxed);
        pacer.sent(body.len() + overhead, tokio::time::Instant::now());
        lane.packets = lane.packets.wrapping_add(1);
        lane.octets = lane.octets.wrapping_add((body.len() - 12) as u32);
        state.stats.packets.fetch_add(1, Ordering::Relaxed);
        state
            .stats
            .bytes
            .fetch_add((body.len() - 12) as u64, Ordering::Relaxed);
        *state.stats.status.lock().unwrap() = "sending";
    }
}
