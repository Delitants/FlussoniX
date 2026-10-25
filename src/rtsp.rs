//! RTSP/1.0 live playback using the shared H.264/HEVC/AAC/MPEG audio RTP profile.
pub mod protocol;
mod publication;
pub mod tls;
pub mod udp;
use crate::{
    playback_auth::ViewerRequest,
    rtp::{Packet, Receiver},
    server::{App, rtsp_access::Playback},
};
use protocol::{Event, Request, Transport};
use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
const PUBLIC: &str = "OPTIONS, DESCRIBE, SETUP, PLAY, ANNOUNCE, RECORD, GET_PARAMETER, TEARDOWN";
const MAX_PREAUTH_CONNECTIONS: usize = 64;
const MAX_PREAUTH_PER_IP: usize = 16;
struct PreAuthGate {
    slots: Arc<Semaphore>,
    by_ip: Mutex<HashMap<IpAddr, usize>>,
}
impl PreAuthGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(MAX_PREAUTH_CONNECTIONS)),
            by_ip: Mutex::new(HashMap::new()),
        })
    }
    fn acquire(self: &Arc<Self>, ip: IpAddr) -> Option<PreAuthLease> {
        let permit = self.slots.clone().try_acquire_owned().ok()?;
        let mut by_ip = self.by_ip.lock().unwrap_or_else(|e| e.into_inner());
        let count = by_ip.entry(ip).or_default();
        if *count >= MAX_PREAUTH_PER_IP {
            return None;
        }
        *count += 1;
        Some(PreAuthLease {
            gate: self.clone(),
            ip,
            _permit: permit,
            started: Instant::now(),
        })
    }
}
pub(super) struct PreAuthLease {
    gate: Arc<PreAuthGate>,
    ip: IpAddr,
    _permit: OwnedSemaphorePermit,
    started: Instant,
}
impl Drop for PreAuthLease {
    fn drop(&mut self) {
        let mut by_ip = self.gate.by_ip.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = by_ip.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                by_ip.remove(&self.ip);
            }
        }
    }
}
struct ReaderTask(JoinHandle<()>);
impl Drop for ReaderTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
enum Delivery {
    Tcp(Transport),
    Udp {
        lease: udp::Lease,
        ports: protocol::ClientPorts,
    },
}
#[derive(Clone)]
struct UdpOptions {
    pool: Arc<udp::Pool>,
    rate: f64,
}
struct Sender {
    delivery: Delivery,
    ssrc: u32,
    packets: u32,
    octets: u32,
    report_clock: Option<ReportClock>,
}
#[derive(Clone, Copy)]
struct ReportEpoch {
    instant: std::time::Instant,
    ntp: u64,
}
impl ReportEpoch {
    fn sample() -> Self {
        let before = std::time::Instant::now();
        let ntp = ntp_timestamp(SystemTime::now());
        let after = std::time::Instant::now();
        Self {
            instant: before + (after - before) / 2,
            ntp,
        }
    }
}
struct ReportClock {
    ntp_origin: u64,
    origin_ticks: u128,
}
impl ReportClock {
    fn new(origin_ticks: u128, ntp_origin: u64) -> Self {
        Self {
            ntp_origin,
            origin_ticks,
        }
    }
    fn at(&self, ticks: u128, rate: u32) -> u64 {
        let elapsed = ticks as i128 - self.origin_ticks as i128;
        // One NTP tick is 2^-32 seconds. Derive every report from the same
        // RTP origin so receiver rescaling cannot shift media by one tick.
        (i128::from(self.ntp_origin) + (elapsed * (1i128 << 32)).div_euclid(i128::from(rate)))
            as u64
    }
}
struct Session {
    playback: Playback,
    url: url::Url,
    id: String,
    senders: HashMap<u32, Sender>,
    report_epoch: Option<ReportEpoch>,
    playing: bool,
    initial: VecDeque<Packet>,
    pending: Option<Packet>,
    pacer: Option<udp::Pacer>,
    receiver: Option<Receiver>,
    rtp_info: String,
    feedback_cursor: usize,
}
struct Reply {
    code: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    close: bool,
}
impl Reply {
    fn code(code: u16) -> Self {
        Self {
            code,
            headers: vec![],
            body: vec![],
            close: false,
        }
    }
}
/// Binds independently of HTTP; never enables RTSP implicitly or touches port 554.
pub async fn serve(
    listener: TcpListener,
    app: Arc<App>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    serve_with_udp(listener, app, cancel, None, 100.0).await
}
pub async fn serve_with_udp(
    listener: TcpListener,
    app: Arc<App>,
    cancel: CancellationToken,
    pool: Option<Arc<udp::Pool>>,
    rate: f64,
) -> std::io::Result<()> {
    serve_connections(listener, app, cancel, pool, rate, None).await
}
/// Optional TLS listener; media stays encrypted through TCP interleaving.
pub async fn serve_tls(
    listener: TcpListener,
    app: Arc<App>,
    cancel: CancellationToken,
    config: Arc<tokio_rustls::rustls::ServerConfig>,
) -> std::io::Result<()> {
    serve_connections(
        listener,
        app,
        cancel,
        None,
        100.0,
        Some(tokio_rustls::TlsAcceptor::from(config)),
    )
    .await
}
async fn serve_connections(
    listener: TcpListener,
    app: Arc<App>,
    cancel: CancellationToken,
    pool: Option<Arc<udp::Pool>>,
    rate: f64,
    tls: Option<tokio_rustls::TlsAcceptor>,
) -> std::io::Result<()> {
    udp::Pacer::new(rate).map_err(std::io::Error::other)?;
    let udp = pool.map(|pool| UdpOptions { pool, rate });
    let permits = Arc::new(Semaphore::new(256));
    let preauth = PreAuthGate::new();
    let mut clients = JoinSet::new();
    let result = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break Ok(()),
            Some(_) = clients.join_next(), if !clients.is_empty() => {},
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => break Err(error),
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    continue;
                };
                let Some(preauth_lease) = preauth.acquire(peer.ip()) else {
                    continue;
                };
                let app = app.clone();
                let cancel = cancel.clone();
                let udp = udp.clone();
                let tls = tls.clone();
                clients.spawn(async move {
                    let _permit = permit;
                    if socket.set_nodelay(true).is_err() {
                        return;
                    }
                    if let Some(tls) = tls {
                        let setup_deadline = (preauth_lease.started + Duration::from_secs(30))
                            .min(Instant::now() + Duration::from_secs(8));
                        let accepted = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return,
                            result = tokio::time::timeout_at(setup_deadline, tls.accept(socket)) => result,
                        };
                        if let Ok(Ok(stream)) = accepted {
                            let _ = connection(stream, peer, app, cancel, udp, true, preauth_lease).await;
                        }
                    } else {
                        let _ = connection(socket, peer, app, cancel, udp, false, preauth_lease).await;
                    }
                });
            },
        }
    };
    cancel.cancel();
    let drain = async { while clients.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(3), drain)
        .await
        .is_err()
    {
        clients.abort_all();
        while clients.join_next().await.is_some() {}
    }
    result
}
enum Next {
    Control(Option<Result<Event, protocol::Error>>),
    Media(Result<Option<Packet>, tokio::sync::broadcast::error::RecvError>),
    Feedback(bool),
    Check,
    Report,
    Close,
}
async fn connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static>(
    socket: S,
    peer: SocketAddr,
    app: Arc<App>,
    cancel: CancellationToken,
    udp: Option<UdpOptions>,
    secure: bool,
    preauth_lease: PreAuthLease,
) -> std::io::Result<()> {
    let mut preauth_lease = Some(preauth_lease);
    let (read, mut write) = tokio::io::split(socket);
    let (tx, mut controls) = mpsc::channel(8);
    let _reader = ReaderTask(tokio::spawn(async move {
        let mut read = BufReader::new(read);
        loop {
            let event = protocol::read_event(&mut read).await;
            let failed = event.is_err();
            if tx.send(event).await.is_err() || failed {
                break;
            }
        }
    }));
    let mut session: Option<Session> = None;
    let mut last_control = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut reports = tokio::time::interval(Duration::from_secs(5));
    reports.tick().await;
    loop {
        let mut deadline = last_control
            + Duration::from_secs(if session.as_ref().is_some_and(|s| !s.senders.is_empty()) {
                60
            } else {
                30
            });
        if let Some(lease) = &preauth_lease {
            deadline = deadline.min(lease.started + Duration::from_secs(30));
        }
        let due = match session
            .as_mut()
            .and_then(|s| s.pending.as_ref().zip(s.pacer.as_mut()))
        {
            Some((packet, pacer)) => {
                Some(pacer.ready_at(packet.dts, packet.bytes.len() - 4, Instant::now())?)
            }
            None => None,
        };
        let next = {
            let (playback, initial, receiver, playing, pending, senders, feedback_cursor) =
                match session.as_mut() {
                    Some(s) => (
                        Some(&s.playback),
                        Some(&mut s.initial),
                        Some(&mut s.receiver),
                        s.playing,
                        s.pending.is_some(),
                        Some(&s.senders),
                        Some(&mut s.feedback_cursor),
                    ),
                    None => (None, None, None, false, false, None, None),
                };
            let grant = async {
                if let Some(p) = playback {
                    p.cancelled().await
                } else {
                    std::future::pending::<()>().await
                }
            };
            let closed = async {
                if let Some(p) = playback {
                    p.worker.closed().await
                } else {
                    std::future::pending::<()>().await
                }
            };
            let media = async {
                if playing {
                    if pending {
                        if let Some(at) = due {
                            tokio::time::sleep_until(at).await;
                        }
                        return Ok(None);
                    }
                    if let Some(initial) = initial {
                        if let Some(packet) = initial.pop_front() {
                            return Ok(Some(packet));
                        }
                    }
                    if let Some(Some(rx)) = receiver {
                        return rx.recv_timed().await.map(Some);
                    }
                }
                std::future::pending().await
            };
            let feedback = async {
                if let Some((senders, cursor)) = senders.zip(feedback_cursor) {
                    receive_reports(senders, cursor).await
                } else {
                    std::future::pending().await
                }
            };
            tokio::select! {biased;
                _=cancel.cancelled()=>Next::Close,_=grant=>Next::Close,_=closed=>Next::Close,
                _=tokio::time::sleep_until(deadline)=>Next::Close,
                event=controls.recv()=>Next::Control(event),_=tick.tick()=>Next::Check,
                _=reports.tick()=>Next::Report,packet=media=>Next::Media(packet),accepted=feedback=>Next::Feedback(accepted)
            }
        };
        match next {
            Next::Close => break,
            Next::Check => {
                if let Some(s) = &session {
                    if s.receiver.as_ref().is_some_and(Receiver::is_lagged)
                        || !app.rtsp_current(&s.playback).await
                    {
                        break;
                    }
                }
            }
            Next::Feedback(true) => last_control = Instant::now(),
            Next::Feedback(false) => {}
            Next::Control(None) => break,
            Next::Control(Some(Err(error))) => {
                let _ = preauth_bounded_write(
                    &mut write,
                    &protocol::response(error.code, error.cseq.unwrap_or(0), &[], &[]),
                    &cancel,
                    session.as_ref(),
                    preauth_lease.as_ref(),
                )
                .await;
                break;
            }
            Next::Control(Some(Ok(Event::Interleaved(channel, body)))) => {
                if !session.as_ref().is_some_and(|s| {
                    s.senders
                        .values()
                        .any(|t| matches!(&t.delivery,Delivery::Tcp(p) if p.rtcp==channel))
                }) || !valid_rtcp(&body)
                {
                    break;
                }
                last_control = Instant::now();
            }
            Next::Control(Some(Ok(Event::Request(request)))) => {
                last_control = Instant::now();
                if request.method == "ANNOUNCE" && session.is_none() {
                    publication::receive(
                        request,
                        &mut controls,
                        &mut write,
                        &app,
                        peer,
                        &cancel,
                        secure,
                        udp.as_ref().map(|o| o.pool.clone()),
                        &mut preauth_lease,
                    )
                    .await?;
                    break;
                }
                let reply = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = async { tokio::time::sleep_until(preauth_lease.as_ref().unwrap().started + Duration::from_secs(30)).await }, if preauth_lease.is_some() => break,
                    reply = handle(&request,&mut session,&app,peer,udp.as_ref(),secure) => reply
                };
                if session.is_some() {
                    drop(preauth_lease.take());
                }
                let bytes =
                    protocol::response(reply.code, request.cseq, &reply.headers, &reply.body);
                preauth_bounded_write(
                    &mut write,
                    &bytes,
                    &cancel,
                    session.as_ref(),
                    preauth_lease.as_ref(),
                )
                .await?;
                if reply.close {
                    break;
                }
            }
            Next::Media(Err(_)) => break,
            Next::Media(Ok(packet)) => {
                let Some(s) = &mut session else {
                    continue;
                };
                let packet = match packet {
                    Some(packet) => {
                        let id = u32::from_be_bytes(packet.bytes[..4].try_into().unwrap());
                        if !s.senders.contains_key(&id) {
                            continue;
                        }
                        if s.pacer.is_some() {
                            s.pending = Some(packet);
                            continue;
                        }
                        packet
                    }
                    None => s.pending.take().unwrap(),
                };
                let id = u32::from_be_bytes(packet.bytes[..4].try_into().unwrap());
                let Some(sender) = s.senders.get(&id) else {
                    continue;
                };
                let body = &packet.bytes[4..];
                let bytes = deliver(sender, &mut write, body, false, &cancel, s).await?;
                let is_udp = matches!(sender.delivery, Delivery::Udp { .. });
                let sender = s.senders.get_mut(&id).unwrap();
                sender.packets = sender.packets.wrapping_add(1);
                sender.octets = sender.octets.wrapping_add((body.len() - 12) as u32);
                if let Some(pacer) = &mut s.pacer {
                    pacer.sent(body.len(), Instant::now());
                }
                s.playback.add_bytes(bytes);
                app.rtsp_egress.fetch_add(bytes as u64, Ordering::Relaxed);
                if is_udp {
                    app.rtsp_udp_egress
                        .fetch_add(bytes as u64, Ordering::Relaxed);
                }
            }
            Next::Report => {
                if let Some(s) = &mut session {
                    if s.playing {
                        if s.report_epoch.is_none()
                            && s.senders.values().any(|sender| sender.packets > 0)
                        {
                            s.report_epoch = Some(ReportEpoch::sample());
                        }
                        let sample = std::time::Instant::now();
                        for track in &s.playback.description.tracks {
                            let ticks = s
                                .playback
                                .worker
                                .wire
                                .rtp
                                .clock_at(track.id, sample)
                                .unwrap_or(0);
                            let stamp = ticks as u32;
                            let Some(sender) = s.senders.get_mut(&track.id) else {
                                continue;
                            };
                            let Some(epoch) = s.report_epoch else {
                                continue;
                            };
                            let origin_ticks = s
                                .playback
                                .worker
                                .wire
                                .rtp
                                .clock_at(track.id, epoch.instant)
                                .unwrap_or(ticks);
                            let clock = sender
                                .report_clock
                                .get_or_insert_with(|| ReportClock::new(origin_ticks, epoch.ntp));
                            if sender.packets == 0 {
                                continue;
                            }
                            let ntp = clock.at(ticks, track.clock);
                            let body = sender_report_ntp(
                                sender.ssrc,
                                stamp,
                                sender.packets,
                                sender.octets,
                                ntp,
                            );
                            let sender = s.senders.get(&track.id).unwrap();
                            let bytes =
                                deliver(sender, &mut write, &body, true, &cancel, s).await?;
                            app.rtsp_egress.fetch_add(bytes as u64, Ordering::Relaxed);
                            s.playback.add_bytes(bytes);
                            if matches!(sender.delivery, Delivery::Udp { .. }) {
                                app.rtsp_udp_egress
                                    .fetch_add(bytes as u64, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
async fn receive_reports(senders: &HashMap<u32, Sender>, cursor: &mut usize) -> bool {
    // SDP admits at most eight tracks. Register every socket with the same
    // connection waker, using one fixed receive buffer and no per-track tasks.
    let mut tracks = [None; 8];
    let mut count = 0;
    for sender in senders.values() {
        if matches!(sender.delivery, Delivery::Udp { .. }) {
            tracks[count] = Some(sender);
            count += 1;
        }
    }
    if count == 0 {
        return std::future::pending().await;
    }
    let mut buffer = [0; 8193];
    std::future::poll_fn(|cx| {
        let start = *cursor % count;
        for offset in 0..count {
            let index = (start + offset) % count;
            let sender = tracks[index].unwrap();
            let Delivery::Udp { lease, .. } = &sender.delivery else {
                unreachable!()
            };
            if let std::task::Poll::Ready(result) = lease.poll_recv_rtcp(cx, &mut buffer) {
                // Invalid or foreign traffic consumes this track's turn too.
                *cursor = (index + 1) % count;
                return std::task::Poll::Ready(match result {
                    Ok(Some(n)) => udp::valid_receiver_report(&buffer[..n], sender.ssrc),
                    _ => false,
                });
            }
        }
        std::task::Poll::Pending
    })
    .await
}
async fn deliver<W: tokio::io::AsyncWrite + Unpin>(
    sender: &Sender,
    writer: &mut W,
    body: &[u8],
    rtcp: bool,
    cancel: &CancellationToken,
    session: &Session,
) -> std::io::Result<usize> {
    if !session.playback.current() {
        return Err(std::io::ErrorKind::Interrupted.into());
    }
    match &sender.delivery {
        Delivery::Tcp(transport) => {
            let mut data = vec![b'$', if rtcp { transport.rtcp } else { transport.rtp }];
            data.extend((body.len() as u16).to_be_bytes());
            data.extend(body);
            bounded_write(writer, &data, cancel, Some(session)).await?;
            Ok(data.len())
        }
        Delivery::Udp { lease, .. } => {
            let send = async {
                if rtcp {
                    lease.send_rtcp(body).await
                } else {
                    lease.send_rtp(body).await
                }
            };
            let result = tokio::select! {biased;_=cancel.cancelled()=>return Err(std::io::ErrorKind::Interrupted.into()),_=session.playback.cancelled()=>return Err(std::io::ErrorKind::Interrupted.into()),_=session.playback.worker.closed()=>return Err(std::io::ErrorKind::Interrupted.into()),result=tokio::time::timeout(Duration::from_secs(2),send)=>result.unwrap_or_else(|_|Err(std::io::ErrorKind::TimedOut.into()))};
            let n = result?;
            if !session.playback.current() {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
            if n != body.len() {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            Ok(n)
        }
    }
}
async fn bounded_write<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    cancel: &CancellationToken,
    session: Option<&Session>,
) -> std::io::Result<()> {
    if session.is_some_and(|s| !s.playback.current()) {
        return Err(std::io::ErrorKind::Interrupted.into());
    }
    let revoked = async {
        if let Some(s) = session {
            s.playback.cancelled().await
        } else {
            std::future::pending::<()>().await
        }
    };
    let closed = async {
        if let Some(s) = session {
            s.playback.worker.closed().await
        } else {
            std::future::pending::<()>().await
        }
    };
    let result = tokio::select! {biased;_=cancel.cancelled()=>Err(std::io::ErrorKind::Interrupted.into()),_=revoked=>Err(std::io::ErrorKind::Interrupted.into()),_=closed=>Err(std::io::ErrorKind::Interrupted.into()),result=tokio::time::timeout(Duration::from_secs(2),writer.write_all(bytes))=>result.unwrap_or_else(|_|Err(std::io::ErrorKind::TimedOut.into()))};
    if session.is_some_and(|s| !s.playback.current()) {
        return Err(std::io::ErrorKind::Interrupted.into());
    }
    result
}
async fn preauth_bounded_write<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    cancel: &CancellationToken,
    session: Option<&Session>,
    lease: Option<&PreAuthLease>,
) -> std::io::Result<()> {
    if let Some(lease) = lease {
        tokio::select! {
            biased;
            _=tokio::time::sleep_until(lease.started+Duration::from_secs(30))=>Err(std::io::ErrorKind::TimedOut.into()),
            result=bounded_write(writer,bytes,cancel,session)=>result,
        }
    } else {
        bounded_write(writer, bytes, cancel, session).await
    }
}
fn location(uri: &str) -> Result<(url::Url, String), u16> {
    let url = url::Url::parse(uri).map_err(|_| 400u16)?;
    if !matches!(url.scheme(), "rtsp" | "rtsps")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(400);
    }
    let name = percent_encoding::percent_decode_str(url.path().trim_matches('/'))
        .decode_utf8()
        .map_err(|_| 400u16)?
        .into_owned();
    let stream = name
        .rsplit_once("/trackID=")
        .filter(|(_, id)| id.parse::<u32>().is_ok())
        .map(|(stream, _)| stream)
        .unwrap_or(&name);
    crate::config::valid_name(stream).map_err(|_| 400u16)?;
    let mut keys = std::collections::HashSet::new();
    if url.query_pairs().any(|(k, _)| !keys.insert(k.into_owned())) {
        return Err(400);
    }
    Ok((url, name))
}
fn bound(s: &Session, r: &Request, track: Option<u32>) -> Result<(), u16> {
    let (url, name) = location(&r.uri)?;
    if url.host_str() != s.url.host_str()
        || url.port_or_known_default() != s.url.port_or_known_default()
    {
        return Err(404);
    }
    let expected = track
        .map(|id| format!("{}/trackID={id}", s.playback.name))
        .unwrap_or_else(|| s.playback.name.clone());
    if name != expected {
        return Err(404);
    }
    if url.query().is_some() && url.query() != s.url.query() {
        return Err(403);
    }
    Ok(())
}
fn session_matches(s: &Session, r: &Request) -> bool {
    r.headers
        .get("session")
        .is_some_and(|id| id.split(';').next() == Some(s.id.as_str()))
}
fn session_header(s: &Session) -> (&'static str, String) {
    ("Session", format!("{};timeout=60", s.id))
}
async fn handle(
    r: &Request,
    session: &mut Option<Session>,
    app: &Arc<App>,
    peer: SocketAddr,
    udp: Option<&UdpOptions>,
    secure: bool,
) -> Reply {
    if !secure && url::Url::parse(&r.uri).is_ok_and(|url| url.scheme() == "rtsps") {
        return Reply::code(400);
    }
    if r.headers.contains_key("require") || r.headers.contains_key("proxy-require") {
        return Reply::code(551);
    }
    if r.method == "OPTIONS" {
        let mut reply = Reply::code(200);
        reply.headers.push(("Public", PUBLIC.into()));
        return reply;
    }
    if r.method == "DESCRIBE" {
        if session.is_some() {
            return Reply::code(455);
        }
        if r.headers
            .get("accept")
            .is_some_and(|a| a != "application/sdp" && a != "*/*")
        {
            return Reply::code(415);
        }
        let (url, name) = match location(&r.uri) {
            Ok(location) => location,
            Err(code) => return Reply::code(code),
        };
        if crate::config::valid_name(&name).is_err() {
            return Reply::code(400);
        }
        let viewer = ViewerRequest {
            name,
            proto: "rtsp".into(),
            ip: peer.ip().to_string(),
            token: url
                .query_pairs()
                .find_map(|(k, v)| (k == "token").then(|| v.into_owned()))
                .unwrap_or_default(),
            qs: url.query().unwrap_or("").into(),
            user_agent: r.headers.get("user-agent").cloned().unwrap_or_default(),
            referer: r.headers.get("referer").cloned().unwrap_or_default(),
            host: url.host_str().unwrap().into(),
            finite_media: false,
        };
        let playback = match app.rtsp_admit(viewer, &url, secure).await {
            Ok(crate::server::rtsp_access::Admission::Playback(p)) => p,
            Ok(crate::server::rtsp_access::Admission::Redirect(target)) => {
                return redirect::response(target, &url, secure);
            }
            Err(code) => return Reply::code(code),
        };
        let body = playback.description.sdp().into_bytes();
        let mut base = url.clone();
        base.set_query(None);
        base.set_path(&format!("{}/", url.path().trim_end_matches('/')));
        *session = Some(Session {
            playback,
            url,
            id: uuid::Uuid::new_v4().simple().to_string(),
            senders: HashMap::new(),
            report_epoch: None,
            playing: false,
            initial: VecDeque::new(),
            pending: None,
            pacer: None,
            receiver: None,
            rtp_info: String::new(),
            feedback_cursor: 0,
        });
        let mut reply = Reply::code(200);
        reply.headers = vec![
            ("Content-Type", "application/sdp".into()),
            ("Content-Base", base.to_string()),
        ];
        reply.body = body;
        return reply;
    }
    if ["ANNOUNCE", "RECORD"].contains(&r.method.as_str()) {
        return Reply::code(501);
    }
    if r.method == "PAUSE" {
        return Reply::code(405);
    }
    if !["SETUP", "PLAY", "GET_PARAMETER", "TEARDOWN"].contains(&r.method.as_str()) {
        return Reply::code(405);
    }
    let Some(s) = session.as_mut() else {
        return Reply::code(454);
    };
    if !app.rtsp_current(&s.playback).await {
        let mut reply = Reply::code(403);
        reply.close = true;
        return reply;
    }
    if r.method == "SETUP" {
        if s.playing {
            return Reply::code(455);
        }
        if (!s.senders.is_empty() || r.headers.contains_key("session")) && !session_matches(s, r) {
            return Reply::code(454);
        }
        let id = match location(&r.uri)
            .ok()
            .and_then(|(_, name)| name.rsplit_once("/trackID=")?.1.parse::<u32>().ok())
        {
            Some(id) if s.playback.description.tracks.iter().any(|t| t.id == id) => id,
            _ => return Reply::code(404),
        };
        if let Err(code) = bound(s, r, Some(id)) {
            return Reply::code(code);
        }
        let offer = match r
            .headers
            .get("transport")
            .ok_or(461u16)
            .and_then(|v| protocol::Offer::parse(v))
        {
            Ok(v) => v,
            Err(code) => return Reply::code(code),
        };
        if s.senders.iter().any(|(old, sender)| {
            *old != id
                && match (&sender.delivery, offer) {
                    (Delivery::Tcp(a), protocol::Offer::Tcp(b)) => {
                        [a.rtp, a.rtcp].iter().any(|c| *c == b.rtp || *c == b.rtcp)
                    }
                    (Delivery::Udp { ports: a, .. }, protocol::Offer::Udp(b)) => {
                        [a.rtp, a.rtcp].iter().any(|p| *p == b.rtp || *p == b.rtcp)
                    }
                    _ => true,
                }
        }) {
            return Reply::code(461);
        }
        if s.senders.get(&id).is_some_and(|sender| {
            matches!(
                (&sender.delivery, offer),
                (Delivery::Tcp(_), protocol::Offer::Udp(_))
                    | (Delivery::Udp { .. }, protocol::Offer::Tcp(_))
            )
        }) {
            return Reply::code(461);
        }
        let ssrc = s
            .playback
            .description
            .tracks
            .iter()
            .find(|t| t.id == id)
            .unwrap()
            .ssrc;
        let delivery = match offer {
            protocol::Offer::Tcp(transport) => Delivery::Tcp(transport),
            protocol::Offer::Udp(ports) => {
                let Some(options) = udp else {
                    return Reply::code(461);
                };
                if let Some(Sender {
                    delivery: Delivery::Udp { lease, ports: old },
                    ..
                }) = s.senders.get_mut(&id)
                {
                    if lease.set_client_ports(ports).is_err() {
                        return Reply::code(503);
                    }
                    *old = ports;
                    let mut reply = Reply::code(200);
                    reply.headers = vec![
                        session_header(s),
                        ("Transport", transport_header(&s.senders[&id])),
                    ];
                    return reply;
                }
                match options.pool.lease(peer.ip(), ports).await {
                    Ok(lease) => Delivery::Udp { lease, ports },
                    Err(e) => {
                        return Reply::code(if e.kind() == std::io::ErrorKind::WouldBlock {
                            453
                        } else {
                            503
                        });
                    }
                }
            }
        };
        s.senders.insert(
            id,
            Sender {
                delivery,
                packets: 0,
                octets: 0,
                ssrc,
                report_clock: None,
            },
        );
        let mut reply = Reply::code(200);
        reply.headers = vec![
            session_header(s),
            ("Transport", transport_header(&s.senders[&id])),
        ];
        return reply;
    }

    if !session_matches(s, r) {
        return Reply::code(454);
    }
    if let Err(code) = bound(s, r, None) {
        return Reply::code(code);
    }
    if r.method == "PLAY" {
        if s.senders.is_empty() {
            return Reply::code(455);
        }
        if r.headers.get("range").is_some_and(|v| !live_range(v)) {
            return Reply::code(457);
        }
        if !s.playing {
            let snapshot = match s.playback.worker.wire.rtp.play_snapshot() {
                Ok(v) => v,
                Err(_) => return Reply::code(415),
            };
            if snapshot.description != s.playback.description {
                return Reply::code(503);
            }
            s.rtp_info = snapshot
                .positions
                .iter()
                .filter(|(id, _, _)| s.senders.contains_key(id))
                .map(|(id, seq, stamp)| {
                    let mut url = s.url.clone();
                    url.set_query(None);
                    url.set_path(&format!(
                        "{}/trackID={id}",
                        s.url.path().trim_end_matches('/')
                    ));
                    format!("url={url};seq={seq};rtptime={stamp}")
                })
                .collect::<Vec<_>>()
                .join(",");
            s.initial = snapshot
                .packets
                .into_iter()
                .zip(snapshot.decode_times)
                .map(|(bytes, dts)| Packet { bytes, dts })
                .collect();
            if s.senders
                .values()
                .any(|sender| matches!(sender.delivery, Delivery::Udp { .. }))
            {
                s.pacer = Some(udp::Pacer::new(udp.unwrap().rate).unwrap());
            }
            s.receiver = Some(snapshot.receiver);
            s.playback.attach();
            s.playing = true;
        }
        let mut reply = Reply::code(200);
        reply.headers = vec![
            session_header(s),
            ("Range", "npt=0-".into()),
            ("RTP-Info", s.rtp_info.clone()),
        ];
        return reply;
    }
    let mut reply = Reply::code(200);
    reply.headers.push(session_header(s));
    if r.method == "TEARDOWN" {
        *session = None;
        reply.close = true;
    }
    reply
}
fn transport_header(sender: &Sender) -> String {
    match &sender.delivery {
        Delivery::Tcp(t) => format!(
            "RTP/AVP/TCP;unicast;interleaved={}-{};ssrc={:08X}",
            t.rtp, t.rtcp, sender.ssrc
        ),
        Delivery::Udp { ports, lease } => {
            let (a, b) = lease.server_ports();
            format!(
                "RTP/AVP/UDP;unicast;client_port={}-{};server_port={a}-{b};source={};ssrc={:08X}",
                ports.rtp,
                ports.rtcp,
                lease.source_ip(),
                sender.ssrc
            )
        }
    }
}
fn valid_rtcp(body: &[u8]) -> bool {
    let mut cursor = 0;
    while cursor < body.len() {
        let Some(header) = body.get(cursor..cursor + 4) else {
            return false;
        };
        if header[0] >> 6 != 2 || !(192..=223).contains(&header[1]) {
            return false;
        }
        let size = (u16::from_be_bytes([header[2], header[3]]) as usize + 1) * 4;
        cursor += size;
        if cursor > body.len() {
            return false;
        }
    }
    !body.is_empty()
}
fn ntp_timestamp(time: SystemTime) -> u64 {
    let now = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = now.as_secs().wrapping_add(2208988800) as u32;
    let fraction = ((now.subsec_nanos() as u64) << 32).div_euclid(1_000_000_000) as u32;
    (u64::from(seconds) << 32) | u64::from(fraction)
}
pub(crate) fn sender_report_at(
    ssrc: u32,
    stamp: u32,
    packets: u32,
    octets: u32,
    time: SystemTime,
) -> Vec<u8> {
    sender_report_ntp(ssrc, stamp, packets, octets, ntp_timestamp(time))
}
fn sender_report_ntp(ssrc: u32, stamp: u32, packets: u32, octets: u32, ntp: u64) -> Vec<u8> {
    let mut b = vec![0x80, 200, 0, 6];
    for value in [ssrc, (ntp >> 32) as u32, ntp as u32, stamp, packets, octets] {
        b.extend(value.to_be_bytes());
    }
    let name = b"flussonix";
    let mut sdes = vec![0x81, 202, 0, 0];
    sdes.extend(ssrc.to_be_bytes());
    sdes.extend([1, name.len() as u8]);
    sdes.extend(name);
    sdes.push(0);
    while sdes.len() % 4 != 0 {
        sdes.push(0);
    }
    let words = (sdes.len() / 4 - 1) as u16;
    sdes[2..4].copy_from_slice(&words.to_be_bytes());
    b.extend(sdes);
    b
}

fn live_range(value: &str) -> bool {
    let Some(start) = value.strip_prefix("npt=").and_then(|v| v.strip_suffix('-')) else {
        return false;
    };
    start == "now"
        || (!start.is_empty()
            && start.bytes().all(|b| b.is_ascii_digit() || b == b'.')
            && start.parse::<f64>().is_ok_and(|v| v == 0.0))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn expired_preauth_lease_interrupts_stalled_describe_admission() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            crate::server::Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put(
                "streams",
                "owned",
                serde_json::json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let held_startup = app.media.hold_startups_for_test().await;
        let mut lease = PreAuthGate::new()
            .acquire("127.0.0.1".parse().unwrap())
            .unwrap();
        lease.started = Instant::now() - Duration::from_secs(29);
        let (mut client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(connection(
            server,
            "127.0.0.1:12000".parse().unwrap(),
            app.clone(),
            CancellationToken::new(),
            None,
            false,
            lease,
        ));
        client
            .write_all(b"DESCRIBE rtsp://127.0.0.1/owned RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("expired pre-auth lease remained held inside media admission")
            .unwrap()
            .unwrap();
        drop(held_startup);
        app.media.stop_all().await;
    }

    #[tokio::test]
    async fn expired_preauth_lease_interrupts_blocked_announce_reply() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            crate::server::Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut lease = PreAuthGate::new()
            .acquire("127.0.0.1".parse().unwrap())
            .unwrap();
        lease.started = Instant::now() - Duration::from_secs(29);
        let (mut client, server) = tokio::io::duplex(8);
        let task = tokio::spawn(connection(
            server,
            "127.0.0.1:12001".parse().unwrap(),
            app,
            CancellationToken::new(),
            None,
            false,
            lease,
        ));
        client
            .write_all(b"ANNOUNCE rtsp://127.0.0.1/owned RTSP/1.0\r\nCSeq: 1\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(1500), task)
            .await
            .expect("blocked ANNOUNCE response held the pre-auth lease past its deadline")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn expired_preauth_lease_interrupts_blocked_options_reply() {
        use tokio::io::AsyncWriteExt;

        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            crate::server::Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut lease = PreAuthGate::new()
            .acquire("127.0.0.1".parse().unwrap())
            .unwrap();
        lease.started = Instant::now() - Duration::from_secs(29);
        let (mut client, server) = tokio::io::duplex(8);
        let task = tokio::spawn(connection(
            server,
            "127.0.0.1:12002".parse().unwrap(),
            app,
            CancellationToken::new(),
            None,
            false,
            lease,
        ));
        client
            .write_all(b"OPTIONS rtsp://127.0.0.1/owned RTSP/1.0\r\nCSeq: 1\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(1500), task)
            .await
            .expect("blocked OPTIONS response held the pre-auth lease past its deadline")
            .unwrap()
            .expect_err("blocked response should return the deadline error");
    }

    #[test]
    fn sender_reports_keep_ntp_and_rtp_on_one_exact_media_clock() {
        let start = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for rate in [48_000, 90_000] {
            for first in [90_000, u32::MAX - 180_000] {
                let clock = ReportClock::new(u128::from(first), ntp_timestamp(start));
                let initial = clock.at(u128::from(first), rate);
                let mut ticks = u128::from(first);
                let mut elapsed = 0u64;
                for step in [rate * 5, rate * 5, rate * 7, rate * 11] {
                    ticks += u128::from(step);
                    elapsed += u64::from(step);
                    let ntp = clock.at(ticks, rate);
                    let ntp_delta = ntp.wrapping_sub(initial);
                    let decoded =
                        ((u128::from(ntp_delta) * u128::from(rate) + (1 << 31)) >> 32) as u64;
                    assert_eq!(decoded, elapsed, "{rate} Hz at RTP {ticks}");
                    let report = sender_report_ntp(7, ticks as u32, 1, 100, ntp);
                    assert_eq!(
                        u32::from_be_bytes(report[16..20].try_into().unwrap()),
                        ticks as u32
                    );
                    assert_eq!(
                        u32::from_be_bytes(report[8..12].try_into().unwrap()),
                        (ntp >> 32) as u32
                    );
                    assert_eq!(
                        u32::from_be_bytes(report[12..16].try_into().unwrap()),
                        ntp as u32
                    );
                }
            }
        }
    }

    #[test]
    fn sender_reports_share_one_epoch_across_tracks() {
        let start = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let epoch = ReportEpoch {
            instant: std::time::Instant::now(),
            ntp: ntp_timestamp(start),
        };
        let video = ReportClock::new(90_000, epoch.ntp);
        // A stalled sender loop must not make a second track use a later
        // wall-clock origin for the same media instant.
        std::thread::sleep(Duration::from_millis(20));
        let audio = ReportClock::new(48_000, epoch.ntp);
        assert_eq!(video.at(180_000, 90_000), audio.at(96_000, 48_000));
    }

    #[test]
    fn sender_report_survives_delayed_track_start_beyond_signed_rtp_range() {
        let start = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let origin = 90_000;
        let clock = ReportClock::new(origin, ntp_timestamp(start));
        let first = clock.at(origin, 90_000);
        let delayed = origin + 7 * 60 * 60 * 90_000;
        let next = clock.at(delayed, 90_000);
        assert_eq!((next - first) >> 32, 7 * 60 * 60);
    }

    #[test]
    fn live_ranges_accept_decimal_zero_but_not_seeks() {
        for v in ["npt=0-", "npt=0.000-", "npt=now-"] {
            assert!(super::live_range(v));
        }
        for v in ["npt=1-", "npt=NaN-", "npt=0-3", "clock=anything", "npt=-0-"] {
            assert!(!super::live_range(v));
        }
    }
}

#[cfg(test)]
mod playback_feedback_tests;

#[cfg(test)]
mod blocked_write_tests;

pub(crate) mod redirect;
