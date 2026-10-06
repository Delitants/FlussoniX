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
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
const PUBLIC: &str = "OPTIONS, DESCRIBE, SETUP, PLAY, ANNOUNCE, RECORD, GET_PARAMETER, TEARDOWN";
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
}
struct Session {
    playback: Playback,
    url: url::Url,
    id: String,
    senders: HashMap<u32, Sender>,
    playing: bool,
    initial: VecDeque<Packet>,
    pending: Option<Packet>,
    pacer: Option<udp::Pacer>,
    receiver: Option<Receiver>,
    rtp_info: String,
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
    let mut clients = JoinSet::new();
    let result = loop {
        tokio::select! {biased;_=cancel.cancelled()=>break Ok(()),Some(_)=clients.join_next(),if !clients.is_empty()=>{},accepted=listener.accept()=>{match accepted{Ok((socket,peer))=>{if let Ok(permit)=permits.clone().try_acquire_owned(){let app=app.clone();let cancel=cancel.clone();let udp=udp.clone();let tls=tls.clone();clients.spawn(async move{let _permit=permit;if socket.set_nodelay(true).is_err(){return;}
        if let Some(tls)=tls{let accepted=tokio::select!{biased;_=cancel.cancelled()=>return,result=tokio::time::timeout(Duration::from_secs(8),tls.accept(socket))=>result};if let Ok(Ok(stream))=accepted{let _=connection(stream,peer,app,cancel,udp,true).await;}}else{let _=connection(socket,peer,app,cancel,udp,false).await;}});}},Err(e)=>break Err(e)}}}
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
) -> std::io::Result<()> {
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
        let deadline = last_control
            + Duration::from_secs(if session.as_ref().is_some_and(|s| !s.senders.is_empty()) {
                60
            } else {
                30
            });
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
            let (playback, initial, receiver, playing, pending, senders) = match session.as_mut() {
                Some(s) => (
                    Some(&s.playback),
                    Some(&mut s.initial),
                    Some(&mut s.receiver),
                    s.playing,
                    s.pending.is_some(),
                    Some(&s.senders),
                ),
                None => (None, None, None, false, false, None),
            };
            let grant = async {
                if let Some(p) = playback {
                    p.grant.cancelled().await
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
                if let Some(senders) = senders {
                    receive_reports(senders).await
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
                let _ = bounded_write(
                    &mut write,
                    &protocol::response(error.code, error.cseq.unwrap_or(0), &[], &[]),
                    &cancel,
                    session.as_ref(),
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
                    )
                    .await?;
                    break;
                }
                let reply = tokio::select! {biased;_=cancel.cancelled()=>break,reply=handle(&request,&mut session,&app,peer,udp.as_ref(),secure)=>reply};
                let bytes =
                    protocol::response(reply.code, request.cseq, &reply.headers, &reply.body);
                bounded_write(&mut write, &bytes, &cancel, session.as_ref()).await?;
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
                s.playback.grant.add_bytes(bytes);
                app.rtsp_egress.fetch_add(bytes as u64, Ordering::Relaxed);
                if is_udp {
                    app.rtsp_udp_egress
                        .fetch_add(bytes as u64, Ordering::Relaxed);
                }
            }
            Next::Report => {
                if let Some(s) = &session {
                    if s.playing {
                        for track in &s.playback.description.tracks {
                            if let Some(sender) = s.senders.get(&track.id) {
                                if sender.packets == 0 {
                                    continue;
                                }
                                let stamp = s.playback.worker.wire.rtp.clock(track.id).unwrap_or(0);
                                let body =
                                    sender_report(track.ssrc, stamp, sender.packets, sender.octets);
                                let bytes =
                                    deliver(sender, &mut write, &body, true, &cancel, s).await?;
                                app.rtsp_egress.fetch_add(bytes as u64, Ordering::Relaxed);
                                s.playback.grant.add_bytes(bytes);
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
    }
    Ok(())
}
async fn receive_reports(senders: &HashMap<u32, Sender>) -> bool {
    async fn receive(lease: &udp::Lease, ssrc: u32) -> bool {
        let mut buffer = [0; 8193];
        match lease.recv_rtcp(&mut buffer).await {
            Ok(Some(n)) => udp::valid_receiver_report(&buffer[..n], ssrc),
            _ => false,
        }
    }
    let mut tracks = senders
        .values()
        .filter_map(|sender| match &sender.delivery {
            Delivery::Udp { lease, .. } => Some((lease, sender.ssrc)),
            _ => None,
        });
    match (tracks.next(), tracks.next()) {
        (Some((a, id)), Some((b, other))) => {
            tokio::select! {result=receive(a,id)=>result,result=receive(b,other)=>result}
        }
        (Some((a, id)), None) => receive(a, id).await,
        _ => std::future::pending().await,
    }
}
async fn deliver<W: tokio::io::AsyncWrite + Unpin>(
    sender: &Sender,
    writer: &mut W,
    body: &[u8],
    rtcp: bool,
    cancel: &CancellationToken,
    session: &Session,
) -> std::io::Result<usize> {
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
            let result = tokio::select! {biased;_=cancel.cancelled()=>return Err(std::io::ErrorKind::Interrupted.into()),_=session.playback.grant.cancelled()=>return Err(std::io::ErrorKind::Interrupted.into()),_=session.playback.worker.closed()=>return Err(std::io::ErrorKind::Interrupted.into()),result=tokio::time::timeout(Duration::from_secs(2),send)=>result.unwrap_or_else(|_|Err(std::io::ErrorKind::TimedOut.into()))};
            let n = result?;
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
    let revoked = async {
        if let Some(s) = session {
            s.playback.grant.cancelled().await
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
    tokio::select! {biased;_=cancel.cancelled()=>Err(std::io::ErrorKind::Interrupted.into()),_=revoked=>Err(std::io::ErrorKind::Interrupted.into()),_=closed=>Err(std::io::ErrorKind::Interrupted.into()),result=tokio::time::timeout(Duration::from_secs(2),writer.write_all(bytes))=>result.unwrap_or_else(|_|Err(std::io::ErrorKind::TimedOut.into()))}
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
        };
        let playback = match app.rtsp_admit(viewer).await {
            Ok(p) => p,
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
            playing: false,
            initial: VecDeque::new(),
            pending: None,
            pacer: None,
            receiver: None,
            rtp_info: String::new(),
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
fn sender_report(ssrc: u32, stamp: u32, packets: u32, octets: u32) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut b = vec![0x80, 200, 0, 6];
    for value in [
        ssrc,
        now.as_secs().wrapping_add(2208988800) as u32,
        ((now.subsec_nanos() as u64) << 32).div_euclid(1_000_000_000) as u32,
        stamp,
        packets,
        octets,
    ] {
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
