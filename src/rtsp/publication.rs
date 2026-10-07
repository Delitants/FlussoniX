//! Bounded receiving sessions share HTTP publisher policy and worker ownership.
mod bridge;
mod reports;
mod sdp;
use super::{
    Delivery,
    protocol::{self, Event, Offer, Request, Transport},
    udp,
};
use crate::{
    media::Publication,
    server::{
        App,
        publication::{self as admission, Session as Grant, Snapshot},
    },
};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, mpsc},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use url::Url;
struct Session {
    name: String,
    url: Url,
    id: String,
    expected: Snapshot,
    grant: Grant,
    renew_at: Instant,
    description: sdp::Description,
    tracks: Vec<Option<Delivery>>,
    pool: Option<Arc<udp::Pool>>,
    peer: std::net::IpAddr,
    udp_cursor: usize,
    publication: Option<Publication>,
    bridge: Option<bridge::Bridge>,
    secure: bool,
    progress: Instant,
    _permit: OwnedSemaphorePermit,
}
async fn admit(
    r: &Request,
    app: &Arc<App>,
    peer: SocketAddr,
    secure: bool,
    pool: Option<Arc<udp::Pool>>,
) -> Result<Session, u16> {
    if app.options.role == "lb" {
        return Err(403);
    }
    let (url, name) = super::location(&r.uri)?;
    if !secure && url.scheme() == "rtsps" {
        return Err(400);
    }
    if r.headers.contains_key("require") || r.headers.contains_key("proxy-require") {
        return Err(551);
    }
    if !r.headers.get("content-type").is_some_and(|v| {
        v.split(';')
            .next()
            .unwrap()
            .trim()
            .eq_ignore_ascii_case("application/sdp")
    }) {
        return Err(415);
    }
    let expected = admission::snapshot(app, &name).ok_or_else(|| {
        if app.config.effective(&name).is_some() {
            403u16
        } else {
            404u16
        }
    })?;
    let qs = url.query().unwrap_or("");
    let mut password = String::new();
    let mut token = String::new();
    for (k, v) in url.query_pairs() {
        if ["password", "token"].contains(&k.as_ref()) && v.len() > 1024 {
            return Err(400);
        }
        match k.as_ref() {
            "password" => password = v.into_owned(),
            "token" => token = v.into_owned(),
            _ => {}
        }
    }
    if !expected.policy.accepts_password(&password) {
        return Err(403);
    }
    for k in ["user-agent", "referer", "host"] {
        if r.headers.get(k).is_some_and(|v| v.len() > 4096) {
            return Err(400);
        }
    }
    let description = sdp::parse(&r.body, &url)?;
    let tracks = (0..description.media.tracks.len()).map(|_| None).collect();
    let permit = app
        .publishers
        .clone()
        .try_acquire_owned()
        .map_err(|_| 503u16)?;
    let id = uuid::Uuid::new_v4().to_string();
    let mut grant = Grant {
        metadata: json!({"name":name,"proto":"rtsp","ip":peer.ip().to_string(),"token":token,"qs":qs,"user_agent":r.headers.get("user-agent").map(String::as_str).unwrap_or(""),"referer":r.headers.get("referer").map(String::as_str).unwrap_or(""),"host":r.headers.get("host").map(String::as_str).unwrap_or(""),"session_id":id}),
        started: Instant::now(),
        number: 0,
        bytes: 0,
    };
    let duration = admission::authorize_current(&mut grant, app, &name, &expected, None)
        .await
        .map_err(|s| s.as_u16())?;
    if !admission::current(app, &name, &expected) {
        return Err(403);
    }
    Ok(Session {
        name,
        url,
        id,
        expected,
        grant,
        renew_at: Instant::now() + duration,
        description,
        tracks,
        pool,
        peer: peer.ip(),
        udp_cursor: 0,
        publication: None,
        bridge: None,
        secure,
        progress: Instant::now(),
        _permit: permit,
    })
}
async fn revoked(app: &App, name: &str, expected: &Snapshot) {
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tick.tick().await;
        if !admission::current(app, name, expected) {
            return;
        }
    }
}
async fn reply<W: AsyncWrite + Unpin>(
    write: &mut W,
    cancel: &CancellationToken,
    code: u16,
    cseq: u32,
    headers: &[(&str, String)],
) -> std::io::Result<()> {
    let bytes = protocol::response(code, cseq, headers, &[]);
    tokio::select! {biased;_=cancel.cancelled()=>Err(std::io::Error::other("RTSP publication cancelled")),r=tokio::time::timeout(Duration::from_secs(2),write.write_all(&bytes))=>r.map_err(|_|std::io::Error::other("RTSP publication write stalled"))?}
}
impl Session {
    fn bound(&self, r: &Request, track: bool) -> Result<Option<usize>, u16> {
        // FFmpeg's muxer appends the control suffix after the original query.
        // Only the exact announced URI plus an advertised control suffix is accepted.
        if track {
            for (i, c) in self.description.controls.iter().enumerate() {
                if let Some(suffix) = c.path().strip_prefix(self.url.path()) {
                    if r.uri == format!("{}{suffix}", self.url.as_str()) {
                        if !r
                            .headers
                            .get("session")
                            .is_some_and(|v| v.split(';').next() == Some(self.id.as_str()))
                        {
                            return Err(454);
                        }
                        return Ok(Some(i));
                    }
                }
            }
        }
        let u = Url::parse(&r.uri).map_err(|_| 400u16)?;
        if !u.username().is_empty()
            || u.password().is_some()
            || u.fragment().is_some()
            || u.scheme() != self.url.scheme()
            || u.host_str() != self.url.host_str()
            || u.port() != self.url.port()
            || u.query().is_some() && u.query() != self.url.query()
        {
            return Err(403);
        }
        if !r
            .headers
            .get("session")
            .is_some_and(|v| v.split(';').next() == Some(self.id.as_str()))
        {
            return Err(454);
        }
        if track {
            self.description
                .controls
                .iter()
                .position(|c| c.path() == u.path())
                .map(Some)
                .ok_or(403)
        } else if u.path() == self.url.path() {
            Ok(None)
        } else {
            Err(403)
        }
    }
    fn uses_udp(&self) -> bool {
        self.tracks
            .iter()
            .flatten()
            .any(|t| matches!(t, Delivery::Udp { .. }))
    }
    fn transports(&self) -> Vec<Option<Transport>> {
        self.tracks
            .iter()
            .enumerate()
            .map(|(n, t)| match t {
                Some(Delivery::Tcp(p)) => Some(*p),
                Some(Delivery::Udp { .. }) => Some(Transport {
                    rtp: (2 * n) as u8,
                    rtcp: (2 * n + 1) as u8,
                }),
                None => None,
            })
            .collect()
    }
    async fn setup(&mut self, n: usize, offer: Offer) -> Result<String, u16> {
        // A connection's negotiated transport is fixed, including same-track replacement.
        let udp_offer = matches!(offer, Offer::Udp(_));
        if self
            .tracks
            .iter()
            .flatten()
            .any(|t| matches!(t, Delivery::Udp { .. }) != udp_offer)
        {
            return Err(461);
        }
        match offer {
            Offer::Tcp(t) => {
                if self.tracks.iter().enumerate().any(|(i,x)| i!=n && matches!(x, Some(Delivery::Tcp(p)) if [p.rtp,p.rtcp].iter().any(|c| *c==t.rtp || *c==t.rtcp))) {
                    return Err(461);
                }
                self.tracks[n] = Some(Delivery::Tcp(t));
                Ok(format!(
                    "RTP/AVP/TCP;unicast;interleaved={}-{};mode=record",
                    t.rtp, t.rtcp
                ))
            }
            Offer::Udp(ports) => {
                if self.secure {
                    return Err(461);
                }
                let pool = self.pool.as_ref().ok_or(461u16)?;
                if self.tracks.iter().enumerate().any(|(i, x)| {
                    i != n && matches!(x, Some(Delivery::Udp { ports: p, .. }) if p==&ports)
                }) {
                    return Err(461);
                }
                let lease = match &mut self.tracks[n] {
                    Some(Delivery::Udp {
                        lease,
                        ports: previous,
                    }) => {
                        lease.set_client_ports(ports).map_err(|_| 503u16)?;
                        *previous = ports;
                        lease
                    }
                    _ => {
                        let lease = pool.lease(self.peer, ports).await.map_err(|e| {
                            if e.kind() == std::io::ErrorKind::WouldBlock {
                                453u16
                            } else {
                                503u16
                            }
                        })?;
                        self.tracks[n] = Some(Delivery::Udp { lease, ports });
                        match self.tracks[n].as_mut().unwrap() {
                            Delivery::Udp { lease, .. } => lease,
                            _ => unreachable!(),
                        }
                    }
                };
                let (rtp, rtcp) = lease.server_ports();
                Ok(format!(
                    "RTP/AVP;unicast;client_port={}-{};server_port={rtp}-{rtcp};source={};mode=record",
                    ports.rtp,
                    ports.rtcp,
                    lease.source_ip()
                ))
            }
        }
    }
    // Wait on small readiness futures; allocate no per-track media buffers.
    async fn next_udp(&self) -> std::io::Result<usize> {
        if self.bridge.is_none() || !self.uses_udp() {
            return std::future::pending().await;
        }
        let count = self.tracks.len() * 2;
        let mut reads = Vec::with_capacity(count);
        for offset in 0..count {
            let lane = (self.udp_cursor + offset) % count;
            if let Some(Delivery::Udp { lease, .. }) = &self.tracks[lane / 2] {
                reads.push(Box::pin(async move {
                    lease.readable(lane % 2 == 1).await.map(|_| lane)
                }));
            }
        }
        futures_util::future::select_all(reads).await.0
    }
    async fn forward(
        &mut self,
        channel: u8,
        body: &[u8],
        cancel: &CancellationToken,
    ) -> Result<(), &'static str> {
        let bridge = self.bridge.as_mut().ok_or("Media before RECORD")?;
        let media = tokio::select! {biased;_=cancel.cancelled()=>return Err("Publication cancelled"),r=tokio::time::timeout(Duration::from_millis(250),bridge.forward(channel,body))=>r.map_err(|_|"Media relay stalled")??};
        if media {
            self.progress = Instant::now();
            self.grant.bytes = self.grant.bytes.saturating_add(body.len() as u64);
        }
        Ok(())
    }
    async fn send_control<W: AsyncWrite + Unpin>(
        &self,
        channel: u8,
        body: &[u8],
        write: &mut W,
        cancel: &CancellationToken,
    ) -> std::io::Result<()> {
        let send = async {
            if self.uses_udp() {
                let Some(Delivery::Udp { lease, .. }) = self
                    .tracks
                    .get(channel as usize / 2)
                    .and_then(Option::as_ref)
                else {
                    return Err(std::io::Error::other("Unconfigured RTCP lane"));
                };
                if channel % 2 != 1 {
                    return Err(std::io::Error::other("Invalid RTCP lane"));
                }
                lease.send_rtcp(body).await?;
            } else {
                let mut frame = vec![b'$', channel];
                frame.extend((body.len() as u16).to_be_bytes());
                frame.extend(body);
                write.write_all(&frame).await?;
            }
            Ok(())
        };
        tokio::select! {biased;_=cancel.cancelled()=>Err(std::io::Error::other("Publication cancelled")),_=self.publication.as_ref().unwrap().worker.closed()=>Err(std::io::Error::other("Publication stopped")),r=tokio::time::timeout(Duration::from_secs(2),send)=>r.map_err(|_|std::io::Error::other("RTCP feedback stalled"))?}
    }
    async fn start(&mut self, app: &Arc<App>, cancel: &CancellationToken) -> Result<(), u16> {
        let transports = self.transports();
        let mut bridge = bridge::Bridge::bind(&self.description.media, &transports)
            .await
            .map_err(|_| 503u16)?;
        let p = app
            .media
            .publish_sdp_guarded(&self.name, &self.expected.config, self.secure, async {
                !cancel.is_cancelled() && admission::current(app, &self.name, &self.expected)
            })
            .await
            .map_err(|e| {
                if e == "publisher already connected" {
                    409u16
                } else {
                    503u16
                }
            })?;
        self.publication = Some(p);
        let p = self.publication.as_mut().unwrap();
        let stdin = p.stdin.take().ok_or(503u16)?;
        tokio::select! { biased;
            _=revoked(app,&self.name,&self.expected)=>return Err(403),
            _=tokio::time::sleep_until(self.renew_at),if self.expected.policy.url.is_some()=>return Err(403),
            r=bridge.start(stdin,&p.worker,cancel)=>r.map_err(|_|503u16)?,
        }
        if !admission::current(app, &self.name, &self.expected) {
            return Err(403);
        }
        for t in self.tracks.iter().flatten() {
            if let Delivery::Udp { lease, .. } = t {
                lease.discard_pending().map_err(|_| 503u16)?;
            }
        }
        self.progress = Instant::now();
        self.bridge = Some(bridge);
        Ok(())
    }
    async fn request(
        &mut self,
        r: &Request,
        app: &Arc<App>,
        cancel: &CancellationToken,
    ) -> (u16, Vec<(&'static str, String)>, bool) {
        if r.headers.contains_key("require") || r.headers.contains_key("proxy-require") {
            return (551, vec![], false);
        }
        let code = match r.method.as_str() {
            "OPTIONS" => return (200, vec![("Public", super::PUBLIC.into())], false),
            "SETUP" => {
                if self.publication.is_some() {
                    455
                } else {
                    match self.bound(r, true) {
                        Err(c) => c,
                        Ok(Some(n)) => {
                            match r
                                .headers
                                .get("transport")
                                .ok_or(461u16)
                                .and_then(|s| Offer::record(s))
                            {
                                Err(c) => c,
                                Ok(offer) => match self.setup(n, offer).await {
                                    Err(c) => c,
                                    Ok(transport) => {
                                        return (
                                            200,
                                            vec![
                                                ("Session", self.id.clone()),
                                                ("Transport", transport),
                                            ],
                                            false,
                                        );
                                    }
                                },
                            }
                        }
                        _ => 400,
                    }
                }
            }
            "GET_PARAMETER" | "TEARDOWN" => match self.bound(r, false) {
                Err(c) => c,
                Ok(_) => {
                    return (
                        200,
                        vec![("Session", self.id.clone())],
                        r.method == "TEARDOWN",
                    );
                }
            },
            "RECORD" => {
                if let Err(code) = self.bound(r, false) {
                    code
                } else if r
                    .headers
                    .get("range")
                    .is_some_and(|v| !super::live_range(v))
                {
                    457
                } else if self.publication.is_some() || self.tracks.iter().any(Option::is_none) {
                    455
                } else {
                    match self.start(app, cancel).await {
                        Ok(()) => return (200, vec![("Session", self.id.clone())], false),
                        Err(code) => return (code, vec![], code != 409),
                    }
                }
            }
            "ANNOUNCE" | "DESCRIBE" | "PLAY" => 455,
            _ => 405,
        };
        (code, vec![], false)
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn receive<W: AsyncWrite + Unpin>(
    r: Request,
    controls: &mut mpsc::Receiver<Result<Event, protocol::Error>>,
    write: &mut W,
    app: &Arc<App>,
    peer: SocketAddr,
    cancel: &CancellationToken,
    secure: bool,
    pool: Option<Arc<udp::Pool>>,
) -> std::io::Result<()> {
    let admission = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),s=admit(&r,app,peer,secure,pool)=>s};
    let mut s = match admission {
        Ok(s) => s,
        Err(code) => return reply(write, cancel, code, r.cseq, &[]).await,
    };
    let result = run(&mut s, r.cseq, controls, write, app, cancel).await;
    if let Some(p) = s.publication.take() {
        let worker = p.worker.clone();
        drop(p);
        app.media.stop_if_current(&s.name, &worker).await;
    }
    result
}
async fn run<W: AsyncWrite + Unpin>(
    s: &mut Session,
    cseq: u32,
    controls: &mut mpsc::Receiver<Result<Event, protocol::Error>>,
    write: &mut W,
    app: &Arc<App>,
    cancel: &CancellationToken,
) -> std::io::Result<()> {
    reply(write, cancel, 200, cseq, &[("Session", s.id.clone())]).await?;
    let timeout = Duration::from_secs(
        s.expected.config["flussonix_input_timeout"]
            .as_u64()
            .unwrap_or(15)
            .clamp(1, 300),
    );
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut activity = Instant::now();
    let mut reports = tokio::time::interval(Duration::from_secs(5));
    reports.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    reports.tick().await;
    let mut buffer = vec![0; 8193];
    'session: loop {
        tokio::select! {biased;
        _=cancel.cancelled()=>break,
        _=tick.tick()=>{if !admission::current(app,&s.name,&s.expected)||s.publication.as_ref().is_some_and(|p|p.worker.is_closed())||s.publication.is_some()&&s.progress.elapsed()>=timeout{break;}},
        _=tokio::time::sleep_until(activity+Duration::from_secs(30)),if s.publication.is_none()=>break,
        _=tokio::time::sleep_until(s.renew_at),if s.expected.policy.url.is_some()=>{let renew=tokio::select!{biased;_=cancel.cancelled()=>break,r=admission::authorize_current(&mut s.grant,app,&s.name,&s.expected,s.publication.as_ref().map(|p|p.worker.as_ref()))=>r};match renew{Ok(d)=>s.renew_at=Instant::now()+d,Err(_)=>break}},
        _=reports.tick(),if s.bridge.is_some()=>{
            for (channel,body) in s.bridge.as_mut().unwrap().reports() {
                if s.send_control(channel,&body,write,cancel).await.is_err() { break 'session; }
            }
        },
        event=controls.recv()=>match event{
        Some(Ok(Event::Request(r)))=>{let(code,headers,close)=s.request(&r,app,cancel).await;reply(write,cancel,code,r.cseq,&headers).await?;if close{break;}
        if code==200{activity=Instant::now();}},
        Some(Ok(Event::Interleaved(channel,body)))=>{
        if s.uses_udp() { break; }
        if let Err(reason)=s.forward(channel,&body,cancel).await {tracing::debug!(reason,channel,length=body.len(),"RTSP publication packet rejected");break;}
        activity=s.progress;
        },
        Some(Err(error))=>{tracing::debug!(code=error.code,"RTSP publication framing failed");break;},
        _=>{tracing::debug!("RTSP publisher connection ended");break;},
        },
        feedback=async{match &s.bridge{Some(b)=>b.feedback().await,None=>std::future::pending().await}}=>{match feedback{Ok((channel,body))=>if s.send_control(channel,&body,write,cancel).await.is_err(){break;},Err(reason)=>{tracing::debug!(reason,"RTSP publication feedback failed");break;}}},
        next=s.next_udp()=>{
            let lane=match next {Ok(lane)=>lane,Err(_)=>break};
            s.udp_cursor=(lane+1)%(s.tracks.len()*2);
            let Some(Delivery::Udp {lease,..})=&s.tracks[lane/2] else{break;};
            match lease.receive(lane%2==1,&mut buffer) {
                Ok(Some(n))=>{
                    if let Err(reason)=s.forward(lane as u8,&buffer[..n],cancel).await {tracing::debug!(reason,lane,length=n,"RTSP publication UDP rejected");break;}
                    activity=s.progress;
                }
                Ok(None)=>{},
                Err(_)=>break,
            }
        },
        }
    }
    Ok(())
}
