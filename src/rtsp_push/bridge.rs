//! One upstream identity per attempt; bounded framing and negotiated media only.
use super::Destination;
use crate::rtsp::protocol::{Offer, Transport};
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
fn bad() -> io::Error {
    io::Error::other("invalid RTSP push frame")
}
struct Pending {
    method: String,
    transport: Option<Offer>,
}
#[derive(Default)]
struct Session {
    pending: HashMap<u32, Pending>,
    channels: HashMap<u8, bool>,
    recorded: bool,
    announced: bool,
    udp_tracks: usize,
}
pub(super) struct Bridge {
    url: String,
    route: Route,
    task: Option<JoinHandle<()>>,
    bytes: Arc<AtomicU64>,
    pub peer: std::net::IpAddr,
    pub local: std::net::IpAddr,
    pub udp: bool,
}
#[derive(Clone)]
pub(super) struct Route {
    local: String,
    remote: String,
    path: String,
}
impl Route {
    pub fn upstream(&self, uri: &str) -> io::Result<String> {
        if uri == "*" {
            return Ok("*".to_owned());
        }
        let suffix = uri
            .strip_prefix(&self.local)
            .filter(|s| s.starts_with('/'))
            .ok_or_else(bad)?;
        let parsed = url::Url::parse(uri).map_err(|_| bad())?;
        if parsed.path() != self.path
            && !parsed
                .path()
                .strip_prefix(&self.path)
                .is_some_and(|s| s.starts_with('/'))
        {
            return Err(bad());
        }
        Ok(format!("{}{suffix}", self.remote))
    }
}
impl Bridge {
    pub async fn prepare(destination: &Destination) -> io::Result<Self> {
        let host = destination.url.host_str().unwrap().trim_matches(['[', ']']);
        let port = destination
            .url
            .port()
            .unwrap_or(if destination.tls.is_some() { 322 } else { 554 });
        let socket = TcpStream::connect((host, port)).await?;
        socket.set_nodelay(true)?;
        let peer = socket.peer_addr()?.ip();
        let local = socket.local_addr()?.ip();
        let upstream: Box<dyn Socket> = if let Some(config) = &destination.tls {
            let identity = ServerName::try_from(host.to_owned()).map_err(|_| bad())?;
            Box::new(
                TlsConnector::from(config.clone())
                    .connect(identity, socket)
                    .await
                    .map_err(|_| bad())?,
            )
        } else {
            Box::new(socket)
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut url = destination.url.clone();
        url.set_scheme("rtsp").map_err(|_| bad())?;
        url.set_host(Some("127.0.0.1")).map_err(|_| bad())?;
        url.set_port(Some(listener.local_addr()?.port()))
            .map_err(|_| bad())?;
        let local_origin = url[..url::Position::BeforePath].to_string();
        let remote_origin = destination.url[..url::Position::BeforePath].to_string();
        let route = Route {
            local: local_origin,
            remote: remote_origin,
            path: destination.url.path().to_owned(),
        };
        let forwarding = route.clone();
        let bytes = Arc::new(AtomicU64::new(0));
        let progress = bytes.clone();
        let udp = destination.udp;
        let task = tokio::spawn(async move {
            let accepted = tokio::time::timeout(Duration::from_secs(8), listener.accept()).await;
            drop(listener);
            let Ok(Ok((downstream, _))) = accepted else {
                return;
            };
            let _ = downstream.set_nodelay(true);
            let (read_local, mut write_local) = downstream.into_split();
            let (read_remote, mut write_remote) = tokio::io::split(upstream);
            let state = Arc::new(Mutex::new(Session::default()));
            let mut read_local = BufReader::new(read_local);
            let mut read_remote = BufReader::new(read_remote);
            tokio::select! {
                _=requests(&mut read_local,&mut write_remote,&forwarding,&state,&progress,udp)=>{},
                _=responses(&mut read_remote,&mut write_local,&state,peer)=>{},
            }
        });
        Ok(Self {
            url: url.to_string(),
            route,
            task: Some(task),
            bytes,
            peer,
            local,
            udp,
        })
    }
    pub fn route(&self) -> Route {
        self.route.clone()
    }
    pub fn local_url(&self) -> &str {
        &self.url
    }
    pub fn rtp_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub fn count_udp(&self, size: usize) {
        self.bytes.fetch_add(size as u64, Ordering::Relaxed);
    }
    pub async fn close(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for Bridge {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
pub(super) struct Control {
    pub start: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}
impl Control {
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    pub fn cseq(&self) -> io::Result<u32> {
        let value = self.header("cseq").ok_or_else(bad)?;
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        value.parse().map_err(|_| bad())
    }
    fn wire(&self) -> Vec<u8> {
        let mut wire = format!("{}\r\n", self.start);
        for (k, v) in &self.headers {
            wire.push_str(&format!("{k}: {v}\r\n"));
        }
        wire.push_str("\r\n");
        let mut wire = wire.into_bytes();
        wire.extend_from_slice(&self.body);
        wire
    }
}
pub(super) enum Frame {
    Control(Control),
    Media(u8, Vec<u8>),
}
pub(super) async fn frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Frame> {
    let first = reader.read_u8().await?;
    if first == b'$' {
        let channel = reader.read_u8().await?;
        let size = reader.read_u16().await? as usize;
        if size == 0 || size > 8192 {
            return Err(bad());
        }
        let mut body = vec![0; size];
        reader.read_exact(&mut body).await?;
        return Ok(Frame::Media(channel, body));
    }
    let mut data = vec![first];
    while !data.ends_with(b"\r\n\r\n") {
        if data.len() >= 16384 {
            return Err(bad());
        }
        data.push(reader.read_u8().await?);
    }
    let text = std::str::from_utf8(&data).map_err(|_| bad())?;
    let mut lines = text[..text.len() - 4].split("\r\n");
    let start = lines.next().ok_or_else(bad)?.to_owned();
    if start.bytes().any(|b| !(32..127).contains(&b)) {
        return Err(bad());
    }
    let mut headers = Vec::new();
    for line in lines {
        let (key, value) = line.split_once(':').ok_or_else(bad)?;
        let key = key.to_ascii_lowercase();
        if headers.len() >= 64
            || key.is_empty()
            || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || value.bytes().any(|b| b < 32 && b != 9 || b == 127)
            || key != "www-authenticate" && headers.iter().any(|(k, _)| k == &key)
        {
            return Err(bad());
        }
        headers.push((key, value.trim().to_owned()));
    }
    let mut control = Control {
        start,
        headers,
        body: vec![],
    };
    control.cseq()?;
    if control.header("transfer-encoding").is_some() {
        return Err(bad());
    }
    let size = match control.header("content-length") {
        None => 0,
        Some(v) if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) => {
            v.parse::<usize>().map_err(|_| bad())?
        }
        _ => return Err(bad()),
    };
    if size > 65536 {
        return Err(bad());
    }
    control.body.resize(size, 0);
    reader.read_exact(&mut control.body).await?;
    Ok(Frame::Control(control))
}
pub(super) async fn media<W: AsyncWrite + Unpin>(
    write: &mut W,
    channel: u8,
    body: &[u8],
) -> io::Result<()> {
    write
        .write_all(&[b'$', channel, (body.len() >> 8) as u8, body.len() as u8])
        .await?;
    write.write_all(body).await?;
    write.flush().await
}
async fn requests<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    read: &mut R,
    write: &mut W,
    route: &Route,
    state: &Arc<Mutex<Session>>,
    bytes: &AtomicU64,
    udp: bool,
) -> io::Result<()> {
    loop {
        match frame(read).await? {
            Frame::Control(mut c) => {
                let parts: Vec<_> = c.start.split(' ').collect();
                if parts.len() != 3
                    || parts[2] != "RTSP/1.0"
                    || ![
                        "OPTIONS",
                        "ANNOUNCE",
                        "SETUP",
                        "RECORD",
                        "GET_PARAMETER",
                        "TEARDOWN",
                    ]
                    .contains(&parts[0])
                {
                    return Err(bad());
                }
                let method = parts[0].to_owned();
                let uri = parts[1];
                if uri == "*" && method != "OPTIONS" {
                    return Err(bad());
                }
                let translated = route.upstream(uri)?;
                if method == "ANNOUNCE" && c.body.len() > 16384 {
                    return Err(bad());
                }
                let transport = if method == "SETUP" {
                    let offer =
                        Offer::record(c.header("transport").ok_or_else(bad)?).map_err(|_| bad())?;
                    if matches!(offer, Offer::Udp(_)) != udp {
                        return Err(bad());
                    }
                    Some(offer)
                } else {
                    None
                };
                let seq = c.cseq()?;
                {
                    let mut s = state.lock().unwrap();
                    if s.pending.len() >= 16 || s.pending.contains_key(&seq) {
                        return Err(bad());
                    }
                    s.pending.insert(
                        seq,
                        Pending {
                            method: method.clone(),
                            transport,
                        },
                    );
                }
                c.start = format!("{method} {translated} RTSP/1.0");
                write.write_all(&c.wire()).await?;
            }
            Frame::Media(channel, body) => {
                let is_rtp = {
                    let s = state.lock().unwrap();
                    if !s.recorded {
                        return Err(bad());
                    }
                    *s.channels.get(&channel).ok_or_else(bad)?
                };
                if body.len() < 4 || body[0] >> 6 != 2 || is_rtp && body.len() < 12 {
                    return Err(bad());
                }
                media(write, channel, &body).await?;
                if is_rtp {
                    bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                }
            }
        }
    }
}
pub(super) fn transport_response(value: &str) -> io::Result<Transport> {
    let mut options = Vec::new();
    let mut ssrc = false;
    for part in value.split(';') {
        if let Some(v) = part.trim().strip_prefix("ssrc=") {
            if ssrc || v.len() != 8 || !v.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(bad());
            }
            ssrc = true;
        } else if part.trim().split_once('=').is_some_and(|(key, value)| {
            key.eq_ignore_ascii_case("mode")
                && value.trim_matches('"').eq_ignore_ascii_case("receive")
        }) {
            // Receiver-side RECORD alias used by independent FFmpeg 6 listeners.
            // The strict parser still rejects duplicate mode and foreign options.
            options.push("mode=record");
        } else {
            options.push(part);
        }
    }
    Transport::record(&options.join(";")).map_err(|_| bad())
}
async fn responses<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    read: &mut R,
    write: &mut W,
    state: &Arc<Mutex<Session>>,
    peer: std::net::IpAddr,
) -> io::Result<()> {
    loop {
        match frame(read).await? {
            Frame::Control(c) => {
                let parts: Vec<_> = c.start.splitn(3, ' ').collect();
                if parts.len() != 3
                    || parts[0] != "RTSP/1.0"
                    || parts[1].len() != 3
                    || !parts[1].bytes().all(|b| b.is_ascii_digit())
                    || parts[2].is_empty()
                {
                    return Err(bad());
                }
                let code = parts[1].parse::<u16>().map_err(|_| bad())?;
                if !(100..600).contains(&code) || (300..400).contains(&code) {
                    return Err(bad());
                }
                let seq = c.cseq()?;
                {
                    let mut s = state.lock().unwrap();
                    if !s.pending.contains_key(&seq) {
                        return Err(bad());
                    }
                    if code >= 200 {
                        let p = s.pending.remove(&seq).unwrap();
                        if code == 200 {
                            match p.method.as_str() {
                                "ANNOUNCE" => s.announced = true,
                                "SETUP" => {
                                    let value = c.header("transport").ok_or_else(bad)?;
                                    match p.transport.ok_or_else(bad)? {
                                        Offer::Tcp(offer) => {
                                            let t = transport_response(value)?;
                                            if offer != t
                                                || s.channels.contains_key(&t.rtp)
                                                || s.channels.contains_key(&t.rtcp)
                                            {
                                                return Err(bad());
                                            }
                                            s.channels.insert(t.rtp, true);
                                            s.channels.insert(t.rtcp, false);
                                        }
                                        Offer::Udp(offer) => {
                                            super::udp::response(value, offer, peer)?;
                                            s.udp_tracks += 1;
                                            if s.udp_tracks > 8 {
                                                return Err(bad());
                                            }
                                        }
                                    }
                                }
                                "RECORD" => {
                                    if !s.announced || s.channels.is_empty() && s.udp_tracks == 0 {
                                        return Err(bad());
                                    }
                                    s.recorded = true;
                                }
                                "TEARDOWN" => s.recorded = false,
                                _ => {}
                            }
                        }
                    }
                }
                write.write_all(&c.wire()).await?;
            }
            Frame::Media(channel, body) => {
                {
                    let s = state.lock().unwrap();
                    if !s.recorded
                        || s.channels.get(&channel) != Some(&false)
                        || body.len() < 4
                        || body[0] >> 6 != 2
                    {
                        return Err(bad());
                    }
                }
                media(write, channel, &body).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn authentication_challenges_can_repeat_without_allowing_duplicate_framing_headers() {
        let wire = b"RTSP/1.0 401 Unauthorized\r\nCSeq: 1\r\nWWW-Authenticate: Basic realm=\"owned\"\r\nWWW-Authenticate: Digest realm=\"owned\", nonce=\"n\"\r\nContent-Length: 0\r\n\r\n";
        let Frame::Control(control) = frame(&mut &wire[..])
            .await
            .expect("multiple challenges are valid")
        else {
            panic!("control required")
        };
        assert_eq!(
            control
                .headers
                .iter()
                .filter(|(k, _)| k == "www-authenticate")
                .count(),
            2
        );
        for header in ["CSeq: 1", "Content-Length: 0", "Session: owned"] {
            let wire = format!(
                "RTSP/1.0 401 Unauthorized\r\nCSeq: 1\r\nContent-Length: 0\r\nSession: owned\r\n{header}\r\n\r\n"
            );
            assert!(frame(&mut wire.as_bytes()).await.is_err());
        }
    }
    async fn lab() -> (Bridge, TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let destination=Destination::parse(&serde_json::json!({"url":format!("rtsp://127.0.0.1:{port}/owned?password=test-secret")})).unwrap();
        let bridge = Bridge::prepare(&destination).await.unwrap();
        let (remote, _) = listener.accept().await.unwrap();
        let u = url::Url::parse(bridge.local_url()).unwrap();
        let local = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
            .await
            .unwrap();
        (bridge, local, remote)
    }
    async fn reply(
        local: &mut TcpStream,
        remote: &mut TcpStream,
        url: &str,
        method: &str,
        seq: u32,
        transport: Option<&str>,
        response: &str,
    ) -> String {
        let headers = transport
            .map(|t| format!("Transport: {t}\r\n"))
            .unwrap_or_default();
        local
            .write_all(
                format!(
                    "{method} {url} RTSP/1.0\r\nCSeq: {seq}\r\n{headers}Content-Length: 0\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let Frame::Control(request) = frame(remote).await.unwrap() else {
            panic!("control required")
        };
        let start = request.start;
        remote.write_all(response.as_bytes()).await.unwrap();
        if !response.starts_with("RTSP/1.0 200") {
            return start;
        }
        let Frame::Control(response) = frame(local).await.unwrap() else {
            panic!("response required")
        };
        assert_eq!(response.cseq().unwrap(), seq);
        start
    }
    #[tokio::test]
    async fn translated_authority_retains_track_and_query_and_only_recorded_rtp_counts() {
        let (bridge, mut local, mut remote) = lab().await;
        let destination = remote.local_addr().unwrap();
        let url = bridge.local_url().to_owned();
        let start = reply(
            &mut local,
            &mut remote,
            &url,
            "ANNOUNCE",
            1,
            None,
            "RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n",
        )
        .await;
        assert_eq!(
            start,
            format!("ANNOUNCE rtsp://{destination}/owned?password=test-secret RTSP/1.0")
        );
        let start=reply(&mut local,&mut remote,&format!("{url}/streamid=0"),"SETUP",2,Some("RTP/AVP/TCP;unicast;interleaved=0-1;mode=record"),"RTSP/1.0 200 OK\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=record;ssrc=00000001\r\n\r\n").await;
        assert_eq!(
            start,
            format!("SETUP rtsp://{destination}/owned?password=test-secret/streamid=0 RTSP/1.0")
        );
        reply(
            &mut local,
            &mut remote,
            &url,
            "RECORD",
            3,
            None,
            "RTSP/1.0 200 OK\r\nCSeq: 3\r\n\r\n",
        )
        .await;
        let rtcp = [0x80, 201, 0, 1, 0, 0, 0, 1];
        media(&mut local, 1, &rtcp).await.unwrap();
        assert!(matches!(
            frame(&mut remote).await.unwrap(),
            Frame::Media(1, _)
        ));
        assert_eq!(bridge.rtp_bytes(), 0);
        media(&mut remote, 1, &rtcp).await.unwrap();
        assert!(matches!(
            frame(&mut local).await.unwrap(),
            Frame::Media(1, _)
        ));
        let rtp = [0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1];
        media(&mut local, 0, &rtp).await.unwrap();
        assert!(matches!(
            frame(&mut remote).await.unwrap(),
            Frame::Media(0, _)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while bridge.rtp_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(bridge.rtp_bytes(), 12);
        // Reject a frame before allocating or forwarding its advertised oversized payload.
        local.write_all(&[b'$', 0, 0x20, 1]).await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), remote.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        bridge.close().await;
    }
    #[tokio::test]
    async fn receive_transport_alias_preserves_negotiated_channels_and_record_progress() {
        let (bridge, mut local, mut remote) = lab().await;
        let url = bridge.local_url().to_owned();
        reply(
            &mut local,
            &mut remote,
            &url,
            "ANNOUNCE",
            1,
            None,
            "RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n",
        )
        .await;
        reply(&mut local, &mut remote, &format!("{url}/trackID=1"), "SETUP", 2,
            Some("RTP/AVP/TCP;unicast;interleaved=0-1;mode=record"),
            "RTSP/1.0 200 OK\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;unicast;mode=receive;interleaved=0-1\r\n\r\n").await;
        reply(
            &mut local,
            &mut remote,
            &url,
            "RECORD",
            3,
            None,
            "RTSP/1.0 200 OK\r\nCSeq: 3\r\n\r\n",
        )
        .await;
        let rtp = [0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1];
        media(&mut local, 0, &rtp).await.unwrap();
        assert!(matches!(
            frame(&mut remote).await.unwrap(),
            Frame::Media(0, _)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while bridge.rtp_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(bridge.rtp_bytes(), 12);
        for value in [
            "RTP/AVP/TCP;interleaved=0-1;mode=play",
            "RTP/AVP/TCP;interleaved=0-1;mode=receive;mode=record",
            "RTP/AVP;mode=receive;client_port=4000-4001",
        ] {
            assert!(transport_response(value).is_err());
        }
        bridge.close().await;
    }
    #[tokio::test]
    async fn redirect_mismatched_cseq_duplicate_lengths_and_transport_substitution_fail_closed() {
        for response in [
            "RTSP/1.0 302 Found\r\nCSeq: 1\r\nLocation: rtsp://other.example/secret\r\n\r\n",
            "RTSP/1.0 200 OK\r\nCSeq: 999\r\n\r\n",
            "RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: 0\r\ncontent-length: 0\r\n\r\n",
            "RTSP/1.0 999 Bad\r\nCSeq: 1\r\n\r\n",
        ] {
            let (bridge, mut local, mut remote) = lab().await;
            local
                .write_all(
                    format!("OPTIONS {} RTSP/1.0\r\nCSeq: 1\r\n\r\n", bridge.local_url())
                        .as_bytes(),
                )
                .await
                .unwrap();
            frame(&mut remote).await.unwrap();
            remote.write_all(response.as_bytes()).await.unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), local.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            assert_eq!(bridge.rtp_bytes(), 0);
            bridge.close().await;
        }
        for transport in [
            "RTP/AVP;unicast;client_port=30000-30001",
            "RTP/AVP/TCP;interleaved=2-3",
            "RTP/AVP/TCP;interleaved=0-0",
        ] {
            let (bridge, mut local, mut remote) = lab().await;
            let url = bridge.local_url().to_owned();
            reply(
                &mut local,
                &mut remote,
                &url,
                "ANNOUNCE",
                1,
                None,
                "RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n",
            )
            .await;
            local.write_all(format!("SETUP {url}/streamid=0 RTSP/1.0\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;interleaved=0-1;mode=record\r\n\r\n").as_bytes()).await.unwrap();
            frame(&mut remote).await.unwrap();
            remote
                .write_all(
                    format!("RTSP/1.0 200 OK\r\nCSeq: 2\r\nTransport: {transport}\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), local.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            bridge.close().await;
        }
    }
    #[tokio::test]
    async fn foreign_control_path_and_media_before_record_never_reach_remote() {
        for message in [
            "OPTIONS rtsp://evil.example/owned RTSP/1.0\r\nCSeq: 1\r\n\r\n".to_owned(),
            "foreign".to_owned(),
            "media".to_owned(),
        ] {
            let (bridge, mut local, mut remote) = lab().await;
            let message = if message == "foreign" {
                format!(
                    "ANNOUNCE {}/../other RTSP/1.0\r\nCSeq: 1\r\n\r\n",
                    bridge.local_url().split('?').next().unwrap()
                )
            } else {
                message
            };
            if message == "media" {
                media(&mut local, 0, &[0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1])
                    .await
                    .unwrap();
            } else {
                local.write_all(message.as_bytes()).await.unwrap();
            }
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), remote.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            assert_eq!(bridge.rtp_bytes(), 0);
            bridge.close().await;
        }
    }
    #[tokio::test]
    async fn channel_collisions_and_seventeen_pending_requests_close_the_bridge() {
        let (bridge, mut local, mut remote) = lab().await;
        let url = bridge.local_url().to_owned();
        reply(
            &mut local,
            &mut remote,
            &url,
            "ANNOUNCE",
            1,
            None,
            "RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n",
        )
        .await;
        reply(&mut local,&mut remote,&format!("{url}/one"),"SETUP",2,Some("RTP/AVP/TCP;interleaved=0-1;mode=record"),"RTSP/1.0 200 OK\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;interleaved=0-1;mode=record\r\n\r\n").await;
        local.write_all(format!("SETUP {url}/two RTSP/1.0\r\nCSeq: 3\r\nTransport: RTP/AVP/TCP;interleaved=0-1;mode=record\r\n\r\n").as_bytes()).await.unwrap();
        assert!(matches!(
            frame(&mut remote).await.unwrap(),
            Frame::Control(_)
        ));
        remote.write_all(b"RTSP/1.0 200 OK\r\nCSeq: 3\r\nTransport: RTP/AVP/TCP;interleaved=0-1;mode=record\r\n\r\n").await.unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), local.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(bridge.rtp_bytes(), 0);
        bridge.close().await;
        let (bridge, mut local, mut remote) = lab().await;
        let url = bridge.local_url().to_owned();
        for seq in 1..=16 {
            local
                .write_all(format!("OPTIONS {url} RTSP/1.0\r\nCSeq: {seq}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            assert!(matches!(
                frame(&mut remote).await.unwrap(),
                Frame::Control(_)
            ));
        }
        local
            .write_all(format!("OPTIONS {url} RTSP/1.0\r\nCSeq: 17\r\n\r\n").as_bytes())
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), remote.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        bridge.close().await;
    }
    #[tokio::test]
    async fn header_body_and_frame_limits_reject_before_reading_advertised_payloads() {
        let headers = (0..65)
            .map(|n| format!("X-{n}: value\r\n"))
            .collect::<String>();
        for bytes in [
            format!("RTSP/1.0 200 OK\r\nCSeq: 1\r\n{headers}\r\n").into_bytes(),
            b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: 65537\r\n\r\n".to_vec(),
            format!("RTSP/1.0 200 OK\r\nCSeq: 1\r\nX: {}", "a".repeat(16384)).into_bytes(),
            vec![b'$', 0, 0, 0],
            vec![b'$', 0, 0x20, 1],
        ] {
            let (mut input, mut output) = tokio::io::duplex(32768);
            output.write_all(&bytes).await.unwrap();
            // Leave the peer open: limit rejection must not wait for missing bodies.
            let error = tokio::time::timeout(Duration::from_secs(1), frame(&mut input))
                .await
                .unwrap()
                .err()
                .expect("over-limit frame must be rejected");
            assert_eq!(error.to_string(), "invalid RTSP push frame");
        }
    }
}
