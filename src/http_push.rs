//! Native, bounded continuous MPEG-TS POST. No destination remux process.
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::broadcast,
};
mod response;
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
use tokio_util::sync::CancellationToken;

pub(crate) struct Destination {
    url: url::Url,
    authorization: Option<reqwest::header::HeaderValue>,
    tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>,
    endpoint: String,
    disabled: bool,
    connect_seconds: u64,
    retry_seconds: u64,
}
fn number(item: &Value, key: &str, default: u64, max: u64) -> Result<u64, String> {
    match item.get(key) {
        None => Ok(default),
        Some(v) => v
            .as_u64()
            .filter(|n| (1..=max).contains(n))
            .ok_or_else(|| format!("HTTP push {key} must be a whole number from 1 to {max}")),
    }
}
impl Destination {
    pub fn parse(item: &Value) -> Result<Self, String> {
        let obj = item
            .as_object()
            .ok_or("HTTP destination must be an object")?;
        if obj.keys().any(|k| {
            ![
                "url",
                "connect_timeout",
                "retry_timeout",
                "flussonix_tls_ca",
                "disabled",
                "comment",
            ]
            .contains(&k.as_str())
        }) {
            return Err("HTTP push destination option is not implemented".into());
        }
        let raw = item["url"].as_str().ok_or("HTTP push URL is required")?;
        if raw.len() > 4096 || raw.chars().any(|c| c.is_control() || c.is_whitespace()) {
            return Err("Invalid HTTP push URL".into());
        }
        let bytes = raw.as_bytes();
        for (i, b) in bytes.iter().enumerate() {
            if *b == b'%'
                && (bytes.get(i + 1).is_none_or(|v| !v.is_ascii_hexdigit())
                    || bytes.get(i + 2).is_none_or(|v| !v.is_ascii_hexdigit()))
            {
                return Err("Invalid HTTP push URL encoding".into());
            }
        }
        let normalized = if let Some(rest) = raw.strip_prefix("tshttp://") {
            format!("http://{rest}")
        } else if let Some(rest) = raw.strip_prefix("tshttps://") {
            format!("https://{rest}")
        } else {
            raw.into()
        };
        let mut url = url::Url::parse(&normalized).map_err(|_| "Invalid HTTP push URL")?;
        if !["http", "https"].contains(&url.scheme())
            || url.host_str().is_none()
            || url.port() == Some(0)
            || url.fragment().is_some()
        {
            return Err("HTTP push requires HTTP/HTTPS, a host and no fragment".into());
        }
        let authority = normalized
            .split_once("://")
            .unwrap()
            .1
            .split(['/', '?', '#'])
            .next()
            .unwrap();
        if authority.contains('@') && url.username().is_empty() && url.password().is_none() {
            return Err("invalid HTTP Basic credentials".into());
        }
        let authorization =
            crate::http_basic::take_url_credentials(&mut url).map_err(str::to_owned)?;
        if obj.get("disabled").is_some_and(|v| !v.is_boolean())
            || obj
                .get("comment")
                .is_some_and(|v| v.as_str().is_none_or(|s| s.len() > 1024))
        {
            return Err("Invalid HTTP push enabled or comment setting".into());
        }
        let ca = match item.get("flussonix_tls_ca") {
            None => None,
            Some(v) if url.scheme() == "https" => Some(std::path::Path::new(
                v.as_str().ok_or("HTTPS trusted CA must be a file path")?,
            )),
            Some(_) => return Err("Trusted CA applies only to HTTPS push destinations".into()),
        };
        let connect_seconds = number(item, "connect_timeout", 3, 30)?;
        let retry_seconds = number(item, "retry_timeout", 5, 300)?;
        let tls = if url.scheme() == "https" {
            let mut tls = (*crate::tls_input::client(ca)?).clone();
            tls.alpn_protocols = vec![b"http/1.1".to_vec()];
            Some(Arc::new(tls))
        } else {
            None
        };
        let endpoint = url.origin().ascii_serialization();
        Ok(Self {
            url,
            authorization,
            tls,
            endpoint,
            disabled: item["disabled"] == true,
            connect_seconds,
            retry_seconds,
        })
    }
}
struct Counters {
    status: &'static str,
    attempts: u64,
    body_bytes: u64,
    http_status: Option<u16>,
    last_error: Option<&'static str>,
}
pub(crate) struct State {
    destination: Destination,
    index: usize,
    counters: Mutex<Counters>,
    egress: Arc<AtomicU64>,
}
impl State {
    pub fn new(destination: Destination, index: usize, egress: Arc<AtomicU64>) -> Arc<Self> {
        let status = if destination.disabled {
            "disabled"
        } else {
            "connecting"
        };
        Arc::new(Self {
            destination,
            index,
            egress,
            counters: Mutex::new(Counters {
                status,
                attempts: 0,
                body_bytes: 0,
                http_status: None,
                last_error: None,
            }),
        })
    }
    pub fn stats(&self) -> Value {
        let c = self.counters.lock().unwrap();
        json!({"index":self.index,"endpoint":self.destination.endpoint,"status":c.status,"pid":0,"attempts":c.attempts,"body_bytes":c.body_bytes,"last_error":c.last_error,"http_status":c.http_status})
    }
    pub async fn run(
        self: Arc<Self>,
        mut receiver: broadcast::Receiver<Bytes>,
        cancel: CancellationToken,
    ) {
        if self.destination.disabled {
            return;
        }
        while !cancel.is_cancelled() {
            {
                let mut c = self.counters.lock().unwrap();
                c.status = "connecting";
                c.attempts = c.attempts.saturating_add(1);
                c.http_status = None;
                c.last_error = None;
            }
            let fresh = receiver.resubscribe();
            let error = self.attempt(receiver, &cancel).await;
            if cancel.is_cancelled() {
                break;
            }
            {
                let mut c = self.counters.lock().unwrap();
                c.status = "retrying";
                c.last_error = Some(error);
            }
            tokio::select! {biased;_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(self.destination.retry_seconds))=>{}}
            // A retry joins the live worker, without replaying the failed queue.
            receiver = fresh.resubscribe();
        }
        self.counters.lock().unwrap().status = "stopped";
    }
    async fn connect(&self) -> Result<Box<dyn Io>, ()> {
        let host = self
            .destination
            .url
            .host_str()
            .ok_or(())?
            .trim_matches(['[', ']'])
            .to_owned();
        let port = self.destination.url.port_or_known_default().ok_or(())?;
        let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|_| ())?;
        tcp.set_nodelay(true).map_err(|_| ())?;
        if let Some(tls) = &self.destination.tls {
            let name =
                tokio_rustls::rustls::pki_types::ServerName::try_from(host).map_err(|_| ())?;
            let io = tokio_rustls::TlsConnector::from(tls.clone())
                .connect(name, tcp)
                .await
                .map_err(|_| ())?;
            Ok(Box::new(io))
        } else {
            Ok(Box::new(tcp))
        }
    }
    async fn attempt(
        self: &Arc<Self>,
        mut receiver: broadcast::Receiver<Bytes>,
        cancel: &CancellationToken,
    ) -> &'static str {
        let startup =
            tokio::time::Instant::now() + Duration::from_secs(self.destination.connect_seconds + 5);
        let connected = tokio::select! {biased;_=cancel.cancelled()=>return "push_stopped",result=tokio::time::timeout(Duration::from_secs(self.destination.connect_seconds),self.connect())=>result};
        let Ok(Ok(mut socket)) = connected else {
            return "push_connection_failed";
        };
        let url = &self.destination.url;
        let target = format!(
            "{}{}",
            url.path(),
            url.query().map(|q| format!("?{q}")).unwrap_or_default()
        );
        let host = url.host_str().unwrap();
        let header = format!(
            "POST {target} HTTP/1.1\r\nHost: {host}:{}\r\nContent-Type: video/mp2t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n{}\r\n",
            url.port_or_known_default().unwrap(),
            self.destination
                .authorization
                .as_ref()
                .map(|h| format!("Authorization: {}\r\n", h.to_str().unwrap()))
                .unwrap_or_default()
        );
        let sent = tokio::select! {biased;_=cancel.cancelled()=>return "push_stopped",result=tokio::time::timeout_at(startup,async {socket.write_all(header.as_bytes()).await?;socket.flush().await})=>result};
        if !matches!(sent, Ok(Ok(()))) {
            return "push_connection_failed";
        }
        // Both halves remain in this attempt. Dropping either awaited operation
        // at cancellation/error drops both owners and the actual TCP/TLS socket.
        let (reader, mut writer) = tokio::io::split(socket);
        let send = async {
            let mut deadline = startup;
            loop {
                let chunk = match tokio::time::timeout_at(deadline, receiver.recv()).await {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => return "push_lagged",
                    Ok(Err(broadcast::error::RecvError::Closed)) => return "push_input_closed",
                    Err(_) => return "push_stalled",
                };
                if chunk.is_empty() {
                    continue;
                }
                let sent = tokio::time::timeout_at(deadline, async {
                    writer
                        .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                        .await?;
                    writer.write_all(&chunk).await?;
                    writer.write_all(b"\r\n").await?;
                    writer.flush().await
                })
                .await;
                match sent {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => return "push_connection_failed",
                    Err(_) => return "push_stalled",
                }
                {
                    let mut c = self.counters.lock().unwrap();
                    c.status = "sending";
                    c.body_bytes = c.body_bytes.saturating_add(chunk.len() as u64);
                }
                self.egress.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            }
        };
        tokio::select! {biased;_=cancel.cancelled()=>"push_stopped",reason=response::watch(reader,self.clone())=>reason,reason=send=>reason}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_util::task::AbortOnDropHandle;

    fn owns_output_socket(port: u16) -> bool {
        let inodes = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|f| std::fs::read_link(f.ok()?.path()).ok())
            .filter_map(|p| {
                p.to_str()?
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')
                    .map(str::to_owned)
            })
            .collect::<std::collections::HashSet<_>>();
        ["tcp", "tcp6"].iter().any(|table| {
            std::fs::read_to_string(format!("/proc/net/{table}"))
                .unwrap()
                .lines()
                .skip(1)
                .any(|line| {
                    let fields = line.split_whitespace().collect::<Vec<_>>();
                    fields
                        .get(2)
                        .is_some_and(|s| s.ends_with(&format!(":{port:04X}")))
                        && fields.get(9).is_some_and(|inode| inodes.contains(*inode))
                })
        })
    }
    #[tokio::test]
    async fn nonreading_receiver_is_bounded_and_cancel_stops_body_producer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let _cleanup = cancel.clone().drop_guard();
        let drain = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_drain = drain.clone();
        let (ready, connected) = tokio::sync::oneshot::channel();
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = vec![];
            let mut buf = [0; 4096];
            while !header.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut buf))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(n > 0);
                header.extend_from_slice(&buf[..n]);
                assert!(header.len() < 16384);
            }
            ready.send(()).unwrap();
            // Stop consuming after actual request headers. Keep the socket open
            // so the publisher must handle backpressure rather than a reset.
            tokio::select! {_=server_drain.cancelled()=>{},_=server_cancel.cancelled()=>{server_drain.cancelled().await;}}
            let mut received = header.len();
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    received += n;
                    assert!(received < 32 * 1024 * 1024);
                }
            })
            .await
            .expect("cancelled publisher connection must reach EOF");
        }));
        let egress = Arc::new(AtomicU64::new(0));
        let destination =
            Destination::parse(&json!({"url":format!("http://{address}/owned"),"retry_timeout":1}))
                .unwrap();
        let state = State::new(destination, 0, egress.clone());
        let (sender, receiver) = broadcast::channel(64);
        let sender_cancel = cancel.clone();
        let producer = AbortOnDropHandle::new(tokio::spawn(async move {
            let bytes = Bytes::from(vec![0x47; 188 * 64]);
            let mut tick = tokio::time::interval(Duration::from_millis(1));
            loop {
                tokio::select! {biased;_=sender_cancel.cancelled()=>break,_=tick.tick()=>{let _=sender.send(bytes.clone());}}
            }
        }));
        let publisher =
            AbortOnDropHandle::new(tokio::spawn(state.clone().run(receiver, cancel.clone())));
        tokio::time::timeout(Duration::from_secs(3), connected)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let stats = state.stats();
                if stats["status"] == "retrying"
                    && ["push_stalled", "push_lagged"]
                        .iter()
                        .any(|r| stats["last_error"] == *r)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("slow receiver must terminate and retry within a bounded interval");
        tokio::time::timeout(Duration::from_secs(3), async {
            while state.stats()["attempts"].as_u64().unwrap() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("failed output must start a fresh request");
        assert!(egress.load(Ordering::Relaxed) > 0);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), publisher)
            .await
            .unwrap()
            .unwrap();
        producer.await.unwrap();
        let stopped = egress.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(egress.load(Ordering::Relaxed), stopped);
        assert_eq!(state.stats()["status"], "stopped");
        // Keep the receiving socket paused while verifying that no native
        // connection driver still owns any publisher socket for this endpoint.
        tokio::time::timeout(Duration::from_secs(2), async {
            while owns_output_socket(address.port()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancel must close publisher sockets before receiver resumes");
        drain.cancel();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn early_success_acknowledgement_keeps_uploading_until_cancel() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let _cleanup = cancel.clone().drop_guard();
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = vec![];
            let mut byte = [0];
            while !header.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                header.push(byte[0]);
                assert!(header.len() < 16384);
            }
            socket
                .write_all(
                    b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
                )
                .await
                .unwrap();
            let mut bytes = 0usize;
            let mut buffer = [0; 4096];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes += n;
            }
            bytes
        }));
        let egress = Arc::new(AtomicU64::new(0));
        let destination =
            Destination::parse(&json!({"url":format!("http://{address}/owned")})).unwrap();
        let state = State::new(destination, 0, egress.clone());
        let (sender, receiver) = broadcast::channel(64);
        let producer_cancel = cancel.clone();
        let producer = AbortOnDropHandle::new(tokio::spawn(async move {
            let bytes = Bytes::from(vec![0x47; 188 * 64]);
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {biased;_=producer_cancel.cancelled()=>break,_=tick.tick()=>{let _=sender.send(bytes.clone());}}
            }
        }));
        let publisher =
            AbortOnDropHandle::new(tokio::spawn(state.clone().run(receiver, cancel.clone())));
        tokio::time::timeout(Duration::from_secs(3), async {
            while egress.load(Ordering::Relaxed) < 500_000 || state.stats()["http_status"] != 200 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("early acknowledgement must not end the continuous request");
        assert_eq!(state.stats()["attempts"], 1);
        assert_eq!(state.stats()["status"], "sending");
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), publisher)
            .await
            .unwrap()
            .unwrap();
        producer.await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(received >= egress.load(Ordering::Relaxed) as usize);
        assert!(!owns_output_socket(address.port()));
    }
}
