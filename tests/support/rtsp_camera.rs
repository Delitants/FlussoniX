//! Owned independent Basic/Digest camera-style server with native encoded media.
use super::tls_fixture::Certificates;
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::{
    rtsp,
    server::{App, Options},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

const USER: &str = "camera";
const PASSWORD: &str = "owned-password";
const REALM: &str = "owned-camera";
#[derive(Default)]
pub struct Seen {
    pub initial_auth: AtomicUsize,
    pub final_auth: AtomicUsize,
    pub initial_challenges: AtomicUsize,
    pub final_challenges: AtomicUsize,
    pub source_connections: AtomicUsize,
}
pub struct Camera {
    pub cert: Certificates,
    pub url: String,
    pub seen: Arc<Seen>,
    pub source: Arc<App>,
    cancel: CancellationToken,
    task: AbortOnDropHandle<()>,
    _dir: tempfile::TempDir,
}
async fn header<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Option<String> {
    let mut bytes = vec![];
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() == 16384 {
            return None;
        }
        bytes.push(r.read_u8().await.ok()?);
    }
    String::from_utf8(bytes).ok()
}
fn field<'a>(wire: &'a str, name: &str) -> Option<&'a str> {
    wire.split("\r\n").skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name)
            .then(|| value.trim_matches([' ', '\t']))
    })
}
fn md5(s: &str) -> String {
    format!("{:x}", md5::Md5::digest(s.as_bytes()))
}
fn authenticated(value: Option<&str>, method: &str, uri: &str, nonce: &str, digest: bool) -> bool {
    let Some(value) = value else {
        return false;
    };
    if !digest {
        return value == format!("Basic {}", STANDARD.encode(format!("{USER}:{PASSWORD}")));
    }
    let Some(raw) = value.strip_prefix("Digest ") else {
        return false;
    };
    let p = raw
        .split(',')
        .filter_map(|s| {
            s.trim()
                .split_once('=')
                .map(|(k, v)| (k, v.trim_matches('"')))
        })
        .collect::<std::collections::HashMap<_, _>>();
    if p.get("username") != Some(&USER)
        || p.get("realm") != Some(&REALM)
        || p.get("nonce") != Some(&nonce)
        || p.get("uri") != Some(&uri)
        || p.get("qop") != Some(&"auth")
    {
        return false;
    }
    let Some(nc) = p.get("nc") else {
        return false;
    };
    let Some(cnonce) = p.get("cnonce") else {
        return false;
    };
    let a1 = md5(&format!("{USER}:{REALM}:{PASSWORD}"));
    let a2 = md5(&format!("{method}:{uri}"));
    let expected = md5(&format!("{a1}:{nonce}:{nc}:{cnonce}:auth:{a2}"));
    p.get("response").is_some_and(|actual| *actual == expected)
}
async fn reply<S: tokio::io::AsyncWrite + Unpin>(
    s: &mut S,
    code: u16,
    cseq: &str,
    headers: &str,
) -> bool {
    s.write_all(
        format!("RTSP/1.0 {code} Owned\r\nCSeq: {cseq}\r\n{headers}Content-Length: 0\r\n\r\n")
            .as_bytes(),
    )
    .await
    .is_ok()
}
async fn client(
    socket: TcpStream,
    acceptor: tokio_rustls::TlsAcceptor,
    addr: SocketAddr,
    backend: SocketAddr,
    digest: bool,
    seen: Arc<Seen>,
) {
    let Ok(socket) = acceptor.accept(socket).await else {
        return;
    };
    let mut socket = BufReader::new(socket);
    let mut source: Option<BufReader<TcpStream>> = None;
    while let Some(wire) = header(&mut socket).await {
        let first = wire.lines().next().unwrap();
        let mut words = first.split(' ');
        let method = words.next().unwrap();
        let uri = words.next().unwrap();
        let initial = url::Url::parse(uri).is_ok_and(|u| u.path() == "/entry");
        let nonce = if initial {
            "initial-owned-nonce"
        } else {
            "final-owned-nonce"
        };
        let cseq = field(&wire, "CSeq").unwrap();
        if !authenticated(field(&wire, "Authorization"), method, uri, nonce, digest) {
            if initial {
                seen.initial_challenges.fetch_add(1, Ordering::SeqCst);
            } else {
                seen.final_challenges.fetch_add(1, Ordering::SeqCst);
            }
            let challenge = if digest {
                // Rotate an authenticated initial nonce, without treating wrong
                // credentials as stale. The decoder keeps its Digest state
                // across the redirect and must be told to renew that nonce.
                let stale = !initial
                    && authenticated(
                        field(&wire, "Authorization"),
                        method,
                        uri,
                        "initial-owned-nonce",
                        true,
                    );
                format!(
                    "Digest realm=\"{REALM}\", nonce=\"{nonce}\", algorithm=MD5, qop=\"auth\", stale={stale}"
                )
            } else {
                format!("Basic realm=\"{REALM}\"")
            };
            if !reply(
                &mut socket,
                401,
                cseq,
                &format!("WWW-Authenticate: {challenge}\r\n"),
            )
            .await
            {
                return;
            }
            continue;
        }
        if initial {
            seen.initial_auth.fetch_add(1, Ordering::SeqCst);
            let _ = reply(
                &mut socket,
                302,
                cseq,
                &format!("Location: rtsps://{addr}/owned?token=owned-token\r\n"),
            )
            .await;
            return;
        }
        seen.final_auth.fetch_add(1, Ordering::SeqCst);
        if source.is_none() {
            let Ok(s) = TcpStream::connect(backend).await else {
                return;
            };
            seen.source_connections.fetch_add(1, Ordering::SeqCst);
            source = Some(BufReader::new(s));
        }
        let source = source.as_mut().unwrap();
        if source.write_all(wire.as_bytes()).await.is_err() {
            return;
        }
        let Some(response) = header(source).await else {
            return;
        };
        let size: usize = field(&response, "Content-Length")
            .unwrap_or("0")
            .parse()
            .unwrap();
        assert!(size <= 65536);
        let mut body = vec![0; size];
        if source.read_exact(&mut body).await.is_err()
            || socket.write_all(response.as_bytes()).await.is_err()
            || socket.write_all(&body).await.is_err()
        {
            return;
        }
        if method == "PLAY" {
            let _ = tokio::io::copy_bidirectional(&mut socket, source).await;
            return;
        }
    }
}
impl Camera {
    pub async fn new(digest: bool) -> Self {
        let cert = Certificates::new();
        let dir = tempfile::tempdir().unwrap();
        let source = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        source.config.put("streams", "owned", json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}", Sha256::digest(b"owned-token"))})).unwrap();
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
        let seen = Arc::new(Seen::default());
        let observations = seen.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let app = source.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let source_stop = stop.clone();
            tasks.spawn(async move {
                let _ = rtsp::serve(backend, app, source_stop).await;
            });
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        tasks.spawn(client(socket, acceptor.clone(), addr, backend_addr, digest, observations.clone()));
                    }
                }
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }));
        Self {
            cert,
            url: format!("rtsps://{USER}:{PASSWORD}@{addr}/entry?token=initial-only"),
            seen,
            source,
            cancel,
            task,
            _dir: dir,
        }
    }
    pub async fn stop(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
        self.source.media.stop_all().await;
    }
}
