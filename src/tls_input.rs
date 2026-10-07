//! Verified RTSPS input. FFmpeg receives only a worker-owned loopback RTSP URL.
use std::{path::Path, sync::Arc, time::Duration};
mod redirect;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        self,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};

pub(crate) fn client(ca: Option<&Path>) -> Result<Arc<rustls::ClientConfig>, String> {
    let mut roots = rustls::RootCertStore::empty();
    if let Some(path) = ca {
        if !path.is_absolute() || !path.is_file() {
            return Err("TLS CA must be an absolute regular PEM file path".into());
        }
        let bytes = crate::rtsp::tls::pem(path).map_err(|_| "cannot read TLS CA PEM")?;
        let certs = CertificateDer::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid TLS CA PEM")?;
        if certs.is_empty() || certs.len() > 128 {
            return Err("TLS CA bundle requires 1..128 certificates".into());
        }
        for cert in certs {
            roots.add(cert).map_err(|_| "invalid TLS CA certificate")?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| "TLS configuration failed")?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    ))
}
fn source(input: &str) -> Result<url::Url, String> {
    let url = url::Url::parse(input).map_err(|_| "invalid RTSPS URL")?;
    if url.scheme() != "rtsps" || url.host_str().is_none() || url.fragment().is_some() {
        return Err("RTSPS input requires a host and no fragment".into());
    }
    Ok(url)
}
async fn connect(
    input: &str,
    ca: Option<&Path>,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, String> {
    let url = source(input)?;
    let host = url.host_str().unwrap().trim_matches(['[', ']']).to_owned();
    let identity =
        ServerName::try_from(host.clone()).map_err(|_| "invalid RTSPS server identity")?;
    let port = url.port().unwrap_or(322);
    tokio::time::timeout(Duration::from_secs(10), async {
        let connector = TlsConnector::from(client(ca)?);
        let socket = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|_| "RTSPS connection failed")?;
        socket
            .set_nodelay(true)
            .map_err(|_| "RTSPS socket configuration failed")?;
        connector
            .connect(identity, socket)
            .await
            .map_err(|_| "RTSPS certificate or handshake rejected".into())
    })
    .await
    .map_err(|_| "RTSPS setup timed out")?
}
pub struct Bridge {
    url: String,
    task: Option<JoinHandle<()>>,
}
impl Bridge {
    pub async fn prepare(input: &str, ca: Option<&Path>) -> Result<Self, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let upstream = connect(input, ca).await?;
        Self::bind(
            source(input)?,
            ca.map(Path::to_owned),
            deadline,
            async move { Ok(upstream) },
        )
        .await
    }
    /// Reserve only the loopback endpoint here. Network setup belongs to the
    /// worker task, so failure retains retry/fallback state and never holds
    /// the global worker map while waiting for an upstream handshake.
    pub async fn start(input: &str, ca: Option<&Path>) -> Result<Self, String> {
        let url = source(input)?;
        let input = input.to_owned();
        let ca = ca.map(Path::to_owned);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let trust = ca.clone();
        Self::bind(url, trust, deadline, async move {
            connect(&input, ca.as_deref()).await
        })
        .await
    }
    async fn bind(
        url: url::Url,
        ca: Option<std::path::PathBuf>,
        deadline: tokio::time::Instant,
        upstream: impl std::future::Future<
            Output = Result<tokio_rustls::client::TlsStream<TcpStream>, String>,
        > + Send
        + 'static,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "cannot bind RTSPS input bridge")?;
        let local = listener
            .local_addr()
            .map_err(|_| "RTSPS input bridge address unavailable")?;
        let local_url = redirect::loopback(&url, local)?;
        let task = tokio::spawn(async move {
            // The root task owns every hop; no application data precedes TLS verification.
            let Ok(Ok(upstream)) = tokio::time::timeout_at(deadline, upstream).await else {
                return;
            };
            let _ = redirect::relay(url, listener, upstream, ca, deadline).await;
        });
        Ok(Self {
            url: local_url,
            task: Some(task),
        })
    }
    pub fn local_url(&self) -> &str {
        &self.url
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

enum Frame {
    Data(bool),
    Redirect(url::Url, String),
}
/// Only the supervisor receives a validated remote redirect. It constructs a
/// local response after verification; remote 3xx bytes never reach the decoder.
async fn forward_responses<R: tokio::io::AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin>(
    read: &mut R,
    write: &mut W,
    deadline: tokio::time::Instant,
) -> std::io::Result<(url::Url, String)> {
    use tokio::io::AsyncWriteExt;
    let mut routing = true;
    let mut buffer = Vec::with_capacity(8192);
    loop {
        let frame = if routing {
            tokio::time::timeout_at(deadline, read_frame(read, &mut buffer))
                .await
                .map_err(|_| std::io::Error::other("RTSPS routing timed out"))??
        } else {
            read_frame(read, &mut buffer).await?
        };
        match frame {
            Frame::Redirect(url, cseq) if routing => return Ok((url, cseq)),
            Frame::Redirect(_, _) => {
                return Err(std::io::Error::other(
                    "established RTSPS session redirect rejected",
                ));
            }
            Frame::Data(established) => {
                if routing {
                    tokio::time::timeout_at(deadline, write.write_all(&buffer))
                        .await
                        .map_err(|_| std::io::Error::other("RTSPS routing timed out"))??;
                } else {
                    write.write_all(&buffer).await?;
                }
                routing &= !established;
            }
        }
    }
}
async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    read: &mut R,
    frame: &mut Vec<u8>,
) -> std::io::Result<Frame> {
    use tokio::io::AsyncReadExt;
    let invalid = || std::io::Error::other("invalid or redirected RTSPS response");
    frame.clear();
    let first = read.read_u8().await?;
    frame.push(first);
    if first == b'$' {
        frame.push(read.read_u8().await?);
        let size = read.read_u16().await? as usize;
        if size > 8192 {
            return Err(invalid());
        }
        frame.extend_from_slice(&(size as u16).to_be_bytes());
        frame.resize(4 + size, 0);
        read.read_exact(&mut frame[4..]).await?;
        Ok(Frame::Data(true))
    } else {
        while !frame.ends_with(b"\r\n\r\n") {
            if frame.len() >= 16384 {
                return Err(invalid());
            }
            frame.push(read.read_u8().await?);
        }
        let text = std::str::from_utf8(frame).map_err(|_| invalid())?;
        let mut lines = text[..text.len() - 4].split("\r\n");
        let status = lines.next().ok_or_else(invalid)?;
        let mut fields = status.splitn(3, ' ');
        if fields.next() != Some("RTSP/1.0") {
            return Err(invalid());
        }
        let code = fields.next().ok_or_else(invalid)?;
        if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let code: u16 = code.parse().map_err(|_| invalid())?;
        if !(100..600).contains(&code) {
            return Err(invalid());
        }
        if !fields.next().is_some_and(|reason| {
            !reason.is_empty() && reason.bytes().all(|b| (32..127).contains(&b))
        }) {
            return Err(invalid());
        }
        let mut headers = std::collections::HashMap::new();
        for line in lines {
            let (key, value) = line.split_once(':').ok_or_else(invalid)?;
            if headers.len() >= 64
                || key.is_empty()
                || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                || value.bytes().any(|b| b < 32 && b != 9 || b == 127)
            {
                return Err(invalid());
            }
            if headers
                .insert(key.to_ascii_lowercase(), value.trim_matches([' ', '\t']))
                .is_some()
            {
                return Err(invalid());
            }
        }
        if !headers.get("cseq").is_some_and(|v| {
            !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) && v.parse::<u32>().is_ok()
        }) || headers.contains_key("transfer-encoding")
        {
            return Err(invalid());
        }
        let size = match headers.get("content-length") {
            Some(value) if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                value.parse::<usize>().map_err(|_| invalid())?
            }
            Some(_) => return Err(invalid()),
            None => 0,
        };
        if size > 65536 {
            return Err(invalid());
        }
        let established = headers.contains_key("session")
            || headers.get("content-type").is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .unwrap()
                    .trim()
                    .eq_ignore_ascii_case("application/sdp")
            });
        let redirect = if (300..400).contains(&code) {
            if ![301, 302].contains(&code) || established {
                return Err(invalid());
            }
            let target = headers
                .get("location")
                .and_then(|v| crate::rtsp::redirect::destination(v))
                .filter(|u| u.scheme() == "rtsps" && u.port() != Some(0))
                .ok_or_else(invalid)?;
            Some((target, headers["cseq"].to_owned()))
        } else {
            None
        };
        let end = frame.len();
        frame.resize(end + size, 0);
        read.read_exact(&mut frame[end..]).await?;
        Ok(match redirect {
            Some((url, cseq)) => Frame::Redirect(url, cseq),
            None => Frame::Data(established),
        })
    }
}
