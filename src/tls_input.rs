//! Verified RTSPS input. FFmpeg receives only a worker-owned loopback RTSP URL.
use std::{path::Path, sync::Arc, time::Duration};
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
            return Err("RTSPS CA must be an absolute regular PEM file path".into());
        }
        let bytes = crate::rtsp::tls::pem(path).map_err(|_| "cannot read RTSPS CA PEM")?;
        let certs = CertificateDer::pem_slice_iter(&bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid RTSPS CA PEM")?;
        if certs.is_empty() || certs.len() > 128 {
            return Err("RTSPS CA bundle requires 1..128 certificates".into());
        }
        for cert in certs {
            roots
                .add(cert)
                .map_err(|_| "invalid RTSPS CA certificate")?;
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
pub struct Bridge {
    url: String,
    task: Option<JoinHandle<()>>,
}
impl Bridge {
    pub async fn prepare(input: &str, ca: Option<&Path>) -> Result<Self, String> {
        let mut url = url::Url::parse(input).map_err(|_| "invalid RTSPS URL")?;
        if url.scheme() != "rtsps" || url.fragment().is_some() {
            return Err("RTSPS input requires a URL without a fragment".into());
        }
        let host = url
            .host_str()
            .ok_or("RTSPS host required")?
            .trim_matches(['[', ']'])
            .to_owned();
        let identity =
            ServerName::try_from(host.clone()).map_err(|_| "invalid RTSPS server identity")?;
        let port = url.port().unwrap_or(322);
        let connector = TlsConnector::from(client(ca)?);
        // Trust and identity are checked on the same connection that carries
        // application data. No RTSP request or credential is sent beforehand.
        let mut upstream = tokio::time::timeout(Duration::from_secs(10), async {
            let socket = TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|_| "RTSPS connection failed")?;
            socket
                .set_nodelay(true)
                .map_err(|_| "RTSPS socket configuration failed")?;
            connector
                .connect(identity, socket)
                .await
                .map_err(|_| "RTSPS certificate or handshake rejected")
        })
        .await
        .map_err(|_| "RTSPS setup timed out")??;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "cannot bind RTSPS input bridge")?;
        let local = listener
            .local_addr()
            .map_err(|_| "RTSPS input bridge address unavailable")?;
        url.set_scheme("rtsp")
            .map_err(|_| "cannot translate RTSPS URL")?;
        url.set_host(Some("127.0.0.1"))
            .map_err(|_| "cannot translate RTSPS host")?;
        url.set_port(Some(local.port()))
            .map_err(|_| "cannot translate RTSPS port")?;
        let task = tokio::spawn(async move {
            let accepted = tokio::time::timeout(Duration::from_secs(8), listener.accept()).await;
            // Exactly one local decoder connection; no listener retained while
            // copying, and Tokio's bounded copy buffers apply backpressure.
            drop(listener);
            if let Ok(Ok((mut downstream, _))) = accepted {
                let _ = downstream.set_nodelay(true);
                let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
            }
        });
        Ok(Self {
            url: url.into(),
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
