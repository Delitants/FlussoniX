//! Explicit stream-scoped Flussonic HTTP media authentication.
//!
//! Explicit vendor daemon profiles reach this client through current admitted
//! source authority; ordinary native discovery stays separate. No official
//! Flussonic components are required. Live reference qualification remains a
//! separate gate.
pub(crate) mod diagnostic;
pub(super) mod failure;
use super::{PullOptions, RequestAuth, Requests};
use crate::{m4s::Track, wire::Hub};
pub use failure::InputFailure;
use reqwest::header::{HeaderMap, HeaderValue};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{io::AsyncWrite, sync::oneshot};
use url::Url;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    M4s,
    M4f,
}

/// A privileged key bound to an explicit private origin and canonical stream
/// name. Intentionally has no Debug or serialization implementation.
pub struct MediaInput {
    base: Url,
    control: Url,
    is_m4f: bool,
    key: Box<str>,
    name: Box<str>,
    tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>,
    prepared: tokio::sync::OnceCell<Requests>,
}
impl MediaInput {
    async fn prepare_with<F, Fut>(&self, lookup: F) -> Result<&Requests, String>
    where
        F: FnOnce(String, u16) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<std::net::SocketAddr>, String>>,
    {
        self.prepared
            .get_or_try_init(|| async move {
                let addresses = super::origin_pin::resolve_with(&self.base, lookup).await?;
                let mut builder = reqwest::Client::builder()
                    .no_proxy()
                    .connect_timeout(Duration::from_secs(5))
                    .https_only(self.base.scheme() == "https")
                    .redirect(reqwest::redirect::Policy::none());
                if let Some(tls) = &self.tls {
                    builder = builder.use_preconfigured_tls((**tls).clone());
                }
                let client = super::origin_pin::pin(builder, &self.base, &addresses)
                    .build()
                    .map_err(|_| invalid_media_configuration())?;
                Ok(Requests {
                    client,
                    base: self.base.clone(),
                    control: self.control.clone(),
                    is_m4f: self.is_m4f,
                    auth: RequestAuth::Flussonic {
                        key: self.key.clone(),
                        name: self.name.clone(),
                    },
                })
            })
            .await
    }

    async fn prepared(&self) -> Result<&Requests, String> {
        self.prepare_with(super::origin_pin::lookup).await
    }

    pub(crate) async fn pull_m4f_session_guarded<W: AsyncWrite + Unpin>(
        &self,
        output: &mut W,
        hub: Option<&Hub>,
        state: &mut super::m4f_session::M4fSession,
        metadata: Option<oneshot::Sender<Vec<Track>>>,
        options: PullOptions<'_>,
        authority: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<(), InputFailure> {
        if self.transport() != Transport::M4f {
            return Err(InputFailure::Invalid);
        }
        let slot = std::sync::atomic::AtomicU8::new(InputFailure::Invalid as u8);
        let diagnostic = diagnostic::Diagnostics::default();
        let mut options = options;
        options.failure = Some(&slot);
        options.diagnostic = Some(&diagnostic);
        options.classify_clean_eof = true;
        let requests = self.prepared().await.map_err(|_| {
            let kind = if authority() {
                InputFailure::Transient
            } else {
                InputFailure::Revoked
            };
            diagnostic.report(self.transport(), kind);
            kind
        })?;
        super::consume_guarded_session(
            requests,
            output,
            hub,
            metadata,
            options,
            Some(state),
            Some(authority),
        )
        .await
        .map_err(|_| {
            let kind = if !authority() {
                InputFailure::Revoked
            } else {
                failure::read(&slot)
            };
            diagnostic.report(self.transport(), kind);
            kind
        })
    }

    pub(crate) fn transport(&self) -> Transport {
        if self.is_m4f {
            Transport::M4f
        } else {
            Transport::M4s
        }
    }

    pub(crate) async fn pull_guarded<W: AsyncWrite + Unpin>(
        &self,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
        options: PullOptions<'_>,
        authority: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<(), String> {
        let requests = self.prepared().await.map_err(|_| {
            failure::record(
                options.failure,
                InputFailure::Transient,
                "M4 origin resolution failed",
            )
            .to_owned()
        })?;
        super::consume_guarded(
            requests,
            output,
            hub,
            Some(metadata),
            options,
            Some(authority),
        )
        .await
    }
    /// Typed retry provenance; no endpoint, credential or diagnostic string.
    pub async fn pull_ready_classified<W: AsyncWrite + Unpin>(
        &self,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
    ) -> Result<(), InputFailure> {
        self.pull_guarded_classified(output, hub, metadata, PullOptions::default(), &|| true)
            .await
    }
    pub(crate) async fn pull_guarded_classified<W: AsyncWrite + Unpin>(
        &self,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
        mut options: PullOptions<'_>,
        authority: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<(), InputFailure> {
        let slot = std::sync::atomic::AtomicU8::new(InputFailure::Invalid as u8);
        let diagnostic = diagnostic::Diagnostics::default();
        options.failure = Some(&slot);
        options.diagnostic = Some(&diagnostic);
        self.pull_guarded(output, hub, metadata, options, authority)
            .await
            .map_err(|_| {
                let kind = if !authority() {
                    InputFailure::Revoked
                } else {
                    failure::read(&slot)
                };
                diagnostic.report(self.transport(), kind);
                kind
            })
    }
    /// The HTTP(S) root may include a trusted proxy prefix. `name` is the literal
    /// decoded stream name, never an escaped URL or protocol-suffixed path.
    /// Each path component is encoded once; the digest binds the original UTF-8.
    /// A custom CA replaces public roots and requires HTTPS.
    pub fn new(
        root: &str,
        name: &str,
        transport: Transport,
        key: &str,
        ca: Option<&Path>,
    ) -> Result<Self, String> {
        crate::config::valid_name(name).map_err(|_| invalid_media_configuration())?;
        MediaOrigin::new(root, transport, key, ca)?.input(name)
    }
    /// One input generation. Reopening its control connection keeps the pinned
    /// origin addresses but obtains fresh request credentials;
    /// the caller owns retry/backoff and cancellation. Metadata precedes TS
    /// writes. The existing public ingest defaults apply: subtitle tracks are
    /// omitted from the native relay. No access policy is inferred here.
    pub async fn pull_ready<W: AsyncWrite + Unpin>(
        &self,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
    ) -> Result<(), String> {
        let requests = self.prepared().await?;
        super::consume(
            requests,
            output,
            hub,
            Some(metadata),
            PullOptions::default(),
        )
        .await
    }
}

/// Frozen private origin, key, transport and TLS trust. Shared only inside a
/// validated source profile; creating an input never rereads CA files.
pub(crate) struct MediaOrigin {
    base: Url,
    transport: Transport,
    key: Box<str>,
    tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>,
}
impl MediaOrigin {
    pub(crate) fn new(
        root: &str,
        transport: Transport,
        key: &str,
        ca: Option<&Path>,
    ) -> Result<Self, String> {
        let invalid = invalid_media_configuration;
        if root.len() > 4096
            || root.chars().any(|c| c.is_control() || c.is_whitespace())
            || root.contains('\\')
            || key.is_empty()
            || key.len() > 4096
        {
            return Err(invalid());
        }
        let base = Url::parse(root).map_err(|_| invalid())?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || base.port() == Some(0)
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || ca.is_some() && base.scheme() != "https"
        {
            return Err(invalid());
        }
        let tls = if base.scheme() == "https" {
            Some(crate::tls_input::client(ca).map_err(|_| invalid())?)
        } else {
            None
        };
        Ok(Self {
            base,
            transport,
            key: key.into(),
            tls,
        })
    }
    pub(crate) fn input(&self, name: &str) -> Result<MediaInput, String> {
        let invalid = invalid_media_configuration;
        crate::config::valid_name(name).map_err(|_| invalid())?;
        let mut base = self.base.clone();
        {
            let mut segments = base.path_segments_mut().map_err(|_| invalid())?;
            segments.pop_if_empty();
            for segment in name.split('/') {
                segments.push(segment);
            }
        }
        let mut control = base.clone();
        control.path_segments_mut().map_err(|_| invalid())?.push(
            if self.transport == Transport::M4f {
                "m4f"
            } else {
                "m4s"
            },
        );
        Ok(MediaInput {
            base,
            control,
            is_m4f: self.transport == Transport::M4f,
            key: self.key.clone(),
            name: name.into(),
            tls: self.tls.clone(),
            prepared: tokio::sync::OnceCell::new(),
        })
    }
}
fn invalid_media_configuration() -> String {
    "invalid private media configuration".to_owned()
}

pub(super) fn media_headers(key: &str, name: &str, timestamp: u64) -> HeaderMap {
    // SHA-1 exists solely for the legacy wire contract. Native authentication
    // algorithms are unchanged. Do not format the privileged key into a string.
    let timestamp = timestamp.to_string();
    let mut hash = ring::digest::Context::new(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY);
    hash.update(timestamp.as_bytes());
    hash.update(b":");
    hash.update(key.as_bytes());
    hash.update(b":");
    hash.update(name.as_bytes());
    let digest = hash.finish();
    let hex = digest
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let mut headers = HeaderMap::new();
    for (field, value) in [("cluster-auth-time", timestamp), ("cluster-auth-key", hex)] {
        let mut header =
            HeaderValue::from_str(&value).expect("decimal time and digest are ASCII HTTP values");
        header.set_sensitive(true);
        headers.insert(field, header);
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m4f::Frame;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::AsyncWrite;

    #[tokio::test]
    async fn pinned_hostname_tls_keeps_original_identity_for_native_and_vendor() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let cert = crate::transport_tls_fixture::Certificates::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let Ok(mut stream) = acceptor.accept(socket).await else {
                    continue;
                };
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") && head.len() < 8192 {
                    let mut byte = [0];
                    if stream.read_exact(&mut byte).await.is_err() {
                        break;
                    }
                    head.push(byte[0]);
                }
                if head.ends_with(b"\r\n\r\n") {
                    counted.fetch_add(1, Ordering::SeqCst);
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    let _ = stream.shutdown().await;
                }
            }
        });

        for (hostname, expected_requests) in [("localhost", 1), ("wrong.invalid", 1)] {
            let input = format!("m4ss://{hostname}:{}/owned", address.port());
            let result = super::super::pull_inner_with_resolve(
                &input,
                None,
                &mut Vec::new(),
                None,
                None,
                PullOptions {
                    ca: Some(&cert.ca),
                    ..PullOptions::default()
                },
                move |host, port| async move {
                    assert_eq!(host, hostname);
                    assert_eq!(port, address.port());
                    Ok(vec![address])
                },
            )
            .await;
            assert!(result.is_err(), "empty fixture media must end the pull");
            assert_eq!(
                requests.load(Ordering::SeqCst),
                expected_requests,
                "wrong-host certificate reached native application data"
            );
        }

        for (hostname, expected_requests) in [("localhost", 2), ("wrong.invalid", 2)] {
            let root = format!("https://{hostname}:{}", address.port());
            let input =
                MediaInput::new(&root, "owned", Transport::M4s, "key", Some(&cert.ca)).unwrap();
            let prepared = input
                .prepare_with(move |host, port| async move {
                    assert_eq!(host, hostname);
                    assert_eq!(port, address.port());
                    Ok(vec![address])
                })
                .await
                .unwrap();
            let result = prepared
                .get(prepared.control.clone(), None)
                .unwrap()
                .send()
                .await;
            assert_eq!(result.is_ok(), hostname == "localhost");
            assert_eq!(
                requests.load(Ordering::SeqCst),
                expected_requests,
                "wrong-host certificate reached vendor application data"
            );
        }
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn vendor_m4f_reconnect_and_segments_keep_generation_address_pin() {
        use axum::{Router, body::Body, extract::OriginalUri, response::Response, routing::get};
        use std::{
            net::{IpAddr, Ipv4Addr, SocketAddr},
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
        };
        fn route(hits: Arc<(AtomicUsize, AtomicUsize)>, segment: Vec<u8>) -> Router {
            Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let hits = hits.clone();
                let segment = segment.clone();
                async move {
                    let mut response = if uri.path() == "/owned/m4f" {
                        hits.0.fetch_add(1, Ordering::SeqCst);
                        Response::new(Body::from("1 2026/10/17/00/00/00-2000\n"))
                    } else {
                        assert_eq!(uri.path(), "/owned/2026/10/17/00/00/00.m4f");
                        hits.1.fetch_add(1, Ordering::SeqCst);
                        Response::new(Body::from(segment))
                    };
                    response
                        .headers_mut()
                        .insert("cluster-auth", "yes".parse().unwrap());
                    response
                }
            }))
        }
        let track = Track {
            id: 1,
            codec: "mp3".into(),
            config: vec![],
        };
        let segment = crate::m4f::pack(
            std::slice::from_ref(&track),
            &[crate::m4f::Frame {
                track_id: 1,
                dts: 90_000,
                pts_offset: 0,
                key: true,
                body: include_bytes!("../../tests/fixtures/codecs/mp3.bin").to_vec(),
            }],
            180_000,
        )
        .unwrap();
        let first_hits = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let second_hits = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = first.local_addr().unwrap().port();
        let second = tokio::net::TcpListener::bind((Ipv4Addr::new(127, 0, 0, 2), port))
            .await
            .unwrap();
        let first_router = route(first_hits.clone(), segment.clone());
        let second_router = route(second_hits.clone(), segment);
        let first_server =
            tokio::spawn(async move { axum::serve(first, first_router).await.unwrap() });
        let second_server =
            tokio::spawn(async move { axum::serve(second, second_router).await.unwrap() });
        let root = format!("http://media.invalid:{port}");
        let first_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let second_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), port);
        let input = MediaInput::new(&root, "owned", Transport::M4f, "owned-key", None).unwrap();
        input
            .prepare_with(move |host, _| async move {
                assert_eq!(host, "media.invalid");
                Ok(vec![first_address])
            })
            .await
            .unwrap();
        let mut session = super::super::m4f_session::M4fSession::default();
        let hub = Hub::new();
        let mut output = Vec::new();
        let (metadata, metadata_rx) = oneshot::channel();
        let first_outcome = input
            .pull_m4f_session_guarded(
                &mut output,
                Some(&hub),
                &mut session,
                Some(metadata),
                PullOptions::default(),
                &|| true,
            )
            .await;
        assert_eq!(first_outcome, Err(InputFailure::CleanControlEof));
        assert_eq!(metadata_rx.await.unwrap(), vec![track.clone()]);
        assert!(!output.is_empty());
        assert_eq!(first_hits.0.load(Ordering::SeqCst), 1);
        assert_eq!(first_hits.1.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.0.load(Ordering::SeqCst), 0);
        let reconnect = input
            .pull_m4f_session_guarded(
                &mut output,
                Some(&hub),
                &mut session,
                None,
                PullOptions::default(),
                &|| true,
            )
            .await;
        assert_eq!(reconnect, Err(InputFailure::CleanControlEof));
        assert_eq!(first_hits.0.load(Ordering::SeqCst), 2);
        assert_eq!(first_hits.1.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.0.load(Ordering::SeqCst), 0);
        let replacement =
            MediaInput::new(&root, "owned", Transport::M4f, "owned-key", None).unwrap();
        replacement
            .prepare_with(move |_, _| async move { Ok(vec![second_address]) })
            .await
            .unwrap();
        let mut next_session = super::super::m4f_session::M4fSession::default();
        let (metadata, metadata_rx) = oneshot::channel();
        let next = replacement
            .pull_m4f_session_guarded(
                &mut Vec::new(),
                Some(&Hub::new()),
                &mut next_session,
                Some(metadata),
                PullOptions::default(),
                &|| true,
            )
            .await;
        assert_eq!(next, Err(InputFailure::CleanControlEof));
        assert_eq!(metadata_rx.await.unwrap(), vec![track]);
        assert_eq!(second_hits.0.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.1.load(Ordering::SeqCst), 1);
        first_server.abort();
        second_server.abort();
        let _ = first_server.await;
        let _ = second_server.await;
    }

    #[tokio::test]
    async fn vendor_dns_failure_and_cancelled_setup_leave_input_retryable() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let input = Arc::new(
            MediaInput::new(
                "http://media.invalid:8080",
                "owned",
                Transport::M4f,
                "owned-key",
                None,
            )
            .unwrap(),
        );
        assert!(
            input
                .prepare_with(|_, _| async { Err("synthetic DNS failure".to_owned()) })
                .await
                .is_err()
        );
        let (entered, ready) = oneshot::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let pending_input = input.clone();
        let observed = dropped.clone();
        let task = tokio::spawn(async move {
            pending_input
                .prepare_with(|_, _| async move {
                    let _guard = DropFlag(observed);
                    let _ = entered.send(());
                    std::future::pending::<Result<Vec<std::net::SocketAddr>, String>>().await
                })
                .await
                .map(|_| ())
        });
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .expect("vendor DNS did not start")
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
        let prepared = input
            .prepare_with(|_, _| async { Ok(vec!["127.0.0.1:8080".parse().unwrap()]) })
            .await;
        assert!(
            prepared.is_ok(),
            "failed or cancelled setup poisoned the input"
        );
    }

    struct BrokenWriter;
    impl AsyncWrite for BrokenWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // Break caught: an established, accepted M4F control EOF is confused with
    // a 503, malformed signal, denied segment, or failed local media pipe.
    #[tokio::test]
    async fn clean_m4f_eof_after_acknowledged_media_is_typed() {
        use axum::{
            Router, body::Body, extract::OriginalUri, http::StatusCode, response::Response,
            routing::get,
        };
        #[derive(Clone, Copy)]
        enum Case {
            Complete,
            BeforeMedia,
            Unavailable,
            Malformed,
            TruncatedAfterMedia,
            DeniedSegment,
            BrokenPipe,
        }
        let track = Track {
            id: 1,
            codec: "mp3".into(),
            config: vec![],
        };
        let segment = crate::m4f::pack(
            std::slice::from_ref(&track),
            &[Frame {
                track_id: 1,
                dts: 90_000,
                pts_offset: 0,
                key: true,
                body: include_bytes!("../../tests/fixtures/codecs/mp3.bin").to_vec(),
            }],
            180_000,
        )
        .unwrap();
        for (case, expected) in [
            (Case::Complete, "CleanControlEof"),
            (Case::BeforeMedia, "Transient"),
            (Case::Unavailable, "Transient"),
            (Case::Malformed, "Invalid"),
            (Case::TruncatedAfterMedia, "Invalid"),
            (Case::DeniedSegment, "Rejected"),
            (Case::BrokenPipe, "Local"),
        ] {
            let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let root = format!("http://{}", tcp.local_addr().unwrap());
            let bytes = segment.clone();
            let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let bytes = bytes.clone();
                async move {
                    let mut response = if uri.path() == "/owned/m4f" {
                        match case {
                            Case::Complete | Case::DeniedSegment | Case::BrokenPipe => {
                                Response::new(Body::from("1 2026/10/17/00/00/00-2000\n"))
                            }
                            Case::BeforeMedia => Response::new(Body::empty()),
                            Case::Malformed => Response::new(Body::from("bad\n")),
                            Case::TruncatedAfterMedia => {
                                Response::new(Body::from("1 2026/10/17/00/00/00-2000\nbad"))
                            }
                            Case::Unavailable => {
                                let mut r = Response::new(Body::empty());
                                *r.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                                r
                            }
                        }
                    } else {
                        assert_eq!(uri.path(), "/owned/2026/10/17/00/00/00.m4f");
                        let mut r = Response::new(Body::from(bytes));
                        if matches!(case, Case::DeniedSegment) {
                            *r.status_mut() = StatusCode::FORBIDDEN;
                        }
                        r
                    };
                    response
                        .headers_mut()
                        .insert("cluster-auth", "yes".parse().unwrap());
                    response
                }
            }));
            let server = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
            let input = MediaInput::new(&root, "owned", Transport::M4f, "owned-key", None).unwrap();
            let hub = Hub::new();
            let (tx, rx) = oneshot::channel();
            let mut session = super::super::m4f_session::M4fSession::default();
            let kind = if matches!(case, Case::BrokenPipe) {
                input
                    .pull_m4f_session_guarded(
                        &mut BrokenWriter,
                        Some(&hub),
                        &mut session,
                        Some(tx),
                        PullOptions::default(),
                        &|| true,
                    )
                    .await
                    .unwrap_err()
            } else {
                let mut output = Vec::new();
                let kind = input
                    .pull_m4f_session_guarded(
                        &mut output,
                        Some(&hub),
                        &mut session,
                        Some(tx),
                        PullOptions::default(),
                        &|| true,
                    )
                    .await
                    .unwrap_err();
                if matches!(case, Case::Complete | Case::TruncatedAfterMedia) {
                    assert!(!output.is_empty());
                    assert!(hub.has_info());
                } else {
                    assert!(output.is_empty());
                }
                kind
            };
            if matches!(case, Case::Complete) {
                let (legacy_tx, legacy_rx) = oneshot::channel();
                let legacy = input
                    .pull_guarded_classified(
                        &mut Vec::new(),
                        None,
                        legacy_tx,
                        PullOptions::default(),
                        &|| true,
                    )
                    .await;
                assert_eq!(legacy, Err(InputFailure::Transient));
                assert_eq!(legacy_rx.await.unwrap(), vec![track.clone()]);
            }
            server.abort();
            let _ = server.await;
            if matches!(
                case,
                Case::Complete | Case::TruncatedAfterMedia | Case::BrokenPipe
            ) {
                assert_eq!(rx.await.unwrap(), vec![track.clone()]);
            } else {
                assert!(rx.await.is_err());
            }
            assert_eq!(format!("{kind:?}"), expected);
        }
    }
    // Break caught: inventory hash, wrong byte ordering, minutes/milliseconds,
    // or a credential header without the sensitive diagnostic marker.
    #[test]
    fn media_credentials_match_independent_literal_vectors() {
        for (timestamp, key, name, expected) in [
            (
                0,
                "synthetic-cluster-key",
                "news",
                "1402a8eb49a99e0232b33073358857f01a890745",
            ),
            (
                1791700000,
                "synthetic-cluster-key",
                "region/news",
                "4e477dcdad853d680693f64e8d3497354a299b48",
            ),
            (
                1791700001,
                "synthetic:other-key",
                "region/news",
                "fa4999372ed5cce9cf806b8b0e2a7139497480b3",
            ),
            (
                1791700000,
                "synthetic-ключ",
                "region/канал",
                "746ee10eb973f29f04c7b6c4f09110591e6ea188",
            ),
        ] {
            let headers = media_headers(key, name, timestamp);
            assert_eq!(
                headers
                    .get("cluster-auth-time")
                    .map(|h| h.to_str().unwrap()),
                Some(timestamp.to_string().as_str())
            );
            assert_eq!(
                headers.get("cluster-auth-key").map(|h| h.to_str().unwrap()),
                Some(expected)
            );
            assert!(headers["cluster-auth-key"].is_sensitive());
            assert!(headers["cluster-auth-time"].is_sensitive());
            assert_eq!(headers.len(), 2);
        }
    }

    #[tokio::test]
    async fn revoked_authority_precedes_announced_segment_status_classification() {
        use axum::{
            Router, body::Body, extract::OriginalUri, http::StatusCode, response::Response,
            routing::get,
        };
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;
        #[derive(Clone)]
        struct Failures(Arc<AtomicUsize>);
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Failures {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if event.metadata().target() == "flussonix::private_input" {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        for status in [StatusCode::NOT_FOUND, StatusCode::FORBIDDEN] {
            let failures = Arc::new(AtomicUsize::new(0));
            let subscriber = tracing_subscriber::registry().with(Failures(failures.clone()));
            let current = Arc::new(AtomicBool::new(true));
            let live = current.clone();
            let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let root = format!("http://{}", tcp.local_addr().unwrap());
            let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let live = live.clone();
                async move {
                    let mut response = if uri.path() == "/owned/m4f" {
                        Response::new(Body::from("1 2026/10/17/00/00/00-2000\n"))
                    } else {
                        assert_eq!(uri.path(), "/owned/2026/10/17/00/00/00.m4f");
                        live.store(false, Ordering::SeqCst);
                        let mut response = Response::new(Body::empty());
                        *response.status_mut() = status;
                        response
                    };
                    response
                        .headers_mut()
                        .insert("cluster-auth", "yes".parse().unwrap());
                    response
                }
            }));
            let server = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
            let input = MediaInput::new(&root, "owned", Transport::M4f, "owned-key", None).unwrap();
            let (tx, rx) = oneshot::channel();
            let mut output = Vec::new();
            let hub = Hub::new();
            let authority = || current.load(Ordering::SeqCst);
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                input
                    .pull_guarded_classified(
                        &mut output,
                        Some(&hub),
                        tx,
                        PullOptions::default(),
                        &authority,
                    )
                    .with_subscriber(subscriber),
            )
            .await;
            server.abort();
            let _ = server.await;
            assert_eq!(result.unwrap().unwrap_err(), InputFailure::Revoked);
            assert!(output.is_empty() && rx.await.is_err() && !hub.has_info());
            assert_eq!(
                failures.load(Ordering::SeqCst),
                0,
                "authority revocation is not a private input failure"
            );
        }
    }
}
