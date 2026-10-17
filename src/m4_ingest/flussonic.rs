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
use std::{path::Path, time::Duration};
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
    requests: Requests,
}
impl MediaInput {
    pub(crate) fn transport(&self) -> Transport {
        if self.requests.is_m4f {
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
        super::consume_guarded(
            &self.requests,
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
    /// One input session. Reopening the same client obtains fresh credentials;
    /// the caller owns retry/backoff and cancellation. Metadata precedes TS
    /// writes. The existing public ingest defaults apply: subtitle tracks are
    /// omitted from the native relay. No access policy is inferred here.
    pub async fn pull_ready<W: AsyncWrite + Unpin>(
        &self,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
    ) -> Result<(), String> {
        super::consume(
            &self.requests,
            output,
            hub,
            Some(metadata),
            PullOptions::default(),
        )
        .await
    }
}

/// Frozen private origin, key, transport and prepared TLS client. Shared only
/// inside a validated source profile; creating an input never rereads CA files.
pub(crate) struct MediaOrigin {
    base: Url,
    transport: Transport,
    key: Box<str>,
    client: reqwest::Client,
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
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .https_only(base.scheme() == "https")
            .redirect(reqwest::redirect::Policy::none());
        if base.scheme() == "https" {
            builder = builder.use_preconfigured_tls(
                (*crate::tls_input::client(ca).map_err(|_| invalid())?).clone(),
            );
        }
        Ok(Self {
            base,
            transport,
            key: key.into(),
            client: builder.build().map_err(|_| invalid())?,
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
            requests: Requests {
                client: self.client.clone(),
                base,
                control,
                is_m4f: self.transport == Transport::M4f,
                auth: RequestAuth::Flussonic {
                    key: self.key.clone(),
                    name: name.into(),
                },
            },
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
