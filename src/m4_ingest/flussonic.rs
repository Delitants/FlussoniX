//! Explicit stream-scoped Flussonic HTTP media authentication.
//!
//! This library client is not selected by the daemon's source resolver. No
//! official Flussonic components are required. Live reference authentication,
//! viewer-policy inheritance and vendor source integration remain separate gates.
use super::{PullOptions, RequestAuth, Requests};
use crate::{m4s::Track, wire::Hub};
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
        let invalid = || "invalid private media configuration".to_owned();
        if root.len() > 4096
            || root.chars().any(|c| c.is_control() || c.is_whitespace())
            || root.contains('\\')
            || key.is_empty()
            || key.len() > 4096
        {
            return Err(invalid());
        }
        crate::config::valid_name(name).map_err(|_| invalid())?;
        let mut base = Url::parse(root).map_err(|_| invalid())?;
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
        {
            let mut segments = base.path_segments_mut().map_err(|_| invalid())?;
            segments.pop_if_empty();
            for segment in name.split('/') {
                segments.push(segment);
            }
        }
        let mut control = base.clone();
        control
            .path_segments_mut()
            .map_err(|_| invalid())?
            .push(if transport == Transport::M4f {
                "m4f"
            } else {
                "m4s"
            });
        let mut client = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .https_only(base.scheme() == "https")
            .redirect(reqwest::redirect::Policy::none());
        if base.scheme() == "https" {
            client = client.use_preconfigured_tls(
                (*crate::tls_input::client(ca).map_err(|_| invalid())?).clone(),
            );
        }
        let client = client.build().map_err(|_| invalid())?;
        Ok(Self {
            requests: Requests {
                client,
                base,
                control,
                is_m4f: transport == Transport::M4f,
                auth: RequestAuth::Flussonic {
                    key: key.into(),
                    name: name.into(),
                },
            },
        })
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
}
