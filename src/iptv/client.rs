//! Bounded, authenticated client for the private IPTV lease authority.
use crate::iptv::lease::{SessionKey, SubscriberId};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;
use url::{Host, Url};
use uuid::Uuid;

const MAX_RESPONSE: usize = 8 * 1024;
const MAX_TTL_MS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Unavailable,
    Denied,
    Limit,
    Invalid,
}

#[derive(Clone)]
pub struct LeaseInput {
    pub token: String,
    pub channel: String,
    pub session: SessionKey,
    pub operation: Uuid,
    pub units: u32,
    pub lease_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy)]
pub struct LeaseReply {
    pub subscriber: SubscriberId,
    pub lease_id: Uuid,
    pub revision: u64,
    pub units: u32,
    pub deadline: Instant,
    pub subscriber_expires_at: Option<SystemTime>,
}

#[derive(Deserialize)]
struct WireGrant {
    subscriber_id: String,
    lease_id: String,
    revision: u64,
    units: u32,
    ttl_ms: u64,
    subscriber_expires_at_unix_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireStatus {
    ready: bool,
}

pub struct AuthorityClient {
    origin: Url,
    key: String,
    http: reqwest::Client,
    in_flight: Semaphore,
    started: Instant,
    last_reachable_ms: AtomicU64,
    last_ready_ms: AtomicU64,
    last_probe_refused: AtomicBool,
}

impl AuthorityClient {
    pub fn new(
        origin: Url,
        key: String,
        ca: Option<&Path>,
        allow_loopback: bool,
    ) -> Result<Self, String> {
        let literal_loopback = matches!(origin.host(), Some(Host::Ipv4(ip)) if ip.is_loopback())
            || matches!(origin.host(), Some(Host::Ipv6(ip)) if ip.is_loopback());
        if origin.host().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !(origin.scheme() == "https"
                || (origin.scheme() == "http" && literal_loopback && allow_loopback))
        {
            return Err("IPTV authority requires a credential-free HTTPS origin or explicit literal loopback HTTP".into());
        }
        if key.len() < 12 {
            return Err("IPTV peer key must have at least 12 characters".into());
        }
        if ca.is_some() && origin.scheme() != "https" {
            return Err("IPTV authority CA requires HTTPS".into());
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(8);
        if ca.is_some() {
            builder = builder.use_preconfigured_tls((*crate::tls_input::client(ca)?).clone());
        }
        let http = builder
            .build()
            .map_err(|_| "cannot build IPTV authority client")?;
        Ok(Self {
            origin,
            key,
            http,
            in_flight: Semaphore::new(32),
            started: Instant::now(),
            last_reachable_ms: AtomicU64::new(0),
            last_ready_ms: AtomicU64::new(0),
            last_probe_refused: AtomicBool::new(false),
        })
    }

    pub fn recently_reachable(&self) -> bool {
        let last = self.last_reachable_ms.load(Ordering::Relaxed);
        last != 0
            && self
                .started
                .elapsed()
                .as_millis()
                .saturating_sub(u128::from(last))
                <= 15_000
    }

    pub fn recently_ready(&self) -> bool {
        let last = self.last_ready_ms.load(Ordering::Relaxed);
        last != 0
            && self
                .started
                .elapsed()
                .as_millis()
                .saturating_sub(u128::from(last))
                <= 12_000
    }

    pub fn last_probe_refused(&self) -> bool {
        self.last_probe_refused.load(Ordering::Relaxed)
    }

    pub async fn probe(&self) -> bool {
        let (ready, refused) = match self.send("status", &json!({})).await {
            Ok((reqwest::StatusCode::OK, bytes, _)) => {
                match serde_json::from_slice::<WireStatus>(&bytes) {
                    Ok(status) => (status.ready, !status.ready),
                    Err(_) => (false, false),
                }
            }
            _ => (false, false),
        };
        self.last_probe_refused.store(refused, Ordering::Relaxed);
        self.last_ready_ms.store(
            if ready {
                self.started.elapsed().as_millis().min(u64::MAX as u128) as u64 + 1
            } else {
                0
            },
            Ordering::Relaxed,
        );
        ready
    }

    fn body(input: &LeaseInput) -> Result<Value, Error> {
        if input.token.is_empty()
            || input.token.len() > 4096
            || crate::config::valid_name(&input.channel).is_err()
            || input.units == 0
            || input.units > 1024
        {
            return Err(Error::Invalid);
        }
        Ok(json!({
            "token": input.token, "channel": input.channel,
            "boot": input.session.boot.to_string(), "session": input.session.local.to_string(),
            "operation": input.operation.to_string(), "units": input.units,
            "lease_id": input.lease_id.map(|id| id.to_string())
        }))
    }

    async fn once(
        &self,
        action: &str,
        body: &Value,
    ) -> Result<(reqwest::StatusCode, Vec<u8>, Instant), Error> {
        let url = self
            .origin
            .join(&format!("flussonix/peer/v1/iptv/leases/{action}"))
            .map_err(|_| Error::Invalid)?;
        let sent_at = Instant::now();
        let mut response = self
            .http
            .post(url)
            .header("x-flussonix-peer", &self.key)
            .json(body)
            .send()
            .await
            .map_err(|_| Error::Unavailable)?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE as u64)
        {
            return Err(Error::Unavailable);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Error::Unavailable)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(Error::Unavailable);
            }
            bytes.extend_from_slice(&chunk);
        }
        self.last_reachable_ms.store(
            self.started.elapsed().as_millis().min(u64::MAX as u128) as u64 + 1,
            Ordering::Relaxed,
        );
        Ok((status, bytes, sent_at))
    }

    async fn send(
        &self,
        action: &str,
        body: &Value,
    ) -> Result<(reqwest::StatusCode, Vec<u8>, Instant), Error> {
        let _permit = self
            .in_flight
            .try_acquire()
            .map_err(|_| Error::Unavailable)?;
        for attempt in 0..2 {
            match self.once(action, body).await {
                Ok((status, _, _)) if status == reqwest::StatusCode::FORBIDDEN => {
                    return Err(Error::Denied);
                }
                Ok((status, _, _)) if status == reqwest::StatusCode::UNAUTHORIZED => {
                    return Err(Error::Unavailable);
                }
                Ok((status, _, _)) if status == reqwest::StatusCode::CONFLICT => {
                    return Err(Error::Limit);
                }
                Ok((status, _, _)) if status == reqwest::StatusCode::BAD_REQUEST => {
                    return Err(Error::Invalid);
                }
                Ok((status, _, _)) if status.is_server_error() && attempt == 0 => {
                    continue;
                }
                Ok(result) => return Ok(result),
                Err(Error::Unavailable) if attempt == 0 => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::Unavailable)
    }

    async fn grant(&self, action: &str, input: &LeaseInput) -> Result<LeaseReply, Error> {
        if action != "acquire" && input.lease_id.is_none() {
            return Err(Error::Invalid);
        }
        let body = Self::body(input)?;
        let (status, bytes, sent_at) = self.send(action, &body).await?;
        if status != reqwest::StatusCode::OK {
            return Err(Error::Unavailable);
        }
        let wire: WireGrant = serde_json::from_slice(&bytes).map_err(|_| Error::Unavailable)?;
        if wire.units != input.units || wire.ttl_ms == 0 || wire.ttl_ms > MAX_TTL_MS {
            return Err(Error::Unavailable);
        }
        let subscriber = SubscriberId::parse_hex(&wire.subscriber_id).ok_or(Error::Unavailable)?;
        let lease_id = Uuid::parse_str(&wire.lease_id).map_err(|_| Error::Unavailable)?;
        if input.lease_id.is_some_and(|expected| expected != lease_id) {
            return Err(Error::Unavailable);
        }
        let subscriber_expires_at = wire
            .subscriber_expires_at_unix_ms
            .map(|ms| {
                UNIX_EPOCH
                    .checked_add(Duration::from_millis(ms))
                    .ok_or(Error::Unavailable)
            })
            .transpose()?;
        let deadline = sent_at + Duration::from_millis(wire.ttl_ms);
        if deadline <= Instant::now() {
            return Err(Error::Unavailable);
        }
        Ok(LeaseReply {
            subscriber,
            lease_id,
            revision: wire.revision,
            units: wire.units,
            deadline,
            subscriber_expires_at,
        })
    }

    pub async fn acquire(&self, input: &LeaseInput) -> Result<LeaseReply, Error> {
        self.grant("acquire", input).await
    }
    pub async fn commit(&self, input: &LeaseInput) -> Result<LeaseReply, Error> {
        self.grant("commit", input).await
    }
    pub async fn renew(&self, input: &LeaseInput) -> Result<LeaseReply, Error> {
        self.grant("renew", input).await
    }

    async fn finish(
        &self,
        action: &str,
        input: &LeaseInput,
        reply: &LeaseReply,
    ) -> Result<(), Error> {
        let body = json!({
            "subscriber_id": reply.subscriber.as_hex(),
            "boot": input.session.boot.to_string(), "session": input.session.local.to_string(),
            "lease_id": reply.lease_id.to_string(), "operation": input.operation.to_string(),
        });
        let (status, _, _) = self.send(action, &body).await?;
        if status == reqwest::StatusCode::NO_CONTENT || status == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }
    pub async fn release(&self, input: &LeaseInput, reply: &LeaseReply) -> Result<(), Error> {
        self.finish("release", input, reply).await
    }
    pub async fn rollback(&self, input: &LeaseInput, reply: &LeaseReply) -> Result<(), Error> {
        self.finish("rollback", input, reply).await
    }
}
