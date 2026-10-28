//! Bounded, authenticated client for the private IPTV lease authority.
use crate::iptv::lease::{SessionKey, SubscriberId};
use serde::{Deserialize, Serialize};
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
const HA_TOTAL_TIMEOUT: Duration = Duration::from_secs(4);

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
    primary: OriginState,
    secondary: Option<OriginState>,
    expected_cluster: Option<Uuid>,
    key: String,
    http: reqwest::Client,
    in_flight: Semaphore,
    started: Instant,
}

struct OriginState {
    url: Url,
    last_reachable_ms: AtomicU64,
    last_ready_ms: AtomicU64,
    last_probe_refused: AtomicBool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OriginHealth {
    pub origin: String,
    pub reachable: bool,
    pub ready: bool,
    pub refused: bool,
}

impl OriginState {
    fn new(url: Url) -> Self {
        Self {
            url,
            last_reachable_ms: AtomicU64::new(0),
            last_ready_ms: AtomicU64::new(0),
            last_probe_refused: AtomicBool::new(false),
        }
    }

    fn recent(last: &AtomicU64, started: Instant, window_ms: u128) -> bool {
        let value = last.load(Ordering::Relaxed);
        value != 0
            && started
                .elapsed()
                .as_millis()
                .saturating_sub(u128::from(value))
                <= window_ms
    }

    fn health(&self, started: Instant) -> OriginHealth {
        OriginHealth {
            origin: self.url.to_string(),
            reachable: Self::recent(&self.last_reachable_ms, started, 15_000),
            ready: Self::recent(&self.last_ready_ms, started, 12_000),
            refused: self.last_probe_refused.load(Ordering::Relaxed),
        }
    }
}

enum AttemptError {
    Transport,
    InvalidResponse,
}

fn validate_origin(origin: &Url, allow_loopback: bool) -> Result<(), String> {
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
    Ok(())
}

impl AuthorityClient {
    pub fn new(
        origin: Url,
        key: String,
        ca: Option<&Path>,
        allow_loopback: bool,
    ) -> Result<Self, String> {
        validate_origin(&origin, allow_loopback)?;
        if ca.is_some() && origin.scheme() != "https" {
            return Err("IPTV authority CA requires HTTPS".into());
        }
        Self::build(origin, None, None, key, ca)
    }

    pub fn new_with_secondary(
        primary: Url,
        secondary: Url,
        expected_cluster: Uuid,
        key: String,
        ca: Option<&Path>,
        _allow_loopback: bool,
    ) -> Result<Self, String> {
        validate_origin(&primary, false)?;
        validate_origin(&secondary, false)?;
        if primary == secondary || expected_cluster.is_nil() {
            return Err("IPTV HA requires two distinct origins and a nonempty cluster ID".into());
        }
        Self::build(primary, Some(secondary), Some(expected_cluster), key, ca)
    }

    fn build(
        primary: Url,
        secondary: Option<Url>,
        expected_cluster: Option<Uuid>,
        key: String,
        ca: Option<&Path>,
    ) -> Result<Self, String> {
        if key.len() < 12 {
            return Err("IPTV peer key must have at least 12 characters".into());
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
            primary: OriginState::new(primary),
            secondary: secondary.map(OriginState::new),
            expected_cluster,
            key,
            http,
            in_flight: Semaphore::new(32),
            started: Instant::now(),
        })
    }

    pub fn origin_health(&self) -> Vec<OriginHealth> {
        let mut health = vec![self.primary.health(self.started)];
        if let Some(secondary) = &self.secondary {
            health.push(secondary.health(self.started));
        }
        health
    }

    pub fn recently_reachable(&self) -> bool {
        self.origin_health().iter().any(|origin| origin.reachable)
    }

    pub fn recently_ready(&self) -> bool {
        self.origin_health().iter().any(|origin| origin.ready)
    }

    pub fn last_probe_refused(&self) -> bool {
        self.origin_health().iter().any(|origin| origin.refused)
    }

    pub async fn probe(&self) -> bool {
        let primary = self.probe_one(&self.primary).await;
        let secondary = match &self.secondary {
            Some(origin) => self.probe_one(origin).await,
            None => false,
        };
        primary || secondary
    }

    async fn probe_one(&self, origin: &OriginState) -> bool {
        let response = if self.secondary.is_none() {
            self.send("status", &json!({})).await.ok()
        } else {
            let _permit = match self.in_flight.try_acquire() {
                Ok(permit) => permit,
                Err(_) => return false,
            };
            self.once_at(origin, "status", &json!({})).await.ok()
        };
        let (ready, refused) = match response {
            Some((reqwest::StatusCode::OK, bytes, _)) => {
                match serde_json::from_slice::<WireStatus>(&bytes) {
                    Ok(status) => (status.ready, !status.ready),
                    Err(_) => (false, false),
                }
            }
            _ => (false, false),
        };
        origin.last_probe_refused.store(refused, Ordering::Relaxed);
        origin.last_ready_ms.store(
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

    async fn once_at(
        &self,
        origin: &OriginState,
        action: &str,
        body: &Value,
    ) -> Result<(reqwest::StatusCode, Vec<u8>, Instant), AttemptError> {
        let url = origin
            .url
            .join(&format!("flussonix/peer/v1/iptv/leases/{action}"))
            .map_err(|_| AttemptError::InvalidResponse)?;
        let sent_at = Instant::now();
        let mut response = self
            .http
            .post(url)
            .header("x-flussonix-peer", &self.key)
            .json(body)
            .send()
            .await
            .map_err(|_| AttemptError::Transport)?;
        if let Some(cluster) = self.expected_cluster {
            let marker = response
                .headers()
                .get("x-flussonix-iptv-cluster-id")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| Uuid::parse_str(value).ok());
            if marker != Some(cluster) {
                return Err(AttemptError::InvalidResponse);
            }
        }
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE as u64)
        {
            return Err(AttemptError::InvalidResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| AttemptError::Transport)?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(AttemptError::InvalidResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        origin.last_reachable_ms.store(
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
        if let Some(secondary) = &self.secondary {
            return tokio::time::timeout(HA_TOTAL_TIMEOUT, async {
                let primary = self.once_at(&self.primary, action, body).await;
                match primary {
                    Ok((status, _, _)) if status == reqwest::StatusCode::FORBIDDEN => {
                        Err(Error::Denied)
                    }
                    Ok((status, _, _)) if status == reqwest::StatusCode::UNAUTHORIZED => {
                        Err(Error::Unavailable)
                    }
                    Ok((status, _, _)) if status == reqwest::StatusCode::CONFLICT => {
                        Err(Error::Limit)
                    }
                    Ok((status, _, _)) if status == reqwest::StatusCode::BAD_REQUEST => {
                        Err(Error::Invalid)
                    }
                    Ok((status, _, _)) if status == reqwest::StatusCode::SERVICE_UNAVAILABLE => {
                        self.secondary_attempt(secondary, action, body).await
                    }
                    Err(AttemptError::Transport) => {
                        self.secondary_attempt(secondary, action, body).await
                    }
                    Ok(result) => Ok(result),
                    Err(AttemptError::InvalidResponse) => Err(Error::Unavailable),
                }
            })
            .await
            .map_err(|_| Error::Unavailable)?;
        }
        for attempt in 0..2 {
            match self.once_at(&self.primary, action, body).await {
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
                Ok((status, _, _)) if status.is_server_error() && attempt == 0 => continue,
                Ok(result) => return Ok(result),
                Err(AttemptError::Transport) if attempt == 0 => continue,
                Err(_) => return Err(Error::Unavailable),
            }
        }
        Err(Error::Unavailable)
    }

    async fn secondary_attempt(
        &self,
        secondary: &OriginState,
        action: &str,
        body: &Value,
    ) -> Result<(reqwest::StatusCode, Vec<u8>, Instant), Error> {
        match self.once_at(secondary, action, body).await {
            Ok((status, _, _)) if status == reqwest::StatusCode::FORBIDDEN => Err(Error::Denied),
            Ok((status, _, _)) if status == reqwest::StatusCode::UNAUTHORIZED => {
                Err(Error::Unavailable)
            }
            Ok((status, _, _)) if status == reqwest::StatusCode::CONFLICT => Err(Error::Limit),
            Ok((status, _, _)) if status == reqwest::StatusCode::BAD_REQUEST => Err(Error::Invalid),
            Ok(result) => Ok(result),
            Err(_) => Err(Error::Unavailable),
        }
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
