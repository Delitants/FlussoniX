//! Private FlussoniX-to-FlussoniX subscriber lease authority.
use super::*;
use crate::iptv::{
    ha::connection::HaError,
    lease::{Error as LeaseError, LeaseGrant, LeaseRequest, SessionKey, SubscriberId},
    store::{IptvGrantFacts, IptvSnapshot},
};
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    net::IpAddr,
    sync::Mutex as StdMutex,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

const MAX_BODY: usize = 8 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_GLOBAL: usize = 64;
const MAX_PER_ADDRESS: u8 = 8;
type PeerFailure = (StatusCode, &'static str);
type PeerResult<T> = Result<T, PeerFailure>;

pub(super) struct Admission {
    global: Semaphore,
    by_address: StdMutex<HashMap<IpAddr, u8>>,
}

struct Slot<'a> {
    admission: &'a Admission,
    address: IpAddr,
    _global: SemaphorePermit<'a>,
}

impl Admission {
    pub(super) fn new() -> Self {
        Self {
            global: Semaphore::new(MAX_GLOBAL),
            by_address: StdMutex::new(HashMap::new()),
        }
    }

    fn try_acquire(&self, address: IpAddr) -> Option<Slot<'_>> {
        let global = self.global.try_acquire().ok()?;
        let mut addresses = self.by_address.lock().unwrap();
        let count = addresses.entry(address).or_default();
        if *count >= MAX_PER_ADDRESS {
            return None;
        }
        *count += 1;
        Some(Slot {
            admission: self,
            address,
            _global: global,
        })
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        let mut addresses = self.admission.by_address.lock().unwrap();
        let Some(count) = addresses.get_mut(&self.address) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            addresses.remove(&self.address);
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcquireBody {
    token: String,
    channel: String,
    boot: String,
    session: String,
    operation: String,
    units: u32,
    #[serde(default)]
    lease_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseBody {
    subscriber_id: String,
    boot: String,
    session: String,
    lease_id: String,
    operation: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusBody {}

async fn peer_body<T: DeserializeOwned>(app: &App, request: Request) -> PeerResult<(T, Slot<'_>)> {
    if !app.options.iptv_lease_authority {
        return Err((StatusCode::NOT_FOUND, "IPTV authority unavailable"));
    }
    let secure = request
        .extensions()
        .get::<crate::http_tls::SecureHttp>()
        .is_some();
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|peer| peer.0.ip());
    let loopback = peer.is_some_and(|address| address.is_loopback());
    if !secure && !(app.options.iptv_peer_allow_insecure_loopback && loopback) {
        return Err((StatusCode::FORBIDDEN, "secure IPTV peer transport required"));
    }
    if !app
        .credentials
        .peer(header(request.headers(), "x-flussonix-peer"))
    {
        return Err((StatusCode::UNAUTHORIZED, "peer credentials required"));
    }
    let Some(slot) = peer.and_then(|peer| app.iptv_peer_admission.try_acquire(peer)) else {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "IPTV authority busy"));
    };
    let bytes = match tokio::time::timeout(
        READ_TIMEOUT,
        axum::body::to_bytes(request.into_body(), MAX_BODY),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "IPTV request exceeds limit"));
        }
        Err(_) => return Err((StatusCode::REQUEST_TIMEOUT, "IPTV request timed out")),
    };
    let body = serde_json::from_slice::<T>(&bytes)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid IPTV lease request"))?;
    Ok((body, slot))
}

fn viewer_request(
    app: &App,
    body: &AcquireBody,
    wall: SystemTime,
) -> PeerResult<(IptvGrantFacts, LeaseRequest)> {
    let snapshot = app.iptv.snapshot();
    viewer_request_from_snapshot(&snapshot, body, wall)
}

fn viewer_request_from_snapshot(
    snapshot: &IptvSnapshot,
    body: &AcquireBody,
    wall: SystemTime,
) -> PeerResult<(IptvGrantFacts, LeaseRequest)> {
    if body.token.is_empty() || body.token.len() > 4096 || valid_name(&body.channel).is_err() {
        return Err((StatusCode::BAD_REQUEST, "invalid IPTV lease request"));
    }
    let (Ok(boot), Ok(local), Ok(operation)) = (
        Uuid::parse_str(&body.boot),
        Uuid::parse_str(&body.session),
        Uuid::parse_str(&body.operation),
    ) else {
        return Err((StatusCode::BAD_REQUEST, "invalid IPTV lease request"));
    };
    let Some(facts) = snapshot.authorize(&body.token, &body.channel, wall) else {
        return Err((StatusCode::FORBIDDEN, "IPTV subscriber access denied"));
    };
    let subscriber = SubscriberId::from_name(&facts.subscriber);
    Ok((
        facts.clone(),
        LeaseRequest {
            subscriber,
            session: SessionKey { boot, local },
            operation,
            units: body.units,
            limit: facts.max_sessions,
            revision: facts.revision,
            subscriber_expires_at: facts.expires_at,
        },
    ))
}

fn ha_lease_error(kind: HaError) -> Response {
    let mut response = match kind {
        HaError::Limit => error(StatusCode::CONFLICT, "IPTV session limit reached"),
        HaError::Expired => error(StatusCode::FORBIDDEN, "IPTV subscriber access denied"),
        HaError::Invalid => error(StatusCode::BAD_REQUEST, "invalid IPTV lease request"),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "IPTV authority unavailable",
        ),
    };
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}

fn ha_no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}

fn lease_error(error_kind: LeaseError) -> Response {
    match error_kind {
        LeaseError::Quarantine | LeaseError::Full | LeaseError::Stale | LeaseError::Missing => {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "IPTV authority unavailable",
            )
        }
        LeaseError::Limit => error(StatusCode::CONFLICT, "IPTV session limit reached"),
        LeaseError::Expired => error(StatusCode::FORBIDDEN, "IPTV subscriber access denied"),
        LeaseError::Invalid => error(StatusCode::BAD_REQUEST, "invalid IPTV lease request"),
    }
}

fn grant_response(
    grant: LeaseGrant,
    subscriber: SubscriberId,
    expires_at: Option<SystemTime>,
) -> Response {
    let ttl_ms = grant
        .deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    if ttl_ms == 0 {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "IPTV authority unavailable",
        );
    }
    let mut response = json_response(json!({
        "subscriber_id":subscriber.as_hex(),
        "lease_id":grant.id.to_string(),
        "revision":grant.revision,
        "units":grant.units,
        "ttl_ms":ttl_ms,
        "subscriber_expires_at_unix_ms":expires_at.and_then(|expiry| expiry.duration_since(UNIX_EPOCH).ok()).map(|since| since.as_millis().min(u128::from(u64::MAX)) as u64),
    }));
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}

pub(super) async fn status(State(app): State<Arc<App>>, request: Request) -> Response {
    if app.iptv_ha.is_some() {
        return ha_no_store(
            tokio::time::timeout(READ_TIMEOUT, status_inner(app, request))
                .await
                .unwrap_or_else(|_| ha_lease_error(HaError::Unavailable)),
        );
    }
    status_inner(app, request).await
}

async fn status_inner(app: Arc<App>, request: Request) -> Response {
    let (_, _slot): (StatusBody, _) = match peer_body(&app, request).await {
        Ok(body) => body,
        Err((status, message)) => {
            let response = error(status, message);
            return if app.iptv_ha.is_some() {
                ha_no_store(response)
            } else {
                response
            };
        }
    };
    if let Some(ha) = &app.iptv_ha {
        let ready = ha.pool.verify_startup().await.is_ok()
            && ha
                .catalog
                .load_current()
                .await
                .is_ok_and(|snapshot| snapshot.enabled())
            && ha.ledger.ready().await.unwrap_or(false);
        let mut response = json_response(json!({"ready": ready}));
        response
            .headers_mut()
            .insert("cache-control", "no-store".parse().unwrap());
        response.headers_mut().insert(
            "x-flussonix-iptv-cluster-id",
            ha.pool.cluster_id().to_string().parse().unwrap(),
        );
        return response;
    }
    let ready =
        app.iptv.snapshot().enabled() && app.iptv_leases.ready_for_new_leases(Instant::now());
    let mut response = json_response(json!({"ready": ready}));
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}

pub(super) async fn acquire(State(app): State<Arc<App>>, request: Request) -> Response {
    if app.iptv_ha.is_some() {
        return ha_no_store(
            tokio::time::timeout(READ_TIMEOUT, acquire_inner(app, request))
                .await
                .unwrap_or_else(|_| ha_lease_error(HaError::Unavailable)),
        );
    }
    acquire_inner(app, request).await
}

async fn acquire_inner(app: Arc<App>, request: Request) -> Response {
    let (body, _slot): (AcquireBody, _) = match peer_body(&app, request).await {
        Ok(body) => body,
        Err((status, message)) => return error(status, message),
    };
    if body.lease_id.is_some() {
        return error(StatusCode::BAD_REQUEST, "invalid IPTV lease request");
    }
    if let Some(ha) = &app.iptv_ha {
        for attempt in 0..2 {
            let snapshot = match ha.catalog.load_current().await {
                Ok(snapshot) => snapshot,
                Err(error) => return ha_lease_error(error),
            };
            let (facts, req) =
                match viewer_request_from_snapshot(&snapshot, &body, SystemTime::now()) {
                    Ok(request) => request,
                    Err((status, message)) => return error(status, message),
                };
            let grant = match ha.ledger.acquire(&req, None).await {
                Ok(grant) => grant,
                Err(HaError::StaleRevision) if attempt == 0 => continue,
                Err(error) => return ha_lease_error(error),
            };
            let response = grant_response(grant, req.subscriber, facts.expires_at);
            if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                let _ = ha.ledger.rollback(&req, grant.id).await;
            }
            return response;
        }
        return ha_lease_error(HaError::StaleRevision);
    }
    let wall = SystemTime::now();
    let (facts, req) = match viewer_request(&app, &body, wall) {
        Ok(request) => request,
        Err((status, message)) => return error(status, message),
    };
    let now = Instant::now();
    let grant = match app
        .iptv
        .at_current(facts.revision, || app.iptv_leases.acquire(now, wall, &req))
    {
        Some(Ok(grant)) => grant,
        Some(Err(kind)) => return lease_error(kind),
        None => return lease_error(LeaseError::Stale),
    };
    let response = grant_response(grant, req.subscriber, facts.expires_at);
    if response.status() == StatusCode::SERVICE_UNAVAILABLE {
        app.iptv_leases.rollback(&req, grant.id);
    }
    response
}

pub(super) async fn commit(State(app): State<Arc<App>>, request: Request) -> Response {
    if app.iptv_ha.is_some() {
        return ha_no_store(
            tokio::time::timeout(READ_TIMEOUT, grant_action(app, request, true))
                .await
                .unwrap_or_else(|_| ha_lease_error(HaError::Unavailable)),
        );
    }
    grant_action(app, request, true).await
}

pub(super) async fn renew(State(app): State<Arc<App>>, request: Request) -> Response {
    if app.iptv_ha.is_some() {
        return ha_no_store(
            tokio::time::timeout(READ_TIMEOUT, grant_action(app, request, false))
                .await
                .unwrap_or_else(|_| ha_lease_error(HaError::Unavailable)),
        );
    }
    grant_action(app, request, false).await
}

async fn grant_action(app: Arc<App>, request: Request, commit: bool) -> Response {
    let (body, _slot): (AcquireBody, _) = match peer_body(&app, request).await {
        Ok(body) => body,
        Err((status, message)) => return error(status, message),
    };
    let Some(id) = body
        .lease_id
        .as_deref()
        .and_then(|id| Uuid::parse_str(id).ok())
    else {
        return error(StatusCode::BAD_REQUEST, "invalid IPTV lease request");
    };
    if let Some(ha) = &app.iptv_ha {
        for attempt in 0..2 {
            let snapshot = match ha.catalog.load_current().await {
                Ok(snapshot) => snapshot,
                Err(error) => return ha_lease_error(error),
            };
            let (facts, req) =
                match viewer_request_from_snapshot(&snapshot, &body, SystemTime::now()) {
                    Ok(request) => request,
                    Err((status, message)) => return error(status, message),
                };
            let result = if commit {
                ha.ledger.commit(&req, Some(id)).await
            } else {
                ha.ledger.renew(&req, Some(id)).await
            };
            match result {
                Ok(grant) => return grant_response(grant, req.subscriber, facts.expires_at),
                Err(HaError::StaleRevision) if attempt == 0 => continue,
                Err(error) => return ha_lease_error(error),
            }
        }
        return ha_lease_error(HaError::StaleRevision);
    }
    let wall = SystemTime::now();
    let (facts, req) = match viewer_request(&app, &body, wall) {
        Ok(request) => request,
        Err((status, message)) => return error(status, message),
    };
    let now = Instant::now();
    let grant = match app.iptv.at_current(facts.revision, || {
        if commit {
            app.iptv_leases.commit(now, wall, &req, id)
        } else {
            app.iptv_leases.renew(now, wall, &req, id)
        }
    }) {
        Some(Ok(grant)) => grant,
        Some(Err(kind)) => return lease_error(kind),
        None => return lease_error(LeaseError::Stale),
    };
    grant_response(grant, req.subscriber, facts.expires_at)
}

pub(super) async fn release(State(app): State<Arc<App>>, request: Request) -> Response {
    if app.iptv_ha.is_some() {
        return ha_no_store(
            tokio::time::timeout(READ_TIMEOUT, finish_action(app, request, false))
                .await
                .unwrap_or_else(|_| ha_lease_error(HaError::Unavailable)),
        );
    }
    finish_action(app, request, false).await
}

pub(super) async fn rollback(State(app): State<Arc<App>>, request: Request) -> Response {
    if app.iptv_ha.is_some() {
        return ha_no_store(
            tokio::time::timeout(READ_TIMEOUT, finish_action(app, request, true))
                .await
                .unwrap_or_else(|_| ha_lease_error(HaError::Unavailable)),
        );
    }
    finish_action(app, request, true).await
}

async fn finish_action(app: Arc<App>, request: Request, rollback: bool) -> Response {
    let (body, _slot): (ReleaseBody, _) = match peer_body(&app, request).await {
        Ok(body) => body,
        Err((status, message)) => return error(status, message),
    };
    let (Some(subscriber), Ok(boot), Ok(local), Ok(id), Ok(operation)) = (
        SubscriberId::parse_hex(&body.subscriber_id),
        Uuid::parse_str(&body.boot),
        Uuid::parse_str(&body.session),
        Uuid::parse_str(&body.lease_id),
        Uuid::parse_str(&body.operation),
    ) else {
        return error(StatusCode::BAD_REQUEST, "invalid IPTV lease request");
    };
    let session = SessionKey { boot, local };
    if let Some(ha) = &app.iptv_ha {
        let result = if rollback {
            let req = LeaseRequest {
                subscriber,
                session,
                operation,
                units: 1,
                limit: None,
                revision: 0,
                subscriber_expires_at: None,
            };
            ha.ledger.rollback(&req, id).await
        } else {
            ha.ledger.release(subscriber, session, id, operation).await
        };
        return match result {
            Ok(true) => StatusCode::NO_CONTENT.into_response(),
            Ok(false) => error(StatusCode::NOT_FOUND, "IPTV lease not found"),
            Err(error) => ha_lease_error(error),
        };
    }
    let success = if rollback {
        let req = LeaseRequest {
            subscriber,
            session,
            operation,
            units: 1,
            limit: None,
            revision: 0,
            subscriber_expires_at: None,
        };
        app.iptv_leases.rollback(&req, id)
    } else {
        app.iptv_leases.release(&subscriber, &session, id)
    };
    if success {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::NOT_FOUND, "IPTV lease not found")
    }
}
