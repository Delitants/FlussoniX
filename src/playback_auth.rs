//! Viewer authorization is independent of worker ownership and cluster peer credentials.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub url: Option<String>,
    keys: Vec<String>,
    max_sessions: Option<u64>,
    token_hash: Option<String>,
}
impl Policy {
    pub fn from_config(cfg: &Value, root: &Value) -> Result<Self, String> {
        let mut policy = Self {
            url: None,
            keys: ["name", "proto", "ip", "token"].map(str::to_owned).to_vec(),
            max_sessions: None,
            token_hash: cfg
                .get("flussonix_token_sha256")
                .map(|value| {
                    let hash = value
                        .as_str()
                        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
                        .ok_or("flussonix_token_sha256 must be a 64-digit SHA256 hex string")?;
                    Ok::<String, String>(hash.to_ascii_lowercase())
                })
                .transpose()?,
        };
        if let Some(value) = cfg.get("on_play") {
            let raw = if let Some(url) = value.as_str() {
                url
            } else {
                let obj = value.as_object().ok_or("on_play must be a URL or object")?;
                if obj
                    .keys()
                    .any(|k| !["url", "session_keys", "max_sessions"].contains(&k.as_str()))
                {
                    return Err("unsupported on_play field".into());
                }
                if let Some(keys) = obj.get("session_keys") {
                    policy.keys = keys
                        .as_array()
                        .ok_or("session_keys must be an array")?
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .filter(|k| ["name", "proto", "ip", "token"].contains(k))
                                .map(str::to_owned)
                                .ok_or("unsupported session key")
                        })
                        .collect::<Result<_, _>>()?;
                    if !policy.keys.iter().any(|k| k == "name")
                        || !policy.keys.iter().any(|k| k == "proto")
                        || policy.keys.len() > 32
                    {
                        return Err(
                            "session_keys must include name and proto (at most 32 keys)".into()
                        );
                    }
                }
                if let Some(max) = obj.get("max_sessions") {
                    policy.max_sessions = Some(
                        max.as_u64()
                            .filter(|v| *v > 0)
                            .ok_or("max_sessions must be positive")?,
                    );
                }
                obj.get("url")
                    .and_then(Value::as_str)
                    .ok_or("on_play.url required")?
            };
            let url = if let Some(name) = raw.strip_prefix("auth://") {
                crate::config::valid_name(name)?;
                root["auth_backends"]
                    .as_array()
                    .and_then(|a| a.iter().find(|b| b["name"] == name))
                    .and_then(|b| b["url"].as_str())
                    .ok_or("auth backend not found")?
            } else {
                raw
            };
            validate_http(url)?;
            policy.url = Some(url.to_owned());
        }
        Ok(policy)
    }
    fn accepts_token(&self, token: &str) -> bool {
        self.token_hash.as_ref().is_none_or(|hash| {
            format!("{:x}", Sha256::digest(token.as_bytes())).eq_ignore_ascii_case(hash)
        })
    }
    fn identity(&self, r: &ViewerRequest) -> String {
        // Length framing preserves ordered keys, including repeated keys, without delimiter collisions.
        let mut h = Sha256::new();
        for key in &self.keys {
            let value = match key.as_str() {
                "name" => &r.name,
                "proto" => &r.proto,
                "ip" => &r.ip,
                "token" => &r.token,
                _ => unreachable!(),
            };
            h.update((key.len() as u64).to_be_bytes());
            h.update(key.as_bytes());
            h.update((value.len() as u64).to_be_bytes());
            h.update(value.as_bytes());
        }
        format!("{:x}", h.finalize())
    }
}
fn validate_http(url: &str) -> Result<(), String> {
    let u = url::Url::parse(url).map_err(|_| "invalid HTTP(S) URL")?;
    if !["http", "https"].contains(&u.scheme()) || u.host_str().is_none() {
        return Err("URL must use HTTP(S)".into());
    }
    Ok(())
}
#[derive(Clone)]
pub struct PolicySnapshot {
    name: String,
    revision: u64,
    policy: Policy,
}
struct Authority {
    revision: u64,
    policy: Option<Policy>,
}
#[derive(Clone, Default)]
pub struct ViewerRequest {
    pub name: String,
    pub proto: String,
    pub ip: String,
    pub token: String,
    pub qs: String,
    pub user_agent: String,
    pub referer: String,
    pub host: String,
}
#[derive(Clone)]
enum Decision {
    Unknown,
    Allow,
    Deny,
    Redirect(String),
}
struct State {
    policy: Policy,
    request: ViewerRequest,
    decision: Decision,
    next_check: Instant,
    // Retry scheduling must not extend the permission used for autonomous restart.
    allowed_until: Instant,
    // Explicit operator reauthorization fences recovery; a routine early
    // callback renewal does not retire permission before its hard deadline.
    recovery_fenced: bool,
    created: Instant,
    last_seen: Instant,
    playback_seen: Option<Instant>,
    effective_max: Option<u64>,
    unique: bool,
    number: u64,
    generation: u64,
    user_id: String,
    available: bool,
    revoked_until: Option<Instant>,
    cancel: CancellationToken,
    expiry_task: Option<tokio::task::AbortHandle>,
}
impl State {
    fn cancel_playback(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.expiry_task.take() {
            task.abort();
        }
        self.playback_seen = None;
        self.allowed_until = Instant::now();
    }
    fn recovery_activity(&self, current: Option<&Policy>, now: Instant) -> Option<Instant> {
        let seen = self.playback_seen?;
        (now.duration_since(seen) < Duration::from_secs(30)
            && matches!(self.decision, Decision::Allow)
            && self.available
            && !self.cancel.is_cancelled()
            && ((self.policy.url.is_some() && !self.recovery_fenced) || self.next_check > now)
            && self.allowed_until > now
            && !self.revoked_until.is_some_and(|until| until > now)
            && current == Some(&self.policy)
            && self.policy.accepts_token(&self.request.token))
        .then_some(seen)
    }
}
impl Drop for State {
    fn drop(&mut self) {
        if let Some(task) = self.expiry_task.take() {
            task.abort();
        }
    }
}
struct Entry {
    id: String,
    state: Mutex<State>,
    flight: AsyncMutex<()>,
    live: AtomicU64,
    admissions: AtomicU64,
    bytes: AtomicU64,
    background_pending: AtomicBool,
}
struct BackgroundRenewal(Arc<Entry>);
impl Drop for BackgroundRenewal {
    fn drop(&mut self) {
        self.0.background_pending.store(false, Ordering::Release);
    }
}
/// Per-pass stream/activity only, never cached permission or retained session ownership.
pub(crate) struct RecoveryDemand {
    pub(crate) activity: Instant,
    name: String,
}
/// The primary session cache: grouping avoids an additional index or recovery ledger.
#[derive(Default)]
struct Entries {
    reserved: HashMap<String, ReservedSlot>,
    by_stream: HashMap<String, HashMap<String, Arc<Entry>>>,
}
struct ReservedSlot {
    expires: Instant,
    entry: Option<Weak<Entry>>,
}
impl ReservedSlot {
    fn pending(&self) -> bool {
        self.entry
            .as_ref()
            .and_then(Weak::upgrade)
            .is_none_or(|e| !e.occupied())
    }
}
impl Entries {
    fn values(&self) -> impl Iterator<Item = &Arc<Entry>> {
        self.by_stream
            .values()
            .flat_map(|sessions| sessions.values())
    }
    fn iter(&self) -> impl Iterator<Item = (&String, &Arc<Entry>)> {
        self.by_stream.values().flat_map(|sessions| sessions.iter())
    }
    fn len(&self) -> usize {
        self.by_stream.values().map(HashMap::len).sum()
    }
    fn contains_key(&self, name: &str, identity: &str) -> bool {
        self.by_stream
            .get(name)
            .is_some_and(|sessions| sessions.contains_key(identity))
    }
    fn retain(&mut self, mut keep: impl FnMut(&String, &Arc<Entry>) -> bool) {
        self.by_stream.retain(|_, sessions| {
            sessions.retain(|identity, entry| keep(identity, entry));
            !sessions.is_empty()
        });
    }
    /// Called under the cache lock, which also serializes new Arc/Weak owners.
    fn prune(&mut self, now: Instant, capacity_pressure: bool) {
        self.retain(|_, entry| {
            // Renewal callbacks carry Arc ownership without an admission count.
            if Arc::strong_count(entry) > 1
                || entry.live.load(Ordering::Relaxed) > 0
                || entry.admissions.load(Ordering::Relaxed) > 0
            {
                return true;
            }
            let state = entry.state.lock().unwrap();
            if state.revoked_until.is_some_and(|until| until > now)
                || state
                    .playback_seen
                    .is_some_and(|seen| now.duration_since(seen) < Duration::from_secs(30))
            {
                return true;
            }
            if capacity_pressure {
                // Ordinary failed decisions may be checked early, but allowed
                // sessions keep their normal reconnect/renewal semantics.
                matches!(state.decision, Decision::Allow)
            } else {
                now.duration_since(state.last_seen) < Duration::from_secs(30)
                    || (!matches!(state.decision, Decision::Allow) && state.next_check > now)
            }
        });
    }
}
impl Entry {
    fn occupied(&self) -> bool {
        let s = self.state.lock().unwrap();
        self.live.load(Ordering::Relaxed) > 0
            || (matches!(s.decision, Decision::Allow)
                && s.playback_seen
                    .is_some_and(|seen| seen.elapsed() < Duration::from_secs(30)))
    }
}
// Keep in-flight policy requests in the cache; only admitted grants own capacity.
struct PendingAuthorization(Arc<Entry>);
impl Drop for PendingAuthorization {
    fn drop(&mut self) {
        self.0.admissions.fetch_sub(1, Ordering::Relaxed);
    }
}
/// A short-lived global slot promised to one native HTTP admission ticket.
pub(crate) struct CapacityReservation {
    entries: Arc<Mutex<Entries>>,
    id: String,
}
impl Drop for CapacityReservation {
    fn drop(&mut self) {
        self.entries.lock().unwrap().reserved.remove(&self.id);
    }
}
pub struct Grant {
    capacity_reservation: Option<Arc<CapacityReservation>>,
    output_credit: Option<Arc<crate::admission_credit::OutputCredit>>,
    entry: Option<Arc<Entry>>,
    cancel: CancellationToken,
}
impl Grant {
    pub(crate) fn release_capacity_reservation(&mut self) {
        self.capacity_reservation = None;
    }
    pub(crate) fn set_output_credit(
        &mut self,
        credit: Option<Arc<crate::admission_credit::OutputCredit>>,
    ) {
        self.output_credit = credit;
    }
    pub(crate) fn take_output_credit(
        &mut self,
    ) -> Option<Arc<crate::admission_credit::OutputCredit>> {
        self.output_credit.take()
    }
    /// Read-only recovery permission from this original native viewer grant.
    pub(crate) fn recovery_activity(&self) -> Option<Instant> {
        if self.cancel.is_cancelled() {
            return None;
        }
        let entry = self.entry.as_ref()?;
        let state = entry.state.lock().ok()?;
        state.recovery_activity(Some(&state.policy), Instant::now())
    }
    pub(crate) fn session_identity(&self) -> Option<&str> {
        self.entry.as_ref().map(|entry| entry.id.as_str())
    }
    /// Promote a control admission only once it will acquire playback media.
    pub(crate) fn playback(&self) {
        if let Some(e) = &self.entry {
            let mut s = e.state.lock().unwrap();
            if self.cancel.is_cancelled() {
                return;
            }
            let now = Instant::now();
            s.playback_seen = Some(now);
            s.last_seen = now;
        }
    }
    pub fn peer() -> Self {
        Self {
            entry: None,
            output_credit: None,
            capacity_reservation: None,
            cancel: CancellationToken::new(),
        }
    }
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub fn add_bytes(&self, bytes: usize) {
        if bytes > 0
            && !self.cancel.is_cancelled()
            && let Some(credit) = &self.output_credit
        {
            credit.record(bytes);
        }
        if let Some(e) = &self.entry {
            let mut s = e.state.lock().unwrap();
            if self.cancel.is_cancelled() {
                return;
            }
            e.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            let now = Instant::now();
            s.last_seen = now;
            if s.playback_seen.is_some() {
                s.playback_seen = Some(now);
            }
        }
    }
}
impl Drop for Grant {
    fn drop(&mut self) {
        if let Some(e) = &self.entry {
            // A reservation represented by this live grant must return to the
            // pending count atomically with releasing its live occupancy.
            let _ledger = self
                .capacity_reservation
                .as_ref()
                .map(|r| r.entries.lock().unwrap());
            e.live.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
pub enum AuthOutcome {
    Allowed(Grant),
    Denied,
    Redirect(String),
}
pub struct PlaybackAuth {
    idle_memory: crate::idle_memory::Controller,
    authority: Mutex<HashMap<String, Authority>>,
    entries: Arc<Mutex<Entries>>,
    client: reqwest::Client,
    callbacks: Semaphore,
    background_jobs: Arc<Semaphore>,
    limit: usize,
}
impl PlaybackAuth {
    pub fn new(limit: usize) -> Self {
        Self {
            idle_memory: crate::idle_memory::Controller::default(),
            authority: Mutex::new(HashMap::new()),
            entries: Arc::new(Mutex::new(Entries::default())),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("HTTP client"),
            callbacks: Semaphore::new(128),
            background_jobs: Arc::new(Semaphore::new(64)),
            limit,
        }
    }
    /// Only the configuration/discovery owner publishes policies. Requests consume versioned snapshots.
    pub fn publish(&self, name: &str, policy: Option<Policy>) -> Option<PolicySnapshot> {
        let mut authority = self.authority.lock().unwrap();
        if !authority.contains_key(name) && (policy.is_none() || authority.len() >= 20_000) {
            return None;
        }
        let current = authority.entry(name.into()).or_insert(Authority {
            revision: 0,
            policy: None,
        });
        if current.policy != policy {
            current.revision += 1;
            current.policy = policy.clone();
            for (identity, e) in self.entries.lock().unwrap().iter() {
                let mut s = e.state.lock().unwrap();
                if s.request.name != name {
                    continue;
                }
                s.cancel_playback();
                s.generation += 1;
                s.available = policy
                    .as_ref()
                    .is_some_and(|p| p.identity(&s.request) == *identity);
                if let Some(policy) = &policy {
                    s.policy = policy.clone();
                }
                if s.revoked_until.is_some_and(|until| until > Instant::now()) {
                    s.decision = Decision::Deny;
                    s.next_check = s.revoked_until.unwrap();
                } else {
                    s.decision = Decision::Unknown;
                    s.next_check = Instant::now();
                    s.cancel = CancellationToken::new();
                }
            }
        }
        current.policy.clone().map(|policy| PolicySnapshot {
            name: name.into(),
            revision: current.revision,
            policy,
        })
    }
    pub async fn authorize(&self, snapshot: PolicySnapshot, request: ViewerRequest) -> AuthOutcome {
        self.authorize_inner(snapshot, request, true, None).await
    }
    pub(crate) async fn authorize_control(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
    ) -> AuthOutcome {
        self.authorize_inner(snapshot, request, false, None).await
    }
    pub(crate) fn reserve_capacity(&self, additional: usize) -> Option<Arc<CapacityReservation>> {
        let mut entries = self.entries.lock().unwrap();
        entries
            .reserved
            .retain(|_, slot| slot.expires > Instant::now());
        if entries
            .values()
            .filter(|e| e.occupied())
            .count()
            .saturating_add(
                entries
                    .reserved
                    .values()
                    .filter(|slot| slot.pending())
                    .count(),
            )
            .saturating_add(additional)
            >= self.limit
        {
            return None;
        }
        let id = uuid::Uuid::new_v4().to_string();
        entries.reserved.insert(
            id.clone(),
            ReservedSlot {
                expires: Instant::now() + Duration::from_secs(5),
                entry: None,
            },
        );
        Some(Arc::new(CapacityReservation {
            entries: self.entries.clone(),
            id,
        }))
    }
    pub(crate) async fn authorize_reserved(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
        reservation: Arc<CapacityReservation>,
    ) -> AuthOutcome {
        self.authorize_inner(snapshot, request, false, Some(reservation))
            .await
    }
    async fn authorize_inner(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
        playback: bool,
        reservation: Option<Arc<CapacityReservation>>,
    ) -> AuthOutcome {
        if snapshot.name != request.name {
            return AuthOutcome::Denied;
        }
        let policy = snapshot.policy;
        // Session keys may intentionally omit token. Builtin credentials are
        // still per request: a cached decision must not bypass or be poisoned
        // by a different request's invalid token.
        if !policy.accepts_token(&request.token) {
            return AuthOutcome::Denied;
        }
        let identity = policy.identity(&request);
        let entry = {
            let authority = self.authority.lock().unwrap();
            if !authority.get(&request.name).is_some_and(|p| {
                p.revision == snapshot.revision && p.policy.as_ref() == Some(&policy)
            }) {
                return AuthOutcome::Denied;
            }
            let mut all = self.entries.lock().unwrap();
            all.prune(Instant::now(), false);
            if !all.contains_key(&request.name, &identity) && all.len() >= 20_000 {
                all.prune(Instant::now(), true);
                if all.len() >= 20_000 {
                    return AuthOutcome::Denied;
                }
            }
            let entry = all
                .by_stream
                .entry(request.name.clone())
                .or_default()
                .entry(identity)
                .or_insert_with(|| {
                    Arc::new(Entry {
                        id: uuid::Uuid::new_v4().to_string(),
                        state: Mutex::new(State {
                            policy: policy.clone(),
                            request: request.clone(),
                            decision: Decision::Unknown,
                            next_check: Instant::now(),
                            allowed_until: Instant::now(),
                            recovery_fenced: false,
                            created: Instant::now(),
                            last_seen: Instant::now(),
                            playback_seen: None,
                            effective_max: None,
                            unique: false,
                            number: 0,
                            generation: 0,
                            user_id: format!("{:x}", Sha256::digest(request.token.as_bytes())),
                            available: true,
                            revoked_until: None,
                            cancel: CancellationToken::new(),
                            expiry_task: None,
                        }),
                        flight: AsyncMutex::new(()),
                        live: AtomicU64::new(0),
                        admissions: AtomicU64::new(0),
                        bytes: AtomicU64::new(0),
                        background_pending: AtomicBool::new(false),
                    })
                })
                .clone();
            entry.admissions.fetch_add(1, Ordering::Relaxed);
            entry
        };
        let _authorization = PendingAuthorization(entry.clone());
        {
            let mut s = entry.state.lock().unwrap();
            if s.revoked_until.is_some_and(|until| until > Instant::now()) {
                return AuthOutcome::Denied;
            }
            s.request = request;
            s.last_seen = Instant::now();
        }
        self.refresh(&entry).await;
        let outcome = {
            let authority = self.authority.lock().unwrap();
            if !authority.get(&snapshot.name).is_some_and(|p| {
                p.revision == snapshot.revision && p.policy.as_ref() == Some(&policy)
            }) {
                return AuthOutcome::Denied;
            }
            // Cached policy allows still need current capacity admission. Create
            // the live grant under the same lock as the global/user limit checks.
            let mut all = self.entries.lock().unwrap();
            all.reserved.retain(|_, slot| slot.expires > Instant::now());
            if reservation.as_ref().is_some_and(|r| {
                !Arc::ptr_eq(&r.entries, &self.entries) || !all.reserved.contains_key(&r.id)
            }) {
                return AuthOutcome::Denied;
            }
            let own_pending = reservation
                .as_ref()
                .is_some_and(|r| all.reserved[&r.id].pending());
            let pending = all
                .reserved
                .values()
                .filter(|slot| slot.pending())
                .count()
                .saturating_sub(usize::from(own_pending));
            let mut s = entry.state.lock().unwrap();
            if matches!(s.decision, Decision::Allow)
                && s.policy.url.is_some()
                && s.allowed_until <= Instant::now()
            {
                s.cancel_playback();
                s.decision = Decision::Unknown;
            }
            match &s.decision {
                Decision::Allow => {
                    let others = all
                        .values()
                        .filter(|e| !Arc::ptr_eq(e, &entry))
                        .collect::<Vec<_>>();
                    let already = entry.live.load(Ordering::Relaxed) > 0
                        || s.playback_seen
                            .is_some_and(|seen| seen.elapsed() < Duration::from_secs(30));
                    let slots = others.iter().filter(|e| e.occupied()).count();
                    let user_slots = others
                        .iter()
                        .filter(|e| e.occupied() && e.state.lock().unwrap().user_id == s.user_id)
                        .count();
                    if !already && slots.saturating_add(pending) >= self.limit
                        || !s.unique && s.effective_max.is_some_and(|n| user_slots as u64 >= n)
                    {
                        // Capacity is transient; do not poison the cached policy.
                        AuthOutcome::Denied
                    } else {
                        if s.unique {
                            for e in others {
                                let mut other = e.state.lock().unwrap();
                                if other.user_id == s.user_id {
                                    other.cancel_playback();
                                    other.decision = Decision::Deny;
                                    other.next_check = s.next_check;
                                    other.generation += 1;
                                }
                            }
                        }
                        if playback {
                            s.playback_seen = Some(Instant::now());
                        }
                        entry.live.fetch_add(1, Ordering::Relaxed);
                        if let Some(r) = &reservation {
                            all.reserved.get_mut(&r.id).unwrap().entry =
                                Some(Arc::downgrade(&entry));
                        }
                        AuthOutcome::Allowed(Grant {
                            capacity_reservation: reservation.clone(),
                            output_credit: None,
                            entry: Some(entry.clone()),
                            cancel: s.cancel.clone(),
                        })
                    }
                }
                Decision::Redirect(url) => AuthOutcome::Redirect(url.clone()),
                _ => AuthOutcome::Denied,
            }
        };
        // Model preemption/cancellation before the caller receives the outcome.
        // Its already-constructed grant owns capacity through this boundary.
        #[cfg(test)]
        tokio::task::yield_now().await;
        outcome
    }
    async fn refresh(&self, entry: &Arc<Entry>) {
        let _flight = entry.flight.lock().await;
        let (policy, r, number, generation, duration) = {
            let s = entry.state.lock().unwrap();
            if !s.available
                || s.revoked_until.is_some_and(|until| until > Instant::now())
                || s.next_check > Instant::now()
            {
                return;
            }
            (
                s.policy.clone(),
                s.request.clone(),
                s.number,
                s.generation,
                s.created.elapsed().as_secs(),
            )
        };
        // Background renewal can outlive a media worker. Only real callback
        // work holds this fence; an empty periodic sweep stays passive.
        let _activity = if policy.url.is_some() {
            Some(self.idle_memory.activity().await)
        } else {
            None
        };
        let mut decision = Decision::Allow;
        let mut seconds = 180;
        let mut user_id = None;
        let mut max = policy.max_sessions;
        let mut unique = false;
        let mut callback_latency = Duration::ZERO;
        if !policy.accepts_token(&r.token) {
            decision = Decision::Deny;
        } else if let Some(url) = &policy.url {
            let Ok(_permit) = self.callbacks.try_acquire() else {
                self.retry(entry, generation);
                return;
            };
            let (stream_clients, total_clients) = {
                let all = self.entries.lock().unwrap();
                (
                    all.values()
                        .filter(|e| e.occupied() && e.state.lock().unwrap().request.name == r.name)
                        .count(),
                    all.values().filter(|e| e.occupied()).count(),
                )
            };
            let fields = vec![
                ("name", r.name.clone()),
                ("proto", r.proto.clone()),
                ("ip", r.ip.clone()),
                ("token", r.token.clone()),
                (
                    "request_type",
                    if number == 0 {
                        "new_session"
                    } else {
                        "update_session"
                    }
                    .into(),
                ),
                ("request_number", number.to_string()),
                ("session_id", entry.id.clone()),
                ("stream_clients", stream_clients.to_string()),
                ("total_clients", total_clients.to_string()),
                ("duration", duration.to_string()),
                ("bytes", entry.bytes.load(Ordering::Relaxed).to_string()),
                ("qs", r.qs.clone()),
                ("user_agent", r.user_agent.clone()),
                ("referer", r.referer.clone()),
                ("host", r.host.clone()),
            ];
            {
                let mut s = entry.state.lock().unwrap();
                if s.generation != generation {
                    return;
                }
                s.number += 1;
            }
            let started = Instant::now();
            let Ok(response) = self.client.get(url).query(&fields).send().await else {
                self.retry(entry, generation);
                return;
            };
            callback_latency = started.elapsed();
            let headers = response.headers();
            let header = |key: &str| headers.get(key).and_then(|v| v.to_str().ok());
            seconds = header("x-authduration")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(180)
                .clamp(1, 3600);
            user_id = header("x-userid").map(str::to_owned);
            if let Some(value) = header("x-max-sessions")
                .or_else(|| header("x-maxsessions"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                max = Some(max.map_or(value, |v| v.min(value)));
            }
            unique = header("x-unique").is_some_and(|v| v.eq_ignore_ascii_case("true"));
            if response.status().is_server_error() {
                self.retry(entry, generation);
                return;
            }
            decision = if response.status().is_success() {
                Decision::Allow
            } else if response.status() == 302 {
                match header("location").filter(|url| {
                    if r.proto == "rtsp" {
                        crate::rtsp::redirect::destination(url).is_some()
                    } else {
                        validate_http(url).is_ok()
                    }
                }) {
                    Some(url) => Decision::Redirect(url.into()),
                    None => Decision::Deny,
                }
            } else {
                Decision::Deny
            };
        }
        // Serializing admission and user-limit decisions prevents simultaneous callbacks from oversubscribing.
        let all = self.entries.lock().unwrap();
        let current_user = user_id
            .clone()
            .unwrap_or_else(|| entry.state.lock().unwrap().user_id.clone());
        let others = all
            .values()
            .filter(|e| !Arc::ptr_eq(e, entry))
            .collect::<Vec<_>>();
        let held = entry.occupied();
        if matches!(decision, Decision::Allow) && held {
            let already = entry.occupied();
            let slots = others.iter().filter(|e| e.occupied()).count();
            let user_slots = others
                .iter()
                .filter(|e| e.occupied() && e.state.lock().unwrap().user_id == current_user)
                .count();
            if !already && slots >= self.limit
                || !unique && max.is_some_and(|n| user_slots as u64 >= n)
            {
                decision = Decision::Deny;
            }
        }
        let mut s = entry.state.lock().unwrap();
        if s.generation != generation {
            return;
        }
        if matches!(decision, Decision::Allow) && unique && held {
            for e in others {
                let mut other = e.state.lock().unwrap();
                if other.user_id == current_user {
                    other.cancel_playback();
                    other.decision = Decision::Deny;
                    other.next_check = Instant::now() + Duration::from_secs(seconds);
                    other.generation += 1;
                }
            }
        }
        if !matches!(decision, Decision::Allow) {
            s.cancel_playback();
        } else if s.cancel.is_cancelled() {
            s.cancel = CancellationToken::new();
        }
        s.user_id = current_user;
        s.effective_max = max;
        s.unique = unique;
        s.decision = decision;
        s.recovery_fenced = false;
        let now = Instant::now();
        let lifetime = Duration::from_secs(seconds);
        let expires = now + lifetime;
        s.next_check = expires;
        if let Some(task) = s.expiry_task.take() {
            task.abort();
        }
        if matches!(s.decision, Decision::Allow) {
            s.allowed_until = expires;
        }
        if matches!(s.decision, Decision::Allow) && s.policy.url.is_some() {
            // First renewal is deliberately early so an unmeasured backend
            // has room to respond. Thereafter, measured callback latency plus
            // polling margin avoids repeatedly querying fast backends.
            let lead = if number == 0 {
                lifetime - Duration::from_millis(100)
            } else {
                callback_latency + Duration::from_millis(300)
            }
            .min(Duration::from_secs(4))
            .min(lifetime - Duration::from_millis(100));
            s.next_check = expires - lead;
            let entry = Arc::downgrade(entry);
            let task = tokio::spawn(async move {
                tokio::time::sleep_until(expires.into()).await;
                if let Some(entry) = entry.upgrade() {
                    let mut state = entry.state.lock().unwrap();
                    if matches!(state.decision, Decision::Allow)
                        && state.allowed_until <= Instant::now()
                    {
                        state.cancel_playback();
                        state.decision = Decision::Unknown;
                    }
                }
            });
            s.expiry_task = Some(task.abort_handle());
        }
    }
    fn retry(&self, entry: &Entry, generation: u64) {
        let mut s = entry.state.lock().unwrap();
        if s.generation == generation {
            s.next_check = Instant::now() + Duration::from_secs(10);
        }
    }
    fn due_entries(&self) -> Vec<(bool, Instant, Arc<Entry>)> {
        let mut due = {
            let mut all = self.entries.lock().unwrap();
            all.prune(Instant::now(), false);
            all.values()
                .filter_map(|e| {
                    let s = e.state.lock().unwrap();
                    ((e.live.load(Ordering::Relaxed) > 0
                        || s.last_seen.elapsed() < Duration::from_secs(30))
                        && s.available
                        && s.next_check <= Instant::now()
                        && matches!(s.decision, Decision::Allow | Decision::Unknown))
                    .then_some((
                        !s.recovery_fenced,
                        if s.policy.url.is_some() && matches!(s.decision, Decision::Allow) {
                            s.allowed_until
                        } else {
                            s.next_check
                        },
                        e.clone(),
                    ))
                })
                .collect::<Vec<_>>()
        };
        due.sort_by_key(|(ordinary, time, _)| (*ordinary, *time));
        due
    }
    /// Production polling never waits for a remote callback. One job per
    /// session and a separate 64-job budget bound work during backend stalls.
    pub fn schedule_due(self: &Arc<Self>) {
        for (_, _, entry) in self.due_entries() {
            if entry.background_pending.load(Ordering::Acquire) {
                continue;
            }
            let Ok(permit) = self.background_jobs.clone().try_acquire_owned() else {
                break;
            };
            if entry
                .background_pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let auth = self.clone();
            tokio::spawn(async move {
                let _pending = BackgroundRenewal(entry.clone());
                let _permit = permit;
                auth.refresh(&entry).await;
            });
        }
    }
    /// Deterministic, awaited sweep for tests and explicit maintenance calls.
    pub async fn renew_due(&self) {
        use futures_util::{StreamExt, stream};
        stream::iter(
            self.due_entries()
                .into_iter()
                .take(128)
                .map(|(_, _, e)| async move { self.refresh(&e).await }),
        )
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    }
    /// Only the owner of an unconsumed activation route may call this. No request
    /// snapshot has escaped that route; any authorization entry prevents removal.
    pub(crate) fn forget_unobserved_activation(&self, name: &str) -> bool {
        let mut authority = self.authority.lock().unwrap();
        let entries = self.entries.lock().unwrap();
        if entries.by_stream.contains_key(name) {
            return false;
        }
        authority.remove(name);
        true
    }
    pub fn invalidate(&self, policy_for: impl Fn(&str) -> Option<Policy>) {
        let names = self
            .authority
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in names {
            self.publish(&name, policy_for(&name));
        }
    }
    pub fn revoke(&self, id: &str) -> bool {
        let all = self.entries.lock().unwrap();
        let Some(e) = all.values().find(|e| e.id == id) else {
            return false;
        };
        let mut s = e.state.lock().unwrap();
        s.cancel_playback();
        s.decision = Decision::Deny;
        s.next_check = Instant::now() + Duration::from_secs(180);
        s.revoked_until = Some(s.next_check);
        s.generation += 1;
        true
    }
    /// An operator stop retires activity and in-flight admissions, not the policy.
    pub(crate) fn stop_playback(&self, name: &str) {
        let mut authority = self.authority.lock().unwrap();
        if let Some(current) = authority.get_mut(name) {
            current.revision += 1;
        }
        for entry in self.entries.lock().unwrap().values() {
            let mut state = entry.state.lock().unwrap();
            if state.request.name == name {
                let keep_builtin_allow = state.policy.url.is_none();
                let permission_deadline = state.allowed_until;
                state.cancel_playback();
                if keep_builtin_allow {
                    state.allowed_until = permission_deadline;
                } else {
                    // The old callback timer was retired with the old grant.
                    // A new viewer must obtain a fresh callback and timer.
                    state.decision = Decision::Unknown;
                    state.next_check = Instant::now();
                }
                state.cancel = CancellationToken::new();
                state.generation += 1;
            }
        }
    }
    pub fn force_reauth(&self, name: &str) -> usize {
        let all = self.entries.lock().unwrap();
        let mut count = 0;
        for e in all.values().filter(|e| e.occupied()) {
            let mut s = e.state.lock().unwrap();
            if s.request.name == name
                && s.available
                && matches!(s.decision, Decision::Allow)
                && !s.revoked_until.is_some_and(|until| until > Instant::now())
            {
                s.next_check = Instant::now();
                s.recovery_fenced = true;
                s.generation += 1;
                count += 1;
            }
        }
        count
    }
    pub fn snapshot(&self, id: &str) -> Option<Value> {
        self.entries
            .lock()
            .unwrap()
            .values()
            .find(|e| e.id == id)
            .map(|e| Self::describe(e))
    }
    fn describe(e: &Entry) -> Value {
        let s = e.state.lock().unwrap();
        json!({"id":e.id,"name":s.request.name,"proto":s.request.proto,"ip":s.request.ip,"user_id":s.user_id,"duration":s.created.elapsed().as_secs(),"bytes":e.bytes.load(Ordering::Relaxed),"is_open":matches!(s.decision, Decision::Allow)})
    }
    pub fn active(&self) -> u64 {
        self.entries
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.occupied())
            .count() as u64
    }
    /// Live authorization holders, excluding cached decisions kept for reconnects.
    pub fn live_grants(&self) -> u64 {
        self.entries
            .lock()
            .unwrap()
            .values()
            .map(|e| e.live.load(Ordering::Relaxed))
            .sum()
    }
    /// Recent admitted playback only. Observing recovery demand never renews activity.
    pub(crate) fn recovery_demand(&self) -> HashMap<String, RecoveryDemand> {
        let authority = self.authority.lock().unwrap();
        let entries = self.entries.lock().unwrap();
        let mut demand = HashMap::new();
        for entry in entries.values() {
            let state = entry.state.lock().unwrap();
            let current = authority
                .get(&state.request.name)
                .and_then(|current| current.policy.as_ref());
            let Some(seen) = state.recovery_activity(current, Instant::now()) else {
                continue;
            };
            let candidate =
                demand
                    .entry(state.request.name.clone())
                    .or_insert_with(|| RecoveryDemand {
                        activity: seen,
                        name: state.request.name.clone(),
                    });
            candidate.activity = candidate.activity.max(seen);
        }
        demand
    }
    /// Check the candidate's current sessions, preserving authority -> entries -> state order.
    pub(crate) fn recovery_allowed(&self, name: &str, demand: &RecoveryDemand) -> bool {
        if demand.name != name {
            return false;
        }
        let authority = self.authority.lock().unwrap();
        let current = authority
            .get(name)
            .and_then(|current| current.policy.as_ref());
        if current.is_none() {
            return false;
        }
        let entries = self.entries.lock().unwrap();
        entries.by_stream.get(name).is_some_and(|sessions| {
            sessions.values().any(|entry| {
                let state = entry.state.lock().unwrap();
                state.request.name == name
                    && state.recovery_activity(current, Instant::now()).is_some()
            })
        })
    }

    pub fn snapshots(&self) -> Vec<Value> {
        self.entries
            .lock()
            .unwrap()
            .values()
            .filter(|e| e.occupied())
            .map(|e| Self::describe(e))
            .collect()
    }
    #[cfg(test)]
    pub fn age_activity(&self, seconds: u64) {
        for e in self.entries.lock().unwrap().values() {
            let mut s = e.state.lock().unwrap();
            let aged = Instant::now() - Duration::from_secs(seconds);
            s.last_seen = aged;
            if s.playback_seen.is_some() {
                s.playback_seen = Some(aged);
            }
        }
    }
}

#[cfg(test)]
mod idle_memory_tests;

#[cfg(test)]
mod capacity_tests;

#[cfg(test)]
mod tests {
    use super::*;

    async fn candidate_fixture() -> (Arc<PlaybackAuth>, RecoveryDemand, Arc<Entry>, Arc<Entry>) {
        let auth = Arc::new(PlaybackAuth::new(128));
        let policy = Policy::from_config(
            &json!({"flussonix_token_sha256":format!("{:x}", Sha256::digest(b"owned"))}),
            &json!({}),
        )
        .unwrap();
        for name in ["owned", "unrelated"] {
            let snapshot = auth.publish(name, Some(policy.clone())).unwrap();
            let AuthOutcome::Allowed(grant) = auth
                .authorize(
                    snapshot,
                    ViewerRequest {
                        name: name.into(),
                        proto: "hls".into(),
                        token: "owned".into(),
                        ..Default::default()
                    },
                )
                .await
            else {
                panic!("owned candidate admission")
            };
            drop(grant);
        }
        let find = |name| {
            auth.entries
                .lock()
                .unwrap()
                .values()
                .find(|entry| entry.state.lock().unwrap().request.name == name)
                .unwrap()
                .clone()
        };
        let candidate = find("owned");
        let unrelated = find("unrelated");
        let demand = auth.recovery_demand().remove("owned").unwrap();
        (auth, demand, candidate, unrelated)
    }

    // A whole-cache scan makes this check wait for another stream's byte accounting.
    #[tokio::test]
    async fn candidate_recovery_does_not_wait_for_unrelated_session_state() {
        let (auth, demand, _, unrelated) = candidate_fixture().await;
        let blocked = unrelated.state.lock().unwrap();
        let (entered, waiting) = std::sync::mpsc::channel();
        let (finished, result) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            entered.send(()).unwrap();
            finished
                .send(auth.recovery_allowed("owned", &demand))
                .unwrap();
        });
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        let before_release = result.recv_timeout(Duration::from_secs(1));
        drop(blocked);
        thread.join().unwrap();
        assert_eq!(
            before_release.ok(),
            Some(true),
            "candidate recovery waited for an unrelated session lock"
        );
    }

    // Capturing a candidate must never freeze its permission or renew its activity.
    #[tokio::test]
    async fn candidate_recovery_rechecks_current_authorization_after_capture() {
        for (boundary, expected) in [
            ("valid", true),
            ("wrong-name", false),
            ("expired-playback", false),
            ("missing-playback", false),
            ("callback-due", false),
            ("expired-allow", false),
            ("denied", false),
            ("unknown", false),
            ("redirect", false),
            ("unavailable", false),
            ("canceled", false),
            ("revoked", false),
            ("invalid-token", false),
            ("policy-changed", false),
            ("authority-removed", false),
            ("operator-stop", false),
            ("unrelated-policy-changed", true),
        ] {
            let (auth, demand, entry, _) = candidate_fixture().await;
            let now = Instant::now();
            match boundary {
                "policy-changed" | "unrelated-policy-changed" => {
                    let name = if boundary == "policy-changed" {
                        "owned"
                    } else {
                        "unrelated"
                    };
                    auth.publish(
                        name,
                        Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
                    );
                }
                "authority-removed" => {
                    auth.publish("owned", None);
                }
                "operator-stop" => auth.stop_playback("owned"),
                "revoked" => {
                    assert!(auth.revoke(&entry.id));
                }
                _ => {
                    let mut state = entry.state.lock().unwrap();
                    match boundary {
                        "valid" | "wrong-name" => {}
                        "expired-playback" => {
                            state.playback_seen = Some(now - Duration::from_secs(30))
                        }
                        "missing-playback" => state.playback_seen = None,
                        "callback-due" => state.next_check = now,
                        "expired-allow" => state.allowed_until = now,
                        "denied" => state.decision = Decision::Deny,
                        "unknown" => state.decision = Decision::Unknown,
                        "redirect" => {
                            state.decision = Decision::Redirect("http://localhost/".into())
                        }
                        "unavailable" => state.available = false,
                        "canceled" => state.cancel.cancel(),
                        "invalid-token" => state.request.token = "invalid".into(),
                        _ => unreachable!(),
                    }
                }
            }
            let before = entry.state.lock().unwrap().playback_seen;
            let name = if boundary == "wrong-name" {
                "unrelated"
            } else {
                "owned"
            };
            assert_eq!(auth.recovery_allowed(name, &demand), expected, "{boundary}");
            assert_eq!(
                entry.state.lock().unwrap().playback_seen,
                before,
                "observation changed activity: {boundary}"
            );
        }
    }

    #[tokio::test]
    async fn candidate_recovery_rejects_retired_and_replaced_cache_entries() {
        let (auth, demand, entry, _) = candidate_fixture().await;
        let policy = entry.state.lock().unwrap().policy.clone();
        let request = entry.state.lock().unwrap().request.clone();
        auth.entries
            .lock()
            .unwrap()
            .retain(|_, cached| !Arc::ptr_eq(cached, &entry));
        assert!(
            !auth.recovery_allowed("owned", &demand),
            "retired entry authorized recovery"
        );
        let snapshot = auth.publish("owned", Some(policy)).unwrap();
        let AuthOutcome::Allowed(control) = auth.authorize_control(snapshot, request).await else {
            panic!("replacement control admission")
        };
        assert!(
            !auth.recovery_allowed("owned", &demand),
            "replaced identity reused a retired entry's demand"
        );
        assert!(
            !auth.recovery_demand().contains_key("owned"),
            "control-only replacement became demand"
        );
        drop(control);
    }

    #[tokio::test]
    async fn candidate_recovery_accepts_new_and_existing_current_sessions() {
        let (auth, old_demand, first, _) = candidate_fixture().await;
        let policy = first.state.lock().unwrap().policy.clone();
        let mut request = first.state.lock().unwrap().request.clone();
        request.ip = "another-viewer".into();
        let snapshot = auth.publish("owned", Some(policy)).unwrap();
        let AuthOutcome::Allowed(second) = auth.authorize(snapshot, request).await else {
            panic!("second playback")
        };
        let current_demand = auth.recovery_demand().remove("owned").unwrap();
        assert!(auth.revoke(&first.id));
        assert!(
            auth.recovery_allowed("owned", &old_demand),
            "new valid foreground demand must keep its shared recovery worker"
        );
        assert!(
            auth.recovery_allowed("owned", &current_demand),
            "another captured session is still authorized"
        );
        assert!(auth.revoke(&second.entry.as_ref().unwrap().id));
        assert!(!auth.recovery_allowed("owned", &current_demand));
    }

    #[tokio::test]
    async fn recovery_demand_requires_recent_playback_and_current_authorization() {
        let auth = PlaybackAuth::new(8);
        let policy = Policy::from_config(&json!({}), &json!({})).unwrap();
        let snapshot = auth.publish("owned", Some(policy.clone())).unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(control) = auth
            .authorize_control(snapshot.clone(), request.clone())
            .await
        else {
            panic!("control admission")
        };
        assert!(
            auth.recovery_demand().is_empty(),
            "control-only grant is not playback demand"
        );
        drop(control);
        let AuthOutcome::Allowed(playback) =
            auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("playback admission")
        };
        drop(playback);
        let activity = auth.recovery_demand()["owned"].activity;
        assert_eq!(
            auth.recovery_demand()["owned"].activity,
            activity,
            "observation must not touch demand"
        );
        auth.age_activity(31);
        assert!(
            auth.recovery_demand().is_empty(),
            "expired demand cannot resume"
        );
        let AuthOutcome::Allowed(playback) =
            auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("new playback")
        };
        drop(playback);
        assert!(!auth.recovery_demand().is_empty());
        auth.force_reauth("owned");
        assert!(
            auth.recovery_demand().is_empty(),
            "overdue authorization must be renewed first"
        );
        auth.renew_due().await;
        assert!(!auth.recovery_demand().is_empty());
        let id = auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
        assert!(auth.revoke(&id));
        assert!(
            auth.recovery_demand().is_empty(),
            "revoked demand cannot resume"
        );
        auth.publish("owned", None);
        assert!(auth.recovery_demand().is_empty());
    }

    #[tokio::test]
    async fn changed_policy_and_invalid_token_cannot_retain_recovery_demand() {
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            token: "before".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(playback) = auth.authorize(snapshot, request).await else {
            panic!("initial playback")
        };
        drop(playback);
        assert!(!auth.recovery_demand().is_empty());
        let changed = Policy::from_config(
            &json!({"flussonix_token_sha256":format!("{:x}", Sha256::digest(b"after"))}),
            &json!({}),
        )
        .unwrap();
        auth.publish("owned", Some(changed));
        assert!(auth.recovery_demand().is_empty());
        auth.renew_due().await;
        assert!(auth.recovery_demand().is_empty());
    }

    #[tokio::test]
    async fn policy_renewal_after_change_cannot_reuse_previous_playback_demand() {
        let backend = ControlBackend::new(false).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(snapshot, request).await else {
            panic!("initial playback")
        };
        drop(grant);
        let changed = Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap();
        auth.publish("owned", Some(changed));
        auth.renew_due().await;
        let demand = auth.recovery_demand();
        backend.close().await;
        assert!(
            demand.is_empty(),
            "renewing a changed policy must not restore the prior policy's playback demand"
        );
    }

    #[tokio::test]
    async fn callback_retry_cannot_extend_expired_recovery_authorization() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(grant) = auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("initial authorization")
        };
        drop(grant);
        assert!(!auth.recovery_demand().is_empty());
        backend.close().await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        auth.renew_due().await;
        assert!(
            auth.recovery_demand().is_empty(),
            "callback retry extended an expired allow for recovery"
        );
    }

    #[tokio::test]
    async fn expired_callback_allow_cancels_live_grant_and_denies_reconnect_during_outage() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("initial authorization")
        };
        backend.close().await;
        tokio::time::timeout(Duration::from_secs(2), grant.cancelled())
            .await
            .expect("authorization must expire even when callback renewal fails");
        assert!(grant.is_cancelled());
        assert!(matches!(
            auth.authorize(snapshot, request).await,
            AuthOutcome::Denied
        ));
    }

    #[tokio::test]
    async fn healthy_callback_renewal_keeps_live_playback_open() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(snapshot, request).await else {
            panic!("initial authorization")
        };
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(2300) {
            auth.renew_due().await;
            assert!(
                !grant.is_cancelled(),
                "healthy renewal interrupted playback"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(backend.calls.load(Ordering::Relaxed) >= 3);
        backend.close().await;
    }

    #[tokio::test]
    async fn six_hundred_millisecond_callback_renews_one_second_grant() {
        let backend = ControlBackend::with_duration_and_delay(false, 1, 600).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(grant) = auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("initial authorization")
        };
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(1800) {
            auth.renew_due().await;
            assert!(
                !grant.is_cancelled(),
                "successful short callback missed lease"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(backend.calls.load(Ordering::Relaxed) >= 3);
        backend.close().await;
    }

    #[tokio::test]
    async fn stalled_callback_does_not_block_another_short_lease() {
        let stalled = ControlBackend::with_timing(true, 3600, 0, 2200).await;
        let healthy = ControlBackend::with_duration(true, 1).await;
        let auth = Arc::new(PlaybackAuth::new(8));
        let slow = auth
            .publish(
                "slow",
                Some(Policy::from_config(&json!({"on_play":stalled.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(slow_grant) = auth
            .authorize(
                slow,
                ViewerRequest {
                    name: "slow".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("slow initial authorization")
        };
        assert_eq!(auth.force_reauth("slow"), 1);
        auth.schedule_due();
        tokio::time::timeout(Duration::from_secs(1), async {
            while stalled.calls.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stalled callback did not start");
        let fast = auth
            .publish(
                "fast",
                Some(Policy::from_config(&json!({"on_play":healthy.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(fast_grant) = auth
            .authorize(
                fast,
                ViewerRequest {
                    name: "fast".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("fast initial authorization")
        };
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(1350) {
            auth.schedule_due();
            assert!(
                !fast_grant.is_cancelled(),
                "stalled sibling expired fast viewer"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(healthy.calls.load(Ordering::Relaxed) >= 2);
        assert!(!slow_grant.is_cancelled());
        stalled.close().await;
        healthy.close().await;
    }

    #[tokio::test]
    async fn fast_one_second_callback_renews_without_excessive_requests() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = Arc::new(PlaybackAuth::new(8));
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(grant) = auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("initial authorization")
        };
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(2100) {
            auth.schedule_due();
            assert!(!grant.is_cancelled());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let calls = backend.calls.load(Ordering::Relaxed);
        assert!(calls >= 3, "renewal did not run: {calls}");
        assert!(calls <= 6, "fast callback was overpolled: {calls}");
        backend.close().await;
    }

    #[tokio::test]
    async fn early_callback_renewal_does_not_pause_recovery_demand() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(grant) = auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("initial authorization")
        };
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(
            !auth.recovery_demand().is_empty(),
            "scheduled early callback renewal must not retire valid recovery demand"
        );
        assert!(!grant.is_cancelled());
        auth.force_reauth("owned");
        assert!(auth.recovery_demand().is_empty());
        backend.close().await;
    }

    #[tokio::test]
    async fn operator_stop_requires_new_callback_before_reusing_allow() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = PlaybackAuth::new(8);
        let policy = Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap();
        let snapshot = auth.publish("owned", Some(policy.clone())).unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(old) = auth.authorize(snapshot, request.clone()).await else {
            panic!("initial authorization")
        };
        auth.stop_playback("owned");
        assert!(old.is_cancelled());
        let snapshot = auth.publish("owned", Some(policy)).unwrap();
        let AuthOutcome::Allowed(new) = auth.authorize(snapshot, request).await else {
            panic!("post-stop authorization")
        };
        assert_eq!(backend.calls.load(Ordering::Relaxed), 2);
        backend.close().await;
        tokio::time::timeout(Duration::from_secs(2), new.cancelled())
            .await
            .expect("post-stop callback allow must retain its expiry timer");
    }

    #[tokio::test]
    async fn forced_reauthorization_retires_previous_expiry_tasks() {
        let backend = ControlBackend::new(false).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(grant) = auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("initial authorization")
        };
        let entry = grant.entry.as_ref().unwrap();
        for _ in 0..3 {
            let old = entry
                .state
                .lock()
                .unwrap()
                .expiry_task
                .as_ref()
                .unwrap()
                .clone();
            assert_eq!(auth.force_reauth("owned"), 1);
            auth.renew_due().await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while !old.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("superseded expiry task must be aborted");
            assert!(!grant.is_cancelled());
        }
        backend.close().await;
    }

    #[tokio::test]
    async fn failed_forced_reauthorization_keeps_original_expiry() {
        let backend = ControlBackend::with_duration(false, 1).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(grant) = auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "hls".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("initial authorization")
        };
        assert_eq!(auth.force_reauth("owned"), 1);
        backend.close().await;
        auth.renew_due().await;
        tokio::time::timeout(Duration::from_secs(2), grant.cancelled())
            .await
            .expect("failed forced callback must not extend the original permission");
    }

    #[tokio::test]
    async fn builtin_token_policy_has_no_callback_deadline() {
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("initial authorization")
        };
        {
            let entries = auth.entries.lock().unwrap();
            let entry = entries.values().next().unwrap();
            entry.state.lock().unwrap().allowed_until = Instant::now() - Duration::from_secs(1);
        }
        assert!(!grant.is_cancelled());
        assert!(matches!(
            auth.authorize(snapshot, request).await,
            AuthOutcome::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn stale_control_grant_cannot_repopulate_activity_after_policy_change() {
        let backend = ControlBackend::new(false).await;
        let auth = PlaybackAuth::new(8);
        let old = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(control) = auth.authorize_control(old, request).await else {
            panic!("old control")
        };
        auth.publish(
            "owned",
            Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
        );
        assert!(control.is_cancelled());
        control.playback();
        control.add_bytes(10);
        auth.renew_due().await;
        let demand = auth.recovery_demand();
        drop(control);
        backend.close().await;
        assert!(
            demand.is_empty(),
            "canceled control grant repopulated new-policy demand"
        );
    }

    #[tokio::test]
    async fn stale_bytes_cannot_refresh_activity_from_a_new_grant() {
        let auth = PlaybackAuth::new(8);
        let config = json!({});
        let old = auth
            .publish(
                "owned",
                Some(Policy::from_config(&config, &config).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(stale) = auth.authorize(old, request.clone()).await else {
            panic!("old playback")
        };
        auth.publish("owned", None);
        let current = auth
            .publish(
                "owned",
                Some(Policy::from_config(&config, &config).unwrap()),
            )
            .unwrap();
        let AuthOutcome::Allowed(fresh) = auth.authorize(current, request).await else {
            panic!("new playback")
        };
        drop(fresh);
        auth.age_activity(31);
        assert!(stale.is_cancelled());
        stale.add_bytes(10);
        assert!(
            auth.recovery_demand().is_empty(),
            "canceled byte accounting renewed another grant's activity"
        );
    }

    #[tokio::test]
    async fn denied_playback_cannot_resume_through_control_only_allow() {
        canceled_playback_cannot_resume_through_control_only_allow(false).await;
    }
    #[tokio::test]
    async fn preempted_playback_cannot_resume_through_control_only_allow() {
        canceled_playback_cannot_resume_through_control_only_allow(true).await;
    }
    async fn canceled_playback_cannot_resume_through_control_only_allow(unique: bool) {
        let backend = ControlBackend::with_duration(unique, 1).await;
        let auth = PlaybackAuth::new(8);
        let snapshot = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            token: "b".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(playback) =
            auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("initial playback")
        };
        if unique {
            let other = ViewerRequest {
                token: "a".into(),
                ..request.clone()
            };
            let AuthOutcome::Allowed(preemption) =
                auth.authorize_control(snapshot.clone(), other).await
            else {
                panic!("preemption")
            };
            drop(preemption);
        } else {
            backend.deny.store(true, Ordering::SeqCst);
            auth.force_reauth("owned");
            auth.renew_due().await;
            backend.deny.store(false, Ordering::SeqCst);
        }
        assert!(playback.is_cancelled());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let AuthOutcome::Allowed(control) = auth.authorize_control(snapshot, request).await else {
            panic!("later control")
        };
        let demand = auth.recovery_demand();
        drop(control);
        drop(playback);
        backend.close().await;
        assert!(
            demand.is_empty(),
            "control revived canceled playback; unique={unique}"
        );
    }

    #[tokio::test]
    async fn operator_stop_fences_old_grants_and_snapshots_but_allows_new_playback() {
        let auth = PlaybackAuth::new(8);
        let policy = Policy::from_config(&json!({}), &json!({})).unwrap();
        let snapshot = auth.publish("owned", Some(policy.clone())).unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(old) = auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("initial playback")
        };
        auth.stop_playback("owned");
        assert!(old.is_cancelled());
        old.playback();
        old.add_bytes(10);
        assert!(auth.recovery_demand().is_empty());
        assert!(
            matches!(
                auth.authorize(snapshot, request.clone()).await,
                AuthOutcome::Denied
            ),
            "queued pre-stop admission passed"
        );
        drop(old);
        let snapshot = auth.publish("owned", Some(policy)).unwrap();
        let AuthOutcome::Allowed(control) = auth
            .authorize_control(snapshot.clone(), request.clone())
            .await
        else {
            panic!("post-stop control")
        };
        drop(control);
        assert!(auth.recovery_demand().is_empty());
        let AuthOutcome::Allowed(new) = auth.authorize(snapshot, request).await else {
            panic!("post-stop playback")
        };
        assert!(!new.is_cancelled());
        assert!(!auth.recovery_demand().is_empty());
    }

    struct ControlBackend {
        url: String,
        calls: Arc<AtomicU64>,
        deny: Arc<std::sync::atomic::AtomicBool>,
        stop: CancellationToken,
        task: tokio_util::task::AbortOnDropHandle<()>,
    }
    impl ControlBackend {
        async fn new(unique: bool) -> Self {
            Self::with_duration(unique, 3600).await
        }
        async fn with_duration(unique: bool, duration: u64) -> Self {
            Self::with_duration_and_delay(unique, duration, 0).await
        }
        async fn with_duration_and_delay(unique: bool, duration: u64, delay_ms: u64) -> Self {
            Self::with_timing(unique, duration, delay_ms, 0).await
        }
        async fn with_timing(
            unique: bool,
            duration: u64,
            delay_ms: u64,
            renewal_delay_ms: u64,
        ) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/auth", listener.local_addr().unwrap());
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let deny = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let reject = deny.clone();
            let router = axum::Router::new().route(
                "/auth",
                axum::routing::get(
                    move |axum::extract::Query(q): axum::extract::Query<
                        HashMap<String, String>,
                    >| {
                        let count = count.clone();
                        let reject = reject.clone();
                        async move {
                            count.fetch_add(1, Ordering::Relaxed);
                            let pause = if q
                                .get("request_type")
                                .is_some_and(|value| value == "update_session")
                            {
                                renewal_delay_ms.max(delay_ms)
                            } else {
                                delay_ms
                            };
                            if pause > 0 {
                                tokio::time::sleep(Duration::from_millis(pause)).await;
                            }
                            let mut headers = axum::http::HeaderMap::new();
                            headers.insert("x-authduration", duration.to_string().parse().unwrap());
                            headers.insert("x-userid", "account".parse().unwrap());
                            if unique {
                                if q["token"] == "a" {
                                    headers.insert("x-unique", "true".parse().unwrap());
                                }
                            } else {
                                headers.insert("x-max-sessions", "1".parse().unwrap());
                            }
                            (
                                if reject.load(Ordering::SeqCst) {
                                    axum::http::StatusCode::FORBIDDEN
                                } else {
                                    axum::http::StatusCode::OK
                                },
                                headers,
                            )
                        }
                    },
                ),
            );
            let stop = CancellationToken::new();
            let cancel = stop.clone();
            let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(cancel.cancelled_owned())
                    .await
                    .unwrap()
            }));
            Self {
                url,
                calls,
                deny,
                stop,
                task,
            }
        }
        async fn close(self) {
            self.stop.cancel();
            tokio::time::timeout(Duration::from_secs(2), self.task)
                .await
                .unwrap()
                .unwrap();
        }
    }
    #[tokio::test]
    async fn warmed_control_allow_rechecks_callback_user_limit_without_another_callback() {
        let backend = ControlBackend::new(false).await;
        let auth = PlaybackAuth::new(8);
        let p = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let a = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            token: "a".into(),
            ..Default::default()
        };
        let b = ViewerRequest {
            token: "b".into(),
            ..a.clone()
        };
        let AuthOutcome::Allowed(g) = auth.authorize_control(p.clone(), a.clone()).await else {
            panic!("warm")
        };
        drop(g);
        let AuthOutcome::Allowed(g) = auth.authorize_control(p.clone(), b).await else {
            panic!("held")
        };
        assert!(matches!(
            auth.authorize_control(p.clone(), a.clone()).await,
            AuthOutcome::Denied
        ));
        drop(g);
        assert!(matches!(
            auth.authorize_control(p, a).await,
            AuthOutcome::Allowed(_)
        ));
        assert_eq!(backend.calls.load(Ordering::Relaxed), 2);
        backend.close().await;
    }
    #[tokio::test]
    async fn warmed_control_unique_decision_revokes_another_held_user_grant() {
        let backend = ControlBackend::new(true).await;
        let auth = PlaybackAuth::new(8);
        let p = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({"on_play":backend.url}), &json!({})).unwrap()),
            )
            .unwrap();
        let a = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            token: "a".into(),
            ..Default::default()
        };
        let b = ViewerRequest {
            token: "b".into(),
            ..a.clone()
        };
        let AuthOutcome::Allowed(g) = auth.authorize_control(p.clone(), a.clone()).await else {
            panic!("warm")
        };
        drop(g);
        let AuthOutcome::Allowed(b) = auth.authorize_control(p.clone(), b).await else {
            panic!("held")
        };
        let AuthOutcome::Allowed(a) = auth.authorize_control(p, a).await else {
            panic!("cached unique")
        };
        assert!(b.is_cancelled());
        drop(b);
        assert_eq!(auth.active(), 1);
        drop(a);
        assert_eq!(backend.calls.load(Ordering::Relaxed), 2);
        backend.close().await;
    }
    #[tokio::test]
    async fn warmed_control_allow_rechecks_global_capacity_without_poisoning_cache() {
        let auth = PlaybackAuth::new(1);
        let p = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let a = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            token: "a".into(),
            ..Default::default()
        };
        let b = ViewerRequest {
            token: "b".into(),
            ..a.clone()
        };
        let AuthOutcome::Allowed(g) = auth.authorize_control(p.clone(), a.clone()).await else {
            panic!("warm")
        };
        drop(g);
        let AuthOutcome::Allowed(g) = auth.authorize_control(p.clone(), b).await else {
            panic!("held")
        };
        assert!(matches!(
            auth.authorize_control(p.clone(), a.clone()).await,
            AuthOutcome::Denied
        ));
        assert_eq!(auth.active(), 1);
        drop(g);
        assert!(matches!(
            auth.authorize_control(p, a).await,
            AuthOutcome::Allowed(_)
        ));
    }
    #[tokio::test]
    async fn concurrent_control_admissions_hold_capacity_through_grant_transfer() {
        let auth = PlaybackAuth::new(1);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let results = futures_util::future::join_all((0..16).map(|i| {
            auth.authorize_control(
                policy.clone(),
                ViewerRequest {
                    name: "owned".into(),
                    proto: "rtsp".into(),
                    token: i.to_string(),
                    ..Default::default()
                },
            )
        }))
        .await;
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, AuthOutcome::Allowed(_)))
                .count(),
            1
        );
        assert_eq!(auth.live_grants(), 1);
        drop(results);
        assert_eq!(auth.active(), 0);
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            token: "pending".into(),
            ..Default::default()
        };
        let mut pending = Box::pin(auth.authorize_control(policy.clone(), request.clone()));
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        assert_eq!(auth.active(), 1);
        assert_eq!(
            auth.live_grants(),
            1,
            "outcome owns the admitted grant before handoff"
        );
        auth.age_activity(31);
        auth.renew_due().await;
        assert_eq!(auth.active(), 1, "cleanup must retain pending admission");
        drop(pending);
        assert_eq!(auth.active(), 0, "cancelled transfer releases capacity");
        assert!(matches!(
            auth.authorize_control(
                policy,
                ViewerRequest {
                    token: "after-cancel".into(),
                    ..request
                }
            )
            .await,
            AuthOutcome::Allowed(_)
        ));
    }
    #[tokio::test]
    async fn control_decisions_do_not_linger_in_playback_capacity() {
        let auth = PlaybackAuth::new(1);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "rtsp".into(),
            token: "first".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(control) = auth
            .authorize_control(policy.clone(), request.clone())
            .await
        else {
            panic!("control")
        };
        assert_eq!(auth.active(), 1);
        assert_eq!(auth.live_grants(), 1);
        drop(control);
        assert_eq!(auth.active(), 0);
        assert_eq!(auth.live_grants(), 0);
        let AuthOutcome::Allowed(playback) = auth
            .authorize_control(policy.clone(), request.clone())
            .await
        else {
            panic!("cached control")
        };
        playback.playback();
        drop(playback);
        assert_eq!(auth.active(), 1);
        let other = ViewerRequest {
            token: "other".into(),
            ..request
        };
        assert!(matches!(
            auth.authorize_control(policy.clone(), other.clone()).await,
            AuthOutcome::Denied
        ));
        auth.age_activity(29);
        let AuthOutcome::Allowed(control) = auth
            .authorize_control(
                policy,
                ViewerRequest {
                    token: "first".into(),
                    ..other
                },
            )
            .await
        else {
            panic!("control during playback grace")
        };
        drop(control);
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(
            auth.active(),
            0,
            "control requests must not extend playback grace"
        );
    }
    #[test]
    fn session_key_order_is_distinct_even_when_values_are_equal() {
        let a = Policy::from_config(
            &json!({"on_play":{"url":"https://auth.example/play","session_keys":["name","proto"]}}),
            &json!({}),
        )
        .unwrap();
        let b = Policy::from_config(
            &json!({"on_play":{"url":"https://auth.example/play","session_keys":["proto","name"]}}),
            &json!({}),
        )
        .unwrap();
        let r = ViewerRequest {
            name: "hls".into(),
            proto: "hls".into(),
            ..Default::default()
        };
        assert_ne!(a.identity(&r), b.identity(&r));
    }
    #[tokio::test]
    async fn idle_sessions_expire_but_live_grants_retain_client_slots() {
        let auth = PlaybackAuth::new(1);
        let policy = Policy::from_config(&json!({}), &json!({})).unwrap();
        let policy = auth.publish("owned", Some(policy)).unwrap();
        let request = ViewerRequest {
            name: "owned".into(),
            proto: "mpegts".into(),
            token: "first".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(policy.clone(), request.clone()).await
        else {
            panic!("grant")
        };
        auth.age_activity(31);
        auth.renew_due().await;
        assert_eq!(auth.active(), 1);
        let other = ViewerRequest {
            token: "other".into(),
            ..request
        };
        assert!(matches!(
            auth.authorize(policy, other).await,
            AuthOutcome::Denied
        ));
        drop(grant);
        auth.renew_due().await;
        assert_eq!(auth.active(), 0);
    }
    #[tokio::test]
    async fn promised_slots_transfer_to_control_grants_without_double_counting() {
        let auth = PlaybackAuth::new(2);
        let policy = auth
            .publish(
                "owned",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let a = auth.reserve_capacity(0).unwrap();
        let b = auth.reserve_capacity(0).unwrap();
        let viewer = ViewerRequest {
            name: "owned".into(),
            proto: "mpegts".into(),
            token: "a".into(),
            ..Default::default()
        };
        // authorize yields with its constructed grant still alive, modelling the
        // ticket handler's media/config checks before it removes the reservation.
        let (left, right) = tokio::join!(
            auth.authorize_reserved(policy.clone(), viewer.clone(), a),
            auth.authorize_reserved(
                policy,
                ViewerRequest {
                    token: "b".into(),
                    ..viewer
                },
                b
            )
        );
        assert!(
            matches!(left, AuthOutcome::Allowed(_)),
            "first promise lost capacity"
        );
        assert!(
            matches!(right, AuthOutcome::Allowed(_)),
            "second promise double-counted the first transfer"
        );
        assert_eq!(auth.active(), 2);
    }
}
