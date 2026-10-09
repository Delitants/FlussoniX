//! Viewer authorization is independent of worker ownership and cluster peer credentials.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
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
}
struct Entry {
    id: String,
    state: Mutex<State>,
    flight: AsyncMutex<()>,
    live: AtomicU64,
    admissions: AtomicU64,
    bytes: AtomicU64,
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
pub struct Grant {
    entry: Option<Arc<Entry>>,
    cancel: CancellationToken,
}
impl Grant {
    /// Promote a control admission only once it will acquire playback media.
    pub(crate) fn playback(&self) {
        if let Some(e) = &self.entry {
            let mut s = e.state.lock().unwrap();
            let now = Instant::now();
            s.playback_seen = Some(now);
            s.last_seen = now;
        }
    }
    pub fn peer() -> Self {
        Self {
            entry: None,
            cancel: CancellationToken::new(),
        }
    }
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub fn add_bytes(&self, bytes: usize) {
        if let Some(e) = &self.entry {
            e.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            let mut s = e.state.lock().unwrap();
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
    authority: Mutex<HashMap<String, Authority>>,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    client: reqwest::Client,
    callbacks: Semaphore,
    limit: usize,
}
impl PlaybackAuth {
    pub fn new(limit: usize) -> Self {
        Self {
            authority: Mutex::new(HashMap::new()),
            entries: Mutex::new(HashMap::new()),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("HTTP client"),
            callbacks: Semaphore::new(128),
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
                s.cancel.cancel();
                s.generation += 1;
                s.playback_seen = None;
                s.allowed_until = Instant::now();
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
        self.authorize_inner(snapshot, request, true).await
    }
    pub(crate) async fn authorize_control(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
    ) -> AuthOutcome {
        self.authorize_inner(snapshot, request, false).await
    }
    async fn authorize_inner(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
        playback: bool,
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
            all.retain(|_, e| {
                let s = e.state.lock().unwrap();
                e.live.load(Ordering::Relaxed) > 0
                    || e.admissions.load(Ordering::Relaxed) > 0
                    || s.last_seen.elapsed() < Duration::from_secs(30)
                    || (!matches!(s.decision, Decision::Allow) && s.next_check > Instant::now())
            });
            if !all.contains_key(&identity) && all.len() >= 20_000 {
                return AuthOutcome::Denied;
            }
            let entry = all
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
                        }),
                        flight: AsyncMutex::new(()),
                        live: AtomicU64::new(0),
                        admissions: AtomicU64::new(0),
                        bytes: AtomicU64::new(0),
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
            let all = self.entries.lock().unwrap();
            let mut s = entry.state.lock().unwrap();
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
                    if !already && slots >= self.limit
                        || !s.unique && s.effective_max.is_some_and(|n| user_slots as u64 >= n)
                    {
                        // Capacity is transient; do not poison the cached policy.
                        AuthOutcome::Denied
                    } else {
                        if s.unique {
                            for e in others {
                                let mut other = e.state.lock().unwrap();
                                if other.user_id == s.user_id {
                                    other.cancel.cancel();
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
                        AuthOutcome::Allowed(Grant {
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
        let mut decision = Decision::Allow;
        let mut seconds = 180;
        let mut user_id = None;
        let mut max = policy.max_sessions;
        let mut unique = false;
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
            let Ok(response) = self.client.get(url).query(&fields).send().await else {
                self.retry(entry, generation);
                return;
            };
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
                    other.cancel.cancel();
                    other.decision = Decision::Deny;
                    other.next_check = Instant::now() + Duration::from_secs(seconds);
                    other.generation += 1;
                }
            }
        }
        if !matches!(decision, Decision::Allow) {
            s.cancel.cancel();
        } else if s.cancel.is_cancelled() {
            s.cancel = CancellationToken::new();
        }
        s.user_id = current_user;
        s.effective_max = max;
        s.unique = unique;
        s.decision = decision;
        s.next_check = Instant::now() + Duration::from_secs(seconds);
        if matches!(s.decision, Decision::Allow) {
            s.allowed_until = s.next_check;
        }
    }
    fn retry(&self, entry: &Entry, generation: u64) {
        let mut s = entry.state.lock().unwrap();
        if s.generation == generation {
            s.next_check = Instant::now() + Duration::from_secs(10);
        }
    }
    /// Only sessions with ongoing activity are renewed. Work is bounded and oldest deadlines run first.
    pub async fn renew_due(&self) {
        use futures_util::{StreamExt, stream};
        let mut due = {
            let mut all = self.entries.lock().unwrap();
            all.retain(|_, e| {
                let s = e.state.lock().unwrap();
                e.live.load(Ordering::Relaxed) > 0
                    || e.admissions.load(Ordering::Relaxed) > 0
                    || s.last_seen.elapsed() < Duration::from_secs(30)
                    || (!matches!(s.decision, Decision::Allow) && s.next_check > Instant::now())
            });
            all.values()
                .filter_map(|e| {
                    let s = e.state.lock().unwrap();
                    ((e.live.load(Ordering::Relaxed) > 0
                        || s.last_seen.elapsed() < Duration::from_secs(30))
                        && s.available
                        && s.next_check <= Instant::now()
                        && matches!(s.decision, Decision::Allow | Decision::Unknown))
                    .then_some((s.next_check, e.clone()))
                })
                .collect::<Vec<_>>()
        };
        due.sort_by_key(|(time, _)| *time);
        stream::iter(
            due.into_iter()
                .take(128)
                .map(|(_, e)| async move { self.refresh(&e).await }),
        )
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
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
        s.cancel.cancel();
        s.decision = Decision::Deny;
        s.next_check = Instant::now() + Duration::from_secs(180);
        s.revoked_until = Some(s.next_check);
        s.generation += 1;
        true
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
    pub(crate) fn recovery_demand(&self) -> HashMap<String, Instant> {
        let authority = self.authority.lock().unwrap();
        let entries = self.entries.lock().unwrap();
        let now = Instant::now();
        let mut demand = HashMap::new();
        for entry in entries.values() {
            let state = entry.state.lock().unwrap();
            let Some(seen) = state.playback_seen else {
                continue;
            };
            if now.duration_since(seen) >= Duration::from_secs(30)
                || !matches!(state.decision, Decision::Allow)
                || !state.available
                || state.cancel.is_cancelled()
                || state.next_check <= now
                || state.allowed_until <= now
                || state.revoked_until.is_some_and(|until| until > now)
                || !state.policy.accepts_token(&state.request.token)
                || !authority
                    .get(&state.request.name)
                    .is_some_and(|current| current.policy.as_ref() == Some(&state.policy))
            {
                continue;
            }
            demand
                .entry(state.request.name.clone())
                .and_modify(|latest: &mut Instant| *latest = (*latest).max(seen))
                .or_insert(seen);
        }
        demand
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
mod tests {
    use super::*;
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
        let activity = auth.recovery_demand()["owned"];
        assert_eq!(
            auth.recovery_demand()["owned"],
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

    struct ControlBackend {
        url: String,
        calls: Arc<AtomicU64>,
        stop: CancellationToken,
        task: tokio_util::task::AbortOnDropHandle<()>,
    }
    impl ControlBackend {
        async fn new(unique: bool) -> Self {
            Self::with_duration(unique, 3600).await
        }
        async fn with_duration(unique: bool, duration: u64) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/auth", listener.local_addr().unwrap());
            let calls = Arc::new(AtomicU64::new(0));
            let count = calls.clone();
            let router = axum::Router::new().route(
                "/auth",
                axum::routing::get(
                    move |axum::extract::Query(q): axum::extract::Query<
                        HashMap<String, String>,
                    >| {
                        let count = count.clone();
                        async move {
                            count.fetch_add(1, Ordering::Relaxed);
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
                            (axum::http::StatusCode::OK, headers)
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
}
