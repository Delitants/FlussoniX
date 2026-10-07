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
    created: Instant,
    last_seen: Instant,
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
    bytes: AtomicU64,
}
impl Entry {
    fn occupied(&self) -> bool {
        let s = self.state.lock().unwrap();
        self.live.load(Ordering::Relaxed) > 0
            || (matches!(s.decision, Decision::Allow)
                && s.last_seen.elapsed() < Duration::from_secs(30))
    }
}
pub struct Grant {
    entry: Option<Arc<Entry>>,
    cancel: CancellationToken,
}
impl Grant {
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
            e.state.lock().unwrap().last_seen = Instant::now();
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
                    || s.last_seen.elapsed() < Duration::from_secs(30)
                    || (!matches!(s.decision, Decision::Allow) && s.next_check > Instant::now())
            });
            if !all.contains_key(&identity) && all.len() >= 20_000 {
                return AuthOutcome::Denied;
            }
            all.entry(identity)
                .or_insert_with(|| {
                    Arc::new(Entry {
                        id: uuid::Uuid::new_v4().to_string(),
                        state: Mutex::new(State {
                            policy: policy.clone(),
                            request: request.clone(),
                            decision: Decision::Unknown,
                            next_check: Instant::now(),
                            created: Instant::now(),
                            last_seen: Instant::now(),
                            number: 0,
                            generation: 0,
                            user_id: format!("{:x}", Sha256::digest(request.token.as_bytes())),
                            available: true,
                            revoked_until: None,
                            cancel: CancellationToken::new(),
                        }),
                        flight: AsyncMutex::new(()),
                        live: AtomicU64::new(0),
                        bytes: AtomicU64::new(0),
                    })
                })
                .clone()
        };
        {
            let mut s = entry.state.lock().unwrap();
            if s.revoked_until.is_some_and(|until| until > Instant::now()) {
                return AuthOutcome::Denied;
            }
            s.request = request;
            s.last_seen = Instant::now();
        }
        self.refresh(&entry).await;
        let authority = self.authority.lock().unwrap();
        if !authority
            .get(&snapshot.name)
            .is_some_and(|p| p.revision == snapshot.revision && p.policy.as_ref() == Some(&policy))
        {
            return AuthOutcome::Denied;
        }
        let s = entry.state.lock().unwrap();
        match &s.decision {
            Decision::Allow => {
                entry.live.fetch_add(1, Ordering::Relaxed);
                AuthOutcome::Allowed(Grant {
                    entry: Some(entry.clone()),
                    cancel: s.cancel.clone(),
                })
            }
            Decision::Redirect(url) => AuthOutcome::Redirect(url.clone()),
            _ => AuthOutcome::Denied,
        }
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
        if matches!(decision, Decision::Allow) {
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
        if matches!(decision, Decision::Allow) && unique {
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
        s.decision = decision;
        s.next_check = Instant::now() + Duration::from_secs(seconds);
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
            e.state.lock().unwrap().last_seen = Instant::now() - Duration::from_secs(seconds);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
