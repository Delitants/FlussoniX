//! Viewer authorization is independent of worker ownership and cluster peer credentials.
use crate::iptv::{
    client::{AuthorityClient, LeaseInput, LeaseReply},
    lease::SessionKey,
    store::IptvStore,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub url: Option<String>,
    kind: PolicyKind,
    keys: Vec<String>,
    max_sessions: Option<u64>,
    token_hash: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PolicyKind {
    Open,
    Callback,
    Iptv,
}
impl Policy {
    pub fn from_config(cfg: &Value, root: &Value) -> Result<Self, String> {
        let mut policy = Self {
            url: None,
            kind: PolicyKind::Open,
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
                if obj.get("url").and_then(Value::as_str) == Some("iptv://")
                    && (obj.contains_key("session_keys") || obj.contains_key("max_sessions"))
                {
                    return Err("IPTV on_play does not accept callback options".into());
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
            if raw == "iptv://" {
                policy.kind = PolicyKind::Iptv;
                return Ok(policy);
            }
            if raw.starts_with("iptv://") {
                return Err("IPTV on_play must be exactly iptv://".into());
            }
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
            policy.kind = PolicyKind::Callback;
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
    /// A finite media response sharing the protocol identity of a live signal.
    pub finite_media: bool,
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
    continuous_live: usize,
    control_live: usize,
    segmented_live: usize,
    segmented_control_live: usize,
    handoff_live: usize,
    effective_max: Option<u64>,
    unique: bool,
    number: u64,
    generation: u64,
    user_id: String,
    available: bool,
    revoked_until: Option<Instant>,
    cancel: CancellationToken,
}
impl State {
    /// Slot count after a prospective grant, using the same grouping as
    /// `capacity_units` but without publishing or changing live state.
    fn projected_capacity_units(&self, kind: GrantKind, consumes_handoff: bool) -> usize {
        let active = self
            .control_live
            .saturating_add(self.handoff_live)
            .saturating_add(self.continuous_live)
            .saturating_sub(usize::from(consumes_handoff))
            .saturating_add(usize::from(matches!(
                kind,
                GrantKind::Control | GrantKind::Continuous | GrantKind::Handoff
            )));
        let segmented = self
            .segmented_live
            .saturating_add(self.segmented_control_live)
            .saturating_add(usize::from(matches!(
                kind,
                GrantKind::Segmented | GrantKind::SegmentedControl
            )));
        active.saturating_add(usize::from(active == 0 && segmented > 0))
    }
    fn capacity_units(&self, live: usize, per_connection: bool) -> usize {
        if live == 0 {
            return usize::from(
                matches!(self.decision, Decision::Allow)
                    && self
                        .playback_seen
                        .is_some_and(|seen| seen.elapsed() < Duration::from_secs(30)),
            );
        }
        if !per_connection {
            return 1;
        }
        // Unclaimed handoffs each hold a slot. Finite media shares a matching
        // continuous signal's slot; ordinary controls remain independent.
        let active = self
            .control_live
            .saturating_add(self.handoff_live)
            .saturating_add(self.continuous_live);
        active.saturating_add(usize::from(
            active == 0 && (self.segmented_live > 0 || self.segmented_control_live > 0),
        ))
    }
    fn add_grant(&mut self, kind: GrantKind) {
        match kind {
            GrantKind::Continuous => self.continuous_live += 1,
            GrantKind::Segmented => self.segmented_live += 1,
            GrantKind::Handoff => self.handoff_live += 1,
            GrantKind::Control => self.control_live += 1,
            GrantKind::SegmentedControl => self.segmented_control_live += 1,
            GrantKind::ConsumedHandoff => {}
        }
    }
    fn remove_grant(&mut self, kind: GrantKind) {
        match kind {
            GrantKind::Continuous => self.continuous_live -= 1,
            GrantKind::Segmented => self.segmented_live -= 1,
            GrantKind::Handoff => self.handoff_live -= 1,
            GrantKind::Control => self.control_live -= 1,
            GrantKind::SegmentedControl => self.segmented_control_live -= 1,
            GrantKind::ConsumedHandoff => {}
        }
    }
    fn cancel_playback(&mut self) {
        self.cancel.cancel();
        self.playback_seen = None;
        self.allowed_until = Instant::now();
    }
    fn recovery_activity(&self, current: Option<&Policy>, now: Instant) -> Option<Instant> {
        let seen = self.playback_seen?;
        (now.duration_since(seen) < Duration::from_secs(30)
            && matches!(self.decision, Decision::Allow)
            && self.available
            && !self.cancel.is_cancelled()
            && self.next_check > now
            && self.allowed_until > now
            && !self.revoked_until.is_some_and(|until| until > now)
            && current == Some(&self.policy)
            && self.policy.accepts_token(&self.request.token))
        .then_some(seen)
    }
}
struct Entry {
    id: String,
    state: Mutex<State>,
    flight: AsyncMutex<()>,
    remote: AsyncMutex<RemoteState>,
    remote_notify: Notify,
    remote_epoch: Instant,
    remote_deadline_micros: AtomicU64,
    live: AtomicU64,
    admissions: AtomicU64,
    bytes: AtomicU64,
}
struct RemoteState {
    current: Option<RemoteLease>,
    running: bool,
    session: uuid::Uuid,
}
impl Default for RemoteState {
    fn default() -> Self {
        Self {
            current: None,
            running: false,
            session: uuid::Uuid::new_v4(),
        }
    }
}
struct RemoteLease {
    input: LeaseInput,
    reply: LeaseReply,
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
    fn set_remote_deadline(&self, deadline: Instant) {
        let micros = deadline
            .saturating_duration_since(self.remote_epoch)
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        self.remote_deadline_micros
            .store(micros.max(1), Ordering::Release);
    }
    fn remote_lease_expired(&self) -> bool {
        let deadline = self.remote_deadline_micros.load(Ordering::Acquire);
        deadline != 0 && self.remote_epoch.elapsed().as_micros() >= u128::from(deadline)
    }
    fn occupied_units(&self, node_capacity: bool) -> usize {
        let s = self.state.lock().unwrap();
        let live = usize::try_from(self.live.load(Ordering::Relaxed)).unwrap_or(usize::MAX);
        s.capacity_units(live, node_capacity || s.policy.kind == PolicyKind::Iptv)
    }
    fn occupied(&self) -> bool {
        self.occupied_units(false) > 0
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
    expires_at: Option<SystemTime>,
    expiry_task: Option<tokio::task::JoinHandle<()>>,
    kind: AtomicU8,
    segmented_delivery: bool,
}
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum GrantKind {
    Control,
    Continuous,
    Segmented,
    Handoff,
    SegmentedControl,
    ConsumedHandoff,
}
impl GrantKind {
    fn for_request(playback: bool, segmented: bool) -> Self {
        match (playback, segmented) {
            (false, false) => Self::Control,
            (false, true) => Self::SegmentedControl,
            (true, false) => Self::Continuous,
            (true, true) => Self::Segmented,
        }
    }
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Continuous,
            2 => Self::Segmented,
            3 => Self::Handoff,
            4 => Self::SegmentedControl,
            5 => Self::ConsumedHandoff,
            _ => Self::Control,
        }
    }
}
impl Grant {
    fn expire_at(&mut self, expires_at: SystemTime) {
        self.expires_at = Some(expires_at);
        // A child token keeps this viewer's deadline separate from the
        // entry's shared cancellation state and wakes token-only transports.
        self.cancel = self.cancel.child_token();
        let cancel = self.cancel.clone();
        self.expiry_task = Some(tokio::spawn(async move {
            loop {
                let Ok(remaining) = expires_at.duration_since(SystemTime::now()) else {
                    cancel.cancel();
                    break;
                };
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(remaining.min(Duration::from_secs(60))) => {}
                }
            }
        }));
    }
    pub(crate) fn mark_handoff(&mut self) {
        if let Some(entry) = &self.entry {
            let mut s = entry.state.lock().unwrap();
            let kind = GrantKind::from_u8(self.kind.load(Ordering::Relaxed));
            if matches!(kind, GrantKind::Control | GrantKind::SegmentedControl) {
                s.remove_grant(kind);
                s.add_grant(GrantKind::Handoff);
                self.kind.store(GrantKind::Handoff as u8, Ordering::Relaxed);
            }
        }
    }
    pub(crate) fn restore_handoff(&mut self) -> bool {
        if self.is_cancelled() {
            return false;
        }
        let Some(entry) = &self.entry else {
            return false;
        };
        let mut state = entry.state.lock().unwrap();
        match GrantKind::from_u8(self.kind.load(Ordering::Relaxed)) {
            GrantKind::Handoff => true,
            GrantKind::ConsumedHandoff => {
                state.add_grant(GrantKind::Handoff);
                self.kind.store(GrantKind::Handoff as u8, Ordering::Relaxed);
                true
            }
            _ => false,
        }
    }
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
        if self.is_cancelled() {
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
            if self.is_cancelled() {
                return;
            }
            let previous = GrantKind::from_u8(self.kind.load(Ordering::Relaxed));
            if matches!(previous, GrantKind::Control | GrantKind::SegmentedControl) {
                let kind = if self.segmented_delivery {
                    GrantKind::Segmented
                } else {
                    GrantKind::Continuous
                };
                s.remove_grant(previous);
                s.add_grant(kind);
                self.kind.store(kind as u8, Ordering::Relaxed);
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
            expires_at: None,
            expiry_task: None,
            kind: AtomicU8::new(GrantKind::Control as u8),
            segmented_delivery: false,
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
            || self
                .expires_at
                .is_some_and(|expires_at| SystemTime::now() >= expires_at)
            || self
                .entry
                .as_ref()
                .is_some_and(|entry| entry.remote_lease_expired())
    }
    pub fn add_bytes(&self, bytes: usize) {
        if bytes > 0
            && !self.is_cancelled()
            && let Some(credit) = &self.output_credit
        {
            credit.record(bytes);
        }
        if let Some(e) = &self.entry {
            let mut s = e.state.lock().unwrap();
            if self.is_cancelled() {
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
        if let Some(task) = self.expiry_task.take() {
            task.abort();
        }
        if let Some(e) = &self.entry {
            // A reservation represented by this live grant must return to the
            // pending count atomically with releasing its live occupancy.
            let _ledger = self
                .capacity_reservation
                .as_ref()
                .map(|r| r.entries.lock().unwrap());
            let mut s = e.state.lock().unwrap();
            s.remove_grant(GrantKind::from_u8(self.kind.load(Ordering::Relaxed)));
            e.live.fetch_sub(1, Ordering::Relaxed);
            drop(s);
            e.remote_notify.notify_one();
        }
    }
}
pub enum AuthOutcome {
    Allowed(Grant),
    Denied,
    Redirect(String),
}
pub struct PlaybackAuth {
    iptv: Option<Arc<IptvStore>>,
    remote_iptv: Option<Arc<AuthorityClient>>,
    remote_boot: uuid::Uuid,
    #[cfg(test)]
    iptv_lookup_gate: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    idle_memory: crate::idle_memory::Controller,
    authority: Mutex<HashMap<String, Authority>>,
    entries: Arc<Mutex<Entries>>,
    client: reqwest::Client,
    callbacks: Semaphore,
    limit: usize,
}
impl PlaybackAuth {
    fn revoke_remote_grant(entry: &Entry) {
        let mut state = entry.state.lock().unwrap();
        state.cancel_playback();
        state.decision = Decision::Unknown;
        state.next_check = Instant::now();
        state.generation += 1;
    }
    async fn retire_remote_lease(remote: &mut RemoteState, client: &AuthorityClient) {
        if let Some(lease) = remote.current.take() {
            // Keep the per-entry operation gate held through the release RPC.
            // The next grant gets a new session key so a delayed old release
            // cannot remove a replacement lease after an ambiguous timeout.
            let _ = client.release(&lease.input, &lease.reply).await;
        }
        remote.session = uuid::Uuid::new_v4();
        remote.running = false;
    }
    async fn watch_remote_lease(entry: Arc<Entry>, client: Arc<AuthorityClient>) {
        loop {
            let wake = entry.remote_notify.notified();
            let mut remote = entry.remote.lock().await;
            let Some(current) = remote.current.as_ref() else {
                remote.running = false;
                return;
            };
            let reply = current.reply;
            let (units, cancelled) = {
                let state = entry.state.lock().unwrap();
                let live =
                    usize::try_from(entry.live.load(Ordering::Relaxed)).unwrap_or(usize::MAX);
                (
                    state.capacity_units(live, true),
                    state.cancel.is_cancelled(),
                )
            };
            if cancelled || units == 0 || reply.deadline <= Instant::now() {
                if reply.deadline <= Instant::now() {
                    Self::revoke_remote_grant(&entry);
                }
                Self::retire_remote_lease(&mut remote, &client).await;
                return;
            }
            let input = current.input.clone();
            let mut updated = None;
            if units != reply.units as usize {
                let Ok(units) = u32::try_from(units) else {
                    Self::revoke_remote_grant(&entry);
                    Self::retire_remote_lease(&mut remote, &client).await;
                    return;
                };
                let mut replacement = input.clone();
                replacement.operation = uuid::Uuid::new_v4();
                replacement.units = units;
                replacement.lease_id = None;
                if let Ok(pending) = client.acquire(&replacement).await {
                    let mut with_id = replacement.clone();
                    with_id.lease_id = Some(pending.lease_id);
                    match client.commit(&with_id).await {
                        Ok(committed) => updated = Some((replacement, committed)),
                        Err(_) => {
                            let _ = client.rollback(&replacement, &pending).await;
                        }
                    }
                }
                if updated.is_none() {
                    Self::revoke_remote_grant(&entry);
                    Self::retire_remote_lease(&mut remote, &client).await;
                    return;
                }
            } else if reply.deadline <= Instant::now() + Duration::from_secs(5) {
                let mut renewal = input.clone();
                renewal.lease_id = Some(reply.lease_id);
                match client.renew(&renewal).await {
                    Ok(renewed) => updated = Some((input.clone(), renewed)),
                    Err(_) => {
                        Self::revoke_remote_grant(&entry);
                        Self::retire_remote_lease(&mut remote, &client).await;
                        return;
                    }
                }
            }
            // A response received after this grant's old local deadline must
            // never turn an already-expired media output back on. The newly
            // renewed authority slot is released and the viewer reconnects.
            if reply.deadline <= Instant::now() {
                Self::revoke_remote_grant(&entry);
                if let Some((input, renewed)) = updated {
                    remote.current = Some(RemoteLease {
                        input,
                        reply: renewed,
                    });
                }
                Self::retire_remote_lease(&mut remote, &client).await;
                return;
            }
            if let Some((input, reply)) = updated {
                remote.current = Some(RemoteLease { input, reply });
                entry.set_remote_deadline(reply.deadline);
                let cancelled = {
                    let mut state = entry.state.lock().unwrap();
                    if state.cancel.is_cancelled() {
                        true
                    } else {
                        state.allowed_until = reply.deadline;
                        state.next_check = reply.deadline;
                        false
                    }
                };
                if cancelled {
                    Self::retire_remote_lease(&mut remote, &client).await;
                    return;
                }
            }
            let remaining = remote
                .current
                .as_ref()
                .unwrap()
                .reply
                .deadline
                .saturating_duration_since(Instant::now());
            let sleep = remaining
                .saturating_sub(Duration::from_secs(5))
                .min(Duration::from_secs(4));
            drop(remote);
            tokio::select! {
                _ = wake => {},
                _ = tokio::time::sleep(sleep) => {},
            }
        }
    }
    fn projected_remote_units(
        entry: &Arc<Entry>,
        playback: bool,
        segmented: bool,
        handoff: Option<&Grant>,
    ) -> Option<usize> {
        let state = entry.state.lock().unwrap();
        if !matches!(state.decision, Decision::Allow) || state.cancel.is_cancelled() {
            return None;
        }
        let handoff_valid = handoff.is_some_and(|grant| {
            grant
                .entry
                .as_ref()
                .is_some_and(|owner| Arc::ptr_eq(owner, entry))
                && GrantKind::from_u8(grant.kind.load(Ordering::Relaxed)) == GrantKind::Handoff
                && !grant.is_cancelled()
        });
        if handoff.is_some() && !handoff_valid {
            return None;
        }
        Some(
            state.projected_capacity_units(
                GrantKind::for_request(playback, segmented),
                handoff_valid,
            ),
        )
    }
    pub fn new(limit: usize) -> Self {
        Self::build(limit, None)
    }
    pub fn with_iptv(limit: usize, store: Arc<IptvStore>) -> Self {
        Self::build(limit, Some(store))
    }
    pub fn with_remote_iptv(limit: usize, client: Arc<AuthorityClient>) -> Self {
        let mut auth = Self::build(limit, None);
        auth.remote_iptv = Some(client);
        auth
    }
    fn build(limit: usize, iptv: Option<Arc<IptvStore>>) -> Self {
        Self {
            iptv,
            remote_iptv: None,
            remote_boot: uuid::Uuid::new_v4(),
            #[cfg(test)]
            iptv_lookup_gate: Mutex::new(None),
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
        self.authorize_inner(snapshot, request, true, None, None, None)
            .await
    }
    pub(crate) async fn authorize_handoff(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
        handoff: &Grant,
    ) -> AuthOutcome {
        self.authorize_inner(snapshot, request, true, None, Some(handoff), None)
            .await
    }
    pub(crate) async fn authorize_handoff_reuse(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
        identity: &str,
    ) -> Option<AuthOutcome> {
        let key = snapshot.policy.identity(&request);
        if snapshot.name != request.name
            || !self
                .entries
                .lock()
                .unwrap()
                .by_stream
                .get(&request.name)
                .and_then(|sessions| sessions.get(&key))
                .is_some_and(|entry| entry.id == identity)
        {
            return None;
        }
        Some(
            self.authorize_inner(snapshot, request, true, None, None, Some(identity))
                .await,
        )
    }
    pub(crate) async fn authorize_control(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
    ) -> AuthOutcome {
        self.authorize_inner(snapshot, request, false, None, None, None)
            .await
    }
    pub(crate) fn reserve_capacity(&self, additional: usize) -> Option<Arc<CapacityReservation>> {
        let mut entries = self.entries.lock().unwrap();
        entries
            .reserved
            .retain(|_, slot| slot.expires > Instant::now());
        if entries
            .values()
            .fold(0usize, |sum, e| sum.saturating_add(e.occupied_units(true)))
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
        self.authorize_inner(snapshot, request, false, Some(reservation), None, None)
            .await
    }
    async fn authorize_inner(
        &self,
        snapshot: PolicySnapshot,
        request: ViewerRequest,
        playback: bool,
        reservation: Option<Arc<CapacityReservation>>,
        handoff: Option<&Grant>,
        reuse_identity: Option<&str>,
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
        if policy.kind == PolicyKind::Iptv
            && self.remote_iptv.is_none()
            && !self.iptv.as_ref().is_some_and(|store| {
                store
                    .snapshot()
                    .authorize(&request.token, &request.name, SystemTime::now())
                    .is_some()
            })
        {
            return AuthOutcome::Denied;
        }
        #[cfg(test)]
        if policy.kind == PolicyKind::Iptv {
            let gate = self.iptv_lookup_gate.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                let _ = entered.send(());
                let _ = release.await;
            }
        }
        let iptv_request = (policy.kind == PolicyKind::Iptv).then(|| request.clone());
        let segmented_delivery = request.proto == "hls" || request.finite_media;
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
            if reuse_identity.is_some_and(|expected| {
                all.by_stream
                    .get(&request.name)
                    .and_then(|sessions| sessions.get(&identity))
                    .is_none_or(|entry| entry.id != expected)
            }) {
                return AuthOutcome::Denied;
            }
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
                            created: Instant::now(),
                            last_seen: Instant::now(),
                            playback_seen: None,
                            continuous_live: 0,
                            control_live: 0,
                            segmented_live: 0,
                            segmented_control_live: 0,
                            handoff_live: 0,
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
                        remote: AsyncMutex::new(RemoteState::default()),
                        remote_notify: Notify::new(),
                        remote_epoch: Instant::now(),
                        remote_deadline_micros: AtomicU64::new(0),
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
        let admit = |remote_units: Option<usize>| {
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
            match &s.decision {
                Decision::Allow => {
                    let others = all
                        .values()
                        .filter(|e| !Arc::ptr_eq(e, &entry))
                        .collect::<Vec<_>>();
                    let live =
                        usize::try_from(entry.live.load(Ordering::Relaxed)).unwrap_or(usize::MAX);
                    let kind = GrantKind::for_request(playback, segmented_delivery);
                    let own_slots = s.capacity_units(live, true);
                    let handoff_valid = handoff.is_some_and(|h| {
                        h.entry.as_ref().is_some_and(|e| Arc::ptr_eq(e, &entry))
                            && GrantKind::from_u8(h.kind.load(Ordering::Relaxed))
                                == GrantKind::Handoff
                            && !h.is_cancelled()
                    });
                    let projected = s.projected_capacity_units(kind, handoff_valid);
                    let additional = projected.saturating_sub(own_slots);
                    let slots = others.iter().fold(own_slots, |sum, e| {
                        sum.saturating_add(e.occupied_units(true))
                    });
                    let other_user_slots = others
                        .iter()
                        .filter(|e| e.state.lock().unwrap().user_id == s.user_id)
                        .fold(0usize, |sum, e| sum.saturating_add(e.occupied_units(false)));
                    let exceeds_user_limit = s.effective_max.is_some_and(|max| {
                        if policy.kind == PolicyKind::Iptv {
                            other_user_slots.saturating_add(projected) as u64 > max
                        } else {
                            other_user_slots as u64 >= max
                        }
                    });
                    if handoff.is_some() && !handoff_valid
                        || remote_units.is_some_and(|units| units != projected)
                        || additional > 0
                            && slots.saturating_add(pending).saturating_add(additional) > self.limit
                        || !s.unique && exceeds_user_limit
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
                        if playback && remote_units.is_none() {
                            s.playback_seen = Some(Instant::now());
                        }
                        if handoff_valid {
                            let handoff = handoff.expect("validated handoff");
                            s.remove_grant(GrantKind::Handoff);
                            handoff
                                .kind
                                .store(GrantKind::ConsumedHandoff as u8, Ordering::Relaxed);
                        }
                        s.add_grant(kind);
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
                            expires_at: None,
                            expiry_task: None,
                            kind: AtomicU8::new(kind as u8),
                            segmented_delivery,
                        })
                    }
                }
                Decision::Redirect(url) => AuthOutcome::Redirect(url.clone()),
                _ => AuthOutcome::Denied,
            }
        };
        let outcome = if let Some(iptv_request) = iptv_request {
            if let Some(client) = &self.remote_iptv {
                let mut remote = entry.remote.lock().await;
                let Some(units) =
                    Self::projected_remote_units(&entry, playback, segmented_delivery, handoff)
                else {
                    return AuthOutcome::Denied;
                };
                let Ok(units_u32) = u32::try_from(units) else {
                    return AuthOutcome::Denied;
                };
                let existing = remote.current.as_ref().filter(|lease| {
                    lease.reply.units == units_u32
                        && lease.reply.deadline > Instant::now() + Duration::from_secs(1)
                        && lease.input.token == iptv_request.token
                        && lease.input.channel == iptv_request.name
                });
                let (input, pending, needs_commit) = if let Some(lease) = existing {
                    (lease.input.clone(), lease.reply, false)
                } else {
                    let input = LeaseInput {
                        token: iptv_request.token.clone(),
                        channel: iptv_request.name.clone(),
                        session: SessionKey {
                            boot: self.remote_boot,
                            local: remote.session,
                        },
                        operation: uuid::Uuid::new_v4(),
                        units: units_u32,
                        lease_id: None,
                    };
                    let Ok(pending) = client.acquire(&input).await else {
                        return AuthOutcome::Denied;
                    };
                    (input, pending, true)
                };
                let mut outcome = admit(Some(units));
                if !matches!(outcome, AuthOutcome::Allowed(_)) {
                    if needs_commit {
                        let _ = client.rollback(&input, &pending).await;
                    }
                    return outcome;
                }
                let committed = if needs_commit {
                    let mut with_id = input.clone();
                    with_id.lease_id = Some(pending.lease_id);
                    match client.commit(&with_id).await {
                        Ok(reply) => reply,
                        Err(_) => {
                            drop(outcome);
                            let _ = client.rollback(&input, &pending).await;
                            return AuthOutcome::Denied;
                        }
                    }
                } else {
                    pending
                };
                if needs_commit {
                    remote.current = Some(RemoteLease {
                        input: input.clone(),
                        reply: committed,
                    });
                }
                entry.set_remote_deadline(committed.deadline);
                let cancelled = {
                    let mut state = entry.state.lock().unwrap();
                    if state.cancel.is_cancelled() {
                        true
                    } else {
                        if playback {
                            state.playback_seen = Some(Instant::now());
                        }
                        state.user_id = committed.subscriber.as_hex();
                        state.allowed_until = committed.deadline;
                        state.next_check = committed.deadline;
                        false
                    }
                };
                if cancelled {
                    drop(outcome);
                    Self::retire_remote_lease(&mut remote, client).await;
                    return AuthOutcome::Denied;
                }
                if let (Some(expiry), AuthOutcome::Allowed(grant)) =
                    (committed.subscriber_expires_at, &mut outcome)
                {
                    grant.expire_at(expiry);
                }
                if !remote.running {
                    remote.running = true;
                    let watcher_entry = entry.clone();
                    let watcher_client = client.clone();
                    tokio::spawn(async move {
                        Self::watch_remote_lease(watcher_entry, watcher_client).await;
                    });
                }
                outcome
            } else {
                let Some(store) = self.iptv.as_ref() else {
                    return AuthOutcome::Denied;
                };
                let current = store.snapshot();
                store
                    .at_current(current.revision(), || {
                        match current.authorize(
                            &iptv_request.token,
                            &iptv_request.name,
                            SystemTime::now(),
                        ) {
                            Some(facts) => {
                                let mut outcome = admit(None);
                                if let (Some(expires_at), AuthOutcome::Allowed(grant)) =
                                    (facts.expires_at, &mut outcome)
                                {
                                    grant.expire_at(expires_at);
                                }
                                outcome
                            }
                            None => AuthOutcome::Denied,
                        }
                    })
                    .unwrap_or(AuthOutcome::Denied)
            }
        } else {
            admit(None)
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
        let _activity = if policy.kind == PolicyKind::Callback {
            Some(self.idle_memory.activity().await)
        } else {
            None
        };
        let mut decision = Decision::Allow;
        let mut seconds = 180;
        let mut user_id = None;
        let mut max = policy.max_sessions;
        let mut unique = false;
        let mut expiry_window = None;
        if !policy.accepts_token(&r.token) {
            decision = Decision::Deny;
        } else if policy.kind == PolicyKind::Iptv && self.remote_iptv.is_none() {
            match self.iptv.as_ref().and_then(|store| {
                store
                    .snapshot()
                    .authorize(&r.token, &r.name, SystemTime::now())
            }) {
                Some(facts) => {
                    user_id = Some(format!("{:x}", Sha256::digest(facts.subscriber.as_bytes())));
                    max = facts.max_sessions.map(u64::from);
                    expiry_window = facts
                        .expires_at
                        .and_then(|expiry| expiry.duration_since(SystemTime::now()).ok());
                    if facts.expires_at.is_some() && expiry_window.is_none() {
                        decision = Decision::Deny;
                    }
                }
                None => decision = Decision::Deny,
            }
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
        s.next_check = Instant::now() + Duration::from_secs(seconds);
        if let Some(window) = expiry_window {
            s.next_check = s.next_check.min(Instant::now() + window);
        }
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
            all.prune(Instant::now(), false);
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
    /// Called while IPTV publication holds the store write lock. The order is
    /// store -> authority -> entries -> state, matching final admission.
    pub fn invalidate_iptv(&self) {
        let _authority = self.authority.lock().unwrap();
        let entries = self.entries.lock().unwrap();
        for entry in entries.values() {
            let mut state = entry.state.lock().unwrap();
            if state.policy.kind != PolicyKind::Iptv {
                continue;
            }
            state.cancel_playback();
            state.decision = Decision::Unknown;
            state.next_check = Instant::now();
            state.generation += 1;
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
                let permission_deadline = state.allowed_until;
                state.cancel_playback();
                state.allowed_until = permission_deadline;
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
        self.entries.lock().unwrap().values().fold(0u64, |sum, e| {
            sum.saturating_add(e.occupied_units(true) as u64)
        })
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

    #[tokio::test]
    async fn remote_grant_checks_its_monotonic_deadline_even_if_watcher_is_delayed() {
        let auth = PlaybackAuth::new(2);
        let snapshot = auth
            .publish(
                "news",
                Some(Policy::from_config(&json!({}), &json!({})).unwrap()),
            )
            .unwrap();
        let viewer = ViewerRequest {
            name: "news".into(),
            proto: "mpegts".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(grant) = auth.authorize(snapshot, viewer).await else {
            panic!("grant");
        };
        let entry = grant.entry.as_ref().unwrap();
        entry.remote_deadline_micros.store(1, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(grant.is_cancelled());
    }

    #[tokio::test]
    async fn projected_iptv_units_share_hls_but_count_independent_continuous_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let store = IptvStore::open(dir.path().join("iptv.json")).unwrap();
        store.set_enabled(true).unwrap();
        store
            .put_package("basic", json!({"channels":["news"]}))
            .unwrap();
        store
            .put_subscriber(
                "alice",
                json!({"token":"subscriber","packages":["basic"],"max_sessions":3}),
            )
            .unwrap();
        let auth = PlaybackAuth::with_iptv(8, store);
        let policy = auth
            .publish(
                "news",
                Some(Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap()),
            )
            .unwrap();
        let viewer = ViewerRequest {
            name: "news".into(),
            proto: "hls".into(),
            token: "subscriber".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(segment) = auth.authorize(policy.clone(), viewer.clone()).await
        else {
            panic!("first segment");
        };
        let entry = segment.entry.as_ref().unwrap().clone();
        {
            let state = entry.state.lock().unwrap();
            assert_eq!(
                state.projected_capacity_units(GrantKind::Segmented, false),
                1
            );
        }
        drop(segment);
        {
            let state = entry.state.lock().unwrap();
            assert_eq!(
                state.capacity_units(0, true),
                1,
                "recent HLS group retains a slot"
            );
            assert_eq!(
                state.projected_capacity_units(GrantKind::Segmented, false),
                1
            );
        }
        let ts = ViewerRequest {
            proto: "mpegts".into(),
            ..viewer
        };
        let AuthOutcome::Allowed(first) = auth.authorize(policy.clone(), ts.clone()).await else {
            panic!("first continuous output");
        };
        let continuous = first.entry.as_ref().unwrap();
        assert_eq!(
            continuous
                .state
                .lock()
                .unwrap()
                .projected_capacity_units(GrantKind::Continuous, false),
            2
        );
        let AuthOutcome::Allowed(mut control) = auth.authorize_control(policy, ts).await else {
            panic!("control handoff");
        };
        control.mark_handoff();
        let owner = control.entry.as_ref().unwrap();
        let state = owner.state.lock().unwrap();
        let live = owner.live.load(Ordering::Relaxed) as usize;
        assert_eq!(
            state.projected_capacity_units(GrantKind::Continuous, true),
            state.capacity_units(live, true)
        );
    }

    #[tokio::test]
    async fn expiring_grant_cancels_its_transport_token_without_canceling_parent() {
        let mut grant = Grant::peer();
        let parent = grant.cancellation_token();
        grant.expire_at(SystemTime::now() + Duration::from_millis(50));
        let transport = grant.cancellation_token();
        tokio::time::timeout(Duration::from_secs(1), transport.cancelled())
            .await
            .expect("expiry must wake token-only transport watchers");
        assert!(grant.is_cancelled());
        assert!(
            !parent.is_cancelled(),
            "other viewers share the parent token"
        );
    }

    #[tokio::test]
    async fn iptv_limit_rejects_a_second_live_connection_with_the_same_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = IptvStore::open(dir.path().join("iptv.json")).unwrap();
        store.set_enabled(true).unwrap();
        store
            .put_package("basic", json!({"channels":["news"]}))
            .unwrap();
        store
            .put_subscriber(
                "alice",
                json!({"token":"subscriber-token","packages":["basic"],"max_sessions":1}),
            )
            .unwrap();
        let auth = PlaybackAuth::with_iptv(8, store);
        let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
        let snapshot = auth.publish("news", Some(policy)).unwrap();
        let viewer = ViewerRequest {
            name: "news".into(),
            proto: "mpegts".into(),
            ip: "192.0.2.10".into(),
            token: "subscriber-token".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(first) = auth.authorize(snapshot.clone(), viewer.clone()).await
        else {
            panic!("first subscriber connection denied")
        };
        assert!(matches!(
            auth.authorize(snapshot.clone(), viewer.clone()).await,
            AuthOutcome::Denied
        ));
        drop(first);
        assert!(matches!(
            auth.authorize(snapshot, viewer).await,
            AuthOutcome::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn iptv_connections_use_global_capacity_without_blocking_hls_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = IptvStore::open(dir.path().join("iptv.json")).unwrap();
        store.set_enabled(true).unwrap();
        store
            .put_package("basic", json!({"channels":["news"]}))
            .unwrap();
        store
            .put_subscriber(
                "alice",
                json!({"token":"subscriber-token","packages":["basic"],"max_sessions":3}),
            )
            .unwrap();
        let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
        let viewer = |proto: &str| ViewerRequest {
            name: "news".into(),
            proto: proto.into(),
            ip: "192.0.2.10".into(),
            token: "subscriber-token".into(),
            ..Default::default()
        };

        let auth = PlaybackAuth::with_iptv(1, store.clone());
        let snapshot = auth.publish("news", Some(policy.clone())).unwrap();
        let AuthOutcome::Allowed(first) = auth.authorize(snapshot.clone(), viewer("mpegts")).await
        else {
            panic!("first connection denied")
        };
        assert!(matches!(
            auth.authorize(snapshot, viewer("mpegts")).await,
            AuthOutcome::Denied
        ));
        drop(first);

        let hls = PlaybackAuth::with_iptv(1, store);
        let snapshot = hls.publish("news", Some(policy)).unwrap();
        let AuthOutcome::Allowed(first_segment) =
            hls.authorize(snapshot.clone(), viewer("hls")).await
        else {
            panic!("first HLS request denied")
        };
        assert!(matches!(
            hls.authorize(snapshot, viewer("hls")).await,
            AuthOutcome::Allowed(_)
        ));
        drop(first_segment);
    }

    #[tokio::test]
    async fn global_limit_counts_matching_continuous_connections_for_open_streams() {
        let auth = PlaybackAuth::new(1);
        let policy = Policy::from_config(&json!({}), &json!({})).unwrap();
        let snapshot = auth.publish("news", Some(policy)).unwrap();
        let request = ViewerRequest {
            name: "news".into(),
            proto: "mpegts".into(),
            ip: "192.0.2.10".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(first) = auth.authorize(snapshot.clone(), request.clone()).await
        else {
            panic!("first open connection denied")
        };
        assert!(matches!(
            auth.authorize(snapshot, request).await,
            AuthOutcome::Denied
        ));
        drop(first);
    }

    #[tokio::test]
    async fn finite_m4f_fragments_share_a_signal_slot_but_two_signals_do_not() {
        let dir = tempfile::tempdir().unwrap();
        let store = IptvStore::open(dir.path().join("iptv.json")).unwrap();
        store.set_enabled(true).unwrap();
        store
            .put_package("basic", json!({"channels":["news"]}))
            .unwrap();
        store
            .put_subscriber(
                "alice",
                json!({"token":"subscriber-token","packages":["basic"],"max_sessions":1}),
            )
            .unwrap();
        let auth = PlaybackAuth::with_iptv(1, store);
        let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
        let snapshot = auth.publish("news", Some(policy)).unwrap();
        let fragment = ViewerRequest {
            name: "news".into(),
            proto: "m4f".into(),
            token: "subscriber-token".into(),
            finite_media: true,
            ..Default::default()
        };
        let AuthOutcome::Allowed(first_fragment) =
            auth.authorize(snapshot.clone(), fragment.clone()).await
        else {
            panic!("first finite M4F fragment denied")
        };
        let AuthOutcome::Allowed(second_fragment) =
            auth.authorize(snapshot.clone(), fragment.clone()).await
        else {
            panic!("overlapping finite M4F fragment denied")
        };
        let signal = ViewerRequest {
            finite_media: false,
            ..fragment
        };
        let AuthOutcome::Allowed(first_signal) =
            auth.authorize(snapshot.clone(), signal.clone()).await
        else {
            panic!("matching M4F signal denied")
        };
        assert_eq!(auth.active(), 1);
        assert!(matches!(
            auth.authorize(snapshot, signal).await,
            AuthOutcome::Denied
        ));
        drop((first_fragment, second_fragment, first_signal));
    }

    #[tokio::test]
    async fn two_matching_rtsp_control_grants_cannot_bypass_capacity_on_promotion() {
        let auth = PlaybackAuth::new(1);
        let policy = Policy::from_config(&json!({}), &json!({})).unwrap();
        let snapshot = auth.publish("news", Some(policy)).unwrap();
        let viewer = ViewerRequest {
            name: "news".into(),
            proto: "rtsp".into(),
            ip: "192.0.2.10".into(),
            ..Default::default()
        };
        let AuthOutcome::Allowed(first) = auth
            .authorize_control(snapshot.clone(), viewer.clone())
            .await
        else {
            panic!("first control admission denied")
        };
        assert!(matches!(
            auth.authorize_control(snapshot.clone(), viewer.clone())
                .await,
            AuthOutcome::Denied
        ));
        first.playback();
        assert_eq!(auth.active(), 1);
        assert!(matches!(
            auth.authorize_control(snapshot, viewer).await,
            AuthOutcome::Denied
        ));
    }

    #[tokio::test]
    async fn unrelated_direct_output_cannot_share_an_unclaimed_handoff_slot() {
        let dir = tempfile::tempdir().unwrap();
        let store = IptvStore::open(dir.path().join("iptv.json")).unwrap();
        store.set_enabled(true).unwrap();
        store
            .put_package("basic", json!({"channels":["news"]}))
            .unwrap();
        store
            .put_subscriber(
                "alice",
                json!({"token":"subscriber-token","packages":["basic"],"max_sessions":3}),
            )
            .unwrap();
        let auth = PlaybackAuth::with_iptv(3, store);
        let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
        let snapshot = auth.publish("news", Some(policy)).unwrap();
        let viewer = ViewerRequest {
            name: "news".into(),
            proto: "mpegts".into(),
            token: "subscriber-token".into(),
            ..Default::default()
        };
        let mut handoffs = Vec::new();
        for _ in 0..2 {
            let slot = auth.reserve_capacity(0).unwrap();
            let AuthOutcome::Allowed(mut handoff) = auth
                .authorize_reserved(snapshot.clone(), viewer.clone(), slot)
                .await
            else {
                panic!("reserved control denied")
            };
            handoff.release_capacity_reservation();
            handoff.mark_handoff();
            handoffs.push(handoff);
        }
        let AuthOutcome::Allowed(direct) = auth.authorize(snapshot.clone(), viewer.clone()).await
        else {
            panic!("third independent slot denied")
        };
        assert_eq!(auth.active(), 3);
        assert!(matches!(
            auth.authorize(snapshot.clone(), viewer.clone()).await,
            AuthOutcome::Denied
        ));
        let AuthOutcome::Allowed(claimed) =
            auth.authorize_handoff(snapshot, viewer, &handoffs[0]).await
        else {
            panic!("claimed handoff lost its promised slot")
        };
        assert_eq!(auth.active(), 3);
        drop((direct, claimed, handoffs));
    }

    #[tokio::test]
    async fn hls_segments_share_a_matching_unclaimed_handoff_identity() {
        let auth = PlaybackAuth::new(1);
        let policy = Policy::from_config(&json!({}), &json!({})).unwrap();
        let snapshot = auth.publish("news", Some(policy)).unwrap();
        let viewer = ViewerRequest {
            name: "news".into(),
            proto: "hls".into(),
            token: "viewer".into(),
            ..Default::default()
        };
        let reservation = auth.reserve_capacity(0).unwrap();
        let AuthOutcome::Allowed(mut handoff) = auth
            .authorize_reserved(snapshot.clone(), viewer.clone(), reservation)
            .await
        else {
            panic!("HLS control handoff denied")
        };
        handoff.release_capacity_reservation();
        handoff.mark_handoff();
        let AuthOutcome::Allowed(segment) = auth.authorize(snapshot.clone(), viewer.clone()).await
        else {
            panic!("same-identity HLS segment denied")
        };
        assert_eq!(auth.active(), 1);
        let AuthOutcome::Allowed(claimed) =
            auth.authorize_handoff(snapshot, viewer, &handoff).await
        else {
            panic!("HLS handoff lost its slot")
        };
        assert_eq!(auth.active(), 1);
        drop((segment, claimed, handoff));
    }

    #[tokio::test]
    async fn subscriber_edit_between_lookup_and_admission_cannot_publish_old_token() {
        for edit in [
            "token",
            "package",
            "subscriber",
            "limit",
            "disable",
            "expiry",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store = IptvStore::open(dir.path().join("iptv.json")).unwrap();
            store.set_enabled(true).unwrap();
            store
                .put_package("basic", json!({"channels":["news","sports"]}))
                .unwrap();
            store
                .put_subscriber(
                    "alice",
                    json!({"token":"old-token","packages":["basic"],"max_sessions":2}),
                )
                .unwrap();
            let auth = Arc::new(PlaybackAuth::with_iptv(8, store.clone()));
            let weak = Arc::downgrade(&auth);
            store.set_on_change(Box::new(move |_| {
                if let Some(auth) = weak.upgrade() {
                    auth.invalidate_iptv();
                }
            }));
            let policy = Policy::from_config(&json!({"on_play":"iptv://"}), &json!({})).unwrap();
            let news = auth.publish("news", Some(policy.clone())).unwrap();
            let sports = auth.publish("sports", Some(policy)).unwrap();
            let viewer = |name: &str| ViewerRequest {
                name: name.into(),
                proto: "mpegts".into(),
                token: "old-token".into(),
                ..Default::default()
            };
            let held = if edit == "limit" {
                let AuthOutcome::Allowed(grant) =
                    auth.authorize(news.clone(), viewer("news")).await
                else {
                    panic!("initial grant");
                };
                Some(grant)
            } else {
                None
            };
            let (selected, request) = if edit == "limit" {
                (sports, viewer("sports"))
            } else {
                (news, viewer("news"))
            };
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            *auth.iptv_lookup_gate.lock().unwrap() = Some((entered_tx, release_rx));
            let pending = tokio::spawn({
                let auth = auth.clone();
                async move { auth.authorize(selected, request).await }
            });
            entered_rx.await.unwrap();
            match edit {
                "token" => {
                    store
                        .put_subscriber("alice", json!({"token":"new-token"}))
                        .unwrap();
                }
                "package" => {
                    store.delete_package("basic").unwrap();
                }
                "subscriber" => {
                    store.delete_subscriber("alice").unwrap();
                }
                "limit" => {
                    store
                        .put_subscriber("alice", json!({"max_sessions":1}))
                        .unwrap();
                }
                "disable" => {
                    store.set_enabled(false).unwrap();
                }
                "expiry" => {
                    store
                        .put_subscriber(
                            "alice",
                            json!({"flussonix_expires_at":"2000-01-01T00:00:00Z"}),
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
            release_tx.send(()).unwrap();
            assert!(
                matches!(pending.await.unwrap(), AuthOutcome::Denied),
                "{edit}"
            );
            drop(held);
        }
    }

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
