//! Bounded, token-free subscriber lease accounting for a single IPTV authority.
//! The peer API and playback grant wiring use this ledger in later stages.
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Mutex, RwLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use uuid::Uuid;

const SHARDS: usize = 64;
const LEASE_TTL: Duration = Duration::from_secs(10);
const PENDING_TTL: Duration = Duration::from_secs(2);
const RESTART_QUARANTINE: Duration = Duration::from_secs(11);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriberId([u8; 32]);

impl SubscriberId {
    pub fn from_name(name: &str) -> Self {
        Self(Sha256::digest(name.as_bytes()).into())
    }

    fn shard(self) -> usize {
        usize::from(self.0[0]) % SHARDS
    }

    pub fn as_hex(self) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write;
            let _ = write!(encoded, "{byte:02x}");
        }
        encoded
    }

    pub fn parse_hex(value: &str) -> Option<Self> {
        if value.len() != 64 || !value.is_ascii() {
            return None;
        }
        let mut bytes = [0; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
        }
        Some(Self(bytes))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub boot: Uuid,
    pub local: Uuid,
}

#[derive(Clone, Debug)]
pub struct LeaseRequest {
    pub subscriber: SubscriberId,
    pub session: SessionKey,
    pub operation: Uuid,
    pub units: u32,
    pub limit: Option<u32>,
    pub revision: u64,
    pub subscriber_expires_at: Option<SystemTime>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseGrant {
    pub id: Uuid,
    pub deadline: Instant,
    pub units: u32,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Quarantine,
    Expired,
    Invalid,
    Limit,
    Full,
    Missing,
    Stale,
}

#[derive(Clone, Copy)]
struct Lease {
    grant: LeaseGrant,
    operation: Uuid,
    committed: bool,
    previous: Option<LeaseGrant>,
}

impl Lease {
    fn occupied_units(self) -> u32 {
        self.grant
            .units
            .max(self.previous.map_or(0, |previous| previous.units))
    }
}

#[derive(Default)]
struct Subscriber {
    revision: u64,
    leases: HashMap<SessionKey, Lease>,
}

pub struct Ledger {
    shards: [Mutex<HashMap<SubscriberId, Subscriber>>; SHARDS],
    active: AtomicUsize,
    sweeps: AtomicU64,
    shards_scanned: AtomicU64,
    revision_floor: RwLock<u64>,
    max_active: usize,
    started: Instant,
    quarantine_until: Instant,
    expiry_gate: RwLock<()>,
    shard_next_expiry: [AtomicU64; SHARDS],
}

impl Ledger {
    pub fn new(started: Instant, max_active: usize, store_revision: u64) -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            active: AtomicUsize::new(0),
            sweeps: AtomicU64::new(0),
            shards_scanned: AtomicU64::new(0),
            revision_floor: RwLock::new(store_revision),
            max_active: max_active.min(100_000),
            started,
            quarantine_until: started + RESTART_QUARANTINE,
            expiry_gate: RwLock::new(()),
            shard_next_expiry: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
        }
    }

    pub fn active_leases(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    pub fn accepting_new_leases(&self, now: Instant) -> bool {
        now >= self.quarantine_until
    }

    /// Whether a new distinct session can be admitted now. Retain the
    /// quarantine-only check above for renewals and idempotent acquires.
    pub fn ready_for_new_leases(&self, now: Instant) -> bool {
        if !self.accepting_new_leases(now) {
            return false;
        }
        self.sweep_under_pressure(now);
        self.active_leases() < self.max_active
    }

    pub fn sweep_count(&self) -> u64 {
        self.sweeps.load(Ordering::Relaxed)
    }

    pub fn shards_scanned(&self) -> u64 {
        self.shards_scanned.load(Ordering::Relaxed)
    }

    pub fn subscriber_records(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().unwrap().len())
            .sum()
    }

    /// The owner-only IPTV store calls this synchronously when publishing a
    /// new snapshot. It is one bounded high-water mark for its global revision.
    pub fn observe_revision(&self, revision: u64) {
        let mut floor = self.revision_floor.write().unwrap();
        *floor = (*floor).max(revision);
    }

    fn tick(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.started)
            .as_micros()
            .min(u128::from(u64::MAX - 1)) as u64
            + 1
    }

    fn note_deadline(&self, subscriber: SubscriberId, deadline: Instant) {
        self.shard_next_expiry[subscriber.shard()].fetch_min(self.tick(deadline), Ordering::AcqRel);
    }

    fn any_due_shard(&self, now_tick: u64) -> bool {
        self.shard_next_expiry
            .iter()
            .any(|next| next.load(Ordering::Acquire) <= now_tick)
    }

    /// Bounded by the configured maximum active leases: empty subscriber
    /// records are removed on every release, expiration and failed acquire.
    pub fn sweep_expired(&self, now: Instant) {
        let _expiry = self.expiry_gate.write().unwrap();
        self.sweep_expired_locked(now);
    }

    fn sweep_expired_locked(&self, now: Instant) {
        let now_tick = self.tick(now);
        let mut scanned = 0;
        for (index, shard) in self.shards.iter().enumerate() {
            if self.shard_next_expiry[index].load(Ordering::Acquire) > now_tick {
                continue;
            }
            scanned += 1;
            let mut next = u64::MAX;
            let mut states = shard.lock().unwrap();
            states.retain(|_, state| {
                self.prune(state, now);
                for lease in state.leases.values() {
                    next = next.min(self.tick(lease.grant.deadline));
                    if let Some(previous) = lease.previous {
                        next = next.min(self.tick(previous.deadline));
                    }
                }
                !state.leases.is_empty()
            });
            self.shard_next_expiry[index].store(next, Ordering::Release);
        }
        if scanned > 0 {
            self.sweeps.fetch_add(1, Ordering::Relaxed);
            self.shards_scanned.fetch_add(scanned, Ordering::Relaxed);
        }
    }

    fn sweep_under_pressure(&self, now: Instant) {
        if self.active_leases() < self.max_active {
            return;
        }
        let now_tick = self.tick(now);
        if !self.any_due_shard(now_tick) {
            return;
        }
        let _expiry = self.expiry_gate.write().unwrap();
        if self.active_leases() < self.max_active {
            return;
        }
        if self.any_due_shard(now_tick) {
            self.sweep_expired_locked(now);
        }
    }

    fn deadline(
        now: Instant,
        wall: SystemTime,
        expires: Option<SystemTime>,
        ttl: Duration,
    ) -> Result<Instant, Error> {
        let ttl = match expires {
            Some(expires) => ttl.min(expires.duration_since(wall).map_err(|_| Error::Expired)?),
            None => ttl,
        };
        if ttl.is_zero() {
            return Err(Error::Expired);
        }
        Ok(now + ttl)
    }

    fn prune(&self, state: &mut Subscriber, now: Instant) {
        let before = state.leases.len();
        state.leases.retain(|_, lease| {
            if !lease.committed && lease.previous.is_some_and(|old| old.deadline <= now) {
                lease.previous = None;
            }
            if !lease.committed && lease.grant.deadline <= now {
                match lease.previous.take() {
                    Some(previous) if previous.deadline > now => {
                        lease.grant = previous;
                        lease.committed = true;
                    }
                    _ => return false,
                }
            }
            lease.grant.deadline > now
        });
        self.active
            .fetch_sub(before - state.leases.len(), Ordering::Relaxed);
    }

    fn reserve_global(&self) -> Result<(), Error> {
        let mut count = self.active.load(Ordering::Relaxed);
        loop {
            if count >= self.max_active {
                return Err(Error::Full);
            }
            match self.active.compare_exchange_weak(
                count,
                count + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => count = actual,
            }
        }
    }

    pub fn acquire(
        &self,
        now: Instant,
        wall: SystemTime,
        req: &LeaseRequest,
    ) -> Result<LeaseGrant, Error> {
        if !self.accepting_new_leases(now) {
            return Err(Error::Quarantine);
        }
        if req.units == 0 || req.units > 1024 || req.limit == Some(0) {
            return Err(Error::Invalid);
        }
        let deadline = Self::deadline(now, wall, req.subscriber_expires_at, PENDING_TTL)?;
        {
            let floor = self.revision_floor.read().unwrap();
            if req.revision != *floor {
                return Err(Error::Stale);
            }
        }
        self.sweep_under_pressure(now);
        let floor = self.revision_floor.read().unwrap();
        if req.revision != *floor {
            return Err(Error::Stale);
        }
        let _expiry = self.expiry_gate.read().unwrap();
        let mut shard = self.shards[req.subscriber.shard()].lock().unwrap();
        if let Some(state) = shard.get_mut(&req.subscriber) {
            self.prune(state, now);
            if state.leases.is_empty() {
                shard.remove(&req.subscriber);
            }
        }
        if let std::collections::hash_map::Entry::Vacant(slot) = shard.entry(req.subscriber) {
            if req.limit.is_some_and(|limit| req.units > limit) {
                return Err(Error::Limit);
            }
            self.reserve_global()?;
            let grant = LeaseGrant {
                id: Uuid::new_v4(),
                deadline,
                units: req.units,
                revision: req.revision,
            };
            slot.insert(Subscriber {
                revision: req.revision,
                leases: HashMap::from([(
                    req.session,
                    Lease {
                        grant,
                        operation: req.operation,
                        committed: false,
                        previous: None,
                    },
                )]),
            });
            self.note_deadline(req.subscriber, deadline);
            return Ok(grant);
        }
        let state = shard.get_mut(&req.subscriber).unwrap();
        if req.revision < state.revision {
            return Err(Error::Stale);
        }
        state.revision = req.revision;
        let existing = state.leases.get(&req.session).copied();
        if let Some(lease) = existing {
            if !lease.committed {
                if lease.operation == req.operation
                    && lease.grant.revision == req.revision
                    && lease.grant.units == req.units
                {
                    return Ok(lease.grant);
                }
                return Err(Error::Invalid);
            }
            if lease.grant.revision == req.revision && req.units == lease.grant.units {
                return Ok(lease.grant);
            }
        }
        let occupied = state.leases.values().fold(0u64, |sum, lease| {
            sum.saturating_add(u64::from(lease.occupied_units()))
        });
        let remaining =
            occupied.saturating_sub(u64::from(existing.map_or(0, Lease::occupied_units)));
        let requested = req.units.max(existing.map_or(0, |lease| lease.grant.units));
        if req
            .limit
            .is_some_and(|limit| remaining.saturating_add(u64::from(requested)) > u64::from(limit))
        {
            return Err(Error::Limit);
        }
        if existing.is_none() {
            self.reserve_global()?;
        }
        let grant = LeaseGrant {
            id: Uuid::new_v4(),
            deadline,
            units: req.units,
            revision: req.revision,
        };
        state.leases.insert(
            req.session,
            Lease {
                grant,
                operation: req.operation,
                committed: false,
                previous: existing.map(|lease| lease.grant),
            },
        );
        self.note_deadline(req.subscriber, deadline);
        Ok(grant)
    }

    pub fn commit(
        &self,
        now: Instant,
        wall: SystemTime,
        req: &LeaseRequest,
        id: Uuid,
    ) -> Result<LeaseGrant, Error> {
        let deadline = Self::deadline(now, wall, req.subscriber_expires_at, LEASE_TTL)?;
        let floor = self.revision_floor.read().unwrap();
        if req.revision != *floor {
            return Err(Error::Stale);
        }
        let _expiry = self.expiry_gate.read().unwrap();
        let mut shard = self.shards[req.subscriber.shard()].lock().unwrap();
        if let Some(state) = shard.get_mut(&req.subscriber) {
            self.prune(state, now);
            if state.leases.is_empty() {
                shard.remove(&req.subscriber);
            }
        }
        let state = shard.get_mut(&req.subscriber).ok_or(Error::Missing)?;
        let lease = state
            .leases
            .get_mut(&req.session)
            .filter(|lease| lease.grant.id == id)
            .ok_or(Error::Missing)?;
        if lease.grant.revision != state.revision || req.revision != state.revision {
            return Err(Error::Stale);
        }
        if !lease.committed {
            if lease.operation != req.operation || lease.grant.units != req.units {
                return Err(Error::Invalid);
            }
            lease.grant.deadline = deadline;
            lease.committed = true;
            lease.previous = None;
        } else if req.units > lease.grant.units {
            return Err(Error::Invalid);
        } else {
            lease.grant.deadline = lease.grant.deadline.min(deadline);
        }
        self.note_deadline(req.subscriber, lease.grant.deadline);
        Ok(lease.grant)
    }

    pub fn rollback(&self, req: &LeaseRequest, id: Uuid) -> bool {
        let _expiry = self.expiry_gate.read().unwrap();
        let mut shard = self.shards[req.subscriber.shard()].lock().unwrap();
        let Some(state) = shard.get_mut(&req.subscriber) else {
            return false;
        };
        let Some(lease) = state.leases.get_mut(&req.session) else {
            return false;
        };
        if lease.grant.id != id || lease.committed || lease.operation != req.operation {
            return false;
        }
        if let Some(previous) = lease.previous.take() {
            lease.grant = previous;
            lease.committed = true;
            self.note_deadline(req.subscriber, previous.deadline);
        } else {
            state.leases.remove(&req.session);
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
        if state.leases.is_empty() {
            shard.remove(&req.subscriber);
        }
        true
    }

    pub fn renew(
        &self,
        now: Instant,
        wall: SystemTime,
        req: &LeaseRequest,
        id: Uuid,
    ) -> Result<LeaseGrant, Error> {
        if !self.accepting_new_leases(now) {
            return Err(Error::Quarantine);
        }
        let deadline = Self::deadline(now, wall, req.subscriber_expires_at, LEASE_TTL)?;
        let floor = self.revision_floor.read().unwrap();
        if req.revision != *floor {
            return Err(Error::Stale);
        }
        let _expiry = self.expiry_gate.read().unwrap();
        let mut shard = self.shards[req.subscriber.shard()].lock().unwrap();
        if let Some(state) = shard.get_mut(&req.subscriber) {
            self.prune(state, now);
            if state.leases.is_empty() {
                shard.remove(&req.subscriber);
            }
        }
        let state = shard.get_mut(&req.subscriber).ok_or(Error::Missing)?;
        let lease = state
            .leases
            .get_mut(&req.session)
            .filter(|lease| {
                lease.grant.id == id || lease.previous.is_some_and(|previous| previous.id == id)
            })
            .ok_or(Error::Missing)?;
        if !lease.committed
            && let Some(previous) = lease.previous.as_mut()
            && previous.id == id
            && req.revision == state.revision
            && previous.revision == req.revision
            && previous.units == req.units
        {
            previous.deadline = deadline;
            self.note_deadline(req.subscriber, previous.deadline);
            return Ok(*previous);
        }
        if req.revision != state.revision
            || lease.grant.revision != req.revision
            || lease.grant.units != req.units
        {
            return Err(Error::Stale);
        }
        if !lease.committed {
            return Err(Error::Invalid);
        }
        lease.grant.deadline = deadline;
        self.note_deadline(req.subscriber, deadline);
        Ok(lease.grant)
    }

    pub fn release(&self, subscriber: &SubscriberId, session: &SessionKey, id: Uuid) -> bool {
        let _expiry = self.expiry_gate.read().unwrap();
        let mut shard = self.shards[subscriber.shard()].lock().unwrap();
        let Some(state) = shard.get_mut(subscriber) else {
            return false;
        };
        if !state
            .leases
            .get(session)
            .is_some_and(|lease| lease.grant.id == id)
        {
            return false;
        }
        state.leases.remove(session);
        self.active.fetch_sub(1, Ordering::Relaxed);
        if state.leases.is_empty() {
            shard.remove(subscriber);
        }
        true
    }
}
