//! Retained admitted HLS demand and caller-owned lease maintenance.
mod maintenance;
use super::*;
use crate::source_directory::modern_coordinator::{
    AdmissionFailure, AdmittedCandidate, SharedInputOptions, SharedInputs,
};
use std::sync::Mutex as StateMutex;
use tokio::sync::Notify;
#[derive(Clone)]
pub struct SharedHlsOptions {
    pub idle_timeout: Duration,
    pub lease_lifetime: Duration,
    pub renew_margin: Duration,
    pub retry_delay: Duration,
    pub maintenance_interval: Duration,
    pub capacity: usize,
}
impl Default for SharedHlsOptions {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            lease_lifetime: Duration::from_secs(30),
            renew_margin: Duration::from_secs(10),
            retry_delay: Duration::from_secs(1),
            maintenance_interval: Duration::from_millis(250),
            capacity: 20000,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlsSessionFailure {
    Invalid,
    Closed,
    AlreadyRunning,
    Limit,
    Authority(SharedFailure),
    Packaging(SharedPackagingFailure),
}
pub struct SharedHlsSessions {
    registry: Arc<SharedInputs>,
    options: SharedHlsOptions,
    state: StateMutex<State>,
    changed: Notify,
}
#[derive(Default)]
struct State {
    active: Option<Active>,
    entries: HashMap<String, Entry>,
    pending: HashMap<String, usize>,
    slots: usize,
    last_sweep: Option<Instant>,
}
struct Active {
    epoch: Arc<()>,
    cancel: CancellationToken,
}
struct Entry {
    playback: PackagedPlayback,
    touched: Instant,
}
struct RunGuard {
    sessions: Arc<SharedHlsSessions>,
    epoch: Arc<()>,
}
struct Reservation<'a> {
    sessions: &'a SharedHlsSessions,
    epoch: Arc<()>,
    key: String,
    cancel: CancellationToken,
}
impl SharedHlsSessions {
    pub fn new(
        registry: Arc<SharedInputs>,
        options: SharedHlsOptions,
    ) -> Result<Self, HlsSessionFailure> {
        if options.capacity == 0
            || options.capacity > 20000
            || options.idle_timeout.is_zero()
            || options.idle_timeout > Duration::from_secs(30)
            || options.lease_lifetime.is_zero()
            || options.lease_lifetime > Duration::from_secs(30)
            || options.renew_margin.is_zero()
            || options.renew_margin >= options.lease_lifetime
            || options.retry_delay.is_zero()
            || options.retry_delay > Duration::from_secs(5)
            || options.maintenance_interval.is_zero()
            || options.maintenance_interval > Duration::from_secs(1)
        {
            return Err(HlsSessionFailure::Invalid);
        }
        Ok(Self {
            registry,
            options,
            state: StateMutex::new(State::default()),
            changed: Notify::new(),
        })
    }
    pub fn running(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .active
            .as_ref()
            .is_some_and(|a| !a.cancel.is_cancelled())
    }
    pub fn retained_sessions(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }
    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        if let Some(active) = state.active.as_ref() {
            active.cancel.cancel();
        }
        state.entries.clear();
        state.pending.clear();
        state.slots = 0;
        self.changed.notify_one();
    }
    fn prune(&self, state: &mut State) {
        state.last_sweep = Some(Instant::now());
        let removed = state
            .entries
            .iter()
            .filter(|(_, e)| {
                e.touched.elapsed() >= self.options.idle_timeout || !e.playback.retained()
            })
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        for key in removed {
            state.entries.remove(&key);
            if !state.pending.contains_key(&key) {
                state.slots -= 1;
            }
        }
    }
    fn reserve(&self, key: String) -> Result<Reservation<'_>, HlsSessionFailure> {
        let mut state = self.state.lock().unwrap();
        // Most HLS requests touch one existing native session in O(1).
        if state.entries.get(&key).is_some_and(|e| {
            e.touched.elapsed() >= self.options.idle_timeout || !e.playback.retained()
        }) {
            state.entries.remove(&key);
            if !state.pending.contains_key(&key) {
                state.slots -= 1;
            }
        }
        if state.slots >= self.options.capacity
            && state
                .last_sweep
                .is_none_or(|last| last.elapsed() >= self.options.maintenance_interval)
        {
            self.prune(&mut state);
        }
        let active = state
            .active
            .as_ref()
            .filter(|a| !a.cancel.is_cancelled())
            .ok_or(HlsSessionFailure::Closed)?;
        let (epoch, cancel) = (active.epoch.clone(), active.cancel.clone());
        if !state.entries.contains_key(&key) && !state.pending.contains_key(&key) {
            if state.slots >= self.options.capacity {
                return Err(HlsSessionFailure::Limit);
            }
            state.slots += 1;
        }
        *state.pending.entry(key.clone()).or_default() += 1;
        Ok(Reservation {
            sessions: self,
            epoch,
            key,
            cancel,
        })
    }
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) -> Result<(), HlsSessionFailure> {
        if cancel.is_cancelled() {
            return Err(HlsSessionFailure::Closed);
        }
        let local = cancel.child_token();
        let guard = {
            let mut state = self.state.lock().unwrap();
            if state.active.is_some() {
                return Err(HlsSessionFailure::AlreadyRunning);
            }
            let epoch = Arc::new(());
            state.active = Some(Active {
                epoch: epoch.clone(),
                cancel: local.clone(),
            });
            RunGuard {
                sessions: self.clone(),
                epoch,
            }
        };
        self.maintain(local).await;
        drop(guard);
        Ok(())
    }
    pub async fn package(
        &self,
        engine: &Engine,
        admission: Box<AdmittedCandidate>,
        options: SharedInputOptions,
        settings: &Value,
    ) -> Result<PackagedPlayback, HlsSessionFailure> {
        self.package_inner(engine, admission, Some(options), settings)
            .await
    }
    pub(crate) async fn package_configured(
        &self,
        engine: &Engine,
        admission: Box<AdmittedCandidate>,
        settings: &Value,
    ) -> Result<PackagedPlayback, HlsSessionFailure> {
        self.package_inner(engine, admission, None, settings).await
    }
    async fn package_inner(
        &self,
        engine: &Engine,
        admission: Box<AdmittedCandidate>,
        options: Option<SharedInputOptions>,
        settings: &Value,
    ) -> Result<PackagedPlayback, HlsSessionFailure> {
        if !self.running() {
            return Err(HlsSessionFailure::Closed);
        }
        let key = self
            .registry
            .hls_session_identity(&admission)
            .map_err(|e| {
                if e == SharedFailure::Authority(AdmissionFailure::Invalid) {
                    HlsSessionFailure::Invalid
                } else {
                    HlsSessionFailure::Authority(e)
                }
            })?;
        let reservation = self.reserve(key)?;
        let attachment = match options {
            Some(options) => self.registry.attach(admission, options),
            None => self.registry.attach_configured(admission, settings),
        }
        .map_err(HlsSessionFailure::Authority)?;
        let playback = tokio::select! {biased;_=reservation.cancel.cancelled()=>return Err(HlsSessionFailure::Closed),r=engine.package_shared(attachment,settings)=>r.map_err(HlsSessionFailure::Packaging)?};
        playback.current().map_err(HlsSessionFailure::Packaging)?;
        {
            let mut state = self.state.lock().unwrap();
            if !state.active.as_ref().is_some_and(|a| {
                Arc::ptr_eq(&a.epoch, &reservation.epoch) && !a.cancel.is_cancelled()
            }) {
                return Err(HlsSessionFailure::Closed);
            }
            let was_empty = state.entries.is_empty();
            state.entries.insert(
                reservation.key.clone(),
                Entry {
                    playback: playback.clone(),
                    touched: Instant::now(),
                },
            );
            if was_empty {
                self.changed.notify_one();
            }
        }
        Ok(playback)
    }
}
impl PackagedPlayback {
    // Retention is not an authority fence. Actual reads and writes use current().
    fn retained(&self) -> bool {
        !self.inner.source.is_closed()
            && !self.inner.worker.is_closed()
            && self.inner.worker.alive.load(Ordering::Relaxed)
    }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut state = self.sessions.state.lock().unwrap();
        if !state
            .active
            .as_ref()
            .is_some_and(|a| Arc::ptr_eq(&a.epoch, &self.epoch))
        {
            return;
        }
        if let Some(count) = state.pending.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                state.pending.remove(&self.key);
                if !state.entries.contains_key(&self.key) {
                    state.slots -= 1;
                }
            }
        }
    }
}
impl Drop for RunGuard {
    fn drop(&mut self) {
        let mut state = self.sessions.state.lock().unwrap();
        if state
            .active
            .as_ref()
            .is_some_and(|a| Arc::ptr_eq(&a.epoch, &self.epoch))
        {
            state.active.take().unwrap().cancel.cancel();
            state.entries.clear();
            state.pending.clear();
            state.slots = 0;
        }
    }
}
