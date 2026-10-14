//! Bounded owner-scheduled replacement using original admitted viewer permission.
#[cfg(test)]
#[path = "failure_retirement_tests.rs"]
mod failure_retirement_tests;
use super::replicas::{Group, Mode};
use super::*;
use crate::{
    m4_ingest::flussonic::InputFailure, media::PackagedPlayback,
    source_directory::modern_coordinator::AdmittedCandidate,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, pin::Pin};
#[derive(Default)]
pub(super) struct State {
    pub entries: HashMap<String, Entry>,
}
pub(super) struct Entry {
    group: Arc<Group>,
    settings: Value,
    anchors: HashMap<String, Arc<AdmittedCandidate>>,
    playback: Option<PackagedPlayback>,
    mode: Mode,
    next_attempt: Instant,
    streak: u32,
    pending: usize,
}
pub(super) struct Attempt<'a> {
    runtime: &'a Runtime,
    name: String,
    group: Arc<Group>,
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if let Some(entry) = self
            .runtime
            .recovery
            .lock()
            .unwrap()
            .entries
            .get_mut(&self.name)
            .filter(|entry| Arc::ptr_eq(&entry.group, &self.group))
        {
            entry.pending = entry.pending.saturating_sub(1);
        }
    }
}
impl Runtime {
    pub(super) fn register_anchor<'a>(
        &'a self,
        name: &str,
        group: Arc<Group>,
        admission: &AdmittedCandidate,
        settings: &Value,
    ) -> Result<Attempt<'a>, Failure> {
        // An old foreground request must not retire a newer recovery generation.
        self.owner
            .with_admitted_current(admission, || {
                self.register_anchor_current(name, group, admission, settings)
            })
            .map_err(|_| Failure::Unavailable)?
    }
    fn register_anchor_current<'a>(
        &'a self,
        name: &str,
        group: Arc<Group>,
        admission: &AdmittedCandidate,
        settings: &Value,
    ) -> Result<Attempt<'a>, Failure> {
        let id = admission
            .session_identity()
            .ok_or(Failure::Unavailable)?
            .to_owned();
        let mut state = self.recovery.lock().map_err(|_| Failure::Unavailable)?;
        state
            .entries
            .retain(|_, e| e.group.revision == group.revision);
        self.inputs
            .retain_failures(|name| state.entries.contains_key(name));
        let count: usize = state.entries.values().map(|e| e.anchors.len()).sum();
        if state.entries.len() >= 256 && !state.entries.contains_key(name) {
            return Err(Failure::Unavailable);
        }
        if count >= 20_000
            && !state
                .entries
                .get(name)
                .is_some_and(|e| e.anchors.contains_key(&id))
        {
            return Err(Failure::Unavailable);
        }
        self.inputs
            .reserve_failure(name)
            .map_err(|_| Failure::Unavailable)?;
        let entry = state.entries.entry(name.into()).or_insert_with(|| Entry {
            group: group.clone(),
            settings: settings.clone(),
            anchors: HashMap::new(),
            playback: None,
            mode: Mode::Normal,
            next_attempt: Instant::now() + Duration::from_secs(1),
            streak: 0,
            pending: 0,
        });
        if !Arc::ptr_eq(&entry.group, &group) {
            return Err(Failure::Unavailable);
        }
        entry.anchors.insert(id, admission.anchor_copy());
        entry.pending += 1;
        Ok(Attempt {
            runtime: self,
            name: name.into(),
            group,
        })
    }
    pub(super) fn remember_recovery(&self, name: &str, playback: &PackagedPlayback) {
        // Source -> selected route -> recovery state. Do not publish readiness or
        // overwrite a successor using a read whose source fence already ended.
        let _ = playback.with_current_source(|| {
            let mut state = self.recovery.lock().unwrap();
            if let Some(entry) = state.entries.get_mut(name) {
                let stats = playback.stats();
                if stats["uptime"].as_u64().unwrap_or(0) >= 30 {
                    entry.streak = 0;
                }
                entry.playback = Some(playback.clone());
                entry.next_attempt = Instant::now() + Duration::from_millis(250);
            }
        });
    }
    pub(super) fn recovery_mode(&self, name: &str, group: &Arc<Group>) -> Mode {
        if group.selector.private_rejected(name).unwrap_or(true) {
            return Mode::Selected;
        }
        self.recovery
            .lock()
            .unwrap()
            .entries
            .get(name)
            .filter(|e| Arc::ptr_eq(&e.group, group))
            .map_or(Mode::Normal, |e| e.mode)
    }
    pub(super) fn selected_recovery(&self, name: &str, group: &Arc<Group>) {
        if let Some(entry) = self.recovery.lock().unwrap().entries.get_mut(name)
            && Arc::ptr_eq(&entry.group, group)
        {
            entry.mode = Mode::Normal;
        }
    }
    pub(super) fn process_failure(&self, name: &str, group: &Arc<Group>) {
        let Some(observation) = self.inputs.take_failure(name) else {
            return;
        };
        // An old worker may finish after a successor was installed. Its source/lease
        // and selected-route generation must still be current before any mutation.
        let playback = self
            .recovery
            .lock()
            .unwrap()
            .entries
            .get(name)
            .and_then(|e| e.playback.clone());
        if playback.is_some_and(|p| p.generation() != observation.generation && p.current().is_ok())
        {
            return;
        }
        if !group.selector.failure_current(&observation.notice) {
            return;
        }
        if let Some(entry) = self.recovery.lock().unwrap().entries.get_mut(name)
            && Arc::ptr_eq(&entry.group, group)
        {
            entry.mode = if observation.notice.kind == InputFailure::Transient {
                Mode::Rotate
            } else {
                Mode::Selected
            };
            entry.next_attempt = Instant::now() + crate::recovery::retry_delay(entry.streak);
        }
    }
    async fn recover_one(&self, app: &App, name: &str, group: Arc<Group>) {
        let _gate = group.gate.lock().await;
        if app.config.revision() != group.revision || !self.running() {
            return;
        }
        self.process_failure(name, &group);
        let playback = self
            .recovery
            .lock()
            .unwrap()
            .entries
            .get(name)
            .and_then(|e| e.playback.clone());
        if let Some(playback) = playback
            && playback.current().is_ok()
        {
            self.remember_recovery(name, &playback);
            return;
        }
        let work = {
            let mut state = self.recovery.lock().unwrap();
            let Some(entry) = state.entries.get_mut(name) else {
                return;
            };
            if !Arc::ptr_eq(&entry.group, &group) || entry.pending > 0 {
                return;
            }
            if entry.next_attempt > Instant::now() {
                return;
            }
            let Some(anchor) = entry
                .anchors
                .values()
                .find(|a| a.recovery_activity().is_some())
                .cloned()
            else {
                return;
            };
            entry.next_attempt = Instant::now() + crate::recovery::retry_delay(entry.streak);
            entry.streak = entry.streak.saturating_add(1);
            (anchor, entry.settings.clone())
        };
        let mode = self.recovery_mode(name, &group);
        let Ok(choice) = self.choose_group_locked(app, &group, name, mode).await else {
            return;
        };
        self.selected_recovery(name, &group);
        let Ok(admission) = self.owner.recover_replica(choice, &work.0) else {
            return;
        };
        let Ok(attachment) = self.inputs.attach_configured(admission, &work.1) else {
            return;
        };
        let Ok(playback) = app.media.package_shared(attachment, &work.1).await else {
            return;
        };
        // package_shared rechecks both source/group and original native demand after
        // asynchronous startup. No playback accounting is promoted for this owner.
        self.remember_recovery(name, &playback);
    }
    pub(super) async fn maintain_replicas(&self, app: &App, cancel: CancellationToken) {
        type Job<'a> = Pin<Box<dyn Future<Output = String> + Send + 'a>>;
        let mut jobs = FuturesUnordered::<Job<'_>>::new();
        let mut active = HashMap::<String, CancellationToken>::new();
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {biased;_=cancel.cancelled()=>break,Some(name)=jobs.next(),if !jobs.is_empty()=>{active.remove(&name);},_=tick.tick()=>{}}
            if cancel.is_cancelled() {
                break;
            }
            let due = {
                let mut state = self.recovery.lock().unwrap();
                state.entries.retain(|_, e| {
                    if e.group.revision != app.config.revision() {
                        return false;
                    }
                    e.anchors
                        .retain(|_, a| a.recovery_activity().is_some() || e.pending > 0);
                    !e.anchors.is_empty() || e.pending > 0
                });
                self.inputs
                    .retain_failures(|name| state.entries.contains_key(name));
                for (name, token) in &active {
                    if !state.entries.contains_key(name) {
                        token.cancel();
                    }
                }
                state
                    .entries
                    .iter()
                    .filter(|(name, e)| {
                        !active.contains_key(*name)
                            && e.pending == 0
                            && e.next_attempt <= Instant::now()
                    })
                    .map(|(name, e)| (name.clone(), e.group.clone()))
                    .take(8usize.saturating_sub(active.len()))
                    .collect::<Vec<_>>()
            };
            if app.options.role == "lb" {
                continue;
            }
            for (name, group) in due {
                let token = cancel.child_token();
                active.insert(name.clone(), token.clone());
                jobs.push(Box::pin(async move{tokio::select!{biased;_=token.cancelled()=>{},_=tokio::time::timeout(Duration::from_secs(8),self.recover_one(app,&name,group))=>{}}name}));
            }
        }
        drop(jobs);
    }
}
