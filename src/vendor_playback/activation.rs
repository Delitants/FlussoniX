//! Daemon-owned static demand. Catalog facts never substitute for activation authority.
mod catalogs;
use super::{
    replicas::{Candidate, Group, Mode},
    *,
};
use crate::{media::PackagedPlayback, source_directory::modern_coordinator::ActivationControl};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Default)]
pub(super) struct State {
    revision: u64,
    catalogs: BTreeMap<String, Catalog>,
    pub demands: BTreeMap<String, Demand>,
    cursor: Option<String>,
    capacity: bool,
}
struct Catalog {
    names: BTreeSet<String>,
    expires: Option<Instant>,
    next_poll: Instant,
    error: Option<&'static str>,
}
pub(super) struct Demand {
    revision: u64,
    epoch: Arc<()>,
    key: String,
    group: Option<Arc<Group>>,
    settings: Value,
    control: Option<ActivationControl>,
    pub playback: Option<PackagedPlayback>,
    pub mode: Mode,
    pub next_attempt: Instant,
    streak: u32,
}
impl Drop for Demand {
    fn drop(&mut self) {
        if let Some(control) = &self.control {
            control.cancel();
        }
    }
}
impl Runtime {
    /// Only config/fact checks here. Final source and selected-route fences are
    /// acquired by activate/attach, outside the static-state lock.
    pub(super) fn sync_static(&self, app: &App) {
        let (root, revision) = app.config.read(|r| (r.clone(), app.config.revision()));
        let names = {
            let state = self.activation.lock().unwrap();
            if state.revision != revision {
                return;
            }
            state
                .catalogs
                .values()
                .filter(|c| c.expires.is_some_and(|t| t > Instant::now()))
                .flat_map(|c| c.names.iter().cloned())
                .collect::<BTreeSet<_>>()
        };
        let mut profiles = HashMap::new();
        let desired = names
            .into_iter()
            .filter_map(|name| {
                let Selection::Vendor { key, .. } = select(&root, &name, revision) else {
                    return None;
                };
                // Prepare each source/group once per pass, rather than once for
                // every stream in a large complete catalog.
                let profile = profiles
                    .entry(key.clone())
                    .or_insert_with(|| {
                        Some((
                            self.output_settings(app, &key, revision).ok()?,
                            self.group_for(app, &key, revision).ok()?,
                        ))
                    })
                    .as_ref()?;
                Some((name, key, profile.0.clone(), profile.1.clone()))
            })
            .collect::<Vec<_>>();
        app.config.at_revision(revision, |_| {
            let mut state = self.activation.lock().unwrap();
            if state.revision != revision {
                return;
            }
            let wanted = desired
                .iter()
                .map(|(n, ..)| n.as_str())
                .collect::<BTreeSet<_>>();
            state
                .demands
                .retain(|name, d| d.revision == revision && wanted.contains(name.as_str()));
            state.capacity = desired.len() > 256;
            for (name, key, settings, group) in desired {
                if state.demands.contains_key(&name) {
                    continue;
                }
                if state.demands.len() >= 256 {
                    state.capacity = true;
                    break;
                }
                // Slots are shared with viewer recovery. Retire them only after
                // both owners disappear, so either path can observe private loss.
                if self.inputs.reserve_failure(&name).is_err() {
                    state.capacity = true;
                    continue;
                }
                state.demands.insert(
                    name,
                    Demand {
                        revision,
                        epoch: Arc::new(()),
                        key,
                        group,
                        settings,
                        control: None,
                        playback: None,
                        mode: Mode::Normal,
                        next_attempt: Instant::now(),
                        streak: 0,
                    },
                );
            }
        });
    }
    pub(super) fn retain_failure_slots(&self, recovery: &recovery::State) {
        // Shared ordering: recovery -> static state -> failure slots.
        let state = self.activation.lock().unwrap();
        self.inputs
            .retain_failures(|n| recovery.entries.contains_key(n) || state.demands.contains_key(n));
    }
    pub(super) fn static_mode(&self, name: &str, group: &Arc<Group>) -> Option<Mode> {
        self.activation
            .lock()
            .unwrap()
            .demands
            .get(name)
            .filter(|d| d.group.as_ref().is_some_and(|g| Arc::ptr_eq(g, group)))
            .map(|d| d.mode)
    }
    pub(super) fn process_static_failure(&self, name: &str, group: &Arc<Group>, mode: Mode) {
        if let Some(d) = self.activation.lock().unwrap().demands.get_mut(name)
            && d.group.as_ref().is_some_and(|g| Arc::ptr_eq(g, group))
        {
            d.mode = mode;
            d.next_attempt = Instant::now() + crate::recovery::retry_delay(d.streak);
        }
    }
    pub(super) fn reset_static_mode(&self, name: &str, group: &Arc<Group>) {
        if let Some(d) = self.activation.lock().unwrap().demands.get_mut(name)
            && d.group.as_ref().is_some_and(|g| Arc::ptr_eq(g, group))
        {
            d.mode = Mode::Normal;
        }
    }
    pub(super) fn static_playback(&self, name: &str) -> Option<PackagedPlayback> {
        self.activation
            .lock()
            .unwrap()
            .demands
            .get(name)?
            .playback
            .clone()
    }
    pub(super) fn static_jobs(&self, active: &HashMap<String, recovery::ActiveJob>) -> Vec<String> {
        let mut state = self.activation.lock().unwrap();
        let due = state
            .demands
            .iter()
            .filter(|(n, d)| !active.contains_key(*n) && d.next_attempt <= Instant::now())
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();
        // Rotate across large catalogs; healthy owners do not starve cold names.
        let cursor = state.cursor.clone();
        let rotated = due
            .iter()
            .filter(|n| cursor.as_ref().is_none_or(|c| *n > c))
            .chain(
                due.iter()
                    .filter(|n| cursor.as_ref().is_some_and(|c| *n <= c)),
            )
            .cloned()
            .collect::<Vec<_>>();
        state.cursor = rotated.get(3).or(rotated.last()).cloned();
        rotated
    }
    pub(super) fn static_job_epoch(&self, name: &str) -> Option<(u64, Arc<()>)> {
        let state = self.activation.lock().unwrap();
        let demand = state.demands.get(name)?;
        Some((demand.revision, demand.epoch.clone()))
    }
    pub(super) fn static_job_current(&self, name: &str, revision: u64, epoch: &Arc<()>) -> bool {
        self.activation
            .lock()
            .unwrap()
            .demands
            .get(name)
            .is_some_and(|d| d.revision == revision && Arc::ptr_eq(&d.epoch, epoch))
    }
    pub(super) async fn start_static(&self, app: &App, name: &str, expected: &Arc<()>) -> bool {
        let _activity = app.media.idle_memory.activity().await;
        let (revision, epoch, key, group, settings, playback) = {
            let mut state = self.activation.lock().unwrap();
            let Some(d) = state
                .demands
                .get_mut(name)
                .filter(|d| Arc::ptr_eq(&d.epoch, expected))
            else {
                return false;
            };
            if d.next_attempt > Instant::now() {
                return false;
            }
            (
                d.revision,
                d.epoch.clone(),
                d.key.clone(),
                d.group.clone(),
                d.settings.clone(),
                d.playback.clone(),
            )
        };
        if !self.running() || app.config.revision() != revision {
            return false;
        }
        if let Some(p) = playback
            && p.current().is_ok()
        {
            self.remember_static(name, &epoch, &p);
            self.remember_recovery(name, &p);
            return true;
        }
        {
            let mut state = self.activation.lock().unwrap();
            let Some(d) = state
                .demands
                .get_mut(name)
                .filter(|d| Arc::ptr_eq(&d.epoch, &epoch))
            else {
                return false;
            };
            d.next_attempt = Instant::now() + crate::recovery::retry_delay(d.streak);
            d.streak = d.streak.saturating_add(1);
        }
        // Serialize static and foreground/recovery selection through the same gate.
        let _gate = match &group {
            Some(g) => Some(g.gate.lock().await),
            None => None,
        };
        let candidate = match &group {
            Some(g) => {
                self.process_failure(name, g);
                let mode = self.recovery_mode(name, g);
                match self.choose_group_locked(app, g, name, mode).await {
                    Ok(c) => Candidate::Replica(g.clone(), c),
                    Err(_) => return false,
                }
            }
            None => match self.direct_candidate(app, &key, revision, name).await {
                Ok(c) => Candidate::Direct(c),
                Err(_) => return false,
            },
        };
        // A successful catalog selection consumes the rotation. If this member
        // is on demand, viewer recovery must attach it rather than rotate twice.
        if let Some(group) = &group {
            self.selected_recovery(name, group);
        }
        let activation = match candidate {
            Candidate::Direct(c) => self.owner.activate(c).ok(),
            Candidate::Replica(g, c) => g.selector.activate(c).ok(),
        };
        let Some(activation) = activation else {
            return false;
        };
        let control = activation.control();
        let accepted = app
            .config
            .at_revision(revision, |_| {
                let mut state = self.activation.lock().unwrap();
                let Some(d) = state
                    .demands
                    .get_mut(name)
                    .filter(|d| Arc::ptr_eq(&d.epoch, &epoch))
                else {
                    return false;
                };
                if let Some(old) = d.control.replace(control.clone()) {
                    old.cancel();
                }
                true
            })
            .unwrap_or(false);
        if !accepted {
            control.cancel();
            return false;
        }
        let Ok(attachment) = self
            .inputs
            .attach_activation_configured(activation, &settings)
        else {
            return true;
        };
        let Ok(playback) = app.media.package_shared(attachment, &settings).await else {
            return true;
        };
        self.remember_static(name, &epoch, &playback);
        self.remember_recovery(name, &playback);
        if let Some(group) = &group {
            self.selected_recovery(name, group);
        }
        true
    }
    fn remember_static(&self, name: &str, epoch: &Arc<()>, playback: &PackagedPlayback) {
        // Source -> selected route -> static state. No state lock calls authority.
        let _ = playback.with_current_source(|| {
            if let Some(d) = self
                .activation
                .lock()
                .unwrap()
                .demands
                .get_mut(name)
                .filter(|d| Arc::ptr_eq(&d.epoch, epoch))
            {
                if playback.stats()["uptime"].as_u64().unwrap_or(0) >= 30 {
                    d.streak = 0;
                }
                d.playback = Some(playback.clone());
                d.next_attempt = Instant::now() + Duration::from_secs(1);
            }
        });
    }
    pub(crate) fn activation_status(&self, app: &App) -> Value {
        let revision = app.config.revision();
        let (mut status, playbacks) = {
            let state = self.activation.lock().unwrap();
            if state.revision != revision {
                return serde_json::json!({"wanted_streams":0,"active_streams":0,"capacity_limited":false,"sources":[]});
            }
            (
                serde_json::json!({"wanted_streams":state.demands.len(),"active_streams":0,"capacity_limited":state.capacity,"sources":state.catalogs.iter().map(|(key,c)|serde_json::json!({"source":key,"inventory_streams":c.names.len(),"lease_valid":c.expires.is_some_and(|t|t>Instant::now()),"last_error":c.error})).collect::<Vec<_>>()}),
                state
                    .demands
                    .values()
                    .filter_map(|d| d.playback.clone())
                    .collect::<Vec<_>>(),
            )
        };
        // Authority takes config/source locks, so never check it under state.
        status["active_streams"] =
            serde_json::json!(playbacks.iter().filter(|p| p.current().is_ok()).count());
        if app.config.revision() != revision {
            return serde_json::json!({"wanted_streams":0,"active_streams":0,"capacity_limited":false,"sources":[]});
        }
        status
    }
}

#[cfg(test)]
mod scheduling_tests;
