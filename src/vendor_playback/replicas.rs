//! Explicit configured groups, coalesced inventory and fenced native admission.
use super::*;
use crate::{
    playback_auth::{PlaybackAuth, ViewerRequest},
    source_directory::modern_coordinator::{
        AdmissionFailure, AdmissionOutcome, AdmittedCandidate, CoordinationFailure,
        ReplicaAdmissionOutcome, ReplicaChoice, ReplicaMember, ReplicaSelector,
    },
};
#[derive(Default)]
pub(super) struct Groups {
    pub revision: u64,
    pub groups: HashMap<String, Arc<Group>>,
}
pub(super) struct Group {
    pub selector: Arc<ReplicaSelector>,
    pub keys: Vec<String>,
    pub revision: u64,
    pub gate: Mutex<()>,
    spec: Value,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Normal,
    Rotate,
    Selected,
}
pub(super) enum Candidate {
    Direct(CoordinatedCandidate),
    Replica(Arc<Group>, ReplicaChoice),
}
impl Candidate {
    pub fn config_revision(&self) -> u64 {
        match self {
            Self::Direct(c) => c.config_revision(),
            Self::Replica(_, c) => c.config_revision(),
        }
    }
    pub fn source_key(&self) -> &str {
        match self {
            Self::Direct(c) => c.source_key(),
            Self::Replica(_, c) => c.source_key(),
        }
    }
}
struct GroupPlan {
    name: String,
    members: Vec<ReplicaMember>,
    spec: Value,
    names: std::collections::HashSet<String>,
    revision: u64,
}
impl Runtime {
    fn capture_group(app: &App, key: &str, revision: u64) -> Result<Option<GroupPlan>, Failure> {
        app.config.at_revision(revision, |root| {
            let Some(sources) = root["sources"].as_array() else { return Ok(None) };
            let Some(source) = sources.iter().find(|s| crate::config::item_key("sources",s) == Some(key)) else { return Ok(None) };
            let Some(name) = source["flussonix_source_group"].as_str() else { return Ok(None) };
            let rows = sources.iter().filter(|s| s["flussonix_source_kind"] == "flussonic" && s["flussonix_source_group"] == name).collect::<Vec<_>>();
            let members = rows.iter().map(|s| ReplicaMember {
                source_key: crate::config::item_key("sources",s).expect("validated source").into(),
                content_id: s["flussonix_content_id"].as_str().expect("validated identity").into(),
            }).collect();
            let spec = serde_json::json!({"sources":rows,"processing":crate::vendor_output::resolve(root,source).map_err(|_| Failure::Unavailable)?});
            let names = sources.iter().filter_map(|s| s["flussonix_source_group"].as_str().map(str::to_owned)).collect();
            Ok(Some(GroupPlan { name:name.into(), members, spec, names, revision }))
        }).ok_or(Failure::Unavailable)?
    }
    pub(super) fn group_for(
        &self,
        app: &App,
        key: &str,
        revision: u64,
    ) -> Result<Option<Arc<Group>>, Failure> {
        let Some(plan) = Self::capture_group(app, key, revision)? else {
            return Ok(None);
        };
        self.resolve_group(app, plan).map(Some)
    }
    fn resolve_group(&self, app: &App, plan: GroupPlan) -> Result<Arc<Group>, Failure> {
        let GroupPlan {
            name,
            members,
            spec,
            names,
            revision,
        } = plan;
        let (ready, previous) = app
            .config
            .at_revision(revision, |_| {
                let mut groups = self.groups.lock().map_err(|_| Failure::Unavailable)?;
                if groups.revision != revision {
                    for group in groups.groups.values() {
                        group.selector.shutdown();
                    }
                    groups.groups.retain(|name, _| names.contains(name));
                    groups.revision = revision;
                }
                let previous = groups.groups.get(&name).cloned();
                Ok((
                    previous.as_ref().is_some_and(|g| g.revision == revision),
                    previous,
                ))
            })
            .ok_or(Failure::Unavailable)??;
        if ready {
            return Ok(previous.expect("current cached group"));
        }
        let keys = members.iter().map(|m| m.source_key.clone()).collect();
        let selector = Arc::new(
            ReplicaSelector::new(self.owner.clone(), &name, members)
                .map_err(|_| Failure::Unavailable)?,
        );
        if let Some(old) = previous
            && old.spec == spec
        {
            selector.inherit_unchanged(&old.selector);
        }
        let group = Arc::new(Group {
            selector,
            keys,
            revision,
            gate: Mutex::new(()),
            spec,
        });
        app.config
            .at_revision(revision, |_| {
                let mut groups = self.groups.lock().map_err(|_| Failure::Unavailable)?;
                if groups.revision != revision {
                    return Err(Failure::Unavailable);
                }
                if let Some(existing) = groups.groups.get(&name)
                    && existing.revision == revision
                {
                    return Ok(existing.clone());
                }
                groups.groups.insert(name, group.clone());
                Ok(group)
            })
            .ok_or(Failure::Unavailable)?
    }
    pub(super) async fn candidate(
        &self,
        app: &App,
        key: &str,
        revision: u64,
        name: &str,
    ) -> Result<Candidate, Failure> {
        let Some(group) = self.group_for(app, key, revision)? else {
            return self
                .direct_candidate(app, key, revision, name)
                .await
                .map(Candidate::Direct);
        };
        let work = async {
            let _gate = group.gate.lock().await;
            self.process_failure(name, &group);
            let mode = self.recovery_mode(name, &group);
            let choice = self.choose_group_locked(app, &group, name, mode).await?;
            self.selected_recovery(name, &group);
            Ok(Candidate::Replica(group.clone(), choice))
        };
        tokio::select! {biased;_=self.closed.cancelled()=>Err(Failure::Closed),result=tokio::time::timeout(Duration::from_secs(4),work)=>result.unwrap_or(Err(Failure::Unavailable))}
    }
    pub(super) async fn admit_selection(
        &self,
        candidate: Candidate,
        auth: &PlaybackAuth,
        viewer: ViewerRequest,
    ) -> Result<AdmissionOutcome, AdmissionFailure> {
        match candidate {
            Candidate::Direct(c) => self.owner.admit(c, auth, viewer).await,
            Candidate::Replica(group, c) => Ok(
                match group
                    .selector
                    .admit(c, auth, viewer)
                    .await
                    .map_err(|_| AdmissionFailure::Revoked)?
                {
                    ReplicaAdmissionOutcome::Allowed(a) => {
                        AdmissionOutcome::Allowed((*a).into_native())
                    }
                    ReplicaAdmissionOutcome::Denied => AdmissionOutcome::Denied,
                    ReplicaAdmissionOutcome::Redirect(url) => AdmissionOutcome::Redirect(url),
                },
            ),
        }
    }
    async fn replica_inventory(
        &self,
        app: &App,
        key: &str,
        revision: u64,
        force: bool,
    ) -> Result<
        (
            SourceHandle,
            Result<Arc<CoordinatedInventory>, CoordinationFailure>,
        ),
        Failure,
    > {
        let slot = {
            let mut caches = self.caches.lock().await;
            if app.config.revision() != revision {
                return Err(Failure::Unavailable);
            }
            if caches.revision != revision {
                caches.sources.clear();
                caches.revision = revision;
            }
            if caches.sources.len() >= 64 && !caches.sources.contains_key(key) {
                return Err(Failure::Unavailable);
            }
            caches.sources.entry(key.into()).or_default().clone()
        };
        let mut cache = slot.lock().await;
        if !force
            && let (Some(handle), Some(rows)) = (&cache.handle, &cache.inventory)
            && self.owner.at_inventory_current(rows, || ()).is_ok()
        {
            return Ok((handle.clone(), Ok(rows.clone())));
        }
        if cache.next_probe.is_some_and(|t| t > Instant::now()) {
            return Err(Failure::Unavailable);
        }
        cache.next_probe = Some(Instant::now() + Duration::from_millis(500));
        let _permit = self.queries.acquire().await.map_err(|_| Failure::Closed)?;
        let handle = if let Some(h) = &cache.handle
            && self.owner.handle_current(h)
        {
            h.clone()
        } else {
            let captured = self.owner.capture(key).map_err(|_| Failure::Unavailable)?;
            if captured.config_revision() != revision {
                return Err(Failure::Unavailable);
            }
            let prepared = captured
                .prepare(Duration::from_millis(750))
                .map_err(|_| Failure::Unavailable)?;
            let handle = self
                .inputs
                .install_idle(prepared)
                .map_err(|_| Failure::Unavailable)?;
            cache.handle = Some(handle.clone());
            handle
        };
        let rows = if self.inputs.source_in_use(&handle) {
            self.inputs
                .renew(&handle, Duration::from_secs(30))
                .await
                .map(|r| r.inventory)
                .map_err(|e| match e {
                    crate::source_directory::modern_coordinator::SharedFailure::Authority(
                        AdmissionFailure::Coordination(e),
                    ) => e,
                    _ => CoordinationFailure::Unavailable,
                })
        } else {
            self.owner.inventory(&handle, Duration::from_secs(30)).await
        };
        let rows = rows.map(Arc::new);
        cache.inventory = rows.as_ref().ok().cloned();
        Ok((handle, rows))
    }
    pub(super) async fn choose_group_locked(
        &self,
        app: &App,
        group: &Arc<Group>,
        name: &str,
        mode: Mode,
    ) -> Result<ReplicaChoice, Failure> {
        if !self.running() || app.config.revision() != group.revision {
            return Err(Failure::Closed);
        }
        if mode == Mode::Normal
            && let Ok(candidate) = self.inputs.current_candidate(name)
            && let Ok(choice) = group.selector.choice_for(candidate, name)
        {
            return Ok(choice);
        }
        let mut lookup = group
            .selector
            .begin(name)
            .map_err(|_| Failure::Unavailable)?;
        let mut original = None;
        // Consult selected authority before probing alternatives. This detects
        // removal/disable/rejection even when only the private transport failed.
        let selected_key = lookup
            .next_source()
            .map_err(|_| Failure::Unavailable)?
            .ok_or(Failure::Unavailable)?;
        let selected_index = group
            .keys
            .iter()
            .position(|key| key == &selected_key)
            .ok_or(Failure::Unavailable)?;
        let (handle, rows) = self
            .replica_inventory(app, &selected_key, group.revision, mode != Mode::Normal)
            .await?;
        if let Some(choice) = lookup
            .observe_cached(&handle, rows.as_deref().map_err(|e| *e))
            .map_err(|_| Failure::Unavailable)?
        {
            if mode != Mode::Rotate {
                return Ok(choice);
            }
            choice
                .fail_current(false)
                .map_err(|_| Failure::Unavailable)?;
            original = rows.ok().map(|rows| (handle, rows));
            lookup = group
                .selector
                .begin_after_media(name)
                .map_err(|_| Failure::Unavailable)?;
        }
        if mode == Mode::Selected {
            return Err(Failure::Unavailable);
        }
        // At most four queries are active globally. Observe results in declared
        // rotation order, regardless of network completion order. Two waves
        // reach all eight members within the bounded single-group budget.
        use futures_util::{StreamExt, stream::FuturesUnordered};
        let mut queries = FuturesUnordered::new();
        for key in group
            .keys
            .iter()
            .cycle()
            .skip(selected_index + 1)
            .take(group.keys.len() - 1)
        {
            queries.push(async move {
                (
                    key.clone(),
                    self.replica_inventory(app, key, group.revision, false)
                        .await,
                )
            });
        }
        let mut observed = HashMap::new();
        while let Some((key, result)) = queries.next().await {
            observed.insert(key, result);
        }
        while let Some(key) = lookup.next_source().map_err(|_| Failure::Unavailable)? {
            let (handle, rows) = observed.remove(&key).ok_or(Failure::Unavailable)??;
            if let Some(choice) = lookup
                .observe_cached(&handle, rows.as_deref().map_err(|e| *e))
                .map_err(|_| Failure::Unavailable)?
            {
                return Ok(choice);
            }
        }
        // After bounded alternatives, a freshly validated original remains a
        // legitimate last retry. No extra fetch, stale import or denial bypass.
        if let Some((handle, rows)) = original {
            return group
                .selector
                .begin(name)
                .map_err(|_| Failure::Unavailable)?
                .observe_cached(&handle, Ok(&rows))
                .map_err(|_| Failure::Unavailable)?
                .ok_or(Failure::Unavailable);
        }
        Err(Failure::Unavailable)
    }
    pub(super) fn register_recovery<'a>(
        &'a self,
        app: &App,
        name: &str,
        key: &str,
        revision: u64,
        admission: &AdmittedCandidate,
        settings: &Value,
    ) -> Result<Option<super::recovery::Attempt<'a>>, Failure> {
        if app.options.role == "lb" {
            return Ok(None);
        }
        let Some(group) = self.group_for(app, key, revision)? else {
            return Ok(None);
        };
        self.register_anchor(name, group, admission, settings)
            .map(Some)
    }
}

impl Runtime {
    pub(super) fn cached_group(&self, app: &App, key: &str, revision: u64) -> Option<Arc<Group>> {
        let name=app.config.at_revision(revision,|root|root["sources"].as_array()?.iter().find(|s|crate::config::item_key("sources",s)==Some(key))?["flussonix_source_group"].as_str().map(str::to_owned))??;
        let groups = self.groups.try_lock().ok()?;
        if groups.revision != revision {
            return None;
        }
        groups
            .groups
            .get(&name)
            .filter(|g| g.revision == revision)
            .cloned()
    }
    pub(crate) fn annotate_stats(&self, app: &App, name: &str, stats: &mut Value) {
        let revision = app.config.revision();
        let Selection::Vendor { key, revision } = select(&app.config.snapshot(), name, revision)
        else {
            return;
        };
        let Some(group) = self.cached_group(app, &key, revision) else {
            return;
        };
        let Ok(status) = group.selector.status(name) else {
            return;
        };
        stats["upstream_source"] = serde_json::json!(status.selected_source);
        stats["source_group"] = serde_json::json!(status.group);
        stats["source_switches"] = serde_json::json!(status.switches);
        stats["source_available"] = serde_json::json!(
            status.available && !group.selector.private_rejected(name).unwrap_or(true)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_captured_group_cannot_retire_current_playback_selector() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            crate::server::Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-native-peer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        for key in ["m4s://one.invalid", "m4s://two.invalid"] {
            app.config.put("sources",key,serde_json::json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-group-key","on_play":"true","flussonix_source_group":"replicas","flussonix_content_id":"music"})).unwrap();
        }
        let owner =
            Arc::new(ModernSourceCoordinator::new(app.config.clone(), "owned-runtime").unwrap());
        let runtime = Runtime::new(owner);
        let key = "m4s://one.invalid";
        let old_revision = app.config.revision();
        let stale = Runtime::capture_group(&app, key, old_revision)
            .unwrap()
            .unwrap();
        app.config
            .put("templates", "unrelated", serde_json::json!({}))
            .unwrap();
        let revision = app.config.revision();
        let current = runtime.group_for(&app, key, revision).unwrap().unwrap();
        assert!(current.selector.begin("owned").is_ok());
        assert!(runtime.resolve_group(&app, stale).is_err());
        let next = runtime.group_for(&app, key, revision).unwrap().unwrap();
        assert!(
            next.selector.begin("owned").is_ok(),
            "stale captured request permanently closed the current selector"
        );
    }
}
