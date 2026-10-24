//! Managed route fencing is independent of the source's renewable lease ticket.
use super::*;
impl ReplicaSelector {
    /// Preserve only trusted local diagnostics/tombstones for an unchanged
    /// relationship. No old input, lease, admission or route epoch is imported.
    pub(crate) fn inherit_unchanged(&self, old: &Self) -> bool {
        if self.group != old.group
            || self.policy != old.policy
            || self.relationship != old.relationship
            || self.relationship_lineages.len() != old.relationship_lineages.len()
            || self
                .relationship_lineages
                .iter()
                .zip(&old.relationship_lineages)
                .any(|(a, b)| !Arc::ptr_eq(a, b))
            || !Arc::ptr_eq(&self.owner, &old.owner)
            || !Arc::ptr_eq(&self.identity, &old.identity)
            || self.members.len() != old.members.len()
            || self
                .members
                .iter()
                .zip(&old.members)
                .any(|(a, b)| a.source_key != b.source_key || a.content_id != b.content_id)
        {
            return false;
        }
        let Ok(previous) = old.state.lock() else {
            return false;
        };
        if previous.permanent_shutdown {
            return false;
        }
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        state.entries = previous
            .entries
            .iter()
            .filter(|(_, entry)| entry.committed)
            .map(|(name, e)| {
                (
                    name.clone(),
                    Entry {
                        identity: Arc::new(AtomicBool::new(true)),
                        committed: true,
                        selected: e.selected,
                        available: false,
                        blocked: e.blocked,
                        private_rejected: e.private_rejected,
                        pending_failure: None,
                        switches: e.switches,
                        turn: Arc::new(()),
                        route: Epoch::new(),
                    },
                )
            })
            .collect();
        state.summaries = previous
            .summaries
            .iter()
            .map(|summary| summary.as_ref().and_then(NegativeSummary::inherited))
            .collect();
        true
    }
    pub(crate) fn choice_for(
        self: &Arc<Self>,
        candidate: CoordinatedCandidate,
        name: &str,
    ) -> Result<ReplicaChoice, ReplicaFailure> {
        let stamp = self
            .owner
            .publish(&candidate, |view, _| {
                if view.name != name || !self.accepts_policy(view.policy) {
                    return Err(ReplicaFailure::PolicyMismatch);
                }
                let state = self.state()?;
                let entry = state.entries.get(name).ok_or(ReplicaFailure::Stale)?;
                if !entry.available
                    || entry.blocked
                    || entry.route.cancel.is_cancelled()
                    || candidate.source_key() != self.members[entry.selected].source_key
                {
                    return Err(ReplicaFailure::Stale);
                }
                Ok(RouteStamp {
                    selector: self.clone(),
                    name: name.into(),
                    identity: entry.identity.clone(),
                    selected: entry.selected,
                    route: entry.route.clone(),
                })
            })
            .map_err(ReplicaFailure::Coordination)??;
        Ok(ReplicaChoice { candidate, stamp })
    }
    pub(crate) fn fail_current(
        &self,
        candidate: &CoordinatedCandidate,
        route: &ReplicaRoute,
        blocked: bool,
    ) -> Result<(), ReplicaFailure> {
        if !std::ptr::eq(self, Arc::as_ptr(&route.stamp.selector)) {
            return Err(ReplicaFailure::Stale);
        }
        self.owner
            .publish(candidate, |view, _| {
                if !self.accepts_policy(view.policy)
                    || candidate.source_key() != self.members[route.stamp.selected].source_key
                {
                    return Err(ReplicaFailure::PolicyMismatch);
                }
                let mut state = self.state()?;
                let entry = state
                    .entries
                    .get_mut(&route.stamp.name)
                    .ok_or(ReplicaFailure::Stale)?;
                if !entry.available
                    || entry.selected != route.stamp.selected
                    || !Arc::ptr_eq(&entry.route, &route.stamp.route)
                    || entry.route.cancel.is_cancelled()
                {
                    return Err(ReplicaFailure::Stale);
                }
                entry.available = false;
                entry.blocked |= blocked;
                entry.route.cancel.cancel();
                entry.turn = Arc::new(());
                Ok(())
            })
            .map_err(ReplicaFailure::Coordination)?
    }
    pub(crate) fn begin_after_media(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<ReplicaLookup, ReplicaFailure> {
        let mut lookup = self.begin(name)?;
        lookup.offset = 1;
        Ok(lookup)
    }
}
impl ReplicaChoice {
    pub(crate) fn config_revision(&self) -> u64 {
        self.candidate.config_revision()
    }
}

impl ReplicaChoice {
    pub(crate) fn into_managed(self) -> (CoordinatedCandidate, ReplicaRoute) {
        (self.candidate, ReplicaRoute { stamp: self.stamp })
    }
    pub(crate) fn fail_current(&self, blocked: bool) -> Result<(), ReplicaFailure> {
        self.stamp.selector.fail_current(
            &self.candidate,
            &ReplicaRoute {
                stamp: self.stamp.clone(),
            },
            blocked,
        )
    }
}

impl ReplicaChoice {
    pub(crate) fn publish_policy(&self, auth: &PlaybackAuth) -> bool {
        self.stamp
            .selector
            .owner
            .publish(&self.candidate, |view, _| {
                self.stamp.selector.at_stamp(&self.stamp, || {
                    auth.publish(view.name, Some(view.policy.clone())).is_some()
                })
            })
            .is_ok_and(|result| result.unwrap_or(false))
    }
}
impl super::ReplicaRoute {
    pub(crate) fn activate(
        self,
        candidate: CoordinatedCandidate,
    ) -> Result<super::super::ActivatedCandidate, ReplicaFailure> {
        let owner = self.stamp.selector.owner.clone();
        owner
            .activate_scoped(candidate, Some(self))
            .map_err(ReplicaFailure::Admission)
    }
}

impl super::ReplicaRoute {
    /// Pure identity comparison: attachment already holds the current scope fence.
    pub(crate) fn same_route(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.stamp.selector, &other.stamp.selector)
            && self.stamp.name == other.stamp.name
            && Arc::ptr_eq(&self.stamp.identity, &other.stamp.identity)
            && self.stamp.selected == other.stamp.selected
            && Arc::ptr_eq(&self.stamp.route, &other.stamp.route)
    }
}

#[cfg(test)]
mod inheritance_tests {
    use super::*;
    use crate::config::ConfigStore;

    fn rss_kb() -> Option<usize> {
        std::fs::read_to_string("/proc/self/status")
            .ok()?
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
    }

    fn setup() -> (
        tempfile::TempDir,
        Arc<ConfigStore>,
        Arc<ModernSourceCoordinator>,
        Vec<ReplicaMember>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
        let members = ["one", "two"]
            .into_iter()
            .map(|host| {
                let key = format!("m4s://{host}.invalid");
                config.put("sources", &key, json!({
                "flussonix_source_kind":"flussonic", "cluster_key":"owned-relationship-key",
                "on_play":"true", "flussonix_source_group":"replicas",
                "flussonix_content_id":"music"
            })).unwrap();
                ReplicaMember {
                    source_key: key,
                    content_id: "music".into(),
                }
            })
            .collect();
        let owner = Arc::new(ModernSourceCoordinator::new(config.clone(), "owned-local").unwrap());
        (dir, config, owner, members)
    }

    #[test]
    fn unrelated_save_allows_transfer_but_explicit_shutdown_does_not() {
        let (_dir, config, owner, members) = setup();
        let old = ReplicaSelector::new(owner.clone(), "replicas", members.clone()).unwrap();
        config.put("templates", "unrelated", json!({})).unwrap();
        let successor = ReplicaSelector::new(owner.clone(), "replicas", members.clone()).unwrap();
        assert!(successor.inherit_unchanged(&old));
        old.shutdown();
        let after_shutdown = ReplicaSelector::new(owner, "replicas", members).unwrap();
        assert!(!after_shutdown.inherit_unchanged(&old));
    }

    #[test]
    fn unchanged_rebind_copies_only_committed_history_with_fresh_route_epochs() {
        let (_dir, config, owner, members) = setup();
        let old =
            Arc::new(ReplicaSelector::new(owner.clone(), "replicas", members.clone()).unwrap());
        let temporary = old.begin("temporary").unwrap();
        let admitted = old.begin("admitted").unwrap();
        {
            let mut state = old.state().unwrap();
            let entry = state.entries.get_mut("admitted").unwrap();
            entry.committed = true;
            entry.selected = 1;
            entry.private_rejected = true;
            entry.switches = 3;
        }
        let old_route = old.state().unwrap().entries["admitted"]
            .route
            .cancel
            .clone();
        config
            .put("templates", "unrelated-history-save", json!({}))
            .unwrap();
        let successor = ReplicaSelector::new(owner, "replicas", members).unwrap();
        assert!(successor.inherit_unchanged(&old));
        let state = successor.state().unwrap();
        assert!(!state.entries.contains_key("temporary"));
        let entry = &state.entries["admitted"];
        assert!(entry.committed);
        assert_eq!(entry.selected, 1);
        assert!(entry.private_rejected);
        assert_eq!(entry.switches, 3);
        assert!(!entry.available);
        assert!(!entry.route.cancel.is_cancelled());
        drop(state);
        old.retire_for_revision();
        assert!(old_route.is_cancelled());
        assert!(
            !successor.state().unwrap().entries["admitted"]
                .route
                .cancel
                .is_cancelled()
        );
        drop((temporary, admitted));
        assert_eq!(successor.state().unwrap().entries.len(), 1);
    }

    #[test]
    fn credentials_namespace_and_exclusions_break_relationship_transfer() {
        for (field, value) in [
            ("cluster_key", json!("new-private-key")),
            ("prefix", json!("different")),
            ("except", json!(["excluded"])),
        ] {
            let (_dir, config, owner, members) = setup();
            let old = ReplicaSelector::new(owner.clone(), "replicas", members.clone()).unwrap();
            let mut root = config.snapshot();
            for source in root["sources"].as_array_mut().unwrap() {
                source[field] = value.clone();
            }
            config.replace(root).unwrap();
            let successor = ReplicaSelector::new(owner, "replicas", members).unwrap();
            assert!(!successor.inherit_unchanged(&old), "{field} changed");
        }
    }

    #[test]
    fn delete_and_readd_of_identical_source_breaks_relationship_transfer() {
        let (_dir, config, owner, members) = setup();
        let old = ReplicaSelector::new(owner.clone(), "replicas", members.clone()).unwrap();
        let key = &members[0].source_key;
        let original = config.snapshot()["sources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["url"] == *key)
            .unwrap()
            .clone();
        assert!(config.delete("sources", key).unwrap());
        config.put("sources", key, original).unwrap();
        let successor = ReplicaSelector::new(owner, "replicas", members).unwrap();
        assert!(!successor.inherit_unchanged(&old));
    }

    #[test]
    fn twenty_thousand_temporary_names_reclaim_under_competing_callers() {
        let (_dir, _config, owner, members) = setup();
        let selector = Arc::new(ReplicaSelector::new(owner, "replicas", members).unwrap());
        let initial_rss = rss_kb();
        let started = std::time::Instant::now();
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let selector = selector.clone();
                scope.spawn(move || {
                    for index in 0..2_504 {
                        drop(
                            selector
                                .begin(&format!("temporary-{worker}-{index}"))
                                .unwrap(),
                        );
                    }
                });
            }
        });
        assert_eq!(selector.state().unwrap().entries.len(), 0);
        eprintln!(
            "20,032 temporary names across 8 callers: {:?}; retained entries: 0; RSS before/after (KiB): {:?}/{:?}",
            started.elapsed(),
            initial_rss,
            rss_kb()
        );
    }
}
