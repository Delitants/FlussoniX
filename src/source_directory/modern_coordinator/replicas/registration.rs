use super::*;
use sha2::{Digest, Sha256};

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn filters(source: &Value) -> (Value, Vec<String>) {
    let mut exclusions: Vec<_> = source["except"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    exclusions.sort();
    exclusions.dedup();
    (source["prefix"].clone(), exclusions)
}
impl ReplicaSelector {
    /// Freeze explicit local equivalence against current config and startup identity.
    /// This first contract requires uniform policy across all possible memberships.
    /// Profile/TLS preparation is outside locks and performs no network requests.
    pub fn new(
        owner: Arc<ModernSourceCoordinator>,
        group: &str,
        members: Vec<ReplicaMember>,
    ) -> Result<Self, ReplicaFailure> {
        Self::build(owner, group, members, false)
    }
    /// Internal route fencing for exactly one source, without declaring replicas.
    /// No uniform cross-stream policy or content equivalence is implied.
    pub(crate) fn single_origin(
        owner: Arc<ModernSourceCoordinator>,
        source_key: &str,
    ) -> Result<Self, ReplicaFailure> {
        Self::build(
            owner,
            "single-origin",
            vec![ReplicaMember {
                source_key: source_key.into(),
                content_id: "single-origin".into(),
            }],
            true,
        )
    }
    fn build(
        owner: Arc<ModernSourceCoordinator>,
        group: &str,
        members: Vec<ReplicaMember>,
        single: bool,
    ) -> Result<Self, ReplicaFailure> {
        if !valid_id(group)
            || if single {
                members.len() != 1
            } else {
                !(2..=8).contains(&members.len())
            }
            || members
                .iter()
                .any(|m| !valid_id(&m.content_id) || m.content_id != members[0].content_id)
        {
            return Err(ReplicaFailure::Invalid);
        }
        let mut keys = HashSet::new();
        if members.iter().any(|m| !keys.insert(&m.source_key)) {
            return Err(ReplicaFailure::Invalid);
        }
        let (revision, identity, sources, backends, settings, relationship_lineages) =
            owner.config.read(|root| {
                let revision = owner.config.revision();
                let mut state = owner.state().map_err(ReplicaFailure::Coordination)?;
                if state.revision != revision {
                    state.sources.clear();
                    state.source_validities.retain(|key, validity| {
                        let present = root["sources"].as_array().is_some_and(|rows| {
                            rows.iter().any(|row| {
                                crate::config::item_key("sources", row) == Some(key.as_str())
                            })
                        });
                        if !present {
                            validity.store(false, Ordering::Release);
                        }
                        present
                    });
                    state.preparations.clear();
                    state.revision = revision;
                }
                let sources = members
                    .iter()
                    .map(|member| {
                        root["sources"]
                            .as_array()
                            .and_then(|rows| {
                                rows.iter().find(|s| {
                                    crate::config::item_key("sources", s)
                                        == Some(member.source_key.as_str())
                                })
                            })
                            .filter(|s| s["flussonix_source_kind"] == "flussonic")
                            .cloned()
                            .ok_or(ReplicaFailure::Invalid)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let relationship_lineages = members
                    .iter()
                    .map(|member| {
                        owner
                            .config
                            .source_lineage(&member.source_key)
                            .ok_or(ReplicaFailure::Invalid)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let settings = crate::vendor_output::resolve(root, &sources[0])
                    .map_err(|_| ReplicaFailure::Invalid)?;
                if sources.iter().any(|source| {
                    filters(source) != filters(&sources[0])
                        || crate::vendor_output::resolve(root, source).ok().as_ref()
                            != Some(&settings)
                }) {
                    return Err(ReplicaFailure::Invalid);
                }
                Ok((
                    revision,
                    state.identity.clone(),
                    sources,
                    root.get("auth_backends")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                    settings,
                    relationship_lineages,
                ))
            })?;
        let relationship: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&(&sources, &backends, &settings))
                .map_err(|_| ReplicaFailure::Invalid)?,
        )
        .into();
        let mut policy = None;
        for source in sources.into_iter().filter(|_| !single) {
            let source =
                crate::source_directory::flussonic_config::normalized_modern_source(&source)
                    .map_err(|_| ReplicaFailure::Invalid)?;
            let authority = SourceAuthority::new_modern(&source, &backends, Duration::from_secs(3))
                .map_err(|_| ReplicaFailure::Invalid)?;
            let current = authority
                .uniform_policy()
                .map_err(|_| ReplicaFailure::PolicyMismatch)?
                .ok_or(ReplicaFailure::PolicyMismatch)?;
            if policy.as_ref().is_some_and(|expected| expected != &current) {
                return Err(ReplicaFailure::PolicyMismatch);
            }
            policy = Some(current);
        }
        let member_count = members.len();
        let selector = Self {
            owner,
            identity,
            revision,
            relationship,
            relationship_lineages,
            group: group.to_owned(),
            members,
            policy,
            state: Mutex::new(State {
                summaries: vec![None; member_count],
                ..State::default()
            }),
        };
        selector.at_plan(|| Ok(()))?;
        Ok(selector)
    }
}
