//! Explicit local relay assertions; not attested remote topology or daemon routes.
use super::{
    CoordinatedCandidate, CoordinatedInventory, CoordinationFailure, ModernSourceCoordinator,
};
use crate::{
    m4_ingest::flussonic::MediaInput,
    source_directory::{
        flussonic::{StreamProvenance, modern::ObservedServerId},
        flussonic_config::{CandidateView, LeaseFailure},
    },
};
use std::{collections::HashMap, sync::Arc, time::Instant};
const MAX_HOPS: usize = 8;
const MAX_ROUTES: usize = 64;
/// Generic diagnostics never contain a configured value or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayFailure {
    Invalid,
    Loop,
    Missing,
    Identity,
    Role,
    Coordination(CoordinationFailure),
}
fn coordination(error: CoordinationFailure) -> RelayFailure {
    RelayFailure::Coordination(error)
}
fn lease(error: LeaseFailure) -> RelayFailure {
    coordination(CoordinationFailure::Lease(error))
}
/// Trusted local declaration. No remote metadata can create or change an edge.
/// Fields are immutable and deliberately nonserializable and non-Debug.
pub struct RelayHop {
    source: String,
    id: ObservedServerId,
    name: String,
}
/// Bounded, acyclic declared path. This does not attest actual remote inputs.
pub struct RelayRoute {
    hops: Vec<RelayHop>,
}
#[derive(Default)]
pub(super) struct RelayRoutes {
    routes: HashMap<String, RelayRoute>,
}
/// Opaque all-hop observation. No intermediate candidate or media is exported.
pub struct RelayCandidate {
    generation: Arc<RelayRoutes>,
    hops: Vec<CoordinatedCandidate>,
}
impl RelayHop {
    /// Exact configured source key, expected management claim and literal name.
    /// Identity comparison preserves custom text and recognizes UUID aliases.
    pub fn new(source: &str, expected_id: &str, name: &str) -> Result<Self, RelayFailure> {
        if source.is_empty()
            || source.len() > 4096
            || source.chars().any(|c| c.is_control() || c.is_whitespace())
        {
            return Err(RelayFailure::Invalid);
        }
        let id = ObservedServerId::parse(expected_id).map_err(|_| RelayFailure::Invalid)?;
        crate::config::valid_name(name).map_err(|_| RelayFailure::Invalid)?;
        Ok(Self {
            source: source.into(),
            id,
            name: name.into(),
        })
    }
}
impl RelayRoute {
    /// Two to eight unique source/identity hops. Last hop is the declared origin;
    /// each earlier hop must later report remote provenance in bound inventory.
    pub fn new(hops: Vec<RelayHop>) -> Result<Self, RelayFailure> {
        if !(2..=MAX_HOPS).contains(&hops.len()) {
            return Err(RelayFailure::Invalid);
        }
        for (i, hop) in hops.iter().enumerate() {
            if hops[..i]
                .iter()
                .any(|earlier| earlier.source == hop.source || earlier.id == hop.id)
            {
                return Err(RelayFailure::Loop);
            }
        }
        Ok(Self { hops })
    }
}
impl RelayCandidate {
    /// Untrusted management claim of the explicitly declared terminal source.
    pub fn origin_id(&self) -> &ObservedServerId {
        self.hops.last().expect("validated path").server_id()
    }
    /// Minimum start-based local deadline. Inspecting it never renews any hop.
    pub fn expires_at(&self) -> Instant {
        self.hops
            .iter()
            .map(|h| h.expires_at())
            .min()
            .expect("validated path")
    }
    pub fn hop_count(&self) -> usize {
        self.hops.len()
    }
}
impl ModernSourceCoordinator {
    /// Atomically replace up to 64 explicit local paths. Entry names are unique.
    /// Accepted equal edits and removal invalidate relay candidates only. Invalid
    /// edits change nothing. No persistence, remote edge inference or routing.
    pub fn replace_relay_routes(&self, routes: Vec<RelayRoute>) -> Result<(), RelayFailure> {
        if routes.len() > MAX_ROUTES {
            return Err(RelayFailure::Invalid);
        }
        let mut by_name = HashMap::new();
        for route in routes {
            if by_name.insert(route.hops[0].name.clone(), route).is_some() {
                return Err(RelayFailure::Invalid);
            }
        }
        let mut state = self.state().map_err(coordination)?;
        if by_name
            .values()
            .flat_map(|r| &r.hops)
            .any(|hop| state.identity.ids.contains(&hop.id))
        {
            return Err(RelayFailure::Loop);
        }
        state.routes = Arc::new(RelayRoutes { routes: by_name });
        Ok(())
    }
    /// Bind an installed local assertion to exactly ordered complete inventories.
    /// Current installed source ownership and pinned claims are required. No I/O
    /// or lease renewal occurs; publication still rechecks every retained fence.
    pub fn relay_candidate(
        &self,
        name: &str,
        inventories: &[&CoordinatedInventory],
    ) -> Result<RelayCandidate, RelayFailure> {
        let generation = self.state().map_err(coordination)?.routes.clone();
        let route = generation.routes.get(name).ok_or(RelayFailure::Missing)?;
        if inventories.len() != route.hops.len() {
            return Err(RelayFailure::Invalid);
        }
        let mut hops = Vec::with_capacity(route.hops.len());
        for (i, (declared, observed)) in route.hops.iter().zip(inventories).enumerate() {
            self.at_handle(&observed.handle, || Ok(()))
                .map_err(coordination)?;
            if observed.handle.stamp.key != declared.source {
                return Err(RelayFailure::Invalid);
            }
            if observed.server_id() != &declared.id {
                return Err(RelayFailure::Identity);
            }
            let intermediate = i + 1 < route.hops.len();
            let provenance = observed
                .inventory
                .path_provenance(&declared.name)
                .map_err(lease)?;
            if if intermediate {
                provenance != Some(StreamProvenance::Remote)
            } else {
                !matches!(
                    provenance,
                    Some(StreamProvenance::Config | StreamProvenance::User)
                )
            } {
                return Err(RelayFailure::Role);
            }
            let candidate = if intermediate {
                observed.inventory.relay_candidate(&declared.name)
            } else {
                observed.inventory.candidate(&declared.name)
            }
            .map_err(lease)?;
            hops.push(CoordinatedCandidate {
                handle: observed.handle.clone(),
                candidate,
            });
        }
        Ok(RelayCandidate { generation, hops })
    }
    /// Hold config -> coordinator -> every unique source (sorted key) through one
    /// synchronous publication. Check all profiles/turns/leases, construct only
    /// entry media, then check all deadlines immediately before closure entry.
    /// Never await/reenter these owners. Exports need fresh async fences; this
    /// grants neither viewer admission nor attested remote topology/ownership.
    pub fn publish_relay<T>(
        &self,
        candidate: &RelayCandidate,
        commit: impl FnOnce(CandidateView<'_>, MediaInput) -> T,
    ) -> Result<T, RelayFailure> {
        let revision = candidate.hops[0].handle.stamp.revision;
        self.config
            .at_revision(revision, |_| {
                let state = self.state().map_err(coordination)?;
                if !Arc::ptr_eq(&state.routes, &candidate.generation) {
                    return Err(coordination(CoordinationFailure::Stale));
                }
                for hop in &candidate.hops {
                    self.check(&hop.handle.stamp, &state)
                        .map_err(coordination)?;
                    if !state
                        .sources
                        .get(&hop.handle.stamp.key)
                        .is_some_and(|source| Arc::ptr_eq(source, &hop.handle.authority))
                    {
                        return Err(coordination(CoordinationFailure::Stale));
                    }
                }
                let mut order = (0..candidate.hops.len()).collect::<Vec<_>>();
                order.sort_by(|a, b| {
                    candidate.hops[*a]
                        .handle
                        .stamp
                        .key
                        .cmp(&candidate.hops[*b].handle.stamp.key)
                });
                let mut guards = Vec::with_capacity(order.len());
                for &index in &order {
                    let hop = &candidate.hops[index];
                    guards.push(
                        hop.handle
                            .authority
                            .guard_modern_lease(&hop.candidate)
                            .map_err(lease)?,
                    );
                }
                let entry = order
                    .iter()
                    .position(|index| *index == 0)
                    .expect("entry in validated path");
                let (view, input) = guards[entry].publication().map_err(lease)?;
                for guard in &guards {
                    guard.check().map_err(lease)?;
                }
                if Instant::now() >= candidate.expires_at() {
                    return Err(lease(LeaseFailure::Expired));
                }
                let result = commit(view, input);
                drop(guards);
                drop(state);
                Ok(result)
            })
            .ok_or_else(|| coordination(CoordinationFailure::Stale))?
    }
}
