//! Destination-wide identity, latest telemetry and short local egress holds.
//! Configuration revisions fence decisions; already-issued redirects keep their holds.
use super::*;
use crate::source_directory::flussonic::{modern::ObservedServerId, routing::Observation};
type ProfileKey = [u8; 32];
#[derive(Clone)]
pub(super) struct Stamp {
    profile: ProfileKey,
    remote: String,
    sequence: u64,
}
#[derive(Default)]
struct Destination {
    id: Option<ObservedServerId>,
    blocked: bool,
    completed: u64,
    latest: Option<(Instant, Value)>,
    routes: HashMap<String, u64>,
}
#[derive(Default)]
pub(super) struct Book {
    revision: u64,
    sequence: u64,
    destinations: HashMap<ProfileKey, Destination>,
    holds: HashMap<String, Vec<(Instant, f64)>>,
}
fn profile(peer: &Value) -> ProfileKey {
    use sha2::Digest;
    sha2::Sha256::digest(
        serde_json::to_vec(&json!([
            peer["api_url"],
            peer["cluster_key"],
            peer["flussonix_tls_ca"]
        ]))
        .expect("JSON values serialize"),
    )
    .into()
}
fn capacity_id(id: &ObservedServerId) -> String {
    uuid::Uuid::parse_str(id.as_str().trim_end_matches(['\r', '\n']))
        .map(|id| id.to_string())
        .unwrap_or_else(|_| id.as_str().to_owned())
}
impl App {
    fn vendor_http_book(&self, revision: u64) -> Option<std::sync::MutexGuard<'_, Book>> {
        let mut book = self.http_routes.vendor.lock().ok()?;
        if self.config.revision() != revision {
            return None;
        }
        if book.revision != revision {
            let active = self.config.read(|root| {
                root["peers"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|p| crate::config::vendor_peers::is_vendor(p))
                    .map(profile)
                    .collect::<std::collections::HashSet<_>>()
            });
            if self.config.revision() != revision {
                return None;
            }
            book.destinations.retain(|key, _| active.contains(key));
            book.revision = revision;
        }
        let now = Instant::now();
        book.holds.retain(|_, pending| {
            pending.retain(|(until, _)| *until > now);
            !pending.is_empty()
        });
        Some(book)
    }
    pub(super) fn begin_vendor_http(
        &self,
        peer: &Value,
        remote: &str,
        revision: u64,
    ) -> Option<Stamp> {
        let mut book = self.vendor_http_book(revision)?;
        let key = profile(peer);
        if book
            .destinations
            .get(&key)
            .is_some_and(|state| state.blocked)
        {
            return None;
        }
        book.sequence = book.sequence.checked_add(1)?;
        Some(Stamp {
            profile: key,
            remote: remote.into(),
            sequence: book.sequence,
        })
    }
    pub(super) fn record_vendor_http(
        &self,
        stamp: Stamp,
        when: Instant,
        result: Result<Observation, crate::source_directory::LookupFailure>,
        revision: u64,
    ) -> Option<(Stamp, Observation)> {
        let mut book = self.vendor_http_book(revision)?;
        let state = book.destinations.entry(stamp.profile).or_default();
        // An older request completing late cannot undo a newer failure or overload.
        if stamp.sequence < state.completed || state.blocked {
            return None;
        }
        state.completed = stamp.sequence;
        state.routes.insert(stamp.remote.clone(), stamp.sequence);
        state.latest = None;
        let observed = result.ok()?;
        if state.id.as_ref().is_some_and(|id| *id != observed.id) {
            state.blocked = true;
            return None;
        }
        state.id = Some(observed.id.clone());
        state.latest = Some((when, observed.value.clone()));
        Some((stamp, observed))
    }
    pub(super) fn http_load(
        &self,
        peer: &Value,
        snapshot: &http_balancer::Snapshot,
        ready: bool,
        elapsed: u64,
        cost: f64,
        revision: u64,
    ) -> Option<NodeLoad> {
        let id = peer["hostname"].as_str()?;
        if !crate::config::vendor_peers::is_vendor(peer) {
            return NodeLoad::from_telemetry(
                id,
                &snapshot.value,
                ready,
                peer["drain"] == true,
                elapsed,
                cost,
            );
        }
        let book = self.vendor_http_book(revision)?;
        book.load(peer, snapshot, ready, cost)
    }
    pub(super) fn hold_vendor_http(
        &self,
        peer: &Value,
        snapshot: &http_balancer::Snapshot,
        cost: f64,
        revision: u64,
    ) -> bool {
        let Some(mut book) = self.vendor_http_book(revision) else {
            return false;
        };
        let Some(load) = book.load(peer, snapshot, false, cost) else {
            return false;
        };
        if crate::cluster::select(&[load], 0.0).is_none() {
            return false;
        }
        let Some(stamp) = snapshot.vendor.as_ref() else {
            return false;
        };
        let Some(id) = book
            .destinations
            .get(&stamp.profile)
            .and_then(|state| state.id.as_ref())
            .map(capacity_id)
        else {
            return false;
        };
        if !book.holds.contains_key(&id) && book.holds.len() >= 64 {
            return false;
        }
        let pending = book.holds.entry(id).or_default();
        if pending.len() >= 10000 {
            return false;
        }
        pending.push((Instant::now() + Duration::from_secs(5), cost));
        true
    }
}
impl Book {
    fn load(
        &self,
        peer: &Value,
        snapshot: &http_balancer::Snapshot,
        ready: bool,
        cost: f64,
    ) -> Option<NodeLoad> {
        let stamp = snapshot.vendor.as_ref()?;
        if profile(peer) != stamp.profile {
            return None;
        }
        let state = self.destinations.get(&stamp.profile)?;
        if state.blocked || state.routes.get(&stamp.remote) != Some(&stamp.sequence) {
            return None;
        }
        let (when, mut value) = state.latest.clone()?;
        let pending = self.holds.get(&capacity_id(state.id.as_ref()?));
        value["reserved_mbps"] = json!(
            pending
                .into_iter()
                .flatten()
                .map(|(_, rate)| rate)
                .sum::<f64>()
        );
        value["reserved"] = json!(pending.map_or(0, Vec::len));
        let elapsed = when.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        NodeLoad::from_telemetry(
            peer["hostname"].as_str()?,
            &value,
            ready,
            peer["drain"] == true,
            elapsed,
            cost,
        )
    }
}
