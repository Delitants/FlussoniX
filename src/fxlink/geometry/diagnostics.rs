//! Fixed, reservation-covered counters. Export allocation is bounded by N;
//! counters never retain wire buffers, media or peer identities.
use std::sync::atomic::{AtomicU64, Ordering};
pub(super) struct Counts<const N: usize>([AtomicU64; N]);
impl<const N: usize> Default for Counts<N> {
    fn default() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(0)))
    }
}
impl<const N: usize> Counts<N> {
    pub(super) fn add(&self, index: usize, amount: usize) {
        let counter = &self.0[index];
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            match counter.compare_exchange_weak(
                current,
                current.saturating_add(amount as u64),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
    pub(super) fn snapshot(&self, names: &[&str; N]) -> serde_json::Value {
        names
            .iter()
            .zip(&self.0)
            .map(|(name, count)| {
                (
                    (*name).into(),
                    serde_json::Value::from(count.load(Ordering::Relaxed)),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }
}
pub(super) const RECEIVED: [&str; 10] = [
    "received_application_bytes",
    "received_frames",
    "usable_entries",
    "ignored_entries",
    "replay_frames",
    "malformed_frames",
    "authority_conflicts",
    "contention_drops",
    "work_drops",
    "pressure_disable_events",
];
pub(super) const SENT: [&str; 8] = [
    "application_bytes",
    "attempted_modeled_bytes",
    "sent_frames",
    "sent_copies",
    "skipped_quota",
    "skipped_rate",
    "skipped_queue",
    "pressure_disable_events",
];
