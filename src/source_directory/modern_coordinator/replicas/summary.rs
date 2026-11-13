//! Bounded negative-only facts from an actual current source observation.
use super::*;
use crate::source_directory::flussonic_config::ModernFact;

#[derive(Clone)]
pub(super) struct NegativeSummary {
    // `None` is used only when an exact unchanged relationship is inherited.
    pub(super) source_generation: Option<Arc<()>>,
    pub(super) source_validity: Arc<AtomicBool>,
    pub(super) observation: Arc<()>,
    // `None` is a source-wide authoritative rejection; `Some` contains only
    // names that passed every existing local candidate projection gate.
    pub(super) eligible: Option<Arc<[String]>>,
}

impl NegativeSummary {
    pub(super) fn complete(inventory: &CoordinatedInventory) -> Self {
        let eligible = inventory.inventory.eligible_names();
        Self {
            source_generation: Some(inventory.handle.generation_token()),
            source_validity: inventory.handle.source_validity(),
            observation: inventory.inventory.observation_turn(),
            eligible: Some(eligible),
        }
    }

    pub(super) fn from_fact(handle: &SourceHandle, fact: ModernFact) -> Self {
        match fact {
            ModernFact::Eligible { observation, names } => Self {
                source_generation: Some(handle.generation_token()),
                source_validity: handle.source_validity(),
                observation,
                eligible: Some(names),
            },
            ModernFact::Rejected { observation } => Self {
                source_generation: Some(handle.generation_token()),
                source_validity: handle.source_validity(),
                observation,
                eligible: None,
            },
        }
    }

    pub(super) fn denies(&self, name: &str, source_generation: &Arc<()>) -> bool {
        if !self.source_validity.load(Ordering::Acquire) {
            return false;
        }
        if self
            .source_generation
            .as_ref()
            .is_some_and(|generation| !Arc::ptr_eq(generation, source_generation))
        {
            return false;
        }
        self.eligible.as_ref().is_none_or(|names| {
            names
                .binary_search_by(|candidate| candidate.as_str().cmp(name))
                .is_err()
        })
    }

    pub(super) fn inherited(&self) -> Option<Self> {
        if !self.source_validity.load(Ordering::Acquire) {
            return None;
        }
        let mut copy = self.clone();
        copy.source_generation = None;
        Some(copy)
    }
}

impl ReplicaSelector {
    pub(super) fn publish_summary(
        &self,
        selected: usize,
        summary: NegativeSummary,
    ) -> Result<(), ReplicaFailure> {
        let mut state = self.state()?;
        let slot = state
            .summaries
            .get_mut(selected)
            .ok_or(ReplicaFailure::Invalid)?;
        if slot.as_ref().is_some_and(|previous| {
            Arc::ptr_eq(&previous.observation, &summary.observation)
                && previous
                    .source_generation
                    .as_ref()
                    .zip(summary.source_generation.as_ref())
                    .is_some_and(|(a, b)| Arc::ptr_eq(a, b))
        }) {
            return Ok(());
        }
        *slot = Some(summary);
        Ok(())
    }
}
