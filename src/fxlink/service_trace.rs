//! Opt-in, bounded diagnostics. Never feeds a transport decision or creates a wake.
use super::{
    Source,
    service_timing::{PHASES, Pending, Phase, Selected, Timeline},
};
use std::time::Instant;
const TARGET: &str = "flussonix::fxlink::service_timing";
pub(super) struct Probe {
    connection: usize,
    subscription: u64,
    generation: u64,
    pending_id: Option<usize>,
    began: Instant,
    report_at: u64,
    timeline: Timeline,
}
impl Probe {
    pub(super) fn new(
        connection: usize,
        subscription: u64,
        generation: u64,
        source: &Source,
    ) -> Option<Self> {
        if !tracing::enabled!(target: TARGET, tracing::Level::TRACE) {
            return None;
        }
        Some(Self {
            connection,
            subscription,
            generation,
            pending_id: source
                .pending_records
                .as_ref()
                .map(|p| std::sync::Arc::as_ptr(p) as *const () as usize),
            began: Instant::now(),
            report_at: 100_000,
            timeline: Timeline::new(0),
        })
    }
    fn elapsed(&self) -> u64 {
        self.began.elapsed().as_micros().min(u64::MAX as u128) as u64
    }
    pub(super) fn mark(&mut self, phase: Phase) {
        self.timeline.mark(self.elapsed(), phase);
    }
    pub(super) fn observe(&mut self, at_ms: u64) {
        self.timeline.observe_retry(at_ms);
        self.mark(Phase::Housekeeping);
        if self.elapsed() >= self.report_at {
            self.report(false);
            self.report_at = self.elapsed().saturating_add(100_000);
        }
    }
    pub(super) fn wait(&mut self, credit: bool, queue: bool, source: &Source, retry: Option<u64>) {
        let pending = match source.pending_records.as_ref().map(|p| p()) {
            None => Pending::Unknown,
            Some(None) => Pending::Retired,
            Some(Some(false)) => Pending::Idle,
            Some(Some(true)) => Pending::Ready,
        };
        self.timeline
            .wait(self.elapsed(), credit, queue, pending, retry);
    }
    pub(super) fn wake(&mut self, selected: Selected) {
        self.timeline
            .wake(self.elapsed(), super::transport::now(), selected);
    }
    fn report(&self, final_report: bool) {
        tracing::trace!(target: TARGET, connection=self.connection,subscription=self.subscription,generation=self.generation,pending_id=?self.pending_id,final_report,elapsed_us=self.timeline.elapsed(),phase_names=?PHASES,phase_us=?self.timeline.phase_us,wait_gate_names=?["open","byte","credit","both"],wait_us=?self.timeline.wait_us,selected_names=?["cancel","source_cancel","drain_retry","tick","feedback","record"],selected=?self.timeline.selected,pending_names=?["unknown","idle","ready","retired"],pending_samples=?self.timeline.pending_samples,max_retry_observation_late_ms=self.timeline.max_retry_observation_late_ms,max_retry_selected_late_ms=self.timeline.max_retry_selected_late_ms,"FXLink service timing");
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.mark(Phase::Housekeeping);
        self.report(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[test]
    fn disabled_probe_does_not_sample_source_or_validity_callbacks() {
        let calls = Arc::new(AtomicUsize::new(0));
        let called = calls.clone();
        let source = Source {
            records: Box::pin(futures_util::stream::empty()),
            pending_records: Some(Arc::new(move || {
                called.fetch_add(1, Ordering::Relaxed);
                Some(true)
            })),
            current: Arc::new(|| panic!("diagnostic must not call authority")),
            cancelled: tokio_util::sync::CancellationToken::new(),
        };
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            assert!(Probe::new(1, 2, 1, &source).is_none())
        });
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
