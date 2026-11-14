//! Fixed-size elapsed accounting. Wait labels describe a sample, not continuous blocking.
#[derive(Clone, Copy, Debug)]
pub(super) enum Phase {
    Housekeeping,
    ByteGate,
    CreditGate,
    BothGates,
    ReadyObserved,
    IdleObserved,
    Unknown,
    RetiredObserved,
    Control,
    Repair,
    Source,
    Encode,
    Announce,
    Data,
    Parity,
}
pub(super) const PHASES: [&str; 15] = [
    "housekeeping",
    "byte_gate",
    "credit_gate",
    "both_gates",
    "ready_observed",
    "idle_observed",
    "unknown",
    "retired_observed",
    "control",
    "repair",
    "source",
    "encode",
    "announce",
    "data",
    "parity",
];
#[derive(Clone, Copy, Debug)]
pub(super) enum Pending {
    Unknown,
    Idle,
    Ready,
    Retired,
}
#[derive(Clone, Copy, Debug)]
pub(super) enum Selected {
    Cancel,
    SourceCancel,
    DrainRetry,
    Tick,
    Feedback,
    Record,
}
#[derive(Debug)]
pub(super) struct Timeline {
    pub phase_us: [u64; 15],
    pub wait_us: [[u64; 4]; 4],
    pub selected: [u64; 6],
    pub pending_samples: [u64; 4],
    pub max_retry_observation_late_ms: u64,
    pub max_retry_selected_late_ms: u64,
    phase: Phase,
    started: u64,
    last: u64,
    retry_due_ms: Option<u64>,
    active_wait: Option<(usize, usize)>,
}
impl Timeline {
    pub(super) fn new(at: u64) -> Self {
        Self {
            phase_us: [0; 15],
            wait_us: [[0; 4]; 4],
            selected: [0; 6],
            pending_samples: [0; 4],
            max_retry_observation_late_ms: 0,
            max_retry_selected_late_ms: 0,
            phase: Phase::Housekeeping,
            started: at,
            last: at,
            retry_due_ms: None,
            active_wait: None,
        }
    }
    pub(super) fn mark(&mut self, at: u64, phase: Phase) {
        if let Some((gate, pending)) = self.active_wait.take() {
            self.wait_us[gate][pending] =
                self.wait_us[gate][pending].saturating_add(at.saturating_sub(self.last));
        }
        self.phase_us[self.phase as usize] =
            self.phase_us[self.phase as usize].saturating_add(at.saturating_sub(self.last));
        self.last = at;
        self.phase = phase;
    }
    pub(super) fn wait(
        &mut self,
        at: u64,
        credit: bool,
        queue: bool,
        pending: Pending,
        retry: Option<u64>,
    ) {
        self.pending_samples[pending as usize] =
            self.pending_samples[pending as usize].saturating_add(1);
        self.retry_due_ms = retry;
        let phase = match (credit, queue, pending) {
            (false, false, _) => Phase::BothGates,
            (false, true, _) => Phase::CreditGate,
            (true, false, _) => Phase::ByteGate,
            (true, true, Pending::Unknown) => Phase::Unknown,
            (true, true, Pending::Idle) => Phase::IdleObserved,
            (true, true, Pending::Ready) => Phase::ReadyObserved,
            (true, true, Pending::Retired) => Phase::RetiredObserved,
        };
        self.mark(at, phase);
        self.active_wait = Some((
            usize::from(!credit) * 2 + usize::from(!queue),
            pending as usize,
        ));
    }
    pub(super) fn observe_retry(&mut self, at_ms: u64) {
        if let Some(due) = self.retry_due_ms {
            self.max_retry_observation_late_ms = self
                .max_retry_observation_late_ms
                .max(at_ms.saturating_sub(due));
            self.retry_due_ms = None;
        }
    }
    pub(super) fn wake(&mut self, at: u64, at_ms: u64, selected: Selected) {
        self.selected[selected as usize] = self.selected[selected as usize].saturating_add(1);
        if matches!(selected, Selected::DrainRetry)
            && let Some(due) = self.retry_due_ms
        {
            self.max_retry_selected_late_ms = self
                .max_retry_selected_late_ms
                .max(at_ms.saturating_sub(due));
        }
        self.mark(
            at,
            match selected {
                Selected::Feedback => Phase::Control,
                Selected::Record => Phase::Source,
                _ => Phase::Housekeeping,
            },
        );
    }
    pub(super) fn elapsed(&self) -> u64 {
        self.last - self.started
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn elapsed_partition_counts_work_and_sampled_wait_once() {
        let mut t = Timeline::new(10);
        t.wait(30, false, true, Pending::Ready, None);
        t.wake(70, 0, Selected::Feedback);
        t.mark(90, Phase::Repair);
        t.mark(120, Phase::Housekeeping);
        assert_eq!(t.phase_us[Phase::Housekeeping as usize], 20);
        assert_eq!(t.phase_us[Phase::CreditGate as usize], 40);
        assert_eq!(t.phase_us[Phase::Control as usize], 20);
        assert_eq!(t.phase_us[Phase::Repair as usize], 30);
        assert_eq!(t.phase_us.iter().sum::<u64>(), 110);
        assert_eq!(t.elapsed(), 110);
    }
    #[test]
    fn combined_gates_do_not_double_count_and_unknown_is_not_idle() {
        let mut t = Timeline::new(0);
        for (at, credit, queue, pending) in [
            (0, false, false, Pending::Ready),
            (10, true, false, Pending::Ready),
            (20, true, true, Pending::Unknown),
            (30, true, true, Pending::Idle),
            (40, true, true, Pending::Retired),
        ] {
            t.wait(at, credit, queue, pending, None);
        }
        t.mark(50, Phase::Housekeeping);
        for phase in [
            Phase::BothGates,
            Phase::ByteGate,
            Phase::Unknown,
            Phase::IdleObserved,
            Phase::RetiredObserved,
        ] {
            assert_eq!(t.phase_us[phase as usize], 10);
        }
        assert_eq!(t.phase_us.iter().sum::<u64>(), 50);
        assert_eq!(t.pending_samples, [1, 1, 2, 1]);
    }
    #[test]
    fn ready_record_does_not_relabel_prior_idle_observation() {
        let mut t = Timeline::new(0);
        t.wait(0, true, true, Pending::Idle, None);
        t.wake(20, 0, Selected::Record);
        t.mark(25, Phase::Encode);
        assert_eq!(t.phase_us[Phase::IdleObserved as usize], 20);
        assert_eq!(t.phase_us[Phase::Source as usize], 5);
        assert_eq!(t.selected[Selected::Record as usize], 1);
    }
    #[test]
    fn previous_due_observation_survives_rearmed_retry_and_other_selection() {
        let mut t = Timeline::new(0);
        t.wait(0, true, false, Pending::Ready, Some(5));
        t.observe_retry(12);
        t.wait(12000, true, false, Pending::Ready, Some(14));
        t.wake(17000, 17, Selected::DrainRetry);
        assert_eq!(t.max_retry_observation_late_ms, 7);
        assert_eq!(t.max_retry_selected_late_ms, 3);
        t.wait(18000, true, false, Pending::Ready, Some(20));
        t.wake(30000, 30, Selected::Feedback);
        assert_eq!(
            t.max_retry_selected_late_ms, 3,
            "feedback selection is not a retry timer wake"
        );
    }
    #[test]
    fn sampled_backlog_conditions_gate_wait_without_charging_work() {
        let mut t = Timeline::new(0);
        t.wait(0, true, false, Pending::Idle, None);
        t.wait(10, true, false, Pending::Ready, None);
        t.wake(30, 0, Selected::Record);
        t.mark(50, Phase::Encode);
        t.mark(80, Phase::Announce);
        t.mark(100, Phase::Data);
        t.mark(110, Phase::Parity);
        t.mark(120, Phase::Housekeeping);
        assert_eq!(t.wait_us[1][1], 10);
        assert_eq!(t.wait_us[1][2], 20);
        assert_eq!(t.wait_us.iter().flatten().sum::<u64>(), 30);
        assert_eq!(t.phase_us[Phase::ByteGate as usize], 30);
        assert_eq!(t.phase_us.iter().sum::<u64>(), 120);
    }
}
