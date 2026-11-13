//! Congestion response applies only to optional v5 compact duplicate shards.
pub(super) struct Gate {
    allowed: bool,
    since: Option<u64>,
    last: Option<u64>,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            allowed: true,
            since: None,
            last: None,
        }
    }
}
impl Gate {
    /// Sample shared-path RTT at a bounded cadence. Stable propagation delay
    /// is not queueing. Unknown samples break continuity without inventing a
    /// recovery; they preserve the current protection state.
    pub(super) fn observe(&mut self, at: u64, rtt: u64, min_rtt: u64, latency: u64) -> bool {
        if self
            .last
            .is_some_and(|last| at < last || at.saturating_sub(last) > 200)
        {
            self.since = None;
        }
        self.last = Some(at);
        if min_rtt == 0 || rtt < min_rtt {
            self.since = None;
            return self.allowed;
        }
        let delay = rtt - min_rtt;
        let (qualifies, hold) = if self.allowed {
            (delay >= latency / 4, latency / 5)
        } else {
            (delay <= latency / 8, latency)
        };
        if qualifies {
            let since = *self.since.get_or_insert(at);
            if at.saturating_sub(since) >= hold {
                self.allowed = !self.allowed;
                self.since = None;
            }
        } else {
            self.since = None;
        }
        self.allowed
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // Break caught: treating a long stable propagation delay as congestion.
    #[test]
    fn baseline_aligned_long_rtt_and_unknown_samples_preserve_protection() {
        let mut gate = Gate::default();
        for at in (0..5000).step_by(100) {
            assert!(gate.observe(at, 700, 700, 500));
        }
        assert!(gate.observe(5100, 0, 0, 500));
        assert!(gate.observe(5200, 500, 700, 500));
    }

    // Break caught: one transient high sample sheds protection, or repeated
    // observations postpone the original sustained-pressure boundary forever.
    #[test]
    fn sustained_queueing_suppresses_parity_but_a_transient_does_not() {
        let mut gate = Gate::default();
        assert!(gate.observe(0, 200, 200, 1000));
        assert!(gate.observe(100, 600, 200, 1000));
        assert!(gate.observe(200, 200, 200, 1000));
        assert!(gate.observe(300, 600, 200, 1000));
        assert!(gate.observe(400, 600, 200, 1000));
        assert!(!gate.observe(500, 600, 200, 1000));
        assert!(!gate.observe(600, 600, 200, 1000));
    }

    // Break caught: two endpoints separated by missing observations are
    // counted as sustained pressure despite no evidence between them.
    #[test]
    fn missing_observations_do_not_complete_the_pressure_dwell() {
        let mut gate = Gate::default();
        assert!(gate.observe(0, 600, 200, 1000));
        assert!(gate.observe(1000, 600, 200, 1000));
        assert!(gate.observe(1100, 600, 200, 1000));
        assert!(!gate.observe(1200, 600, 200, 1000));
    }

    // Break caught: noisy intermediate RTT or an unknown sample prematurely
    // restores all duplicates and recreates the congestion episode.
    #[test]
    fn recovery_requires_a_continuously_low_queue_delay() {
        let mut gate = Gate::default();
        assert!(gate.observe(0, 600, 200, 1000));
        assert!(gate.observe(100, 600, 200, 1000));
        assert!(!gate.observe(200, 600, 200, 1000));
        assert!(!gate.observe(300, 300, 200, 1000));
        assert!(!gate.observe(800, 400, 200, 1000));
        assert!(!gate.observe(900, 300, 200, 1000));
        assert!(!gate.observe(1500, 0, 0, 1000));
        assert!(!gate.observe(1600, 300, 200, 1000));
        for at in (1700..2600).step_by(100) {
            assert!(!gate.observe(at, 300, 200, 1000));
        }
        assert!(gate.observe(2600, 300, 200, 1000));
    }
}
