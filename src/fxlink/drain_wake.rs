//! Bounded early observations of a v5 source's paused byte queue.
#[derive(Default)]
pub(super) struct DrainWake {
    next: Option<u64>,
    used: u8,
}
impl DrainWake {
    /// Call after sampling both record credit and byte-queue admission. Other
    /// events cannot postpone an armed observation or replenish this episode.
    /// After four due observations, only ordinary housekeeping wakes remain.
    pub(super) fn observe(&mut self, blocked: bool, time: u64) -> Option<u64> {
        if !blocked {
            *self = Self::default();
            return None;
        }
        if let Some(next) = self.next {
            if time < next {
                return Some(next);
            }
            self.next = None;
            self.used += 1;
        }
        if self.used == 4 {
            return None;
        }
        let next = time.saturating_add(1 << self.used);
        self.next = Some(next);
        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn other_wakes_do_not_postpone_an_armed_observation() {
        let mut wake = DrainWake::default();
        for _ in 0..500 {
            assert_eq!(wake.observe(true, 100), Some(101));
        }
        assert_eq!(wake.observe(true, 101), Some(103));
        for _ in 0..500 {
            assert_eq!(wake.observe(true, 102), Some(103));
        }
    }
    #[test]
    fn persistent_stall_exhausts_probes_instead_of_polling_fast_forever() {
        let mut wake = DrainWake::default();
        assert_eq!(wake.observe(true, 0), Some(1));
        assert_eq!(wake.observe(true, 1), Some(3));
        assert_eq!(wake.observe(true, 3), Some(7));
        assert_eq!(wake.observe(true, 7), Some(15));
        for time in 15..1000 {
            assert_eq!(wake.observe(true, time), None);
        }
    }
    #[test]
    fn cleared_gate_disables_the_timer_and_starts_a_fresh_episode() {
        let mut wake = DrainWake::default();
        assert_eq!(wake.observe(true, 0), Some(1));
        assert_eq!(wake.observe(true, 1), Some(3));
        for time in 2..100 {
            assert_eq!(wake.observe(false, time), None);
        }
        assert_eq!(wake.observe(true, 100), Some(101));
    }
    #[test]
    fn late_ordinary_wake_consumes_one_probe_without_catchup_spin() {
        let mut wake = DrainWake::default();
        assert_eq!(wake.observe(true, 0), Some(1));
        assert_eq!(wake.observe(true, 100), Some(102));
        assert_eq!(wake.observe(true, 100), Some(102));
        assert_eq!(wake.observe(true, 101), Some(102));
    }
    #[test]
    fn saturated_clock_still_exhausts_the_bounded_episode() {
        let mut wake = DrainWake::default();
        for _ in 0..4 {
            assert_eq!(wake.observe(true, u64::MAX), Some(u64::MAX));
        }
        for _ in 0..100 {
            assert_eq!(wake.observe(true, u64::MAX), None);
        }
    }
}
