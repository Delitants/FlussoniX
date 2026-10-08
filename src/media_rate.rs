//! Recent rate of the shared MPEG-TS output, sampled by the producer.
use std::time::Instant;

#[derive(Default)]
pub(crate) struct OutputRate {
    window: Option<(Instant, u64)>,
    last: Option<Instant>,
    completed: [Option<(Instant, f64)>; 2],
}

pub(crate) const MAX_AGE_MS: u64 = 3000;

impl OutputRate {
    pub(crate) fn record(&mut self, at: Instant, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if self.last.is_some_and(|last| at < last) {
            self.reset(at, bytes);
            return;
        }
        self.last = Some(at);
        let Some((start, previous)) = self.window else {
            self.window = Some((at, bytes));
            return;
        };
        let Some(elapsed) = at.checked_duration_since(start) else {
            self.reset(at, bytes);
            return;
        };
        if elapsed.as_millis() >= u128::from(MAX_AGE_MS) {
            self.reset(at, bytes);
            return;
        }
        let Some(total) = previous.checked_add(bytes) else {
            self.reset(at, bytes);
            return;
        };
        if elapsed.as_secs() >= 1 {
            let mbps = total as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0;
            self.completed = [Some((at, mbps)), self.completed[0]];
            self.window = Some((at, 0));
        } else {
            self.window = Some((start, total));
        }
    }
    fn reset(&mut self, at: Instant, bytes: u64) {
        self.last = Some(at);
        self.window = Some((at, bytes));
        self.completed = [None; 2];
    }
    pub(crate) fn snapshot(&self, at: Instant) -> Option<(f64, u64)> {
        self.completed
            .iter()
            .flatten()
            .filter_map(|(when, mbps)| {
                let age = at.checked_duration_since(*when)?.as_millis();
                (age <= u128::from(MAX_AGE_MS)).then_some((*mbps, age as u64))
            })
            .max_by(|a, b| a.0.total_cmp(&b.0).then_with(|| b.1.cmp(&a.1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn bytes_are_measured_in_decimal_megabits_after_a_complete_interval() {
        let at = Instant::now();
        let mut rate = OutputRate::default();
        rate.record(at, 1);
        assert_eq!(rate.snapshot(at + Duration::from_millis(999)), None);
        rate.record(at + Duration::from_secs(1), 999_999);
        assert_eq!(rate.snapshot(at + Duration::from_secs(1)), Some((8.0, 0)));
        assert_eq!(
            rate.snapshot(at + Duration::from_millis(1500)),
            Some((8.0, 500))
        );
    }

    #[test]
    fn the_recent_peak_does_not_disappear_on_the_first_quiet_interval() {
        let at = Instant::now();
        let mut rate = OutputRate::default();
        rate.record(at, 1_000_000);
        rate.record(at + Duration::from_secs(1), 1_000_000);
        assert_eq!(rate.snapshot(at + Duration::from_secs(1)), Some((16.0, 0)));
        rate.record(at + Duration::from_secs(2), 125_000);
        assert_eq!(
            rate.snapshot(at + Duration::from_secs(2)),
            Some((16.0, 1000))
        );
        rate.record(at + Duration::from_secs(3), 125_000);
        assert_eq!(rate.snapshot(at + Duration::from_secs(3)), Some((1.0, 0)));
    }

    #[test]
    fn management_reads_and_zero_byte_events_cannot_refresh_stale_rates() {
        let at = Instant::now();
        let mut rate = OutputRate::default();
        rate.record(at, 1);
        rate.record(at + Duration::from_secs(1), 999_999);
        assert_eq!(
            rate.snapshot(at + Duration::from_secs(4)),
            Some((8.0, 3000))
        );
        rate.record(at + Duration::from_millis(4001), 0);
        assert_eq!(rate.snapshot(at + Duration::from_millis(4001)), None);
    }

    #[test]
    fn a_long_gap_requires_a_new_interval_instead_of_averaging_in_the_outage() {
        let at = Instant::now();
        let mut rate = OutputRate::default();
        rate.record(at, 1);
        rate.record(at + Duration::from_secs(1), 999_999);
        rate.record(at + Duration::from_secs(5), 1);
        assert_eq!(rate.snapshot(at + Duration::from_secs(5)), None);
        rate.record(at + Duration::from_secs(6), 124_999);
        assert_eq!(rate.snapshot(at + Duration::from_secs(6)), Some((1.0, 0)));
    }

    #[test]
    fn overflowing_bytes_and_regressing_clock_invalidate_the_observation() {
        let at = Instant::now();
        let mut rate = OutputRate::default();
        rate.record(at, u64::MAX);
        rate.record(at + Duration::from_secs(1), 1);
        assert_eq!(rate.snapshot(at + Duration::from_secs(1)), None);
        rate.record(at + Duration::from_secs(2), 999_999);
        assert_eq!(rate.snapshot(at + Duration::from_secs(2)), Some((8.0, 0)));
        rate.record(at, 1);
        assert_eq!(rate.snapshot(at), None);
    }

    #[test]
    fn regression_within_an_open_interval_discards_the_previous_peak() {
        let at = Instant::now();
        let mut rate = OutputRate::default();
        rate.record(at, 1);
        rate.record(at + Duration::from_secs(1), 999_999);
        rate.record(at + Duration::from_millis(1500), 1);
        rate.record(at + Duration::from_millis(1250), 1);
        assert_eq!(rate.snapshot(at + Duration::from_millis(1500)), None);
    }
}
