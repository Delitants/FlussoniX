//! Per-output estimates credited only for bandwidth absent from the same sampler interval.
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
#[derive(Default)]
struct History {
    previous: Option<(Instant, u64)>,
    measured: Option<((Instant, Instant), f64)>,
}
pub(crate) struct OutputCredit {
    mbps: f64,
    created: Instant,
    bytes: AtomicU64,
    history: Mutex<History>,
}
impl OutputCredit {
    pub(crate) fn new(mbps: f64) -> Self {
        Self {
            mbps,
            created: Instant::now(),
            bytes: AtomicU64::new(0),
            history: Mutex::new(History::default()),
        }
    }
    pub(crate) fn record(&self, bytes: usize) {
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub(crate) fn estimate(&self) -> f64 {
        self.mbps
    }
    pub(crate) fn sample(&self, from: Option<Instant>, to: Instant) {
        let bytes = self.bytes.load(Ordering::Relaxed);
        let mut h = self.history.lock().unwrap();
        h.measured = from.and_then(|from| {
            let old = match h.previous {
                Some((at, old)) if at == from => Some(old),
                _ if self.created >= from => Some(0),
                _ => None,
            }?;
            let seconds = to.saturating_duration_since(from).as_secs_f64();
            (seconds > 0.0)
                .then(|| {
                    bytes
                        .checked_sub(old)
                        .map(|delta| ((from, to), delta as f64 * 8.0 / 1_000_000.0 / seconds))
                })
                .flatten()
        });
        h.previous = Some((to, bytes));
    }
    pub(crate) fn measured(&self, window: Option<(Instant, Instant)>) -> f64 {
        self.history
            .lock()
            .unwrap()
            .measured
            .filter(|(w, _)| Some(*w) == window)
            .map_or(0.0, |(_, rate)| rate)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn output_credit_uses_actual_bytes_from_the_exact_measurement_interval() {
        let from = Instant::now();
        let credit = OutputCredit::new(80.0);
        let to = from + Duration::from_secs(1);
        assert_eq!(credit.measured(Some((from, to))), 0.0);
        credit.record(7_500_000); // 60 Mbps during this one-second interval.
        credit.sample(Some(from), to);
        assert_eq!(credit.measured(Some((from, to))), 60.0);
        assert_eq!(credit.estimate() - credit.measured(Some((from, to))), 20.0);
        credit.record(10_000_000); // A full 80 Mbps now replaces the whole estimate.
        let end = to + Duration::from_secs(1);
        credit.sample(Some(to), end);
        assert_eq!(credit.measured(Some((to, end))), 80.0);
        assert_eq!(
            credit.measured(Some((from, to))),
            0.0,
            "obsolete windows cannot discount current admission"
        );
        assert_eq!(
            credit.measured(None),
            0.0,
            "missing sampling cannot discount an estimate"
        );
        credit.sample(Some(end), end + Duration::from_secs(1));
        assert_eq!(
            credit.measured(Some((end, end + Duration::from_secs(1)))),
            0.0,
            "a segmented output gap keeps its estimate"
        );
    }
}
