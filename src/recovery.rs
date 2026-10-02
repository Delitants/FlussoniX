//! Monotonic media-progress and retry state. Error labels never contain source secrets.
use std::time::{Duration, Instant};

pub fn retry_delay(streak: u32) -> Duration {
    Duration::from_secs((1u64 << streak.min(5)).min(30))
}
struct Failure {
    reason: &'static str,
    when: Instant,
    delay: Duration,
    next_streak: u32,
}
pub struct Recovery {
    first_media: Option<Instant>,
    last_media: Option<Instant>,
    failure: Option<Failure>,
    streak: u32,
}
impl Recovery {
    pub fn new(streak: u32) -> Self {
        Self {
            first_media: None,
            last_media: None,
            failure: None,
            streak,
        }
    }
    pub fn progress(&mut self) {
        let now = Instant::now();
        self.first_media.get_or_insert(now);
        self.last_media = Some(now);
    }
    pub fn fail(&mut self, reason: &'static str) {
        if self.failure.is_some() {
            return;
        }
        let streak = if self
            .first_media
            .zip(self.last_media)
            .is_some_and(|(first, last)| last.duration_since(first) >= Duration::from_secs(30))
        {
            0
        } else {
            self.streak
        };
        self.failure = Some(Failure {
            reason,
            when: Instant::now(),
            delay: retry_delay(streak),
            next_streak: streak.saturating_add(1),
        });
    }
    pub fn retry_in(&self) -> Option<Duration> {
        self.failure
            .as_ref()
            .map(|f| f.delay.saturating_sub(f.when.elapsed()))
    }
    pub fn next_streak(&self) -> u32 {
        self.failure.as_ref().map_or(self.streak, |f| f.next_streak)
    }
    pub fn last_error(&self) -> Option<&'static str> {
        self.failure.as_ref().map(|f| f.reason)
    }
    pub fn media_age_ms(&self) -> Option<u128> {
        self.last_media.map(|last| last.elapsed().as_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_long_final_stall_is_not_a_healthy_media_window() {
        let mut state = Recovery::new(4);
        let only_progress = Instant::now() - Duration::from_secs(31);
        state.first_media = Some(only_progress);
        state.last_media = Some(only_progress);
        state.fail("input_stalled");
        assert_eq!(state.next_streak(), 5);
        assert!(state.retry_in().unwrap() > Duration::from_secs(15));
    }
    #[test]
    fn short_failures_back_off_and_healthy_media_resets_the_streak() {
        let delays: Vec<_> = (0..8).map(|i| retry_delay(i).as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(retry_delay(u32::MAX).as_secs(), 30);
        let mut state = Recovery::new(4);
        state.first_media = Some(Instant::now() - Duration::from_secs(31));
        state.last_media = Some(Instant::now());
        state.fail("input_closed");
        assert!(state.retry_in().unwrap() <= Duration::from_secs(1));
        assert_eq!(state.next_streak(), 1);
        state.fail("packaging_failed");
        assert_eq!(state.last_error(), Some("input_closed"));
    }
}
