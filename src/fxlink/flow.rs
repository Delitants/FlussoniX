//! Record credit supplements, rather than replaces, byte and media-age bounds.

#[derive(Default, Debug)]
pub(super) struct Window {
    acknowledged: u64,
    stalled_until: Option<u64>,
}
impl Window {
    pub fn can_admit(
        &mut self,
        sent: u64,
        limit: usize,
        at: u64,
        latency: u64,
    ) -> Result<bool, String> {
        let outstanding = sent
            .checked_sub(self.acknowledged)
            .ok_or("FXLink invalid record credit")?;
        if outstanding > limit as u64 {
            return Err("FXLink record credit exceeded".into());
        }
        if outstanding < limit as u64 {
            self.stalled_until = None;
            return Ok(true);
        }
        // This independent liveness timer includes the feedback return trip.
        // It never changes any original media/cache deadline and survives cache pruning.
        let deadline = self
            .stalled_until
            .get_or_insert_with(|| at.saturating_add(latency.saturating_mul(3)));
        if at >= *deadline {
            return Err("FXLink record credit stalled".into());
        }
        Ok(false)
    }

    /// Progress is the next unconsumed record. A delayed repair below this
    /// validated frontier cannot help the subscriber and must not spend credit.
    pub fn has_consumed(&self, record: u64) -> bool {
        record < self.acknowledged
    }

    pub fn progress(&mut self, next: u64, sent: u64) -> Result<(), String> {
        if next < self.acknowledged || next > sent {
            return Err("FXLink invalid record progress".into());
        }
        if next > self.acknowledged {
            self.acknowledged = next;
            self.stalled_until = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Window;

    #[test]
    fn cumulative_credit_admits_only_the_negotiated_window() {
        let mut window = Window::default();
        assert!(window.can_admit(0, 4, 0, 1000).unwrap());
        assert!(window.can_admit(3, 4, 0, 1000).unwrap());
        assert!(!window.can_admit(4, 4, 0, 1000).unwrap());
        window.progress(2, 4).unwrap();
        assert!(window.can_admit(4, 4, 100, 1000).unwrap());
        assert!(!window.can_admit(6, 4, 100, 1000).unwrap());
        assert!(window.progress(7, 6).is_err());
        assert!(window.progress(1, 6).is_err());
        assert!(!window.can_admit(6, 4, 101, 1000).unwrap());
        window.progress(6, 6).unwrap();
        assert!(window.can_admit(6, 4, 102, 1000).unwrap());
    }

    #[test]
    fn duplicate_progress_cannot_renew_a_stalled_credit_deadline() {
        let mut window = Window::default();
        assert!(!window.can_admit(4, 4, 100, 1000).unwrap());
        window.progress(0, 4).unwrap();
        assert!(!window.can_admit(4, 4, 3099, 1000).unwrap());
        window.progress(0, 4).unwrap();
        assert!(window.can_admit(4, 4, 3100, 1000).is_err());
    }

    #[test]
    fn credit_return_wait_is_independent_of_the_original_media_ttl() {
        let mut window = Window::default();
        assert!(!window.can_admit(4, 4, 0, 500).unwrap());
        // A record reaching its receiver within500ms can acknowledge at590ms.
        // Credit bookkeeping survives original repair-cache expiry at500ms.
        assert!(!window.can_admit(4, 4, 590, 500).unwrap());
        window.progress(4, 4).unwrap();
        assert!(window.can_admit(4, 4, 590, 500).unwrap());
        assert!(!window.can_admit(8, 4, 600, 500).unwrap());
        assert!(window.can_admit(8, 4, 2100, 500).is_err());
    }
}
