//! Independent optional-metadata accounting; no media/repair allowance authority.
pub(crate) struct Bucket {
    units: u64,
    capacity: u64,
}
impl Bucket {
    pub(crate) fn route() -> Self {
        Self {
            units: 256 * 64,
            capacity: 256 * 64,
        }
    }
    pub(crate) fn connection() -> Self {
        Self {
            units: 256 * 64,
            capacity: 1024 * 64,
        }
    }
    /// One original application byte buys exactly one 1/64-byte accounting unit.
    /// Only the caller witnessing actual original enqueue may grant this credit.
    pub(crate) fn original_admitted(&mut self, bytes: usize) {
        self.units = self.units.saturating_add(bytes as u64).min(self.capacity);
    }
}
pub(crate) fn debit(route: &mut Bucket, connection: &mut Bucket, frame_len: usize) -> bool {
    if !funded(route, connection, frame_len) {
        return false;
    }
    let cost = (frame_len as u64 + super::super::packet::OUTER_PACKET_ALLOWANCE as u64) * 64;
    route.units -= cost;
    connection.units -= cost;
    true
}
pub(super) fn funded(route: &Bucket, connection: &Bucket, frame_len: usize) -> bool {
    (44..=128).contains(&frame_len) && {
        let cost = (frame_len as u64 + super::super::packet::OUTER_PACKET_ALLOWANCE as u64) * 64;
        route.units >= cost && connection.units >= cost
    }
}
pub(crate) struct Spacing {
    next: Option<u64>,
    period: u64,
}
impl Spacing {
    pub(crate) fn route() -> Self {
        Self {
            next: Some(0),
            period: 20,
        }
    }
    pub(crate) fn connection() -> Self {
        Self {
            next: Some(0),
            period: 10,
        }
    }
    pub(crate) fn next_send_at(&self) -> Option<u64> {
        self.next
    }
    /// Called only after both byte debits, immediately before polling enqueue.
    pub(crate) fn commit(&mut self, at: u64) -> bool {
        if !self.next.is_some_and(|next| at >= next) {
            return false;
        }
        self.next = at.checked_add(self.period);
        true
    }
}
pub(crate) struct WorkGate {
    tokens: u8,
    capacity: u8,
    period: u64,
    last: Option<u64>,
}
impl WorkGate {
    pub(crate) fn route() -> Self {
        Self {
            tokens: 2,
            capacity: 2,
            period: 20,
            last: None,
        }
    }
    pub(crate) fn connection() -> Self {
        Self {
            tokens: 4,
            capacity: 4,
            period: 10,
            last: None,
        }
    }
    pub(crate) fn admit(&mut self, at: u64) -> bool {
        if let Some(last) = self.last {
            let Some(delta) = at.checked_sub(last) else {
                return false;
            };
            let refill = delta / self.period;
            if refill != 0 {
                self.tokens = self
                    .tokens
                    .saturating_add(refill.min(u64::from(self.capacity)) as u8)
                    .min(self.capacity);
                // Product and sum are <= delta and at respectively, including MAX.
                self.last = Some(last + refill * self.period);
            }
        } else {
            self.last = Some(at);
        }
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }
}
