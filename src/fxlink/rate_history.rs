//! Fixed-size, chronological evidence; no formatting or growth on the source clock.
#[derive(Debug)]
pub(super) struct History<T: Copy, const N: usize> {
    entries: [Option<T>; N],
    next: usize,
    len: usize,
}
impl<T: Copy, const N: usize> History<T, N> {
    pub(super) fn new() -> Self {
        assert!(N > 0);
        Self {
            entries: [None; N],
            next: 0,
            len: 0,
        }
    }
    pub(super) fn push(&mut self, value: T) {
        self.entries[self.next] = Some(value);
        self.next = (self.next + 1) % N;
        self.len = (self.len + 1).min(N);
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        (0..self.len).map(move |i| {
            let first = if self.len == N { self.next } else { 0 };
            self.entries[(first + i) % N]
                .as_ref()
                .expect("initialized history")
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overflow_retains_latest_failure_and_its_preceding_attempts_in_order() {
        let mut h = History::<_, 3>::new();
        for value in [(10, true), (11, true), (12, true), (13, true), (14, false)] {
            h.push(value);
        }
        assert_eq!(
            h.iter().copied().collect::<Vec<_>>(),
            [(12, true), (13, true), (14, false)]
        );
    }
    #[test]
    fn partially_filled_history_has_no_fabricated_attempts() {
        let mut h = History::<_, 4>::new();
        h.push(7);
        h.push(8);
        assert_eq!(h.iter().copied().collect::<Vec<_>>(), [7, 8]);
    }
}
