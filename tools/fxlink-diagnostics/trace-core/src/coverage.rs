//! Bounded local accounting. Unknown or partial intervals never establish absence.
use std::sync::atomic::{AtomicU64, Ordering};
pub const BUCKET_NS: u64 = 100_000_000;
pub const BUCKET_COUNT: usize = 5000;
pub struct Coverage {
    bins: Vec<[AtomicU64; 4]>,
    pub out_of_range: AtomicU64,
}
impl Coverage {
    pub fn new() -> Self {
        Self {
            bins: (0..BUCKET_COUNT)
                .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
                .collect(),
            out_of_range: AtomicU64::new(0),
        }
    }
    pub fn attempt(&self, ns: u64) -> Option<usize> {
        let i = ns / BUCKET_NS;
        if i >= BUCKET_COUNT as u64 {
            self.out_of_range.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        self.bins[i as usize][0].fetch_add(1, Ordering::Relaxed);
        Some(i as usize)
    }
    pub fn note(&self, index: Option<usize>, field: usize) {
        if let Some(i) = index {
            self.bins[i][field].fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn overwritten(&self, ns: u64) {
        let i = ns / BUCKET_NS;
        if i < BUCKET_COUNT as u64 {
            self.note(Some(i as usize), 3);
        }
    }
    pub fn snapshot(&self) -> Vec<[u64; 4]> {
        self.bins
            .iter()
            .map(|b| std::array::from_fn(|i| b[i].load(Ordering::Relaxed)))
            .collect()
    }
}
