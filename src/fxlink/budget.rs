use super::Error;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Debug)]
struct Counter {
    used: AtomicUsize,
    limit: usize,
}
#[derive(Debug, Clone)]
pub struct Budget {
    chain: Vec<Arc<Counter>>,
    pressure: Option<Weak<dyn Fn() + Send + Sync>>,
}
// Weak references follow the counters, so descendant budgets and reservations
// keep a registry identity alive even after its original Budget is dropped.
pub(crate) struct WeakBudget {
    chain: Vec<Weak<Counter>>,
}
impl WeakBudget {
    pub(crate) fn upgrade(&self) -> Option<Budget> {
        self.chain
            .iter()
            .map(Weak::upgrade)
            .collect::<Option<Vec<_>>>()
            .map(|chain| Budget {
                chain,
                pressure: None,
            })
    }
}
#[derive(Debug)]
pub struct Reservation {
    chain: Vec<Arc<Counter>>,
    bytes: usize,
}
impl Budget {
    pub(crate) fn with_pressure(mut self, pressure: Weak<dyn Fn() + Send + Sync>) -> Self {
        self.pressure = Some(pressure);
        self
    }
    pub fn new(limit: usize) -> Self {
        Self {
            chain: vec![Arc::new(Counter {
                used: AtomicUsize::new(0),
                limit,
            })],
            pressure: None,
        }
    }
    pub(crate) fn downgrade(&self) -> WeakBudget {
        WeakBudget {
            chain: self.chain.iter().map(Arc::downgrade).collect(),
        }
    }
    pub fn child(&self, limit: usize) -> Self {
        let mut chain = self.chain.clone();
        chain.push(Arc::new(Counter {
            used: AtomicUsize::new(0),
            limit,
        }));
        Self {
            chain,
            pressure: self.pressure.clone(),
        }
    }
    pub fn used(&self) -> usize {
        self.chain.last().unwrap().used.load(Ordering::Acquire)
    }
    pub fn limit(&self) -> usize {
        self.chain.last().unwrap().limit
    }
    pub(super) fn reservation_metadata_bytes(&self) -> usize {
        self.chain
            .len()
            .saturating_mul(std::mem::size_of::<Arc<Counter>>())
    }
    pub fn reserve(&self, bytes: usize) -> Result<Reservation, Error> {
        match self.reserve_optional(bytes) {
            Ok(reservation) => Ok(reservation),
            Err(error) => {
                let Some(pressure) = self.pressure.as_ref().and_then(Weak::upgrade) else {
                    return Err(error);
                };
                // All partial counter claims have rolled back. Destruction
                // completes synchronously before this one ordinary retry.
                pressure();
                self.reserve_optional(bytes)
            }
        }
    }
    /// New optional metadata cannot reclaim other owners to fund itself, nor
    /// recursively call a reclaimer while holding its own registration lock.
    pub(crate) fn reserve_optional(&self, bytes: usize) -> Result<Reservation, Error> {
        for (index, c) in self.chain.iter().enumerate() {
            let mut current = c.used.load(Ordering::Acquire);
            let admitted = loop {
                let Some(next) = current.checked_add(bytes).filter(|v| *v <= c.limit) else {
                    break false;
                };
                match c.used.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break true,
                    Err(observed) => current = observed,
                }
            };
            if !admitted {
                for previous in &self.chain[..index] {
                    previous.used.fetch_sub(bytes, Ordering::AcqRel);
                }
                return Err(Error::BudgetExceeded);
            }
        }
        Ok(Reservation {
            chain: self.chain.clone(),
            bytes,
        })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        for c in &self.chain {
            c.used.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
mod geometry_pressure_tests {
    use super::*;
    use crate::fxlink::{
        Receiver, Settings,
        geometry::{
            receive::Ingress,
            wire::{Frame, Summary},
        },
    };
    use std::sync::Mutex;

    #[test]
    fn mandatory_record_retries_before_receiver_failure_and_preserves_original_age() {
        let root = Budget::new(128 * 1024);
        let child = root.child(64 * 1024);
        let settings = Settings::default();
        let ingress = Ingress::new(&child, &settings).unwrap();
        let frame = Frame {
            subscription: 7,
            generation: 1,
            sequence: 1,
            count: 1,
            entries: std::array::from_fn(|i| {
                if i == 0 {
                    Summary {
                        record_id: 0,
                        record_len: 3000,
                    }
                } else {
                    Summary::default()
                }
            }),
        };
        ingress.offer(&frame, 90, 0);
        let snapshot = ingress.snapshot(90, 0).unwrap();
        let weak = Arc::downgrade(&ingress);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let reclaim: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            count.fetch_add(1, Ordering::Relaxed);
            if let Some(ingress) = weak.upgrade() {
                ingress.disable();
            }
        });
        let scoped = child.clone().with_pressure(Arc::downgrade(&reclaim));
        let mut receiver = Receiver::new(settings, 7, 1, scoped).unwrap();
        receiver.announce(1, 1000, 80).unwrap();
        let pressure = child.reserve(child.limit() - child.used() - 1024).unwrap();
        assert!(
            receiver.announce(0, 3000, 100).is_ok(),
            "optional geometry retired the existing Receiver"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(!receiver.is_retired());
        assert!(
            !receiver.missing_shape(1),
            "retry cleared pre-existing media"
        );
        assert_eq!(snapshot.shape(0), None);
        assert!(receiver.tick(1079).is_ok());
        assert!(
            matches!(receiver.tick(1080), Err(Error::Expired { record_id: 0 })),
            "retry renewed inherited age"
        );
        drop(receiver);
        drop(pressure);
        drop(ingress);
        drop(snapshot);
        assert_eq!(root.used(), 0);
    }
    #[test]
    fn partial_counter_claims_roll_back_before_one_reclaim_and_optional_never_reclaims() {
        let root = Budget::new(10_000);
        let hold = root.reserve(100).unwrap();
        let child = root.child(1000);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observations = seen.clone();
        let observed_root = root.clone();
        let reclaim: Arc<dyn Fn() + Send + Sync> =
            Arc::new(move || observations.lock().unwrap().push(observed_root.used()));
        let scoped = child.with_pressure(Arc::downgrade(&reclaim));
        assert!(scoped.reserve_optional(2000).is_err());
        assert!(seen.lock().unwrap().is_empty());
        assert!(scoped.reserve(2000).is_err());
        assert_eq!(*seen.lock().unwrap(), [100]);
        assert_eq!(root.used(), 100);
        drop(hold);
        assert_eq!(root.used(), 0);
    }
}
