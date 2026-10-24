//! Bounded, connection-local upstream original-data demand.
use std::{
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

/// Nonblocking metadata only. Implementations must not call back into QUIC.
pub trait ApplicationData: Send + Sync {
    /// One validity-bearing snapshot: None means cancelled/retired, Some(false)
    /// active but no known unread originals, Some(true) known unread originals.
    /// A ready-data guard must still consult validity, even without unread data.
    fn pending_records(&self) -> Option<bool>;
}
/// Connection-local demand, shared across path-controller clones. Registrations
/// are weak and fixed at sixteen; no payload or application task is owned here.
#[derive(Clone, Default)]
pub struct ApplicationDemand(Arc<Hub>);
struct Hub {
    slots: Mutex<[Weak<State>; 16]>,
    live: AtomicUsize,
}
impl Default for Hub {
    fn default() -> Self {
        Self {
            slots: Mutex::new(std::array::from_fn(|_| Weak::new())),
            live: AtomicUsize::new(0),
        }
    }
}
impl fmt::Debug for ApplicationDemand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplicationDemand")
            .field("registrations", &self.0.live.load(Ordering::Acquire))
            .finish()
    }
}
struct State {
    active: AtomicBool,
    paused: AtomicBool,
    ready: AtomicUsize,
    data: Arc<dyn ApplicationData>,
}
/// Sole owner of one subscription's registration. Dropping it retires even
/// outstanding ready-data guards, without modifying a potentially reused slot.
pub struct DemandRegistration {
    state: Arc<State>,
    hub: Weak<Hub>,
}
/// Stack-scoped demand for originals already encoded and awaiting admission.
/// This guard must not cover CPU/FEC work or optional parity/repair-only sends.
pub struct ReadyData(Arc<State>);
impl ApplicationDemand {
    /// Register one admitted subscription. Returns None at the fixed bound.
    pub fn register(&self, data: Arc<dyn ApplicationData>) -> Option<DemandRegistration> {
        let mut slots = self.0.slots.lock().unwrap();
        let slot = slots.iter_mut().find(|slot| {
            slot.upgrade()
                .is_none_or(|s| !s.active.load(Ordering::Acquire))
        })?;
        let state = Arc::new(State {
            active: AtomicBool::new(true),
            paused: AtomicBool::new(false),
            ready: AtomicUsize::new(0),
            data,
        });
        *slot = Arc::downgrade(&state);
        self.0.live.fetch_add(1, Ordering::Release);
        Some(DemandRegistration {
            state,
            hub: Arc::downgrade(&self.0),
        })
    }
    pub(crate) fn pending(&self) -> bool {
        if self.0.live.load(Ordering::Acquire) == 0 {
            return false;
        }
        // Release the registry lock before consulting application metadata. The
        // snapshot is a fixed stack array, with no allocation on the send path.
        let slots = self.0.slots.lock().unwrap().clone();
        slots.iter().filter_map(Weak::upgrade).any(|s| {
            if !s.active.load(Ordering::Acquire) {
                return false;
            }
            let Some(pending) = s.data.pending_records() else {
                return false;
            };
            (s.ready.load(Ordering::Acquire) != 0 || (s.paused.load(Ordering::Acquire) && pending))
                && s.active.load(Ordering::Acquire)
        })
    }
}
impl DemandRegistration {
    /// Publish an admission pause, separately from CPU work and producer idle.
    pub fn set_admission_paused(&self, paused: bool) {
        self.state.paused.store(paused, Ordering::Release);
    }
    /// Mark encoded originals until their reliable announcement and QUIC
    /// enqueue complete. Does not alter flow control, deadlines or admission.
    pub fn ready_data(&self) -> ReadyData {
        self.state.ready.fetch_add(1, Ordering::AcqRel);
        ReadyData(self.state.clone())
    }
}
impl Drop for DemandRegistration {
    fn drop(&mut self) {
        self.state.active.store(false, Ordering::Release);
        if let Some(hub) = self.hub.upgrade() {
            hub.live.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
impl Drop for ReadyData {
    fn drop(&mut self) {
        self.0.ready.fetch_sub(1, Ordering::AcqRel);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Data {
        active: AtomicBool,
        pending: AtomicBool,
    }
    impl ApplicationData for Data {
        fn pending_records(&self) -> Option<bool> {
            self.active
                .load(Ordering::Acquire)
                .then(|| self.pending.load(Ordering::Acquire))
        }
    }

    fn data(pending: bool) -> Arc<Data> {
        Arc::new(Data {
            active: AtomicBool::new(true),
            pending: AtomicBool::new(pending),
        })
    }
    #[test]
    fn only_admission_blocked_known_records_and_encoded_originals_signal_demand() {
        let hub = ApplicationDemand::default();
        let source = data(true);
        let sub = hub.register(source.clone()).unwrap();
        assert!(!hub.pending(), "unpolled/CPU encoding wait is not demand");
        sub.set_admission_paused(true);
        assert!(hub.pending());
        source.pending.store(false, Ordering::Release);
        assert!(!hub.pending(), "genuine producer idle");
        sub.set_admission_paused(false);
        let ready = sub.ready_data();
        assert!(hub.pending());
        let nested = sub.ready_data();
        drop(ready);
        assert!(hub.pending());
        drop(nested);
        assert!(!hub.pending());
        source.pending.store(true, Ordering::Release);
        assert!(!hub.pending());
    }
    #[test]
    fn cancellation_and_retirement_invalidate_held_ready_guards() {
        let hub = ApplicationDemand::default();
        let source = data(true);
        let sub = hub.register(source.clone()).unwrap();
        sub.set_admission_paused(true);
        let ready = sub.ready_data();
        assert!(hub.pending());
        source.active.store(false, Ordering::Release);
        assert!(!hub.pending());
        source.active.store(true, Ordering::Release);
        drop(sub);
        assert!(!hub.pending());
        let replacement = hub.register(data(false)).unwrap();
        drop(ready);
        assert!(!hub.pending(), "old guard cannot affect reused slot");
        let ready = replacement.ready_data();
        assert!(hub.pending());
        drop(replacement);
        assert!(!hub.pending());
        drop(ready);
    }
    #[test]
    fn registrations_are_bounded_isolated_and_weakly_owned() {
        let hub = ApplicationDemand::default();
        let other = ApplicationDemand::default();
        let mut subs = Vec::new();
        for _ in 0..16 {
            subs.push(hub.register(data(false)).unwrap());
        }
        assert!(hub.register(data(true)).is_none());
        let source = data(true);
        let weak = Arc::downgrade(&source);
        subs.pop();
        let sub = hub.register(source.clone()).unwrap();
        sub.set_admission_paused(true);
        assert!(hub.pending());
        assert!(!other.pending());
        drop(source);
        drop(sub);
        assert!(weak.upgrade().is_none());
        assert!(!hub.pending());
        assert!(hub.register(data(true)).is_some());
    }
}
