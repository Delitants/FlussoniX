//! Weak shapes have no media, gap or timing authority. One charged payload owns
//! both the table and its fixed snapshot; outstanding handles cannot retain it.
use super::{
    diagnostics::{Counts, RECEIVED},
    quota::WorkGate,
    wire::{Frame, Replay},
};
use crate::fxlink::{Budget, Reservation, Settings};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OfferResult {
    Admitted {
        stored: u8,
        ignored: u8,
        conflicts: u8,
    },
    Replay,
    Contended,
    Disabled,
}
#[derive(Clone, Copy, Default)]
struct Entry {
    id: u64,
    length: usize,
    expires: u64,
    // 0 vacant, 1 weak shape, 2 expired weak tombstone, 3 strong marker.
    state: u8,
}
#[derive(Clone, Copy, Default)]
struct Shape {
    id: u64,
    length: usize,
}
struct Payload {
    entries: [Entry; 64],
    snapshot: [Shape; 64],
    epoch: u64,
    replay: Replay,
}
struct Owned {
    // Field drop order is intentional: free the Box allocation BEFORE credit.
    payload: Box<Payload>,
    _charge: Reservation,
}
pub(super) struct Inner {
    published: u64,
    owned: Option<Owned>,
}
pub(crate) struct Ingress {
    counts: Counts<10>,
    settings: Settings,
    pub(super) inner: Mutex<Inner>,
    work: Mutex<WorkGate>,
    disabled: AtomicBool,
    pub(crate) changed: Notify,
    // Small synchronization/Arc allocation remains charged through the last
    // Snapshot/router handle, including after optional payload destruction.
    _handle: Reservation,
}
pub(crate) struct Snapshot {
    owner: Arc<Ingress>,
    epoch: u64,
    at: u64,
}
pub(crate) struct ConnectionIngress {
    routes: Mutex<[Weak<Ingress>; 16]>,
    work: Mutex<WorkGate>,
    disabled: AtomicBool,
    pressure: Arc<dyn Fn() + Send + Sync>,
    _charge: Reservation,
}
impl ConnectionIngress {
    pub(crate) fn new(budget: &Budget) -> Option<Arc<Self>> {
        let bytes = std::mem::size_of::<Self>() + budget.reservation_metadata_bytes() + 144;
        if bytes > 1024 {
            return None;
        }
        let charge = budget.reserve_optional(bytes).ok()?;
        Some(Arc::new_cyclic(|weak: &Weak<Self>| {
            let owner = weak.clone();
            Self {
                routes: Mutex::new(std::array::from_fn(|_| Weak::new())),
                work: Mutex::new(WorkGate::connection()),
                disabled: AtomicBool::new(false),
                pressure: Arc::new(move || {
                    if let Some(owner) = owner.upgrade() {
                        owner.disable();
                    }
                }),
                _charge: charge,
            }
        }))
    }
    pub(crate) fn scoped(&self, budget: &Budget) -> Budget {
        budget.clone().with_pressure(Arc::downgrade(&self.pressure))
    }
    pub(crate) fn admit(&self, budget: &Budget, settings: &Settings) -> Option<Arc<Ingress>> {
        let mut routes = self.routes.lock().unwrap();
        if self.disabled.load(Ordering::Acquire) {
            return None;
        }
        let index = routes.iter().position(|route| {
            route
                .upgrade()
                .is_none_or(|route| route.disabled.load(Ordering::Acquire))
        })?;
        let ingress = Ingress::new(budget, settings)?;
        if self.disabled.load(Ordering::Acquire) {
            ingress.disable();
            return None;
        }
        routes[index] = Arc::downgrade(&ingress);
        Some(ingress)
    }
    pub(crate) fn admit_work(&self, at: u64) -> bool {
        !self.disabled.load(Ordering::Acquire)
            && self.work.try_lock().is_ok_and(|mut gate| gate.admit(at))
    }
    pub(crate) fn disable(&self) {
        self.disabled.store(true, Ordering::Release);
        let routes = std::mem::replace(
            &mut *self.routes.lock().unwrap(),
            std::array::from_fn(|_| Weak::new()),
        );
        for route in routes {
            if let Some(route) = route.upgrade() {
                route.disable_inner(true);
            }
        }
    }
}
impl Inner {
    fn publish(&mut self, next: u64) {
        self.published = self.published.max(next);
        if let Some(owned) = &mut self.owned {
            for entry in &mut owned.payload.entries {
                if entry.state != 0 && entry.id < self.published {
                    *entry = Entry::default();
                }
            }
            for shape in &mut owned.payload.snapshot {
                if shape.length != 0 && shape.id < self.published {
                    *shape = Shape::default();
                }
            }
        }
    }
}
impl Ingress {
    pub(crate) fn diagnostics(&self) -> serde_json::Value {
        self.counts.snapshot(&RECEIVED)
    }
    pub(crate) fn receive(
        &self,
        connection: &ConnectionIngress,
        packet: &[u8],
        at: u64,
        published: u64,
    ) {
        self.counts.add(0, packet.len());
        self.counts.add(1, 1);
        if !connection.admit_work(at) || !self.admit_work(at) {
            self.counts.add(8, 1);
            return;
        }
        let Ok(frame) = super::wire::decode(packet, &self.settings) else {
            self.counts.add(5, 1);
            return;
        };
        match self.offer(&frame, at, published) {
            OfferResult::Admitted {
                stored,
                ignored,
                conflicts,
            } => {
                self.counts.add(2, stored.into());
                self.counts.add(3, ignored.into());
                self.counts.add(6, conflicts.into());
            }
            OfferResult::Replay => self.counts.add(4, 1),
            OfferResult::Contended => self.counts.add(7, 1),
            OfferResult::Disabled => {}
        }
    }
    pub(crate) fn new(budget: &Budget, settings: &Settings) -> Option<Arc<Self>> {
        settings.validate().ok()?;
        let handle_bytes = std::mem::size_of::<Self>()
            + std::mem::size_of::<Snapshot>()
            + budget.reservation_metadata_bytes()
            + 80; // Arc header + allocator metadata.
        let payload_bytes =
            std::mem::size_of::<Payload>() + budget.reservation_metadata_bytes() + 64;
        if handle_bytes.checked_add(payload_bytes)? > 8192 {
            return None;
        }
        let handle = budget.reserve_optional(handle_bytes).ok()?;
        let charge = budget.reserve_optional(payload_bytes).ok()?;
        let payload = Box::new(Payload {
            entries: [Entry::default(); 64],
            snapshot: [Shape::default(); 64],
            epoch: 0,
            replay: Replay::default(),
        });
        Some(Arc::new(Self {
            counts: Counts::default(),
            settings: settings.clone(),
            inner: Mutex::new(Inner {
                published: 0,
                owned: Some(Owned {
                    payload,
                    _charge: charge,
                }),
            }),
            work: Mutex::new(WorkGate::route()),
            disabled: AtomicBool::new(false),
            changed: Notify::new(),
            _handle: handle,
        }))
    }
    pub(crate) fn admit_work(&self, observed: u64) -> bool {
        !self.disabled.load(Ordering::Acquire)
            && self
                .work
                .try_lock()
                .is_ok_and(|mut gate| gate.admit(observed))
    }
    /// Only fully decoded frames for this admitted route enter here. Work gates
    /// run BEFORE decode; this method deliberately does not charge a second time.
    pub(crate) fn offer(&self, frame: &Frame, observed: u64, published: u64) -> OfferResult {
        if self.disabled.load(Ordering::Acquire) {
            return OfferResult::Disabled;
        }
        let Ok(mut inner) = self.inner.try_lock() else {
            return OfferResult::Contended;
        };
        if self.disabled.load(Ordering::Acquire) {
            return OfferResult::Disabled;
        }
        inner.publish(published);
        let published = inner.published;
        let Some(owned) = &mut inner.owned else {
            return OfferResult::Disabled;
        };
        let payload = &mut owned.payload;
        if !payload.replay.admit(frame.sequence) {
            return OfferResult::Replay;
        }
        let (mut stored, mut ignored, mut conflicts) = (0, 0, 0);
        for summary in &frame.entries[..frame.count] {
            let id = summary.record_id;
            if id
                .checked_sub(published)
                .is_none_or(|delta| delta >= self.settings.max_records as u64)
            {
                ignored += 1;
                continue;
            }
            let entry = &mut payload.entries[(id % self.settings.max_records as u64) as usize];
            if entry.state != 0 && entry.id == id {
                if entry.state == 1 && observed >= entry.expires {
                    entry.state = 2;
                }
                if matches!(entry.state, 1 | 3) && entry.length != summary.record_len as usize {
                    conflicts += 1;
                } else {
                    ignored += 1;
                }
                continue;
            }
            let Some(expires) = observed.checked_add(self.settings.latency_ms) else {
                ignored += 1;
                continue;
            };
            *entry = Entry {
                id,
                length: summary.record_len as usize,
                expires,
                state: 1,
            };
            stored += 1;
        }
        if stored != 0 {
            self.changed.notify_one();
        }
        OfferResult::Admitted {
            stored,
            ignored,
            conflicts,
        }
    }
    pub(crate) fn snapshot(self: &Arc<Self>, at: u64, published: u64) -> Option<Snapshot> {
        if self.disabled.load(Ordering::Acquire) {
            return None;
        }
        let mut inner = self.inner.try_lock().ok()?;
        if self.disabled.load(Ordering::Acquire) {
            return None;
        }
        inner.publish(published);
        let payload = &mut inner.owned.as_mut()?.payload;
        let epoch = payload.epoch.checked_add(1)?;
        payload.snapshot = [Shape::default(); 64];
        for (entry, shape) in payload.entries.iter_mut().zip(&mut payload.snapshot) {
            if entry.state == 1 && at >= entry.expires {
                entry.state = 2;
            }
            if entry.state == 1 {
                *shape = Shape {
                    id: entry.id,
                    length: entry.length,
                };
            }
        }
        payload.epoch = epoch;
        Some(Snapshot {
            owner: self.clone(),
            epoch,
            at,
        })
    }
    pub(crate) fn publish(&self, next: u64) {
        self.inner.lock().unwrap().publish(next);
    }
    /// Remember exact in-window authority even when its packet is discarded
    /// before the first hint. Markers never supply shapes, gaps or media age.
    /// Returns whether an existing conflicting weak shape was invalidated.
    pub(crate) fn authoritative(&self, id: u64, length: usize) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if id
            .checked_sub(inner.published)
            .is_none_or(|delta| delta >= self.settings.max_records as u64)
        {
            return false;
        }
        let Some(owned) = &mut inner.owned else {
            return false;
        };
        let payload = &mut owned.payload;
        let index = (id % self.settings.max_records as u64) as usize;
        let entry = &mut payload.entries[index];
        let conflict = entry.state == 1 && entry.id == id && entry.length != length;
        *entry = Entry {
            id,
            length,
            expires: 0,
            state: 3,
        };
        payload.snapshot[index] = Shape::default();
        if conflict {
            self.counts.add(6, 1);
        }
        conflict
    }
    pub(crate) fn disable(&self) {
        self.disable_inner(false);
    }
    fn disable_inner(&self, pressure: bool) {
        if !self.disabled.swap(true, Ordering::AcqRel) && pressure {
            self.counts.add(9, 1);
        }
        let owned = self.inner.lock().unwrap().owned.take();
        drop(owned); // uniquely owned payload deallocates before its reservation.
        self.changed.notify_one();
    }
}
impl Snapshot {
    #[cfg(test)]
    pub(crate) fn shape(&self, record: u64) -> Option<usize> {
        self.shape_at(record, self.at)
    }
    /// A held snapshot may lose expired/invalidated shapes, never gain entries
    /// from later router updates. Lookup time is not a media-age witness.
    pub(crate) fn shape_at(&self, record: u64, at: u64) -> Option<usize> {
        if self.owner.disabled.load(Ordering::Acquire) {
            return None;
        }
        let mut inner = self.owner.inner.try_lock().ok()?;
        if record < inner.published {
            return None;
        }
        let payload = &mut inner.owned.as_mut()?.payload;
        if payload.epoch != self.epoch {
            return None;
        }
        let index = (record % self.owner.settings.max_records as u64) as usize;
        let shape = payload.snapshot[index];
        if shape.id != record || shape.length == 0 {
            return None;
        }
        let entry = &mut payload.entries[index];
        if entry.id != record || entry.state != 1 {
            return None;
        }
        if at.max(self.at) >= entry.expires {
            entry.state = 2;
            payload.snapshot[index] = Shape::default();
            return None;
        }
        Some(shape.length)
    }
}
