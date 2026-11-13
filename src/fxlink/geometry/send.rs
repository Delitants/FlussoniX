//! Connection-owned, bounded optional geometry dispatcher. No task or media FIFO.
use super::{
    diagnostics::{Counts, SENT},
    quota::{self, Bucket, Spacing},
    wire::{self, Frame, Summary},
};
use crate::fxlink::{Budget, MAX_BLOCKS_PER_RECORD, Reservation, Settings};
use bytes::Bytes;
use futures_util::FutureExt;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
#[derive(Debug, Default)]
pub(crate) struct AttemptResult {
    pub sent: bool,
    pub application_bytes: usize,
    pub attempted_modeled_bytes: usize,
}
#[derive(Clone, Copy, Default)]
struct Recent {
    summary: Summary,
    created: u64,
}
#[derive(Clone, Copy)]
enum Kind {
    Fresh,
    Copy(u64),
}
struct Pending {
    wire: [u8; 128],
    length: usize,
    expires: u64,
    kind: Kind,
}
struct Route {
    id: u64,
    generation: u64,
    cookie: u64,
    settings: Settings,
    current: Arc<dyn Fn() -> bool + Send + Sync>,
    cancel: CancellationToken,
    recent: [Recent; 8],
    count: usize,
    last: Option<u64>,
    consumed: u64,
    dirty: bool,
    pending: Option<Pending>,
    next_sequence: Option<u64>,
    bucket: Bucket,
    spacing: Spacing,
}
// Payload must deallocate before its accounting reservation is released.
struct OwnedRoute {
    payload: Box<Route>,
    _charge: Reservation,
}
struct ConnectionState {
    slots: [Option<OwnedRoute>; 16],
    bucket: Bucket,
    spacing: Spacing,
    cursor: usize,
    next_cookie: Option<u64>,
}
struct OwnedConnection {
    payload: Box<ConnectionState>,
    _charge: Reservation,
}
pub(crate) struct Hub {
    counts: Counts<8>,
    inner: Mutex<Option<OwnedConnection>>,
    disabled: AtomicBool,
    pub(crate) changed: Notify,
    pressure: Arc<dyn Fn() + Send + Sync>,
    _handle: Reservation,
}
pub(crate) struct SourceHandle {
    hub: Weak<Hub>,
    slot: usize,
    cookie: u64,
}
pub(crate) struct Flight<'a> {
    handle: &'a SourceHandle,
    armed: bool,
}
impl Flight<'_> {
    pub(crate) fn commit(mut self) {
        self.armed = false;
    }
}
impl Drop for Flight<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.handle.retire();
        }
    }
}
impl Route {
    fn valid(&self) -> bool {
        !self.cancel.is_cancelled() && (self.current)()
    }
    fn useful(&self, entry: &Recent, at: u64) -> bool {
        entry.summary.record_len != 0
            && entry.summary.record_id >= self.consumed
            && entry
                .created
                .checked_add(self.settings.latency_ms)
                .is_some()
            && at
                .checked_sub(entry.created)
                .and_then(|age| age.checked_add(5))
                .is_some_and(|age| age < self.settings.latency_ms)
    }
    fn refresh(&mut self, at: u64) {
        let mut retained = [Recent::default(); 8];
        let mut count = 0;
        for entry in &self.recent[..self.count] {
            if self.useful(entry, at) {
                retained[count] = *entry;
                count += 1;
            }
        }
        if count != self.count {
            self.recent = retained;
            self.count = count;
            self.dirty = true;
            self.pending = None;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| at >= pending.expires)
        {
            self.pending = None;
        }
        if !self.dirty {
            return;
        }
        self.dirty = false;
        self.pending = None;
        if self.count == 0 {
            return;
        }
        let Some(sequence) = self.next_sequence else {
            return;
        };
        let Some(next) = sequence.checked_add(1) else {
            self.next_sequence = None;
            return;
        };
        let mut frame = Frame {
            subscription: self.id,
            generation: self.generation,
            sequence,
            entries: [Summary::default(); 8],
            count: self.count,
        };
        for (out, entry) in frame.entries.iter_mut().zip(&self.recent[..self.count]) {
            *out = entry.summary;
        }
        frame.entries[..frame.count].sort_unstable_by_key(|entry| entry.record_id);
        let Ok(bytes) = wire::encode(&frame, &self.settings) else {
            self.next_sequence = None;
            return;
        };
        let mut pending = Pending {
            wire: [0; 128],
            length: bytes.len(),
            expires: u64::MAX,
            kind: Kind::Fresh,
        };
        pending.wire[..bytes.len()].copy_from_slice(&bytes);
        for entry in &self.recent[..self.count] {
            pending.expires = pending
                .expires
                .min(entry.created + self.settings.latency_ms - 5);
        }
        self.next_sequence = Some(next);
        self.pending = Some(pending);
    }
    fn ready_at(&self, connection: &Spacing) -> Option<u64> {
        let pending = self.pending.as_ref()?;
        let due = match pending.kind {
            Kind::Fresh => 0,
            Kind::Copy(due) => due,
        };
        Some(
            due.max(self.spacing.next_send_at()?)
                .max(connection.next_send_at()?),
        )
    }
}
impl Hub {
    pub(crate) fn diagnostics(&self) -> serde_json::Value {
        self.counts.snapshot(&SENT)
    }
    pub(crate) fn scoped(&self, budget: &Budget) -> Budget {
        budget.clone().with_pressure(Arc::downgrade(&self.pressure))
    }
    pub(crate) fn new(budget: &Budget) -> Option<Arc<Self>> {
        let handle_bytes = std::mem::size_of::<Self>() + budget.reservation_metadata_bytes() + 144;
        let payload_bytes =
            std::mem::size_of::<ConnectionState>() + budget.reservation_metadata_bytes() + 64;
        if handle_bytes.checked_add(payload_bytes)? > 4096 {
            return None;
        }
        let handle = budget.reserve_optional(handle_bytes).ok()?;
        let charge = budget.reserve_optional(payload_bytes).ok()?;
        let state = ConnectionState {
            slots: std::array::from_fn(|_| None),
            bucket: Bucket::connection(),
            spacing: Spacing::connection(),
            cursor: 0,
            next_cookie: Some(1),
        };
        Some(Arc::new_cyclic(|weak: &Weak<Self>| {
            let owner = weak.clone();
            Self {
                counts: Counts::default(),
                inner: Mutex::new(Some(OwnedConnection {
                    payload: Box::new(state),
                    _charge: charge,
                })),
                disabled: AtomicBool::new(false),
                changed: Notify::new(),
                pressure: Arc::new(move || {
                    if let Some(owner) = owner.upgrade() {
                        owner.disable_inner(true);
                    }
                }),
                _handle: handle,
            }
        }))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register(
        self: &Arc<Self>,
        id: u64,
        generation: u64,
        settings: Settings,
        budget: &Budget,
        current: Arc<dyn Fn() -> bool + Send + Sync>,
        cancel: CancellationToken,
    ) -> Option<SourceHandle> {
        settings.validate().ok()?;
        if generation == 0
            || self.disabled.load(Ordering::Acquire)
            || cancel.is_cancelled()
            || !current()
        {
            return None;
        }
        // Codec/transfer transient storage plus the native validity wrapper's
        // fixed Arc allocation are included in the route's 2 KiB maximum.
        let bytes = std::mem::size_of::<Route>() + budget.reservation_metadata_bytes() + 384;
        if bytes > 2048 {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        let state = &mut inner.as_mut()?.payload;
        if state
            .slots
            .iter()
            .flatten()
            .any(|slot| slot.payload.id == id)
        {
            return None;
        }
        let slot = state.slots.iter().position(Option::is_none)?;
        let cookie = state.next_cookie?;
        let next = cookie.checked_add(1)?;
        let charge = budget.reserve_optional(bytes).ok()?;
        state.slots[slot] = Some(OwnedRoute {
            payload: Box::new(Route {
                id,
                generation,
                cookie,
                settings,
                current,
                cancel,
                recent: [Recent::default(); 8],
                count: 0,
                last: None,
                consumed: 0,
                dirty: false,
                pending: None,
                next_sequence: Some(1),
                bucket: Bucket::route(),
                spacing: Spacing::route(),
            }),
            _charge: charge,
        });
        state.next_cookie = Some(next);
        Some(SourceHandle {
            hub: Arc::downgrade(self),
            slot,
            cookie,
        })
    }
    pub(crate) fn attempt(&self, connection: &quinn::Connection, at: u64) -> AttemptResult {
        self.attempt_send(at, |bytes| {
            matches!(
                connection.send_datagram_wait(bytes).now_or_never(),
                Some(Ok(()))
            )
        })
    }
    #[cfg(test)]
    pub(in crate::fxlink) fn attempt_for_test(
        &self,
        at: u64,
        send: impl FnMut(Bytes) -> bool,
    ) -> AttemptResult {
        self.attempt_send(at, send)
    }
    pub(super) fn attempt_send(
        &self,
        at: u64,
        mut send: impl FnMut(Bytes) -> bool,
    ) -> AttemptResult {
        if self.disabled.load(Ordering::Acquire) {
            return AttemptResult::default();
        }
        let mut inner = self.inner.lock().unwrap();
        let Some(owned) = &mut *inner else {
            return AttemptResult::default();
        };
        let state = &mut owned.payload;
        for offset in 0..16 {
            let index = (state.cursor + offset) % 16;
            if state.slots[index]
                .as_ref()
                .is_some_and(|slot| !slot.payload.valid())
            {
                state.slots[index] = None;
                continue;
            }
            let Some(slot) = &mut state.slots[index] else {
                continue;
            };
            let route = &mut slot.payload;
            route.refresh(at);
            let Some(ready) = route.ready_at(&state.spacing) else {
                continue;
            };
            if ready > at {
                self.counts.add(5, 1);
                continue;
            }
            let pending = route.pending.as_ref().unwrap();
            let length = pending.length;
            if !quota::debit(&mut route.bucket, &mut state.bucket, length) {
                self.counts.add(4, 1);
                if matches!(pending.kind, Kind::Copy(_)) {
                    route.pending = None;
                }
                continue;
            }
            let route_committed = route.spacing.commit(at);
            let connection_committed = state.spacing.commit(at);
            debug_assert!(route_committed && connection_committed);
            // Independent transfer buffer: after actual enqueue, the bounded,
            // prepaid QUIC send queue owns it, not the optional route payload.
            let bytes = Bytes::copy_from_slice(&pending.wire[..length]);
            let kind = pending.kind;
            let sent = send(bytes);
            self.counts.add(1, length + 128);
            if sent {
                self.counts.add(0, length);
                self.counts.add(2, 1);
                if matches!(kind, Kind::Copy(_)) {
                    self.counts.add(3, 1);
                }
            } else {
                self.counts.add(6, 1);
            }
            match kind {
                Kind::Copy(_) => route.pending = None,
                Kind::Fresh if sent => {
                    if let Some(due) = at.checked_add(20).filter(|due| *due < pending.expires) {
                        route.pending.as_mut().unwrap().kind = Kind::Copy(due);
                    } else {
                        route.pending = None;
                    }
                }
                _ => {}
            }
            state.cursor = (index + 1) % 16;
            return AttemptResult {
                sent,
                application_bytes: if sent { length } else { 0 },
                attempted_modeled_bytes: length + 128,
            };
        }
        AttemptResult::default()
    }
    pub(crate) fn wake_at(&self, at: u64) -> Option<u64> {
        if self.disabled.load(Ordering::Acquire) {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        let state = &mut inner.as_mut()?.payload;
        let mut wake = None;
        for slot in &mut state.slots {
            if slot.as_ref().is_some_and(|slot| !slot.payload.valid()) {
                *slot = None;
                continue;
            }
            let Some(slot) = slot else {
                continue;
            };
            let route = &mut slot.payload;
            route.refresh(at);
            let Some(ready) = route.ready_at(&state.spacing) else {
                continue;
            };
            let pending = route.pending.as_ref().unwrap();
            if !quota::funded(&route.bucket, &state.bucket, pending.length) {
                if ready <= at && matches!(pending.kind, Kind::Copy(_)) {
                    route.pending = None;
                }
                continue;
            }
            let next = if ready > at {
                ready
            } else {
                at.checked_add(1)?
            };
            if next < pending.expires {
                wake = Some(wake.map_or(next, |old: u64| old.min(next)));
            }
        }
        wake
    }
    pub(crate) fn disable(&self) {
        self.disable_inner(false);
    }
    fn disable_inner(&self, pressure: bool) {
        if !self.disabled.swap(true, Ordering::AcqRel) && pressure {
            self.counts.add(7, 1);
        }
        let payload = self.inner.lock().unwrap().take();
        drop(payload);
        self.changed.notify_one();
    }
}
impl SourceHandle {
    pub(crate) fn in_flight(&self) -> Flight<'_> {
        Flight {
            handle: self,
            armed: true,
        }
    }
    fn update(&self, apply: impl FnOnce(&mut Route, &mut Bucket)) {
        let Some(hub) = self.hub.upgrade() else {
            return;
        };
        if hub.disabled.load(Ordering::Acquire) {
            return;
        }
        let mut inner = hub.inner.lock().unwrap();
        let Some(owned) = &mut *inner else {
            return;
        };
        let state = &mut owned.payload;
        let Some(slot) = &mut state.slots[self.slot] else {
            return;
        };
        if slot.payload.cookie != self.cookie || !slot.payload.valid() {
            return;
        }
        apply(&mut slot.payload, &mut state.bucket);
        hub.changed.notify_one();
    }
    pub(crate) fn encoded(&self, id: u64, length: usize, created: u64) {
        self.update(|route, _| {
            if route.last.is_some_and(|last| id <= last)
                || route.next_sequence.is_none()
                || !(12..=route.settings.max_record_len).contains(&length)
                || route.settings.block_count(length) > route.settings.max_blocks
                || id.checked_mul(MAX_BLOCKS_PER_RECORD as u64).is_none()
            {
                return;
            }
            let entry = Recent {
                summary: Summary {
                    record_id: id,
                    record_len: length as u32,
                },
                created,
            };
            if route.count == 8 {
                route.recent.rotate_left(1);
                route.recent[7] = entry;
            } else {
                route.recent[route.count] = entry;
                route.count += 1;
            }
            route.last = Some(id);
            route.dirty = true;
            route.pending = None;
        });
    }
    pub(crate) fn original_admitted(&self, bytes: usize) {
        self.update(|route, connection| {
            route.bucket.original_admitted(bytes);
            connection.original_admitted(bytes);
        });
    }
    pub(crate) fn consumed(&self, next: u64) {
        self.update(|route, _| {
            if next <= route.consumed {
                return;
            }
            route.consumed = next;
            if route.recent[..route.count]
                .iter()
                .any(|entry| entry.summary.record_id < next)
            {
                route.dirty = true;
                route.pending = None;
            }
        });
    }
    pub(crate) fn retire(&self) {
        let Some(hub) = self.hub.upgrade() else {
            return;
        };
        let mut inner = hub.inner.lock().unwrap();
        let Some(owned) = &mut *inner else {
            return;
        };
        let state = &mut owned.payload;
        if state.slots[self.slot]
            .as_ref()
            .is_some_and(|slot| slot.payload.cookie == self.cookie)
        {
            state.slots[self.slot] = None;
            hub.changed.notify_one();
        }
    }
}
impl Drop for SourceHandle {
    fn drop(&mut self) {
        self.retire();
    }
}
