//! One immutable payload ring shared by all viewers, bounded by both bytes and records.
use bytes::Bytes;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, broadcast::error::RecvError};
struct State {
    records: VecDeque<(u64, Bytes)>,
    bytes: usize,
    next: u64,
    closed: bool,
    receivers: usize,
    cursors: Option<BTreeMap<u64, usize>>,
}
struct Inner {
    state: Mutex<State>,
    changed: Notify,
    max_records: usize,
    max_bytes: usize,
    retain_without_receivers: bool,
}
#[derive(Clone)]
pub struct Channel {
    inner: Arc<Inner>,
}
pub struct Receiver {
    inner: Arc<Inner>,
    next: u64,
    demand_cursor: Option<Arc<DemandCursor>>,
}
impl Channel {
    /// Clean producer EOF: existing receivers drain queued records then see
    /// Closed. Worker/grant cancellation remains a separate immediate fence.
    pub(crate) fn close(&self) {
        self.inner.state.lock().unwrap().closed = true;
        self.inner.changed.notify_waiters();
    }
    pub fn new(max_records: usize, max_bytes: usize) -> Self {
        Self::with_retention(max_records, max_bytes, true)
    }
    /// Live delivery uses a separate bootstrap. History has no consumer when
    /// there are no receivers, so do not fill a queue for an unused output.
    pub(crate) fn for_subscribers(max_records: usize, max_bytes: usize) -> Self {
        Self::with_retention(max_records, max_bytes, false)
    }
    /// Continuous outputs need unread backlog only. Track shared cursor counts
    /// so payload ownership ends as soon as the last interested viewer advances.
    pub(crate) fn live(max_records: usize, max_bytes: usize) -> Self {
        let channel = Self::for_subscribers(max_records, max_bytes);
        channel.inner.state.lock().unwrap().cursors = Some(BTreeMap::new());
        channel
    }
    fn with_retention(
        max_records: usize,
        max_bytes: usize,
        retain_without_receivers: bool,
    ) -> Self {
        assert!(max_records > 0 && max_bytes > 0);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    records: VecDeque::new(),
                    bytes: 0,
                    next: 0,
                    closed: false,
                    receivers: 0,
                    cursors: None,
                }),
                changed: Notify::new(),
                max_records,
                max_bytes,
                retain_without_receivers,
            }),
        }
    }
    pub fn send(&self, bytes: Bytes) -> Result<(), String> {
        if bytes.len() > self.inner.max_bytes {
            return Err("wire record exceeds queue byte budget".into());
        }
        let mut s = self.inner.state.lock().unwrap();
        if s.closed {
            return Err("wire channel is closed".into());
        }
        let sequence = s.next;
        s.next = s.next.checked_add(1).ok_or("wire sequence exhausted")?;
        if !self.inner.retain_without_receivers && s.receivers == 0 {
            return Ok(());
        }
        while s.records.len() >= self.inner.max_records
            || s.bytes + bytes.len() > self.inner.max_bytes
        {
            let (_, old) = s.records.pop_front().unwrap();
            s.bytes -= old.len();
        }
        s.bytes += bytes.len();
        s.records.push_back((sequence, bytes));
        drop(s);
        self.inner.changed.notify_waiters();
        Ok(())
    }
    pub fn subscribe(&self) -> Receiver {
        let mut state = self.inner.state.lock().unwrap();
        state.receivers += 1;
        let next = state.next;
        if let Some(cursors) = &mut state.cursors {
            *cursors.entry(next).or_default() += 1;
        }
        Receiver {
            inner: self.inner.clone(),
            next: state.next,
            demand_cursor: None,
        }
    }
    pub(crate) fn has_receivers(&self) -> bool {
        self.inner.state.lock().unwrap().receivers != 0
    }
    pub fn retained(&self) -> (usize, usize) {
        let s = self.inner.state.lock().unwrap();
        (s.records.len(), s.bytes)
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(cursor) = &self.demand_cursor {
            cursor.alive.store(false, Ordering::Release);
        }
        let mut state = self.inner.state.lock().unwrap();
        state.receivers -= 1;
        state.move_cursor(self.next, None);
        if state.receivers == 0 && !self.inner.retain_without_receivers {
            state.records.clear();
            state.bytes = 0;
        }
    }
}
impl State {
    fn move_cursor(&mut self, previous: u64, next: Option<u64>) {
        let Some(cursors) = &mut self.cursors else {
            return;
        };
        let count = cursors
            .get_mut(&previous)
            .expect("registered receiver cursor");
        *count -= 1;
        if *count == 0 {
            cursors.remove(&previous);
        }
        if let Some(next) = next {
            *cursors.entry(next).or_default() += 1;
        }
        let consumed = cursors
            .first_key_value()
            .map_or(self.next, |(next, _)| *next);
        while self
            .records
            .front()
            .is_some_and(|(sequence, _)| *sequence < consumed)
        {
            let (_, old) = self.records.pop_front().unwrap();
            self.bytes -= old.len();
        }
    }
}
/// Weak metadata view; observing it never consumes or retains a media record.
pub(crate) struct DemandProbe {
    inner: Weak<Inner>,
    cursor: Weak<DemandCursor>,
}
struct DemandCursor {
    next: AtomicU64,
    alive: AtomicBool,
}
#[derive(Debug)]
pub(crate) struct DemandSnapshot {
    pub reader_id: usize,
    pub producer_next: u64,
    pub reader_next: u64,
    pub unread_records: u64,
    pub missed_records: u64,
    pub retained_records: usize,
    pub alive: bool,
    pub closed: bool,
}
impl DemandProbe {
    /// Diagnostic metadata only. Cursors are absolute; missed is the current eviction gap.
    pub(crate) fn diagnostic(&self) -> Option<DemandSnapshot> {
        let inner = self.inner.upgrade()?;
        let cursor = self.cursor.upgrade()?;
        let state = inner.state.lock().unwrap();
        let next = cursor.next.load(Ordering::Acquire);
        let first = state
            .records
            .front()
            .map_or(state.next, |(first, _)| *first);
        Some(DemandSnapshot {
            reader_id: self.reader_id(),
            producer_next: state.next,
            reader_next: next,
            unread_records: state.next.saturating_sub(next),
            missed_records: first.saturating_sub(next),
            retained_records: state.records.len(),
            alive: cursor.alive.load(Ordering::Acquire),
            closed: state.closed,
        })
    }
    pub(crate) fn reader_id(&self) -> usize {
        self.cursor.as_ptr() as usize
    }

    /// None invalidates closed, evicted or dropped readers. Lock only the queue
    /// metadata; never consume, clone payloads or renew an age.
    pub(crate) fn unread(&self) -> Option<bool> {
        let inner = self.inner.upgrade()?;
        let cursor = self.cursor.upgrade()?;
        let state = inner.state.lock().unwrap();
        let next = cursor.next.load(Ordering::Acquire);
        if state.closed
            || !cursor.alive.load(Ordering::Acquire)
            || state
                .records
                .front()
                .is_some_and(|(first, _)| next < *first)
        {
            return None;
        }
        Some(next < state.next)
    }
}
impl Receiver {
    pub(crate) fn demand_probe(&mut self) -> DemandProbe {
        let cursor = self.demand_cursor.get_or_insert_with(|| {
            Arc::new(DemandCursor {
                next: AtomicU64::new(self.next),
                alive: AtomicBool::new(true),
            })
        });
        DemandProbe {
            inner: Arc::downgrade(&self.inner),
            cursor: Arc::downgrade(cursor),
        }
    }
    fn publish_cursor(&self) {
        if let Some(cursor) = &self.demand_cursor {
            cursor.next.store(self.next, Ordering::Release);
        }
    }
    /// Join live delivery without replaying the failed attempt's backlog.
    pub fn resubscribe(&self) -> Self {
        Channel {
            inner: self.inner.clone(),
        }
        .subscribe()
    }
    /// Inspect eviction without consuming a record during an outstanding paced send.
    pub fn is_lagged(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap()
            .records
            .front()
            .is_some_and(|(first, _)| self.next < *first)
    }
    pub async fn recv(&mut self) -> Result<Bytes, RecvError> {
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut s = self.inner.state.lock().unwrap();
                if let Some(first) = s.records.front().map(|(first, _)| *first) {
                    if self.next < first {
                        let missed = first - self.next;
                        s.move_cursor(self.next, Some(first));
                        self.next = first;
                        self.publish_cursor();
                        return Err(RecvError::Lagged(missed));
                    }
                    if self.next < s.next {
                        let index = (self.next - first) as usize;
                        let bytes = s.records[index].1.clone();
                        s.move_cursor(self.next, Some(self.next + 1));
                        self.next += 1;
                        self.publish_cursor();
                        return Ok(bytes);
                    }
                }
                if s.closed {
                    return Err(RecvError::Closed);
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) struct OwnedPayload(pub(super) Arc<[u8; 4]>);
    impl AsRef<[u8]> for OwnedPayload {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
    }
    #[tokio::test]
    async fn live_byte_limit_evicts_slow_backlog_without_affecting_fast_viewer() {
        let q = Channel::live(100, 8);
        let mut slow = q.subscribe();
        let mut fast = q.subscribe();
        for sequence in 0..6u32 {
            q.send(Bytes::copy_from_slice(&sequence.to_be_bytes()))
                .unwrap();
            assert_eq!(&fast.recv().await.unwrap()[..], &sequence.to_be_bytes());
            assert!(q.retained().1 <= 8);
        }
        assert_eq!(q.retained(), (2, 8));
        assert!(slow.is_lagged());
        assert_eq!(
            slow.recv().await,
            Err(RecvError::Lagged(4)),
            "never silently skip TS"
        );
        drop(slow);
        assert_eq!(q.retained(), (0, 0));
        q.send(Bytes::from_static(b"next")).unwrap();
        assert_eq!(&fast.recv().await.unwrap()[..], b"next");
        assert_eq!(q.retained(), (0, 0));
    }

    #[tokio::test]
    async fn live_record_limit_bounds_metadata_even_for_empty_records() {
        let q = Channel::live(2, 100);
        let mut slow = q.subscribe();
        for _ in 0..3 {
            q.send(Bytes::new()).unwrap();
        }
        assert_eq!(q.retained(), (2, 0));
        assert_eq!(slow.recv().await, Err(RecvError::Lagged(1)));
        drop(slow);
        assert_eq!(q.retained(), (0, 0));
    }

    #[tokio::test]
    async fn live_retry_starts_at_current_position_and_close_drains_then_wakes() {
        let q = Channel::live(8, 100);
        let mut original = q.subscribe();
        q.send(Bytes::from_static(b"old")).unwrap();
        let mut retry = original.resubscribe();
        q.send(Bytes::from_static(b"tail")).unwrap();
        q.close();
        assert_eq!(&retry.recv().await.unwrap()[..], b"tail");
        assert_eq!(&original.recv().await.unwrap()[..], b"old");
        assert_eq!(&original.recv().await.unwrap()[..], b"tail");
        assert_eq!(q.retained(), (0, 0));
        assert_eq!(retry.recv().await, Err(RecvError::Closed));
        assert_eq!(original.recv().await, Err(RecvError::Closed));
        assert_eq!(q.subscribe().recv().await, Err(RecvError::Closed));
        assert!(q.send(Bytes::from_static(b"late")).is_err());
        let q = Channel::live(8, 100);
        let mut waiting = q.subscribe();
        let read = tokio::spawn(async move { waiting.recv().await });
        tokio::task::yield_now().await;
        q.close();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_millis(100), read).await,
            Ok(Ok(Err(RecvError::Closed)))
        ));
    }

    #[tokio::test]
    async fn live_queue_reclaims_only_after_every_viewer_consumes_or_leaves() {
        let q = Channel::live(8, 100);
        let mut fast = q.subscribe();
        let mut slow = q.subscribe();
        q.send(Bytes::from_static(b"first")).unwrap();
        let mut late = q.subscribe();
        q.send(Bytes::from_static(b"second")).unwrap();
        assert_eq!(&fast.recv().await.unwrap()[..], b"first");
        assert_eq!(&fast.recv().await.unwrap()[..], b"second");
        assert_eq!(
            &late.recv().await.unwrap()[..],
            b"second",
            "no history replay"
        );
        assert_eq!(q.retained(), (2, 11), "slow reader still owns backlog");
        assert_eq!(&slow.recv().await.unwrap()[..], b"first");
        assert_eq!(q.retained(), (1, 6), "consumed prefix must be released");
        drop(slow);
        assert_eq!(q.retained(), (0, 0), "remaining viewers are caught up");
        q.send(Bytes::from_static(b"tail")).unwrap();
        drop(fast);
        drop(late);
        assert_eq!(q.retained(), (0, 0));
        q.send(Bytes::from_static(b"unused")).unwrap();
        assert_eq!(q.retained(), (0, 0));
    }

    #[tokio::test]
    async fn live_queue_keeps_read_payload_alive_after_reclamation() {
        let q = Channel::live(8, 100);
        let mut rx = q.subscribe();
        let owner = Arc::new(*b"live");
        let weak = Arc::downgrade(&owner);
        q.send(Bytes::from_owner(OwnedPayload(owner))).unwrap();
        let data = rx.recv().await.unwrap();
        assert_eq!(q.retained(), (0, 0));
        assert!(weak.upgrade().is_some());
        assert_eq!(&data[..], b"live");
        drop(data);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn subscriber_queue_releases_payload_ownership_without_invalidating_a_read() {
        let q = Channel::for_subscribers(2, 8);
        let unused = Arc::new(*b"idle");
        let unused_owner = Arc::downgrade(&unused);
        q.send(Bytes::from_owner(OwnedPayload(unused))).unwrap();
        assert!(unused_owner.upgrade().is_none());

        let mut rx = q.subscribe();
        let used = Arc::new(*b"live");
        let used_owner = Arc::downgrade(&used);
        q.send(Bytes::from_owner(OwnedPayload(used))).unwrap();
        let data = rx.recv().await.unwrap();
        assert_eq!(&data[..], b"live");
        drop(rx);
        assert_eq!(q.retained(), (0, 0));
        assert!(used_owner.upgrade().is_some(), "a body still owns its data");
        drop(data);
        assert!(
            used_owner.upgrade().is_none(),
            "the queue released its owner"
        );
        assert!(q.send(Bytes::from_static(b"oversized")).is_err());
    }

    #[tokio::test]
    async fn clean_close_drains_existing_receivers_but_rejects_new_records() {
        let q = Channel::new(8, 100);
        let mut rx = q.subscribe();
        q.send(Bytes::from_static(b"tail")).unwrap();
        q.close();
        assert_eq!(rx.recv().await.unwrap(), Bytes::from_static(b"tail"));
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await,
            Ok(Err(RecvError::Closed))
        ));
        assert!(q.send(Bytes::from_static(b"late")).is_err());
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_millis(100), q.subscribe().recv()).await,
            Ok(Err(RecvError::Closed))
        ));
    }
    #[tokio::test]
    async fn clean_close_wakes_a_receiver_waiting_for_media() {
        let q = Channel::new(8, 100);
        let mut rx = q.subscribe();
        let reader = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        q.close();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_millis(100), reader).await,
            Ok(Ok(Err(RecvError::Closed)))
        ));
    }
}

#[cfg(test)]
mod demand_tests {
    use super::*;
    #[tokio::test]
    async fn demand_distinguishes_each_reader_without_consuming_records() {
        let queue = Channel::live(4, 100);
        let mut slow = queue.subscribe();
        let mut fast = queue.subscribe();
        let slow_probe = slow.demand_probe();
        let fast_probe = fast.demand_probe();
        assert_eq!(slow_probe.unread(), Some(false));
        queue.send(Bytes::from_static(b"one")).unwrap();
        assert_eq!(slow_probe.unread(), Some(true));
        assert_eq!(fast_probe.unread(), Some(true));
        assert_eq!(fast.recv().await.unwrap(), b"one"[..]);
        assert_eq!(fast_probe.unread(), Some(false));
        assert_eq!(slow_probe.unread(), Some(true));
        assert_eq!(slow.recv().await.unwrap(), b"one"[..]);
        assert_eq!(slow_probe.unread(), Some(false));
        assert_eq!(queue.retained(), (0, 0));
    }
    #[tokio::test]
    async fn demand_invalidates_evicted_closed_and_dropped_readers() {
        let queue = Channel::for_subscribers(1, 8);
        let mut reader = queue.subscribe();
        let probe = reader.demand_probe();
        queue.send(Bytes::from_static(b"one")).unwrap();
        queue.send(Bytes::from_static(b"two")).unwrap();
        assert_eq!(
            probe.unread(),
            None,
            "evicted reader is terminal until resync"
        );
        assert_eq!(reader.recv().await, Err(RecvError::Lagged(1)));
        assert_eq!(probe.unread(), Some(true));
        queue.close();
        assert_eq!(probe.unread(), None, "EOF must not advertise live demand");
        assert_eq!(
            reader.recv().await.unwrap(),
            b"two"[..],
            "EOF drain unchanged"
        );
        let queue = Channel::new(4, 100);
        let mut reader = queue.subscribe();
        let probe = reader.demand_probe();
        drop(reader);
        assert_eq!(probe.unread(), None);
    }
    #[test]
    fn demand_probe_never_keeps_reader_channel_or_payload_alive() {
        let queue = Channel::live(4, 100);
        let mut reader = queue.subscribe();
        let probe = reader.demand_probe();
        let data = Arc::new([1u8; 4]);
        queue
            .send(Bytes::from_owner(super::tests::OwnedPayload(data.clone())))
            .unwrap();
        assert_eq!(Arc::strong_count(&data), 2);
        drop(reader);
        assert_eq!(Arc::strong_count(&data), 1);
        let weak = Arc::downgrade(&queue.inner);
        drop(queue);
        assert!(weak.upgrade().is_none());
        assert_eq!(probe.unread(), None);
    }
}

#[cfg(test)]
mod timing_snapshot_tests {
    use super::*;
    #[tokio::test]
    async fn diagnostic_preserves_absolute_cursor_and_does_not_consume() {
        let q = Channel::new(2, 100);
        q.send(Bytes::from_static(b"before1")).unwrap();
        q.send(Bytes::from_static(b"before2")).unwrap();
        let mut rx = q.subscribe();
        let p = rx.demand_probe();
        q.send(Bytes::from_static(b"live")).unwrap();
        for _ in 0..2 {
            let s = p.diagnostic().expect("live reader metadata");
            assert_eq!(
                (
                    s.producer_next,
                    s.reader_next,
                    s.unread_records,
                    s.missed_records
                ),
                (3, 2, 1, 0)
            );
            assert_eq!(s.reader_id, p.reader_id());
            assert!(s.alive && !s.closed);
            assert_eq!(s.retained_records, 2);
        }
        assert_eq!(rx.recv().await.unwrap(), Bytes::from_static(b"live"));
        assert_eq!(p.diagnostic().unwrap().reader_next, 3);
    }
    #[test]
    fn diagnostic_exposes_current_eviction_and_does_not_keep_reader_alive() {
        let q = Channel::new(2, 100);
        let mut rx = q.subscribe();
        let p = rx.demand_probe();
        for _ in 0..3 {
            q.send(Bytes::from_static(b"x")).unwrap();
        }
        let s = p.diagnostic().expect("evicted cursor remains observable");
        assert_eq!(
            (
                s.producer_next,
                s.reader_next,
                s.unread_records,
                s.missed_records
            ),
            (3, 0, 3, 1)
        );
        q.close();
        assert!(p.diagnostic().unwrap().closed);
        drop(rx);
        assert!(p.diagnostic().is_none());
    }
}
