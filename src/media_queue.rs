//! One immutable payload ring shared by all viewers, bounded by both bytes and records.
use bytes::Bytes;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, broadcast::error::RecvError};
struct State {
    records: VecDeque<(u64, Bytes)>,
    bytes: usize,
    next: u64,
    closed: bool,
    receivers: usize,
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
        Receiver {
            inner: self.inner.clone(),
            next: state.next,
        }
    }
    pub fn retained(&self) -> (usize, usize) {
        let s = self.inner.state.lock().unwrap();
        (s.records.len(), s.bytes)
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.receivers -= 1;
        if state.receivers == 0 && !self.inner.retain_without_receivers {
            state.records.clear();
            state.bytes = 0;
        }
    }
}
impl Receiver {
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
                let s = self.inner.state.lock().unwrap();
                if let Some((first, _)) = s.records.front() {
                    if self.next < *first {
                        let missed = *first - self.next;
                        self.next = *first;
                        return Err(RecvError::Lagged(missed));
                    }
                    if self.next < s.next {
                        let index = (self.next - *first) as usize;
                        let bytes = s.records[index].1.clone();
                        self.next += 1;
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
    struct OwnedPayload(Arc<[u8; 4]>);
    impl AsRef<[u8]> for OwnedPayload {
        fn as_ref(&self) -> &[u8] {
            self.0.as_ref()
        }
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
