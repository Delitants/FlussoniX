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
}
struct Inner {
    state: Mutex<State>,
    changed: Notify,
    max_records: usize,
    max_bytes: usize,
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
        assert!(max_records > 0 && max_bytes > 0);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    records: VecDeque::new(),
                    bytes: 0,
                    next: 0,
                    closed: false,
                }),
                changed: Notify::new(),
                max_records,
                max_bytes,
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
        let next = self.inner.state.lock().unwrap().next;
        Receiver {
            inner: self.inner.clone(),
            next,
        }
    }
    pub fn retained(&self) -> (usize, usize) {
        let s = self.inner.state.lock().unwrap();
        (s.records.len(), s.bytes)
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
