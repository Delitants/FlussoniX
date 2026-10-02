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
    pub fn new(max_records: usize, max_bytes: usize) -> Self {
        assert!(max_records > 0 && max_bytes > 0);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    records: VecDeque::new(),
                    bytes: 0,
                    next: 0,
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
            }
            notified.await;
        }
    }
}
