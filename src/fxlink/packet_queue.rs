//! Pending local shards are not network loss. Their ownership follows decoding.
use super::{Budget, Reservation, packet::Header, transport::Charged};
use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::mpsc;

pub(super) const CAPACITY: usize = 256;
// Includes the route, a 32-packet decode batch and prepared/deferred packets.
// Enforce a separate ceiling even if a future consumer holds more packets.
const MAX_PENDING: usize = 512;
// Covers the preallocated hash buckets, duplicate counts and shared ownership.
const INDEX_BYTES: usize = 64 * 1024;
type Key = (u64, u16);

pub(super) struct Entries {
    shards: HashMap<Key, u16>,
    packets: usize,
}
impl Entries {
    pub(super) fn contains_record(&self, record: u64) -> bool {
        self.shards
            .keys()
            .any(|(block, _)| *block / super::MAX_BLOCKS_PER_RECORD as u64 == record)
    }
    pub(super) fn contains(&self, block: u64, shard: u16) -> bool {
        self.shards.contains_key(&(block, shard))
    }
}
pub(super) struct Pending {
    entries: Mutex<Entries>,
    _charge: Reservation,
}
impl Pending {
    pub(super) fn lock(&self) -> MutexGuard<'_, Entries> {
        self.entries.lock().unwrap()
    }
}
pub(super) struct Sender {
    tx: mpsc::Sender<Packet>,
    pending: Arc<Pending>,
}
pub(super) struct Ingress {
    pub(super) packets: mpsc::Receiver<Packet>,
    pub(super) pending: Arc<Pending>,
}
pub(super) enum SendError {
    Full,
    Closed,
    Limit,
}
pub(super) fn channel(budget: &Budget) -> Result<(Sender, Ingress), String> {
    let charge = budget.reserve(INDEX_BYTES).map_err(|e| e.to_string())?;
    let pending = Arc::new(Pending {
        entries: Mutex::new(Entries {
            shards: HashMap::with_capacity(MAX_PENDING),
            packets: 0,
        }),
        _charge: charge,
    });
    let (tx, packets) = mpsc::channel(CAPACITY);
    Ok((
        Sender {
            tx,
            pending: pending.clone(),
        },
        Ingress { packets, pending },
    ))
}
impl Sender {
    pub(super) fn try_send(&self, packet: Charged, header: &Header) -> Result<(), SendError> {
        let slot = self.tx.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => SendError::Full,
            mpsc::error::TrySendError::Closed(_) => SendError::Closed,
        })?;
        let key = (header.block_id, header.shard_index);
        {
            let mut entries = self.pending.lock();
            if entries.packets >= MAX_PENDING {
                return Err(SendError::Limit);
            }
            entries.packets += 1;
            *entries.shards.entry(key).or_default() += 1;
        }
        // Register before publication; never suppress a packet rejected by the
        // bounded route. The permit cannot fail after registration.
        slot.send(Packet {
            packet,
            key,
            pending: self.pending.clone(),
        });
        Ok(())
    }
}
pub(super) struct Packet {
    packet: Charged,
    key: Key,
    pending: Arc<Pending>,
}
impl Deref for Packet {
    type Target = Charged;
    fn deref(&self) -> &Charged {
        &self.packet
    }
}
impl Drop for Packet {
    fn drop(&mut self) {
        let mut entries = self.pending.lock();
        entries.packets -= 1;
        let count = entries.shards.get_mut(&self.key).unwrap();
        *count -= 1;
        if *count == 0 {
            entries.shards.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fxlink::{Encoder, Settings};
    use bytes::Bytes;

    fn shard(len: usize) -> (Bytes, Header) {
        let mut bytes = vec![17; len];
        bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
        let encoded = Encoder::new(Settings::default(), 0, 1, Budget::new(1 << 20))
            .unwrap()
            .encode(Bytes::from(bytes), 0)
            .unwrap();
        let bytes = encoded.datagrams[0].clone();
        let header = Header::parse(&bytes).unwrap();
        (bytes, header)
    }

    #[test]
    fn duplicate_presence_follows_last_owner_and_is_scoped_to_its_route() {
        let budget = Budget::new(1 << 20);
        let (tx, mut rx) = channel(&budget).unwrap();
        let (other_tx, other_rx) = channel(&budget).unwrap();
        let (bytes, header) = shard(1000);
        for _ in 0..2 {
            assert!(
                tx.try_send(Charged::new(bytes.clone(), &budget).unwrap(), &header)
                    .is_ok()
            );
        }
        let first = rx.packets.try_recv().unwrap();
        let second = rx.packets.try_recv().unwrap();
        assert!(rx.pending.lock().contains(0, 0));
        assert!(!rx.pending.lock().contains(0, 1));
        assert!(!rx.pending.lock().contains(2048, 0));
        assert!(!other_rx.pending.lock().contains(0, 0));
        drop(first);
        assert!(rx.pending.lock().contains(0, 0));
        drop(second);
        assert!(!rx.pending.lock().contains(0, 0));
        drop((tx, rx, other_tx, other_rx));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn queued_unknown_head_does_not_hide_missing_siblings_or_charge_skipped_history() {
        use crate::fxlink::{Receiver, repair::Pacing};
        let budget = Budget::new(2 << 20);
        let (tx, mut rx) = channel(&budget).unwrap();
        let (bytes, header) = shard(20000);
        for _ in 0..2 {
            assert!(
                tx.try_send(Charged::new(bytes.clone(), &budget).unwrap(), &header)
                    .is_ok()
            );
        }
        let first = rx.packets.try_recv().unwrap();
        let second = rx.packets.try_recv().unwrap();
        let mut receiver = Receiver::new(Settings::default(), 0, 1, budget.clone()).unwrap();
        // Record1 proves unknown record0 existed. Its queued shard0 must not
        // suppress the rest of the fixed unknown-head prefix.
        receiver.announce(1, 1000, 0).unwrap();
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        let query = |receiver: &Receiver, pacing: &Pacing| {
            let present = rx.pending.lock();
            receiver.repair_requests_avoiding(
                20,
                1,
                5,
                |block, shard| pacing.eligible(block, shard, 20),
                |block, shard| present.contains(block, shard),
            )
        };
        let requests = query(&receiver, &pacing);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].block_id, 0);
        assert_eq!(
            requests[0].shard_indices,
            [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
            ]
        );
        assert_eq!(requests[1].block_id, 2048);
        assert_eq!(requests[1].shard_indices, [0]);
        for request in &requests {
            pacing
                .sent(request.block_id, &request.shard_indices, 20, 1)
                .unwrap();
        }
        drop(first);
        assert!(query(&receiver, &pacing).is_empty());
        // The last local owner disappearing makes shard0 immediately eligible,
        // even while the actual sent siblings remain in their retry cooldown.
        drop(second);
        let requests = query(&receiver, &pacing);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].block_id, 0);
        assert_eq!(requests[0].shard_indices, [0]);
        drop((receiver, pacing, tx, rx));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn intact_queued_prefix_does_not_steal_the_only_repair_history_slot() {
        use crate::fxlink::{Receiver, repair::Pacing};
        let budget = Budget::new(2 << 20);
        let (tx, rx) = channel(&budget).unwrap();
        let mut bytes = vec![17; 20000];
        bytes[..4].copy_from_slice(&19996u32.to_be_bytes());
        let encoded = Encoder::new(Settings::default(), 0, 1, Budget::new(1 << 20))
            .unwrap()
            .encode(Bytes::from(bytes), 0)
            .unwrap();
        for bytes in encoded.datagrams {
            let header = Header::parse(&bytes).unwrap();
            assert!(
                tx.try_send(Charged::new(bytes, &budget).unwrap(), &header)
                    .is_ok()
            );
        }
        let mut receiver = Receiver::new(Settings::default(), 0, 1, budget.clone()).unwrap();
        receiver.announce(1, 1000, 0).unwrap();
        let pacing = Pacing::new(budget.clone(), 1).unwrap();
        let requests = {
            let present = rx.pending.lock();
            let eligible = pacing.batch_eligible(20);
            receiver.repair_requests_avoiding(20, 1, 5, eligible, |block, shard| {
                present.contains(block, shard)
            })
        };
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].block_id, 2048);
        assert_eq!(requests[0].shard_indices, [0]);
        drop((receiver, pacing, tx, rx));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn full_queue_rejects_without_retaining_a_false_loss_suppression() {
        let budget = Budget::new(1 << 20);
        let (tx, mut rx) = channel(&budget).unwrap();
        let (bytes, mut header) = shard(1000);
        for _ in 0..CAPACITY {
            assert!(
                tx.try_send(Charged::new(bytes.clone(), &budget).unwrap(), &header)
                    .is_ok()
            );
        }
        header.block_id = 2048;
        let before = budget.used();
        assert!(matches!(
            tx.try_send(Charged::new(bytes, &budget).unwrap(), &header),
            Err(SendError::Full)
        ));
        assert_eq!(budget.used(), before);
        assert!(!rx.pending.lock().contains(2048, 0));
        let held = rx.packets.try_recv().unwrap();
        drop((tx, rx));
        assert!(budget.used() > INDEX_BYTES);
        drop(held);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn pending_limit_and_parent_budget_release_after_held_work_is_dropped() {
        let parent = Budget::new(2 << 20);
        let budget = parent.child(1 << 20);
        let (tx, mut rx) = channel(&budget).unwrap();
        let (bytes, header) = shard(1000);
        let mut held = Vec::new();
        for _ in 0..MAX_PENDING {
            assert!(
                tx.try_send(Charged::new(bytes.clone(), &budget).unwrap(), &header)
                    .is_ok()
            );
            held.push(rx.packets.try_recv().unwrap());
        }
        let before = parent.used();
        assert!(matches!(
            tx.try_send(Charged::new(bytes.clone(), &budget).unwrap(), &header),
            Err(SendError::Limit)
        ));
        assert_eq!(parent.used(), before);
        drop(held.pop());
        assert!(
            tx.try_send(Charged::new(bytes, &budget).unwrap(), &header)
                .is_ok()
        );
        drop((held, tx, rx));
        assert_eq!(budget.used(), 0);
        assert_eq!(parent.used(), 0);
        let tiny = Budget::new(INDEX_BYTES - 1);
        assert!(channel(&tiny).is_err());
        assert_eq!(tiny.used(), 0);
    }
}
