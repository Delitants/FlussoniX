//! Borrowed, bounded source work for an explicit bootstrap request.
use super::{EncodedRecord, Error, packet::Header};
use bytes::Bytes;

#[derive(Default)]
pub(super) struct Gate {
    next_bootstrap_at: u64,
}
impl Gate {
    pub(super) fn admit(&mut self, at: u64) -> bool {
        let Some(next) = at.checked_add(20) else {
            return false;
        };
        if at < self.next_bootstrap_at {
            return false;
        }
        self.next_bootstrap_at = next;
        true
    }
}
pub(super) fn age_valid(created: u64, at: u64, latency: u64) -> bool {
    at.checked_sub(created)
        .is_some_and(|age| age.saturating_add(5) < latency)
}

pub(super) struct Replies<'a> {
    packets: std::slice::Iter<'a, Bytes>,
    block_id: u64,
    remaining: u16,
    data_seen: u16,
    done: bool,
}
pub(super) fn packets(record: &EncodedRecord, block_id: u64, reply_limit: u16) -> Replies<'_> {
    Replies {
        packets: record.datagrams.iter(),
        block_id,
        remaining: reply_limit,
        data_seen: 0,
        done: false,
    }
}
impl<'a> Iterator for Replies<'a> {
    type Item = Result<&'a Bytes, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.remaining == 0 {
            return None;
        }
        let packet = self.packets.next()?;
        let header = match Header::parse(packet) {
            Ok(header) => header,
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        };
        if header.block_id != self.block_id {
            self.done = true;
            return None;
        }
        // Encoder caches each block's ordered systematic prefix before parity.
        // Stop on inconsistent cache order instead of expanding another block.
        if header.shard_index != self.data_seen {
            self.done = true;
            return Some(Err(Error::InvalidPacket("cached bootstrap order")));
        }
        if header.shard_index < header.data_count {
            self.data_seen += 1;
            if self.data_seen == header.data_count && header.parity_count == 0 {
                self.done = true;
            }
        } else {
            self.done = true;
        }
        self.remaining -= 1;
        Some(Ok(packet))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fxlink::{Budget, Capabilities, Encoder, Settings, packet::Header};
    use bytes::Bytes;

    fn record(len: usize) -> Bytes {
        let mut bytes = vec![8; len];
        bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
        Bytes::from(bytes)
    }
    #[test]
    fn cached_first_block_is_borrowed_bounded_and_data_first() {
        let settings = Settings::default();
        let budget = Budget::new(4 << 20);
        let mut encoder = Encoder::new(settings.clone(), 9, 1, budget.clone())
            .unwrap()
            .with_capabilities(Capabilities::V7);
        let encoded = encoder.encode(record(9781), 100).unwrap();
        let selected = packets(&encoded, 0, 20)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|p| Header::parse(p).unwrap().shard_index)
                .collect::<Vec<_>>(),
            (0..=10).collect::<Vec<_>>()
        );
        assert!(
            selected
                .iter()
                .zip(&encoded.datagrams)
                .all(|(selected, original)| std::ptr::eq(*selected, original))
        );
        assert_eq!(
            packets(&encoded, 0, 2)
                .map(|p| Header::parse(p.unwrap()).unwrap().shard_index)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(packets(&encoded, 2048, 20).count(), 0);
        drop(selected);
        drop(encoded);
        drop(encoder);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn multi_block_and_short_records_do_not_expand_beyond_the_first_block() {
        for parity in [0, 4] {
            let settings = Settings {
                parity_shards: parity,
                ..Settings::default()
            };
            let budget = Budget::new(4 << 20);
            let mut encoder = Encoder::new(settings, 9, 1, budget.clone())
                .unwrap()
                .with_capabilities(Capabilities::V7)
                .with_short_record_fec(true);
            for len in [12, 387, 800, 1004, 9781, 40000] {
                let encoded = encoder.encode(record(len), 100).unwrap();
                let block = encoded.record_id * 2048;
                let selected = packets(&encoded, block, 20)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert!(!selected.is_empty());
                assert!(selected.len() <= 20);
                assert!(
                    selected
                        .iter()
                        .all(|p| Header::parse(p).unwrap().block_id == block)
                );
                let parity_count = selected
                    .iter()
                    .filter(|p| {
                        let h = Header::parse(p).unwrap();
                        h.shard_index >= h.data_count
                    })
                    .count();
                assert!(parity_count <= 1);
                if len == 12 && parity == 4 {
                    assert_eq!(selected.len(), 2);
                }
                if len == 800 {
                    assert_eq!(selected.len(), 1);
                }
            }
            drop(encoder);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn source_gate_counts_cache_misses_and_never_wraps() {
        let mut gate = Gate::default();
        assert_eq!(
            [gate.admit(100), gate.admit(119), gate.admit(120)],
            [true, false, true]
        );
        // The validated attempt at120 may miss the cache; the next sequence
        // still cannot expand another request during the same20ms interval.
        assert!(!gate.admit(120));
        assert!(!gate.admit(139));
        assert!(gate.admit(140));
        assert!(!gate.admit(u64::MAX));
        assert!(!gate.admit(u64::MAX));
    }
    #[test]
    fn original_source_age_margin_is_strict() {
        assert!(age_valid(1000, 1494, 500));
        assert!(!age_valid(1000, 1495, 500));
        assert!(!age_valid(1000, 1500, 500));
        assert!(!age_valid(1000, 999, 500));
        assert!(age_valid(0, 1494, 1500));
        assert!(!age_valid(0, 1495, 1500));
        assert!(!age_valid(0, u64::MAX, 1500));
    }
}
