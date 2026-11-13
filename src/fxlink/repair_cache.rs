//! Private immutable repair cache. Canonical order is established while the
//! existing original-send loop parses headers, before a record is published.
use super::{
    Capabilities, EncodedRecord, Error, Layout, MAX_BLOCKS_PER_RECORD, Settings,
    packet::{FRAGMENT_HEADER_LEN, Header},
};
use bytes::Bytes;
use std::ops::Deref;

#[derive(Clone, Copy, Debug)]
struct Geometry {
    blocks: u16,
    stride: u16,
    tail: u16,
}
#[derive(Debug)]
pub(super) struct CachedRecord {
    record: EncodedRecord,
    geometry: Geometry,
}
impl Deref for CachedRecord {
    type Target = EncodedRecord;
    fn deref(&self) -> &EncodedRecord {
        &self.record
    }
}
#[derive(Debug)]
pub(super) struct PendingRecord {
    record: EncodedRecord,
    validation: Validator,
}
#[derive(Debug)]
pub(super) struct Validator {
    first: Header,
    geometry: Geometry,
    total: usize,
    last_data: u16,
    last_parity: u16,
    seen: usize,
}
fn invalid() -> Error {
    Error::InvalidPacket("cached repair geometry/order")
}

impl PendingRecord {
    pub(super) fn new(
        record: EncodedRecord,
        settings: &Settings,
        subscription: u64,
        generation: u64,
        caps: Capabilities,
    ) -> Result<Self, Error> {
        let first = Header::parse(record.datagrams.first().ok_or_else(invalid)?)?;
        first.validate_profile(settings, caps)?;
        if first.record_id != record.record_id
            || first.subscription != subscription
            || first.generation != generation
            || first.block_index != 0
            || first.shard_index != 0
        {
            return Err(invalid());
        }
        let blocks = first.block_count as usize;
        let configured_data = usize::from(first.configured_data);
        let stride = if first.layout == Layout::Fixed {
            configured_data + usize::from(first.configured_parity)
        } else {
            usize::from(first.data_count) + usize::from(first.parity_count)
        };
        let fragments = (first.record_len as usize)
            .div_ceil(usize::from(first.shard_size) - FRAGMENT_HEADER_LEN);
        let before = (blocks - 1)
            .checked_mul(configured_data)
            .ok_or_else(invalid)?;
        let last_data = fragments
            .checked_sub(before)
            .filter(|n| *n > 0 && *n <= configured_data)
            .ok_or_else(invalid)?;
        let last_parity = match first.layout {
            Layout::Compact => 1,
            Layout::Trimmed => 0,
            Layout::Fixed => last_data * usize::from(first.configured_parity) / configured_data,
        };
        let tail = last_data + last_parity;
        let total = (blocks - 1)
            .checked_mul(stride)
            .and_then(|n| n.checked_add(tail))
            .ok_or_else(invalid)?;
        if total != record.datagrams.len() {
            return Err(invalid());
        }
        let geometry = Geometry {
            blocks: u16::try_from(blocks).map_err(|_| invalid())?,
            stride: u16::try_from(stride).map_err(|_| invalid())?,
            tail: u16::try_from(tail).map_err(|_| invalid())?,
        };
        Ok(Self {
            record,
            validation: Validator {
                first,
                geometry,
                total,
                last_data: last_data as u16,
                last_parity: last_parity as u16,
                seen: 0,
            },
        })
    }
    pub(super) fn parts(&mut self) -> (&EncodedRecord, &mut Validator) {
        (&self.record, &mut self.validation)
    }
    pub(super) fn finish(self) -> Result<CachedRecord, Error> {
        if self.validation.seen != self.validation.total {
            return Err(invalid());
        }
        Ok(CachedRecord {
            record: self.record,
            geometry: self.validation.geometry,
        })
    }
}
impl Validator {
    pub(super) fn observe(&mut self, header: &Header) -> Result<(), Error> {
        if self.seen >= self.total {
            return Err(invalid());
        }
        let stride = usize::from(self.geometry.stride);
        let block = (self.seen / stride).min(usize::from(self.geometry.blocks) - 1);
        let last = block + 1 == usize::from(self.geometry.blocks);
        let mut expected = self.first.clone();
        expected.block_id = expected
            .block_id
            .checked_add(block as u64)
            .ok_or_else(invalid)?;
        expected.block_index = block as u32;
        expected.shard_index = (self.seen - block * stride) as u16;
        expected.data_count = if last {
            self.last_data
        } else {
            self.first.configured_data
        };
        expected.parity_count = if last {
            self.last_parity
        } else {
            self.first.configured_parity
        };
        if header != &expected {
            return Err(invalid());
        }
        self.seen += 1;
        Ok(())
    }
}
impl CachedRecord {
    pub(super) fn ordinary_packets(&self, block: u64, mask: &Mask) -> &[Bytes] {
        if mask.is_empty() {
            return &[];
        }
        // Sealing proves the immutable Vec has these exact canonical ranges.
        // Untrusted block IDs are rejected before any index or packet access.
        let base = self.record.record_id * MAX_BLOCKS_PER_RECORD as u64;
        let Some(index) = block.checked_sub(base) else {
            return &[];
        };
        if index >= u64::from(self.geometry.blocks) {
            return &[];
        }
        let start = index as usize * usize::from(self.geometry.stride);
        let width = if index + 1 == u64::from(self.geometry.blocks) {
            self.geometry.tail
        } else {
            self.geometry.stride
        };
        if !mask.any_below(usize::from(width)) {
            return &[];
        }
        &self.record.datagrams[start..start + usize::from(width)]
    }
}
#[derive(Default)]
pub(super) struct Mask([u64; 4]);
impl Mask {
    pub(super) fn new(shards: &[u16]) -> Self {
        let mut mask = Self::default();
        for &shard in shards {
            let n = usize::from(shard);
            if let Some(word) = mask.0.get_mut(n / 64) {
                *word |= 1u64 << (n % 64);
            }
        }
        mask
    }
    pub(super) fn contains(&self, index: u16) -> bool {
        let n = usize::from(index);
        self.0
            .get(n / 64)
            .is_some_and(|word| word & (1u64 << (n % 64)) != 0)
    }
    pub(super) fn is_empty(&self) -> bool {
        self.0 == [0; 4]
    }
    fn any_below(&self, width: usize) -> bool {
        let width = width.min(256);
        let full = width / 64;
        let remaining = width % 64;
        self.0[..full].iter().any(|word| *word != 0)
            || (remaining != 0 && self.0[full] & ((1u64 << remaining) - 1) != 0)
    }
}
#[cfg(test)]
#[path = "repair_cache_tests.rs"]
mod tests;
