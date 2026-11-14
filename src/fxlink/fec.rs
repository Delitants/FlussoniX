use super::{
    Budget, Capabilities, Error, Layout, MAX_BLOCKS_PER_RECORD, Reservation, Settings,
    packet::{FRAGMENT_HEADER_LEN, HEADER_LEN, Header},
};
use bytes::Bytes;
use reed_solomon_erasure::galois_8::ReedSolomon;
fn geometry(data: usize, parity: usize, total: usize) -> Result<(), Error> {
    if data == 0 || data > 128 || parity > 128 || data + parity > 256 || total != data + parity {
        Err(Error::InvalidPacket("FEC geometry"))
    } else {
        Ok(())
    }
}
pub fn encode(shards: &mut [Vec<u8>], data: usize, parity: usize) -> Result<(), Error> {
    geometry(data, parity, shards.len())?;
    let len = shards[0].len();
    if len == 0 || len > 1024 || shards.iter().any(|s| s.len() != len) {
        return Err(Error::InvalidPacket("FEC shard width"));
    }
    if parity == 0 {
        return Ok(());
    }
    ReedSolomon::new(data, parity)
        .map_err(|_| Error::Corrupt)?
        .encode(shards)
        .map_err(|_| Error::Corrupt)
}
pub fn reconstruct(
    shards: &mut [Option<Vec<u8>>],
    data: usize,
    parity: usize,
) -> Result<(), Error> {
    geometry(data, parity, shards.len())?;
    let len = shards
        .iter()
        .find_map(|s| s.as_ref().map(Vec::len))
        .ok_or(Error::Corrupt)?;
    if len == 0 || len > 1024 || shards.iter().flatten().any(|s| s.len() != len) {
        return Err(Error::InvalidPacket("FEC shard width"));
    }
    if shards[..data].iter().all(Option::is_some) {
        return Ok(());
    }
    if parity == 0 {
        return Err(Error::Corrupt);
    }
    ReedSolomon::new(data, parity)
        .map_err(|_| Error::Corrupt)?
        .reconstruct_data(shards)
        .map_err(|_| Error::Corrupt)
}
#[derive(Debug)]
pub struct EncodedRecord {
    pub record_id: u64,
    pub created_ms: u64,
    pub datagrams: Vec<Bytes>,
    _reservation: Reservation,
}
pub struct Encoder {
    settings: Settings,
    subscription: u64,
    generation: u64,
    budget: Budget,
    next: u64,
    short_fec: bool,
    capabilities: Capabilities,
}
impl Encoder {
    pub fn new(
        settings: Settings,
        subscription: u64,
        generation: u64,
        budget: Budget,
    ) -> Result<Self, Error> {
        settings.validate()?;
        Ok(Self {
            settings,
            subscription,
            generation,
            budget,
            next: 0,
            short_fec: false,
            capabilities: Capabilities::Legacy,
        })
    }
    /// Explicitly enabled only after compact-format capability negotiation.
    pub fn with_short_record_fec(mut self, enabled: bool) -> Self {
        self.short_fec = enabled;
        self
    }
    /// Enable new layouts only after explicit capability selection.
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self.short_fec = capabilities != Capabilities::Legacy;
        self
    }
    pub fn next_record_id(&self) -> u64 {
        self.next
    }
    pub fn encode(&mut self, record: Bytes, now_ms: u64) -> Result<EncodedRecord, Error> {
        super::validate_record(&record, self.settings.max_record_len)?;
        let s = &self.settings;
        let block_count = s.block_count(record.len());
        if block_count > MAX_BLOCKS_PER_RECORD || block_count > s.max_blocks {
            return Err(Error::BudgetExceeded);
        }
        let next = self
            .next
            .checked_add(1)
            .ok_or(Error::InvalidRecord("record counter overflow"))?;
        self.next
            .checked_mul(MAX_BLOCKS_PER_RECORD as u64)
            .and_then(|v| v.checked_add(block_count as u64))
            .ok_or(Error::InvalidRecord("block counter overflow"))?;
        if matches!(self.capabilities, Capabilities::V6 | Capabilities::V7)
            && let Some(width) = s.trimmed_width(record.len())
        {
            return self.encode_trimmed(record, now_ms, width, next);
        }
        if self.short_fec
            && let Some(width) = s.compact_width(record.len())
        {
            return self.encode_compact(record, now_ms, width, next);
        }
        let datagram_count = (0..block_count)
            .try_fold(0usize, |n, i| {
                let k = s.block_data(record.len(), i);
                n.checked_add(k + k * s.parity_shards / s.data_shards)
            })
            .ok_or(Error::BudgetExceeded)?;
        let charge = datagram_count
            .checked_mul(s.shard_size + HEADER_LEN + 128)
            .and_then(|v| v.checked_add(512))
            .ok_or(Error::BudgetExceeded)?;
        let reservation = self.budget.reserve(charge)?;
        // Includes temporary coded buffers and RS matrix/cache work for one bounded block.
        let _scratch = self
            .budget
            .reserve((s.data_shards + s.parity_shards) * s.shard_size * 2 + 256 * 256 * 4 + 4096)?;
        let common_codec = if s.parity_shards > 0 && s.fragment_count(record.len()) >= s.data_shards
        {
            Some(ReedSolomon::new(s.data_shards, s.parity_shards).map_err(|_| Error::Corrupt)?)
        } else {
            None
        };
        let mut datagrams = Vec::with_capacity(datagram_count);
        for block_index in 0..block_count {
            let k = s.block_data(record.len(), block_index);
            let p = k * s.parity_shards / s.data_shards;
            let mut shards = vec![vec![0u8; s.shard_size]; k + p];
            for (i, shard) in shards.iter_mut().take(k).enumerate() {
                let offset = (block_index * s.data_shards + i) * s.fragment_size();
                let len = (record.len() - offset).min(s.fragment_size());
                shard[..8].copy_from_slice(&self.next.to_be_bytes());
                shard[8..12].copy_from_slice(&(record.len() as u32).to_be_bytes());
                shard[12..16].copy_from_slice(&(offset as u32).to_be_bytes());
                shard[16..18].copy_from_slice(&(len as u16).to_be_bytes());
                shard[FRAGMENT_HEADER_LEN..FRAGMENT_HEADER_LEN + len]
                    .copy_from_slice(&record[offset..offset + len]);
            }
            if k == s.data_shards && p > 0 {
                common_codec
                    .as_ref()
                    .unwrap()
                    .encode(&mut shards)
                    .map_err(|_| Error::Corrupt)?;
            } else {
                encode(&mut shards, k, p)?;
            }
            for (shard_index, shard) in shards.into_iter().enumerate() {
                let h = Header {
                    layout: Layout::Fixed,
                    subscription: self.subscription,
                    generation: self.generation,
                    block_id: self.next * MAX_BLOCKS_PER_RECORD as u64 + block_index as u64,
                    record_id: self.next,
                    record_len: record.len() as u32,
                    block_count: block_count as u32,
                    block_index: block_index as u32,
                    shard_size: s.shard_size as u16,
                    data_count: k as u16,
                    parity_count: p as u16,
                    shard_index: shard_index as u16,
                    configured_data: s.data_shards as u16,
                    configured_parity: s.parity_shards as u16,
                };
                let mut wire = vec![0; HEADER_LEN + s.shard_size];
                h.write(&mut wire)?;
                wire[HEADER_LEN..].copy_from_slice(&shard);
                datagrams.push(Bytes::from(wire));
            }
        }
        let result = EncodedRecord {
            record_id: self.next,
            created_ms: now_ms,
            datagrams,
            _reservation: reservation,
        };
        self.next = next;
        Ok(result)
    }
    fn encode_compact(
        &mut self,
        record: Bytes,
        created_ms: u64,
        width: usize,
        next: u64,
    ) -> Result<EncodedRecord, Error> {
        let reservation = self.budget.reserve(2 * (HEADER_LEN + width + 128) + 512)?;
        let _scratch = self.budget.reserve(2 * width + 512)?;
        let mut shard = vec![0u8; width];
        shard[..8].copy_from_slice(&self.next.to_be_bytes());
        shard[8..12].copy_from_slice(&(record.len() as u32).to_be_bytes());
        shard[16..18].copy_from_slice(&(record.len() as u16).to_be_bytes());
        shard[FRAGMENT_HEADER_LEN..].copy_from_slice(&record);
        let mut datagrams = Vec::with_capacity(2);
        for index in 0..2 {
            let header = Header {
                layout: Layout::Compact,
                subscription: self.subscription,
                generation: self.generation,
                block_id: self.next * MAX_BLOCKS_PER_RECORD as u64,
                record_id: self.next,
                record_len: record.len() as u32,
                block_count: 1,
                block_index: 0,
                shard_size: width as u16,
                data_count: 1,
                parity_count: 1,
                shard_index: index,
                configured_data: self.settings.data_shards as u16,
                configured_parity: self.settings.parity_shards as u16,
            };
            let mut wire = vec![0u8; HEADER_LEN + width];
            header.write(&mut wire)?;
            wire[HEADER_LEN..].copy_from_slice(&shard);
            datagrams.push(Bytes::from(wire));
        }
        let encoded = EncodedRecord {
            record_id: self.next,
            created_ms,
            datagrams,
            _reservation: reservation,
        };
        self.next = next;
        Ok(encoded)
    }
    fn encode_trimmed(
        &mut self,
        record: Bytes,
        created_ms: u64,
        width: usize,
        next: u64,
    ) -> Result<EncodedRecord, Error> {
        let reservation = self.budget.reserve(HEADER_LEN + width + 128 + 512)?;
        let header = Header {
            layout: Layout::Trimmed,
            subscription: self.subscription,
            generation: self.generation,
            block_id: self.next * MAX_BLOCKS_PER_RECORD as u64,
            record_id: self.next,
            record_len: record.len() as u32,
            block_count: 1,
            block_index: 0,
            shard_size: width as u16,
            data_count: 1,
            parity_count: 0,
            shard_index: 0,
            configured_data: self.settings.data_shards as u16,
            configured_parity: self.settings.parity_shards as u16,
        };
        // One charged contiguous allocation: no coded buffers, copy slot or matrix.
        let mut wire = vec![0u8; HEADER_LEN + width];
        header.write(&mut wire)?;
        let shard = &mut wire[HEADER_LEN..];
        shard[..8].copy_from_slice(&self.next.to_be_bytes());
        shard[8..12].copy_from_slice(&(record.len() as u32).to_be_bytes());
        shard[16..18].copy_from_slice(&(record.len() as u16).to_be_bytes());
        shard[FRAGMENT_HEADER_LEN..].copy_from_slice(&record);
        let encoded = EncodedRecord {
            record_id: self.next,
            created_ms,
            datagrams: vec![Bytes::from(wire)],
            _reservation: reservation,
        };
        self.next = next;
        Ok(encoded)
    }
}

#[cfg(test)]
mod trimmed_counter_tests {
    use super::*;
    #[test]
    fn trimmed_tests_counter_overflow_is_rejected_before_allocation() {
        for next in [u64::MAX, u64::MAX / MAX_BLOCKS_PER_RECORD as u64 + 1] {
            let b = Budget::new(1125);
            let mut encoder = Encoder::new(Settings::default(), 9, 3, b.clone())
                .unwrap()
                .with_capabilities(Capabilities::V6);
            encoder.next = next;
            let mut record = vec![0; 401];
            record[..4].copy_from_slice(&397u32.to_be_bytes());
            assert!(encoder.encode(Bytes::from(record), 0).is_err());
            assert_eq!(encoder.next_record_id(), next);
            assert_eq!(b.used(), 0);
        }
    }
}
