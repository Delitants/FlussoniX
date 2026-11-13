use super::{
    Budget, Capabilities, Error, Layout, MAX_BLOCKS_PER_RECORD, Reservation, Settings,
    packet::{FRAGMENT_HEADER_LEN, HEADER_LEN, Header},
};
use bytes::Bytes;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::collections::BTreeMap;
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub received_shards: u64,
    pub trimmed_shards: u64,
    pub duplicate_shards: u64,
    pub original_lost_shards: u64,
    pub fec_recovered_shards: u64,
    pub expired_records: u64,
    pub late_shards: u64,
    pub unprotected_blocks: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairRequest {
    pub block_id: u64,
    pub shard_indices: Vec<u16>,
}
struct Block {
    shards: Vec<Option<Vec<u8>>>,
    _reservation: Reservation,
}
enum BlockState {
    Missing,
    Open(Block),
    Done,
}
struct Record {
    layout: Option<Layout>,
    bytes: Vec<u8>,
    blocks: Vec<BlockState>,
    deadline: u64,
    complete: usize,
    _reservation: Reservation,
}
struct DecodeCodec {
    codec: ReedSolomon,
    uses: u8,
    _reservation: Reservation,
}
/// A completed record whose reassembly reservation follows it into downstream queues.
/// Retain this owner while retaining its bytes; legacy `ingest` deliberately releases it.
#[derive(Debug)]
pub struct ReceivedRecord {
    pub bytes: Bytes,
    pub deadline_ms: u64,
    pub(crate) reservation: Reservation,
}
pub struct Receiver {
    capabilities: Capabilities,
    settings: Settings,
    subscription: u64,
    generation: u64,
    budget: Budget,
    records: BTreeMap<u64, Record>,
    next: u64,
    waiting_deadline: Option<u64>,
    retired: bool,
    stats: Stats,
    common_codec: Option<DecodeCodec>,
}
impl Receiver {
    pub fn new(
        settings: Settings,
        subscription: u64,
        generation: u64,
        budget: Budget,
    ) -> Result<Self, Error> {
        settings.validate()?;
        Ok(Self {
            capabilities: Capabilities::Legacy,
            settings,
            subscription,
            generation,
            budget,
            records: BTreeMap::new(),
            next: 0,
            waiting_deadline: None,
            retired: false,
            stats: Stats::default(),
            common_codec: None,
        })
    }
    /// Enable only for an explicitly negotiated compact-capable peer.
    pub fn with_short_record_fec(mut self, enabled: bool) -> Self {
        self.capabilities = enabled.into();
        self
    }
    /// Select the complete layout capability set from the negotiated peer.
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    pub fn is_retired(&self) -> bool {
        self.retired
    }
    pub(super) fn unknown_middle(&self, block_id: u64) -> bool {
        let stride = MAX_BLOCKS_PER_RECORD as u64;
        let record = block_id / stride;
        block_id % stride == 0
            && record > self.next
            && !self.records.contains_key(&record)
            && self
                .records
                .range((
                    std::ops::Bound::Excluded(record),
                    std::ops::Bound::Unbounded,
                ))
                .next()
                .is_some()
    }
    pub fn cancel(&mut self) {
        self.retired = true;
        self.records.clear();
        self.common_codec = None;
        self.waiting_deadline = None;
    }
    fn fail<T>(&mut self, error: Error) -> Result<T, Error> {
        self.cancel();
        Err(error)
    }
    pub fn tick(&mut self, now_ms: u64) -> Result<(), Error> {
        if self.retired {
            return Err(Error::Retired);
        }
        let expired = self
            .waiting_deadline
            .filter(|d| now_ms >= *d)
            .map(|_| self.next)
            .or_else(|| {
                self.records
                    .iter()
                    .find(|(_, r)| r.complete < r.blocks.len() && now_ms >= r.deadline)
                    .map(|(id, _)| *id)
            });
        if let Some(record_id) = expired {
            tracing::debug!(
                record_id,
                now_ms,
                pending_records = self.records.len(),
                head_present = self.records.contains_key(&record_id),
                first_pending = self.records.keys().next().copied(),
                last_pending = self.records.keys().next_back().copied(),
                "FXLink record expiry state"
            );
            if let Some(record) = self.records.get(&record_id) {
                for (index, state) in record.blocks.iter().enumerate() {
                    let data = self.settings.block_data(record.bytes.len(), index);
                    let missing = match state {
                        BlockState::Missing => data,
                        BlockState::Open(block) => {
                            block.shards[..data].iter().filter(|s| s.is_none()).count()
                        }
                        BlockState::Done => 0,
                    };
                    self.stats.original_lost_shards += missing as u64;
                }
            }
            self.stats.expired_records += 1;
            return self.fail(Error::Expired { record_id });
        }
        Ok(())
    }
    pub fn announce(
        &mut self,
        record_id: u64,
        record_len: usize,
        now_ms: u64,
    ) -> Result<(), Error> {
        self.tick(now_ms)?;
        if record_id < self.next {
            self.stats.late_shards += 1;
            return Ok(());
        }
        if record_len < 12
            || record_len > self.settings.max_record_len
            || record_id
                .checked_mul(MAX_BLOCKS_PER_RECORD as u64)
                .is_none()
        {
            return self.fail(Error::InvalidRecord("announcement bounds"));
        }
        if let Some(r) = self.records.get(&record_id) {
            if r.bytes.len() != record_len {
                return self.fail(Error::InvalidPacket("conflicting record announcement"));
            }
            return Ok(());
        }
        let count = self.settings.block_count(record_len);
        if self.records.len() >= self.settings.max_records
            || count > self.settings.max_blocks
            || count > MAX_BLOCKS_PER_RECORD
            || self.records.values().map(|r| r.blocks.len()).sum::<usize>() + count
                > self.settings.max_blocks
        {
            return self.fail(Error::BudgetExceeded);
        }
        let charge = record_len
            .checked_add(count * 128 + 512)
            .ok_or(Error::BudgetExceeded)?;
        let reservation = match self.budget.reserve(charge) {
            Ok(v) => v,
            Err(e) => return self.fail(e),
        };
        let deadline = now_ms.saturating_add(self.settings.latency_ms);
        // First observation of a gap, including a future record, sets its deadline.
        self.waiting_deadline = Some(self.waiting_deadline.map_or(deadline, |d| d.min(deadline)));
        self.records.insert(
            record_id,
            Record {
                layout: None,
                bytes: vec![0; record_len],
                blocks: (0..count).map(|_| BlockState::Missing).collect(),
                deadline,
                complete: 0,
                _reservation: reservation,
            },
        );
        Ok(())
    }
    pub fn ingest(&mut self, datagram: &[u8], now_ms: u64) -> Result<Vec<Bytes>, Error> {
        self.ingest_charged(datagram, now_ms)
            .map(|records| records.into_iter().map(|record| record.bytes).collect())
    }
    /// Transfer existing reservations without a release/re-reserve gap.
    pub fn ingest_charged(
        &mut self,
        datagram: &[u8],
        now_ms: u64,
    ) -> Result<Vec<ReceivedRecord>, Error> {
        if self.retired {
            return Err(Error::Retired);
        }
        let h = Header::parse(datagram)?;
        if h.subscription != self.subscription || h.generation != self.generation {
            return Err(Error::Stale);
        }
        self.tick(now_ms)?;
        if h.validate_profile(&self.settings, self.capabilities)
            .is_err()
        {
            return self.fail(Error::InvalidPacket("unnegotiated profile"));
        }
        if h.record_id < self.next {
            self.stats.late_shards += 1;
            return Ok(Vec::new());
        }
        self.announce(h.record_id, h.record_len as usize, now_ms)?;
        let index = h.block_index as usize;
        let r = self.records.get_mut(&h.record_id).unwrap();
        if r.layout.is_some_and(|layout| layout != h.layout) {
            return self.fail(Error::InvalidPacket("conflicting record format"));
        }
        r.layout = Some(h.layout);
        if matches!(r.blocks[index], BlockState::Done) {
            if h.layout == Layout::Trimmed {
                // Completed future records remain retained behind an older gap.
                // Their single shard must still reject contradictory duplicates.
                let mut prefix = [0u8; FRAGMENT_HEADER_LEN];
                prefix[..8].copy_from_slice(&h.record_id.to_be_bytes());
                prefix[8..12].copy_from_slice(&h.record_len.to_be_bytes());
                prefix[16..18].copy_from_slice(&(r.bytes.len() as u16).to_be_bytes());
                let shard = &datagram[HEADER_LEN..];
                if shard[..FRAGMENT_HEADER_LEN] != prefix || shard[FRAGMENT_HEADER_LEN..] != r.bytes
                {
                    return self.fail(Error::InvalidPacket("conflicting duplicate shard"));
                }
            }
            self.stats.duplicate_shards += 1;
            return Ok(Vec::new());
        }
        if matches!(r.blocks[index], BlockState::Missing) {
            let total = h.data_count as usize + h.parity_count as usize;
            // Charge all received/reconstructed shard storage and slot metadata before allocation.
            let charge = total * (h.shard_size as usize * 2 + 128) + 512;
            let reservation = match self.budget.reserve(charge) {
                Ok(v) => v,
                Err(e) => return self.fail(e),
            };
            r.blocks[index] = BlockState::Open(Block {
                shards: vec![None; total],
                _reservation: reservation,
            });
        }
        let BlockState::Open(block) = &mut r.blocks[index] else {
            unreachable!()
        };
        let slot = &mut block.shards[h.shard_index as usize];
        if let Some(previous) = slot {
            if previous.as_slice() != &datagram[HEADER_LEN..] {
                return self.fail(Error::InvalidPacket("conflicting duplicate shard"));
            }
            self.stats.duplicate_shards += 1;
            return Ok(Vec::new());
        }
        *slot = Some(datagram[HEADER_LEN..].to_vec());
        self.stats.received_shards += 1;
        if h.layout == Layout::Trimmed {
            self.stats.trimmed_shards += 1;
        }
        let k = h.data_count as usize;
        let received = block.shards.iter().filter(|s| s.is_some()).count();
        if received < k {
            return Ok(Vec::new());
        }
        let missing = block.shards[..k].iter().filter(|s| s.is_none()).count();
        if missing > 0 {
            if h.layout == Layout::Compact {
                // One systematic shard: its parity is the same protected bytes.
                // Block admission already reserved the reconstructed data copy.
                block.shards[0] = block.shards[1].clone();
            } else {
                // Bound transient matrix construction independently of buffered blocks.
                let _scratch = match self.budget.reserve(256 * 256 * 4 + 4096) {
                    Ok(v) => v,
                    Err(e) => return self.fail(e),
                };
                let result = if k == self.settings.data_shards {
                    // The library keeps a 254-entry decode cache. Rebuild after eight
                    // recoveries so its retained matrices fit this explicit reservation.
                    if self.common_codec.as_ref().is_some_and(|c| c.uses >= 8) {
                        self.common_codec = None;
                    }
                    if self.common_codec.is_none() {
                        let reservation = match self.budget.reserve(256 * 256 * 4 + 4096) {
                            Ok(v) => v,
                            Err(e) => return self.fail(e),
                        };
                        let codec = match ReedSolomon::new(k, h.parity_count as usize) {
                            Ok(v) => v,
                            Err(_) => return self.fail(Error::Corrupt),
                        };
                        self.common_codec = Some(DecodeCodec {
                            codec,
                            uses: 0,
                            _reservation: reservation,
                        });
                    }
                    let codec = self.common_codec.as_mut().unwrap();
                    codec.uses += 1;
                    codec
                        .codec
                        .reconstruct_data(&mut block.shards)
                        .map_err(|_| Error::Corrupt)
                } else {
                    super::fec::reconstruct(&mut block.shards, k, h.parity_count as usize)
                };
                if let Err(e) = result {
                    return self.fail(e);
                }
            }
            self.stats.original_lost_shards += missing as u64;
            self.stats.fec_recovered_shards += missing as u64;
        }
        for (i, shard) in block.shards.iter().take(k).enumerate() {
            let shard = shard.as_ref().unwrap();
            let id = u64::from_be_bytes(shard[..8].try_into().unwrap());
            let total = u32::from_be_bytes(shard[8..12].try_into().unwrap()) as usize;
            let offset = u32::from_be_bytes(shard[12..16].try_into().unwrap()) as usize;
            let len = u16::from_be_bytes(shard[16..18].try_into().unwrap()) as usize;
            let expected_offset =
                (index * self.settings.data_shards + i) * self.settings.fragment_size();
            let expected_len = (r.bytes.len() - expected_offset).min(self.settings.fragment_size());
            if id != h.record_id
                || total != r.bytes.len()
                || offset != expected_offset
                || len != expected_len
                || shard[18..20] != [0, 0]
                || shard[FRAGMENT_HEADER_LEN + len..].iter().any(|v| *v != 0)
            {
                return self.fail(Error::Corrupt);
            }
            r.bytes[offset..offset + len]
                .copy_from_slice(&shard[FRAGMENT_HEADER_LEN..FRAGMENT_HEADER_LEN + len]);
        }
        if h.parity_count == 0 {
            self.stats.unprotected_blocks += 1;
        }
        r.blocks[index] = BlockState::Done;
        r.complete += 1;

        let mut out = Vec::new();
        while self
            .records
            .get(&self.next)
            .is_some_and(|r| r.complete == r.blocks.len())
        {
            let r = self.records.remove(&self.next).unwrap();
            if let Err(e) = super::validate_record(&r.bytes, self.settings.max_record_len) {
                return self.fail(e);
            }
            out.push(ReceivedRecord {
                bytes: Bytes::from(r.bytes),
                deadline_ms: r.deadline,
                reservation: r._reservation,
            });
            self.next += 1;
            self.waiting_deadline = self.records.values().map(|r| r.deadline).min();
        }
        Ok(out)
    }
    pub fn repair_requests(
        &self,
        now_ms: u64,
        rtt_ms: u64,
        scheduling_ms: u64,
    ) -> Vec<RepairRequest> {
        self.repair_requests_if(now_ms, rtt_ms, scheduling_ms, |_, _| true)
    }
    pub(super) fn repair_requests_if(
        &self,
        now_ms: u64,
        rtt_ms: u64,
        scheduling_ms: u64,
        eligible: impl FnMut(u64, u16) -> bool,
    ) -> Vec<RepairRequest> {
        self.repair_requests_avoiding(now_ms, rtt_ms, scheduling_ms, eligible, |_, _| false)
    }
    pub(super) fn repair_requests_avoiding(
        &self,
        now: u64,
        rtt: u64,
        scheduling: u64,
        eligible: impl FnMut(u64, u16) -> bool,
        pending: impl FnMut(u64, u16) -> bool,
    ) -> Vec<RepairRequest> {
        self.repair_requests_with_shapes(now, rtt, scheduling, eligible, pending, |_| None)
    }
    fn repair_requests_baseline(
        &self,
        now_ms: u64,
        rtt_ms: u64,
        scheduling_ms: u64,
        mut eligible: impl FnMut(u64, u16) -> bool,
        mut pending: impl FnMut(u64, u16) -> bool,
    ) -> Vec<RepairRequest> {
        if self.retired {
            return Vec::new();
        }
        let Some(arrival) = now_ms
            .checked_add(rtt_ms)
            .and_then(|v| v.checked_add(scheduling_ms))
        else {
            return Vec::new();
        };
        if self.waiting_deadline.is_some_and(|d| arrival >= d) {
            return Vec::new();
        }
        let mut left = self.settings.max_repair_shards;
        let mut out = Vec::new();
        let mut parity = Vec::new();
        let used_blocks = self.records.values().map(|r| r.blocks.len()).sum::<usize>();
        let mut probe_slots = self
            .settings
            .max_records
            .saturating_sub(self.records.len())
            .min(self.settings.max_blocks.saturating_sub(used_blocks));
        if self.waiting_deadline.is_some()
            && !self.records.contains_key(&self.next)
            && probe_slots == 0
        {
            // Returning the oldest missing record must fit before spending
            // quota or retry history on any later record.
            return out;
        }
        let mut cursor = self.next;
        let mut scans_left = self.settings.max_records;
        for (id, r) in &self.records {
            // A known successor proves intervening records existed, including
            // gaps behind an older blocked head. Bound numeric gap traversal
            // independently of an authenticated peer's record-ID distance.
            while cursor < *id && probe_slots > 0 && scans_left > 0 {
                if left == 0 {
                    return out;
                }
                probe_slots -= 1;
                scans_left -= 1;
                let block_id = cursor * MAX_BLOCKS_PER_RECORD as u64;
                cursor += 1;
                // Reserve a slot even during cooldown: another speculative gap
                // cannot consume capacity needed by an older in-flight probe.
                let prefix = if block_id == self.next * MAX_BLOCKS_PER_RECORD as u64 {
                    self.settings
                        .data_shards
                        .min(self.settings.max_repair_shards)
                } else {
                    // One header is enough to learn a speculative middle gap's
                    // shape. Its prefix cannot grow when earlier repairs stop
                    // consuming the shared batch cap during their cooldown.
                    1
                };
                // Pending shard0 must not suppress a genuinely absent shard1.
                // Keep the original block/probe cooldown gate independent of
                // per-shard local ownership, and spend no history on an intact
                // locally queued prefix.
                if (0..prefix).all(|index| pending(block_id, index as u16))
                    || !eligible(block_id, 0)
                {
                    continue;
                }
                let missing = (0..prefix)
                    // Without a header, retry the same prefix. Walking through
                    // fresh indices could renew cooldown and starve shard 0.
                    .map(|index| index as u16)
                    .filter(|index| !pending(block_id, *index))
                    .take(left)
                    .filter(|index| eligible(block_id, *index))
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    left -= missing.len();
                    out.push(RepairRequest {
                        block_id,
                        shard_indices: missing,
                    });
                }
            }
            cursor = id + 1;
            if arrival >= r.deadline {
                continue;
            }
            for (index, state) in r.blocks.iter().enumerate() {
                if left == 0 {
                    return out;
                }
                let k = self.settings.block_data(r.bytes.len(), index);
                let block_id = id * MAX_BLOCKS_PER_RECORD as u64 + index as u64;
                let missing = (0..k)
                    .filter(|i| match state {
                        BlockState::Missing => true,
                        BlockState::Open(b) => b.shards[*i].is_none(),
                        BlockState::Done => false,
                    })
                    .filter(|i| !pending(block_id, *i as u16))
                    .filter(|i| eligible(block_id, *i as u16))
                    .take(left)
                    .map(|i| i as u16)
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    // Query while this block is admitted by batch_eligible.
                    // Retain only a bounded candidate for this selected request;
                    // optional parity cannot reserve another history node.
                    if self.capabilities == Capabilities::V7
                        && k > 1
                        && k * self.settings.parity_shards / self.settings.data_shards > 0
                        && match state {
                            BlockState::Missing => true,
                            BlockState::Open(b) => b.shards[k].is_none(),
                            BlockState::Done => false,
                        }
                        && !pending(block_id, k as u16)
                        && eligible(block_id, k as u16)
                    {
                        parity.push((out.len(), k as u16));
                    }
                    left -= missing.len();
                    out.push(RepairRequest {
                        block_id,
                        shard_indices: missing,
                    });
                }
            }
        }
        // Missing data across the entire batch has priority. Extras consume
        // only remaining slots, on already admitted blocks, at a fixed index.
        for (request, index) in parity.into_iter().take(left) {
            out[request].shard_indices.push(index);
        }
        out
    }
    pub(crate) fn repair_requests_with_shapes(
        &self,
        now: u64,
        rtt: u64,
        scheduling: u64,
        eligible: impl FnMut(u64, u16) -> bool,
        pending: impl FnMut(u64, u16) -> bool,
        shape: impl Fn(u64) -> Option<usize>,
    ) -> Vec<RepairRequest> {
        let mut requests = self.repair_requests_baseline(now, rtt, scheduling, eligible, pending);
        self.bound_shape_requests(requests.iter_mut(), shape);
        requests.retain(|request| !request.shard_indices.is_empty());
        requests
    }
    /// Bound the prospective ordinary requests without repeating stateful
    /// eligibility/history admission or introducing a gap from weak metadata.
    pub(super) fn bound_shape_requests<'a>(
        &self,
        requests: impl Iterator<Item = &'a mut RepairRequest>,
        shape: impl Fn(u64) -> Option<usize>,
    ) {
        for request in requests {
            let record = request.block_id / MAX_BLOCKS_PER_RECORD as u64;
            if self.records.contains_key(&record) {
                continue;
            }
            let Some(length) = shape(record).filter(|length| {
                (12..=self.settings.max_record_len).contains(length)
                    && self.settings.block_count(*length) <= self.settings.max_blocks
            }) else {
                continue;
            };
            let block = (request.block_id % MAX_BLOCKS_PER_RECORD as u64) as usize;
            if block >= self.settings.block_count(length) {
                request.shard_indices.clear();
                continue;
            }
            let count = self.settings.block_data(length, block);
            request
                .shard_indices
                .retain(|index| usize::from(*index) < count);
        }
    }
    pub(super) fn missing_shape(&self, record: u64) -> bool {
        record >= self.next && !self.records.contains_key(&record)
    }
    /// Include every earlier hypothetical record, even if cooldown/queued data
    /// removed its request. A later expansion may not consume that room.
    pub(super) fn hint_extra_block_capacity(&self) -> usize {
        let used = self
            .records
            .values()
            .map(|record| record.blocks.len())
            .sum::<usize>();
        let mut room = self.settings.max_blocks.saturating_sub(used);
        let mut records = self.settings.max_records.saturating_sub(self.records.len());
        let mut cursor = self.next;
        for id in self.records.keys() {
            let count = id
                .saturating_sub(cursor)
                .min(records as u64)
                .min(room as u64) as usize;
            room -= count;
            records -= count;
            cursor = id.saturating_add(1);
        }
        room
    }
}
