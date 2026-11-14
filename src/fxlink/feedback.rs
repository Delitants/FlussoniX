//! Versioned unordered repair feedback. QUIC authenticates frames; routes exist only
//! after subscription admission. Media datagram and legacy control are unchanged.
use super::{Budget, RepairRequest, Reservation, Settings};
use bytes::Bytes;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

const HEADER: usize = 24;
pub(super) const MAX_FRAME: usize = 1024;
const MAX_BLOCKS: usize = 64;
const MAX_SHARDS: usize = 256;
const ROUTE_BYTES: usize = 16 * 1024;

#[path = "feedback_bootstrap.rs"]
mod bootstrap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DatagramRequest {
    Ordinary(RepairRequest),
    Bootstrap { block_id: u64, reply_limit: u16 },
}
impl DatagramRequest {
    pub(super) fn block_id(&self) -> u64 {
        match self {
            Self::Ordinary(request) => request.block_id,
            Self::Bootstrap { block_id, .. } => *block_id,
        }
    }
    pub(super) fn units(&self) -> usize {
        match self {
            Self::Ordinary(request) => request.shard_indices.len(),
            Self::Bootstrap { reply_limit, .. } => usize::from(*reply_limit),
        }
    }
}
pub(super) fn encode_bootstrap(
    id: u64,
    requests: &[DatagramRequest],
    settings: &Settings,
    next: &mut u64,
) -> Result<Vec<Bytes>, String> {
    bootstrap::encode(id, requests, settings, next)
}

fn header(bytes: &[u8]) -> Option<(u64, u8, Option<u64>, usize)> {
    let size = match bytes.get(..8)? {
        b"FXRQ\x01\x00\x00\x18" => HEADER,
        b"FXRQ\x02\x00\x00\x20" | b"FXRQ\x03\x00\x00\x20" | b"FXRQ\x04\x00\x00\x20" => 32,
        _ => return None,
    };
    if !(size + 12..=MAX_FRAME).contains(&bytes.len()) || bytes[16..24] != 1u64.to_be_bytes() {
        return None;
    }
    let sequence = if size == 32 {
        let sequence = u64::from_be_bytes(bytes[24..32].try_into().ok()?);
        if sequence == 0 {
            return None;
        }
        Some(sequence)
    } else {
        None
    };
    Some((
        u64::from_be_bytes(bytes[8..16].try_into().ok()?),
        if bytes[4] == 4 {
            8
        } else if bytes[4] == 3 {
            7
        } else if size == 32 {
            4
        } else {
            3
        },
        sequence,
        size,
    ))
}
#[cfg(test)]
pub(super) fn identity(bytes: &[u8]) -> Option<u64> {
    Some(header(bytes)?.0)
}

pub(super) fn decode(bytes: &[u8], settings: &Settings) -> Result<VecDeque<RepairRequest>, String> {
    let (_, version, _, size) = header(bytes).ok_or("FXLink feedback header")?;
    if version == 8 {
        return Err("FXLink feedback requires typed v8 decode".into());
    }
    let width = settings.data_shards
        + if version == 7 {
            settings.parity_shards
        } else {
            0
        };
    let mut requests: VecDeque<RepairRequest> = VecDeque::new();
    let mut offset = size;
    let mut total = 0;
    while offset < bytes.len() {
        if offset + 10 > bytes.len() || requests.len() >= MAX_BLOCKS {
            return Err("FXLink feedback block limit".into());
        }
        let block = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let count = u16::from_be_bytes(bytes[offset + 8..offset + 10].try_into().unwrap()) as usize;
        offset += 10;
        total += count;
        if count == 0
            || count > width
            || total > settings.max_repair_shards
            || total > MAX_SHARDS
            || offset + count * 2 > bytes.len()
            || requests.iter().any(|r| r.block_id == block)
        {
            return Err("FXLink feedback shard limit".into());
        }
        let mut shards = Vec::with_capacity(count);
        let mut seen = [0u64; 4];
        for _ in 0..count {
            let shard = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
            offset += 2;
            if shard as usize >= width
                || shard >= 256
                || seen[shard as usize / 64] & (1u64 << (shard % 64)) != 0
            {
                return Err("FXLink feedback data index".into());
            }
            seen[shard as usize / 64] |= 1u64 << (shard % 64);
            shards.push(shard);
        }
        requests.push_back(RepairRequest {
            block_id: block,
            shard_indices: shards,
        });
    }
    Ok(requests)
}

pub(super) fn encode(
    id: u64,
    requests: &[RepairRequest],
    settings: &Settings,
) -> Result<Vec<Bytes>, String> {
    encode_inner(id, requests, settings, None, false)
}
pub(super) fn encode_sequenced(
    id: u64,
    requests: &[RepairRequest],
    settings: &Settings,
    next: &mut u64,
) -> Result<Vec<Bytes>, String> {
    // Commit the sequence only after the entire bounded batch validates.
    let mut candidate = *next;
    let frames = encode_inner(id, requests, settings, Some(&mut candidate), false)?;
    *next = candidate;
    Ok(frames)
}
pub(super) fn encode_parity(
    id: u64,
    requests: &[RepairRequest],
    settings: &Settings,
    next: &mut u64,
) -> Result<Vec<Bytes>, String> {
    let mut candidate = *next;
    let frames = encode_inner(id, requests, settings, Some(&mut candidate), true)?;
    *next = candidate;
    Ok(frames)
}
fn encode_inner(
    id: u64,
    requests: &[RepairRequest],
    settings: &Settings,
    mut sequence: Option<&mut u64>,
    parity: bool,
) -> Result<Vec<Bytes>, String> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if requests.len() > MAX_SHARDS
        || requests
            .iter()
            .map(|r| r.shard_indices.len())
            .sum::<usize>()
            > settings.max_repair_shards
    {
        return Err("FXLink feedback batch limit".into());
    }
    let size = if sequence.is_some() { 32 } else { HEADER };
    let mut make_header = || -> Result<Vec<u8>, String> {
        let mut bytes = Vec::with_capacity(MAX_FRAME);
        bytes.extend_from_slice(if parity {
            b"FXRQ\x03\x00\x00\x20"
        } else if size == 32 {
            b"FXRQ\x02\x00\x00\x20"
        } else {
            b"FXRQ\x01\x00\x00\x18"
        });
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(&1u64.to_be_bytes());
        if let Some(next) = sequence.as_deref_mut() {
            if *next == 0 {
                return Err("FXLink feedback sequence exhausted".into());
            }
            let current = *next;
            *next = next
                .checked_add(1)
                .ok_or("FXLink feedback sequence exhausted")?;
            bytes.extend_from_slice(&current.to_be_bytes());
        }
        Ok(bytes)
    };
    let mut bytes = make_header()?;
    let mut blocks = 0;
    let mut result = Vec::new();
    for request in requests {
        let needed = 10 + request.shard_indices.len() * 2;
        if needed > MAX_FRAME - size {
            return Err("FXLink feedback request size".into());
        }
        if bytes.len() + needed > MAX_FRAME || blocks == MAX_BLOCKS {
            decode(&bytes, settings)?;
            result.push(Bytes::from(bytes));
            bytes = make_header()?;
            blocks = 0;
        }
        bytes.extend_from_slice(&request.block_id.to_be_bytes());
        bytes.extend_from_slice(&(request.shard_indices.len() as u16).to_be_bytes());
        for shard in &request.shard_indices {
            bytes.extend_from_slice(&shard.to_be_bytes());
        }
        blocks += 1;
    }
    decode(&bytes, settings)?;
    result.push(Bytes::from(bytes));
    if result.len() > 4 {
        return Err("FXLink feedback frame limit".into());
    }
    Ok(result)
}

// Bits are distances below the largest validated consumed sequence. Moving the
// window costs eight words, regardless of an authenticated sender's jump size.
#[derive(Default)]
struct Seen {
    largest: u64,
    bits: [u64; 8],
}
impl Seen {
    fn contains(&self, sequence: u64) -> bool {
        if sequence == 0 {
            return true;
        }
        if sequence > self.largest {
            return false;
        }
        let distance = self.largest - sequence;
        distance >= 512 || self.bits[distance as usize / 64] & (1u64 << (distance % 64)) != 0
    }
    fn claim(&mut self, sequence: u64) -> bool {
        if self.contains(sequence) {
            return false;
        }
        if sequence > self.largest {
            let delta = sequence - self.largest;
            if delta >= 512 {
                self.bits = [0; 8];
            } else {
                let words = delta as usize / 64;
                let bits = delta as usize % 64;
                for index in (0usize..8).rev() {
                    self.bits[index] = index.checked_sub(words).map_or(0, |source| {
                        (self.bits[source] << bits)
                            | if bits == 0 {
                                0
                            } else {
                                source
                                    .checked_sub(1)
                                    .map_or(0, |lower| self.bits[lower] >> (64 - bits))
                            }
                    });
                }
            }
            self.largest = sequence;
        }
        let distance = self.largest - sequence;
        self.bits[distance as usize / 64] |= 1u64 << (distance % 64);
        true
    }
}

struct Entry {
    version: u8,
    seen: Seen,
    sender: mpsc::Sender<Bytes>,
    since: u64,
    count: u8,
}
#[derive(Clone, Default)]
pub(super) struct Routes(Arc<Mutex<HashMap<u64, Entry>>>);
impl Routes {
    // Called only after Provider admission; ID is the unique stream index.
    pub(super) fn admit(&self, id: u64, version: u8, budget: &Budget) -> Result<Route, String> {
        if !matches!(version, 3..=9) {
            return Err("FXLink feedback version".into());
        }
        // V5 changes media protection and inherits the exact v4 feedback wire.
        let version = if version == 9 {
            8 // Only negotiated v9 inherits the exact format-4 route.
        } else if version >= 7 {
            version
        } else {
            version.min(4)
        };
        let guard = budget.reserve(ROUTE_BYTES).map_err(|e| e.to_string())?;
        let (sender, receiver) = mpsc::channel(8);
        let mut entries = self.0.lock().unwrap();
        if entries.len() >= 16 || entries.contains_key(&id) {
            return Err("FXLink feedback route limit".into());
        }
        entries.insert(
            id,
            Entry {
                version,
                seen: Seen::default(),
                sender,
                since: 0,
                count: 0,
            },
        );
        Ok(Route {
            id,
            routes: self.clone(),
            receiver,
            pending: VecDeque::new(),
            _guard: guard,
        })
    }
    pub(super) fn receive(&self, bytes: Bytes, at: u64) {
        let Some((id, version, sequence, _)) = header(&bytes) else {
            return;
        };
        let mut entries = self.0.lock().unwrap();
        let Some(entry) = entries.get_mut(&id) else {
            return;
        };
        if entry.version != version {
            return;
        }
        #[cfg(test)]
        if super::feedback_copy_tests::discard(&bytes) {
            return;
        }
        if sequence.is_some_and(|sequence| entry.seen.contains(sequence)) {
            return;
        }
        if at.saturating_sub(entry.since) >= 20 {
            entry.since = at;
            entry.count = 0;
        }
        if entry.count >= if version >= 4 { 8 } else { 4 } {
            return;
        }
        entry.count += 1;
        // Optional feedback may be dropped under pressure; it never resets a
        // healthy subscription or creates another task or queue.
        if entry.sender.capacity() > 0 {
            // A small QUIC Bytes view can retain a larger UDP/GRO allocation.
            // Detach after admission/rate/capacity checks so each queued owner
            // fits the route reservation rather than retaining its parent.
            let _ = entry.sender.try_send(Bytes::copy_from_slice(&bytes));
        }
    }
}
pub(super) struct Route {
    id: u64,
    routes: Routes,
    receiver: mpsc::Receiver<Bytes>,
    pending: VecDeque<DatagramRequest>,
    _guard: Reservation,
}
impl Route {
    pub(super) async fn next(&mut self, settings: &Settings) -> DatagramRequest {
        loop {
            if let Some(request) = self.pending.pop_front() {
                return request;
            }
            let Some(bytes) = self.receiver.recv().await else {
                return std::future::pending().await;
            };
            let requests = if header(&bytes).is_some_and(|(_, version, _, _)| version == 8) {
                bootstrap::decode(&bytes, settings)
            } else {
                decode(&bytes, settings).map(|requests| {
                    requests
                        .into_iter()
                        .map(DatagramRequest::Ordinary)
                        .collect()
                })
            };
            if let Ok(requests) = requests {
                let (_, _, sequence, _) = header(&bytes).unwrap();
                if let Some(sequence) = sequence {
                    let mut entries = self.routes.0.lock().unwrap();
                    let Some(entry) = entries.get_mut(&self.id) else {
                        continue;
                    };
                    // Claim before the first request is exposed. Copies already
                    // pending in the bounded queue must also be suppressed.
                    if !entry.seen.claim(sequence) {
                        continue;
                    }
                }
                self.pending = requests;
            }
        }
    }
}
impl Drop for Route {
    fn drop(&mut self) {
        self.routes.0.lock().unwrap().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn v4(sequence: u64, block: u64) -> Bytes {
        let mut bytes = b"FXRQ\x02\x00\x00\x20".to_vec();
        bytes.extend_from_slice(&7u64.to_be_bytes());
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&block.to_be_bytes());
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        Bytes::from(bytes)
    }
    #[test]
    fn literal_sequenced_header_decodes_and_legacy_wire_stays_exact() {
        let literal = v4(1, 9);
        assert_eq!(literal.len(), 44);
        assert_eq!(identity(&literal), Some(7));
        let requests = decode(&literal, &Settings::default()).expect("valid v4 feedback");
        assert_eq!(
            requests[0],
            RepairRequest {
                block_id: 9,
                shard_indices: vec![0]
            }
        );
        let legacy = encode(
            7,
            &requests.into_iter().collect::<Vec<_>>(),
            &Settings::default(),
        )
        .unwrap();
        let mut expected = b"FXRQ\x01\x00\x00\x18".to_vec();
        expected.extend_from_slice(&7u64.to_be_bytes());
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.extend_from_slice(&literal[32..]);
        assert_eq!(legacy, [Bytes::from(expected)]);
        assert!(decode(&v4(0, 9), &Settings::default()).is_err());
        for offset in [4, 5, 6, 7, 23] {
            let mut bad = literal.to_vec();
            bad[offset] = 255;
            assert!(decode(&bad, &Settings::default()).is_err());
        }
    }
    #[test]
    fn sequenced_batches_have_unique_ids_and_fail_transactionally_before_wrap() {
        let settings = Settings {
            max_repair_shards: 256,
            ..Settings::default()
        };
        let requests: Vec<_> = (0..256)
            .map(|block_id| RepairRequest {
                block_id,
                shard_indices: vec![0],
            })
            .collect();
        let mut next = 1;
        let frames = encode_sequenced(7, &requests, &settings, &mut next).unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(next, 5);
        let mut decoded = Vec::new();
        for (index, frame) in frames.iter().enumerate() {
            assert!(frame.len() <= MAX_FRAME);
            assert_eq!(header(frame), Some((7, 4, Some(index as u64 + 1), 32)));
            decoded.extend(decode(frame, &settings).unwrap());
        }
        assert_eq!(decoded, requests);
        next = u64::MAX - 1;
        assert!(encode_sequenced(7, &requests, &settings, &mut next).is_err());
        assert_eq!(next, u64::MAX - 1);
        next = 0;
        assert!(encode_sequenced(7, &requests[..1], &settings, &mut next).is_err());
        next = 1;
        assert_eq!(
            encode_sequenced(
                7,
                &[RepairRequest {
                    block_id: 9,
                    shard_indices: vec![0]
                }],
                &settings,
                &mut next
            )
            .unwrap(),
            [v4(1, 9)]
        );
    }
    #[tokio::test]
    async fn queued_copies_are_suppressed_before_exposing_the_first_repair() {
        let routes = Routes::default();
        let budget = Budget::new(ROUTE_BYTES);
        let mut route = routes.admit(7, 4, &budget).unwrap();
        for frame in [v4(1, 9), v4(1, 9), v4(2, 10)] {
            routes.receive(frame, 100);
        }
        assert!(matches!(
            route.next(&Settings::default()).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 9, .. })
        ));
        assert!(
            matches!(
                route.next(&Settings::default()).await,
                DatagramRequest::Ordinary(RepairRequest { block_id: 10, .. })
            ),
            "queued duplicate spent repair work"
        );
        drop(route);
        assert_eq!(budget.used(), 0);
    }
    #[tokio::test]
    async fn malformed_and_queue_dropped_sequences_do_not_poison_fresh_frames() {
        use futures_util::FutureExt;
        let routes = Routes::default();
        let budget = Budget::new(ROUTE_BYTES);
        let settings = Settings::default();
        let mut route = routes.admit(7, 4, &budget).unwrap();
        let mut bad = v4(5000, 9).to_vec();
        bad[41] = 0; // zero count, otherwise valid high identity
        routes.receive(Bytes::from(bad), 100);
        routes.receive(v4(2, 2), 100);
        routes.receive(v4(1, 1), 100);
        assert!(matches!(
            route.next(&settings).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 2, .. })
        ));
        assert!(matches!(
            route.next(&settings).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 1, .. })
        ));
        for n in 3..11 {
            routes.receive(v4(n, n), 120);
        }
        routes.receive(v4(6000, 6000), 140); // full queue: do not mark
        for n in 3..11 {
            assert!(
                matches!(route.next(&settings).await, DatagramRequest::Ordinary(RepairRequest { block_id: block, .. }) if block==n)
            );
        }
        routes.receive(v4(11, 11), 160);
        assert!(matches!(
            route.next(&settings).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 11, .. })
        ));
        routes.receive(v4(6000, 6000), 180);
        assert!(matches!(
            route.next(&settings).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 6000, .. })
        ));
        routes.receive(v4(5488, 5488), 200); // distance512, stale
        routes.receive(v4(5489, 5489), 200); // distance511, fresh reordered
        assert!(
            matches!(
                route.next(&settings).await,
                DatagramRequest::Ordinary(RepairRequest { block_id: 5489, .. })
            ),
            "stale sequence accepted"
        );
        routes.receive(v4(6000, 6000), 220);
        routes.receive(v4(5489, 5489), 220);
        assert!(
            route.next(&settings).now_or_never().is_none(),
            "processed copy was not suppressed"
        );
        drop(route);
        assert_eq!(budget.used(), 0);
    }
    #[tokio::test]
    async fn admitted_versions_isolate_wire_formats_and_preserve_rate_bounds() {
        for version in [3, 4] {
            let routes = Routes::default();
            let budget = Budget::new(ROUTE_BYTES);
            let route = routes.admit(7, version, &budget).unwrap();
            let legacy = encode(
                7,
                &[RepairRequest {
                    block_id: 9,
                    shard_indices: vec![0],
                }],
                &Settings::default(),
            )
            .unwrap()
            .pop()
            .unwrap();
            let (good, bad) = if version == 4 {
                (v4(1, 9), legacy)
            } else {
                (legacy, v4(1, 9))
            };
            routes.receive(bad, 100);
            assert_eq!(route.receiver.len(), 0);
            for _ in 0..20 {
                routes.receive(good.clone(), 100);
            }
            assert_eq!(route.receiver.len(), if version == 4 { 8 } else { 4 });
            drop(route);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn seen_window_matches_a_set_across_bit_word_and_large_jump_boundaries() {
        let mut seen = Seen::default();
        let mut reference = std::collections::BTreeSet::new();
        let mut largest = 0;
        for base in [
            1u64,
            63,
            64,
            65,
            127,
            128,
            511,
            512,
            513,
            1025,
            u64::MAX - 512,
        ] {
            for sequence in [
                base,
                base,
                base.saturating_sub(1),
                base.saturating_add(1),
                base.saturating_sub(511),
                base.saturating_sub(512),
            ] {
                let expected = sequence != 0
                    && !(sequence <= largest && largest - sequence >= 512)
                    && reference.insert(sequence);
                assert_eq!(
                    seen.claim(sequence),
                    expected,
                    "sequence={sequence}, high={largest}"
                );
                largest = largest.max(sequence);
            }
        }
    }
    #[test]
    fn maximum_batch_is_bounded_and_round_trips_with_no_truncation() {
        let settings = Settings {
            max_repair_shards: 256,
            ..Settings::default()
        };
        let requests: Vec<_> = (0..256)
            .map(|block| RepairRequest {
                block_id: block,
                shard_indices: vec![0],
            })
            .collect();
        let frames = encode(7, &requests, &settings).unwrap();
        assert_eq!(frames.len(), 4);
        let mut decoded = Vec::new();
        for frame in frames {
            assert!(frame.len() <= MAX_FRAME);
            assert_eq!(identity(&frame), Some(7));
            decoded.extend(decode(&frame, &settings).unwrap());
        }
        assert_eq!(decoded, requests);
    }
    #[test]
    fn malformed_or_duplicate_or_excessive_indices_are_rejected() {
        let settings = Settings::default();
        let one = encode(
            7,
            &[RepairRequest {
                block_id: 0,
                shard_indices: vec![0],
            }],
            &settings,
        )
        .unwrap()
        .pop()
        .unwrap();
        for len in 0..one.len() {
            assert!(decode(&one[..len], &settings).is_err());
        }
        for offset in [0, 4, 5, 6, 7, 23, 33, 35] {
            let mut bad = one.to_vec();
            bad[offset] = 255;
            assert!(decode(&bad, &settings).is_err(), "{offset}");
        }
        for shards in [vec![], vec![0, 0], vec![128], vec![20]] {
            assert!(
                encode(
                    0,
                    &[RepairRequest {
                        block_id: 0,
                        shard_indices: shards
                    }],
                    &settings
                )
                .is_err()
            );
        }
        let mut bad = one.to_vec();
        bad.extend_from_slice(&one[HEADER..]);
        assert!(decode(&bad, &settings).is_err());
        let mut too_long = one.to_vec();
        too_long.resize(MAX_FRAME + 1, 0);
        assert!(decode(&too_long, &settings).is_err());
        assert!(
            decode(
                &one,
                &Settings {
                    max_repair_shards: 1,
                    data_shards: 1,
                    ..settings
                }
            )
            .is_ok()
        );
    }
    #[tokio::test]
    async fn unadmitted_overloaded_and_retired_routes_remain_bounded_and_isolated() {
        let budget = Budget::new(ROUTE_BYTES * 2);
        let routes = Routes::default();
        let frames = encode(
            7,
            &[RepairRequest {
                block_id: 0,
                shard_indices: vec![0],
            }],
            &Settings::default(),
        )
        .unwrap();
        routes.receive(frames[0].clone(), 100);
        assert_eq!(budget.used(), 0);
        let mut route = routes.admit(7, 3, &budget).unwrap();
        let sibling = routes.admit(8, 3, &budget).unwrap();
        assert_eq!(budget.used(), ROUTE_BYTES * 2);
        assert!(routes.admit(9, 3, &budget).is_err());
        for at in 100..200 {
            for _ in 0..32 {
                routes.receive(frames[0].clone(), at);
            }
        }
        assert_eq!(route.receiver.len(), 8);
        assert_eq!(sibling.receiver.len(), 0);
        while route.receiver.try_recv().is_ok() {}
        let mut backing = vec![0u8; 1024 * 1024];
        backing[..frames[0].len()].copy_from_slice(&frames[0]);
        let view = Bytes::from(backing).slice(..frames[0].len());
        routes.receive(view.clone(), 300);
        let queued = route.receiver.try_recv().unwrap();
        assert_eq!(queued, view);
        assert_ne!(
            queued.as_ptr(),
            view.as_ptr(),
            "route retained oversized UDP backing storage"
        );
        routes.receive(frames[0].clone(), 320);
        assert!(matches!(
            route.next(&Settings::default()).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 0, .. })
        ));
        drop(route);
        routes.receive(frames[0].clone(), 300);
        assert_eq!(routes.0.lock().unwrap().len(), 1);
        drop(sibling);
        assert_eq!(budget.used(), 0);
        assert!(routes.0.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod compact_feedback_tests {
    use super::*;
    #[tokio::test]
    async fn compact_v5_reuses_v4_sequenced_feedback_and_deduplication() {
        let settings = Settings::default();
        let budget = Budget::new(65536);
        let routes = Routes::default();
        let mut route = routes.admit(7, 5, &budget).unwrap();
        let mut sequence = 1;
        let frames = encode_sequenced(
            7,
            &[RepairRequest {
                block_id: 0,
                shard_indices: vec![0],
            }],
            &settings,
            &mut sequence,
        )
        .unwrap();
        routes.receive(frames[0].clone(), 20);
        routes.receive(frames[0].clone(), 20);
        assert!(matches!(
            route.next(&settings).await,
            DatagramRequest::Ordinary(RepairRequest { block_id: 0, .. })
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), route.next(&settings))
                .await
                .is_err()
        );
        drop(route);
        assert_eq!(budget.used(), 0);
    }
}
