//! Explicit v8 repair intent. Limits count possible replies, never request entries.
use super::{DatagramRequest, MAX_BLOCKS, MAX_FRAME, MAX_SHARDS, RepairRequest, Settings};
use crate::fxlink::MAX_BLOCKS_PER_RECORD;
use bytes::Bytes;
use std::collections::VecDeque;

const HEADER: usize = 32;
const ENTRY: usize = 12;

fn valid_request(request: &DatagramRequest, settings: &Settings) -> bool {
    match request {
        DatagramRequest::Bootstrap {
            block_id,
            reply_limit,
        } => {
            let stride = MAX_BLOCKS_PER_RECORD as u64;
            (2..=settings.data_shards).contains(&usize::from(*reply_limit))
                && *block_id % stride == 0
                && (block_id / stride)
                    .checked_mul(stride)
                    .and_then(|first| first.checked_add(1))
                    .is_some()
        }
        DatagramRequest::Ordinary(request) => {
            let Some(width) = settings
                .data_shards
                .checked_add(settings.parity_shards)
                .filter(|width| *width <= MAX_SHARDS)
            else {
                return false;
            };
            if request.shard_indices.is_empty() || request.shard_indices.len() > width {
                return false;
            }
            let mut seen = [0u64; 4];
            for &index in &request.shard_indices {
                let index = usize::from(index);
                if index >= width || seen[index / 64] & (1u64 << (index % 64)) != 0 {
                    return false;
                }
                seen[index / 64] |= 1u64 << (index % 64);
            }
            true
        }
    }
}

pub(super) fn decode(
    bytes: &[u8],
    settings: &Settings,
) -> Result<VecDeque<DatagramRequest>, String> {
    let (_, version, _, _) = super::header(bytes).ok_or("FXLink bootstrap header")?;
    if version != 8 {
        return Err("FXLink bootstrap version".into());
    }
    let mut offset = HEADER;
    let mut requests = VecDeque::<DatagramRequest>::new();
    let mut total = 0usize;
    while offset < bytes.len() {
        if bytes.len() - offset < ENTRY || requests.len() >= MAX_BLOCKS {
            return Err("FXLink bootstrap entry limit".into());
        }
        let block_id = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let mode = bytes[offset + 8];
        let reserved = bytes[offset + 9];
        let count = u16::from_be_bytes(bytes[offset + 10..offset + 12].try_into().unwrap());
        offset += ENTRY;
        total += usize::from(count);
        if reserved != 0
            || count == 0
            || total > settings.max_repair_shards
            || total > MAX_SHARDS
            || requests
                .iter()
                .any(|request| request.block_id() == block_id)
        {
            return Err("FXLink bootstrap reply limit".into());
        }
        let request = match mode {
            0 => {
                let length = usize::from(count) * 2;
                if bytes.len() - offset < length {
                    return Err("FXLink bootstrap indices truncated".into());
                }
                let shard_indices = bytes[offset..offset + length]
                    .chunks_exact(2)
                    .map(|index| u16::from_be_bytes(index.try_into().unwrap()))
                    .collect();
                offset += length;
                DatagramRequest::Ordinary(RepairRequest {
                    block_id,
                    shard_indices,
                })
            }
            1 => DatagramRequest::Bootstrap {
                block_id,
                reply_limit: count,
            },
            _ => return Err("FXLink bootstrap mode".into()),
        };
        if !valid_request(&request, settings) {
            return Err("FXLink bootstrap request".into());
        }
        requests.push_back(request);
    }
    Ok(requests)
}

fn make_header(id: u64, sequence: &mut u64) -> Result<Vec<u8>, String> {
    if *sequence == 0 {
        return Err("FXLink feedback sequence exhausted".into());
    }
    let current = *sequence;
    *sequence = sequence
        .checked_add(1)
        .ok_or("FXLink feedback sequence exhausted")?;
    let mut bytes = Vec::with_capacity(MAX_FRAME);
    bytes.extend_from_slice(b"FXRQ\x04\x00\x00\x20");
    for value in [id, 1, current] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    Ok(bytes)
}

pub(super) fn encode(
    id: u64,
    requests: &[DatagramRequest],
    settings: &Settings,
    next: &mut u64,
) -> Result<Vec<Bytes>, String> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    if requests.len() > MAX_SHARDS {
        return Err("FXLink bootstrap batch limit".into());
    }
    let mut total = 0usize;
    for (index, request) in requests.iter().enumerate() {
        total = total.saturating_add(request.units());
        if total > settings.max_repair_shards
            || total > MAX_SHARDS
            || !valid_request(request, settings)
            || requests[..index]
                .iter()
                .any(|previous| previous.block_id() == request.block_id())
        {
            return Err("FXLink bootstrap batch request".into());
        }
    }
    let mut sequence = *next;
    let mut bytes = make_header(id, &mut sequence)?;
    let mut entries = 0;
    let mut frames = Vec::new();
    for request in requests {
        let trailing = match request {
            DatagramRequest::Ordinary(r) => r.shard_indices.len() * 2,
            _ => 0,
        };
        if entries == MAX_BLOCKS || bytes.len() + ENTRY + trailing > MAX_FRAME {
            frames.push(Bytes::from(bytes));
            if frames.len() >= 4 {
                return Err("FXLink bootstrap frame limit".into());
            }
            bytes = make_header(id, &mut sequence)?;
            entries = 0;
        }
        bytes.extend_from_slice(&request.block_id().to_be_bytes());
        bytes.extend_from_slice(&[
            u8::from(matches!(request, DatagramRequest::Bootstrap { .. })),
            0,
        ]);
        bytes.extend_from_slice(&(request.units() as u16).to_be_bytes());
        if let DatagramRequest::Ordinary(request) = request {
            for index in &request.shard_indices {
                bytes.extend_from_slice(&index.to_be_bytes());
            }
        }
        entries += 1;
    }
    frames.push(Bytes::from(bytes));
    *next = sequence;
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fxlink::{Budget, Capabilities, RepairRequest, Settings, control, feedback};
    use bytes::Bytes;
    use feedback::DatagramRequest::{self, Bootstrap, Ordinary};
    use futures_util::FutureExt;
    use std::collections::VecDeque;

    fn literal(sequence: u64, block: u64, mode: u8, values: &[u16]) -> Vec<u8> {
        let mut bytes = b"FXRQ\x04\x00\x00\x20".to_vec();
        for value in [9, 1, sequence, block] {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes.extend_from_slice(&[mode, 0]);
        let count = if mode == 1 {
            values[0]
        } else {
            values.len() as u16
        };
        bytes.extend_from_slice(&count.to_be_bytes());
        if mode == 0 {
            for value in values {
                bytes.extend_from_slice(&value.to_be_bytes());
            }
        }
        bytes
    }
    fn ordinary(block_id: u64, indices: &[u16]) -> DatagramRequest {
        Ordinary(RepairRequest {
            block_id,
            shard_indices: indices.to_vec(),
        })
    }

    #[test]
    fn literal_bootstrap_and_mixed_ordinary_wire_is_exact() {
        let bytes = [
            0x46, 0x58, 0x52, 0x51, 4, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 8, 0, 1, 0, 0, 20,
        ];
        let settings = Settings::default();
        let request = Bootstrap {
            block_id: 2048,
            reply_limit: 20,
        };
        assert_eq!(
            decode(&bytes, &settings).unwrap(),
            VecDeque::from([request.clone()])
        );
        let mut sequence = 1;
        assert_eq!(
            feedback::encode_bootstrap(9, std::slice::from_ref(&request), &settings, &mut sequence)
                .unwrap(),
            [Bytes::copy_from_slice(&bytes)]
        );
        assert_eq!(sequence, 2);
        assert_eq!((request.block_id(), request.units()), (2048, 20));
        let mut mixed = bytes.to_vec();
        mixed.extend_from_slice(&literal(1, 0, 0, &[0])[32..]);
        assert_eq!(
            decode(&mixed, &settings).unwrap(),
            VecDeque::from([request, ordinary(0, &[0])])
        );
        assert_eq!(
            decode(&literal(1, 0, 0, &[0]), &settings).unwrap(),
            VecDeque::from([ordinary(0, &[0])])
        );
        assert_eq!(
            serde_json::to_vec(&control::Control::Repair {
                block: 2048,
                shards: vec![0]
            })
            .unwrap(),
            br#"{"Repair":{"block":2048,"shards":[0]}}"#
        );
    }

    #[test]
    fn bootstrap_limits_modes_mapping_and_atomic_validation() {
        let settings = Settings::default();
        for bytes in [
            literal(1, 2048, 1, &[0]),
            literal(1, 2048, 1, &[1]),
            literal(1, 2048, 1, &[21]),
            literal(1, 2049, 1, &[2]),
            literal(1, 2048, 2, &[2]),
            literal(1, 0, 0, &[]),
            literal(1, 0, 0, &[24]),
            literal(1, 0, 0, &[0, 0]),
            literal(0, 0, 0, &[0]),
        ] {
            assert!(decode(&bytes, &settings).is_err(), "accepted {bytes:?}");
        }
        let good = literal(1, 2048, 1, &[20]);
        for offset in [4, 5, 6, 7, 23, 41] {
            let mut bad = good.clone();
            bad[offset] = 255;
            assert!(decode(&bad, &settings).is_err());
        }
        for length in 0..good.len() {
            assert!(decode(&good[..length], &settings).is_err());
        }
        let mut bad = good.clone();
        bad.extend_from_slice(&[0]);
        assert!(decode(&bad, &settings).is_err());
        let mut duplicate = good.clone();
        duplicate.extend_from_slice(&good[32..]);
        assert!(decode(&duplicate, &settings).is_err());
        let mut invalid_tail = good.clone();
        invalid_tail.extend_from_slice(&literal(1, 0, 0, &[256])[32..]);
        assert!(decode(&invalid_tail, &settings).is_err());
        let last_aligned = u64::MAX - 2047;
        assert!(decode(&literal(1, last_aligned, 1, &[2]), &settings).is_ok());
        assert!(decode(&literal(1, u64::MAX, 1, &[2]), &settings).is_err());
    }

    #[test]
    fn exact_frame_entry_unit_and_batch_boundaries() {
        let settings = Settings {
            data_shards: 128,
            parity_shards: 128,
            max_repair_shards: 256,
            ..Settings::default()
        };
        let requests: Vec<_> = (0..40)
            .map(|block| {
                ordinary(
                    block,
                    &(0..if block == 39 { 22 } else { 6 }).collect::<Vec<_>>(),
                )
            })
            .collect();
        let mut next = 1;
        let frames = feedback::encode_bootstrap(9, &requests, &settings, &mut next).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 1024);
        assert_eq!(
            decode(&frames[0], &settings)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            requests
        );
        let mut oversized = frames[0].to_vec();
        oversized.push(0);
        assert!(decode(&oversized, &settings).is_err());
        let requests: Vec<_> = (0..256).map(|block| ordinary(block, &[0])).collect();
        let mut next = 1;
        let frames = feedback::encode_bootstrap(9, &requests, &settings, &mut next).unwrap();
        assert_eq!((frames.len(), next), (4, 5));
        assert!(
            frames
                .iter()
                .all(|frame| decode(frame, &settings).unwrap().len() == 64)
        );
        let mut too_many_entries = frames[0].to_vec();
        too_many_entries.extend_from_slice(&literal(1, 256, 0, &[0])[32..]);
        assert!(decode(&too_many_entries, &settings).is_err());
        let mut too_many_units = literal(1, 0, 0, &(0..256).collect::<Vec<_>>());
        too_many_units.extend_from_slice(&literal(1, 1, 0, &[0])[32..]);
        assert!(decode(&too_many_units, &settings).is_err());
        let narrow = Settings {
            max_repair_shards: 19,
            ..Settings::default()
        };
        assert!(decode(&literal(1, 2048, 1, &[20]), &narrow).is_err());
    }

    #[test]
    fn outbound_validation_is_transactional_across_frames() {
        let settings = Settings {
            max_repair_shards: 256,
            ..Settings::default()
        };
        let mut duplicates: Vec<_> = (0..65).map(|block| ordinary(block, &[0])).collect();
        duplicates.push(ordinary(0, &[0]));
        for requests in [
            duplicates,
            vec![Bootstrap {
                block_id: 2048,
                reply_limit: 1,
            }],
            vec![ordinary(0, &[24])],
            vec![ordinary(0, &[0]); 257],
        ] {
            let mut next = 1;
            assert!(feedback::encode_bootstrap(9, &requests, &settings, &mut next).is_err());
            assert_eq!(next, 1);
        }
        let split: Vec<_> = (0..65).map(|block| ordinary(block, &[0])).collect();
        for initial in [0, u64::MAX, u64::MAX - 1] {
            let mut next = initial;
            assert!(feedback::encode_bootstrap(9, &split, &settings, &mut next).is_err());
            assert_eq!(next, initial);
        }
        let full = [
            Bootstrap {
                block_id: 2048,
                reply_limit: 20,
            },
            ordinary(0, &[0]),
        ];
        let cap = Settings {
            max_repair_shards: 20,
            ..Settings::default()
        };
        let mut next = 1;
        assert!(feedback::encode_bootstrap(9, &full, &cap, &mut next).is_err());
        assert_eq!(next, 1);
        assert!(
            feedback::encode_bootstrap(9, &[], &cap, &mut next)
                .unwrap()
                .is_empty()
        );
        assert_eq!(next, 1);
    }

    #[tokio::test]
    async fn invalid_high_sequence_does_not_consume_valid_retry() {
        let settings = Settings::default();
        let budget = Budget::new(16384);
        let routes = feedback::Routes::default();
        let mut route = routes.admit(9, 8, &budget).unwrap();
        let mut bad = literal(5000, 2048, 1, &[20]);
        bad.extend_from_slice(&literal(5000, 0, 0, &[256])[32..]);
        routes.receive(Bytes::from(bad), 100);
        assert!(
            route.next(&settings).now_or_never().is_none(),
            "valid prefix exposed"
        );
        let good = Bytes::from(literal(5000, 2048, 1, &[20]));
        routes.receive(good.clone(), 120);
        routes.receive(good, 120);
        assert_eq!(
            route.next(&settings).await,
            Bootstrap {
                block_id: 2048,
                reply_limit: 20
            }
        );
        assert!(
            route.next(&settings).now_or_never().is_none(),
            "duplicate exposed"
        );
        drop(route);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn admitted_version_generation_and_route_isolation() {
        let settings = Settings::default();
        let budget = Budget::new(1 << 20);
        let routes = feedback::Routes::default();
        let mut route = routes.admit(9, 8, &budget).unwrap();
        for format in [1, 2, 3] {
            let request = RepairRequest {
                block_id: 0,
                shard_indices: vec![0],
            };
            let mut next = 1;
            let frames = match format {
                1 => feedback::encode(9, &[request], &settings).unwrap(),
                2 => feedback::encode_sequenced(9, &[request], &settings, &mut next).unwrap(),
                _ => feedback::encode_parity(9, &[request], &settings, &mut next).unwrap(),
            };
            routes.receive(frames[0].clone(), 100);
        }
        let mut generation = literal(1, 2048, 1, &[20]);
        generation[23] = 2;
        routes.receive(Bytes::from(generation), 100);
        let mut wrong_id = literal(1, 2048, 1, &[20]);
        wrong_id[15] = 10;
        routes.receive(Bytes::from(wrong_id), 100);
        assert!(route.next(&settings).now_or_never().is_none());
        routes.receive(Bytes::from(literal(1, 2048, 1, &[20])), 120);
        assert_eq!(
            route.next(&settings).await,
            Bootstrap {
                block_id: 2048,
                reply_limit: 20
            }
        );
        drop(route);
        for version in 3..=7 {
            let mut old = routes.admit(9, version, &budget).unwrap();
            routes.receive(Bytes::from(literal(2, 2048, 1, &[20])), 140);
            assert!(old.next(&settings).now_or_never().is_none());
            let request = RepairRequest {
                block_id: 0,
                shard_indices: vec![0],
            };
            let mut sequence = 2;
            let frames = if version == 7 {
                feedback::encode_parity(9, std::slice::from_ref(&request), &settings, &mut sequence)
            } else if version >= 4 {
                feedback::encode_sequenced(
                    9,
                    std::slice::from_ref(&request),
                    &settings,
                    &mut sequence,
                )
            } else {
                feedback::encode(9, std::slice::from_ref(&request), &settings)
            }
            .unwrap();
            routes.receive(frames[0].clone(), 160);
            assert_eq!(old.next(&settings).await, Ordinary(request));
            drop(old);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn v8_media_capabilities_are_inherited_without_advertisement() {
        assert_eq!(Capabilities::for_version(8), Capabilities::V7);
        assert_eq!(
            control::protocols_with_features(Default::default()),
            control::protocols()
        );
        assert!(
            !control::protocols_with_features(crate::fxlink::Features {
                geometry_hints: false,
                trimmed_records: true,
                repair_parity: true,
                bootstrap_repair: false
            })
            .contains(&b"fxlink/8".to_vec())
        );
    }
}
