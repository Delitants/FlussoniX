use super::*;
use crate::fxlink::{Budget, Encoder, Layout};
use std::collections::VecDeque;

fn record(n: usize) -> Bytes {
    let mut bytes = vec![17; n];
    bytes[..4].copy_from_slice(&((n - 4) as u32).to_be_bytes());
    bytes.into()
}
fn seal(
    record: EncodedRecord,
    settings: &Settings,
    caps: Capabilities,
) -> Result<CachedRecord, Error> {
    let mut pending = PendingRecord::new(record, settings, 9, 1, caps)?;
    let (record, validation) = pending.parts();
    for packet in &record.datagrams {
        validation.observe(&Header::parse(packet)?)?;
    }
    pending.finish()
}
fn encoded(settings: &Settings, caps: Capabilities, n: usize) -> EncodedRecord {
    Encoder::new(settings.clone(), 9, 1, Budget::new(64 << 20))
        .unwrap()
        .with_capabilities(caps)
        .encode(record(n), 100)
        .unwrap()
}
fn old_selection(record: &EncodedRecord, block: u64, shards: &[u16], parity: bool) -> Vec<Bytes> {
    let mut selected = false;
    record
        .datagrams
        .iter()
        .filter_map(|packet| {
            let h = Header::parse(packet).unwrap();
            if h.block_id == block
                && shards.contains(&h.shard_index)
                && (h.shard_index < h.data_count || (parity && !selected))
            {
                if h.shard_index >= h.data_count {
                    selected = true;
                }
                Some(packet.clone())
            } else {
                None
            }
        })
        .collect()
}
fn selection(
    record: &CachedRecord,
    block: u64,
    shards: &[u16],
    parity: bool,
) -> (Vec<Bytes>, usize) {
    let mask = Mask::new(shards);
    let mut selected = false;
    let mut inspected = 0;
    let result = record
        .ordinary_packets(block, &mask)
        .iter()
        .filter_map(|packet| {
            inspected += 1; // Before parsing/filtering, matching the actual source loop.
            let h = Header::parse(packet).unwrap();
            if h.block_id == block
                && mask.contains(h.shard_index)
                && (h.shard_index < h.data_count || (parity && !selected))
            {
                if h.shard_index >= h.data_count {
                    selected = true;
                }
                Some(packet.clone())
            } else {
                None
            }
        })
        .collect();
    (result, inspected)
}
#[test]
fn direct_lookup_has_literal_full_middle_terminal_widths_and_no_missing_block_work() {
    let settings = Settings {
        data_shards: 4,
        parity_shards: 2,
        shard_size: 64,
        ..Settings::default()
    };
    // 467 bytes /44-byte fragments =>11 fragments: widths6,6,4.
    let encoded = encoded(&settings, Capabilities::Legacy, 467);
    assert_eq!(encoded.datagrams.len(), 16);
    let cached = seal(encoded, &settings, Capabilities::Legacy).unwrap();
    for (block, width) in [(0, 6), (1, 6), (2, 4), (3, 0), (2047, 0), (u64::MAX, 0)] {
        let (packets, inspected) = selection(&cached, block, &[0], false);
        assert_eq!(inspected, width);
        assert_eq!(packets.len(), usize::from(width != 0));
    }
    assert_eq!(selection(&cached, 2, &[5, 65535], true).1, 0);
    assert_eq!(selection(&cached, 0, &[], true).1, 0);
}
#[test]
fn bounded_work_and_exact_output_cover_all_layouts_partial_tails_and_maximum_width() {
    for settings in [
        Settings::default(),
        Settings {
            parity_shards: 0,
            ..Settings::default()
        },
        Settings {
            data_shards: 128,
            parity_shards: 128,
            shard_size: 64,
            ..Settings::default()
        },
    ] {
        for caps in [Capabilities::Legacy, Capabilities::V5, Capabilities::V7] {
            for len in [12, 387, 401, 1004, 9781, 40001, 1_000_004] {
                let original = encoded(&settings, caps, len);
                let headers = original
                    .datagrams
                    .iter()
                    .map(|p| Header::parse(p).unwrap())
                    .collect::<Vec<_>>();
                let blocks = headers[0].block_count as u64;
                let cached = seal(original, &settings, caps).unwrap();
                for block in [0, blocks / 2, blocks - 1, blocks, 2047, u64::MAX] {
                    for shards in [
                        &[][..],
                        &[3, 0, 3, 65535][..],
                        &[0, 127, 128, 255][..],
                        &[254, 255][..],
                    ] {
                        for parity in [false, true] {
                            let expected = old_selection(&cached, block, shards, parity);
                            let (actual, inspected) = selection(&cached, block, shards, parity);
                            assert_eq!(actual, expected, "{caps:?},len={len},block={block}");
                            assert!(inspected <= 256, "record size must not expand request work");
                            assert!(actual.iter().all(|p| {
                                cached.datagrams.iter().any(|q| p.as_ptr() == q.as_ptr())
                            }));
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn compact_and_trimmed_use_actual_width_without_changing_exact_budget() {
    let settings = Settings::default();
    for (caps, len, layout, width) in [
        (Capabilities::V5, 387, Layout::Compact, 2),
        (Capabilities::V7, 401, Layout::Trimmed, 1),
    ] {
        let original = encoded(&settings, caps, len);
        assert_eq!(
            Header::parse(&original.datagrams[0]).unwrap().layout,
            layout
        );
        let cached = seal(original, &settings, caps).unwrap();
        assert_eq!(cached.ordinary_packets(0, &Mask::new(&[0])).len(), width);
    }
    let budget = Budget::new(1125).child(1125).child(1125);
    let held = budget.reserve(1).unwrap();
    let mut encoder = Encoder::new(settings.clone(), 9, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    assert!(matches!(
        encoder.encode(record(401), 100),
        Err(Error::BudgetExceeded)
    ));
    assert_eq!(encoder.next_record_id(), 0);
    drop(held);
    let cached = seal(
        encoder.encode(record(401), 100).unwrap(),
        &settings,
        Capabilities::V7,
    )
    .unwrap();
    assert_eq!(budget.used(), 1125);
    let mut queue = VecDeque::new();
    queue.push_back(cached);
    // Include the initial allocation's spare capacity, not just logical length.
    assert!(
        queue.capacity() * std::mem::size_of::<CachedRecord>()
            + budget.reservation_metadata_bytes()
            <= 512
    );
    drop(queue);
    drop(encoder);
    assert_eq!(budget.used(), 0);
}
#[test]
fn public_mutations_and_wrong_negotiated_context_cannot_publish_a_stale_cache() {
    let settings = Settings::default();
    for damage in 0..8 {
        let mut original = encoded(&settings, Capabilities::Legacy, 40001);
        match damage {
            0 => {
                original.datagrams.swap(0, 1);
            }
            1 => {
                original.datagrams.pop();
            }
            2 => {
                original.datagrams.push(original.datagrams[0].clone());
            }
            3 => {
                original.record_id = 1;
            }
            4 => {
                original.datagrams.swap(5, 6);
            }
            5 => {
                let mut bytes = original.datagrams[5].to_vec();
                let mut h = Header::parse(&bytes).unwrap();
                h.generation += 1;
                h.write(&mut bytes).unwrap();
                original.datagrams[5] = bytes.into();
            }
            6 => {
                for packet in &mut original.datagrams {
                    let mut bytes = packet.to_vec();
                    let mut h = Header::parse(&bytes).unwrap();
                    h.generation += 1;
                    h.write(&mut bytes).unwrap();
                    *packet = bytes.into();
                }
            }
            _ => {
                original.datagrams[5] = original.datagrams[6].clone();
            }
        }
        assert!(
            seal(original, &settings, Capabilities::Legacy).is_err(),
            "mutation {damage}"
        );
    }
    let mut pending = PendingRecord::new(
        encoded(&settings, Capabilities::Legacy, 40001),
        &settings,
        9,
        1,
        Capabilities::Legacy,
    )
    .unwrap();
    let (record, validation) = pending.parts();
    validation
        .observe(&Header::parse(&record.datagrams[0]).unwrap())
        .unwrap();
    assert!(
        pending.finish().is_err(),
        "partial observation must never publish cache"
    );
    assert!(
        PendingRecord::new(
            encoded(&settings, Capabilities::Legacy, 40001),
            &settings,
            10,
            1,
            Capabilities::Legacy
        )
        .is_err()
    );
}
#[test]
fn mask_never_narrows_large_indices_or_duplicates_and_preserves_upper_bits() {
    let mask = Mask::new(&[3, 0, 3, 65535, 255, 128]);
    assert!(mask.contains(0) && mask.contains(3) && mask.contains(128) && mask.contains(255));
    assert!(
        !mask.contains(1) && !mask.contains(127) && !mask.contains(254) && !mask.contains(65535)
    );
    assert!(!Mask::new(&[65535]).any_below(256));
    assert!(!Mask::new(&[255]).any_below(255));
    assert!(Mask::new(&[255]).any_below(256));
}
