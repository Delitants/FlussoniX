//! Compact very small records; ordinary constructors keep the legacy wire.
use super::*;
use bytes::Bytes;
fn native(length: usize) -> Bytes {
    let mut bytes = vec![83; length];
    bytes[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
#[test]
fn compact_short_record_recovers_a_lost_original_without_a_repair() {
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(Settings::default(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let encoded = encoder.encode(native(350), 0).unwrap();
    assert_eq!(
        encoded.datagrams.len(),
        2,
        "350-byte record has no proactive protection"
    );
    let mut receiver = Receiver::new(Settings::default(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let output = receiver.ingest(&encoded.datagrams[1], 150).unwrap();
    assert_eq!(output, [native(350)]);
    assert_eq!(receiver.stats().fec_recovered_shards, 1);
    assert!(receiver.repair_requests(170, 300, 5).is_empty());
    drop(encoded);
    drop(encoder);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_literal_header_payload_and_modeled_packet_budget() {
    let budget = Budget::new(8192);
    let mut encoder = Encoder::new(Settings::default(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let encoded = encoder.encode(native(350), 0).unwrap();
    let mut literal = b"FXLK\x01\x01\x00\x40".to_vec();
    for value in [7u64, 1, 0, 0] {
        literal.extend_from_slice(&value.to_be_bytes());
    }
    for value in [350u32, 1, 0] {
        literal.extend_from_slice(&value.to_be_bytes());
    }
    for value in [370u16, 1, 1, 0, 20, 4] {
        literal.extend_from_slice(&value.to_be_bytes());
    }
    assert_eq!(literal.len(), 64);
    assert_eq!(&encoded.datagrams[0][..64], literal);
    literal[58..60].copy_from_slice(&1u16.to_be_bytes());
    assert_eq!(&encoded.datagrams[1][..64], literal);
    let mut fragment = Vec::new();
    fragment.extend_from_slice(&0u64.to_be_bytes());
    fragment.extend_from_slice(&350u32.to_be_bytes());
    fragment.extend_from_slice(&0u32.to_be_bytes());
    fragment.extend_from_slice(&350u16.to_be_bytes());
    fragment.extend_from_slice(&[0, 0]);
    fragment.extend_from_slice(&native(350));
    assert_eq!(&encoded.datagrams[0][64..], fragment);
    assert_eq!(&encoded.datagrams[1][64..], fragment);
    assert_eq!(encoded.datagrams.iter().map(Bytes::len).sum::<usize>(), 868);
    assert_eq!(
        encoded
            .datagrams
            .iter()
            .map(|p| p.len() + 128)
            .sum::<usize>(),
        1124
    );
    drop(encoded);
    drop(encoder);
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_boundaries_preserve_legacy_bytes_outside_the_modeled_budget() {
    let base = Settings::default();
    for (settings, length, compact) in [
        (base.clone(), 12, true),
        (base.clone(), 191, true),
        (base.clone(), 350, true),
        (base.clone(), 396, true),
        (base.clone(), 397, false),
        (base.clone(), 460, false),
        (base.clone(), 1004, false),
        (base.clone(), 1005, false),
        (base.clone(), 12000, false),
        (
            Settings {
                shard_size: 640,
                ..base.clone()
            },
            204,
            true,
        ),
        (
            Settings {
                shard_size: 640,
                ..base.clone()
            },
            205,
            false,
        ),
        (
            Settings {
                shard_size: 256,
                ..base.clone()
            },
            12,
            true,
        ),
        (
            Settings {
                shard_size: 255,
                ..base.clone()
            },
            12,
            false,
        ),
        (
            Settings {
                parity_shards: 0,
                ..base.clone()
            },
            350,
            false,
        ),
        (
            Settings {
                data_shards: 1,
                parity_shards: 1,
                ..base.clone()
            },
            350,
            false,
        ),
        (
            Settings {
                parity_shards: 20,
                ..base.clone()
            },
            350,
            false,
        ),
        (
            Settings {
                parity_shards: 2,
                ..base.clone()
            },
            350,
            true,
        ),
        (
            Settings {
                parity_shards: 6,
                ..base
            },
            350,
            true,
        ),
    ] {
        let budget = Budget::new(2 << 20);
        let mut ordinary = Encoder::new(settings.clone(), 7, 1, budget.clone()).unwrap();
        let old = ordinary.encode(native(length), 0).unwrap();
        let mut encoder = Encoder::new(settings.clone(), 7, 1, budget.clone())
            .unwrap()
            .with_short_record_fec(true);
        let encoded = encoder.encode(native(length), 0).unwrap();
        assert_eq!(
            packet::Header::parse(&encoded.datagrams[0]).unwrap().layout == Layout::Compact,
            compact,
            "{settings:?},L{length}"
        );
        if compact {
            assert_eq!(encoded.datagrams.len(), 2);
            assert!(
                encoded
                    .datagrams
                    .iter()
                    .map(|p| p.len() + 128)
                    .sum::<usize>()
                    <= old.datagrams.iter().map(|p| p.len() + 128).sum::<usize>()
            );
            let mut receiver = Receiver::new(settings, 7, 1, budget.clone())
                .unwrap()
                .with_short_record_fec(true);
            assert_eq!(
                receiver.ingest(&encoded.datagrams[1], 0).unwrap(),
                [native(length)]
            );
        } else {
            assert_eq!(encoded.datagrams, old.datagrams);
        }
        drop(old);
        drop(encoded);
        drop(ordinary);
        drop(encoder);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn compact_rejection_precedes_receiver_allocation_and_malformed_shapes_fail() {
    let budget = Budget::new(1 << 20);
    let mut encoder = Encoder::new(Settings::default(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let encoded = encoder.encode(native(350), 0).unwrap();
    let baseline = budget.used();
    let mut legacy = Receiver::new(Settings::default(), 7, 1, budget.clone()).unwrap();
    assert!(legacy.ingest(&encoded.datagrams[1], 0).is_err());
    assert_eq!(budget.used(), baseline);
    let mut small = Receiver::new(
        Settings {
            shard_size: 640,
            ..Settings::default()
        },
        7,
        1,
        budget.clone(),
    )
    .unwrap()
    .with_short_record_fec(true);
    assert!(small.ingest(&encoded.datagrams[1], 0).is_err());
    assert_eq!(budget.used(), baseline);
    for (offset, value) in [
        (5, 2),
        (5, 3),
        (5, 0),
        (55, 2),
        (57, 0),
        (59, 2),
        (53, 1),
        (47, 2),
    ] {
        let mut bad = encoded.datagrams[1].to_vec();
        bad[offset] = value;
        assert!(
            packet::Header::parse(&bad).is_err(),
            "bad header offset{offset},value{value}"
        );
    }
    let mut extra = encoded.datagrams[1].to_vec();
    extra.push(0);
    assert!(packet::Header::parse(&extra).is_err());
    drop(encoded);
    drop(encoder);
    drop(legacy);
    drop(small);
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_and_legacy_cannot_conflict_within_a_retained_record() {
    let budget = Budget::new(1 << 20);
    let settings = Settings::default();
    let mut encoder = Encoder::new(settings.clone(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let _head = encoder.encode(native(1000), 0).unwrap();
    let compact = encoder.encode(native(350), 0).unwrap();
    let mut old = Encoder::new(settings.clone(), 7, 1, budget.clone()).unwrap();
    let _old_head = old.encode(native(1000), 0).unwrap();
    let ordinary = old.encode(native(350), 0).unwrap();
    let mut receiver = Receiver::new(settings, 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    assert!(
        receiver
            .ingest(&compact.datagrams[0], 0)
            .unwrap()
            .is_empty()
    );
    assert!(receiver.ingest(&ordinary.datagrams[0], 1).is_err());
    assert!(receiver.is_retired());
    drop(_head);
    drop(_old_head);
    drop(compact);
    drop(ordinary);
    drop(encoder);
    drop(old);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_parity_cannot_renew_an_original_gap_and_late_copies_allocate_nothing() {
    let budget = Budget::new(1 << 20);
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let mut encoder = Encoder::new(settings.clone(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let first = encoder.encode(native(350), 0).unwrap();
    let future = encoder.encode(native(350), 0).unwrap();
    let mut receiver = Receiver::new(settings.clone(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    assert!(receiver.ingest(&future.datagrams[0], 0).unwrap().is_empty());
    assert!(
        receiver
            .ingest(&future.datagrams[1], 499)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        receiver.tick(500),
        Err(Error::Expired { record_id: 0 })
    ));
    let mut receiver = Receiver::new(settings, 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    assert_eq!(
        receiver.ingest(&first.datagrams[0], 0).unwrap(),
        [native(350)]
    );
    let before = budget.used();
    assert!(
        receiver
            .ingest(&first.datagrams[1], 1500)
            .unwrap()
            .is_empty()
    );
    assert_eq!(budget.used(), before);
    assert_eq!(receiver.stats().late_shards, 1);
    drop(first);
    drop(future);
    drop(encoder);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn compact_small_budget_covers_encoding_and_copy_only_reconstruction() {
    let budget = Budget::new(8192);
    let mut encoder = Encoder::new(Settings::default(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    let encoded = encoder.encode(native(350), 0).unwrap();
    let mut receiver = Receiver::new(Settings::default(), 7, 1, budget.clone())
        .unwrap()
        .with_short_record_fec(true);
    assert_eq!(
        receiver.ingest(&encoded.datagrams[1], 0).unwrap(),
        [native(350)]
    );
    assert_eq!(receiver.stats().fec_recovered_shards, 1);
    drop(encoded);
    drop(encoder);
    drop(receiver);
    assert_eq!(budget.used(), 0);
    let tiny = Budget::new(1600);
    let mut denied = Encoder::new(Settings::default(), 7, 1, tiny.clone())
        .unwrap()
        .with_short_record_fec(true);
    assert!(denied.encode(native(350), 0).is_err());
    assert_eq!(denied.next_record_id(), 0);
    assert_eq!(tiny.used(), 0);
}
