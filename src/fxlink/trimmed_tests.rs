//! V6 literal wire fixtures and default-off capability checks.
use super::*;
use bytes::Bytes;

fn native(len: usize) -> Bytes {
    let mut bytes = vec![0x6b; len];
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}

#[test]
fn trimmed_tests_v6_encodes_401_native_bytes_as_485_wire_bytes() {
    let budget = Budget::new(2 * 1024 * 1024);
    let mut encoder = Encoder::new(Settings::default(), 9, 3, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    let encoded = encoder.encode(native(401), 100).unwrap();
    assert_eq!(encoded.datagrams.len(), 1);
    assert_eq!(encoded.datagrams[0].len(), 485);
    assert_eq!(&encoded.datagrams[0][..8], b"FXLK\x01\x02\0\x40");
    assert_eq!(&encoded.datagrams[0][84..], &native(401)[..]);
    assert_eq!(encoded.created_ms, 100);
    drop(encoded);
    drop(encoder);
    assert_eq!(budget.used(), 0);
}

#[test]
fn trimmed_tests_v6_decodes_independent_485_byte_fixture() {
    let bytes = native(401);
    let mut wire = vec![0; 485];
    wire[..8].copy_from_slice(b"FXLK\x01\x02\0\x40");
    wire[8..16].copy_from_slice(&9u64.to_be_bytes());
    wire[16..24].copy_from_slice(&3u64.to_be_bytes());
    wire[40..44].copy_from_slice(&401u32.to_be_bytes());
    wire[44..48].copy_from_slice(&1u32.to_be_bytes());
    wire[52..54].copy_from_slice(&421u16.to_be_bytes());
    wire[54..56].copy_from_slice(&1u16.to_be_bytes());
    wire[60..62].copy_from_slice(&20u16.to_be_bytes());
    wire[62..64].copy_from_slice(&4u16.to_be_bytes());
    wire[72..76].copy_from_slice(&401u32.to_be_bytes());
    wire[80..82].copy_from_slice(&401u16.to_be_bytes());
    wire[84..].copy_from_slice(&bytes);
    packet::Header::parse(&wire).expect("structurally valid trimmed layout");
    let budget = Budget::new(2 * 1024 * 1024);
    let mut receiver = Receiver::new(Settings::default(), 9, 3, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    assert_eq!(receiver.ingest(&wire, 100).unwrap(), vec![bytes]);
    assert_eq!(receiver.stats().unprotected_blocks, 1);
    drop(receiver);
    assert_eq!(budget.used(), 0);
}

#[test]
fn trimmed_tests_boundaries_and_protection_preserve_legacy_fallback() {
    for (length, layout, packets, width) in [
        (12, Layout::Compact, 2, 96),
        (350, Layout::Compact, 2, 434),
        (396, Layout::Compact, 2, 480),
        (397, Layout::Trimmed, 1, 481),
        (401, Layout::Trimmed, 1, 485),
        (500, Layout::Trimmed, 1, 584),
        (1003, Layout::Trimmed, 1, 1087),
        (1004, Layout::Fixed, 1, 1088),
        (1005, Layout::Fixed, 2, 1088),
    ] {
        let b = Budget::new(2 << 20);
        let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        let encoded = e.encode(native(length), 20).unwrap();
        assert_eq!(encoded.datagrams.len(), packets, "L{length}");
        assert!(encoded.datagrams.iter().all(|p| p.len() == width));
        assert_eq!(
            packet::Header::parse(&encoded.datagrams[0]).unwrap().layout,
            layout
        );
        let mut r = Receiver::new(Settings::default(), 9, 3, b.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        let mut out = Vec::new();
        for p in &encoded.datagrams {
            out.extend(r.ingest(p, 30).unwrap());
        }
        assert_eq!(out, [native(length)]);
        drop((r, e, encoded));
        assert_eq!(b.used(), 0);
    }
    for (settings, length, layout, count, bytes) in [
        (
            Settings {
                shard_size: 256,
                ..Settings::default()
            },
            12,
            Layout::Compact,
            2,
            96,
        ),
        (
            Settings {
                shard_size: 256,
                ..Settings::default()
            },
            13,
            Layout::Trimmed,
            1,
            97,
        ),
        (
            Settings {
                shard_size: 256,
                ..Settings::default()
            },
            235,
            Layout::Trimmed,
            1,
            319,
        ),
        (
            Settings {
                shard_size: 256,
                ..Settings::default()
            },
            236,
            Layout::Fixed,
            1,
            320,
        ),
        (
            Settings {
                parity_shards: 0,
                ..Settings::default()
            },
            12,
            Layout::Trimmed,
            1,
            96,
        ),
        (
            Settings {
                data_shards: 1,
                parity_shards: 1,
                ..Settings::default()
            },
            401,
            Layout::Fixed,
            2,
            1088,
        ),
        (
            Settings {
                data_shards: 2,
                parity_shards: 3,
                ..Settings::default()
            },
            401,
            Layout::Fixed,
            2,
            1088,
        ),
    ] {
        let b = Budget::new(2 << 20);
        let mut e = Encoder::new(settings.clone(), 9, 3, b.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        let encoded = e.encode(native(length), 0).unwrap();
        assert_eq!(encoded.datagrams.len(), count);
        assert!(encoded.datagrams.iter().all(|p| p.len() == bytes));
        assert_eq!(
            packet::Header::parse(&encoded.datagrams[0]).unwrap().layout,
            layout
        );
        let mut r = Receiver::new(settings, 9, 3, b.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        let mut out = Vec::new();
        for p in &encoded.datagrams {
            out.extend(r.ingest(p, 0).unwrap());
        }
        assert_eq!(out, [native(length)]);
        drop((r, e, encoded));
        assert_eq!(b.used(), 0);
    }
    let b = Budget::new(2 << 20);
    let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6)
        .with_short_record_fec(false);
    let fallback = e.encode(native(396), 0).unwrap();
    assert_eq!(fallback.datagrams.len(), 1);
    assert_eq!(fallback.datagrams[0].len(), 1088);
    assert_eq!(
        packet::Header::parse(&fallback.datagrams[0])
            .unwrap()
            .layout,
        Layout::Fixed
    );
    drop((fallback, e));
    assert_eq!(b.used(), 0);
}

#[test]
fn trimmed_tests_v5_and_default_reject_before_reassembly_charge() {
    let b = Budget::new(2 << 20);
    let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    let encoded = e.encode(native(401), 0).unwrap();
    for cap in [Capabilities::Legacy, Capabilities::V5] {
        let own = Budget::new(2 << 20);
        let mut r = Receiver::new(Settings::default(), 9, 3, own.clone())
            .unwrap()
            .with_capabilities(cap);
        assert!(r.ingest(&encoded.datagrams[0], 0).is_err());
        assert!(r.is_retired());
        assert_eq!(own.used(), 0);
        let mut legacy = Encoder::new(Settings::default(), 9, 3, own.clone())
            .unwrap()
            .with_capabilities(cap);
        let old = legacy.encode(native(401), 0).unwrap();
        assert_eq!(old.datagrams[0].len(), 1088);
        assert_eq!(old.datagrams[0][5], 0);
        drop((old, legacy, r));
        assert_eq!(own.used(), 0);
    }
    drop((encoded, e));
    assert_eq!(b.used(), 0);
}

#[test]
fn trimmed_tests_small_budget_has_no_rs_scratch_or_counter_loss() {
    let b = Budget::new(1125);
    let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    let encoded = e.encode(native(401), 10).unwrap();
    assert_eq!(b.used(), 1125);
    assert_eq!(encoded.datagrams[0].len(), 485);
    drop(encoded);
    assert_eq!(b.used(), 0);
    let held = b.reserve(1).unwrap();
    assert!(matches!(
        e.encode(native(401), 20),
        Err(Error::BudgetExceeded)
    ));
    assert_eq!(e.next_record_id(), 1);
    drop(held);
    let next = e.encode(native(401), 30).unwrap();
    assert_eq!(next.record_id, 1);
    assert_eq!(next.created_ms, 30);
    drop((next, e));
    assert_eq!(b.used(), 0);
}

#[test]
fn trimmed_tests_conflicting_duplicates_fail_while_record_is_retained() {
    let b = Budget::new(2 << 20);
    let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    drop(e.encode(native(401), 0).unwrap());
    let future = e.encode(native(401), 10).unwrap();
    let mut r = Receiver::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    assert!(r.ingest(&future.datagrams[0], 20).unwrap().is_empty());
    let mut contradictory = future.datagrams[0].to_vec();
    contradictory[100] ^= 1;
    assert!(
        r.ingest(&contradictory, 30).is_err(),
        "retained record accepted contradictory data"
    );
    assert!(r.is_retired());
    drop((r, e, future));
    assert_eq!(b.used(), 0);
}

#[test]
fn trimmed_tests_duplicate_does_not_renew_gap_age_and_layouts_cannot_mix() {
    let b = Budget::new(2 << 20);
    let mut trimmed = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    let mut fixed = Encoder::new(Settings::default(), 9, 3, b.clone()).unwrap();
    drop(trimmed.encode(native(401), 0).unwrap());
    drop(fixed.encode(native(401), 0).unwrap());
    let t = trimmed.encode(native(401), 0).unwrap();
    let f = fixed.encode(native(401), 0).unwrap();
    for (first, second) in [
        (&t.datagrams[0], &f.datagrams[0]),
        (&f.datagrams[0], &t.datagrams[0]),
    ] {
        let mut r = Receiver::new(Settings::default(), 9, 3, b.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        assert!(r.ingest(first, 100).unwrap().is_empty());
        assert!(r.ingest(second, 101).is_err());
        assert!(r.is_retired());
    }
    let mut r = Receiver::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    r.announce(1, 401, 50).unwrap();
    assert!(r.ingest(&t.datagrams[0], 100).unwrap().is_empty());
    assert!(r.ingest(&t.datagrams[0], 1049).unwrap().is_empty());
    assert!(r.tick(1050).is_err());
    drop((r, trimmed, fixed, t, f));
    assert_eq!(b.used(), 0);
}

#[test]
fn trimmed_tests_malformed_geometry_and_profiles_do_not_allocate() {
    let b = Budget::new(2 << 20);
    let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    let encoded = e.encode(native(401), 0).unwrap();
    for (offset, bytes) in [
        (5, vec![3]),
        (5, vec![255]),
        (54, 2u16.to_be_bytes().to_vec()),
        (56, 1u16.to_be_bytes().to_vec()),
        (58, 1u16.to_be_bytes().to_vec()),
        (44, 2u32.to_be_bytes().to_vec()),
        (48, 1u32.to_be_bytes().to_vec()),
        (32, u64::MAX.to_be_bytes().to_vec()),
    ] {
        let mut p = encoded.datagrams[0].to_vec();
        p[offset..offset + bytes.len()].copy_from_slice(&bytes);
        assert!(packet::Header::parse(&p).is_err(), "offset{offset}");
    }
    for length in [396usize, 1004] {
        let mut ordinary = Encoder::new(Settings::default(), 9, 3, b.clone()).unwrap();
        let legacy = ordinary.encode(native(length), 0).unwrap();
        let mut forged = legacy.datagrams[0].to_vec();
        forged.truncate(length + 84);
        forged[5] = 2;
        forged[52..54].copy_from_slice(&((length + 20) as u16).to_be_bytes());
        packet::Header::parse(&forged).unwrap();
        let own = Budget::new(2 << 20);
        let mut r = Receiver::new(Settings::default(), 9, 3, own.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        assert!(
            r.ingest(&forged, 0).is_err(),
            "compact-eligible or untrimmed width accepted: L{length}"
        );
        assert_eq!(own.used(), 0);
        drop((r, legacy, ordinary));
    }
    for settings in [
        Settings {
            max_record_len: 400,
            ..Settings::default()
        },
        Settings {
            data_shards: 21,
            ..Settings::default()
        },
    ] {
        let own = Budget::new(2 << 20);
        let mut r = Receiver::new(settings, 9, 3, own.clone())
            .unwrap()
            .with_capabilities(Capabilities::V6);
        assert!(r.ingest(&encoded.datagrams[0], 0).is_err());
        assert_eq!(own.used(), 0);
    }
    let own = Budget::new(2 << 20);
    let mut stale = Receiver::new(Settings::default(), 9, 4, own.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    assert_eq!(stale.ingest(&encoded.datagrams[0], 0), Err(Error::Stale));
    assert_eq!(own.used(), 0);
    drop((stale, e, encoded));
    assert_eq!(b.used(), 0);
}

#[test]
fn trimmed_tests_missing_announcement_discovers_head_and_late_repair_keeps_order() {
    let b = Budget::new(2 << 20);
    let mut e = Encoder::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    let first = e.encode(native(401), 0).unwrap();
    let second = e.encode(native(401), 1).unwrap();
    let mut r = Receiver::new(Settings::default(), 9, 3, b.clone())
        .unwrap()
        .with_capabilities(Capabilities::V6);
    // Neither announcement reaches this receiver; future media exposes the head gap.
    assert!(r.ingest(&second.datagrams[0], 100).unwrap().is_empty());
    let requests = r.repair_requests(120, 10, 5);
    assert!(
        requests
            .iter()
            .any(|p| p.block_id == 0 && p.shard_indices.contains(&0))
    );
    assert_eq!(
        r.ingest(&first.datagrams[0], 200).unwrap(),
        [native(401), native(401)]
    );
    assert!(r.ingest(&first.datagrams[0], 250).unwrap().is_empty());
    assert_eq!(r.stats().late_shards, 1);
    r.cancel();
    drop((r, e, first, second));
    assert_eq!(b.used(), 0);
}
