use bytes::Bytes;
use flussonix::fxlink::{
    Address, Budget, Encoder, Receiver, Settings, fec,
    packet::{HEADER_LEN, Header},
};

fn record(body_len: usize) -> Bytes {
    flussonix::m4s::encode_gop(&flussonix::m4s::PackedGop {
        utc: 1700000000,
        dts_ms: 1234.0,
        sequence: 7,
        duration_ms: 40.0,
        body: Bytes::from((0..body_len).map(|n| (n % 251) as u8).collect::<Vec<_>>()),
    })
    .unwrap()
}
fn budget() -> Budget {
    Budget::new(512 << 20).child(128 << 20).child(32 << 20)
}
fn pair(settings: Settings) -> (Encoder, Receiver) {
    (
        Encoder::new(settings.clone(), 1, 2, budget()).unwrap(),
        Receiver::new(settings, 1, 2, budget()).unwrap(),
    )
}
#[test]
fn settings_and_safe_addresses() {
    let s = Settings::default();
    assert_eq!(
        (s.latency_ms, s.data_shards, s.parity_shards, s.shard_size),
        (1000, 20, 4, 1024)
    );
    for latency_ms in [0, 499, 1501, u64::MAX] {
        assert!(
            Settings {
                latency_ms,
                ..s.clone()
            }
            .validate()
            .is_err()
        );
    }
    for shard_size in [0, 20, 1025, usize::MAX] {
        assert!(
            Settings {
                shard_size,
                ..s.clone()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        Settings {
            data_shards: 0,
            ..s.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        Settings {
            data_shards: 255,
            parity_shards: 255,
            ..s.clone()
        }
        .validate()
        .is_err()
    );
    assert_eq!(
        Address::parse("fxlink://example.test:55000/live/news%20room", true)
            .unwrap()
            .stream
            .as_deref(),
        Some("live/news room")
    );
    assert_eq!(
        Address::parse("fxlink://[::1]:55000", false).unwrap().host,
        "::1"
    );
    for input in [
        "fxlink://a/live",
        "fxlink://a:0/live",
        "fxlink://u:p@a:50/live",
        "fxlink://a:50/live?q=1",
        "fxlink://a:50/live#x",
        "fxlink://a:50/../x",
        "fxlink://a:50/%2e%2e/x",
        "fxlink://a:50/a%2fb",
        "fxlink://a:50/%00",
        "fxlink://a:50/%zz",
        "fxlink://a:50/",
    ] {
        assert!(Address::parse(input, true).is_err(), "{input}");
    }
}
#[test]
fn hierarchical_budget_unwinds_errors_and_cancellation() {
    let global = Budget::new(1000);
    let peer = global.child(800);
    let sub = peer.child(500);
    let a = sub.reserve(400).unwrap();
    assert_eq!(global.used(), 400);
    assert!(sub.reserve(101).is_err());
    assert_eq!(peer.used(), 400);
    drop(a);
    assert_eq!(global.used(), 0);
    let b = budget();
    let (mut e, _) = pair(Settings::default());
    let p = e.encode(record(40000), 0).unwrap();
    let mut r = Receiver::new(Settings::default(), 1, 2, b.clone()).unwrap();
    r.ingest(&p.datagrams[0], 0).unwrap();
    assert!(b.used() > 40000);
    r.cancel();
    assert_eq!(b.used(), 0);
    assert!(r.is_retired());
    let tiny = Budget::new(10);
    let mut e = Encoder::new(Settings::default(), 1, 2, tiny.clone()).unwrap();
    assert!(e.encode(record(100), 0).is_err());
    assert_eq!(tiny.used(), 0);
}
#[test]
fn fec_golden_vector_and_all_small_erasure_patterns() {
    let mut shards = vec![vec![1, 2, 3], vec![4, 5, 6], vec![0; 3]];
    fec::encode(&mut shards, 2, 1).unwrap();
    assert_eq!(shards[2], vec![11, 12, 9]);
    let mut original = vec![
        vec![3, 7, 11, 255],
        vec![9, 0, 18, 54],
        vec![32, 5, 77, 123],
        vec![0; 4],
        vec![0; 4],
    ];
    fec::encode(&mut original, 3, 2).unwrap();
    for mask in 0u32..32 {
        if mask.count_ones() > 2 {
            continue;
        }
        let mut damaged = original
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if mask & (1 << i) == 0 {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        fec::reconstruct(&mut damaged, 3, 2).unwrap();
        for i in 0..3 {
            assert_eq!(damaged[i].as_ref(), Some(&original[i]), "mask {mask}");
        }
    }
}
#[test]
fn literal_wire_header_and_closed_short_block() {
    let (mut e, mut r) = pair(Settings::default());
    let native = record(10);
    let encoded = e.encode(native.clone(), 0).unwrap();
    assert_eq!(encoded.datagrams.len(), 1);
    let d = &encoded.datagrams[0];
    assert_eq!(&d[..8], b"FXLK\x01\x00\x00\x40");
    assert_eq!(&d[8..24], &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
    assert_eq!(d.len(), HEADER_LEN + 1024);
    let h = Header::parse(d).unwrap();
    assert_eq!(
        (h.data_count, h.parity_count, h.record_len),
        (1, 0, native.len() as u32)
    );
    assert_eq!(r.ingest(d, 10).unwrap(), vec![native]);
    assert_eq!(r.stats().unprotected_blocks, 1);
    for len in 0..HEADER_LEN {
        assert!(Header::parse(&d[..len]).is_err());
    }
    let mut bad = d.to_vec();
    bad[4] = 2;
    assert!(Header::parse(&bad).is_err());
}
#[test]
fn exact_native_records_recover_reorder_and_duplicate() {
    let settings = Settings {
        data_shards: 4,
        parity_shards: 2,
        ..Settings::default()
    };
    let (mut e, mut r) = pair(settings);
    let a = record(22000);
    let b = record(3000);
    let ea = e.encode(a.clone(), 0).unwrap();
    let eb = e.encode(b.clone(), 0).unwrap();
    let mut out = Vec::new();
    for d in eb.datagrams.iter().rev() {
        out.extend(r.ingest(d, 10).unwrap());
    }
    assert!(out.is_empty());
    for d in ea.datagrams.iter().rev() {
        let h = Header::parse(d).unwrap();
        if h.shard_index == 0 {
            continue;
        }
        out.extend(r.ingest(d, 20).unwrap());
        out.extend(r.ingest(d, 20).unwrap());
    }
    assert_eq!(out, vec![a, b]);
    assert!(r.stats().fec_recovered_shards > 0);
    assert!(r.stats().original_lost_shards > 0);
    assert!(r.stats().duplicate_shards > 0);
}
#[test]
fn insufficient_parity_and_entire_missing_record_expire_without_deadline_extension() {
    let (mut e, mut r) = pair(Settings::default());
    let p = e.encode(record(40000), 0).unwrap();
    r.ingest(&p.datagrams[5], 7).unwrap();
    r.ingest(&p.datagrams[5], 1000).unwrap();
    assert!(r.tick(1006).is_ok());
    assert!(r.tick(1007).is_err());
    assert!(r.is_retired());
    assert_eq!(r.stats().expired_records, 1);
    let _ = e.encode(record(100), 0).unwrap();
    let future = e.encode(record(100), 0).unwrap();
    let mut r = Receiver::new(Settings::default(), 1, 2, budget()).unwrap();
    for d in &future.datagrams {
        assert!(r.ingest(d, 10).unwrap().is_empty());
    }
    assert!(r.tick(1010).is_err());
}
#[test]
fn malformed_protected_fragment_retires_before_delivery_and_stale_is_rejected() {
    let (mut e, mut r) = pair(Settings::default());
    let p = e.encode(record(100), 0).unwrap();
    let mut bad = p.datagrams[0].to_vec();
    bad[HEADER_LEN + 15] ^= 1;
    assert!(r.ingest(&bad, 0).is_err());
    assert!(r.is_retired());
    let mut r = Receiver::new(Settings::default(), 1, 3, budget()).unwrap();
    assert!(r.ingest(&p.datagrams[0], 0).is_err());
    assert!(!r.is_retired());
}
#[test]
fn repairs_are_bounded_and_cannot_extend_the_latency_budget() {
    let settings = Settings {
        max_repair_shards: 3,
        ..Settings::default()
    };
    let (mut e, mut r) = pair(settings);
    let p = e.encode(record(40000), 0).unwrap();
    r.ingest(&p.datagrams[0], 0).unwrap();
    assert_eq!(
        r.repair_requests(10, 100, 10)
            .iter()
            .map(|r| r.shard_indices.len())
            .sum::<usize>(),
        3
    );
    assert!(r.repair_requests(900, 100, 1).is_empty());
}
#[test]
fn maximum_record_and_over_limit_record() {
    let s = Settings::default();
    let (mut e, mut r) = pair(s.clone());
    let native = record((16 << 20) - 80);
    assert_eq!(native.len(), s.max_record_len);
    let p = e.encode(native.clone(), 0).unwrap();
    let mut out = Vec::new();
    for d in &p.datagrams {
        out.extend(r.ingest(d, 1).unwrap());
    }
    assert_eq!(out, vec![native]);
    let mut bad = vec![0; s.max_record_len + 1];
    let prefix = ((bad.len() - 4) as u32).to_be_bytes();
    bad[..4].copy_from_slice(&prefix);
    assert!(e.encode(Bytes::from(bad), 0).is_err());
}

#[test]
fn reliable_announcement_expires_total_erasure_and_duplicates_do_not_extend() {
    let mut r = Receiver::new(Settings::default(), 1, 2, budget()).unwrap();
    r.announce(0, record(100).len(), 10).unwrap();
    assert!(!r.repair_requests(11, 100, 0).is_empty());
    r.announce(0, record(100).len(), 900).unwrap();
    assert!(r.tick(1009).is_ok());
    assert!(r.tick(1010).is_err());
}

#[test]
fn packet_level_all_small_erasure_patterns_and_unrecoverable_boundary() {
    let settings = Settings {
        data_shards: 3,
        parity_shards: 2,
        ..Settings::default()
    };
    let (mut encoder, _) = pair(settings.clone());
    let native = record(2200);
    let encoded = encoder.encode(native.clone(), 0).unwrap();
    assert_eq!(encoded.datagrams.len(), 5);
    for mask in 0u32..32 {
        let mut receiver = Receiver::new(settings.clone(), 1, 2, budget()).unwrap();
        receiver.announce(0, native.len(), 0).unwrap();
        let mut out = Vec::new();
        for (i, d) in encoded.datagrams.iter().enumerate().rev() {
            if mask & (1 << i) == 0 {
                out.extend(receiver.ingest(d, 100).unwrap());
            }
        }
        if mask.count_ones() <= 2 {
            assert_eq!(out, vec![native.clone()], "mask {mask}");
        } else {
            assert!(out.is_empty());
            assert!(receiver.tick(1000).is_err());
        }
    }
}
#[test]
fn hostile_headers_do_not_allocate_and_record_conflicts_fail_closed() {
    let (mut encoder, _) = pair(Settings::default());
    let encoded = encoder.encode(record(5000), 0).unwrap();
    let storage = budget();
    let mut receiver = Receiver::new(Settings::default(), 1, 2, storage.clone()).unwrap();
    for (offset, bytes) in [
        (40, &u32::MAX.to_be_bytes()[..]),
        (44, &u32::MAX.to_be_bytes()[..]),
        (52, &u16::MAX.to_be_bytes()[..]),
        (54, &u16::MAX.to_be_bytes()[..]),
        (58, &u16::MAX.to_be_bytes()[..]),
    ] {
        let mut wire = encoded.datagrams[0].to_vec();
        wire[offset..offset + bytes.len()].copy_from_slice(bytes);
        assert!(receiver.ingest(&wire, 0).is_err());
        assert_eq!(storage.used(), 0);
    }
    receiver.ingest(&encoded.datagrams[0], 0).unwrap();
    let mut conflict = encoded.datagrams[1].to_vec();
    conflict[40..44].copy_from_slice(&((record(5000).len() + 1) as u32).to_be_bytes());
    assert!(receiver.ingest(&conflict, 1).is_err());
    assert!(receiver.is_retired());
    assert_eq!(storage.used(), 0);
}
#[test]
fn byte_and_independent_count_caps_release_every_reservation() {
    let (mut encoder, _) = pair(Settings::default());
    let encoded = encoder.encode(record(40000), 0).unwrap();
    let tiny = Budget::new(1000);
    let mut receiver = Receiver::new(Settings::default(), 1, 2, tiny.clone()).unwrap();
    assert!(receiver.ingest(&encoded.datagrams[0], 0).is_err());
    assert_eq!(tiny.used(), 0);
    assert!(receiver.is_retired());
    let store = budget();
    let mut receiver = Receiver::new(
        Settings {
            max_records: 1,
            ..Settings::default()
        },
        1,
        2,
        store.clone(),
    )
    .unwrap();
    receiver.announce(1, 1000, 0).unwrap();
    assert!(receiver.announce(2, 1000, 1).is_err());
    assert_eq!(store.used(), 0);
    let small = Settings {
        data_shards: 1,
        parity_shards: 0,
        ..Settings::default()
    };
    let mut encoder = Encoder::new(small, 1, 2, budget()).unwrap();
    assert!(encoder.encode(record(3 << 20), 0).is_err());
    assert_eq!(encoder.next_record_id(), 0);
}
#[test]
fn invalid_native_prefix_and_encoder_drop_are_accounted() {
    let store = budget();
    let mut encoder = Encoder::new(Settings::default(), 1, 2, store.clone()).unwrap();
    for value in [
        Bytes::new(),
        Bytes::from_static(b"\0\0\0\x07abcdefgh"),
        Bytes::from_static(b"\0\0\0\x08abc"),
    ] {
        assert!(encoder.encode(value, 0).is_err());
        assert_eq!(store.used(), 0);
    }
    let encoded = encoder.encode(record(30000), 0).unwrap();
    assert!(store.used() > 30000);
    drop(encoded);
    assert_eq!(store.used(), 0);
    let mut receiver = Receiver::new(Settings::default(), 1, 2, store.clone()).unwrap();
    receiver.announce(0, 40000, 0).unwrap();
    assert!(store.used() > 40000);
    drop(receiver);
    assert_eq!(store.used(), 0);
}

#[test]
fn expiry_counts_unrecovered_original_data_and_releases_bytes() {
    let store = budget();
    let mut receiver = Receiver::new(Settings::default(), 1, 2, store.clone()).unwrap();
    receiver.announce(0, record(40000).len(), 0).unwrap();
    assert!(receiver.tick(1000).is_err());
    assert!(receiver.stats().original_lost_shards > 0);
    assert_eq!(store.used(), 0);
}
#[test]
fn repeated_recovery_keeps_codec_cache_bounded_and_cancel_releases_it() {
    let settings = Settings {
        data_shards: 4,
        parity_shards: 2,
        ..Settings::default()
    };
    let store = budget();
    let mut encoder = Encoder::new(settings.clone(), 1, 2, budget()).unwrap();
    let mut receiver = Receiver::new(settings, 1, 2, store.clone()).unwrap();
    let native = record(100000);
    let encoded = encoder.encode(native.clone(), 0).unwrap();
    let mut out = Vec::new();
    for d in &encoded.datagrams {
        let h = Header::parse(d).unwrap();
        if h.shard_index != 0 {
            out.extend(receiver.ingest(d, 0).unwrap());
        }
    }
    assert_eq!(out, vec![native]);
    assert!(receiver.stats().fec_recovered_shards > 8);
    assert!(store.used() < (1 << 20));
    receiver.cancel();
    assert_eq!(store.used(), 0);
}
