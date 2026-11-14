// These vectors detect wire drift, partial validation and replay-window errors.
use super::wire::{self, Frame, Replay, Summary};
use crate::fxlink::{MAX_BLOCKS_PER_RECORD, Settings};

fn frame(count: usize) -> Frame {
    Frame {
        subscription: 7,
        generation: 1,
        sequence: 1,
        entries: std::array::from_fn(|i| {
            if i < count {
                Summary {
                    record_id: i as u64,
                    record_len: 1000,
                }
            } else {
                Summary::default()
            }
        }),
        count,
    }
}

#[test]
fn literal_minimum_and_maximum_frames() {
    let settings = Settings::default();
    let mut minimum = b"FXGH\x01\x00\x00\x20".to_vec();
    minimum.extend_from_slice(&7u64.to_be_bytes());
    minimum.extend_from_slice(&1u64.to_be_bytes());
    minimum.extend_from_slice(&1u64.to_be_bytes());
    minimum.extend_from_slice(&0u64.to_be_bytes());
    minimum.extend_from_slice(&1000u32.to_be_bytes());
    assert_eq!(
        wire::encode(&frame(1), &settings).unwrap().as_ref(),
        minimum
    );
    let decoded = wire::decode(&minimum, &settings).unwrap();
    assert_eq!(decoded, frame(1));
    assert_eq!(decoded.encoded_len(), 44);
    let maximum = wire::encode(&frame(8), &settings).unwrap();
    assert_eq!(maximum.len(), 128);
    assert_eq!(&maximum[..44], minimum);
    assert_eq!(wire::decode(&maximum, &settings).unwrap(), frame(8));
    let mut another = frame(1);
    another.generation = 42;
    assert_eq!(
        wire::peek_route(&wire::encode(&another, &settings).unwrap()),
        Some((7, 42))
    );
}

#[test]
fn invalid_tail_does_not_validate() {
    let settings = Settings::default();
    let valid = wire::encode(&frame(8), &settings).unwrap();
    for length in 0..valid.len() {
        if length >= 44 && (length - 32) % 12 == 0 {
            continue;
        }
        assert!(
            wire::decode(&valid[..length], &settings).is_err(),
            "length {length}"
        );
    }
    for (offset, value) in [(0, b'X'), (4, 2), (5, 1), (7, 31)] {
        let mut bytes = valid.to_vec();
        bytes[offset] = value;
        assert!(wire::decode(&bytes, &settings).is_err(), "offset {offset}");
    }
    for extra in [1, 12] {
        let mut bytes = valid.to_vec();
        bytes.resize(bytes.len() + extra, 0);
        assert!(wire::decode(&bytes, &settings).is_err());
    }
    let mut bytes = valid.to_vec();
    bytes[124..128].copy_from_slice(&11u32.to_be_bytes());
    assert!(wire::decode(&bytes, &settings).is_err());
    // Recognition deliberately precedes full validation, for receiver work gates.
    assert_eq!(wire::peek_route(&bytes), Some((7, 1)));
    assert_eq!(wire::peek_route(&bytes[..23]), None);
    bytes[0] = b'X';
    assert_eq!(wire::peek_route(&bytes), None);
    for (range, value) in [(24..32, 0u64), (116..124, 0u64), (116..124, u64::MAX)] {
        let mut bytes = valid.to_vec();
        bytes[range].copy_from_slice(&value.to_be_bytes());
        assert!(wire::decode(&bytes, &settings).is_err());
    }
    let mut tiny = settings.clone();
    tiny.max_blocks = 1;
    let mut bytes = valid.to_vec();
    bytes[124..128].copy_from_slice(&30_000u32.to_be_bytes());
    assert!(wire::decode(&bytes, &tiny).is_err());
}

#[test]
fn semantic_bounds_and_checked_ids() {
    let mut settings = Settings::default();
    for count in [0, 9, usize::MAX] {
        assert!(wire::encode(&frame(count), &settings).is_err());
    }
    let mut zero_sequence = frame(1);
    zero_sequence.sequence = 0;
    assert!(wire::encode(&zero_sequence, &settings).is_err());
    for id in [0, 1] {
        let mut f = frame(2);
        f.entries[0].record_id = 1;
        f.entries[1].record_id = id;
        assert!(wire::encode(&f, &settings).is_err());
    }
    for len in [0, 11, settings.max_record_len as u32 + 1] {
        let mut f = frame(1);
        f.entries[0].record_len = len;
        assert!(wire::encode(&f, &settings).is_err());
    }
    let mut f = frame(1);
    f.entries[0].record_id = u64::MAX / MAX_BLOCKS_PER_RECORD as u64;
    assert!(wire::encode(&f, &settings).is_ok());
    f.entries[0].record_id += 1;
    assert!(wire::encode(&f, &settings).is_err());
    f = frame(1);
    f.entries[0].record_len = 30_000;
    settings.max_blocks = 1;
    assert!(wire::encode(&f, &settings).is_err());
}

#[test]
fn replay_has_an_independent_512_sequence_window() {
    let mut replay = Replay::default();
    assert!(!replay.admit(0));
    assert!(replay.admit(1000));
    assert!(!replay.admit(1000));
    assert!(replay.admit(489));
    assert!(!replay.admit(488));
    assert!(replay.admit(1001));
    assert!(!replay.admit(489));
    assert!(replay.admit(490));
    assert!(replay.admit(1065));
    assert!(!replay.admit(1000));
    assert!(replay.admit(1064));
    assert!(replay.admit(u64::MAX));
    assert!(!replay.admit(1));
    assert!(!replay.admit(u64::MAX));
    assert!(replay.admit(u64::MAX - 511));
    assert!(!replay.admit(u64::MAX - 512));
    assert!(std::mem::size_of::<Replay>() <= 80);
}
