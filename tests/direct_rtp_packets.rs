use flussonix::direct_rtp::packet::{Reorder, packet, parse};
use std::time::{Duration, Instant};
fn ts() -> Vec<u8> {
    let mut b = vec![0xff; 188];
    b[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    b
}
#[test]
fn packet_parser_rejects_truncation_wrong_payload_and_invalid_ts() {
    let body = ts();
    let p = packet(65535, 90, 42, &body);
    let r = parse(&p).unwrap();
    assert_eq!(r.sequence, 65535);
    assert_eq!(r.ssrc, 42);
    assert_eq!(r.payload, body);
    for n in 0..p.len() {
        assert!(parse(&p[..n]).is_err());
    }
    let mut wrong = p.clone();
    wrong[1] = 34;
    assert!(parse(&wrong).is_err());
    let mut wrong = p.clone();
    wrong[12] = 0;
    assert!(parse(&wrong).is_err());
    let mut ext = p.clone();
    ext[0] |= 0x10;
    assert!(parse(&ext).is_err());
    let mut padded = p.clone();
    padded[0] |= 0x20;
    padded.extend([0, 0, 0, 4]);
    assert_eq!(parse(&padded).unwrap().payload, body);
    *padded.last_mut().unwrap() = 255;
    assert!(parse(&padded).is_err());
    let mut csrc = p.clone();
    csrc[0] |= 1;
    csrc.splice(12..12, [0, 0, 0, 7]);
    assert_eq!(parse(&csrc).unwrap().payload, body);
    let mut ext = p.clone();
    ext[0] |= 0x10;
    ext.splice(12..12, [0, 1, 0, 1, 0, 0, 0, 9]);
    assert_eq!(parse(&ext).unwrap().payload, body);
}
#[test]
fn reorder_wraps_rejects_duplicates_and_bounds_gaps() {
    let now = Instant::now();
    let mut r = Reorder::new(Duration::from_millis(20));
    assert_eq!(r.push(65534, vec![1], now), vec![vec![1]]);
    assert!(r.push(0, vec![3], now).is_empty());
    assert_eq!(r.push(65535, vec![2], now), vec![vec![2], vec![3]]);
    assert!(r.push(65535, vec![2], now).is_empty());
    assert_eq!(r.duplicates, 1);
    assert!(r.push(2, vec![5], now).is_empty());
    assert!(r.flush(now + Duration::from_millis(19)).is_empty());
    assert_eq!(r.flush(now + Duration::from_millis(20)), vec![vec![5]]);
    assert_eq!(r.lost, 1);
    for seq in 4..500u16 {
        r.push(seq, vec![6], now);
        assert!(r.pending() <= 64);
    }
}
#[test]
fn reorder_does_not_delay_contiguous_data_when_jitter_is_zero() {
    let now = Instant::now();
    let mut r = Reorder::new(Duration::ZERO);
    assert_eq!(r.push(2, vec![2], now), vec![vec![2]]);
    assert_eq!(r.push(4, vec![4], now), vec![vec![4]]);
    assert_eq!(r.lost, 1);
}

#[test]
fn adaptation_fields_require_correct_occupancy_and_flag_directed_lengths() {
    let mut body = ts();
    for (afc, length, flags) in [
        (0x20, 0, 0),
        (0x30, 183, 0),
        (0x30, 1, 0x10),
        (0x30, 1, 0x08),
        (0x30, 1, 0x04),
        (0x30, 1, 0x02),
        (0x30, 1, 0x01),
    ] {
        body[3] = afc;
        body[4] = length;
        body[5] = flags;
        assert!(
            parse(&packet(1, 0, 42, &body)).is_err(),
            "AFC={afc:x} length={length} flags={flags:x}"
        );
    }
    body[3] = 0x30;
    body[4] = 2;
    body[5] = 2;
    body[6] = 10;
    assert!(
        parse(&packet(1, 0, 42, &body)).is_err(),
        "truncated private data"
    );
    body[5] = 1;
    body[6] = 10;
    assert!(
        parse(&packet(1, 0, 42, &body)).is_err(),
        "truncated extension"
    );
    for flag in [0x80, 0x40, 0x20] {
        body[4] = 3;
        body[5] = 1;
        body[6] = 1;
        body[7] = flag;
        assert!(
            parse(&packet(1, 0, 42, &body)).is_err(),
            "truncated extension field"
        );
    }
    body = ts();
    body[3] = 0x20;
    body[4] = 183;
    body[5] = 0;
    assert!(parse(&packet(1, 0, 42, &body)).is_ok());
    body = ts();
    body[3] = 0x30;
    body[4] = 0;
    assert!(parse(&packet(1, 0, 42, &body)).is_ok());
    // PCR, OPCR, splice countdown, private data, and all extension fields.
    body[4] = 33;
    body[5] = 0x1f;
    body[19] = 2;
    body[22] = 11;
    body[23] = 0xe0;
    assert!(parse(&packet(1, 0, 42, &body)).is_ok());
}
#[test]
fn rtcp_sdes_requires_terminated_bounded_aligned_chunks() {
    use flussonix::direct_rtp::packet::{Reception, receiver_report, valid_rtcp};
    let good = receiver_report(42, 43, &Reception::default());
    assert!(valid_rtcp(&good));
    for chunk in [
        vec![0x81, 202, 0, 1, 0, 0, 0, 42],
        vec![0x81, 202, 0, 2, 0, 0, 0, 42, 1, 8, 0, 0],
        vec![0x81, 202, 0, 2, 0, 0, 0, 42, 0, 1, 0, 0],
        vec![0x82, 202, 0, 2, 0, 0, 0, 42, 0, 0, 0, 0],
    ] {
        let mut bad = good[..32].to_vec();
        bad.extend(chunk);
        assert!(!valid_rtcp(&bad));
    }
}
