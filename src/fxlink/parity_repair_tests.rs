//! Negotiated parity repair: real clock-injected FEC/selection, no socket model.
use super::repair::Pacing;
use super::{Budget, Capabilities, Encoder, Error, Receiver, RepairRequest, Settings};
use bytes::Bytes;
fn record(id: u8, len: usize) -> Bytes {
    let mut bytes = vec![id; len];
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}
fn receiver(settings: &Settings, budget: &Budget, version: u8) -> Receiver {
    Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::for_version(version))
}
fn select(receiver: &Receiver, pacing: &mut Pacing, at: u64, rtt: u64) -> Vec<RepairRequest> {
    pacing.prune(at, 0);
    let requests = receiver.repair_requests_if(at, rtt, 5, pacing.batch_eligible(at));
    for request in &requests {
        pacing
            .sent(request.block_id, &request.shard_indices, at, rtt)
            .unwrap();
    }
    requests
}
#[test]
fn delayed_geometry_lost_later_data_repair_recovers_with_negotiated_parity_before_500ms() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let expected = [record(0, 384), record(1, 5125), record(2, 420)];
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let frames = expected
        .iter()
        .map(|r| encoder.encode(r.clone(), 0).unwrap())
        .collect::<Vec<_>>();
    let mut receiver = receiver(&settings, &budget, 7);
    let mut pacing = Pacing::new(budget.clone(), settings.max_blocks).unwrap();
    assert!(
        receiver
            .ingest(&frames[2].datagrams[0], 0)
            .unwrap()
            .is_empty()
    );
    let first = select(&receiver, &mut pacing, 53, 195);
    assert_eq!(
        first[1].shard_indices,
        [0],
        "unknown middle prefix stays bounded"
    );
    receiver.announce(0, 384, 180).unwrap();
    receiver.announce(1, 5125, 198).unwrap();
    let second = select(&receiver, &mut pacing, 202, 190);
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].shard_indices,
        [1, 2, 3, 4, 5, 6],
        "reserve one useful cached parity reply"
    );
    let mut output = receiver.ingest(&frames[1].datagrams[0], 245).unwrap();
    for (index, at) in [(1, 389), (5, 395), (4, 396), (2, 398), (6, 399)] {
        output.extend(receiver.ingest(&frames[1].datagrams[index], at).unwrap());
    }
    output.extend(receiver.ingest(&frames[0].datagrams[0], 452).unwrap());
    assert_eq!(output, expected);
    assert_eq!(receiver.stats().fec_recovered_shards, 1);
    assert_eq!(receiver.stats().expired_records, 0);
    receiver.tick(500).unwrap();
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}
#[test]
fn optional_parity_never_steals_a_later_records_data_quota() {
    for (cap, want_first) in [(7, vec![0, 1, 2, 3, 4, 5]), (8, vec![0, 1, 2, 3, 4, 5, 6])] {
        let settings = Settings {
            latency_ms: 500,
            max_repair_shards: cap,
            ..Settings::default()
        };
        let budget = Budget::new(1 << 20);
        let mut receiver = receiver(&settings, &budget, 7);
        receiver.announce(0, 5125, 0).unwrap();
        receiver.announce(1, 1000, 0).unwrap();
        let requests = receiver.repair_requests(60, 190, 5);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].shard_indices, want_first);
        assert_eq!(requests[1].shard_indices, [0]);
        drop(receiver);
        assert_eq!(budget.used(), 0);
    }
}
#[test]
fn legacy_pending_received_and_deadline_fences_keep_parity_optional() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    for version in [1, 5, 6, 7] {
        let mut receiver = receiver(&settings, &budget, version);
        receiver.announce(0, 5125, 0).unwrap();
        let requests = receiver.repair_requests_avoiding(60, 190, 5, |_, _| true, |_, s| s == 6);
        assert_eq!(requests[0].shard_indices, [0, 1, 2, 3, 4, 5]);
        if version < 7 {
            assert_eq!(
                receiver.repair_requests(60, 190, 5)[0].shard_indices,
                [0, 1, 2, 3, 4, 5]
            );
        }
        assert!(receiver.repair_requests(305, 190, 5).is_empty());
        assert_eq!(receiver.tick(500), Err(Error::Expired { record_id: 0 }));
    }
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let frame = encoder.encode(record(1, 5125), 0).unwrap();
    let mut receiver = receiver(&settings, &budget, 7);
    receiver.ingest(&frame.datagrams[6], 0).unwrap();
    assert_eq!(
        receiver.repair_requests(60, 190, 5)[0].shard_indices,
        [0, 1, 2, 3, 4, 5]
    );
    drop(receiver);
    assert_eq!(budget.used(), 0);
}
#[test]
fn parity_retries_do_not_walk_indices_or_renew_the_original_deadline() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut receiver = receiver(&settings, &budget, 7);
    let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
    receiver.announce(0, 5125, 0).unwrap();
    assert_eq!(
        select(&receiver, &mut pacing, 60, 190)[0].shard_indices,
        [0, 1, 2, 3, 4, 5, 6]
    );
    assert!(select(&receiver, &mut pacing, 80, 190).is_empty());
    assert_eq!(
        select(&receiver, &mut pacing, 255, 190)[0].shard_indices,
        [0, 1, 2, 3, 4, 5, 6]
    );
    assert!(select(&receiver, &mut pacing, 305, 190).is_empty());
    assert_eq!(receiver.tick(500), Err(Error::Expired { record_id: 0 }));
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}
#[test]
fn repair_history_tracks_127_128_and_255_without_aliasing() {
    let budget = Budget::new(1 << 20);
    let mut pacing = Pacing::new(budget.clone(), 1).unwrap();
    pacing.sent(0, &[255], 0, 190).unwrap();
    assert!(!pacing.eligible(0, 255, 1));
    assert!(pacing.eligible(0, 127, 1));
    assert!(pacing.eligible(0, 128, 1));
    pacing.sent(0, &[127, 128], 20, 190).unwrap();
    for index in [127, 128, 255] {
        assert!(!pacing.eligible(0, index, 21));
    }
    assert!(pacing.sent(1, &[0], 21, 190).is_err());
    pacing.prune(215, 0);
    for index in [127, 128, 255] {
        assert!(pacing.eligible(0, index, 215));
    }
    drop(pacing);
    assert_eq!(budget.used(), 0);
}
fn literal(format: u8, sequence: u64, indices: &[u16]) -> Bytes {
    let mut packet = b"FXRQ\x03\x00\x00\x20".to_vec();
    packet[4] = format;
    for value in [7u64, 1, sequence, 9] {
        packet.extend_from_slice(&value.to_be_bytes());
    }
    packet.extend_from_slice(&(indices.len() as u16).to_be_bytes());
    for index in indices {
        packet.extend_from_slice(&index.to_be_bytes());
    }
    Bytes::from(packet)
}
#[test]
fn negotiated_feedback_accepts_upper_parity_indices_but_preserves_legacy_bounds() {
    let settings = Settings {
        data_shards: 128,
        parity_shards: 128,
        max_repair_shards: 3,
        ..Settings::default()
    };
    let requests = super::feedback::decode(&literal(3, 1, &[127, 128, 255]), &settings)
        .expect("negotiated parity format");
    assert_eq!(requests[0].shard_indices, [127, 128, 255]);
    for (format, sequence, indices) in [
        (2, 1, vec![128]),
        (3, 0, vec![127]),
        (3, 1, vec![255, 255]),
        (3, 1, vec![256]),
        (3, 1, vec![0, 1, 2, 3]),
    ] {
        assert!(super::feedback::decode(&literal(format, sequence, &indices), &settings).is_err());
    }
}
#[tokio::test]
async fn new_feedback_is_bound_to_negotiated_route_and_claimed_once() {
    let budget = Budget::new(1 << 20);
    let routes = super::feedback::Routes::default();
    let mut route = routes.admit(7, 7, &budget).expect("v7 route");
    routes.receive(literal(2, 1, &[0]), 0);
    routes.receive(literal(3, 1, &[6]), 0);
    routes.receive(literal(3, 1, &[6]), 0);
    let message = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        route.next(&Settings::default()),
    )
    .await
    .unwrap();
    let super::feedback::DatagramRequest::Ordinary(RepairRequest {
        block_id: block,
        shard_indices: shards,
    }) = message
    else {
        panic!("repair")
    };
    assert_eq!((block, shards), (9, vec![6]));
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            route.next(&Settings::default())
        )
        .await
        .is_err()
    );
    drop(route);
    let mut old = routes.admit(7, 6, &budget).unwrap();
    routes.receive(literal(3, 2, &[6]), 20);
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            old.next(&Settings::default())
        )
        .await
        .is_err()
    );
    drop(old);
    assert_eq!(budget.used(), 0);
}

#[test]
fn parity_encoder_is_bounded_transactional_and_literal_wire_exact() {
    let settings = Settings {
        data_shards: 128,
        parity_shards: 128,
        max_repair_shards: 256,
        ..Settings::default()
    };
    let request = RepairRequest {
        block_id: 9,
        shard_indices: vec![127, 128, 255],
    };
    let mut next = 1;
    let frames = super::feedback::encode_parity(7, &[request], &settings, &mut next).unwrap();
    assert_eq!(frames, [literal(3, 1, &[127, 128, 255])]);
    assert_eq!(next, 2);
    let requests = (0..256)
        .map(|block_id| RepairRequest {
            block_id,
            shard_indices: vec![255],
        })
        .collect::<Vec<_>>();
    let frames = super::feedback::encode_parity(7, &requests, &settings, &mut next).unwrap();
    assert_eq!(frames.len(), 4);
    assert_eq!(next, 6);
    assert!(frames.iter().all(|frame| frame.len() <= 1024));
    next = u64::MAX - 1;
    assert!(super::feedback::encode_parity(7, &requests, &settings, &mut next).is_err());
    assert_eq!(next, u64::MAX - 1);
    let bad = RepairRequest {
        block_id: 9,
        shard_indices: vec![255, 255],
    };
    next = 1;
    assert!(super::feedback::encode_parity(7, &[bad], &settings, &mut next).is_err());
    assert_eq!(next, 1);
}
#[test]
fn negotiated_optins_preserve_old_protocol_defaults_and_layouts() {
    let defaults = super::control::protocols();
    assert_eq!(
        super::control::protocols_with_features(super::Features::default()),
        defaults
    );
    assert_eq!(
        super::control::protocols_with_features(super::Features {
            repair_parity: true,
            ..Default::default()
        }),
        defaults
    );
    let protocols = super::control::protocols_with_features(super::Features {
        geometry_hints: false,
        trimmed_records: true,
        repair_parity: true,
        bootstrap_repair: false,
    });
    assert_eq!(
        &protocols[..2],
        &[b"fxlink/7".to_vec(), b"fxlink/6".to_vec()]
    );
    assert_eq!(&protocols[2..], &defaults);
    assert_eq!(Capabilities::for_version(7), Capabilities::V7);
}

#[test]
fn parity_reuses_the_only_history_slot_and_loss_keeps_original_expiry() {
    let settings = Settings {
        latency_ms: 500,
        max_blocks: 1,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut receiver = receiver(&settings, &budget, 7);
    let mut pacing = Pacing::new(budget.clone(), 1).unwrap();
    let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
    let frame = encoder.encode(record(0, 5125), 0).unwrap();
    receiver.announce(0, 5125, 0).unwrap();
    let request = select(&receiver, &mut pacing, 60, 190);
    assert_eq!(request[0].shard_indices, [0, 1, 2, 3, 4, 5, 6]);
    // Lose both data3 and its optional parity: the new capability must not
    // claim recovery, request a moving parity prefix or renew the500ms age.
    for index in [0, 1, 2, 4, 5] {
        assert!(
            receiver
                .ingest(&frame.datagrams[index], 450)
                .unwrap()
                .is_empty()
        );
    }
    assert!(select(&receiver, &mut pacing, 451, 190).is_empty());
    assert_eq!(
        receiver.ingest(&frame.datagrams[6], 500),
        Err(Error::Expired { record_id: 0 })
    );
    assert_eq!(receiver.stats().expired_records, 1);
    assert_eq!(receiver.stats().original_lost_shards, 1);
    drop(receiver);
    drop(pacing);
    assert_eq!(budget.used(), 0);
}
