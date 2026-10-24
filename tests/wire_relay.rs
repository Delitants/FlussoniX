use bytes::Bytes;
use flussonix::{
    m4f::{Frame, pack},
    m4s::{Decoder, Event, PackedGop, Track},
    media_queue::Channel,
    wire::{Hub, Segment, encode_frame, encode_info},
};
use std::time::Duration;
fn tracks() -> Vec<Track> {
    vec![Track {
        id: 7,
        codec: "h264".into(),
        config: vec![1, 100, 0, 40],
    }]
}
fn frame(key: bool, dts: u64) -> Frame {
    Frame {
        track_id: 7,
        dts,
        pts_offset: -3600,
        key,
        body: vec![0, 0, 0, 1, if key { 101 } else { 65 }],
    }
}
#[tokio::test]
async fn queue_caps_bytes_and_records_and_reports_lag() {
    let q = Channel::new(4, 10);
    let mut slow = q.subscribe();
    q.send(Bytes::from_static(b"123456")).unwrap();
    q.send(Bytes::from_static(b"abcdef")).unwrap();
    assert_eq!(q.retained(), (1, 6));
    assert!(slow.recv().await.is_err());
    assert_eq!(slow.recv().await.unwrap(), Bytes::from_static(b"abcdef"));
    assert!(q.send(Bytes::from(vec![0; 11])).is_err());
    let q = Channel::new(2, 100);
    let mut slow = q.subscribe();
    for _ in 0..3 {
        q.send(Bytes::from_static(b"a")).unwrap();
    }
    assert_eq!(q.retained(), (2, 2));
    assert!(slow.recv().await.is_err());
}
#[tokio::test]
async fn waiting_receiver_wakes_after_send_without_losing_notification() {
    let q = Channel::new(4, 10);
    let mut rx = q.subscribe();
    let task = tokio::spawn(async move { rx.recv().await.unwrap() });
    tokio::task::yield_now().await;
    q.send(Bytes::from_static(b"hello")).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"hello")
    );
}
#[tokio::test]
async fn original_records_and_segments_are_preserved_and_cached_before_signal() {
    let h = Hub::new();
    let info = Bytes::from(encode_info(&tracks()).unwrap());
    h.relay_info(tracks(), info.clone()).unwrap();
    let f = frame(true, 900000000);
    let wire = Bytes::from(encode_frame(&tracks()[0], &f).unwrap());
    h.relay_frame(f, wire.clone()).unwrap();
    let (boot, _) = h.m4s_subscribe();
    assert_eq!(boot, vec![info.clone(), wire.clone()]);
    let body = Bytes::from(pack(&tracks(), &[frame(true, 900000000)], 3600).unwrap());
    let signal = Bytes::from_static(b"17 2026/10/01/08/09/10-00040\n");
    let (_, mut rx) = h.signal_subscribe();
    h.relay_segment(
        Segment {
            name: "2026/10/01/08/09/10.m4f".into(),
            signal: signal.clone(),
            bytes: body.clone(),
        },
        tracks(),
        PackedGop {
            utc: 1700000000,
            dts_ms: 10000000.0,
            sequence: 17,
            duration_ms: 40.0,
            body: body.clone(),
        },
    )
    .unwrap();
    assert_eq!(rx.recv().await.unwrap(), signal);
    assert_eq!(h.segment("2026/10/01/08/09/10.m4f"), Some(body.clone()));
    h.relay_segment(
        Segment {
            name: "2026/10/01/08/09/10.m4f".into(),
            signal: signal.clone(),
            bytes: body.clone(),
        },
        tracks(),
        PackedGop {
            utc: 1700000000,
            dts_ms: 10000000.0,
            sequence: 17,
            duration_ms: 40.0,
            body: body.clone(),
        },
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), rx.recv())
            .await
            .is_err()
    );
    let (boot, _) = h.m4s_subscribe();
    let mut decoder = Decoder::default();
    let mut gops = 0;
    for b in boot {
        for e in decoder.push(&b).unwrap() {
            if let Event::Gop { gop, .. } = e {
                assert_eq!(gop.body, body);
                assert_eq!(gop.sequence, 17);
                gops += 1;
            }
        }
    }
    assert_eq!(gops, 1);
}
#[test]
fn new_codec_information_discards_old_keyframe_bootstrap() {
    let h = Hub::new();
    h.relay_info(tracks(), Bytes::from(encode_info(&tracks()).unwrap()))
        .unwrap();
    let f = frame(true, 90000);
    h.relay_frame(
        f.clone(),
        Bytes::from(encode_frame(&tracks()[0], &f).unwrap()),
    )
    .unwrap();
    let mut new = tracks();
    new[0].config = vec![1, 77, 0, 30];
    let info = Bytes::from(encode_info(&new).unwrap());
    h.relay_info(new, info.clone()).unwrap();
    assert_eq!(h.m4s_subscribe().0, vec![info]);
}

#[test]
fn signal_parser_bounds_partial_lines_and_preserves_exact_notification() {
    use flussonix::m4_ingest::Signals;
    let line = b"57252 2026/10/02/04/51/02-06000\n";
    let mut p = Signals::default();
    let mut out = Vec::new();
    for b in line {
        out.extend(p.push(&[*b]).unwrap());
    }
    assert_eq!(out[0].sequence, 57252);
    assert_eq!(out[0].duration_ms, 6000.0);
    assert_eq!(out[0].wire.as_ref(), line);
    assert_eq!(out[0].name, "2026/10/02/04/51/02.m4f");
    for bad in [
        "1 ../../x-2000\n",
        "1 2026/10/02/04/51/02-0\n",
        "1 2026/10/02/04/51/02-NaN\n",
    ] {
        assert!(Signals::default().push(bad.as_bytes()).is_err());
    }
    assert!(Signals::default().push(&vec![b'x'; 8193]).is_err());
    let many = line.repeat(500);
    assert_eq!(Signals::default().push(&many).unwrap().len(), 500);
}

#[test]
fn bootstrap_overflow_waits_for_a_new_video_keyframe() {
    let h = Hub::new();
    let info = Bytes::from(encode_info(&tracks()).unwrap());
    h.relay_info(tracks(), info.clone()).unwrap();
    for i in 0..4 {
        let mut f = frame(i == 0, 90000 + i * 3600);
        f.body = vec![0; 9 * 1024 * 1024];
        let wire = Bytes::from(encode_frame(&tracks()[0], &f).unwrap());
        h.relay_frame(f, wire).unwrap();
    }
    assert_eq!(h.m4s_subscribe().0, vec![info.clone()]);
    let f = frame(false, 110000);
    h.relay_frame(
        f.clone(),
        Bytes::from(encode_frame(&tracks()[0], &f).unwrap()),
    )
    .unwrap();
    assert_eq!(h.m4s_subscribe().0, vec![info.clone()]);
    let f = frame(true, 180000);
    let wire = Bytes::from(encode_frame(&tracks()[0], &f).unwrap());
    h.relay_frame(f, wire.clone()).unwrap();
    assert_eq!(h.m4s_subscribe().0, vec![info, wire]);
}

#[test]
fn repeated_identical_metadata_preserves_a_decodable_bootstrap() {
    let h = Hub::new();
    h.info(tracks()).unwrap();
    h.frame(frame(true, 0)).unwrap();
    h.info(tracks()).unwrap();
    h.frame(frame(false, 3600)).unwrap();
    let mut d = Decoder::default();
    let keys: Vec<_> = h
        .m4s_subscribe()
        .0
        .iter()
        .flat_map(|b| d.push(b).unwrap())
        .filter_map(|e| {
            if let Event::Frame { key, .. } = e {
                Some(key)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(keys, vec![true, false]);
}

fn overflow(h: &Hub) {
    h.info(tracks()).unwrap();
    for i in 0..4 {
        let mut f = frame(i == 0, i * 3600);
        f.body = vec![0; 9 * 1024 * 1024];
        h.frame(f).unwrap();
    }
}

#[tokio::test]
async fn overflow_late_join_waits_for_a_decodable_live_boundary() {
    let h = Hub::new();
    overflow(&h);
    let (_, mut rx) = h.m4s_subscribe();
    h.frame(frame(false, 14000)).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), rx.recv())
            .await
            .is_err()
    );
    h.frame(frame(true, 90000)).unwrap();
    let next = rx.recv().await.unwrap();
    assert!(matches!(
        &Decoder::default().push(&next).unwrap()[0],
        Event::Frame { key: true, .. }
    ));
}

#[test]
fn overflow_recovered_m4f_excludes_samples_before_the_keyframe() {
    let h = Hub::new();
    overflow(&h);
    h.frame(frame(false, 14000)).unwrap();
    h.frame(frame(true, 90000)).unwrap();
    h.frame(frame(true, 180000)).unwrap();
    let (signals, _) = h.signal_subscribe();
    let line = std::str::from_utf8(&signals[0]).unwrap();
    let stamp = line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .split('-')
        .next()
        .unwrap();
    let (_, frames) = flussonix::m4f::unpack(&h.segment(&format!("{stamp}.m4f")).unwrap()).unwrap();
    assert!(frames[0].key);
    assert_eq!(frames[0].dts, 90000);
}
#[tokio::test]
async fn early_m4f_subscriber_receives_metadata_before_first_and_changed_gops() {
    let hub = Hub::new();
    let (bootstrap, mut early) = hub.m4s_subscribe();
    assert!(bootstrap.is_empty());
    let mut tracks = vec![Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }];
    for sequence in 1..=3u64 {
        if sequence == 2 {
            tracks.push(Track {
                id: 7,
                codec: "subtitle".into(),
                config: vec![],
            });
        }
        let frame = Frame {
            track_id: 1,
            dts: sequence * 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        };
        let body = Bytes::from(pack(&tracks, &[frame], 3600).unwrap());
        let gop = PackedGop {
            utc: 1700000000,
            dts_ms: sequence as f64 * 1000.0,
            sequence: sequence as u32,
            duration_ms: 40.0,
            body: body.clone(),
        };
        let wire = flussonix::m4s::encode_gop(&gop).unwrap();
        hub.relay_segment(
            Segment {
                name: format!("2026/10/01/08/09/{sequence:02}.m4f"),
                signal: Bytes::from(format!("{sequence} 2026/10/01/08/09/{sequence:02}-40\n")),
                bytes: body,
            },
            tracks.clone(),
            gop,
        )
        .unwrap();
        let first = tokio::time::timeout(Duration::from_millis(100), early.recv())
            .await
            .unwrap()
            .unwrap();
        let info = Bytes::from(encode_info(&tracks).unwrap());
        if sequence <= 2 {
            assert_eq!(
                first, info,
                "an already-waiting subscriber must receive new metadata before its GOP"
            );
            let next = tokio::time::timeout(Duration::from_millis(100), early.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                next, wire,
                "the original GOP must remain byte-for-byte unchanged"
            );
        } else {
            assert_eq!(
                first, wire,
                "unchanged metadata must not introduce a duplicate announcement"
            );
        }
        assert_eq!(hub.m4s_subscribe().0, vec![info, wire]);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), early.recv())
                .await
                .is_err()
        );
    }
}

struct OwnedWire(std::sync::Arc<Vec<u8>>);
impl AsRef<[u8]> for OwnedWire {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
fn owned_wire(data: Vec<u8>) -> (Bytes, std::sync::Weak<Vec<u8>>) {
    let owner = std::sync::Arc::new(data);
    let weak = std::sync::Arc::downgrade(&owner);
    (Bytes::from_owner(OwnedWire(owner)), weak)
}

#[test]
fn m4s_history_without_subscribers_releases_old_frame_records() {
    let hub = Hub::new();
    hub.info(tracks()).unwrap();
    let first = frame(true, 0);
    let (wire, old) = owned_wire(encode_frame(&tracks()[0], &first).unwrap());
    hub.relay_frame(first, wire).unwrap();
    assert!(
        old.upgrade().is_some(),
        "current bootstrap must own its keyframe"
    );
    hub.frame(frame(true, 90000)).unwrap();
    assert!(
        old.upgrade().is_none(),
        "unused fan-out must release the old GOP"
    );
    let (bootstrap, _) = hub.m4s_subscribe();
    let mut decoder = Decoder::default();
    let events: Vec<_> = bootstrap
        .iter()
        .flat_map(|b| decoder.push(b).unwrap())
        .collect();
    assert!(matches!(
        events.as_slice(),
        [
            Event::Info { .. },
            Event::Frame {
                key: true,
                dts: 90000,
                ..
            }
        ]
    ));
}

#[tokio::test]
async fn m4s_history_last_subscriber_release_preserves_returned_bytes_and_late_join() {
    let hub = Hub::new();
    hub.info(tracks()).unwrap();
    let (_, first) = hub.m4s_subscribe();
    let (_, mut second) = hub.m4s_subscribe();
    let f = frame(true, 0);
    let original = encode_frame(&tracks()[0], &f).unwrap();
    let (wire, old) = owned_wire(original.clone());
    hub.relay_frame(f, wire).unwrap();
    hub.frame(frame(true, 90000)).unwrap();
    drop(first);
    assert!(
        old.upgrade().is_some(),
        "another viewer still needs this record"
    );
    let held = tokio::time::timeout(Duration::from_secs(1), second.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.as_ref(), original);
    drop(second);
    assert_eq!(
        held.as_ref(),
        original,
        "returned bytes outlive the last receiver"
    );
    drop(held);
    assert!(
        old.upgrade().is_none(),
        "last receiver must release old history"
    );
    let (bootstrap, mut late) = hub.m4s_subscribe();
    let mut decoder = Decoder::default();
    let events: Vec<_> = bootstrap
        .iter()
        .flat_map(|b| decoder.push(b).unwrap())
        .collect();
    assert!(matches!(
        events.as_slice(),
        [
            Event::Info { .. },
            Event::Frame {
                key: true,
                dts: 90000,
                ..
            }
        ]
    ));
    let next = frame(false, 93600);
    let original = encode_frame(&tracks()[0], &next).unwrap();
    hub.relay_frame(next, Bytes::from(original.clone()))
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), late.recv())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        original
    );
}

#[test]
fn m4s_history_without_subscribers_releases_packed_wire_and_keeps_m4f_segments() {
    let hub = Hub::new();
    let mut old = None;
    let mut first_segment = None;
    for sequence in 1..=2 {
        let body =
            Bytes::from(pack(&tracks(), &[frame(true, sequence as u64 * 90000)], 90000).unwrap());
        let gop = PackedGop {
            utc: 1700000000 + sequence * 2,
            dts_ms: sequence as f64 * 1000.0,
            sequence,
            duration_ms: 1000.0,
            body: body.clone(),
        };
        let name = if sequence == 1 {
            "2023/11/14/22/13/22.m4f"
        } else {
            "2023/11/14/22/13/24.m4f"
        };
        let (wire, weak) = owned_wire(flussonix::m4s::encode_gop(&gop).unwrap().to_vec());
        hub.relay_gop(gop, tracks(), wire).unwrap();
        if sequence == 1 {
            old = Some(weak);
            first_segment = Some(body.clone());
        }
        assert_eq!(
            hub.segment(name),
            if sequence == 1 {
                first_segment.clone()
            } else {
                Some(body)
            }
        );
    }
    assert!(
        old.unwrap().upgrade().is_none(),
        "unused fan-out must release the previous packed GOP record"
    );
    assert_eq!(hub.segment("2023/11/14/22/13/22.m4f"), first_segment);
    let (bootstrap, _) = hub.m4s_subscribe();
    let mut decoder = Decoder::default();
    let events: Vec<_> = bootstrap
        .iter()
        .flat_map(|b| decoder.push(b).unwrap())
        .collect();
    assert!(matches!(
        events.as_slice(),
        [
            Event::Info { .. },
            Event::Gop {
                gop: PackedGop { sequence: 2, .. },
                ..
            }
        ]
    ));
}

#[tokio::test]
async fn m4s_history_active_subscriber_still_reports_record_backlog_lag() {
    let hub = Hub::new();
    hub.info(tracks()).unwrap();
    let (_, mut slow) = hub.m4s_subscribe();
    for i in 0..257 {
        hub.frame(frame(i == 0, i * 3600)).unwrap();
    }
    assert!(matches!(
        slow.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(1))
    ));
    assert_eq!(
        slow.recv().await.unwrap().as_ref(),
        encode_frame(&tracks()[0], &frame(false, 3600)).unwrap()
    );
}
