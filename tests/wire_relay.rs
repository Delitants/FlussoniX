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
    let info = Bytes::from(encode_info(&tracks()));
    h.relay_info(tracks(), info.clone());
    let f = frame(true, 900000000);
    let wire = Bytes::from(encode_frame(&tracks()[0], &f));
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
    h.relay_info(tracks(), Bytes::from(encode_info(&tracks())));
    let f = frame(true, 90000);
    h.relay_frame(f.clone(), Bytes::from(encode_frame(&tracks()[0], &f)))
        .unwrap();
    let mut new = tracks();
    new[0].config = vec![1, 77, 0, 30];
    let info = Bytes::from(encode_info(&new));
    h.relay_info(new, info.clone());
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
    let info = Bytes::from(encode_info(&tracks()));
    h.relay_info(tracks(), info.clone());
    for i in 0..4 {
        let mut f = frame(i == 0, 90000 + i * 3600);
        f.body = vec![0; 9 * 1024 * 1024];
        let wire = Bytes::from(encode_frame(&tracks()[0], &f));
        h.relay_frame(f, wire).unwrap();
    }
    assert_eq!(h.m4s_subscribe().0, vec![info.clone()]);
    let f = frame(false, 110000);
    h.relay_frame(f.clone(), Bytes::from(encode_frame(&tracks()[0], &f)))
        .unwrap();
    assert_eq!(h.m4s_subscribe().0, vec![info.clone()]);
    let f = frame(true, 180000);
    let wire = Bytes::from(encode_frame(&tracks()[0], &f));
    h.relay_frame(f, wire.clone()).unwrap();
    assert_eq!(h.m4s_subscribe().0, vec![info, wire]);
}
