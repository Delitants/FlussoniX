use flussonix::{m4f::Frame, m4s::Track, rtp::Hub};
fn video() -> Track {
    Track {
        id: 7,
        codec: "h264".into(),
        config: vec![
            1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
        ],
    }
}
fn audio() -> Track {
    Track {
        id: 9,
        codec: "aac".into(),
        config: vec![0x12, 0x10],
    }
}
fn frame(id: u32, body: Vec<u8>, key: bool, dts: u64, offset: i64) -> Frame {
    Frame {
        track_id: id,
        body,
        key,
        dts,
        pts_offset: offset,
    }
}
fn avcc(nals: &[Vec<u8>]) -> Vec<u8> {
    nals.iter()
        .flat_map(|n| {
            let mut b = (n.len() as u32).to_be_bytes().to_vec();
            b.extend(n);
            b
        })
        .collect()
}
#[test]
fn h264_fua_reconstructs_access_unit_with_pts_and_final_marker() {
    let h = Hub::new();
    h.configure(&[video()]);
    let nals = vec![
        vec![0x67, 100, 0, 31],
        [vec![0x65], vec![42; 3500]].concat(),
    ];
    h.frame(&frame(7, avcc(&nals), true, 20, -40));
    let (d, packets, _) = h.subscribe().unwrap();
    assert!(d.sdp().contains("sprop-parameter-sets=Z2QAHw==,aAA="));
    assert!(packets.len() > 3);
    let mut restored = vec![];
    let mut fragmented = vec![];
    let mut previous = None;
    for (i, b) in packets.iter().enumerate() {
        let p = &b[4..];
        assert!(p.len() <= 1200);
        assert_eq!(
            u32::from_be_bytes(p[4..8].try_into().unwrap()),
            (-20i64) as u32
        );
        assert_eq!(p[1] & 128 != 0, i == packets.len() - 1);
        let seq = u16::from_be_bytes(p[2..4].try_into().unwrap());
        if let Some(prev) = previous {
            assert_eq!(seq, u16::wrapping_add(prev, 1));
        }
        previous = Some(seq);
        if p[12] & 31 == 28 {
            if p[13] & 128 != 0 {
                fragmented = vec![(p[12] & 0xe0) | (p[13] & 31)];
            }
            fragmented.extend_from_slice(&p[14..]);
            if p[13] & 64 != 0 {
                restored.push(fragmented.clone());
            }
        } else {
            restored.push(p[12..].to_vec());
        }
    }
    assert_eq!(restored, nals);
}
#[test]
fn aac_fragments_carry_full_au_size_and_sample_clock() {
    let h = Hub::new();
    h.configure(&[audio()]);
    let body = vec![11; 3000];
    h.frame(&frame(9, body.clone(), true, 90000, 0));
    let (d, packets, _) = h.subscribe().unwrap();
    assert!(d.sdp().contains("MPEG4-GENERIC/44100/2"));
    assert!(d.sdp().contains("config=1210"));
    let mut decoded = vec![];
    for (i, b) in packets.iter().enumerate() {
        let p = &b[4..];
        assert_eq!(&p[12..14], &16u16.to_be_bytes());
        assert_eq!(u16::from_be_bytes(p[14..16].try_into().unwrap()) >> 3, 3000);
        assert_eq!(u32::from_be_bytes(p[4..8].try_into().unwrap()), 44100);
        assert_eq!(p[1] & 128 != 0, i == packets.len() - 1);
        decoded.extend_from_slice(&p[16..]);
    }
    assert_eq!(decoded, body);
}
#[test]
fn malformed_metadata_and_access_units_fail_only_rtsp_profile() {
    let h = Hub::new();
    h.configure(&[Track {
        config: vec![1, 100, 0, 31],
        ..video()
    }]);
    assert!(h.subscribe().is_err());
    h.configure(&[video()]);
    h.frame(&frame(7, vec![0, 0, 255, 255, 0x65], true, 0, 0));
    assert!(h.subscribe().is_err());
    let h = Hub::new();
    h.configure(&[Track {
        config: vec![0x2a, 0x10],
        ..audio()
    }]);
    assert!(h.subscribe().is_err());
}
#[test]
fn metadata_and_late_join_do_not_replay_before_keyframe() {
    let h = Hub::new();
    h.configure(&[video(), audio()]);
    h.frame(&frame(7, avcc(&[vec![0x41, 1]]), false, 0, 0));
    assert!(h.subscribe().unwrap().1.is_empty());
    h.frame(&frame(7, avcc(&[vec![0x65, 2]]), true, 90000, 0));
    h.frame(&frame(9, vec![3; 200], true, 91000, 0));
    let before = h.subscribe().unwrap().1;
    assert_eq!(before.len(), 2);
    h.configure(&[video(), audio()]);
    assert_eq!(h.subscribe().unwrap().1, before);
    h.frame(&frame(7, avcc(&[vec![0x65, 4]]), true, 180000, 0));
    let after = h.subscribe().unwrap().1;
    assert_eq!(after.len(), 1);
    assert_eq!(&after[0][16..], &[0x65, 4]);
    let mut changed = video();
    changed.config[3] = 32;
    h.configure(&[changed, audio()]);
    assert!(h.subscribe().unwrap().1.is_empty());
}
#[test]
fn audio_only_bootstrap_rolls_and_large_clock_wraps_without_overflow() {
    let h = Hub::new();
    h.configure(&[audio()]);
    for i in 0..8 {
        h.frame(&frame(
            9,
            vec![1; 200],
            true,
            u64::MAX - 900000 + i * 45000,
            0,
        ));
    }
    assert!(h.subscribe().unwrap().1.len() <= 5);
}
#[tokio::test]
async fn shared_queue_is_bounded_and_reports_lag() {
    let h = Hub::new();
    h.configure(&[audio()]);
    let (_, _, mut rx) = h.subscribe().unwrap();
    for i in 0..4100 {
        h.frame(&frame(9, vec![1; 200], true, i * 1024, 0));
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_ok_and(|r| r.is_err())
    );
}
