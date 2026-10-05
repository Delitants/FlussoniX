use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::{m4f::Frame, m4s::Track, rtp::Hub};
fn track(width: usize) -> Track {
    let mut config = include_bytes!("fixtures/codecs/hevc.hvcc").to_vec();
    config[21] = (config[21] & !3) | (width as u8 - 1);
    Track {
        id: 7,
        codec: "hevc".into(),
        config,
    }
}
fn body(nals: &[Vec<u8>], width: usize) -> Vec<u8> {
    nals.iter()
        .flat_map(|n| {
            let mut b = (n.len() as u32).to_be_bytes()[4 - width..].to_vec();
            b.extend(n);
            b
        })
        .collect()
}
fn sample(nals: &[Vec<u8>], width: usize) -> Frame {
    Frame {
        track_id: 7,
        body: body(nals, width),
        key: true,
        dts: 90000,
        pts_offset: -3600,
    }
}
fn reconstruct(packets: &[bytes::Bytes]) -> Vec<Vec<u8>> {
    let mut nals = vec![];
    let mut nal = vec![];
    let mut previous = None;
    for (i, b) in packets.iter().enumerate() {
        let p = &b[4..];
        assert!(p.len() <= 1200);
        assert_eq!(p[1] & 128 != 0, i == packets.len() - 1);
        assert_eq!(u32::from_be_bytes(p[4..8].try_into().unwrap()), 86400);
        let seq = u16::from_be_bytes(p[2..4].try_into().unwrap());
        if let Some(prev) = previous {
            assert_eq!(seq, u16::wrapping_add(prev, 1));
        }
        previous = Some(seq);
        if p[12] >> 1 & 63 == 49 {
            assert_eq!(p[12] & 0x81, 0);
            assert_eq!(p[13], 5);
            if p[14] & 128 != 0 {
                assert!(nal.is_empty());
                nal = vec![(p[12] & 0x81) | ((p[14] & 63) << 1), p[13]];
            }
            assert!(!nal.is_empty());
            nal.extend_from_slice(&p[15..]);
            if p[14] & 64 != 0 {
                nals.push(std::mem::take(&mut nal));
            }
        } else {
            assert!(nal.is_empty());
            nals.push(p[12..].to_vec());
        }
    }
    assert!(nal.is_empty());
    nals
}
#[test]
fn hevc_sdp_and_fragmentation_preserve_nals_and_presentation_clock() {
    for width in [1, 2, 4] {
        let h = Hub::new();
        let t = track(width);
        h.configure(&[t]);
        let size = if width == 1 { 120 } else { 3500 };
        let nals = vec![vec![78, 5, 1, 2], [vec![38, 5], vec![42; size]].concat()];
        h.frame(&sample(&nals, width));
        let (d, p, _) = h
            .subscribe()
            .expect("HEVC must be described and packetized");
        assert_eq!(d.tracks[0].encoding, "H265/90000");
        assert_eq!(d.tracks[0].clock, 90000);
        let sdp = d.sdp();
        for (key, kind) in [("sprop-vps=", 32), ("sprop-sps=", 33), ("sprop-pps=", 34)] {
            let value = sdp
                .split(key)
                .nth(1)
                .unwrap()
                .split([';', '\r'])
                .next()
                .unwrap();
            for set in value.split(',') {
                let bytes = STANDARD.decode(set).unwrap();
                assert_eq!(bytes[0] >> 1 & 63, kind);
            }
        }
        assert!(sdp.contains("sprop-max-don-diff=0"));
        assert_eq!(reconstruct(&p), nals);
    }
}
#[test]
fn hevc_can_share_a_description_with_aac_but_not_another_video() {
    let h = Hub::new();
    h.configure(&[
        track(4),
        Track {
            id: 9,
            codec: "aac".into(),
            config: vec![0x12, 0x10],
        },
    ]);
    let d = h
        .description()
        .unwrap()
        .expect("HEVC/AAC must be supported");
    assert_eq!(d.tracks.len(), 2);
    assert!(d.sdp().contains("MPEG4-GENERIC/44100/2"));
    let mut duplicate = track(4);
    duplicate.id = 8;
    h.configure(&[track(4), duplicate]);
    assert!(h.description().unwrap().is_err());
}
#[test]
fn malformed_hevc_access_units_reject_the_complete_unit() {
    for nal in [
        vec![],
        vec![38],
        vec![0x80 | 38, 1],
        vec![38, 0],
        vec![96, 1],
        vec![38, 9],
    ] {
        let h = Hub::new();
        h.configure(&[track(4)]);
        assert!(h.description().unwrap().is_ok());
        h.frame(&sample(&[vec![78, 1, 3], nal], 4));
        assert!(h.subscribe().is_err());
    }
    let h = Hub::new();
    h.configure(&[track(4)]);
    let mut f = sample(&[vec![38, 1, 4]], 4);
    f.body.pop();
    h.frame(&f);
    assert!(h.subscribe().is_err());
}
#[test]
fn malformed_hevc_configuration_is_rejected() {
    let h = Hub::new();
    let mut t = track(4);
    t.config.truncate(24);
    h.configure(&[t]);
    assert!(h.description().unwrap().is_err());
    let mut t = track(4);
    t.config[21] = (t.config[21] & !3) | 2;
    h.configure(&[t]);
    assert!(h.description().unwrap().is_err());
}
