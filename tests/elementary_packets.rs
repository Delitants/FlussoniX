use flussonix::direct_rtp::{
    elementary::{
        packet,
        sdp::{Codec, Track},
    },
    packet::packet as rtp,
};
fn track(codec: Codec) -> Track {
    Track {
        port: 20000,
        payload: 96,
        clock: 90000,
        video: matches!(codec, Codec::H264 | Codec::H265),
        codec,
        encoding: String::new(),
        fmtp: String::new(),
    }
}
fn body(payload: &[u8]) -> Vec<u8> {
    let mut b = rtp(1, 0, 123, payload);
    b[1] = 96;
    b
}
#[test]
fn elementary_payloads_are_bounded_and_codec_checked_before_pin() {
    for (codec, valid, invalid) in [
        (Codec::H264, vec![0x65, 1], vec![0x80, 1]),
        (Codec::H264, vec![0x7c, 0x85, 1], vec![0x7c, 0xc5, 1]),
        (Codec::H265, vec![0x26, 1, 1], vec![0x26, 0, 1]),
        (Codec::H265, vec![0x62, 1, 0x93, 1], vec![0x62, 1, 0xd3, 1]),
        (
            Codec::Aac,
            vec![0, 16, 0, 16, 1, 2],
            vec![0, 16, 0, 24, 1, 2],
        ),
        (
            Codec::Mpa,
            vec![0, 0, 0, 0, 255, 253, 164, 0, 1],
            vec![0, 0, 0, 0, 255, 255, 255, 255],
        ),
    ] {
        let t = track(codec);
        let good = body(&valid);
        assert!(packet::parse(&good, &t).is_ok(), "{valid:?}");
        assert!(packet::parse(&body(&invalid), &t).is_err(), "{invalid:?}");
        let mut wrong = good.clone();
        wrong[1] = 95;
        assert!(packet::parse(&wrong, &t).is_err());
        for end in 0..12 {
            assert!(packet::parse(&good[..end], &t).is_err());
        }
        let mut oversized = good.clone();
        oversized.resize(1601, 0);
        assert!(packet::parse(&oversized, &t).is_err());
    }
    let t = track(Codec::H264);
    for p in [vec![24, 0, 4, 0x65, 1], vec![28], vec![29, 0, 0], vec![0]] {
        assert!(packet::parse(&body(&p), &t).is_err());
    }
}
#[test]
fn multitrack_packetizer_routes_equal_mpeg_payload_types_by_track_and_ssrc() {
    use flussonix::{m4f::Frame, m4s::Track, rtp::Hub};
    let h = Hub::new_multitrack();
    let tracks: Vec<_> = (0..8)
        .map(|n| Track {
            id: n + 1,
            codec: if n % 2 == 0 { "m2a" } else { "mp3" }.into(),
            config: vec![],
        })
        .collect();
    h.configure(&tracks);
    let description = h.description().unwrap().unwrap();
    assert_eq!(description.tracks.len(), 8);
    let mut ids = std::collections::HashSet::new();
    for t in &description.tracks {
        assert_eq!(t.payload, 14);
        assert!(ids.insert(t.ssrc));
    }
    for t in &tracks {
        h.frame(&Frame {
            track_id: t.id,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: if t.codec == "m2a" {
                include_bytes!("fixtures/codecs/mp2.bin").to_vec()
            } else {
                include_bytes!("fixtures/codecs/mp3.bin").to_vec()
            },
        });
    }
    let (_, packets, _) = h.subscribe().unwrap();
    assert_eq!(packets.len(), 8);
    for p in packets {
        let id = u32::from_be_bytes(p[..4].try_into().unwrap());
        let ssrc = u32::from_be_bytes(p[12..16].try_into().unwrap());
        assert_eq!(
            description.tracks.iter().find(|t| t.id == id).unwrap().ssrc,
            ssrc
        );
    }
    let mut too_many = tracks;
    too_many.push(Track {
        id: 9,
        codec: "mp3".into(),
        config: vec![],
    });
    h.configure(&too_many);
    assert!(h.description().unwrap().is_err());
}
