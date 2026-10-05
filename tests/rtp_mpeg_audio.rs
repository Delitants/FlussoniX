use flussonix::{codec::Codec, m4f::Frame, m4s::Track, mpeg_audio, rtp::Hub};
fn track(codec: &str) -> Track {
    Track {
        id: 88,
        codec: codec.into(),
        config: vec![],
    }
}
fn fixture(codec: &str) -> Vec<u8> {
    if codec == "m2a" {
        include_bytes!("fixtures/codecs/mp2.bin").to_vec()
    } else {
        include_bytes!("fixtures/codecs/mp3.bin").to_vec()
    }
}
fn frame(body: Vec<u8>, dts: u64) -> Frame {
    Frame {
        track_id: 88,
        body,
        dts,
        pts_offset: 0,
        key: true,
    }
}
#[test]
fn mpeg_audio_descriptions_use_the_90k_clock_without_aac_configuration() {
    for codec in ["m2a", "mp3"] {
        let h = Hub::new();
        h.configure(&[track(codec)]);
        let d = h
            .description()
            .unwrap()
            .expect("MPEG audio must be described");
        assert_eq!(d.tracks[0].encoding, "MPA/90000");
        assert_eq!(d.tracks[0].clock, 90000);
        assert_eq!(d.tracks[0].payload, 14);
        assert!(d.sdp().contains("a=rtpmap:14 MPA/90000\r\n"));
        assert!(!d.sdp().contains("a=fmtp:"));
        assert!(!d.sdp().contains("MPEG4-GENERIC"));
    }
}
#[test]
fn mpeg_audio_payloads_preserve_frames_and_mark_only_a_talkspurt_start() {
    for codec in ["m2a", "mp3"] {
        let h = Hub::new();
        h.configure(&[track(codec)]);
        let body = fixture(codec);
        let kind = Codec::parse(codec).unwrap();
        let duration = mpeg_audio::inspect(kind, &body).unwrap().duration_90k();
        h.frame(&frame(body.clone(), 90000));
        h.frame(&frame(body.clone(), 90000 + u64::from(duration)));
        let (_, packets, _) = h.subscribe().expect("MPEG audio must be packetized");
        assert_eq!(packets.len(), 2);
        for (i, b) in packets.iter().enumerate() {
            let p = &b[4..];
            assert_eq!(p[1] & 127, 14);
            assert_eq!(p[1] & 128 != 0, i == 0);
            assert_eq!(&p[12..16], &[0, 0, 0, 0]);
            assert_eq!(&p[16..], &body);
            assert_eq!(
                u32::from_be_bytes(p[4..8].try_into().unwrap()),
                90000 + i as u32 * duration
            );
        }
    }
}
#[test]
fn mpeg_fragment_offsets_reconstruct_large_layer_two_and_three_frames() {
    for (codec, header, size) in [
        ("m2a", [255, 253, 232, 0], 1728),
        ("mp3", [255, 251, 232, 0], 1440),
    ] {
        let mut body = vec![42; size];
        body[..4].copy_from_slice(&header);
        let h = Hub::new();
        h.configure(&[track(codec)]);
        h.frame(&frame(body.clone(), 90000));
        let (_, packets, _) = h.subscribe().expect("large MPEG frames must be fragmented");
        assert_eq!(packets.len(), 2);
        let mut restored = vec![];
        let mut previous = None;
        for (i, b) in packets.iter().enumerate() {
            let p = &b[4..];
            assert!(p.len() <= 1200);
            assert_eq!(&p[12..14], &[0, 0]);
            assert_eq!(
                u16::from_be_bytes(p[14..16].try_into().unwrap()) as usize,
                restored.len()
            );
            assert_eq!(p[1] & 128 != 0, i == 0);
            let sequence = u16::from_be_bytes(p[2..4].try_into().unwrap());
            if let Some(prev) = previous {
                assert_eq!(sequence, u16::wrapping_add(prev, 1));
            }
            previous = Some(sequence);
            restored.extend_from_slice(&p[16..]);
        }
        assert_eq!(restored, body);
    }
}
#[test]
fn malformed_mpeg_frames_invalidate_only_the_rtp_profile() {
    for codec in ["m2a", "mp3"] {
        let good = fixture(codec);
        let mut short = good.clone();
        short.pop();
        let mut reserved = good.clone();
        reserved[2] |= 0xf0;
        let bad = [
            short,
            reserved,
            [good.clone(), good.clone()].concat(),
            fixture(if codec == "m2a" { "mp3" } else { "m2a" }),
        ];
        for body in bad {
            let h = Hub::new();
            h.configure(&[track(codec)]);
            assert!(h.description().unwrap().is_ok());
            h.frame(&frame(body, 90000));
            assert!(h.subscribe().is_err());
        }
    }
}
#[test]
fn mpeg_clock_wrap_and_silence_gap_do_not_use_an_audio_sample_rate_clock() {
    let h = Hub::new();
    h.configure(&[track("mp3")]);
    let body = fixture("mp3");
    let duration = u64::from(
        mpeg_audio::inspect(Codec::Mp3, &body)
            .unwrap()
            .duration_90k(),
    );
    let start = u32::MAX as u64 - 100;
    for dts in [start, start + duration, start + duration * 2 + 90000] {
        h.frame(&frame(body.clone(), dts));
    }
    let (_, p, _) = h.subscribe().unwrap();
    assert_eq!(p.len(), 3);
    for (i, b) in p.iter().enumerate() {
        let expected = [start, start + duration, start + duration * 2 + 90000][i] as u32;
        assert_eq!(u32::from_be_bytes(b[8..12].try_into().unwrap()), expected);
        assert_eq!(b[5] & 128 != 0, i != 1);
    }
}
#[test]
fn mpeg_audio_can_pair_with_video_but_multiple_audio_tracks_remain_explicitly_rejected() {
    let video = Track {
        id: 7,
        codec: "hevc".into(),
        config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
    };
    let h = Hub::new();
    h.configure(&[track("m2a"), video]);
    assert_eq!(
        h.description()
            .unwrap()
            .expect("HEVC and MPEG audio must pair")
            .tracks
            .len(),
        2
    );
    let mut second = track("mp3");
    second.id = 89;
    h.configure(&[track("m2a"), second]);
    assert!(h.description().unwrap().is_err());
}

#[test]
fn mpeg_two_point_five_remains_outside_the_rtp_profile() {
    let mut body = vec![0; 417];
    body[..4].copy_from_slice(&[255, 227, 128, 192]);
    assert!(mpeg_audio::inspect(Codec::Mp3, &body).is_ok());
    let h = Hub::new();
    h.configure(&[track("mp3")]);
    h.frame(&frame(body, 90000));
    assert!(h.subscribe().is_err());
}
#[test]
fn opaque_mpeg_configuration_is_bounded_and_never_becomes_an_aac_fmtp() {
    let h = Hub::new();
    let mut t = track("m2a");
    t.config = vec![42; 65536];
    h.configure(&[t.clone()]);
    assert!(!h.description().unwrap().unwrap().sdp().contains("fmtp"));
    t.config.push(42);
    h.configure(&[t]);
    assert!(h.description().unwrap().is_err());
}
