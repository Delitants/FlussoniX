use flussonix::{codec::Codec, hevc::Configuration, mpeg_audio::inspect};

#[test]
fn explicit_codec_identity_matches_native_kind_and_tag() {
    for (name, video, tag) in [
        ("h264", true, *b"h264"),
        ("hevc", true, *b"hevc"),
        ("aac", false, *b" aac"),
        ("m2a", false, *b" m2a"),
        ("mp3", false, *b" mp3"),
    ] {
        let codec = Codec::parse(name).unwrap();
        assert_eq!(codec.is_video(), video);
        assert_eq!(codec.tag(), tag);
    }
    for unsupported in ["h265", "mp2", "HEVC", "opus", ""] {
        assert!(Codec::parse(unsupported).is_err());
    }
}

#[test]
fn owned_hevc_configuration_and_access_units_preserve_picture_identity() {
    let raw = include_bytes!("fixtures/codecs/hevc.hvcc");
    let cfg = Configuration::parse(raw).unwrap();
    assert_eq!(cfg.nal_length_size, 4);
    assert!(cfg.annex_b().starts_with(&[0, 0, 0, 1, 0x40]));
    let key = cfg
        .access_unit(include_bytes!("fixtures/codecs/hevc-00.bin"))
        .unwrap();
    let dependent = cfg
        .access_unit(include_bytes!("fixtures/codecs/hevc-01.bin"))
        .unwrap();
    assert!(key.has_irap);
    assert!(!dependent.has_irap);
    assert!(key.annex_b.starts_with(&[0, 0, 0, 1]));
    // A parameter-set-only access unit is not a random-access picture.
    assert!(!cfg.access_unit(&[0, 0, 0, 2, 0x40, 1]).unwrap().has_irap);
    for width in [1, 2, 4] {
        let mut raw = raw.to_vec();
        raw[21] = (raw[21] & !3) | (width - 1);
        let cfg = Configuration::parse(&raw).unwrap();
        let mut sample = vec![0; width as usize];
        *sample.last_mut().unwrap() = 3;
        sample.extend_from_slice(&[0x26, 1, 128]);
        assert!(cfg.access_unit(&sample).unwrap().has_irap);
    }
}

#[test]
fn hevc_rejects_bad_configuration_and_access_unit_lengths() {
    let raw = include_bytes!("fixtures/codecs/hevc.hvcc");
    for length in [0, 1, 22, raw.len() - 1] {
        assert!(Configuration::parse(&raw[..length]).is_err());
    }
    let mut invalid = raw.to_vec();
    invalid[0] = 2;
    assert!(Configuration::parse(&invalid).is_err());
    let mut invalid = raw.to_vec();
    invalid[21] = (invalid[21] & !3) | 2;
    assert!(Configuration::parse(&invalid).is_err());
    let mut invalid = raw.to_vec();
    invalid.push(0);
    assert!(Configuration::parse(&invalid).is_err());
    let mut invalid = raw.to_vec();
    invalid[22] = 0;
    assert!(Configuration::parse(&invalid[..23]).is_err());
    assert!(Configuration::parse(&vec![0; 1024 * 1024 + 1]).is_err());
    let cfg = Configuration::parse(raw).unwrap();
    for sample in [
        vec![],
        vec![0, 0, 0, 0],
        vec![0, 0, 0, 1, 0x26],
        vec![0, 0, 0, 3, 0x26, 1],
        vec![0, 0, 0, 2, 0xa6, 1],
        vec![0, 0, 0, 2, 0x26, 0],
    ] {
        assert!(cfg.access_unit(&sample).is_err());
    }
    assert!(cfg.access_unit(&vec![0; 16 * 1024 * 1024 + 1]).is_err());
}

#[test]
fn hevc_access_unit_rechecks_public_length_width() {
    let mut cfg = Configuration::parse(include_bytes!("fixtures/codecs/hevc.hvcc")).unwrap();
    cfg.nal_length_size = 3;
    assert!(cfg.access_unit(&[0, 0, 2, 0x26, 1]).is_err());
    cfg.nal_length_size = usize::MAX;
    assert!(cfg.access_unit(&[0, 0, 0, 2, 0x26, 1]).is_err());
}

fn mpeg_frame(version: u8, layer: u8, rate: u8, bitrate: u8, padding: bool, mono: bool) -> Vec<u8> {
    let header = [
        0xff,
        0xe1 | version << 3 | layer << 1,
        bitrate << 4 | rate << 2 | u8::from(padding) << 1,
        if mono { 0xc0 } else { 0 },
    ];
    let hz = [44100, 48000, 32000][rate as usize]
        / match version {
            3 => 1,
            2 => 2,
            _ => 4,
        };
    let kbps = if version == 3 {
        if layer == 2 {
            [
                0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
            ][bitrate as usize]
        } else {
            [
                0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
            ][bitrate as usize]
        }
    } else {
        [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160][bitrate as usize]
    };
    let coefficient = if layer == 1 && version != 3 {
        72000
    } else {
        144000
    };
    let n = coefficient * kbps / hz + u32::from(padding);
    let mut frame = vec![0; n as usize];
    frame[..4].copy_from_slice(&header);
    frame
}

#[test]
fn mpeg_audio_uses_observed_layer_rate_and_version_for_duration() {
    let mp2 = inspect(Codec::M2a, include_bytes!("fixtures/codecs/mp2.bin")).unwrap();
    assert_eq!(
        (
            mp2.sample_rate,
            mp2.samples,
            mp2.frame_bytes,
            mp2.duration_90k()
        ),
        (48000, 1152, 576, 2160)
    );
    let mp3 = inspect(Codec::Mp3, include_bytes!("fixtures/codecs/mp3.bin")).unwrap();
    assert_eq!(
        (
            mp3.sample_rate,
            mp3.samples,
            mp3.frame_bytes,
            mp3.duration_90k()
        ),
        (22050, 576, 208, 2351)
    );
    for version in [0, 2, 3] {
        for layer in [1, 2] {
            for rate in 0..3 {
                for padding in [false, true] {
                    let codec = if layer == 2 { Codec::M2a } else { Codec::Mp3 };
                    let frame = mpeg_frame(version, layer, rate, 8, padding, true);
                    let h = inspect(codec, &frame).unwrap();
                    assert_eq!(h.channels, 1);
                    assert_eq!(
                        h.samples,
                        if layer == 1 && version != 3 {
                            576
                        } else {
                            1152
                        }
                    );
                    assert_eq!(h.frame_bytes, frame.len());
                    assert!(h.duration_90k() > 0);
                }
            }
        }
    }
}

#[test]
fn mpeg_audio_rejects_reserved_free_format_mislabeled_and_partial_frames() {
    let good = include_bytes!("fixtures/codecs/mp2.bin");
    assert!(inspect(Codec::Mp3, good).is_err());
    assert!(inspect(Codec::Aac, good).is_err());
    for length in [0, 3, good.len() - 1] {
        assert!(inspect(Codec::M2a, &good[..length]).is_err());
    }
    assert!(inspect(Codec::M2a, &[good.as_slice(), good.as_slice()].concat()).is_err());
    for (at, value) in [
        (0, 0),
        (1, 0xe9),
        (1, 0xff),
        (2, 0),
        (2, 0xf0),
        (2, 0x8c),
        (3, 2),
    ] {
        let mut invalid = good.to_vec();
        invalid[at] = value;
        assert!(
            inspect(Codec::M2a, &invalid).is_err(),
            "accepted mutation {at}={value}"
        );
    }
}
