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

fn native_tracks() -> Vec<flussonix::m4s::Track> {
    use flussonix::m4s::Track;
    vec![
        Track {
            id: 7,
            codec: "hevc".into(),
            config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 12,
            codec: "m2a".into(),
            config: vec![],
        },
        Track {
            id: 13,
            codec: "mp3".into(),
            config: vec![],
        },
    ]
}
fn native_frames() -> Vec<flussonix::m4f::Frame> {
    use flussonix::m4f::Frame;
    vec![
        Frame {
            track_id: 7,
            dts: 90000,
            pts_offset: -3600,
            key: true,
            body: include_bytes!("fixtures/codecs/hevc-00.bin").to_vec(),
        },
        Frame {
            track_id: 12,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        },
        Frame {
            track_id: 13,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        },
    ]
}

#[test]
fn native_new_codec_metadata_frames_and_packed_gop_preserve_identity() {
    use flussonix::{
        m4f,
        m4s::{Decoder, Event, PackedGop, encode_gop},
        wire,
    };
    let tracks = native_tracks();
    let frames = native_frames();
    let info = wire::encode_info(&tracks).unwrap();
    let mut decoder = Decoder::default();
    let mut events = Vec::new();
    for byte in &info {
        events.extend(decoder.push(&[*byte]).unwrap());
    }
    match &events[0] {
        Event::Info {
            tracks: decoded,
            wire,
        } => {
            assert_eq!(decoded, &tracks);
            assert_eq!(wire.as_ref(), info);
        }
        _ => panic!("metadata"),
    }
    for (track, frame) in tracks.iter().zip(&frames) {
        let wire = wire::encode_frame(track, frame).unwrap();
        assert!(wire.windows(4).any(|w| w == track.kind().unwrap().tag()));
        match &decoder.push(&wire).unwrap()[0] {
            Event::Frame {
                track_id,
                dts,
                pts_offset,
                body,
                ..
            } => {
                assert_eq!(*track_id, frame.track_id);
                assert_eq!(*dts, frame.dts);
                assert_eq!(*pts_offset, frame.pts_offset);
                assert_eq!(body, &frame.body);
            }
            _ => panic!("frame"),
        }
    }
    let segment = m4f::pack(&tracks, &frames, 3600).unwrap();
    let (decoded, samples) = m4f::unpack(&segment).unwrap();
    assert_eq!(decoded, tracks);
    for (expected, actual) in frames.iter().zip(samples) {
        assert_eq!(
            (actual.track_id, actual.dts, actual.pts_offset, actual.body),
            (
                expected.track_id,
                expected.dts,
                expected.pts_offset,
                expected.body.clone()
            )
        );
    }
    let packet = encode_gop(&PackedGop {
        utc: 1700000000,
        dts_ms: 1000.0,
        sequence: 10,
        duration_ms: 40.0,
        body: segment.clone().into(),
    })
    .unwrap();
    match &decoder.push(&packet).unwrap()[0] {
        Event::Gop {
            tracks: decoded,
            gop,
            frames: samples,
            ..
        } => {
            assert_eq!(decoded, &tracks);
            assert_eq!(gop.body.as_ref(), segment);
            assert_eq!(samples.len(), 3);
        }
        _ => panic!("GOP"),
    }
}

#[test]
fn native_mpeg_tail_duration_uses_actual_frame_and_44100_timing() {
    use flussonix::{
        m4f,
        m4s::{Track, boxes},
    };
    for (codec, body, expected) in [
        (
            "m2a",
            include_bytes!("fixtures/codecs/mp2.bin").as_slice(),
            2160,
        ),
        (
            "mp3",
            include_bytes!("fixtures/codecs/mp3.bin").as_slice(),
            2351,
        ),
    ] {
        let frame = flussonix::m4f::Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: body.to_vec(),
        };
        let segment = m4f::pack(
            &[Track {
                id: 1,
                codec: codec.into(),
                config: vec![],
            }],
            &[frame],
            expected,
        )
        .unwrap();
        let root = boxes(&segment).unwrap();
        let moov = boxes(root[0].1).unwrap();
        let track = boxes(moov.iter().find(|(k, _)| *k == b"trak").unwrap().1).unwrap();
        let stts = track.iter().find(|(k, _)| *k == b"stts").unwrap().1;
        assert_eq!(
            u32::from_be_bytes(stts[12..16].try_into().unwrap()) as u64,
            expected
        );
        let (_, frames) = m4f::unpack(&segment).unwrap();
        assert!(frames[0].key);
    }
}

#[test]
fn native_metadata_bounds_ids_kinds_and_worker_bridge_fail_closed() {
    use flussonix::m4s::{Decoder, Track, atom, validate_tracks};
    let audio = Track {
        id: 1,
        codec: "m2a".into(),
        config: vec![],
    };
    let mut many = vec![audio.clone(); 16];
    for (i, t) in many.iter_mut().enumerate() {
        t.id = i as u32;
    }
    assert!(validate_tracks(&many).is_ok());
    many.push(Track {
        id: 20,
        ..audio.clone()
    });
    assert!(validate_tracks(&many).is_err());
    assert!(validate_tracks(&[audio.clone(), audio]).is_err());
    let mut video = native_tracks()[0].clone();
    video.id = 8;
    assert!(validate_tracks(&[native_tracks()[0].clone(), video]).is_err());
    let mut header = vec![0; 4];
    header.extend_from_slice(&1u32.to_be_bytes());
    header.extend_from_slice(b"sounhevc\0");
    let record = atom(
        b"MDin",
        &atom(
            b"trak",
            &[atom(b"hdlr", &header), atom(b"cnfg", &[0; 4])].concat(),
        ),
    );
    assert!(
        Decoder::default()
            .push(&[(record.len() as u32).to_be_bytes().as_slice(), &record].concat())
            .is_err()
    );
    for track in native_tracks() {
        assert!(flussonix::m4s::flv_config(&track).is_err());
        assert!(flussonix::m4s::flv_frame(&track, 0, 0, true, &[1, 2, 3], 0).is_err());
    }
    assert!(flussonix::m4_ingest::validate_bridge(&native_tracks()).is_err());
    let baseline = Track {
        id: 1,
        codec: "h264".into(),
        config: vec![1, 100, 0, 40],
    };
    assert!(flussonix::m4_ingest::validate_bridge(&[baseline]).is_ok());
    let audio = Track {
        id: 2,
        codec: "aac".into(),
        config: vec![0x11, 0x90],
    };
    assert!(
        flussonix::m4_ingest::validate_bridge(&[audio.clone(), Track { id: 3, ..audio }]).is_err()
    );
}

#[test]
fn hevc_bootstrap_requires_a_picture_boundary_and_metadata_changes_reset_it() {
    use flussonix::{m4f::Frame, wire::Hub};
    let hub = Hub::new();
    let tracks = native_tracks();
    hub.info(tracks.clone()).unwrap();
    let mut f = native_frames()[0].clone();
    f.key = false;
    hub.frame(f.clone()).unwrap();
    assert_eq!(hub.m4s_subscribe().0.len(), 1);
    f.key = true;
    hub.frame(f).unwrap();
    assert_eq!(hub.m4s_subscribe().0.len(), 2);
    hub.frame(Frame {
        track_id: 12,
        dts: 93600,
        pts_offset: 0,
        key: true,
        body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
    })
    .unwrap();
    assert_eq!(hub.m4s_subscribe().0.len(), 3);
    let mut changed = tracks;
    changed[0].config.push(0);
    hub.info(changed).unwrap();
    assert_eq!(hub.m4s_subscribe().0.len(), 1);
}

#[test]
fn audio_only_native_segments_close_without_video_and_cache_before_signal() {
    use flussonix::{
        m4f::{Frame, unpack},
        m4s::Track,
        wire::Hub,
    };
    for (codec, config, body, step) in [
        ("aac", vec![0x11, 0x90], vec![1, 2, 3], 1920),
        (
            "m2a",
            vec![],
            include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
            2160,
        ),
        (
            "mp3",
            vec![],
            include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
            2351,
        ),
    ] {
        let h = Hub::new();
        h.info(vec![Track {
            id: 1,
            codec: codec.into(),
            config,
        }])
        .unwrap();
        for i in 0..=180000 / step + 1 {
            h.frame(Frame {
                track_id: 1,
                dts: i * step,
                pts_offset: 0,
                key: true,
                body: body.clone(),
            })
            .unwrap();
        }
        let (signals, _) = h.signal_subscribe();
        assert_eq!(signals.len(), 1, "{codec} never closed audio segment");
        let signal = std::str::from_utf8(&signals[0]).unwrap();
        let stamp = signal
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        let segment = h
            .segment(&format!("{stamp}.m4f"))
            .expect("signal precedes cache");
        let (tracks, frames) = unpack(&segment).unwrap();
        assert_eq!(tracks[0].codec, codec);
        assert!(frames.iter().all(|f| f.key && f.body == body));
        assert!(frames.last().unwrap().dts < 180000);
        assert!(
            h.m4s_subscribe().0.len() < 5,
            "audio bootstrap did not reset at segment boundary"
        );
    }
}

#[test]
fn native_frame_tag_kind_and_track_id_must_match_advertised_metadata() {
    use flussonix::{m4s::Decoder, wire};
    let tracks = native_tracks();
    let info = wire::encode_info(&tracks).unwrap();
    let good = wire::encode_frame(&tracks[1], &native_frames()[1]).unwrap();
    let at = good.windows(4).position(|b| b == b"fhdr").unwrap() + 4;
    for edit in [0, 1, 2] {
        let mut bad = good.clone();
        match edit {
            0 => bad[at + 4] = 1,
            1 => bad[at + 8..at + 12].copy_from_slice(b" mp3"),
            _ => bad[at..at + 4].copy_from_slice(&99u32.to_be_bytes()),
        }
        let mut decoder = Decoder::default();
        decoder.push(&info).unwrap();
        assert!(
            decoder.push(&bad).is_err(),
            "accepted frame mutation {edit}"
        );
    }
}

#[test]
fn native_originators_reject_unknown_samples_and_inconsistent_timeline() {
    use flussonix::{m4f, wire};
    let tracks = native_tracks();
    let mut frames = native_frames();
    frames[0].track_id = 99;
    assert!(m4f::pack(&tracks, &frames, 3600).is_err());
    assert!(wire::encode_frame(&tracks[0], &frames[0]).is_err());
    let mut samples = vec![native_frames()[0].clone(); 2];
    samples[0].dts = 93600;
    assert!(m4f::pack(&tracks, &samples, 3600).is_err());
    assert!(
        wire::encode_frame(
            &flussonix::m4s::Track {
                id: 1,
                codec: "opus".into(),
                config: vec![]
            },
            &flussonix::m4f::Frame {
                track_id: 1,
                ..native_frames()[0].clone()
            }
        )
        .is_err()
    );
}

#[test]
fn owned_native_samples_reconstruct_independently_decodable_elementary_streams() {
    use flussonix::m4f::{Frame, pack, unpack};
    use std::{fs, process::Command};
    let mut frames = Vec::new();
    let timing: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    for (i, p) in timing.iter().enumerate() {
        let dts = p["dts"].as_i64().unwrap();
        let pts = p["pts"].as_i64().unwrap();
        frames.push(Frame {
            track_id: 7,
            dts: ((dts + 1024) * 90000 / 12800) as u64,
            pts_offset: (pts - dts) * 90000 / 12800,
            key: p["flags"].as_str().unwrap().starts_with('K'),
            body: fs::read(format!(
                "{}/tests/fixtures/codecs/hevc-{i:02}.bin",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        });
    }
    for i in 0..5 {
        frames.push(Frame {
            track_id: 12,
            dts: i * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        });
        frames.push(Frame {
            track_id: 13,
            dts: i * 2351,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        });
    }
    frames.sort_by_key(|f| f.dts);
    let segment = pack(&native_tracks(), &frames, 43200).unwrap();
    let (tracks, decoded) = unpack(&segment).unwrap();
    for (expected, actual) in frames.iter().zip(&decoded) {
        assert_eq!(
            (actual.track_id, actual.dts, actual.pts_offset, &actual.body),
            (
                expected.track_id,
                expected.dts,
                expected.pts_offset,
                &expected.body
            )
        );
    }
    let dir = tempfile::tempdir().unwrap();
    for track in tracks {
        let codec = track.kind().unwrap();
        let mut stream = Vec::new();
        if codec == Codec::Hevc {
            let cfg = Configuration::parse(&track.config).unwrap();
            stream.extend(cfg.annex_b());
            for frame in decoded.iter().filter(|f| f.track_id == track.id) {
                stream.extend(cfg.access_unit(&frame.body).unwrap().annex_b);
            }
        } else {
            for frame in decoded.iter().filter(|f| f.track_id == track.id) {
                stream.extend_from_slice(&frame.body);
            }
        }
        let input = dir.path().join(&track.codec);
        fs::write(&input, stream).unwrap();
        let result = Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-threads",
                "2",
                "-f",
                if codec == Codec::Hevc { "hevc" } else { "mp3" },
                "-i",
            ])
            .arg(input)
            .args(["-f", "framemd5", "-"])
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{} decode failed: {}",
            track.codec,
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            result.stderr.is_empty(),
            "{} decoder diagnostics: {}",
            track.codec,
            String::from_utf8_lossy(&result.stderr)
        );
        let output = String::from_utf8(result.stdout).unwrap();
        assert_eq!(
            output
                .lines()
                .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
                .count(),
            if codec == Codec::Hevc { 12 } else { 5 }
        );
    }
}

#[test]
fn native_composition_table_uses_observed_version_zero_signed_offsets() {
    use flussonix::{m4f, m4s::boxes};
    let mut f = native_frames()[0].clone();
    f.pts_offset = -3600;
    let segment = m4f::pack(&native_tracks()[..1], &[f], 3600).unwrap();
    let at = segment.windows(4).position(|b| b == b"ctts").unwrap() + 4;
    assert_eq!(segment[at], 0, "reference M4F ignores version-one ctts");
    let (_, frames) = m4f::unpack(&segment).unwrap();
    assert_eq!(frames[0].pts_offset, -3600);
    let mut legacy = segment.clone();
    legacy[at] = 1;
    assert_eq!(m4f::unpack(&legacy).unwrap().1[0].pts_offset, -3600);
    let mut malformed = segment;
    malformed[at] = 2;
    assert!(m4f::unpack(&malformed).is_err());
    assert!(boxes(&legacy).is_ok());
}

#[test]
fn generated_native_records_respect_the_decoders_total_record_bound() {
    use flussonix::{m4s::Track, wire};
    let many: Vec<_> = (0..16)
        .map(|id| Track {
            id,
            codec: "m2a".into(),
            config: vec![0; 1024 * 1024],
        })
        .collect();
    assert!(wire::encode_info(&many).is_err());
    let mut frame = native_frames()[0].clone();
    frame.body = vec![0; 16 * 1024 * 1024];
    assert!(wire::encode_frame(&native_tracks()[0], &frame).is_err());
}

#[test]
fn native_m4f_segment_bounds_include_metadata_tables_and_payload_together() {
    use flussonix::{
        m4f::{Frame, pack, unpack},
        m4s::Track,
    };
    let tracks: Vec<_> = (0..15)
        .map(|id| Track {
            id,
            codec: "m2a".into(),
            config: vec![0; 1024 * 1024],
        })
        .collect();
    let frames: Vec<_> = (0..32768u64)
        .map(|i| Frame {
            track_id: (i % 15) as u32,
            dts: i / 15 * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        })
        .collect();
    assert!(
        pack(&tracks, &frames, 5000000).is_err(),
        "whole segment exceeds 32 MiB despite separate bounds"
    );
    let accepted = pack(&tracks, &frames[..16000], 5000000).unwrap();
    assert!(accepted.len() <= 32 * 1024 * 1024);
    let (decoded, samples) = unpack(&accepted).unwrap();
    assert_eq!(decoded, tracks);
    assert_eq!(samples.len(), 16000);
}

#[test]
fn native_sample_gaps_that_do_not_fit_the_table_fail_instead_of_truncating() {
    let track = native_tracks()[0].clone();
    let first = native_frames()[0].clone();
    let mut second = first.clone();
    second.dts = first.dts + u64::from(u32::MAX) + 1;
    assert!(flussonix::m4f::pack(&[track], &[first, second], 3600).is_err());
}
