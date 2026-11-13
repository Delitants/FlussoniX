use flussonix::m4s::{Decoder, Event, atom};

fn sparse_subtitle_track(id: u32) -> Vec<u8> {
    let mut handler = vec![0; 4];
    handler.extend_from_slice(&id.to_be_bytes());
    handler.extend_from_slice(b"textsubtitle\0");
    let mut shift = vec![0; 4];
    shift.extend_from_slice(&90_000u32.to_be_bytes());
    shift.extend_from_slice(&[0; 8]);
    atom(
        b"trak",
        &[
            atom(b"shft", &shift),
            atom(b"hdlr", &handler),
            atom(b"btrt", &[0; 8]),
            atom(b"lang", b"eng"),
            atom(b"mpeg", &[0; 14]),
        ]
        .concat(),
    )
}

#[test]
fn m4s_accepts_vendor_application_subtitle_handler_without_configuration() {
    let audio = atom(
        b"trak",
        &[
            atom(
                b"hdlr",
                &[&[0; 4][..], &1u32.to_be_bytes(), b"sounaac\0"].concat(),
            ),
            atom(b"cnfg", &[0, 0, 0, 0, 0x11, 0x90]),
        ]
        .concat(),
    );
    let subtitle = atom(
        b"trak",
        &atom(
            b"hdlr",
            &[&[0; 4][..], &9u32.to_be_bytes(), b"applsubtitle\0"].concat(),
        ),
    );
    let info = atom(b"MDin", &[audio, subtitle].concat());
    let packet = [(info.len() as u32).to_be_bytes().to_vec(), info].concat();
    let events = Decoder::default().push(&packet).unwrap();
    let Event::Info { tracks, .. } = &events[0] else {
        panic!("expected media info");
    };
    assert_eq!(tracks.len(), 2);
    assert_eq!(tracks[1].id, 9);
    assert_eq!(tracks[1].codec, "subtitle");
    assert!(tracks[1].config.is_empty());

    let wrong_kind = atom(
        b"trak",
        &[
            atom(
                b"hdlr",
                &[&[0; 4][..], &1u32.to_be_bytes(), b"applaac\0"].concat(),
            ),
            atom(b"cnfg", &[0, 0, 0, 0, 0x11, 0x90]),
        ]
        .concat(),
    );
    let info = atom(b"MDin", &wrong_kind);
    let packet = [(info.len() as u32).to_be_bytes().to_vec(), info].concat();
    assert_eq!(
        Decoder::default().push(&packet).unwrap_err(),
        "native handler/codec kind mismatch"
    );
}

#[test]
fn m4s_accepts_silent_subtitle_metadata_without_configuration() {
    let mut handler = vec![0; 4];
    handler.extend_from_slice(&1u32.to_be_bytes());
    handler.extend_from_slice(b"sounaac\0");
    let audio = atom(
        b"trak",
        &[
            atom(b"hdlr", &handler),
            atom(b"cnfg", &[0, 0, 0, 0, 0x12, 0x10]),
        ]
        .concat(),
    );
    let info = atom(b"MDin", &[audio.clone(), sparse_subtitle_track(9)].concat());
    let packet = [(info.len() as u32).to_be_bytes().to_vec(), info].concat();
    let events = Decoder::default().push(&packet).unwrap();
    let Event::Info { tracks, wire } = &events[0] else {
        panic!("expected media info");
    };
    assert_eq!(wire.as_ref(), packet.as_slice());
    assert_eq!(tracks.len(), 2);
    assert_eq!(tracks[1].id, 9);
    assert_eq!(tracks[1].codec, "subtitle");
    assert!(tracks[1].config.is_empty());

    let no_audio_config = atom(
        b"MDin",
        &[
            atom(b"trak", &atom(b"hdlr", &handler)),
            sparse_subtitle_track(9),
        ]
        .concat(),
    );
    let bad = [
        (no_audio_config.len() as u32).to_be_bytes().to_vec(),
        no_audio_config,
    ]
    .concat();
    assert!(Decoder::default().push(&bad).is_err());
}

#[test]
fn m4f_preserves_silent_subtitle_track_but_rejects_partial_tables() {
    use flussonix::{
        m4f::{Frame, pack, unpack},
        m4s::{Track, boxes},
    };
    let audio = Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    };
    let frame = Frame {
        track_id: 1,
        dts: 90_000,
        pts_offset: 0,
        key: true,
        body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
    };
    let packed = pack(std::slice::from_ref(&audio), &[frame], 90_000).unwrap();
    let root = boxes(&packed).unwrap();
    let moov = boxes(root[0].1).unwrap();
    let sparse = sparse_subtitle_track(9);
    let build = |extra: Vec<u8>| {
        [
            atom(b"moov", &[root[0].1, extra.as_slice()].concat()),
            atom(b"mdat", root[1].1),
        ]
        .concat()
    };
    assert_eq!(moov.iter().filter(|(kind, _)| *kind == b"trak").count(), 1);
    let sparse_segment = build(sparse.clone());
    let (tracks, frames) = unpack(&sparse_segment).unwrap();
    assert_eq!(
        tracks,
        vec![
            audio,
            Track {
                id: 9,
                codec: "subtitle".into(),
                config: vec![]
            }
        ]
    );
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].track_id, 1);

    // M4S packed GOPs embed the same M4F body. This is the live source's
    // media path after its separate MDin announcement.
    let wire = flussonix::m4s::encode_gop(&flussonix::m4s::PackedGop {
        utc: 1,
        dts_ms: 1000.0,
        sequence: 1,
        duration_ms: 1000.0,
        body: bytes::Bytes::from(sparse_segment),
    })
    .unwrap();
    let events = Decoder::default().push(&wire).unwrap();
    let Event::Gop { tracks, frames, .. } = &events[0] else {
        panic!("expected packed GOP");
    };
    assert_eq!(tracks[1].codec, "subtitle");
    assert_eq!(frames.len(), 1);

    let body = boxes(&sparse).unwrap()[0].1;
    let partial = atom(
        b"trak",
        &[body, atom(b"stsz", &[0; 12]).as_slice()].concat(),
    );
    assert!(unpack(&build(partial)).is_err());

    // Sparse sample tables are a subtitle-only allowance. A silent audio
    // track must not be mistaken for an acceptable vendor text declaration.
    let audio_metadata = boxes(moov[1].1)
        .unwrap()
        .into_iter()
        .filter(|(kind, _)| *kind == b"shft" || *kind == b"hdlr" || *kind == b"cnfg")
        .flat_map(|(kind, value)| atom(kind.try_into().unwrap(), value))
        .collect::<Vec<_>>();
    let missing_audio_tables = [
        atom(
            b"moov",
            &[
                atom(b"segm", moov[0].1),
                atom(b"trak", &audio_metadata),
                sparse,
            ]
            .concat(),
        ),
        atom(b"mdat", root[1].1),
    ]
    .concat();
    assert!(unpack(&missing_audio_tables).is_err());
}

#[test]
fn m4f_fifteen_track_hevc_aac_segment_keeps_silent_text_and_builds_ts_muxer() {
    use flussonix::{
        m4f::{Frame, pack, unpack},
        m4s::{Track, boxes},
        worker_ts::Muxer,
    };
    let mut tracks = vec![Track {
        id: 1,
        codec: "hevc".into(),
        config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
    }];
    for id in 2..=8 {
        tracks.push(Track {
            id,
            codec: "aac".into(),
            config: vec![0x12, 0x10],
        });
    }
    let frames = tracks
        .iter()
        .map(|track| Frame {
            track_id: track.id,
            dts: 90_000,
            pts_offset: 0,
            key: true,
            body: if track.id == 1 {
                include_bytes!("fixtures/codecs/hevc-00.bin").to_vec()
            } else {
                vec![0x11, 0x22, 0x33, 0x44]
            },
        })
        .collect::<Vec<_>>();
    let packed = pack(&tracks, &frames, 3600).unwrap();
    let root = boxes(&packed).unwrap();
    let silent = (9..=15)
        .map(sparse_subtitle_track)
        .collect::<Vec<_>>()
        .concat();
    let segment = [
        atom(b"moov", &[root[0].1, silent.as_slice()].concat()),
        atom(b"mdat", root[1].1),
    ]
    .concat();
    let (parsed, samples) = unpack(&segment).unwrap();
    assert_eq!(parsed.len(), 15);
    assert_eq!(samples.len(), 8);
    assert_eq!(
        parsed
            .iter()
            .filter(|track| track.codec == "subtitle")
            .count(),
        7
    );
    assert!(!Muxer::new(&parsed).unwrap().tables().is_empty());
}

#[test]
fn split_packets_and_avc_aac_headers_are_decoded_without_guessing_fmp4() {
    let mut handler = vec![0, 0, 0, 0];
    handler.extend_from_slice(&1u32.to_be_bytes());
    handler.extend_from_slice(b"videh264\0");
    let mut cfg = vec![0, 0, 0, 0];
    cfg.extend_from_slice(&[1, 100, 0, 40, 255, 225, 0, 2, 103, 1, 1, 0, 2, 104, 1]);
    let track = atom(
        b"trak",
        &[atom(b"hdlr", &handler), atom(b"cnfg", &cfg)].concat(),
    );
    let info = atom(b"MDin", &track);
    let packet = [(info.len() as u32).to_be_bytes().to_vec(), info].concat();
    let mut d = Decoder::default();
    assert!(d.push(&packet[..7]).unwrap().is_empty());
    let events = d.push(&packet[7..]).unwrap();
    match &events[0] {
        Event::Info { tracks, .. } => {
            assert_eq!(tracks[0].id, 1);
            assert_eq!(tracks[0].codec, "h264");
            assert_eq!(tracks[0].config[0], 1)
        }
        _ => panic!("expected media info"),
    }
    let mut fh = 1u32.to_be_bytes().to_vec();
    fh.extend_from_slice(&[1, 2, 1, 128]);
    fh.extend_from_slice(b"h264");
    fh.extend_from_slice(&90000u64.to_be_bytes());
    fh.extend_from_slice(&(-3600i64).to_be_bytes());
    fh.extend_from_slice(&0u32.to_be_bytes());
    let f = atom(
        b"FRam",
        &[atom(b"fhdr", &fh), atom(b"body", &[0, 0, 0, 2, 101, 1])].concat(),
    );
    let packet = [(f.len() as u32).to_be_bytes().to_vec(), f].concat();
    match &d.push(&packet).unwrap()[0] {
        Event::Frame {
            track_id,
            dts,
            pts_offset,
            key,
            body,
            ..
        } => {
            assert_eq!(*track_id, 1);
            assert_eq!(*dts, 90000);
            assert_eq!(*pts_offset, -3600);
            assert!(*key);
            assert_eq!(body.len(), 6)
        }
        _ => panic!("expected frame"),
    }
    assert!(
        Decoder::default()
            .push(&0xffff_ffffu32.to_be_bytes())
            .is_err()
    );
}
#[test]
fn m4f_roundtrip_preserves_samples_timestamps_and_rejects_truncation() {
    use flussonix::m4f::{Frame, pack, unpack};
    let track = flussonix::m4s::Track {
        id: 1,
        codec: "h264".into(),
        config: vec![1, 100, 0, 40],
    };
    let frames = vec![
        Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 3600,
            key: true,
            body: vec![0, 0, 0, 1, 101],
        },
        Frame {
            track_id: 1,
            dts: 93600,
            pts_offset: 0,
            key: false,
            body: vec![0, 0, 0, 1, 65],
        },
    ];
    let b = pack(&[track], &frames, 7200).unwrap();
    let (tracks, decoded) = unpack(&b).unwrap();
    assert_eq!(tracks[0].codec, "h264");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].dts, 90000);
    assert_eq!(decoded[0].pts_offset, 3600);
    assert_eq!(decoded[1].body, frames[1].body);
    assert!(unpack(&b[..b.len() - 1]).is_err());
}

#[test]
fn malformed_m4f_tracks_and_empty_compositions_return_errors_without_amplification() {
    use flussonix::{
        m4f::{Frame, pack, unpack},
        m4s::{Track, boxes},
    };
    let track = Track {
        id: 1,
        codec: "h264".into(),
        config: vec![1, 100, 0, 40],
    };
    let frame = Frame {
        track_id: 1,
        dts: 0,
        pts_offset: 0,
        key: true,
        body: vec![0; 1024],
    };
    let input = pack(&[track], &[frame], 3600).unwrap();
    let root = boxes(&input).unwrap();
    let moov = boxes(root[0].1).unwrap();
    let segm = atom(b"segm", moov[0].1);
    let trak = atom(b"trak", moov[1].1);
    let many = [segm.clone(), trak.repeat(100)].concat();
    assert!(unpack(&[atom(b"moov", &many), atom(b"mdat", root[1].1)].concat()).is_err());
    let empty_ctts = [moov[1].1.to_vec(), atom(b"ctts", &[])].concat();
    let malformed = [
        atom(b"moov", &[segm, atom(b"trak", &empty_ctts)].concat()),
        atom(b"mdat", root[1].1),
    ]
    .concat();
    assert!(unpack(&malformed).is_err());
}

fn packed_record(dts: f64, duration: f64, extra: &[u8]) -> Vec<u8> {
    use flussonix::{
        m4f::{Frame, pack},
        m4s::Track,
    };
    let body = pack(
        &[Track {
            id: 1,
            codec: "h264".into(),
            config: vec![1, 100, 0, 40],
        }],
        &[Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: -3600,
            key: true,
            body: vec![0, 0, 0, 1, 101],
        }],
        3600,
    )
    .unwrap();
    let h = [
        atom(b" utc", &1700000000u32.to_be_bytes()),
        atom(b" dts", &dts.to_be_bytes()),
        atom(b" num", &7u32.to_be_bytes()),
        atom(b" dur", &duration.to_be_bytes()),
        extra.to_vec(),
    ]
    .concat();
    let p = atom(b"Fgop", &[atom(b"goph", &h), atom(b"body", &body)].concat());
    [(p.len() as u32).to_be_bytes().to_vec(), p].concat()
}
#[test]
fn fragmented_packed_gop_preserves_source_metadata_payload_and_composition() {
    let packet = packed_record(1000.0, 40.0, &[]);
    let mut decoder = Decoder::default();
    let mut events = Vec::new();
    for chunk in packet.chunks(7) {
        events.extend(decoder.push(chunk).expect("packed GOP should decode"));
    }
    match &events[0] {
        Event::Gop {
            gop,
            tracks,
            frames,
            wire,
        } => {
            assert_eq!(gop.utc, 1700000000);
            assert_eq!(gop.dts_ms, 1000.0);
            assert_eq!(gop.sequence, 7);
            assert_eq!(gop.duration_ms, 40.0);
            assert_eq!(wire.as_ref(), packet.as_slice());
            assert_eq!(tracks[0].id, 1);
            assert_eq!(frames[0].dts, 90000);
            assert_eq!(frames[0].pts_offset, -3600);
            assert!(gop.body.windows(4).any(|w| w == b"mdat"));
        }
        _ => panic!("expected packed GOP"),
    }
}
#[test]
fn malformed_gop_numbers_and_duplicate_required_fields_fail_closed() {
    for (dts, duration) in [
        (f64::NAN, 40.0),
        (f64::INFINITY, 40.0),
        (-1.0, 40.0),
        (1.0, 0.0),
        (1.0, f64::INFINITY),
    ] {
        assert!(
            Decoder::default()
                .push(&packed_record(dts, duration, &[]))
                .is_err()
        );
    }
    assert!(
        Decoder::default()
            .push(&packed_record(
                1.0,
                40.0,
                &atom(b" utc", &1u32.to_be_bytes())
            ))
            .is_err()
    );
}

#[test]
fn excessive_empty_boxes_are_rejected_before_container_allocation_amplifies() {
    assert!(flussonix::m4s::boxes(&atom(b"junk", &[]).repeat(1025)).is_err());
}
