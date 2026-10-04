use flussonix::{
    m4f::{self, Frame},
    m4s::{self, Track},
    wire,
    worker_ts::Muxer,
};
fn text(id: u32) -> Track {
    Track {
        id,
        codec: "subtitle".into(),
        config: vec![],
    }
}
fn cue(id: u32, dts: u64, end: u64, body: &[u8]) -> Frame {
    Frame {
        track_id: id,
        dts,
        pts_offset: (end - dts) as i64,
        key: true,
        body: body.to_vec(),
    }
}
#[test]
fn native_text_metadata_and_frames_use_reference_handler_and_tag() {
    let t = text(7);
    let mut decoder = m4s::Decoder::default();
    let info = wire::encode_info(std::slice::from_ref(&t)).unwrap();
    if let Ok(dir) = std::env::var("FLUSSONIX_NATIVE_SUBTITLE_ORACLE_DIR") {
        std::fs::write(std::path::Path::new(&dir).join("info.bin"), &info).unwrap();
    }
    if let Ok(dir) = std::env::var("FLUSSONIX_NATIVE_SUBTITLE_ORACLE_DIR") {
        let dir = std::path::Path::new(&dir);
        if dir.join("ref-info.bin").exists() {
            let mut reference = m4s::Decoder::default();
            assert!(
                matches!(&reference.push(&std::fs::read(dir.join("ref-info.bin")).unwrap()).unwrap()[0],m4s::Event::Info{tracks,..} if tracks==&vec![t.clone()])
            );
            for (index, payload) in ["EUROPE GRÜSSE".as_bytes(), &[]].into_iter().enumerate() {
                match &reference
                    .push(&std::fs::read(dir.join(format!("ref-frame-{index}.bin"))).unwrap())
                    .unwrap()[0]
                {
                    m4s::Event::Frame {
                        track_id,
                        dts,
                        pts_offset,
                        body,
                        ..
                    } => {
                        assert_eq!((*track_id, *dts, *pts_offset), (7, 90000, 63000));
                        assert_eq!(body, payload);
                    }
                    _ => panic!("reference subtitle frame lost"),
                }
            }
        }
    }
    assert!(info.windows(4).any(|b| b == b"text"));
    assert!(
        matches!(&decoder.push(&info).unwrap()[0],m4s::Event::Info{tracks,..} if tracks==&vec![t.clone()])
    );
    for (index, body) in ["EUROPE GRÜSSE".as_bytes(), &[]].into_iter().enumerate() {
        let f = cue(7, 90000, 153000, body);
        let bytes = wire::encode_frame(&t, &f).unwrap();
        if let Ok(dir) = std::env::var("FLUSSONIX_NATIVE_SUBTITLE_ORACLE_DIR") {
            std::fs::write(
                std::path::Path::new(&dir).join(format!("frame-{index}.bin")),
                &bytes,
            )
            .unwrap();
        }
        assert!(bytes.windows(4).any(|b| b == b"subt"));
        match &decoder.push(&bytes).unwrap()[0] {
            m4s::Event::Frame {
                track_id,
                dts,
                pts_offset,
                body: out,
                ..
            } => {
                assert_eq!((*track_id, *dts, *pts_offset), (7, 90000, 63000));
                assert_eq!(out, body)
            }
            _ => panic!("missing subtitle frame"),
        }
    }
}
#[test]
fn native_m4f_retains_cue_end_and_empty_clear_sample() {
    let t = text(7);
    let frames = vec![cue(7, 90000, 153000, b"HELLO"), cue(7, 180000, 180000, b"")];
    let data = m4f::pack(std::slice::from_ref(&t), &frames, 180000).unwrap();
    if let Ok(dir) = std::env::var("FLUSSONIX_NATIVE_SUBTITLE_ORACLE_DIR") {
        std::fs::write(std::path::Path::new(&dir).join("segment.m4f"), &data).unwrap();
    }
    if let Ok(dir) = std::env::var("FLUSSONIX_NATIVE_SUBTITLE_ORACLE_DIR") {
        let file = std::path::Path::new(&dir).join("ref-segment.m4f");
        if file.exists() {
            let (tracks, samples) = m4f::unpack(&std::fs::read(file).unwrap()).unwrap();
            assert_eq!(tracks, vec![t.clone()]);
            assert_eq!(samples.len(), frames.len());
            for (a, b) in frames.iter().zip(samples) {
                assert_eq!(
                    (a.dts, a.pts_offset, a.body.as_slice()),
                    (b.dts, b.pts_offset, b.body.as_slice())
                );
            }
        }
    }
    let (tracks, out) = m4f::unpack(&data).unwrap();
    assert_eq!(tracks, vec![t]);
    assert_eq!(out.len(), 2);
    for (a, b) in frames.iter().zip(out) {
        assert_eq!(
            (a.track_id, a.dts, a.pts_offset, a.body.as_slice()),
            (b.track_id, b.dts, b.pts_offset, b.body.as_slice())
        )
    }
}
#[test]
fn subtitle_track_does_not_become_a_worker_audio_stream() {
    let tracks = vec![
        text(7),
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
    ];
    let mut mux = Muxer::new(&tracks).unwrap();
    assert!(
        mux.frame(&cue(7, 90000, 153000, b"HELLO"))
            .unwrap()
            .is_empty()
    );
    let bytes = mux
        .frame(&Frame {
            track_id: 2,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        })
        .unwrap();
    assert!(!bytes.is_empty());
    assert!(Muxer::new(&[text(7)]).is_err());
}
#[test]
fn native_subtitles_do_not_disable_existing_aac_rtp() {
    let hub = wire::Hub::new();
    hub.info(vec![
        text(7),
        Track {
            id: 2,
            codec: "aac".into(),
            config: vec![0x11, 0x88],
        },
    ])
    .unwrap();
    let description = hub.rtp.description().unwrap().unwrap();
    assert_eq!(description.tracks.len(), 1);
    assert_eq!(description.tracks[0].id, 2);
    hub.frame(cue(7, 90000, 153000, b"HELLO")).unwrap();
}

#[test]
fn native_text_rejects_mismatched_handlers_kinds_and_duplicate_ids() {
    let track = text(7);
    let info = wire::encode_info(std::slice::from_ref(&track)).unwrap();
    let mut wrong_handler = info.to_vec();
    let at = wrong_handler.windows(4).position(|b| b == b"text").unwrap();
    wrong_handler[at..at + 4].copy_from_slice(b"soun");
    assert!(m4s::Decoder::default().push(&wrong_handler).is_err());
    assert!(wire::encode_info(&[track.clone(), track.clone()]).is_err());
    let frame = wire::encode_frame(&track, &cue(7, 90000, 153000, b"HELLO")).unwrap();
    let mut wrong_kind = frame.to_vec();
    let header = wrong_kind.windows(4).position(|b| b == b"fhdr").unwrap() + 4;
    wrong_kind[header + 4] = 2; // Audio cannot carry the native text frame tag.
    let mut decoder = m4s::Decoder::default();
    decoder.push(&info).unwrap();
    assert!(decoder.push(&wrong_kind).is_err());
}
