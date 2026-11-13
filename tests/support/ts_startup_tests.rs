use super::*;

// Independent, literal ISO/IEC 13818-1 section and PES fixtures. These helpers
// don't call the startup parser or its table writer to derive expected output.
fn section(pid: u16, mut s: Vec<u8>) -> Vec<u8> {
    let len = s.len() + 1;
    s[1] = 0xb0 | ((len >> 8) as u8);
    s[2] = len as u8;
    let mut crc = 0xffff_ffffu32;
    for &b in &s {
        crc ^= u32::from(b) << 24;
        for _ in 0..8 {
            crc = (crc << 1)
                ^ if crc & 0x8000_0000 != 0 {
                    0x04c1_1db7
                } else {
                    0
                };
        }
    }
    s.extend(crc.to_be_bytes());
    let mut p = vec![0x47, 0x40 | ((pid >> 8) as u8), pid as u8, 0x10, 0];
    p.extend(s);
    p.resize(188, 0xff);
    p
}
fn pat() -> Vec<u8> {
    section(0, vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0])
}
fn pmt(kind: u8, version: u8) -> Vec<u8> {
    // Include a private subtitle PID and descriptor to detect accidental loss.
    section(
        4096,
        vec![
            2,
            0xb0,
            0,
            0,
            1,
            0xc1 | (version << 1),
            0,
            0,
            0xe1,
            0,
            0xf0,
            0,
            kind,
            0xe1,
            0,
            0xf0,
            0,
            6,
            0xe1,
            1,
            0xf0,
            4,
            0x56,
            2,
            0x65,
            0x6e,
        ],
    )
}
fn packet(pid: u16, start: bool, cc: u8, data: &[u8]) -> Vec<u8> {
    assert!(data.len() <= 182);
    let mut p = vec![0xff; 188];
    p[..4].copy_from_slice(&[
        0x47,
        ((pid >> 8) as u8) | if start { 0x40 } else { 0 },
        pid as u8,
        0x30 | cc,
    ]);
    let off = 188 - data.len();
    p[4] = (off - 5) as u8;
    p[5] = 0;
    p[off..].copy_from_slice(data);
    p
}
fn pes(body: &[u8], audio: bool) -> Vec<u8> {
    let mut data = vec![0, 0, 1, if audio { 0xc0 } else { 0xe0 }, 0, 0, 0x80, 0, 0];
    data.extend(body);
    data
}
fn key(hevc: bool) -> &'static [u8] {
    if hevc {
        &[
            0, 0, 1, 0x40, 1, 0x80, 0, 0, 1, 0x42, 1, 0x80, 0, 0, 1, 0x44, 1, 0x80, 0, 0, 1, 0x26,
            1, 0x80,
        ]
    } else {
        &[
            0, 0, 1, 0x67, 0x42, 0, 0, 1, 0x68, 0xc0, 0, 0, 1, 0x65, 0x88,
        ]
    }
}
fn setup(kind: u8) -> Startup {
    let mut s = Startup::default();
    assert!(s.push(pat().into()).is_none());
    assert!(s.push(pmt(kind, 0).into()).is_none());
    s
}

#[test]
fn video_requires_parameter_sets_and_random_access_not_just_a_pes_start() {
    for (kind, hevc, nonkey, barekey) in [
        (
            0x1b,
            false,
            &[0, 0, 1, 0x41, 0x80][..],
            &[0, 0, 1, 0x65, 0x88][..],
        ),
        (
            0x24,
            true,
            &[0, 0, 1, 0x02, 1, 0x80][..],
            &[0, 0, 1, 0x26, 1, 0x80][..],
        ),
    ] {
        let mut s = setup(kind);
        for (cc, body) in [(0, nonkey), (1, barekey)] {
            assert!(
                s.push(packet(256, true, cc, &pes(body, false)).into())
                    .is_none()
            );
        }
        let video = packet(256, true, 2, &pes(key(hevc), false));
        let subtitle = packet(257, true, 0, b"original-subtitle-payload");
        let input = [video.clone(), subtitle.clone(), vec![9, 8, 7]].concat();
        let out = s.push(input.into()).expect("valid key must start output");
        assert_eq!(&out[..188], pat());
        assert_eq!(&out[188..376], pmt(kind, 0));
        assert_eq!(&out[376..], [video, subtitle, vec![9, 8, 7]].concat());
    }
}

#[test]
fn arbitrary_reads_and_fragmented_pes_headers_keep_the_first_keyframe() {
    for (kind, hevc) in [(0x1b, false), (0x24, true)] {
        let data = pes(key(hevc), false);
        let mut input = [pat(), pmt(kind, 0)].concat();
        for (cc, part) in data.chunks(2).enumerate() {
            input.extend(packet(256, cc == 0, cc as u8 & 15, part));
        }
        for chunk in [1, 7, 187, 188, 189, 4096] {
            let mut s = Startup::default();
            let mut output = None;
            for part in input.chunks(chunk) {
                if let Some(out) = s.push(Bytes::copy_from_slice(part)) {
                    output = Some(out);
                    break;
                }
            }
            let output = output.expect("fragmented startup must complete");
            assert_eq!(&output[..376], &input[..376]);
            assert_eq!(&output[376..], &input[376..376 + output.len() - 376]);
        }
    }
}

#[test]
fn audio_only_waits_for_a_fresh_pes_for_aac_mp2_and_mp3() {
    for kind in [0x0f, 3, 4] {
        let mut s = setup(kind);
        assert!(
            s.push(packet(256, false, 4, b"middle-of-audio").into())
                .is_none()
        );
        let audio = packet(256, true, 5, &pes(b"encoded audio", true));
        let out = s.push(audio.clone().into()).unwrap();
        assert_eq!(&out[376..], audio);
    }
}

#[test]
fn corruption_and_continuity_gaps_cannot_complete_parameter_bootstrap() {
    for damage in [0, 1, 2, 3] {
        let mut s = setup(0x1b);
        let header = packet(
            256,
            true,
            0,
            &pes(&[0, 0, 1, 0x67, 0x42, 0, 0, 1, 0x68, 0xc0], false),
        );
        assert!(s.push(header.into()).is_none());
        let mut tail = packet(256, false, 1, &[0, 0, 1, 0x65, 0x88]);
        match damage {
            0 => tail[1] |= 0x80,
            1 => tail[3] |= 0x80,
            2 => tail[3] = (tail[3] & 0xf0) | 3,
            _ => tail[5] = 0x80,
        }
        assert!(s.push(tail.into()).is_none());
        assert!(
            s.push(packet(256, true, 4, &pes(&[0, 0, 1, 0x65, 0x88], false)).into())
                .is_none()
        );
        assert!(
            s.push(packet(256, true, 5, &pes(key(false), false)).into())
                .is_some()
        );
    }
}

#[test]
fn invalid_psi_and_changed_pmt_do_not_reuse_old_parameters() {
    let mut s = Startup::default();
    let mut bad = pat();
    bad[16] ^= 1;
    assert!(s.push(bad.into()).is_none());
    assert!(s.push(pmt(0x1b, 0).into()).is_none());
    assert!(
        s.push(packet(256, true, 0, &pes(key(false), false)).into())
            .is_none()
    );
    assert!(s.push(pat().into()).is_none());
    assert!(s.push(pmt(0x1b, 0).into()).is_none());
    assert!(
        s.push(
            packet(
                256,
                true,
                1,
                &pes(&[0, 0, 1, 0x67, 0x42, 0, 0, 1, 0x68, 0xc0], false)
            )
            .into()
        )
        .is_none()
    );
    assert!(s.push(pmt(0x24, 1).into()).is_none());
    assert!(
        s.push(packet(256, true, 2, &pes(&[0, 0, 1, 0x26, 1, 0x80], false)).into())
            .is_none()
    );
    assert!(
        s.push(packet(256, true, 3, &pes(key(true), false)).into())
            .is_some()
    );
}

#[test]
fn parameter_only_pes_can_precede_the_random_access_pes() {
    let mut s = setup(0x1b);
    let parameters = packet(
        256,
        true,
        0,
        &pes(&[0, 0, 1, 0x67, 0x42, 0, 0, 1, 0x68, 0xc0], false),
    );
    assert!(s.push(parameters.clone().into()).is_none());
    let key = packet(256, true, 1, &pes(&[0, 0, 1, 0x65, 0x88], false));
    let out = s.push(key.clone().into()).unwrap();
    assert_eq!(&out[376..], [parameters, key].concat());
}

#[test]
fn oversized_parameter_preroll_is_discarded_and_can_recover() {
    let mut s = setup(0x1b);
    assert!(
        s.push(packet(256, true, 0, &pes(&[0, 0, 1, 0x67, 0x42], false)).into())
            .is_none()
    );
    let filler = packet(257, false, 0, &[0xff; 182]);
    for _ in 0..10_000 {
        assert!(s.push(Bytes::copy_from_slice(&filler)).is_none());
        assert!(s.candidate.len() <= 64 * 1024);
        assert!(s.pending.len() < 188);
    }
    assert!(
        s.push(packet(256, true, 1, &pes(key(false), false)).into())
            .is_some()
    );
}

#[test]
fn unsupported_multiplexes_keep_raw_output_instead_of_waiting_for_an_unknown_codec() {
    let mut s = Startup::default();
    let pat = section(
        0,
        vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0, 0, 2, 0xf0, 1],
    );
    assert_eq!(s.push(pat.clone().into()).unwrap(), pat);
    for kind in [2, 0x10, 6] {
        let mut s = Startup::default();
        assert!(s.push(self::pat().into()).is_none());
        let pmt = pmt(kind, 0);
        let out = s.push(pmt.clone().into()).unwrap();
        assert_eq!(&out[188..], pmt);
    }
    let mut s = Startup::default();
    let multi = section(0, vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 1, 0, 1, 0xf0, 0]);
    assert!(s.push(multi.into()).is_some());
}

#[test]
fn malformed_pes_does_not_accumulate_unbounded_headers_or_open_the_gate() {
    for header in [vec![0; 9], vec![0, 0, 1, 0xe0, 0, 0, 0, 0, 0]] {
        let mut s = setup(0x1b);
        assert!(s.push(packet(256, true, 0, &header).into()).is_none());
        assert!(s.candidate.is_empty());
        assert!(
            s.push(packet(256, true, 1, &pes(key(false), false)).into())
                .is_some()
        );
    }
}

#[test]
fn a_discontinuity_can_start_fresh_psi_or_a_self_contained_key_pes() {
    for marked in 0..3 {
        let mut s = Startup::default();
        let mut pat = pat();
        let mut pmt = pmt(0x1b, 0);
        let mut video = packet(256, true, 0, &pes(key(false), false));
        if marked < 2 {
            let table = if marked == 0 { &mut pat } else { &mut pmt };
            let pid = (u16::from(table[1] & 31) << 8) | u16::from(table[2]);
            let len = 3 + ((usize::from(table[6] & 15) << 8) | usize::from(table[7]));
            *table = packet(pid, true, 0, &table[4..5 + len]);
            table[5] = 0x80;
        } else {
            video[5] = 0x80;
        }
        assert!(s.push(pat.into()).is_none());
        assert!(s.push(pmt.into()).is_none());
        assert!(
            s.push(video.into()).is_some(),
            "fresh discontinuity packet {marked} was lost"
        );
    }
}
