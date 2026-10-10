use flussonix::{
    m4f::Frame,
    m4s::Track,
    worker_output::{Decoder, Event},
    worker_ts::Muxer,
};
fn track(codec: &str, id: u32, config: &[u8]) -> Track {
    Track {
        id,
        codec: codec.into(),
        config: config.to_vec(),
    }
}
fn frame(id: u32, dts: u64, body: &[u8]) -> Frame {
    Frame {
        track_id: id,
        dts,
        pts_offset: 0,
        key: true,
        body: body.to_vec(),
    }
}
fn mux(tracks: &[Track], frames: &[Frame]) -> Vec<u8> {
    let mut m = Muxer::new(tracks).unwrap();
    let mut data = m.tables();
    for f in frames {
        data.extend(m.frame(f).unwrap());
    }
    data
}
fn decode(data: &[u8], chunk: usize) -> Vec<Event> {
    let mut d = Decoder::default();
    let mut out = vec![];
    for b in data.chunks(chunk) {
        out.extend(d.push(b).unwrap());
    }
    out.extend(d.finish().unwrap());
    out
}
fn samples(events: &[Event]) -> Vec<&Frame> {
    assert!(matches!(events.first(), Some(Event::Info(_))));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::Info(_)))
            .count(),
        1
    );
    events
        .iter()
        .filter_map(|e| {
            if let Event::Frame(f) = e {
                Some(f)
            } else {
                None
            }
        })
        .collect()
}
const MP2: &[u8] = include_bytes!("fixtures/codecs/mp2.bin");
const MP3: &[u8] = include_bytes!("fixtures/codecs/mp3.bin");
#[test]
fn mpeg_identity_payload_and_clocks_survive_single_byte_chunks() {
    let frames = [
        frame(7, 90000, MP2),
        frame(9, 90000, MP3),
        frame(7, 92160, MP2),
        frame(9, 92351, MP3),
    ];
    let events = decode(
        &mux(&[track("m2a", 7, &[]), track("mp3", 9, &[])], &frames),
        1,
    );
    let Event::Info(t) = &events[0] else {
        unreachable!()
    };
    assert_eq!(
        t.iter()
            .map(|t| (t.id, t.codec.as_str(), t.config.len()))
            .collect::<Vec<_>>(),
        [(256, "m2a", 0), (257, "mp3", 0)]
    );
    let got = samples(&events);
    assert_eq!(got.len(), 4);
    for (a, b) in got.iter().zip(&frames) {
        assert_eq!((a.dts, a.pts_offset, &a.body), (b.dts, 0, &b.body));
    }
}
#[test]
fn hevc_configuration_and_signed_composition_survive_with_audio_before_video() {
    let cfg = include_bytes!("fixtures/codecs/hevc.hvcc");
    let video = include_bytes!("fixtures/codecs/hevc-00.bin");
    let mut v = frame(1, 90000, video);
    v.pts_offset = -3600;
    let events = decode(
        &mux(
            &[track("hevc", 1, cfg), track("m2a", 2, &[])],
            &[frame(2, 90000, MP2), v],
        ),
        79,
    );
    let Event::Info(t) = &events[0] else {
        unreachable!()
    };
    assert_eq!(t[0].codec, "hevc");
    // PTL, chroma and depth come from the actual SPS, rather than a fabricated default.
    assert_eq!(&t[0].config[1..13], &cfg[1..13]);
    assert_eq!(&t[0].config[16..19], &cfg[16..19]);
    let got = samples(&events);
    assert_eq!(got.len(), 2);
    let v = got.iter().find(|f| f.track_id == 256).unwrap();
    assert_eq!((v.dts, v.pts_offset, v.key), (90000, -3600, true));
    // The mux inserts SEI from hvcC alongside the parameter sets on a key frame.
    // Preserve those bytes and the exact original coded-picture sample.
    let mut expected = vec![];
    let mut at = 23;
    for _ in 0..cfg[22] {
        let kind = cfg[at] & 63;
        let count = u16::from_be_bytes([cfg[at + 1], cfg[at + 2]]);
        at += 3;
        for _ in 0..count {
            let n = u16::from_be_bytes([cfg[at], cfg[at + 1]]) as usize;
            at += 2;
            if matches!(kind, 39 | 40) {
                expected.extend_from_slice(&(n as u32).to_be_bytes());
                expected.extend_from_slice(&cfg[at..at + n]);
            }
            at += n;
        }
    }
    expected.extend_from_slice(video);
    assert!(
        v.body == expected,
        "coded picture and initialization SEI bytes must survive"
    );
    assert_eq!(got.iter().find(|f| f.track_id == 257).unwrap().body, MP2);
}
#[test]
fn aac_adts_header_is_removed_and_asc_is_observed() {
    let events = decode(
        &mux(
            &[track("aac", 3, &[0x11, 0x90])],
            &[frame(3, 90000, &[1, 2, 3, 4])],
        ),
        188 * 64,
    );
    let Event::Info(t) = &events[0] else {
        unreachable!()
    };
    assert_eq!(t[0].config, [0x11, 0x90]);
    assert_eq!(samples(&events)[0].body, [1, 2, 3, 4]);
}
#[test]
fn small_audio_pes_corrections_cannot_overlap_decoded_samples() {
    // Literal source-clock pattern from an owned AAC/SRTP recording: a late
    // clock correction compressed six complete audio frames into a few ticks.
    let stamps = [
        90000, 91920, 91922, 91924, 91926, 91928, 91929, 93780, 95700, 120000,
    ];
    let frames: Vec<_> = stamps
        .into_iter()
        .map(|dts| frame(3, dts, &[1, 2, 3, 4]))
        .collect();
    let events = decode(&mux(&[track("aac", 3, &[0x11, 0x90])], &frames), 79);
    let got = samples(&events);
    assert_eq!(
        got.iter().map(|f| f.dts).collect::<Vec<_>>(),
        [
            90000, 91920, 93840, 95760, 97680, 99600, 101520, 103440, 105360, 120000
        ]
    );
    assert!(got.iter().all(|f| f.body == [1, 2, 3, 4]));
}
#[test]
fn mpeg_audio_corrections_keep_sample_duration_and_fractional_remainder() {
    for (codec, body, expected) in [
        ("m2a", MP2, [90000, 92160, 94320, 96480, 98640]),
        ("mp3", MP3, [90000, 92351, 94702, 97053, 99404]),
    ] {
        let frames: Vec<_> = [90000, 90001, 90002, 90003, 90004]
            .into_iter()
            .map(|dts| frame(7, dts, body))
            .collect();
        let events = decode(&mux(&[track(codec, 7, &[])], &frames), 187);
        let got = samples(&events);
        assert_eq!(got.iter().map(|f| f.dts).collect::<Vec<_>>(), expected);
        assert!(got.iter().all(|f| f.body == body));
    }
}
#[test]
fn sustained_audio_clock_compression_closes_the_generation_at_a_quarter_second() {
    let frames: Vec<_> = (0..14)
        .map(|i| frame(3, 90000 + i, &[1, 2, 3, 4]))
        .collect();
    let mut decoder = Decoder::default();
    let result = decoder.push(&mux(&[track("aac", 3, &[0x11, 0x90])], &frames));
    assert!(
        result.is_err(),
        "compressed source timestamps must not accumulate unbounded AV drift"
    );
    assert!(
        decoder.finish().is_err(),
        "clock rejection must be terminal"
    );
}
#[test]
fn audio_clock_overlap_limit_accepts_22500_ticks_and_rejects_22501() {
    // Fourteen literal 48kHz AAC-LC frames advance from 90000 to 116880.
    // The next PES clock is nondecreasing but exactly 250ms (or one tick more)
    // behind the sample clock. No value is calculated by the implementation.
    let adts = [255, 241, 76, 128, 1, 31, 252, 42];
    for (source, accepted) in [(94380, true), (94379, false)] {
        let mut data = Muxer::new(&[track("aac", 1, &[0x11, 0x90])])
            .unwrap()
            .tables();
        let mut cc = 0;
        data.extend(packets(256, &pes(&adts.repeat(14), 90000), &mut cc));
        let mut decoder = Decoder::default();
        let first = decoder.push(&data).unwrap();
        assert_eq!(samples(&first).last().unwrap().dts, 114960);
        let next = decoder.push(&packets(256, &pes(&adts, source), &mut cc));
        if accepted {
            let next = next.unwrap();
            assert!(
                matches!(next.as_slice(), [Event::Frame(f)] if f.dts == 116880 && f.body == [42])
            );
        } else {
            assert!(next.is_err());
            assert!(decoder.finish().is_err());
        }
    }
}
#[test]
fn ordinary_backwards_source_pes_clock_is_rejected_after_sample_clock_correction() {
    let mut decoder = Decoder::default();
    let first = mux(
        &[track("aac", 1, &[0x11, 0x90])],
        &[frame(1, 90000, &[42]), frame(1, 90001, &[42])],
    );
    decoder.push(&first).unwrap();
    let adts = [255, 241, 76, 128, 1, 31, 252, 42];
    let cc = first
        .chunks_exact(188)
        .rfind(|p| p[1] & 31 == 1 && p[2] == 0)
        .unwrap()[3]
        & 15;
    let mut cc = (cc + 1) & 15;
    assert!(
        decoder
            .push(&packets(256, &pes(&adts, 90000), &mut cc))
            .is_err()
    );
    assert!(decoder.finish().is_err());
}
fn stamp(value: u64, kind: u8) -> [u8; 5] {
    [
        kind << 4 | ((value >> 29) as u8 & 14) | 1,
        (value >> 22) as u8,
        ((value >> 14) as u8 & 254) | 1,
        (value >> 7) as u8,
        ((value << 1) as u8 & 254) | 1,
    ]
}
fn pes(payload: &[u8], dts: u64) -> Vec<u8> {
    let n = payload.len() + 8;
    let mut out = vec![0, 0, 1, 0xc0, (n >> 8) as u8, n as u8, 0x80, 0x80, 5];
    out.extend(stamp(dts, 2));
    out.extend(payload);
    out
}
fn packets(pid: u16, data: &[u8], cc: &mut u8) -> Vec<u8> {
    let mut out = vec![];
    for (i, b) in data.chunks(184).enumerate() {
        let mut p = vec![
            0x47,
            ((pid >> 8) as u8) | if i == 0 { 0x40 } else { 0 },
            pid as u8,
            *cc | if b.len() == 184 { 0x10 } else { 0x30 },
        ];
        *cc = (*cc + 1) & 15;
        if b.len() < 184 {
            let n = 183 - b.len();
            p.push(n as u8);
            if n > 0 {
                p.push(0);
                p.resize(188 - b.len(), 255);
            }
        }
        p.extend(b);
        assert_eq!(p.len(), 188);
        out.extend(p);
    }
    out
}
#[test]
fn aggregated_and_split_mpeg_frames_keep_fractional_sample_clock() {
    let mut data = Muxer::new(&[track("mp3", 1, &[])]).unwrap().tables();
    let mut cc = 0;
    // First PES contains one frame and half the next; next PES finishes that frame,
    // then supplies a timestamp for the first newly starting frame in that PES.
    let mut first = MP3.to_vec();
    first.extend(&MP3[..100]);
    data.extend(packets(256, &pes(&first, 90000), &mut cc));
    let mut second = MP3[100..].to_vec();
    second.extend(MP3);
    second.extend(MP3);
    data.extend(packets(256, &pes(&second, 94702), &mut cc));
    let events = decode(&data, 187);
    let got = samples(&events);
    assert_eq!(
        got.iter().map(|f| f.dts).collect::<Vec<_>>(),
        [90000, 92351, 94702, 97053]
    );
    assert!(got.iter().all(|f| f.body == MP3));
}
#[test]
fn shared_wrap_keeps_audio_and_video_in_the_same_epoch() {
    let near = (1u64 << 33) - 2160;
    let events = decode(
        &mux(
            &[track("m2a", 1, &[]), track("mp3", 2, &[])],
            &[
                frame(1, near, MP2),
                frame(2, (1 << 33) + 191, MP3),
                frame(1, 1 << 33, MP2),
            ],
        ),
        4096,
    );
    assert_eq!(
        samples(&events).iter().map(|f| f.dts).collect::<Vec<_>>(),
        [near, (1 << 33) + 191, 1 << 33]
    );
}
#[test]
fn duplicate_payload_and_adaptation_only_do_not_advance_continuity() {
    let data = mux(
        &[track("m2a", 1, &[])],
        &[frame(1, 90000, MP2), frame(1, 92160, MP2)],
    );
    let mut changed = vec![];
    for p in data.chunks_exact(188) {
        changed.extend(p);
        changed.extend(p);
        let mut a = p[..4].to_vec();
        a[1] &= 0xbf;
        a[3] = (p[3] & 15) | 0x20;
        a.push(183);
        a.push(0);
        a.resize(188, 255);
        changed.extend(a);
    }
    assert_eq!(samples(&decode(&changed, 233)).len(), 2);
}
#[test]
fn malformed_transport_is_terminal_and_never_publishes_partial_media() {
    let good = mux(&[track("m2a", 1, &[])], &[frame(1, 90000, MP2)]);
    for (at, xor) in [(0, 1), (1, 0x80), (3, 0x80), (182, 1)] {
        let mut bad = good.clone();
        bad[at] ^= xor;
        let mut d = Decoder::default();
        assert!(d.push(&bad).is_err(), "mutation {at}");
        assert!(d.push(&good).is_err());
        assert!(d.finish().is_err());
    }
    let mut d = Decoder::default();
    d.push(&good[..good.len() - 1]).unwrap();
    assert!(d.finish().is_err());
    let mut bad = good.clone();
    let second = good
        .chunks_exact(188)
        .enumerate()
        .filter(|(_, p)| p[1] & 31 == 1 && p[2] == 0)
        .nth(1)
        .unwrap()
        .0;
    bad[188 * second + 3] ^= 1;
    assert!(Decoder::default().push(&bad).is_err());
}
#[test]
fn changed_parameter_sets_fail_instead_of_silently_reusing_metadata() {
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let mut m = Muxer::new(std::slice::from_ref(&t)).unwrap();
    let mut data = m.tables();
    data.extend(
        m.frame(&frame(
            1,
            90000,
            include_bytes!("fixtures/codecs/hevc-00.bin"),
        ))
        .unwrap(),
    );
    let mut cfg = t.config.clone();
    cfg[32] ^= 1;
    let mut other = Muxer::new(&[track("hevc", 1, &cfg)]).unwrap();
    other.tables();
    let mut next = other
        .frame(&frame(
            1,
            93600,
            include_bytes!("fixtures/codecs/hevc-00.bin"),
        ))
        .unwrap();
    // Continue the transport counter of the original mux, allowing codec change to be tested.
    let last = data.chunks_exact(188).last().unwrap()[3] & 15;
    let mut cc = (last + 1) & 15;
    for p in next
        .chunks_exact_mut(188)
        .filter(|p| p[1] & 31 == 1 && p[2] == 0)
    {
        p[3] = (p[3] & 0xf0) | cc;
        cc = (cc + 1) & 15;
        data.extend_from_slice(p);
    }
    let mut d = Decoder::default();
    let r = d.push(&data).and_then(|_| d.finish());
    let error = r.unwrap_err();
    assert!(error.contains("parameter set changed"), "{error}");
}

#[test]
fn initialization_wait_has_a_sample_count_bound_even_for_tiny_audio_payloads() {
    let tracks = [
        track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc")),
        track("aac", 2, &[0x11, 0x88]),
    ];
    let mut d = Decoder::default();
    d.push(&Muxer::new(&tracks).unwrap().tables()).unwrap();
    let mut cc = 0;
    let mut rejected = false;
    let body = [255, 241, 76, 64, 1, 31, 252, 0].repeat(256);
    for i in 0..260 {
        let data = packets(257, &pes(&body, 90000 + i * 256 * 1920), &mut cc);
        match d.push(&data) {
            Ok(events) => assert!(events.is_empty(), "must wait for video initialization"),
            Err(_) => {
                rejected = true;
                break;
            }
        }
    }
    assert!(
        rejected,
        "tiny audio frames must not create an unbounded initialization queue"
    );
}
#[test]
fn independently_encoded_profiles_reconstruct_every_decoded_picture_and_audio_sample() {
    use std::process::Command;
    for (video, audio, ten_bit) in [
        ("libx264", "aac", false),
        ("libx265", "mp2", false),
        ("libx265", "libmp3lame", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("original.ts");
        let output = dir.path().join("reconstructed.ts");
        let mut cmd = Command::new("ffmpeg");
        cmd.args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=700:sample_rate=48000",
            "-map",
            "0:v",
            "-map",
            "1:a",
            "-t",
            "1",
            "-c:v",
            video,
            "-threads:v",
            "1",
            "-pix_fmt",
            if ten_bit { "yuv420p10le" } else { "yuv420p" },
            "-c:a",
            audio,
            "-b:a",
            "96k",
        ]);
        if video == "libx265" {
            cmd.args([
                "-x265-params",
                "pools=none:frame-threads=1:bframes=2:keyint=6:log-level=error",
            ]);
        } else {
            cmd.args(["-bf", "2", "-g", "6"]);
        }
        let generated = cmd.args(["-f", "mpegts"]).arg(&input).output().unwrap();
        assert!(
            generated.status.success(),
            "owned profile generation failed: {}",
            String::from_utf8_lossy(&generated.stderr)
        );
        let events = decode(&std::fs::read(&input).unwrap(), 997);
        let Event::Info(tracks) = &events[0] else {
            unreachable!()
        };
        assert_eq!(tracks.len(), 2);
        assert_eq!(
            tracks[0].codec,
            if video == "libx264" { "h264" } else { "hevc" }
        );
        assert_eq!(
            tracks[1].codec,
            match audio {
                "aac" => "aac",
                "mp2" => "m2a",
                _ => "mp3",
            }
        );
        if ten_bit {
            assert_eq!(tracks[0].config[17] & 7, 2);
            assert_eq!(tracks[0].config[18] & 7, 2);
        }
        let mut frames: Vec<_> = samples(&events).into_iter().cloned().collect();
        frames.sort_by_key(|f| f.dts);
        assert_eq!(
            frames.iter().filter(|f| f.track_id == tracks[0].id).count(),
            25
        );
        std::fs::write(&output, mux(tracks, &frames)).unwrap();
        for stream in 0..2 {
            let hashes = |path: &std::path::Path| {
                let result = Command::new("ffmpeg")
                    .args(["-nostdin", "-v", "error", "-threads", "1", "-i"])
                    .arg(path)
                    .args(["-map", &format!("0:{stream}"), "-f", "framemd5", "-"])
                    .output()
                    .unwrap();
                assert!(
                    result.status.success() && result.stderr.is_empty(),
                    "independent decode failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                String::from_utf8(result.stdout)
                    .unwrap()
                    .lines()
                    .filter(|line| !line.starts_with('#'))
                    .map(|line| {
                        line.split(',')
                            .skip(4)
                            .map(str::trim)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .collect::<Vec<_>>()
            };
            let expected = hashes(&input);
            assert!(!expected.is_empty());
            assert_eq!(
                hashes(&output),
                expected,
                "decoded profile {video}/{audio} stream {stream}"
            );
        }
    }
}

#[test]
fn aac_lc_accepts_both_mpeg_adts_ids_without_changing_codec_identity() {
    let mut data = Muxer::new(&[track("aac", 1, &[0x11, 0x88])])
        .unwrap()
        .tables();
    let mut cc = 0;
    for (id, dts) in [(0, 90000), (8, 91920)] {
        let body = [255, 241 | id, 76, 64, 1, 31, 252, 0];
        data.extend(packets(256, &pes(&body, dts), &mut cc));
    }
    let events = decode(&data, 127);
    assert_eq!(samples(&events).len(), 2);
    let Event::Info(t) = &events[0] else {
        unreachable!()
    };
    assert_eq!(t[0].config, [0x11, 0x88]);
}
#[test]
fn aggregated_audio_uses_total_samples_instead_of_truncated_frame_durations() {
    let mut data = Muxer::new(&[track("mp3", 1, &[])]).unwrap().tables();
    let mut cc = 0;
    data.extend(packets(256, &pes(&MP3.repeat(100), 90000), &mut cc));
    let events = decode(&data, 188 * 64);
    let got = samples(&events);
    assert_eq!(got.len(), 100);
    assert_eq!(got[99].dts, 322751); // 90000 + floor(99*576*90000/22050), not 90000+99*2351.
}
#[test]
fn malformed_pes_headers_and_incomplete_audio_fail_explicitly() {
    for (at, xor) in [
        (0, 1),
        (3, 0x20),
        (6, 0x40),
        (7, 0x80),
        (8, 0xff),
        (9, 1),
        (11, 1),
        (13, 1),
    ] {
        let mut data = Muxer::new(&[track("m2a", 1, &[])]).unwrap().tables();
        let mut cc = 0;
        let mut p = pes(MP2, 90000);
        p[at] ^= xor;
        data.extend(packets(256, &p, &mut cc));
        let mut d = Decoder::default();
        assert!(d.push(&data).is_err(), "PES mutation {at}");
    }
    for body in [&MP2[..100], &[255, 241, 76, 64, 1, 31, 252][..]] {
        let codec = if body.len() == 7 { "aac" } else { "m2a" };
        let cfg = if codec == "aac" {
            vec![0x11, 0x88]
        } else {
            vec![]
        };
        let mut data = Muxer::new(&[track(codec, 1, &cfg)]).unwrap().tables();
        let mut cc = 0;
        data.extend(packets(256, &pes(body, 90000), &mut cc));
        let mut d = Decoder::default();
        assert!(d.push(&data).unwrap().is_empty());
        assert!(d.finish().is_err());
    }
}
#[test]
fn audio_header_changes_and_unsupported_adts_modes_do_not_invent_configuration() {
    let mut data = Muxer::new(&[track("m2a", 1, &[])]).unwrap().tables();
    let mut cc = 0;
    data.extend(packets(256, &pes(MP2, 90000), &mut cc));
    data.extend(packets(256, &pes(MP3, 92160), &mut cc));
    assert!(
        Decoder::default()
            .push(&data)
            .unwrap_err()
            .contains("format changed")
    );
    for (at, xor) in [(1, 1), (2, 0x40), (3, 0x40), (6, 1)] {
        let mut data = Muxer::new(&[track("aac", 1, &[0x11, 0x88])])
            .unwrap()
            .tables();
        let mut cc = 0;
        let mut b = [255, 241, 76, 64, 1, 31, 252, 0];
        b[at] ^= xor;
        data.extend(packets(256, &pes(&b, 90000), &mut cc));
        assert!(Decoder::default().push(&data).is_err());
    }
}
#[test]
fn positive_video_composition_offset_can_cross_the_transport_wrap() {
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let mut f = frame(
        1,
        (1 << 33) - 1800,
        include_bytes!("fixtures/codecs/hevc-00.bin"),
    );
    f.pts_offset = 3600;
    let mut second = f.clone();
    second.dts = (1 << 33) + 1800;
    second.pts_offset = -1800;
    let events = decode(&mux(&[t], &[f, second]), 188);
    let got = samples(&events);
    assert_eq!(
        got.iter()
            .map(|f| (f.dts, f.pts_offset))
            .collect::<Vec<_>>(),
        [((1 << 33) - 1800, 3600), ((1 << 33) + 1800, -1800)]
    );
}
#[test]
fn multiple_pictures_in_one_video_pes_are_rejected() {
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let body = include_bytes!("fixtures/codecs/hevc-00.bin").repeat(2);
    let data = mux(&[t], &[frame(1, 90000, &body)]);
    let mut d = Decoder::default();
    d.push(&data).unwrap();
    assert!(d.finish().unwrap_err().contains("multiple pictures"));
}
#[test]
fn input_chunks_and_unterminated_video_pes_have_hard_bounds() {
    assert!(Decoder::default().push(&vec![0; 188 * 64 + 1]).is_err());
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let mut d = Decoder::default();
    d.push(&Muxer::new(&[t]).unwrap().tables()).unwrap();
    let mut p = pes(&[0; 170], 90000);
    p[3] = 0xe0;
    p[4] = 0;
    p[5] = 0;
    let mut cc = 0;
    d.push(&packets(256, &p, &mut cc)).unwrap();
    let mut rejected = false;
    for _ in 0..1500 {
        let mut continuation = vec![];
        for _ in 0..64 {
            continuation.extend([0x47, 1, 0, 0x10 | cc]);
            continuation.extend([0; 184]);
            cc = (cc + 1) & 15;
        }
        if let Err(error) = d.push(&continuation) {
            assert!(error.contains("PES exceeds bound"));
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    assert!(d.finish().is_err());
}

#[test]
fn annex_b_length_conversion_cannot_expand_a_sample_past_its_bound() {
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let first = mux(
        &[t],
        &[frame(
            1,
            90000,
            include_bytes!("fixtures/codecs/hevc-00.bin"),
        )],
    );
    let mut d = Decoder::default();
    for b in first.chunks(188 * 64) {
        d.push(b).unwrap();
    }
    let mut cc = (first.chunks_exact(188).last().unwrap()[3] + 1) & 15;
    let mut body = vec![0, 0, 1, 0x4e, 1, 5];
    body.resize(16 * 1024 * 1024 - 14 - 31 * 6 - 7, 0x55);
    for _ in 0..31 {
        body.extend([0, 0, 1, 0x4e, 1, 5]);
    }
    body.extend([0, 0, 1, 0x28, 1, 0x80, 1]);
    let mut p = pes(&body, 93600);
    p[3] = 0xe0;
    p[4] = 0;
    p[5] = 0;
    let data = packets(256, &p, &mut cc);
    for b in data.chunks(188 * 64) {
        d.push(b).unwrap();
    }
    match d.finish() {
        Ok(_) => panic!("expanded video sample should fail"),
        Err(e) => assert!(e.contains("sample exceeds bound"), "{e}"),
    }
}

#[test]
fn hevc_pes_requires_a_picture_start_before_any_continuation_slice() {
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let complete = include_bytes!("fixtures/codecs/hevc-00.bin");
    let mut partial = complete.to_vec();
    partial[6] &= 0x7f;
    for body in [partial.clone(), [partial, complete.to_vec()].concat()] {
        let data = mux(std::slice::from_ref(&t), &[frame(1, 90000, &body)]);
        let mut d = Decoder::default();
        for b in data.chunks(188 * 64) {
            d.push(b).unwrap();
        }
        assert!(
            d.finish().is_err(),
            "a missing first slice must not publish a complete key picture"
        );
    }
}
#[test]
fn initial_completion_order_across_wrap_rebases_all_unpublished_audio_clocks() {
    let wrap = 1u64 << 33;
    let input = [
        frame(2, wrap + 191, MP3),
        frame(1, wrap - 2160, MP2),
        frame(2, wrap + 2542, MP3),
        frame(1, wrap, MP2),
    ];
    let events = decode(
        &mux(&[track("m2a", 1, &[]), track("mp3", 2, &[])], &input),
        188,
    );
    let got = samples(&events);
    assert_eq!(got.len(), 4);
    for (a, b) in got.iter().zip(input) {
        assert_eq!((a.dts, a.pts_offset, &a.body), (b.dts, 0, &b.body));
    }
}

#[test]
fn initial_wrap_resolution_retimes_held_video_and_its_next_picture_clock() {
    let wrap = 1u64 << 33;
    let t = track("hevc", 1, include_bytes!("fixtures/codecs/hevc.hvcc"));
    let mut first = frame(
        1,
        wrap + 3600,
        include_bytes!("fixtures/codecs/hevc-00.bin"),
    );
    first.pts_offset = -1800;
    let mut second = first.clone();
    second.dts = wrap + 7200;
    second.pts_offset = 3600;
    let events = decode(
        &mux(
            &[t, track("m2a", 2, &[])],
            &[
                first,
                second,
                frame(2, wrap - 2160, MP2),
                frame(2, wrap, MP2),
            ],
        ),
        257,
    );
    let got = samples(&events);
    assert_eq!(got.len(), 4);
    assert_eq!(
        got.iter()
            .filter(|f| f.track_id == 256)
            .map(|f| (f.dts, f.pts_offset))
            .collect::<Vec<_>>(),
        [(wrap + 3600, -1800), (wrap + 7200, 3600)]
    );
    assert_eq!(
        got.iter()
            .filter(|f| f.track_id == 257)
            .map(|f| f.dts)
            .collect::<Vec<_>>(),
        [wrap - 2160, wrap]
    );
}
#[test]
fn avc_pes_requires_the_first_slice_before_continuation_slices() {
    let t = track(
        "h264",
        1,
        &[
            1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 1,
        ],
    );
    let first = [0, 0, 0, 2, 0x65, 0x80]; // first_mb_in_slice = 0
    let partial = [0, 0, 0, 2, 0x65, 0x40]; // first_mb_in_slice = 1
    for body in [
        partial.to_vec(),
        [partial.to_vec(), first.to_vec()].concat(),
    ] {
        let mut d = Decoder::default();
        d.push(&mux(std::slice::from_ref(&t), &[frame(1, 90000, &body)]))
            .unwrap();
        assert!(d.finish().unwrap_err().contains("partial picture"));
    }
}
#[test]
fn a_published_generation_cannot_reinterpret_a_backwards_clock_as_initial_wrap() {
    let mut d = Decoder::default();
    let mut cc = 0;
    let tables = Muxer::new(&[track("m2a", 1, &[])]).unwrap().tables();
    d.push(&tables).unwrap();
    let first = d.push(&packets(256, &pes(MP2, 1000), &mut cc)).unwrap();
    assert_eq!(samples(&first)[0].dts, 1000);
    assert!(
        d.push(&packets(256, &pes(MP2, (1 << 33) - 1160), &mut cc))
            .is_err()
    );
    assert!(d.finish().is_err());
}
