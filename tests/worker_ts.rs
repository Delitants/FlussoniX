use flussonix::{m4f::Frame, m4s::Track, worker_ts::Muxer};
use serde_json::Value;
use std::process::Command;
fn audio(codec: &str, id: u32) -> Track {
    Track {
        id,
        codec: codec.into(),
        config: vec![],
    }
}
fn video() -> Track {
    Track {
        id: 1,
        codec: "hevc".into(),
        config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
    }
}
fn mp2() -> Vec<u8> {
    include_bytes!("fixtures/codecs/mp2.bin").to_vec()
}
fn frame(id: u32, dts: u64, body: Vec<u8>) -> Frame {
    Frame {
        track_id: id,
        dts,
        pts_offset: 0,
        key: true,
        body,
    }
}
fn owned() -> (Vec<Track>, Vec<Frame>) {
    let timing: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    let mut frames = vec![];
    for (i, t) in timing.iter().enumerate() {
        let d = t["dts"].as_i64().unwrap();
        let p = t["pts"].as_i64().unwrap();
        frames.push(Frame {
            track_id: 1,
            dts: (90000 + (d + 1024) * 90000 / 12800) as u64,
            pts_offset: (p - d) * 90000 / 12800,
            key: t["flags"].as_str().unwrap().contains('K'),
            body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
        });
    }
    for i in 0..20 {
        frames.push(frame(2, 90000 + i * 2160, mp2()));
        frames.push(frame(
            3,
            90000 + i * 2351,
            include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        ));
    }
    frames.sort_by_key(|f| f.dts);
    (vec![video(), audio("m2a", 2), audio("mp3", 3)], frames)
}
fn write_mux(tracks: &[Track], frames: &[Frame]) -> Vec<u8> {
    let mut mux = Muxer::new(tracks).unwrap();
    let mut out = mux.tables();
    for f in frames {
        out.extend(mux.frame(f).unwrap());
    }
    out
}
#[test]
fn owned_hevc_and_multiple_mpeg_audio_decode_with_exact_packet_times() {
    let (tracks, frames) = owned();
    let data = write_mux(&tracks, &frames);
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("owned.ts");
    std::fs::write(&file, data).unwrap();
    let p = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_streams",
            "-show_packets",
            "-of",
            "json",
        ])
        .arg(&file)
        .output()
        .unwrap();
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    let info: Value = serde_json::from_slice(&p.stdout).unwrap();
    let streams = info["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 3);
    assert_eq!(streams[0]["codec_name"], "hevc");
    assert_eq!(streams[1]["codec_name"], "mp2");
    assert_eq!(streams[2]["codec_name"], "mp3");
    let video_packets: Vec<_> = info["packets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["stream_index"] == 0)
        .collect();
    let expected: Vec<_> = frames.iter().filter(|f| f.track_id == 1).collect();
    assert_eq!(video_packets.len(), expected.len());
    for (p, f) in video_packets.iter().zip(expected) {
        assert_eq!(p["dts"].as_u64(), Some(f.dts));
        assert_eq!(p["pts"].as_i64(), Some(f.dts as i64 + f.pts_offset));
    }
    let decoded = Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&file)
        .args(["-map", "0", "-f", "null", "-"])
        .output()
        .unwrap();
    assert!(
        decoded.status.success(),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    assert!(
        decoded.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
}
fn crc(data: &[u8]) -> u32 {
    let mut c = 0xffff_ffffu32;
    for b in data {
        c ^= u32::from(*b) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x8000_0000 != 0 { 0x04c1_1db7 } else { 0 };
        }
    }
    c
}
#[test]
fn tables_crc_continuity_and_pcr_remain_valid() {
    let (tracks, frames) = owned();
    let data = write_mux(&tracks, &frames);
    assert_eq!(data.len() % 188, 0);
    let mut counters = std::collections::HashMap::new();
    let mut tables = 0;
    let mut pcr = 0;
    for p in data.chunks_exact(188) {
        assert_eq!(p[0], 0x47);
        let pid = (u16::from(p[1] & 31) << 8) | u16::from(p[2]);
        let cc = p[3] & 15;
        if let Some(old) = counters.insert(pid, cc) {
            assert_eq!(cc, (old + 1) & 15);
        }
        let at = if p[3] & 0x20 != 0 {
            5 + usize::from(p[4])
        } else {
            4
        };
        if pid == 0 || pid == 4096 {
            let start = at + 1 + usize::from(p[at]);
            let n = (usize::from(p[start + 1] & 15) << 8) | usize::from(p[start + 2]);
            assert_eq!(crc(&p[start..start + n + 3]), 0);
            tables += 1;
        } else if p[3] & 0x20 != 0 && p[4] > 0 && p[5] & 0x10 != 0 {
            assert_eq!(pid, 256);
            pcr += 1;
        }
    }
    assert!(tables >= 4);
    assert_eq!(pcr, 12);
}
#[test]
fn malformed_frames_fail_without_mutating_continuity_or_timing() {
    let tracks = vec![audio("m2a", 2)];
    let mut mux = Muxer::new(&tracks).unwrap();
    let mut expected = Muxer::new(&tracks).unwrap();
    assert_eq!(mux.tables(), expected.tables());
    let f = frame(2, 90000, mp2());
    assert_eq!(mux.frame(&f).unwrap(), expected.frame(&f).unwrap());
    for bad in [
        frame(99, 92000, mp2()),
        frame(2, 89999, mp2()),
        frame(2, 92000, vec![0; 4]),
        Frame {
            pts_offset: -100000,
            ..frame(2, 92000, mp2())
        },
        frame(2, 92000, vec![0; 16 * 1024 * 1024 + 1]),
    ] {
        assert!(mux.frame(&bad).is_err());
    }
    let next = frame(2, 92160, mp2());
    assert_eq!(mux.frame(&next).unwrap(), expected.frame(&next).unwrap());
}
#[test]
fn timestamps_wrap_at_33_bits_and_audio_only_has_pcr() {
    let data = write_mux(&[audio("m2a", 2)], &[frame(2, (1 << 33) + 90000, mp2())]);
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("wrap.ts");
    std::fs::write(&f, &data).unwrap();
    let r = Command::new("ffprobe")
        .args(["-v", "error", "-show_packets", "-of", "json"])
        .arg(f)
        .output()
        .unwrap();
    let info: Value = serde_json::from_slice(&r.stdout).unwrap();
    assert_eq!(info["packets"][0]["pts"], 90000);
    assert_eq!(info["packets"][0]["dts"], 90000);
    assert!(
        data.chunks_exact(188)
            .any(|p| p[3] & 0x20 != 0 && p[4] >= 7 && p[5] & 0x10 != 0)
    );
}
#[test]
fn unsupported_configuration_and_adts_size_fail_explicitly() {
    for cfg in [
        vec![0],
        vec![0x2a, 0x10],
        vec![0x17, 0x80],
        vec![0x12, 0x00],
    ] {
        assert!(
            Muxer::new(&[Track {
                id: 1,
                codec: "aac".into(),
                config: cfg
            }])
            .is_err()
        );
    }
    assert!(
        Muxer::new(&[Track {
            config: vec![],
            ..video()
        }])
        .is_err()
    );
    assert!(Muxer::new(&[audio("m2a", 1), audio("mp3", 1)]).is_err());
    let mut mux = Muxer::new(&[Track {
        id: 1,
        codec: "aac".into(),
        config: vec![0x12, 0x10],
    }])
    .unwrap();
    assert!(mux.frame(&frame(1, 0, vec![0; 8192])).is_err());
}

#[test]
fn aac_lc_accepts_explicit_disabled_sbr_extension_and_rejects_other_tails() {
    let track = Track {
        id: 1,
        codec: "aac".into(),
        config: vec![0x11, 0x90, 0x56, 0xe5, 0],
    };
    assert!(Muxer::new(std::slice::from_ref(&track)).is_ok());
    for tail in [vec![0x56, 0xe5, 0x80], vec![0, 0, 0], vec![0x56, 0xe5]] {
        let mut bad = track.clone();
        bad.config = vec![0x11, 0x90];
        bad.config.extend(tail);
        assert!(Muxer::new(&[bad]).is_err());
    }
}

#[test]
fn negative_composition_offset_is_explicitly_preserved_in_pes() {
    let sample = Frame {
        pts_offset: -3600,
        ..frame(
            1,
            90000,
            include_bytes!("fixtures/codecs/hevc-00.bin").to_vec(),
        )
    };
    let bytes = write_mux(&[video()], &[sample]);
    let packet = bytes
        .chunks_exact(188)
        .find(|p| p[1] & 0x40 != 0 && p[1] & 31 == 1 && p[2] == 0)
        .unwrap();
    let start = if packet[3] & 0x20 != 0 {
        5 + usize::from(packet[4])
    } else {
        4
    };
    let pes = &packet[start..];
    assert_eq!(&pes[..4], &[0, 0, 1, 0xe0]);
    let decode = |b: &[u8]| {
        (u64::from((b[0] >> 1) & 7) << 30)
            | (u64::from(b[1]) << 22)
            | (u64::from(b[2] >> 1) << 15)
            | (u64::from(b[3]) << 7)
            | u64::from(b[4] >> 1)
    };
    assert_eq!(decode(&pes[9..14]), 86400);
    assert_eq!(decode(&pes[14..19]), 90000);
}
