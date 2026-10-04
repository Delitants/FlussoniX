use flussonix::{m4f::Frame, m4s::Track};
pub fn fixture(bad: bool) -> (Vec<Track>, Vec<Frame>) {
    let tracks = vec![
        Track {
            id: 1,
            codec: "hevc".into(),
            config: include_bytes!("../fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        },
        Track {
            id: u32::MAX,
            codec: "subtitle".into(),
            config: vec![],
        },
    ];
    let timing: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("../fixtures/codecs/hevc-timing.json")).unwrap();
    let mut frames = vec![];
    for cycle in 0..30u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            frames.push(Frame {
                track_id: 1,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
            });
        }
    }
    for (id, text) in [
        (7, "AMERICA <HELLO>\r\n\r\nsecond line"),
        (u32::MAX, "EUROPE GRÜSSE"),
    ] {
        frames.push(Frame {
            track_id: id,
            dts: 270000,
            pts_offset: 180000,
            key: true,
            body: if bad && id == 7 {
                vec![0xff]
            } else {
                text.as_bytes().to_vec()
            },
        });
        // Early clear must truncate the explicit four-second cue at three seconds.
        frames.push(Frame {
            track_id: id,
            dts: 360000,
            pts_offset: 0,
            key: true,
            body: vec![],
        });
    }
    frames.sort_by_key(|f| f.dts);
    (tracks, frames)
}
