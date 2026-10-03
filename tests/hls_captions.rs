use flussonix::captions::{Decoder, configuration};
use serde_json::json;
fn p(b: u8) -> u8 {
    b | if b.count_ones() % 2 == 0 { 128 } else { 0 }
}
fn send(d: &mut Decoder, f: u8, a: u8, b: u8, t: u64) {
    d.push(f, [p(a), p(b)], t)
}
fn decoder() -> Decoder {
    Decoder::new(configuration(&json!({"flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"},{"channel":2,"language":"es","name":"Spanish"},{"channel":3,"language":"fr","name":"French"},{"channel":4,"language":"de","name":"German"}]})).unwrap())
}
#[test]
fn pop_on_uses_display_time_and_erase_closes_cue() {
    let mut d = decoder();
    d.observe(90000);
    send(&mut d, 0, 0x14, 0x20, 180000);
    send(&mut d, 0, b'H', b'I', 180000);
    assert!(d.snapshot().is_empty());
    send(&mut d, 0, 0x14, 0x2f, 270000);
    send(&mut d, 0, 0x14, 0x2f, 273000);
    let q = d.snapshot();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].text, "HI");
    assert_eq!(q[0].start, 270000);
    assert_eq!(q[0].end, None);
    send(&mut d, 0, 0x14, 0x2c, 450000);
    let q = d.snapshot();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].end, Some(450000));
}
#[test]
fn four_channels_have_independent_display_and_valid_parity() {
    let mut d = decoder();
    for (f, c, text) in [
        (0, 0x14, b"AA"),
        (0, 0x1c, b"BB"),
        (1, 0x14, b"CC"),
        (1, 0x1c, b"DD"),
    ] {
        send(&mut d, f, c, 0x29, 90000);
        send(&mut d, f, text[0], text[1], 180000);
    }
    d.push(0, [0x41, 0x41], 200000);
    let q = d.snapshot();
    for (c, text) in [(1, "AA"), (2, "BB"), (3, "CC"), (4, "DD")] {
        assert!(
            q.iter()
                .any(|v| v.channel == c && v.text == text && v.end.is_none())
        );
    }
}
#[test]
fn roll_up_carriage_return_keeps_last_two_rows() {
    let mut d = decoder();
    send(&mut d, 0, 0x14, 0x25, 0);
    send(&mut d, 0, b'O', b'N', 90000);
    send(&mut d, 0, 0x14, 0x2d, 180000);
    send(&mut d, 0, b'T', b'W', 270000);
    assert!(
        d.snapshot()
            .iter()
            .any(|c| c.end.is_none() && c.text == "ON\nTW")
    );
    send(&mut d, 0, 0x14, 0x2d, 360000);
    send(&mut d, 0, b'T', b'H', 450000);
    assert!(
        d.snapshot()
            .iter()
            .any(|c| c.end.is_none() && c.text == "TW\nTH")
    );
}
#[test]
fn special_extended_characters_and_reset_are_visible() {
    let mut d = decoder();
    send(&mut d, 0, 0x14, 0x29, 0);
    send(&mut d, 0, b'e', b' ', 90000);
    send(&mut d, 0, 0x11, 0x37, 180000);
    assert!(
        d.snapshot()
            .iter()
            .any(|c| c.end.is_none() && c.text == "e ♪")
    );
    d.reset(270000);
    assert!(d.snapshot().iter().all(|c| c.end.is_some()));
}
#[test]
fn policy_is_strict_and_changes_media_generation() {
    assert!(configuration(&json!({})).unwrap().is_empty());
    for value in [
        json!("convert"),
        json!([{"channel":5,"language":"en","name":"x"}]),
        json!([{"channel":1,"language":"en\"\n","name":"x"}]),
        json!([{"channel":1,"language":"en","name":"x","service":1}]),
        json!([{"channel":1,"language":"en","name":"x"},{"channel":1,"language":"en","name":"y"}]),
    ] {
        assert!(configuration(&json!({"flussonix_hls_captions":value})).is_err());
    }
    let a = json!({"inputs":[{"url":"testsrc://"}]});
    let mut b = a.clone();
    b["flussonix_hls_captions"] = json!([{"channel":1,"language":"en","name":"English"}]);
    assert_ne!(
        flussonix::media::media_signature(&a),
        flussonix::media::media_signature(&b)
    );
}
fn wire_caption(
    hevc: bool,
    pairs: &[(u8, u8, u8)],
    pts: u64,
    mux: &mut flussonix::worker_ts::Muxer,
) -> Vec<u8> {
    let mut data = b"\xb5\x00\x31GA94\x03".to_vec();
    data.extend([0x40 | pairs.len() as u8, 0xff]);
    for (f, a, b) in pairs {
        data.extend([0xfc | f, p(*a), p(*b)]);
    }
    data.push(255);
    let mut nal = if hevc { vec![0x4e, 1] } else { vec![6] };
    nal.extend([4, data.len() as u8]);
    nal.extend(data);
    nal.push(128);
    let mut body = (nal.len() as u32).to_be_bytes().to_vec();
    body.extend(nal);
    body.extend([0, 0, 0, 2, if hevc { 0x26 } else { 0x65 }, 1]);
    mux.frame(&flussonix::m4f::Frame {
        track_id: 1,
        dts: pts,
        pts_offset: 0,
        key: true,
        body,
    })
    .unwrap()
}
#[test]
fn transport_decodes_both_video_families_and_pts_wrap() {
    for hevc in [false, true] {
        let track = flussonix::m4s::Track {
            id: 1,
            codec: if hevc { "hevc" } else { "h264" }.into(),
            config: if hevc {
                include_bytes!("fixtures/codecs/hevc.hvcc").to_vec()
            } else {
                vec![
                    1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
                ]
            },
        };
        let mut mux = flussonix::worker_ts::Muxer::new(&[track]).unwrap();
        let mut d = decoder();
        let mut t = flussonix::caption_transport::Transport::default();
        let base = (1u64 << 33) - 90000;
        let mut bytes = mux.tables();
        bytes.extend(wire_caption(
            hevc,
            &[(0, 0x14, 0x29), (0, b'O', b'K')],
            base,
            &mut mux,
        ));
        bytes.extend(wire_caption(
            hevc,
            &[(0, 0x14, 0x2c)],
            base + 180000,
            &mut mux,
        ));
        bytes.extend(wire_caption(hevc, &[], base + 360000, &mut mux));
        for chunk in bytes.chunks(37) {
            t.push(chunk, &mut d)
        }
        assert_eq!(d.first_pts, Some(base));
        let q = d.snapshot();
        assert_eq!(q[0].text, "OK");
        assert_eq!(q[0].start, base);
        assert_eq!(q[0].end, Some(base + 180000));
    }
}
#[test]
fn cue_history_is_bounded_and_old_state_expires() {
    let mut d = decoder();
    send(&mut d, 0, 0x14, 0x29, 0);
    for i in 0..5000 {
        send(&mut d, 0, b'A', b'B', i * 90000);
        send(&mut d, 0, 0x14, 0x2c, i * 90000 + 45000);
    }
    assert!(d.snapshot().len() <= 241);
}
#[test]
fn template_configuration_can_be_disabled_and_inherited_again() {
    let temp = tempfile::tempdir().unwrap();
    let store = flussonix::config::ConfigStore::open(temp.path().join("config.json")).unwrap();
    let rows = json!([{"channel":1,"language":"en","name":"English"}]);
    store
        .put("templates", "cc", json!({"flussonix_hls_captions":rows}))
        .unwrap();
    store
        .put(
            "streams",
            "group/channel",
            json!({"template":"cc","inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    assert_eq!(
        configuration(&store.effective("group/channel").unwrap())
            .unwrap()
            .len(),
        1
    );
    store
        .put(
            "streams",
            "group/channel",
            json!({"flussonix_hls_captions":[]}),
        )
        .unwrap();
    assert!(
        configuration(&store.effective("group/channel").unwrap())
            .unwrap()
            .is_empty()
    );
    store
        .put(
            "streams",
            "group/channel",
            json!({"flussonix_hls_captions":null}),
        )
        .unwrap();
    assert_eq!(
        configuration(&store.effective("group/channel").unwrap())
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn transport_sync_loss_closes_stale_caption() {
    let mut d = decoder();
    send(&mut d, 0, 0x14, 0x29, 90000);
    send(&mut d, 0, b'H', b'I', 180000);
    let mut t = flussonix::caption_transport::Transport::default();
    t.push(&[0; 188], &mut d);
    assert!(d.error.is_some());
    assert!(d.snapshot().iter().all(|c| c.end.is_some()));
}
