use flussonix::captions::{Decoder, Service, configuration};
use serde_json::json;
fn digital(numbers: &[u8]) -> Decoder {
    Decoder::new(
        numbers
            .iter()
            .map(|n| Service {
                channel: 64 + n,
                language: "en".into(),
                name: format!("Service {n}"),
            })
            .collect(),
    )
}
// Independently constructed DTVCC service blocks, padded to an odd payload.
fn packet(d: &mut Decoder, seq: u8, blocks: &[(u8, &[u8])], pts: u64) {
    let mut b = vec![];
    for (s, bytes) in blocks {
        assert!(bytes.len() <= 31);
        b.push((s.min(&7) << 5) | bytes.len() as u8);
        if *s >= 7 {
            b.push(*s);
        }
        b.extend_from_slice(bytes);
    }
    if b.len() % 2 == 0 {
        b.push(0);
    }
    assert!(b.len() < 128);
    d.push(3, [(seq << 6) | b.len().div_ceil(2) as u8, b[0]], pts);
    for p in b[1..].chunks_exact(2) {
        d.push(2, [p[0], p[1]], pts);
    }
}
fn open(d: &Decoder, id: u8) -> Option<String> {
    d.snapshot()
        .into_iter()
        .find(|c| c.channel == id && c.end.is_none())
        .map(|c| c.text)
}
#[test]
fn accepts_digital_selector_without_colliding_with_cc1_or_leaking_internal_id() {
    let c=configuration(&json!({"flussonix_hls_captions":[{"channel":1,"language":"en","name":"Analog"},{"service":1,"language":"es","name":"Digital"}]})).unwrap();
    assert_eq!(
        serde_json::to_value(c).unwrap(),
        json!([{"channel":1,"language":"en","name":"Analog"},{"service":1,"language":"es","name":"Digital"}])
    );
    for row in [
        json!({"service":0,"language":"en","name":"x"}),
        json!({"service":64,"language":"en","name":"x"}),
        json!({"channel":1,"service":1,"language":"en","name":"x"}),
        json!({"language":"en","name":"x"}),
        json!({"service":1,"language":"en","name":"x","extra":true}),
    ] {
        assert!(configuration(&json!({"flussonix_hls_captions":[row]})).is_err());
    }
}
#[test]
fn digital_pop_on_display_and_hide_use_source_time() {
    let mut d = digital(&[1]);
    // DF0 invisible, two rows and 32 columns, then text and DSW0.
    packet(
        &mut d,
        0,
        &[(
            1,
            &[0x98, 0x18, 70, 80, 1, 31, 0, b'U', b'S', b'A', 0x89, 1],
        )],
        90000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("USA"));
    packet(&mut d, 1, &[(1, &[0x8a, 1])], 270000);
    let q = d.snapshot();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].start, 90000);
    assert_eq!(q[0].end, Some(270000));
}
#[test]
fn extended_services_and_visible_windows_are_isolated_and_anchor_sorted() {
    let mut d = digital(&[1, 7, 63]);
    packet(
        &mut d,
        0,
        &[
            (1, &[0x98, 0x20, 80, 0, 1, 20, 0, b'L']),
            (7, &[0x98, 0x20, 0, 0, 0, 20, 0, 0xd1]),
            (63, &[0x98, 0x20, 0, 0, 0, 20, 0, 0x18, 0x03, 0xa9]),
        ],
        90000,
    );
    assert_eq!(open(&d, 71).as_deref(), Some("Ñ"));
    assert_eq!(open(&d, 127).as_deref(), Some("Ω"));
    packet(
        &mut d,
        1,
        &[(1, &[0x99, 0x20, 0, 0, 0, 20, 0, b'T'])],
        180000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("T\nL"));
    packet(&mut d, 2, &[(1, &[0x8b, 2])], 270000);
    assert_eq!(open(&d, 65).as_deref(), Some("L"));
    packet(&mut d, 3, &[(1, &[0x8c, 1])], 360000);
    assert_eq!(open(&d, 65), None);
}
#[test]
fn delay_uses_silent_video_clock_and_cancel_reset_ignore_parameter_bytes() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(1, &[0x98, 0, 0, 0, 0, 20, 0, b'A', 0x8d, 10, 0x89, 1])],
        90000,
    );
    assert_eq!(open(&d, 65), None);
    d.observe(179999);
    assert_eq!(open(&d, 65), None);
    d.observe(200000);
    assert_eq!(open(&d, 65).as_deref(), Some("A"));
    assert_eq!(d.snapshot().last().unwrap().start, 180000);
    packet(
        &mut d,
        1,
        &[(1, &[0x8d, 100, 0x91, 0x8e, 0x8f, 0, b'B'])],
        270000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("A"));
    packet(&mut d, 2, &[(1, &[0x8e])], 360000);
    assert_eq!(open(&d, 65).as_deref(), Some("AB"));
    packet(&mut d, 3, &[(1, &[0x8d, 20, b'C', 0x8f])], 450000);
    d.observe(900000);
    assert_eq!(open(&d, 65), None);
}
#[test]
fn split_tokens_and_reserved_parameters_do_not_become_text_or_controls() {
    let mut d = digital(&[1]);
    packet(&mut d, 0, &[(1, &[0x98, 0x20, 0])], 90000);
    packet(
        &mut d,
        1,
        &[(
            1,
            &[
                0, 0, 20, 0, b'A', 0x10, 0x18, 0x8f, 0x89, 0xff, 0x10, 0x90, 3, b'Z', 0x8f, 0x89,
                b'B', 0x18, 0x03,
            ],
        )],
        180000,
    );
    packet(&mut d, 2, &[(1, &[0xa9, 0x10, 0x25, 0x7f])], 270000);
    assert_eq!(open(&d, 65).as_deref(), Some("ABΩ…♪"));
}
#[test]
fn duplicate_toggle_is_suppressed_and_sequence_gaps_clear_only_digital() {
    let mut d = digital(&[1]);
    let parity = |b: u8| b | if b.count_ones() % 2 == 0 { 128 } else { 0 };
    d.push(0, [parity(0x14), parity(0x29)], 0);
    d.push(0, [parity(b'C'), parity(b'C')], 0);
    packet(
        &mut d,
        0,
        &[(1, &[0x98, 0x20, 0, 0, 0, 20, 0, b'A'])],
        90000,
    );
    packet(&mut d, 1, &[(1, &[0x8b, 1])], 180000);
    packet(&mut d, 1, &[(1, &[0x8b, 1])], 190000);
    assert_eq!(open(&d, 65), None);
    packet(&mut d, 2, &[(1, &[0x89, 1])], 270000);
    assert!(open(&d, 65).is_some());
    packet(&mut d, 0, &[(1, b"X")], 360000);
    assert_eq!(open(&d, 65), None);
    assert_eq!(open(&d, 1).as_deref(), Some("CC"));
    assert_eq!(d.error, Some("caption_708_packet_gap"));
}
#[test]
fn malformed_blocks_and_stalled_packets_clear_stale_text_and_recover() {
    let mut d = digital(&[1]);
    packet(&mut d, 0, &[(1, &[0x98, 0x20, 0, 0, 0, 20, 0, b'A'])], 0);
    // One-word packet declaring three service bytes but carrying none.
    d.push(3, [0x41, 0x23], 90000);
    assert_eq!(open(&d, 65), None);
    packet(
        &mut d,
        0,
        &[(1, &[0x98, 0x20, 0, 0, 0, 20, 0, b'B'])],
        180000,
    );
    d.push(3, [0x7f, 0], 270000);
    d.observe(720001);
    assert_eq!(open(&d, 65), None);
    assert_eq!(d.error, Some("caption_708_packet_timeout"));
    packet(
        &mut d,
        2,
        &[(1, &[0x98, 0x20, 0, 0, 0, 20, 0, b'C'])],
        810000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("C"));
}
#[test]
fn source_delay_queue_overflow_is_bounded_and_clears_visible_state() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(1, &[0x98, 0x20, 0, 0, 0, 20, 0, b'A', 0x8d, 255])],
        0,
    );
    for n in 1..=18 {
        packet(&mut d, n & 3, &[(1, &[b'B'; 31])], n as u64 * 900);
    }
    assert_eq!(d.error, Some("caption_708_command_limit"));
    assert_eq!(open(&d, 65), None);
}
#[test]
fn carriage_return_backspace_and_pen_location_preserve_declared_grid() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(
            1,
            &[
                0x98, 0x20, 0, 0, 1, 3, 0, b'A', b'B', 0x08, b'C', 0x0d, b'D', 0x0d, b'E',
            ],
        )],
        90000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("D\nE"));
    packet(&mut d, 1, &[(1, &[0x92, 0, 2, b'F', 0x0e, b'G'])], 180000);
    assert_eq!(open(&d, 65).as_deref(), Some("G\nE"));
}
#[test]
fn digital_transport_preserves_raw_bytes_for_h264_hevc_and_clock_wrap() {
    use flussonix::{caption_transport::Transport, m4f::Frame, m4s::Track, worker_ts::Muxer};
    for hevc in [false, true] {
        let track = Track {
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
        let mut mux = Muxer::new(&[track]).unwrap();
        let mut bytes = mux.tables();
        let base = (1u64 << 33) - 90000;
        // DF0 visible + raw ASCII, a one-byte padded service block.
        let data = [0x29, 0x98, 0x20, 0, 0, 0, 20, 0, b'O', b'K', 0];
        let mut pairs = vec![(3, 6, data[0])];
        for p in data[1..].chunks_exact(2) {
            pairs.push((2, p[0], p[1]));
        }
        for (pts, pairs) in [
            (base, pairs),
            (base + 180000, vec![(3, 0x42, 0x22), (2, 0x8a, 1)]),
            (base + 360000, vec![]),
        ] {
            let mut payload = b"\xb5\x00\x31GA94\x03".to_vec();
            payload.extend([0x40 | pairs.len() as u8, 255]);
            for (f, a, b) in pairs {
                payload.extend([0xfc | f, a, b]);
            }
            payload.push(255);
            let mut rbsp = vec![4, payload.len() as u8];
            rbsp.extend(payload);
            rbsp.push(128);
            let mut nal = if hevc { vec![0x4e, 1] } else { vec![6] };
            let mut zeros = 0;
            for b in rbsp {
                if zeros >= 2 && b <= 3 {
                    nal.push(3);
                    zeros = 0;
                }
                nal.push(b);
                zeros = if b == 0 { zeros + 1 } else { 0 };
            }
            let mut body = (nal.len() as u32).to_be_bytes().to_vec();
            body.extend(nal);
            body.extend([0, 0, 0, 2, if hevc { 0x26 } else { 0x65 }, 1]);
            bytes.extend(
                mux.frame(&Frame {
                    track_id: 1,
                    dts: pts,
                    pts_offset: 0,
                    key: true,
                    body,
                })
                .unwrap(),
            );
        }
        let mut d = digital(&[1]);
        let mut t = Transport::default();
        for b in bytes.chunks(37) {
            t.push(b, &mut d);
        }
        let q = d.snapshot();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].text, "OK");
        assert_eq!(q[0].start, base);
        assert_eq!(q[0].end, Some(base + 180000));
    }
}
#[test]
fn word_wrap_moves_the_whole_trailing_word_to_next_row() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(
            1,
            &[
                0x98, 0x20, 0, 0, 1, 3, 0, 0x97, 0, 0, 0x4c, 0, b'A', b'B', b' ', b'C', b'D',
            ],
        )],
        90000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("AB\nCD"));
}
#[test]
fn clearing_memory_and_redefining_pen_style_keep_existing_pen_location() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(1, &[0x98, 0x20, 0, 0, 0, 10, 0, b'A', b'B'])],
        0,
    );
    packet(&mut d, 1, &[(1, &[0x88, 1, b'C'])], 90000);
    assert_eq!(open(&d, 65).as_deref(), Some("  C"));
    packet(
        &mut d,
        2,
        &[(1, &[0x98, 0x20, 0, 0, 0, 10, 1, b'D'])],
        180000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("  CD"));
}
#[test]
fn zero_packet_size_means_full_128_bytes_and_waits_for_last_pair() {
    let mut d = digital(&[1]);
    let mut bytes = vec![0x3f, 0x98, 0x20, 0, 0, 0, 41, 0];
    bytes.extend([b'A'; 24]);
    bytes.resize(127, 0);
    d.push(3, [0, bytes[0]], 0);
    for p in bytes[1..125].chunks_exact(2) {
        d.push(2, [p[0], p[1]], 90000);
    }
    assert_eq!(open(&d, 65), None);
    d.push(2, [bytes[125], bytes[126]], 180000);
    assert_eq!(open(&d, 65).as_deref(), Some("AAAAAAAAAAAAAAAAAAAAAAAA"));
    assert_eq!(d.error, None);
}
#[test]
fn unselected_command_bytes_are_not_interpreted_and_vertical_style_seven_advances_rows() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[
            (63, &[0x8f, 0x10, 0x98]),
            (1, &[0x98, 0x20, 0, 0, 2, 2, 0x38, b'A', b'B', b'C']),
        ],
        90000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("A\nB\nC"));
    packet(&mut d, 1, &[(1, &[0x0d, b'D'])], 180000);
    assert_eq!(open(&d, 65).as_deref(), Some("AD\nB\nC"));
}
fn reordered_delay(control: u8) {
    use flussonix::{caption_transport::Transport, m4f::Frame, m4s::Track, worker_ts::Muxer};
    let track = Track {
        id: 1,
        codec: "h264".into(),
        config: vec![
            1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
        ],
    };
    let mut mux = Muxer::new(&[track]).unwrap();
    let mut t = Transport::default();
    let mut d = digital(&[1]);
    t.push(&mux.tables(), &mut d);
    let commands = [0x98, 0, 0, 0, 0, 20, 0, b'A', 0x8d, 4, 0x89, 1];
    for (index, (pts, dts)) in [
        (18000, 0),
        (45000, 9000),
        (27000, 18000),
        (36000, 27000),
        (72000, 36000),
        (54000, 45000),
        (63000, 54000),
    ]
    .into_iter()
    .enumerate()
    {
        let cmd: &[u8] = if index == 0 {
            &commands
        } else if index == 1 {
            std::slice::from_ref(&control)
        } else {
            &[]
        };
        let mut payload = b"\xb5\x00\x31GA94\x03".to_vec();
        let mut triples = vec![];
        if !cmd.is_empty() {
            let mut block = vec![0x20 | cmd.len() as u8];
            block.extend(cmd);
            if block.len() % 2 == 0 {
                block.push(0);
            }
            triples.push((
                3,
                ((index as u8) << 6) | block.len().div_ceil(2) as u8,
                block[0],
            ));
            for p in block[1..].chunks_exact(2) {
                triples.push((2, p[0], p[1]));
            }
        }
        payload.extend([0x40 | triples.len() as u8, 255]);
        for (kind, a, b) in triples {
            payload.extend([0xfc | kind, a, b]);
        }
        payload.push(255);
        let mut rbsp = vec![4, payload.len() as u8];
        rbsp.extend(payload);
        rbsp.push(128);
        let mut nal = vec![6];
        let mut zeros = 0;
        for b in rbsp {
            if zeros >= 2 && b <= 3 {
                nal.push(3);
                zeros = 0;
            }
            nal.push(b);
            zeros = if b == 0 { zeros + 1 } else { 0 };
        }
        let mut body = (nal.len() as u32).to_be_bytes().to_vec();
        body.extend(nal);
        body.extend([0, 0, 0, 2, 0x65, 1]);
        t.push(
            &mux.frame(&Frame {
                track_id: 1,
                dts,
                pts_offset: pts as i64 - dts as i64,
                key: true,
                body,
            })
            .unwrap(),
            &mut d,
        );
        if index <= 4 {
            assert_eq!(
                open(&d, 65),
                None,
                "caption cannot display before the safe source frontier reaches its cancel/reset command"
            );
        }
    }
    assert_eq!(d.first_pts, Some(18000));
    assert_eq!(d.latest_pts, 54000);
    if control == 0x8e || control == 0 {
        let cue = d.snapshot().into_iter().find(|c| c.end.is_none()).unwrap();
        assert_eq!(cue.text, "A");
        assert_eq!(cue.start, if control == 0 { 54000 } else { 45000 });
    } else {
        assert!(d.snapshot().is_empty());
    }
}
#[test]
fn reordered_dlc_runs_before_delay_deadline_despite_future_reference_pts() {
    reordered_delay(0x8e);
}
#[test]
fn reordered_rst_discards_delayed_display_without_a_premature_flash() {
    reordered_delay(0x8f);
}
#[test]
fn backspace_at_row_boundary_replaces_the_previous_rows_last_character() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(
            1,
            &[
                0x98, 0x20, 0, 0, 1, 3, 0, b'A', b'B', b'C', b'D', 0x0d, 0x08, b'Z',
            ],
        )],
        90000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("ABCZ"));
}
#[test]
fn right_to_left_backspace_crosses_to_previous_row_without_scrolling() {
    let mut d = digital(&[1]);
    packet(
        &mut d,
        0,
        &[(
            1,
            &[
                0x98, 0x20, 0, 0, 1, 3, 0, 0x97, 0, 0, 0x1c, 0, 0x92, 0, 3, b'A', b'B', b'C', b'D',
                0x0d, 0x08, b'Z',
            ],
        )],
        90000,
    );
    assert_eq!(open(&d, 65).as_deref(), Some("ZCBA"));
}

#[test]
fn delayed_display_advances_during_silent_reordered_video() {
    reordered_delay(0);
}
#[test]
fn vertical_backspace_crosses_columns_without_scrolling_or_leaving_the_grid() {
    for (direction, start_row, want) in [(0x24, 0, "A\nZ"), (0x34, 1, "Z\nA")] {
        let mut d = digital(&[1]);
        packet(
            &mut d,
            0,
            &[(
                1,
                &[
                    0x98, 0x20, 0, 0, 1, 1, 0, 0x97, 0, 0, direction, 0, 0x92, start_row, 0, b'A',
                    b'B', 0x0d, 0x08, b'Z',
                ],
            )],
            90000,
        );
        assert_eq!(open(&d, 65).as_deref(), Some(want));
    }
}
