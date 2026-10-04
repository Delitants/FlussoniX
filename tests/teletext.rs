#[path = "support/teletext_fixture.rs"]
mod f;
use flussonix::{
    caption_transport::Transport,
    captions::{Decoder, configuration},
};
use serde_json::json;
fn decoder(pages: &[u16]) -> Decoder {
    Decoder::new(configuration(&json!({"flussonix_hls_captions":pages.iter().map(|p|json!({"teletext_page":p,"language":"de","name":format!("Page {p}")})).collect::<Vec<_>>()})).expect("teletext page configuration"))
}
#[test]
fn teletext_selector_round_trips_and_rejects_multiple_selectors() {
    let cfg = json!({"flussonix_hls_captions":[{"teletext_page":888,"language":"de","name":"German"},{"channel":1,"language":"en","name":"English"},{"service":63,"language":"es","name":"Spanish"}]});
    let s = configuration(&cfg).expect("valid mixed subtitle formats");
    assert_eq!(
        serde_json::to_value(&s).unwrap(),
        cfg["flussonix_hls_captions"]
    );
    assert_eq!(s[0].key(), "ttx888");
    for p in [99, 900] {
        assert!(configuration(&json!({"flussonix_hls_captions":[{"teletext_page":p,"language":"de","name":"Bad"}]})).is_err());
    }
    assert!(configuration(&json!({"flussonix_hls_captions":[{"teletext_page":888,"channel":1,"language":"de","name":"Bad"}]})).is_err());
}
#[test]
fn boxed_page_has_source_header_timestamp_and_closes_on_blank_page() {
    let mut d = decoder(&[888]);
    let mut t = Transport::default();
    let (mut v, mut s) = (0, 0);
    t.push(&f::tables(&[(0x121, 888)], 0), &mut d);
    t.push(&f::video(0, &mut v), &mut d);
    t.push(
        &f::pes(
            0x121,
            90000,
            &f::body(&[
                f::header(888, true, 0, false, false, 0),
                f::row(888, 1, b"EUROPE TELETEXT"),
            ]),
            &mut s,
        ),
        &mut d,
    );
    t.push(&f::video(108000, &mut v), &mut d);
    let cues = d.snapshot();
    assert_eq!(cues.len(), 1);
    assert_eq!(cues[0].text, "EUROPE TELETEXT");
    assert_eq!(cues[0].start, 90000);
    assert_eq!(cues[0].end, None);
    t.push(
        &f::pes(
            0x121,
            180000,
            &f::body(&[f::header(888, true, 0, false, false, 0)]),
            &mut s,
        ),
        &mut d,
    );
    t.push(&f::video(198000, &mut v), &mut d);
    assert_eq!(d.snapshot()[0].end, Some(180000));
}
#[test]
fn pages_on_same_pid_remain_isolated_with_national_characters() {
    let mut d = decoder(&[888, 889]);
    let mut t = Transport::default();
    let (mut v, mut s) = (0, 0);
    t.push(&f::tables(&[(0x121, 888), (0x121, 889)], 0), &mut d);
    t.push(&f::video(0, &mut v), &mut d);
    t.push(
        &f::pes(
            0x121,
            90000,
            &f::body(&[
                f::header(888, true, 1, false, false, 0),
                f::row(888, 1, b"GR]SSE"),
                f::header(889, true, 4, false, false, 0),
                f::row(889, 1, b"fran~ais"),
            ]),
            &mut s,
        ),
        &mut d,
    );
    t.push(&f::video(108000, &mut v), &mut d);
    let cues = d.snapshot();
    assert_eq!(cues.len(), 2);
    assert!(cues.iter().any(|c| c.text == "GRÜSSE"));
    assert!(cues.iter().any(|c| c.text == "français"));
}

struct Run {
    d: Decoder,
    t: Transport,
    v: u8,
    s: u8,
}
impl Run {
    fn new(pages: &[u16], announced: &[(u16, u16)]) -> Self {
        let mut r = Self {
            d: decoder(pages),
            t: Transport::default(),
            v: 0,
            s: 0,
        };
        r.feed(&f::tables(announced, 0));
        r.video(0);
        r
    }
    fn feed(&mut self, b: &[u8]) {
        self.t.push(b, &mut self.d);
    }
    fn video(&mut self, t: u64) {
        let b = f::video(t, &mut self.v);
        self.feed(&b);
    }
    fn units(&mut self, pid: u16, t: u64, u: &[Vec<u8>]) {
        let b = f::pes(pid, t, &f::body(u), &mut self.s);
        self.feed(&b);
    }
    fn show(&mut self, page: u16, t: u64, text: &[u8]) {
        self.units(
            0x121,
            t,
            &[
                f::header(page, true, 0, false, false, 0),
                f::row(page, 1, text),
            ],
        );
        self.video(t + 18000);
    }
    fn open(&self) -> Vec<String> {
        self.d
            .snapshot()
            .iter()
            .filter(|c| c.end.is_none())
            .map(|c| c.text.clone())
            .collect()
    }
}
#[test]
fn announced_valid_page_starts_without_false_unavailable_error() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.show(888, 90000, b"OK");
    assert_eq!(
        r.d.error, None,
        "valid PSI must not latch an error before first PMT"
    );
    assert_eq!(r.d.teletext_stats()[0]["status"], "available");
}
#[test]
fn partial_updates_retain_other_rows_and_subpage_changes_erase_them() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.show(888, 90000, b"ONE");
    r.units(
        0x121,
        180000,
        &[
            f::header(888, false, 0, false, false, 0),
            f::row(888, 2, b"TWO"),
        ],
    );
    r.video(198000);
    assert_eq!(r.open(), ["ONE\nTWO"]);
    r.units(
        0x121,
        270000,
        &[
            f::header(888, false, 0, false, false, 1),
            f::row(888, 1, b"NEW"),
        ],
    );
    r.video(288000);
    assert_eq!(r.open(), ["NEW"]);
    assert_eq!(r.d.snapshot()[1].end, Some(270000));
}
#[test]
fn retransmission_deduplicates_text_and_inhibit_closes_at_header_pts() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.show(888, 90000, b"ONE");
    r.show(888, 180000, b"ONE");
    assert_eq!(r.d.snapshot().len(), 1);
    r.units(0x121, 270000, &[f::header(888, false, 0, false, true, 0)]);
    r.video(288000);
    assert!(r.open().is_empty());
    assert_eq!(r.d.snapshot()[0].end, Some(270000));
}
#[test]
fn boxes_conceal_and_mosaics_do_not_leak_non_subtitle_content() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    let mut bytes = [f::parity(b' '); 40];
    for (i, b) in [
        b'O', b'U', b'T', 11, 11, b'A', 24, b'X', 7, b'B', 16, 0x21, 7, b'C', 10, b'O',
    ]
    .into_iter()
    .enumerate()
    {
        bytes[i] = f::parity(b);
    }
    r.units(
        0x121,
        90000,
        &[
            f::header(888, true, 0, false, false, 0),
            f::unit(8, 1, bytes),
        ],
    );
    r.video(108000);
    assert_eq!(r.open(), ["A   B   C"]);
}
#[test]
fn single_hamming_bit_is_corrected_and_double_error_clears_only_teletext() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    let mut h = f::header(888, true, 0, false, false, 0);
    h[4] ^= 1;
    r.units(0x121, 90000, &[h, f::row(888, 1, b"ONE")]);
    r.video(108000);
    assert_eq!(r.open(), ["ONE"]);
    r.d.push(0, [f::parity(0x14), f::parity(0x29)], 108000);
    r.d.push(0, [f::parity(b'U'), f::parity(b'S')], 108000);
    let mut h = f::header(888, true, 0, false, false, 0);
    h[4] ^= 3;
    r.units(0x121, 180000, &[h]);
    r.video(198000);
    assert_eq!(r.open(), ["US"]);
    assert_eq!(r.d.error, Some("teletext_hamming"));
}
#[test]
fn bad_character_parity_is_replaced_without_inventing_control_codes() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    let mut row = f::row(888, 1, b"ABC");
    row[9] ^= 1; // B parity bit after wire reversal
    r.units(
        0x121,
        90000,
        &[f::header(888, true, 0, false, false, 0), row],
    );
    r.video(108000);
    assert_eq!(r.open(), ["A�C"]);
    assert_eq!(r.d.error, Some("teletext_character_parity"));
}
#[test]
fn parallel_and_serial_magazines_keep_their_page_association() {
    for serial in [false, true] {
        let mut r = Run::new(&[888, 188], &[(0x121, 888), (0x121, 188)]);
        r.units(
            0x121,
            90000,
            &[
                f::header(888, true, 0, serial, false, 0),
                f::row(888, 1, b"EIGHT"),
                f::header(188, true, 0, serial, false, 0),
                f::row(188, 1, b"ONE"),
                f::row(888, 2, b"LATE"),
            ],
        );
        r.video(108000);
        let cues = r.d.snapshot();
        let eight = cues
            .iter()
            .find(|c| c.channel == 1912 && c.end.is_none())
            .unwrap();
        assert_eq!(eight.text, if serial { "EIGHT" } else { "EIGHT\nLATE" });
    }
}
#[test]
fn missing_and_ambiguous_announcements_never_merge_unrelated_pages() {
    for announced in [vec![], vec![(0x121, 888), (0x122, 888)]] {
        let mut r = Run::new(&[888], &announced);
        r.show(888, 90000, b"WRONG");
        assert!(r.open().is_empty());
        assert!(r.d.error.is_some());
        assert_eq!(r.d.teletext_stats()[0]["status"], "unavailable");
    }
}
#[test]
fn pmt_replacement_clears_removed_page_and_accepts_new_pid() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.show(888, 90000, b"OLD");
    r.feed(&f::tables(&[], 1));
    assert!(r.open().is_empty());
    assert_eq!(r.d.teletext_stats()[0]["status"], "unavailable");
    r.feed(&f::tables(&[(0x122, 888)], 2));
    r.s = 0;
    r.units(
        0x122,
        180000,
        &[
            f::header(888, true, 0, false, false, 0),
            f::row(888, 1, b"NEW"),
        ],
    );
    r.video(198000);
    assert_eq!(r.open(), ["NEW"]);
    assert_eq!(r.d.teletext_stats()[0]["pid"], 0x122);
}
#[test]
fn gap_and_malformed_unit_clear_teletext_without_stalling_video() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.show(888, 90000, b"OLD");
    r.s = (r.s + 1) & 15;
    r.units(0x121, 180000, &[f::header(888, true, 0, false, false, 0)]);
    r.video(198000);
    assert!(r.open().is_empty());
    r.show(888, 270000, b"RECOVERED");
    assert_eq!(r.open(), ["RECOVERED"]);
    let bad = f::pes(0x121, 360000, &[0x10, 3, 44, 0], &mut r.s);
    r.feed(&bad);
    r.video(378000);
    assert!(r.open().is_empty());
    assert!(r.d.latest_pts >= 378000);
    assert_eq!(r.d.error, Some("teletext_unit_length"));
}
#[test]
fn future_reference_pts_does_not_commit_page_before_earlier_rows_arrive() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.units(0x121, 18000, &[f::header(888, true, 0, false, false, 0)]);
    let b = f::video_reordered(45000, 9000, &mut r.v);
    r.feed(&b);
    assert!(r.open().is_empty());
    r.units(0x121, 27000, &[f::row(888, 1, b"ORDERED")]);
    for (pts, dts) in [(27000, 18000), (72000, 27000)] {
        let b = f::video_reordered(pts, dts, &mut r.v);
        r.feed(&b);
        assert!(r.open().is_empty());
    }
    let b = f::video_reordered(54000, 36000, &mut r.v);
    r.feed(&b);
    assert_eq!(r.open(), ["ORDERED"]);
    assert_eq!(r.d.snapshot()[0].start, 18000);
}
#[test]
fn teletext_pts_wraps_to_the_video_epoch() {
    let mut d = decoder(&[888]);
    let mut t = Transport::default();
    let (mut v, mut s) = (0, 0);
    let first = (1 << 33) - 90000;
    t.push(&f::tables(&[(0x121, 888)], 0), &mut d);
    t.push(&f::video(first, &mut v), &mut d);
    t.push(
        &f::pes(
            0x121,
            first + 45000,
            &f::body(&[
                f::header(888, true, 0, false, false, 0),
                f::row(888, 1, b"WRAP"),
            ]),
            &mut s,
        ),
        &mut d,
    );
    t.push(&f::video(9000, &mut v), &mut d);
    assert_eq!(d.snapshot()[0].start, first + 45000);
    assert_eq!(d.latest_pts, (1 << 33) + 9000);
}
#[test]
fn unsupported_enhancement_clears_page_and_reports_degradation() {
    for number in [26, 28, 29] {
        let mut r = Run::new(&[888], &[(0x121, 888)]);
        r.show(888, 90000, b"OLD");
        r.units(0x121, 180000, &[f::unit(8, number, [f::ham(0); 40])]);
        r.video(198000);
        assert!(r.open().is_empty());
        assert_eq!(r.d.error, Some("teletext_enhancement_unsupported"));
    }
}
#[test]
fn future_pes_queue_is_bounded_and_recovers_on_new_page() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    for _ in 0..65 {
        r.units(0x121, 900000, &[f::header(888, true, 0, false, false, 0)]);
    }
    assert_eq!(r.d.error, Some("teletext_reorder_limit"));
    r.show(888, 90000, b"RECOVERED");
    assert_eq!(r.open(), ["RECOVERED"]);
}

#[test]
fn pmt_page_reassignment_on_same_pid_discards_old_queued_pes() {
    let mut r = Run::new(&[888, 889], &[(0x121, 888)]);
    r.units(
        0x121,
        90000,
        &[
            f::header(889, true, 0, false, false, 0),
            f::row(889, 1, b"STALE GENERATION"),
        ],
    );
    r.feed(&f::tables(&[(0x121, 889)], 1));
    r.video(108000);
    assert!(
        r.open().is_empty(),
        "new page binding cannot interpret old queued media"
    );
}
#[test]
fn reused_counter_with_different_payload_degrades_instead_of_ignoring_update() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    r.show(888, 90000, b"OLD");
    r.s = (r.s + 15) & 15; // reuse the preceding PES counter with new payload
    r.units(
        0x121,
        180000,
        &[
            f::header(888, true, 0, false, false, 0),
            f::row(888, 1, b"NEW"),
        ],
    );
    r.video(198000);
    assert_eq!(r.open(), ["NEW"]);
    assert_eq!(r.d.error, Some("teletext_transport_gap"));
}

#[test]
fn default_latin_national_options_match_literal_standard_characters() {
    let expected = [
        "£$@←½→↑#—¼‖¾÷",
        "#$§ÄÖÜ^_°äöüß",
        "#¤ÉÄÖÅÜ_éäöåü",
        "£$é°ç→↑#ùàòèì",
        "éïàëêùî#èâôûç",
        "ç$¡áéíóú¿üñèà",
        "#ůčťžýířéáěúš",
    ];
    for (n, want) in expected.into_iter().enumerate() {
        let mut r = Run::new(&[888], &[(0x121, 888)]);
        r.units(
            0x121,
            90000,
            &[
                f::header(888, true, n as u8, false, false, 0),
                f::row(888, 1, b"#$@[\\]^_`{|}~"),
            ],
        );
        r.video(108000);
        assert_eq!(r.open(), [want]);
    }
}
#[test]
fn split_large_pes_and_arbitrary_input_chunks_preserve_all_rows() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    let mut units = vec![f::header(888, true, 0, false, false, 0)];
    for row in 1..=24 {
        units.push(f::row(888, row, b"LINE"));
    }
    let bytes = f::pes(0x121, 90000, &f::body(&units), &mut r.s);
    for part in bytes.chunks(7) {
        r.feed(part);
    }
    r.video(108000);
    assert_eq!(r.open()[0].lines().count(), 24);
    assert!(r.open()[0].lines().all(|l| l == "LINE"));
}
#[test]
fn exact_duplicate_ts_packet_is_idempotent() {
    let mut r = Run::new(&[888], &[(0x121, 888)]);
    let b = f::pes(
        0x121,
        90000,
        &f::body(&[
            f::header(888, true, 0, false, false, 0),
            f::row(888, 1, b"ONE"),
        ]),
        &mut r.s,
    );
    r.feed(&b);
    r.feed(&b);
    r.video(108000);
    assert_eq!(r.open(), ["ONE"]);
    assert_eq!(r.d.error, None);
}
