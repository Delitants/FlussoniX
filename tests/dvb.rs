use flussonix::captions::configuration;
use serde_json::json;
#[test]
fn dvb_selector_preserves_full_page_and_per_service_ocr_language() {
    let cfg = json!({"flussonix_hls_captions":[{"dvb_page":65535,"ocr_language":"eng+deu","language":"de","name":"DVB German"},{"channel":1,"language":"en","name":"CC"},{"teletext_page":888,"language":"fr","name":"Teletext"}]});
    let s = configuration(&cfg).expect("DVB selector and OCR language must be supported");
    assert_eq!(
        serde_json::to_value(&s).unwrap(),
        cfg["flussonix_hls_captions"]
    );
    assert_eq!(s[0].key(), "dvb65535");
    assert_eq!(s[1].key(), "cc1");
    assert_eq!(s[2].key(), "ttx888");
    assert!(configuration(&json!({"flussonix_hls_captions":[{"dvb_page":0,"ocr_language":"eng","language":"en","name":"Zero"}]})).is_ok());
}
#[test]
fn dvb_selectors_reject_ambiguous_ids_and_unsafe_or_missing_ocr_models() {
    for extra in [
        json!({}),
        json!({"ocr_language":"../eng"}),
        json!({"ocr_language":"eng --psm 0"}),
        json!({"ocr_language":"eng+"}),
        json!({"ocr_language":"eng+deu+fra+spa+ita"}),
        json!({"ocr_language":"eng","channel":1}),
        json!({"ocr_language":"eng","service":1}),
        json!({"ocr_language":"eng","teletext_page":888}),
    ] {
        let mut row = json!({"dvb_page":1,"language":"en","name":"Invalid"});
        row.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(configuration(&json!({"flussonix_hls_captions":[row]})).is_err());
    }
    for page in [-1, 65536] {
        assert!(configuration(&json!({"flussonix_hls_captions":[{"dvb_page":page,"ocr_language":"eng","language":"en","name":"Invalid"}]})).is_err());
    }
    assert!(configuration(&json!({"flussonix_hls_captions":[{"channel":1,"ocr_language":"eng","language":"en","name":"CC"}]})).is_err());
}
#[path = "support/dvb_fixture.rs"]
mod f;
fn native(pages: &[u16]) -> flussonix::dvb::Decoder {
    let mut d = flussonix::dvb::Decoder::new(pages.iter().copied());
    d.bindings(&pages.iter().map(|p| (*p, (0x120, 2))).collect(), 0);
    d
}
#[test]
fn literal_bitmap_has_exact_pixels_source_pts_and_empty_page_clear() {
    let mut d = native(&[1]);
    let frames = d.push(0x120, &f::tiny(1), 90000);
    assert_eq!(frames.len(), 1, "complete bitmap display must be emitted");
    let frame = &frames[0];
    assert_eq!((frame.page, frame.pts, frame.expires), (1, 90000, 270000));
    let image = frame.image.as_ref().unwrap();
    assert_eq!((image.width, image.height), (2, 2));
    assert_eq!(image.pixels, vec![[255, 255], [0, 0], [0, 0], [255, 255]]);
    let clear = d.push(
        0x120,
        &f::body(&[f::pcs(1, 1, 0, 2, &[]), f::eod(1)]),
        180000,
    );
    assert_eq!(clear.len(), 1);
    assert!(clear[0].image.is_none());
    assert_eq!(clear[0].pts, 180000);
}
#[test]
fn end_of_display_waits_for_same_pts_pes_fragments_and_source_timeout() {
    let mut d = native(&[1]);
    assert!(
        d.push(
            0x120,
            &f::body(&[
                f::pcs(1, 0, 2, 2, &[(1, 0, 0)]),
                f::region(1, 2, 2, 1, true, &[(9, 0, 0)])
            ]),
            90000
        )
        .is_empty()
    );
    let result = d.push(
        0x120,
        &f::body(&[
            f::object(1, 9, 0, false, &[0x10, 0x44, 0, 0xf0], &[]),
            f::eod(1),
        ]),
        90000,
    );
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].image.as_ref().unwrap().pixels,
        vec![[255, 255], [0, 0], [255, 255], [0, 0]]
    );
    assert!(d.advance(269999).is_empty());
    let result = d.advance(270000);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].pts, 270000);
    assert!(result[0].image.is_none());
}
fn bits(s: &str) -> Vec<u8> {
    let mut out = vec![0; s.len().div_ceil(8)];
    for (i, b) in s.bytes().enumerate() {
        assert!(b == b'0' || b == b'1');
        out[i / 8] |= (b - b'0') << (7 - i % 8);
    }
    out
}
fn pixels_with(depth: u8, width: u16, field: &[u8], palette: Option<Vec<u8>>) -> Vec<[u8; 2]> {
    let mut d = native(&[1]);
    let mut segments = vec![
        f::pcs(1, 0, 2, 2, &[(1, 0, 0)]),
        f::region(1, width, 2, depth, true, &[(9, 0, 0)]),
    ];
    if let Some(p) = palette {
        segments.push(p);
    }
    segments.extend([f::object(1, 9, 0, false, field, &[]), f::eod(1)]);
    let mut frames = d.push(0x120, &f::body(&segments), 90000);
    assert_eq!(
        frames.len(),
        1,
        "valid RLE should reconstruct a page: {:?}",
        d.error
    );
    frames.pop().unwrap().image.unwrap().pixels
}
#[test]
fn two_bit_literal_and_every_run_switch_reconstruct_exact_scanline() {
    let mut field = vec![0x10];
    field.extend(bits(
        "010001000001001000100000100000010000110000000001000000",
    ));
    field.push(0xf0);
    let p = pixels_with(1, 48, &field, None);
    assert_eq!(
        &p[..7],
        &[
            [255, 255],
            [0, 0],
            [0, 0],
            [0, 0],
            [0, 255],
            [0, 255],
            [0, 255]
        ]
    );
    assert!(p[7..48].iter().all(|p| *p == [255, 255]));
    assert_eq!(&p[..48], &p[48..]);
}
#[test]
fn four_bit_runs_and_full_clut_override_default_colors() {
    let mut field = vec![0x11];
    field.extend(bits(
        "000100001100000011010000000100001000000100001110000000010000111100000000000100000000",
    ));
    field.push(0xf0);
    let palette = f::seg(0x12, 1, &[0, 0, 1, 0x41, 235, 128, 128, 0]);
    let p = pixels_with(2, 45, &field, Some(palette));
    assert_eq!(
        &p[..7],
        &[[255, 255], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]
    );
    assert!(p[7..45].iter().all(|p| *p == [255, 255]));
    assert_eq!(&p[..45], &p[45..]);
}
#[test]
fn eight_bit_runs_and_reduced_clut_decode_luminance_and_alpha() {
    // Reduced entry: Y63, Cr8, Cb8, T0; white fully opaque.
    let palette = f::seg(0x12, 1, &[0, 0, 1, 0x20, 0xfe, 0x20]);
    let p = pixels_with(
        3,
        7,
        &[0x12, 1, 0, 3, 0, 0x83, 1, 0, 0, 0xf0],
        Some(palette),
    );
    assert_eq!(
        &p[..7],
        &[
            [255, 255],
            [0, 0],
            [0, 0],
            [0, 0],
            [255, 255],
            [255, 255],
            [255, 255]
        ]
    );
    assert_eq!(&p[..7], &p[7..]);
}
#[test]
fn custom_maps_select_the_target_clut_and_keep_transparency() {
    let palette = f::seg(0x12, 1, &[0, 0, 5, 0x41, 16, 128, 128, 0]);
    let p = pixels_with(
        2,
        2,
        &[0x20, 0x05, 0xaf, 0x10, 0x44, 0, 0xf0],
        Some(palette),
    );
    assert_eq!(p, vec![[0, 255], [0, 0], [0, 255], [0, 0]]);
    let mut field = vec![0x22];
    field.extend([0, 42, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    field.extend([0x11, 0x10, 0, 0xf0]);
    let palette = f::seg(0x12, 1, &[0, 0, 42, 0x21, 235, 128, 128, 127]);
    assert_eq!(
        pixels_with(3, 1, &field, Some(palette)),
        vec![[255, 128]; 2]
    );
}
#[test]
fn default_mapping_between_pixel_depths_uses_standard_entries() {
    assert_eq!(
        pixels_with(2, 2, &[0x10, 0x44, 0, 0xf0], None),
        vec![[255, 255], [0, 0], [255, 255], [0, 0]]
    );
    // 2->8 color1 maps119: fully opaque white.
    assert_eq!(
        pixels_with(3, 2, &[0x10, 0x44, 0, 0xf0], None),
        vec![[255, 255], [0, 0], [255, 255], [0, 0]]
    );
}
#[test]
fn ancillary_objects_and_palettes_are_used_only_by_the_bound_service() {
    let mut d = native(&[1]);
    let segments = vec![
        f::pcs(1, 0, 2, 2, &[(1, 0, 0)]),
        f::region(1, 2, 2, 3, true, &[(9, 0, 0)]),
        f::seg(0x12, 2, &[0, 0, 1, 0x21, 235, 128, 128, 0]),
        f::object(2, 9, 0, false, &[0x12, 1, 0, 1, 0, 0, 0xf0], &[]),
        f::eod(1),
    ];
    let out = d.push(0x120, &f::body(&segments), 90000);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].image.as_ref().unwrap().pixels,
        vec![[255, 255], [0, 0], [255, 255], [0, 0]]
    );
    let cleared = d.bindings(&[(1, (0x120, 3))].into(), 180000);
    assert!(cleared.iter().any(|f| f.image.is_none()));
    let out = d.push(0x120, &f::body(&segments), 270000);
    assert!(out.iter().all(|f| f.image.is_none()));
    assert!(
        d.error.is_some(),
        "old ancillary objects cannot survive rebinding"
    );
}
#[test]
fn normal_updates_preserve_pixels_and_non_modifying_color_holes() {
    let mut d = native(&[1]);
    assert_eq!(d.push(0x120, &f::tiny(1), 90000).len(), 1);
    // Non-modifying color1 leaves first pixel unchanged; zero overwrites second.
    let s = f::body(&[
        f::pcs(1, 1, 0, 2, &[(1, 20, 30)]),
        f::object(1, 9, 1, true, &[0x10, 0x44, 0, 0xf0], &[]),
        f::eod(1),
    ]);
    let out = d.push(0x120, &s, 180000);
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0].image.as_ref().unwrap().pixels,
        vec![[255, 255], [0, 0], [0, 0], [0, 0]]
    );
    assert_eq!(out[0].expires, 360000);
}
#[test]
fn incomplete_or_unsupported_sets_clear_without_emitting_partial_images() {
    for bad in [
        f::seg(0x13, 1, &[0, 9, 4]),
        f::seg(0x16, 1, &[0]),
        f::seg(0x13, 1, &[0, 9, 0, 0, 1, 0, 0, 0x10]),
    ] {
        let mut d = native(&[1]);
        d.push(0x120, &f::tiny(1), 90000);
        let b = f::body(&[f::pcs(1, 1, 0, 2, &[(1, 20, 30)]), bad, f::eod(1)]);
        let out = d.push(0x120, &b, 180000);
        assert!(d.error.is_some());
        assert!(out.iter().all(|f| f.image.is_none()));
        assert!(out.iter().any(|f| f.pts == 180000));
    }
    let mut d = native(&[1]);
    d.push(0x120, &f::body(&[f::pcs(1, 0, 2, 2, &[(1, 0, 0)])]), 90000);
    let out = d.push(0x120, &f::body(&[f::eod(1)]), 180000);
    assert!(d.error.is_some());
    assert!(out.iter().all(|f| f.image.is_none()));
}
#[test]
fn dimensions_object_offsets_and_pixel_runs_are_checked_before_allocation() {
    for (w, h, x, field) in [
        (0, 2, 0, vec![0x12, 1, 0, 0, 0xf0]),
        (4096, 2304, 0, vec![0x12, 1, 0, 0, 0xf0]),
        (2, 2, 2, vec![0x12, 1, 0, 0, 0xf0]),
        (2, 2, 0, vec![0x12, 0, 0xff, 1, 0, 0, 0xf0]),
    ] {
        let mut d = native(&[1]);
        let b = f::body(&[
            f::pcs(1, 0, 2, 2, &[(1, 0, 0)]),
            f::region(1, w, h, 3, true, &[(9, x, 0)]),
            f::object(1, 9, 0, false, &field, &[]),
            f::eod(1),
        ]);
        let out = d.push(0x120, &b, 90000);
        assert!(d.error.is_some());
        assert!(out.iter().all(|f| f.image.is_none()));
    }
}
#[test]
fn one_services_corrupt_set_does_not_clear_a_different_pid() {
    let mut d = flussonix::dvb::Decoder::new([1, 3]);
    d.bindings(&[(1, (0x120, 2)), (3, (0x121, 4))].into(), 0);
    d.push(0x120, &f::tiny(1), 90000);
    d.push(0x121, &f::tiny(3), 90000);
    let bad = d.push(0x120, &[0x20, 0, 15, 0x13, 0, 1, 0, 99], 180000);
    assert!(bad.iter().all(|f| f.page == 1));
    let expired = d.advance(270000);
    assert!(expired.iter().any(|f| f.page == 3 && f.pts == 270000));
}
#[test]
fn an_unused_cached_region_cannot_prevent_visible_region_display() {
    let mut d = native(&[1]);
    let mut unused = f::region(1, 2, 2, 1, true, &[(10, 0, 0)]);
    unused[6] = 2;
    let mut b = f::tiny(1);
    b.truncate(b.len() - 7); // remove EODS and PES terminator
    b.extend(unused);
    b.extend(f::eod(1));
    b.push(255);
    let out = d.push(0x120, &b, 90000);
    assert_eq!(out.len(), 1);
    assert!(
        out[0].image.is_some(),
        "invisible cached region must not need its object yet"
    );
}
fn integrated(pages: &[u16]) -> flussonix::captions::Decoder {
    flussonix::captions::Decoder::new(configuration(&json!({"flussonix_hls_captions":pages.iter().map(|p|json!({"dvb_page":p,"ocr_language":"eng","language":"en","name":format!("DVB {p}")})).collect::<Vec<_>>()})).unwrap())
}
#[test]
fn full_transport_reconstructs_announced_dvb_on_the_safe_video_frontier() {
    let mut d = integrated(&[1]);
    let mut t = flussonix::caption_transport::Transport::default();
    let (mut v, mut s) = (0, 0);
    t.push(&f::tables(&[(0x120, 1, 2)], 0), &mut d);
    t.push(&f::carrier::video(0, &mut v), &mut d);
    t.push(&f::carrier::pes(0x120, 90000, &f::tiny(1), &mut s), &mut d);
    assert!(d.take_dvb_frames().iter().all(|f| f.image.is_none()));
    t.push(
        &f::carrier::video_reordered_padding(180000, 90000, &mut v),
        &mut d,
    );
    let frames = d.take_dvb_frames();
    let frame = frames
        .iter()
        .find(|f| f.image.is_some())
        .expect("announced page on safe frontier");
    assert_eq!(frame.pts, 90000);
    assert_eq!(frame.expires, 270000);
}
#[test]
fn changed_ancillary_binding_discards_queued_old_pes_even_on_same_pid() {
    let mut d = integrated(&[1]);
    let mut t = flussonix::caption_transport::Transport::default();
    let (mut v, mut s) = (0, 0);
    t.push(&f::tables(&[(0x120, 1, 2)], 0), &mut d);
    t.push(&f::carrier::video(0, &mut v), &mut d);
    t.push(&f::carrier::pes(0x120, 180000, &f::tiny(1), &mut s), &mut d);
    t.push(&f::tables(&[(0x120, 1, 3)], 1), &mut d);
    t.push(&f::carrier::video(180000, &mut v), &mut d);
    assert!(d.take_dvb_frames().iter().all(|f| f.image.is_none()));
    t.push(&f::carrier::pes(0x120, 198000, &f::tiny(1), &mut s), &mut d);
    t.push(&f::carrier::video(198000, &mut v), &mut d);
    assert!(
        d.take_dvb_frames()
            .iter()
            .any(|f| f.image.is_some() && f.pts == 198000)
    );
}
#[test]
fn ambiguous_composition_pages_are_unbound_instead_of_merging_sources() {
    let mut d = integrated(&[1]);
    let mut t = flussonix::caption_transport::Transport::default();
    let (mut v, mut s) = (0, 0);
    t.push(&f::tables(&[(0x120, 1, 2), (0x121, 1, 2)], 0), &mut d);
    t.push(&f::carrier::video(0, &mut v), &mut d);
    t.push(&f::carrier::pes(0x120, 90000, &f::tiny(1), &mut s), &mut d);
    t.push(&f::carrier::video(90000, &mut v), &mut d);
    assert!(d.take_dvb_frames().iter().all(|f| f.image.is_none()));
    assert_eq!(d.error, Some("dvb_page_ambiguous"));
}
#[test]
fn transport_error_clears_only_the_dvb_service_on_that_pid() {
    let mut d = integrated(&[1, 3]);
    let mut t = flussonix::caption_transport::Transport::default();
    let (mut v, mut a, mut b) = (0, 0, 0);
    t.push(&f::tables(&[(0x120, 1, 2), (0x121, 3, 4)], 0), &mut d);
    t.push(&f::carrier::video(0, &mut v), &mut d);
    t.push(&f::carrier::pes(0x120, 90000, &f::tiny(1), &mut a), &mut d);
    t.push(&f::carrier::pes(0x121, 90000, &f::tiny(3), &mut b), &mut d);
    t.push(&f::carrier::video(90000, &mut v), &mut d);
    assert_eq!(
        d.take_dvb_frames()
            .iter()
            .filter(|f| f.image.is_some())
            .count(),
        2
    );
    let mut bad = f::carrier::pes(0x120, 180000, &f::tiny(1), &mut a);
    bad[1] |= 0x80;
    t.push(&bad, &mut d);
    let frames = d.take_dvb_frames();
    assert!(frames.iter().any(|f| f.page == 1 && f.image.is_none()));
    assert!(!frames.iter().any(|f| f.page == 3));
    assert_eq!(d.error, Some("dvb_transport_gap"));
}
#[test]
fn dvb_pts_wrap_uses_the_common_unwrapped_video_epoch() {
    let mut d = integrated(&[1]);
    let mut t = flussonix::caption_transport::Transport::default();
    let (mut v, mut s) = (0, 0);
    let period = 1u64 << 33;
    t.push(&f::tables(&[(0x120, 1, 2)], 0), &mut d);
    t.push(&f::carrier::video(period - 3600, &mut v), &mut d);
    t.push(&f::carrier::pes(0x120, 90000, &f::tiny(1), &mut s), &mut d);
    t.push(&f::carrier::video(90000, &mut v), &mut d);
    let frames = d.take_dvb_frames();
    let frame = frames
        .iter()
        .find(|f| f.image.is_some())
        .expect("wrapped bitmap page");
    assert_eq!(frame.pts, period + 90000);
    assert_eq!(frame.expires, period + 270000);
}
#[test]
fn reserved_vertical_position_bits_do_not_change_the_object_address() {
    let mut d = native(&[1]);
    let mut region = f::region(1, 2, 2, 1, true, &[(9, 0, 0)]);
    region[20] |= 0xf0;
    let out = d.push(
        0x120,
        &f::body(&[
            f::pcs(1, 0, 2, 2, &[(1, 0, 0)]),
            region,
            f::object(1, 9, 0, false, &[0x10, 0x44, 0, 0xf0], &[]),
            f::eod(1),
        ]),
        90000,
    );
    assert!(
        out.iter().any(|f| f.image.is_some()),
        "reserved bits are not coordinate bits: {:?}",
        d.error
    );
}
#[test]
fn embedded_clock_observers_cannot_expire_a_page_before_due_dvb_updates() {
    let mut d = integrated(&[1]);
    let mut t = flussonix::caption_transport::Transport::default();
    let (mut v, mut s) = (0, 0);
    t.push(&f::tables(&[(0x120, 1, 2)], 0), &mut d);
    t.push(&f::carrier::video_reordered_padding(0, 0, &mut v), &mut d);
    t.push(&f::carrier::pes(0x120, 90000, &f::tiny(1), &mut s), &mut d);
    t.push(
        &f::carrier::video_reordered_padding(90000, 90000, &mut v),
        &mut d,
    );
    d.take_dvb_frames();
    t.push(&f::carrier::pes(0x120, 180000, &f::tiny(1), &mut s), &mut d);
    t.push(
        &f::carrier::video_reordered_padding(360000, 360000, &mut v),
        &mut d,
    );
    let frames = d.take_dvb_frames();
    assert!(frames.iter().any(|f| f.pts == 180000 && f.image.is_some()));
    assert!(
        !frames.iter().any(|f| f.pts == 270000),
        "new page replaced the old timeout before observers ran"
    );
    assert!(frames.iter().any(|f| f.pts == 360000 && f.image.is_none()));
}
#[test]
fn cache_churn_cannot_exceed_region_object_or_palette_bounds() {
    for kind in [0x11, 0x12, 0x13] {
        let mut d = native(&[1]);
        d.push(0x120, &f::tiny(1), 90000);
        let mut s = vec![f::pcs(1, 1, 0, 2, &[(1, 20, 30)])];
        let count = if kind == 0x13 { 65 } else { 17 };
        for id in 0..count {
            let segment = match kind {
                0x11 => {
                    let mut b = f::region(1, 2, 2, 1, true, &[(9, 0, 0)]);
                    b[6] = id as u8;
                    b
                }
                0x12 => f::seg(0x12, 1, &[id as u8, 0, 1, 0x81, 235, 128, 128, 0]),
                _ => f::object(1, id, 0, false, &[0x10, 0x44, 0, 0xf0], &[]),
            };
            s.push(segment);
        }
        s.push(f::eod(1));
        let out = d.push(0x120, &f::body(&s), 180000);
        assert!(d.error.is_some());
        assert!(out.iter().all(|f| f.image.is_none()));
    }
}
