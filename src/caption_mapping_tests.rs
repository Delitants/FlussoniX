use super::*;

fn crc(section: &mut Vec<u8>) {
    let mut value = u32::MAX;
    for &byte in section.iter() {
        value ^= u32::from(byte) << 24;
        for _ in 0..8 {
            value = (value << 1)
                ^ if value & 0x8000_0000 != 0 {
                    0x04c1_1db7
                } else {
                    0
                };
        }
    }
    section.extend(value.to_be_bytes());
}
fn table(pid: u16, mut section: Vec<u8>, out: &mut Vec<u8>) {
    crc(&mut section);
    let mut packet = [0xff; 188];
    packet[..5].copy_from_slice(&[0x47, 0x40 | (pid >> 8) as u8, pid as u8, 0x10, 0]);
    packet[5..5 + section.len()].copy_from_slice(&section);
    out.extend(packet);
}
fn escape(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    let mut zeros = 0;
    for &byte in input {
        if zeros == 2 && byte <= 3 {
            output.push(3);
            zeros = 0;
        }
        output.push(byte);
        zeros = if byte == 0 { zeros + 1 } else { 0 };
    }
    output
}
fn pes(hevc: bool, filler: usize, messages: usize) -> (Vec<u8>, Vec<u8>) {
    let mut raw = vec![137, 3, 10, 20, 30, 5, 4, 0, 0, 1, 0x55];
    let mut suppressed = raw.clone();
    for _ in 0..messages {
        for target in [&mut raw, &mut suppressed] {
            target.extend([4, 104, 0xb5, 0, 0x31, b'G', b'A', b'9', b'4', 3]);
        }
        raw.extend([0xdf, 0xff]);
        suppressed.extend([0x80, 0xff]);
        for i in 0..31 {
            let kind = i % 4;
            raw.extend([0xfc | kind, 0x11, 0x22]);
            suppressed.extend([0xf8 | kind, 0x11, 0x22]);
        }
        raw.push(0xff);
        suppressed.push(0xff);
    }
    raw.push(0x80);
    suppressed.push(0x80);
    let mut prefix = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0, 0, 0, 0, 1];
    prefix.extend(if hevc { &[0x4c, 1][..] } else { &[12][..] });
    prefix.extend(std::iter::repeat_n(0xff, filler));
    prefix.push(0x80);
    prefix.extend([0, 0, 1]);
    prefix.extend(if hevc { &[0x4e, 1][..] } else { &[6][..] });
    let mut original = prefix.clone();
    original.extend(escape(&raw));
    let mut expected = prefix;
    expected.extend(escape(&suppressed));
    assert_eq!(original.len(), expected.len());
    assert!(original.len() <= 2 * 1024 * 1024);
    (original, expected)
}
fn transport(hevc: bool, records: &[Vec<u8>], variable: bool) -> (Vec<u8>, usize) {
    let mut output = Vec::new();
    table(
        0,
        vec![0, 0xb0, 13, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0],
        &mut output,
    );
    table(
        4096,
        vec![
            2,
            0xb0,
            18,
            0,
            1,
            0xc1,
            0,
            0,
            0xe1,
            0,
            0xf0,
            0,
            if hevc { 0x24 } else { 0x1b },
            0xe1,
            0,
            0xf0,
            0,
        ],
        &mut output,
    );
    let mut rows = 0usize;
    let mut null_cc = 0u8;
    for record in records {
        let mut at = 0;
        while at < record.len() {
            let limit = if variable {
                [181, 7, 183, 29][rows % 4]
            } else {
                184
            };
            let n = (record.len() - at).min(limit);
            let mut packet = [0xff; 188];
            packet[..4].copy_from_slice(&[
                0x47,
                1 | if at == 0 { 0x40 } else { 0 },
                0,
                if n == 184 { 0x10 } else { 0x30 } | (rows as u8 & 15),
            ]);
            let off = 188 - n;
            if n != 184 {
                packet[4] = (183 - n) as u8;
                if n < 183 {
                    packet[5] = 0;
                }
            }
            packet[off..].copy_from_slice(&record[at..at + n]);
            output.extend(packet);
            at += n;
            rows += 1;
            if variable && rows % 17 == 0 {
                let mut null = [0xee; 188];
                null[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10 | null_cc]);
                null_cc = (null_cc + 1) & 15;
                output.extend(null);
            }
        }
    }
    (output, rows)
}
fn counted_filter(bytes: &mut [u8]) -> Result<usize, String> {
    MAPPING_WORK.with(|work| work.set(0));
    ts(bytes)?;
    Ok(MAPPING_WORK.with(|work| work.get()))
}
#[test]
fn dense_caption_ts_mapping_work_is_linear_and_byte_exact() {
    for hevc in [false, true] {
        let (original, expected) = pes(hevc, 1_750_000, 2047);
        let (mut bytes, rows) = transport(hevc, &[original], false);
        let (expected, _) = transport(hevc, &[expected], false);
        let work = counted_filter(&mut bytes).unwrap();
        assert_eq!(
            bytes, expected,
            "every unrelated TS, PES, SEI and video byte must survive"
        );
        let patches = 2047 * 32;
        let bound = rows + patches;
        println!(
            "hevc={hevc},mapping_rows={rows},patches={patches},mapping_row_inspections={work},linear_bound={bound},byte_exact=true"
        );
        assert!(
            work <= bound,
            "caption patch translation must not multiply PES mapping rows by patch count: {work}>{bound}"
        );
    }
}
#[test]
fn mapping_handles_variable_packets_multiple_pes_and_transactional_errors() {
    for hevc in [false, true] {
        let (one, one_expected) = pes(hevc, 301, 4);
        let (two, two_expected) = pes(hevc, 15, 2);
        let (mut bytes, rows) = transport(hevc, &[one.clone(), two.clone()], true);
        let (expected, _) = transport(hevc, &[one_expected, two_expected], true);
        let work = counted_filter(&mut bytes).unwrap();
        assert_eq!(bytes, expected);
        assert!(work <= rows + 6 * 32);
        let mut malformed = two;
        malformed[0] = 7;
        let (mut bytes, _) = transport(hevc, &[one, malformed], true);
        let original = bytes.clone();
        assert_eq!(
            counted_filter(&mut bytes).unwrap_err(),
            "invalid caption PES"
        );
        assert_eq!(
            bytes, original,
            "a later PES error must not publish earlier patches"
        );
    }
}
#[test]
fn mapping_keeps_the_exact_patch_limit_and_clears_every_608_708_valid_bit() {
    for hevc in [false, true] {
        for messages in [2048, 2049] {
            let (input, expected) = pes(hevc, 0, messages);
            let (mut bytes, rows) = transport(hevc, &[input], true);
            let original = bytes.clone();
            if messages == 2048 {
                let work = counted_filter(&mut bytes).unwrap();
                assert!(work <= rows + 65536);
                assert_eq!(bytes, transport(hevc, &[expected], true).0);
            } else {
                assert_eq!(
                    counted_filter(&mut bytes).unwrap_err(),
                    "caption filter patch limit"
                );
                assert_eq!(bytes, original);
            }
        }
    }
}

#[test]
fn ordered_multiple_nals_and_hevc_suffix_preserve_mapping_and_noncaption_bytes() {
    for hevc in [false, true] {
        let (mut first, mut first_expected) = pes(hevc, 17, 2);
        let (mut second, mut second_expected) = pes(hevc, 0, 2);
        if hevc {
            for bytes in [&mut second, &mut second_expected] {
                let at = bytes
                    .windows(5)
                    .position(|p| p == [0, 0, 1, 0x4e, 1])
                    .unwrap();
                bytes[at + 3] = 0x50; // HEVC suffix SEI, identical registered payload.
            }
        }
        first.extend_from_slice(&second[9..]);
        first_expected.extend_from_slice(&second_expected[9..]);
        let (mut bytes, rows) = transport(hevc, &[first], true);
        let expected = transport(hevc, &[first_expected], true).0;
        let work = counted_filter(&mut bytes).unwrap();
        assert!(work <= rows + 4 * 32);
        assert_eq!(bytes, expected);
        let (untouched, _) = pes(hevc, 1001, 0);
        let (mut no_captions, _) = transport(hevc, &[untouched], true);
        let original = no_captions.clone();
        assert_eq!(counted_filter(&mut no_captions).unwrap(), 0);
        assert_eq!(no_captions, original);
    }
}
