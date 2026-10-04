use super::Probe;
fn section(pid: u16, mut bytes: Vec<u8>) -> Vec<u8> {
    let length = bytes.len() + 1;
    bytes[1] = 0xb0 | ((length >> 8) as u8);
    bytes[2] = length as u8;
    let mut crc = 0xffff_ffffu32;
    for &byte in &bytes {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            crc = (crc << 1)
                ^ if crc & 0x8000_0000 != 0 {
                    0x04c1_1db7
                } else {
                    0
                };
        }
    }
    bytes.extend(crc.to_be_bytes());
    let mut packet = vec![0x47, 0x40 | ((pid >> 8) as u8), pid as u8, 0x10, 0];
    packet.extend(bytes);
    packet.resize(188, 0xff);
    packet
}
fn pat() -> Vec<u8> {
    section(0, vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0])
}
fn pmt(kinds: &[u8], program: u16) -> Vec<u8> {
    let mut s = vec![
        2,
        0xb0,
        0,
        (program >> 8) as u8,
        program as u8,
        0xc1,
        0,
        0,
        0xe1,
        0,
        0xf0,
        0,
    ];
    for &kind in kinds {
        s.extend([kind, 0xe1, 1, 0xf0, 0]);
    }
    section(4096, s)
}
#[test]
fn metadata_uses_first_audio_codec_and_handles_no_audio() {
    for (kinds, expected) in [
        (vec![0x1b, 0xf], Some(0xf)),
        (vec![0x24, 3, 0xf], Some(3)),
        (vec![4], Some(4)),
        (vec![0x1b, 6], None),
    ] {
        let mut probe = Probe::default();
        let mut bytes = pat();
        bytes.extend(pmt(&kinds, 1));
        for part in bytes.chunks(13) {
            probe.push(part)
        }
        assert_eq!(probe.audio, Some(expected));
    }
}
#[test]
fn invalid_crc_and_wrong_program_do_not_choose_a_filter() {
    let mut probe = Probe::default();
    probe.push(&pat());
    let mut bad = pmt(&[0x1b, 0xf], 1);
    bad[27] ^= 1;
    probe.push(&bad);
    assert_eq!(probe.audio, None);
    probe.push(&pmt(&[0x1b, 0xf], 2));
    assert_eq!(probe.audio, None);
    probe.push(&pmt(&[0x1b, 3], 1));
    assert_eq!(probe.audio, Some(Some(3)));
}
#[test]
fn incomplete_psi_does_not_invent_a_codec() {
    let mut probe = Probe::default();
    probe.push(&pat());
    let bytes = pmt(&[0x1b, 0xf], 1);
    probe.push(&bytes[..100]);
    assert_eq!(probe.audio, None);
    probe.push(&bytes[100..]);
    assert_eq!(probe.audio, Some(Some(0xf)));
}
