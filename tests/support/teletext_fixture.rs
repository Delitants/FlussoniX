// Independently authored ETSI Level 1 packets. No vendor media or decoder code.
#![allow(dead_code)]
pub fn ham(n: u8) -> u8 {
    [
        0x15, 0x02, 0x49, 0x5e, 0x64, 0x73, 0x38, 0x2f, 0xd0, 0xc7, 0x8c, 0x9b, 0xa1, 0xb6, 0xfd,
        0xea,
    ][n as usize]
}
pub fn parity(b: u8) -> u8 {
    b | if b.count_ones() % 2 == 0 { 128 } else { 0 }
}
pub fn unit(mag: u8, row: u8, bytes: [u8; 40]) -> Vec<u8> {
    let address = (row << 3) | (mag & 7);
    let mut out = vec![
        3,
        44,
        0x29,
        0xe4,
        ham(address & 15).reverse_bits(),
        ham(address >> 4).reverse_bits(),
    ];
    out.extend(bytes.map(u8::reverse_bits));
    out
}
pub fn header(
    page: u16,
    erase: bool,
    national: u8,
    serial: bool,
    inhibit: bool,
    sub: u8,
) -> Vec<u8> {
    let mut data = [parity(b' '); 40];
    let national = ((national & 1) << 3) | ((national & 2) << 1) | ((national & 4) >> 1);
    let n = [
        (page % 10) as u8,
        ((page / 10) % 10) as u8,
        sub,
        if erase { 8 } else { 0 },
        0,
        8,
        if inhibit { 8 } else { 0 },
        national | u8::from(serial),
    ];
    for (i, v) in n.into_iter().enumerate() {
        data[i] = ham(v);
    }
    unit((page / 100) as u8, 0, data)
}
pub fn row(page: u16, row: u8, text: &[u8]) -> Vec<u8> {
    let mut data = [parity(b' '); 40];
    data[0] = parity(0x0b);
    data[1] = parity(0x0b);
    for (i, &b) in text.iter().take(35).enumerate() {
        data[i + 2] = parity(b);
    }
    data[text.len().min(35) + 2] = parity(0x0a);
    unit((page / 100) as u8, row, data)
}
pub fn body(units: &[Vec<u8>]) -> Vec<u8> {
    let mut b = vec![0x10];
    for u in units {
        b.extend(u);
    }
    b
}
fn crc(bytes: &[u8]) -> u32 {
    let mut c = 0xffff_ffff;
    for &b in bytes {
        c ^= u32::from(b) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x8000_0000 != 0 { 0x04c1_1db7 } else { 0 };
        }
    }
    c
}
pub fn packetize(pid: u16, bytes: &[u8], cc: &mut u8) -> Vec<u8> {
    let mut out = vec![];
    for (i, chunk) in bytes.chunks(184).enumerate() {
        let mut p = [0xff; 188];
        p[0] = 0x47;
        p[1] = ((pid >> 8) as u8) | if i == 0 { 0x40 } else { 0 };
        p[2] = pid as u8;
        p[3] = 0x10 | *cc;
        *cc = (*cc + 1) & 15;
        let off = if chunk.len() < 184 {
            p[3] |= 0x20;
            p[4] = (183 - chunk.len()) as u8;
            if p[4] > 0 {
                p[5] = 0;
            }
            5 + p[4] as usize
        } else {
            4
        };
        p[off..].copy_from_slice(chunk);
        out.extend(p);
    }
    out
}
pub fn section(pid: u16, mut b: Vec<u8>, cc: &mut u8) -> Vec<u8> {
    let len = b.len() + 1;
    b[1] = 0xb0 | ((len >> 8) as u8);
    b[2] = len as u8;
    b.extend(crc(&b).to_be_bytes());
    b.insert(0, 0);
    packetize(pid, &b, cc)
}
pub fn tables(pages: &[(u16, u16)], version: u8) -> Vec<u8> {
    let mut out = section(0, vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0], &mut 0);
    let mut s = vec![
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
        0x1b,
        0xe1,
        0,
        0xf0,
        0,
    ];
    for &(pid, page) in pages {
        let mag = if page / 100 == 8 {
            0
        } else {
            (page / 100) as u8
        };
        s.extend([
            6,
            0xe0 | (pid >> 8) as u8,
            pid as u8,
            0xf0,
            7,
            0x56,
            5,
            b'd',
            b'e',
            b'u',
            0x10 | mag,
            ((((page / 10) % 10) << 4) | (page % 10)) as u8,
        ]);
    }
    out.extend(section(4096, s, &mut 0));
    out
}
fn pts(t: u64, prefix: u8) -> [u8; 5] {
    [
        prefix | ((t >> 29) as u8 & 14) | 1,
        (t >> 22) as u8,
        ((t >> 14) as u8 & 254) | 1,
        (t >> 7) as u8,
        ((t << 1) as u8) | 1,
    ]
}
pub fn pes(pid: u16, t: u64, bytes: &[u8], cc: &mut u8) -> Vec<u8> {
    let n = bytes.len() + 8;
    let mut b = vec![
        0,
        0,
        1,
        if pid == 256 { 0xe0 } else { 0xbd },
        (n >> 8) as u8,
        n as u8,
        0x80,
        0x80,
        5,
    ];
    b.extend(pts(t, 0x20));
    b.extend(bytes);
    packetize(pid, &b, cc)
}
pub fn video(t: u64, cc: &mut u8) -> Vec<u8> {
    pes(256, t, &[0, 0, 1, 9, 0xf0, 0, 0, 1, 0x65, 0x88], cc)
}
pub fn video_reordered(t: u64, dts: u64, cc: &mut u8) -> Vec<u8> {
    let mut b = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0xc0, 10];
    b.extend(pts(t, 0x30));
    b.extend(pts(dts, 0x10));
    b.extend([0, 0, 1, 9, 0xf0]);
    packetize(256, &b, cc)
}
