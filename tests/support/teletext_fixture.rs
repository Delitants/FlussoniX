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
    out.extend(program_pmt(1, pages, version));
    out
}
pub fn program_pmt(program: u16, pages: &[(u16, u16)], version: u8) -> Vec<u8> {
    let mut s = vec![
        2,
        0xb0,
        0,
        (program >> 8) as u8,
        program as u8,
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
    section(4096, s, &mut (version & 15))
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

#[path = "subtitle_fixture.rs"]
pub mod original;
pub const TTX888_DESC: &[u8] = &[0x56, 5, b'd', b'e', b'u', 0x10, 0x88];
pub const TTX889_DESC: &[u8] = &[0x56, 5, b'f', b'r', b'a', 0x10, 0x89];
pub fn page_body(page: u16, national: u8, text: &[u8]) -> Vec<u8> {
    body(&[
        header(page, true, national, false, false, 0),
        row(page, 1, text),
        header(887, true, 0, false, false, 0),
    ])
}
fn broadcast_pes(pid: u16, t: u64, body: &[u8], cc: &mut u8) -> Vec<u8> {
    assert_eq!((45 + body.len()) % 184, 0);
    let n = body.len() + 39;
    let mut b = vec![0, 0, 1, 0xbd, (n >> 8) as u8, n as u8, 0x84, 0x80, 36];
    b.extend(pts(t, 0x20));
    b.extend([0xff; 31]);
    b.extend(body);
    packetize(pid, &b, cc)
}
fn av() -> &'static Vec<u8> {
    static AV: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    AV.get_or_init(|| {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("owned-av.ts");
        let r = std::process::Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=64x48:rate=25",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=700:sample_rate=48000",
                "-t",
                "16",
                "-threads",
                "1",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "25",
                "-bf",
                "0",
                "-c:a",
                "aac",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
                "-pcr_period",
                "20",
                "-f",
                "mpegts",
            ])
            .arg(&file)
            .output()
            .unwrap();
        assert!(r.status.success(), "{}", String::from_utf8_lossy(&r.stderr));
        std::fs::read(file).unwrap()
    })
}
pub fn transport() -> Vec<u8> {
    let bytes = transport_with_events(true);
    if let Ok(file) = std::env::var("FLUSSONIX_TELETEXT_FIXTURE_FILE") {
        std::fs::write(file, &bytes).unwrap();
    }
    bytes
}
pub fn transport_with_events(emit: bool) -> Vec<u8> {
    let mut out = vec![];
    let (mut dc, mut a, mut b, mut frame) = (0, 0, 0, 0);
    for p in av().chunks_exact(188) {
        if original::pid(p) == 4096 {
            let payload = original::payload(p).unwrap();
            let start = 1 + usize::from(payload[0]);
            let s = &payload[start..];
            let len = 3 + ((usize::from(s[1] & 15) << 8) | usize::from(s[2]));
            let mut s = s[..len - 4].to_vec();
            for (pid, desc) in [
                (0x120u16, original::DVB_DESC),
                (0x121, TTX888_DESC),
                (0x122, TTX889_DESC),
            ] {
                s.extend([
                    6,
                    0xe0 | (pid >> 8) as u8,
                    pid as u8,
                    0xf0,
                    desc.len() as u8,
                ]);
                s.extend(desc);
            }
            out.extend(section(4096, s, &mut (p[3] & 15)));
        } else {
            out.extend(p);
        }
        if original::pid(p) == 256 && p[1] & 0x40 != 0 {
            let t = original::pts(original::payload(p).unwrap()).unwrap();
            if frame == 0 {
                out.extend(pes(0x120, t, original::DVB_BODY, &mut dc));
            }
            if emit && [0, 29, 75, 103, 150].contains(&frame) {
                let (de, fr) = match frame {
                    29 => (&b"GR]SSE"[..], &b"fran~ais"[..]),
                    103 => (&b"LIVE <&>"[..], &b"LIVE <&>"[..]),
                    _ => (&b""[..], &b""[..]),
                };
                out.extend(broadcast_pes(0x121, t, &page_body(888, 1, de), &mut a));
                out.extend(broadcast_pes(0x122, t, &page_body(889, 4, fr), &mut b));
            }
            frame += 1;
        }
    }
    out
}

pub fn video_reordered_padding(t: u64, dts: u64, cc: &mut u8) -> Vec<u8> {
    let mut b = vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0xc0, 10];
    b.extend(pts(t, 0x30));
    b.extend(pts(dts, 0x10));
    // A valid registered GA94 SEI containing an unselected analog padding pair.
    // Processing it must not run teletext deadlines before due subtitle PES.
    let payload = b"\xb5\x00\x31GA94\x03\x41\xff\xfc\x80\x80\xff";
    b.extend([0, 0, 1, 6, 4, payload.len() as u8]);
    b.extend(payload);
    b.push(0x80);
    packetize(256, &b, cc)
}
