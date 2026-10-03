// Independently authored transport fixtures. No reference-server media or code.
use std::collections::BTreeMap;
use std::process::Command;

pub const DVB_DESC: &[u8] = &[0x59, 8, b'e', b'n', b'g', 0x10, 0, 1, 0, 2];
pub const TTX_DESC: &[u8] = &[0x56, 5, b'd', b'e', b'u', 0x10, 0x88];
pub const DVB_BODY: &[u8] = &[
    0x20, 0x00, 0x0f, 0x10, 0, 1, 0, 2, 2, 4, 0x0f, 0x80, 0, 1, 0, 0, 0xff,
];

pub fn crc(bytes: &[u8]) -> u32 {
    let mut c = 0xffff_ffffu32;
    for b in bytes {
        c ^= u32::from(*b) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x8000_0000 != 0 { 0x04c1_1db7 } else { 0 };
        }
    }
    c
}
fn parity(b: u8) -> u8 {
    b | if b.count_ones() % 2 == 0 { 0x80 } else { 0 }
}
fn ham(n: u8) -> u8 {
    [
        0x15, 0x02, 0x49, 0x5e, 0x64, 0x73, 0x38, 0x2f, 0xd0, 0xc7, 0x8c, 0x9b, 0xa1, 0xb6, 0xfd,
        0xea,
    ][usize::from(n)]
}
pub fn teletext_body() -> Vec<u8> {
    let mut out = vec![0x10];
    // Magazine eight, subtitle page 888, followed by a visible row.
    for row in [0u8, 1] {
        let mut unit = vec![
            0x03,
            44,
            0x20,
            0xe4,
            ham(row << 3).reverse_bits(),
            ham(row >> 1).reverse_bits(),
        ];
        let mut text = [parity(b' '); 40];
        if row == 0 {
            text[..8].copy_from_slice(&[
                ham(8),
                ham(8),
                ham(0),
                ham(0),
                ham(0),
                ham(8),
                ham(0),
                ham(0),
            ]);
        } else {
            for (slot, b) in text.iter_mut().zip(b"EUROPE TELETEXT") {
                *slot = parity(*b);
            }
        }
        unit.extend(text.map(u8::reverse_bits));
        out.extend(unit);
    }
    out
}
pub fn payload(packet: &[u8]) -> Option<&[u8]> {
    if packet.len() != 188 || packet[0] != 0x47 || packet[3] & 0x10 == 0 {
        return None;
    }
    let off = if packet[3] & 0x20 != 0 {
        5 + usize::from(packet[4])
    } else {
        4
    };
    packet.get(off..)
}
pub fn pid(p: &[u8]) -> u16 {
    u16::from(p[1] & 31) << 8 | u16::from(p[2])
}
pub fn pts(pes: &[u8]) -> Option<u64> {
    let p = pes.get(9..14)?;
    Some(
        (u64::from(p[0] & 14) << 29)
            | (u64::from(p[1]) << 22)
            | (u64::from(p[2] & 254) << 14)
            | (u64::from(p[3]) << 7)
            | u64::from(p[4] >> 1),
    )
}
fn encode_pts(t: u64) -> [u8; 5] {
    [
        0x21 | ((t >> 29) as u8 & 14),
        (t >> 22) as u8,
        ((t >> 14) as u8 & 254) | 1,
        (t >> 7) as u8,
        ((t << 1) as u8) | 1,
    ]
}
fn packetize(id: u16, body: &[u8], cc: &mut u8) -> Vec<u8> {
    let mut out = vec![];
    for (i, chunk) in body.chunks(184).enumerate() {
        let mut p = vec![0xff; 188];
        p[0] = 0x47;
        p[1] = (id >> 8) as u8 | if i == 0 { 0x40 } else { 0 };
        p[2] = id as u8;
        p[3] = 0x10 | *cc;
        *cc = (*cc + 1) & 15;
        let off = if chunk.len() < 184 {
            p[3] |= 0x20;
            p[4] = (183 - chunk.len()) as u8;
            if p[4] > 0 {
                p[5] = 0;
            }
            5 + usize::from(p[4])
        } else {
            4
        };
        p[off..].copy_from_slice(chunk);
        out.extend(p);
    }
    out
}
fn pes(id: u16, t: u64, body: &[u8], cc: &mut u8) -> Vec<u8> {
    let size = (body.len() + 8) as u16;
    let mut p = vec![0, 0, 1, 0xbd, (size >> 8) as u8, size as u8, 0x80, 0x80, 5];
    p.extend(encode_pts(t));
    p.extend(body);
    packetize(id, &p, cc)
}
pub fn transport() -> Vec<u8> {
    transport_for("8")
}
pub fn transport_for(seconds: &str) -> Vec<u8> {
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("owned.ts");
    let output = Command::new("ffmpeg")
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
            seconds,
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
            "-f",
            "mpegts",
        ])
        .arg(&file)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data = std::fs::read(file).unwrap();
    let mut out = vec![];
    let mut dc = 0;
    let mut tc = 0;
    let mut frames = 0;
    for p in data.chunks_exact(188) {
        if pid(p) == 4096 {
            let b = payload(p).unwrap();
            let start = 1 + usize::from(b[0]);
            let section = &b[start..];
            let len = 3 + ((usize::from(section[1] & 15) << 8) | usize::from(section[2]));
            let mut section = section[..len - 4].to_vec();
            for (id, desc) in [(0x120u16, DVB_DESC), (0x121, TTX_DESC)] {
                section.extend([
                    6,
                    0xe0 | ((id >> 8) as u8),
                    id as u8,
                    0xf0,
                    desc.len() as u8,
                ]);
                section.extend(desc);
            }
            let len = section.len() + 1;
            section[1] = 0xb0 | ((len >> 8) as u8);
            section[2] = len as u8;
            section.extend(crc(&section).to_be_bytes());
            let mut replaced = p.to_vec();
            replaced[4..].fill(0xff);
            replaced[4] = 0;
            replaced[5..5 + section.len()].copy_from_slice(&section);
            out.extend(replaced);
        } else {
            out.extend(p);
        }
        if pid(p) == 256 && p[1] & 0x40 != 0 {
            frames += 1;
            if frames % 10 == 1 {
                let t = pts(payload(p).unwrap()).unwrap();
                out.extend(pes(0x120, t, DVB_BODY, &mut dc));
                out.extend(pes(0x121, t, &teletext_body(), &mut tc));
            }
        }
    }
    out
}
// Reassemble PSI/PES from transport packets, honoring adaptation fields and length.
pub fn descriptors(ts: &[u8]) -> BTreeMap<u16, Vec<u8>> {
    for p in ts.chunks_exact(188) {
        if pid(p) == 4096 && p[1] & 0x40 != 0 {
            let b = payload(p).unwrap();
            let section = &b[1 + usize::from(b[0])..];
            let len = 3 + ((usize::from(section[1] & 15) << 8) | usize::from(section[2]));
            assert_eq!(crc(&section[..len]), 0);
            let mut i = 12 + ((usize::from(section[10] & 15) << 8) | usize::from(section[11]));
            let mut out = BTreeMap::new();
            while i < len - 4 {
                let id = u16::from(section[i + 1] & 31) << 8 | u16::from(section[i + 2]);
                let n = (usize::from(section[i + 3] & 15) << 8) | usize::from(section[i + 4]);
                out.insert(id, section[i + 5..i + 5 + n].to_vec());
                i += 5 + n;
            }
            return out;
        }
    }
    BTreeMap::new()
}
pub fn pes_bodies(ts: &[u8], id: u16) -> Vec<Vec<u8>> {
    let mut packets = vec![];
    let mut current = vec![];
    for p in ts.chunks_exact(188).filter(|p| pid(p) == id) {
        if p[1] & 0x40 != 0 && !current.is_empty() {
            packets.push(std::mem::take(&mut current));
        }
        if let Some(b) = payload(p) {
            current.extend(b);
        }
    }
    if !current.is_empty() {
        packets.push(current);
    }
    packets
        .into_iter()
        .filter_map(|p| {
            if p.len() < 9 || p[..3] != [0, 0, 1] {
                return None;
            }
            let end = 6 + (usize::from(p[4]) << 8 | usize::from(p[5]));
            let start = 9 + usize::from(p[8]);
            (end > start && end <= p.len()).then(|| p[start..end].to_vec())
        })
        .collect()
}
