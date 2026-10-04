// Independently authored DVB segments and literal pixel code strings.
#![allow(dead_code)]
#[path = "teletext_fixture.rs"]
pub mod carrier;
pub fn seg(kind: u8, page: u16, b: &[u8]) -> Vec<u8> {
    let mut out = vec![15, kind];
    out.extend(page.to_be_bytes());
    out.extend((b.len() as u16).to_be_bytes());
    out.extend(b);
    out
}
pub fn body(segments: &[Vec<u8>]) -> Vec<u8> {
    let mut b = vec![0x20, 0];
    for s in segments {
        b.extend(s);
    }
    b.push(255);
    b
}
pub fn pcs(page: u16, version: u8, state: u8, timeout: u8, regions: &[(u8, u16, u16)]) -> Vec<u8> {
    let mut b = vec![timeout, (version << 4) | (state << 2)];
    for &(id, x, y) in regions {
        b.extend([id, 0]);
        b.extend(x.to_be_bytes());
        b.extend(y.to_be_bytes());
    }
    seg(0x10, page, &b)
}
pub fn region(
    page: u16,
    w: u16,
    h: u16,
    depth: u8,
    fill: bool,
    refs: &[(u16, u16, u16)],
) -> Vec<u8> {
    let mut b = vec![1, u8::from(fill) << 3];
    b.extend(w.to_be_bytes());
    b.extend(h.to_be_bytes());
    b.extend([(depth << 5) | (depth << 2), 0, 0, 0]);
    for &(id, x, y) in refs {
        b.extend(id.to_be_bytes());
        b.extend(x.to_be_bytes());
        b.extend(y.to_be_bytes());
    }
    seg(0x11, page, &b)
}
pub fn object(page: u16, id: u16, version: u8, nonmod: bool, top: &[u8], bottom: &[u8]) -> Vec<u8> {
    let mut b = id.to_be_bytes().to_vec();
    b.push((version << 4) | (u8::from(nonmod) << 1));
    b.extend((top.len() as u16).to_be_bytes());
    b.extend((bottom.len() as u16).to_be_bytes());
    b.extend(top);
    b.extend(bottom);
    if b.len() % 2 != 0 {
        b.push(0);
    }
    seg(0x13, page, &b)
}
pub fn eod(page: u16) -> Vec<u8> {
    seg(0x80, page, &[])
}
pub fn tiny(page: u16) -> Vec<u8> {
    body(&[
        pcs(page, 0, 2, 2, &[(1, 20, 30)]),
        region(page, 2, 2, 1, true, &[(9, 0, 0)]),
        object(
            page,
            9,
            0,
            false,
            &[0x10, 0x44, 0, 0xf0],
            &[0x10, 0x14, 0, 0xf0],
        ),
        eod(page),
    ])
}
pub fn tables(bindings: &[(u16, u16, u16)], version: u8) -> Vec<u8> {
    let mut pat = vec![0, 0xb0, 0, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0];
    let mut out = carrier::section(0, std::mem::take(&mut pat), &mut version.clone());
    let mut pmt = vec![
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
    for &(pid, page, ancillary) in bindings {
        let mut desc = vec![0x59, 8, b'e', b'n', b'g', 0x10];
        desc.extend(page.to_be_bytes());
        desc.extend(ancillary.to_be_bytes());
        pmt.extend([
            6,
            0xe0 | (pid >> 8) as u8,
            pid as u8,
            0xf0,
            desc.len() as u8,
        ]);
        pmt.extend(desc);
    }
    out.extend(carrier::section(4096, pmt, &mut version.clone()));
    out
}
pub fn glyph(text: &str) -> flussonix::dvb::Image {
    use std::process::Command;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("words.txt");
    let output = dir.path().join("glyph.pgm");
    std::fs::write(&input, text).unwrap();
    let status=Command::new("ffmpeg").args(["-nostdin","-v","error","-f","lavfi","-i","color=white:size=640x80","-vf"])
        .arg(format!("drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf:textfile={}:x=15:y=8:fontsize=48:fontcolor=black",input.display()))
        .args(["-frames:v","1","-threads","1","-y"]).arg(&output).status().unwrap();
    assert!(status.success());
    let bytes = std::fs::read(output).unwrap();
    let mut at = 0;
    let mut fields = vec![];
    for _ in 0..4 {
        while bytes[at].is_ascii_whitespace() {
            at += 1
        }
        let start = at;
        while !bytes[at].is_ascii_whitespace() {
            at += 1
        }
        fields.push(std::str::from_utf8(&bytes[start..at]).unwrap());
    }
    assert_eq!(fields, vec!["P5", "640", "80", "255"]);
    at += 1;
    assert_eq!(bytes.len() - at, 640 * 80);
    flussonix::dvb::Image {
        width: 640,
        height: 80,
        pixels: bytes[at..]
            .iter()
            .map(|&v| if v < 128 { [255, 255] } else { [0, 0] })
            .collect(),
    }
}
pub const ENG_DESC: &[u8] = &[0x59, 8, b'e', b'n', b'g', 0x10, 0, 1, 0, 11];
pub const DEU_DESC: &[u8] = &[0x59, 8, b'd', b'e', b'u', 0x10, 0, 2, 0, 12];
pub fn bitmap(page: u16, text: &str) -> Vec<u8> {
    if text.is_empty() {
        return body(&[pcs(page, 0, 2, 2, &[]), eod(page)]);
    }
    let image = glyph(text);
    let mut fields = [vec![], vec![]];
    for y in 0..image.height {
        let f = &mut fields[y % 2];
        f.push(0x12);
        let mut x = 0;
        while x < image.width {
            if image.pixels[y * image.width + x][1] != 0 {
                f.push(1);
                x += 1
            } else {
                let start = x;
                while x < image.width
                    && image.pixels[y * image.width + x][1] == 0
                    && x - start < 127
                {
                    x += 1
                }
                f.extend([0, (x - start) as u8]);
            }
        }
        f.extend([0, 0, 0xf0]);
    }
    let clut = seg(0x12, page, &[0, 0, 1, 0x21, 235, 128, 128, 0]);
    let out = body(&[
        pcs(page, 0, 2, 2, &[(1, 20, 30)]),
        region(page, 640, 80, 3, true, &[(9, 0, 0)]),
        clut,
        object(page, 9, 0, false, &fields[0], &fields[1]),
        eod(page),
    ]);
    assert!(out.len() < 65527);
    out
}
pub fn transport() -> Vec<u8> {
    use carrier::original;
    static MEDIA: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    let bytes = MEDIA
        .get_or_init(|| {
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
            let events = [(1, bitmap(1, "EUROPE DVB")), (2, bitmap(2, "GRÜSSE"))];
            let later = [(1, bitmap(1, "LIVE <&>")), (2, bitmap(2, "LIVE <&>"))];
            let mut out = vec![];
            let (mut a, mut b, mut frame) = (0, 0, 0);
            for p in std::fs::read(file).unwrap().chunks_exact(188) {
                if original::pid(p) == 4096 {
                    let payload = original::payload(p).unwrap();
                    let start = 1 + usize::from(payload[0]);
                    let s = &payload[start..];
                    let len = 3 + ((usize::from(s[1] & 15) << 8) | usize::from(s[2]));
                    let mut s = s[..len - 4].to_vec();
                    for (pid, desc) in [(0x121u16, ENG_DESC), (0x122, DEU_DESC)] {
                        s.extend([
                            6,
                            0xe0 | (pid >> 8) as u8,
                            pid as u8,
                            0xf0,
                            desc.len() as u8,
                        ]);
                        s.extend(desc);
                    }
                    out.extend(carrier::section(4096, s, &mut (p[3] & 15)));
                } else {
                    out.extend(p)
                }
                if original::pid(p) == 256 && p[1] & 0x40 != 0 {
                    let t = original::pts(original::payload(p).unwrap()).unwrap();
                    if [0, 29, 75, 103, 150].contains(&frame) {
                        for (i, (page, _)) in events.iter().enumerate() {
                            let body = match frame {
                                29 => events[i].1.clone(),
                                103 => later[i].1.clone(),
                                _ => bitmap(*page, ""),
                            };
                            out.extend(carrier::pes(
                                0x121 + i as u16,
                                t,
                                &body,
                                if i == 0 { &mut a } else { &mut b },
                            ));
                        }
                    }
                    frame += 1;
                }
            }
            out
        })
        .clone();
    if let Ok(file) = std::env::var("FLUSSONIX_DVB_FIXTURE_FILE") {
        std::fs::write(file, &bytes).unwrap()
    }
    bytes
}
pub fn mixed_transport() -> Vec<u8> {
    use carrier::original;
    let mut out = vec![];
    let (mut frame, mut cc) = (0, 0);
    for p in transport().chunks_exact(188) {
        if original::pid(p) == 4096 {
            let payload = original::payload(p).unwrap();
            let start = 1 + usize::from(payload[0]);
            let s = &payload[start..];
            let len = 3 + ((usize::from(s[1] & 15) << 8) | usize::from(s[2]));
            let mut s = s[..len - 4].to_vec();
            s.extend([6, 0xe1, 0x23, 0xf0, 7]);
            s.extend(carrier::TTX888_DESC);
            out.extend(carrier::section(4096, s, &mut (p[3] & 15)));
        } else {
            out.extend(p)
        }
        if original::pid(p) == 256 && p[1] & 0x40 != 0 {
            let t = original::pts(original::payload(p).unwrap()).unwrap();
            if [0, 29, 75, 103, 150].contains(&frame) {
                let words = if frame == 29 {
                    &b"TELETEXT"[..]
                } else {
                    &b""[..]
                };
                let body = carrier::page_body(888, 1, words);
                let n = body.len() + 39;
                let mut pes = vec![0, 0, 1, 0xbd, (n >> 8) as u8, n as u8, 0x84, 0x80, 36];
                pes.extend(carrier::pts(t, 0x20));
                pes.extend([0xff; 31]);
                pes.extend(body);
                out.extend(carrier::packetize(0x123, &pes, &mut cc));
            }
            frame += 1;
        }
    }
    out
}
