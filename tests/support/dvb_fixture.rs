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
