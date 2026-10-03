//! Bounded TS/PES/SEI caption extraction; no video decode or upstream subscription.
use crate::captions::Decoder;
const LIMIT: usize = 2 * 1024 * 1024;
const MASK: u64 = (1 << 33) - 1;
#[derive(Default)]
pub struct Transport {
    pending: Vec<u8>,
    pmt: Option<u16>,
    video: Option<(u16, bool)>,
    pat_section: Vec<u8>,
    pmt_section: Vec<u8>,
    pes: Vec<u8>,
    pes_pts: Option<u64>,
    counter: Option<u8>,
    last_pts: Option<u64>,
}
impl Transport {
    pub fn push(&mut self, data: &[u8], decoder: &mut Decoder) {
        // Caller drains fixed-size reads. Process arbitrarily large supplied chunks
        // incrementally rather than retaining a caller-sized allocation.
        for part in data.chunks(188 * 64) {
            self.pending.extend(part);
            while self.pending.len() >= 188 {
                if self.pending[0] != 0x47 {
                    self.pending.remove(0);
                    decoder.error = Some("caption_transport_sync");
                    self.pes.clear();
                    decoder.reset(decoder.latest_pts);
                    continue;
                }
                let packet: [u8; 188] = self.pending[..188].try_into().unwrap();
                self.pending.drain(..188);
                self.packet(&packet, decoder);
            }
        }
    }
    fn packet(&mut self, p: &[u8; 188], d: &mut Decoder) {
        let pid = (u16::from(p[1] & 31) << 8) | u16::from(p[2]);
        let start = p[1] & 0x40 != 0;
        let offset = if p[3] & 0x20 != 0 {
            5 + usize::from(p[4])
        } else {
            4
        };
        if offset > 188 || p[1] & 0x80 != 0 || p[3] & 0xc0 != 0 {
            if self.video.is_some_and(|v| v.0 == pid) {
                self.gap(d)
            }
            return;
        }
        if self.video.is_some_and(|v| v.0 == pid)
            && p[3] & 0x20 != 0
            && p[4] > 0
            && p[5] & 0x80 != 0
        {
            self.gap(d)
        }
        if p[3] & 0x10 == 0 || offset == 188 {
            return;
        }
        let bytes = &p[offset..];
        if pid == 0 || self.pmt == Some(pid) {
            let section = if pid == 0 {
                &mut self.pat_section
            } else {
                &mut self.pmt_section
            };
            if start {
                let Some(&pointer) = bytes.first() else {
                    return;
                };
                let off = 1 + usize::from(pointer);
                if off > bytes.len() {
                    section.clear();
                    return;
                }
                section.clear();
                section.extend(&bytes[off..]);
            } else {
                section.extend(bytes)
            }
            if section.len() > 4096 {
                section.clear();
                return;
            }
            if section.len() < 3 {
                return;
            }
            let n = 3 + ((usize::from(section[1] & 15) << 8) | usize::from(section[2]));
            if n < 12 || n > 4096 || section.len() < n {
                return;
            }
            let s = section[..n].to_vec();
            section.clear();
            if crc(&s) != 0 {
                return;
            }
            if pid == 0 {
                for entry in s[8..n - 4].chunks_exact(4) {
                    if entry[0] != 0 || entry[1] != 0 {
                        self.pmt = Some((u16::from(entry[2] & 31) << 8) | u16::from(entry[3]));
                        break;
                    }
                }
            } else {
                let mut at = 12 + ((usize::from(s[10] & 15) << 8) | usize::from(s[11]));
                while at + 5 <= n - 4 {
                    let kind = s[at];
                    if kind == 0x1b || kind == 0x24 {
                        let video = (
                            (u16::from(s[at + 1] & 31) << 8) | u16::from(s[at + 2]),
                            kind == 0x24,
                        );
                        if self.video.is_some_and(|old| old != video) {
                            self.gap(d)
                        }
                        self.video = Some(video);
                        break;
                    }
                    at += 5 + ((usize::from(s[at + 3] & 15) << 8) | usize::from(s[at + 4]));
                }
            }
            return;
        }
        let Some((video, hevc)) = self.video else {
            return;
        };
        if video != pid {
            return;
        }
        let cc = p[3] & 15;
        if self.counter == Some(cc) {
            return;
        }
        if self.counter.is_some_and(|old| (old + 1) & 15 != cc) {
            self.gap(d)
        }
        self.counter = Some(cc);
        if start {
            self.finish(d, hevc);
            self.pes.clear();
            self.pes_pts = None;
            if bytes.len() < 14 || bytes[..3] != [0, 0, 1] || bytes[7] & 0x80 == 0 {
                d.error = Some("caption_pes_header");
                return;
            }
            let Some(raw) = pts(&bytes[9..14]) else {
                d.error = Some("caption_pes_timestamp");
                return;
            };
            let t = if let Some(last) = self.last_pts {
                let delta = ((raw.wrapping_sub(last & MASK).wrapping_add(1 << 32)) & MASK) as i64
                    - (1 << 32);
                last.saturating_add_signed(delta)
            } else {
                raw
            };
            self.last_pts = Some(t);
            d.observe(t);
            self.pes_pts = Some(t);
        }
        if self.pes_pts.is_some() {
            if self.pes.len() + bytes.len() > LIMIT {
                self.gap(d);
                d.error = Some("caption_pes_limit")
            } else {
                self.pes.extend(bytes)
            }
        }
    }
    fn gap(&mut self, d: &mut Decoder) {
        self.pes.clear();
        self.pes_pts = None;
        self.counter = None;
        d.reset(d.latest_pts);
        d.error = Some("caption_transport_gap")
    }
    fn finish(&mut self, d: &mut Decoder, hevc: bool) {
        let Some(t) = self.pes_pts else { return };
        let Some(&n) = self.pes.get(8) else { return };
        let off = 9 + usize::from(n);
        let Some(body) = self.pes.get(off..) else {
            return;
        };
        sei(body, hevc, t, d)
    }
}
pub fn pts(b: &[u8]) -> Option<u64> {
    if b.len() < 5 || b[0] & 1 == 0 || b[2] & 1 == 0 || b[4] & 1 == 0 {
        return None;
    }
    Some(
        (u64::from(b[0] & 14) << 29)
            | (u64::from(b[1]) << 22)
            | (u64::from(b[2] & 254) << 14)
            | (u64::from(b[3]) << 7)
            | u64::from(b[4] >> 1),
    )
}
fn crc(b: &[u8]) -> u32 {
    let mut c = 0xffff_ffffu32;
    for v in b {
        c ^= u32::from(*v) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x80000000 != 0 { 0x04c11db7 } else { 0 }
        }
    }
    c
}
fn sei(body: &[u8], hevc: bool, pts: u64, d: &mut Decoder) {
    let mut boundaries = vec![];
    let mut i = 0;
    while i + 3 <= body.len() {
        if body[i..i + 3] == [0, 0, 1] {
            boundaries.push(i + 3);
            i += 3
        } else {
            i += 1
        }
    }
    boundaries.push(body.len() + 3);
    for pair in boundaries.windows(2) {
        let start = pair[0];
        let end = (pair[1] - 3).min(body.len());
        let Some(nal) = body.get(start..end) else {
            continue;
        };
        if nal.is_empty() {
            continue;
        }
        let skip = if hevc {
            if (nal[0] >> 1) & 63 != 39 && (nal[0] >> 1) & 63 != 40 {
                continue;
            }
            2
        } else {
            if nal[0] & 31 != 6 {
                continue;
            }
            1
        };
        let Some(raw) = nal.get(skip..) else { continue };
        let mut rbsp = Vec::with_capacity(raw.len());
        let mut zeros = 0;
        for &b in raw {
            if zeros >= 2 && b == 3 {
                zeros = 0;
                continue;
            }
            rbsp.push(b);
            zeros = if b == 0 { zeros + 1 } else { 0 };
        }
        let mut at = 0;
        while at + 2 <= rbsp.len() {
            let mut kind = 0usize;
            while at < rbsp.len() && rbsp[at] == 255 {
                kind += 255;
                at += 1
            }
            let Some(&b) = rbsp.get(at) else { break };
            kind += usize::from(b);
            at += 1;
            let mut size = 0usize;
            while at < rbsp.len() && rbsp[at] == 255 {
                size += 255;
                at += 1
            }
            let Some(&b) = rbsp.get(at) else { break };
            size += usize::from(b);
            at += 1;
            let Some(payload) = rbsp.get(at..at.saturating_add(size)) else {
                break;
            };
            if kind == 4
                && payload.len() >= 11
                && payload[..8] == *b"\xb5\x00\x31GA94\x03"
                && payload[8] & 0x40 != 0
            {
                let count = usize::from(payload[8] & 31);
                if 10 + 3 * count < payload.len() {
                    for triple in payload[10..10 + 3 * count].chunks_exact(3) {
                        if triple[0] & 4 != 0 && triple[0] & 3 < 2 {
                            d.push(triple[0] & 3, [triple[1], triple[2]], pts)
                        }
                    }
                }
            }
            at += size;
        }
    }
}
