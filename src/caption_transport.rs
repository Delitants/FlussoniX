//! Bounded TS/PES/SEI caption extraction; no video decode or upstream subscription.
use crate::captions::Decoder;
const LIMIT: usize = 2 * 1024 * 1024;
type Pair = (u8, [u8; 2]);
struct Timed {
    pts: u64,
    pairs: Vec<Pair>,
}
const MASK: u64 = (1 << 33) - 1;
#[derive(Default)]
pub struct Transport {
    pending: Vec<u8>,
    teletext: crate::teletext_transport::Transport,
    dvb: crate::subtitle_transport::Transport<true>,
    pmt: Option<u16>,
    video: Option<(u16, bool)>,
    pat_section: Vec<u8>,
    pmt_section: Vec<u8>,
    pes: Vec<u8>,
    pes_pts: Option<u64>,
    counter: Option<u8>,
    last_pts: Option<u64>,
    events: Vec<Timed>,
}
impl Transport {
    pub fn push(&mut self, data: &[u8], decoder: &mut Decoder) {
        // Caller drains fixed-size reads. Process arbitrarily large supplied chunks
        // incrementally rather than retaining a caller-sized allocation.
        for part in data.chunks(188 * 64) {
            self.pending.extend(part);
            while self.pending.len() >= 188 {
                if self.pending[0] != 0x47 {
                    let skip = self
                        .pending
                        .iter()
                        .position(|b| *b == 0x47)
                        .unwrap_or(self.pending.len());
                    self.pending.drain(..skip);
                    self.gap(decoder);
                    decoder.error = Some("caption_transport_sync");
                    continue;
                }
                let packet: [u8; 188] = self.pending[..188].try_into().unwrap();
                self.pending.drain(..188);
                self.packet(&packet, decoder);
            }
        }
    }
    fn packet(&mut self, p: &[u8; 188], d: &mut Decoder) {
        self.teletext.packet(p, d);
        self.dvb.packet(p, d);
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
            if !(12..=4096).contains(&n) || section.len() < n {
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
                    if kind == 2 || kind == 0x10 {
                        d.error = Some("caption_video_codec_unsupported");
                        return;
                    }
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
            if bytes.len() < 14 || bytes[..3] != [0, 0, 1] || bytes[7] & 0x80 == 0 {
                self.gap(d);
                d.error = Some("caption_pes_header");
                return;
            }
            let Some(raw) = pts(&bytes[9..14]) else {
                self.gap(d);
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
            let watermark = if bytes[7] & 0xc0 == 0xc0 {
                pts(bytes.get(14..19).unwrap_or(&[]))
                    .map(|raw| {
                        t.saturating_add_signed(
                            ((raw.wrapping_sub(t & MASK).wrapping_add(1 << 32)) & MASK) as i64
                                - (1 << 32),
                        )
                    })
                    .unwrap_or(t)
            } else {
                t
            };
            self.finish(d, hevc, watermark);
            self.pes.clear();
            self.pes_pts = None;
            if self.last_pts.is_some_and(|last| {
                t.saturating_add(180000) < last || t > last.saturating_add(30 * 90000)
            }) {
                self.gap(d);
                d.error = Some("caption_clock_discontinuity");
            }
            self.last_pts = Some(t);
            self.teletext.advance(watermark, d);
            self.dvb.advance(watermark, d);
            d.observe_video(t, watermark);
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
        self.teletext.reset();
        self.dvb.reset();
        self.pes.clear();
        self.events.clear();
        self.pes_pts = None;
        self.counter = None;
        d.reset(d.latest_pts);
        d.error = Some("caption_transport_gap")
    }
    fn finish(&mut self, d: &mut Decoder, hevc: bool, watermark: u64) {
        let Some(t) = self.pes_pts else { return };
        let Some(&n) = self.pes.get(8) else { return };
        let off = 9 + usize::from(n);
        let Some(body) = self.pes.get(off..) else {
            return;
        };
        let pairs = match sei(body, hevc) {
            Ok(pairs) => pairs,
            Err(reason) => {
                self.gap(d);
                d.error = Some(reason);
                return;
            }
        };
        if pairs.len() > 512
            || self.events.len() >= 64
            || self.events.iter().map(|e| e.pairs.len()).sum::<usize>() + pairs.len() > 4096
        {
            self.gap(d);
            d.error = Some("caption_reorder_limit");
            return;
        }
        if !pairs.is_empty() {
            self.events.push(Timed { pts: t, pairs });
            self.events.sort_by_key(|e| e.pts);
        }
        // Embedded pairs call Decoder::push/observe, which executes page
        // deadlines. Apply all due subtitle rows at this safe frontier first.
        self.teletext.advance(watermark, d);
        self.dvb.advance(watermark, d);
        while self.events.first().is_some_and(|e| e.pts <= watermark) {
            let event = self.events.remove(0);
            for (field, pair) in event.pairs {
                d.push(field, pair, event.pts);
            }
        }
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
fn sei(body: &[u8], hevc: bool) -> Result<Vec<Pair>, &'static str> {
    let mut pairs = vec![];
    let mut boundaries = vec![];
    let mut i = 0;
    while i + 3 <= body.len() {
        if body[i..i + 3] == [0, 0, 1] {
            boundaries.push(i + 3);
            if boundaries.len() > 4096 {
                return Err("caption_nal_limit");
            }
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
        while at < rbsp.len() {
            if rbsp[at] == 0x80 && rbsp[at + 1..].iter().all(|b| *b == 0) {
                break;
            }
            let mut kind = 0usize;
            while at < rbsp.len() && rbsp[at] == 255 {
                kind += 255;
                at += 1
            }
            let Some(&b) = rbsp.get(at) else {
                return Err("caption_sei_truncated");
            };
            kind += usize::from(b);
            at += 1;
            let mut size = 0usize;
            while at < rbsp.len() && rbsp[at] == 255 {
                size += 255;
                at += 1
            }
            let Some(&b) = rbsp.get(at) else {
                return Err("caption_sei_truncated");
            };
            size += usize::from(b);
            at += 1;
            let Some(payload) = rbsp.get(at..at.saturating_add(size)) else {
                return Err("caption_sei_truncated");
            };
            if kind == 4 && payload.len() >= 8 && payload[..8] == *b"\xb5\x00\x31GA94\x03" {
                if payload.len() < 11 {
                    return Err("caption_sei_truncated");
                }
                if payload[8] & 0x40 == 0 {
                    at += size;
                    continue;
                }
                let count = usize::from(payload[8] & 31);
                if 10 + 3 * count >= payload.len() {
                    return Err("caption_sei_truncated");
                }
                {
                    for triple in payload[10..10 + 3 * count].chunks_exact(3) {
                        if triple[0] & 4 != 0 {
                            pairs.push((triple[0] & 3, [triple[1], triple[2]]));
                            if pairs.len() > 512 {
                                return Err("caption_reorder_limit");
                            }
                        }
                    }
                }
            }
            at += size;
        }
    }
    Ok(pairs)
}
#[cfg(test)]
mod limits {
    use super::*;
    #[test]
    fn oversized_pes_resets_display_without_retaining_unbounded_data() {
        let track = crate::m4s::Track {
            id: 1,
            codec: "h264".into(),
            config: vec![
                1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
            ],
        };
        let mut mux = crate::worker_ts::Muxer::new(&[track]).unwrap();
        let mut d = Decoder::new(vec![]);
        let parity = |b: u8| b | if b.count_ones() % 2 == 0 { 128 } else { 0 };
        d.push(0, [parity(0x14), parity(0x29)], 0);
        d.push(0, [parity(b'H'), parity(b'I')], 90000);
        let mut body = ((LIMIT + 1024) as u32).to_be_bytes().to_vec();
        body.extend(vec![0x65; LIMIT + 1024]);
        let frame = crate::m4f::Frame {
            track_id: 1,
            dts: 180000,
            pts_offset: 0,
            key: true,
            body,
        };
        let mut t = Transport::default();
        t.push(&mux.tables(), &mut d);
        t.push(&mux.frame(&frame).unwrap(), &mut d);
        assert!(t.pes.len() <= LIMIT);
        assert!(t.pending.len() < 188);
        assert_eq!(d.error, Some("caption_pes_limit"));
        assert!(d.snapshot().iter().all(|c| c.end.is_some()));
    }
}
#[cfg(test)]
mod nal_limit {
    use super::*;
    #[test]
    fn excessive_nals_reset_stale_display_and_report_the_limit() {
        let mut d = Decoder::new(vec![]);
        let mut t = Transport {
            pes: vec![0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 5, 0x21, 0, 1, 0, 1],
            pes_pts: Some(90000),
            ..Default::default()
        };
        for _ in 0..4200 {
            t.pes.extend([0, 0, 0, 1, 0x65, 0x88]);
        }
        t.pes_pts = Some(90000);
        t.finish(&mut d, false, 180000);
        assert_eq!(d.error, Some("caption_nal_limit"));
        assert!(t.pes.is_empty());
    }
}
#[cfg(test)]
mod malformed_registered {
    use super::*;
    #[test]
    fn truncation_resets_display_and_reports_failure() {
        for truncation in [0, 1, 2] {
            let track = crate::m4s::Track {
                id: 1,
                codec: "h264".into(),
                config: vec![
                    1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
                ],
            };
            let mut m = crate::worker_ts::Muxer::new(&[track]).unwrap();
            let parity = |b: u8| b | if b.count_ones() % 2 == 0 { 128 } else { 0 };
            let mut d = Decoder::new(vec![]);
            d.observe(90000);
            d.push(0, [parity(0x14), parity(0x29)], 90000);
            d.push(0, [parity(b'H'), parity(b'I')], 90000);
            let mut payload = b"\xb5\x00\x31GA94\x03".to_vec();
            payload.extend([0x41, 255, 255]);
            let mut nal = vec![
                6,
                4,
                if truncation == 1 {
                    30
                } else {
                    payload.len() as u8
                },
            ];
            nal.extend(payload);
            nal.push(128);
            if truncation == 2 {
                assert!(
                    sei(&[0, 0, 1, 6, 4, 255], false).is_err(),
                    "truncated SEI size header must report failure"
                );
                nal = vec![6, 4, 255];
            }
            let mut body = (nal.len() as u32).to_be_bytes().to_vec();
            body.extend(nal);
            body.extend([0, 0, 0, 2, 0x65, 1]);
            let mut bytes = m.tables();
            bytes.extend(
                m.frame(&crate::m4f::Frame {
                    track_id: 1,
                    dts: 180000,
                    pts_offset: 0,
                    key: true,
                    body,
                })
                .unwrap(),
            );
            bytes.extend(
                m.frame(&crate::m4f::Frame {
                    track_id: 1,
                    dts: 270000,
                    pts_offset: 0,
                    key: true,
                    body: vec![0, 0, 0, 2, 0x65, 1],
                })
                .unwrap(),
            );
            Transport::default().push(&bytes, &mut d);
            assert!(
                d.error.is_some(),
                "malformed recognized captions must report failure"
            );
            assert!(
                d.snapshot().iter().all(|c| c.end.is_some()),
                "malformed captions must close stale display"
            );
        }
    }
}
