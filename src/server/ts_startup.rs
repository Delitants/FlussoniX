//! Bounded, per-subscription startup for raw HTTP TS. After startup the delivery
//! path forwards the original Bytes without parsing or retaining a GOP history.
use crate::subtitle_transport::Psi;
use bytes::{Buf, Bytes, BytesMut};

const PREROLL_LIMIT: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Media {
    Video { pid: u16, hevc: bool },
    Audio(u16),
    Other,
}

pub(super) struct Startup {
    pending: BytesMut,
    pat: Psi,
    pmt: Psi,
    pat_section: Vec<u8>,
    pmt_section: Vec<u8>,
    pat_packets: Vec<u8>,
    pmt_packets: Vec<u8>,
    program: Option<(u16, u16)>,
    media: Option<Media>,
    candidate: Vec<u8>,
    pes: Pes,
    counter: Option<u8>,
}
impl Default for Startup {
    fn default() -> Self {
        Self {
            pending: BytesMut::new(),
            pat: Psi::all_current_sections(),
            pmt: Psi::all_current_sections(),
            pat_section: vec![],
            pmt_section: vec![],
            pat_packets: vec![],
            pmt_packets: vec![],
            program: None,
            media: None,
            candidate: vec![],
            pes: Pes::default(),
            counter: None,
        }
    }
}

impl Startup {
    pub(super) fn push(&mut self, bytes: Bytes) -> Option<Bytes> {
        // Worker reads are normally small. Chunk here too so even an oversized
        // caller cannot grow the partial-packet buffer without a fixed bound.
        let mut consumed = 0;
        for part in bytes.chunks(188 * 32) {
            consumed += part.len();
            self.pending.extend_from_slice(part);
            while self.pending.len() >= 188 {
                if self.pending[0] != 0x47 {
                    let skip = self
                        .pending
                        .iter()
                        .position(|b| *b == 0x47)
                        .unwrap_or(self.pending.len());
                    self.pending.advance(skip);
                    self.reset_candidate();
                    continue;
                }
                let packet: [u8; 188] = self.pending[..188].try_into().unwrap();
                self.pending.advance(188);
                if self.packet(&packet) {
                    let mut out = Vec::with_capacity(
                        self.pat_packets.len()
                            + self.pmt_packets.len()
                            + self.candidate.len()
                            + self.pending.len()
                            + bytes.len()
                            - consumed,
                    );
                    out.extend_from_slice(&self.pat_packets);
                    out.extend_from_slice(&self.pmt_packets);
                    out.append(&mut self.candidate);
                    out.extend_from_slice(&self.pending);
                    out.extend_from_slice(&bytes[consumed..]);
                    return Some(out.into());
                }
            }
        }
        None
    }

    fn reset_candidate(&mut self) {
        self.candidate.clear();
        self.pes = Pes::default();
        self.counter = None;
    }

    fn packet(&mut self, p: &[u8; 188]) -> bool {
        let pid = (u16::from(p[1] & 31) << 8) | u16::from(p[2]);
        let start = p[1] & 0x40 != 0;
        let cc = p[3] & 15;
        let afc = (p[3] >> 4) & 3;
        let invalid = p[1] & 0x80 != 0 || p[3] & 0xc0 != 0 || afc == 0;
        let off = if afc & 2 != 0 {
            5 + usize::from(p[4])
        } else {
            4
        };
        let discontinuity = afc & 2 != 0 && p[4] > 0 && p[5] & 0x80 != 0;
        if invalid || off > 188 || discontinuity {
            self.reset_candidate();
            if pid == 0 {
                self.pat = Psi::all_current_sections();
            }
            if self.program.is_some_and(|(_, pmt)| pmt == pid) {
                self.pmt = Psi::all_current_sections();
            }
            // A discontinuity invalidates old assembly, but this same packet
            // may be a complete new PSI/PES start. Only malformed packets are
            // rejected outright; orphan continuations still have no assembly.
            if invalid || off > 188 {
                return false;
            }
        }
        let payload = if afc & 1 != 0 { &p[off..] } else { &[] };
        if pid == 0 && !payload.is_empty() {
            for section in self.pat.push(payload, start, cc) {
                if section[0] != 0 || (section.len() - 12) % 4 != 0 {
                    continue;
                }
                if section != self.pat_section {
                    self.reset_candidate();
                    self.pmt = Psi::all_current_sections();
                    self.pmt_section.clear();
                    self.pmt_packets.clear();
                    self.media = None;
                    let mut programs = section[8..section.len() - 4]
                        .chunks_exact(4)
                        .filter(|e| e[0] != 0 || e[1] != 0);
                    self.program = programs.next().map(|e| {
                        (
                            u16::from_be_bytes([e[0], e[1]]),
                            (u16::from(e[2] & 31) << 8) | u16::from(e[3]),
                        )
                    });
                    // Unsupported multiplexes retain their existing raw behavior.
                    // The qualified startup gate is for single-program A/V.
                    if self.program.is_none() || programs.next().is_some() || section[7] != 0 {
                        self.media = Some(Media::Other);
                    }
                    self.pat_section = section;
                }
                self.pat_packets = table_packets(0, &self.pat_section, cc);
            }
        } else if self.program.is_some_and(|(_, pmt)| pmt == pid) && !payload.is_empty() {
            for section in self.pmt.push(payload, start, cc) {
                if section[0] != 2
                    || section.len() < 16
                    || Some(u16::from_be_bytes([section[3], section[4]]))
                        != self.program.map(|(program, _)| program)
                {
                    continue;
                }
                let Some(media) = (if section[7] == 0 {
                    mapping(&section)
                } else {
                    Some(Media::Other)
                }) else {
                    continue;
                };
                if section != self.pmt_section {
                    self.reset_candidate();
                    self.media = Some(media);
                    self.pmt_section = section;
                }
                self.pmt_packets = table_packets(pid, &self.pmt_section, cc);
            }
        }
        let Some(media) = self.media else {
            return false;
        };
        if media == Media::Other {
            return true;
        }
        let target = match media {
            Media::Video { pid, .. } | Media::Audio(pid) => pid,
            _ => unreachable!(),
        };
        if pid == target && !payload.is_empty() {
            if self.counter.is_some_and(|last| (last + 1) & 15 != cc) {
                self.reset_candidate();
            }
            if start {
                if !self.pes.has_parameters() {
                    self.reset_candidate();
                }
                self.pes.new_header();
            } else if self.candidate.is_empty() {
                return false;
            }
            self.counter = Some(cc);
        }
        if self.candidate.is_empty() && !(pid == target && start && !payload.is_empty()) {
            return false;
        }
        if self.candidate.len() + 188 > PREROLL_LIMIT {
            self.reset_candidate();
            return false;
        }
        self.candidate.extend_from_slice(p);
        if pid == target && !payload.is_empty() {
            match self.pes.push(payload, media) {
                Scan::Ready => return true,
                Scan::Reject => self.reset_candidate(),
                Scan::Wait => {}
            }
        }
        false
    }
}

#[cfg(test)]
#[path = "../../tests/support/ts_startup_tests.rs"]
mod tests;

fn mapping(s: &[u8]) -> Option<Media> {
    let end = s.len() - 4;
    let mut at = 12 + ((usize::from(s[10] & 15) << 8) | usize::from(s[11]));
    let mut video = None;
    let mut audio = None;
    let mut other_video = false;
    while at + 5 <= end {
        let next = at + 5 + ((usize::from(s[at + 3] & 15) << 8) | usize::from(s[at + 4]));
        if next > end {
            return None;
        }
        let pid = (u16::from(s[at + 1] & 31) << 8) | u16::from(s[at + 2]);
        match s[at] {
            0x1b | 0x24 => {
                if video.is_some() {
                    other_video = true;
                }
                video = Some(Media::Video {
                    pid,
                    hevc: s[at] == 0x24,
                });
            }
            2 | 0x10 => other_video = true,
            3 | 4 | 0x0f | 0x11 | 0x81 | 0x87 => {
                audio.get_or_insert(Media::Audio(pid));
            }
            _ => {}
        }
        at = next;
    }
    if at != end {
        return None;
    }
    Some(if other_video {
        Media::Other
    } else {
        video.or(audio).unwrap_or(Media::Other)
    })
}

// Keep validated PSI contents, including subtitle descriptors and CRC, intact.
fn table_packets(pid: u16, section: &[u8], last_cc: u8) -> Vec<u8> {
    let count = (section.len() + 1).div_ceil(184);
    let mut cc = last_cc.wrapping_sub(count as u8 - 1) & 15;
    let mut out = Vec::with_capacity(count * 188);
    let mut at = 0;
    while at < section.len() {
        let first = at == 0;
        let mut p = [0xff; 188];
        p[..4].copy_from_slice(&[
            0x47,
            ((pid >> 8) as u8) | if first { 0x40 } else { 0 },
            pid as u8,
            0x10 | cc,
        ]);
        let off = if first {
            p[4] = 0;
            5
        } else {
            4
        };
        let n = (188 - off).min(section.len() - at);
        p[off..off + n].copy_from_slice(&section[at..at + n]);
        out.extend_from_slice(&p);
        at += n;
        cc = (cc + 1) & 15;
    }
    out
}

enum Scan {
    Wait,
    Ready,
    Reject,
}
#[derive(Default)]
struct Pes {
    header: Vec<u8>,
    header_done: bool,
    zeros: u8,
    nal_header: bool,
    parameters: u8,
}
impl Pes {
    fn has_parameters(&self) -> bool {
        self.parameters != 0
    }
    fn new_header(&mut self) {
        self.header.clear();
        self.header_done = false;
        self.zeros = 0;
        self.nal_header = false;
    }
    fn push(&mut self, bytes: &[u8], media: Media) -> Scan {
        for &b in bytes {
            if !self.header_done {
                self.header.push(b);
                if self.header.len() == 9 {
                    let h = &self.header;
                    let stream_ok = match media {
                        Media::Audio(_) => (0xc0..=0xdf).contains(&h[3]) || h[3] == 0xbd,
                        _ => (0xe0..=0xef).contains(&h[3]),
                    };
                    if h[..3] != [0, 0, 1] || h[6] & 0xc0 != 0x80 || !stream_ok {
                        return Scan::Reject;
                    }
                }
                if self.header.len() >= 9 && self.header.len() == 9 + usize::from(self.header[8]) {
                    self.header_done = true;
                }
                continue;
            }
            if matches!(media, Media::Audio(_)) {
                return Scan::Ready;
            }
            let hevc = matches!(media, Media::Video { hevc: true, .. });
            if self.nal_header {
                self.nal_header = false;
                if b & 0x80 != 0 {
                    return Scan::Reject;
                }
                let kind = if hevc { (b >> 1) & 63 } else { b & 31 };
                if hevc {
                    match kind {
                        32..=34 => self.parameters |= 1 << (kind - 32),
                        16..=21 if self.parameters == 7 => return Scan::Ready,
                        0..=31 => return Scan::Reject,
                        _ => {}
                    }
                } else {
                    match kind {
                        7 => self.parameters |= 1,
                        8 => self.parameters |= 2,
                        5 if self.parameters == 3 => return Scan::Ready,
                        1..=5 => return Scan::Reject,
                        _ => {}
                    }
                }
            }
            if b == 1 && self.zeros >= 2 {
                self.nal_header = true;
            }
            self.zeros = if b == 0 {
                self.zeros.saturating_add(1)
            } else {
                0
            };
        }
        Scan::Wait
    }
}
