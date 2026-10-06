//! First-program audio metadata from bounded, CRC-checked transport PSI.
use crate::subtitle_transport::Psi;
#[derive(Default)]
pub(crate) struct Probe {
    pending: Vec<u8>,
    pat: Psi,
    pmt: Psi,
    program: Option<(u16, u16)>,
    pub audio: Option<Option<u8>>,
    pub audio_types: Vec<u8>,
    pub unsupported_tracks: bool,
    pub video: Option<u8>,
}
impl Probe {
    pub fn push(&mut self, bytes: &[u8]) {
        for part in bytes.chunks(188 * 64) {
            self.pending.extend(part);
            while self.pending.len() >= 188 {
                if self.pending[0] != 0x47 {
                    let skip = self
                        .pending
                        .iter()
                        .position(|b| *b == 0x47)
                        .unwrap_or(self.pending.len());
                    self.pending.drain(..skip);
                    continue;
                }
                let packet: [u8; 188] = self.pending[..188].try_into().unwrap();
                self.pending.drain(..188);
                self.packet(&packet);
                if self.audio.is_some() {
                    return;
                }
            }
        }
    }
    fn packet(&mut self, p: &[u8; 188]) {
        if p[1] & 0x80 != 0 || p[3] & 0xc0 != 0 || p[3] & 0x10 == 0 {
            return;
        }
        let offset = if p[3] & 0x20 != 0 {
            5 + usize::from(p[4])
        } else {
            4
        };
        if offset >= 188 {
            return;
        }
        let pid = (u16::from(p[1] & 31) << 8) | u16::from(p[2]);
        let start = p[1] & 0x40 != 0;
        if pid == 0 {
            for s in self.pat.push(&p[offset..], start, p[3] & 15) {
                if s[0] != 0 || (s.len() - 12) % 4 != 0 {
                    continue;
                }
                for entry in s[8..s.len() - 4].chunks_exact(4) {
                    let program = u16::from_be_bytes([entry[0], entry[1]]);
                    if program != 0 {
                        self.program = Some((
                            program,
                            (u16::from(entry[2] & 31) << 8) | u16::from(entry[3]),
                        ));
                        break;
                    }
                }
            }
        } else if self.program.is_some_and(|(_, pmt)| pmt == pid) {
            for s in self.pmt.push(&p[offset..], start, p[3] & 15) {
                if s[0] != 2
                    || s.len() < 16
                    || Some(u16::from_be_bytes([s[3], s[4]]))
                        != self.program.map(|(program, _)| program)
                {
                    continue;
                }
                let end = s.len() - 4;
                let mut at = 12 + ((usize::from(s[10] & 15) << 8) | usize::from(s[11]));
                let mut audio = None;
                let mut audio_types = Vec::new();
                let mut unsupported_tracks = false;
                let mut video = None;
                while at + 5 <= end {
                    let next =
                        at + 5 + ((usize::from(s[at + 3] & 15) << 8) | usize::from(s[at + 4]));
                    if next > end {
                        return;
                    }
                    if video.is_none() && [2, 0x10, 0x1b, 0x24].contains(&s[at]) {
                        video = Some(s[at]);
                    }
                    if [3, 4, 0x0f, 0x11, 0x81, 0x87].contains(&s[at]) {
                        if audio.is_none() {
                            audio = Some(s[at]);
                        }
                        audio_types.push(s[at]);
                    } else if ![0x1b, 0x24].contains(&s[at]) {
                        unsupported_tracks = true;
                    }
                    at = next;
                }
                if at == end {
                    self.audio = Some(audio);
                    self.audio_types = audio_types;
                    self.unsupported_tracks = unsupported_tracks;
                    self.video = video;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/ts_profile_tests.rs"]
mod tests;
