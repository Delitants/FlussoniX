//! Independent, bounded MPEG-TS worker output reconstruction.
//!
//! This library is not yet connected to the active worker. The initial profile
//! accepts one stable program, one picture per video PES, AVC/single-layer HEVC,
//! unprotected AAC-LC ADTS and MPEG Layers II/III. Caller chunks are at most
//! 188*64 bytes. Errors are terminal; restart with a fresh decoder generation.
mod audio;
mod video;
use crate::{m4f::Frame, m4s::Track};
use std::collections::BTreeMap;
const PES_LIMIT: usize = 16 * 1024 * 1024;
const MEDIA_LIMIT: usize = 32 * 1024 * 1024;
const MASK: u64 = (1 << 33) - 1;
#[derive(Debug)]
pub enum Event {
    Info(Vec<Track>),
    Frame(Frame),
}
#[derive(Default)]
pub struct Decoder {
    transport: Vec<u8>,
    packets: BTreeMap<u16, [u8; 188]>,
    pat: Psi,
    pmt: Psi,
    program: Option<(u16, u16)>,
    streams: BTreeMap<u16, Stream>,
    anchor: Option<u64>,
    waiting: Vec<Frame>,
    waiting_bytes: usize,
    published: bool,
    ended: bool,
    failed: bool,
}
struct Stream {
    ty: u8,
    pes: Vec<u8>,
    last: Option<u64>,
    audio: audio::Audio,
    video: video::Video,
}
impl Stream {
    fn new(ty: u8) -> Self {
        Self {
            ty,
            pes: vec![],
            last: None,
            audio: audio::Audio::default(),
            video: video::Video::default(),
        }
    }
    fn video(&self) -> bool {
        matches!(self.ty, 0x1b | 0x24)
    }
    fn track(&self) -> Option<&Track> {
        if self.video() {
            self.video.track()
        } else {
            self.audio.track()
        }
    }
}
impl Decoder {
    pub fn push(&mut self, data: &[u8]) -> Result<Vec<Event>, String> {
        if self.failed || self.ended {
            return Err("worker decoder generation is closed".into());
        }
        let r = self.consume(data);
        if r.is_err() {
            self.failed = true;
        }
        r
    }
    fn consume(&mut self, data: &[u8]) -> Result<Vec<Event>, String> {
        if data.len() > 188 * 64 {
            return Err("worker decoder input chunk exceeds bound".into());
        }
        self.transport.extend_from_slice(data);
        let mut out = vec![];
        let complete = self.transport.len() / 188 * 188;
        for at in (0..complete).step_by(188) {
            let p: [u8; 188] = self.transport[at..at + 188].try_into().unwrap();
            self.packet(&p, &mut out)?;
            let retained = self
                .streams
                .values()
                .map(|s| s.pes.len() + s.audio.retained() + s.video.retained())
                .sum::<usize>()
                + self.waiting_bytes
                + out
                    .iter()
                    .map(|e| {
                        if let Event::Frame(f) = e {
                            f.body.len()
                        } else {
                            0
                        }
                    })
                    .sum::<usize>();
            if retained > MEDIA_LIMIT {
                return Err("worker decoder retained media exceeds bound".into());
            }
        }
        self.transport.drain(..complete);
        Ok(out)
    }
    fn packet(&mut self, p: &[u8; 188], out: &mut Vec<Event>) -> Result<(), String> {
        if p[0] != 0x47 || p[1] & 0x80 != 0 || p[3] & 0xc0 != 0 {
            return Err("invalid/scrambled worker TS packet".into());
        }
        let pid = (u16::from(p[1] & 31) << 8) | u16::from(p[2]);
        let start = p[1] & 0x40 != 0;
        let control = (p[3] >> 4) & 3;
        if control == 0 {
            return Err("invalid worker adaptation control".into());
        }
        let mut at = 4;
        if control & 2 != 0 {
            let n = usize::from(p[4]);
            at = 5 + n;
            if at > 188 || (control == 2 && at != 188) || (control == 3 && at >= 188) {
                return Err("invalid worker adaptation length".into());
            }
            adaptation(&p[5..at])?;
        }
        if control & 1 == 0 || pid == 8191 {
            return Ok(());
        }
        let tracked = pid == 0
            || self.program.is_some_and(|(_, pmt)| pid == pmt)
            || self.streams.contains_key(&pid);
        if !tracked {
            return Ok(());
        }
        if let Some(old) = self.packets.get(&pid) {
            if old[3] & 15 == p[3] & 15 {
                if old == p {
                    return Ok(());
                }
                return Err("conflicting duplicate worker TS packet".into());
            }
            if p[3] & 15 != ((old[3] + 1) & 15) {
                return Err("worker TS continuity gap".into());
            }
        }
        self.packets.insert(pid, *p);
        let payload = &p[at..];
        if pid == 0 {
            for section in self.pat.push(payload, start)? {
                self.program(&section)?;
            }
        } else if self.program.is_some_and(|(_, pmt)| pid == pmt) {
            for section in self.pmt.push(payload, start)? {
                self.layout(&section)?;
            }
        } else {
            let s = self.streams.get_mut(&pid).unwrap();
            if start && !s.pes.is_empty() {
                self.flush(pid, out, false)?;
            }
            let s = self.streams.get_mut(&pid).unwrap();
            if s.pes.is_empty() && !start {
                return Err("worker PES continuation without start".into());
            }
            if s.pes.len() + payload.len() > PES_LIMIT {
                return Err("worker PES exceeds bound".into());
            }
            s.pes.extend_from_slice(payload);
            if s.pes.len() >= 6 {
                let n = u16::from_be_bytes([s.pes[4], s.pes[5]]) as usize;
                if n != 0 && s.pes.len() >= n + 6 {
                    if s.pes.len() != n + 6 {
                        return Err("worker PES has trailing payload".into());
                    }
                    self.flush(pid, out, true)?;
                }
            }
        }
        Ok(())
    }
    fn program(&mut self, b: &[u8]) -> Result<(), String> {
        if b[0] != 0 || b.len() < 16 || (b.len() - 12) % 4 != 0 {
            return Err("invalid worker PAT".into());
        }
        let mut programs = vec![];
        for e in b[8..b.len() - 4].chunks_exact(4) {
            let program = u16::from_be_bytes([e[0], e[1]]);
            if program != 0 {
                programs.push((program, (u16::from(e[2] & 31) << 8) | u16::from(e[3])));
            }
        }
        if programs.len() != 1 || programs[0].1 == 0 || programs[0].1 == 8191 {
            return Err("worker requires one valid program".into());
        }
        if self.program.is_some_and(|old| old != programs[0]) {
            return Err("worker program changed".into());
        }
        self.program = Some(programs[0]);
        Ok(())
    }
    fn layout(&mut self, b: &[u8]) -> Result<(), String> {
        if b[0] != 2
            || b.len() < 16
            || self
                .program
                .is_none_or(|(program, _)| program != u16::from_be_bytes([b[3], b[4]]))
        {
            return Err("invalid worker PMT".into());
        }
        let end = b.len() - 4;
        let descriptors = (usize::from(b[10] & 15) << 8) | usize::from(b[11]);
        let mut at = 12 + descriptors;
        if at > end {
            return Err("worker PMT descriptors exceed section".into());
        }
        let mut layout = BTreeMap::new();
        let mut videos = 0;
        while at < end {
            let e = b
                .get(at..at + 5)
                .filter(|_| at + 5 <= end)
                .ok_or("truncated worker PMT stream")?;
            let ty = e[0];
            let pid = (u16::from(e[1] & 31) << 8) | u16::from(e[2]);
            let n = (usize::from(e[3] & 15) << 8) | usize::from(e[4]);
            at += 5 + n;
            if at > end
                || pid < 16
                || pid == 8191
                || self.program.is_some_and(|(_, pmt)| pid == pmt)
                || layout.insert(pid, ty).is_some()
            {
                return Err("invalid/duplicate worker elementary PID".into());
            }
            if !matches!(ty, 0x1b | 0x24 | 0x0f | 3 | 4) {
                return Err("unsupported worker elementary codec".into());
            }
            videos += usize::from(matches!(ty, 0x1b | 0x24));
        }
        if layout.is_empty() || layout.len() > 16 || videos > 1 {
            return Err("unsupported worker track layout".into());
        }
        if !self.streams.is_empty() {
            if layout != self.streams.iter().map(|(pid, s)| (*pid, s.ty)).collect() {
                return Err("worker track layout changed".into());
            }
        } else {
            self.streams = layout
                .into_iter()
                .map(|(pid, ty)| (pid, Stream::new(ty)))
                .collect();
        }
        Ok(())
    }
    fn clock(&mut self, raw: u64) -> Result<u64, String> {
        let expanded = if let Some(anchor) = self.anchor {
            let delta = ((raw.wrapping_sub(anchor & MASK).wrapping_add(1 << 32)) & MASK) as i64
                - (1i64 << 32);
            let mut stamp = i128::from(anchor) + i128::from(delta);
            if delta.unsigned_abs() > 60 * 90000 {
                return Err("worker program timestamp discontinuity".into());
            }
            // The first completed PID may start just after wrap while another
            // PID's first PES starts just before it. No metadata or frames have
            // escaped yet, so choose a common nonnegative initialization epoch.
            if stamp < 0 && !self.published {
                self.shift_initial_epoch()?;
                stamp += 1i128 << 33;
            }
            if stamp < 0 || stamp > i128::from(u64::MAX) {
                return Err("worker program timestamp discontinuity".into());
            }
            stamp as u64
        } else {
            raw
        };
        self.anchor = Some(self.anchor.map_or(expanded, |a| a.max(expanded)));
        Ok(expanded)
    }
    fn shift_initial_epoch(&mut self) -> Result<(), String> {
        let shift = 1u64 << 33;
        let add = |value: u64| value.checked_add(shift).ok_or("worker epoch overflow");
        self.anchor = self.anchor.map(add).transpose()?;
        for frame in &mut self.waiting {
            frame.dts = add(frame.dts)?;
        }
        for stream in self.streams.values_mut() {
            stream.last = stream.last.map(add).transpose()?;
            stream.audio.shift_epoch(shift)?;
            stream.video.shift_epoch(shift)?;
        }
        Ok(())
    }
    fn flush(&mut self, pid: u16, out: &mut Vec<Event>, complete: bool) -> Result<(), String> {
        let s = self.streams.get_mut(&pid).unwrap();
        let b = std::mem::take(&mut s.pes);
        if b.len() < 9 || b[..3] != [0, 0, 1] || b[6] & 0xf0 != 0x80 || b[7] & 0x3f != 0 {
            return Err("invalid worker PES header".into());
        }
        if (s.video() && b[3] & 0xf0 != 0xe0) || (!s.video() && b[3] & 0xe0 != 0xc0) {
            return Err("worker PES stream type mismatch".into());
        }
        let n = u16::from_be_bytes([b[4], b[5]]) as usize;
        if (n == 0 && !s.video()) || (n != 0 && b.len() != n + 6) || (complete && n == 0) {
            return Err("incomplete/unsupported worker PES length".into());
        }
        let flags = b[7] >> 6;
        let len = usize::from(b[8]);
        let at = 9 + len;
        if at >= b.len() || !matches!(flags, 2 | 3) || len < if flags == 3 { 10 } else { 5 } {
            return Err("worker PES requires PTS/DTS".into());
        }
        let pts = timestamp(&b[9..14], if flags == 3 { 3 } else { 2 })?;
        let raw = if flags == 3 {
            timestamp(&b[14..19], 1)?
        } else {
            pts
        };
        let offset = ((pts.wrapping_sub(raw).wrapping_add(1 << 32)) & MASK) as i64 - (1i64 << 32);
        if offset.unsigned_abs() > 60 * 90000 {
            return Err("worker composition offset exceeds bound".into());
        }
        let dts = self.clock(raw)?;
        let s = self.streams.get_mut(&pid).unwrap();
        if s.last
            .is_some_and(|last| dts < last || dts - last > 60 * 90000)
        {
            return Err("worker PES DTS discontinuity".into());
        }
        s.last = Some(dts);
        let frames = if s.video() {
            s.video
                .push(pid, s.ty, &b[at..], dts, offset)?
                .into_iter()
                .collect()
        } else {
            s.audio.push(pid, s.ty, &b[at..], dts, offset)?
        };
        let bytes = frames
            .iter()
            .map(|f| f.body.len() + std::mem::size_of::<Frame>())
            .sum::<usize>();
        if self.waiting.len() + frames.len() > 65536 || self.waiting_bytes + bytes > MEDIA_LIMIT {
            return Err("worker initialization queue exceeds sample/byte bound".into());
        }
        self.waiting_bytes += bytes;
        self.waiting.extend(frames);
        self.emit(out);
        Ok(())
    }
    fn emit(&mut self, out: &mut Vec<Event>) {
        if !self.published {
            let tracks: Option<Vec<_>> =
                self.streams.values().map(|s| s.track().cloned()).collect();
            if let Some(tracks) = tracks.filter(|t| !t.is_empty()) {
                out.push(Event::Info(tracks));
                self.published = true;
            }
        }
        if self.published {
            out.extend(self.waiting.drain(..).map(Event::Frame));
            self.waiting_bytes = 0;
        }
    }
    pub fn finish(&mut self) -> Result<Vec<Event>, String> {
        if self.failed || self.ended {
            return Err("worker decoder generation is closed".into());
        }
        let r = self.end();
        if r.is_err() {
            self.failed = true;
        } else {
            self.ended = true;
        }
        r
    }
    fn end(&mut self) -> Result<Vec<Event>, String> {
        if !self.transport.is_empty() || !self.pat.bytes.is_empty() || !self.pmt.bytes.is_empty() {
            return Err("incomplete worker transport/PSI".into());
        }
        let mut out = vec![];
        let pids: Vec<_> = self.streams.keys().copied().collect();
        for pid in pids {
            if !self.streams[&pid].pes.is_empty() {
                self.flush(pid, &mut out, false)?;
            }
            self.streams[&pid].audio.finish()?;
        }
        if !self.published {
            return Err("incomplete worker track initialization".into());
        }
        Ok(out)
    }
}
fn timestamp(b: &[u8], kind: u8) -> Result<u64, String> {
    if b.len() != 5 || b[0] >> 4 != kind || b[0] & 1 != 1 || b[2] & 1 != 1 || b[4] & 1 != 1 {
        return Err("invalid worker timestamp markers".into());
    }
    Ok((u64::from((b[0] >> 1) & 7) << 30)
        | (u64::from(b[1]) << 22)
        | (u64::from(b[2] >> 1) << 15)
        | (u64::from(b[3]) << 7)
        | u64::from(b[4] >> 1))
}
fn adaptation(b: &[u8]) -> Result<(), String> {
    if b.is_empty() {
        return Ok(());
    }
    if b[0] & 0x80 != 0 {
        return Err("worker transport discontinuity".into());
    }
    let mut at = 1;
    for flag in [0x10, 0x08] {
        if b[0] & flag != 0 {
            let p = b.get(at..at + 6).ok_or("truncated worker PCR")?;
            if p[4] & 0x7e != 0x7e || ((u16::from(p[4] & 1) << 8) | u16::from(p[5])) >= 300 {
                return Err("invalid worker PCR".into());
            }
            at += 6;
        }
    }
    if b[0] & 4 != 0 {
        at += 1;
    }
    for flag in [2, 1] {
        if b[0] & flag != 0 {
            let n = *b.get(at).ok_or("truncated worker adaptation field")?;
            at += 1 + usize::from(n);
        }
    }
    if at > b.len() {
        return Err("worker adaptation field exceeds packet".into());
    }
    Ok(())
}
#[derive(Default)]
struct Psi {
    bytes: Vec<u8>,
}
impl Psi {
    fn push(&mut self, b: &[u8], start: bool) -> Result<Vec<Vec<u8>>, String> {
        let mut out = vec![];
        let mut at = 0;
        if start {
            let n = usize::from(*b.first().ok_or("missing worker PSI pointer")?);
            at = 1 + n;
            if at > b.len() {
                return Err("worker PSI pointer exceeds packet".into());
            }
            if !self.bytes.is_empty() {
                self.append(&b[1..at], &mut out)?;
                if !self.bytes.is_empty() {
                    return Err("incomplete worker PSI at section start".into());
                }
            }
        } else if self.bytes.is_empty() {
            return Err("worker PSI continuation without start".into());
        }
        self.append(&b[at..], &mut out)?;
        Ok(out)
    }
    fn append(&mut self, b: &[u8], out: &mut Vec<Vec<u8>>) -> Result<(), String> {
        for x in b {
            if self.bytes.is_empty() && *x == 255 {
                continue;
            }
            self.bytes.push(*x);
            if self.bytes.len() >= 3 {
                let n = (usize::from(self.bytes[1] & 15) << 8) | usize::from(self.bytes[2]);
                if !(9..=1021).contains(&n) || self.bytes[1] & 0xf0 != 0xb0 {
                    return Err("invalid worker PSI section length/syntax".into());
                }
                if self.bytes.len() == n + 3 {
                    if crc(&self.bytes) != 0
                        || self.bytes[5] & 0xc1 != 0xc1
                        || self.bytes[6] != 0
                        || self.bytes[7] != 0
                    {
                        return Err("invalid CRC/noncurrent/multisection worker PSI".into());
                    }
                    out.push(std::mem::take(&mut self.bytes));
                }
            }
        }
        Ok(())
    }
}
fn crc(data: &[u8]) -> u32 {
    let mut c = 0xffff_ffffu32;
    for b in data {
        c ^= u32::from(*b) << 24;
        for _ in 0..8 {
            c = (c << 1) ^ if c & 0x8000_0000 != 0 { 0x04c1_1db7 } else { 0 };
        }
    }
    c
}
