//! Bounded MPEG-TS bridge from native encoded samples to an independent worker.
use crate::{
    codec::Codec,
    m4f::Frame,
    m4s::{Track, validate_tracks},
};
use std::collections::HashMap;
const SAMPLE_LIMIT: usize = 16 * 1024 * 1024;
const MASK: u64 = (1 << 33) - 1;
struct Stream {
    track: Track,
    pid: u16,
    kind: Codec,
    format: Format,
    last_dts: Option<u64>,
}
enum Format {
    Avc { width: usize, init: Vec<u8> },
    Hevc(crate::hevc::Configuration),
    Aac { rate: u8, channels: u8 },
    Mpeg,
}
pub struct Muxer {
    streams: Vec<Stream>,
    pcr: u16,
    counters: HashMap<u16, u8>,
    last_tables: Option<u64>,
    subtitles: Vec<u32>,
}
impl Muxer {
    pub fn new(tracks: &[Track]) -> Result<Self, String> {
        validate_tracks(tracks)?;
        let mut streams = vec![];
        let mut subtitles = vec![];
        for (i, t) in tracks.iter().enumerate() {
            let kind = t.kind()?;
            let format = match kind {
                Codec::H264 => {
                    let (width, sets) = crate::rtp::avcc(&t.config)?;
                    let mut init = vec![];
                    for set in sets {
                        init.extend([0, 0, 0, 1]);
                        init.extend(set);
                    }
                    Format::Avc { width, init }
                }
                Codec::Hevc => Format::Hevc(crate::hevc::Configuration::parse(&t.config)?),
                Codec::Aac => {
                    let c = &t.config;
                    if !matches!(c.len(), 2 | 5)
                        || c[0] >> 3 != 2
                        || (c.len() == 5 && c[2..] != [0x56, 0xe5, 0])
                    {
                        return Err("worker requires AAC-LC ASC with optional disabled SBR".into());
                    }
                    let rate = ((c[0] & 7) << 1) | (c[1] >> 7);
                    let channels = (c[1] >> 3) & 15;
                    if rate > 12 || channels == 0 || channels > 7 || c[1] & 7 != 0 {
                        return Err("unsupported AAC rate/channel/extension".into());
                    }
                    Format::Aac { rate, channels }
                }
                Codec::M2a | Codec::Mp3 => Format::Mpeg,
                Codec::Subtitle => {
                    subtitles.push(t.id);
                    continue;
                }
            };
            streams.push(Stream {
                track: t.clone(),
                pid: 256 + i as u16,
                kind,
                format,
                last_dts: None,
            });
        }
        if streams.is_empty() {
            return Err("native source requires audio or video".into());
        }
        let pcr = streams
            .iter()
            .find(|s| s.kind.is_video())
            .unwrap_or(&streams[0])
            .pid;
        Ok(Self {
            streams,
            pcr,
            counters: HashMap::new(),
            last_tables: None,
            subtitles,
        })
    }
    pub fn tables(&mut self) -> Vec<u8> {
        let mut pat = vec![0, 0xb0, 13, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0];
        checksum(&mut pat);
        let n = 13 + 5 * self.streams.len();
        let mut pmt = vec![
            2,
            0xb0 | ((n >> 8) as u8),
            n as u8,
            0,
            1,
            0xc1,
            0,
            0,
            0xe0 | ((self.pcr >> 8) as u8),
            self.pcr as u8,
            0xf0,
            0,
        ];
        for s in &self.streams {
            let ty = match s.kind {
                Codec::H264 => 0x1b,
                Codec::Hevc => 0x24,
                Codec::Aac => 0x0f,
                Codec::M2a | Codec::Mp3 => 3,
                Codec::Subtitle => unreachable!("native text is not a TS audio/video stream"),
            };
            pmt.extend([ty, 0xe0 | ((s.pid >> 8) as u8), s.pid as u8, 0xf0, 0]);
        }
        checksum(&mut pmt);
        let mut out = vec![];
        self.packets(0, &[vec![0], pat].concat(), None, false, &mut out);
        self.packets(4096, &[vec![0], pmt].concat(), None, false, &mut out);
        out
    }
    pub(crate) fn sparse_tracks(&mut self, tracks: &[Track]) -> Result<(), String> {
        validate_tracks(tracks)?;
        let av: Vec<_> = tracks.iter().filter(|t| t.codec != "subtitle").collect();
        if av != self.streams.iter().map(|s| &s.track).collect::<Vec<_>>() {
            return Err("native metadata changed; worker restart required".into());
        }
        self.subtitles = tracks
            .iter()
            .filter(|t| t.codec == "subtitle")
            .map(|t| t.id)
            .collect();
        Ok(())
    }
    pub fn frame(&mut self, f: &Frame) -> Result<Vec<u8>, String> {
        if self.subtitles.contains(&f.track_id) {
            return Ok(Vec::new());
        }
        let index = self
            .streams
            .iter()
            .position(|s| s.track.id == f.track_id)
            .ok_or("unknown worker track")?;
        let s = &self.streams[index];
        if f.body.is_empty() || f.body.len() > SAMPLE_LIMIT {
            return Err("invalid worker sample size".into());
        }
        if s.last_dts.is_some_and(|d| f.dts < d) {
            return Err("worker track DTS goes backwards".into());
        }
        let pts = u64::try_from(i128::from(f.dts) + i128::from(f.pts_offset))
            .map_err(|_| "worker PTS outside timeline")?;
        let mut payload = vec![];
        match &s.format {
            Format::Hevc(config) => {
                let unit = config.access_unit(&f.body)?;
                payload.extend([0, 0, 0, 1, 0x46, 1, 0x50]);
                if f.key {
                    payload.extend(config.annex_b());
                }
                payload.extend(unit.annex_b);
            }
            Format::Avc { width, init } => {
                payload.extend([0, 0, 0, 1, 9, 0xf0]);
                if f.key {
                    payload.extend(init);
                }
                let mut at = 0;
                let mut count = 0;
                while at < f.body.len() {
                    let size = f.body.get(at..at + width).ok_or("short AVC length")?;
                    let n = size.iter().fold(0usize, |v, b| (v << 8) | usize::from(*b));
                    at += width;
                    let nal = f.body.get(at..at + n).ok_or("short AVC sample")?;
                    if n == 0 || nal[0] & 0x80 != 0 || nal[0] & 31 == 0 || nal[0] & 31 >= 24 {
                        return Err("invalid AVC NAL".into());
                    }
                    count += 1;
                    if count > 4096 {
                        return Err("too many AVC NALs".into());
                    }
                    payload.extend([0, 0, 0, 1]);
                    payload.extend(nal);
                    at += n;
                }
            }
            Format::Aac { rate, channels } => {
                let n = f.body.len() + 7;
                if n > 8191 {
                    return Err("AAC sample exceeds ADTS length".into());
                }
                payload.extend([
                    0xff,
                    0xf1,
                    0x40 | (rate << 2) | (channels >> 2),
                    (channels << 6) | ((n >> 11) as u8),
                    (n >> 3) as u8,
                    ((n as u8 & 7) << 5) | 31,
                    0xfc,
                ]);
                payload.extend(&f.body);
            }
            Format::Mpeg => {
                crate::mpeg_audio::inspect(s.kind, &f.body)?;
                payload.extend(&f.body);
            }
        }
        let pid = s.pid;
        let video = s.kind.is_video();
        let size = 13 + payload.len();
        if !video && size > 65535 {
            return Err("audio PES exceeds length".into());
        }
        let len = if video { 0 } else { size as u16 };
        let mut pes = vec![
            0,
            0,
            1,
            if video { 0xe0 } else { 0xc0 + index as u8 },
            (len >> 8) as u8,
            len as u8,
            0x80,
            0xc0,
            10,
        ];
        pes.extend(timestamp(pts, 3));
        pes.extend(timestamp(f.dts, 1));
        pes.extend(payload);
        // All fallible validation precedes continuity, clock and table state updates.
        let mut out = vec![];
        if self
            .last_tables
            .is_none_or(|d| f.dts.saturating_sub(d) >= 9000)
        {
            out.extend(self.tables());
            self.last_tables = Some(f.dts);
        }
        self.packets(
            pid,
            &pes,
            if pid == self.pcr { Some(f.dts) } else { None },
            f.key,
            &mut out,
        );
        self.streams[index].last_dts = Some(f.dts);
        Ok(out)
    }
    fn packets(&mut self, pid: u16, data: &[u8], pcr: Option<u64>, key: bool, out: &mut Vec<u8>) {
        let mut at = 0;
        while at < data.len() {
            let first = at == 0;
            let clock = if first { pcr } else { None };
            let max = if clock.is_some() { 176 } else { 184 };
            let n = (data.len() - at).min(max);
            let adaptation = n < 184;
            let cc = self.counters.entry(pid).or_insert(0);
            let mut p = [0xff; 188];
            p[0] = 0x47;
            p[1] = ((pid >> 8) as u8) | if first { 0x40 } else { 0 };
            p[2] = pid as u8;
            p[3] = if adaptation { 0x30 } else { 0x10 } | *cc;
            *cc = (*cc + 1) & 15;
            let start = if adaptation {
                let len = 183 - n;
                p[4] = len as u8;
                if len > 0 {
                    p[5] = if first && key { 0x40 } else { 0 };
                    if let Some(d) = clock {
                        let b = d & MASK;
                        p[5] |= 0x10;
                        p[6..12].copy_from_slice(&[
                            (b >> 25) as u8,
                            (b >> 17) as u8,
                            (b >> 9) as u8,
                            (b >> 1) as u8,
                            ((b & 1) << 7) as u8 | 0x7e,
                            0,
                        ]);
                    }
                }
                5 + len
            } else {
                4
            };
            p[start..start + n].copy_from_slice(&data[at..at + n]);
            out.extend(p);
            at += n;
        }
    }
}
fn timestamp(time: u64, prefix: u8) -> [u8; 5] {
    let t = time & MASK;
    [
        (prefix << 4) | (((t >> 30) as u8 & 7) << 1) | 1,
        (t >> 22) as u8,
        (((t >> 15) as u8 & 127) << 1) | 1,
        (t >> 7) as u8,
        ((t as u8 & 127) << 1) | 1,
    ]
}
fn checksum(data: &mut Vec<u8>) {
    let mut crc = 0xffff_ffffu32;
    for b in data.iter() {
        crc ^= u32::from(*b) << 24;
        for _ in 0..8 {
            crc = (crc << 1)
                ^ if crc & 0x8000_0000 != 0 {
                    0x04c1_1db7
                } else {
                    0
                };
        }
    }
    data.extend(crc.to_be_bytes());
}
