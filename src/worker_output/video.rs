use crate::{m4f::Frame, m4s::Track};
#[derive(Default)]
pub(super) struct Video {
    sets: [Option<Vec<u8>>; 3],
    track: Option<Track>,
    last: Option<u64>,
}
impl Video {
    pub(super) fn track(&self) -> Option<&Track> {
        self.track.as_ref()
    }
    pub(super) fn retained(&self) -> usize {
        self.sets.iter().flatten().map(Vec::len).sum()
    }
    pub(super) fn push(
        &mut self,
        pid: u16,
        ty: u8,
        data: &[u8],
        dts: u64,
        offset: i64,
    ) -> Result<Option<Frame>, String> {
        let mut body = vec![];
        let mut key = false;
        let mut picture = false;
        let mut boundaries = 0;
        for nal in nals(data)? {
            if nal[0] & 0x80 != 0 {
                return Err("invalid video NAL header".into());
            }
            let (kind, set, aud, vcl, first, irap) = if ty == 0x24 {
                if nal.len() < 3 || nal[1] & 7 == 0 || nal[0] & 1 != 0 || nal[1] & 0xf8 != 0 {
                    return Err("worker requires single-layer HEVC".into());
                }
                let kind = (nal[0] >> 1) & 63;
                (
                    kind,
                    if (32..=34).contains(&kind) {
                        Some((kind - 32) as usize)
                    } else {
                        None
                    },
                    kind == 35,
                    kind <= 31,
                    nal[2] & 0x80 != 0,
                    (16..=21).contains(&kind),
                )
            } else {
                let kind = nal[0] & 31;
                if kind == 0 || kind >= 24 {
                    return Err("unsupported AVC NAL header".into());
                }
                let first = if (1..=5).contains(&kind) {
                    Bits::new(&rbsp(&nal[1..])?).ue()? == 0
                } else {
                    false
                };
                (
                    kind,
                    match kind {
                        7 => Some(1),
                        8 => Some(2),
                        _ => None,
                    },
                    kind == 9,
                    (1..=5).contains(&kind),
                    first,
                    kind == 5,
                )
            };
            if let Some(i) = set {
                if nal.len() > u16::MAX as usize {
                    return Err("worker video parameter set too large".into());
                }
                if let Some(old) = &self.sets[i] {
                    if old != nal {
                        return Err("worker video parameter set changed".into());
                    }
                } else {
                    self.sets[i] = Some(nal.to_vec());
                }
                if self.retained() > 64 * 1024 {
                    return Err("worker video configuration exceeds bound".into());
                }
                continue;
            }
            if aud {
                if picture {
                    return Err("multiple access units in worker video PES".into());
                }
                continue;
            }
            if vcl {
                boundaries += usize::from(first);
                if boundaries > 1 {
                    return Err("multiple pictures in worker video PES".into());
                }
                picture = true;
                key |= irap;
            }
            // EOS/EOB/filler are retained, as are prefix/suffix SEI.
            let _ = kind;
            if body.len() + 4 + nal.len() > 16 * 1024 * 1024 {
                return Err("worker video sample exceeds bound".into());
            }
            body.extend_from_slice(&(nal.len() as u32).to_be_bytes());
            body.extend_from_slice(nal);
        }
        if self.track.is_none()
            && self.sets[1].is_some()
            && self.sets[2].is_some()
            && (ty != 0x24 || self.sets[0].is_some())
        {
            let config = if ty == 0x24 {
                hvcc(&self.sets)?
            } else {
                avcc(&self.sets)?
            };
            if config.len() > 64 * 1024 {
                return Err("worker video configuration exceeds bound".into());
            }
            self.track = Some(Track {
                id: u32::from(pid),
                codec: if ty == 0x24 { "hevc" } else { "h264" }.into(),
                config,
            });
        }
        if !picture {
            return Ok(None);
        }
        if self.track.is_none() {
            return Err("worker video picture precedes initialization".into());
        }
        if self
            .last
            .is_some_and(|last| dts < last || dts - last > 60 * 90000)
        {
            return Err("worker video timestamp discontinuity".into());
        }
        self.last = Some(dts);
        Ok(Some(Frame {
            track_id: u32::from(pid),
            dts,
            pts_offset: offset,
            key,
            body,
        }))
    }
}
fn nals(data: &[u8]) -> Result<Vec<&[u8]>, String> {
    let mut starts = vec![];
    let mut zeros = 0;
    for (i, b) in data.iter().enumerate() {
        if *b == 1 && zeros >= 2 {
            starts.push((i - zeros, i + 1));
            if starts.len() > 4096 {
                return Err("worker NAL count exceeds bound".into());
            }
        }
        zeros = if *b == 0 { zeros + 1 } else { 0 };
    }
    if starts.is_empty() || data[..starts[0].0].iter().any(|b| *b != 0) {
        return Err("worker video requires Annex B framing".into());
    }
    let mut out = vec![];
    for (i, (_, at)) in starts.iter().enumerate() {
        let mut end = starts.get(i + 1).map_or(data.len(), |s| s.0);
        while end > *at && data[end - 1] == 0 {
            end -= 1;
        }
        if end == *at {
            return Err("empty worker video NAL".into());
        }
        out.push(&data[*at..end]);
    }
    Ok(out)
}
fn avcc(sets: &[Option<Vec<u8>>; 3]) -> Result<Vec<u8>, String> {
    let sps = sets[1].as_ref().ok_or("missing AVC SPS")?;
    let pps = sets[2].as_ref().ok_or("missing AVC PPS")?;
    if sps.len() < 4 || pps.len() < 2 {
        return Err("truncated AVC parameter sets".into());
    }
    let mut b = vec![1, sps[1], sps[2], sps[3], 255, 225];
    b.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    b.extend_from_slice(sps);
    b.push(1);
    b.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    b.extend_from_slice(pps);
    Ok(b)
}
fn hvcc(sets: &[Option<Vec<u8>>; 3]) -> Result<Vec<u8>, String> {
    let sps = sets[1].as_ref().ok_or("missing HEVC SPS")?;
    let raw = rbsp(&sps[2..])?;
    let mut bits = Bits::new(&raw);
    bits.get(4)?;
    let layers = bits.get(3)?;
    let nested = bits.get(1)?;
    if layers > 6 {
        return Err("invalid HEVC temporal layer count".into());
    }
    let mut ptl = [0u8; 12];
    for byte in &mut ptl {
        *byte = bits.get(8)? as u8;
    }
    let mut flags = vec![];
    for _ in 0..layers {
        flags.push((bits.get(1)?, bits.get(1)?));
    }
    if layers > 0 {
        bits.skip((8 - layers) as usize * 2)?;
    }
    for (profile, level) in flags {
        if profile != 0 {
            bits.skip(88)?;
        }
        if level != 0 {
            bits.skip(8)?;
        }
    }
    bits.ue()?;
    let chroma = bits.ue()?;
    if chroma > 3 {
        return Err("invalid HEVC chroma format".into());
    }
    if chroma == 3 {
        bits.get(1)?;
    }
    if bits.ue()? == 0 || bits.ue()? == 0 {
        return Err("invalid HEVC picture dimensions".into());
    }
    if bits.get(1)? != 0 {
        for _ in 0..4 {
            bits.ue()?;
        }
    }
    let luma = bits.ue()?;
    let chroma_depth = bits.ue()?;
    if luma > 7 || chroma_depth > 7 {
        return Err("unsupported HEVC bit depth".into());
    }
    // The single qualified parameter-set family must agree on its general PTL.
    let vps = rbsp(&sets[0].as_ref().ok_or("missing HEVC VPS")?[2..])?;
    if vps.len() < 16 || vps[4..16] != ptl {
        return Err("HEVC VPS/SPS profile mismatch".into());
    }
    let mut b = vec![1];
    b.extend(ptl);
    b.extend([
        0xf0,
        0,
        0xfc,
        0xfc | chroma as u8,
        0xf8 | luma as u8,
        0xf8 | chroma_depth as u8,
        0,
        0,
        ((layers as u8 + 1) << 3) | ((nested as u8) << 2) | 3,
        3,
    ]);
    for (i, n) in sets.iter().enumerate() {
        let n = n.as_ref().ok_or("missing HEVC parameter set")?;
        b.extend([0x80 | (32 + i as u8), 0, 1]);
        b.extend_from_slice(&(n.len() as u16).to_be_bytes());
        b.extend(n);
    }
    crate::hevc::Configuration::parse(&b)?;
    Ok(b)
}
fn rbsp(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut b = Vec::with_capacity(data.len());
    let mut zeros = 0;
    for (i, x) in data.iter().enumerate() {
        if zeros >= 2 && *x == 3 {
            if data.get(i + 1).is_none_or(|v| *v > 3) {
                return Err("invalid NAL emulation prevention".into());
            }
            zeros = 0;
            continue;
        }
        b.push(*x);
        zeros = if *x == 0 { zeros + 1 } else { 0 };
    }
    Ok(b)
}
struct Bits<'a> {
    data: &'a [u8],
    at: usize,
}
impl<'a> Bits<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }
    fn get(&mut self, n: usize) -> Result<u32, String> {
        if n > 32 || self.at + n > self.data.len() * 8 {
            return Err("truncated video parameter syntax".into());
        }
        let mut out = 0;
        for _ in 0..n {
            out = (out << 1) | u32::from((self.data[self.at / 8] >> (7 - self.at % 8)) & 1);
            self.at += 1;
        }
        Ok(out)
    }
    fn skip(&mut self, n: usize) -> Result<(), String> {
        if self.at + n > self.data.len() * 8 {
            return Err("truncated video parameter syntax".into());
        }
        self.at += n;
        Ok(())
    }
    fn ue(&mut self) -> Result<u32, String> {
        let mut n = 0;
        while self.get(1)? == 0 {
            n += 1;
            if n > 31 {
                return Err("video Exp-Golomb exceeds bound".into());
            }
        }
        Ok(((1u32 << n) - 1) + self.get(n)?)
    }
}
