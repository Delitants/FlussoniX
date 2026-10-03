//! Bounded HEVC decoder configuration and encoded access-unit inspection.
//! This validates framing, not the full coded-picture syntax.
#[derive(Debug)]
pub struct Configuration {
    pub nal_length_size: usize,
    initialization: Vec<Vec<u8>>,
}
#[derive(Debug)]
pub struct AccessUnit {
    pub annex_b: Vec<u8>,
    pub has_irap: bool,
}
fn nal_type(nal: &[u8]) -> Result<u8, String> {
    if nal.len() < 2 || nal[0] & 0x80 != 0 || nal[1] & 7 == 0 {
        return Err("invalid HEVC NAL header".into());
    }
    Ok((nal[0] >> 1) & 63)
}
impl Configuration {
    pub fn parse(data: &[u8]) -> Result<Self, String> {
        if data.len() < 23 || data.len() > 1024 * 1024 || data[0] != 1 {
            return Err("invalid HEVC decoder configuration".into());
        }
        let nal_length_size = usize::from(data[21] & 3) + 1;
        if nal_length_size == 3 {
            return Err("unsupported HEVC NAL length width".into());
        }
        let arrays = data[22] as usize;
        if arrays > 64 {
            return Err("too many HEVC configuration arrays".into());
        }
        let mut at = 23;
        let mut initialization = Vec::new();
        let mut required = [false; 3];
        for _ in 0..arrays {
            let header = data.get(at..at + 3).ok_or("truncated HEVC array")?;
            at += 3;
            if header[0] & 0x40 != 0 {
                return Err("invalid HEVC array header".into());
            }
            let kind = header[0] & 63;
            let count = u16::from_be_bytes([header[1], header[2]]) as usize;
            if initialization.len() + count > 4096 {
                return Err("too many HEVC initialization NALs".into());
            }
            for _ in 0..count {
                let n = data.get(at..at + 2).ok_or("truncated HEVC NAL size")?;
                at += 2;
                let n = u16::from_be_bytes([n[0], n[1]]) as usize;
                let nal = data.get(at..at + n).ok_or("truncated HEVC NAL")?;
                at += n;
                if nal_type(nal)? != kind {
                    return Err("HEVC array/NAL type mismatch".into());
                }
                if (32..=34).contains(&kind) {
                    required[(kind - 32) as usize] = true;
                }
                initialization.push(nal.to_vec());
            }
        }
        if at != data.len() || required.contains(&false) {
            return Err("incomplete HEVC initialization".into());
        }
        Ok(Self {
            nal_length_size,
            initialization,
        })
    }
    pub fn annex_b(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for nal in &self.initialization {
            out.extend_from_slice(&[0, 0, 0, 1]);
            out.extend_from_slice(nal);
        }
        out
    }
    pub fn access_unit(&self, data: &[u8]) -> Result<AccessUnit, String> {
        if !matches!(self.nal_length_size, 1 | 2 | 4) {
            return Err("unsupported HEVC NAL length width".into());
        }
        if data.is_empty() || data.len() > 16 * 1024 * 1024 {
            return Err("invalid HEVC access-unit size".into());
        }
        let mut at = 0;
        let mut annex_b = Vec::with_capacity(data.len());
        let mut has_irap = false;
        let mut count = 0;
        while at < data.len() {
            count += 1;
            if count > 4096 {
                return Err("too many HEVC access-unit NALs".into());
            }
            let size = data
                .get(at..at + self.nal_length_size)
                .ok_or("truncated HEVC sample length")?;
            at += self.nal_length_size;
            let n = size.iter().fold(0usize, |n, b| (n << 8) | usize::from(*b));
            let end = at.checked_add(n).ok_or("HEVC NAL size overflow")?;
            let nal = data.get(at..end).ok_or("HEVC NAL outside access unit")?;
            has_irap |= (16..=21).contains(&nal_type(nal)?);
            annex_b.extend_from_slice(&[0, 0, 0, 1]);
            annex_b.extend_from_slice(nal);
            at = end;
        }
        Ok(AccessUnit { annex_b, has_irap })
    }
}
