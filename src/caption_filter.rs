//! Length-preserving HLS caption suppression. Unrelated SEI and AV bytes survive.
use crate::m4s::boxes;
type Patch = (usize, u8);
fn number(b: &[u8], at: usize) -> Result<u32, String> {
    Ok(u32::from_be_bytes(
        b.get(at..at + 4)
            .ok_or("short caption container field")?
            .try_into()
            .unwrap(),
    ))
}
fn field<'a>(b: &'a [u8], kind: &[u8]) -> Result<&'a [u8], String> {
    boxes(b)?
        .into_iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, v)| v)
        .ok_or("caption container field absent".into())
}
// Keep emulation-prevention bytes and payload lengths unchanged. The process flag
// is cleared, count zeroed, and every valid bit disabled, including 708 triples.
fn nal_patches(nal: &[u8], hevc: bool) -> Result<Vec<Patch>, String> {
    if nal.len() > 2 * 1024 * 1024 {
        return Err("caption NAL exceeds limit".into());
    }
    let skip = if hevc {
        if nal.len() < 2 || !matches!((nal[0] >> 1) & 63, 39 | 40) {
            return Ok(vec![]);
        }
        2
    } else {
        if nal.first().is_none_or(|b| b & 31 != 6) {
            return Ok(vec![]);
        }
        1
    };
    let mut rbsp = vec![];
    let mut indices = vec![];
    let mut zeros = 0;
    for (i, &b) in nal.iter().enumerate().skip(skip) {
        if zeros >= 2 && b == 3 {
            zeros = 0;
            continue;
        }
        rbsp.push(b);
        indices.push(i);
        zeros = if b == 0 { zeros + 1 } else { 0 };
    }
    let mut at = 0;
    let mut patches = vec![];
    while at < rbsp.len() {
        if rbsp[at] == 0x80 && rbsp[at + 1..].iter().all(|b| *b == 0) {
            break;
        }
        let mut read_extended = || -> Result<usize, String> {
            let mut n = 0;
            loop {
                let b = *rbsp.get(at).ok_or("truncated caption SEI")?;
                at += 1;
                n += usize::from(b);
                if b != 255 {
                    return Ok(n);
                }
            }
        };
        let kind = read_extended()?;
        let size = read_extended()?;
        let payload = rbsp
            .get(at..at + size)
            .ok_or("truncated caption SEI payload")?;
        if kind == 4 && payload.starts_with(b"\xb5\x00\x31GA94\x03") {
            if payload.len() < 11 {
                return Err("invalid registered caption flags".into());
            }
            let count = usize::from(payload[8] & 31);
            if 10 + 3 * count >= payload.len() {
                return Err("truncated registered captions".into());
            }
            patches.push((indices[at + 8], payload[8] & 0xa0));
            for i in 0..count {
                let pos = at + 10 + 3 * i;
                patches.push((indices[pos], rbsp[pos] & !4));
            }
        }
        at += size;
    }
    Ok(patches)
}
fn annex_patches(data: &[u8], hevc: bool) -> Result<Vec<Patch>, String> {
    let mut starts = vec![];
    let mut i = 0;
    while i + 3 <= data.len() {
        if data[i..].starts_with(&[0, 0, 1]) {
            starts.push((i, i + 3));
            i += 3
        } else if data[i..].starts_with(&[0, 0, 0, 1]) {
            starts.push((i, i + 4));
            i += 4
        } else {
            i += 1
        }
        if starts.len() > 4096 {
            return Err("caption NAL count exceeds limit".into());
        }
    }
    let mut patches = vec![];
    for n in 0..starts.len() {
        let start = starts[n].1;
        let end = starts.get(n + 1).map_or(data.len(), |s| s.0);
        for (offset, v) in nal_patches(&data[start..end], hevc)? {
            patches.push((start + offset, v));
        }
    }
    Ok(patches)
}
fn ts_payload(p: &[u8]) -> Result<(u16, bool, usize), String> {
    if p.len() != 188 || p[0] != 0x47 || p[1] & 0x80 != 0 || p[3] & 0xc0 != 0 {
        return Err("unsupported caption TS packet".into());
    }
    let offset = if p[3] & 0x20 != 0 {
        5 + usize::from(p[4])
    } else {
        4
    };
    if offset > 188 {
        return Err("invalid caption TS adaptation".into());
    }
    Ok((
        ((u16::from(p[1] & 31)) << 8) | u16::from(p[2]),
        p[1] & 0x40 != 0,
        if p[3] & 0x10 != 0 { offset } else { 188 },
    ))
}
fn video_pid(data: &[u8]) -> Result<Option<(u16, bool)>, String> {
    let mut pmt = None;
    let mut section: Vec<u8> = vec![];
    for p in data.chunks_exact(188) {
        let (pid, start, off) = ts_payload(p)?;
        if off == 188 {
            continue;
        }
        if pid == 0 && start {
            let b = &p[off..];
            let ptr = usize::from(b[0]);
            let s = b.get(1 + ptr..).ok_or("invalid caption PAT")?;
            if s.len() >= 12 && s[0] == 0 {
                pmt = Some((u16::from(s[10] & 31) << 8) | u16::from(s[11]));
            }
        }
        if Some(pid) != pmt {
            continue;
        }
        let b = &p[off..];
        if start {
            section.clear();
            section.extend(
                b.get(1 + usize::from(b[0])..)
                    .ok_or("invalid caption PMT")?,
            )
        } else {
            section.extend(b)
        }
        if section.len() > 4096 {
            return Err("caption PMT limit".into());
        }
        if section.len() < 12 {
            continue;
        }
        let n = 3 + ((usize::from(section[1] & 15) << 8) | usize::from(section[2]));
        if n < 16 || n > section.len() {
            continue;
        }
        let mut at = 12 + ((usize::from(section[10] & 15) << 8) | usize::from(section[11]));
        while at + 5 <= n - 4 {
            let kind = section[at];
            let id = (u16::from(section[at + 1] & 31) << 8) | u16::from(section[at + 2]);
            if kind == 0x1b || kind == 0x24 {
                return Ok(Some((id, kind == 0x24)));
            }
            if kind == 2 || kind == 0x10 {
                return Err("caption filtering requires H.264 or HEVC".into());
            }
            if !matches!(kind, 3 | 4 | 0x0f | 0x11) {
                return Err("unsupported caption TS stream type".into());
            }
            at += 5 + ((usize::from(section[at + 3] & 15) << 8) | usize::from(section[at + 4]));
        }
        if at != n - 4 {
            return Err("invalid caption PMT entries".into());
        }
        return Ok(None);
    }
    Err("caption video metadata absent".into())
}
pub fn ts(data: &mut [u8]) -> Result<(), String> {
    if data.len() % 188 != 0 {
        return Err("unaligned caption TS".into());
    }
    let Some((pid, hevc)) = video_pid(data)? else {
        return Ok(());
    };
    let mut pes = vec![];
    let mut mapping: Vec<(usize, usize, usize)> = vec![];
    let mut patches = vec![];
    let flush = |pes: &[u8],
                 mapping: &[(usize, usize, usize)],
                 patches: &mut Vec<Patch>|
     -> Result<(), String> {
        if pes.is_empty() {
            return Ok(());
        }
        if !pes.starts_with(&[0, 0, 1]) || pes.len() < 9 {
            return Err("invalid caption PES".into());
        }
        let start = 9 + usize::from(pes[8]);
        let payload = pes.get(start..).ok_or("short caption PES")?;
        for (offset, value) in annex_patches(payload, hevc)? {
            let at = start + offset;
            let (_, original, local) = mapping
                .iter()
                .find(|(len, _, local)| at >= *local && at < local + len)
                .ok_or("caption mapping absent")?;
            patches.push((original + at - local, value));
            if patches.len() > 65536 {
                return Err("caption filter patch limit".into());
            }
        }
        Ok(())
    };
    for (i, p) in data.chunks_exact(188).enumerate() {
        let (id, start, off) = ts_payload(p)?;
        if id != pid || off == 188 {
            continue;
        }
        if start {
            flush(&pes, &mapping, &mut patches)?;
            pes.clear();
            mapping.clear();
        }
        if pes.is_empty() && !start {
            return Err("partial caption PES".into());
        }
        if pes.len() + 188 - off > 2 * 1024 * 1024 {
            return Err("caption PES exceeds limit".into());
        }
        mapping.push((188 - off, i * 188 + off, pes.len()));
        pes.extend(&p[off..]);
    }
    flush(&pes, &mapping, &mut patches)?;
    if patches.len() > 65536 {
        return Err("caption filter patch limit".into());
    }
    for (at, value) in patches {
        data[at] = value
    }
    Ok(())
}
fn video_id(init: &[u8]) -> Result<Option<(u32, bool, usize)>, String> {
    let mut audio = false;
    for (kind, trak) in boxes(field(init, b"moov")?)? {
        if kind != b"trak" {
            continue;
        }
        let mdia = field(trak, b"mdia")?;
        let handler = field(mdia, b"hdlr")?
            .get(8..12)
            .ok_or("short caption track handler")?;
        if handler != b"vide" {
            audio |= handler == b"soun";
            continue;
        }
        let tkhd = field(trak, b"tkhd")?;
        let id = number(tkhd, if tkhd[0] == 1 { 20 } else { 12 })?;
        let stsd = field(field(field(mdia, b"minf")?, b"stbl")?, b"stsd")?;
        let entry = stsd.get(8..).ok_or("short caption sample entry")?;
        let (kind, body) = boxes(entry)?
            .into_iter()
            .next()
            .ok_or("missing video sample entry")?;
        let hevc = match kind {
            b"avc1" | b"avc3" => false,
            b"hev1" | b"hvc1" => true,
            _ => return Err("caption filtering requires H.264 or HEVC".into()),
        };
        let config = field(
            body.get(78..).ok_or("short video sample entry")?,
            if hevc { b"hvcC" } else { b"avcC" },
        )?;
        let width = usize::from(
            config
                .get(if hevc { 21 } else { 4 })
                .ok_or("short video configuration")?
                & 3,
        ) + 1;
        return Ok(Some((id, hevc, width)));
    }
    if audio {
        Ok(None)
    } else {
        Err("caption video metadata absent".into())
    }
}
pub fn mp4(init: &[u8], data: &mut [u8]) -> Result<(), String> {
    let Some((id, hevc, width)) = video_id(init)? else {
        boxes(data)?;
        return Ok(());
    };
    let mut patches = vec![];
    for (kind, moof) in boxes(data)? {
        if kind != b"moof" {
            continue;
        }
        let base = moof.as_ptr() as usize - data.as_ptr() as usize - 8;
        for (kind, traf) in boxes(moof)? {
            if kind != b"traf" {
                continue;
            }
            let tfhd = field(traf, b"tfhd")?;
            if number(tfhd, 4)? != id {
                continue;
            }
            let flags = number(tfhd, 0)? & 0xffffff;
            if flags & 1 != 0 || flags & 0x20000 == 0 {
                return Err("unsupported caption fragment base".into());
            }
            let mut at = 8;
            if flags & 2 != 0 {
                at += 4
            }
            if flags & 8 != 0 {
                at += 4
            }
            let default_size = if flags & 16 != 0 {
                number(tfhd, at)? as usize
            } else {
                0
            };
            for (kind, trun) in boxes(traf)? {
                if kind != b"trun" {
                    continue;
                }
                let flags = number(trun, 0)? & 0xffffff;
                let count = number(trun, 4)? as usize;
                if count > 65536 || flags & 1 == 0 {
                    return Err("unsupported caption sample run".into());
                }
                let relative = number(trun, 8)? as i32;
                let mut cursor = base
                    .checked_add_signed(relative as isize)
                    .ok_or("invalid caption sample offset")?;
                let mut at = 12;
                if flags & 4 != 0 {
                    at += 4
                }
                for _ in 0..count {
                    if flags & 0x100 != 0 {
                        at += 4
                    }
                    let size = if flags & 0x200 != 0 {
                        let v = number(trun, at)? as usize;
                        at += 4;
                        v
                    } else {
                        default_size
                    };
                    for f in [0x400, 0x800] {
                        if flags & f != 0 {
                            at += 4
                        }
                    }
                    if at > trun.len() {
                        return Err("truncated caption sample run".into());
                    }
                    let end = cursor
                        .checked_add(size)
                        .ok_or("caption sample exceeds limit")?;
                    let sample = data
                        .get(cursor..end)
                        .ok_or("invalid caption sample range")?;
                    let mut pos = 0;
                    let mut nals = 0;
                    while pos < sample.len() {
                        let mut len = 0usize;
                        for b in sample
                            .get(pos..pos + width)
                            .ok_or("short caption NAL size")?
                        {
                            len = (len << 8) | usize::from(*b)
                        }
                        pos += width;
                        let nal = sample.get(pos..pos + len).ok_or("short caption NAL")?;
                        for (offset, v) in nal_patches(nal, hevc)? {
                            patches.push((cursor + pos + offset, v));
                            if patches.len() > 65536 {
                                return Err("caption filter patch limit".into());
                            }
                        }
                        pos += len;
                        nals += 1;
                        if nals > 4096 {
                            return Err("caption NAL count exceeds limit".into());
                        }
                    }
                    cursor = end;
                }
            }
        }
    }
    if patches.len() > 65536 {
        return Err("caption filter patch limit".into());
    }
    for (at, value) in patches {
        data[at] = value
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn targeted_608_708_filter_preserves_other_metadata_and_length() {
        for hevc in [false, true] {
            let mut nal = if hevc { vec![0x4e, 1] } else { vec![6] };
            let hdr = [137, 3, 10, 20, 30];
            nal.extend(hdr);
            nal.extend([
                4, 17, 0xb5, 0, 0x31, b'G', b'A', b'9', b'4', 3, 0xc2, 0xff, 0xfc, 0x94, 0xae,
                0xfe, 0x21, 0x41, 0xff, 0x80,
            ]);
            let original = nal.clone();
            let patches = nal_patches(&nal, hevc).unwrap();
            assert_eq!(patches.len(), 3);
            for (at, v) in patches {
                nal[at] = v
            }
            let head = if hevc { 2 } else { 1 };
            assert_eq!(&nal[head..head + 5], &hdr);
            assert_eq!(nal.len(), original.len());
            assert!(
                nal_patches(&nal, hevc)
                    .unwrap()
                    .iter()
                    .all(|(at, v)| nal[*at] == *v)
            );
            assert_eq!(nal[head + 5 + 2 + 8], 0x80);
        }
    }
}
