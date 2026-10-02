//! Independent AVC/AAC M4F sample-table adapter. No vendor runtime dependency.
use crate::m4s::{Track, atom, boxes};
#[derive(Debug, Clone)]
pub struct Frame {
    pub track_id: u32,
    pub dts: u64,
    pub pts_offset: i64,
    pub key: bool,
    pub body: Vec<u8>,
}
fn u32b(n: u32) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}
fn field<'a>(b: &[(&[u8], &'a [u8])], k: &[u8]) -> Result<&'a [u8], String> {
    b.iter()
        .find(|(n, _)| *n == k)
        .map(|(_, v)| *v)
        .ok_or_else(|| format!("missing M4F {}", String::from_utf8_lossy(k)))
}
fn read32(b: &[u8], at: usize) -> Result<u32, String> {
    Ok(u32::from_be_bytes(
        b.get(at..at + 4)
            .ok_or("short sample table")?
            .try_into()
            .unwrap(),
    ))
}
fn read64(b: &[u8], at: usize) -> Result<u64, String> {
    Ok(u64::from_be_bytes(
        b.get(at..at + 8)
            .ok_or("short sample table")?
            .try_into()
            .unwrap(),
    ))
}
pub fn pack(tracks: &[Track], frames: &[Frame], duration: u64) -> Result<Vec<u8>, String> {
    if frames.is_empty() || frames.len() > 100000 {
        return Err("invalid M4F sample count".into());
    }
    let start = frames.iter().map(|f| f.dts).min().unwrap();
    let mut segm = vec![0; 4];
    segm.extend_from_slice(&90000u32.to_be_bytes());
    segm.extend_from_slice(&start.to_be_bytes());
    segm.extend_from_slice(&duration.to_be_bytes());
    let mut moov = atom(b"segm", &segm);
    let mut payload = Vec::new();
    for track in tracks {
        if !["h264", "aac"].contains(&track.codec.as_str()) {
            return Err("unsupported M4F codec".into());
        }
        let samples = frames
            .iter()
            .filter(|f| f.track_id == track.id)
            .collect::<Vec<_>>();
        if samples.is_empty() {
            continue;
        }
        let shift =
            i32::try_from(samples[0].dts - start).map_err(|_| "track offset exceeds limit")?;
        let mut shft = vec![0; 4];
        shft.extend_from_slice(&90000u32.to_be_bytes());
        shft.extend_from_slice(&0u32.to_be_bytes());
        shft.extend_from_slice(&shift.to_be_bytes());
        let mut t = atom(b"shft", &shft);
        let mut handler = vec![0; 4];
        handler.extend_from_slice(&track.id.to_be_bytes());
        handler.extend_from_slice(if track.codec == "h264" {
            b"videh264\0"
        } else {
            b"sounaac\0"
        });
        t.extend(atom(b"hdlr", &handler));
        t.extend(atom(b"cnfg", &[vec![0; 4], track.config.clone()].concat()));
        t.extend(atom(
            b"stsc",
            &[0u32, 1, 1, samples.len() as u32, 1]
                .into_iter()
                .flat_map(u32b)
                .collect::<Vec<_>>(),
        ));
        t.extend(atom(
            b"stco",
            &[0u32, 1, payload.len() as u32 + 8]
                .into_iter()
                .flat_map(u32b)
                .collect::<Vec<_>>(),
        ));
        let mut stts = [0u32, samples.len() as u32]
            .into_iter()
            .flat_map(u32b)
            .collect::<Vec<_>>();
        let mut stsz = [0u32, 0, samples.len() as u32]
            .into_iter()
            .flat_map(u32b)
            .collect::<Vec<_>>();
        let mut ctts = [0x01000000u32, samples.len() as u32]
            .into_iter()
            .flat_map(u32b)
            .collect::<Vec<_>>();
        let mut keys = Vec::new();
        for (i, s) in samples.iter().enumerate() {
            let delta = samples
                .get(i + 1)
                .map(|next| next.dts.saturating_sub(s.dts))
                .unwrap_or(if track.codec == "h264" { 3600 } else { 1920 });
            stts.extend_from_slice(&1u32.to_be_bytes());
            stts.extend_from_slice(&(delta as u32).to_be_bytes());
            stsz.extend_from_slice(&(s.body.len() as u32).to_be_bytes());
            ctts.extend_from_slice(&1u32.to_be_bytes());
            ctts.extend_from_slice(
                &i32::try_from(s.pts_offset)
                    .map_err(|_| "composition offset too large")?
                    .to_be_bytes(),
            );
            if s.key {
                keys.push((i + 1) as u32)
            }
            payload.extend_from_slice(&s.body);
            if payload.len() > 32 * 1024 * 1024 {
                return Err("M4F segment exceeds size limit".into());
            }
        }
        t.extend(atom(b"stts", &stts));
        t.extend(atom(b"stsz", &stsz));
        if samples.iter().any(|s| s.pts_offset != 0) {
            t.extend(atom(b"ctts", &ctts))
        }
        let mut stss = [0u32, keys.len() as u32]
            .into_iter()
            .flat_map(u32b)
            .collect::<Vec<_>>();
        for k in keys {
            stss.extend_from_slice(&k.to_be_bytes())
        }
        t.extend(atom(b"stss", &stss));
        moov.extend(atom(b"trak", &t));
    }
    Ok([atom(b"moov", &moov), atom(b"mdat", &payload)].concat())
}
fn runs(b: &[u8], signed: bool, max: usize) -> Result<Vec<i64>, String> {
    let n = read32(b, 4)? as usize;
    if n > max || b.len() != 8 + n * 8 {
        return Err("invalid run table".into());
    }
    let mut out = Vec::new();
    for i in 0..n {
        let count = read32(b, 8 + i * 8)? as usize;
        let value = read32(b, 12 + i * 8)?;
        if out.len() + count > max {
            return Err("run table exceeds sample count".into());
        }
        out.extend(std::iter::repeat_n(
            if signed {
                value as i32 as i64
            } else {
                value as i64
            },
            count,
        ));
    }
    Ok(out)
}
pub fn unpack(data: &[u8]) -> Result<(Vec<Track>, Vec<Frame>), String> {
    if data.len() > 32 * 1024 * 1024 {
        return Err("M4F segment exceeds size limit".into());
    }
    let root = boxes(data)?;
    let moov = boxes(field(&root, b"moov")?)?;
    let payload = field(&root, b"mdat")?;
    let segm = field(&moov, b"segm")?;
    let scale = read32(segm, 4)? as u64;
    if scale == 0 {
        return Err("invalid timescale".into());
    }
    let base = read64(segm, 8)?
        .checked_mul(90000)
        .ok_or("timestamp overflow")?
        / scale;
    let mut tracks = Vec::new();
    let mut frames = Vec::new();
    let mut decoded_bytes = 0usize;
    let mut ids = std::collections::HashSet::new();
    for (k, body) in &moov {
        if *k != b"trak" {
            continue;
        }
        if tracks.len() >= 2 {
            return Err("M4F supports at most two AVC/AAC tracks".into());
        }
        let fields = boxes(body)?;
        let h = field(&fields, b"hdlr")?;
        let id = read32(h, 4)?;
        if !ids.insert(id) {
            return Err("duplicate M4F track id".into());
        }
        let codec = String::from_utf8_lossy(h.get(12..).ok_or("short handler")?)
            .trim_end_matches('\0')
            .to_owned();
        if !["h264", "aac"].contains(&codec.as_str()) {
            return Err("unsupported M4F codec".into());
        }
        let cnfg = field(&fields, b"cnfg")?;
        let config = cnfg.get(4..).ok_or("short configuration")?.to_vec();
        tracks.push(Track { id, codec, config });
        let shft = field(&fields, b"shft")?;
        let track_scale = read32(shft, 4)? as u64;
        if track_scale == 0 {
            return Err("invalid track timescale".into());
        }
        let shift = read32(shft, 12)? as i32 as i64;
        let mut dts = i64::try_from(base)
            .map_err(|_| "timestamp overflow")?
            .checked_add(shift * 90000 / track_scale as i64)
            .ok_or("timestamp overflow")?;
        let stsz = field(&fields, b"stsz")?;
        let count = read32(stsz, 8)? as usize;
        if frames.len() + count > 100000 || count == 0 {
            return Err("invalid sample count".into());
        }
        let fixed_size = read32(stsz, 4)? as usize;
        if fixed_size == 0 && stsz.len() != 12 + count * 4 {
            return Err("invalid sample size table".into());
        }
        let stsc = field(&fields, b"stsc")?;
        if read32(stsc, 4)? != 1 || read32(stsc, 8)? != 1 || read32(stsc, 12)? as usize != count {
            return Err("M4F multi-chunk track not implemented".into());
        }
        let stco = field(&fields, b"stco")?;
        if read32(stco, 4)? != 1 {
            return Err("M4F multi-chunk track not implemented".into());
        }
        let mut offset = (read32(stco, 8)? as usize)
            .checked_sub(8)
            .ok_or("invalid chunk offset")?;
        let durations = runs(field(&fields, b"stts")?, false, count)?;
        if durations.len() != count {
            return Err("duration count mismatch".into());
        }
        let compositions = if let Ok(c) = field(&fields, b"ctts") {
            runs(c, *c.first().ok_or("short composition table")? == 1, count)?
        } else {
            vec![0; count]
        };
        if compositions.len() != count {
            return Err("composition count mismatch".into());
        }
        let mut key_samples = std::collections::HashSet::new();
        if let Ok(ss) = field(&fields, b"stss") {
            let n = read32(ss, 4)? as usize;
            if n > count || ss.len() != 8 + n * 4 {
                return Err("invalid sync samples".into());
            }
            for i in 0..n {
                key_samples.insert(read32(ss, 8 + i * 4)? as usize);
            }
        }
        for i in 0..count {
            let size = if fixed_size > 0 {
                fixed_size
            } else {
                read32(stsz, 12 + i * 4)? as usize
            };
            decoded_bytes = decoded_bytes
                .checked_add(size)
                .ok_or("decoded payload overflow")?;
            if decoded_bytes > 32 * 1024 * 1024 {
                return Err("decoded M4F payload exceeds limit".into());
            }
            let end = offset.checked_add(size).ok_or("payload overflow")?;
            let body = payload
                .get(offset..end)
                .ok_or("sample outside payload")?
                .to_vec();
            frames.push(Frame {
                track_id: id,
                dts: u64::try_from(dts).map_err(|_| "negative timestamp")?,
                pts_offset: compositions[i] * 90000 / track_scale as i64,
                key: tracks.last().unwrap().codec == "aac" || key_samples.contains(&(i + 1)),
                body,
            });
            offset = end;
            dts = dts
                .checked_add(durations[i] * 90000 / track_scale as i64)
                .ok_or("timestamp overflow")?;
        }
    }
    frames.sort_by_key(|f| f.dts);
    if tracks.is_empty() {
        return Err("no supported M4F tracks".into());
    }
    Ok((tracks, frames))
}
