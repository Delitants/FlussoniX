//! Remove declared native text tracks without rebuilding unrelated track metadata.
//! Callers supply records/segments already validated by the native decoder.
use crate::m4s::{atom, boxes, decode_track};
use bytes::Bytes;
fn field<'a>(fields: &[(&[u8], &'a [u8])], name: &[u8]) -> Result<&'a [u8], String> {
    fields
        .iter()
        .find(|(k, _)| *k == name)
        .map(|(_, v)| *v)
        .ok_or("missing native subtitle-filter field".into())
}
fn u32_at(bytes: &[u8], at: usize) -> Result<u32, String> {
    Ok(u32::from_be_bytes(
        bytes
            .get(at..at + 4)
            .ok_or("short native subtitle-filter field")?
            .try_into()
            .unwrap(),
    ))
}
pub(crate) fn info(wire: &Bytes) -> Result<Bytes, String> {
    let atoms = boxes(wire.get(4..).ok_or("short native metadata record")?)?;
    let fields = boxes(field(&atoms, b"MDin")?)?;
    let mut body = vec![];
    for (k, v) in fields {
        if k == b"trak" && decode_track(&boxes(v)?)?.codec == "subtitle" {
            continue;
        }
        body.extend(atom(k.try_into().map_err(|_| "invalid native atom")?, v));
    }
    let record = atom(b"MDin", &body);
    Ok(Bytes::from(
        [(record.len() as u32).to_be_bytes().to_vec(), record].concat(),
    ))
}
pub(crate) fn segment(data: &Bytes) -> Result<Bytes, String> {
    let root = boxes(data)?;
    let moov = boxes(field(&root, b"moov")?)?;
    let payload = field(&root, b"mdat")?;
    let mut metadata = vec![];
    let mut samples = vec![];
    for (k, v) in moov {
        if k != b"trak" {
            metadata.extend(atom(k.try_into().map_err(|_| "invalid native atom")?, v));
            continue;
        }
        let fields = boxes(v)?;
        if decode_track(&fields)?.codec == "subtitle" {
            continue;
        }
        let sizes = field(&fields, b"stsz")?;
        let count = u32_at(sizes, 8)? as usize;
        let fixed = u32_at(sizes, 4)? as usize;
        if count > 100000 {
            return Err("native filter sample count exceeds limit".into());
        }
        let mut length = 0usize;
        for i in 0..count {
            length = length
                .checked_add(if fixed == 0 {
                    u32_at(sizes, 12 + i * 4)? as usize
                } else {
                    fixed
                })
                .ok_or("native filter payload overflow")?;
        }
        let offsets = field(&fields, b"stco")?;
        if u32_at(offsets, 4)? != 1 {
            return Err("native filter requires one chunk".into());
        }
        let start = (u32_at(offsets, 8)? as usize)
            .checked_sub(8)
            .ok_or("invalid native filter chunk offset")?;
        let end = start
            .checked_add(length)
            .ok_or("native filter payload overflow")?;
        let bytes = payload
            .get(start..end)
            .ok_or("native filter sample outside payload")?;
        let offset =
            u32::try_from(samples.len() + 8).map_err(|_| "native filter chunk offset overflow")?;
        let mut track = vec![];
        for (name, body) in fields {
            if name == b"stco" {
                let mut body = body.to_vec();
                body.get_mut(8..12)
                    .ok_or("short native chunk offset")?
                    .copy_from_slice(&offset.to_be_bytes());
                track.extend(atom(b"stco", &body));
            } else {
                track.extend(atom(
                    name.try_into().map_err(|_| "invalid native atom")?,
                    body,
                ));
            }
        }
        metadata.extend(atom(b"trak", &track));
        samples.extend_from_slice(bytes);
    }
    let mut out = vec![];
    for (k, v) in root {
        out.extend(atom(
            k.try_into().map_err(|_| "invalid native atom")?,
            if k == b"moov" {
                &metadata
            } else if k == b"mdat" {
                &samples
            } else {
                v
            },
        ));
    }
    if out.len() > data.len() {
        return Err("native filter unexpectedly expanded segment".into());
    }
    Ok(Bytes::from(out))
}

/// Native frame-to-segment copies retain opaque track metadata from MDin.
/// Sample-table fields belong to the newly packed segment and are never replaced.
pub(crate) fn carry_metadata(data: Bytes, info: &Bytes) -> Result<Bytes, String> {
    let root = boxes(info.get(4..).ok_or("short native metadata")?)?;
    let mut extras = Vec::new();
    for (name, value) in boxes(field(&root, b"MDin")?)? {
        if name != b"trak" {
            continue;
        }
        let fields = boxes(value)?;
        let track = decode_track(&fields)?;
        let mut extra = Vec::new();
        for (kind, body) in fields {
            if [
                b"hdlr", b"cnfg", b"shft", b"stsc", b"stco", b"stts", b"stsz", b"ctts", b"stss",
            ]
            .iter()
            .any(|k| kind == *k)
            {
                continue;
            }
            extra.extend(atom(
                kind.try_into().map_err(|_| "invalid native atom")?,
                body,
            ));
        }
        if !extra.is_empty() {
            extras.push((track.id, extra));
        }
    }
    if extras.is_empty() {
        return Ok(data);
    }
    let budget = extras.iter().try_fold(data.len(), |size, (_, value)| {
        size.checked_add(value.len())
            .ok_or("native metadata size overflow")
    })?;
    if budget > 32 * 1024 * 1024 {
        return Err("native segment metadata exceeds limit".into());
    }
    let root = boxes(&data)?;
    let mut out = Vec::new();
    for (kind, body) in root {
        if kind == b"moov" {
            let mut moov = Vec::new();
            for (name, value) in boxes(body)? {
                let mut track = value.to_vec();
                if name == b"trak" {
                    let id = decode_track(&boxes(value)?)?.id;
                    if let Some((_, extra)) = extras.iter().find(|(key, _)| *key == id) {
                        track.extend(extra);
                    }
                }
                moov.extend(atom(
                    name.try_into().map_err(|_| "invalid native atom")?,
                    &track,
                ));
            }
            out.extend(atom(b"moov", &moov));
        } else {
            out.extend(atom(
                kind.try_into().map_err(|_| "invalid native atom")?,
                body,
            ));
        }
    }
    Ok(Bytes::from(out))
}
