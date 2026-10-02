//! Independent decoder for the observed M4S length-prefixed media records.
//! Supports observed AVC/AAC MDin, FRam and packed Fgop records.
use crate::m4f::Frame;
use bytes::{Buf, Bytes, BytesMut};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Track {
    pub id: u32,
    pub codec: String,
    pub config: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct PackedGop {
    pub utc: u32,
    pub dts_ms: f64,
    pub sequence: u32,
    pub duration_ms: f64,
    pub body: Bytes,
}
#[derive(Debug)]
pub enum Event {
    Info {
        tracks: Vec<Track>,
        wire: Bytes,
    },
    Frame {
        track_id: u32,
        dts: u64,
        pts_offset: i64,
        key: bool,
        body: Vec<u8>,
        wire: Bytes,
    },
    Gop {
        gop: PackedGop,
        tracks: Vec<Track>,
        frames: Vec<Frame>,
        wire: Bytes,
    },
    Other {
        wire: Bytes,
    },
}
#[derive(Default)]
pub struct Decoder {
    buffer: BytesMut,
}
impl Decoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, String> {
        if self.buffer.len() + bytes.len() > 32 * 1024 * 1024 {
            return Err("M4S buffered data exceeds limit".into());
        }
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while self.buffer.len() >= 4 {
            let n = u32::from_be_bytes(self.buffer[..4].try_into().unwrap()) as usize;
            if !(8..=16 * 1024 * 1024).contains(&n) {
                return Err("invalid M4S record length".into());
            }
            if self.buffer.len() < n + 4 {
                break;
            }
            let wire = self.buffer.split_to(n + 4).freeze();
            let packet = &wire[4..];
            let atoms = boxes(packet)?;
            if atoms.len() != 1 {
                return Err("M4S record must contain one box".into());
            }
            let (kind, body) = atoms[0];
            let event = match kind {
                b"MDin" => {
                    let mut tracks = Vec::new();
                    for (k, body) in boxes(body)? {
                        if k != b"trak" {
                            continue;
                        }
                        let fields = boxes(body)?;
                        let h = find(&fields, b"hdlr").ok_or("missing track handler")?;
                        if h.len() < 16 {
                            return Err("short track handler".into());
                        }
                        let id = u32::from_be_bytes(h[4..8].try_into().unwrap());
                        let codec = String::from_utf8_lossy(&h[12..])
                            .trim_end_matches('\0')
                            .to_string();
                        if !["h264", "aac"].contains(&codec.as_str()) {
                            return Err(format!("unsupported M4S codec: {codec}"));
                        }
                        let config = find(&fields, b"cnfg").ok_or("missing codec configuration")?;
                        if config.len() < 5 {
                            return Err("short codec configuration".into());
                        }
                        if tracks.len() >= 2
                            || tracks.iter().any(|t: &Track| t.id == id)
                            || config.len() > 1024 * 1024
                        {
                            return Err("invalid or excessive M4S tracks/configuration".into());
                        }
                        tracks.push(Track {
                            id,
                            codec,
                            config: config[4..].to_vec(),
                        });
                    }
                    if tracks.is_empty() {
                        return Err("M4S media information has no supported tracks".into());
                    }
                    Event::Info { tracks, wire }
                }
                b"FRam" => {
                    let fields = boxes(body)?;
                    let h = find(&fields, b"fhdr").ok_or("missing frame header")?;
                    if h.len() < 32 {
                        return Err("short frame header".into());
                    }
                    let track_id = u32::from_be_bytes(h[..4].try_into().unwrap());
                    let dts = u64::from_be_bytes(h[12..20].try_into().unwrap());
                    let pts_offset = i64::from_be_bytes(h[20..28].try_into().unwrap());
                    let key = h[5] == 2;
                    let body = find(&fields, b"body")
                        .ok_or("missing frame payload")?
                        .to_vec();
                    Event::Frame {
                        track_id,
                        dts,
                        pts_offset,
                        key,
                        body,
                        wire,
                    }
                }
                b"Fgop" => {
                    let fields = boxes(body)?;
                    let header = boxes(required(&fields, b"goph")?)?;
                    let utc = u32::from_be_bytes(exact(required(&header, b" utc")?)?);
                    let dts_ms = f64::from_be_bytes(exact(required(&header, b" dts")?)?);
                    let sequence = u32::from_be_bytes(exact(required(&header, b" num")?)?);
                    let duration_ms = f64::from_be_bytes(exact(required(&header, b" dur")?)?);
                    if !dts_ms.is_finite()
                        || dts_ms < 0.0
                        || dts_ms > (u64::MAX / 90) as f64
                        || !duration_ms.is_finite()
                        || duration_ms <= 0.0
                        || duration_ms > 3600000.0
                    {
                        return Err("invalid M4S GOP timing".into());
                    }
                    let payload = required(&fields, b"body")?;
                    let (tracks, frames) = crate::m4f::unpack(payload)?;
                    if frames.is_empty() {
                        return Err("empty M4S GOP".into());
                    }
                    let offset = payload.as_ptr() as usize - wire.as_ptr() as usize;
                    let payload = wire.slice(offset..offset + payload.len());
                    Event::Gop {
                        gop: PackedGop {
                            utc,
                            dts_ms,
                            sequence,
                            duration_ms,
                            body: payload,
                        },
                        tracks,
                        frames,
                        wire,
                    }
                }
                _ => Event::Other { wire },
            };
            events.push(event);
        }
        Ok(events)
    }
}
pub fn atom(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(8 + body.len());
    b.extend_from_slice(&((body.len() + 8) as u32).to_be_bytes());
    b.extend_from_slice(kind);
    b.extend_from_slice(body);
    b
}
pub type BoxView<'a> = (&'a [u8], &'a [u8]);
pub fn boxes(mut data: &[u8]) -> Result<Vec<BoxView<'_>>, String> {
    let mut out = Vec::new();
    while !data.is_empty() {
        if out.len() >= 1024 {
            return Err("too many container boxes".into());
        }
        if data.len() < 8 {
            return Err("truncated box header".into());
        }
        let n = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        if n < 8 || n > data.len() {
            return Err("invalid box length".into());
        }
        out.push((&data[4..8], &data[8..n]));
        data.advance(n);
    }
    Ok(out)
}
fn find<'a>(fields: &[(&[u8], &'a [u8])], name: &[u8]) -> Option<&'a [u8]> {
    fields.iter().find(|(k, _)| *k == name).map(|(_, v)| *v)
}
pub fn flv_header() -> Vec<u8> {
    b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00".to_vec()
}
pub fn flv_tag(kind: u8, timestamp: u32, data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() > 0xffffff {
        return Err("FLV tag exceeds size limit".into());
    }
    let mut b = Vec::with_capacity(data.len() + 15);
    b.push(kind);
    b.extend_from_slice(&(data.len() as u32).to_be_bytes()[1..]);
    b.extend_from_slice(&timestamp.to_be_bytes()[1..]);
    b.push(timestamp.to_be_bytes()[0]);
    b.extend_from_slice(&[0, 0, 0]);
    b.extend_from_slice(data);
    b.extend_from_slice(&((data.len() + 11) as u32).to_be_bytes());
    Ok(b)
}
pub fn flv_config(track: &Track) -> Result<Vec<u8>, String> {
    let mut body = if track.codec == "h264" {
        vec![0x17, 0, 0, 0, 0]
    } else {
        vec![0xaf, 0]
    };
    body.extend_from_slice(&track.config);
    flv_tag(if track.codec == "h264" { 9 } else { 8 }, 0, &body)
}
pub fn flv_frame(
    track: &Track,
    dts: u64,
    offset: i64,
    key: bool,
    body: &[u8],
    origin: u64,
) -> Result<Vec<u8>, String> {
    let ts = (dts.saturating_sub(origin) / 90).min(u32::MAX as u64) as u32;
    let mut b = if track.codec == "h264" {
        let cts = offset / 90;
        if !(-8388608..8388608).contains(&cts) {
            return Err("composition offset exceeds FLV range".into());
        }
        let mut b = vec![if key { 0x17 } else { 0x27 }, 1];
        b.extend_from_slice(&(cts as i32).to_be_bytes()[1..]);
        b
    } else {
        vec![0xaf, 1]
    };
    b.extend_from_slice(body);
    flv_tag(if track.codec == "h264" { 9 } else { 8 }, ts, &b)
}

fn required<'a>(fields: &[BoxView<'a>], name: &[u8]) -> Result<&'a [u8], String> {
    let mut found = fields.iter().filter(|(k, _)| *k == name);
    let value = found.next().ok_or("missing M4S field")?.1;
    if found.next().is_some() {
        return Err("duplicate M4S field".into());
    }
    Ok(value)
}
fn exact<const N: usize>(bytes: &[u8]) -> Result<[u8; N], String> {
    bytes
        .try_into()
        .map_err(|_| "invalid M4S field width".into())
}

pub fn encode_gop(gop: &PackedGop) -> Result<Bytes, String> {
    let header = [
        atom(b" utc", &gop.utc.to_be_bytes()),
        atom(b" dts", &gop.dts_ms.to_be_bytes()),
        atom(b" num", &gop.sequence.to_be_bytes()),
        atom(b" dur", &gop.duration_ms.to_be_bytes()),
    ]
    .concat();
    let packet = atom(
        b"Fgop",
        &[atom(b"goph", &header), atom(b"body", &gop.body)].concat(),
    );
    if packet.len() > 16 * 1024 * 1024 {
        return Err("GOP exceeds M4S record limit".into());
    }
    Ok(Bytes::from(
        [(packet.len() as u32).to_be_bytes().to_vec(), packet].concat(),
    ))
}
