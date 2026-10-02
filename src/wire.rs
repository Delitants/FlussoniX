//! Shared bounded M4S frame fan-out and M4F live segment window.
use crate::{
    m4f::{Frame, pack},
    m4s::{Track, atom},
};
use bytes::{Buf, Bytes, BytesMut};
use std::{collections::VecDeque, sync::Mutex};
use tokio::sync::broadcast;
#[derive(Clone)]
pub struct Segment {
    pub name: String,
    pub signal: Bytes,
    pub bytes: Bytes,
}
struct State {
    tracks: Vec<Track>,
    info: Option<Bytes>,
    bootstrap: Vec<Bytes>,
    frames: Vec<Frame>,
    segments: VecDeque<Segment>,
    sequence: u64,
    utc: i64,
}
pub struct Hub {
    pub m4s: broadcast::Sender<Bytes>,
    pub signals: broadcast::Sender<Bytes>,
    state: Mutex<State>,
}
impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}
impl Hub {
    pub fn new() -> Self {
        Self {
            m4s: broadcast::channel(256).0,
            signals: broadcast::channel(16).0,
            state: Mutex::new(State {
                tracks: vec![],
                info: None,
                bootstrap: vec![],
                frames: vec![],
                segments: VecDeque::new(),
                sequence: 0,
                utc: chrono::Utc::now().timestamp_millis(),
            }),
        }
    }
    pub fn info(&self, tracks: Vec<Track>) {
        let wire = Bytes::from(encode_info(&tracks));
        let mut s = self.state.lock().unwrap();
        s.tracks = tracks;
        s.info = Some(wire.clone());
        s.bootstrap = vec![wire.clone()];
        let _ = self.m4s.send(wire);
    }
    pub fn frame(&self, frame: Frame) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        let track = s
            .tracks
            .iter()
            .find(|t| t.id == frame.track_id)
            .ok_or("unknown wire track")?;
        let video = track.codec == "h264";
        let wire = Bytes::from(encode_frame(track, &frame));
        if video && frame.key {
            s.bootstrap.clear();
            if let Some(i) = s.info.clone() {
                s.bootstrap.push(i)
            }
            if !s.frames.is_empty() && frame.dts.saturating_sub(s.frames[0].dts) >= 90000 {
                let duration = frame.dts - s.frames[0].dts;
                let bytes = Bytes::from(pack(&s.tracks, &s.frames, duration)?);
                let ms = s.utc + s.frames[0].dts as i64 / 90;
                let date =
                    chrono::DateTime::from_timestamp_millis(ms).ok_or("invalid UTC timestamp")?;
                let stamp = date.format("%Y/%m/%d/%H/%M/%S").to_string();
                s.sequence += 1;
                let signal =
                    Bytes::from(format!("{} {}-{:05}\n", s.sequence, stamp, duration / 90));
                let segment = Segment {
                    name: format!("{stamp}.m4f"),
                    signal: signal.clone(),
                    bytes,
                };
                s.segments.push_back(segment);
                s.frames.clear();
                while s.segments.len() > 8
                    || s.segments.iter().map(|v| v.bytes.len()).sum::<usize>() > 64 * 1024 * 1024
                {
                    s.segments.pop_front();
                }
                let _ = self.signals.send(signal);
            }
        }
        if s.bootstrap.iter().map(Bytes::len).sum::<usize>() + wire.len() <= 8 * 1024 * 1024 {
            s.bootstrap.push(wire.clone())
        } else {
            s.bootstrap.clear();
            if let Some(i) = s.info.clone() {
                s.bootstrap.push(i)
            }
        }
        let _ = self.m4s.send(wire);
        if s.frames.len() < 100000
            && s.frames.iter().map(|v| v.body.len()).sum::<usize>() + frame.body.len()
                <= 32 * 1024 * 1024
        {
            s.frames.push(frame)
        } else {
            s.frames.clear();
        }
        Ok(())
    }
    pub fn has_info(&self) -> bool {
        self.state.lock().unwrap().info.is_some()
    }
    pub fn m4s_subscribe(&self) -> (Vec<Bytes>, broadcast::Receiver<Bytes>) {
        let s = self.state.lock().unwrap();
        (s.bootstrap.clone(), self.m4s.subscribe())
    }
    pub fn signal_subscribe(&self) -> (Vec<Bytes>, broadcast::Receiver<Bytes>) {
        let s = self.state.lock().unwrap();
        (
            s.segments
                .back()
                .map(|v| vec![v.signal.clone()])
                .unwrap_or_default(),
            self.signals.subscribe(),
        )
    }
    pub fn segment(&self, name: &str) -> Option<Bytes> {
        self.state
            .lock()
            .unwrap()
            .segments
            .iter()
            .find(|v| v.name == name)
            .map(|v| v.bytes.clone())
    }
}
fn packet(body: Vec<u8>) -> Vec<u8> {
    [(body.len() as u32).to_be_bytes().to_vec(), body].concat()
}
pub fn encode_info(tracks: &[Track]) -> Vec<u8> {
    let mut body = Vec::new();
    for t in tracks {
        let mut handler = vec![0; 4];
        handler.extend_from_slice(&t.id.to_be_bytes());
        handler.extend_from_slice(if t.codec == "h264" {
            b"videh264\0"
        } else {
            b"sounaac\0"
        });
        let mut shft = vec![0; 4];
        shft.extend_from_slice(&90000u32.to_be_bytes());
        shft.extend_from_slice(&0u64.to_be_bytes());
        body.extend(atom(
            b"trak",
            &[
                atom(b"shft", &shft),
                atom(b"hdlr", &handler),
                atom(b"cnfg", &[vec![0; 4], t.config.clone()].concat()),
            ]
            .concat(),
        ));
    }
    packet(atom(b"MDin", &body))
}
pub fn encode_frame(track: &Track, frame: &Frame) -> Vec<u8> {
    let mut h = frame.track_id.to_be_bytes().to_vec();
    h.extend_from_slice(&[
        if track.codec == "h264" { 1 } else { 2 },
        if frame.key { 2 } else { 3 },
        u8::from(frame.key && track.codec == "h264"),
        0,
    ]);
    h.extend_from_slice(if track.codec == "h264" {
        b"h264"
    } else {
        b" aac"
    });
    h.extend_from_slice(&frame.dts.to_be_bytes());
    h.extend_from_slice(&frame.pts_offset.to_be_bytes());
    h.extend_from_slice(&0u32.to_be_bytes());
    packet(atom(
        b"FRam",
        &[atom(b"fhdr", &h), atom(b"body", &frame.body)].concat(),
    ))
}
#[derive(Default)]
pub struct FlvDecoder {
    buffer: BytesMut,
    header: bool,
    tracks: Vec<Track>,
}
impl FlvDecoder {
    pub fn push(&mut self, bytes: &[u8], hub: &Hub) -> Result<(), String> {
        if self.buffer.len() + bytes.len() > 32 * 1024 * 1024 {
            return Err("FLV buffer exceeds limit".into());
        }
        self.buffer.extend_from_slice(bytes);
        if !self.header {
            if self.buffer.len() < 13 {
                return Ok(());
            }
            if &self.buffer[..3] != b"FLV" {
                return Err("invalid FLV header".into());
            }
            self.buffer.advance(13);
            self.header = true;
        }
        while self.buffer.len() >= 11 {
            let kind = self.buffer[0];
            let n = ((self.buffer[1] as usize) << 16)
                | ((self.buffer[2] as usize) << 8)
                | self.buffer[3] as usize;
            if n > 16 * 1024 * 1024 {
                return Err("FLV tag exceeds limit".into());
            }
            if self.buffer.len() < n + 15 {
                break;
            }
            let ts = ((self.buffer[7] as u64) << 24)
                | ((self.buffer[4] as u64) << 16)
                | ((self.buffer[5] as u64) << 8)
                | self.buffer[6] as u64;
            let data = self.buffer.split_to(n + 15);
            let body = &data[11..11 + n];
            let (id, codec, config, payload, key, offset) =
                if kind == 9 && body.len() >= 5 && body[0] & 15 == 7 {
                    let raw = ((body[2] as i32) << 16) | ((body[3] as i32) << 8) | body[4] as i32;
                    let offset = ((raw << 8) >> 8) as i64 * 90;
                    (
                        1,
                        "h264",
                        body[1] == 0,
                        &body[5..],
                        body[0] >> 4 == 1,
                        offset,
                    )
                } else if kind == 8 && body.len() >= 2 && body[0] >> 4 == 10 {
                    (2, "aac", body[1] == 0, &body[2..], true, 0)
                } else {
                    continue;
                };
            if config {
                self.tracks.retain(|t| t.id != id);
                self.tracks.push(Track {
                    id,
                    codec: codec.into(),
                    config: payload.to_vec(),
                });
                hub.info(self.tracks.clone());
            } else if !payload.is_empty() {
                hub.frame(Frame {
                    track_id: id,
                    dts: ts * 90,
                    pts_offset: offset,
                    key,
                    body: payload.to_vec(),
                })?;
            }
        }
        Ok(())
    }
}
