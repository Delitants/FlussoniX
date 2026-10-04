//! Shared bounded M4S frame fan-out and M4F live segment window.
use crate::{
    m4f::{Frame, pack},
    m4s::{PackedGop, Track, atom, encode_gop, validate_tracks},
    media_queue::{Channel, Receiver},
};
use bytes::{Buf, Bytes, BytesMut};
use std::{collections::VecDeque, sync::Mutex};

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
    origin: Option<u64>,
    bootstrap_bytes: usize,
    frame_bytes: usize,
    bootstrap_ready: bool,
    segment_ready: bool,
}
pub struct Hub {
    pub m4s: Channel,
    pub signals: Channel,
    pub rtp: crate::rtp::Hub,
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
            m4s: Channel::new(256, 16 * 1024 * 1024),
            signals: Channel::new(16, 8192),
            rtp: crate::rtp::Hub::new(),
            state: Mutex::new(State {
                tracks: vec![],
                info: None,
                bootstrap: vec![],
                frames: vec![],
                segments: VecDeque::new(),
                sequence: 0,
                utc: chrono::Utc::now().timestamp_millis(),
                origin: None,
                bootstrap_bytes: 0,
                frame_bytes: 0,
                bootstrap_ready: false,
                segment_ready: false,
            }),
        }
    }
    pub fn info(&self, tracks: Vec<Track>) -> Result<(), String> {
        let wire = Bytes::from(encode_info(&tracks)?);
        self.relay_info(tracks, wire)
    }
    pub fn relay_info(&self, tracks: Vec<Track>, wire: Bytes) -> Result<(), String> {
        validate_tracks(&tracks)?;
        if wire.len() > 16 * 1024 * 1024 {
            return Err("native metadata record exceeds queue limit".into());
        }
        let mut s = self.state.lock().unwrap();
        self.rtp.configure(&tracks);
        let changed = s.tracks != tracks;
        if changed {
            s.frames.clear();
            s.frame_bytes = 0;
            s.bootstrap_ready = false;
            s.segment_ready = false;
            s.bootstrap.clear();
            s.bootstrap_bytes = 0;
        }
        s.tracks = tracks;
        s.info = Some(wire.clone());
        // Unchanged codec metadata must replace only metadata, retaining the
        // saved keyframe (and any original packed GOP) for late subscribers.
        if let Some(info) = s.bootstrap.first_mut() {
            let old_len = info.len();
            *info = wire.clone();
            s.bootstrap_bytes = s.bootstrap_bytes - old_len + wire.len();
        } else {
            s.bootstrap.push(wire.clone());
            s.bootstrap_bytes = wire.len();
        }
        if s.bootstrap_bytes > 32 * 1024 * 1024 {
            s.bootstrap = vec![wire.clone()];
            s.bootstrap_bytes = wire.len();
            s.bootstrap_ready = false;
        }
        self.m4s.send(wire)
    }
    pub fn frame(&self, frame: Frame) -> Result<(), String> {
        let tracks = self.state.lock().unwrap().tracks.clone();
        let track = tracks
            .iter()
            .find(|t| t.id == frame.track_id)
            .ok_or("unknown wire track")?;
        let wire = Bytes::from(encode_frame(track, &frame)?);
        self.relay_frame(frame, wire)
    }
    pub fn relay_frame(&self, frame: Frame, wire: Bytes) -> Result<(), String> {
        let mut s = self.state.lock().unwrap();
        let kind = s
            .tracks
            .iter()
            .find(|t| t.id == frame.track_id)
            .ok_or("unknown wire track")?
            .kind()?;
        let video = kind.is_video();
        let has_video = s
            .tracks
            .iter()
            .any(|t| t.kind().is_ok_and(|k| k.is_video()));
        self.rtp.frame(&frame);
        let origin = *s.origin.get_or_insert(frame.dts);
        let audio_boundary = kind.is_audio()
            && !has_video
            && (!s.segment_ready
                || !s.bootstrap_ready
                || s.frames
                    .first()
                    .is_some_and(|f| frame.dts.saturating_sub(f.dts) >= 180000));
        if video && frame.key || audio_boundary {
            s.bootstrap = s.info.clone().into_iter().collect();
            s.bootstrap_bytes = s.bootstrap.iter().map(Bytes::len).sum();
            s.bootstrap_ready = true;
            if s.segment_ready
                && !s.frames.is_empty()
                && frame.dts.saturating_sub(s.frames[0].dts)
                    >= if has_video { 90000 } else { 180000 }
            {
                let start = s.frames.iter().map(|f| f.dts).min().unwrap();
                let duration = frame
                    .dts
                    .checked_sub(start)
                    .ok_or("invalid segment timeline")?;
                let bytes = Bytes::from(pack(&s.tracks, &s.frames, duration)?);
                let bytes = if let Some(info) = &s.info {
                    crate::native_subtitles::carry_metadata(bytes, info)?
                } else {
                    bytes
                };
                let offset = start.saturating_sub(origin) / 90;
                let ms = s
                    .utc
                    .checked_add(i64::try_from(offset).map_err(|_| "invalid segment timeline")?)
                    .ok_or("invalid segment timeline")?;
                let date =
                    chrono::DateTime::from_timestamp_millis(ms).ok_or("invalid UTC timestamp")?;
                let stamp = date.format("%Y/%m/%d/%H/%M/%S").to_string();
                s.sequence += 1;
                let signal =
                    Bytes::from(format!("{} {}-{:05}\n", s.sequence, stamp, duration / 90));
                Self::cache(
                    &mut s,
                    Segment {
                        name: format!("{stamp}.m4f"),
                        signal: signal.clone(),
                        bytes,
                    },
                );
                s.frames.clear();
                s.frame_bytes = 0;
                self.signals.send(signal)?;
            }
            s.segment_ready = true;
        }
        if !has_video {
            s.bootstrap_ready = true;
            s.segment_ready = true;
        }
        if s.bootstrap_ready && s.bootstrap_bytes + wire.len() <= 32 * 1024 * 1024 {
            s.bootstrap_bytes += wire.len();
            s.bootstrap.push(wire.clone());
        } else if s.bootstrap_ready {
            s.bootstrap = s.info.clone().into_iter().collect();
            s.bootstrap_bytes = s.bootstrap.iter().map(Bytes::len).sum();
            s.bootstrap_ready = false;
        }
        // After a reset/overflow, withhold dependent samples until a keyframe.
        // This also protects a subscriber whose bootstrap contains only info.
        if s.bootstrap_ready {
            self.m4s.send(wire)?;
        }
        if s.segment_ready
            && s.frames.len() < 100000
            && s.frame_bytes + frame.body.len() <= 32 * 1024 * 1024
        {
            s.frame_bytes += frame.body.len();
            s.frames.push(frame)
        } else if s.segment_ready {
            s.frames.clear();
            s.frame_bytes = 0;
            s.segment_ready = false;
        }
        Ok(())
    }
    fn cache(s: &mut State, segment: Segment) {
        s.segments.push_back(segment);
        while s.segments.len() > 8
            || s.segments.iter().map(|v| v.bytes.len()).sum::<usize>() > 64 * 1024 * 1024
        {
            s.segments.pop_front();
        }
    }
    pub fn relay_gop(&self, gop: PackedGop, tracks: Vec<Track>, wire: Bytes) -> Result<(), String> {
        let date = chrono::DateTime::from_timestamp(gop.utc as i64, 0).ok_or("invalid GOP UTC")?;
        let stamp = date.format("%Y/%m/%d/%H/%M/%S").to_string();
        let signal = Bytes::from(format!(
            "{} {}-{:05}\n",
            gop.sequence,
            stamp,
            gop.duration_ms.round() as u64
        ));
        self.segment_with_wire(
            Segment {
                name: format!("{stamp}.m4f"),
                signal,
                bytes: gop.body.clone(),
            },
            tracks,
            wire,
        )
    }
    pub fn relay_segment(
        &self,
        segment: Segment,
        tracks: Vec<Track>,
        gop: PackedGop,
    ) -> Result<(), String> {
        if segment.bytes != gop.body {
            return Err("segment/body identity mismatch".into());
        }
        let wire = encode_gop(&gop)?;
        self.segment_with_wire(segment, tracks, wire)
    }
    fn segment_with_wire(
        &self,
        segment: Segment,
        tracks: Vec<Track>,
        wire: Bytes,
    ) -> Result<(), String> {
        validate_tracks(&tracks)?;
        if segment.bytes.len() > 16 * 1024 * 1024
            || wire.len() > 16 * 1024 * 1024
            || segment.signal.len() > 8192
        {
            return Err("relay segment exceeds limit".into());
        }
        let mut s = self.state.lock().unwrap();
        if let Some(old) = s.segments.iter().find(|v| v.name == segment.name) {
            return if old.bytes == segment.bytes {
                Ok(())
            } else {
                Err("segment path reused with different payload".into())
            };
        }
        if let Ok((decoded_tracks, frames)) = crate::m4f::unpack(&segment.bytes) {
            self.rtp.configure(&decoded_tracks);
            self.rtp.frames(&frames);
        }
        if s.tracks != tracks {
            s.tracks = tracks;
            s.info = Some(Bytes::from(encode_info(&s.tracks)?));
            s.frames.clear();
            s.frame_bytes = 0;
        }
        s.bootstrap = s.info.clone().into_iter().collect();
        s.bootstrap.push(wire.clone());
        s.bootstrap_bytes = s.bootstrap.iter().map(Bytes::len).sum();
        s.bootstrap_ready = false;
        let signal = segment.signal.clone();
        Self::cache(&mut s, segment);
        self.m4s.send(wire)?;
        self.signals.send(signal)?;
        Ok(())
    }
    pub fn has_info(&self) -> bool {
        self.state.lock().unwrap().info.is_some()
    }
    pub fn m4s_subscribe(&self) -> (Vec<Bytes>, Receiver) {
        let s = self.state.lock().unwrap();
        (s.bootstrap.clone(), self.m4s.subscribe())
    }
    pub fn signal_subscribe(&self) -> (Vec<Bytes>, Receiver) {
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
pub fn encode_info(tracks: &[Track]) -> Result<Vec<u8>, String> {
    validate_tracks(tracks)?;
    let total = 12
        + tracks
            .iter()
            .map(|t| 65 + t.codec.len() + t.config.len())
            .sum::<usize>();
    if total > 16 * 1024 * 1024 {
        return Err("native metadata record exceeds queue limit".into());
    }
    let mut body = Vec::new();
    for t in tracks {
        let mut handler = vec![0; 4];
        handler.extend_from_slice(&t.id.to_be_bytes());
        handler.extend_from_slice(t.kind()?.handler());
        handler.extend_from_slice(t.codec.as_bytes());
        handler.push(0);
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
    Ok(packet(atom(b"MDin", &body)))
}
pub fn encode_frame(track: &Track, frame: &Frame) -> Result<Vec<u8>, String> {
    let kind = track.kind()?;
    if track.id != frame.track_id {
        return Err("native frame/track id mismatch".into());
    }
    if frame.body.len() > 16 * 1024 * 1024 - 60 {
        return Err("native frame record exceeds queue limit".into());
    }
    let mut h = frame.track_id.to_be_bytes().to_vec();
    h.extend_from_slice(&[
        kind.content_type(),
        if frame.key { 2 } else { 3 },
        u8::from(frame.key && kind.is_video()),
        0,
    ]);
    h.extend_from_slice(&kind.tag());
    h.extend_from_slice(&frame.dts.to_be_bytes());
    h.extend_from_slice(&frame.pts_offset.to_be_bytes());
    h.extend_from_slice(&0u32.to_be_bytes());
    Ok(packet(atom(
        b"FRam",
        &[atom(b"fhdr", &h), atom(b"body", &frame.body)].concat(),
    )))
}
#[derive(Default)]
pub struct FlvDecoder {
    buffer: BytesMut,
    header: bool,
    expected: u8,
    published: bool,
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
            self.expected = self.buffer[4] & 5;
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
                // FFmpeg can send placeholder sequence headers before codec
                // extradata is available. Never advertise an incomplete input.
                if payload.len() < if codec == "h264" { 7 } else { 2 } {
                    continue;
                }
                self.tracks.retain(|t| t.id != id);
                self.tracks.push(Track {
                    id,
                    codec: codec.into(),
                    config: payload.to_vec(),
                });
                let complete = (self.expected & 1 == 0
                    || self.tracks.iter().any(|t| t.codec == "h264"))
                    && (self.expected & 4 == 0 || self.tracks.iter().any(|t| t.codec == "aac"));
                if complete {
                    hub.info(self.tracks.clone())?;
                    self.published = true;
                }
            } else if self.published && !payload.is_empty() {
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
