//! Independent H.264/AAC RTP packetization shared by all RTSP viewers.
use crate::{m4f::Frame, m4s::Track, media_queue::Channel};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use std::{collections::VecDeque, sync::Mutex, time::Instant};
const MTU: usize = 1200;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Description {
    pub tracks: Vec<MediaTrack>,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaTrack {
    pub id: u32,
    pub payload: u8,
    pub clock: u32,
    pub ssrc: u32,
    pub fmtp: String,
    pub encoding: String,
    pub video: bool,
}
impl Description {
    pub fn sdp(&self) -> String {
        let mut text="v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=FlussoniX live\r\nc=IN IP4 0.0.0.0\r\nt=0 0\r\na=control:*\r\na=range:npt=0-\r\n".to_string();
        for t in &self.tracks {
            text.push_str(&format!(
                "m={} 0 RTP/AVP {}\r\na=rtpmap:{} {}\r\na=fmtp:{} {}\r\na=control:trackID={}\r\n",
                if t.video { "video" } else { "audio" },
                t.payload,
                t.payload,
                t.encoding,
                t.payload,
                t.fmtp,
                t.id
            ));
        }
        text
    }
}
struct PacketTrack {
    description: MediaTrack,
    length: usize,
    sequence: u16,
}
struct State {
    input: Vec<Track>,
    tracks: Vec<PacketTrack>,
    error: Option<String>,
    generation: u64,
    bootstrap: VecDeque<(u64, Bytes)>,
    bytes: usize,
    ready: bool,
    origin: Option<(u64, Instant)>,
}
/// Shares immutable AU/GOP batches; each viewer advances packet slices without copying.
pub struct Packet {
    pub bytes: Bytes,
    pub dts: u64,
}
pub struct Receiver {
    inner: crate::media_queue::Receiver,
    batch: Bytes,
    offset: usize,
    waiting_key: bool,
}
impl Receiver {
    pub fn is_lagged(&self) -> bool {
        self.inner.is_lagged()
    }
    pub async fn recv(&mut self) -> Result<Bytes, tokio::sync::broadcast::error::RecvError> {
        self.recv_timed().await.map(|packet| packet.bytes)
    }
    pub async fn recv_timed(&mut self) -> Result<Packet, tokio::sync::broadcast::error::RecvError> {
        loop {
            if self.offset == self.batch.len() {
                self.batch = self.inner.recv().await?;
                self.offset = 0;
            }
            let at = self.offset;
            let len = u32::from_be_bytes(self.batch[at..at + 4].try_into().unwrap()) as usize;
            if self.batch[at + 4] != 0 {
                self.waiting_key = false;
            }
            let dts = u64::from_be_bytes(self.batch[at + 5..at + 13].try_into().unwrap());
            self.offset += 13 + len;
            if !self.waiting_key {
                return Ok(Packet {
                    bytes: self.batch.slice(at + 13..self.offset),
                    dts,
                });
            }
        }
    }
}

pub struct PlaySnapshot {
    pub description: Description,
    pub packets: Vec<Bytes>,
    pub decode_times: Vec<u64>,
    pub receiver: Receiver,
    pub positions: Vec<(u32, u16, u32)>,
}
pub struct Hub {
    q: Channel,
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
            q: Channel::new(4096, 64 * 1024 * 1024),
            state: Mutex::new(State {
                input: vec![],
                tracks: vec![],
                error: None,
                generation: 0,
                bootstrap: VecDeque::new(),
                bytes: 0,
                ready: false,
                origin: None,
            }),
        }
    }
    pub fn configure(&self, tracks: &[Track]) {
        let mut s = self.state.lock().unwrap();
        if s.input == tracks {
            return;
        }
        s.input = tracks.to_vec();
        s.generation = s.generation.wrapping_add(1);
        s.bootstrap.clear();
        s.bytes = 0;
        s.ready = false;
        s.origin = None;
        match parse_tracks(tracks) {
            Ok(parsed) => {
                s.tracks = parsed;
                s.error = None;
            }
            Err(error) => {
                s.tracks.clear();
                s.error = Some(error);
            }
        }
    }
    pub fn description(&self) -> Option<Result<Description, String>> {
        let s = self.state.lock().unwrap();
        if s.input.is_empty() {
            None
        } else {
            Some(describe(&s))
        }
    }
    pub fn subscribe(&self) -> Result<(Description, Vec<Bytes>, Receiver), String> {
        let s = self.state.lock().unwrap();
        Ok((
            describe(&s)?,
            s.bootstrap.iter().map(|(_, b)| b.clone()).collect(),
            self.receiver(&s),
        ))
    }
    pub fn play_snapshot(&self) -> Result<PlaySnapshot, String> {
        let s = self.state.lock().unwrap();
        let description = describe(&s)?;
        let packets: Vec<Bytes> = s.bootstrap.iter().map(|(_, b)| b.clone()).collect();
        let positions =
            s.tracks
                .iter()
                .map(|t| {
                    if let Some(packet) = packets.iter().find(|b| {
                        u32::from_be_bytes(b[..4].try_into().unwrap()) == t.description.id
                    }) {
                        (
                            t.description.id,
                            u16::from_be_bytes(packet[6..8].try_into().unwrap()),
                            u32::from_be_bytes(packet[8..12].try_into().unwrap()),
                        )
                    } else {
                        (
                            t.description.id,
                            t.sequence,
                            s.origin
                                .map(|(dts, _)| {
                                    ((dts as u128 * t.description.clock as u128) / 90000) as u32
                                })
                                .unwrap_or(0),
                        )
                    }
                })
                .collect();
        Ok(PlaySnapshot {
            description,
            decode_times: s.bootstrap.iter().map(|(dts, _)| *dts).collect(),
            packets,
            receiver: self.receiver(&s),
            positions,
        })
    }
    fn receiver(&self, s: &State) -> Receiver {
        Receiver {
            inner: self.q.subscribe(),
            batch: Bytes::new(),
            offset: 0,
            waiting_key: !s.ready && s.tracks.iter().any(|t| t.description.video),
        }
    }
    pub fn clock(&self, id: u32) -> Option<u32> {
        let s = self.state.lock().unwrap();
        let t = s.tracks.iter().find(|t| t.description.id == id)?;
        let (dts, at) = s.origin?;
        Some(
            ((dts as u128 * t.description.clock as u128 / 90000)
                + (at.elapsed().as_nanos() * t.description.clock as u128 / 1_000_000_000))
                as u32,
        )
    }
    pub fn frame(&self, f: &Frame) {
        self.frames(std::slice::from_ref(f));
    }
    /// A packed GOP is one publication so packet bursts cannot evict their own beginning.
    pub fn frames(&self, frames: &[Frame]) {
        let mut s = self.state.lock().unwrap();
        let mut batch = Vec::new();
        for f in frames {
            Self::packetize(&mut s, f, &mut batch);
            if s.error.is_some() {
                return;
            }
        }
        if !batch.is_empty() {
            let _ = self.q.send(Bytes::from(batch));
        }
    }
    fn packetize(s: &mut State, f: &Frame, batch: &mut Vec<u8>) {
        if s.error.is_some() {
            return;
        }
        let Some(index) = s.tracks.iter().position(|t| t.description.id == f.track_id) else {
            return;
        };
        let video = s.tracks[index].description.video;
        // Validate the complete access unit before publishing any packet.
        let payloads = match payloads(&s.tracks[index], &f.body) {
            Ok(p) => p,
            Err(e) => {
                s.error = Some(e);
                s.generation = s.generation.wrapping_add(1);
                s.bootstrap.clear();
                s.bytes = 0;
                return;
            }
        };
        s.origin.get_or_insert((f.dts, Instant::now()));
        if video && f.key {
            s.bootstrap.clear();
            s.bytes = 0;
            s.ready = true;
        }
        let audio_only = !s.tracks.iter().any(|t| t.description.video);
        if audio_only {
            s.ready = true;
        }
        if audio_only {
            while s
                .bootstrap
                .front()
                .is_some_and(|(dts, _)| f.dts.saturating_sub(*dts) > 180000)
            {
                let (_, p) = s.bootstrap.pop_front().unwrap();
                s.bytes -= p.len();
            }
        }
        let track = &mut s.tracks[index];
        let stamp = ((f.dts as i128 + f.pts_offset as i128) * track.description.clock as i128
            / 90000)
            .rem_euclid(1i128 << 32) as u32;
        let count = payloads.len();
        let mut packets = Vec::with_capacity(count);
        for (i, payload) in payloads.into_iter().enumerate() {
            let mut b = Vec::with_capacity(16 + payload.len());
            b.extend(f.track_id.to_be_bytes());
            b.push(0x80);
            b.push(track.description.payload | if i + 1 == count { 128 } else { 0 });
            b.extend(track.sequence.to_be_bytes());
            b.extend(stamp.to_be_bytes());
            b.extend(track.description.ssrc.to_be_bytes());
            b.extend(payload);
            track.sequence = track.sequence.wrapping_add(1);
            packets.push(Bytes::from(b));
        }
        let added: usize = packets.iter().map(Bytes::len).sum();
        if s.bytes + added > 32 * 1024 * 1024 || s.bootstrap.len() + count > 100000 {
            s.bootstrap.clear();
            s.bytes = 0;
            s.ready = false;
        }
        for (i, p) in packets.into_iter().enumerate() {
            if batch.len() + 13 + p.len() > 64 * 1024 * 1024 {
                s.error = Some("RTP producer batch exceeds 64 MiB".into());
                s.generation = s.generation.wrapping_add(1);
                s.bootstrap.clear();
                s.bytes = 0;
                s.ready = false;
                batch.clear();
                return;
            }
            batch.extend((p.len() as u32).to_be_bytes());
            batch.push(u8::from(video && f.key && i == 0));
            batch.extend(f.dts.to_be_bytes());
            batch.extend_from_slice(&p);
            if s.ready {
                s.bytes += p.len();
                s.bootstrap.push_back((f.dts, p));
            }
        }
    }
}
fn describe(s: &State) -> Result<Description, String> {
    if let Some(e) = &s.error {
        return Err(e.clone());
    }
    if s.tracks.is_empty() {
        return Err("RTP media not ready".into());
    }
    Ok(Description {
        tracks: s.tracks.iter().map(|t| t.description.clone()).collect(),
        generation: s.generation,
    })
}
fn parse_tracks(tracks: &[Track]) -> Result<Vec<PacketTrack>, String> {
    if tracks.is_empty() || tracks.len() > 2 {
        return Err("RTSP requires one or two H.264/AAC tracks".into());
    }
    let mut out: Vec<PacketTrack> = vec![];
    for t in tracks {
        if out
            .iter()
            .any(|old| old.description.id == t.id || old.description.video == (t.codec == "h264"))
        {
            return Err("duplicate RTP track or codec".into());
        }
        let uuid = uuid::Uuid::new_v4();
        let random = uuid.as_bytes();
        let ssrc = u32::from_be_bytes(random[0..4].try_into().unwrap());
        let sequence = u16::from_be_bytes(random[4..6].try_into().unwrap());
        let (length, description) = match t.codec.as_str() {
            "h264" => {
                let (length, sets) = avcc(&t.config)?;
                let profile = format!("{:02x}{:02x}{:02x}", sets[0][1], sets[0][2], sets[0][3]);
                (
                    length,
                    MediaTrack {
                        id: t.id,
                        payload: 96,
                        clock: 90000,
                        ssrc,
                        video: true,
                        encoding: "H264/90000".into(),
                        fmtp: format!(
                            "packetization-mode=1;profile-level-id={profile};sprop-parameter-sets={}",
                            sets.iter()
                                .map(|b| STANDARD.encode(b))
                                .collect::<Vec<_>>()
                                .join(",")
                        ),
                    },
                )
            }
            "aac" => {
                let c = &t.config;
                if !(c.len() == 2 || c.len() == 5 && c[2..] == [0x56, 0xe5, 0]) {
                    return Err("RTSP AAC requires AAC-LC AudioSpecificConfig".into());
                }
                let object = c[0] >> 3;
                let frequency = ((c[0] & 7) << 1) | (c[1] >> 7);
                let channels = (c[1] >> 3) & 15;
                if object != 2 || frequency > 12 || !(1..=7).contains(&channels) || c[1] & 7 != 0 {
                    return Err(
                        "RTSP AAC requires AAC-LC, indexed rate and standard channels".into(),
                    );
                }
                let rate = [
                    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025,
                    8000, 7350,
                ][frequency as usize];
                let channels = if channels == 7 { 8 } else { channels };
                (
                    0,
                    MediaTrack {
                        id: t.id,
                        payload: 97,
                        clock: rate,
                        ssrc,
                        video: false,
                        encoding: format!("MPEG4-GENERIC/{rate}/{channels}"),
                        fmtp: format!(
                            "streamtype=5;profile-level-id=1;mode=AAC-hbr;config={:02x}{:02x};sizeLength=13;indexLength=3;indexDeltaLength=3;constantDuration=1024",
                            c[0], c[1]
                        ),
                    },
                )
            }
            _ => return Err("unsupported RTSP codec".into()),
        };
        out.push(PacketTrack {
            description,
            length,
            sequence,
        });
    }
    Ok(out)
}
fn avcc(c: &[u8]) -> Result<(usize, Vec<&[u8]>), String> {
    let error = || "invalid AVCDecoderConfigurationRecord".to_string();
    if c.len() < 7 || c.len() > 65536 || c[0] != 1 || c[4] & 3 == 2 {
        return Err(error());
    }
    let width = (c[4] & 3) as usize + 1;
    let mut cursor = 6;
    let mut sets = vec![];
    for (ty, count) in [(7, (c[5] & 31) as usize), (8, usize::MAX)] {
        let count = if count == usize::MAX {
            let n = *c.get(cursor).ok_or_else(error)? as usize;
            cursor += 1;
            n
        } else {
            count
        };
        if count == 0 || count > 16 {
            return Err(error());
        }
        for _ in 0..count {
            let size = c.get(cursor..cursor + 2).ok_or_else(error)?;
            cursor += 2;
            let n = u16::from_be_bytes(size.try_into().unwrap()) as usize;
            let bytes = c.get(cursor..cursor + n).ok_or_else(error)?;
            cursor += n;
            if n < if ty == 7 { 4 } else { 2 } || bytes[0] & 0x80 != 0 || bytes[0] & 31 != ty {
                return Err(error());
            }
            sets.push(bytes);
        }
    }
    Ok((width, sets))
}
fn payloads(t: &PacketTrack, body: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if body.is_empty() || body.len() > 16 * 1024 * 1024 {
        return Err("invalid RTP access unit size".into());
    }
    let mut out = vec![];
    if !t.description.video {
        if body.len() > 8191 {
            return Err("AAC AU exceeds 13-bit length".into());
        }
        for chunk in body.chunks(MTU - 16) {
            let mut p = 16u16.to_be_bytes().to_vec();
            p.extend(((body.len() as u16) << 3).to_be_bytes());
            p.extend(chunk);
            out.push(p);
        }
        return Ok(out);
    }
    let mut cursor = 0;
    let mut nals = 0;
    while cursor < body.len() {
        nals += 1;
        if nals > 4096 {
            return Err("too many AVC NAL units".into());
        }
        let len = body
            .get(cursor..cursor + t.length)
            .ok_or("truncated AVC length")?
            .iter()
            .fold(0usize, |n, b| (n << 8) | *b as usize);
        cursor += t.length;
        let nal = body
            .get(cursor..cursor.checked_add(len).ok_or("AVC length overflow")?)
            .ok_or("truncated AVC NAL")?;
        cursor += len;
        if nal.is_empty() || nal[0] & 0x80 != 0 || !(1..=23).contains(&(nal[0] & 31)) {
            return Err("invalid single AVC NAL type".into());
        }
        if nal.len() <= MTU - 12 {
            out.push(nal.to_vec());
        } else {
            let chunks = nal[1..].chunks(MTU - 14);
            let count = chunks.len();
            for (i, chunk) in chunks.enumerate() {
                let mut p = vec![
                    (nal[0] & 0xe0) | 28,
                    (nal[0] & 31)
                        | if i == 0 { 128 } else { 0 }
                        | if i + 1 == count { 64 } else { 0 },
                ];
                p.extend(chunk);
                out.push(p);
            }
        }
    }
    Ok(out)
}
