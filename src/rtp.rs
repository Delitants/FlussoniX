//! Independent H.264/HEVC and AAC/MPEG audio RTP packetization.
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
                "m={} 0 RTP/AVP {}\r\na=rtpmap:{} {}\r\n",
                if t.video { "video" } else { "audio" },
                t.payload,
                t.payload,
                t.encoding
            ));
            if !t.fmtp.is_empty() {
                text.push_str(&format!("a=fmtp:{} {}\r\n", t.payload, t.fmtp));
            }
            text.push_str(&format!("a=control:trackID={}\r\n", t.id));
        }
        text
    }
}
struct PacketTrack {
    description: MediaTrack,
    length: usize,
    hevc: bool,
    mpeg_audio: Option<crate::codec::Codec>,
    audio_end: Option<i128>,
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
    pub(crate) fn finish(&self) {
        self.q.close();
    }
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
        let positions = s
            .tracks
            .iter()
            .map(|t| {
                if let Some(packet) = packets
                    .iter()
                    .find(|b| u32::from_be_bytes(b[..4].try_into().unwrap()) == t.description.id)
                {
                    (
                        t.description.id,
                        u16::from_be_bytes(packet[6..8].try_into().unwrap()),
                        u32::from_be_bytes(packet[8..12].try_into().unwrap()),
                    )
                } else {
                    (
                        t.description.id,
                        t.sequence,
                        // A keyframe may have cleared a track's cached packets.
                        // Map that track to this GOP's media time, not stream startup.
                        s.bootstrap
                            .front()
                            .map(|(dts, _)| *dts)
                            .or_else(|| s.origin.map(|(dts, _)| dts))
                            .map(|dts| ((dts as u128 * t.description.clock as u128) / 90000) as u32)
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
        let prepared = if let Some(codec) = s.tracks[index].mpeg_audio {
            mpeg_payloads(codec, &f.body).map(|(packets, duration)| (packets, Some(duration)))
        } else {
            payloads(&s.tracks[index], &f.body).map(|packets| (packets, None))
        };
        let (payloads, mpeg_duration) = match prepared {
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
        let pts = f.dts as i128 + f.pts_offset as i128;
        let talkspurt =
            mpeg_duration.is_some() && track.audio_end.is_none_or(|end| (pts - end).abs() > 1);
        if let Some(duration) = mpeg_duration {
            track.audio_end = Some(pts + i128::from(duration));
        }
        let stamp = (pts * track.description.clock as i128 / 90000).rem_euclid(1i128 << 32) as u32;
        let count = payloads.len();
        let mut packets = Vec::with_capacity(count);
        for (i, payload) in payloads.into_iter().enumerate() {
            let mut b = Vec::with_capacity(16 + payload.len());
            b.extend(f.track_id.to_be_bytes());
            b.push(0x80);
            let marker = if mpeg_duration.is_some() {
                i == 0 && talkspurt
            } else {
                i + 1 == count
            };
            b.push(track.description.payload | if marker { 128 } else { 0 });
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
    let tracks: Vec<_> = tracks.iter().filter(|t| t.codec != "subtitle").collect();
    if tracks.is_empty() || tracks.len() > 2 {
        return Err(
            "RTSP requires one video (H.264/HEVC) and/or one audio (AAC/MPEG) track".into(),
        );
    }
    let mut out: Vec<PacketTrack> = vec![];
    for t in tracks {
        if out.iter().any(|old| {
            old.description.id == t.id
                || old.description.video == matches!(t.codec.as_str(), "h264" | "hevc")
        }) {
            return Err("duplicate RTP track or codec".into());
        }
        let uuid = uuid::Uuid::new_v4();
        let random = uuid.as_bytes();
        let ssrc = u32::from_be_bytes(random[0..4].try_into().unwrap());
        let sequence = u16::from_be_bytes(random[4..6].try_into().unwrap());
        let (length, description) = match t.codec.as_str() {
            "m2a" | "mp3" => {
                if t.config.len() > 65536 {
                    return Err("RTSP MPEG audio configuration exceeds 64 KiB".into());
                }
                // MPEG audio headers carry their own rate/layer/channel data.
                // Native opaque configuration is not converted into AAC ASC.
                (
                    0,
                    MediaTrack {
                        id: t.id,
                        payload: 14,
                        clock: 90000,
                        ssrc,
                        video: false,
                        encoding: "MPA/90000".into(),
                        fmtp: String::new(),
                    },
                )
            }
            "hevc" => {
                if t.config.len() > 65536 {
                    return Err("RTSP HEVC configuration exceeds 64 KiB".into());
                }
                let config = crate::hevc::Configuration::parse(&t.config)?;
                let mut sets: [Vec<String>; 3] = Default::default();
                for nal in config.initialization_nals() {
                    let kind = hevc_nal_type(nal)?;
                    if (32..=34).contains(&kind) {
                        sets[(kind - 32) as usize].push(STANDARD.encode(nal));
                    }
                }
                (
                    config.nal_length_size,
                    MediaTrack {
                        id: t.id,
                        payload: 96,
                        clock: 90000,
                        ssrc,
                        video: true,
                        encoding: "H265/90000".into(),
                        fmtp: format!(
                            "sprop-vps={};sprop-sps={};sprop-pps={};sprop-max-don-diff=0",
                            sets[0].join(","),
                            sets[1].join(","),
                            sets[2].join(",")
                        ),
                    },
                )
            }
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
            hevc: t.codec == "hevc",
            mpeg_audio: match t.codec.as_str() {
                "m2a" => Some(crate::codec::Codec::M2a),
                "mp3" => Some(crate::codec::Codec::Mp3),
                _ => None,
            },
            audio_end: None,
            sequence,
        });
    }
    Ok(out)
}
fn mpeg_payloads(codec: crate::codec::Codec, body: &[u8]) -> Result<(Vec<Vec<u8>>, u32), String> {
    let header = crate::mpeg_audio::inspect(codec, body)?;
    // RFC 2250's profile covers MPEG-1/2; MPEG-2.5 is a separate extension.
    if (body[1] >> 3) & 3 == 0 {
        return Err("RTSP MPEG-2.5 audio is not supported".into());
    }
    let mut packets = Vec::new();
    for (i, chunk) in body.chunks(MTU - 16).enumerate() {
        let mut payload = vec![0, 0];
        payload.extend(((i * (MTU - 16)) as u16).to_be_bytes());
        payload.extend_from_slice(chunk);
        packets.push(payload);
    }
    Ok((packets, header.duration_90k()))
}
pub(crate) fn avcc(c: &[u8]) -> Result<(usize, Vec<&[u8]>), String> {
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
    if t.hevc {
        return hevc_payloads(t.length, body);
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

// Single-layer SRST, decode-order transmission without DONL (RFC 7798).
fn hevc_nal_type(nal: &[u8]) -> Result<u8, String> {
    if nal.len() < 2 || nal[0] & 0x81 != 0 || nal[1] & 0xf8 != 0 || nal[1] & 7 == 0 {
        return Err("RTSP HEVC requires a valid single-layer NAL header".into());
    }
    let kind = (nal[0] >> 1) & 63;
    if kind >= 48 {
        return Err("invalid single HEVC NAL type".into());
    }
    Ok(kind)
}
fn hevc_payloads(width: usize, body: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::new();
    let mut cursor = 0;
    let mut count = 0;
    while cursor < body.len() {
        count += 1;
        if count > 4096 {
            return Err("too many HEVC NAL units".into());
        }
        let size = body
            .get(cursor..cursor + width)
            .ok_or("truncated HEVC length")?
            .iter()
            .fold(0usize, |n, b| (n << 8) | usize::from(*b));
        cursor += width;
        let end = cursor.checked_add(size).ok_or("HEVC length overflow")?;
        let nal = body.get(cursor..end).ok_or("truncated HEVC NAL")?;
        cursor = end;
        let kind = hevc_nal_type(nal)?;
        if nal.len() <= MTU - 12 {
            out.push(nal.to_vec());
        } else {
            let chunks = nal[2..].chunks(MTU - 15);
            let fragments = chunks.len();
            for (i, chunk) in chunks.enumerate() {
                let mut p = vec![
                    (nal[0] & 0x81) | (49 << 1),
                    nal[1],
                    kind | if i == 0 { 128 } else { 0 } | if i + 1 == fragments { 64 } else { 0 },
                ];
                p.extend_from_slice(chunk);
                out.push(p);
            }
        }
    }
    Ok(out)
}
