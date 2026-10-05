//! Bounded cross-track DTS ordering for a single worker generation.
use super::{Decoder, Event};
use crate::{m4f::Frame, m4s::Track, wire::Hub};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
const RECORDS: usize = 8192;
const BYTES: usize = 32 * 1024 * 1024;
type Key = (u64, u8, u64);
struct Clock {
    last: Option<u64>,
    progress: Instant,
    video: bool,
}
struct Interleave {
    tracks: BTreeMap<u32, Clock>,
    frames: BTreeMap<Key, Frame>,
    bytes: usize,
    sequence: u64,
    started: Instant,
}
impl Default for Interleave {
    fn default() -> Self {
        Self {
            tracks: BTreeMap::new(),
            frames: BTreeMap::new(),
            bytes: 0,
            sequence: 0,
            started: Instant::now(),
        }
    }
}
impl Interleave {
    fn info(&mut self, tracks: &[Track], now: Instant) -> Result<(), &'static str> {
        if !self.tracks.is_empty() || tracks.is_empty() || tracks.len() > 16 {
            return Err("wire_decode_failed");
        }
        for track in tracks {
            let kind = track.kind().map_err(|_| "wire_decode_failed")?;
            if !kind.is_video() && !kind.is_audio() || self.tracks.contains_key(&track.id) {
                return Err("wire_decode_failed");
            }
            self.tracks.insert(
                track.id,
                Clock {
                    last: None,
                    progress: now,
                    video: kind.is_video(),
                },
            );
        }
        Ok(())
    }
    fn size(frame: &Frame) -> usize {
        frame.body.len() + std::mem::size_of::<Frame>() + std::mem::size_of::<Key>()
    }
    fn frame(&mut self, frame: Frame, now: Instant) -> Result<Vec<Frame>, &'static str> {
        let clock = self
            .tracks
            .get_mut(&frame.track_id)
            .ok_or("wire_decode_failed")?;
        if clock.last.is_some_and(|last| frame.dts < last) {
            return Err("wire_decode_failed");
        }
        if clock.last.is_none_or(|last| frame.dts > last) {
            clock.progress = now;
        }
        clock.last = Some(frame.dts);
        let key = (frame.dts, u8::from(!clock.video), self.sequence);
        self.sequence = self.sequence.checked_add(1).ok_or("wire_decode_failed")?;
        let bytes = Self::size(&frame);
        if self.frames.len() >= RECORDS || bytes > BYTES - self.bytes {
            return Err("wire_decode_failed");
        }
        self.bytes += bytes;
        self.frames.insert(key, frame);
        let watermark = self.tracks.values().try_fold(u64::MAX, |minimum, clock| {
            clock.last.map(|last| minimum.min(last))
        });
        let mut out = vec![];
        if let Some(watermark) = watermark {
            while self
                .frames
                .first_key_value()
                .is_some_and(|(k, _)| k.0 <= watermark)
            {
                let (_, frame) = self.frames.pop_first().unwrap();
                self.bytes -= Self::size(&frame);
                out.push(frame);
            }
        }
        Ok(out)
    }
    fn finish(&mut self) -> Vec<Frame> {
        self.bytes = 0;
        std::mem::take(&mut self.frames).into_values().collect()
    }
    fn check(&self, now: Instant, timeout: Duration) -> Result<(), &'static str> {
        if self.tracks.is_empty() {
            if now.saturating_duration_since(self.started) >= timeout {
                return Err("startup_timeout");
            }
        } else if self
            .tracks
            .values()
            .any(|c| now.saturating_duration_since(c.progress) >= timeout)
        {
            return Err("input_stalled");
        }
        Ok(())
    }
}
pub(crate) struct Forwarder {
    decoder: Decoder,
    queue: Interleave,
    timeout: Duration,
    closed: bool,
}
impl Forwarder {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            decoder: Decoder::default(),
            queue: Interleave::default(),
            timeout,
            closed: false,
        }
    }
    fn events(&mut self, events: Vec<Event>, hub: &Hub) -> Result<(), &'static str> {
        let now = Instant::now();
        for event in events {
            match event {
                Event::Info(tracks) => {
                    self.queue.info(&tracks, now)?;
                    hub.info(tracks).map_err(|_| "wire_decode_failed")?;
                }
                Event::Frame(frame) => {
                    for frame in self.queue.frame(frame, now)? {
                        hub.frame(frame).map_err(|_| "wire_decode_failed")?;
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn push(&mut self, data: &[u8], hub: &Hub) -> Result<(), &'static str> {
        if self.closed {
            return Err("wire_decode_failed");
        }
        let result = (|| {
            self.queue.check(Instant::now(), self.timeout)?;
            let events = self.decoder.push(data).map_err(|_| "wire_decode_failed")?;
            self.events(events, hub)
        })();
        if result.is_err() {
            self.closed = true;
        }
        result
    }
    pub(crate) fn finish(&mut self, hub: &Hub) -> Result<(), &'static str> {
        if self.closed {
            return Err("wire_decode_failed");
        }
        self.closed = true;
        let events = self.decoder.finish().map_err(|_| "wire_decode_failed")?;
        self.events(events, hub)?;
        for frame in self.queue.finish() {
            hub.frame(frame).map_err(|_| "wire_decode_failed")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tracks() -> Vec<Track> {
        vec![
            Track {
                id: 1,
                codec: "h264".into(),
                config: vec![],
            },
            Track {
                id: 2,
                codec: "m2a".into(),
                config: vec![],
            },
        ]
    }
    fn f(id: u32, dts: u64, key: bool) -> Frame {
        Frame {
            track_id: id,
            dts,
            pts_offset: 0,
            key,
            body: vec![1],
        }
    }
    #[test]
    fn waits_for_every_track_and_flushes_by_timestamp_at_eof() {
        let now = Instant::now();
        let mut q = Interleave::default();
        q.info(&tracks(), now).unwrap();
        assert!(q.frame(f(2, 90, true), now).unwrap().is_empty());
        assert!(q.frame(f(2, 180, true), now).unwrap().is_empty());
        let out = q.frame(f(1, 90, true), now).unwrap();
        assert_eq!(out.iter().map(|f| f.track_id).collect::<Vec<_>>(), [1, 2]);
        let out = q.frame(f(1, 170, false), now).unwrap();
        assert_eq!(out[0].dts, 170);
        assert_eq!(q.finish()[0].dts, 180);
    }
    #[test]
    fn holds_audio_until_next_keyframe_and_prioritizes_key_on_equal_dts() {
        let now = Instant::now();
        let mut q = Interleave::default();
        q.info(&tracks(), now).unwrap();
        q.frame(f(1, 0, true), now).unwrap();
        q.frame(f(2, 0, true), now).unwrap();
        assert!(q.frame(f(2, 90_000, true), now).unwrap().is_empty());
        let out = q.frame(f(1, 90_000, true), now).unwrap();
        assert_eq!(out.iter().map(|f| f.track_id).collect::<Vec<_>>(), [1, 2]);
    }
    #[test]
    fn rejects_unknown_backwards_and_record_or_byte_overflow() {
        let now = Instant::now();
        let mut q = Interleave::default();
        q.info(&tracks(), now).unwrap();
        assert!(q.frame(f(3, 0, true), now).is_err());
        let mut q = Interleave::default();
        q.info(&tracks(), now).unwrap();
        q.frame(f(2, 10, true), now).unwrap();
        assert!(q.frame(f(2, 9, true), now).is_err());
        let mut q = Interleave::default();
        q.info(&tracks(), now).unwrap();
        for i in 0..8192 {
            q.frame(f(2, i, true), now).unwrap();
        }
        assert!(q.frame(f(2, 8192, true), now).is_err());
        let mut q = Interleave::default();
        q.info(&tracks(), now).unwrap();
        let mut frame = f(2, 0, true);
        frame.body = vec![0; 16 * 1024 * 1024];
        q.frame(frame.clone(), now).unwrap();
        assert!(q.frame(frame, now).is_err());
    }
    #[test]
    fn deadlines_include_missing_metadata_silent_and_frozen_track_clocks() {
        let now = Instant::now();
        let timeout = Duration::from_secs(1);
        let mut q = Interleave::default();
        assert_eq!(q.check(now + timeout * 2, timeout), Err("startup_timeout"));
        q.info(&tracks(), now).unwrap();
        q.frame(f(1, 0, true), now).unwrap();
        assert_eq!(q.check(now + timeout, timeout), Err("input_stalled"));
        q.frame(f(2, 0, true), now).unwrap();
        q.frame(f(1, 0, true), now + Duration::from_millis(900))
            .unwrap();
        q.frame(f(2, 1, true), now + Duration::from_millis(900))
            .unwrap();
        assert_eq!(q.check(now + timeout, timeout), Err("input_stalled"));
    }
}
