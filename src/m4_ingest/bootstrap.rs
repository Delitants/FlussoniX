//! FXLink-only bootstrap inspection. Key flags do not prove random access.
//! This checks access-unit framing and NAL types, not full coded-picture syntax.
use crate::m4s::{Event, Track};
use bytes::Bytes;
#[derive(Default)]
pub(super) struct Bootstrap {
    tracks: Vec<Track>,
    video: Option<(u32, Encoding)>,
    ready: bool,
    discard_rasl: bool,
}
enum Encoding {
    H264(usize),
    Hevc(crate::hevc::Configuration),
}
enum Decision {
    Keep,
    Wait,
    Leading,
}
impl Bootstrap {
    pub(super) fn ready(&self) -> bool {
        !self.tracks.is_empty() && (self.video.is_none() || self.ready)
    }
    fn configure(&mut self, tracks: &[Track]) -> Result<(), String> {
        let next = tracks
            .iter()
            .find(|t| matches!(t.codec.as_str(), "h264" | "hevc"));
        let old = self
            .tracks
            .iter()
            .find(|t| matches!(t.codec.as_str(), "h264" | "hevc"));
        if old != next {
            self.video = next
                .map(|track| {
                    Ok::<_, String>((
                        track.id,
                        if track.codec == "h264" {
                            Encoding::H264(crate::rtp::avcc(&track.config)?.0)
                        } else {
                            Encoding::Hevc(crate::hevc::Configuration::parse(&track.config)?)
                        },
                    ))
                })
                .transpose()?;
            self.ready = false;
            self.discard_rasl = false;
        }
        self.tracks = tracks.to_vec();
        Ok(())
    }
    fn accept(&mut self, id: u32, body: &[u8]) -> Result<Decision, String> {
        if !self.tracks.iter().any(|t| t.id == id) {
            return Err("FXLink sample before matching metadata".into());
        }
        let Some((video, encoding)) = &self.video else {
            return Ok(Decision::Keep);
        };
        if id != *video || self.ready && !self.discard_rasl {
            return Ok(Decision::Keep);
        }
        let mut kinds = Vec::new();
        match encoding {
            Encoding::Hevc(config) => {
                config.visit_access_unit(body, |nal| {
                    let kind = (nal[0] >> 1) & 63;
                    if kind < 32 {
                        kinds.push(kind);
                    }
                })?;
            }
            Encoding::H264(width) => {
                let mut at = 0;
                let mut count = 0;
                while at < body.len() {
                    count += 1;
                    if count > 4096 {
                        return Err("too many AVC bootstrap NALs".into());
                    }
                    let bytes = body
                        .get(at..at + width)
                        .ok_or("truncated AVC bootstrap length")?;
                    let len = bytes.iter().fold(0usize, |n, b| (n << 8) | usize::from(*b));
                    at += width;
                    let nal = body
                        .get(at..at.checked_add(len).ok_or("AVC bootstrap overflow")?)
                        .ok_or("truncated AVC bootstrap sample")?;
                    let first = *nal.first().ok_or("empty AVC bootstrap NAL")?;
                    let kind = first & 31;
                    if first & 0x80 != 0 || kind == 0 || kind >= 24 {
                        return Err("invalid AVC bootstrap NAL".into());
                    }
                    if kind <= 5 {
                        kinds.push(kind);
                    }
                    at += len;
                }
            }
        }
        let independent = match encoding {
            Encoding::H264(_) => !kinds.is_empty() && kinds.iter().all(|n| *n == 5),
            Encoding::Hevc(_) => {
                !kinds.is_empty()
                    && kinds.iter().all(|n| (16..=21).contains(n))
                    && kinds.iter().all(|n| *n == kinds[0])
            }
        };
        if !self.ready {
            if !independent {
                return Ok(Decision::Wait);
            }
            self.ready = true;
            self.discard_rasl =
                matches!(encoding, Encoding::Hevc(_)) && matches!(kinds[0], 16..=18 | 21);
        } else if self.discard_rasl {
            if kinds.iter().any(|n| matches!(n, 8 | 9)) {
                return Ok(Decision::Leading);
            }
            if independent {
                self.discard_rasl = false;
            }
        }
        Ok(Decision::Keep)
    }
    pub(super) fn events(&mut self, events: Vec<Event>) -> Result<Vec<Event>, String> {
        let mut output = Vec::new();
        for event in events {
            match event {
                Event::Info { ref tracks, .. } => {
                    self.configure(tracks)?;
                    output.push(event);
                }
                Event::Frame {
                    track_id, ref body, ..
                } => {
                    if matches!(self.accept(track_id, body)?, Decision::Keep) {
                        output.push(event);
                    }
                }
                Event::Gop {
                    gop,
                    tracks,
                    frames,
                    wire,
                } => {
                    self.configure(&tracks)?;
                    if self.ready && !self.discard_rasl || self.video.is_none() {
                        output.push(Event::Gop {
                            gop,
                            tracks,
                            frames,
                            wire,
                        });
                        continue;
                    }
                    let previous = (self.ready, self.discard_rasl);
                    let mut waiting = false;
                    let mut leading = Vec::new();
                    for (index, frame) in frames.iter().enumerate() {
                        match self.accept(frame.track_id, &frame.body)? {
                            Decision::Keep => {}
                            Decision::Wait => {
                                waiting = true;
                                break;
                            }
                            Decision::Leading => leading.push(index),
                        }
                    }
                    if waiting {
                        (self.ready, self.discard_rasl) = previous;
                        output.push(Event::Info {
                            wire: Bytes::from(crate::wire::encode_info(&tracks)?),
                            tracks,
                        });
                    } else if leading.is_empty() {
                        output.push(Event::Gop {
                            gop,
                            tracks,
                            frames,
                            wire,
                        });
                    } else {
                        let frames = frames
                            .into_iter()
                            .enumerate()
                            .filter(|(index, _)| leading.binary_search(index).is_err())
                            .map(|(_, f)| f)
                            .collect::<Vec<_>>();
                        if frames.is_empty() {
                            return Err("FXLink bootstrap has no independent samples".into());
                        }
                        let body = Bytes::from(crate::m4f::pack(
                            &tracks,
                            &frames,
                            (gop.duration_ms * 90.0).round() as u64,
                        )?);
                        let gop = crate::m4s::PackedGop { body, ..gop };
                        let wire = crate::m4s::encode_gop(&gop)?;
                        output.push(Event::Gop {
                            gop,
                            tracks,
                            frames,
                            wire,
                        });
                    }
                }
                other => output.push(other),
            }
        }
        Ok(output)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{m4s::Track, wire};
    use bytes::Bytes;
    fn track(codec: &str) -> Track {
        Track {
            id: 1,
            codec: codec.into(),
            config: if codec == "hevc" {
                include_bytes!("../../tests/fixtures/codecs/hevc.hvcc").to_vec()
            } else {
                vec![
                    1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
                ]
            },
        }
    }
    fn info(track: &Track) -> Event {
        Event::Info {
            tracks: vec![track.clone()],
            wire: Bytes::from(wire::encode_info(std::slice::from_ref(track)).unwrap()),
        }
    }
    fn frame(track: &Track, nal_type: u8) -> Event {
        let nal = if track.codec == "hevc" {
            vec![nal_type << 1, 1]
        } else {
            vec![0x60 | nal_type, 1]
        };
        let mut body = (nal.len() as u32).to_be_bytes().to_vec();
        body.extend(nal);
        let f = crate::m4f::Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body,
        };
        Event::Frame {
            track_id: 1,
            dts: f.dts,
            pts_offset: 0,
            key: true,
            body: f.body.clone(),
            wire: Bytes::from(wire::encode_frame(track, &f).unwrap()),
        }
    }
    #[test]
    fn fxlink_bootstrap_rejects_forged_h264_and_hevc_key_flags() {
        for (codec, picture, idr) in [("h264", 1, 5), ("hevc", 1, 19)] {
            let track = track(codec);
            let mut gate = Bootstrap::default();
            assert_eq!(gate.events(vec![info(&track)]).unwrap().len(), 1);
            assert!(
                gate.events(vec![frame(&track, picture)])
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(gate.events(vec![frame(&track, idr)]).unwrap().len(), 1);
            assert_eq!(gate.events(vec![frame(&track, picture)]).unwrap().len(), 1);
        }
    }
    #[test]
    fn fxlink_bootstrap_cra_discards_unsafe_leading_rasl_until_next_access_point() {
        let track = track("hevc");
        let mut gate = Bootstrap::default();
        gate.events(vec![info(&track)]).unwrap();
        assert_eq!(gate.events(vec![frame(&track, 21)]).unwrap().len(), 1);
        assert!(gate.events(vec![frame(&track, 8)]).unwrap().is_empty());
        assert!(gate.events(vec![frame(&track, 9)]).unwrap().is_empty());
        assert_eq!(gate.events(vec![frame(&track, 1)]).unwrap().len(), 1);
        assert_eq!(gate.events(vec![frame(&track, 21)]).unwrap().len(), 1);
        assert_eq!(gate.events(vec![frame(&track, 8)]).unwrap().len(), 1);
    }
    #[test]
    fn fxlink_bootstrap_rejects_truncated_access_unit_and_frame_before_metadata() {
        let track = track("h264");
        let mut gate = Bootstrap::default();
        assert!(gate.events(vec![frame(&track, 5)]).is_err());
        gate.events(vec![info(&track)]).unwrap();
        let mut f = frame(&track, 5);
        if let Event::Frame { body, .. } = &mut f {
            body[3] = 50;
        }
        assert!(gate.events(vec![f]).is_err());
    }
    fn gop(track: &Track, kinds: &[u8]) -> Event {
        let frames = kinds
            .iter()
            .enumerate()
            .map(|(i, kind)| {
                let Event::Frame { body, .. } = frame(track, *kind) else {
                    unreachable!()
                };
                crate::m4f::Frame {
                    track_id: 1,
                    dts: 90000 + i as u64 * 3600,
                    pts_offset: 0,
                    key: true,
                    body,
                }
            })
            .collect::<Vec<_>>();
        let body =
            Bytes::from(crate::m4f::pack(std::slice::from_ref(track), &frames, 90000).unwrap());
        let gop = crate::m4s::PackedGop {
            utc: 1700000000,
            dts_ms: 1000.0,
            sequence: 1,
            duration_ms: 1000.0,
            body,
        };
        let wire = crate::m4s::encode_gop(&gop).unwrap();
        Event::Gop {
            gop,
            tracks: vec![track.clone()],
            frames,
            wire,
        }
    }
    #[test]
    fn fxlink_bootstrap_preserves_safe_gop_and_rejects_dependent_prefix_as_a_whole() {
        let track = track("h264");
        let mut gate = Bootstrap::default();
        let events = gate.events(vec![gop(&track, &[1, 5, 1])]).unwrap();
        assert!(matches!(events.as_slice(), [Event::Info { .. }]));
        assert!(!gate.ready());
        let original = gop(&track, &[5, 1, 1]);
        let Event::Gop { wire: expected, .. } = &original else {
            unreachable!()
        };
        let expected = expected.clone();
        let events = gate.events(vec![original]).unwrap();
        let [Event::Gop { wire, .. }] = events.as_slice() else {
            panic!("safe bootstrap GOP was discarded")
        };
        assert_eq!(*wire, expected);
        assert!(gate.ready());
    }
    #[test]
    fn fxlink_bootstrap_cra_gop_filters_rasl_without_modifying_kept_codec_bytes() {
        let track = track("hevc");
        let mut gate = Bootstrap::default();
        let events = gate.events(vec![gop(&track, &[21, 8, 9, 1])]).unwrap();
        let [Event::Gop { frames, wire, .. }] = events.as_slice() else {
            panic!("expected filtered independent GOP")
        };
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames
                .iter()
                .map(|f| (f.body[4] >> 1) & 63)
                .collect::<Vec<_>>(),
            vec![21, 1]
        );
        let decoded = crate::m4s::Decoder::default().push(wire).unwrap();
        let [
            Event::Gop {
                frames: round_trip, ..
            },
        ] = decoded.as_slice()
        else {
            panic!("invalid filtered native wire")
        };
        assert_eq!(
            round_trip.iter().map(|f| &f.body).collect::<Vec<_>>(),
            frames.iter().map(|f| &f.body).collect::<Vec<_>>()
        );
    }
    #[test]
    fn fxlink_bootstrap_audio_only_is_ready_after_metadata() {
        let track = Track {
            id: 1,
            codec: "m2a".into(),
            config: vec![],
        };
        let mut gate = Bootstrap::default();
        assert!(!gate.ready());
        gate.events(vec![info(&track)]).unwrap();
        assert!(gate.ready());
    }
}
