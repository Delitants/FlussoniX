//! Selected literal UTF-8 native text. Source clocks are observed before filtering.
use crate::{caption_hls::State, captions::NATIVE_BASE, m4f::Frame, m4s::Track};
use std::sync::atomic::Ordering;
fn text(body: &[u8]) -> Result<String, &'static str> {
    if body.len() > 16 * 1024 {
        return Err("native_subtitle_text_limit");
    }
    let value = std::str::from_utf8(body).map_err(|_| "native_subtitle_invalid_utf8")?;
    if value
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\r' | '\n' | '\t'))
    {
        return Err("native_subtitle_invalid_text");
    }
    let value = value.replace("\r\n", "\n").replace('\r', "\n");
    if value.split('\n').count() > 64 {
        return Err("native_subtitle_text_limit");
    }
    if value.trim().is_empty() {
        return Ok(String::new());
    }
    // WebVTT blank lines delimit cues. A blank display row must stay inside this cue.
    Ok(value
        .trim()
        .split('\n')
        .map(|line| {
            if line.trim().is_empty() {
                "\u{a0}"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n"))
}
fn clock_track(tracks: &[Track]) -> Option<&Track> {
    tracks
        .iter()
        .find(|t| t.kind().is_ok_and(|c| c.is_video()))
        .or_else(|| tracks.iter().find(|t| t.kind().is_ok_and(|c| c.is_audio())))
}
impl State {
    fn native_failed(&self, reason: &'static str) {
        self.decoder.lock().unwrap().error = Some(reason);
        self.failed.store(true, Ordering::Relaxed);
    }
    pub(crate) fn native_tracks(&self, tracks: &[Track]) {
        if self.failed.load(Ordering::Relaxed) {
            return;
        }
        if clock_track(tracks).is_none() {
            self.native_failed("native_subtitle_av_required");
            return;
        }
        self.decoder.lock().unwrap().native_audio_only =
            !tracks.iter().any(|t| t.kind().is_ok_and(|c| c.is_video()));
        let invalid = self
            .decoder
            .lock()
            .unwrap()
            .services
            .iter()
            .filter_map(|s| s.native_track())
            .any(|id| tracks.iter().any(|t| t.id == id && t.codec != "subtitle"));
        if invalid {
            self.native_failed("native_subtitle_track_mismatch");
        }
    }
    pub(crate) fn native_frame(&self, tracks: &[Track], frame: &Frame) {
        if self.failed.load(Ordering::Relaxed) {
            return;
        }
        let Some(track) = tracks.iter().find(|t| t.id == frame.track_id) else {
            return;
        };
        if track.kind().is_ok_and(|c| c.is_video() || c.is_audio()) {
            // The AV packager's first video owns the clock, or first audio with no video.
            if clock_track(tracks).map(|t| t.id) != Some(track.id) {
                return;
            }
            if track.kind().is_ok_and(|c| c.is_audio()) && frame.pts_offset != 0 {
                self.native_failed("native_subtitle_clock_discontinuity");
                return;
            }
            if frame.pts_offset.unsigned_abs() > 2 * 90000 {
                self.native_failed("native_subtitle_clock_discontinuity");
                return;
            }
            let Some(pts) = frame
                .dts
                .checked_add_signed(frame.pts_offset)
                .filter(|pts| *pts < i64::MAX as u64)
            else {
                self.native_failed("native_subtitle_clock_discontinuity");
                return;
            };
            let mut d = self.decoder.lock().unwrap();
            if d.native_clock_dts
                .is_some_and(|last| frame.dts < last || frame.dts.saturating_sub(last) > 10 * 90000)
            {
                d.error = Some("native_subtitle_clock_discontinuity");
                self.failed.store(true, Ordering::Relaxed);
                return;
            }
            d.native_clock_dts = Some(frame.dts);
            d.observe(pts);
        } else if track.codec == "subtitle" {
            let channel = NATIVE_BASE + u64::from(frame.track_id);
            if !self
                .decoder
                .lock()
                .unwrap()
                .services
                .iter()
                .any(|s| s.channel == channel)
            {
                return;
            }
            let result = (|| {
                if self
                    .decoder
                    .lock()
                    .unwrap()
                    .native_clock_dts
                    .is_some_and(|dts| frame.dts < dts)
                {
                    return Err("native_subtitle_clock_discontinuity");
                }
                let value = text(&frame.body)?;
                let end = frame
                    .dts
                    .checked_add_signed(frame.pts_offset)
                    .filter(|end| *end < i64::MAX as u64)
                    .ok_or("native_subtitle_invalid_timing")?;
                if end < frame.dts
                    || end - frame.dts > 120 * 90000
                    || (!value.is_empty() && end == frame.dts)
                {
                    return Err("native_subtitle_invalid_timing");
                }
                self.decoder
                    .lock()
                    .unwrap()
                    .native_cue(channel, frame.dts, end, value)
            })();
            if let Err(reason) = result {
                self.native_failed(reason);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn state() -> State {
        State::new(crate::captions::Decoder::new(crate::captions::configuration(&json!({"flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"}]})).unwrap()), "owned".into(), 0)
    }
    fn track() -> Track {
        Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        }
    }
    fn frame(body: Vec<u8>, dts: u64, offset: i64) -> Frame {
        Frame {
            track_id: 7,
            dts,
            pts_offset: offset,
            key: true,
            body,
        }
    }
    #[test]
    fn native_clear_truncates_a_cue_without_becoming_text() {
        let s = state();
        s.native_frame(&[track()], &frame(b"hello".to_vec(), 90000, 180000));
        s.native_frame(&[track()], &frame(vec![], 180000, 0));
        assert!(!s.failed.load(Ordering::Relaxed));
        let d = s.decoder.lock().unwrap();
        assert_eq!(d.snapshot().len(), 1);
        assert_eq!(d.snapshot()[0].end, Some(180000));
    }
    #[test]
    fn native_limits_disable_conversion_without_accepting_invalid_cues() {
        for (body, offset, error) in [
            (vec![b'x'; 16385], 90000, "native_subtitle_text_limit"),
            (vec![0xff], 90000, "native_subtitle_invalid_utf8"),
            (b"a\0b".to_vec(), 90000, "native_subtitle_invalid_text"),
            (b"a".to_vec(), 0, "native_subtitle_invalid_timing"),
            (b"a".to_vec(), -1, "native_subtitle_invalid_timing"),
            (
                b"a".to_vec(),
                120 * 90000 + 1,
                "native_subtitle_invalid_timing",
            ),
            (
                "a\n".repeat(65).into_bytes(),
                90000,
                "native_subtitle_text_limit",
            ),
        ] {
            let s = state();
            s.native_frame(&[track()], &frame(body, 90000, offset));
            assert!(s.failed.load(Ordering::Relaxed));
            assert_eq!(s.decoder.lock().unwrap().error, Some(error));
            assert!(s.decoder.lock().unwrap().snapshot().is_empty());
        }
    }
    #[test]
    fn native_text_normalizes_display_rows_without_vtt_delimiters() {
        assert_eq!(
            text(b"one\r\n\r\ntwo\rthree").unwrap(),
            "one\n\u{a0}\ntwo\nthree"
        );
        assert_eq!(text(b"\r\n \t ").unwrap(), "");
    }
}
#[cfg(test)]
mod clock_tests {
    use super::*;
    use serde_json::json;
    fn state() -> State {
        State::new(crate::captions::Decoder::new(crate::captions::configuration(&json!({"flussonix_hls_captions":[{"native_track":7,"language":"en","name":"Text"}]})).unwrap()),"owned".into(),0)
    }
    fn video(dts: u64, pts_offset: i64) -> Frame {
        Frame {
            track_id: 1,
            dts,
            pts_offset,
            key: true,
            body: vec![],
        }
    }
    #[test]
    fn late_native_text_and_video_pts_jumps_fail_instead_of_rewriting_served_cues() {
        let tracks = [
            Track {
                id: 1,
                codec: "hevc".into(),
                config: vec![],
            },
            Track {
                id: 7,
                codec: "subtitle".into(),
                config: vec![],
            },
        ];
        let s = state();
        s.native_frame(&tracks, &video(90000, 0));
        s.native_frame(&tracks, &video(900000, 0));
        s.native_frame(
            &tracks,
            &Frame {
                track_id: 7,
                dts: 180000,
                pts_offset: 90000,
                key: true,
                body: b"late".to_vec(),
            },
        );
        assert_eq!(
            s.decoder.lock().unwrap().error,
            Some("native_subtitle_clock_discontinuity")
        );
        assert!(s.failed.load(Ordering::Relaxed));
        let s = state();
        s.native_frame(&tracks, &video(90000, 0));
        s.native_frame(&tracks, &video(93600, 900000));
        assert!(s.failed.load(Ordering::Relaxed));
    }
    #[test]
    fn native_history_has_a_byte_bound_as_well_as_a_cue_bound() {
        let s = state();
        let tracks = [Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        }];
        for n in 0..65 {
            s.native_frame(
                &tracks,
                &Frame {
                    track_id: 7,
                    dts: 90000 + n * 900,
                    pts_offset: 900,
                    key: true,
                    body: vec![b'x'; 16384],
                },
            );
        }
        assert_eq!(
            s.decoder.lock().unwrap().error,
            Some("native_subtitle_history_limit")
        );
        assert!(s.failed.load(Ordering::Relaxed));
    }
}
#[cfg(test)]
mod frontier_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn native_frontier_uses_decoded_dts_and_rejects_older_text_events() {
        let s=State::new(crate::captions::Decoder::new(crate::captions::configuration(&json!({"flussonix_hls_captions":[{"native_track":7,"language":"en","name":"Text"}]})).unwrap()),"owned".into(),0);
        let tracks = [
            Track {
                id: 1,
                codec: "hevc".into(),
                config: vec![],
            },
            Track {
                id: 7,
                codec: "subtitle".into(),
                config: vec![],
            },
        ];
        s.native_frame(
            &tracks,
            &Frame {
                track_id: 1,
                dts: 180000,
                pts_offset: 90000,
                key: true,
                body: vec![],
            },
        );
        assert_eq!(
            s.decoder.lock().unwrap().publication_frontier(),
            180000,
            "future video PTS must not publish cues beyond source event progress"
        );
        s.native_frame(
            &tracks,
            &Frame {
                track_id: 7,
                dts: 150000,
                pts_offset: 90000,
                key: true,
                body: b"late".to_vec(),
            },
        );
        assert!(
            s.failed.load(Ordering::Relaxed),
            "late cue must not be silently omitted from an immutable rendition"
        );
    }
}

#[cfg(test)]
mod audio_clock_tests {
    use super::*;
    use serde_json::json;
    fn state() -> State {
        State::new(crate::captions::Decoder::new(crate::captions::configuration(&json!({"flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"}]})).unwrap()),"owned".into(),0)
    }
    fn track(id: u32, codec: &str) -> Track {
        Track {
            id,
            codec: codec.into(),
            config: vec![],
        }
    }
    fn frame(id: u32, dts: u64) -> Frame {
        Frame {
            track_id: id,
            dts,
            pts_offset: 0,
            key: true,
            body: vec![],
        }
    }
    #[test]
    fn audio_clock_uses_first_audio_only_when_video_is_absent() {
        let s = state();
        let tracks = [track(7, "subtitle"), track(2, "aac"), track(3, "mp3")];
        s.native_tracks(&tracks);
        assert!(!s.failed.load(Ordering::Relaxed));
        s.native_frame(&tracks, &frame(3, 99000));
        assert!(s.decoder.lock().unwrap().first_pts.is_none());
        s.native_frame(&tracks, &frame(2, 90000));
        assert_eq!(s.decoder.lock().unwrap().first_pts, Some(90000));
        let s = state();
        let tracks = [track(2, "aac"), track(1, "hevc"), track(7, "subtitle")];
        s.native_tracks(&tracks);
        s.native_frame(&tracks, &frame(2, 90000));
        assert!(s.decoder.lock().unwrap().first_pts.is_none());
        s.native_frame(&tracks, &frame(1, 97200));
        assert_eq!(s.decoder.lock().unwrap().first_pts, Some(97200));
    }
    #[test]
    fn audio_clock_rejects_gaps_backwards_time_and_late_cues() {
        let s = state();
        let tracks = [track(2, "aac"), track(7, "subtitle")];
        s.native_tracks(&tracks);
        let mut f = frame(2, 90000);
        f.pts_offset = 1;
        s.native_frame(&tracks, &f);
        assert_eq!(
            s.decoder.lock().unwrap().error,
            Some("native_subtitle_clock_discontinuity")
        );

        for next in [89999, 90000 + 10 * 90000 + 1] {
            let s = state();
            let tracks = [track(2, "aac"), track(7, "subtitle")];
            s.native_tracks(&tracks);
            s.native_frame(&tracks, &frame(2, 90000));
            s.native_frame(&tracks, &frame(2, next));
            assert!(s.failed.load(Ordering::Relaxed));
            assert_eq!(
                s.decoder.lock().unwrap().error,
                Some("native_subtitle_clock_discontinuity")
            );
        }
        let s = state();
        let tracks = [track(2, "aac"), track(7, "subtitle")];
        s.native_tracks(&tracks);
        s.native_frame(&tracks, &frame(2, 90000));
        s.native_frame(
            &tracks,
            &Frame {
                track_id: 7,
                dts: 89999,
                pts_offset: 90000,
                key: true,
                body: b"late".to_vec(),
            },
        );
        assert_eq!(
            s.decoder.lock().unwrap().error,
            Some("native_subtitle_clock_discontinuity")
        );
        let s = state();
        s.native_tracks(&[track(7, "subtitle")]);
        assert!(s.failed.load(Ordering::Relaxed));
    }
}
