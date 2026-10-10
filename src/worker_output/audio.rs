use crate::{codec::Codec, m4f::Frame, m4s::Track};

#[derive(Default)]
pub(super) struct Audio {
    pending: Vec<u8>,
    format: Option<(Codec, u32, u8)>,
    track: Option<Track>,
    base: u64,
    samples: u64,
    last: Option<u64>,
}
struct Header {
    codec: Codec,
    rate: u32,
    channels: u8,
    samples: u32,
    size: usize,
    skip: usize,
    config: Vec<u8>,
}
impl Audio {
    pub(super) fn track(&self) -> Option<&Track> {
        self.track.as_ref()
    }
    pub(super) fn retained(&self) -> usize {
        self.pending.len()
    }
    pub(super) fn shift_epoch(&mut self, shift: u64) -> Result<(), String> {
        if self.format.is_some() || !self.pending.is_empty() {
            self.base = self
                .base
                .checked_add(shift)
                .ok_or("worker audio epoch overflow")?;
        }
        self.last = self
            .last
            .map(|last| last.checked_add(shift).ok_or("worker audio epoch overflow"))
            .transpose()?;
        Ok(())
    }
    pub(super) fn finish(&self) -> Result<(), String> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err("incomplete worker audio frame".into())
        }
    }
    fn next(&self) -> u64 {
        self.base + self.samples * 90000 / u64::from(self.format.map_or(1, |f| f.1))
    }
    fn seed(&mut self, dts: u64) -> Result<(), String> {
        if self.last.is_some() {
            let next = self.next();
            // The transport decoder independently checks monotonic PES DTS.
            // A corrected source clock can nevertheless put its next complete
            // audio frame inside the preceding frame's decoded sample span.
            // Retain the sample clock through bounded overlap; never compress
            // encoded audio duration or accumulate unlimited AV drift.
            if dts.abs_diff(next) > 60 * 90000 || next.saturating_sub(dts) > 90000 / 4 {
                return Err("worker audio timestamp discontinuity".into());
            }
            // PES clocks are integral ticks. Preserve fractional frame duration across PES.
            if dts <= next || dts.abs_diff(next) <= 1 {
                return Ok(());
            }
        }
        self.base = dts;
        self.samples = 0;
        Ok(())
    }
    pub(super) fn push(
        &mut self,
        pid: u16,
        ty: u8,
        data: &[u8],
        dts: u64,
        offset: i64,
    ) -> Result<Vec<Frame>, String> {
        if offset != 0 {
            return Err("worker audio requires equal PTS and DTS".into());
        }
        let mut out = vec![];
        let mut at = 0;
        let mut continuation = !self.pending.is_empty();
        if !continuation {
            self.seed(dts)?;
        }
        while at < data.len() {
            let minimum = if ty == 0x0f { 7 } else { 4 };
            if self.pending.len() < minimum {
                let n = (minimum - self.pending.len()).min(data.len() - at);
                self.pending.extend_from_slice(&data[at..at + n]);
                at += n;
                if self.pending.len() < minimum {
                    break;
                }
            }
            let h = header(ty, &self.pending)?;
            if self.pending.len() > h.size {
                return Err("invalid worker audio framing".into());
            }
            let n = (h.size - self.pending.len()).min(data.len() - at);
            self.pending.extend_from_slice(&data[at..at + n]);
            at += n;
            if self.pending.len() < h.size {
                break;
            }
            let format = (h.codec, h.rate, h.channels);
            if self.format.is_some_and(|f| f != format) {
                return Err("worker audio format changed".into());
            }
            if let Some(t) = &self.track {
                if t.config != h.config {
                    return Err("worker audio configuration changed".into());
                }
            } else {
                self.track = Some(Track {
                    id: u32::from(pid),
                    codec: match h.codec {
                        Codec::Aac => "aac",
                        Codec::M2a => "m2a",
                        Codec::Mp3 => "mp3",
                        _ => unreachable!(),
                    }
                    .into(),
                    config: h.config,
                });
                self.format = Some(format);
            }
            let stamp = self.next();
            if self.last.is_some_and(|last| stamp < last) {
                return Err("worker audio DTS moved backwards".into());
            }
            if out.len() >= 65536 {
                return Err("worker audio sample count exceeds bound".into());
            }
            out.push(Frame {
                track_id: u32::from(pid),
                dts: stamp,
                pts_offset: 0,
                key: true,
                body: self.pending[h.skip..].to_vec(),
            });
            self.last = Some(stamp);
            self.samples = self
                .samples
                .checked_add(u64::from(h.samples))
                .ok_or("worker audio clock overflow")?;
            self.pending.clear();
            if continuation {
                continuation = false;
                // A PES PTS addresses the first frame starting in this PES, not the tail
                // of a frame which started in the preceding PES.
                if at < data.len() {
                    self.seed(dts)?;
                }
            }
        }
        Ok(out)
    }
}
fn header(ty: u8, b: &[u8]) -> Result<Header, String> {
    if ty == 0x0f {
        if b.len() < 7 || b[0] != 255 || b[1] & 0xf6 != 0xf0 || b[2] >> 6 != 1 {
            return Err("worker requires unprotected AAC-LC ADTS".into());
        }
        // CRC-protected ADTS and multiple raw blocks need a separate qualified profile.
        if b[1] & 1 == 0 || b[6] & 3 != 0 {
            return Err("unsupported ADTS CRC/raw block layout".into());
        }
        let rate_index = (b[2] >> 2) & 15;
        let rate = *[
            96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350,
        ]
        .get(usize::from(rate_index))
        .ok_or("unsupported ADTS rate")?;
        let channels = ((b[2] & 1) << 2) | (b[3] >> 6);
        if channels == 0 {
            return Err("ADTS program configuration elements unsupported".into());
        }
        let size =
            (usize::from(b[3] & 3) << 11) | (usize::from(b[4]) << 3) | usize::from(b[5] >> 5);
        if size <= 7 {
            return Err("invalid ADTS frame length".into());
        }
        Ok(Header {
            codec: Codec::Aac,
            rate,
            channels,
            samples: 1024,
            size,
            skip: 7,
            config: vec![
                0x10 | (rate_index >> 1),
                (rate_index << 7) | (channels << 3),
            ],
        })
    } else {
        if b.len() < 4 {
            return Err("incomplete MPEG audio header".into());
        }
        let codec = match (b[1] >> 1) & 3 {
            2 => Codec::M2a,
            1 => Codec::Mp3,
            _ => return Err("unsupported MPEG audio layer".into()),
        };
        let h = crate::mpeg_audio::header(codec, b)?;
        Ok(Header {
            codec,
            rate: h.sample_rate,
            channels: h.channels,
            samples: h.samples,
            size: h.frame_bytes,
            skip: 0,
            config: vec![],
        })
    }
}
