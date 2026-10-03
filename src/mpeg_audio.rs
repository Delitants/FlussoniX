//! One complete encoded MPEG audio frame; no AAC assumptions or free-format
//! bitrate search. Payload bytes are never rewritten.
use crate::codec::Codec;
#[derive(Debug)]
pub struct Header {
    pub sample_rate: u32,
    pub channels: u8,
    pub samples: u32,
    pub frame_bytes: usize,
    pub bitrate: u32,
}
impl Header {
    pub fn duration_90k(&self) -> u32 {
        self.samples * 90000 / self.sample_rate
    }
}
pub fn inspect(codec: Codec, data: &[u8]) -> Result<Header, String> {
    if !matches!(codec, Codec::M2a | Codec::Mp3)
        || data.len() < 4
        || data[0] != 255
        || data[1] & 0xe0 != 0xe0
    {
        return Err("invalid MPEG audio header".into());
    }
    let version = (data[1] >> 3) & 3;
    let layer = (data[1] >> 1) & 3;
    if version == 1 || layer != if codec == Codec::M2a { 2 } else { 1 } {
        return Err("MPEG audio version/layer mismatch".into());
    }
    let bitrate_index = usize::from(data[2] >> 4);
    let rate_index = usize::from((data[2] >> 2) & 3);
    if bitrate_index == 0 || bitrate_index == 15 || rate_index == 3 || data[3] & 3 == 2 {
        return Err("reserved or free-format MPEG audio header".into());
    }
    let sample_rate = [44100, 48000, 32000][rate_index]
        / match version {
            3 => 1,
            2 => 2,
            _ => 4,
        };
    let kbps: u32 = if version == 3 {
        if layer == 2 {
            [
                0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
            ][bitrate_index]
        } else {
            [
                0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
            ][bitrate_index]
        }
    } else {
        [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160][bitrate_index]
    };
    let samples = if layer == 1 && version != 3 {
        576
    } else {
        1152
    };
    let coefficient = if samples == 576 { 72000 } else { 144000 };
    let frame_bytes = (coefficient * kbps / sample_rate + u32::from((data[2] >> 1) & 1)) as usize;
    if data.len() != frame_bytes || frame_bytes < if data[1] & 1 == 0 { 6 } else { 4 } {
        return Err("MPEG audio frame length mismatch".into());
    }
    Ok(Header {
        sample_rate,
        channels: if data[3] >> 6 == 3 { 1 } else { 2 },
        samples,
        frame_bytes,
        bitrate: kbps * 1000,
    })
}
