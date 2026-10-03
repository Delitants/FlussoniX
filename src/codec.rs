//! Explicit native codec identities. Aliases are resolved by input adapters,
//! never by guessing the meaning of a wire tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    H264,
    Hevc,
    Aac,
    M2a,
    Mp3,
}

impl Codec {
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "h264" => Ok(Self::H264),
            "hevc" => Ok(Self::Hevc),
            "aac" => Ok(Self::Aac),
            "m2a" => Ok(Self::M2a),
            "mp3" => Ok(Self::Mp3),
            _ => Err("unsupported native codec".into()),
        }
    }
    pub fn is_video(self) -> bool {
        matches!(self, Self::H264 | Self::Hevc)
    }
    pub fn tag(self) -> [u8; 4] {
        match self {
            Self::H264 => *b"h264",
            Self::Hevc => *b"hevc",
            Self::Aac => *b" aac",
            Self::M2a => *b" m2a",
            Self::Mp3 => *b" mp3",
        }
    }
}
