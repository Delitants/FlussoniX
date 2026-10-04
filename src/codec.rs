//! Explicit native codec identities. Aliases are resolved by input adapters,
//! never by guessing the meaning of a wire tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    H264,
    Hevc,
    Aac,
    M2a,
    Mp3,
    Subtitle,
}

impl Codec {
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "h264" => Ok(Self::H264),
            "hevc" => Ok(Self::Hevc),
            "aac" => Ok(Self::Aac),
            "m2a" => Ok(Self::M2a),
            "mp3" => Ok(Self::Mp3),
            "subtitle" => Ok(Self::Subtitle),
            _ => Err("unsupported native codec".into()),
        }
    }
    pub fn is_video(self) -> bool {
        matches!(self, Self::H264 | Self::Hevc)
    }
    pub fn is_audio(self) -> bool {
        matches!(self, Self::Aac | Self::M2a | Self::Mp3)
    }
    pub fn handler(self) -> &'static [u8; 4] {
        if self.is_video() {
            b"vide"
        } else if self.is_audio() {
            b"soun"
        } else {
            b"text"
        }
    }
    pub fn content_type(self) -> u8 {
        if self.is_video() {
            1
        } else if self.is_audio() {
            2
        } else {
            4
        }
    }
    pub fn tag(self) -> [u8; 4] {
        match self {
            Self::H264 => *b"h264",
            Self::Hevc => *b"hevc",
            Self::Aac => *b" aac",
            Self::M2a => *b" m2a",
            Self::Mp3 => *b" mp3",
            Self::Subtitle => *b"subt",
        }
    }
}
