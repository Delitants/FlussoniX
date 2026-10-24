use super::{Error, MAX_BLOCKS_PER_RECORD, MAX_RECORD_LEN};
/// Immutable endpoint advertisement policy, selected once at startup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Features {
    pub trimmed_records: bool,
    /// V7 is advertised only together with the explicit v6 layout opt-in.
    pub repair_parity: bool,
    /// V8 requires both trimmed layouts and cached parity repair.
    pub bootstrap_repair: bool,
    /// V9 geometry is a separate immutable opt-in, defaulting off.
    pub geometry_hints: bool,
}
/// Layouts explicitly accepted from a negotiated peer; core defaults stay legacy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Capabilities {
    #[default]
    Legacy,
    V5,
    V6,
    V7,
}
impl Capabilities {
    pub(crate) fn for_version(version: u8) -> Self {
        match version {
            7..=9 => Self::V7,
            6 => Self::V6,
            5 => Self::V5,
            _ => Self::Legacy,
        }
    }
}
impl From<bool> for Capabilities {
    fn from(compact: bool) -> Self {
        if compact { Self::V5 } else { Self::Legacy }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub latency_ms: u64,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub shard_size: usize,
    pub max_record_len: usize,
    pub max_records: usize,
    pub max_blocks: usize,
    pub max_repair_shards: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            latency_ms: 1000,
            data_shards: 20,
            parity_shards: 4,
            shard_size: 1024,
            max_record_len: MAX_RECORD_LEN,
            max_records: 64,
            max_blocks: 2048,
            max_repair_shards: 64,
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<(), Error> {
        if !(500..=1500).contains(&self.latency_ms) {
            return Err(Error::InvalidSettings("latency must be 500..1500ms"));
        }
        if self.data_shards == 0
            || self.data_shards > 128
            || self.parity_shards > 128
            || self.data_shards + self.parity_shards > 256
        {
            return Err(Error::InvalidSettings("FEC geometry"));
        }
        if !(21..=1024).contains(&self.shard_size)
            || !(12..=MAX_RECORD_LEN).contains(&self.max_record_len)
            || !(1..=64).contains(&self.max_records)
            || !(1..=MAX_BLOCKS_PER_RECORD).contains(&self.max_blocks)
            || !(1..=256).contains(&self.max_repair_shards)
        {
            return Err(Error::InvalidSettings("resource bounds"));
        }
        Ok(())
    }
    /// Fits one systematic shard plus one compact copy within the modeled
    /// legacy packet cost, including an allowance for the extra outer packet.
    pub(crate) fn compact_width(&self, length: usize) -> Option<usize> {
        if !(12..=self.max_record_len).contains(&length)
            || self.parity_shards == 0
            || self.parity_shards >= self.data_shards
        {
            return None;
        }
        let width = length.checked_add(super::packet::FRAGMENT_HEADER_LEN)?;
        let cost = 2 * (super::packet::HEADER_LEN + width + super::packet::OUTER_PACKET_ALLOWANCE);
        (width <= self.shard_size
            && cost
                <= super::packet::HEADER_LEN
                    + self.shard_size
                    + super::packet::OUTER_PACKET_ALLOWANCE)
            .then_some(width)
    }
    pub(crate) fn fragment_size(&self) -> usize {
        self.shard_size - super::packet::FRAGMENT_HEADER_LEN
    }
    /// Trim only an otherwise unprotected, non-compact single shard.
    pub(crate) fn trimmed_width(&self, length: usize) -> Option<usize> {
        if !(12..=self.max_record_len).contains(&length)
            || self.compact_width(length).is_some()
            || self.fragment_count(length) != 1
            || self.parity_shards / self.data_shards != 0
        {
            return None;
        }
        let width = length.checked_add(super::packet::FRAGMENT_HEADER_LEN)?;
        (width < self.shard_size).then_some(width)
    }
    pub(crate) fn fragment_count(&self, len: usize) -> usize {
        len.div_ceil(self.fragment_size())
    }
    pub(crate) fn block_count(&self, len: usize) -> usize {
        self.fragment_count(len).div_ceil(self.data_shards)
    }
    pub(crate) fn block_data(&self, len: usize, index: usize) -> usize {
        (self.fragment_count(len) - index * self.data_shards).min(self.data_shards)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub host: String,
    pub port: u16,
    pub stream: Option<String>,
}
impl Address {
    pub fn parse(input: &str, require_stream: bool) -> Result<Self, Error> {
        let fail = || Error::InvalidAddress("expected fxlink://host:port/stream with safe path");
        if input.len() > 4096 || !input.starts_with("fxlink://") {
            return Err(fail());
        }
        let rest = &input[9..];
        let authority = rest.split('/').next().ok_or_else(fail)?;
        if authority.contains('@') || input.contains('?') || input.contains('#') {
            return Err(fail());
        }
        let (_, port_text) = authority.rsplit_once(':').ok_or_else(fail)?;
        if port_text.is_empty() || !port_text.bytes().all(|v| v.is_ascii_digit()) {
            return Err(fail());
        }
        let port = port_text.parse::<u16>().map_err(|_| fail())?;
        if port == 0 {
            return Err(fail());
        }
        let parsed = url::Url::parse(input).map_err(|_| fail())?;
        let host = parsed
            .host_str()
            .ok_or_else(fail)?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        if host.is_empty() || host.len() > 253 || parsed.port() != Some(port) {
            return Err(fail());
        }
        // Inspect raw components before URL normalization can erase dot segments.
        let raw = rest
            .strip_prefix(authority)
            .unwrap_or("")
            .strip_prefix('/')
            .unwrap_or("");
        let stream = if raw.is_empty() {
            None
        } else {
            let mut components = Vec::new();
            for segment in raw.split('/') {
                let b = segment.as_bytes();
                let mut i = 0;
                while i < b.len() {
                    if b[i] == b'%' {
                        if i + 2 >= b.len()
                            || !b[i + 1].is_ascii_hexdigit()
                            || !b[i + 2].is_ascii_hexdigit()
                        {
                            return Err(fail());
                        }
                        i += 2;
                    }
                    i += 1;
                }
                let decoded = percent_encoding::percent_decode_str(segment)
                    .decode_utf8()
                    .map_err(|_| fail())?;
                if decoded.is_empty()
                    || decoded == "."
                    || decoded == ".."
                    || decoded
                        .chars()
                        .any(|c| c.is_control() || c == '/' || c == '\\' || c == '?' || c == '#')
                {
                    return Err(fail());
                }
                components.push(decoded.into_owned());
            }
            let name = components.join("/");
            if name.len() > 1024 {
                return Err(fail());
            }
            Some(name)
        };
        if require_stream && stream.is_none() {
            return Err(fail());
        }
        Ok(Self { host, port, stream })
    }
}
