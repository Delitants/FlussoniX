use super::{Error, MAX_BLOCKS_PER_RECORD, MAX_RECORD_LEN};
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 64;
pub const FRAGMENT_HEADER_LEN: usize = 20;
pub(crate) const OUTER_PACKET_ALLOWANCE: usize = 128;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Layout {
    Fixed = 0,
    Compact = 1,
    Trimmed = 2,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub layout: Layout,
    pub subscription: u64,
    pub generation: u64,
    pub block_id: u64,
    pub record_id: u64,
    pub record_len: u32,
    pub block_count: u32,
    pub block_index: u32,
    pub shard_size: u16,
    pub data_count: u16,
    pub parity_count: u16,
    pub shard_index: u16,
    pub configured_data: u16,
    pub configured_parity: u16,
}
impl Header {
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER_LEN
            || &bytes[..4] != b"FXLK"
            || bytes[4] != VERSION
            || bytes[5] > 2
            || bytes[6..8] != [0, 64]
        {
            return Err(Error::InvalidPacket("wire version/header"));
        }
        let u64at = |i| u64::from_be_bytes(bytes[i..i + 8].try_into().unwrap());
        let u32at = |i| u32::from_be_bytes(bytes[i..i + 4].try_into().unwrap());
        let u16at = |i| u16::from_be_bytes(bytes[i..i + 2].try_into().unwrap());
        let h = Self {
            layout: match bytes[5] {
                0 => Layout::Fixed,
                1 => Layout::Compact,
                _ => Layout::Trimmed,
            },
            subscription: u64at(8),
            generation: u64at(16),
            block_id: u64at(24),
            record_id: u64at(32),
            record_len: u32at(40),
            block_count: u32at(44),
            block_index: u32at(48),
            shard_size: u16at(52),
            data_count: u16at(54),
            parity_count: u16at(56),
            shard_index: u16at(58),
            configured_data: u16at(60),
            configured_parity: u16at(62),
        };
        h.validate()?;
        if bytes.len() != HEADER_LEN + h.shard_size as usize {
            return Err(Error::InvalidPacket("datagram length"));
        }
        Ok(h)
    }
    fn validate(&self) -> Result<(), Error> {
        let k = self.configured_data as usize;
        let p = self.configured_parity as usize;
        if k == 0
            || k > 128
            || p > 128
            || k + p > 256
            || self.data_count == 0
            || self.data_count as usize > k
            || (self.layout != Layout::Compact
                && self.parity_count as usize != (self.data_count as usize * p / k))
            || !(21..=1024).contains(&self.shard_size)
            || !(12..=MAX_RECORD_LEN as u32).contains(&self.record_len)
            || self.block_count == 0
            || self.block_count as usize > MAX_BLOCKS_PER_RECORD
            || self.block_index >= self.block_count
            || self.shard_index as usize >= self.data_count as usize + self.parity_count as usize
        {
            return Err(Error::InvalidPacket("geometry bounds"));
        }
        if self.layout == Layout::Compact
            && (p == 0
                || p >= k
                || self.data_count != 1
                || self.parity_count != 1
                || self.block_count != 1
                || self.block_index != 0
                || self.shard_size as usize != self.record_len as usize + FRAGMENT_HEADER_LEN
                || 2 * (HEADER_LEN + self.shard_size as usize + OUTER_PACKET_ALLOWANCE)
                    > HEADER_LEN + 1024 + OUTER_PACKET_ALLOWANCE)
        {
            return Err(Error::InvalidPacket("compact geometry"));
        }
        if self.layout == Layout::Trimmed
            && (p >= k
                || self.data_count != 1
                || self.parity_count != 0
                || self.shard_index != 0
                || self.block_count != 1
                || self.block_index != 0
                || self.shard_size as usize != self.record_len as usize + FRAGMENT_HEADER_LEN)
        {
            return Err(Error::InvalidPacket("trimmed geometry"));
        }
        let fragments =
            (self.record_len as usize).div_ceil(self.shard_size as usize - FRAGMENT_HEADER_LEN);
        if self.block_count as usize != fragments.div_ceil(k)
            || self.data_count as usize != (fragments - self.block_index as usize * k).min(k)
            || self
                .record_id
                .checked_mul(MAX_BLOCKS_PER_RECORD as u64)
                .and_then(|x| x.checked_add(self.block_index as u64))
                != Some(self.block_id)
        {
            return Err(Error::InvalidPacket("fragment geometry"));
        }
        Ok(())
    }
    pub(crate) fn validate_profile(
        &self,
        settings: &super::Settings,
        capabilities: impl Into<super::Capabilities>,
    ) -> Result<(), Error> {
        let capabilities = capabilities.into();
        let width = match self.layout {
            Layout::Compact => {
                if capabilities == super::Capabilities::Legacy {
                    return Err(Error::InvalidPacket("unnegotiated compact format"));
                }
                settings.compact_width(self.record_len as usize)
            }
            Layout::Trimmed => {
                if !matches!(
                    capabilities,
                    super::Capabilities::V6 | super::Capabilities::V7
                ) {
                    return Err(Error::InvalidPacket("unnegotiated trimmed format"));
                }
                settings.trimmed_width(self.record_len as usize)
            }
            Layout::Fixed => Some(settings.shard_size),
        };
        if self.configured_data as usize != settings.data_shards
            || self.configured_parity as usize != settings.parity_shards
            || width != Some(self.shard_size as usize)
            || self.record_len as usize > settings.max_record_len
            || self.block_count as usize > settings.max_blocks
        {
            return Err(Error::InvalidPacket("unnegotiated profile"));
        }
        Ok(())
    }
    pub fn write(&self, out: &mut [u8]) -> Result<(), Error> {
        self.validate()?;
        if out.len() < HEADER_LEN {
            return Err(Error::InvalidPacket("header destination"));
        }
        out[..4].copy_from_slice(b"FXLK");
        out[4..8].copy_from_slice(&[VERSION, self.layout as u8, 0, 64]);
        for (i, v) in [
            (8, self.subscription),
            (16, self.generation),
            (24, self.block_id),
            (32, self.record_id),
        ] {
            out[i..i + 8].copy_from_slice(&v.to_be_bytes());
        }
        for (i, v) in [
            (40, self.record_len),
            (44, self.block_count),
            (48, self.block_index),
        ] {
            out[i..i + 4].copy_from_slice(&v.to_be_bytes());
        }
        for (i, v) in [
            (52, self.shard_size),
            (54, self.data_count),
            (56, self.parity_count),
            (58, self.shard_index),
            (60, self.configured_data),
            (62, self.configured_parity),
        ] {
            out[i..i + 2].copy_from_slice(&v.to_be_bytes());
        }
        Ok(())
    }
}
