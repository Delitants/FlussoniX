use crate::fxlink::{Error, MAX_BLOCKS_PER_RECORD, Settings};
use bytes::Bytes;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Summary {
    pub record_id: u64,
    pub record_len: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Frame {
    pub subscription: u64,
    pub generation: u64,
    pub sequence: u64,
    pub entries: [Summary; 8],
    pub count: usize,
}
impl Frame {
    pub(crate) fn encoded_len(&self) -> usize {
        32usize.saturating_add(self.count.saturating_mul(12))
    }
}
const PREFIX: &[u8; 8] = b"FXGH\x01\x00\x00\x20";
fn validate(frame: &Frame, settings: &Settings) -> Result<(), Error> {
    settings.validate()?;
    if frame.sequence == 0 || !(1..=8).contains(&frame.count) {
        return Err(Error::InvalidPacket("geometry sequence/count"));
    }
    let mut previous = None;
    for entry in &frame.entries[..frame.count] {
        let length = entry.record_len as usize;
        if previous.is_some_and(|id| entry.record_id <= id)
            || !(12..=settings.max_record_len).contains(&length)
            || settings.block_count(length) > settings.max_blocks
            || entry
                .record_id
                .checked_mul(MAX_BLOCKS_PER_RECORD as u64)
                .is_none()
        {
            return Err(Error::InvalidPacket("geometry record bounds/order"));
        }
        previous = Some(entry.record_id);
    }
    Ok(())
}
pub(crate) fn encode(frame: &Frame, settings: &Settings) -> Result<Bytes, Error> {
    // Validate every descriptor before creating any owned wire allocation.
    validate(frame, settings)?;
    let mut bytes = Vec::with_capacity(frame.encoded_len());
    bytes.extend_from_slice(PREFIX);
    bytes.extend_from_slice(&frame.subscription.to_be_bytes());
    bytes.extend_from_slice(&frame.generation.to_be_bytes());
    bytes.extend_from_slice(&frame.sequence.to_be_bytes());
    for entry in &frame.entries[..frame.count] {
        bytes.extend_from_slice(&entry.record_id.to_be_bytes());
        bytes.extend_from_slice(&entry.record_len.to_be_bytes());
    }
    Ok(bytes.into())
}
pub(crate) fn decode(bytes: &[u8], settings: &Settings) -> Result<Frame, Error> {
    if !(44..=128).contains(&bytes.len())
        || (bytes.len() - 32) % 12 != 0
        || bytes.get(..8) != Some(PREFIX.as_slice())
    {
        return Err(Error::InvalidPacket("geometry wire header/length"));
    }
    let u64_at = |at| u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap());
    let mut frame = Frame {
        subscription: u64_at(8),
        generation: u64_at(16),
        sequence: u64_at(24),
        entries: [Summary::default(); 8],
        count: (bytes.len() - 32) / 12,
    };
    for (entry, raw) in frame.entries[..frame.count]
        .iter_mut()
        .zip(bytes[32..].chunks_exact(12))
    {
        *entry = Summary {
            record_id: u64::from_be_bytes(raw[..8].try_into().unwrap()),
            record_len: u32::from_be_bytes(raw[8..].try_into().unwrap()),
        };
    }
    validate(&frame, settings)?;
    Ok(frame)
}
/// Recognition only: even a bad version/size/tail must pay the routed work gate.
pub(crate) fn peek_route(bytes: &[u8]) -> Option<(u64, u64)> {
    if bytes.get(..4)? != b"FXGH" {
        return None;
    }
    Some((
        u64::from_be_bytes(bytes.get(8..16)?.try_into().ok()?),
        u64::from_be_bytes(bytes.get(16..24)?.try_into().ok()?),
    ))
}
#[derive(Default)]
pub(crate) struct Replay {
    largest: u64,
    bits: [u64; 8],
}
impl Replay {
    pub(crate) fn admit(&mut self, sequence: u64) -> bool {
        if sequence == 0 {
            return false;
        }
        if sequence > self.largest {
            let delta = sequence - self.largest;
            if delta >= 512 {
                self.bits = [0; 8];
            } else {
                let words = delta as usize / 64;
                let shift = delta as usize % 64;
                for index in (0usize..8).rev() {
                    self.bits[index] = index.checked_sub(words).map_or(0, |source| {
                        (self.bits[source] << shift)
                            | if shift == 0 {
                                0
                            } else {
                                source
                                    .checked_sub(1)
                                    .map_or(0, |lower| self.bits[lower] >> (64 - shift))
                            }
                    });
                }
            }
            self.largest = sequence;
        }
        let distance = self.largest - sequence;
        if distance >= 512 {
            return false;
        }
        let mask = 1u64 << (distance % 64);
        let word = &mut self.bits[distance as usize / 64];
        if *word & mask != 0 {
            return false;
        }
        *word |= mask;
        true
    }
}
