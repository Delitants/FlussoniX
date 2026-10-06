use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
pub const MAX_PACKET: usize = 1600;
// Every SDES chunk must contain a bounded item list terminated by END and
// zero alignment bytes. A claimed SSRC alone is not a source description.
fn valid_sdes(b: &[u8], count: usize) -> bool {
    let mut at = 0;
    for _ in 0..count {
        if b.get(at..at + 4).is_none() {
            return false;
        }
        at += 4;
        loop {
            let Some(&kind) = b.get(at) else {
                return false;
            };
            at += 1;
            if kind == 0 {
                break;
            }
            let Some(&n) = b.get(at) else {
                return false;
            };
            at += 1;
            if b.get(at..at + usize::from(n)).is_none() {
                return false;
            }
            at += usize::from(n);
        }
        while at % 4 != 0 {
            if b.get(at) != Some(&0) {
                return false;
            }
            at += 1;
        }
    }
    at == b.len()
}
fn skip(b: &mut &[u8], n: usize) -> bool {
    if let Some(rest) = b.get(n..) {
        *b = rest;
        true
    } else {
        false
    }
}
fn valid_ts(p: &[u8]) -> bool {
    if p[0] != 0x47 || p[1] & 0x80 != 0 {
        return false;
    }
    match p[3] & 0x30 {
        0 => return false,
        0x10 => return true,
        0x20 if p[4] != 183 => return false,
        0x30 if p[4] > 182 => return false,
        _ => {}
    }
    let length = usize::from(p[4]);
    if length == 0 {
        return true;
    }
    let flags = p[5];
    let mut fields = &p[6..5 + length];
    for (flag, n) in [(0x10, 6), (0x08, 6), (0x04, 1)] {
        if flags & flag != 0 && !skip(&mut fields, n) {
            return false;
        }
    }
    if flags & 0x02 != 0 {
        let Some(&n) = fields.first() else {
            return false;
        };
        if !skip(&mut fields, 1 + usize::from(n)) {
            return false;
        }
    }
    if flags & 0x01 != 0 {
        let Some((&n, rest)) = fields.split_first() else {
            return false;
        };
        let Some(mut extension) = rest.get(..usize::from(n)) else {
            return false;
        };
        if let Some((&flags, rest)) = extension.split_first() {
            extension = rest;
            for (flag, n) in [(0x80, 2), (0x40, 3), (0x20, 5)] {
                if flags & flag != 0 && !skip(&mut extension, n) {
                    return false;
                }
            }
        }
    }
    true
}
pub fn valid_rtcp(mut b: &[u8]) -> bool {
    if b.is_empty() || b.len() > 2048 {
        return false;
    }
    while !b.is_empty() {
        if b.len() < 4 || b[0] >> 6 != 2 {
            return false;
        }
        let len = (usize::from(u16::from_be_bytes([b[2], b[3]])) + 1) * 4;
        if len > b.len() {
            return false;
        }
        let rc = usize::from(b[0] & 31);
        let min = match b[1] {
            200 => 28 + 24 * rc,
            201 => 8 + 24 * rc,
            202 => 4 + 4 * rc,
            203 => 4 + 4 * rc,
            204 => 12,
            _ => return false,
        };
        if len < min
            || b[0] & 32 != 0
                && (len != b.len() || b[len - 1] == 0 || usize::from(b[len - 1]) > len - min)
        {
            return false;
        }
        let padding = if b[0] & 32 != 0 {
            usize::from(b[len - 1])
        } else {
            0
        };
        if b[1] == 202 && !valid_sdes(&b[4..len - padding], rc) {
            return false;
        }
        b = &b[len..];
    }
    true
}
pub struct Parsed<'a> {
    pub sequence: u16,
    pub ssrc: u32,
    pub timestamp: u32,
    pub payload: &'a [u8],
}
pub fn parse(b: &[u8]) -> Result<Parsed<'_>, &'static str> {
    if b.len() < 12 || b.len() > MAX_PACKET || b[0] >> 6 != 2 || b[1] & 127 != 33 {
        return Err("invalid RTP/MP2T header");
    }
    let mut start = 12 + usize::from(b[0] & 15) * 4;
    if b[0] & 16 != 0 {
        let h = b.get(start..start + 4).ok_or("truncated RTP extension")?;
        let len = usize::from(u16::from_be_bytes([h[2], h[3]])) * 4;
        if len > 512 {
            return Err("RTP extension exceeds limit");
        }
        start += 4 + len;
    }
    let padding = if b[0] & 32 != 0 {
        let n = usize::from(*b.last().unwrap());
        if n == 0 {
            return Err("invalid RTP padding");
        }
        n
    } else {
        0
    };
    let end = b.len().checked_sub(padding).ok_or("invalid RTP padding")?;
    let data = b.get(start..end).ok_or("invalid RTP payload bounds")?;
    if data.is_empty()
        || data.len() > 7 * 188
        || data.len() % 188 != 0
        || data.chunks_exact(188).any(|p| !valid_ts(p))
    {
        return Err("invalid MP2T payload");
    }
    Ok(Parsed {
        sequence: u16::from_be_bytes([b[2], b[3]]),
        timestamp: u32::from_be_bytes(b[4..8].try_into().unwrap()),
        ssrc: u32::from_be_bytes(b[8..12].try_into().unwrap()),
        payload: data,
    })
}
pub fn packet(sequence: u16, timestamp: u32, ssrc: u32, payload: &[u8]) -> Vec<u8> {
    let mut b = vec![0x80, 33];
    b.extend(sequence.to_be_bytes());
    b.extend(timestamp.to_be_bytes());
    b.extend(ssrc.to_be_bytes());
    b.extend(payload);
    b
}
pub struct Reorder {
    expected: Option<i64>,
    queue: BTreeMap<i64, (Instant, Vec<u8>)>,
    delay: Duration,
    pub duplicates: u64,
    pub lost: u64,
}
impl Reorder {
    pub fn highest(&self) -> u32 {
        self.expected.map_or(0, |s| s.saturating_sub(1) as u32)
    }
    pub fn new(delay: Duration) -> Self {
        Self {
            expected: None,
            queue: BTreeMap::new(),
            delay,
            duplicates: 0,
            lost: 0,
        }
    }
    pub fn pending(&self) -> usize {
        self.queue.len()
    }
    pub fn push(&mut self, sequence: u16, payload: Vec<u8>, now: Instant) -> Vec<Vec<u8>> {
        let expected = *self.expected.get_or_insert(i64::from(sequence));
        let seq = expected + i64::from(sequence.wrapping_sub(expected as u16) as i16);
        if seq < expected || self.queue.contains_key(&seq) {
            self.duplicates += 1;
            return vec![];
        }
        self.queue.insert(seq, (now, payload));
        self.flush(now)
    }
    pub fn flush(&mut self, now: Instant) -> Vec<Vec<u8>> {
        let mut output = Vec::new();
        let Some(mut expected) = self.expected else {
            return output;
        };
        loop {
            if let Some((_, data)) = self.queue.remove(&expected) {
                output.push(data);
                expected += 1;
                continue;
            }
            let Some((&next, _)) = self.queue.first_key_value() else {
                break;
            };
            let expired = self
                .queue
                .values()
                .any(|(at, _)| now.saturating_duration_since(*at) >= self.delay);
            if self.queue.len() >= 64 || expired {
                self.lost = self.lost.saturating_add((next - expected) as u64);
                expected = next;
                continue;
            }
            break;
        }
        self.expected = Some(expected);
        output
    }
}
pub fn sdes(ssrc: u32) -> Vec<u8> {
    let name = format!("fx-{ssrc:08x}");
    let mut sdes = vec![0x81, 202, 0, 0];
    sdes.extend(ssrc.to_be_bytes());
    sdes.extend([1, name.len() as u8]);
    sdes.extend(name.as_bytes());
    sdes.push(0);
    while sdes.len() % 4 != 0 {
        sdes.push(0);
    }
    let length = (sdes.len() / 4 - 1) as u16;
    sdes[2..4].copy_from_slice(&length.to_be_bytes());
    sdes
}
#[derive(Default)]
pub struct Reception {
    pub highest: u32,
    pub lost: u64,
    pub fraction: u8,
    pub jitter: u32,
    pub last_sr: u32,
    pub delay_sr: u32,
}
pub fn receiver_report(local: u32, source: u32, r: &Reception) -> Vec<u8> {
    let mut b = vec![0x81, 201, 0, 7];
    b.extend(local.to_be_bytes());
    b.extend(source.to_be_bytes());
    let loss = (r.lost.min(0x7fffff) as u32).to_be_bytes();
    b.push(r.fraction);
    b.extend(&loss[1..]);
    for n in [r.highest, r.jitter, r.last_sr, r.delay_sr] {
        b.extend(n.to_be_bytes());
    }
    b.extend(sdes(local));
    b
}
