use super::sdp::{Codec, Track};
use crate::direct_rtp::packet::{MAX_PACKET, Parsed};
fn aggregate(mut bytes: &[u8], hevc: bool) -> bool {
    let mut count = 0;
    while !bytes.is_empty() {
        if bytes.len() < 2 {
            return false;
        }
        let n = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
        let Some(nal) = bytes.get(2..2 + n) else {
            return false;
        };
        if count >= 64 || !nal_valid(nal, hevc) {
            return false;
        }
        count += 1;
        bytes = &bytes[2 + n..];
    }
    count > 0
}
fn nal_valid(b: &[u8], hevc: bool) -> bool {
    if hevc {
        b.len() >= 2 && b[0] & 128 == 0 && b[1] & 7 != 0 && (b[0] >> 1) & 63 < 48
    } else {
        b.len() >= 2 && b[0] & 128 == 0 && (1..=23).contains(&(b[0] & 31))
    }
}
fn payload(b: &[u8], codec: &Codec) -> bool {
    match codec {
        Codec::H264 => {
            if b.is_empty() || b[0] & 128 != 0 {
                return false;
            }
            match b[0] & 31 {
                1..=23 => nal_valid(b, false),
                24 => aggregate(&b[1..], false),
                28 => {
                    b.len() >= 3
                        && b[1] & 0x20 == 0
                        && b[1] & 0xc0 != 0xc0
                        && (1..=23).contains(&(b[1] & 31))
                }
                _ => false,
            }
        }
        Codec::H265 => {
            if b.len() < 2 || b[0] & 128 != 0 || b[1] & 7 == 0 {
                return false;
            }
            match (b[0] >> 1) & 63 {
                0..=47 => nal_valid(b, true),
                48 => aggregate(&b[2..], true),
                49 => b.len() >= 4 && b[2] & 0xc0 != 0xc0 && b[2] & 63 < 48,
                _ => false,
            }
        }
        Codec::Aac => {
            if b.len() < 4 {
                return false;
            }
            let bits = usize::from(u16::from_be_bytes([b[0], b[1]]));
            if bits == 0 || bits % 16 != 0 || bits > 64 * 16 || 2 + bits / 8 >= b.len() {
                return false;
            }
            let mut total = 0;
            for h in b[2..2 + bits / 8].chunks_exact(2) {
                let word = u16::from_be_bytes([h[0], h[1]]);
                let n = usize::from(word >> 3);
                if n == 0 || word & 7 != 0 {
                    return false;
                }
                total += n;
            }
            total == b.len() - 2 - bits / 8
        }
        Codec::Mpa => {
            if b.len() < 5 || b[..2] != [0, 0] {
                return false;
            }
            if b[2..4] != [0, 0] {
                return true;
            }
            crate::mpeg_audio::header(crate::codec::Codec::M2a, &b[4..]).is_ok()
                || crate::mpeg_audio::header(crate::codec::Codec::Mp3, &b[4..]).is_ok()
        }
    }
}
pub fn parse<'a>(b: &'a [u8], track: &Track) -> Result<Parsed<'a>, &'static str> {
    if b.len() < 12 || b.len() > MAX_PACKET || b[0] >> 6 != 2 || b[1] & 127 != track.payload {
        return Err("invalid elementary RTP header");
    }
    let mut start = 12 + usize::from(b[0] & 15) * 4;
    if b[0] & 16 != 0 {
        let h = b
            .get(start..start + 4)
            .ok_or("truncated elementary RTP extension")?;
        let n = usize::from(u16::from_be_bytes([h[2], h[3]])) * 4;
        if n > 512 {
            return Err("elementary RTP extension exceeds limit");
        }
        start += 4 + n;
    }
    let padding = if b[0] & 32 != 0 {
        let n = usize::from(*b.last().unwrap());
        if n == 0 {
            return Err("invalid elementary RTP padding");
        }
        n
    } else {
        0
    };
    let end = b
        .len()
        .checked_sub(padding)
        .ok_or("invalid elementary RTP padding")?;
    let bytes = b
        .get(start..end)
        .ok_or("truncated elementary RTP payload")?;
    if !payload(bytes, &track.codec) {
        return Err("invalid elementary codec payload");
    }
    Ok(Parsed {
        sequence: u16::from_be_bytes([b[2], b[3]]),
        timestamp: u32::from_be_bytes(b[4..8].try_into().unwrap()),
        ssrc: u32::from_be_bytes(b[8..12].try_into().unwrap()),
        payload: bytes,
    })
}
