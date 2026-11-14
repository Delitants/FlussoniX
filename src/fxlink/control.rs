//! Length bounded reliable control. Credentials deliberately have no Debug implementation.
use super::Settings;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Control {
    Subscribe {
        version: u8,
        id: u64,
        generation: u64,
        profile: [u64; 8],
        name: String,
        peer_key: String,
        token: String,
    },
    Accepted,
    Counters {
        data: u64,
        parity: u64,
        repair: u64,
    },
    Denied {
        status: u16,
    },
    Announce {
        record: u64,
        len: usize,
    },
    Repair {
        block: u64,
        shards: Vec<u16>,
    },
    Progress {
        next: u64,
    },
}
pub(crate) fn protocols() -> Vec<Vec<u8>> {
    vec![
        b"fxlink/5".to_vec(),
        b"fxlink/4".to_vec(),
        b"fxlink/3".to_vec(),
        b"fxlink/2".to_vec(),
        b"fxlink/1".to_vec(),
    ]
}
pub(crate) fn version(connection: &quinn::Connection) -> Result<u8, String> {
    let data = connection
        .handshake_data()
        .ok_or("FXLink missing TLS negotiation")?
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .map_err(|_| "FXLink invalid TLS negotiation")?;
    match data.protocol.as_deref() {
        Some(b"fxlink/9") => Ok(9),
        Some(b"fxlink/8") => Ok(8),
        Some(b"fxlink/7") => Ok(7),
        Some(b"fxlink/6") => Ok(6),
        Some(b"fxlink/5") => Ok(5),
        Some(b"fxlink/4") => Ok(4),
        Some(b"fxlink/3") => Ok(3),
        Some(b"fxlink/2") => Ok(2),
        Some(b"fxlink/1") => Ok(1),
        _ => Err("FXLink unsupported TLS protocol".into()),
    }
}
pub(crate) fn protocols_with_features(features: super::Features) -> Vec<Vec<u8>> {
    let mut supported = protocols();
    if features.trimmed_records {
        supported.insert(0, b"fxlink/6".to_vec());
        if features.repair_parity {
            supported.insert(0, b"fxlink/7".to_vec());
            if features.bootstrap_repair {
                supported.insert(0, b"fxlink/8".to_vec());
                if features.geometry_hints {
                    supported.insert(0, b"fxlink/9".to_vec());
                }
            }
        }
    }
    supported
}
pub(crate) fn profile(s: &Settings) -> [u64; 8] {
    [
        s.latency_ms,
        s.data_shards as u64,
        s.parity_shards as u64,
        s.shard_size as u64,
        s.max_record_len as u64,
        s.max_records as u64,
        s.max_blocks as u64,
        s.max_repair_shards as u64,
    ]
}
pub(crate) async fn write(s: &mut quinn::SendStream, m: &Control) -> Result<(), String> {
    let bytes = serde_json::to_vec(m).map_err(|_| "FXLink control encoding")?;
    if bytes.len() > 16384 {
        return Err("FXLink control limit".into());
    }
    s.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|_| "FXLink control closed")?;
    s.write_all(&bytes)
        .await
        .map_err(|_| "FXLink control closed".into())
}
pub(crate) async fn read(s: &mut quinn::RecvStream) -> Result<Control, String> {
    let mut prefix = [0; 4];
    s.read_exact(&mut prefix)
        .await
        .map_err(|_| "FXLink control closed")?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len == 0 || len > 16384 {
        return Err("FXLink control limit".into());
    }
    let mut bytes = vec![0; len];
    s.read_exact(&mut bytes)
        .await
        .map_err(|_| "FXLink control closed")?;
    serde_json::from_slice(&bytes).map_err(|_| "FXLink invalid control".into())
}
pub(crate) async fn next(
    mut stream: quinn::RecvStream,
) -> (quinn::RecvStream, Result<Control, String>) {
    let result = read(&mut stream).await;
    (stream, result)
}

/// A cancel-safe reader can stop consuming frame bytes at the record ceiling
/// while continuing to observe RESET_STREAM. Partial prefixes/bodies survive
/// media/timer branches winning the caller's select.
pub(crate) struct Reader {
    stream: quinn::RecvStream,
    prefix: [u8; 4],
    prefix_used: usize,
    body: Vec<u8>,
    body_used: usize,
}
impl Reader {
    pub(crate) fn new(stream: quinn::RecvStream) -> Self {
        Self {
            stream,
            prefix: [0; 4],
            prefix_used: 0,
            body: Vec::new(),
            body_used: 0,
        }
    }
    pub(crate) async fn next(&mut self, paused: bool) -> Result<Control, String> {
        if paused {
            self.stream
                .received_reset()
                .await
                .map_err(|_| "FXLink control closed")?;
            return Err("FXLink control closed".into());
        }
        while self.prefix_used < self.prefix.len() {
            let n = self
                .stream
                .read(&mut self.prefix[self.prefix_used..])
                .await
                .map_err(|_| "FXLink control closed")?
                .ok_or("FXLink control closed")?;
            self.prefix_used += n;
        }
        if self.body.is_empty() {
            let len = u32::from_be_bytes(self.prefix) as usize;
            if len == 0 || len > 16384 {
                return Err("FXLink control limit".into());
            }
            self.body.resize(len, 0);
        }
        while self.body_used < self.body.len() {
            let n = self
                .stream
                .read(&mut self.body[self.body_used..])
                .await
                .map_err(|_| "FXLink control closed")?
                .ok_or("FXLink control closed")?;
            self.body_used += n;
        }
        let result =
            serde_json::from_slice(&self.body).map_err(|_| "FXLink invalid control".into());
        self.prefix_used = 0;
        self.body_used = 0;
        self.body.clear();
        result
    }
}
/// Select one immutable profile per subscription; listener settings are resource ceilings.
pub(crate) fn negotiate(profile: [u64; 8], ceiling: &Settings) -> Result<Settings, String> {
    let size =
        |value: u64| usize::try_from(value).map_err(|_| "FXLink profile size overflow".to_owned());
    let requested = Settings {
        latency_ms: profile[0],
        data_shards: size(profile[1])?,
        parity_shards: size(profile[2])?,
        shard_size: size(profile[3])?,
        max_record_len: size(profile[4])?,
        max_records: size(profile[5])?,
        max_blocks: size(profile[6])?,
        max_repair_shards: size(profile[7])?,
    };
    requested.validate().map_err(|e| e.to_string())?;
    let geometry = (requested.data_shards, requested.parity_shards);
    if !matches!(geometry, (20, 2) | (20, 4) | (20, 6) | (1, 1))
        && geometry != (ceiling.data_shards, ceiling.parity_shards)
    {
        return Err("FXLink unsupported protection profile".into());
    }
    if requested.shard_size > ceiling.shard_size
        || requested.max_record_len > ceiling.max_record_len
        || requested.max_records > ceiling.max_records
        || requested.max_blocks > ceiling.max_blocks
        || requested.max_repair_shards > ceiling.max_repair_shards
    {
        return Err("FXLink profile exceeds listener resource ceilings".into());
    }
    Ok(requested)
}

#[cfg(test)]
mod bootstrap_negotiation_tests {
    use super::*;
    #[test]
    fn bootstrap_advertisement_requires_all_three_options() {
        for bits in 0..8 {
            let f = crate::fxlink::Features {
                geometry_hints: false,
                trimmed_records: bits & 1 != 0,
                repair_parity: bits & 2 != 0,
                bootstrap_repair: bits & 4 != 0,
            };
            let mut expected = protocols();
            if bits & 1 != 0 {
                expected.insert(0, b"fxlink/6".to_vec());
                if bits & 2 != 0 {
                    expected.insert(0, b"fxlink/7".to_vec());
                    if bits & 4 != 0 {
                        expected.insert(0, b"fxlink/8".to_vec());
                    }
                }
            }
            assert_eq!(protocols_with_features(f), expected);
        }
        assert_eq!(protocols_with_features(Default::default()), protocols());
    }
}
