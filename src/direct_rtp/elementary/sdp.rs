//! Strict static SDP, regenerated before passing to the independent decoder.
use super::super::config::Settings;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::{
    collections::{BTreeMap, HashSet},
    net::{IpAddr, SocketAddr},
    path::Path,
};
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Codec {
    H264,
    H265,
    Aac,
    Mpa,
}
#[derive(Clone, Debug)]
pub struct Track {
    pub port: u16,
    pub payload: u8,
    pub clock: u32,
    pub codec: Codec,
    pub encoding: String,
    pub fmtp: String,
    pub video: bool,
}
#[derive(Clone, Debug)]
pub struct Session {
    pub address: IpAddr,
    pub tracks: Vec<Track>,
}
struct Raw {
    port: u16,
    payload: u8,
    video: bool,
    encoding: Option<String>,
    fmtp: Option<String>,
    address: Option<IpAddr>,
}
fn connection(value: &str) -> Result<IpAddr, String> {
    let f: Vec<_> = value.split_whitespace().collect();
    if f.len() != 3 || f[0] != "IN" {
        return Err("SDP requires a literal connection address".into());
    }
    let address = f[2]
        .split('/')
        .next()
        .unwrap()
        .parse::<IpAddr>()
        .map_err(|_| "SDP requires a literal connection address")?;
    if address.is_unspecified()
        || address.is_ipv4() != (f[1] == "IP4")
        || !["IP4", "IP6"].contains(&f[1])
    {
        return Err("Invalid SDP connection address".into());
    }
    // IPv4 multicast TTL notation is metadata only; socket policy comes from configuration.
    let parts: Vec<_> = f[2].split('/').collect();
    if parts.len() > 2
        || parts.len() == 2
            && (!address.is_multicast() || !address.is_ipv4() || parts[1].parse::<u8>().is_err())
    {
        return Err("Unsupported SDP connection range".into());
    }
    Ok(address)
}
fn number(v: &str) -> Result<u16, String> {
    v.parse().map_err(|_| "Invalid SDP number".into())
}
fn sets(value: &str, hevc: bool) -> Result<(), String> {
    if value.len() > 8192 {
        return Err("SDP codec metadata exceeds limit".into());
    }
    let mut count = 0;
    for set in value.split(',') {
        count += 1;
        let b = STANDARD
            .decode(set)
            .map_err(|_| "Invalid SDP parameter set")?;
        if count > 16
            || b.len() < if hevc { 2 } else { 4 }
            || b.len() > 4096
            || b[0] & 128 != 0
            || hevc && b[1] & 7 == 0
        {
            return Err("Invalid SDP parameter set".into());
        }
    }
    Ok(())
}
fn validate(raw: Raw) -> Result<Track, String> {
    let encoding = raw.encoding.unwrap_or_else(|| {
        if raw.payload == 14 {
            "MPA/90000".into()
        } else {
            String::new()
        }
    });
    let e: Vec<_> = encoding.split('/').collect();
    if e.len() < 2 || e.len() > 3 {
        return Err("SDP RTP encoding required".into());
    }
    let clock = e[1].parse::<u32>().map_err(|_| "Invalid SDP clock")?;
    let codec = match e[0].to_ascii_uppercase().as_str() {
        "H264" => Codec::H264,
        "H265" => Codec::H265,
        "MPEG4-GENERIC" => Codec::Aac,
        "MPA" => Codec::Mpa,
        _ => return Err("Unsupported elementary SDP codec".into()),
    };
    if raw.video != matches!(codec, Codec::H264 | Codec::H265)
        || codec != Codec::Aac && (clock != 90000 || e.len() != 2)
        || raw.payload < 96 && !(codec == Codec::Mpa && raw.payload == 14)
    {
        return Err("SDP media, payload and codec clock disagree".into());
    }
    let mut fmtp = raw.fmtp.unwrap_or_default();
    let mut p = BTreeMap::new();
    if !fmtp.is_empty() {
        for item in fmtp.split(';') {
            let (k, v) = item
                .trim()
                .split_once('=')
                .ok_or("Invalid SDP codec parameter")?;
            if k.is_empty() || v.is_empty() || p.insert(k.to_ascii_lowercase(), v.trim()).is_some()
            {
                return Err("Duplicate or empty SDP codec parameter".into());
            }
        }
    }
    let allowed: &[&str] = match codec {
        Codec::H264 => &[
            "packetization-mode",
            "profile-level-id",
            "sprop-parameter-sets",
        ],
        Codec::H265 => &["sprop-vps", "sprop-sps", "sprop-pps", "sprop-max-don-diff"],
        Codec::Aac => &[
            "streamtype",
            "profile-level-id",
            "mode",
            "config",
            "sizelength",
            "indexlength",
            "indexdeltalength",
            "constantduration",
        ],
        Codec::Mpa => &[],
    };
    if p.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err("Unsupported SDP codec parameter".into());
    }
    match codec {
        Codec::H264 => {
            if p.get("packetization-mode") != Some(&"1") {
                return Err("H264 elementary SDP requires packetization-mode=1".into());
            }
            if let Some(v) = p.get("profile-level-id") {
                if v.len() != 6 || !v.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err("Invalid H264 profile metadata".into());
                }
            }
            if let Some(v) = p.get("sprop-parameter-sets") {
                sets(v, false)?;
            }
        }
        Codec::H265 => {
            if p.get("sprop-max-don-diff").is_some_and(|v| *v != "0") {
                return Err("HEVC DON ordering is not implemented".into());
            }
            for k in ["sprop-vps", "sprop-sps", "sprop-pps"] {
                if let Some(v) = p.get(k) {
                    sets(v, true)?;
                }
            }
        }
        Codec::Mpa => {}
        Codec::Aac => {
            if p.get("streamtype").is_some_and(|v| *v != "5") {
                return Err("Unsupported AAC stream type".into());
            }
            for (k, v) in [
                ("mode", "AAC-hbr"),
                ("sizelength", "13"),
                ("indexlength", "3"),
                ("indexdeltalength", "3"),
            ] {
                if !p.get(k).is_some_and(|x| x.eq_ignore_ascii_case(v)) {
                    return Err("Unsupported AAC RTP framing".into());
                }
            }
            if p.get("profile-level-id").is_some_and(|v| *v != "1")
                || p.get("constantduration").is_some_and(|v| *v != "1024")
            {
                return Err("Unsupported AAC RTP profile".into());
            }
            let config = p.get("config").ok_or("AAC configuration required")?;
            if ![4, 10].contains(&config.len()) || !config.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("AAC-LC configuration required".into());
            }
            let bytes: Vec<_> = (0..config.len())
                .step_by(2)
                .map(|n| u8::from_str_radix(&config[n..n + 2], 16).unwrap())
                .collect();
            let freq = usize::from(((bytes[0] & 7) << 1) | (bytes[1] >> 7));
            let ch = (bytes[1] >> 3) & 15;
            if bytes[0] >> 3 != 2
                || freq > 12
                || !(1..=7).contains(&ch)
                || bytes[1] & 7 != 0
                || bytes.len() == 5 && bytes[2..] != [0x56, 0xe5, 0]
            {
                return Err("AAC-LC indexed configuration required".into());
            }
            let rate = [
                96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000,
                7350,
            ][freq];
            let channels = if ch == 7 { 8 } else { ch };
            if clock != rate || e.len() != 3 || e[2].parse::<u8>().ok() != Some(channels) {
                return Err("AAC configuration and RTP clock/channels disagree".into());
            }
        }
    }
    let implicit_audio = codec == Codec::Aac && !p.contains_key("streamtype");
    drop(p);
    // FFmpeg's independent SDP emitter omits this required MIME parameter.
    // Infer audio only after validating the AAC-LC ASC and fixed AU framing.
    if implicit_audio {
        fmtp.push_str(";streamtype=5");
    }
    Ok(Track {
        port: raw.port,
        payload: raw.payload,
        clock,
        codec,
        encoding,
        fmtp,
        video: raw.video,
    })
}
impl Session {
    pub fn parse(bytes: &[u8], settings: &Settings) -> Result<Self, String> {
        if bytes.len() > 16384 || bytes.contains(&0) {
            return Err("SDP exceeds its bounded text profile".into());
        }
        let text = std::str::from_utf8(bytes).map_err(|_| "SDP must be UTF-8")?;
        let mut global = None;
        let mut raws: Vec<Raw> = vec![];
        let mut seen = HashSet::new();
        for line in text.lines() {
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            if line.len() > 8192 || line.chars().any(|c| c.is_control()) {
                return Err("Invalid SDP line".into());
            }
            let (kind, value) = line.split_once('=').ok_or("Invalid SDP field")?;
            match kind {
                "v" | "o" | "s" | "t" => {
                    if !raws.is_empty() || !seen.insert(kind) {
                        return Err("Duplicate or misplaced SDP session field".into());
                    }
                    if kind == "v" && value != "0" || kind == "t" && value != "0 0" {
                        return Err("Only live SDP version 0 is supported".into());
                    }
                }
                "c" => {
                    let ip = connection(value)?;
                    if let Some(raw) = raws.last_mut() {
                        if raw.address.replace(ip).is_some() {
                            return Err("Duplicate SDP connection".into());
                        }
                    } else if global.replace(ip).is_some() {
                        return Err("Duplicate SDP connection".into());
                    }
                }
                "m" => {
                    let f: Vec<_> = value.split_whitespace().collect();
                    if raws.len() >= 8
                        || f.len() != 4
                        || !["audio", "video"].contains(&f[0])
                        || f[2]
                            != if settings.secure {
                                "RTP/SAVP"
                            } else {
                                "RTP/AVP"
                            }
                    {
                        return Err(
                            "SDP requires matching AVP/SAVP transport for up to eight audio/video tracks, one payload each"
                                .into(),
                        );
                    }
                    let port = number(f[1])?;
                    let payload = number(f[3])?;
                    if !(1024..=65534).contains(&port) || payload > 127 {
                        return Err("Invalid SDP media port or payload".into());
                    }
                    raws.push(Raw {
                        port,
                        payload: payload as u8,
                        video: f[0] == "video",
                        encoding: None,
                        fmtp: None,
                        address: None,
                    });
                }
                "a" => {
                    if let Some((kind, value)) = value.split_once(':') {
                        if kind == "tool" && value.len() <= 256 {
                            continue;
                        }
                        if !["rtpmap", "fmtp"].contains(&kind) {
                            return Err("Unsupported SDP attribute or indirection".into());
                        }
                        let raw = raws
                            .last_mut()
                            .ok_or("SDP codec attribute requires media")?;
                        let (pt, value) =
                            value.split_once(' ').ok_or("Invalid SDP codec attribute")?;
                        if number(pt)? != u16::from(raw.payload) {
                            return Err("SDP payload metadata disagrees".into());
                        }
                        let target = if kind == "rtpmap" {
                            &mut raw.encoding
                        } else {
                            &mut raw.fmtp
                        };
                        if target.replace(value.trim().into()).is_some() {
                            return Err("Duplicate SDP codec attribute".into());
                        }
                    } else if !["recvonly", "sendonly", "sendrecv"].contains(&value) {
                        return Err("Unsupported SDP attribute".into());
                    }
                }
                "b" => {
                    let (kind, v) = value.split_once(':').ok_or("Invalid SDP bandwidth")?;
                    if kind != "AS" || v.parse::<u32>().is_err() {
                        return Err("Unsupported SDP bandwidth".into());
                    }
                }
                "i" => {}
                _ => return Err("Unsupported SDP field".into()),
            }
        }
        if !["v", "o", "s", "t"].iter().all(|k| seen.contains(k)) || raws.is_empty() {
            return Err("SDP session fields and media required".into());
        }
        let address = raws[0]
            .address
            .or(global)
            .ok_or("SDP connection required")?;
        if SocketAddr::new(address, raws[0].port) != settings.address {
            return Err("First SDP media address/port must match the input URL".into());
        }
        let mut ports = HashSet::new();
        let mut tracks = vec![];
        for raw in raws {
            if raw.address.or(global) != Some(address)
                || !ports.insert(raw.port)
                || !ports.insert(raw.port + 1)
            {
                return Err("SDP tracks require one address and exclusive RTP/RTCP ports".into());
            }
            tracks.push(validate(raw)?);
        }
        Ok(Self { address, tracks })
    }
    pub fn read(path: &Path, settings: &Settings) -> Result<Self, String> {
        use std::{
            fs::OpenOptions,
            io::Read,
            os::unix::fs::{MetadataExt, OpenOptionsExt},
        };
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| "SDP file unavailable")?;
        let m = file.metadata().map_err(|_| "SDP file unavailable")?;
        // Descriptor metadata prevents a path swap; nonblocking open prevents FIFO stalls.
        if !m.is_file()
            || m.uid() != unsafe { libc::geteuid() }
            || m.mode() & 0o022 != 0
            || m.len() > 16384
        {
            return Err("SDP file must be regular, daemon-owned, not group/world writable and at most 16 KiB".into());
        }
        let mut bytes = vec![];
        file.take(16385)
            .read_to_end(&mut bytes)
            .map_err(|_| "SDP file unavailable")?;
        Self::parse(&bytes, settings)
    }
    pub fn decoder_sdp(&self, ports: &[u16]) -> String {
        assert_eq!(ports.len(), self.tracks.len());
        let mut out="v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=FlussoniX validated decoder\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n".to_string();
        for (t, port) in self.tracks.iter().zip(ports) {
            out.push_str(&format!(
                "m={} {port} RTP/AVP {}\r\na=rtpmap:{} {}\r\n",
                if t.video { "video" } else { "audio" },
                t.payload,
                t.payload,
                t.encoding
            ));
            if !t.fmtp.is_empty() {
                out.push_str(&format!("a=fmtp:{} {}\r\n", t.payload, t.fmtp));
            }
        }
        out
    }
}
