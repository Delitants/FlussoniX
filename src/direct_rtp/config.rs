use serde_json::Value;
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrackMedia {
    Audio,
    Video,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputTrackKey {
    pub track_id: u32,
    pub media: TrackMedia,
    pub encoding: String,
    pub key_file: PathBuf,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    pub address: SocketAddr,
    pub secure: bool,
    pub interface: Option<Ipv4Addr>,
    pub source_ip: Option<IpAddr>,
    pub jitter: Duration,
    pub receive_buffer_bytes: Option<u32>,
    pub ttl: u32,
    pub key_file: Option<PathBuf>,
    pub track_key_files: Option<Vec<PathBuf>>,
    pub expected_ssrcs: Option<Vec<u32>>,
    pub track_keys: Option<Vec<OutputTrackKey>>,
    pub allow_legacy_reused_key: bool,
    pub elementary: bool,
    pub sdp_file: Option<PathBuf>,
}
impl Settings {
    pub fn input(item: &Value) -> Result<Option<Self>, String> {
        let raw = item["url"].as_str().unwrap_or("");
        if !raw.starts_with("rtp://") && !raw.starts_with("srtp://") {
            if item.get("flussonix_rtp").is_some() {
                return Err("RTP options require a direct RTP or SRTP input".into());
            }
            return Ok(None);
        }
        let settings = Self::parse(item)?;
        if settings.track_keys.is_some() {
            return Err("Elementary SRTP output track keys are not input options".into());
        }
        if settings.elementary && settings.sdp_file.is_none() {
            return Err("Elementary RTP input requires an SDP file".into());
        }
        if let Some(interface) = settings.interface {
            if !settings.address.ip().is_multicast()
                && !settings.address.ip().is_unspecified()
                && settings.address.ip() != IpAddr::V4(interface)
            {
                return Err(
                    "RTP input interface must match its bind address or restrict a wildcard".into(),
                );
            }
        }
        Ok(Some(settings))
    }
    pub fn parse(item: &Value) -> Result<Self, String> {
        let raw = item["url"].as_str().ok_or("Direct RTP URL required")?;
        let authority = raw
            .split_once("://")
            .map(|(_, rest)| rest)
            .ok_or("Invalid direct RTP URL")?;
        if raw.chars().any(|c| c.is_control() || c.is_whitespace())
            || authority
                .strip_suffix('/')
                .unwrap_or(authority)
                .contains('/')
        {
            return Err("Direct RTP URL cannot contain whitespace or a path".into());
        }
        let u = url::Url::parse(raw).map_err(|_| "Invalid direct RTP URL")?;
        if !["rtp", "srtp"].contains(&u.scheme())
            || !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
            || !["", "/"].contains(&u.path())
        {
            return Err("Direct RTP requires rtp://IP:PORT or srtp://IP:PORT without credentials, path or query".into());
        }
        let ip: IpAddr = u
            .host_str()
            .ok_or("Direct RTP IP required")?
            .trim_matches(['[', ']'])
            .parse()
            .map_err(|_| "Direct RTP requires a literal IP address")?;
        let port = u
            .port()
            .filter(|p| (1024..=65534).contains(p))
            .ok_or("Direct RTP port must be 1024..65534 (next port is RTCP)")?;
        let opts = item
            .get("flussonix_rtp")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let object = opts.as_object().ok_or("RTP options must be an object")?;
        if object.keys().any(|k| {
            ![
                "interface",
                "source_ip",
                "jitter_ms",
                "receive_buffer_bytes",
                "ttl",
                "key_file",
                "track_key_files",
                "expected_ssrcs",
                "track_keys",
                "allow_legacy_reused_key",
                "profile",
                "sdp_file",
            ]
            .contains(&k.as_str())
        }) {
            return Err("Unknown direct RTP option".into());
        }
        let elementary = match opts.get("profile").map(Value::as_str) {
            None | Some(Some("mp2t")) => false,
            Some(Some("elementary")) => true,
            _ => return Err("RTP profile must be mp2t or elementary".into()),
        };
        let sdp_file = opts
            .get("sdp_file")
            .map(|v| {
                let value = v
                    .as_str()
                    .filter(|s| s.len() <= 4096 && !s.chars().any(char::is_control))
                    .ok_or("SDP file must be an absolute path")?;
                let path = PathBuf::from(value);
                if !path.is_absolute() {
                    return Err("SDP file must be an absolute path");
                }
                Ok(path)
            })
            .transpose()?;
        if (!elementary && sdp_file.is_some())
            || (elementary && (port > 65520 || ip.is_unspecified()))
        {
            return Err("Elementary RTP requires a concrete IP with port 1024..65520; SDP files require the elementary profile".into());
        }
        let interface = opts
            .get("interface")
            .map(|v| {
                v.as_str()
                    .ok_or("RTP interface must be an IPv4 address")?
                    .parse::<Ipv4Addr>()
                    .map_err(|_| "RTP interface must be an IPv4 address")
            })
            .transpose()?;
        if interface.is_some_and(|ip| ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast())
        {
            return Err("RTP interface must be a concrete unicast IPv4 address".into());
        }
        let source_ip = opts
            .get("source_ip")
            .map(|v| {
                v.as_str()
                    .ok_or("RTP source filter must be an IP address")?
                    .parse::<IpAddr>()
                    .map_err(|_| "RTP source filter must be an IP address")
            })
            .transpose()?;
        if ip.is_multicast() && (!ip.is_ipv4() || interface.is_none()) {
            return Err("IPv4 multicast requires an explicit IPv4 interface; IPv6 multicast is not implemented".into());
        }
        if !ip.is_ipv4() && interface.is_some() {
            return Err("IPv4 interface cannot be used with an IPv6 endpoint".into());
        }
        if source_ip
            .is_some_and(|s| s.is_multicast() || s.is_unspecified() || s.is_ipv4() != ip.is_ipv4())
        {
            return Err(
                "RTP source filter must be a unicast IP in the endpoint address family".into(),
            );
        }
        let secure = u.scheme() == "srtp";
        let allow_legacy_reused_key = opts
            .get("allow_legacy_reused_key")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or("SRTP legacy key reuse option must be boolean")
            })
            .transpose()?
            .unwrap_or(false);
        if !secure && opts.get("allow_legacy_reused_key").is_some() {
            return Err("Plain RTP cannot override SRTP receiver key history".into());
        }
        let key_file = opts.get("key_file").map(key_path).transpose()?;
        let track_key_files = opts
            .get("track_key_files")
            .map(|value| {
                let entries = bounded_tracks(value, "SRTP track key files require 1..8 paths")?;
                let paths: Vec<_> = entries.iter().map(key_path).collect::<Result<_, _>>()?;
                if paths.iter().collect::<HashSet<_>>().len() != paths.len() {
                    return Err("SRTP track key files must be distinct");
                }
                Ok(paths)
            })
            .transpose()?;
        let expected_ssrcs = opts
            .get("expected_ssrcs")
            .map(|value| {
                let entries = bounded_tracks(value, "SRTP expected SSRCs require 1..8 values")?;
                let ssrcs: Vec<u32> = entries
                    .iter()
                    .map(|value| {
                        value
                            .as_u64()
                            .and_then(|n| u32::try_from(n).ok())
                            .filter(|n| *n != 0)
                            .ok_or("SRTP expected SSRC must be a nonzero u32")
                    })
                    .collect::<Result<_, _>>()?;
                if ssrcs.iter().collect::<HashSet<_>>().len() != ssrcs.len() {
                    return Err("SRTP expected SSRCs must be distinct");
                }
                Ok(ssrcs)
            })
            .transpose()?;
        let track_keys = opts
            .get("track_keys")
            .map(|value| {
                let entries = bounded_tracks(value, "SRTP output track keys require 1..8 entries")?;
                let keys: Vec<OutputTrackKey> = entries
                    .iter()
                    .map(|value| {
                        let object = value
                            .as_object()
                            .ok_or("SRTP output track key must be an object")?;
                        if object.len() != 4
                            || object.keys().any(|key| {
                                !["track_id", "media", "encoding", "key_file"]
                                    .contains(&key.as_str())
                            })
                        {
                            return Err(
                                "SRTP output track key requires ID, media, encoding and key file",
                            );
                        }
                        let track_id = value["track_id"]
                            .as_u64()
                            .and_then(|n| u32::try_from(n).ok())
                            .filter(|n| *n != 0)
                            .ok_or("SRTP output track ID must be a nonzero u32")?;
                        let media = match value["media"].as_str() {
                            Some("audio") => TrackMedia::Audio,
                            Some("video") => TrackMedia::Video,
                            _ => return Err("SRTP output track media must be audio or video"),
                        };
                        let encoding = value["encoding"]
                            .as_str()
                            .filter(|s| valid_track_encoding(s, &media))
                            .ok_or("SRTP output track encoding is invalid")?
                            .to_owned();
                        Ok(OutputTrackKey {
                            track_id,
                            media,
                            encoding,
                            key_file: key_path(&value["key_file"])?,
                        })
                    })
                    .collect::<Result<_, &'static str>>()?;
                if keys
                    .iter()
                    .map(|k| k.track_id)
                    .collect::<HashSet<_>>()
                    .len()
                    != keys.len()
                    || keys
                        .iter()
                        .map(|k| &k.key_file)
                        .collect::<HashSet<_>>()
                        .len()
                        != keys.len()
                {
                    return Err("SRTP output track IDs and key paths must be distinct");
                }
                Ok(keys)
            })
            .transpose()?;
        if secure {
            if elementary {
                let modes = usize::from(key_file.is_some())
                    + usize::from(track_key_files.is_some())
                    + usize::from(track_keys.is_some());
                if modes != 1 || expected_ssrcs.is_some() && key_file.is_none() {
                    return Err("Elementary SRTP requires one track protection mode".into());
                }
            } else if key_file.is_none()
                || track_key_files.is_some()
                || expected_ssrcs.is_some()
                || track_keys.is_some()
            {
                return Err(
                    "MP2T SRTP requires one key file without elementary track options".into(),
                );
            }
        } else if key_file.is_some()
            || track_key_files.is_some()
            || expected_ssrcs.is_some()
            || track_keys.is_some()
        {
            return Err("Plaintext RTP forbids SRTP track protection".into());
        }
        let receive_buffer_bytes = opts
            .get("receive_buffer_bytes")
            .map(|_| number(&opts, "receive_buffer_bytes", 0, 65536, 33554432).map(|n| n as u32))
            .transpose()?;
        Ok(Self {
            address: SocketAddr::new(ip, port),
            secure,
            interface,
            source_ip,
            jitter: Duration::from_millis(number(&opts, "jitter_ms", 50, 0, 1000)?),
            receive_buffer_bytes,
            ttl: number(&opts, "ttl", 16, 1, 255)? as u32,
            key_file,
            track_key_files,
            expected_ssrcs,
            track_keys,
            allow_legacy_reused_key,
            elementary,
            sdp_file,
        })
    }
    pub fn endpoint(&self) -> String {
        format!(
            "{}://{}",
            if self.secure { "srtp" } else { "rtp" },
            self.address
        )
    }
}
#[derive(PartialEq, Eq)]
pub struct Output {
    pub settings: Settings,
    pub disabled: bool,
    pub max_mbps: u64,
}
pub fn outputs(cfg: &Value) -> Result<Vec<Output>, String> {
    let Some(rows) = cfg.get("flussonix_rtp_outputs") else {
        return Ok(vec![]);
    };
    let rows = rows
        .as_array()
        .ok_or("Direct RTP outputs must be an array")?;
    if rows.len() > 4 {
        return Err("At most four direct RTP/SRTP destinations are supported".into());
    }
    let outputs: Vec<Output> =
        rows.iter()
            .map(|row| {
                let obj = row
                    .as_object()
                    .ok_or("Direct RTP destination must be an object")?;
                if obj.keys().any(|k| {
                    !["url", "flussonix_rtp", "max_mbps", "disabled"].contains(&k.as_str())
                }) {
                    return Err("Unknown direct RTP destination option".into());
                }
                if row.get("disabled").is_some_and(|v| !v.is_boolean()) {
                    return Err("RTP enabled setting must be boolean".into());
                }
                let settings = Settings::parse(row)?;
                if settings.track_key_files.is_some()
                    || settings.expected_ssrcs.is_some()
                    || row["flussonix_rtp"]
                        .get("allow_legacy_reused_key")
                        .is_some()
                {
                    return Err("SRTP receive track bindings are not output options".into());
                }
                if settings.address.ip().is_unspecified()
                    || settings.sdp_file.is_some()
                    || settings.source_ip.is_some()
                    || settings.receive_buffer_bytes.is_some()
                    || row["flussonix_rtp"].get("jitter_ms").is_some()
                {
                    return Err(
                    "RTP destinations require a concrete address and cannot use receive options"
                        .into(),
                );
                }
                Ok(Output {
                    settings,
                    disabled: row["disabled"] == true,
                    max_mbps: number(row, "max_mbps", 100, 1, 10000)?,
                })
            })
            .collect::<Result<_, String>>()?;
    for (i, a) in outputs.iter().enumerate().filter(|(_, o)| !o.disabled) {
        let start = a.settings.address.port();
        let end = start + if a.settings.elementary { 15 } else { 1 };
        for b in outputs.iter().skip(i + 1).filter(|o| !o.disabled) {
            let other = b.settings.address.port();
            let other_end = other + if b.settings.elementary { 15 } else { 1 };
            if a.settings.address.ip() == b.settings.address.ip()
                && start <= other_end
                && other <= end
            {
                return Err("Enabled RTP destinations on the same address require non-overlapping RTP/RTCP port ranges".into());
            }
        }
    }
    Ok(outputs)
}
fn bounded_tracks<'a>(value: &'a Value, error: &'static str) -> Result<&'a [Value], &'static str> {
    value
        .as_array()
        .map(Vec::as_slice)
        .filter(|entries| (1..=8).contains(&entries.len()))
        .ok_or(error)
}
fn key_path(value: &Value) -> Result<PathBuf, &'static str> {
    let s = value
        .as_str()
        .filter(|s| s.len() <= 4096 && !s.chars().any(char::is_control))
        .ok_or("SRTP key file must be an absolute path")?;
    let path = PathBuf::from(s);
    if !path.is_absolute() {
        return Err("SRTP key file must be an absolute path");
    }
    Ok(path)
}
fn valid_track_encoding(value: &str, media: &TrackMedia) -> bool {
    if value.len() > 64 {
        return false;
    }
    match media {
        TrackMedia::Video => ["H264/90000", "H265/90000"].contains(&value),
        TrackMedia::Audio if value == "MPA/90000" => true,
        TrackMedia::Audio => {
            let Some(rest) = value.strip_prefix("MPEG4-GENERIC/") else {
                return false;
            };
            let Some((rate, channels)) = rest.split_once('/') else {
                return false;
            };
            let Ok(rate_number) = rate.parse::<u32>() else {
                return false;
            };
            let Ok(channel_number) = channels.parse::<u8>() else {
                return false;
            };
            rate == rate_number.to_string()
                && channels == channel_number.to_string()
                && [
                    96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025,
                    8000, 7350,
                ]
                .contains(&rate_number)
                && (1..=8).contains(&channel_number)
        }
    }
}
pub fn enabled(cfg: &Value) -> bool {
    cfg["flussonix_rtp_outputs"]
        .as_array()
        .is_some_and(|rows| rows.iter().any(|r| r["disabled"] != true))
}
fn number(v: &Value, key: &str, default: u64, min: u64, max: u64) -> Result<u64, String> {
    v.get(key).map_or(Ok(default), |n| {
        n.as_u64()
            .filter(|n| (min..=max).contains(n))
            .ok_or_else(|| format!("RTP {key} must be a whole number from {min} to {max}"))
    })
}
