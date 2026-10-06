use serde_json::Value;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

#[derive(Clone)]
pub struct Settings {
    pub address: SocketAddr,
    pub secure: bool,
    pub interface: Option<Ipv4Addr>,
    pub source_ip: Option<IpAddr>,
    pub jitter: Duration,
    pub ttl: u32,
    pub key_file: Option<PathBuf>,
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
            !["interface", "source_ip", "jitter_ms", "ttl", "key_file"].contains(&k.as_str())
        }) {
            return Err("Unknown direct RTP option".into());
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
        let key_file = opts
            .get("key_file")
            .map(|v| {
                let s = v
                    .as_str()
                    .filter(|s| !s.chars().any(char::is_control))
                    .ok_or("SRTP key file must be an absolute path")?;
                let p = PathBuf::from(s);
                if !p.is_absolute() || s.len() > 4096 {
                    return Err("SRTP key file must be an absolute path");
                }
                Ok(p)
            })
            .transpose()?;
        if secure != key_file.is_some() {
            return Err("SRTP requires a key file; plaintext RTP forbids one".into());
        }
        Ok(Self {
            address: SocketAddr::new(ip, port),
            secure,
            interface,
            source_ip,
            jitter: Duration::from_millis(number(&opts, "jitter_ms", 50, 0, 1000)?),
            ttl: number(&opts, "ttl", 16, 1, 255)? as u32,
            key_file,
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
    rows.iter()
        .map(|row| {
            let obj = row
                .as_object()
                .ok_or("Direct RTP destination must be an object")?;
            if obj
                .keys()
                .any(|k| !["url", "flussonix_rtp", "max_mbps", "disabled"].contains(&k.as_str()))
            {
                return Err("Unknown direct RTP destination option".into());
            }
            if row.get("disabled").is_some_and(|v| !v.is_boolean()) {
                return Err("RTP enabled setting must be boolean".into());
            }
            let settings = Settings::parse(row)?;
            if settings.address.ip().is_unspecified()
                || settings.source_ip.is_some()
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
        .collect()
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
