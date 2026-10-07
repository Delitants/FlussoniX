//! Bounded RTSP/1.0 framing. A dedicated reader owns partial message state.
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt};
#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub uri: String,
    pub cseq: u32,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}
#[derive(Debug)]
pub enum Event {
    Request(Request),
    Interleaved(u8, Vec<u8>),
}
#[derive(Debug)]
pub struct Error {
    pub code: u16,
    pub cseq: Option<u32>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transport {
    pub rtp: u8,
    pub rtcp: u8,
}
impl Transport {
    pub fn parse(s: &str) -> Result<Self, u16> {
        Self::parse_mode(s, "PLAY")
    }
    pub(crate) fn record(s: &str) -> Result<Self, u16> {
        Self::parse_mode(s, "RECORD")
    }
    fn parse_mode(s: &str, mode: &str) -> Result<Self, u16> {
        let mut parts = s.split(';');
        if !parts
            .next()
            .is_some_and(|p| p.trim().eq_ignore_ascii_case("RTP/AVP/TCP"))
        {
            return Err(461);
        }
        let mut seen = std::collections::HashSet::new();
        let mut channels = None;
        for p in parts {
            let (key, value) = p.trim().split_once('=').unwrap_or((p.trim(), ""));
            let key = key.to_ascii_lowercase();
            if !seen.insert(key.clone()) {
                return Err(461);
            }
            match key.as_str() {
                "unicast" if value.is_empty() => {}
                "mode" if value.trim_matches('"').eq_ignore_ascii_case(mode) => {}
                "interleaved" => {
                    let (a, b) = value.split_once('-').ok_or(461u16)?;
                    let a = a.parse::<u8>().map_err(|_| 461u16)?;
                    let b = b.parse::<u8>().map_err(|_| 461u16)?;
                    if a == b {
                        return Err(461);
                    }
                    channels = Some(Self { rtp: a, rtcp: b });
                }
                _ => return Err(461),
            }
        }
        channels.ok_or(461)
    }
}
pub async fn read_event<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Event, Error> {
    let bad = || Error {
        code: 400,
        cseq: None,
    };
    let first = reader.read_u8().await.map_err(|_| bad())?;
    if first == b'$' {
        let channel = reader.read_u8().await.map_err(|_| bad())?;
        let size = reader.read_u16().await.map_err(|_| bad())? as usize;
        if size > 8192 {
            return Err(bad());
        }
        let mut body = vec![0; size];
        reader.read_exact(&mut body).await.map_err(|_| bad())?;
        return Ok(Event::Interleaved(channel, body));
    }
    let mut data = vec![first];
    while !data.ends_with(b"\r\n\r\n") {
        if data.len() >= 16384 {
            return Err(bad());
        }
        data.push(reader.read_u8().await.map_err(|_| bad())?);
    }
    let text = std::str::from_utf8(&data).map_err(|_| bad())?;
    let mut lines = text[..text.len() - 4].split("\r\n");
    let mut fields = lines.next().ok_or_else(bad)?.split(' ');
    let method = fields.next().ok_or_else(bad)?;
    let uri = fields.next().ok_or_else(bad)?;
    let version = fields.next().ok_or_else(bad)?;
    if fields.next().is_some()
        || method.is_empty()
        || !method.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
        || uri.is_empty()
        || uri.len() > 8192
        || uri.bytes().any(|b| b <= 32 || b >= 127)
    {
        return Err(bad());
    }
    let mut headers = HashMap::new();
    for line in lines {
        if headers.len() >= 64 {
            return Err(bad());
        }
        let (k, v) = line.split_once(':').ok_or_else(bad)?;
        if k.is_empty()
            || !k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || v.bytes().any(|b| b < 32 && b != 9 || b == 127)
        {
            return Err(bad());
        }
        if headers
            .insert(k.to_ascii_lowercase(), v.trim().to_string())
            .is_some()
        {
            return Err(bad());
        }
    }
    let cseq = headers
        .get("cseq")
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        .ok_or_else(bad)?
        .parse()
        .map_err(|_| bad())?;
    if version != "RTSP/1.0" {
        return Err(Error {
            code: 505,
            cseq: Some(cseq),
        });
    }
    let size = match headers.get("content-length") {
        Some(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse::<usize>().map_err(|_| bad())?
        }
        Some(_) => return Err(bad()),
        None => 0,
    };
    if size > 65536 {
        return Err(Error {
            code: 413,
            cseq: Some(cseq),
        });
    }
    let mut body = vec![0; size];
    reader.read_exact(&mut body).await.map_err(|_| bad())?;
    Ok(Event::Request(Request {
        method: method.into(),
        uri: uri.into(),
        cseq,
        headers,
        body,
    }))
}
pub fn response(code: u16, cseq: u32, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Request Entity Too Large",
        415 => "Unsupported Media Type",
        453 => "Not Enough Bandwidth",
        454 => "Session Not Found",
        455 => "Method Not Valid in This State",
        457 => "Invalid Range",
        461 => "Unsupported Transport",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        505 => "RTSP Version Not Supported",
        551 => "Option Not Supported",
        _ => "Error",
    };
    let mut out = format!(
        "RTSP/1.0 {code} {reason}\r\nCSeq: {cseq}\r\nServer: FlussoniX\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (k, v) in headers {
        debug_assert!(!v.contains(['\r', '\n']));
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let mut bytes = out.into_bytes();
    bytes.extend(body);
    bytes
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientPorts {
    pub rtp: u16,
    pub rtcp: u16,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Offer {
    Tcp(Transport),
    Udp(ClientPorts),
}
impl ClientPorts {
    pub fn valid(self) -> bool {
        self.rtp >= 1024 && self.rtp % 2 == 0 && self.rtp.checked_add(1) == Some(self.rtcp)
    }
}
impl Offer {
    pub fn parse(value: &str) -> Result<Self, u16> {
        Self::parse_mode(value, "PLAY")
    }
    pub(crate) fn record(value: &str) -> Result<Self, u16> {
        Self::parse_mode(value, "RECORD")
    }
    fn parse_mode(value: &str, mode: &str) -> Result<Self, u16> {
        let mut parts = value.split(';');
        let protocol = parts.next().unwrap_or("").trim();
        if protocol.eq_ignore_ascii_case("RTP/AVP/TCP") {
            return if mode == "RECORD" {
                Transport::record(value)
            } else {
                Transport::parse(value)
            }
            .map(Self::Tcp);
        }
        if !["RTP/AVP", "RTP/AVP/UDP"]
            .iter()
            .any(|p| protocol.eq_ignore_ascii_case(p))
        {
            return Err(461);
        }
        let mut seen = std::collections::HashSet::new();
        let mut ports = None;
        let mut unicast = false;
        for part in parts {
            let (key, value) = part.trim().split_once('=').unwrap_or((part.trim(), ""));
            let key = key.to_ascii_lowercase();
            if !seen.insert(key.clone()) {
                return Err(461);
            }
            match key.as_str() {
                "unicast" if value.is_empty() => unicast = true,
                "mode"
                    if value.eq_ignore_ascii_case(mode)
                        || value.eq_ignore_ascii_case(&format!("\"{mode}\"")) => {}
                "client_port" => {
                    let (a, b) = value.split_once('-').ok_or(461u16)?;
                    if a.is_empty()
                        || b.is_empty()
                        || !a.bytes().chain(b.bytes()).all(|c| c.is_ascii_digit())
                    {
                        return Err(461);
                    }
                    let p = ClientPorts {
                        rtp: a.parse().map_err(|_| 461u16)?,
                        rtcp: b.parse().map_err(|_| 461u16)?,
                    };
                    if !p.valid() {
                        return Err(461);
                    }
                    ports = Some(p);
                }
                _ => return Err(461),
            }
        }
        if !unicast {
            return Err(461);
        }
        ports.map(Self::Udp).ok_or(461)
    }
}
