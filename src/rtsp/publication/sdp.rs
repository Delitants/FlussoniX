//! Accept codec metadata only; advertised network destinations never reach FFmpeg.
use crate::direct_rtp::{config::Settings, elementary::sdp::Session};
use std::collections::HashSet;
use url::Url;
pub(super) struct Description {
    pub media: Session,
    pub controls: Vec<Url>,
}
pub(super) fn parse(bytes: &[u8], aggregate: &Url) -> Result<Description, u16> {
    if bytes.len() > 16384 || bytes.contains(&0) {
        return Err(415);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| 415u16)?;
    let mut canonical = String::new();
    let mut controls: Vec<Option<Url>> = Vec::new();
    let mut session_control = false;
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if line.len() > 8192 || line.chars().any(|c| c.is_control()) {
            return Err(415);
        }
        let (kind, value) = line.split_once('=').ok_or(415u16)?;
        match kind {
            "m" => {
                let f: Vec<_> = value.split_whitespace().collect();
                if f.len() != 4 || controls.len() >= 8 || f[1].parse::<u16>().is_err() {
                    return Err(415);
                }
                canonical += &format!(
                    "m={} {} {} {}\r\n",
                    f[0],
                    20000 + 2 * controls.len(),
                    f[2],
                    f[3]
                );
                controls.push(None);
            }
            "c" => {
                let f: Vec<_> = value.split_whitespace().collect();
                if f.len() != 3 || f[0] != "IN" || !["IP4", "IP6"].contains(&f[1]) {
                    return Err(415);
                }
                canonical += "c=IN IP4 127.0.0.1\r\n";
            }
            "a" if value.starts_with("control:") => {
                let control = &value[8..];
                if let Some(slot) = controls.last_mut() {
                    if slot.is_some() {
                        return Err(415);
                    }
                    if control.is_empty()
                        || control.contains(['?', '#', '\\'])
                        || control.split('/').any(|p| p == ".." || p == ".")
                    {
                        return Err(415);
                    }
                    let mut base = aggregate.clone();
                    base.set_query(None);
                    base.set_path(&format!("{}/", base.path().trim_end_matches('/')));
                    let u = base.join(control).map_err(|_| 415u16)?;
                    if u.scheme() != aggregate.scheme()
                        || u.host_str() != aggregate.host_str()
                        || u.port() != aggregate.port()
                        || !u.username().is_empty()
                        || u.password().is_some()
                        || !u.path().starts_with(base.path())
                        || u.path() == base.path()
                    {
                        return Err(415);
                    }
                    let decoded = percent_encoding::percent_decode_str(u.path())
                        .decode_utf8()
                        .map_err(|_| 415u16)?;
                    if decoded
                        .split('/')
                        .any(|s| s == ".." || s == "." || s.contains(['\\', '?', '#']))
                    {
                        return Err(415);
                    }
                    *slot = Some(u);
                } else {
                    if session_control || control != "*" {
                        return Err(415);
                    }
                    session_control = true;
                }
            }
            "a" if value == "type:broadcast"
                || value == "range:npt=0-"
                || value == "range:npt=now-" => {}
            _ => {
                canonical += line;
                canonical += "\r\n";
            }
        }
    }
    let controls: Vec<Url> = controls.into_iter().collect::<Option<_>>().ok_or(415u16)?;
    let mut seen = HashSet::new();
    if controls.iter().any(|u| !seen.insert(u.path().to_string())) {
        return Err(415);
    }
    let settings=Settings::parse(&serde_json::json!({"url":"rtp://127.0.0.1:20000","flussonix_rtp":{"profile":"elementary"}})).map_err(|_|415u16)?;
    let media = Session::parse(canonical.as_bytes(), &settings).map_err(|_| 415u16)?;
    if media.tracks.iter().filter(|t| t.video).count() > 1 {
        return Err(415);
    }
    Ok(Description { media, controls })
}
