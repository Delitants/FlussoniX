use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLoad {
    pub name: String,
    pub uplink: f64,
    pub cpu: f64,
    pub ram: f64,
    pub ready: bool,
    pub drain: bool,
    pub age_ms: u64,
    pub active: u64,
    pub limit: u64,
}
pub fn select(nodes: &[NodeLoad], expected_uplink: f64) -> Option<String> {
    nodes
        .iter()
        .filter(|n| {
            n.age_ms <= 10000
                && !n.drain
                && n.active < n.limit
                && n.uplink.is_finite()
                && n.cpu.is_finite()
                && n.ram.is_finite()
                && n.uplink + expected_uplink < 0.9
                && n.cpu < 0.9
                && n.ram < 0.95
        })
        .min_by(|a, b| score(a, expected_uplink).total_cmp(&score(b, expected_uplink)))
        .map(|n| n.name.clone())
}
fn score(n: &NodeLoad, expected: f64) -> f64 {
    (n.uplink + expected) * 0.65 + n.cpu * 0.25 + n.ram * 0.1 + if n.ready { 0.0 } else { 0.2 }
}

/// Keep LAN endpoint prefix/query and stream path separate from the selected media scheme.
pub fn source_input_url(endpoint: &str, name: &str, transport: &str) -> Result<String, String> {
    crate::config::valid_name(name)?;
    let mut url = url::Url::parse(endpoint).map_err(|_| "invalid private endpoint")?;
    let secure = match url.scheme() {
        "http" => false,
        "https" => true,
        _ => return Err("private endpoint must use HTTP(S)".into()),
    };
    if url.host_str().is_none() {
        return Err("private endpoint requires a host".into());
    }
    let scheme = match (transport, secure) {
        ("hls", false) => "hls",
        ("hls", true) => "hlss",
        ("m4s", false) => "m4s",
        ("m4s", true) => "m4ss",
        ("m4f", false) => "m4f",
        ("m4f", true) => "m4fs",
        _ => return Err("unsupported source transport".into()),
    };
    let name = name
        .split('/')
        .map(|s| {
            percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
        })
        .collect::<Vec<_>>()
        .join("/");
    let suffix = if transport == "hls" {
        "/index.m3u8"
    } else {
        ""
    };
    url.set_path(&format!(
        "{}/{name}{suffix}",
        url.path().trim_end_matches('/')
    ));
    url.set_fragment(None);
    Ok(format!(
        "{scheme}://{}",
        url.as_str().split_once("://").unwrap().1
    ))
}
