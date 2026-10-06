//! Non-invasive readiness for the owned Linux decoder's UDP sockets.
//! Binding a port to probe it can race the decoder and make its bind fail.
use std::io::Read;
const LIMIT: usize = 1024 * 1024;
pub(crate) fn bound(ports: &[u16]) -> Result<bool, &'static str> {
    let file = std::fs::File::open("/proc/self/net/udp")
        .map_err(|_| "Private RTP decoder socket table unavailable")?;
    let mut bytes = Vec::new();
    file.take((LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "Private RTP decoder socket table unavailable")?;
    if bytes.len() > LIMIT {
        return Err("Private RTP decoder socket table exceeds limit");
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| "Private RTP decoder socket table invalid")?;
    Ok(contains(text, ports))
}
fn contains(text: &str, ports: &[u16]) -> bool {
    let loopback = u32::from_ne_bytes([127, 0, 0, 1]);
    let mut found = std::collections::HashSet::new();
    for line in text.lines().skip(1) {
        let Some(local) = line.split_ascii_whitespace().nth(1) else {
            continue;
        };
        let Some((ip, port)) = local.split_once(':') else {
            continue;
        };
        let (Ok(ip), Ok(port)) = (u32::from_str_radix(ip, 16), u16::from_str_radix(port, 16))
        else {
            continue;
        };
        if ip == loopback {
            found.insert(port);
        }
    }
    ports.iter().all(|port| found.contains(port))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_readiness_observes_pairs_without_claiming_free_ports() {
        let first = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = first.local_addr().unwrap().port();
        assert!(bound(&[p]).unwrap());
        drop(first);
        assert!(!bound(&[p]).unwrap());
        // The decoder can bind immediately after every readiness observation;
        // the observer never holds a probe socket while it starts.
        for _ in 0..128 {
            assert!(!bound(&[p]).unwrap());
            let decoder = std::net::UdpSocket::bind(("127.0.0.1", p)).unwrap();
            assert!(bound(&[p]).unwrap());
            drop(decoder);
        }
    }
    #[test]
    fn readiness_requires_every_loopback_port_and_rejects_wildcards() {
        let ip = u32::from_ne_bytes([127, 0, 0, 1]);
        let text = format!(
            "header\n 1: {ip:08X}:1388 0 0\n 2: 00000000:1389 0 0\n 3: 0102A8C0:138A 0 0\n"
        );
        assert!(contains(&text, &[5000]));
        assert!(!contains(&text, &[5000, 5001]));
        assert!(!contains(&text, &[5000, 5002]));
        assert!(!contains("header\n garbage\n", &[5000]));
    }
}
