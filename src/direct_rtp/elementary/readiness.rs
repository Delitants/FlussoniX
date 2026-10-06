//! Non-invasive readiness for the owned Linux decoder's UDP sockets.
//! Binding a port to probe it can race the decoder and make its bind fail.
use std::io::Read;
const LIMIT: usize = 1024 * 1024;
pub(crate) fn bound(ports: &[u16]) -> Result<bool, &'static str> {
    Ok(contains(&table()?, ports))
}
// Port occupancy can briefly expose released reservation sockets. RECORD must
// wait for these exact loopback ports to belong to the actual decoder process.
pub(crate) fn owned(ports: &[u16], pid: u32, excluded: &[u64]) -> Result<bool, &'static str> {
    if pid == 0 || ports.is_empty() {
        return Ok(false);
    }
    let entries = match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("Private RTP decoder descriptors unavailable"),
    };
    let mut inodes = std::collections::HashSet::new();
    for (n, entry) in entries.enumerate() {
        if n >= 4096 {
            return Err("Private RTP decoder descriptors exceed limit");
        }
        let entry = entry.map_err(|_| "Private RTP decoder descriptors unavailable")?;
        let link = match std::fs::read_link(entry.path()) {
            Ok(link) => link,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("Private RTP decoder descriptors unavailable"),
        };
        if let Some(inode) = link
            .to_str()
            .and_then(|s| s.strip_prefix("socket:["))
            .and_then(|s| s.strip_suffix(']'))
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n != 0 && !excluded.contains(n))
        {
            inodes.insert(inode);
        }
    }
    let table = table()?;
    let loopback = u32::from_ne_bytes([127, 0, 0, 1]);
    let mut found = std::collections::HashSet::new();
    for line in table.lines().skip(1) {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        let Some((ip, port)) = fields.get(1).and_then(|s| s.split_once(':')) else {
            continue;
        };
        let (Ok(ip), Ok(port)) = (u32::from_str_radix(ip, 16), u16::from_str_radix(port, 16))
        else {
            continue;
        };
        let inode = fields.get(9).and_then(|s| s.parse::<u64>().ok());
        if ip == loopback && inode.is_some_and(|n| inodes.contains(&n)) {
            found.insert(port);
        }
    }
    Ok(ports.iter().all(|p| found.contains(p)))
}
pub(crate) fn socket_inode(fd: std::os::fd::RawFd) -> Result<u64, &'static str> {
    std::fs::read_link(format!("/proc/self/fd/{fd}"))
        .ok()
        .and_then(|p| {
            p.to_str()
                .and_then(|s| s.strip_prefix("socket:["))
                .and_then(|s| s.strip_suffix(']'))
                .and_then(|s| s.parse().ok())
        })
        .ok_or("Private RTP reservation descriptor unavailable")
}
fn table() -> Result<String, &'static str> {
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
    Ok(text.to_owned())
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
    fn publication_readiness_requires_the_actual_decoder_socket_owner() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        let inode = socket_inode(std::os::fd::AsRawFd::as_raw_fd(&socket)).unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        let premature = owned(&[port], child.id(), &[inode]).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            !premature,
            "A foreign socket must not make the decoder ready"
        );
        assert!(owned(&[port], std::process::id(), &[]).unwrap());
        drop(socket);
        assert!(!owned(&[port], std::process::id(), &[]).unwrap());
    }
    #[test]
    fn publication_readiness_rejects_handoff_reservation_inodes() {
        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        let inode = socket_inode(std::os::fd::AsRawFd::as_raw_fd(&reservation)).unwrap();
        assert!(
            !owned(&[port], std::process::id(), &[inode]).unwrap(),
            "An inherited handoff reservation is not a decoder socket"
        );
        drop(reservation);
        let decoder = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
        assert!(
            owned(&[port], std::process::id(), &[inode]).unwrap(),
            "A new decoder socket on the same port must qualify"
        );
        drop(decoder);
    }
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
