//! Passive readiness for the independent receiver in owned Linux media fixtures.
use std::{
    collections::HashSet,
    io::{self, Read},
};
pub fn ready(pid: u32, ports: &[u16]) -> io::Result<bool> {
    if pid == 0 || ports.is_empty() {
        return Ok(false);
    }
    let entries = match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    let mut owned = HashSet::new();
    for (n, entry) in entries.enumerate() {
        if n >= 4096 {
            return Err(io::Error::other("receiver descriptor limit exceeded"));
        }
        let link = match std::fs::read_link(entry?.path()) {
            Ok(link) => link,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        if let Some(inode) = link
            .to_str()
            .and_then(|s| s.strip_prefix("socket:["))
            .and_then(|s| s.strip_suffix(']'))
            .and_then(|s| s.parse::<u64>().ok())
        {
            owned.insert(inode);
        }
    }
    let mut table = String::new();
    std::fs::File::open("/proc/self/net/udp")?
        .take(1024 * 1024 + 1)
        .read_to_string(&mut table)?;
    if table.len() > 1024 * 1024 {
        return Err(io::Error::other("receiver socket table limit exceeded"));
    }
    let loopback = u32::from_ne_bytes([127, 0, 0, 1]);
    let mut found = HashSet::new();
    for line in table.lines().skip(1) {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        let Some((ip, port)) = fields.get(1).and_then(|s| s.split_once(':')) else {
            continue;
        };
        let (Ok(ip), Ok(port)) = (u32::from_str_radix(ip, 16), u16::from_str_radix(port, 16))
        else {
            continue;
        };
        let inode = fields.get(9).and_then(|s| s.parse::<u64>().ok());
        // The independent receiver allows FFmpeg's default wildcard bind.
        if (ip == 0 || ip == loopback) && inode.is_some_and(|n| owned.contains(&n)) {
            found.insert(port);
        }
    }
    Ok(ports.iter().all(|p| found.contains(p)))
}
