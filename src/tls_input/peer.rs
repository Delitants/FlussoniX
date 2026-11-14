//! Bind a loopback RTSP TCP connection to the decoder that owns its client socket.
//! TCP has no SO_PEERCRED. Match the established four-tuple to a socket inode in
//! that decoder's descriptor table; an arbitrary local connector is rejected.
use std::{
    collections::HashSet,
    io::{self, BufRead},
    net::SocketAddr,
};
use tokio::net::TcpStream;

const TABLE_LIMIT: u64 = 32 * 1024 * 1024;
const FD_LIMIT: usize = 4096;
const FAILURE: &str = "RTSPS decoder peer check unavailable";

pub(super) async fn belongs(pid: u32, socket: &TcpStream) -> Result<bool, &'static str> {
    if pid == 0 {
        return Ok(false);
    }
    let local = socket.local_addr().map_err(|_| FAILURE)?;
    let remote = socket.peer_addr().map_err(|_| FAILURE)?;
    if !local.ip().is_loopback() || !remote.ip().is_loopback() {
        return Ok(false);
    }
    tokio::task::spawn_blocking(move || inspect(pid, remote, local))
        .await
        .map_err(|_| FAILURE)?
}

fn inspect(pid: u32, client: SocketAddr, server: SocketAddr) -> Result<bool, &'static str> {
    let (SocketAddr::V4(client), SocketAddr::V4(server)) = (client, server) else {
        return Ok(false);
    };
    let entries = match std::fs::read_dir(format!("/proc/{pid}/fd")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(FAILURE),
    };
    let mut owned = HashSet::new();
    for (index, entry) in entries.enumerate() {
        if index >= FD_LIMIT {
            return Err(FAILURE);
        }
        let entry = entry.map_err(|_| FAILURE)?;
        let link = match std::fs::read_link(entry.path()) {
            Ok(link) => link,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(FAILURE),
        };
        if let Some(inode) = link
            .to_str()
            .and_then(|s| s.strip_prefix("socket:["))
            .and_then(|s| s.strip_suffix(']'))
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|inode| *inode != 0)
        {
            owned.insert(inode);
        }
    }
    if owned.is_empty() {
        return Ok(false);
    }
    let file = std::fs::File::open("/proc/self/net/tcp").map_err(|_| FAILURE)?;
    let mut table = io::BufReader::with_capacity(4096, file);
    let client = endpoint(&client);
    let server = endpoint(&server);
    let mut line = String::new();
    let mut consumed = 0u64;
    let mut header = true;
    loop {
        line.clear();
        let count = table.read_line(&mut line).map_err(|_| FAILURE)?;
        if count == 0 {
            break;
        }
        consumed = consumed.saturating_add(count as u64);
        if count > 1024 || consumed > TABLE_LIMIT {
            return Err(FAILURE);
        }
        if header {
            header = false;
            continue;
        }
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        if fields.len() < 10 || fields[2] != server || fields[1] != client || fields[3] != "01" {
            continue;
        }
        if fields[9]
            .parse::<u64>()
            .is_ok_and(|inode| owned.contains(&inode))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn endpoint(addr: &std::net::SocketAddrV4) -> String {
    format!(
        "{:08X}:{:04X}",
        u32::from_ne_bytes(addr.ip().octets()),
        addr.port()
    )
}
