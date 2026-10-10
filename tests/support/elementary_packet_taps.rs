//! Test-only passive owned UDP boundary capture; never linked into the server.
use std::{io, os::fd::RawFd};

pub fn attach_filter(fd: RawFd, ports: &[u16]) -> io::Result<()> {
    if ports.is_empty() || ports.len() > 8 || ports.contains(&0) {
        return Err(io::Error::other("invalid owned packet tap scope"));
    }
    let instruction = |code, jt, jf, k| libc::sock_filter { code, jt, jf, k };
    let mut code = vec![
        instruction(0x30, 0, 0, 0), // IPv4, exactly 20-byte header.
        instruction(0x15, 0, 0, 0x45),
        instruction(0x30, 0, 0, 9),
        instruction(0x15, 0, 0, 17), // UDP only.
        instruction(0x20, 0, 0, 12),
        instruction(0x15, 0, 0, 0x7f000001),
        instruction(0x20, 0, 0, 16),
        instruction(0x15, 0, 0, 0x7f000001),
        instruction(0x28, 0, 0, 6),
        instruction(0x45, 0, 0, 0x3fff), // No fragments, including first fragment.
        instruction(0x80, 0, 0, 0),
        instruction(0x35, 0, 0, 28),
        instruction(0x28, 0, 0, 22), // Destination port, never source port.
    ];
    for port in ports {
        code.push(instruction(0x15, 0, 0, u32::from(*port)));
    }
    let reject = code.len();
    code.push(instruction(0x06, 0, 0, 0));
    let accept = code.len();
    code.push(instruction(0x06, 0, 0, 65535));
    for index in [1, 3, 5, 7, 11] {
        code[index].jf = (reject - index - 1) as u8;
    }
    code[9].jt = (reject - 10) as u8;
    for (index, entry) in code.iter_mut().enumerate().take(reject).skip(13) {
        entry.jt = (accept - index - 1) as u8;
    }
    let filter = libc::sock_fprog {
        len: code.len() as u16,
        filter: code.as_mut_ptr(),
    };
    // The kernel copies the initialized program while these buffers remain live.
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_FILTER,
            (&filter as *const libc::sock_fprog).cast(),
            std::mem::size_of_val(&filter) as libc::socklen_t,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub fn udp_packet(packet: &[u8]) -> Option<(u16, u16, &[u8])> {
    if packet.len() < 28
        || packet[0] != 0x45
        || packet[9] != 17
        || packet[12..16] != [127, 0, 0, 1]
        || packet[16..20] != [127, 0, 0, 1]
        || u16::from_be_bytes(packet[6..8].try_into().ok()?) & 0x3fff != 0
    {
        return None;
    }
    let total = usize::from(u16::from_be_bytes(packet[2..4].try_into().ok()?));
    let length = usize::from(u16::from_be_bytes(packet[24..26].try_into().ok()?));
    if total < 28 || total > packet.len() || length != total - 20 {
        return None;
    }
    Some((
        u16::from_be_bytes(packet[20..22].try_into().ok()?),
        u16::from_be_bytes(packet[22..24].try_into().ok()?),
        &packet[28..total],
    ))
}

use super::{
    decoder_readiness,
    elementary_diagnostics::{DatagramCapture, Evidence},
};
use serde_json::{Value, json};
use std::{
    net::UdpSocket,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub struct Tap<'a> {
    evidence: &'a Evidence,
    boundary: &'static str,
    stop: Arc<AtomicBool>,
    done: mpsc::Receiver<(Value, DatagramCapture)>,
    thread: Option<JoinHandle<()>>,
    outcome: Option<(Value, Option<DatagramCapture>)>,
}
impl<'a> Tap<'a> {
    pub fn public(
        reserved: &[UdpSocket],
        evidence: &'a Evidence,
        artifact: &Path,
        origin: Instant,
    ) -> io::Result<Self> {
        let ports = reserved
            .iter()
            .map(|socket| {
                let address = socket.local_addr()?;
                if address.ip() != std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST) {
                    return Err(io::Error::other(
                        "packet tap requires owned loopback sockets",
                    ));
                }
                Ok(address.port())
            })
            .collect::<io::Result<Vec<_>>>()?;
        Self::start(
            &ports,
            "source_to_public",
            evidence,
            artifact,
            origin,
            json!({"ownership":"fixture_reserved_public_ports"}),
        )
    }
    pub fn decoder(
        pid: u32,
        ports: &[u16],
        evidence: &'a Evidence,
        artifact: &Path,
        origin: Instant,
    ) -> io::Result<Self> {
        let start_ticks = super::elementary_diagnostics::identity(pid)?.0;
        if ports.is_empty()
            || !decoder_readiness::ready(pid, ports)?
            || super::elementary_diagnostics::identity(pid)?.0 != start_ticks
        {
            return Err(io::Error::other(
                "private tap requires the owned decoder sockets",
            ));
        }
        Self::start(
            ports,
            "relay_to_decoder",
            evidence,
            artifact,
            origin,
            json!({"ownership":"decoder_fd_inodes_at_activation", "pid":pid, "start_ticks":start_ticks}),
        )
    }
    fn start(
        ports: &[u16],
        boundary: &'static str,
        evidence: &'a Evidence,
        artifact: &Path,
        origin: Instant,
        ownership: Value,
    ) -> io::Result<Self> {
        let socket = match packet_socket(ports) {
            Ok(socket) => socket,
            Err(error) => {
                evidence.save_packet_tap(json!({"boundary":boundary,"incomplete":true,"observation":"unavailable","setup_os_error":error.raw_os_error()}),None);
                return Err(error);
            }
        };
        let activated = origin.elapsed().as_nanos() as u64;
        let stop = Arc::new(AtomicBool::new(false));
        let cancellation = stop.clone();
        let (finished, done) = mpsc::channel();
        let mut capture = DatagramCapture::new(artifact.join(format!("{boundary}.packets")));
        let ports = ports.to_vec();
        let thread = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            let mut records = 0u64;
            let mut malformed = 0u64;
            let mut capped = false;
            let mut receive_error = false;
            let mut duplicates_ignored = 0u64;
            let mut packet = [0u8; 2048];
            while !cancellation.load(Ordering::Relaxed) && Instant::now() < deadline {
                let mut poll = libc::pollfd {
                    fd: socket.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                let ready = unsafe { libc::poll(&mut poll, 1, 50) };
                if ready < 0 {
                    if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    receive_error = true;
                    break;
                }
                if ready == 0 {
                    continue;
                }
                if poll.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                    receive_error = true;
                    break;
                }
                let mut from: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
                let mut length = std::mem::size_of_val(&from) as libc::socklen_t;
                let n = unsafe {
                    libc::recvfrom(
                        socket.as_raw_fd(),
                        packet.as_mut_ptr().cast(),
                        packet.len(),
                        libc::MSG_TRUNC,
                        (&mut from as *mut libc::sockaddr_ll).cast(),
                        &mut length,
                    )
                };
                if n < 0 {
                    if matches!(
                        io::Error::last_os_error().kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) {
                        continue;
                    }
                    receive_error = true;
                    break;
                }
                // Loopback supplies outgoing and host copies. Keep only the host copy.
                if from.sll_pkttype == libc::PACKET_OUTGOING {
                    duplicates_ignored += 1;
                    continue;
                }
                if from.sll_pkttype != libc::PACKET_HOST || n as usize > packet.len() {
                    malformed += 1;
                    continue;
                }
                let Some((source, destination, payload)) = udp_packet(&packet[..n as usize]) else {
                    malformed += 1;
                    continue;
                };
                if !ports.contains(&destination) {
                    malformed += 1;
                    continue;
                }
                // Each existing BE32 frame contains BE64 observation time since the
                // fixture origin, BE16 source/destination ports, then unchanged UDP payload.
                let mut record = Vec::with_capacity(12 + payload.len());
                record.extend_from_slice(&(origin.elapsed().as_nanos() as u64).to_be_bytes());
                record.extend_from_slice(&source.to_be_bytes());
                record.extend_from_slice(&destination.to_be_bytes());
                record.extend_from_slice(payload);
                if capture.append(&record).is_err() {
                    capped = true;
                    break;
                }
                records += 1;
            }
            let coverage_end = origin.elapsed().as_nanos() as u64;
            let sampling_deadline = Instant::now() >= deadline;
            // Linux packet socket v1 counters; reading resets them, so read once at stop.
            #[repr(C)]
            struct PacketStats {
                packets: u32,
                drops: u32,
            }
            let mut stats = PacketStats {
                packets: 0,
                drops: 0,
            };
            let mut size = std::mem::size_of_val(&stats) as libc::socklen_t;
            let available = unsafe {
                libc::getsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_PACKET,
                    6,
                    (&mut stats as *mut PacketStats).cast(),
                    &mut size,
                )
            } == 0
                && size as usize == std::mem::size_of_val(&stats);
            // Packets still queued at shutdown are not silently called complete.
            let mut pending: libc::c_int = 0;
            let queue_available =
                unsafe { libc::ioctl(socket.as_raw_fd(), libc::FIONREAD, &mut pending) } == 0;
            let incomplete = !available
                || !queue_available
                || stats.drops > 0
                || pending > 0
                || malformed > 0
                || capped
                || sampling_deadline
                || receive_error;
            let summary = json!({"boundary":boundary,"ports":ports,"ownership":ownership,
                "observation":"kernel_loopback_host_copy","application_receipt_proven":false,
                "coverage_start_ns":activated,"coverage_end_ns":coverage_end,
                "private_startup_not_observed":boundary=="relay_to_decoder",
                "records":records,"outgoing_copies_ignored":duplicates_ignored,
                "kernel_packets":if available {Some(stats.packets)} else {None},
                "kernel_drops":if available {Some(stats.drops)} else {None},
                "queued_next_packet_bytes":if queue_available {Some(pending)} else {None},
                "malformed_or_truncated":malformed,"capped":capped,"sampling_deadline":sampling_deadline,
                "receive_error":receive_error,"incomplete":incomplete});
            drop(socket);
            let _ = finished.send((summary, capture));
        });
        Ok(Self {
            evidence,
            boundary,
            stop,
            done,
            thread: Some(thread),
            outcome: None,
        })
    }
}
fn packet_socket(ports: &[u16]) -> io::Result<OwnedFd> {
    // Protocol zero disables reception until the strict filter is installed.
    let raw = unsafe {
        libc::socket(
            libc::AF_PACKET,
            libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        )
    };
    if raw == -1 {
        return Err(io::Error::last_os_error());
    }
    // Own exactly the descriptor returned by socket, including on setup error.
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    attach_filter(socket.as_raw_fd(), ports)?;
    let lock: libc::c_int = 1;
    option(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_LOCK_FILTER,
        &lock,
    )?;
    let buffer: libc::c_int = 1024 * 1024;
    option(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_RCVBUF,
        &buffer,
    )?;
    let interface = unsafe { libc::if_nametoindex(c"lo".as_ptr()) };
    if interface == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut address: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
    address.sll_family = libc::AF_PACKET as u16;
    address.sll_protocol = (libc::ETH_P_IP as u16).to_be();
    address.sll_ifindex = interface as libc::c_int;
    if unsafe {
        libc::bind(
            socket.as_raw_fd(),
            (&address as *const libc::sockaddr_ll).cast(),
            std::mem::size_of_val(&address) as libc::socklen_t,
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    Ok(socket)
}
fn option<T>(fd: RawFd, level: libc::c_int, name: libc::c_int, value: &T) -> io::Result<()> {
    if unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (value as *const T).cast(),
            std::mem::size_of_val(value) as libc::socklen_t,
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
impl Tap<'_> {
    pub fn stop(&mut self) {
        if self.thread.is_none() {
            return;
        }
        self.stop.store(true, Ordering::Relaxed);
        let outcome = match self.done.recv_timeout(Duration::from_secs(2)) {
            Ok((summary, capture)) => {
                if self.thread.take().unwrap().join().is_err() {
                    (
                        json!({"boundary":self.boundary,"incomplete":true,"thread_failed":true}),
                        None,
                    )
                } else {
                    (summary, Some(capture))
                }
            }
            Err(_) => {
                self.thread.take();
                (
                    json!({"boundary":self.boundary,"incomplete":true,"completion_unavailable":true}),
                    None,
                )
            }
        };
        self.outcome = Some(outcome);
    }
}
impl Drop for Tap<'_> {
    fn drop(&mut self) {
        self.stop();
        let (summary, capture) = self.outcome.take().unwrap();
        self.evidence.save_packet_tap(summary, capture);
    }
}
