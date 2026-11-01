#![cfg(target_os = "linux")]
#[allow(dead_code)]
#[path = "support/elementary_diagnostics.rs"]
mod elementary_diagnostics;
#[allow(dead_code)]
#[path = "support/elementary_packet_taps.rs"]
mod taps;
use std::{
    io,
    os::{fd::AsRawFd, unix::net::UnixDatagram},
    time::Duration,
};
fn packet(source: u16, destination: u16) -> Vec<u8> {
    let mut p = vec![0u8; 40];
    p[0] = 0x45;
    p[2..4].copy_from_slice(&40u16.to_be_bytes());
    p[9] = 17;
    p[12..16].copy_from_slice(&[127, 0, 0, 1]);
    p[16..20].copy_from_slice(&[127, 0, 0, 1]);
    p[20..22].copy_from_slice(&source.to_be_bytes());
    p[22..24].copy_from_slice(&destination.to_be_bytes());
    p[24..26].copy_from_slice(&20u16.to_be_bytes());
    p[28] = 0x80;
    p[29] = 96;
    p[30..32].copy_from_slice(&65535u16.to_be_bytes());
    p[32..36].copy_from_slice(&90000u32.to_be_bytes());
    p[36..40].copy_from_slice(&42u32.to_be_bytes());
    p
}
fn delivered(packet: &[u8], ports: &[u16]) -> io::Result<bool> {
    let (tx, rx) = UnixDatagram::pair()?;
    rx.set_read_timeout(Some(Duration::from_millis(30)))?;
    taps::attach_filter(rx.as_raw_fd(), ports)?;
    tx.send(packet)?;
    match rx.recv(&mut [0; 2048]) {
        Ok(_) => Ok(true),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            Ok(false)
        }
        Err(e) => Err(e),
    }
}
fn pair_delivered(packet: &[u8], sources: &[u16], destinations: &[u16]) -> io::Result<bool> {
    let (tx, rx) = UnixDatagram::pair()?;
    rx.set_read_timeout(Some(Duration::from_millis(30)))?;
    taps::attach_pair_filter(rx.as_raw_fd(), sources, destinations)?;
    tx.send(packet)?;
    match rx.recv(&mut [0; 2048]) {
        Ok(_) => Ok(true),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            Ok(false)
        }
        Err(e) => Err(e),
    }
}
#[test]
fn pair_filter_requires_owned_source_and_destination() {
    assert!(pair_delivered(&packet(41000, 42000), &[41000, 41002], &[42000, 42002]).unwrap());
    assert!(!pair_delivered(&packet(41001, 42000), &[41000], &[42000]).unwrap());
    assert!(!pair_delivered(&packet(41000, 42001), &[41000], &[42000]).unwrap());
    let mut nonloopback = packet(41000, 42000);
    nonloopback[12] = 192;
    assert!(!pair_delivered(&nonloopback, &[41000], &[42000]).unwrap());
    let mut fragment = packet(41000, 42000);
    fragment[6..8].copy_from_slice(&0x2000u16.to_be_bytes());
    assert!(!pair_delivered(&fragment, &[41000], &[42000]).unwrap());
    let (_, rx) = UnixDatagram::pair().unwrap();
    for invalid in [vec![], vec![0], vec![41000, 41000], vec![41000; 9]] {
        assert!(taps::attach_pair_filter(rx.as_raw_fd(), &invalid, &[42000]).is_err());
        assert!(taps::attach_pair_filter(rx.as_raw_fd(), &[41000], &invalid).is_err());
    }
}
#[test]
fn sender_source_snapshot_uses_local_inodes_and_rejects_missing_or_foreign_sockets() {
    let source = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let port = source.local_addr().unwrap().port();
    let (_, owned) = elementary_diagnostics::udp_source_ownership(std::process::id(), 1).unwrap();
    assert_eq!(owned.len(), 1);
    assert!(owned.contains_key(&port));
    assert!(taps::Owner::sender(std::process::id(), 1).is_ok());
    assert!(taps::Owner::sender(std::process::id(), 2).is_err());
    let mut child = std::process::Command::new("sleep")
        .arg("10")
        .spawn()
        .unwrap();
    assert!(taps::Owner::sender(child.id(), 1).is_err());
    child.kill().unwrap();
    child.wait().unwrap();
    drop(source);
    assert!(taps::Owner::sender(std::process::id(), 1).is_err());
}
#[test]
fn kernel_filter_admits_only_owned_loopback_udp_destinations() {
    assert!(delivered(&packet(41000, 42000), &[42000, 42002]).unwrap());
    assert!(delivered(&packet(41000, 42002), &[42000, 42002]).unwrap());
    assert!(
        !delivered(&packet(42000, 42004), &[42000, 42002]).unwrap(),
        "An owned source port must not admit an unrelated destination"
    );
    for offset in [12, 16] {
        let mut p = packet(41000, 42000);
        p[offset] = 192;
        assert!(
            !delivered(&p, &[42000]).unwrap(),
            "Non-loopback traffic must be rejected in kernel"
        );
    }
    let mut p = packet(41000, 42000);
    p[9] = 6;
    assert!(!delivered(&p, &[42000]).unwrap());
}
#[test]
fn kernel_filter_rejects_fragments_short_packets_and_ip_options() {
    for fragment in [0x2000u16, 1, 0x3fff] {
        let mut p = packet(41000, 42000);
        p[6..8].copy_from_slice(&fragment.to_be_bytes());
        assert!(!delivered(&p, &[42000]).unwrap());
    }
    for first in [0x44, 0x46, 0x65] {
        let mut p = packet(41000, 42000);
        p[0] = first;
        assert!(!delivered(&p, &[42000]).unwrap());
    }
    assert!(!delivered(&packet(41000, 42000)[..23], &[42000]).unwrap());
}
#[test]
fn empty_zero_or_excessive_filter_scope_is_rejected() {
    let (_, rx) = UnixDatagram::pair().unwrap();
    for ports in [vec![], vec![0], vec![42000; 9]] {
        assert!(taps::attach_filter(rx.as_raw_fd(), &ports).is_err());
    }
}
#[test]
fn udp_parser_preserves_rtp_headers_and_rejects_truncation() {
    let p = packet(41000, 42000);
    let (source, dest, rtp) = taps::udp_packet(&p).expect("Complete owned packet");
    assert_eq!((source, dest), (41000, 42000));
    assert_eq!(rtp, &p[28..]);
    for n in 0..p.len() {
        assert!(taps::udp_packet(&p[..n]).is_none());
    }
    let mut padded = p.clone();
    padded.extend([1, 2, 3]);
    assert_eq!(taps::udp_packet(&padded).unwrap().2, rtp);
    let mut invalid = p.clone();
    invalid[24..26].copy_from_slice(&21u16.to_be_bytes());
    assert!(taps::udp_packet(&invalid).is_none());
}

#[test]
fn private_capture_rejects_sockets_owned_by_another_process_before_activation() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let evidence = elementary_diagnostics::Evidence::start(source.path(), artifact.path()).unwrap();
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut child = std::process::Command::new("sleep")
        .arg("10")
        .spawn()
        .unwrap();
    let result = taps::Tap::decoder(
        child.id(),
        &[socket.local_addr().unwrap().port()],
        &evidence,
        artifact.path(),
        std::time::Instant::now(),
    );
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        result.is_err(),
        "Foreign port ownership must fail before capture activation"
    );
    drop(result);
    drop(evidence);
    assert_unavailable(artifact.path());
}
#[test]
fn public_capture_rejects_a_wildcard_reservation_before_activation() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let evidence = elementary_diagnostics::Evidence::start(source.path(), artifact.path()).unwrap();
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    assert!(
        taps::Tap::public(
            &[socket],
            &evidence,
            artifact.path(),
            std::time::Instant::now()
        )
        .is_err()
    );
    drop(evidence);
    assert_unavailable(artifact.path());
}

fn assert_unavailable(path: &std::path::Path) {
    let entries: Vec<serde_json::Value> =
        std::fs::read_to_string(path.join("boundary-evidence.jsonl"))
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
    assert!(
        entries
            .iter()
            .flat_map(|v| v["packet_taps"].as_array().into_iter().flatten())
            .any(|v| v["observation"] == "unavailable" && v["incomplete"] == true),
        "Unavailable tap must be explicit"
    );
    assert_eq!(entries.last().unwrap()["incomplete"], true);
}
#[test]
fn exited_owner_rebound_port_discards_ambiguous_datagrams() {
    use std::{
        io::BufRead,
        process::{Command, Stdio},
    };
    let mut child=Command::new("python3").args(["-u","-c","import socket,sys; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); sys.stdin.read(1)"]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let port = line.trim().parse::<u16>().unwrap();
    let witness = taps::Owner::snapshot(child.id(), &[port]);
    child.kill().unwrap();
    child.wait().unwrap();
    let witness = witness.unwrap();
    let rebound = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
    discard_later_owner(witness, rebound);
}
#[test]
fn same_process_rebound_socket_inode_discards_ambiguous_datagrams() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    let witness = taps::Owner::snapshot(std::process::id(), &[port]).unwrap();
    drop(socket);
    let rebound = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
    discard_later_owner(witness, rebound);
}
fn discard_later_owner(witness: taps::Owner, rebound: std::net::UdpSocket) {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let evidence = elementary_diagnostics::Evidence::start(source.path(), artifact.path()).unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let marker = b"later-owned-synthetic-marker-must-not-be-retained";
    sender
        .send_to(marker, rebound.local_addr().unwrap())
        .unwrap();
    rebound
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut bytes = [0u8; 128];
    let n = rebound.recv(&mut bytes).unwrap();
    assert_eq!(&bytes[..n], marker);
    let path = artifact.path().join("ambiguous.packets");
    let mut capture = elementary_diagnostics::DatagramCapture::new(path.clone());
    capture.append(&bytes[..n]).unwrap();
    let (summary, capture) = taps::enforce_owner(
        &witness,
        serde_json::json!({"boundary":"source_to_public","incomplete":false}),
        capture,
    );
    evidence.save_packet_tap(summary, capture);
    drop(evidence);
    assert!(
        !path.exists(),
        "The whole ambiguous trace must be discarded after ownership changes"
    );
    assert_unavailable(artifact.path());
}

fn handoff_fixture() -> (
    std::process::Child,
    std::net::UdpSocket,
    taps::HandoffWitness,
) {
    use std::io::BufRead;
    let mut sender = std::process::Command::new("python3")
        .args(["-u", "-c", "import socket,sys; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('0.0.0.0',0)); print(s.getsockname()[1]); sys.stdin.read(1)"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn().unwrap();
    let mut line = String::new();
    std::io::BufReader::new(sender.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let sender_port = line.trim().parse::<u16>().unwrap();
    assert!(
        elementary_diagnostics::udp_source_ownership(sender.id(), 1)
            .unwrap()
            .1
            .contains_key(&sender_port)
    );
    let reserved = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let destination_port = reserved.local_addr().unwrap().port();
    let witness = taps::HandoffWitness::new(
        taps::Owner::snapshot(std::process::id(), &[destination_port]).unwrap(),
        taps::Owner::sender(sender.id(), 1).unwrap(),
    );
    (sender, reserved, witness)
}
fn handoff_retention(witness: &taps::HandoffWitness) -> (serde_json::Value, bool) {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let evidence = elementary_diagnostics::Evidence::start(source.path(), artifact.path()).unwrap();
    let path = artifact.path().join("handoff.packets");
    let mut capture = elementary_diagnostics::DatagramCapture::new(path.clone());
    capture.append(b"owned-marker").unwrap();
    let (summary, capture) = taps::enforce_handoff(
        witness,
        serde_json::json!({"boundary":"source_to_public","records":1,"coverage_start_ns":0,"coverage_end_ns":1000000000000u64,"incomplete":false}),
        capture,
    );
    evidence.save_packet_tap(summary.clone(), capture);
    drop(evidence);
    (summary, path.exists())
}
#[test]
fn handoff_missing_markers_and_release_races_discard_raw() {
    let (mut sender, reserved, mut witness) = handoff_fixture();
    let origin = std::time::Instant::now();
    let (summary, retained) = handoff_retention(&witness);
    assert!(!retained);
    assert_eq!(summary["incomplete"], true);
    witness.release(origin).unwrap();
    let (summary, retained) = handoff_retention(&witness);
    assert!(!retained);
    assert_eq!(summary["incomplete"], true);
    assert!(witness.bind_native(std::process::id(), origin).is_err());
    sender.kill().unwrap();
    sender.wait().unwrap();
    drop(reserved);
}
#[test]
fn handoff_same_port_rebind_retains_only_stable_sender_and_native() {
    let (mut sender, reserved, mut witness) = handoff_fixture();
    let port = reserved.local_addr().unwrap().port();
    let origin = std::time::Instant::now();
    witness.release(origin).unwrap();
    drop(reserved);
    let native = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
    witness.bind_native(std::process::id(), origin).unwrap();
    let (summary, retained) = handoff_retention(&witness);
    assert!(retained);
    assert_eq!(summary["handoff_captured"], true);
    drop(native);
    let rebound = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
    let (summary, retained) = handoff_retention(&witness);
    assert!(!retained);
    assert_eq!(summary["observation"], "unavailable");
    drop(rebound);
    sender.kill().unwrap();
    sender.wait().unwrap();
}
#[test]
fn handoff_sender_exit_and_stop_boundary_race_discard_raw() {
    let (mut sender, reserved, mut witness) = handoff_fixture();
    let port = reserved.local_addr().unwrap().port();
    let origin = std::time::Instant::now();
    witness.release(origin).unwrap();
    drop(reserved);
    let native = std::net::UdpSocket::bind(("127.0.0.1", port)).unwrap();
    witness.bind_native(std::process::id(), origin).unwrap();
    sender.kill().unwrap();
    sender.wait().unwrap();
    let (summary, retained) = handoff_retention(&witness);
    assert!(!retained);
    assert_eq!(summary["incomplete"], true);
    drop(native);
}
#[test]
fn handoff_rejects_foreign_native_owner() {
    use std::io::BufRead;
    let (mut sender, reserved, mut witness) = handoff_fixture();
    let port = reserved.local_addr().unwrap().port();
    let origin = std::time::Instant::now();
    witness.release(origin).unwrap();
    drop(reserved);
    let script = format!(
        "import socket,sys; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('127.0.0.1',{port})); print('ready'); sys.stdin.read(1)"
    );
    let mut foreign = std::process::Command::new("python3")
        .args(["-u", "-c", &script])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    std::io::BufReader::new(foreign.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert_eq!(line.trim(), "ready");
    let result = witness.bind_native(foreign.id(), origin);
    foreign.kill().unwrap();
    foreign.wait().unwrap();
    sender.kill().unwrap();
    sender.wait().unwrap();
    assert!(
        result.is_err(),
        "only the fixture process may own native input"
    );
    let (summary, retained) = handoff_retention(&witness);
    assert!(!retained);
    assert_eq!(summary["incomplete"], true);
}

#[test]
#[ignore = "requires CAP_NET_RAW and uses an owned loopback sender"]
fn public_handoff_keeps_one_capture_across_rebind() {
    use std::io::{BufRead, Write};
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let evidence = elementary_diagnostics::Evidence::start(source.path(), artifact.path()).unwrap();
    taps::check_capability(&evidence).unwrap();
    let reserved = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = reserved.local_addr().unwrap().port();
    let mut sender = std::process::Command::new("python3")
        .args(["-u", "-c", "import socket,sys; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('0.0.0.0',0)); print(s.getsockname()[1]); [(s.sendto(bytes.fromhex(line.split()[1]),('127.0.0.1',int(line.split()[0])))) for line in sys.stdin]"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn().unwrap();
    let mut line = String::new();
    std::io::BufReader::new(sender.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    line.trim().parse::<u16>().unwrap();
    let result = (|| -> io::Result<()> {
        let origin = std::time::Instant::now();
        let mut tap = taps::Tap::public_handoff(
            &[reserved.try_clone()?],
            sender.id(),
            1,
            &evidence,
            artifact.path(),
            origin,
        )?;
        writeln!(sender.stdin.as_mut().unwrap(), "{port} 505245")?;
        std::thread::sleep(Duration::from_millis(100));
        tap.release_reservation(origin)?;
        drop(reserved);
        let native = std::net::UdpSocket::bind(("127.0.0.1", port))?;
        tap.bind_native(std::process::id(), origin)?;
        writeln!(sender.stdin.as_mut().unwrap(), "{port} 504f5354")?;
        std::thread::sleep(Duration::from_millis(100));
        tap.stop();
        drop(tap);
        drop(native);
        Ok(())
    })();
    sender.kill().unwrap();
    sender.wait().unwrap();
    drop(evidence);
    result.unwrap();
    let bytes = std::fs::read(artifact.path().join("source_to_public.packets")).unwrap();
    let pre = bytes
        .windows(3)
        .position(|window| window == b"PRE")
        .unwrap();
    let post = bytes
        .windows(4)
        .position(|window| window == b"POST")
        .unwrap();
    assert!(
        pre < post,
        "one tap must retain packets on both sides of handoff"
    );
    let summaries =
        std::fs::read_to_string(artifact.path().join("boundary-evidence.jsonl")).unwrap();
    assert!(summaries.lines().any(|line| {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        entry["packet_taps"].as_array().is_some_and(|taps| {
            taps.iter()
                .any(|tap| tap["handoff_captured"] == true && tap["incomplete"] == false)
        })
    }));
}
