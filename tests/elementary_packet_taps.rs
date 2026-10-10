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
