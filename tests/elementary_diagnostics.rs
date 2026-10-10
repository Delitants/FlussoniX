//! Passive fixture evidence must distinguish actual kernel drops from missing observations.
#[path = "support/elementary_diagnostics.rs"]
mod diagnostics;
use diagnostics::Evidence;
use serde_json::Value;
use std::{net::UdpSocket, os::fd::AsRawFd};
fn entries(path: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(path.join("boundary-evidence.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
#[test]
fn owned_udp_overflow_is_retained_with_queue_and_kernel_drop_counts() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let size: libc::c_int = 4096;
    // One initialized integer is passed to the socket option syscall.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        },
        0
    );
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
    evidence.watch_process("native_test_process", std::process::id());
    for _ in 0..256 {
        sender
            .send_to(&[0; 512], socket.local_addr().unwrap())
            .unwrap();
    }
    evidence.stage("overflow_observed");
    drop(evidence);
    let records = entries(artifact.path());
    let port = socket.local_addr().unwrap().port();
    let rows: Vec<_> = records
        .iter()
        .flat_map(|r| r["processes"].as_array().into_iter().flatten())
        .filter(|p| p["role"] == "native_test_process")
        .flat_map(|p| p["udp"].as_array().into_iter().flatten())
        .filter(|s| s["local_port"] == port)
        .collect();
    assert!(
        rows.iter()
            .any(|s| s["drops"].as_u64().unwrap() > 0 && s["rx_queue_bytes"].as_u64().unwrap() > 0),
        "{records:?}"
    );
    assert!(records.iter().any(|r| r["event"] == "finished"));
}
#[test]
fn unavailable_process_is_reported_instead_of_a_zero_drop_measurement() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
    evidence.watch_process("missing", u32::MAX);
    evidence.stage("observe");
    drop(evidence);
    let records = entries(artifact.path());
    let process = records
        .iter()
        .flat_map(|r| r["processes"].as_array().into_iter().flatten())
        .find(|p| p["role"] == "missing")
        .unwrap();
    assert_eq!(process["observation"], "unavailable");
    assert!(process.get("udp").is_none());
}
#[test]
fn panic_retains_failure_stage_logs_and_sanitized_sdp_without_key_material() {
    use std::os::unix::fs::PermissionsExt;
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("sender.log"), "fixture sender failure").unwrap();
    std::fs::write(
        source.path().join("input.sdp"),
        "v=0\r\na=crypto:1 secret\r\nm=video 5000 RTP/AVP 96\r\n",
    )
    .unwrap();
    std::fs::write(source.path().join("owned.key"), "never-copy-this").unwrap();
    let result = std::panic::catch_unwind(|| {
        let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
        evidence.stage("sender_sdp_startup");
        panic!("controlled fixture failure");
    });
    assert!(result.is_err());
    let records = entries(artifact.path());
    assert!(
        records
            .iter()
            .any(|r| r["event"] == "unwinding" && r["stage"] == "sender_sdp_startup")
    );
    assert_eq!(
        std::fs::read_to_string(artifact.path().join("sender.log")).unwrap(),
        "fixture sender failure"
    );
    assert_eq!(
        std::fs::read_to_string(artifact.path().join("input.sdp")).unwrap(),
        "v=0\r\nm=video 5000 RTP/AVP 96\r\n"
    );
    assert!(!artifact.path().join("owned.key").exists());
    assert_eq!(
        std::fs::metadata(artifact.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(artifact.path().join("boundary-evidence.jsonl"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
#[test]
fn unrelated_process_sockets_are_not_attributed_to_watched_child() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut child = std::process::Command::new("sleep")
        .arg("5")
        .spawn()
        .unwrap();
    let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
    evidence.watch_process("child", child.id());
    evidence.stage("owned_sockets_only");
    drop(evidence);
    child.kill().unwrap();
    child.wait().unwrap();
    let records = entries(artifact.path());
    let observations: Vec<_> = records
        .iter()
        .flat_map(|r| r["processes"].as_array().into_iter().flatten())
        .filter(|p| p["role"] == "child" && p["observation"] == "available")
        .collect();
    assert!(!observations.is_empty());
    assert!(
        observations
            .iter()
            .all(|p| p["udp"].as_array().unwrap().is_empty()),
        "{records:?}"
    );
    drop(socket);
}

#[test]
fn exited_process_loses_observation_instead_of_reusing_its_last_socket_state() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new("sleep")
        .arg("5")
        .spawn()
        .unwrap();
    let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
    evidence.watch_process("child", child.id());
    child.kill().unwrap();
    child.wait().unwrap();
    evidence.stage("child_exited");
    drop(evidence);
    let records = entries(artifact.path());
    let final_record = records.last().unwrap();
    let process = final_record["processes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["role"] == "child")
        .unwrap();
    assert_eq!(process["observation"], "unavailable");
    assert!(process.get("udp").is_none());
    assert_eq!(final_record["stage"], "child_exited");
}
#[test]
fn blocked_evidence_writer_does_not_block_fixture_stage_notifications() {
    use std::{ffi::CString, io::Read, os::unix::ffi::OsStrExt, sync::mpsc, time::Duration};
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let path = artifact.path().join("boundary-evidence.jsonl");
    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    // The owned temporary FIFO provides real backpressure on evidence writes.
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    let (complete, release) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut input = std::fs::File::open(path).unwrap();
        let stages_completed = release.recv_timeout(Duration::from_secs(2)).is_ok();
        let mut bytes = Vec::new();
        input.read_to_end(&mut bytes).unwrap();
        (stages_completed, bytes)
    });
    let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
    evidence.watch_process("native_test_process", std::process::id());
    for _ in 0..1000 {
        evidence.stage("backpressured_writer");
    }
    let _ = complete.send(());
    drop(evidence);
    let (completed, bytes) = reader.join().unwrap();
    assert!(
        completed,
        "Fixture stages waited for the blocked evidence writer"
    );
    assert!(!bytes.is_empty());
    let text = String::from_utf8(bytes).unwrap();
    let final_record: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(final_record["event"], "finished");
    assert_eq!(final_record["stage"], "backpressured_writer");
    assert!(final_record["dropped_commands"].as_u64().unwrap() > 0);
}
