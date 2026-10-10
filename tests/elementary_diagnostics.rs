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
    let process = records
        .iter()
        .rev()
        .flat_map(|r| r["processes"].as_array().into_iter().flatten())
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
        // A small real pipe capacity ensures the bounded notification queue can fill it.
        assert_eq!(
            unsafe { libc::fcntl(input.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) },
            4096
        );
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
#[test]
fn assertion_unwinding_does_not_wait_for_a_blocked_evidence_writer_to_release() {
    blocked_terminal_writer(true);
}
#[test]
fn normal_teardown_timeout_is_reported_as_incomplete() {
    blocked_terminal_writer(false);
}
fn blocked_terminal_writer(unwind: bool) {
    use std::{ffi::CString, io::Read, os::unix::ffi::OsStrExt, sync::mpsc, time::Duration};
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let path = artifact.path().join("boundary-evidence.jsonl");
    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    let (complete, release) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut input = std::fs::File::open(path).unwrap();
        // A small real pipe capacity ensures the bounded notification queue can fill it.
        assert_eq!(
            unsafe { libc::fcntl(input.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) },
            4096
        );
        let unwind_completed = release.recv_timeout(Duration::from_secs(6)).is_ok();
        let mut bytes = Vec::new();
        input.read_to_end(&mut bytes).unwrap();
        (unwind_completed, bytes)
    });
    let result = std::panic::catch_unwind(|| {
        let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
        evidence.watch_process("native_test_process", std::process::id());
        for _ in 0..1000 {
            evidence.stage("blocked_terminal_writer");
        }
        if unwind {
            panic!("controlled fixture failure while evidence writer remains blocked");
        }
        drop(evidence);
    });
    let _ = complete.send(());
    let (completed, bytes) = reader.join().unwrap();
    assert!(result.is_err());
    assert!(completed, "Unwinding waited for writer release");
    let text = String::from_utf8(bytes).unwrap();
    let final_record: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(
        final_record["event"],
        if unwind { "unwinding" } else { "finished" }
    );
    assert_eq!(final_record["stage"], "blocked_terminal_writer");
    assert_eq!(final_record["shutdown_timed_out"], true);
}

#[test]
fn capped_evidence_retains_terminal_failure_stage_and_marks_incompleteness() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let result = std::panic::catch_unwind(|| {
        let evidence = Evidence::start_limited(source.path(), artifact.path(), 1024).unwrap();
        evidence.watch_process("native_test_process", std::process::id());
        for _ in 0..20 {
            evidence.stage("near_evidence_cap");
        }
        evidence.stage("failed_after_evidence_cap");
        panic!("controlled failure after evidence cap");
    });
    assert!(result.is_err());
    let records = entries(artifact.path());
    let final_record = records.last().unwrap();
    assert_eq!(final_record["event"], "unwinding");
    assert_eq!(final_record["stage"], "failed_after_evidence_cap");
    assert_eq!(final_record["incomplete"], true);
    assert!(records.iter().any(|r| r["event"] == "evidence_capped"));
    assert!(
        std::fs::metadata(artifact.path().join("boundary-evidence.jsonl"))
            .unwrap()
            .len()
            <= 1024
    );
}

#[test]
fn worker_log_retention_enforces_size_limit_and_private_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("worker.log"), "owned decoder error").unwrap();
    drop(Evidence::start(source.path(), artifact.path()).unwrap());
    assert_eq!(
        std::fs::read_to_string(artifact.path().join("worker.log")).unwrap(),
        "owned decoder error"
    );
    assert_eq!(
        std::fs::metadata(artifact.path().join("worker.log"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let second = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("worker.log"), vec![0; 1024 * 1024 + 1]).unwrap();
    drop(Evidence::start(source.path(), second.path()).unwrap());
    assert!(!second.path().join("worker.log").exists());
    assert_eq!(entries(second.path()).last().unwrap()["incomplete"], true);
}
#[test]
fn datagram_capture_preserves_framing_and_rejects_overflow_without_partial_data() {
    use diagnostics::DatagramCapture;
    use std::os::unix::fs::PermissionsExt;
    let artifact = tempfile::tempdir().unwrap();
    let path = artifact.path().join("packets.rtp");
    let mut capture = DatagramCapture::new(path.clone());
    capture.append(&[1, 2, 3]).unwrap();
    capture.append(&[4, 5]).unwrap();
    assert!(capture.append(&vec![0; 4 * 1024 * 1024]).is_err());
    assert!(
        !path.exists(),
        "Capture must not write files while delivery is active"
    );
    let source = tempfile::tempdir().unwrap();
    let evidence = Evidence::start(source.path(), artifact.path()).unwrap();
    evidence.stage("worker_stopped");
    evidence.save_captures(vec![capture]);
    drop(evidence);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        [0, 0, 0, 3, 1, 2, 3, 0, 0, 0, 2, 4, 5]
    );
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn sdp_retention_caps_sanitized_output_before_creating_file() {
    let source = tempfile::tempdir().unwrap();
    let artifact = tempfile::tempdir().unwrap();
    let input = "a\n".repeat(400_000);
    assert!(input.len() < 1024 * 1024);
    std::fs::write(source.path().join("input.sdp"), input).unwrap();
    drop(Evidence::start(source.path(), artifact.path()).unwrap());
    assert!(!artifact.path().join("input.sdp").exists());
    assert_eq!(entries(artifact.path()).last().unwrap()["incomplete"], true);
}

#[test]
fn reused_case_cannot_mix_prior_recordings_with_a_new_startup_failure() {
    let base = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("worker.ts"), b"prior worker recording").unwrap();
    std::fs::write(
        source.path().join("received.ts"),
        b"prior receiver recording",
    )
    .unwrap();
    let case = diagnostics::create_case_directory(base.path(), "owned-case").unwrap();
    let first = Evidence::start(source.path(), &case).unwrap();
    first.stage("qualified");
    drop(first);
    std::fs::write(case.join("caller-note.txt"), b"keep this caller file").unwrap();
    let prior: std::collections::BTreeMap<_, _> = std::fs::read_dir(&case)
        .unwrap()
        .map(|p| {
            let p = p.unwrap().path();
            (p.file_name().unwrap().to_owned(), std::fs::read(p).unwrap())
        })
        .collect();
    let empty_source = tempfile::tempdir().unwrap();
    let second = diagnostics::create_case_directory(base.path(), "owned-case")
        .and_then(|out| Evidence::start(empty_source.path(), &out));
    let error = match second {
        Err(error) => error,
        Ok(evidence) => {
            evidence.stage("failed_before_recording");
            drop(evidence);
            panic!("A second run was admitted into the previous case directory");
        }
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    let current: std::collections::BTreeMap<_, _> = std::fs::read_dir(&case)
        .unwrap()
        .map(|p| {
            let p = p.unwrap().path();
            (p.file_name().unwrap().to_owned(), std::fs::read(p).unwrap())
        })
        .collect();
    assert_eq!(
        current, prior,
        "Previous evidence and caller files must survive unchanged"
    );
}

#[test]
fn existing_empty_case_or_directory_alias_is_not_reclaimed() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let base = tempfile::tempdir().unwrap();
    let reserved = diagnostics::create_case_directory(base.path(), "reserved").unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(target.path().join("caller-note.txt"), b"untouched target").unwrap();
    std::fs::set_permissions(target.path(), std::fs::Permissions::from_mode(0o750)).unwrap();
    symlink(target.path(), base.path().join("alias")).unwrap();
    for name in ["reserved", "alias"] {
        let error = diagnostics::create_case_directory(base.path(), name).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    }
    assert!(reserved.read_dir().unwrap().next().is_none());
    assert!(base.path().join("alias").is_symlink());
    assert_eq!(
        std::fs::read(target.path().join("caller-note.txt")).unwrap(),
        b"untouched target"
    );
    assert_eq!(
        target.path().metadata().unwrap().permissions().mode() & 0o777,
        0o750
    );
}
