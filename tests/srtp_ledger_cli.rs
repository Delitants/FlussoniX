use std::{fs, os::unix::fs::PermissionsExt, process::Command};

#[test]
fn explicit_ledger_initialization_exits_without_daemon_credentials_or_listener() {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.path().join("receiver-history");
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_flussonix"))
            .arg("--init-srtp-receiver-ledger")
            .arg(&path)
            .env_remove("FLUSSONIX_ADMIN_PASSWORD")
            .env_remove("FLUSSONIX_PEER_KEY")
            .output()
            .unwrap()
    };
    let first = run();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(path.exists());
    assert!(dir.path().join("receiver-history.lock").exists());
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let prior = fs::read(&path).unwrap();
    let second = run();
    assert!(!second.status.success());
    assert_eq!(fs::read(&path).unwrap(), prior);
    let stdout = String::from_utf8_lossy(&first.stdout);
    assert!(stdout.contains("initialized"));
    assert!(!stdout.contains(path.to_str().unwrap()));
}

#[test]
fn configured_receiver_ledger_flag_is_named_without_exposing_its_path() {
    let help = Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let text = String::from_utf8_lossy(&help.stdout);
    assert!(text.contains("--srtp-receiver-ledger"));
    assert!(text.contains("--init-srtp-receiver-ledger"));
}
