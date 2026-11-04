use flussonix::direct_rtp::key_ledger::Ledger;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::Command,
};

fn ledger(path: &Path) -> Ledger {
    Ledger::initialize(path).unwrap();
    let ledger = Ledger::new(path.to_path_buf());
    ledger.probe().unwrap();
    ledger
}
fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[test]
fn initialization_is_private_exclusive_and_claim_history_survives_reopen() {
    let dir = private_dir();
    let path = dir.path().join("history");
    let ledger = ledger(&path);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(dir.path().join("history.lock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(Ledger::initialize(&path).is_err());
    ledger.claim([1; 32]).unwrap();
    ledger.claim_many(&[[2; 32], [3; 32]]).unwrap();
    let reopened = Ledger::new(path);
    assert!(reopened.claim([1; 32]).is_err());
    assert!(reopened.claim([2; 32]).is_err());
    reopened.claim([4; 32]).unwrap();
    assert!(reopened.claim([1; 32]).is_err());
}

#[test]
fn a_reused_member_or_invalid_batch_cannot_partially_claim_other_members() {
    let dir = private_dir();
    let ledger = ledger(&dir.path().join("history"));
    assert!(ledger.claim_many(&[]).is_err());
    assert!(ledger.claim_many(&[[1; 32]; 9]).is_err());
    assert!(ledger.claim_many(&[[1; 32], [1; 32]]).is_err());
    ledger.claim([2; 32]).unwrap();
    assert!(ledger.claim_many(&[[3; 32], [2; 32], [4; 32]]).is_err());
    ledger.claim_many(&[[3; 32], [4; 32]]).unwrap();
    for n in [2, 3, 4] {
        assert!(ledger.claim([n; 32]).is_err());
    }
}

#[test]
fn missing_unsafe_or_corrupt_history_fails_closed_without_path_leakage() {
    let dir = private_dir();
    let path = dir.path().join("history");
    let uninitialized = Ledger::new(path.clone());
    let error = uninitialized.claim([1; 32]).unwrap_err();
    assert!(!error.contains(path.to_str().unwrap()));
    let ledger = ledger(&path);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(ledger.probe().is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&path, b"truncated\n").unwrap();
    assert!(ledger.claim([2; 32]).is_err());
    fs::write(&path, b"FXSRTP1\nnot-a-fingerprint\n").unwrap();
    assert!(ledger.probe().is_err());
    fs::write(&path, b"FXSRTP1\n").unwrap();
    let lock = dir.path().join("history.lock");
    fs::remove_file(&lock).unwrap();
    symlink(&path, &lock).unwrap();
    assert!(ledger.probe().is_err());
    fs::remove_file(&lock).unwrap();
    fs::write(&lock, []).unwrap();
    fs::set_permissions(&lock, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(ledger.probe().is_err());
    fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(ledger.claim([3; 32]).is_err());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    ledger.probe().unwrap();
}

#[test]
fn duplicate_history_record_and_nonregular_lock_are_rejected() {
    let dir = private_dir();
    let path = dir.path().join("history");
    let ledger = ledger(&path);
    ledger.claim([9; 32]).unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_within(8..);
    fs::write(&path, bytes).unwrap();
    assert!(ledger.probe().is_err());
    let lock = dir.path().join("history.lock");
    fs::remove_file(&lock).unwrap();
    fs::create_dir(&lock).unwrap();
    assert!(ledger.claim([10; 32]).is_err());
}

#[test]
fn oversized_sparse_history_and_symlinked_ledger_are_rejected() {
    let dir = private_dir();
    let path = dir.path().join("history");
    let ledger = ledger(&path);
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(65_000_009)
        .unwrap();
    assert!(ledger.probe().is_err());
    fs::remove_file(&path).unwrap();
    let other = dir.path().join("other");
    fs::write(&other, b"FXSRTP1\n").unwrap();
    symlink(&other, &path).unwrap();
    assert!(ledger.claim([1; 32]).is_err());
}

#[test]
fn subprocess_claim_helper() {
    let Ok(path) = std::env::var("FLUSSONIX_TEST_SRTP_LEDGER") else {
        return;
    };
    Ledger::new(path.into())
        .claim_many(&[[7; 32], [8; 32]])
        .unwrap();
}

#[test]
fn separate_processes_cannot_claim_the_same_batch() {
    let dir = private_dir();
    let path = dir.path().join("history");
    let ledger = ledger(&path);
    let binary = std::env::current_exe().unwrap();
    let spawn = || {
        Command::new(&binary)
            .arg("subprocess_claim_helper")
            .arg("--exact")
            .env("FLUSSONIX_TEST_SRTP_LEDGER", &path)
            .output()
            .unwrap()
    };
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(spawn);
        let second = scope.spawn(spawn);
        (first.join().unwrap(), second.join().unwrap())
    });
    assert_ne!(first.status.success(), second.status.success());
    assert!(ledger.claim([7; 32]).is_err());
    assert!(ledger.claim([8; 32]).is_err());
}
