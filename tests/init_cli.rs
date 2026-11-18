use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::process::Command;

fn init_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_flussonix"));
    command
        .args(["--init", "--non-interactive"])
        .env_remove("FLUSSONIX_ADMIN_USER")
        .env_remove("FLUSSONIX_ADMIN_PASSWORD")
        .env_remove("FLUSSONIX_PEER_KEY");
    command
}

#[test]
fn non_root_init_fails_before_reading_secrets() {
    let executable = tempfile::Builder::new()
        .prefix("flussonix-init-cli-")
        .tempfile_in("/tmp")
        .unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_flussonix"), executable.path()).unwrap();
    std::fs::set_permissions(executable.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let executable = executable.into_temp_path();
    let mut command = Command::new(&executable);
    command
        .args(["--init", "--non-interactive"])
        .env_remove("FLUSSONIX_ADMIN_USER")
        .env_remove("FLUSSONIX_ADMIN_PASSWORD")
        .env_remove("FLUSSONIX_PEER_KEY");
    if unsafe { libc::geteuid() } == 0 {
        command.uid(65534);
    }
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("root"), "diagnostic was: {diagnostic}");
    assert!(!diagnostic.contains("FLUSSONIX_ADMIN_PASSWORD"));
    assert!(!diagnostic.contains("FLUSSONIX_PEER_KEY"));
}

#[test]
fn init_early_failures_never_echo_supplied_secrets() {
    let admin_secret = "admin-secret-that-must-not-leak";
    let peer_secret = "peer-secret-that-must-not-leak";
    let output = init_command()
        .env("FLUSSONIX_ADMIN_USER", "admin")
        .env("FLUSSONIX_ADMIN_PASSWORD", admin_secret)
        .env("FLUSSONIX_PEER_KEY", peer_secret)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains(admin_secret));
    assert!(!combined.contains(peer_secret));
}

#[test]
fn init_rejects_secret_command_line_options_without_echoing_values() {
    let admin_secret = "admin-argv-secret";
    let peer_secret = "peer-argv-secret";
    let output = Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .args([
            "--init",
            "--admin-password",
            admin_secret,
            "--peer-key",
            peer_secret,
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(combined.contains("setup secrets must be entered"));
    assert!(!combined.contains(admin_secret));
    assert!(!combined.contains(peer_secret));
}
