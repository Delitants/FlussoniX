use super::*;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::Path;

struct FakeService {
    state: ServiceState,
    fail_verify: bool,
    events: Vec<&'static str>,
}

impl FakeService {
    fn stopped() -> Self {
        Self {
            state: ServiceState {
                active: false,
                enabled: false,
            },
            fail_verify: false,
            events: Vec::new(),
        }
    }
}

impl ServiceManager for FakeService {
    fn state(&mut self) -> Result<ServiceState, SetupError> {
        Ok(self.state)
    }

    fn verify(&mut self, _layout: &Layout, _staged_drop_in: &Path) -> Result<(), SetupError> {
        self.events.push("verify");
        if self.fail_verify {
            Err(SetupError::service("unit verification failed"))
        } else {
            Ok(())
        }
    }

    fn daemon_reload(&mut self) -> Result<(), SetupError> {
        self.events.push("reload");
        Ok(())
    }

    fn enable_now(&mut self) -> Result<(), SetupError> {
        self.events.push("enable");
        self.state = ServiceState {
            active: true,
            enabled: true,
        };
        Ok(())
    }

    fn restart(&mut self) -> Result<(), SetupError> {
        self.events.push("restart");
        self.state.active = true;
        Ok(())
    }

    fn disable_now(&mut self) -> Result<(), SetupError> {
        self.events.push("disable");
        self.state = ServiceState {
            active: false,
            enabled: false,
        };
        Ok(())
    }

    fn restore_state(&mut self, previous: ServiceState) -> Result<(), SetupError> {
        self.events.push("restore");
        self.state = previous;
        Ok(())
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    layout: Layout,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::rooted(root.path());
        fs::create_dir_all(layout.credentials.parent().unwrap()).unwrap();
        fs::set_permissions(
            layout.credentials.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::create_dir_all(layout.drop_in.parent().unwrap()).unwrap();
        fs::set_permissions(
            layout.drop_in.parent().unwrap(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        Self {
            _root: root,
            layout,
        }
    }
}

fn request(password: &str, peer_key: &str) -> InitRequest {
    InitRequest {
        admin_user: "admin".to_owned(),
        admin_password: Secret::new(password.to_owned()),
        peer_key: Secret::new(peer_key.to_owned()),
        role: NodeRole::Cdn,
        node_name: "cdn-test".to_owned(),
        listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18210),
        enable_now: true,
    }
}

fn options(reconfigure: bool) -> InitOptions {
    InitOptions {
        reconfigure,
        non_interactive: true,
        no_start: false,
    }
}

fn decode_systemd_quoted(value: &str) -> String {
    let value = value.strip_prefix('"').unwrap().strip_suffix('"').unwrap();
    let mut result = String::new();
    let mut escaped = false;
    for character in value.chars() {
        if escaped {
            result.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else {
            result.push(character);
        }
    }
    assert!(!escaped);
    result
}

#[test]
fn environment_values_round_trip_special_characters() {
    let original = " space \\\"quote`$dollar end ";
    let encoded = files::encode_environment_value(original).unwrap();

    assert_eq!(decode_systemd_quoted(&encoded), original);
}

#[test]
fn environment_values_reject_newlines_without_leaking_secret() {
    let secret = "never-print-this\nsecond-line";
    let error = files::encode_environment_value(secret).unwrap_err();

    assert!(!error.to_string().contains("never-print-this"));
    assert!(error.to_string().contains("control character"));
}

#[test]
fn fresh_init_creates_0600_credentials_and_0644_drop_in() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();

    let outcome = apply(
        &fixture.layout,
        &request("a-secret", "peer-secret"),
        &options(false),
        &mut service,
    )
    .unwrap()
    .complete()
    .unwrap();

    let credentials = fs::read_to_string(&fixture.layout.credentials).unwrap();
    let drop_in = fs::read_to_string(&fixture.layout.drop_in).unwrap();
    assert_eq!(
        fs::metadata(&fixture.layout.credentials)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&fixture.layout.drop_in)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
    assert!(credentials.contains("FLUSSONIX_ADMIN_PASSWORD=\"a-secret\""));
    assert!(credentials.contains("FLUSSONIX_PEER_KEY=\"peer-secret\""));
    assert!(!credentials.contains("FLUSSONIX_ADMIN_USER"));
    assert!(drop_in.contains("--admin-user=admin"));
    assert!(drop_in.contains("--role=cdn"));
    assert!(drop_in.contains("--node-name=cdn-test"));
    assert!(drop_in.contains("--config=/var/lib/flussonix/config.json"));
    assert!(drop_in.contains("--media-dir=/var/lib/flussonix/media"));
    assert!(drop_in.contains("--web-dir=/usr/share/flussonix/web"));
    assert!(!drop_in.contains("a-secret"));
    assert!(!drop_in.contains("peer-secret"));
    assert_eq!(service.events, ["verify", "reload", "enable"]);
    assert!(outcome.configured && outcome.active && outcome.enabled);
    assert_eq!(outcome.admin_url, "http://127.0.0.1:18210/admin/");
    assert!(!fixture.layout.journal.exists());
}

#[test]
fn fresh_init_enforces_modes_under_daemon_umask() {
    const CHILD: &str = "FLUSSONIX_SETUP_UMASK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        // SAFETY: this branch runs in a dedicated child process before it creates worker threads.
        unsafe { libc::umask(0o077) };
        let fixture = Fixture::new();
        let mut service = FakeService::stopped();
        apply(
            &fixture.layout,
            &request("a-secret", "peer-secret"),
            &options(false),
            &mut service,
        )
        .unwrap()
        .complete()
        .unwrap();
        assert_eq!(
            fs::metadata(&fixture.layout.credentials)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&fixture.layout.drop_in)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
        return;
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .env(CHILD, "1")
        .args([
            "--exact",
            "setup::tests::fresh_init_enforces_modes_under_daemon_umask",
            "--nocapture",
        ])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn unsafe_symlink_or_wrong_owner_is_untouched() {
    let fixture = Fixture::new();
    let target = fixture._root.path().join("outside-secret");
    fs::write(&target, "do-not-change").unwrap();
    symlink(&target, &fixture.layout.credentials).unwrap();
    let mut service = FakeService::stopped();

    let error = apply(
        &fixture.layout,
        &request("new-secret", "new-peer"),
        &options(false),
        &mut service,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unsafe generated path"));
    assert_eq!(fs::read_to_string(target).unwrap(), "do-not-change");
    assert!(
        fs::symlink_metadata(&fixture.layout.credentials)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(!fixture.layout.drop_in.exists());
    assert!(service.events.is_empty());
}

#[test]
fn plain_init_refuses_existing_configuration() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();
    apply(
        &fixture.layout,
        &request("old-secret", "old-peer"),
        &options(false),
        &mut service,
    )
    .unwrap()
    .complete()
    .unwrap();
    let before = fs::read(&fixture.layout.credentials).unwrap();

    let error = apply(
        &fixture.layout,
        &request("new-secret", "new-peer"),
        &options(false),
        &mut service,
    )
    .unwrap_err();

    assert!(error.to_string().contains("--reconfigure"));
    assert_eq!(fs::read(&fixture.layout.credentials).unwrap(), before);
}

#[test]
fn verification_failure_restores_reconfiguration() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();
    apply(
        &fixture.layout,
        &request("old-secret", "old-peer"),
        &options(false),
        &mut service,
    )
    .unwrap()
    .complete()
    .unwrap();
    let credentials = fs::read(&fixture.layout.credentials).unwrap();
    let drop_in = fs::read(&fixture.layout.drop_in).unwrap();
    let prior = service.state;
    service.fail_verify = true;

    let error = apply(
        &fixture.layout,
        &request("new-secret", "new-peer"),
        &options(true),
        &mut service,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unit verification failed"));
    assert_eq!(fs::read(&fixture.layout.credentials).unwrap(), credentials);
    assert_eq!(fs::read(&fixture.layout.drop_in).unwrap(), drop_in);
    assert_eq!(service.state, prior);
    assert!(!fixture.layout.journal.exists());
}

#[test]
fn wrong_owner_is_rejected_when_test_can_change_ownership() {
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let fixture = Fixture::new();
    fs::write(&fixture.layout.credentials, "foreign").unwrap();
    let path =
        std::ffi::CString::new(fixture.layout.credentials.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::chown(path.as_ptr(), 1, 1) }, 0);
    let mut service = FakeService::stopped();

    let error = apply(
        &fixture.layout,
        &request("new-secret", "new-peer"),
        &options(true),
        &mut service,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unsafe generated path"));
    assert_eq!(
        fs::symlink_metadata(&fixture.layout.credentials)
            .unwrap()
            .uid(),
        1
    );
}

#[test]
fn recovery_rejects_a_dangling_journal_symlink_without_replacing_it() {
    let fixture = Fixture::new();
    let missing = fixture._root.path().join("missing-journal-target");
    symlink(&missing, &fixture.layout.journal).unwrap();
    let mut service = FakeService::stopped();

    let error = recover(&fixture.layout, &mut service).unwrap_err();

    assert!(error.to_string().contains("unsafe generated path"));
    assert!(
        fs::symlink_metadata(&fixture.layout.journal)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(!missing.exists());
}

#[test]
fn fresh_crash_phases_remove_only_transaction_files_and_restore_service_state() {
    let phases = [
        files::TransactionPhase::Staged,
        files::TransactionPhase::CredentialsSwapped,
        files::TransactionPhase::DropInSwapped,
        files::TransactionPhase::UnitVerified,
        files::TransactionPhase::ServiceApplied,
    ];

    for phase in phases {
        let fixture = Fixture::new();
        let unrelated = fixture._root.path().join("operator-certificate.pem");
        fs::write(&unrelated, "operator-owned").unwrap();
        let mut service = FakeService::stopped();
        files::set_test_failpoint(Some(phase));

        let error = apply(
            &fixture.layout,
            &request("new-secret", "new-peer"),
            &options(false),
            &mut service,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected setup transaction crash")
        );
        assert!(
            fixture.layout.journal.exists(),
            "missing journal at {phase:?}"
        );
        recover(&fixture.layout, &mut service).unwrap();
        assert!(
            !fixture.layout.credentials.exists(),
            "credentials at {phase:?}"
        );
        assert!(!fixture.layout.drop_in.exists(), "drop-in at {phase:?}");
        assert!(!fixture.layout.journal.exists(), "journal at {phase:?}");
        assert_eq!(fs::read_to_string(&unrelated).unwrap(), "operator-owned");
        assert_eq!(service.state, ServiceState::default(), "state at {phase:?}");
        recover(&fixture.layout, &mut service).unwrap();
    }
}

#[test]
fn reconfiguration_crash_phases_restore_the_complete_prior_pair_and_state() {
    let phases = [
        files::TransactionPhase::Staged,
        files::TransactionPhase::CredentialsSwapped,
        files::TransactionPhase::DropInSwapped,
        files::TransactionPhase::UnitVerified,
        files::TransactionPhase::ServiceApplied,
    ];

    for phase in phases {
        let fixture = Fixture::new();
        let mut service = FakeService::stopped();
        apply(
            &fixture.layout,
            &request("old-secret", "old-peer"),
            &options(false),
            &mut service,
        )
        .unwrap()
        .complete()
        .unwrap();
        let credentials = fs::read(&fixture.layout.credentials).unwrap();
        let drop_in = fs::read(&fixture.layout.drop_in).unwrap();
        let prior = ServiceState {
            active: true,
            enabled: false,
        };
        service.state = prior;
        files::set_test_failpoint(Some(phase));

        let error = apply(
            &fixture.layout,
            &request("new-secret", "new-peer"),
            &options(true),
            &mut service,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected setup transaction crash")
        );
        recover(&fixture.layout, &mut service).unwrap();
        assert_eq!(fs::read(&fixture.layout.credentials).unwrap(), credentials);
        assert_eq!(fs::read(&fixture.layout.drop_in).unwrap(), drop_in);
        assert_eq!(service.state, prior, "state at {phase:?}");
        assert!(!fixture.layout.journal.exists(), "journal at {phase:?}");
        recover(&fixture.layout, &mut service).unwrap();
    }
}

#[test]
fn first_install_health_failure_keeps_valid_configuration_for_diagnosis() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();

    apply(
        &fixture.layout,
        &request("new-secret", "new-peer"),
        &options(false),
        &mut service,
    )
    .unwrap()
    .fail_health(&mut service)
    .unwrap();

    assert!(fixture.layout.credentials.exists());
    assert!(fixture.layout.drop_in.exists());
    assert!(!fixture.layout.journal.exists());
    assert_eq!(
        service.state,
        ServiceState {
            active: true,
            enabled: true,
        }
    );
}

#[test]
fn reconfiguration_health_failure_restores_prior_pair_and_state() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();
    apply(
        &fixture.layout,
        &request("old-secret", "old-peer"),
        &options(false),
        &mut service,
    )
    .unwrap()
    .complete()
    .unwrap();
    let credentials = fs::read(&fixture.layout.credentials).unwrap();
    let drop_in = fs::read(&fixture.layout.drop_in).unwrap();
    let prior = ServiceState {
        active: true,
        enabled: false,
    };
    service.state = prior;

    apply(
        &fixture.layout,
        &request("new-secret", "new-peer"),
        &options(true),
        &mut service,
    )
    .unwrap()
    .fail_health(&mut service)
    .unwrap();

    assert_eq!(fs::read(&fixture.layout.credentials).unwrap(), credentials);
    assert_eq!(fs::read(&fixture.layout.drop_in).unwrap(), drop_in);
    assert_eq!(service.state, prior);
    assert!(!fixture.layout.journal.exists());
}
