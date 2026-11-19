use super::*;
use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

struct FakeService {
    state: ServiceState,
    fail_verify: bool,
    fail_enable: bool,
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
            fail_enable: false,
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
        if self.fail_enable {
            return Err(SetupError::service("service activation failed"));
        }
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

    fn install_package_layout(&self) {
        fs::create_dir_all(self.layout.binary.parent().unwrap()).unwrap();
        fs::write(&self.layout.binary, b"qualified flussonix fixture").unwrap();
        fs::set_permissions(&self.layout.binary, fs::Permissions::from_mode(0o755)).unwrap();
        fs::create_dir_all(self.layout.base_unit.parent().unwrap()).unwrap();
        fs::write(
            &self.layout.base_unit,
            b"[Service]\nExecStart=/usr/bin/flussonix\n",
        )
        .unwrap();
        fs::set_permissions(&self.layout.base_unit, fs::Permissions::from_mode(0o644)).unwrap();
        fs::create_dir_all(&self.layout.state_dir).unwrap();
        fs::set_permissions(&self.layout.state_dir, fs::Permissions::from_mode(0o700)).unwrap();
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

fn available_tool(detail: &str) -> ToolStatus {
    ToolStatus::available(detail)
}

fn ready_dependencies() -> DependencyReport {
    DependencyReport {
        ffmpeg: available_tool("ffmpeg 6.1"),
        ffprobe: available_tool("ffprobe 6.1"),
        h264_aac_profile: available_tool("H.264/AAC encode available"),
        srt: available_tool("libsrt 1.5 available"),
        srtp: available_tool("libsrtp2 available"),
        gpu: vec![CapabilityStatus::unavailable(
            "hevc_nvenc",
            "NVIDIA driver unavailable",
        )],
    }
}

fn preflight_facts(fixture: &Fixture) -> PreflightFacts {
    let owner = unsafe { libc::geteuid() };
    PreflightFacts {
        effective_uid: owner,
        trusted_uid: owner,
        service_uid: owner,
        pid1_comm: "systemd".to_owned(),
        unit_fragment: fixture.layout.base_unit.clone(),
        listener_available: true,
    }
}

#[test]
fn preflight_required_dependencies_fail_independently_but_gpu_is_diagnostic_only() {
    let mut report = ready_dependencies();
    assert!(report.required_ready());
    assert!(!report.gpu[0].available);

    report.ffmpeg = ToolStatus::missing("install ffmpeg");
    assert!(!report.required_ready());
    report = ready_dependencies();
    report.ffprobe = ToolStatus::missing("install ffmpeg for ffprobe");
    assert!(!report.required_ready());
    report = ready_dependencies();
    report.h264_aac_profile = ToolStatus::failed("H.264/AAC probe failed");
    assert!(!report.required_ready());
    report = ready_dependencies();
    report.srt = ToolStatus::missing("install libsrt1.5-gnutls");
    assert!(!report.required_ready());
    report = ready_dependencies();
    report.srtp = ToolStatus::missing("install libsrtp2-1");
    assert!(!report.required_ready());
}

#[test]
fn preflight_rejects_non_root_before_package_or_listener_checks() {
    let fixture = Fixture::new();
    let mut facts = preflight_facts(&fixture);
    facts.effective_uid = facts.trusted_uid.saturating_add(1);

    let error = evaluate_preflight(&fixture.layout, ready_dependencies(), None, facts).unwrap_err();

    assert!(error.to_string().contains("root"));
    assert!(!fixture.layout.binary.exists());
}

#[test]
fn preflight_rejects_non_systemd_pid_one() {
    let fixture = Fixture::new();
    fixture.install_package_layout();
    let mut facts = preflight_facts(&fixture);
    facts.pid1_comm = "init".to_owned();

    let error = evaluate_preflight(&fixture.layout, ready_dependencies(), None, facts).unwrap_err();

    assert!(error.to_string().contains("systemd"));
}

#[test]
fn preflight_rejects_wrong_package_mode_without_changing_it() {
    let fixture = Fixture::new();
    fixture.install_package_layout();
    fs::set_permissions(&fixture.layout.binary, fs::Permissions::from_mode(0o777)).unwrap();
    let before = fs::metadata(&fixture.layout.binary)
        .unwrap()
        .permissions()
        .mode();

    let error = evaluate_preflight(
        &fixture.layout,
        ready_dependencies(),
        None,
        preflight_facts(&fixture),
    )
    .unwrap_err();

    assert!(error.to_string().contains("package layout"));
    assert_eq!(
        fs::metadata(&fixture.layout.binary)
            .unwrap()
            .permissions()
            .mode(),
        before
    );
}

#[test]
fn preflight_rejects_unmanaged_service_fragment() {
    let fixture = Fixture::new();
    fixture.install_package_layout();
    let mut facts = preflight_facts(&fixture);
    facts.unit_fragment = fixture._root.path().join("operator/flussonix.service");

    let error = evaluate_preflight(&fixture.layout, ready_dependencies(), None, facts).unwrap_err();

    assert!(error.to_string().contains("unmanaged flussonix.service"));
}

#[test]
fn preflight_reports_occupied_listener_without_writing_setup() {
    let fixture = Fixture::new();
    fixture.install_package_layout();
    let mut facts = preflight_facts(&fixture);
    facts.listener_available = false;

    let report = evaluate_preflight(&fixture.layout, ready_dependencies(), None, facts).unwrap();

    assert!(!report.listener_available);
    assert!(report.current.is_none());
    assert!(!fixture.layout.credentials.exists());
    assert!(!fixture.layout.drop_in.exists());
    assert!(!fixture.layout.journal.exists());
}

#[derive(Clone)]
struct RecordedCommands {
    calls: Arc<Mutex<Vec<RecordedCommand>>>,
}

type RecordedCommand = (PathBuf, Vec<OsString>);

impl RecordedCommands {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn snapshot(&self) -> Vec<RecordedCommand> {
        self.calls.lock().unwrap().clone()
    }
}

struct RecordingRunner {
    recorded: RecordedCommands,
    replies: VecDeque<systemd::CommandOutput>,
}

impl RecordingRunner {
    fn with_replies(recorded: RecordedCommands, replies: &[(i32, &str, &str)]) -> Self {
        Self {
            recorded,
            replies: replies
                .iter()
                .map(|(status, stdout, stderr)| systemd::CommandOutput {
                    status: *status,
                    stdout: stdout.as_bytes().to_vec(),
                    stderr: stderr.as_bytes().to_vec(),
                })
                .collect(),
        }
    }
}

impl systemd::CommandRunner for RecordingRunner {
    fn run(
        &mut self,
        program: &Path,
        args: &[OsString],
    ) -> Result<systemd::CommandOutput, SetupError> {
        self.recorded
            .calls
            .lock()
            .unwrap()
            .push((program.to_path_buf(), args.to_vec()));
        self.replies
            .pop_front()
            .ok_or_else(|| SetupError::service("recording runner has no queued response"))
    }
}

fn os_args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

#[test]
fn systemd_state_and_fragment_use_fixed_argument_arrays() {
    let recorded = RecordedCommands::new();
    let runner = RecordingRunner::with_replies(
        recorded.clone(),
        &[
            (0, "active\n", ""),
            (1, "disabled\n", ""),
            (0, "/usr/lib/systemd/system/flussonix.service\n", ""),
        ],
    );
    let mut manager = SystemdManager::with_runner(Box::new(runner));

    assert_eq!(
        manager.state().unwrap(),
        ServiceState {
            active: true,
            enabled: false,
        }
    );
    assert_eq!(
        manager.fragment_path().unwrap(),
        PathBuf::from("/usr/lib/systemd/system/flussonix.service")
    );

    assert_eq!(
        recorded.snapshot(),
        vec![
            (
                PathBuf::from("/usr/bin/systemctl"),
                os_args(&[
                    "--no-pager",
                    "--no-ask-password",
                    "is-active",
                    "flussonix.service"
                ]),
            ),
            (
                PathBuf::from("/usr/bin/systemctl"),
                os_args(&[
                    "--no-pager",
                    "--no-ask-password",
                    "is-enabled",
                    "flussonix.service"
                ]),
            ),
            (
                PathBuf::from("/usr/bin/systemctl"),
                os_args(&[
                    "--no-pager",
                    "--no-ask-password",
                    "show",
                    "--property=FragmentPath",
                    "--value",
                    "flussonix.service",
                ]),
            ),
        ]
    );
}

#[test]
fn systemd_lifecycle_keeps_enablement_and_activation_failures_separate() {
    let recorded = RecordedCommands::new();
    let runner = RecordingRunner::with_replies(recorded.clone(), &[(0, "", ""); 7]);
    let mut manager = SystemdManager::with_runner(Box::new(runner));

    manager.daemon_reload().unwrap();
    manager.enable_now().unwrap();
    manager.restart().unwrap();
    manager.disable_now().unwrap();

    let actions = recorded
        .snapshot()
        .into_iter()
        .map(|(_, args)| args)
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        vec![
            os_args(&["--no-pager", "--no-ask-password", "daemon-reload"]),
            os_args(&[
                "--no-pager",
                "--no-ask-password",
                "enable",
                "flussonix.service"
            ]),
            os_args(&[
                "--no-pager",
                "--no-ask-password",
                "start",
                "flussonix.service"
            ]),
            os_args(&[
                "--no-pager",
                "--no-ask-password",
                "restart",
                "flussonix.service"
            ]),
            os_args(&[
                "--no-pager",
                "--no-ask-password",
                "stop",
                "flussonix.service"
            ]),
            os_args(&[
                "--no-pager",
                "--no-ask-password",
                "disable",
                "flussonix.service"
            ]),
        ]
    );
}

#[test]
fn systemd_enablement_and_activation_report_the_failing_operation() {
    let recorded = RecordedCommands::new();
    let runner = RecordingRunner::with_replies(recorded.clone(), &[(1, "", "enable denied")]);
    let mut manager = SystemdManager::with_runner(Box::new(runner));
    let error = manager.enable_now().unwrap_err();
    assert!(error.to_string().contains("enable flussonix"));
    assert_eq!(recorded.snapshot().len(), 1);

    let recorded = RecordedCommands::new();
    let runner =
        RecordingRunner::with_replies(recorded.clone(), &[(0, "", ""), (1, "", "start denied")]);
    let mut manager = SystemdManager::with_runner(Box::new(runner));
    let error = manager.enable_now().unwrap_err();
    assert!(error.to_string().contains("start flussonix"));
    assert!(error.to_string().contains("enabled at boot"));
    assert!(
        error
            .to_string()
            .contains("systemctl disable flussonix.service")
    );
    assert_eq!(recorded.snapshot().len(), 2);
}

#[test]
fn systemd_restore_preserves_active_and_enabled_state_independently() {
    let cases = [
        (true, true, ["enable", "restart"]),
        (true, false, ["disable", "restart"]),
        (false, true, ["enable", "stop"]),
        (false, false, ["disable", "stop"]),
    ];
    for (active, enabled, expected) in cases {
        let recorded = RecordedCommands::new();
        let runner = RecordingRunner::with_replies(recorded.clone(), &[(0, "", ""); 2]);
        let mut manager = SystemdManager::with_runner(Box::new(runner));

        manager
            .restore_state(ServiceState { active, enabled })
            .unwrap();

        let calls = recorded.snapshot();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].1[2], OsString::from(expected[0]));
        assert_eq!(calls[1].1[2], OsString::from(expected[1]));
    }
}

#[test]
fn systemd_verify_uses_systemd_analyze_without_a_shell() {
    let fixture = Fixture::new();
    fixture.install_package_layout();
    fs::write(
        &fixture.layout.drop_in_staged,
        b"[Service]\nRestart=on-failure\n",
    )
    .unwrap();
    let recorded = RecordedCommands::new();
    let runner = RecordingRunner::with_replies(recorded.clone(), &[(0, "", "")]);
    let mut manager = SystemdManager::with_runner(Box::new(runner));

    manager
        .verify(&fixture.layout, &fixture.layout.drop_in_staged)
        .unwrap();

    let calls = recorded.snapshot();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, PathBuf::from("/usr/bin/systemd-analyze"));
    assert_eq!(calls[0].1[0], OsString::from("verify"));
    assert_eq!(calls[0].1[1], OsString::from("--man=no"));
    assert!(calls[0].1[2].to_string_lossy().starts_with("--root="));
    assert_eq!(
        calls[0].1[3],
        OsString::from("/usr/lib/systemd/system/flussonix.service")
    );
}

#[test]
fn systemd_verify_root_contains_implicit_targets_and_executable_marker() {
    struct InspectingRunner;
    impl systemd::CommandRunner for InspectingRunner {
        fn run(
            &mut self,
            _program: &Path,
            args: &[OsString],
        ) -> Result<systemd::CommandOutput, SetupError> {
            let root = args
                .iter()
                .find_map(|argument| argument.to_str()?.strip_prefix("--root="))
                .map(PathBuf::from)
                .expect("verification root argument");
            for target in [
                "sysinit.target",
                "basic.target",
                "shutdown.target",
                "network-online.target",
                "multi-user.target",
            ] {
                assert!(
                    root.join("usr/lib/systemd/system").join(target).is_file(),
                    "missing isolated verification target {target}"
                );
            }
            let binary = root.join("usr/bin/flussonix");
            assert!(binary.is_file());
            assert_ne!(
                fs::metadata(binary).unwrap().permissions().mode() & 0o111,
                0
            );
            Ok(systemd::CommandOutput {
                status: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    let fixture = Fixture::new();
    fixture.install_package_layout();
    fs::write(
        &fixture.layout.drop_in_staged,
        b"[Service]\nRestart=always\n",
    )
    .unwrap();
    let mut manager = SystemdManager::with_runner(Box::new(InspectingRunner));

    manager
        .verify(&fixture.layout, &fixture.layout.drop_in_staged)
        .unwrap();
}

#[test]
fn preflight_reads_only_non_secret_current_setup_fields() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();
    let expected = request("password-must-not-be-read", "peer-key-must-not-be-read");
    apply(
        &fixture.layout,
        &expected,
        &InitOptions {
            reconfigure: false,
            non_interactive: true,
            no_start: true,
        },
        &mut service,
    )
    .unwrap()
    .complete()
    .unwrap();

    let current = read_current_setup(
        &fixture.layout,
        ServiceState {
            active: false,
            enabled: false,
        },
    )
    .unwrap()
    .unwrap();

    assert_eq!(current.admin_user, expected.admin_user);
    assert_eq!(current.role, expected.role);
    assert_eq!(current.node_name, expected.node_name);
    assert_eq!(current.listen, expected.listen);
}

#[test]
fn preflight_listener_table_matches_wildcard_and_selected_ipv4_only() {
    let directory = tempfile::tempdir().unwrap();
    let table = directory.path().join("tcp");
    fs::write(
        &table,
        "  sl  local_address rem_address   st\n   0: 0100007F:4722 00000000:0000 0A\n",
    )
    .unwrap();
    let selected = "127.0.0.1:18210".parse().unwrap();
    assert!(occupied_tcp_listeners(&table, selected).unwrap());
    let other = "127.0.0.2:18210".parse().unwrap();
    assert!(!occupied_tcp_listeners(&table, other).unwrap());

    fs::write(
        &table,
        "  sl  local_address rem_address   st\n   0: 00000000:4722 00000000:0000 0A\n",
    )
    .unwrap();
    assert!(occupied_tcp_listeners(&table, other).unwrap());
}

struct ScriptedPrompt {
    lines: VecDeque<Result<String, io::Error>>,
    hidden: VecDeque<Result<String, io::Error>>,
    output: String,
}

impl ScriptedPrompt {
    fn new(lines: &[&str], hidden: &[&str]) -> Self {
        Self {
            lines: lines.iter().map(|value| Ok((*value).to_owned())).collect(),
            hidden: hidden.iter().map(|value| Ok((*value).to_owned())).collect(),
            output: String::new(),
        }
    }
}

impl wizard::PromptIo for ScriptedPrompt {
    fn write(&mut self, text: &str) -> Result<(), io::Error> {
        self.output.push_str(text);
        Ok(())
    }

    fn read_line(&mut self, prompt: &str) -> Result<String, io::Error> {
        self.output.push_str(prompt);
        self.lines.pop_front().expect("scripted line input")
    }

    fn read_hidden(&mut self, prompt: &str) -> Result<String, io::Error> {
        self.output.push_str(prompt);
        self.hidden.pop_front().expect("scripted hidden input")
    }
}

struct FixedRandom(u8);

impl wizard::RandomSource for FixedRandom {
    fn fill(&mut self, output: &mut [u8]) -> Result<(), SetupError> {
        output.fill(self.0);
        Ok(())
    }
}

fn wizard_defaults() -> ExplicitInitOptions {
    ExplicitInitOptions {
        role: NodeRole::Standalone,
        node_name: "local".to_owned(),
        listen: "127.0.0.1:18210".parse().unwrap(),
    }
}

fn collect_scripted(
    prompt: &mut ScriptedPrompt,
    defaults: &ExplicitInitOptions,
    no_start: bool,
    listener_available: &mut dyn FnMut(SocketAddr) -> Result<bool, SetupError>,
) -> Result<InitRequest, SetupError> {
    wizard::collect_interactive(
        prompt,
        defaults,
        no_start,
        listener_available,
        &mut FixedRandom(0x5a),
    )
}

#[test]
fn wizard_reprompts_after_password_mismatch() {
    let mut prompt = ScriptedPrompt::new(
        &["", "", "", "n", "", ""],
        &["first", "different", "correct", "correct"],
    );
    let request =
        collect_scripted(&mut prompt, &wizard_defaults(), false, &mut |_| Ok(true)).unwrap();

    assert_eq!(request.admin_password.expose(), "correct");
    assert!(prompt.output.contains("Passwords did not match"));
}

#[test]
fn wizard_accepts_an_existing_peer_key_only_through_hidden_input() {
    let peer_key = "existing-cluster-peer-key";
    let mut prompt = ScriptedPrompt::new(
        &["", "", "", "y", "", ""],
        &["password", "password", peer_key],
    );
    let request =
        collect_scripted(&mut prompt, &wizard_defaults(), false, &mut |_| Ok(true)).unwrap();

    assert_eq!(request.peer_key.expose(), peer_key);
    assert!(!prompt.output.contains(peer_key));
}

#[test]
fn wizard_generates_a_32_byte_peer_key_and_uses_role_listener_defaults() {
    let defaults = ExplicitInitOptions {
        role: NodeRole::Cdn,
        node_name: "cdn-default".to_owned(),
        listen: "127.0.0.2:19000".parse().unwrap(),
    };
    let mut prompt = ScriptedPrompt::new(&["", "", "", "n", "", ""], &["password", "password"]);
    let request = collect_scripted(&mut prompt, &defaults, false, &mut |_| Ok(true)).unwrap();

    assert_eq!(request.peer_key.expose().len(), 64);
    assert_eq!(request.peer_key.expose(), "5a".repeat(32));
    assert_eq!(request.role, NodeRole::Cdn);
    assert_eq!(request.node_name, "cdn-default");
    assert_eq!(request.listen, defaults.listen);
}

#[test]
fn wizard_retries_an_occupied_listener() {
    let mut prompt = ScriptedPrompt::new(
        &["", "", "", "n", "127.0.0.1:19001", "127.0.0.1:19002", ""],
        &["password", "password"],
    );
    let mut checked = Vec::new();
    let request = collect_scripted(&mut prompt, &wizard_defaults(), false, &mut |address| {
        checked.push(address);
        Ok(address.port() == 19002)
    })
    .unwrap();

    assert_eq!(request.listen.port(), 19002);
    assert_eq!(checked.len(), 2);
    assert!(prompt.output.contains("already in use"));
}

#[test]
fn wizard_summary_is_redacted_and_enablement_defaults_to_yes() {
    let password = "summary-admin-secret";
    let peer_key = "summary-peer-secret";
    let mut prompt = ScriptedPrompt::new(
        &["operator", "source", "origin-1", "y", "", ""],
        &[password, password, peer_key],
    );
    let request =
        collect_scripted(&mut prompt, &wizard_defaults(), false, &mut |_| Ok(true)).unwrap();

    assert!(request.enable_now);
    assert!(prompt.output.contains("origin-1"));
    assert!(prompt.output.contains("source"));
    assert!(!prompt.output.contains(password));
    assert!(!prompt.output.contains(peer_key));
}

#[test]
fn wizard_can_decline_enablement_or_force_no_start() {
    let mut declined = ScriptedPrompt::new(&["", "", "", "n", "", "n"], &["password", "password"]);
    let request =
        collect_scripted(&mut declined, &wizard_defaults(), false, &mut |_| Ok(true)).unwrap();
    assert!(!request.enable_now);

    let mut no_start = ScriptedPrompt::new(&["", "", "", "n", ""], &["password", "password"]);
    let request =
        collect_scripted(&mut no_start, &wizard_defaults(), true, &mut |_| Ok(true)).unwrap();
    assert!(!request.enable_now);
    assert!(!no_start.output.contains("Enable and start"));
}

#[test]
fn wizard_interruption_before_summary_returns_without_a_request() {
    let mut prompt = ScriptedPrompt::new(&[], &[]);
    prompt
        .lines
        .push_back(Err(io::Error::from(io::ErrorKind::Interrupted)));
    let result = collect_scripted(&mut prompt, &wizard_defaults(), false, &mut |_| Ok(true));

    assert!(result.is_err());
    assert!(!prompt.output.contains("Setup summary"));
}

#[test]
fn wizard_terminal_reader_observes_an_interrupt_even_without_input() {
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;

    let (reader, _writer) = UnixStream::pair().unwrap();
    let reader = fs::File::from(OwnedFd::from(reader));
    let mut prompt = wizard::TtyPrompt::from_file_for_test(reader);
    let _guard = wizard::InterruptGuard::install().unwrap();
    wizard::mark_interrupted_for_test();

    let error = prompt.read_raw_line().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
}

#[test]
fn unattended_setup_requires_exact_environment_inputs() {
    let mut requested = Vec::new();
    let result = wizard::collect_unattended(&wizard_defaults(), false, &mut |name| {
        requested.push(name.to_owned());
        match name {
            "FLUSSONIX_ADMIN_USER" => Some("admin".to_owned()),
            "FLUSSONIX_ADMIN_PASSWORD" => Some("password".to_owned()),
            _ => None,
        }
    });
    let error = match result {
        Ok(_) => panic!("missing peer key must fail closed"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("FLUSSONIX_PEER_KEY"));
    assert_eq!(
        requested,
        vec![
            "FLUSSONIX_ADMIN_USER",
            "FLUSSONIX_ADMIN_PASSWORD",
            "FLUSSONIX_PEER_KEY"
        ]
    );
}

#[test]
fn no_start_never_enables_or_restarts_the_service() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();
    apply(
        &fixture.layout,
        &request("admin-secret", "peer-secret"),
        &InitOptions {
            reconfigure: false,
            non_interactive: true,
            no_start: true,
        },
        &mut service,
    )
    .unwrap()
    .complete()
    .unwrap();

    assert!(!service.events.contains(&"enable"));
    assert!(!service.events.contains(&"restart"));
}

#[test]
fn fresh_start_failure_keeps_the_verified_configuration_for_retry() {
    let fixture = Fixture::new();
    let mut service = FakeService::stopped();
    service.fail_enable = true;

    let error = match apply(
        &fixture.layout,
        &request("admin-secret", "peer-secret"),
        &InitOptions {
            reconfigure: false,
            non_interactive: true,
            no_start: false,
        },
        &mut service,
    ) {
        Ok(_) => panic!("activation failure must be reported"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("activation"));
    assert!(fixture.layout.credentials.exists());
    assert!(fixture.layout.drop_in.exists());
    assert!(!fixture.layout.journal.exists());
    assert!(service.events.contains(&"enable"));
}

#[test]
fn plain_rerun_reports_current_setup_while_reconfigure_continues() {
    let current = CurrentSetup {
        admin_user: "admin".to_owned(),
        role: NodeRole::Cdn,
        node_name: "cdn-1".to_owned(),
        listen: "127.0.0.1:18210".parse().unwrap(),
        service: ServiceState {
            active: true,
            enabled: true,
        },
    };
    let mut prompt = ScriptedPrompt::new(&[], &[]);
    let outcome = wizard::report_existing(&mut prompt, &current, false)
        .unwrap()
        .expect("plain rerun exits after reporting current setup");
    assert!(outcome.configured);
    assert!(prompt.output.contains("cdn-1"));
    assert!(prompt.output.contains("127.0.0.1:18210"));

    assert!(
        wizard::report_existing(&mut prompt, &current, true)
            .unwrap()
            .is_none(),
        "explicit reconfigure must continue into collection and apply"
    );
}
