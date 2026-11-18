use super::{Layout, ServiceManager, ServiceState, SetupError};
use std::ffi::OsString;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const SYSTEMCTL: &str = "/usr/bin/systemctl";
const SYSTEMD_ANALYZE: &str = "/usr/bin/systemd-analyze";
const UNIT: &str = "flussonix.service";
const OUTPUT_LIMIT: u64 = 64 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct CommandOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub(crate) trait CommandRunner: Send {
    fn run(&mut self, program: &Path, args: &[OsString]) -> Result<CommandOutput, SetupError>;
}

struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&mut self, program: &Path, args: &[OsString]) -> Result<CommandOutput, SetupError> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| SetupError::io("start system service command", error))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SetupError::service("system service command stdout was unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| SetupError::service("system service command stderr was unavailable"))?;
        let stdout_reader = thread::spawn(move || read_bounded(stdout));
        let stderr_reader = thread::spawn(move || read_bounded(stderr));
        let started = Instant::now();
        let status = loop {
            match child
                .try_wait()
                .map_err(|error| SetupError::io("wait for system service command", error))?
            {
                Some(status) => break status,
                None if started.elapsed() < COMMAND_TIMEOUT => {
                    thread::sleep(Duration::from_millis(20));
                }
                None => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(SetupError::service("system service command timed out"));
                }
            }
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| SetupError::service("system service stdout reader failed"))??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| SetupError::service("system service stderr reader failed"))??;
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout,
            stderr,
        })
    }
}

fn read_bounded(reader: impl Read) -> Result<Vec<u8>, SetupError> {
    let mut output = Vec::new();
    reader
        .take(OUTPUT_LIMIT + 1)
        .read_to_end(&mut output)
        .map_err(|error| SetupError::io("read system service command output", error))?;
    if output.len() as u64 > OUTPUT_LIMIT {
        return Err(SetupError::service(
            "system service command output exceeded its limit",
        ));
    }
    Ok(output)
}

pub struct SystemdManager {
    runner: Box<dyn CommandRunner>,
}

impl SystemdManager {
    pub fn system() -> Self {
        Self::with_runner(Box::new(SystemCommandRunner))
    }

    pub(crate) fn with_runner(runner: Box<dyn CommandRunner>) -> Self {
        Self { runner }
    }

    pub(crate) fn fragment_path(&mut self) -> Result<PathBuf, SetupError> {
        let output = self.systemctl(&["show", "--property=FragmentPath", "--value", UNIT])?;
        require_success(
            "inspect systemd unit provenance",
            output.status,
            &output.stderr,
        )?;
        let value = String::from_utf8(output.stdout)
            .map_err(|_| SetupError::service("systemd unit fragment path was not UTF-8"))?;
        let value = value.trim();
        if value.is_empty() {
            return Err(SetupError::service(
                "systemd unit has no managed fragment path",
            ));
        }
        let path = PathBuf::from(value);
        if !path.is_absolute() {
            return Err(SetupError::service(
                "systemd unit fragment path was not absolute",
            ));
        }
        Ok(path)
    }

    fn systemctl(&mut self, action: &[&str]) -> Result<CommandOutput, SetupError> {
        let mut args = vec![
            OsString::from("--no-pager"),
            OsString::from("--no-ask-password"),
        ];
        args.extend(action.iter().map(OsString::from));
        self.runner.run(Path::new(SYSTEMCTL), &args)
    }

    fn service_action(
        &mut self,
        action: &'static str,
        description: &'static str,
    ) -> Result<(), SetupError> {
        let output = self.systemctl(&[action, UNIT])?;
        require_success(description, output.status, &output.stderr)
    }
}

impl ServiceManager for SystemdManager {
    fn state(&mut self) -> Result<ServiceState, SetupError> {
        let active = self.systemctl(&["is-active", UNIT])?;
        let active = match active.status {
            0 => true,
            3 => false,
            _ => {
                return Err(command_failure(
                    "inspect systemd activation",
                    &active.stderr,
                ));
            }
        };
        let enabled = self.systemctl(&["is-enabled", UNIT])?;
        let enabled = match enabled.status {
            0 => true,
            1 => false,
            _ => {
                return Err(command_failure(
                    "inspect systemd enablement",
                    &enabled.stderr,
                ));
            }
        };
        Ok(ServiceState { active, enabled })
    }

    fn verify(&mut self, layout: &Layout, staged_drop_in: &Path) -> Result<(), SetupError> {
        let root = tempfile::Builder::new()
            .prefix("flussonix-unit-verify-")
            .tempdir()
            .map_err(|error| SetupError::io("create systemd verification root", error))?;
        let unit_dir = root.path().join("usr/lib/systemd/system");
        let drop_in_dir = root.path().join("etc/systemd/system/flussonix.service.d");
        fs::create_dir_all(&unit_dir)
            .and_then(|_| fs::create_dir_all(&drop_in_dir))
            .map_err(|error| SetupError::io("create systemd verification layout", error))?;
        fs::copy(&layout.base_unit, unit_dir.join(UNIT))
            .and_then(|_| fs::copy(staged_drop_in, drop_in_dir.join("10-setup.conf")))
            .map_err(|error| SetupError::io("copy staged systemd configuration", error))?;
        let args = vec![
            OsString::from("verify"),
            OsString::from("--man=no"),
            OsString::from(format!("--root={}", root.path().display())),
            OsString::from("/usr/lib/systemd/system/flussonix.service"),
        ];
        let output = self.runner.run(Path::new(SYSTEMD_ANALYZE), &args)?;
        require_success("verify staged systemd unit", output.status, &output.stderr)
    }

    fn daemon_reload(&mut self) -> Result<(), SetupError> {
        let output = self.systemctl(&["daemon-reload"])?;
        require_success(
            "reload systemd configuration",
            output.status,
            &output.stderr,
        )
    }

    fn enable_now(&mut self) -> Result<(), SetupError> {
        self.service_action("enable", "enable flussonix service")?;
        self.service_action("start", "start flussonix service")
    }

    fn restart(&mut self) -> Result<(), SetupError> {
        self.service_action("restart", "restart flussonix service")
    }

    fn disable_now(&mut self) -> Result<(), SetupError> {
        self.service_action("stop", "stop flussonix service")?;
        self.service_action("disable", "disable flussonix service")
    }

    fn restore_state(&mut self, previous: ServiceState) -> Result<(), SetupError> {
        if previous.enabled {
            self.service_action("enable", "restore flussonix enablement")?;
        } else {
            self.service_action("disable", "restore flussonix disablement")?;
        }
        if previous.active {
            self.service_action("restart", "restore active flussonix service")
        } else {
            self.service_action("stop", "restore stopped flussonix service")
        }
    }
}

fn require_success(action: &'static str, status: i32, stderr: &[u8]) -> Result<(), SetupError> {
    if status == 0 {
        Ok(())
    } else {
        Err(command_failure(action, stderr))
    }
}

fn command_failure(action: &'static str, stderr: &[u8]) -> SetupError {
    let detail = String::from_utf8_lossy(stderr)
        .trim()
        .chars()
        .take(512)
        .collect::<String>();
    if detail.is_empty() {
        SetupError::service(format!("failed to {action}"))
    } else {
        SetupError::service(format!("failed to {action}: {detail}"))
    }
}
