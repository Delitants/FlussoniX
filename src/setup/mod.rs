mod dependencies;
mod files;
mod systemd;

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::ffi::CString;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use dependencies::{CapabilityStatus, DependencyReport, ToolStatus};
pub use systemd::SystemdManager;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Standalone,
    Source,
    Cdn,
    LoadBalancer,
}

impl fmt::Display for NodeRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Standalone => "standalone",
            Self::Source => "source",
            Self::Cdn => "cdn",
            Self::LoadBalancer => "lb",
        })
    }
}

impl FromStr for NodeRole {
    type Err = SetupError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "standalone" => Ok(Self::Standalone),
            "source" => Ok(Self::Source),
            "cdn" => Ok(Self::Cdn),
            "lb" => Ok(Self::LoadBalancer),
            _ => Err(SetupError::invalid(
                "role must be one of standalone, source, cdn or lb",
            )),
        }
    }
}

pub struct Secret(String);

impl Secret {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

pub struct InitRequest {
    pub admin_user: String,
    pub admin_password: Secret,
    pub peer_key: Secret,
    pub role: NodeRole,
    pub node_name: String,
    pub listen: SocketAddr,
    pub enable_now: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InitOptions {
    pub reconfigure: bool,
    pub non_interactive: bool,
    pub no_start: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceState {
    pub active: bool,
    pub enabled: bool,
}

pub trait ServiceManager {
    fn state(&mut self) -> Result<ServiceState, SetupError>;
    fn verify(&mut self, layout: &Layout, staged_drop_in: &Path) -> Result<(), SetupError>;
    fn daemon_reload(&mut self) -> Result<(), SetupError>;
    fn enable_now(&mut self) -> Result<(), SetupError>;
    fn restart(&mut self) -> Result<(), SetupError>;
    fn disable_now(&mut self) -> Result<(), SetupError>;
    fn restore_state(&mut self, previous: ServiceState) -> Result<(), SetupError>;
}

#[derive(Clone, Debug)]
pub struct Layout {
    pub(crate) binary: PathBuf,
    pub(crate) base_unit: PathBuf,
    pub(crate) state_dir: PathBuf,
    pub(crate) credentials: PathBuf,
    pub(crate) credentials_staged: PathBuf,
    pub(crate) credentials_backup: PathBuf,
    pub(crate) drop_in: PathBuf,
    pub(crate) drop_in_staged: PathBuf,
    pub(crate) drop_in_backup: PathBuf,
    pub(crate) journal: PathBuf,
    pub(crate) journal_temp: PathBuf,
}

impl Layout {
    pub fn system() -> Self {
        Self::rooted(Path::new("/"))
    }

    pub(crate) fn rooted(root: &Path) -> Self {
        let config = root.join("etc/flussonix");
        let drop_in_dir = root.join("etc/systemd/system/flussonix.service.d");
        Self {
            binary: root.join("usr/bin/flussonix"),
            base_unit: root.join("usr/lib/systemd/system/flussonix.service"),
            state_dir: root.join("var/lib/flussonix"),
            credentials: config.join("credentials.env"),
            credentials_staged: config.join(".credentials.env.staged"),
            credentials_backup: config.join(".credentials.env.backup"),
            drop_in: drop_in_dir.join("10-setup.conf"),
            drop_in_staged: drop_in_dir.join(".10-setup.conf.staged"),
            drop_in_backup: drop_in_dir.join(".10-setup.conf.backup"),
            journal: config.join(".setup-transaction.json"),
            journal_temp: config.join(".setup-transaction.json.tmp"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentSetup {
    pub admin_user: String,
    pub role: NodeRole,
    pub node_name: String,
    pub listen: SocketAddr,
    pub service: ServiceState,
}

#[derive(Clone, Debug)]
pub struct PreflightReport {
    pub dependencies: DependencyReport,
    pub current: Option<CurrentSetup>,
    pub listener_available: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PreflightFacts {
    effective_uid: u32,
    trusted_uid: u32,
    service_uid: u32,
    pid1_comm: String,
    unit_fragment: PathBuf,
    listener_available: bool,
}

pub(crate) fn evaluate_preflight(
    layout: &Layout,
    dependencies: DependencyReport,
    current: Option<CurrentSetup>,
    facts: PreflightFacts,
) -> Result<PreflightReport, SetupError> {
    if facts.effective_uid != facts.trusted_uid {
        return Err(SetupError::invalid("flussonix --init must run as root"));
    }
    if facts.pid1_comm.trim() != "systemd" {
        return Err(SetupError::invalid(
            "flussonix --init requires systemd as PID 1",
        ));
    }
    validate_package_layout(layout, facts.trusted_uid, facts.service_uid)?;
    if facts.unit_fragment != layout.base_unit {
        return Err(SetupError::invalid(
            "refusing to take over an unmanaged flussonix.service",
        ));
    }
    Ok(PreflightReport {
        dependencies,
        current,
        listener_available: facts.listener_available,
    })
}

pub async fn preflight(
    layout: &Layout,
    request: Option<&InitRequest>,
) -> Result<PreflightReport, SetupError> {
    // SAFETY: geteuid reads process credentials and has no side effects.
    let effective_uid = unsafe { libc::geteuid() };
    if effective_uid != 0 {
        return Err(SetupError::invalid("flussonix --init must run as root"));
    }
    let pid1_comm = std::fs::read_to_string("/proc/1/comm")
        .map_err(|source| SetupError::io("inspect PID 1", source))?;
    if pid1_comm.trim() != "systemd" {
        return Err(SetupError::invalid(
            "flussonix --init requires systemd as PID 1",
        ));
    }
    let service_uid = account_uid("flussonix")?;
    validate_package_layout(layout, 0, service_uid)?;
    files::validate_layout(layout)?;

    let mut service = SystemdManager::system();
    let unit_fragment = service.fragment_path()?;
    let service_state = service.state()?;
    let current = read_current_setup(layout, service_state)?;
    let listener_available = match request {
        Some(request)
            if current.as_ref().is_some_and(|existing| {
                existing.listen == request.listen && service_state.active
            }) =>
        {
            true
        }
        Some(request) => listener_is_available(request.listen)?,
        None => true,
    };
    let dependencies = dependencies::inspect().await?;
    evaluate_preflight(
        layout,
        dependencies,
        current,
        PreflightFacts {
            effective_uid,
            trusted_uid: 0,
            service_uid,
            pid1_comm,
            unit_fragment,
            listener_available,
        },
    )
}

fn account_uid(name: &str) -> Result<u32, SetupError> {
    let name = CString::new(name)
        .map_err(|_| SetupError::invalid("service account name contains an invalid byte"))?;
    let mut record = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0_u8; 16 * 1024];
    // SAFETY: all pointers refer to live, correctly sized storage for the duration of the call.
    let status = unsafe {
        libc::getpwnam_r(
            name.as_ptr(),
            record.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if status != 0 {
        return Err(SetupError::io(
            "look up flussonix service account",
            std::io::Error::from_raw_os_error(status),
        ));
    }
    if result.is_null() {
        return Err(SetupError::invalid(
            "the flussonix service account is missing",
        ));
    }
    // SAFETY: getpwnam_r populated record and returned its address through result.
    Ok(unsafe { record.assume_init() }.pw_uid)
}

fn read_current_setup(
    layout: &Layout,
    service: ServiceState,
) -> Result<Option<CurrentSetup>, SetupError> {
    if !files::configuration_exists(layout)? {
        return Ok(None);
    }
    let metadata = std::fs::metadata(&layout.drop_in)
        .map_err(|source| SetupError::io("inspect existing setup drop-in", source))?;
    if metadata.len() > 64 * 1024 {
        return Err(SetupError::invalid("existing setup drop-in is too large"));
    }
    let text = std::fs::read_to_string(&layout.drop_in)
        .map_err(|source| SetupError::io("read existing setup drop-in", source))?;
    let command = text
        .lines()
        .find_map(|line| line.strip_prefix("ExecStart=/usr/bin/flussonix "))
        .ok_or_else(|| SetupError::invalid("existing setup drop-in has no managed ExecStart"))?;
    let field = |name: &str| {
        command
            .split_ascii_whitespace()
            .find_map(|item| item.strip_prefix(name))
            .map(str::to_owned)
            .ok_or_else(|| SetupError::invalid("existing setup drop-in is incomplete"))
    };
    let listen = field("--listen=")?
        .parse::<SocketAddr>()
        .map_err(|_| SetupError::invalid("existing setup listener is invalid"))?;
    let role = field("--role=")?.parse::<NodeRole>()?;
    Ok(Some(CurrentSetup {
        admin_user: field("--admin-user=")?,
        role,
        node_name: field("--node-name=")?,
        listen,
        service,
    }))
}

fn listener_is_available(address: SocketAddr) -> Result<bool, SetupError> {
    let mut occupied = occupied_tcp_listeners(Path::new("/proc/net/tcp"), address)?;
    occupied |= occupied_tcp_listeners(Path::new("/proc/net/tcp6"), address)?;
    Ok(!occupied)
}

fn occupied_tcp_listeners(path: &Path, address: SocketAddr) -> Result<bool, SetupError> {
    let table = match std::fs::read_to_string(path) {
        Ok(table) => table,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(SetupError::io("inspect active TCP listeners", source)),
    };
    for line in table.lines().skip(1) {
        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        if fields.len() < 4 || fields[3] != "0A" {
            continue;
        }
        let Some((encoded_address, encoded_port)) = fields[1].split_once(':') else {
            continue;
        };
        let Ok(port) = u16::from_str_radix(encoded_port, 16) else {
            continue;
        };
        if port != address.port() {
            continue;
        }
        if path.ends_with("tcp6") {
            return Ok(true);
        }
        let Ok(raw) = u32::from_str_radix(encoded_address, 16) else {
            continue;
        };
        let existing = Ipv4Addr::from(raw.to_le_bytes());
        match address.ip() {
            IpAddr::V4(selected) if existing.is_unspecified() || existing == selected => {
                return Ok(true);
            }
            IpAddr::V6(_) => return Ok(true),
            _ => {}
        }
    }
    Ok(false)
}

fn validate_package_layout(
    layout: &Layout,
    trusted_uid: u32,
    service_uid: u32,
) -> Result<(), SetupError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn check(path: &Path, directory: bool, owner: u32, mode: u32) -> Result<(), SetupError> {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| SetupError::invalid("packaged setup layout is incomplete"))?;
        let kind_matches = if directory {
            metadata.is_dir()
        } else {
            metadata.is_file()
        };
        if !kind_matches || metadata.uid() != owner || metadata.permissions().mode() & 0o777 != mode
        {
            return Err(SetupError::invalid(
                "package layout has unsafe ownership, type or mode",
            ));
        }
        Ok(())
    }

    check(&layout.binary, false, trusted_uid, 0o755)?;
    check(&layout.base_unit, false, trusted_uid, 0o644)?;
    check(
        layout
            .credentials
            .parent()
            .ok_or_else(|| SetupError::unsafe_path(&layout.credentials))?,
        true,
        trusted_uid,
        0o700,
    )?;
    check(
        layout
            .drop_in
            .parent()
            .ok_or_else(|| SetupError::unsafe_path(&layout.drop_in))?,
        true,
        trusted_uid,
        0o755,
    )?;
    check(&layout.state_dir, true, service_uid, 0o700)
}

#[derive(Debug)]
pub enum SetupError {
    Invalid(String),
    UnsafePath(PathBuf),
    Io {
        action: &'static str,
        source: std::io::Error,
    },
    Service(String),
    #[cfg(test)]
    InjectedCrash,
}

impl SetupError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }

    pub fn service(message: impl Into<String>) -> Self {
        Self::Service(message.into())
    }

    fn io(action: &'static str, source: std::io::Error) -> Self {
        Self::Io { action, source }
    }

    fn unsafe_path(path: &Path) -> Self {
        Self::UnsafePath(path.to_path_buf())
    }

    #[cfg(test)]
    fn injected_crash(&self) -> bool {
        matches!(self, Self::InjectedCrash)
    }

    #[cfg(not(test))]
    fn injected_crash(&self) -> bool {
        false
    }
}

impl fmt::Display for SetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Service(message) => formatter.write_str(message),
            Self::UnsafePath(path) => {
                write!(formatter, "unsafe generated path: {}", path.display())
            }
            Self::Io { action, source } => write!(formatter, "{action}: {source}"),
            #[cfg(test)]
            Self::InjectedCrash => formatter.write_str("injected setup transaction crash"),
        }
    }
}

impl Error for SetupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct InitOutcome {
    pub configured: bool,
    pub active: bool,
    pub enabled: bool,
    pub admin_url: String,
}

#[derive(Debug)]
pub struct PendingInit {
    layout: Layout,
    reconfigure: bool,
    outcome: InitOutcome,
}

impl PendingInit {
    pub fn complete(self) -> Result<InitOutcome, SetupError> {
        files::finish(&self.layout)?;
        Ok(self.outcome)
    }

    pub fn fail_health(self, service: &mut dyn ServiceManager) -> Result<(), SetupError> {
        if self.reconfigure {
            recover(&self.layout, service)
        } else {
            files::finish(&self.layout)
        }
    }
}

pub fn apply(
    layout: &Layout,
    request: &InitRequest,
    options: &InitOptions,
    service: &mut dyn ServiceManager,
) -> Result<PendingInit, SetupError> {
    files::validate_request(request)?;
    files::validate_layout(layout)?;
    if files::journal_present(layout)? {
        recover(layout, service)?;
        files::validate_layout(layout)?;
    }

    let exists = files::configuration_exists(layout)?;
    match (exists, options.reconfigure) {
        (true, false) => {
            return Err(SetupError::invalid(
                "FlussoniX is already initialized; use --init --reconfigure to change setup",
            ));
        }
        (false, true) => {
            return Err(SetupError::invalid(
                "FlussoniX is not initialized; omit --reconfigure for the first setup",
            ));
        }
        _ => {}
    }

    let prior = service.state()?;
    if let Err(error) = files::begin(layout, request, prior, options.reconfigure) {
        if !error.injected_crash() {
            let _ = recover(layout, service);
        }
        return Err(error);
    }

    let operation = (|| {
        files::install_credentials(layout)?;
        files::install_drop_in(layout)?;
        service.verify(layout, &layout.drop_in)?;
        files::set_phase(layout, files::TransactionPhase::UnitVerified)?;
        files::check_failpoint(files::TransactionPhase::UnitVerified)?;
        service.daemon_reload()?;
        if !options.no_start {
            if request.enable_now {
                service.enable_now()?;
            } else if prior.active {
                service.restart()?;
            }
        }
        files::set_phase(layout, files::TransactionPhase::ServiceApplied)?;
        files::check_failpoint(files::TransactionPhase::ServiceApplied)?;
        service.state()
    })();

    let current = match operation {
        Ok(state) => state,
        Err(error) => {
            if !error.injected_crash() {
                let _ = recover(layout, service);
            }
            return Err(error);
        }
    };

    Ok(PendingInit {
        layout: layout.clone(),
        reconfigure: options.reconfigure,
        outcome: InitOutcome {
            configured: true,
            active: current.active,
            enabled: current.enabled,
            admin_url: format!("http://{}/admin/", request.listen),
        },
    })
}

pub fn recover(layout: &Layout, service: &mut dyn ServiceManager) -> Result<(), SetupError> {
    if !files::journal_present(layout)? {
        files::validate_layout(layout)?;
        return Ok(());
    }
    let journal = files::read_journal(layout)?;
    match journal.kind {
        files::TransactionKind::Fresh => {
            files::recover_fresh(layout, &journal)?;
            service.daemon_reload()?;
            service.restore_state(journal.prior_state)?;
        }
        files::TransactionKind::Reconfigure => {
            files::recover_reconfiguration(layout, &journal)?;
            service.daemon_reload()?;
            service.restore_state(journal.prior_state)?;
        }
    }
    files::finish(layout)
}
