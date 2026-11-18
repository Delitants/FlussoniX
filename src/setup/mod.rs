mod files;

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

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
