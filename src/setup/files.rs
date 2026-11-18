use super::{InitRequest, Layout, ServiceState, SetupError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TransactionKind {
    Fresh,
    Reconfigure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TransactionPhase {
    Staged,
    CredentialsSwapped,
    DropInSwapped,
    UnitVerified,
    ServiceApplied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct FileIdentity {
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Journal {
    version: u8,
    pub(crate) kind: TransactionKind,
    phase: TransactionPhase,
    pub(crate) prior_state: ServiceState,
    old_credentials: Option<FileIdentity>,
    old_drop_in: Option<FileIdentity>,
    new_credentials: FileIdentity,
    new_drop_in: FileIdentity,
}

#[cfg(test)]
thread_local! {
    static FAILPOINT: Cell<Option<TransactionPhase>> = const { Cell::new(None) };
}

pub(crate) fn encode_environment_value(value: &str) -> Result<String, SetupError> {
    if value.chars().any(char::is_control) {
        return Err(SetupError::invalid(
            "credential value contains a control character",
        ));
    }
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for character in value.chars() {
        if matches!(character, '\\' | '"' | '`' | '$') {
            encoded.push('\\');
        }
        encoded.push(character);
    }
    encoded.push('"');
    Ok(encoded)
}

pub(crate) fn validate_request(request: &InitRequest) -> Result<(), SetupError> {
    fn atom(value: &str, field: &str) -> Result<(), SetupError> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        {
            return Err(SetupError::invalid(format!(
                "{field} must contain only ASCII letters, digits, dot, underscore or hyphen"
            )));
        }
        Ok(())
    }

    atom(&request.admin_user, "admin username")?;
    atom(&request.node_name, "node name")?;
    encode_environment_value(request.admin_password.expose())?;
    encode_environment_value(request.peer_key.expose())?;
    Ok(())
}

pub(crate) fn validate_layout(layout: &Layout) -> Result<(), SetupError> {
    validate_directory(parent(&layout.credentials)?, 0o700)?;
    validate_directory(parent(&layout.drop_in)?, 0o755)?;
    validate_optional_file(&layout.credentials, 0o600)?;
    validate_optional_file(&layout.drop_in, 0o644)?;
    validate_auxiliary(layout)?;
    Ok(())
}

pub(crate) fn configuration_exists(layout: &Layout) -> Result<bool, SetupError> {
    let credentials = layout.credentials.exists();
    let drop_in = layout.drop_in.exists();
    if credentials != drop_in {
        return Err(SetupError::invalid(
            "setup has only one generated file; recover or remove the partial setup first",
        ));
    }
    Ok(credentials)
}

pub(crate) fn journal_present(layout: &Layout) -> Result<bool, SetupError> {
    match fs::symlink_metadata(&layout.journal) {
        Ok(metadata)
            if metadata.is_file()
                && metadata.uid() == effective_uid()
                && metadata.permissions().mode() & 0o777 == 0o600 =>
        {
            Ok(true)
        }
        Ok(_) => Err(SetupError::unsafe_path(&layout.journal)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(SetupError::io("cannot inspect transaction journal", source)),
    }
}

pub(crate) fn begin(
    layout: &Layout,
    request: &InitRequest,
    prior_state: ServiceState,
    reconfigure: bool,
) -> Result<(), SetupError> {
    let credentials = render_credentials(request)?;
    let drop_in = render_drop_in(request);
    let journal = Journal {
        version: 1,
        kind: if reconfigure {
            TransactionKind::Reconfigure
        } else {
            TransactionKind::Fresh
        },
        phase: TransactionPhase::Staged,
        prior_state,
        old_credentials: if reconfigure {
            Some(identity_path(&layout.credentials, 1024 * 1024)?)
        } else {
            None
        },
        old_drop_in: if reconfigure {
            Some(identity_path(&layout.drop_in, 1024 * 1024)?)
        } else {
            None
        },
        new_credentials: identity(&credentials),
        new_drop_in: identity(&drop_in),
    };
    write_journal(layout, &journal)?;
    if reconfigure {
        copy_exclusive(&layout.credentials, &layout.credentials_backup, 0o600)?;
        copy_exclusive(&layout.drop_in, &layout.drop_in_backup, 0o644)?;
    }
    write_exclusive(&layout.credentials_staged, &credentials, 0o600)?;
    write_exclusive(&layout.drop_in_staged, &drop_in, 0o644)?;
    sync_parent(&layout.credentials_staged)?;
    sync_parent(&layout.drop_in_staged)?;
    check_failpoint(TransactionPhase::Staged)
}

pub(crate) fn install_credentials(layout: &Layout) -> Result<(), SetupError> {
    fs::rename(&layout.credentials_staged, &layout.credentials)
        .map_err(|source| SetupError::io("cannot install credentials", source))?;
    sync_parent(&layout.credentials)?;
    set_phase(layout, TransactionPhase::CredentialsSwapped)?;
    check_failpoint(TransactionPhase::CredentialsSwapped)
}

pub(crate) fn install_drop_in(layout: &Layout) -> Result<(), SetupError> {
    fs::rename(&layout.drop_in_staged, &layout.drop_in)
        .map_err(|source| SetupError::io("cannot install systemd drop-in", source))?;
    sync_parent(&layout.drop_in)?;
    set_phase(layout, TransactionPhase::DropInSwapped)?;
    check_failpoint(TransactionPhase::DropInSwapped)
}

pub(crate) fn set_phase(layout: &Layout, phase: TransactionPhase) -> Result<(), SetupError> {
    let mut journal = read_journal(layout)?;
    journal.phase = phase;
    write_journal(layout, &journal)
}

pub(crate) fn read_journal(layout: &Layout) -> Result<Journal, SetupError> {
    validate_required_file(&layout.journal, 0o600)?;
    let data = read_limited(&layout.journal, 64 * 1024)?;
    let journal: Journal = serde_json::from_slice(&data)
        .map_err(|_| SetupError::invalid("setup transaction journal is invalid"))?;
    if journal.version != 1 {
        return Err(SetupError::invalid(
            "setup transaction journal version is unsupported",
        ));
    }
    Ok(journal)
}

pub(crate) fn recover_fresh(layout: &Layout, journal: &Journal) -> Result<(), SetupError> {
    remove_if_identity(&layout.credentials_staged, &journal.new_credentials)?;
    remove_if_identity(&layout.drop_in_staged, &journal.new_drop_in)?;
    remove_if_identity(&layout.credentials, &journal.new_credentials)?;
    remove_if_identity(&layout.drop_in, &journal.new_drop_in)?;
    Ok(())
}

pub(crate) fn recover_reconfiguration(
    layout: &Layout,
    journal: &Journal,
) -> Result<(), SetupError> {
    let old_credentials = journal
        .old_credentials
        .as_ref()
        .ok_or_else(|| SetupError::invalid("transaction lacks prior credentials identity"))?;
    let old_drop_in = journal
        .old_drop_in
        .as_ref()
        .ok_or_else(|| SetupError::invalid("transaction lacks prior drop-in identity"))?;
    restore_one(
        &layout.credentials_backup,
        &layout.credentials,
        old_credentials,
        0o600,
    )?;
    restore_one(&layout.drop_in_backup, &layout.drop_in, old_drop_in, 0o644)?;
    remove_if_identity(&layout.credentials_staged, &journal.new_credentials)?;
    remove_if_identity(&layout.drop_in_staged, &journal.new_drop_in)?;
    Ok(())
}

pub(crate) fn finish(layout: &Layout) -> Result<(), SetupError> {
    for path in [
        &layout.credentials_staged,
        &layout.credentials_backup,
        &layout.drop_in_staged,
        &layout.drop_in_backup,
        &layout.journal_temp,
    ] {
        remove_managed_file(path)?;
    }
    remove_managed_file(&layout.journal)?;
    sync_parent(&layout.credentials)?;
    sync_parent(&layout.drop_in)?;
    Ok(())
}

fn render_credentials(request: &InitRequest) -> Result<Vec<u8>, SetupError> {
    Ok(format!(
        "FLUSSONIX_ADMIN_PASSWORD={}\nFLUSSONIX_PEER_KEY={}\n",
        encode_environment_value(request.admin_password.expose())?,
        encode_environment_value(request.peer_key.expose())?
    )
    .into_bytes())
}

fn render_drop_in(request: &InitRequest) -> Vec<u8> {
    format!(
        "[Service]\nEnvironmentFile=/etc/flussonix/credentials.env\nExecStart=\nExecStart=/usr/bin/flussonix --listen={} --config=/var/lib/flussonix/config.json --media-dir=/var/lib/flussonix/media --web-dir=/usr/share/flussonix/web --admin-user={} --role={} --node-name={}\n",
        request.listen, request.admin_user, request.role, request.node_name
    )
    .into_bytes()
}

fn validate_auxiliary(layout: &Layout) -> Result<(), SetupError> {
    let journal_exists = journal_present(layout)?;
    if !journal_exists {
        for path in [
            &layout.credentials_staged,
            &layout.credentials_backup,
            &layout.drop_in_staged,
            &layout.drop_in_backup,
            &layout.journal_temp,
        ] {
            if fs::symlink_metadata(path).is_ok() {
                return Err(SetupError::invalid(format!(
                    "orphaned setup transaction file: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn validate_directory(path: &Path, mode: u32) -> Result<(), SetupError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| SetupError::io("required setup directory is unavailable", source))?;
    if !metadata.is_dir()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o777 != mode
    {
        return Err(SetupError::unsafe_path(path));
    }
    Ok(())
}

fn validate_optional_file(path: &Path, mode: u32) -> Result<(), SetupError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_metadata(path, &metadata, mode),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(SetupError::io("cannot inspect generated path", source)),
    }
}

fn validate_required_file(path: &Path, mode: u32) -> Result<(), SetupError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| SetupError::io("cannot inspect generated path", source))?;
    validate_metadata(path, &metadata, mode)
}

fn validate_metadata(path: &Path, metadata: &fs::Metadata, mode: u32) -> Result<(), SetupError> {
    if !metadata.is_file()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o777 != mode
    {
        return Err(SetupError::unsafe_path(path));
    }
    Ok(())
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid reads process credentials and has no side effects.
    unsafe { libc::geteuid() }
}

fn parent(path: &Path) -> Result<&Path, SetupError> {
    path.parent().ok_or_else(|| SetupError::unsafe_path(path))
}

fn identity(data: &[u8]) -> FileIdentity {
    FileIdentity {
        bytes: data.len() as u64,
        sha256: format!("{:x}", Sha256::digest(data)),
    }
}

fn identity_path(path: &Path, limit: usize) -> Result<FileIdentity, SetupError> {
    Ok(identity(&read_limited(path, limit)?))
}

fn read_limited(path: &Path, limit: usize) -> Result<Vec<u8>, SetupError> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|source| SetupError::io("cannot open generated file", source))?;
    let mut data = Vec::new();
    Read::by_ref(&mut file)
        .take(limit as u64 + 1)
        .read_to_end(&mut data)
        .map_err(|source| SetupError::io("cannot read generated file", source))?;
    if data.len() > limit {
        return Err(SetupError::invalid(
            "generated setup file exceeds size limit",
        ));
    }
    Ok(data)
}

fn write_journal(layout: &Layout, journal: &Journal) -> Result<(), SetupError> {
    remove_managed_file(&layout.journal_temp)?;
    let mut data = serde_json::to_vec(journal)
        .map_err(|_| SetupError::invalid("cannot encode setup transaction journal"))?;
    data.push(b'\n');
    write_exclusive(&layout.journal_temp, &data, 0o600)?;
    fs::rename(&layout.journal_temp, &layout.journal)
        .map_err(|source| SetupError::io("cannot install setup transaction journal", source))?;
    sync_parent(&layout.journal)
}

fn write_exclusive(path: &Path, data: &[u8], mode: u32) -> Result<(), SetupError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|source| SetupError::io("cannot create setup file", source))?;
    file.set_permissions(fs::Permissions::from_mode(mode))
        .map_err(|source| SetupError::io("cannot set setup file permissions", source))?;
    file.write_all(data)
        .map_err(|source| SetupError::io("cannot write setup file", source))?;
    file.sync_all()
        .map_err(|source| SetupError::io("cannot sync setup file", source))?;
    Ok(())
}

fn copy_exclusive(source: &Path, target: &Path, mode: u32) -> Result<(), SetupError> {
    let data = read_limited(source, 1024 * 1024)?;
    write_exclusive(target, &data, mode)
}

fn restore_one(
    backup: &Path,
    live: &Path,
    expected: &FileIdentity,
    mode: u32,
) -> Result<(), SetupError> {
    if backup.exists() {
        validate_required_file(backup, mode)?;
        if identity_path(backup, 1024 * 1024)? != *expected {
            return Err(SetupError::invalid(
                "setup backup does not match the recorded prior file",
            ));
        }
        fs::rename(backup, live)
            .map_err(|source| SetupError::io("cannot restore prior setup file", source))?;
        sync_parent(live)?;
    } else if !live.exists() || identity_path(live, 1024 * 1024)? != *expected {
        return Err(SetupError::invalid(
            "prior setup file is unavailable for recovery",
        ));
    }
    Ok(())
}

fn remove_if_identity(path: &Path, expected: &FileIdentity) -> Result<(), SetupError> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| SetupError::io("cannot inspect recovery file", source))?;
    if !metadata.is_file() || metadata.uid() != effective_uid() {
        return Err(SetupError::unsafe_path(path));
    }
    if identity_path(path, 1024 * 1024)? != *expected {
        return Err(SetupError::invalid(
            "recovery file does not match the transaction journal",
        ));
    }
    fs::remove_file(path)
        .map_err(|source| SetupError::io("cannot remove recovery file", source))?;
    Ok(())
}

fn remove_managed_file(path: &Path) -> Result<(), SetupError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && metadata.uid() == effective_uid() => {
            fs::remove_file(path)
                .map_err(|source| SetupError::io("cannot remove transaction file", source))
        }
        Ok(_) => Err(SetupError::unsafe_path(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(SetupError::io("cannot inspect transaction file", source)),
    }
}

fn sync_parent(path: &Path) -> Result<(), SetupError> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(parent(path)?)
        .map_err(|source| SetupError::io("cannot open setup directory", source))?;
    directory
        .sync_all()
        .map_err(|source| SetupError::io("cannot sync setup directory", source))
}

#[cfg(test)]
pub(crate) fn set_test_failpoint(phase: Option<TransactionPhase>) {
    FAILPOINT.with(|value| value.set(phase));
}

pub(crate) fn check_failpoint(phase: TransactionPhase) -> Result<(), SetupError> {
    #[cfg(test)]
    if FAILPOINT.with(|value| value.get() == Some(phase)) {
        FAILPOINT.with(|value| value.set(None));
        return Err(SetupError::InjectedCrash);
    }
    Ok(())
}
