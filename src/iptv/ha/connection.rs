//! Bounded, TLS-verified PostgreSQL connections for the opt-in IPTV authority.
use serde::Deserialize;
use std::{
    fs::{File, OpenOptions},
    io::Read,
    net::IpAddr,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_postgres::{Client, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

const MAX_CONFIG_BYTES: u64 = 16 * 1024;
const MAX_CONNECTIONS: usize = 16;
const DATABASE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialsFile {
    cluster_id: String,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    ca_pem: PathBuf,
}

#[derive(Clone)]
pub struct HaOptions {
    pub path: PathBuf,
    pub cluster_id: Uuid,
    credentials: CredentialsFile,
}

impl HaOptions {
    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.is_absolute() {
            return Err("IPTV HA credential file must have an absolute path".into());
        }
        let mut open = OpenOptions::new();
        open.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let file: File = open
            .open(path)
            .map_err(|_| "cannot open IPTV HA credential file")?;
        let metadata = file
            .metadata()
            .map_err(|_| "cannot inspect IPTV HA credential file")?;
        if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
            return Err("IPTV HA credential file must be a bounded regular file".into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // SAFETY: geteuid has no arguments and no memory ownership contract.
            let owner = unsafe { libc::geteuid() };
            if metadata.uid() != owner
                || metadata.mode() & 0o177 != 0
                || metadata.mode() & 0o400 == 0
            {
                return Err(
                    "IPTV HA credential file must be owned by this user and mode 0600 or stricter"
                        .into(),
                );
            }
        }
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read IPTV HA credential file")?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err("IPTV HA credential file exceeds 16 KiB".into());
        }
        let credentials: CredentialsFile =
            serde_json::from_slice(&bytes).map_err(|_| "invalid IPTV HA credential file")?;
        let cluster_id =
            Uuid::parse_str(&credentials.cluster_id).map_err(|_| "invalid IPTV HA cluster ID")?;
        let host = credentials.host.as_str();
        if host.is_empty()
            || host.len() > 253
            || host
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || c == '/')
            || (host.parse::<IpAddr>().is_err()
                && !host
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-')))
            || credentials.port == 0
            || !valid_identifier(&credentials.database)
            || !valid_identifier(&credentials.user)
            || credentials.password.is_empty()
            || credentials.password.len() > 4096
            || credentials.password.chars().any(char::is_control)
            || !credentials.ca_pem.is_absolute()
            || !credentials.ca_pem.is_file()
        {
            return Err("invalid IPTV HA database connection settings".into());
        }
        Ok(Self {
            path: path.to_owned(),
            cluster_id,
            credentials,
        })
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaError {
    InvalidConfig,
    Unavailable,
    UnsupportedSchema,
    WrongCluster,
    StaleEpoch,
}

impl std::fmt::Display for HaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidConfig => "invalid IPTV HA database configuration",
            Self::Unavailable => "IPTV HA database unavailable",
            Self::UnsupportedSchema => "unsupported IPTV HA database schema",
            Self::WrongCluster => "IPTV HA database cluster mismatch",
            Self::StaleEpoch => "IPTV HA database epoch moved backwards",
        };
        f.write_str(message)
    }
}
impl std::error::Error for HaError {}

pub struct HaPool {
    config: tokio_postgres::Config,
    tls: Arc<tokio_rustls::rustls::ClientConfig>,
    cluster_id: Uuid,
    available: Mutex<Vec<Client>>,
    slots: Semaphore,
    seen_epoch: AtomicI64,
}

pub struct HaConnection<'a> {
    pool: &'a HaPool,
    _slot: SemaphorePermit<'a>,
    client: Option<Client>,
}

impl HaPool {
    pub fn new(options: &HaOptions) -> Result<Self, HaError> {
        let mut config = tokio_postgres::Config::new();
        config.host(&options.credentials.host);
        config.port(options.credentials.port);
        config.dbname(&options.credentials.database);
        config.user(&options.credentials.user);
        config.password(&options.credentials.password);
        config.application_name("flussonix-iptv-ha");
        config.ssl_mode(SslMode::Require);
        config.connect_timeout(DATABASE_TIMEOUT);
        let tls = crate::tls_input::client(Some(&options.credentials.ca_pem))
            .map_err(|_| HaError::InvalidConfig)?;
        Ok(Self {
            config,
            tls,
            cluster_id: options.cluster_id,
            available: Mutex::new(Vec::new()),
            slots: Semaphore::new(MAX_CONNECTIONS),
            seen_epoch: AtomicI64::new(0),
        })
    }

    pub async fn acquire(&self) -> Result<HaConnection<'_>, HaError> {
        let slot = tokio::time::timeout(DATABASE_TIMEOUT, self.slots.acquire())
            .await
            .map_err(|_| HaError::Unavailable)?
            .map_err(|_| HaError::Unavailable)?;
        let idle = self.available.lock().unwrap().pop();
        let client = if let Some(client) = idle {
            if tokio::time::timeout(DATABASE_TIMEOUT, client.check_connection())
                .await
                .is_ok_and(|result| result.is_ok())
            {
                client
            } else {
                self.connect().await?
            }
        } else {
            self.connect().await?
        };
        Ok(HaConnection {
            pool: self,
            _slot: slot,
            client: Some(client),
        })
    }

    async fn connect(&self) -> Result<Client, HaError> {
        let tls = MakeRustlsConnect::new((*self.tls).clone());
        let (client, connection) = tokio::time::timeout(DATABASE_TIMEOUT, self.config.connect(tls))
            .await
            .map_err(|_| HaError::Unavailable)?
            .map_err(|_| HaError::Unavailable)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    pub async fn verify_startup(&self) -> Result<(), HaError> {
        let client = self.acquire().await?;
        let row = tokio::time::timeout(
            DATABASE_TIMEOUT,
            client.query_one(
                "SELECT pg_is_in_recovery(), current_setting('transaction_read_only')",
                &[],
            ),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
        .map_err(|_| HaError::Unavailable)?;
        if row.get::<_, bool>(0) || row.get::<_, String>(1) != "off" {
            return Err(HaError::Unavailable);
        }
        let row = tokio::time::timeout(
            DATABASE_TIMEOUT,
            client.query_opt(
                "SELECT cluster_id, schema_version, epoch FROM fx_iptv_meta LIMIT 1",
                &[],
            ),
        )
        .await
        .map_err(|_| HaError::Unavailable)?
        .map_err(|error| {
            if error.code().is_some_and(|code| code.code() == "42P01") {
                HaError::UnsupportedSchema
            } else {
                HaError::Unavailable
            }
        })?
        .ok_or(HaError::UnsupportedSchema)?;
        let cluster: String = row.try_get(0).map_err(|_| HaError::UnsupportedSchema)?;
        let version: i32 = row.try_get(1).map_err(|_| HaError::UnsupportedSchema)?;
        let epoch: i64 = row.try_get(2).map_err(|_| HaError::UnsupportedSchema)?;
        validate_metadata(self.cluster_id, &self.seen_epoch, &cluster, version, epoch)
    }
}

fn validate_metadata(
    expected: Uuid,
    seen_epoch: &AtomicI64,
    cluster: &str,
    version: i32,
    epoch: i64,
) -> Result<(), HaError> {
    if version != 1 || epoch < 1 {
        return Err(HaError::UnsupportedSchema);
    }
    if Uuid::parse_str(cluster).ok() != Some(expected) {
        return Err(HaError::WrongCluster);
    }
    let previous = seen_epoch.fetch_max(epoch, Ordering::AcqRel);
    if epoch < previous {
        return Err(HaError::StaleEpoch);
    }
    Ok(())
}

impl Deref for HaConnection<'_> {
    type Target = Client;
    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}

impl DerefMut for HaConnection<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}

impl Drop for HaConnection<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.available.lock().unwrap().push(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_metadata_rejects_wrong_cluster_and_replayed_epoch() {
        let expected = Uuid::parse_str("a3192376-6df5-4b76-a15c-43942fd3d466").unwrap();
        let other = "19b3774a-3879-461d-a89f-a6de5461abb8";
        let seen = AtomicI64::new(0);
        assert_eq!(
            validate_metadata(expected, &seen, other, 1, 1),
            Err(HaError::WrongCluster)
        );
        assert_eq!(
            validate_metadata(expected, &seen, &expected.to_string(), 2, 1),
            Err(HaError::UnsupportedSchema)
        );
        assert_eq!(
            validate_metadata(expected, &seen, &expected.to_string(), 1, 2),
            Ok(())
        );
        assert_eq!(
            validate_metadata(expected, &seen, &expected.to_string(), 1, 1),
            Err(HaError::StaleEpoch)
        );
    }
}
