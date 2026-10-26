use crate::config::valid_name;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PACKAGES: usize = 1_024;
const MAX_SUBSCRIBERS: usize = 100_000;
const MAX_CHANNELS: usize = 4_096;
const MAX_MEMBERSHIPS: usize = 64;
const MAX_TOKEN_BYTES: usize = 4_096;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IptvPackage {
    pub name: String,
    #[serde(default)]
    pub channels: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IptvSubscriber {
    pub name: String,
    pub token: String,
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub max_sessions: Option<u32>,
    #[serde(default)]
    pub flussonix_expires_at: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IptvDocument {
    pub version: u32,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub packages: Vec<IptvPackage>,
    #[serde(default)]
    pub subscribers: Vec<IptvSubscriber>,
}

impl Default for IptvDocument {
    fn default() -> Self {
        Self {
            version: 1,
            enabled: false,
            packages: Vec::new(),
            subscribers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IptvGrantFacts {
    pub subscriber: String,
    pub expires_at: Option<SystemTime>,
    pub max_sessions: Option<u32>,
    pub revision: u64,
}

struct SubscriberIndex {
    identity: String,
    memberships: Vec<String>,
    max_sessions: Option<u32>,
    expires_at: Option<SystemTime>,
}

pub struct IptvSnapshot {
    document: IptvDocument,
    packages: HashMap<String, HashSet<String>>,
    names: HashMap<String, usize>,
    tokens: HashMap<String, SubscriberIndex>,
    revision: u64,
}

impl IptvSnapshot {
    pub fn enabled(&self) -> bool {
        self.document.enabled
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn authorize(&self, token: &str, channel: &str, now: SystemTime) -> Option<IptvGrantFacts> {
        if !self.document.enabled {
            return None;
        }
        let subscriber = self.tokens.get(token)?;
        if !subscriber.memberships.iter().any(|package| {
            self.packages
                .get(package)
                .is_some_and(|channels| channels.contains(channel))
        }) || subscriber.expires_at.is_some_and(|expiry| now >= expiry)
        {
            return None;
        }
        Some(IptvGrantFacts {
            subscriber: subscriber.identity.clone(),
            expires_at: subscriber.expires_at,
            max_sessions: subscriber.max_sessions,
            revision: self.revision,
        })
    }

    pub(crate) fn document(&self) -> &IptvDocument {
        &self.document
    }

    pub(crate) fn subscriber(&self, name: &str) -> Option<&IptvSubscriber> {
        self.names
            .get(name)
            .map(|index| &self.document.subscribers[*index])
    }

    pub(crate) fn package_channels(&self, name: &str) -> Option<&HashSet<String>> {
        self.packages.get(name)
    }

    pub(crate) fn subscriber_token_valid(&self, name: &str, token: &str, now: SystemTime) -> bool {
        self.document.enabled
            && self.tokens.get(token).is_some_and(|subscriber| {
                subscriber.identity == name
                    && subscriber.expires_at.is_none_or(|expiry| now < expiry)
            })
    }
}

pub struct IptvStore {
    path: PathBuf,
    writer: Mutex<()>,
    active: RwLock<Arc<IptvSnapshot>>,
    on_change: Mutex<Option<ChangeHook>>,
}

type ChangeHook = Box<dyn Fn(u64) + Send + Sync>;

impl IptvStore {
    pub fn open(path: PathBuf) -> Result<Arc<Self>, String> {
        let document = read_document(&path)?.unwrap_or_default();
        let snapshot = Arc::new(index_document(document, 0)?);
        Ok(Arc::new(Self {
            path,
            writer: Mutex::new(()),
            active: RwLock::new(snapshot),
            on_change: Mutex::new(None),
        }))
    }

    pub fn snapshot(&self) -> Arc<IptvSnapshot> {
        self.active.read().unwrap().clone()
    }

    pub fn revision(&self) -> u64 {
        self.active.read().unwrap().revision
    }

    pub fn at_current<T>(&self, revision: u64, f: impl FnOnce() -> T) -> Option<T> {
        let current = self.active.read().unwrap();
        (current.revision == revision).then(f)
    }

    pub fn set_on_change(&self, callback: Box<dyn Fn(u64) + Send + Sync>) {
        *self.on_change.lock().unwrap() = Some(callback);
    }

    pub fn transact(
        &self,
        edit: impl FnOnce(&mut IptvDocument) -> Result<(), String>,
    ) -> Result<Arc<IptvSnapshot>, String> {
        let _writer = self.writer.lock().unwrap();
        let current = self.snapshot();
        let mut next_document = current.document.clone();
        edit(&mut next_document)?;
        self.commit(next_document, current.revision)
    }

    fn commit(
        &self,
        document: IptvDocument,
        current_revision: u64,
    ) -> Result<Arc<IptvSnapshot>, String> {
        let next_revision = current_revision
            .checked_add(1)
            .ok_or("IPTV revision exhausted")?;
        let next = Arc::new(index_document(document, next_revision)?);
        let bytes = serde_json::to_vec_pretty(&next.document).map_err(|_| "invalid IPTV data")?;
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err("IPTV data exceeds 64 MiB".into());
        }
        save_document(&self.path, &bytes)?;
        let mut active = self.active.write().unwrap();
        *active = next.clone();
        if let Some(callback) = self.on_change.lock().unwrap().as_ref() {
            callback(next_revision);
        }
        Ok(next)
    }

    pub fn set_enabled(&self, enabled: bool) -> Result<u64, String> {
        self.transact(|document| {
            document.enabled = enabled;
            Ok(())
        })
        .map(|snapshot| snapshot.revision)
    }

    pub fn reload_validated(&self) -> Result<u64, String> {
        let _writer = self.writer.lock().unwrap();
        let revision = self.revision();
        let mut document = match read_document(&self.path)? {
            Some(document) => document,
            None if revision == 0 => IptvDocument::default(),
            None => return Err("IPTV data file disappeared; reload refused".into()),
        };
        document.enabled = true;
        self.commit(document, revision)
            .map(|snapshot| snapshot.revision)
    }

    pub fn put_package(&self, name: &str, patch: Value) -> Result<(u64, Value), String> {
        valid_name(name)?;
        validate_patch(&patch, name, &["name", "channels"])?;
        let mut result = Value::Null;
        let snapshot = self.transact(|document| {
            let existing = document.packages.iter().position(|item| item.name == name);
            let base = existing
                .map(|index| serde_json::to_value(&document.packages[index]).unwrap())
                .unwrap_or_else(|| json!({"name":name,"channels":[]}));
            let merged = crate::config::merge(&base, &patch);
            let package: IptvPackage =
                serde_json::from_value(merged).map_err(|_| "invalid IPTV package")?;
            if package.name != name {
                return Err("IPTV package name does not match path".into());
            }
            result = serde_json::to_value(&package).map_err(|_| "invalid IPTV package")?;
            match existing {
                Some(index) => document.packages[index] = package,
                None => document.packages.push(package),
            }
            Ok(())
        })?;
        Ok((snapshot.revision, result))
    }

    pub fn delete_package(&self, name: &str) -> Result<Option<u64>, String> {
        let result = self.transact(|document| {
            let before = document.packages.len();
            document.packages.retain(|package| package.name != name);
            if before == document.packages.len() {
                return Err("IPTV item not found".into());
            }
            for subscriber in &mut document.subscribers {
                subscriber.packages.retain(|package| package != name);
            }
            Ok(())
        });
        match result {
            Ok(snapshot) => Ok(Some(snapshot.revision)),
            Err(message) if message == "IPTV item not found" => Ok(None),
            Err(message) => Err(message),
        }
    }

    pub fn put_subscriber(&self, name: &str, patch: Value) -> Result<(u64, Value), String> {
        valid_name(name)?;
        validate_patch(
            &patch,
            name,
            &[
                "name",
                "token",
                "packages",
                "max_sessions",
                "flussonix_expires_at",
            ],
        )?;
        let mut result = Value::Null;
        let snapshot = self.transact(|document| {
            let existing = document
                .subscribers
                .iter()
                .position(|item| item.name == name);
            let base = existing
                .map(|index| serde_json::to_value(&document.subscribers[index]).unwrap())
                .unwrap_or_else(|| json!({"name":name,"packages":[]}));
            let mut merged = crate::config::merge(&base, &patch);
            if merged.get("token").is_none_or(Value::is_null) {
                merged["token"] = Value::String(format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    uuid::Uuid::new_v4().simple()
                ));
            }
            let subscriber: IptvSubscriber =
                serde_json::from_value(merged).map_err(|_| "invalid IPTV subscriber")?;
            if subscriber.name != name {
                return Err("IPTV subscriber name does not match path".into());
            }
            result = serde_json::to_value(&subscriber).map_err(|_| "invalid IPTV subscriber")?;
            match existing {
                Some(index) => document.subscribers[index] = subscriber,
                None => document.subscribers.push(subscriber),
            }
            Ok(())
        })?;
        Ok((snapshot.revision, result))
    }

    pub fn delete_subscriber(&self, name: &str) -> Result<Option<u64>, String> {
        let result = self.transact(|document| {
            let before = document.subscribers.len();
            document
                .subscribers
                .retain(|subscriber| subscriber.name != name);
            if before == document.subscribers.len() {
                return Err("IPTV item not found".into());
            }
            Ok(())
        });
        match result {
            Ok(snapshot) => Ok(Some(snapshot.revision)),
            Err(message) if message == "IPTV item not found" => Ok(None),
            Err(message) => Err(message),
        }
    }
}

pub(crate) fn validate_patch(patch: &Value, name: &str, fields: &[&str]) -> Result<(), String> {
    let object = patch.as_object().ok_or("IPTV item must be an object")?;
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err("unsupported IPTV item field".into());
    }
    if object.get("name").is_some_and(|value| value != name) {
        return Err("IPTV item name does not match path".into());
    }
    Ok(())
}

pub(crate) fn index_document(
    document: IptvDocument,
    revision: u64,
) -> Result<IptvSnapshot, String> {
    if document.version != 1 {
        return Err("unsupported IPTV data version".into());
    }
    if document.packages.len() > MAX_PACKAGES || document.subscribers.len() > MAX_SUBSCRIBERS {
        return Err("IPTV collection limit exceeded".into());
    }
    let mut packages = HashMap::<String, HashSet<String>>::with_capacity(document.packages.len());
    for package in &document.packages {
        valid_name(&package.name)?;
        if package.channels.len() > MAX_CHANNELS {
            return Err("IPTV package channel limit exceeded".into());
        }
        let mut channels = HashSet::with_capacity(package.channels.len());
        for channel in &package.channels {
            valid_name(channel)?;
            if !channels.insert(channel.clone()) {
                return Err("duplicate IPTV channel".into());
            }
        }
        if packages.insert(package.name.clone(), channels).is_some() {
            return Err("duplicate IPTV package".into());
        }
    }
    let mut names = HashMap::with_capacity(document.subscribers.len());
    let mut tokens = HashMap::with_capacity(document.subscribers.len());
    for (index, subscriber) in document.subscribers.iter().enumerate() {
        valid_name(&subscriber.name)?;
        if names.insert(subscriber.name.clone(), index).is_some() {
            return Err("duplicate IPTV subscriber".into());
        }
        if subscriber.token.is_empty()
            || subscriber.token.len() > MAX_TOKEN_BYTES
            || subscriber.token.chars().any(char::is_control)
        {
            return Err("invalid IPTV subscriber token".into());
        }
        if subscriber.packages.len() > MAX_MEMBERSHIPS {
            return Err("IPTV subscriber package limit exceeded".into());
        }
        if subscriber.max_sessions == Some(0) {
            return Err("IPTV max_sessions must be positive".into());
        }
        let expires_at = subscriber
            .flussonix_expires_at
            .as_deref()
            .map(parse_expiry)
            .transpose()?;
        let mut memberships = HashSet::with_capacity(subscriber.packages.len());
        for package in &subscriber.packages {
            if !memberships.insert(package.as_str()) {
                return Err("duplicate IPTV subscriber package".into());
            }
            if !packages.contains_key(package) {
                return Err("unknown IPTV subscriber package".into());
            }
        }
        if tokens
            .insert(
                subscriber.token.clone(),
                SubscriberIndex {
                    identity: subscriber.name.clone(),
                    memberships: subscriber.packages.clone(),
                    max_sessions: subscriber.max_sessions,
                    expires_at,
                },
            )
            .is_some()
        {
            return Err("duplicate IPTV subscriber token".into());
        }
    }
    Ok(IptvSnapshot {
        document,
        packages,
        names,
        tokens,
        revision,
    })
}

fn parse_expiry(value: &str) -> Result<SystemTime, String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|_| "invalid IPTV subscriber expiry")?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err("IPTV subscriber expiry must use UTC".into());
    }
    let seconds = parsed.timestamp();
    if seconds < 0 {
        return Err("IPTV subscriber expiry precedes Unix epoch".into());
    }
    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds as u64))
        .and_then(|time| {
            time.checked_add(Duration::from_nanos(parsed.timestamp_subsec_nanos() as u64))
        })
        .ok_or_else(|| "invalid IPTV subscriber expiry".into())
}

pub(crate) fn read_document(path: &Path) -> Result<Option<IptvDocument>, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot open IPTV data file".into()),
    };
    let metadata = file.metadata().map_err(|_| "cannot stat IPTV data file")?;
    if !metadata.is_file() {
        return Err("IPTV data path is not a regular file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("IPTV data file must be owner-only (0600)".into());
        }
    }
    if metadata.len() > MAX_FILE_BYTES {
        return Err("IPTV data exceeds 64 MiB".into());
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read IPTV data file")?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err("IPTV data exceeds 64 MiB".into());
    }
    let document = serde_json::from_slice(&bytes).map_err(|_| "invalid IPTV data file")?;
    Ok(Some(document))
}

pub(crate) fn save_document(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path.file_name().ok_or("invalid IPTV data path")?;
    let tmp = parent.join(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| -> Result<(), String> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&tmp)
            .map_err(|_| "cannot create IPTV data file")?;
        file.write_all(bytes)
            .map_err(|_| "cannot write IPTV data file")?;
        file.sync_all().map_err(|_| "cannot sync IPTV data file")?;
        drop(file);
        fs::rename(&tmp, path).map_err(|_| "cannot replace IPTV data file")?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| "cannot sync IPTV data directory")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}
