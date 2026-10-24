mod diagnostic;
mod stream_watch;
mod text;
pub(crate) mod vendor_peers;
mod vendor_sources;
pub(crate) use diagnostic::nested_validation_path;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
pub(crate) use stream_watch::StreamWatch;
pub(crate) use text::parse_text;

pub const KINDS: &[&str] = &["streams", "templates", "peers", "sources", "auth_backends"];
const INPUT_FIELDS: &[&str] = &[
    "url",
    "rtp",
    "flussonix_tls_ca",
    "flussonix_rtp",
    "flussonix_fxlink",
    "flussonix_fxlink_token",
    "flussonix_rtsp_startup",
];
const INPUT_FIELDS_DETAIL: &str = "supported input fields: url, RTSP rtp=udp and flussonix_rtsp_startup, verified TLS flussonix_tls_ca, direct RTP/SRTP flussonix_rtp, FXLink flussonix_fxlink/token";

pub fn merge(old: &Value, patch: &Value) -> Value {
    if let Some(p) = patch.as_object() {
        let mut result = if p.get("$reset") == Some(&Value::Bool(true)) {
            serde_json::Map::new()
        } else {
            old.as_object().cloned().unwrap_or_default()
        };
        for (k, v) in p {
            if k == "$reset" {
                continue;
            }
            if v.is_null() {
                result.remove(k);
            } else {
                result.insert(k.clone(), merge(result.get(k).unwrap_or(&Value::Null), v));
            }
        }
        Value::Object(result)
    } else {
        patch.clone()
    }
}

pub struct ConfigStore {
    path: PathBuf,
    data: Mutex<Value>,
    // A source relationship changes identity at the successful save boundary,
    // including delete/re-add between two reads of the final configuration.
    source_lineages: Mutex<HashMap<String, Arc<()>>>,
    revision: AtomicU64,
    stream_watches: stream_watch::Watches,
}
impl ConfigStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_owned();
        let mut data = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            json!({"streams":[],"templates":[],"peers":[],"sources":[],"auth_backends":[]})
        };
        normalize_sources(&mut data)?;
        validate_root(&data)?;
        let source_lineages = data["sources"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|source| item_key("sources", source))
            .map(|key| (key.to_owned(), Arc::new(())))
            .collect();
        Ok(Self {
            path,
            data: Mutex::new(data),
            source_lineages: Mutex::new(source_lineages),
            revision: AtomicU64::new(0),
            stream_watches: Default::default(),
        })
    }
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    /// Hold the config lock while installing a resolved policy, so a stale fetch cannot publish.
    pub fn at_revision<T>(&self, revision: u64, read: impl FnOnce(&Value) -> T) -> Option<T> {
        let root = self.data.lock().unwrap();
        (self.revision() == revision).then(|| read(&root))
    }
    pub fn read<T>(&self, read: impl FnOnce(&Value) -> T) -> T {
        read(&self.data.lock().unwrap())
    }
    pub fn snapshot(&self) -> Value {
        self.data.lock().unwrap().clone()
    }
    /// Call while reading the configuration to bind an exact source
    /// relationship to its successful edit history.
    pub(crate) fn source_lineage(&self, key: &str) -> Option<Arc<()>> {
        self.source_lineages.lock().unwrap().get(key).cloned()
    }
    fn refresh_source_lineages(&self, current: &Value, next: &Value) {
        let mut lineages = self.source_lineages.lock().unwrap();
        let mut updated = HashMap::new();
        let previous: HashMap<&str, &Value> = current["sources"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|source| item_key("sources", source).map(|key| (key, source)))
            .collect();
        let backends_same = current["auth_backends"] == next["auth_backends"];
        let templates_same = current["templates"] == next["templates"];
        if let Some(sources) = next["sources"].as_array() {
            for source in sources {
                let Some(key) = item_key("sources", source) else {
                    continue;
                };
                let same_relationship = previous.get(key).is_some_and(|old| {
                    *old == source
                        && backends_same
                        && (templates_same
                            || crate::vendor_output::resolve(current, old).ok()
                                == crate::vendor_output::resolve(next, source).ok())
                });
                let lineage = if same_relationship {
                    lineages.get(key).cloned().unwrap_or_else(|| Arc::new(()))
                } else {
                    Arc::new(())
                };
                updated.insert(key.to_owned(), lineage);
            }
        }
        *lineages = updated;
    }
    pub fn validate(&self, patch: Value) -> Result<Value, String> {
        let mut next = merge(&self.snapshot(), &patch);
        normalize_sources(&mut next)?;
        validate_root(&next)?;
        Ok(next)
    }
    fn save(&self, value: &Value) -> Result<(), String> {
        let parent = self.path.parent().unwrap_or(Path::new("."));
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let tmp = parent.join(format!(".config-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut f = options.open(&tmp)?;
            f.write_all(&serde_json::to_vec_pretty(value)?)?;
            f.sync_all()?;
            fs::rename(&tmp, &self.path)?;
            Ok::<(), std::io::Error>(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(tmp);
        }
        result.map_err(|e| e.to_string())
    }
    pub fn replace(&self, patch: Value) -> Result<Value, String> {
        let mut data = self.data.lock().unwrap();
        let mut next = merge(&data, &patch);
        normalize_sources(&mut next)?;
        validate_root(&next)?;
        self.save(&next)?;
        self.retire_stream_watches(&next);
        self.refresh_source_lineages(&data, &next);
        *data = next.clone();
        self.revision.fetch_add(1, Ordering::Release);
        Ok(next)
    }
    pub fn put(&self, kind: &str, name: &str, patch: Value) -> Result<Value, String> {
        if !KINDS.contains(&kind) {
            return Err("unknown collection".into());
        }
        valid_key(kind, name)?;
        if !patch.is_object() {
            return Err("configuration must be an object".into());
        }
        if kind == "sources"
            && name.contains("://")
            && patch
                .get("url")
                .is_some_and(|v| !v.is_null() && v.as_str() != Some(name))
        {
            return Err("source URL must match the request identity".into());
        }
        let mut data = self.data.lock().unwrap();
        let mut next = data.clone();
        let items = next[kind]
            .as_array_mut()
            .ok_or("collection must be an array")?;
        let index = items.iter().position(|x| item_key(kind, x) == Some(name));
        let old = index.map(|i| items[i].clone()).unwrap_or(json!({}));
        let mut item = merge(&old, &patch);
        item[if kind == "sources" && name.contains("://") {
            "url"
        } else if matches!(kind, "peers" | "sources") {
            "hostname"
        } else {
            "name"
        }] = json!(name);
        if let Some(i) = index {
            items[i] = item.clone()
        } else {
            items.push(item.clone())
        }
        normalize_sources(&mut next)?;
        validate_root(&next)?;
        self.save(&next)?;
        self.retire_stream_watches(&next);
        self.refresh_source_lineages(&data, &next);
        let item = next[kind]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| item_key(kind, row) == Some(name))
            .unwrap()
            .clone();
        *data = next;
        self.revision.fetch_add(1, Ordering::Release);
        Ok(item)
    }
    pub fn delete(&self, kind: &str, name: &str) -> Result<bool, String> {
        if !KINDS.contains(&kind) {
            return Err("unknown collection".into());
        }
        let mut data = self.data.lock().unwrap();
        let mut next = data.clone();
        let items = next[kind]
            .as_array_mut()
            .ok_or("collection must be an array")?;
        let before = items.len();
        items.retain(|x| item_key(kind, x) != Some(name));
        let removed = items.len() != before;
        validate_root(&next)?;
        self.save(&next)?;
        self.retire_stream_watches(&next);
        self.refresh_source_lineages(&data, &next);
        *data = next;
        self.revision.fetch_add(1, Ordering::Release);
        Ok(removed)
    }
    pub fn effective(&self, name: &str) -> Option<Value> {
        effective(&self.snapshot(), name)
    }
}
pub fn effective(root: &Value, name: &str) -> Option<Value> {
    let disk = root["streams"]
        .as_array()?
        .iter()
        .find(|s| s["name"] == name)?;
    let mut result = json!({"static":true,"disabled":false});
    if let Some(template) = disk["template"].as_str() {
        if let Some(t) = root["templates"]
            .as_array()?
            .iter()
            .find(|t| t["name"] == template)
        {
            result = merge(&result, t);
            // Legacy templates implicitly encode H.264. Resolve that default
            // before audio-only stream overrides change the object's shape.
            if result["transcoder"].as_object().is_some_and(|profile| {
                !profile.contains_key("encoder")
                    && (profile.contains_key("vb") || profile.is_empty())
            }) {
                result["transcoder"]["encoder"] = json!("libx264");
            }
        }
    }
    // Remove inherited hardware-dependent settings when a stream changes family
    // or rate control. Explicit incompatible fields remain and are rejected.
    if let Some(t) = result.get_mut("transcoder").and_then(Value::as_object_mut) {
        let override_t = &disk["transcoder"];
        if let Some(encoder) = override_t["encoder"].as_str() {
            if !matches!(encoder, "h264_vaapi" | "hevc_vaapi") {
                for key in ["vaapi_device", "low_power", "vaapi_rc", "qp"] {
                    t.remove(key);
                }
            } else if override_t["vaapi_rc"].as_str() != Some("cbr")
                && t.get("vaapi_rc").and_then(Value::as_str) != Some("cbr")
            {
                t.remove("vb");
            }
        }
        match override_t["vaapi_rc"].as_str() {
            Some("cbr") => {
                t.remove("qp");
            }
            Some("cqp") => {
                t.remove("vb");
            }
            _ => {}
        }
    }
    result = merge(&result, disk);
    result["config_on_disk"] = disk.clone();
    Some(result)
}
/// Separate subtitle track policy; embedded video captions are unaffected.
pub fn subtitle_tracks(cfg: &Value) -> Result<&'static str, String> {
    match cfg.get("flussonix_subtitle_tracks") {
        None => Ok("drop"),
        Some(Value::String(value)) if value == "preserve" => Ok("preserve"),
        Some(Value::String(value)) if value == "drop" => Ok("drop"),
        _ => Err("flussonix_subtitle_tracks must be preserve or drop".into()),
    }
}

/// HLS-only subtitle mode; older caption rows imply conversion.
pub fn hls_subtitles(cfg: &Value) -> Result<&'static str, String> {
    match cfg.get("flussonix_hls_subtitles") {
        None => Ok(
            if cfg["flussonix_hls_captions"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
            {
                "convert"
            } else {
                "passthrough"
            },
        ),
        Some(Value::String(v)) => match v.as_str() {
            "passthrough" => Ok("passthrough"),
            "convert" => Ok("convert"),
            "drop" => Ok("drop"),
            _ => Err("HLS subtitle mode must be passthrough, convert or drop".into()),
        },
        _ => Err("HLS subtitle mode must be passthrough, convert or drop".into()),
    }
}
/// Stable collection identity; URL-keyed sources coexist with existing native aliases.
pub fn item_key<'a>(kind: &str, item: &'a Value) -> Option<&'a str> {
    match kind {
        "sources" => item.get("url").or_else(|| item.get("hostname"))?.as_str(),
        "peers" => item["hostname"].as_str(),
        _ => item["name"].as_str(),
    }
}
fn valid_key(kind: &str, key: &str) -> Result<(), String> {
    if kind == "sources" && key.contains("://") {
        source_address(key).map(|_| ())
    } else {
        valid_name(key)
    }
}
/// A source identity is a server root, never a stream path or credential carrier.
pub(crate) fn source_address(raw: &str) -> Result<(String, &'static str), String> {
    let invalid = || {
        "source URL requires m4f/m4fs/m4s/m4ss://HOST[:PORT], without credentials, path, query or fragment".to_string()
    };
    if raw.len() > 4096
        || raw.bytes().any(|b| !(0x21..=0x7e).contains(&b))
        || raw.contains(['@', '?', '#', '\\', '%'])
    {
        return Err(invalid());
    }
    let (scheme, authority) = raw.split_once("://").ok_or_else(invalid)?;
    let (http, transport) = match scheme {
        "m4f" => ("http", "m4f"),
        "m4fs" => ("https", "m4f"),
        "m4s" => ("http", "m4s"),
        "m4ss" => ("https", "m4s"),
        _ => return Err(invalid()),
    };
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.is_empty() || authority.contains('/') || authority.ends_with(':') {
        return Err(invalid());
    }
    let endpoint = url::Url::parse(&format!("{http}://{authority}")).map_err(|_| invalid())?;
    if endpoint.host_str().is_none() || endpoint.port() == Some(0) || endpoint.path() != "/" {
        return Err(invalid());
    }
    Ok((endpoint.to_string(), transport))
}
fn normalize_sources(root: &mut Value) -> Result<(), String> {
    for source in root
        .get_mut("sources")
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        if let Some(raw) = source.get("url") {
            let (api, transport) =
                source_address(raw.as_str().ok_or("source URL must be a string")?)?;
            let row = source.as_object_mut().ok_or("source must be an object")?;
            row.entry("api_url").or_insert(json!(api));
            row.entry("flussonix_transport").or_insert(json!(transport));
        }
    }
    Ok(())
}

pub fn valid_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 256
        || name
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
        || name
            .chars()
            .any(|c| c.is_control() || matches!(c, '\\' | '?' | '#'))
    {
        return Err("invalid name".into());
    }
    Ok(())
}
pub fn valid_identity(value: &Value) -> Result<(), String> {
    if value.as_str().is_some_and(|id| {
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    }) {
        Ok(())
    } else {
        Err("identity must contain 1..128 ASCII letters, digits, dot, underscore or hyphen".into())
    }
}
fn validate_root(root: &Value) -> Result<(), String> {
    if !root.is_object() {
        return Err("configuration must be an object".into());
    }
    for key in root.as_object().unwrap().keys() {
        if !KINDS.contains(&key.as_str()) {
            return Err(format!(
                "server option {key} is not implemented; listener/auth/limits are startup options"
            ));
        }
    }
    if root["sources"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| {
            !s.get("only")
                .is_some_and(|v| v.as_array().is_some_and(Vec::is_empty))
        })
        .count()
        > 64
    {
        return Err(
            "at most 64 sources may keep streams active; set other sources to On demand".into(),
        );
    }
    vendor_sources::validate_count(root)?;
    let mut groups = std::collections::HashMap::new();
    for source in root["sources"].as_array().into_iter().flatten() {
        if let Some(group) = source["flussonix_source_group"].as_str() {
            let count = groups.entry(group).or_insert(0usize);
            *count += 1;
            if *count > 8 {
                return Err("a failover group supports at most eight sources".into());
            }
        }
    }
    for kind in KINDS {
        let items = root[*kind]
            .as_array()
            .ok_or_else(|| format!("{kind} must be an array"))?;
        let mut names = std::collections::HashSet::new();
        for item in items {
            let name = item_key(kind, item).ok_or("missing collection identity")?;
            if *kind == "sources" && item.get("url").is_some() {
                source_address(name)?;
            } else {
                valid_name(name)?;
            }
            if *kind == "sources" && item.get("url").is_some() && item.get("hostname").is_some() {
                return Err("source requires either url or hostname, not both".into());
            }
            if !names.insert(name) {
                return Err("duplicate name".into());
            }
            let allowed: &[&str] = match *kind {
                "streams" | "templates" => &[
                    "name",
                    "title",
                    "comment",
                    "position",
                    "template",
                    "static",
                    "disabled",
                    "inputs",
                    "pushes",
                    "flussonix_rtp_outputs",
                    "transcoder",
                    "on_play",
                    "on_publish",
                    "password",
                    "flussonix_token_sha256",
                    "flussonix_input_timeout",
                    "flussonix_peer_key",
                    "flussonix_allow_insecure_peer_http",
                    "flussonix_subtitle_tracks",
                    "flussonix_hls_captions",
                    "flussonix_hls_subtitles",
                    "flussonix_content_id",
                ],
                "peers" | "sources" => &[
                    "hostname",
                    "url",
                    "api_url",
                    "public_payload_url",
                    "flussonix_rtsp_url",
                    "flussonix_rtsps_url",
                    "flussonix_srt_url",
                    "flussonix_srt_encrypted",
                    "flussonix_peer_kind",
                    "flussonix_stream_map",
                    "private_payload_url",
                    "cluster_key",
                    "flussonix_allow_insecure_peer_http",
                    "drain",
                    "flussonix_transport",
                    "flussonix_fxlink_url",
                    "flussonix_fxlink",
                    "flussonix_fxlink_token",
                    "flussonix_source_group",
                    "prefix",
                    "except",
                    "only",
                    "flussonix_tls_ca",
                    "flussonix_media_tls_ca",
                ],
                "auth_backends" => &["name", "url"],
                _ => &[],
            };
            for key in item.as_object().ok_or("item must be an object")?.keys() {
                if !allowed.contains(&key.as_str())
                    && !(*kind == "sources" && vendor_sources::FIELDS.contains(&key.as_str()))
                {
                    return Err(format!("{kind} option {key} is not implemented"));
                }
            }
            if item
                .get("flussonix_allow_insecure_peer_http")
                .is_some_and(|value| !value.is_boolean())
            {
                return Err("flussonix_allow_insecure_peer_http must be boolean".into());
            }
            if *kind == "sources" {
                vendor_sources::validate(item, root)?;
                if item.get("flussonix_peer_kind").is_some()
                    || item.get("flussonix_stream_map").is_some()
                    || item.get("flussonix_srt_url").is_some()
                    || item.get("flussonix_srt_encrypted").is_some()
                {
                    return Err("destination type and mappings are peer-only".into());
                }
            }
            if *kind == "peers" {
                vendor_peers::validate(item)?;
            }
            for (field, scheme) in [
                ("flussonix_rtsp_url", "rtsp"),
                ("flussonix_rtsps_url", "rtsps"),
            ] {
                if let Some(value) = item.get(field) {
                    if *kind != "peers" {
                        return Err(format!("{field} is peer-only"));
                    }
                    let url = value
                        .as_str()
                        .and_then(crate::rtsp::redirect::destination)
                        .ok_or_else(|| format!("{field} requires a safe {scheme} listener URL"))?;
                    if url.scheme() != scheme
                        || !matches!(url.path(), "" | "/")
                        || url.query().is_some()
                        || url.port() == Some(0)
                    {
                        return Err(format!(
                            "{field} requires a {scheme} listener root without query or path prefix"
                        ));
                    }
                }
            }
            if *kind == "peers" && item.get("url").is_some() {
                return Err("source URL identity is source-only".into());
            }
            if let Some(id) = item.get("flussonix_content_id") {
                valid_identity(id)?;
            }
            if let Some(prefix) = item.get("prefix") {
                let prefix = prefix
                    .as_str()
                    .ok_or("source prefix must be a name string")?;
                if *kind != "sources" || prefix.contains('*') || valid_name(prefix).is_err() {
                    return Err(
                        "source prefix requires a valid name without wildcards or a trailing slash"
                            .into(),
                    );
                }
            }
            for field in ["except", "only"] {
                if let Some(except) = item.get(field) {
                    if *kind != "sources" {
                        return Err(format!("{field} is source-only"));
                    }
                    let patterns =
                        except
                            .as_array()
                            .filter(|p| p.len() <= 1024)
                            .ok_or_else(|| {
                                format!("{field} must be an array of at most 1024 stream patterns")
                            })?;
                    for pattern in patterns {
                        let pattern = pattern
                            .as_str()
                            .ok_or_else(|| format!("{field} patterns must be strings"))?;
                        let name = pattern.strip_suffix("/*").unwrap_or(pattern);
                        if pattern.len() > 256 || name.contains('*') || valid_name(name).is_err() {
                            return Err(format!(
                                "{field} requires exact stream names or prefix/* patterns"
                            ));
                        }
                    }
                }
            }
            if let Some(group) = item.get("flussonix_source_group") {
                if *kind != "sources" {
                    return Err("flussonix_source_group is source-only".into());
                }
                valid_identity(group)?;
            }
            if matches!(*kind, "peers" | "sources" | "auth_backends") {
                for field in [
                    "api_url",
                    "public_payload_url",
                    "private_payload_url",
                    "url",
                ] {
                    if *kind == "sources" && field == "url" {
                        continue;
                    }
                    if let Some(v) = item.get(field) {
                        let url =
                            url::Url::parse(v.as_str().ok_or("endpoint must be a URL string")?)
                                .map_err(|_| "invalid endpoint URL")?;
                        if !["http", "https"].contains(&url.scheme()) || url.host_str().is_none() {
                            return Err("endpoint must use HTTP(S)".into());
                        }
                    }
                }
                if let Some(key) = item.get("cluster_key") {
                    if !key
                        .as_str()
                        .is_some_and(|k| k.len() >= 12 && !k.contains(['\r', '\n']))
                    {
                        return Err("cluster_key must be a string of at least 12 characters".into());
                    }
                }
                for (field, endpoint) in [
                    ("flussonix_tls_ca", item["api_url"].as_str()),
                    (
                        "flussonix_media_tls_ca",
                        item["private_payload_url"]
                            .as_str()
                            .or(item["api_url"].as_str()),
                    ),
                ] {
                    if let Some(ca) = item.get(field) {
                        if field == "flussonix_media_tls_ca" && *kind != "sources" {
                            return Err("private media CA is source-only".into());
                        }
                        if !(field == "flussonix_media_tls_ca"
                            && item["flussonix_transport"] == "fxlink")
                            && !endpoint
                                .and_then(|u| url::Url::parse(u).ok())
                                .is_some_and(|u| u.scheme() == "https")
                        {
                            return Err("cluster CA requires an HTTPS endpoint".into());
                        }
                        let path = ca.as_str().ok_or("TLS CA must be an absolute PEM path")?;
                        crate::tls_input::client(Some(std::path::Path::new(path)))?;
                    }
                }
                let fxlink_source = *kind == "sources" && item["flussonix_transport"] == "fxlink";
                let fxlink_fields = [
                    "flussonix_fxlink_url",
                    "flussonix_fxlink",
                    "flussonix_fxlink_token",
                ];
                if fxlink_source {
                    if !crate::source_directory::native_source(item) || item.get("url").is_some() {
                        return Err(
                            "FXLink requires a named native source with explicit management URL"
                                .into(),
                        );
                    }
                    let endpoint = item["flussonix_fxlink_url"]
                        .as_str()
                        .ok_or("FXLink source requires an explicit UDP endpoint")?;
                    let address = crate::fxlink::Address::parse(endpoint, false)
                        .map_err(|e| e.to_string())?;
                    if address.stream.is_some() {
                        return Err("FXLink source endpoint must omit the stream name".into());
                    }
                    fxlink_settings(item)?;
                    fxlink_token(item)?;
                } else if fxlink_fields.iter().any(|field| item.get(field).is_some()) {
                    return Err("FXLink source controls require a native FXLink source".into());
                }
                if item.get("drain").is_some_and(|v| !v.is_boolean()) {
                    return Err("drain must be boolean".into());
                }
                if let Some(transport) = item.get("flussonix_transport") {
                    if *kind != "sources"
                        || !transport
                            .as_str()
                            .is_some_and(|t| ["hls", "m4s", "m4f", "mpegts", "fxlink"].contains(&t))
                    {
                        return Err(
                            "flussonix_transport is source-only: hls, m4s, m4f, mpegts or fxlink"
                                .into(),
                        );
                    }
                }
                if *kind != "auth_backends" && item["api_url"].as_str().is_none() {
                    return Err("api_url required".into());
                }
                if *kind == "auth_backends" && item["url"].as_str().is_none() {
                    return Err("auth backend URL required".into());
                }
            }
            if matches!(*kind, "streams" | "templates") {
                if let Some(template) = item.get("template") {
                    let template = template.as_str().ok_or("template must be a name string")?;
                    valid_name(template)?;
                    if *kind == "templates" {
                        return Err("nested templates are not implemented".into());
                    }
                }
                if let Some(timeout) = item.get("flussonix_input_timeout") {
                    if !timeout
                        .as_u64()
                        .is_some_and(|value| (1..=300).contains(&value))
                    {
                        return Err(
                            "flussonix_input_timeout must be an integer from 1 to 300 seconds"
                                .into(),
                        );
                    }
                }
                subtitle_tracks(item)?;
                hls_subtitles(item)?;
                crate::captions::configuration(item)?;
                crate::playback_auth::Policy::from_config(item, root)?;
                crate::publish::Policy::from_config(item, root)?;
                if let Some(hash) = item.get("flussonix_token_sha256") {
                    if !hash
                        .as_str()
                        .is_some_and(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()))
                    {
                        return Err(
                            "flussonix_token_sha256 must be a 64-digit SHA256 hex string".into(),
                        );
                    }
                }
                for flag in ["static", "disabled"] {
                    if let Some(v) = item.get(flag) {
                        if !v.is_boolean() {
                            return Err(format!("{flag} must be boolean"));
                        }
                    }
                }
                if let Some(key) = item.get("flussonix_peer_key") {
                    let value = key.as_str().ok_or("native peer key must be a string")?;
                    if value.len() < 12 || value.len() > 4096 || value.chars().any(char::is_control)
                    {
                        return Err(
                            "native peer key must contain 12 to 4096 non-control bytes".into()
                        );
                    }
                }
                if let Some(inputs) = item.get("inputs") {
                    let inputs = inputs.as_array().ok_or("inputs must be array")?;
                    for input in inputs {
                        if input
                            .as_object()
                            .is_none_or(|v| v.keys().any(|k| !INPUT_FIELDS.contains(&k.as_str())))
                        {
                            return Err(INPUT_FIELDS_DETAIL.into());
                        }
                        let u = input["url"].as_str().ok_or("input url required")?;
                        let scheme = u.split("://").next().unwrap_or("");
                        crate::direct_rtp::config::Settings::input(input)?;
                        if scheme == "srt" {
                            crate::srt_input::Input::parse(u)?;
                        }
                        if ["rtsp", "rtsp-udp", "rtsp2", "rtsps"].contains(&scheme) {
                            let target = if scheme == "rtsps" {
                                u.to_owned()
                            } else {
                                crate::media::translate_input(u)?
                            };
                            crate::media::validate_rtsp_input_url(&target)?;
                        }
                        if let Some(mode) = input.get("flussonix_rtsp_startup") {
                            if !["rtsp", "rtsp-udp", "rtsp2", "rtsps"].contains(&scheme)
                                || !matches!(mode.as_str(), Some("h264" | "conservative"))
                            {
                                return Err("RTSP startup mode requires an RTSP input and value h264 or conservative".into());
                            }
                        }
                        if scheme == "fxlink" {
                            crate::fxlink::Address::parse(u, true).map_err(|e| e.to_string())?;
                            fxlink_settings(input)?;
                            fxlink_token(input)?;
                        } else if input.get("flussonix_fxlink").is_some()
                            || input.get("flussonix_fxlink_token").is_some()
                        {
                            return Err("FXLink controls require an fxlink input".into());
                        }
                        if scheme == "publish"
                            && (u != "publish://"
                                || inputs.len() != 1
                                || input.as_object().unwrap().len() != 1)
                        {
                            return Err(
                                "publish:// must be the exact sole input without options".into()
                            );
                        }
                        if input.get("rtp").is_some()
                            && (!["rtsp", "rtsp-udp", "rtsp2"].contains(&scheme)
                                || input["rtp"] != "udp")
                        {
                            return Err("rtp input option requires RTSP and value udp".into());
                        }
                        if let Some(ca) = input.get("flussonix_tls_ca") {
                            if ![
                                "rtsps", "m4fs", "m4ss", "hlss", "tshttps", "https", "fxlink",
                            ]
                            .contains(&scheme)
                            {
                                return Err(
                                    "TLS CA input option requires RTSPS, M4FS, M4SS, HLSS, TSHTTPS, HTTPS or FXLink".into()
                                );
                            }
                            let path = ca.as_str().ok_or("TLS CA must be an absolute PEM path")?;
                            crate::tls_input::client(Some(std::path::Path::new(path)))?;
                        }
                        if ["hlss", "tshttps", "https"].contains(&scheme) {
                            let mut parsed = url::Url::parse(&crate::media::translate_input(u)?)
                                .map_err(|_| "invalid HTTPS input URL")?;
                            if parsed.host_str().is_none() || parsed.fragment().is_some() {
                                return Err(
                                    "HTTPS inputs require a host and forbid fragments".into()
                                );
                            }
                            // Match the origin-scoped fetcher's Basic profile without
                            // removing userinfo from the persisted input configuration.
                            crate::http_basic::take_url_credentials(&mut parsed)?;
                        }
                        if ![
                            "testsrc", "http", "https", "hls", "hlss", "tshttp", "tshttps", "rtsp",
                            "rtsps", "rtsp-udp", "rtsp2", "srt", "m4s", "m4ss", "m4f", "m4fs",
                            "publish", "rtp", "srtp", "fxlink",
                        ]
                        .contains(&scheme)
                        {
                            return Err(format!("unsupported input protocol: {scheme}"));
                        }
                    }
                }
                crate::transcoder::Profile::validate_partial(item)?;
                crate::push::configuration(item)?;
                crate::direct_rtp::config::outputs(item)?;
                if item.get("dvr").is_some() {
                    return Err("dvr is not implemented in this build".into());
                }
            }
        }
    }
    for s in root["streams"].as_array().unwrap() {
        if let Some(effective) = effective(root, s["name"].as_str().unwrap()) {
            crate::transcoder::Profile::resolve(&effective, false)?;
            if effective["inputs"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|input| {
                    input["url"]
                        .as_str()
                        .is_some_and(|url| url.starts_with("fxlink://"))
                })
                && !effective["flussonix_peer_key"].is_string()
            {
                return Err(
                    "FXLink input requires a native peer key on the stream or template".into(),
                );
            }
        }
        if let Some(t) = s["template"].as_str() {
            if !root["templates"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["name"] == t)
            {
                return Err(format!("template {t} not found"));
            }
        }
    }
    let templates = root["templates"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|template| Some((template["name"].as_str()?, template)))
        .collect::<HashMap<_, _>>();
    for stream in root["streams"].as_array().unwrap() {
        let resolved = stream["template"]
            .as_str()
            .and_then(|name| templates.get(name))
            .map_or_else(|| stream.clone(), |template| merge(template, stream));
        if crate::iptv::tokenless_output_conflict(&resolved) {
            return Err(crate::iptv::TOKENLESS_OUTPUT_ERROR.into());
        }
    }
    vendor_sources::validate_groups(root)?;
    Ok(())
}

/// Only operator-facing FXLink controls may override negotiated defaults.
pub(crate) fn fxlink_settings(input: &Value) -> Result<crate::fxlink::Settings, String> {
    let mut settings = crate::fxlink::Settings::default();
    if let Some(value) = input.get("flussonix_fxlink") {
        let fields = value
            .as_object()
            .ok_or("FXLink settings must be an object")?;
        for (key, value) in fields {
            let n = value
                .as_u64()
                .ok_or("FXLink settings must be positive integers")?;
            match key.as_str() {
                "latency_ms" => settings.latency_ms = n,
                "data_shards" => {
                    settings.data_shards =
                        usize::try_from(n).map_err(|_| "invalid FXLink data count")?
                }
                "parity_shards" => {
                    settings.parity_shards =
                        usize::try_from(n).map_err(|_| "invalid FXLink parity count")?
                }
                _ => return Err("unsupported FXLink setting".into()),
            }
        }
    }
    settings.validate().map_err(|e| e.to_string())?;
    if ![(20, 2), (20, 4), (20, 6), (1, 1)]
        .contains(&(settings.data_shards, settings.parity_shards))
    {
        return Err("FXLink protection must use a supported FEC preset".into());
    }
    Ok(settings)
}

fn fxlink_token(input: &Value) -> Result<(), String> {
    if let Some(token) = input.get("flussonix_fxlink_token") {
        let value = token.as_str().ok_or("FXLink token must be a string")?;
        if value.len() > 4096 || value.chars().any(char::is_control) {
            return Err("FXLink token must contain at most 4096 non-control bytes".into());
        }
    }
    Ok(())
}
