use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

pub const KINDS: &[&str] = &["streams", "templates", "peers", "sources", "auth_backends"];

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
    revision: AtomicU64,
}
impl ConfigStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_owned();
        let data = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            json!({"streams":[],"templates":[],"peers":[],"sources":[],"auth_backends":[]})
        };
        validate_root(&data)?;
        Ok(Self {
            path,
            data: Mutex::new(data),
            revision: AtomicU64::new(0),
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
    pub fn validate(&self, patch: Value) -> Result<Value, String> {
        let next = merge(&self.snapshot(), &patch);
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
        let next = merge(&data, &patch);
        validate_root(&next)?;
        self.save(&next)?;
        *data = next.clone();
        self.revision.fetch_add(1, Ordering::Release);
        Ok(next)
    }
    pub fn put(&self, kind: &str, name: &str, patch: Value) -> Result<Value, String> {
        if !KINDS.contains(&kind) {
            return Err("unknown collection".into());
        }
        valid_name(name)?;
        if !patch.is_object() {
            return Err("configuration must be an object".into());
        }
        let mut data = self.data.lock().unwrap();
        let mut next = data.clone();
        let items = next[kind]
            .as_array_mut()
            .ok_or("collection must be an array")?;
        let index = items
            .iter()
            .position(|x| x["name"] == name || x["hostname"] == name);
        let old = index.map(|i| items[i].clone()).unwrap_or(json!({}));
        let mut item = merge(&old, &patch);
        item[if matches!(kind, "peers" | "sources") {
            "hostname"
        } else {
            "name"
        }] = json!(name);
        if let Some(i) = index {
            items[i] = item.clone()
        } else {
            items.push(item.clone())
        }
        validate_root(&next)?;
        self.save(&next)?;
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
        items.retain(|x| x["name"] != name && x["hostname"] != name);
        let removed = items.len() != before;
        validate_root(&next)?;
        self.save(&next)?;
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
            let name = item[if matches!(*kind, "sources" | "peers") {
                "hostname"
            } else {
                "name"
            }]
            .as_str()
            .ok_or("missing name/hostname")?;
            valid_name(name)?;
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
                    "flussonix_subtitle_tracks",
                    "flussonix_hls_captions",
                    "flussonix_hls_subtitles",
                    "flussonix_content_id",
                ],
                "peers" | "sources" => &[
                    "hostname",
                    "api_url",
                    "public_payload_url",
                    "private_payload_url",
                    "cluster_key",
                    "drain",
                    "flussonix_transport",
                    "flussonix_source_group",
                    "flussonix_tls_ca",
                    "flussonix_media_tls_ca",
                ],
                "auth_backends" => &["name", "url"],
                _ => &[],
            };
            for key in item.as_object().ok_or("item must be an object")?.keys() {
                if !allowed.contains(&key.as_str()) {
                    return Err(format!("{kind} option {key} is not implemented"));
                }
            }
            if let Some(id) = item.get("flussonix_content_id") {
                valid_identity(id)?;
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
                        if !endpoint
                            .and_then(|u| url::Url::parse(u).ok())
                            .is_some_and(|u| u.scheme() == "https")
                        {
                            return Err("cluster CA requires an HTTPS endpoint".into());
                        }
                        let path = ca.as_str().ok_or("TLS CA must be an absolute PEM path")?;
                        crate::tls_input::client(Some(std::path::Path::new(path)))?;
                    }
                }
                if item.get("drain").is_some_and(|v| !v.is_boolean()) {
                    return Err("drain must be boolean".into());
                }
                if let Some(transport) = item.get("flussonix_transport") {
                    if *kind != "sources"
                        || !transport
                            .as_str()
                            .is_some_and(|t| ["hls", "m4s", "m4f", "mpegts"].contains(&t))
                    {
                        return Err(
                            "flussonix_transport is source-only: hls, m4s, m4f or mpegts".into(),
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
                if let Some(inputs) = item.get("inputs") {
                    let inputs = inputs.as_array().ok_or("inputs must be array")?;
                    for input in inputs {
                        if input.as_object().is_none_or(|v| {
                            v.keys().any(|k| {
                                k != "url"
                                    && k != "rtp"
                                    && k != "flussonix_tls_ca"
                                    && k != "flussonix_rtp"
                            })
                        }) {
                            return Err("supported input fields: url, RTSP rtp=udp, secure HTTP/RTSP flussonix_tls_ca, and direct RTP/SRTP flussonix_rtp".into());
                        }
                        let u = input["url"].as_str().ok_or("input url required")?;
                        let scheme = u.split("://").next().unwrap_or("");
                        crate::direct_rtp::config::Settings::input(input)?;
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
                            && (!["rtsp", "rtsp-udp"].contains(&scheme) || input["rtp"] != "udp")
                        {
                            return Err("rtp input option requires RTSP and value udp".into());
                        }
                        if let Some(ca) = input.get("flussonix_tls_ca") {
                            if !["rtsps", "m4fs", "m4ss", "hlss", "tshttps", "https"]
                                .contains(&scheme)
                            {
                                return Err(
                                    "TLS CA input option requires RTSPS, M4FS, M4SS, HLSS, TSHTTPS or HTTPS".into()
                                );
                            }
                            let path = ca.as_str().ok_or("TLS CA must be an absolute PEM path")?;
                            crate::tls_input::client(Some(std::path::Path::new(path)))?;
                        }
                        if ["hlss", "tshttps", "https"].contains(&scheme) {
                            let parsed = url::Url::parse(&crate::media::translate_input(u)?)
                                .map_err(|_| "invalid HTTPS input URL")?;
                            if parsed.host_str().is_none()
                                || parsed.fragment().is_some()
                                || !parsed.username().is_empty()
                                || parsed.password().is_some()
                            {
                                return Err("HTTPS inputs require a host and forbid embedded credentials or fragments".into());
                            }
                        }
                        if scheme == "rtsps" {
                            let parsed = url::Url::parse(u).map_err(|_| "invalid RTSPS URL")?;
                            if parsed.host_str().is_none() || parsed.fragment().is_some() {
                                return Err("RTSPS host required and fragments forbidden".into());
                            }
                        }
                        if ![
                            "testsrc", "http", "https", "hls", "hlss", "tshttp", "tshttps", "rtsp",
                            "rtsps", "rtsp-udp", "srt", "m4s", "m4ss", "m4f", "m4fs", "publish",
                            "rtp", "srtp",
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
    Ok(())
}
