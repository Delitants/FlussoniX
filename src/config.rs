use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
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
        })
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
            result = merge(&result, t)
        }
    }
    result = merge(&result, disk);
    result["config_on_disk"] = disk.clone();
    Some(result)
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
                    "transcoder",
                    "on_play",
                    "flussonix_token_sha256",
                ],
                "peers" | "sources" => &[
                    "hostname",
                    "api_url",
                    "public_payload_url",
                    "private_payload_url",
                    "cluster_key",
                    "drain",
                ],
                "auth_backends" => &["name", "url"],
                _ => &[],
            };
            for key in item.as_object().ok_or("item must be an object")?.keys() {
                if !allowed.contains(&key.as_str()) {
                    return Err(format!("{kind} option {key} is not implemented"));
                }
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
                if item.get("drain").is_some_and(|v| !v.is_boolean()) {
                    return Err("drain must be boolean".into());
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
                if let Some(policy) = item.get("on_play") {
                    let policy = policy.as_str().ok_or("on_play must be a URL string")?;
                    if let Some(name) = policy.strip_prefix("auth://") {
                        valid_name(name)?;
                        if !root["auth_backends"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|b| b["name"] == name)
                        {
                            return Err("auth backend not found".into());
                        }
                    } else {
                        let url = url::Url::parse(policy).map_err(|_| "invalid on_play URL")?;
                        if !["http", "https"].contains(&url.scheme()) || url.host_str().is_none() {
                            return Err("on_play must use HTTP(S) or auth://backend".into());
                        }
                    }
                }
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
                        if input
                            .as_object()
                            .is_none_or(|v| v.keys().any(|k| k != "url"))
                        {
                            return Err("only input url is implemented".into());
                        }
                        let u = input["url"].as_str().ok_or("input url required")?;
                        let scheme = u.split("://").next().unwrap_or("");
                        if ![
                            "testsrc", "http", "https", "hls", "hlss", "tshttp", "tshttps", "rtsp",
                            "srt", "m4s", "m4ss", "m4f", "m4fs",
                        ]
                        .contains(&scheme)
                        {
                            return Err(format!("unsupported input protocol: {scheme}"));
                        }
                    }
                }
                if let Some(t) = item.get("transcoder") {
                    if !t.is_object() {
                        return Err("transcoder must be an object".into());
                    }
                    for k in t.as_object().unwrap().keys() {
                        if !["encoder", "vb"].contains(&k.as_str()) {
                            return Err(format!("transcoder option {k} is not implemented"));
                        }
                    }
                    if let Some(v) = t.get("vb") {
                        if !v.as_u64().is_some_and(|n| (100..=50000).contains(&n)) {
                            return Err("vb must be 100..50000 kbps".into());
                        }
                    }
                    if let Some(e) = t.get("encoder") {
                        if !e
                            .as_str()
                            .is_some_and(|e| ["libx264", "h264_nvenc"].contains(&e))
                        {
                            return Err("supported encoders: libx264, h264_nvenc".into());
                        }
                    }
                }
                for field in ["dvr", "pushes"] {
                    if item.get(field).is_some() {
                        return Err(format!("{field} is not implemented in this build"));
                    }
                }
            }
        }
    }
    for s in root["streams"].as_array().unwrap() {
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
