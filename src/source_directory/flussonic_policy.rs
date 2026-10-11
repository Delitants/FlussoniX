//! Explicit local viewer-policy composition for future Flussonic sources.
//!
//! Inventory and privileged media credentials are not viewer authority. This
//! immutable library gate does not publish routes or modify native semantics.
use crate::playback_auth::Policy;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

const MAX_CONFIGURED: usize = 128;
const MAX_MEMBERSHIPS: usize = 64;
const MAX_LAYER_BYTES: usize = 16 * 1024;

/// Generic diagnostics deliberately contain no policy, name, URL or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyFailure {
    Invalid,
    Unsupported,
    Ambiguous,
    Unresolved,
    Limit,
}

#[derive(Clone, Default)]
struct Layer {
    callback: Option<(Value, Policy)>,
    token_hash: Option<String>,
}
impl Layer {
    fn parse(value: &Value, root: &Value) -> Result<Self, PolicyFailure> {
        bounded(value)?;
        let object = value.as_object().ok_or(PolicyFailure::Invalid)?;
        if object
            .keys()
            .any(|k| !["on_play", "flussonix_token_sha256"].contains(&k.as_str()))
        {
            return Err(PolicyFailure::Unsupported);
        }
        let callback = object
            .get("on_play")
            .map(|value| {
                let config = if value.as_str() == Some("true") {
                    json!({})
                } else {
                    json!({"on_play":value})
                };
                let policy =
                    Policy::from_config(&config, root).map_err(|_| PolicyFailure::Invalid)?;
                Ok((value.clone(), policy))
            })
            .transpose()?;
        let token_hash = object
            .get("flussonix_token_sha256")
            .map(|value| {
                Policy::from_config(&json!({"flussonix_token_sha256":value}), root)
                    .map_err(|_| PolicyFailure::Invalid)?;
                Ok(value
                    .as_str()
                    .ok_or(PolicyFailure::Invalid)?
                    .to_ascii_lowercase())
            })
            .transpose()?;
        Ok(Self {
            callback,
            token_hash,
        })
    }
}

/// Validated local policy inputs. No Debug/serialization: callbacks may contain
/// credentials. Hold this object under the caller's configuration revision fence.
pub struct PolicySet {
    source: Layer,
    groups: HashMap<String, Layer>,
    root: Value,
}
impl PolicySet {
    /// Inputs are policy-only source/group objects and local backend entries.
    /// Unknown fields (including template inheritance) fail even when shadowed.
    /// This is not an importer for complete source or remote inventory objects.
    pub fn new(source: &Value, groups: &Value, backends: &Value) -> Result<Self, PolicyFailure> {
        let backends_array = backends.as_array().ok_or(PolicyFailure::Invalid)?;
        let groups_map = groups.as_object().ok_or(PolicyFailure::Invalid)?;
        if backends_array.len() > MAX_CONFIGURED || groups_map.len() > MAX_CONFIGURED {
            return Err(PolicyFailure::Limit);
        }
        let mut names = HashSet::new();
        for backend in backends_array {
            bounded(backend)?;
            let object = backend.as_object().ok_or(PolicyFailure::Invalid)?;
            if object
                .keys()
                .any(|k| !["name", "url"].contains(&k.as_str()))
            {
                return Err(PolicyFailure::Unsupported);
            }
            let name = backend["name"].as_str().ok_or(PolicyFailure::Invalid)?;
            crate::config::valid_name(name).map_err(|_| PolicyFailure::Invalid)?;
            if !names.insert(name) {
                return Err(PolicyFailure::Ambiguous);
            }
            // Validate a direct HTTP(S) URL without resolving another backend.
            let url = backend["url"].as_str().ok_or(PolicyFailure::Invalid)?;
            Policy::from_config(&json!({"on_play":url}), &json!({}))
                .map_err(|_| PolicyFailure::Invalid)?;
        }
        let root = json!({"auth_backends":backends});
        let source = Layer::parse(source, &root)?;
        let groups = groups_map
            .iter()
            .map(|(name, value)| {
                crate::config::valid_name(name).map_err(|_| PolicyFailure::Invalid)?;
                Ok((name.clone(), Layer::parse(value, &root)?))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            source,
            groups,
            root,
        })
    }

    /// Memberships must be explicit, unique literal names. Unconfigured groups
    /// contribute nothing. Matching group fields override source fields, but
    /// conflicting groups fail rather than depend on remote inventory order.
    pub fn for_groups(&self, memberships: &Value) -> Result<Policy, PolicyFailure> {
        let memberships = memberships.as_array().ok_or(PolicyFailure::Invalid)?;
        if memberships.len() > MAX_MEMBERSHIPS {
            return Err(PolicyFailure::Limit);
        }
        let mut names = HashSet::new();
        let mut overrides = Layer::default();
        for name in memberships {
            let name = name.as_str().ok_or(PolicyFailure::Invalid)?;
            crate::config::valid_name(name).map_err(|_| PolicyFailure::Invalid)?;
            if !names.insert(name) {
                return Err(PolicyFailure::Ambiguous);
            }
            let Some(layer) = self.groups.get(name) else {
                continue;
            };
            if let Some(callback) = &layer.callback {
                if overrides
                    .callback
                    .as_ref()
                    .is_some_and(|old| old.1 != callback.1)
                {
                    return Err(PolicyFailure::Ambiguous);
                }
                overrides.callback = Some(callback.clone());
            }
            if let Some(hash) = &layer.token_hash {
                if overrides.token_hash.as_ref().is_some_and(|old| old != hash) {
                    return Err(PolicyFailure::Ambiguous);
                }
                overrides.token_hash = Some(hash.clone());
            }
        }
        let callback = overrides
            .callback
            .as_ref()
            .or(self.source.callback.as_ref());
        let token_hash = overrides
            .token_hash
            .as_ref()
            .or(self.source.token_hash.as_ref());
        if callback.is_none() && token_hash.is_none() {
            return Err(PolicyFailure::Unresolved);
        }
        let mut config = json!({});
        if let Some((value, _)) = callback
            && value.as_str() != Some("true")
        {
            config["on_play"] = value.clone();
        }
        if let Some(hash) = token_hash {
            config["flussonix_token_sha256"] = json!(hash);
        }
        Policy::from_config(&config, &self.root).map_err(|_| PolicyFailure::Invalid)
    }
}

fn bounded(value: &Value) -> Result<(), PolicyFailure> {
    if serde_json::to_vec(value)
        .map_err(|_| PolicyFailure::Invalid)?
        .len()
        > MAX_LAYER_BYTES
    {
        return Err(PolicyFailure::Limit);
    }
    Ok(())
}
