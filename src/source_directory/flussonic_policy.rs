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
    fn resolve(
        value: &Value,
        root: &Value,
        templates: &HashMap<String, Self>,
    ) -> Result<Self, PolicyFailure> {
        bounded(value)?;
        let mut object = value.as_object().ok_or(PolicyFailure::Invalid)?.clone();
        let base = object
            .remove("template")
            .map(|reference| {
                let name = reference.as_str().ok_or(PolicyFailure::Invalid)?;
                crate::config::valid_name(name).map_err(|_| PolicyFailure::Invalid)?;
                templates
                    .get(name)
                    .cloned()
                    .ok_or(PolicyFailure::Unresolved)
            })
            .transpose()?
            .unwrap_or_default();
        // Parse the direct layer before merging: an inherited value never hides
        // invalid or unsupported local fields. Callback replacement is atomic.
        let direct = Self::parse(&Value::Object(object), root)?;
        Ok(Self {
            callback: direct.callback.or(base.callback),
            token_hash: direct.token_hash.or(base.token_hash),
        })
    }

    fn config(&self) -> Value {
        let mut value = json!({});
        if let Some((callback, _)) = &self.callback {
            value["on_play"] = callback.clone();
        }
        if let Some(hash) = &self.token_hash {
            value["flussonix_token_sha256"] = json!(hash);
        }
        value
    }

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
    /// Opt into explicit local policy-only templates. A source/group may name
    /// one template; its direct fields replace that template's defaults.
    /// All templates validate, even unused. Nested templates are unsupported.
    pub fn with_templates(
        source: &Value,
        groups: &Value,
        templates: &Value,
        backends: &Value,
    ) -> Result<Self, PolicyFailure> {
        Self::build(source, groups, Some(templates), backends)
    }
    /// Inputs are policy-only source/group objects and local backend entries.
    /// Unknown fields (including template inheritance) fail even when shadowed.
    /// This is not an importer for complete source or remote inventory objects.
    pub fn new(source: &Value, groups: &Value, backends: &Value) -> Result<Self, PolicyFailure> {
        Self::build(source, groups, None, backends)
    }

    fn build(
        source: &Value,
        groups: &Value,
        templates: Option<&Value>,
        backends: &Value,
    ) -> Result<Self, PolicyFailure> {
        let backends_array = backends.as_array().ok_or(PolicyFailure::Invalid)?;
        let groups_map = groups.as_object().ok_or(PolicyFailure::Invalid)?;
        let templates_map = templates
            .map(|value| value.as_object().ok_or(PolicyFailure::Invalid))
            .transpose()?;
        if backends_array.len() > MAX_CONFIGURED || groups_map.len() > MAX_CONFIGURED {
            return Err(PolicyFailure::Limit);
        }
        if templates_map.is_some_and(|map| map.len() > MAX_CONFIGURED) {
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
        let mut resolved_templates = HashMap::new();
        if let Some(map) = templates_map {
            for (name, value) in map {
                crate::config::valid_name(name).map_err(|_| PolicyFailure::Invalid)?;
                resolved_templates.insert(name.clone(), Layer::parse(value, &root)?);
            }
        }
        let parse = |value: &Value| {
            if templates.is_some() {
                Layer::resolve(value, &root, &resolved_templates)
            } else {
                Layer::parse(value, &root)
            }
        };
        let source = parse(source)?;
        let groups = groups_map
            .iter()
            .map(|(name, value)| {
                crate::config::valid_name(name).map_err(|_| PolicyFailure::Invalid)?;
                Ok((name.clone(), parse(value)?))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            source,
            groups,
            root,
        })
    }

    /// Internal projection only after complete validation. Preserve explicit
    /// public decisions and independent guards; never project remote objects.
    pub(super) fn materialized_inputs(&self) -> (Value, Value) {
        (
            self.source.config(),
            Value::Object(
                self.groups
                    .iter()
                    .map(|(name, layer)| (name.clone(), layer.config()))
                    .collect(),
            ),
        )
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
