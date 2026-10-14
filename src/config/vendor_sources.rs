//! Explicit local vendor intent; URL-only sources retain native semantics.
use serde_json::Value;
use std::time::Duration;

pub(super) const FIELDS: &[&str] = &[
    "flussonix_source_kind",
    "on_play",
    "flussonix_token_sha256",
    "group_config",
    "flussonix_output_template",
    "flussonix_content_id",
];
pub(super) fn validate_count(root: &Value) -> Result<(), String> {
    if root["sources"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|source| source["flussonix_source_kind"] == "flussonic")
        .count()
        > 64
    {
        return Err("at most 64 Flussonic sources may be configured".into());
    }
    Ok(())
}
pub(super) fn validate(source: &Value, root: &Value) -> Result<(), String> {
    let vendor = match source.get("flussonix_source_kind") {
        None => false,
        Some(Value::String(kind)) if kind == "native" => false,
        Some(Value::String(kind)) if kind == "flussonic" => true,
        _ => return Err("source kind must be native or flussonic".into()),
    };
    if !vendor {
        if [
            "on_play",
            "flussonix_token_sha256",
            "group_config",
            "flussonix_output_template",
            "flussonix_content_id",
        ]
        .iter()
        .any(|key| source.get(*key).is_some())
        {
            return Err("source viewer policies and processing templates require explicit Flussonic source kind".into());
        }
        return Ok(());
    }
    if source.get("flussonix_source_group").is_some()
        != source.get("flussonix_content_id").is_some()
    {
        return Err("vendor failover requires both a group and content identity".into());
    }
    crate::vendor_output::resolve(root, source)?;
    let profile = crate::source_directory::flussonic_config::normalized_modern_source(source)
        .map_err(|_| {
            "Flussonic source requires a supported URL profile and matching transport".to_string()
        })?;
    // Same complete profile gate as coordinator preparation, including all unused
    // local group policies/backends and TLS trust. Client construction performs no
    // network I/O; errors deliberately contain no credentials or endpoint values.
    crate::source_directory::flussonic_config::SourceAuthority::new_modern(
        &profile,
        &root["auth_backends"],
        Duration::from_secs(3),
    )
    .map_err(|_| "invalid or unsupported Flussonic source profile or viewer policy".to_string())?;
    Ok(())
}

/// Validate complete local equivalence, including every potential group policy.
/// A singleton can be staged; the daemon requires two members before playback.
pub(super) fn validate_groups(root: &Value) -> Result<(), String> {
    use crate::source_directory::flussonic_config::{SourceAuthority, normalized_modern_source};
    let mut groups = std::collections::HashMap::<&str, Vec<&Value>>::new();
    for source in root["sources"].as_array().into_iter().flatten() {
        if let Some(group) = source["flussonix_source_group"].as_str() {
            groups.entry(group).or_default().push(source);
        }
    }
    let invalid = || {
        "vendor replica group requires equal content identity, namespace, exclusions, processing and uniform viewer policy".to_owned()
    };
    for members in groups.values().filter(|members| {
        members
            .iter()
            .any(|s| s["flussonix_source_kind"] == "flussonic")
    }) {
        let first = members[0];
        let filters = |source: &Value| {
            let mut excluded: Vec<_> = source["except"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect();
            excluded.sort();
            excluded.dedup();
            (source["prefix"].clone(), excluded)
        };
        let processing = crate::vendor_output::resolve(root, first)?;
        let mut policy = None;
        for source in members {
            if source["flussonix_source_kind"] != "flussonic"
                || source["flussonix_content_id"] != first["flussonix_content_id"]
                || filters(source) != filters(first)
                || crate::vendor_output::resolve(root, source)? != processing
            {
                return Err(invalid());
            }
            let authority = SourceAuthority::new_modern(
                &normalized_modern_source(source).map_err(|_| invalid())?,
                &root["auth_backends"],
                Duration::from_secs(3),
            )
            .map_err(|_| invalid())?;
            let current = authority
                .uniform_policy()
                .map_err(|_| invalid())?
                .ok_or_else(invalid)?;
            if policy.as_ref().is_some_and(|expected| expected != &current) {
                return Err(invalid());
            }
            policy = Some(current);
        }
    }
    Ok(())
}
