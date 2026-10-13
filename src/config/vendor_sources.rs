//! Explicit local vendor intent; URL-only sources retain native semantics.
use serde_json::Value;
use std::time::Duration;

pub(super) const FIELDS: &[&str] = &[
    "flussonix_source_kind",
    "on_play",
    "flussonix_token_sha256",
    "group_config",
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
        if ["on_play", "flussonix_token_sha256", "group_config"]
            .iter()
            .any(|key| source.get(*key).is_some())
        {
            return Err("source viewer policies require explicit Flussonic source kind".into());
        }
        return Ok(());
    }
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
