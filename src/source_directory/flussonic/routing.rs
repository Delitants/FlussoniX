//! Bounded metadata-only CDN observation. Cluster permission is never viewer permission.
use super::{InventoryClient, LookupFailure, MAX_BYTES, modern::ObservedServerId};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use url::Url;

pub(crate) struct Observation {
    pub value: Value,
    pub ready: bool,
    pub id: ObservedServerId,
}
pub(crate) async fn observe(
    base: &str,
    key: &str,
    ca: Option<&Path>,
    name: &str,
    local_ids: &[ObservedServerId],
) -> Result<Observation, LookupFailure> {
    crate::config::valid_name(name).map_err(|_| LookupFailure::Invalid)?;
    let transport = InventoryClient::prepare(base, key, ca, Duration::from_secs(3))?;
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut config = Url::parse(base).map_err(|_| LookupFailure::Invalid)?;
        config.set_path(&format!(
            "{}/streamer/api/v3/config",
            config.path().trim_end_matches('/')
        ));
        let mut stream = config.clone();
        stream
            .path_segments_mut()
            .map_err(|_| LookupFailure::Invalid)?
            .pop()
            .push("streams")
            .push(name);
        let mut remaining = MAX_BYTES;
        let before = transport
            .modern_json(config.clone(), &mut remaining)
            .await?;
        let id = identity(&before)?;
        if local_ids.contains(&id) {
            return Err(LookupFailure::Invalid);
        }
        let remote = transport.modern_json(stream, &mut remaining).await?;
        if remote["name"].as_str() != Some(name)
            || remote["disabled"].as_bool().unwrap_or(false)
            || !matches!(remote["named_by"].as_str(), Some("config" | "remote"))
        {
            return Err(LookupFailure::Invalid);
        }
        if remote.get("disabled").is_some_and(|v| !v.is_boolean()) {
            return Err(LookupFailure::Invalid);
        }
        let after = transport.modern_json(config, &mut remaining).await?;
        if identity(&after)? != id
            || remote.get("server_id").is_some_and(|v| {
                v.as_str()
                    .and_then(|s| ObservedServerId::parse(s).ok())
                    .as_ref()
                    != Some(&id)
            })
        {
            return Err(LookupFailure::Invalid);
        }
        let mut value = project(&after)?;
        if let Some(rate) = remote["stats"]["bitrate"].as_u64().filter(|n| *n > 0) {
            value["stream_bitrates"][name] = json!({"mbps":rate as f64/1000.0,"age_ms":0});
        }
        Ok(Observation {
            value,
            ready: remote["stats"]["running"]
                .as_bool()
                .ok_or(LookupFailure::Invalid)?,
            id,
        })
    })
    .await
    .unwrap_or(Err(LookupFailure::Unavailable))
}
fn identity(v: &Value) -> Result<ObservedServerId, LookupFailure> {
    ObservedServerId::parse(v["stats"]["id"].as_str().ok_or(LookupFailure::Invalid)?)
}
fn project(v: &Value) -> Result<Value, LookupFailure> {
    let stats = &v["stats"];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LookupFailure::Unavailable)?
        .as_millis();
    let stamp = stats["now"].as_u64().ok_or(LookupFailure::Invalid)? as u128;
    if now.abs_diff(stamp) > 5000 || stats["streamer_status"] != "running" {
        return Err(LookupFailure::Invalid);
    }
    let percent = |field: &str| {
        stats[field]
            .as_u64()
            .filter(|n| *n <= 100)
            .map(|n| n as f64 / 100.0)
            .ok_or(LookupFailure::Invalid)
    };
    let capacity = stats["total_bandwidth"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or(LookupFailure::Invalid)? as f64
        / 1_000_000.0;
    let output = stats["output_kbit"]
        .as_u64()
        .ok_or(LookupFailure::Invalid)? as f64
        / 1000.0;
    let active = stats["total_clients"]
        .as_u64()
        .ok_or(LookupFailure::Invalid)?;
    Ok(
        json!({"role":"cdn","uplink":output/capacity,"uplink_mbps":capacity,"cpu":percent("cpu_usage")?,"ram":percent("memory_usage")?,"drain":false,"age_ms":0,"active":active,"reserved":0,"reserved_mbps":0.0,"limit":u64::MAX,"stream_bitrates":{}}),
    )
}
