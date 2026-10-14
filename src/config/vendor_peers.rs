//! Explicit operator-owned destination mappings; never inferred from source credentials.
use serde_json::Value;
pub(crate) fn is_vendor(peer: &Value) -> bool {
    peer["flussonix_peer_kind"] == "flussonic"
}
pub(crate) fn remote_name<'a>(peer: &'a Value, name: &str) -> Option<&'a str> {
    peer["flussonix_stream_map"]
        .as_array()?
        .iter()
        .find(|row| row["name"] == name)?["remote_name"]
        .as_str()
}
pub(super) fn validate(peer: &Value) -> Result<(), String> {
    match peer.get("flussonix_peer_kind").and_then(Value::as_str) {
        None if peer.get("flussonix_peer_kind").is_none() => {}
        Some("native" | "flussonic") => {}
        _ => return Err("peer type must be native or flussonic".into()),
    }
    if !is_vendor(peer) {
        if peer.get("flussonix_stream_map").is_some() {
            return Err("stream mappings require a Flussonic peer".into());
        }
        return Ok(());
    }
    let key = peer["cluster_key"]
        .as_str()
        .filter(|s| s.len() >= 12 && s.len() <= 4096)
        .ok_or("Flussonic peer requires an explicit Cluster key")?;
    super::super::source_directory::flussonic::InventoryClient::new(
        peer["api_url"].as_str().ok_or("management URL required")?,
        key,
        None,
    )
    .map_err(|_| "invalid Flussonic management URL")?;
    let public = peer["public_payload_url"]
        .as_str()
        .and_then(|s| url::Url::parse(s).ok())
        .ok_or("public delivery URL required")?;
    if !matches!(public.scheme(), "http" | "https")
        || public.host_str().is_none()
        || public.port() == Some(0)
        || !public.username().is_empty()
        || public.password().is_some()
        || public.query().is_some()
        || public.fragment().is_some()
    {
        return Err(
            "public delivery URL must be an HTTP(S) root without credentials, query or fragment"
                .into(),
        );
    }
    let rows = peer["flussonix_stream_map"]
        .as_array()
        .filter(|rows| !rows.is_empty() && rows.len() <= 128)
        .ok_or("Flussonic peer requires 1 to 128 stream mappings")?;
    let mut seen = std::collections::HashSet::new();
    for row in rows {
        let o = row
            .as_object()
            .filter(|o| o.len() == 2)
            .ok_or("mapping requires only name and remote_name")?;
        let name = o
            .get("name")
            .and_then(Value::as_str)
            .ok_or("mapping name required")?;
        super::valid_name(name)?;
        super::valid_name(
            o.get("remote_name")
                .and_then(Value::as_str)
                .ok_or("remote stream name required")?,
        )?;
        if !seen.insert(name) {
            return Err("duplicate local stream mapping".into());
        }
    }
    Ok(())
}
