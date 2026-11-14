use crate::playback_auth::ViewerRequest;
use axum::http::StatusCode;
use serde_json::Value;
use std::net::IpAddr;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SrtViewer {
    token: String,
    qs: String,
    ip: String,
    user_agent: String,
    referer: String,
    host: String,
}

/// Only the native credential-bearing SRT form reconstructs a private viewer.
pub(super) fn srt_viewer(body: &Value, name: &str) -> Result<Option<ViewerRequest>, StatusCode> {
    let Some(raw) = body.get("srt_viewer") else {
        return Ok(None);
    };
    if body.as_object().is_none_or(|fields| {
        fields.keys().any(|key| {
            !matches!(
                key.as_str(),
                "name" | "protocol" | "encrypted" | "token_hash" | "bitrate_mbps" | "srt_viewer"
            )
        })
    }) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let input: SrtViewer =
        serde_json::from_value(raw.clone()).map_err(|_| StatusCode::BAD_REQUEST)?;
    let ip: IpAddr = input.ip.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let pairs = url::form_urlencoded::parse(input.qs.as_bytes()).collect::<Vec<_>>();
    let [(_, original)] = pairs.as_slice() else {
        return Err(StatusCode::BAD_REQUEST);
    };
    if pairs[0].0 != "streamid"
        || input.token.is_empty()
        || body["protocol"] != "srt"
        || body["encrypted"] != true
        || body["token_hash"]
            != super::rtsp_balancer::token_hash(&ViewerRequest {
                token: input.token.clone(),
                ..Default::default()
            })
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let selection =
        crate::srt_playback::Selection::parse(original).map_err(|_| StatusCode::BAD_REQUEST)?;
    if selection.name != name || selection.token != input.token {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(Some(ViewerRequest {
        name: name.to_owned(),
        proto: "srt".into(),
        ip: ip.to_string(),
        token: input.token,
        qs: input.qs,
        user_agent: input.user_agent,
        referer: input.referer,
        host: input.host,
        finite_media: false,
    }))
}

// Admission tickets are small; bound both announced and streamed peer replies.
pub(super) async fn read_json(mut response: reqwest::Response) -> Option<Value> {
    const LIMIT: usize = 16 * 1024;
    if response.content_length().is_some_and(|n| n > LIMIT as u64) {
        return None;
    }
    let mut bytes = vec![];
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len().checked_add(chunk.len())? > LIMIT {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}
