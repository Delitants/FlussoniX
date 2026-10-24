use super::*;
use std::{collections::BTreeSet, time::SystemTime};

pub(super) fn get(app: Arc<App>, request: Request, subscriber_name: &str) -> Response {
    let snapshot = app.iptv.snapshot();
    if !snapshot.enabled() {
        return no_store(error(StatusCode::FORBIDDEN, "IPTV is disabled"));
    }
    let Some(subscriber) = snapshot.subscriber(subscriber_name) else {
        return no_store(error(StatusCode::NOT_FOUND, "subscriber not found"));
    };
    let query = parse_query(request.uri().query());
    let Some(token) = query.get("token") else {
        return no_store(error(StatusCode::FORBIDDEN, "subscriber token required"));
    };
    if !snapshot.subscriber_token_valid(subscriber_name, token, SystemTime::now()) {
        return no_store(error(StatusCode::FORBIDDEN, "subscriber token denied"));
    }
    let Some(origin) = app.options.iptv_public_origin.as_ref() else {
        return no_store(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "IPTV public origin is not configured",
        ));
    };

    let config = app.config.snapshot();
    let templates: HashMap<&str, &Value> = config["templates"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item["name"].as_str().map(|name| (name, item)))
        .collect();
    let streams: HashMap<&str, &Value> = config["streams"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item["name"].as_str().map(|name| (name, item)))
        .collect();
    let mut channels = BTreeSet::new();
    for package in &subscriber.packages {
        if let Some(members) = snapshot.package_channels(package) {
            channels.extend(members.iter().map(String::as_str));
        }
    }
    let mut body = String::from("#EXTM3U\n");
    for channel in channels {
        let Some(stream) = streams.get(channel) else {
            continue;
        };
        let template = stream["template"]
            .as_str()
            .and_then(|name| templates.get(name).copied());
        let disabled = stream["disabled"]
            .as_bool()
            .or_else(|| template.and_then(|value| value["disabled"].as_bool()))
            .unwrap_or(false);
        if disabled {
            continue;
        }
        let title = stream["title"]
            .as_str()
            .or_else(|| template.and_then(|value| value["title"].as_str()))
            .unwrap_or(channel);
        let safe_title = safe_m3u_title(title);
        let title = if safe_title.is_empty() {
            channel
        } else {
            &safe_title
        };
        let mut media_url = origin.clone();
        {
            let mut segments = media_url
                .path_segments_mut()
                .expect("validated HTTP(S) origin");
            segments.push(channel).push("index.m3u8");
        }
        media_url.query_pairs_mut().append_pair("token", token);
        body.push_str(&format!("#EXTINF:-1,{title}\n{media_url}\n"));
    }
    let mut response = body.into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/vnd.apple.mpegurl"),
    );
    no_store(response)
}

fn safe_m3u_title(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    response
}
