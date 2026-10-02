//! Credential-scoped native HLS fetcher. FFmpeg receives only loopback URLs.
//! URI lines and quoted URI attributes are resolved relative to their playlist
//! (RFC 8216), then constrained to the configured HTTP(S) origin.
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use url::Url;

pub struct PeerHls {
    pub url: String,
    cancel: CancellationToken,
}
impl Drop for PeerHls {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
#[derive(Clone)]
struct Fetcher {
    origin: Url,
    local: String,
    client: reqwest::Client,
    key: String,
    slots: Arc<Semaphore>,
    cancel: CancellationToken,
}
fn permitted(origin: &Url, url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.origin() == origin.origin()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}
fn local_uri(f: &Fetcher, base: &Url, value: &str) -> Result<String, ()> {
    let url = base.join(value).map_err(|_| ())?;
    if !permitted(&f.origin, &url) {
        return Err(());
    }
    let resource = URL_SAFE_NO_PAD.encode(url.as_str());
    // Retain a safe extension for FFmpeg's media-format consistency checks.
    let extension = url
        .path()
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .filter(|ext| {
            !ext.is_empty() && ext.len() <= 10 && ext.bytes().all(|b| b.is_ascii_alphanumeric())
        })
        .unwrap_or("bin");
    Ok(format!("{}{resource}/resource.{extension}", f.local))
}
fn playlist(f: &Fetcher, base: &Url, bytes: &[u8]) -> Result<Bytes, ()> {
    if bytes.len() > 1024 * 1024 {
        return Err(());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| ())?;
    let mut out = String::new();
    for line in text.lines() {
        if line.starts_with('#') {
            // Split attribute lists without treating commas inside quoted
            // values as separators. Only actual URI attributes are rewritten.
            if let Some((tag, attributes)) = line.split_once(':') {
                out.push_str(tag);
                out.push(':');
                let mut quoted = false;
                let mut start = 0;
                for (i, ch) in attributes
                    .char_indices()
                    .chain(std::iter::once((attributes.len(), ',')))
                {
                    if ch == '"' {
                        quoted = !quoted;
                    }
                    if ch == ',' && !quoted {
                        if start != 0 {
                            out.push(',');
                        }
                        let attribute = &attributes[start..i];
                        if let Some(value) = attribute.strip_prefix("URI=") {
                            let uri = value
                                .strip_prefix('"')
                                .and_then(|v| v.strip_suffix('"'))
                                .ok_or(())?;
                            out.push_str("URI=\"");
                            out.push_str(&local_uri(f, base, uri)?);
                            out.push('"');
                        } else {
                            out.push_str(attribute);
                        }
                        start = i + 1;
                    }
                }
                if quoted {
                    return Err(());
                }
            } else {
                out.push_str(line);
            }
        } else if !line.trim().is_empty() {
            out.push_str(&local_uri(f, base, line.trim())?);
        }
        out.push('\n');
        if out.len() > 4 * 1024 * 1024 {
            return Err(());
        }
    }
    Ok(Bytes::from(out))
}
async fn fetch(f: &Fetcher, resource: &str, headers: &HeaderMap) -> Result<Response, ()> {
    if resource.len() > 16384 {
        return Err(());
    }
    let decoded = URL_SAFE_NO_PAD.decode(resource).map_err(|_| ())?;
    let url = Url::parse(std::str::from_utf8(&decoded).map_err(|_| ())?).map_err(|_| ())?;
    if !permitted(&f.origin, &url) {
        return Err(());
    }
    let mut request = f.client.get(url.clone()).header("X-Flussonix-Peer", &f.key);
    if let Some(range) = headers.get("range") {
        request = request.header("range", range);
    }
    let response = request.send().await.map_err(|_| ())?;
    if !response.status().is_success() {
        return Err(());
    }
    if response
        .content_length()
        .is_some_and(|len| len > 32 * 1024 * 1024)
    {
        return Err(());
    }
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let mut data = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ())?;
        if data.len() + chunk.len() > 32 * 1024 * 1024 {
            return Err(());
        }
        data.extend_from_slice(&chunk);
    }
    let is_playlist = data.starts_with(b"#EXTM3U");
    let body = if is_playlist {
        playlist(f, &url, &data)?
    } else {
        data.freeze()
    };
    let mut builder = Response::builder()
        .status(status)
        .header("cache-control", "no-store");
    if is_playlist {
        builder = builder.header("content-type", "application/vnd.apple.mpegurl");
    } else {
        for name in ["content-type", "content-range", "accept-ranges"] {
            if let Some(value) = upstream_headers.get(name) {
                builder = builder.header(name, value);
            }
        }
    }
    builder.body(Body::from(body)).map_err(|_| ())
}
async fn resource(
    State(f): State<Fetcher>,
    Path((resource, _file)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let Ok(_slot) = f.slots.clone().try_acquire_owned() else {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::empty())
            .unwrap();
    };
    let result = tokio::select! { biased; _ = f.cancel.cancelled() => Err(()), result = fetch(&f, &resource, &headers) => result };
    result.unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::empty())
            .unwrap()
    })
}
impl PeerHls {
    pub async fn start(input: &str, key: &str) -> Result<Self, String> {
        let origin = Url::parse(input).map_err(|_| "invalid peer HLS URL")?;
        if !permitted(&origin, &origin) || !origin.path().ends_with(".m3u8") {
            return Err("native peer HTTP input requires an HLS playlist URL".into());
        }
        let key = reqwest::header::HeaderValue::from_str(key).map_err(|_| "invalid peer key")?;
        let key = key.to_str().map_err(|_| "invalid peer key")?.to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "cannot bind peer HLS fetcher")?;
        let local = format!(
            "http://{}/{}/",
            listener
                .local_addr()
                .map_err(|_| "cannot bind peer HLS fetcher")?,
            uuid::Uuid::new_v4()
        );
        let cancel = CancellationToken::new();
        let f = Fetcher {
            origin: origin.clone(),
            local: local.clone(),
            key,
            slots: Arc::new(Semaphore::new(8)),
            cancel: cancel.clone(),
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| "cannot build peer HLS fetcher")?,
        };
        let url = local_uri(&f, &origin, origin.as_str()).map_err(|_| "invalid peer HLS URL")?;
        let route = format!(
            "/{}/{{resource}}/{{file}}",
            local.trim_end_matches('/').rsplit('/').next().unwrap()
        );
        let app = Router::new().route(&route, get(resource)).with_state(f);
        let shutdown = cancel.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await;
        });
        Ok(Self { url, cancel })
    }
}
