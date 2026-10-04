//! Credential-scoped native HLS/continuous MPEG-TS fetcher. FFmpeg receives only loopback URLs.
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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

type Profile = (Option<u8>, Option<u8>);
pub struct PeerHls {
    pub url: String,
    cancel: CancellationToken,
    profile: Option<tokio::sync::oneshot::Receiver<Result<Profile, String>>>,
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
    live: bool,
    inspected: bool,
    prefetched: Arc<std::sync::Mutex<Option<(Bytes, reqwest::Response)>>>,
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
async fn fetch(
    f: &Fetcher,
    resource: &str,
    headers: &HeaderMap,
    slot: OwnedSemaphorePermit,
) -> Result<Response, ()> {
    if resource.len() > 16384 {
        return Err(());
    }
    let decoded = URL_SAFE_NO_PAD.decode(resource).map_err(|_| ())?;
    let url = Url::parse(std::str::from_utf8(&decoded).map_err(|_| ())?).map_err(|_| ())?;
    if !permitted(&f.origin, &url) {
        return Err(());
    }
    if f.live && url != f.origin {
        return Err(());
    }
    let (prefix, response) = if f.live && f.inspected {
        f.prefetched.lock().unwrap().take().ok_or(())?
    } else {
        let mut request = f.client.get(url.clone()).header("X-Flussonix-Peer", &f.key);
        if let Some(range) = headers.get("range").filter(|_| !f.live) {
            request = request.header("range", range);
        }
        (Bytes::new(), request.send().await.map_err(|_| ())?)
    };
    if !response.status().is_success() {
        return Err(());
    }
    if f.live {
        if response.status() != StatusCode::OK {
            return Err(());
        }
        let cancel = f.cancel.clone();
        let body = futures_util::stream::unfold(
            (
                futures_util::stream::once(std::future::ready(Ok(prefix)))
                    .chain(response.bytes_stream())
                    .boxed(),
                cancel,
                slot,
            ),
            |(mut stream, cancel, slot)| async move {
                let chunk = tokio::select! { biased; _ = cancel.cancelled() => return None, chunk = stream.next() => chunk? };
                Some((chunk, (stream, cancel, slot)))
            },
        );
        return Response::builder()
            .header("content-type", "video/mp2t")
            .header("cache-control", "no-store")
            .body(Body::from_stream(body))
            .map_err(|_| ());
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
    let result = tokio::select! { biased; _ = f.cancel.cancelled() => Err(()), result = fetch(&f, &resource, &headers, _slot) => result };
    result.unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::empty())
            .unwrap()
    })
}
async fn inspect_transport(
    f: &Fetcher,
) -> Result<(Bytes, reqwest::Response, Option<u8>, Option<u8>), String> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = f
            .client
            .get(f.origin.clone())
            .header("X-Flussonix-Peer", &f.key)
            .send()
            .await
            .map_err(|_| "cannot connect peer transport")?;
        if response.status() != StatusCode::OK {
            return Err("peer transport rejected".into());
        }
        let mut probe = crate::ts_profile::Probe::default();
        let mut prefix = BytesMut::new();
        while probe.audio.is_none() {
            let chunk = response
                .chunk()
                .await
                .map_err(|_| "peer transport metadata read failed")?
                .ok_or("peer transport metadata missing")?;
            if prefix.len() + chunk.len() > 1024 * 1024 {
                return Err("peer transport metadata exceeds limit".into());
            }
            probe.push(&chunk);
            prefix.extend_from_slice(&chunk);
        }
        Ok::<_, String>((prefix.freeze(), response, probe.audio.unwrap(), probe.video))
    })
    .await
    .map_err(|_| "peer transport metadata timeout")?
}
impl PeerHls {
    pub(crate) async fn metadata(&mut self) -> Result<Profile, String> {
        self.profile
            .take()
            .ok_or("peer transport metadata not requested")?
            .await
            .map_err(|_| "peer transport metadata task stopped")?
    }
    pub async fn start(input: &str, key: &str) -> Result<Self, String> {
        Self::start_inner(input, key, false, None).await
    }
    pub(crate) async fn start_inspected(
        input: &str,
        key: &str,
        ca: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        Self::start_inner(input, key, true, ca).await
    }
    async fn start_inner(
        input: &str,
        key: &str,
        inspect: bool,
        ca: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        let origin = Url::parse(input).map_err(|_| "invalid peer media URL")?;
        let live = origin.path().ends_with("/mpegts");
        if !permitted(&origin, &origin) || !(origin.path().ends_with(".m3u8") || live) {
            return Err("native peer HTTP input requires an HLS playlist or /mpegts URL".into());
        }
        if ca.is_some() && origin.scheme() != "https" {
            return Err("peer media CA requires HTTPS".into());
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
        let mut client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5));
        if origin.scheme() == "https" {
            client = client
                .https_only(true)
                .use_preconfigured_tls((*crate::tls_input::client(ca)?).clone());
        }
        client = if live {
            client.read_timeout(Duration::from_secs(10))
        } else {
            client.timeout(Duration::from_secs(10))
        };
        let client = client
            .build()
            .map_err(|_| "cannot build peer media fetcher")?;
        let (metadata_tx, metadata_rx) = tokio::sync::oneshot::channel();
        let f = Fetcher {
            origin: origin.clone(),
            local: local.clone(),
            key,
            slots: Arc::new(Semaphore::new(if live { 1 } else { 8 })),
            cancel: cancel.clone(),
            live,
            inspected: inspect,
            prefetched: Arc::new(std::sync::Mutex::new(None)),
            client,
        };
        let profile = if live && inspect {
            let inspected = f.clone();
            tokio::spawn(async move {
                let result = tokio::select! {biased;_=inspected.cancel.cancelled()=>Err("peer transport canceled".into()),result=inspect_transport(&inspected)=>result};
                let profile = result.map(|(prefix, response, audio, video)| {
                    *inspected.prefetched.lock().unwrap() = Some((prefix, response));
                    (audio, video)
                });
                let _ = metadata_tx.send(profile);
            });
            Some(metadata_rx)
        } else {
            None
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
        Ok(Self {
            url,
            cancel,
            profile,
        })
    }
}
