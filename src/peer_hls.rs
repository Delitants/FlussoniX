//! Origin-scoped native peer and verified external HLS/continuous MPEG-TS fetcher. FFmpeg receives only loopback URLs.
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
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
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
    tokens: Arc<ResourceTokens>,
    client: reqwest::Client,
    key: Option<reqwest::header::HeaderValue>,
    slots: Arc<Semaphore>,
    cancel: CancellationToken,
    live: bool,
    inspected: bool,
    prefetched: Arc<std::sync::Mutex<Option<(Bytes, reqwest::Response)>>>,
}
const TOKEN_AAD: &[u8] = b"flussonix-peer-hls-url-v1";
struct ResourceTokens {
    cipher: aead::LessSafeKey,
    next: AtomicU64,
}
impl ResourceTokens {
    fn new() -> Result<Self, ()> {
        let mut key = [0u8; 32];
        SystemRandom::new().fill(&mut key).map_err(|_| ())?;
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key);
        key.fill(0);
        let key = unbound.map_err(|_| ())?;
        Ok(Self {
            cipher: aead::LessSafeKey::new(key),
            next: AtomicU64::new(0),
        })
    }
    fn seal(&self, url: &Url) -> Result<String, ()> {
        let mut sequence = self.next.load(Ordering::Relaxed);
        loop {
            let next = sequence.checked_add(1).ok_or(())?;
            match self.next.compare_exchange_weak(
                sequence,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => sequence = current,
            }
        }
        let mut nonce = [0u8; 12];
        nonce[4..].copy_from_slice(&sequence.to_be_bytes());
        let mut ciphertext = url.as_str().as_bytes().to_vec();
        self.cipher
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(TOKEN_AAD),
                &mut ciphertext,
            )
            .map_err(|_| ())?;
        let mut token = nonce.to_vec();
        token.extend_from_slice(&ciphertext);
        Ok(URL_SAFE_NO_PAD.encode(token))
    }
    fn open(&self, resource: &str) -> Result<Url, ()> {
        let mut token = URL_SAFE_NO_PAD.decode(resource).map_err(|_| ())?;
        if token.len() < 12 + aead::AES_256_GCM.tag_len() {
            return Err(());
        }
        let (prefix, ciphertext) = token.split_at_mut(12);
        let nonce: [u8; 12] = prefix.try_into().map_err(|_| ())?;
        let plaintext = self
            .cipher
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(TOKEN_AAD),
                ciphertext,
            )
            .map_err(|_| ())?;
        Url::parse(std::str::from_utf8(plaintext).map_err(|_| ())?).map_err(|_| ())
    }
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
    let resource = f.tokens.seal(&url)?;
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

fn unexpected_text_media(bytes: &[u8]) -> bool {
    // HLS media is binary except for WebVTT. FFmpeg can autodetect a textual
    // manifest in a segment response and fetch URLs from that manifest without
    // going through this origin-scoped proxy. Inspect content, not the suffix
    // or MIME type, which the upstream controls.
    if bytes.first() == Some(&0x47) && bytes.len() % 188 == 0 {
        return false;
    }
    let body = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
    let start = body
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(body.len());
    let body = &body[start..];
    let prefix_bytes = &body[..body.len().min(256)];
    let prefix = match std::str::from_utf8(prefix_bytes) {
        Ok(text) => text,
        Err(error) => std::str::from_utf8(&prefix_bytes[..error.valid_up_to()]).unwrap(),
    };
    if prefix.starts_with("WEBVTT") {
        return false;
    }
    let mut chars = prefix.chars().take(32);
    chars.clone().count() == 32 && chars.all(|ch| !ch.is_control() || ch.is_whitespace())
}
async fn fetch(
    f: &Fetcher,
    resource: &str,
    headers: &HeaderMap,
    slot: OwnedSemaphorePermit,
) -> Result<Response, ()> {
    if resource.len() > 16448 {
        return Err(());
    }
    let url = f.tokens.open(resource)?;
    if !permitted(&f.origin, &url) {
        return Err(());
    }
    if f.live && url != f.origin {
        return Err(());
    }
    let (prefix, response) = if f.live && f.inspected {
        f.prefetched.lock().unwrap().take().ok_or(())?
    } else {
        let mut request = f.client.get(url.clone());
        if let Some(key) = &f.key {
            request = request.header("X-Flussonix-Peer", key);
        }
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
    let response_url = response.url().clone();
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
    if !f.live && url == f.origin && !is_playlist {
        // The configured HLS entry must remain a playlist. FFmpeg may
        // otherwise auto-detect a different network manifest format and
        // resolve its media references outside this origin-scoped proxy.
        return Err(());
    }
    if !is_playlist && unexpected_text_media(&data) {
        return Err(());
    }
    let body = if is_playlist {
        playlist(f, &response_url, &data)?
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
    let mut request = f.client.get(f.origin.clone());
    if let Some(key) = &f.key {
        request = request.header("X-Flussonix-Peer", key);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| "cannot connect peer transport")?;
    if response.status() != StatusCode::OK {
        return Err("peer transport rejected".into());
    }
    // A healthy publishing source may still be probing. Engine bounds total
    // startup; the independent PMT budget starts once actual media arrives.
    let first = loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|_| "peer transport metadata read failed")?
            .ok_or("peer transport metadata missing")?;
        if !chunk.is_empty() {
            break chunk;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut probe = crate::ts_profile::Probe::default();
        let mut prefix = BytesMut::new();
        let mut first = Some(first);
        while probe.audio.is_none() {
            let chunk = if let Some(chunk) = first.take() {
                chunk
            } else {
                response
                    .chunk()
                    .await
                    .map_err(|_| "peer transport metadata read failed")?
                    .ok_or("peer transport metadata missing")?
            };
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
        Self::start_inner(input, Some(key), false, false, None, None).await
    }
    pub(crate) async fn start_inspected(
        input: &str,
        key: &str,
        ca: Option<&std::path::Path>,
        allow_insecure_peer_http: bool,
    ) -> Result<Self, String> {
        Self::start_inner(input, Some(key), allow_insecure_peer_http, true, ca, None).await
    }
    pub(crate) async fn start_external(
        input: &str,
        ca: Option<&std::path::Path>,
        live: bool,
    ) -> Result<Self, String> {
        Self::start_inner(input, None, false, false, ca, Some(live)).await
    }
    async fn start_inner(
        input: &str,
        key: Option<&str>,
        allow_insecure_peer_http: bool,
        inspect: bool,
        ca: Option<&std::path::Path>,
        external_live: Option<bool>,
    ) -> Result<Self, String> {
        let mut origin = Url::parse(input).map_err(|_| "invalid peer media URL")?;
        let authorization = crate::http_basic::take_url_credentials(&mut origin)?;
        if authorization.is_some() && key.is_some() {
            return Err("HTTP Basic input credentials cannot be combined with a peer key".into());
        }
        if key.is_some() {
            crate::peer_transport::require(&origin, allow_insecure_peer_http)
                .map_err(str::to_owned)?;
        }
        let live = external_live.unwrap_or_else(|| origin.path().ends_with("/mpegts"));
        if external_live.is_some() && !matches!(origin.scheme(), "http" | "https") {
            return Err("external input requires HTTP(S)".into());
        }
        if !permitted(&origin, &origin)
            || (external_live.is_none() && !(origin.path().ends_with(".m3u8") || live))
        {
            return Err("native peer HTTP input requires an HLS playlist or /mpegts URL".into());
        }
        if ca.is_some() && origin.scheme() != "https" {
            return Err("peer media CA requires HTTPS".into());
        }
        let key = key
            .map(reqwest::header::HeaderValue::from_str)
            .transpose()
            .map_err(|_| "invalid peer key")?;
        let redirect = if key.is_some() {
            reqwest::redirect::Policy::none()
        } else {
            let configured = origin.clone();
            reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 3 || !permitted(&configured, attempt.url()) {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            })
        };
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
            .redirect(redirect)
            .connect_timeout(Duration::from_secs(5));
        if let Some(authorization) = authorization {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(reqwest::header::AUTHORIZATION, authorization);
            client = client.default_headers(headers);
        }
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
            tokens: Arc::new(ResourceTokens::new().map_err(|_| "cannot create peer media token")?),
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

#[cfg(test)]
mod external_origin_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn media_text_guard_accepts_hls_media_and_rejects_embedded_manifests() {
        let ts_packet = [0x47u8; 188];
        for body in [
            b"<?xml version=\"1.0\"?><MPD><BaseURL>https://example.invalid/clip.mp4</BaseURL></MPD>".as_slice(),
            "<?xml version=\"1.0\"?><!--é--><MPD><BaseURL>https://example.invalid/clip.mp4</BaseURL></MPD>".as_bytes(),
            b"\xef\xbb\xbf  <MPD><BaseURL>https://example.invalid/clip.mp4</BaseURL></MPD>",
            b"ffconcat version 1.0\nfile http://example.invalid/clip.ts\n",
            b"   #EXTM3U\nhttps://example.invalid/clip.ts\n",
        ] {
            assert!(unexpected_text_media(body), "rejected text manifest: {body:?}");
        }
        for body in [
            b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello\n".as_slice(),
            ts_packet.as_slice(),
            b"\x00\x00\x00\x18ftypiso6\x00\x00\x00\x00iso6dash",
            b"0123456789abcdef",
        ] {
            assert!(!unexpected_text_media(body), "accepted media: {body:?}");
        }
        assert!(!unexpected_text_media(&[0xff; 188]));
    }

    #[tokio::test]
    async fn proxy_rejects_text_manifest_bytes_in_a_segment() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_addr = listener.local_addr().unwrap();
        let source = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route(
                        "/entry.m3u8",
                        get(|| async { "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nsegment.bin\n#EXT-X-ENDLIST\n" }),
                    )
                    .route(
                        "/segment.bin",
                        get(|| async {
                            "<?xml version=\"1.0\"?><MPD><BaseURL>http://example.invalid/foreign.mp4</BaseURL></MPD>"
                        }),
                    ),
            )
            .await
            .unwrap();
        });
        let input = format!("http://{source_addr}/entry.m3u8");
        let bridge = PeerHls::start_external(&input, None, false).await.unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let playlist = client
            .get(&bridge.url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let segment = playlist
            .lines()
            .find(|line| line.starts_with("http://"))
            .unwrap();
        assert_eq!(client.get(segment).send().await.unwrap().status(), 502);
        drop(bridge);
        source.abort();
    }

    #[test]
    fn resource_tokens_reject_modification_and_other_fetchers() {
        let first = ResourceTokens::new().unwrap();
        let second = ResourceTokens::new().unwrap();
        let origin = Url::parse("https://example.test/segment.ts?token=owned-secret").unwrap();
        let token = first.seal(&origin).unwrap();
        assert_eq!(first.open(&token).unwrap(), origin);
        assert!(second.open(&token).is_err());
        let mut changed = URL_SAFE_NO_PAD.decode(&token).unwrap();
        *changed.last_mut().unwrap() ^= 1;
        assert!(first.open(&URL_SAFE_NO_PAD.encode(changed)).is_err());
    }

    #[tokio::test]
    async fn loopback_resource_tokens_do_not_disclose_source_queries() {
        let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_addr = source_listener.local_addr().unwrap();
        let source = tokio::spawn(async move {
            axum::serve(
                source_listener,
                Router::new()
                    .route(
                        "/entry.m3u8",
                        get(|query: axum::extract::Query<std::collections::HashMap<String, String>>| async move {
                            assert_eq!(query.get("token").map(String::as_str), Some("owned-root-secret"));
                            "#EXTM3U\n#EXTINF:1,\nsegment.ts?token=owned-segment-secret\n#EXT-X-ENDLIST\n"
                        }),
                    )
                    .route(
                        "/segment.ts",
                        get(|query: axum::extract::Query<std::collections::HashMap<String, String>>| async move {
                            assert_eq!(query.get("token").map(String::as_str), Some("owned-segment-secret"));
                            vec![0x47; 188]
                        }),
                    ),
            )
            .await
            .unwrap();
        });
        let origin = format!("http://{}/entry.m3u8?token=owned-root-secret", source_addr);
        let bridge = PeerHls::start_external(&origin, None, false).await.unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let playlist = client
            .get(&bridge.url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let segment = playlist
            .lines()
            .find(|line| line.starts_with("http://"))
            .unwrap();
        for local in [&bridge.url, segment] {
            let parsed = Url::parse(local).unwrap();
            let token = parsed.path_segments().unwrap().nth(1).unwrap();
            let decoded = URL_SAFE_NO_PAD.decode(token).unwrap();
            for secret in ["owned-root-secret", "owned-segment-secret"] {
                assert!(
                    !decoded
                        .windows(secret.len())
                        .any(|window| window == secret.as_bytes())
                );
            }
        }
        let response = client.get(segment).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().len(), 188);
        drop(bridge);
        source.abort();
    }

    #[tokio::test]
    async fn uncredentialed_playlist_rejects_foreign_aes_key_uri() {
        let foreign_hits = Arc::new(AtomicUsize::new(0));
        let observed = foreign_hits.clone();
        let foreign_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!(
            "http://{}/secret.key",
            foreign_listener.local_addr().unwrap()
        );
        let foreign = tokio::spawn(async move {
            axum::serve(
                foreign_listener,
                Router::new().route(
                    "/secret.key",
                    get(move || {
                        observed.fetch_add(1, Ordering::SeqCst);
                        async { vec![0u8; 16] }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let source_hits = Arc::new(AtomicUsize::new(0));
        let seen = source_hits.clone();
        let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!(
            "http://{}/entry.m3u8",
            source_listener.local_addr().unwrap()
        );
        let source = tokio::spawn(async move {
            axum::serve(
                source_listener,
                Router::new().route("/entry.m3u8", get(move || {
                    seen.fetch_add(1, Ordering::SeqCst);
                    let key = key.clone();
                    async move { format!("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"{key}\"\n#EXTINF:1,\nsegment.ts\n") }
                })),
            )
            .await
            .unwrap();
        });
        let bridge = PeerHls::start_external(&origin, None, false).await.unwrap();
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(&bridge.url)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(source_hits.load(Ordering::SeqCst), 1);
        assert_eq!(foreign_hits.load(Ordering::SeqCst), 0);
        drop(bridge);
        source.abort();
        foreign.abort();
    }
}
