//! Independent M4 HTTP ingest. Privileged peer credentials never follow redirects.
pub(crate) mod authority;
pub mod flussonic;
use crate::{
    m4s::{Decoder, Event, PackedGop, Track},
    wire::{Hub, Segment},
    worker_ts::Muxer,
};
use authority::{Authority, check as check_authority};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::oneshot,
};

pub struct Notification {
    pub name: String,
    pub stamp: String,
    pub utc: u32,
    pub sequence: u32,
    pub duration_ms: f64,
    pub wire: Bytes,
}
#[derive(Default)]
pub struct Signals {
    pending: BytesMut,
}
impl Signals {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Notification>, String> {
        let mut out = Vec::new();
        for part in bytes.split_inclusive(|b| *b == b'\n') {
            if self.pending.len() + part.len() > 8192 {
                return Err("M4F signal exceeds limit".into());
            }
            self.pending.extend_from_slice(part);
            if part.last() == Some(&b'\n') {
                let wire = self.pending.split().freeze();
                let line = std::str::from_utf8(&wire).map_err(|_| "invalid M4F signal")?;
                let mut fields = line.split_whitespace();
                let sequence = fields
                    .next()
                    .ok_or("missing M4F sequence")?
                    .parse::<u32>()
                    .map_err(|_| "invalid M4F sequence")?;
                let path = fields.next().ok_or("missing M4F segment path")?;
                if fields.next().is_some() {
                    return Err("unsupported M4F signal fields".into());
                }
                let (stamp, duration) = path.split_once('-').ok_or("missing M4F duration")?;
                let date = chrono::NaiveDateTime::parse_from_str(stamp, "%Y/%m/%d/%H/%M/%S")
                    .map_err(|_| "invalid M4F segment path")?;
                // Reject noncanonical paths before constructing any fetch URL.
                if date.format("%Y/%m/%d/%H/%M/%S").to_string() != stamp {
                    return Err("noncanonical M4F segment path".into());
                }
                let utc =
                    u32::try_from(date.and_utc().timestamp()).map_err(|_| "invalid M4F UTC")?;
                let duration_ms = duration
                    .parse::<u64>()
                    .ok()
                    .filter(|d| *d > 0 && *d <= 3600000)
                    .ok_or("invalid M4F duration")? as f64;
                out.push(Notification {
                    name: format!("{stamp}.m4f"),
                    stamp: stamp.into(),
                    utc,
                    sequence,
                    duration_ms,
                    wire,
                });
            }
        }
        Ok(out)
    }
}
#[derive(Clone, Copy)]
struct SubtitlePolicy<'a> {
    preserve: bool,
    copy: bool,
    sparse: bool,
    detected: Option<&'a AtomicU64>,
    conversion: Option<&'a crate::caption_hls::State>,
    authority: Authority<'a>,
    failure: Option<&'a std::sync::atomic::AtomicU8>,
}
async fn configs<W: AsyncWrite + Unpin>(
    stdin: &mut W,
    tracks: &mut Vec<Track>,
    declared: &mut Option<Vec<Track>>,
    muxer: &mut Option<Muxer>,
    new: Vec<Track>,
    metadata: &mut Option<oneshot::Sender<Vec<Track>>>,
    policy: SubtitlePolicy<'_>,
) -> Result<(), String> {
    check_authority(policy.authority)?;
    if let Some(state) = policy.conversion {
        state.native_tracks(&new);
    }
    if let Some(count) = policy.detected {
        count.fetch_max(
            new.iter().filter(|t| t.codec == "subtitle").count() as u64,
            Ordering::Relaxed,
        );
    }
    if policy.preserve && !policy.copy && new.iter().any(|t| t.codec == "subtitle") {
        return Err("native_subtitle_transcode_unsupported".into());
    }
    if !policy.sparse {
        if declared.as_ref().is_some_and(|old| old != &new) {
            return Err("native metadata changed; worker restart required".into());
        }
        *declared = Some(new.clone());
    }
    if muxer.is_some() {
        if *tracks != new {
            // Segment inventories omit silent text tracks. Keep the existing AV
            // muxer, continuity counters and clock when only text presence changes.
            muxer.as_mut().unwrap().sparse_tracks(&new)?;
            *tracks = new;
        }
        return Ok(());
    }
    let mut next = Muxer::new(&new)?;
    if let Some(sender) = metadata.take() {
        check_authority(policy.authority)?;
        sender.send(new.clone()).map_err(|_| {
            flussonic::failure::record(
                policy.failure,
                flussonic::InputFailure::Local,
                "native startup cancelled",
            )
        })?;
    }
    stdin
        .write_all(&next.tables())
        .await
        .map_err(|_| "media pipe closed")?;
    *tracks = new;
    *muxer = Some(next);
    Ok(())
}
/// Validate the bounded native-to-MPEG-TS worker representation.
pub fn validate_bridge(tracks: &[Track]) -> Result<(), String> {
    Muxer::new(tracks).map(|_| ())
}
async fn write_frame<W: AsyncWrite + Unpin>(
    stdin: &mut W,
    muxer: &mut Option<Muxer>,
    frame: &crate::m4f::Frame,
) -> Result<(), String> {
    let bytes = muxer
        .as_mut()
        .ok_or("native frame before metadata")?
        .frame(frame)?;
    stdin
        .write_all(&bytes)
        .await
        .map_err(|_| "media pipe closed".into())
}
pub async fn pull(
    input: &str,
    key: Option<&str>,
    stdin: &mut tokio::process::ChildStdin,
    hub: Option<&Hub>,
) -> Result<(), String> {
    pull_inner(input, key, stdin, hub, None, PullOptions::default()).await
}
/// Supply validated metadata before the first TS write, on the same input session.
pub async fn pull_ready<W: AsyncWrite + Unpin>(
    input: &str,
    key: Option<&str>,
    output: &mut W,
    hub: Option<&Hub>,
    metadata: oneshot::Sender<Vec<Track>>,
) -> Result<(), String> {
    pull_ready_with_options(input, key, output, hub, metadata, PullOptions::default()).await
}
#[derive(Clone, Copy, Default)]
pub(crate) struct PullOptions<'a> {
    pub ca: Option<&'a std::path::Path>,
    pub preserve: bool,
    pub detected: Option<&'a AtomicU64>,
    pub conversion: Option<&'a crate::caption_hls::State>,
    pub failure: Option<&'a std::sync::atomic::AtomicU8>,
}
pub(crate) async fn pull_ready_with_options<W: AsyncWrite + Unpin>(
    input: &str,
    key: Option<&str>,
    output: &mut W,
    hub: Option<&Hub>,
    metadata: oneshot::Sender<Vec<Track>>,
    subtitles: PullOptions<'_>,
) -> Result<(), String> {
    pull_inner(input, key, output, hub, Some(metadata), subtitles).await
}
async fn pull_inner<W: AsyncWrite + Unpin>(
    input: &str,
    key: Option<&str>,
    stdin: &mut W,
    hub: Option<&Hub>,
    metadata: Option<oneshot::Sender<Vec<Track>>>,
    subtitles: PullOptions<'_>,
) -> Result<(), String> {
    let ca = subtitles.ca;
    let is_m4f = input.starts_with("m4f");
    let suffix = if is_m4f { "/m4f" } else { "/m4s" };
    let scheme = if input.starts_with("m4ss://") || input.starts_with("m4fs://") {
        "https"
    } else {
        "http"
    };
    let mut base = url::Url::parse(&format!(
        "{scheme}://{}",
        input.split_once("://").ok_or("invalid URL")?.1
    ))
    .map_err(|_| "invalid media URL")?;
    let authorization = crate::http_basic::take_url_credentials(&mut base)?;
    if authorization.is_some() && key.is_some() {
        return Err("HTTP Basic input credentials cannot be combined with a peer key".into());
    }
    let path = base
        .path()
        .trim_end_matches('/')
        .strip_suffix(suffix)
        .unwrap_or(base.path().trim_end_matches('/'))
        .to_owned();
    base.set_path(&path);
    let mut control = base.clone();
    control.set_path(&format!("{}{suffix}", base.path()));
    if ca.is_some() && scheme != "https" {
        return Err("TLS CA requires a secure native input".into());
    }
    let origin = base.origin();
    let redirects = if key.is_some() {
        reqwest::redirect::Policy::none()
    } else if scheme == "https" || authorization.is_some() {
        reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= 3
                || attempt.url().origin() != origin
                || !attempt.url().username().is_empty()
                || attempt.url().password().is_some()
            {
                attempt.stop()
            } else {
                attempt.follow()
            }
        })
    } else {
        reqwest::redirect::Policy::limited(3)
    };
    let mut client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .https_only(scheme == "https")
        .redirect(redirects);
    if let Some(authorization) = authorization {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, authorization);
        client = client.default_headers(headers);
    }
    if scheme == "https" {
        client = client.use_preconfigured_tls((*crate::tls_input::client(ca)?).clone());
    }
    let client = client.build().map_err(|_| "cannot build input client")?;
    let requests = Requests {
        client,
        base,
        control,
        auth: RequestAuth::Native(key.map(Into::into)),
        is_m4f,
    };
    consume(&requests, stdin, hub, metadata, subtitles).await
}

// Both explicit vendor and native inputs consume the same independent framing,
// codec, subtitle, metadata and worker bridge implementation.
struct Requests {
    client: reqwest::Client,
    base: url::Url,
    control: url::Url,
    auth: RequestAuth,
    is_m4f: bool,
}
enum RequestAuth {
    Native(Option<Box<str>>),
    Flussonic { key: Box<str>, name: Box<str> },
}
impl Requests {
    fn get(&self, url: url::Url) -> Result<reqwest::RequestBuilder, String> {
        let request = self.client.get(url);
        Ok(match &self.auth {
            RequestAuth::Native(Some(key)) => request.header("X-Flussonix-Peer", key.as_ref()),
            RequestAuth::Native(None) => request,
            RequestAuth::Flussonic { key, name } => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| "media authentication clock unavailable")?
                    .as_secs();
                request.headers(flussonic::media_headers(key, name, timestamp))
            }
        })
    }
    fn accepts(&self, response: &reqwest::Response) -> bool {
        match self.auth {
            RequestAuth::Flussonic { .. } => {
                // A public media200 is not acknowledgment of privileged auth.
                // This marker confirms acceptance only, never server identity.
                let mut markers = response.headers().get_all("cluster-auth").iter();
                response.status() == reqwest::StatusCode::OK
                    && markers
                        .next()
                        .is_some_and(|value| value.as_bytes() == b"yes")
                    && markers.next().is_none()
            }
            RequestAuth::Native(_) => response.status().is_success(),
        }
    }
}
async fn consume<W: AsyncWrite + Unpin>(
    requests: &Requests,
    stdin: &mut W,
    hub: Option<&Hub>,
    metadata: Option<oneshot::Sender<Vec<Track>>>,
    subtitles: PullOptions<'_>,
) -> Result<(), String> {
    consume_guarded(requests, stdin, hub, metadata, subtitles, None).await
}
async fn consume_guarded<W: AsyncWrite + Unpin>(
    requests: &Requests,
    output: &mut W,
    hub: Option<&Hub>,
    mut metadata: Option<oneshot::Sender<Vec<Track>>>,
    subtitles: PullOptions<'_>,
    authority: Authority<'_>,
) -> Result<(), String> {
    check_authority(authority)?;
    let mut writer = flussonic::failure::Writer {
        output,
        slot: subtitles.failure,
    };
    let mut guarded = authority::Writer {
        inner: &mut writer,
        authority,
    };
    let stdin = &mut guarded;
    let PullOptions {
        preserve,
        detected,
        conversion,
        ..
    } = subtitles;
    let transient = |message| {
        flussonic::failure::record(
            subtitles.failure,
            flussonic::InputFailure::Transient,
            message,
        )
    };
    let rejection = |response: &reqwest::Response, message| {
        flussonic::failure::record(
            subtitles.failure,
            if response.status().is_server_error() {
                flussonic::InputFailure::Transient
            } else {
                flussonic::InputFailure::Rejected
            },
            message,
        )
    };
    let is_m4f = requests.is_m4f;
    let policy = SubtitlePolicy {
        preserve,
        copy: hub.is_some(),
        sparse: is_m4f,
        detected,
        conversion,
        authority,
        failure: subtitles.failure,
    };
    let request = requests
        .get(requests.control.clone())?
        .header("X-Supported", "prepush,drop_status");
    check_authority(authority)?;
    let response = tokio::time::timeout(Duration::from_secs(10), request.send())
        .await
        .map_err(|_| transient("media setup timed out"))?
        .map_err(|_| transient("media connection failed"))?;
    check_authority(authority)?;
    if !requests.accepts(&response) {
        return Err(rejection(&response, "source rejected input").into());
    }
    let mut stream = response.bytes_stream();
    let mut decoder = Decoder::default();
    let mut signals = Signals::default();
    let mut tracks = Vec::new();
    let mut declared = None;
    let mut muxer = None;
    let mut seen: VecDeque<String> = VecDeque::new();
    loop {
        let bytes = tokio::time::timeout(Duration::from_secs(15), stream.next())
            .await
            .map_err(|_| transient("source timed out"))?
            .ok_or_else(|| transient("source closed"))?
            .map_err(|_| transient("transport failed"))?;
        check_authority(authority)?;
        if is_m4f {
            for n in signals.push(&bytes)? {
                check_authority(authority)?;
                if seen.contains(&n.name) {
                    continue;
                }
                let mut url = requests.base.clone();
                url.set_path(&format!("{}/{}", requests.base.path(), n.name));
                let request = requests.get(url)?.timeout(Duration::from_secs(10));
                check_authority(authority)?;
                let response = request
                    .send()
                    .await
                    .map_err(|_| transient("M4F fetch failed"))?;
                check_authority(authority)?;
                if !requests.accepts(&response) {
                    return Err(rejection(&response, "M4F segment denied").into());
                }
                let mut body = BytesMut::new();
                let mut chunks = response.bytes_stream();
                while let Some(chunk) = chunks.next().await {
                    check_authority(authority)?;
                    let chunk = chunk.map_err(|_| transient("M4F fetch failed"))?;
                    if body.len() + chunk.len() > 16 * 1024 * 1024 {
                        return Err("M4F segment too large".into());
                    }
                    body.extend_from_slice(&chunk);
                }
                let body = body.freeze();
                let (new, frames) = crate::m4f::unpack(&body)?;
                check_authority(authority)?;
                if let Some(state) = conversion {
                    state.native_tracks(&new);
                    for f in &frames {
                        check_authority(authority)?;
                        state.native_frame(&new, f);
                    }
                }
                let source_start = frames.iter().map(|f| f.dts).min();
                let source_tracks = new.clone();
                let (body, new, frames) = if !preserve && new.iter().any(|t| t.codec == "subtitle")
                {
                    let filtered = crate::native_subtitles::segment(&body)?;
                    let (new, frames) = crate::m4f::unpack(&filtered)?;
                    (filtered, new, frames)
                } else {
                    (body, new, frames)
                };
                if frames.is_empty() {
                    return Err("empty M4F segment".into());
                }
                configs(
                    stdin,
                    &mut tracks,
                    &mut declared,
                    &mut muxer,
                    source_tracks,
                    &mut metadata,
                    policy,
                )
                .await?;
                for f in &frames {
                    write_frame(stdin, &mut muxer, f).await?;
                }
                if let Some(hub) = hub {
                    check_authority(authority)?;
                    let gop = PackedGop {
                        utc: n.utc,
                        dts_ms: source_start.unwrap() as f64 / 90.0,
                        sequence: n.sequence,
                        duration_ms: n.duration_ms,
                        body: body.clone(),
                    };
                    hub.relay_segment(
                        Segment {
                            name: n.name.clone(),
                            signal: n.wire,
                            bytes: body,
                        },
                        new.clone(),
                        gop,
                    )?;
                }
                seen.push_back(n.name);
                if seen.len() > 16 {
                    seen.pop_front();
                }
            }
        } else {
            for event in decoder.push(&bytes)? {
                check_authority(authority)?;
                match event {
                    Event::Info { tracks: new, wire } => {
                        configs(
                            stdin,
                            &mut tracks,
                            &mut declared,
                            &mut muxer,
                            new.clone(),
                            &mut metadata,
                            policy,
                        )
                        .await?;
                        if let Some(h) = hub {
                            check_authority(authority)?;
                            let text = new.iter().any(|t| t.codec == "subtitle");
                            let output = if !preserve && text {
                                crate::native_subtitles::info(&wire)?
                            } else {
                                wire
                            };
                            h.relay_info(
                                new.iter()
                                    .filter(|t| preserve || t.codec != "subtitle")
                                    .cloned()
                                    .collect(),
                                output,
                            )?;
                        }
                    }
                    Event::Frame {
                        track_id,
                        dts,
                        pts_offset,
                        key,
                        body,
                        wire,
                    } => {
                        let f = crate::m4f::Frame {
                            track_id,
                            dts,
                            pts_offset,
                            key,
                            body,
                        };
                        if let Some(state) = conversion {
                            state.native_frame(&tracks, &f);
                        }
                        write_frame(stdin, &mut muxer, &f).await?;
                        if let Some(h) = hub {
                            check_authority(authority)?;
                            if preserve
                                || tracks
                                    .iter()
                                    .any(|t| t.id == f.track_id && t.codec != "subtitle")
                            {
                                h.relay_frame(f.clone(), wire)?;
                            }
                        }
                    }
                    Event::Gop {
                        gop,
                        tracks: new,
                        frames,
                        wire,
                    } => {
                        configs(
                            stdin,
                            &mut tracks,
                            &mut declared,
                            &mut muxer,
                            new.clone(),
                            &mut metadata,
                            SubtitlePolicy {
                                sparse: true,
                                ..policy
                            },
                        )
                        .await?;
                        for f in &frames {
                            check_authority(authority)?;
                            if let Some(state) = conversion {
                                state.native_frame(&new, f);
                            }
                            write_frame(stdin, &mut muxer, f).await?;
                        }
                        if let Some(h) = hub {
                            check_authority(authority)?;
                            if !preserve && new.iter().any(|t| t.codec == "subtitle") {
                                let body = crate::native_subtitles::segment(&gop.body)?;
                                let filtered = PackedGop { body, ..gop };
                                let wire = crate::m4s::encode_gop(&filtered)?;
                                h.relay_gop(
                                    filtered,
                                    new.iter()
                                        .filter(|t| t.codec != "subtitle")
                                        .cloned()
                                        .collect(),
                                    wire,
                                )?;
                            } else {
                                h.relay_gop(gop, new.clone(), wire)?;
                            }
                        }
                    }
                    Event::Other { wire } => {
                        if let Some(h) = hub {
                            check_authority(authority)?;
                            h.m4s.send(wire)?;
                        }
                    }
                }
            }
        }
    }
}
