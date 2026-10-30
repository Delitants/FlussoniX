//! Independent M4 HTTP ingest. Privileged peer credentials never follow redirects.
pub(crate) mod authority;
mod bootstrap;
pub mod flussonic;
pub(crate) mod m4f_session;
pub(crate) mod records;
use crate::{
    m4s::{Decoder, Event, PackedGop, Track},
    wire::{Hub, Segment},
    worker_ts::Muxer,
};
use authority::{Authority, check as check_authority};
use bytes::{Bytes, BytesMut};
use flussonic::diagnostic::{Diagnostics, Phase};
use futures_util::StreamExt;
use std::{
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

#[cfg(test)]
mod peer_transport_tests {
    use super::*;

    // Break caught: M4 media fetches can send the native key over remote HTTP.
    #[tokio::test]
    async fn keyed_remote_m4s_input_requires_secure_transport_by_default() {
        let result = pull_inner(
            "m4s://localhost:9/music",
            Some("owned-native-peer-key"),
            &mut tokio::io::sink(),
            None,
            None,
            PullOptions::default(),
        )
        .await;
        assert_eq!(
            result,
            Err("peer key requires HTTPS or literal loopback".into())
        );
    }
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
    diagnostic: Option<&'a Diagnostics>,
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
    Diagnostics::phase(policy.diagnostic, Phase::BridgeMetadata);
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
        Diagnostics::metadata(policy.diagnostic);
    }
    Diagnostics::phase(policy.diagnostic, Phase::OutputWrite);
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
    diagnostic: Option<&Diagnostics>,
) -> Result<(), String> {
    Diagnostics::phase(diagnostic, Phase::BridgeFrame);
    let bytes = muxer
        .as_mut()
        .ok_or("native frame before metadata")?
        .frame(frame)?;
    Diagnostics::phase(diagnostic, Phase::OutputWrite);
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
    pub allow_insecure_peer_http: bool,
    pub preserve: bool,
    pub detected: Option<&'a AtomicU64>,
    pub conversion: Option<&'a crate::caption_hls::State>,
    pub failure: Option<&'a std::sync::atomic::AtomicU8>,
    pub diagnostic: Option<&'a Diagnostics>,
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
    if key.is_some() {
        crate::peer_transport::require(&base, subtitles.allow_insecure_peer_http)
            .map_err(str::to_owned)?;
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
    } else {
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
    fn get(
        &self,
        url: url::Url,
        playback_session: Option<&str>,
    ) -> Result<reqwest::RequestBuilder, String> {
        let request = self.client.get(url);
        Ok(match &self.auth {
            RequestAuth::Native(Some(key)) => request.header("X-Flussonix-Peer", key.as_ref()),
            RequestAuth::Native(None) => request,
            RequestAuth::Flussonic { key, name } => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| "media authentication clock unavailable")?
                    .as_secs();
                let request = request.headers(flussonic::media_headers(key, name, timestamp));
                if let Some(session) = playback_session {
                    request.header("x-playback-session-id", session)
                } else {
                    request
                }
            }
        })
    }
    fn accepts(&self, response: &reqwest::Response) -> bool {
        match self.auth {
            RequestAuth::Flussonic { .. } => {
                // A public media200 is not acknowledgment of privileged auth.
                // This marker confirms acceptance only, never server identity.
                response.status() == reqwest::StatusCode::OK && self.private_acknowledged(response)
            }
            RequestAuth::Native(_) => response.status().is_success(),
        }
    }
    fn private_acknowledged(&self, response: &reqwest::Response) -> bool {
        if !matches!(self.auth, RequestAuth::Flussonic { .. }) {
            return false;
        }
        let mut markers = response.headers().get_all("cluster-auth").iter();
        markers
            .next()
            .is_some_and(|value| value.as_bytes() == b"yes")
            && markers.next().is_none()
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
    metadata: Option<oneshot::Sender<Vec<Track>>>,
    subtitles: PullOptions<'_>,
    authority: Authority<'_>,
) -> Result<(), String> {
    let mut session = m4f_session::M4fSession::default();
    consume_guarded_session(
        requests,
        output,
        hub,
        metadata,
        subtitles,
        Some(&mut session),
        false,
        authority,
    )
    .await
}

async fn consume_guarded_session<W: AsyncWrite + Unpin>(
    requests: &Requests,
    output: &mut W,
    hub: Option<&Hub>,
    mut metadata: Option<oneshot::Sender<Vec<Track>>>,
    subtitles: PullOptions<'_>,
    session: Option<&mut m4f_session::M4fSession>,
    classify_clean_eof: bool,
    authority: Authority<'_>,
) -> Result<(), String> {
    let mut fallback = m4f_session::M4fSession::default();
    let session = session.unwrap_or(&mut fallback);
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
        diagnostic: subtitles.diagnostic,
    };
    // A closed M4F playback identity can remain denied upstream. Scope the ID
    // to this pull, keeping control and segment requests in the same session.
    let playback_session = (is_m4f && matches!(requests.auth, RequestAuth::Flussonic { .. }))
        .then(|| format!("m4f-{}", uuid::Uuid::new_v4()));
    Diagnostics::phase(subtitles.diagnostic, Phase::ControlRequest);
    let request = requests
        .get(requests.control.clone(), playback_session.as_deref())?
        .header("X-Supported", "prepush,drop_status");
    check_authority(authority)?;
    let response = tokio::time::timeout(Duration::from_secs(10), request.send())
        .await
        .map_err(|_| transient("media setup timed out"))?
        .map_err(|_| transient("media connection failed"))?;
    check_authority(authority)?;
    Diagnostics::phase(subtitles.diagnostic, Phase::ControlResponse);
    Diagnostics::response(
        subtitles.diagnostic,
        response.status().as_u16(),
        requests.private_acknowledged(&response),
    );
    if !requests.accepts(&response) {
        return Err(rejection(&response, "source rejected input").into());
    }
    let mut stream = response.bytes_stream();
    let mut decoder = Decoder::default();
    let mut signals = Signals::default();
    loop {
        Diagnostics::phase(subtitles.diagnostic, Phase::ControlRead);
        let bytes = tokio::time::timeout(Duration::from_secs(15), stream.next())
            .await
            .map_err(|_| transient("source timed out"))?
            .ok_or_else(|| {
                if classify_clean_eof && is_m4f && !signals.pending.is_empty() {
                    flussonic::failure::record(
                        subtitles.failure,
                        flussonic::InputFailure::Invalid,
                        "incomplete M4F signal",
                    )
                } else if classify_clean_eof
                    && is_m4f
                    && session.accepted_segments > 0
                    && !session.tracks.is_empty()
                    && session.muxer.is_some()
                {
                    flussonic::failure::record(
                        subtitles.failure,
                        flussonic::InputFailure::CleanControlEof,
                        "source closed",
                    )
                } else {
                    transient("source closed")
                }
            })?
            .map_err(|_| transient("transport failed"))?;
        check_authority(authority)?;
        if is_m4f {
            Diagnostics::phase(subtitles.diagnostic, Phase::SignalDecode);
            for n in signals.push(&bytes)? {
                check_authority(authority)?;
                if session.seen.contains(&n.name)
                    || session.skipped_startup_segment.as_ref() == Some(&n.name)
                {
                    continue;
                }
                let mut url = requests.base.clone();
                url.set_path(&format!("{}/{}", requests.base.path(), n.name));
                let request = requests
                    .get(url, playback_session.as_deref())?
                    .timeout(Duration::from_secs(10));
                Diagnostics::phase(subtitles.diagnostic, Phase::SegmentRequest);
                check_authority(authority)?;
                let response = request
                    .send()
                    .await
                    .map_err(|_| transient("M4F fetch failed"))?;
                check_authority(authority)?;
                Diagnostics::phase(subtitles.diagnostic, Phase::SegmentResponse);
                Diagnostics::response(
                    subtitles.diagnostic,
                    response.status().as_u16(),
                    requests.private_acknowledged(&response),
                );
                // Missing memory segments can fall through to DVR. A vendor
                // without DVR reports400 with X-Reason:no_dvr instead of404.
                // Only acknowledged announced objects use these availability
                // exceptions; error bodies are never consumed or refetched.
                let no_dvr = response.status() == reqwest::StatusCode::BAD_REQUEST && {
                    let mut reasons = response.headers().get_all("x-reason").iter();
                    reasons.next().is_some_and(|v| v.as_bytes() == b"no_dvr")
                        && reasons.next().is_none()
                };
                if (response.status() == reqwest::StatusCode::NOT_FOUND || no_dvr)
                    && requests.private_acknowledged(&response)
                {
                    // A live announcement can outlast the origin's memory copy.
                    // Before any metadata, allow one distinct missing object and
                    // keep the existing control session for the next GOP. Worker
                    // startup/cancellation deadlines still own this entire wait.
                    if session.tracks.is_empty() && session.skipped_startup_segment.is_none() {
                        session.skipped_startup_segment = Some(n.name);
                        tracing::debug!(
                            target: "flussonix::private_input",
                            protocol = "m4f",
                            last_http_status = response.status().as_u16(),
                            last_private_ack = true,
                            metadata_sent = false,
                            "skipped unavailable startup segment"
                        );
                        continue;
                    }
                    return Err(transient("M4F segment unavailable").into());
                }
                if !requests.accepts(&response) {
                    return Err(rejection(&response, "M4F segment denied").into());
                }
                let mut body = BytesMut::new();
                let mut chunks = response.bytes_stream();
                Diagnostics::phase(subtitles.diagnostic, Phase::SegmentRead);
                while let Some(chunk) = chunks.next().await {
                    check_authority(authority)?;
                    let chunk = chunk.map_err(|_| transient("M4F fetch failed"))?;
                    if body.len() + chunk.len() > 16 * 1024 * 1024 {
                        return Err("M4F segment too large".into());
                    }
                    body.extend_from_slice(&chunk);
                }
                let body = body.freeze();
                Diagnostics::phase(subtitles.diagnostic, Phase::SegmentDecode);
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
                    &mut session.tracks,
                    &mut session.declared,
                    &mut session.muxer,
                    source_tracks,
                    &mut metadata,
                    policy,
                )
                .await?;
                if let Some(hub) = hub {
                    // A paced bridge can block within this GOP. Validate all
                    // samples before exposing the original native relay object.
                    Diagnostics::phase(subtitles.diagnostic, Phase::BridgeFrame);
                    session
                        .muxer
                        .as_ref()
                        .ok_or("native frame before metadata")?
                        .validate_frames(&frames)?;
                    Diagnostics::phase(subtitles.diagnostic, Phase::Relay);
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
                for f in &frames {
                    write_frame(stdin, &mut session.muxer, f, subtitles.diagnostic).await?;
                }
                session.accepted_segments = session.accepted_segments.saturating_add(1);
                session.seen.push_back(n.name);
                if session.seen.len() > 16 {
                    session.seen.pop_front();
                }
            }
        } else {
            Diagnostics::phase(subtitles.diagnostic, Phase::M4sDecode);
            records::events(
                stdin,
                records::State {
                    tracks: &mut session.tracks,
                    declared: &mut session.declared,
                    muxer: &mut session.muxer,
                    metadata: &mut metadata,
                },
                hub,
                policy,
                decoder.push(&bytes)?,
            )
            .await?;
        }
    }
}
