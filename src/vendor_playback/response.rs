use super::*;
use crate::{
    media::PackagedSubscription,
    playback_auth::ViewerRequest,
    server::{error, header, parse_query, rewrite_playlist},
    source_directory::modern_coordinator::AdmissionOutcome,
};
use axum::{
    body::Body,
    extract::{ConnectInfo, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use std::net::SocketAddr;
pub(crate) struct HttpRequest {
    raw_path: String,
    name: String,
    file: String,
    key: String,
    revision: u64,
    query: HashMap<String, String>,
    viewer: ViewerRequest,
    secure: bool,
}
impl HttpRequest {
    pub(crate) fn new(
        request: &Request,
        name: &str,
        file: &str,
        key: String,
        revision: u64,
    ) -> Self {
        let query = parse_query(request.uri().query());
        let hls = !matches!(file, "mpegts" | "m4s" | "m4f") && !file.ends_with(".m4f");
        let viewer = ViewerRequest {
            name: name.into(),
            proto: if hls {
                "hls"
            } else if file == "mpegts" {
                "mpegts"
            } else if file == "m4s" {
                "m4s"
            } else {
                "m4f"
            }
            .into(),
            ip: request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|c| c.0.ip().to_string())
                .unwrap_or_else(|| "127.0.0.1".into()),
            token: query.get("token").cloned().unwrap_or_default(),
            qs: App::http_viewer_query(request.uri().query().unwrap_or("")),
            user_agent: header(request.headers(), "user-agent").unwrap_or("").into(),
            referer: header(request.headers(), "referer").unwrap_or("").into(),
            host: header(request.headers(), "host").unwrap_or("").into(),
            finite_media: file.ends_with(".m4f"),
        };
        Self {
            raw_path: request.uri().path().to_owned(),
            name: name.into(),
            file: file.into(),
            key,
            revision,
            query,
            viewer,
            secure: request
                .extensions()
                .get::<crate::http_tls::SecureHttp>()
                .is_some(),
        }
    }
}
impl Runtime {
    pub(crate) async fn serve(&self, app: &Arc<App>, request: HttpRequest) -> Response {
        let HttpRequest {
            raw_path,
            name,
            file,
            key,
            revision,
            query,
            viewer,
            secure,
        } = request;
        let (name, file, key) = (name.as_str(), file.as_str(), key.as_str());
        if app.modern_source_coordinator().is_err() || !self.running() {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "vendor playback owner unavailable",
            );
        }
        if let Some(ticket) = query.get("flussonix_ticket")
            && (app.options.role == "lb" || !app.vendor_http_ticket_current(name, ticket).await)
        {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "admission ticket invalid or expired",
            );
        }
        if !matches!(file, "mpegts" | "m4s" | "m4f")
            && ![".m3u8", ".ts", ".mp4", ".m4s", ".m4f", ".vtt"]
                .iter()
                .any(|suffix| file.ends_with(suffix))
        {
            return error(StatusCode::NOT_FOUND, "media path unavailable");
        }
        let candidate = match self.candidate(app, key, revision, name).await {
            Ok(c) => c,
            Err(Failure::Absent) => return error(StatusCode::NOT_FOUND, "stream not found"),
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "vendor source unavailable"),
        };
        let raw_viewer_query = viewer.qs.clone();
        let output_revision = candidate.config_revision();
        let output_key = candidate.source_key().to_owned();
        let outcome = tokio::select! {biased;_=self.closed.cancelled()=>return error(StatusCode::SERVICE_UNAVAILABLE,"vendor playback stopped"),outcome=self.admit_selection(candidate,&app.playback_auth,viewer)=>outcome};
        let admission = match outcome {
            Ok(AdmissionOutcome::Allowed(a)) => a,
            Ok(AdmissionOutcome::Denied) => return error(StatusCode::FORBIDDEN, "playback denied"),
            Ok(AdmissionOutcome::Redirect(url)) => {
                if secure && !url::Url::parse(&url).is_ok_and(|u| u.scheme() == "https") {
                    return error(
                        StatusCode::FORBIDDEN,
                        "secure playback redirect cannot downgrade",
                    );
                }
                return (StatusCode::FOUND, [("location", url)]).into_response();
            }
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "vendor authority changed"),
        };
        if app.options.role == "lb" {
            return tokio::select! {biased;
                _=self.closed.cancelled()=>error(StatusCode::SERVICE_UNAVAILABLE,"vendor playback stopped"),
                response=app.balance_vendor(&self.owner,&admission,&raw_path,&raw_viewer_query,secure)=>response,
            };
        }
        let hls = !matches!(file, "mpegts" | "m4s" | "m4f") && !file.ends_with(".m4f");
        let settings = match self.output_settings(app, &output_key, output_revision) {
            Ok(settings) => settings,
            Err(_) => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "vendor processing unavailable",
                );
            }
        };
        if let Some(ticket) = query.get("flussonix_ticket") {
            if !app
                .redeem_vendor_http_ticket(name, ticket, &self.owner, &admission)
                .await
            {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "admission ticket invalid or expired",
                );
            }
            return App::http_ticket_redirect(&raw_path, &query);
        }
        let _attempt = match self.register_recovery(app, name, key, revision, &admission, &settings)
        {
            Ok(attempt) => attempt,
            Err(_) => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "vendor recovery capacity unavailable",
                );
            }
        };
        let package = async {
            if hls {
                self.sessions
                    .package_configured(&app.media, admission, &settings)
                    .await
                    .map_err(|_| ())
            } else {
                let attachment = self
                    .inputs
                    .attach_configured(admission, &settings)
                    .map_err(|_| ())?;
                app.media
                    .package_shared(attachment, &settings)
                    .await
                    .map_err(|_| ())
            }
        };
        let playback = match tokio::select! {biased;_=self.closed.cancelled()=>Err(()),result=package=>result}
        {
            Ok(p) => p,
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "vendor media unavailable"),
        };
        self.remember_recovery(name, &playback);
        if matches!(file, "mpegts" | "m4s" | "m4f") {
            if file != "mpegts" {
                let deadline = Instant::now() + Duration::from_secs(8);
                let deadline = if file == "m4f" {
                    deadline.max(playback.startup_deadline())
                } else {
                    deadline
                };
                let worker = playback.worker();
                let ready = || {
                    if file == "m4f" {
                        worker.wire.has_m4f_segment()
                    } else {
                        worker.bytes.load(std::sync::atomic::Ordering::Relaxed) > 0
                    }
                };
                while playback.current().is_ok() && Instant::now() < deadline && !ready() {
                    tokio::select! {biased;_=self.closed.cancelled()=>return error(StatusCode::SERVICE_UNAVAILABLE,"vendor playback stopped"),_=tokio::time::sleep(Duration::from_millis(25))=>{}}
                }
                if file == "m4f" && !ready() {
                    return error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "M4F segment not available yet",
                    );
                }
            }
            let subscription = match file {
                "mpegts" => playback.subscribe_ts(),
                "m4s" => playback.subscribe_m4s(),
                _ => playback.subscribe_m4f(),
            };
            let Ok(subscription) = subscription else {
                return error(StatusCode::SERVICE_UNAVAILABLE, "vendor media not ready");
            };
            return continuous(subscription, app.egress.clone(), file);
        }
        // Keep the normal read grace, but do not cut short a current worker's
        // configured startup allowance. The allowance belongs to its generation.
        let deadline = (Instant::now() + Duration::from_secs(8)).max(playback.startup_deadline());
        let bytes = loop {
            let read = tokio::select! {biased;_=self.closed.cancelled()=>return error(StatusCode::SERVICE_UNAVAILABLE,"vendor playback stopped"),result=app.media.read_shared(&playback,file)=>result};
            match read {
                Ok(bytes) => break bytes,
                Err(_)
                    if file.ends_with(".m3u8")
                        && Instant::now() < deadline
                        && playback.current().is_ok() =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "media not ready"),
            }
        };
        let bytes = if file.ends_with(".m3u8") {
            let suffix = url::form_urlencoded::Serializer::new(String::new())
                .extend_pairs(query.iter())
                .finish();
            Bytes::from(rewrite_playlist(&String::from_utf8_lossy(&bytes), &suffix))
        } else {
            bytes
        };
        if playback.current().is_err() {
            return error(StatusCode::FORBIDDEN, "playback revoked");
        }
        app.egress.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let content_type = if file.ends_with(".m3u8") {
            "application/vnd.apple.mpegurl"
        } else if file.ends_with(".ts") {
            "video/mp2t"
        } else if file.ends_with(".vtt") {
            "text/vtt; charset=utf-8"
        } else if file.ends_with(".m4f") {
            "application/x-video-m4f"
        } else {
            "video/mp4"
        };
        (
            [
                ("content-type", content_type),
                ("cache-control", "no-store"),
            ],
            bytes,
        )
            .into_response()
    }
}
fn continuous(
    subscription: PackagedSubscription,
    egress: Arc<std::sync::atomic::AtomicU64>,
    file: &str,
) -> Response {
    let stream = futures_util::stream::unfold(
        (subscription, egress),
        |(mut subscription, egress)| async move {
            let bytes = subscription.recv().await.ok()?;
            egress.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            Some((Ok::<Bytes, std::io::Error>(bytes), (subscription, egress)))
        },
    );
    let content_type = match file {
        "mpegts" => "video/mp2t",
        "m4s" => "application/x-video-m4s",
        _ => "application/x-video-m4f-signal",
    };
    (
        [
            ("content-type", content_type),
            ("cache-control", "no-store"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}
