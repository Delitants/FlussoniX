//! Streaming MPEG-TS publication with bounded publisher admission and renewal.
use super::{App, error, header};
use crate::publish::{Policy, is_input};
use axum::{
    body::Body,
    extract::{ConnectInfo, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, time::Instant};

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub(crate) config: Value,
    pub(crate) policy: Policy,
    signature: String,
}
pub(crate) fn snapshot(app: &App, name: &str) -> Option<Snapshot> {
    app.config.read(|root| {
        let config = crate::config::effective(root, name)?;
        if config["disabled"] == true || !is_input(&config) {
            return None;
        }
        let policy = Policy::from_config(&config, root).ok()?;
        let signature = crate::media::media_signature(&config);
        Some(Snapshot {
            config,
            policy,
            signature,
        })
    })
}
pub(crate) fn current(app: &App, name: &str, expected: &Snapshot) -> bool {
    snapshot(app, name)
        .is_some_and(|s| s.signature == expected.signature && s.policy == expected.policy)
}
pub(crate) struct Session {
    pub(crate) metadata: Value,
    pub(crate) started: Instant,
    pub(crate) number: u64,
    pub(crate) bytes: u64,
}
impl Session {
    async fn authorize(&mut self, app: &App, policy: &Policy) -> Result<Duration, ()> {
        let Some(url) = &policy.url else {
            return Ok(Duration::from_secs(3600));
        };
        let mut metadata = self.metadata.clone();
        metadata["request_number"] = json!(self.number);
        metadata["request_type"] = json!(if self.number == 0 {
            "new_session"
        } else {
            "update_session"
        });
        metadata["duration"] = json!(self.started.elapsed().as_secs());
        metadata["bytes"] = json!(self.bytes);
        metadata["stream_clients"] =
            app.stream_stats(metadata["name"].as_str().ok_or(())?).await["online_clients"].clone();
        metadata["total_clients"] = json!(app.active().await);
        self.number = self.number.saturating_add(1);
        let response = app
            .client
            .post(url)
            .json(&metadata)
            .send()
            .await
            .map_err(|_| ())?;
        if response.status() != StatusCode::OK {
            return Err(());
        }
        let seconds = match response.headers().get("x-authduration") {
            Some(v) => v
                .to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|s| (1..=3600).contains(s))
                .ok_or(())?,
            None => 30,
        };
        Ok(Duration::from_secs(seconds))
    }
}
pub(crate) async fn authorize_current(
    session: &mut Session,
    app: &App,
    name: &str,
    expected: &Snapshot,
    worker: Option<&crate::media::Worker>,
) -> Result<Duration, StatusCode> {
    let decision = session.authorize(app, &expected.policy);
    tokio::pin!(decision);
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let closed = async {
        match worker {
            Some(w) => w.closed().await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(closed);
    loop {
        tokio::select! { biased;
            _ = &mut closed => return Err(StatusCode::SERVICE_UNAVAILABLE),
            _ = tick.tick() => { if !current(app, name, expected) { return Err(StatusCode::FORBIDDEN); } },
            result = &mut decision => return result.map_err(|_| StatusCode::FORBIDDEN),
        }
    }
}
#[derive(Default)]
struct Packets {
    partial: Vec<u8>,
}
impl Packets {
    fn push(&mut self, piece: &[u8]) -> Result<Vec<u8>, ()> {
        let mut data = std::mem::take(&mut self.partial);
        data.extend_from_slice(piece);
        let complete = data.len() / 188 * 188;
        if data[..complete].chunks_exact(188).any(|p| p[0] != 0x47) {
            return Err(());
        }
        self.partial = data.split_off(complete);
        Ok(data)
    }
}
pub async fn receive(app: Arc<App>, request: Request<Body>) -> Response {
    let raw = request.uri().path().trim_start_matches('/');
    let Some(raw) = raw.strip_suffix("/mpegts") else {
        return error(StatusCode::NOT_FOUND, "publication path required");
    };
    let name = match percent_encoding::percent_decode_str(raw).decode_utf8() {
        Ok(n) => n.into_owned(),
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid stream name"),
    };
    if crate::config::valid_name(&name).is_err() {
        return error(StatusCode::BAD_REQUEST, "invalid stream name");
    }
    if app.options.role == "lb" {
        return error(StatusCode::FORBIDDEN, "publication requires a source node");
    }
    let Some(expected) = snapshot(&app, &name) else {
        return error(
            if app.config.effective(&name).is_some() {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::NOT_FOUND
            },
            "stream does not accept publications",
        );
    };
    let qs = request.uri().query().unwrap_or("");
    if qs.len() > 16384 {
        return error(StatusCode::BAD_REQUEST, "publication query too large");
    }
    let mut password = None;
    let mut token = None;
    for (key, value) in url::form_urlencoded::parse(qs.as_bytes()) {
        let slot = match key.as_ref() {
            "password" => &mut password,
            "token" => &mut token,
            _ => continue,
        };
        if slot.is_some() || value.len() > 1024 {
            return error(
                StatusCode::BAD_REQUEST,
                "ambiguous or oversized publication credential",
            );
        }
        *slot = Some(value.into_owned());
    }
    if !expected
        .policy
        .accepts_password(password.as_deref().unwrap_or(""))
    {
        return error(StatusCode::FORBIDDEN, "publication denied");
    }
    for key in ["user-agent", "referer", "host"] {
        if request
            .headers()
            .get(key)
            .is_some_and(|v| v.len() > 4096 || v.to_str().is_err())
        {
            return error(
                StatusCode::BAD_REQUEST,
                "publication metadata too large or invalid",
            );
        }
    }
    if let Some(content) = header(request.headers(), "content-type") {
        if !["application/octet-stream", "video/mp2t"].contains(
            &content
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
                .as_str(),
        ) {
            return error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "MPEG-TS body required");
        }
    }
    let Ok(_permit) = app.publishers.try_acquire() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "publication limit reached");
    };
    let ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|v| v.0.ip().to_string())
        .unwrap_or_else(|| "127.0.0.1".into());
    let mut session = Session {
        metadata: json!({"name":name,"proto":"mpegts","ip":ip,"token":token.unwrap_or_default(),"qs":qs,"user_agent":header(request.headers(),"user-agent").unwrap_or(""),"referer":header(request.headers(),"referer").unwrap_or(""),"host":header(request.headers(),"host").unwrap_or(""),"session_id":uuid::Uuid::new_v4().to_string()}),
        started: Instant::now(),
        number: 0,
        bytes: 0,
    };
    let mut renew_at = match authorize_current(&mut session, &app, &name, &expected, None).await {
        Ok(d) => Instant::now() + d,
        Err(_) => return error(StatusCode::FORBIDDEN, "publication denied"),
    };
    if !current(&app, &name, &expected) {
        return error(StatusCode::FORBIDDEN, "publication policy changed");
    }
    let mut publication = match app
        .media
        .publish_guarded(&name, &expected.config, async {
            current(&app, &name, &expected)
        })
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return error(
                if e == "publisher already connected" {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                },
                "publication unavailable",
            );
        }
    };
    if !current(&app, &name, &expected) {
        return error(StatusCode::FORBIDDEN, "publication policy changed");
    }
    let Some(mut stdin) = publication.stdin.take() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "publication pipe unavailable",
        );
    };
    let worker = &publication.worker;
    let timeout = Duration::from_secs(
        expected.config["flussonix_input_timeout"]
            .as_u64()
            .unwrap_or(15)
            .clamp(1, 300),
    );
    let mut progress = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut body = request.into_body().into_data_stream();
    let mut packets = Packets::default();
    let mut chunk = Bytes::new();
    let mut output = Vec::new();
    let mut offset = 0;
    loop {
        if offset == output.len() && !chunk.is_empty() {
            let n = chunk.len().min(65536);
            output = match packets.push(&chunk[..n]) {
                Ok(v) => v,
                Err(_) => {
                    return error(StatusCode::BAD_REQUEST, "invalid MPEG-TS packet alignment");
                }
            };
            chunk = chunk.slice(n..);
            offset = 0;
        }
        tokio::select! {biased;
            _=worker.closed()=>return error(StatusCode::SERVICE_UNAVAILABLE,"publication worker stopped"),
            _=tick.tick()=>{if !current(&app,&name,&expected){return error(StatusCode::FORBIDDEN,"publication policy changed");}},
            _=tokio::time::sleep_until(renew_at),if expected.policy.url.is_some()=>{
                let decision=authorize_current(&mut session,&app,&name,&expected,Some(worker)).await;
                renew_at=match decision {Ok(d)=>Instant::now()+d,Err(code)=>return error(code,"publication renewal denied")};
                if !current(&app,&name,&expected){return error(StatusCode::FORBIDDEN,"publication policy changed");}
            },
            _=tokio::time::sleep_until(progress+timeout)=>return error(StatusCode::REQUEST_TIMEOUT,"publication stalled"),
            result=stdin.write(&output[offset..]),if offset<output.len()=>{
                match result {Ok(n) if n>0=>{offset+=n;progress=Instant::now();},_=>return error(StatusCode::SERVICE_UNAVAILABLE,"publication pipe stopped")}
            },
            result=body.next(),if chunk.is_empty()&&offset==output.len()=>{
                match result {
                    Some(Ok(data))=>{if !data.is_empty(){session.bytes=session.bytes.saturating_add(data.len() as u64);progress=Instant::now();chunk=data;}},
                    Some(Err(_))=>return error(StatusCode::BAD_REQUEST,"publication body interrupted"),
                    None=>break,
                }
            },
        }
    }
    if session.bytes == 0 || !packets.partial.is_empty() {
        return error(StatusCode::BAD_REQUEST, "incomplete MPEG-TS body");
    }
    drop(stdin);
    let _ = tokio::time::timeout(Duration::from_secs(3), worker.closed()).await;
    if !current(&app, &name, &expected) {
        return error(StatusCode::FORBIDDEN, "publication policy changed");
    }
    if worker.bytes.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "publication produced no media",
        );
    }
    StatusCode::NO_CONTENT.into_response()
}
