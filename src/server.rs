use crate::{
    auth::{Credentials, Role},
    cluster::{NodeLoad, select},
    config::{ConfigStore, KINDS, valid_name},
    media::{Engine, Worker},
    playback_auth::{AuthOutcome, Grant, PlaybackAuth, Policy, PolicySnapshot, ViewerRequest},
};
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Options {
    pub admin_user: String,
    pub admin_password: String,
    pub view_user: Option<String>,
    pub view_password: Option<String>,
    pub peer_key: String,
    pub ffmpeg: String,
    pub role: String,
    pub node_name: String,
    pub uplink_mbps: f64,
    pub uplink_interface: String,
    pub client_limit: u64,
    pub web_dir: PathBuf,
    pub drain: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            admin_user: "admin".into(),
            admin_password: String::new(),
            view_user: None,
            view_password: None,
            peer_key: String::new(),
            ffmpeg: "ffmpeg".into(),
            role: "standalone".into(),
            node_name: "local".into(),
            uplink_mbps: 1000.0,
            uplink_interface: "auto".into(),
            client_limit: 1000,
            web_dir: "web/dist".into(),
            drain: false,
        }
    }
}
struct Reservation {
    stream: String,
    expires: Instant,
}
#[derive(Clone)]
struct Mirror {
    when: Instant,
    config: Value,
    source: Value,
    available: bool,
    denied: bool,
    known: bool,
    serial: u64,
    switches: u64,
}
struct Resolved {
    config: Value,
    policy: PolicySnapshot,
    revision: u64,
}
pub struct App {
    http_delivery: std::sync::Mutex<Value>,
    pub config: ConfigStore,
    pub media: Engine,
    credentials: Credentials,
    pub options: Options,
    client: reqwest::Client,
    pub playback_auth: PlaybackAuth,
    reservations: Mutex<HashMap<String, Reservation>>,
    pub egress: Arc<AtomicU64>,
    pub rtsp_egress: Arc<AtomicU64>,
    pub rtsp_udp_egress: Arc<AtomicU64>,
    telemetry: crate::telemetry::Sampler,
    pub started: Instant,
    mirrors: Mutex<HashMap<String, Mirror>>,
    source_queries: tokio::sync::Semaphore,
    publishers: tokio::sync::Semaphore,
    source_lookups: Mutex<HashMap<String, origin_resolution::Ticket>>,
}
impl App {
    pub fn new(
        config: impl AsRef<Path>,
        media: impl AsRef<Path>,
        options: Options,
    ) -> Result<Arc<Self>, String> {
        if options.admin_password.is_empty() || options.peer_key.len() < 12 {
            return Err("admin password required; peer key must be at least 12 characters".into());
        }
        if !["standalone", "source", "cdn", "lb"].contains(&options.role.as_str()) {
            return Err("unknown node role".into());
        }
        if !(options.uplink_mbps > 0.0 && options.uplink_mbps.is_finite())
            || options.client_limit == 0
        {
            return Err("positive uplink and client limit required".into());
        }
        let credentials = Credentials::new(
            &options.admin_user,
            &options.admin_password,
            options
                .view_user
                .as_deref()
                .zip(options.view_password.as_deref()),
            &options.peer_key,
        );
        let app = Arc::new(Self {
            http_delivery: std::sync::Mutex::new(
                json!({"http":null,"https":null,"https_only":false}),
            ),
            config: ConfigStore::open(config)?,
            media: Engine::new(media, &options.ffmpeg),
            credentials,
            options: options.clone(),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| e.to_string())?,
            playback_auth: PlaybackAuth::new(options.client_limit as usize),
            reservations: Mutex::new(HashMap::new()),
            egress: Arc::new(AtomicU64::new(0)),
            rtsp_egress: Arc::new(AtomicU64::new(0)),
            rtsp_udp_egress: Arc::new(AtomicU64::new(0)),
            telemetry: crate::telemetry::Sampler::new(&options.uplink_interface)?,
            started: Instant::now(),
            mirrors: Mutex::new(HashMap::new()),
            source_queries: tokio::sync::Semaphore::new(64),
            publishers: tokio::sync::Semaphore::new(64),
            source_lookups: Mutex::new(HashMap::new()),
        });
        app.sample_metrics();
        Ok(app)
    }
    pub fn set_http_delivery(&self, http: Option<SocketAddr>, https: Option<SocketAddr>) {
        *self.http_delivery.lock().unwrap() = json!({"http":http.map(|a|a.to_string()),"https":https.map(|a|a.to_string()),"https_only":http.is_none() && https.is_some()});
    }
    async fn media_config(&self, name: &str) -> Option<(Value, u64)> {
        let mirrors = self.mirrors.lock().await;
        self.config.read(|root| {
            let config = crate::config::effective(root, name).or_else(|| {
                mirrors
                    .get(name)
                    .filter(|m| {
                        m.available
                            && root["sources"]
                                .as_array()
                                .is_some_and(|sources| sources.contains(&m.source))
                    })
                    .map(|m| m.config.clone())
            })?;
            Some((config, self.config.revision()))
        })
    }
    async fn recover_current(&self, name: &str, config: &Value, _revision: u64) {
        let signature = crate::media::media_signature(config);
        if !self.media_config(name).await.is_some_and(|(c, _)| {
            c["disabled"] != true && crate::media::media_signature(&c) == signature
        }) {
            return;
        }
        let check = async {
            self.media_config(name).await.is_some_and(|(c, _)| {
                c["disabled"] != true && crate::media::media_signature(&c) == signature
            })
        };
        if let Ok(worker) = self.media.ensure_guarded(name, config, false, check).await {
            // A save can race filesystem/child startup. Stop only the exact
            // stale attempt; a later request may already have replaced it.
            if !self.media_config(name).await.is_some_and(|(c, _)| {
                c["disabled"] != true && crate::media::media_signature(&c) == signature
            }) {
                self.media.stop_if_current(name, &worker).await;
            }
        }
    }
    pub async fn reconcile(&self) {
        // Retirement comes first: a background retry must not extend demand.
        for name in self.media.idle().await {
            if self
                .config
                .effective(&name)
                .is_none_or(|c| c["static"] == false)
            {
                self.media.stop(&name).await;
            }
        }
        self.refresh_sources().await;
        for (name, _) in self.media.workers().await {
            if let Some((config, revision)) = self.media_config(&name).await {
                if config["disabled"] == true {
                    self.media.stop(&name).await;
                } else {
                    self.recover_current(&name, &config, revision).await;
                }
            } else {
                self.media.stop(&name).await;
            }
        }
        let root = self.config.snapshot();
        if let Some(streams) = root["streams"].as_array() {
            for disk in streams {
                if let Some(name) = disk["name"].as_str() {
                    if let Some((config, revision)) = self.media_config(name).await {
                        if config["disabled"] != true
                            && config["static"] != false
                            && self.options.role != "lb"
                        {
                            self.recover_current(name, &config, revision).await;
                        }
                    }
                }
            }
        }
    }
    async fn invalidate_sessions(&self) {
        let mut mirrors = self.mirrors.lock().await;
        self.config.read(|root| {
            // Cache eviction is not source removal: retain mirrors whose discovery endpoint is unchanged.
            mirrors.retain(|_, m| {
                root["sources"]
                    .as_array()
                    .is_some_and(|sources| sources.contains(&m.source))
            });
            self.playback_auth.invalidate(|name| {
                let cfg = crate::config::effective(root, name).or_else(|| {
                    mirrors
                        .get(name)
                        .filter(|m| m.available)
                        .map(|m| m.config.clone())
                })?;
                if cfg["disabled"] == true {
                    return None;
                }
                Policy::from_config(&cfg, root).ok()
            });
        });
    }
    async fn active(&self) -> u64 {
        self.playback_auth.active()
    }
    async fn node(&self) -> Value {
        let cfg = self.config.snapshot();
        let mut streams = Vec::new();
        let names = cfg["streams"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["name"].as_str())
            .map(str::to_owned)
            .chain(
                self.mirrors
                    .lock()
                    .await
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .collect::<std::collections::BTreeSet<_>>();
        for name in names {
            streams.push(json!({"name":name,"ready":self.media.ready(&name).await,"stats":self.stream_stats(&name).await}));
        }
        let mut metrics = self.telemetry.snapshot(self.options.uplink_mbps);
        metrics["rtsp_udp_bytes_out"] = json!(self.rtsp_udp_egress.load(Ordering::Relaxed));
        let mut reservations = self.reservations.lock().await;
        reservations.retain(|_, v| v.expires > Instant::now());
        let reserved = reservations.len() as u64;
        drop(reservations);
        let node = json!({"name":self.options.node_name,"role":self.options.role,"uptime":self.started.elapsed().as_secs(),"streams":streams,"uplink_mbps":self.options.uplink_mbps,"active":self.active().await,"reserved":reserved,"limit":self.options.client_limit,"drain":self.options.drain,"http_delivery":self.http_delivery.lock().unwrap().clone()});
        metrics
            .as_object_mut()
            .unwrap()
            .extend(node.as_object().unwrap().clone());
        metrics
    }
    pub fn sample_metrics(&self) {
        self.telemetry.sample_media(
            self.egress.load(Ordering::Relaxed),
            self.rtsp_egress.load(Ordering::Relaxed),
        );
    }

    fn resolved(&self, name: &str, config: Value, revision: u64) -> Option<Resolved> {
        self.config.at_revision(revision, |root| {
            let policy = if config["disabled"] == true {
                None
            } else {
                Policy::from_config(&config, root).ok()
            };
            self.playback_auth
                .publish(name, policy)
                .map(|policy| Resolved {
                    config,
                    policy,
                    revision,
                })
        })?
    }
}
fn header<'a>(h: &'a HeaderMap, key: &str) -> Option<&'a str> {
    h.get(key).and_then(|v| v.to_str().ok())
}

fn error(code: StatusCode, message: &str) -> Response {
    (code, axum::Json(json!({"errors":[{"message":message}]}))).into_response()
}
fn json_response(value: Value) -> Response {
    axum::Json(value).into_response()
}
fn parse_query(q: Option<&str>) -> HashMap<String, String> {
    url::form_urlencoded::parse(q.unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}
pub fn router(app: Arc<App>) -> Router {
    Router::new().route("/health",axum::routing::get(||async{axum::Json(json!({"status":"ok","service":"FlussoniX","version":env!("CARGO_PKG_VERSION")}))}))
 .route("/streamer/api/v3/{*tail}",axum::routing::any(management))
 .route("/flussonix/api/v1/{*tail}",axum::routing::any(native))
 .nest_service("/admin",tower_http::services::ServeDir::new(&app.options.web_dir).append_index_html_on_directories(true))
 .fallback(media_request).with_state(app).layer(axum::extract::DefaultBodyLimit::max(2*1024*1024))
}
async fn management(State(app): State<Arc<App>>, request: Request) -> Response {
    let role = app
        .credentials
        .authorize(header(request.headers(), "authorization"));
    let Some(role) = role else {
        let mut r = error(StatusCode::UNAUTHORIZED, "management credentials required");
        r.headers_mut().insert(
            "www-authenticate",
            "Basic realm=\"FlussoniX\"".parse().unwrap(),
        );
        return r;
    };
    let method = request.method().clone();
    if method != axum::http::Method::GET && role != Role::Edit {
        return error(StatusCode::FORBIDDEN, "edit credentials required");
    }
    let tail = percent_encoding::percent_decode_str(
        request.uri().path().trim_start_matches("/streamer/api/v3/"),
    )
    .decode_utf8_lossy()
    .to_string();
    let query = parse_query(request.uri().query());
    let mut parts = tail.splitn(2, '/');
    let resource = parts.next().unwrap_or("");
    let name = parts.next();
    if resource == "config" {
        if name == Some("stats") && method == "GET" {
            return json_response(app.node().await);
        }
        if name.is_some() {
            return error(StatusCode::NOT_FOUND, "unknown config operation");
        }
        if method == "GET" {
            return json_response(app.config.snapshot());
        }
        let body = match read_json(request).await {
            Ok(b) => b,
            Err((code, message)) => return error(code, &message),
        };
        let result = if method == "POST" {
            app.config.validate(body)
        } else if method == "PUT" {
            app.config.replace(body)
        } else {
            return error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
        };
        return match result {
            Ok(v) => {
                if method == "PUT" {
                    app.invalidate_sessions().await;
                    app.reconcile().await;
                }
                json_response(v)
            }
            Err(e) => error(StatusCode::BAD_REQUEST, &e),
        };
    }
    let (kind, name) = if resource == "cluster" {
        let Some(n) = name else {
            return error(StatusCode::NOT_FOUND, "collection required");
        };
        let mut p = n.splitn(2, '/');
        (p.next().unwrap(), p.next())
    } else {
        (resource, name)
    };
    if kind == "sessions" {
        if name == Some("reauth") && method == "POST" {
            let Some(stream) = query.get("name") else {
                return error(StatusCode::BAD_REQUEST, "name required");
            };
            if app.resolve(stream).await.is_none() {
                return error(StatusCode::NOT_FOUND, "stream not found");
            }
            let count = app.playback_auth.force_reauth(stream);
            app.playback_auth.renew_due().await;
            return json_response(json!({"estimated_count":count}));
        }
        if let Some(id) = name {
            if method == "GET" {
                return match app.playback_auth.snapshot(id) {
                    Some(s) => json_response(s),
                    None => error(StatusCode::NOT_FOUND, "session not found"),
                };
            }
            if method == "DELETE" {
                return if app.playback_auth.revoke(id) {
                    StatusCode::NO_CONTENT.into_response()
                } else {
                    error(StatusCode::NOT_FOUND, "session not found")
                };
            }
        } else if method == "GET" {
            let mut sessions = app.playback_auth.snapshots();
            if let Some(name) = query.get("name") {
                sessions.retain(|s| s["name"] == name.as_str());
            }
            sessions.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
            return json_response(json!({"estimated_count":sessions.len(),"sessions":sessions}));
        }
        return error(
            StatusCode::METHOD_NOT_ALLOWED,
            "session operation not implemented",
        );
    }
    if !KINDS.contains(&kind) {
        return error(StatusCode::NOT_FOUND, "API operation not implemented");
    }
    if let Some(name) = name {
        if kind == "streams" && method == "POST" && name.ends_with("/stop") {
            app.media.stop(name.trim_end_matches("/stop")).await;
            return json_response(json!({"status":"stopped"}));
        }
        if method == "GET" {
            let root = app.config.snapshot();
            let item = if kind == "streams" {
                app.config.effective(name)
            } else {
                root[kind]
                    .as_array()
                    .and_then(|v| {
                        v.iter()
                            .find(|v| v["name"] == name || v["hostname"] == name)
                    })
                    .cloned()
            };
            return match item {
                Some(mut v) => {
                    if kind == "streams" {
                        v["stats"] = app.stream_stats(name).await;
                    }
                    json_response(v)
                }
                None => error(StatusCode::NOT_FOUND, "not found"),
            };
        }
        if method == "PUT" {
            let body = match read_json(request).await {
                Ok(b) => b,
                Err((code, message)) => return error(code, &message),
            };
            return match app.config.put(kind, name, body) {
                Ok(mut v) => {
                    app.invalidate_sessions().await;
                    if kind == "sources" {
                        app.invalidate_sessions().await;
                        app.reconcile().await;
                    }
                    if matches!(kind, "streams" | "templates") {
                        app.invalidate_sessions().await;
                        app.reconcile().await;
                        if kind == "streams" {
                            v = app.config.effective(name).unwrap_or(v);
                            v["stats"] = app.stream_stats(name).await;
                        }
                    }
                    json_response(v)
                }
                Err(e) => error(StatusCode::BAD_REQUEST, &e),
            };
        }
        if method == "DELETE" {
            return match app.config.delete(kind, name) {
                Ok(true) => {
                    app.invalidate_sessions().await;
                    if kind == "streams" {
                        app.media.stop(name).await;
                    }
                    if kind == "sources" {
                        app.invalidate_sessions().await;
                        app.reconcile().await;
                    }
                    StatusCode::NO_CONTENT.into_response()
                }
                Ok(false) => error(StatusCode::NOT_FOUND, "not found"),
                Err(e) => error(StatusCode::BAD_REQUEST, &e),
            };
        }
        return error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if method != "GET" {
        return error(StatusCode::METHOD_NOT_ALLOWED, "collection is read only");
    }
    let root = app.config.snapshot();
    let mut items = root[kind].as_array().cloned().unwrap_or_default();
    if kind == "streams" {
        for item in &mut items {
            if let Some(name) = item["name"].as_str().map(str::to_owned) {
                *item = app.config.effective(&name).unwrap_or(item.clone());
                item["stats"] = app.stream_stats(&name).await;
            }
        }
    }
    if let Some(q) = query.get("q") {
        items.retain(|v| v.to_string().to_lowercase().contains(&q.to_lowercase()));
    }
    items.sort_by_key(|v| {
        v["name"]
            .as_str()
            .or(v["hostname"].as_str())
            .unwrap_or("")
            .to_owned()
    });
    if query.get("sort").is_some_and(|s| s == "-name") {
        items.reverse()
    }
    let total = items.len();
    let offset = query
        .get("cursor")
        .and_then(|s| STANDARD.decode(s).ok())
        .and_then(|b| String::from_utf8(b).ok())
        .map(|s| parse_query(Some(&s)))
        .and_then(|p| p.get("$position_gt").and_then(|v| v.parse::<usize>().ok()))
        .map(|p| p + 1)
        .unwrap_or(0);
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let page = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let mut result = json!({"estimated_count":total,"timing":{},"next":if offset+page.len()<total{Some(STANDARD.encode(format!("%24position_gt={}",offset+page.len()-1)))}else{None::<String>},"prev":null});
    result[kind] = json!(page);
    json_response(result)
}
async fn read_json(request: Request) -> Result<Value, (StatusCode, String)> {
    let bytes = axum::body::to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, "body exceeds limit".into()))?;
    serde_json::from_slice(&bytes).map_err(|_| (StatusCode::BAD_REQUEST, "invalid JSON".into()))
}
async fn native(State(app): State<Arc<App>>, request: Request) -> Response {
    let tail = percent_encoding::percent_decode_str(
        request
            .uri()
            .path()
            .trim_start_matches("/flussonix/api/v1/"),
    )
    .decode_utf8_lossy()
    .to_string();
    let peer = app
        .credentials
        .peer(header(request.headers(), "x-flussonix-peer"));
    let role = app
        .credentials
        .authorize(header(request.headers(), "authorization"));
    if !peer && role.is_none() {
        return error(StatusCode::UNAUTHORIZED, "credentials required");
    }
    if tail == "node" && request.method() == "GET" {
        return json_response(app.node().await);
    }
    if tail == "capabilities" && request.method() == "GET" {
        return json_response(
            json!({"api":"Flussonic v3 subset","input":["hls","hlss","tshttp","tshttps","rtsp","rtsps (verified TLS, interleaved TCP)","srt","publish:// (HTTP MPEG-TS receive)","m4s (H.264/AAC frames and packed GOPs)","m4f (single-chunk H.264/AAC)","testsrc"],"output":["hls","mpegts","fmp4-hls","https (opt-in TLS delivery and MPEG-TS publication)","rtsp (TCP / opt-in unicast UDP playback, H.264/AAC-LC)","rtsps (opt-in TLS TCP playback, H.264/AAC-LC)","m4s (H.264/AAC frames and packed GOPs)","m4f (single-chunk H.264/AAC)"],"unimplemented":["direct rtp","srtp","rtsp publication / push","rtsp Basic / Digest viewer auth","dvr","push"],"transcoding":{"cpu":"libx264 / AAC","gpu":"h264_nvenc, requires supported NVIDIA hardware and runtime"},"cluster":"native HLS/M4S/M4F source discovery and reserved HTTP redirects"}),
        );
    }
    if let Some(name) = tail.strip_prefix("stream/") {
        if request.method() == "GET" && peer {
            return match app.config.effective(name) {
                Some(mut c) => {
                    if let Some(value) = c.get("on_play").cloned() {
                        let policy = match Policy::from_config(&c, &app.config.snapshot()) {
                            Ok(p) => p,
                            Err(_) => {
                                return error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "source authentication policy unavailable",
                                );
                            }
                        };
                        if value.is_object() {
                            c["on_play"]["url"] = json!(policy.url);
                        } else {
                            c["on_play"] = json!(policy.url);
                        }
                    }
                    // Discovery conveys playback policy and identity, never publisher
                    // credentials, source inputs or raw saved configuration.
                    let fields = [
                        "name",
                        "title",
                        "comment",
                        "position",
                        "disabled",
                        "flussonix_content_id",
                        "flussonix_input_timeout",
                        "on_play",
                        "flussonix_token_sha256",
                    ];
                    let discovery = fields
                        .into_iter()
                        .filter_map(|key| c.get(key).cloned().map(|value| (key.to_owned(), value)))
                        .collect();
                    json_response(Value::Object(discovery))
                }
                None => error(StatusCode::NOT_FOUND, "stream not found"),
            };
        }
    }
    if tail == "admit" && request.method() == "POST" && peer {
        let b = match read_json(request).await {
            Ok(b) => b,
            Err((code, message)) => return error(code, &message),
        };
        let Some(name) = b["name"].as_str() else {
            return error(StatusCode::BAD_REQUEST, "name required");
        };
        if valid_name(name).is_err() {
            return error(StatusCode::BAD_REQUEST, "invalid name");
        }
        let n = app.node().await;
        let expected =
            b["bitrate_mbps"].as_f64().unwrap_or(2.0).clamp(0.1, 100.0) / app.options.uplink_mbps;
        let mut reservations = app.reservations.lock().await;
        reservations.retain(|_, v| v.expires > Instant::now());
        if app.options.drain
            || n["active"].as_u64().unwrap_or(0) + reservations.len() as u64
                >= app.options.client_limit
            || n["uplink"].as_f64().unwrap_or(1.0) + expected * (reservations.len() + 1) as f64
                >= 0.9
            || n["cpu"].as_f64().unwrap_or(1.0) >= 0.9
            || n["ram"].as_f64().unwrap_or(1.0) >= 0.95
        {
            return error(StatusCode::SERVICE_UNAVAILABLE, "node has no capacity");
        }
        let ticket = uuid::Uuid::new_v4().to_string();
        reservations.insert(
            ticket.clone(),
            Reservation {
                stream: name.into(),
                expires: Instant::now() + Duration::from_secs(5),
            },
        );
        return json_response(json!({"ticket":ticket,"expires_in":5}));
    }
    error(StatusCode::NOT_FOUND, "operation not implemented")
}
async fn balance(
    app: &Arc<App>,
    name: &str,
    path: &str,
    query: &HashMap<String, String>,
    secure: bool,
) -> Response {
    let root = app.config.snapshot();
    let peers = root["peers"].as_array().cloned().unwrap_or_default();
    let mut nodes = Vec::new();
    let mut valid = HashMap::new();
    let calls = peers.into_iter().map(|p| {
        let app = app.clone();
        let name = name.to_owned();
        async move {
            if secure
                && !p["public_payload_url"]
                    .as_str()
                    .and_then(|u| url::Url::parse(u).ok())
                    .is_some_and(|u| {
                        u.scheme() == "https"
                            && u.host_str().is_some()
                            && u.username().is_empty()
                            && u.password().is_none()
                    })
            {
                return None;
            }
            let api = p["api_url"].as_str()?;
            let key = p["cluster_key"].as_str().unwrap_or(&app.options.peer_key);
            let r = app
                .client
                .get(format!(
                    "{}/flussonix/api/v1/node",
                    api.trim_end_matches('/')
                ))
                .header("X-Flussonix-Peer", key)
                .send()
                .await
                .ok()?;
            if !r.status().is_success() {
                return None;
            }
            let n = r.json::<Value>().await.ok()?;
            let ready = n["streams"]
                .as_array()
                .is_some_and(|a| a.iter().any(|s| s["name"] == name && s["ready"] == true));
            let id = p["hostname"].as_str()?.to_owned();
            let limit = n["limit"].as_u64()?;
            let load = NodeLoad {
                name: id.clone(),
                uplink: n["uplink"].as_f64()?
                    + n["reserved"].as_u64().unwrap_or(0) as f64 * 2.0
                        / n["uplink_mbps"].as_f64()?.max(1.0),
                cpu: n["cpu"].as_f64()?,
                ram: n["ram"].as_f64()?,
                ready,
                drain: n["drain"].as_bool().unwrap_or(true) || p["drain"] == true,
                age_ms: n["age_ms"].as_u64().unwrap_or(u64::MAX),
                active: n["active"].as_u64()? + n["reserved"].as_u64().unwrap_or(0),
                limit,
            };
            Some((load, p))
        }
    });
    for (load, p) in futures_util::future::join_all(calls)
        .await
        .into_iter()
        .flatten()
    {
        valid.insert(load.name.clone(), p);
        nodes.push(load)
    }
    while let Some(id) = select(&nodes, 0.01) {
        let p = &valid[&id];
        let api = p["api_url"].as_str().unwrap_or("");
        let key = p["cluster_key"].as_str().unwrap_or(&app.options.peer_key);
        let response = app
            .client
            .post(format!(
                "{}/flussonix/api/v1/admit",
                api.trim_end_matches('/')
            ))
            .header("X-Flussonix-Peer", key)
            .json(&json!({"name":name,"bitrate_mbps":2.0}))
            .send()
            .await;
        if let Ok(r) = response {
            if r.status().is_success() {
                if let Ok(body) = r.json::<Value>().await {
                    if let (Some(public), Some(ticket)) =
                        (p["public_payload_url"].as_str(), body["ticket"].as_str())
                    {
                        let mut q = query.clone();
                        q.insert("flussonix_ticket".into(), ticket.into());
                        let query = url::form_urlencoded::Serializer::new(String::new())
                            .extend_pairs(q.iter())
                            .finish();
                        let url = format!(
                            "{}/{}?{}",
                            public.trim_end_matches('/'),
                            path.trim_start_matches('/'),
                            query
                        );
                        return (StatusCode::FOUND, [("location", url)]).into_response();
                    }
                }
            }
        }
        nodes.retain(|n| n.name != id);
    }
    error(StatusCode::SERVICE_UNAVAILABLE, "no available CDN node")
}
async fn media_request(State(app): State<Arc<App>>, request: Request) -> Response {
    let mut response = if request.method() == "OPTIONS" {
        StatusCode::NO_CONTENT.into_response()
    } else if request.method() == "POST" {
        publication::receive(app, request).await
    } else {
        serve_media_request(app, request).await
    };
    let headers = response.headers_mut();
    headers.insert(
        "access-control-allow-origin",
        axum::http::HeaderValue::from_static("*"),
    );
    headers.insert(
        "access-control-allow-methods",
        axum::http::HeaderValue::from_static("GET, POST, OPTIONS"),
    );
    headers.insert(
        "access-control-allow-headers",
        axum::http::HeaderValue::from_static("Authorization, Range"),
    );
    response
}
async fn serve_media_request(app: Arc<App>, request: Request) -> Response {
    let secure = request
        .extensions()
        .get::<crate::http_tls::SecureHttp>()
        .is_some();
    if request.method() != "GET" {
        return error(StatusCode::METHOD_NOT_ALLOWED, "playback requires GET");
    }
    let raw_path = request.uri().path();
    let path = percent_encoding::percent_decode_str(raw_path.trim_start_matches('/'))
        .decode_utf8_lossy()
        .to_string();
    let special = if path.ends_with(".m4f") {
        let p = path.rsplitn(7, '/').collect::<Vec<_>>();
        if p.len() == 7 {
            Some((
                p[6].to_owned(),
                p[..6].iter().rev().copied().collect::<Vec<_>>().join("/"),
            ))
        } else {
            None
        }
    } else {
        None
    };
    let (name, file) = if let Some((n, f)) = &special {
        (n.as_str(), f.as_str())
    } else if let Some(n) = path.strip_suffix("/m4f") {
        (n, "m4f")
    } else if let Some(n) = path.strip_suffix("/m4s") {
        (n, "m4s")
    } else if let Some(n) = path.strip_suffix("/mpegts") {
        (n, "mpegts")
    } else if let Some((n, _f)) = path.split_once("/fmp4/") {
        (n, &path[n.len() + 1..])
    } else if let Some((n, f)) = path.rsplit_once('/') {
        (n, f)
    } else {
        return error(StatusCode::NOT_FOUND, "media path required");
    };
    if valid_name(name).is_err() {
        return error(StatusCode::BAD_REQUEST, "invalid stream name");
    }
    let Some(resolved) = app.resolve(name).await else {
        return error(StatusCode::NOT_FOUND, "stream not found");
    };
    let cfg = resolved.config;
    if cfg["disabled"] == true {
        return error(StatusCode::NOT_FOUND, "stream disabled");
    }
    let query = parse_query(request.uri().query());
    let ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|v| v.0.ip().to_string())
        .unwrap_or_else(|| "127.0.0.1".into());
    let grant = if app
        .credentials
        .peer(header(request.headers(), "x-flussonix-peer"))
    {
        Grant::peer()
    } else {
        let qs = request
            .uri()
            .query()
            .unwrap_or("")
            .split('&')
            .filter(|pair| {
                url::form_urlencoded::parse(pair.as_bytes())
                    .next()
                    .is_none_or(|(key, _)| key != "flussonix_ticket")
            })
            .collect::<Vec<_>>()
            .join("&");
        let viewer = ViewerRequest {
            name: name.into(),
            proto: if file == "mpegts" {
                "mpegts"
            } else if file == "m4s" {
                "m4s"
            } else if file == "m4f" || file.ends_with(".m4f") {
                "m4f"
            } else {
                "hls"
            }
            .into(),
            ip,
            token: query.get("token").cloned().unwrap_or_default(),
            qs,
            user_agent: header(request.headers(), "user-agent").unwrap_or("").into(),
            referer: header(request.headers(), "referer").unwrap_or("").into(),
            host: header(request.headers(), "host").unwrap_or("").into(),
        };
        match app.playback_auth.authorize(resolved.policy, viewer).await {
            AuthOutcome::Allowed(g) => g,
            AuthOutcome::Denied => return error(StatusCode::FORBIDDEN, "playback denied"),
            AuthOutcome::Redirect(url) => {
                if secure && !url::Url::parse(&url).is_ok_and(|u| u.scheme() == "https") {
                    return error(
                        StatusCode::FORBIDDEN,
                        "secure playback redirect cannot downgrade",
                    );
                }
                return (StatusCode::FOUND, [("location", url)]).into_response();
            }
        }
    };
    if app.config.revision() != resolved.revision {
        // Re-resolve after a concurrent save. Unrelated metadata changes retain the grant,
        // while policy/source changes publish a new authority revision and cancel it.
        let Some(_) = app.resolve(name).await else {
            return error(StatusCode::NOT_FOUND, "stream unavailable");
        };
    }
    // Authorization can await a callback while an equivalent-origin switch
    // changes media without changing the root revision or viewer policy.
    let Some((cfg, _)) = app.media_config(name).await else {
        return error(StatusCode::NOT_FOUND, "stream unavailable");
    };
    if grant.is_cancelled() {
        return error(StatusCode::FORBIDDEN, "playback policy changed");
    }
    if app.options.role == "lb" {
        return balance(&app, name, raw_path, &query, secure).await;
    }
    if let Some(ticket) = query.get("flussonix_ticket") {
        let mut reservations = app.reservations.lock().await;
        match reservations.remove(ticket) {
            Some(r) if r.stream == name && r.expires > Instant::now() => {
                let clean = url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(
                        query
                            .iter()
                            .filter(|(k, _)| k.as_str() != "flussonix_ticket"),
                    )
                    .finish();
                let location = if clean.is_empty() {
                    raw_path.to_owned()
                } else {
                    format!("{raw_path}?{clean}")
                };
                return (StatusCode::FOUND, [("location", location)]).into_response();
            }
            _ => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "admission ticket invalid or expired",
                );
            }
        }
    }
    let signature = crate::media::media_signature(&cfg);
    let check = async {
        !grant.is_cancelled()
            && app.media_config(name).await.is_some_and(|(c, _)| {
                c["disabled"] != true && crate::media::media_signature(&c) == signature
            })
    };
    let worker = match app.media.ensure_guarded(name, &cfg, true, check).await {
        Ok(w) => w,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "stream input unavailable"),
    };
    if grant.is_cancelled()
        || !app.media_config(name).await.is_some_and(|(c, _)| {
            c["disabled"] != true && crate::media::media_signature(&c) == worker.signature()
        })
    {
        app.media.stop_if_current(name, &worker).await;
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "stream changed during startup",
        );
    }
    if file == "m4s" || file == "m4f" {
        let deadline = Instant::now() + Duration::from_secs(8);
        while !grant.is_cancelled()
            && !worker.wire.has_info()
            && worker.alive.load(Ordering::Relaxed)
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !worker.wire.has_info() {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "wire outputs require H.264/AAC media",
            );
        }
        let Some((initial, rx)) = (if file == "m4f" {
            Some(worker.wire.signal_subscribe())
        } else {
            worker.m4s_subscribe()
        }) else {
            return error(
                StatusCode::NOT_IMPLEMENTED,
                "M4S relay requires an M4S input",
            );
        };
        worker.viewers.fetch_add(1, Ordering::Relaxed);
        let guard = ViewerGuard(worker.clone(), grant);
        let egress = app.egress.clone();
        let live = futures_util::stream::unfold(
            (std::collections::VecDeque::from(initial), rx, guard, egress),
            |(mut boot, mut rx, guard, egress)| async move {
                if guard.1.is_cancelled() || guard.0.is_closed() {
                    return None;
                }
                let bytes = if let Some(bytes) = boot.pop_front() {
                    bytes
                } else {
                    tokio::select! {biased; _=guard.1.cancelled()=>return None,_=guard.0.closed()=>return None,result=rx.recv()=> match result { Ok(bytes)=>bytes, Err(_)=>return None } }
                };
                guard.1.add_bytes(bytes.len());
                egress.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                Some((
                    Ok::<Bytes, std::io::Error>(bytes),
                    (boot, rx, guard, egress),
                ))
            },
        );
        return (
            [
                (
                    "content-type",
                    if file == "m4f" {
                        "application/x-video-m4f-signal"
                    } else {
                        "application/x-video-m4s"
                    },
                ),
                ("cache-control", "no-store"),
            ],
            Body::from_stream(live),
        )
            .into_response();
    }
    if file == "mpegts" {
        let rx = worker.subscribe();
        worker.viewers.fetch_add(1, Ordering::Relaxed);
        let guard = ViewerGuard(worker.clone(), grant);
        let egress = app.egress.clone();
        let stream = futures_util::stream::unfold(
            (rx, guard, egress),
            |(mut rx, guard, egress)| async move {
                match tokio::select! {biased; _=guard.1.cancelled()=>Err(tokio::sync::broadcast::error::RecvError::Closed),_=guard.0.closed()=>Err(tokio::sync::broadcast::error::RecvError::Closed),result=rx.recv()=>result}
                {
                    Ok(bytes) => {
                        guard.1.add_bytes(bytes.len());
                        egress.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        Some((Ok::<Bytes, std::io::Error>(bytes), (rx, guard, egress)))
                    }
                    Err(_) => None,
                }
            },
        );
        return (
            [
                ("Content-Type".to_owned(), "video/mp2t".to_owned()),
                ("Cache-Control".into(), "no-store".into()),
            ],
            Body::from_stream(stream),
        )
            .into_response();
    }
    let deadline = Instant::now() + Duration::from_secs(8);
    let bytes = loop {
        if grant.is_cancelled() {
            return error(StatusCode::FORBIDDEN, "playback revoked");
        }
        match app.media.read(name, file).await {
            Ok(b) => break b,
            Err(_)
                if file.ends_with(".m3u8")
                    && Instant::now() < deadline
                    && worker.alive.load(Ordering::Relaxed) =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "media not ready"),
        }
    };
    let bytes = if file.ends_with(".m3u8") {
        let mut q = query.clone();
        q.remove("flussonix_ticket");
        let suffix = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(q.iter())
            .finish();
        Bytes::from(rewrite_playlist(&String::from_utf8_lossy(&bytes), &suffix))
    } else {
        bytes
    };
    if grant.is_cancelled() {
        return error(StatusCode::FORBIDDEN, "playback revoked");
    }
    grant.add_bytes(bytes.len());
    app.egress.fetch_add(bytes.len() as u64, Ordering::Relaxed);
    let content_type = if file.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if file.ends_with(".ts") {
        "video/mp2t"
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
struct ViewerGuard(Arc<Worker>, #[allow(dead_code)] Grant);
impl Drop for ViewerGuard {
    fn drop(&mut self) {
        self.0.viewers.fetch_sub(1, Ordering::Relaxed);
        self.0.touch();
    }
}
pub fn rewrite_playlist(text: &str, query: &str) -> String {
    if query.is_empty() {
        return text.to_owned();
    }
    let append = |u: &str| format!("{}{}{}", u, if u.contains('?') { "&" } else { "?" }, query);
    text.lines()
        .map(|line| {
            if !line.starts_with('#') && !line.is_empty() {
                append(line)
            } else if let Some((before, after)) = line.split_once("URI=\"") {
                if let Some((uri, rest)) = after.split_once('"') {
                    format!("{before}URI=\"{}\"{rest}", append(uri))
                } else {
                    line.into()
                }
            } else {
                line.into()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

#[cfg(test)]
mod continuous_session_tests {
    use super::*;
    use tower::ServiceExt;
    #[tokio::test]
    async fn live_http_body_keeps_viewer_counted_after_inactivity_window() {
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("c.json"),
            d.path().join("media"),
            Options {
                admin_password: "test-admin".into(),
                peer_key: "test-peer-secret".into(),
                client_limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let response = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri("/owned/mpegts?token=first")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        app.playback_auth.age_activity(31);
        assert_eq!(
            app.node().await["active"],
            1,
            "an open live response is still an active viewer"
        );
        let second = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri("/owned/index.m3u8?token=second")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            second.status(),
            403,
            "live viewers retain their client slots"
        );
        app.invalidate_sessions().await;
        assert_eq!(
            app.node().await["active"],
            1,
            "config changes still account for open bodies"
        );
        drop(response);
        assert_eq!(app.node().await["active"], 0);
        app.media.stop_all().await;
    }
}

#[path = "origin_resolution.rs"]
mod origin_resolution;

pub(crate) mod rtsp_access;

mod publication;
