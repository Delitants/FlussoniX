use crate::{
    auth::{Credentials, Role},
    cluster::{NodeLoad, TieRotation},
    config::{ConfigStore, KINDS, nested_validation_path, parse_text, valid_name},
    iptv::store::IptvStore,
    media::{Engine, Worker},
    playback_auth::{
        AuthOutcome, Grant, PlaybackAuth, Policy, PolicySnapshot, RecoveryDemand, ViewerRequest,
    },
};
use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
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
    pub admin_public_origin: Option<url::Url>,
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
    pub iptv_public_origin: Option<url::Url>,
    pub iptv_data: Option<PathBuf>,
    pub drain: bool,
    pub fxlink_features: crate::fxlink::Features,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            admin_user: "admin".into(),
            admin_password: String::new(),
            admin_public_origin: None,
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
            iptv_public_origin: None,
            iptv_data: None,
            drain: false,
            fxlink_features: crate::fxlink::Features::default(),
        }
    }
}
struct Reservation {
    http_slot: Option<Arc<crate::playback_auth::CapacityReservation>>,
    srt: Option<srt_tickets::Binding>,
    vendor: Option<Arc<crate::source_directory::modern_coordinator::ReservedCandidate>>,
    kind: rtsp_balancer::Kind,
    bitrate_mbps: f64,
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
    // True until any request consumes this background-only route.
    activation_only: bool,
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
    srt_playback: std::sync::Mutex<Value>,
    fxlink: std::sync::Mutex<(Value, Arc<AtomicU64>)>,
    rtsp_publication: std::sync::Mutex<Value>,
    pub config: Arc<ConfigStore>,
    pub iptv: Arc<IptvStore>,
    node_name: String,
    modern_sources:
        Option<Arc<crate::source_directory::modern_coordinator::ModernSourceCoordinator>>,
    vendor_playback: Option<crate::vendor_playback::Runtime>,
    pub media: Engine,
    credentials: Credentials,
    pub options: Options,
    client: reqwest::Client,
    cluster_clients: std::sync::Mutex<(u64, HashMap<String, reqwest::Client>)>,
    pub playback_auth: Arc<PlaybackAuth>,
    reservations: Mutex<HashMap<String, Reservation>>,
    http_handoffs: http_handoff::Registry,
    rtsp_routes: rtsp_balancer::Registry,
    http_routes: http_balancer::Registry,
    routing_rotation: TieRotation,
    pub egress: Arc<AtomicU64>,
    pub rtsp_egress: Arc<AtomicU64>,
    pub rtsp_udp_egress: Arc<AtomicU64>,
    pub srt_egress: Arc<AtomicU64>,
    pub(crate) srt_connections: AtomicU64,
    telemetry: crate::telemetry::Sampler,
    pub started: Instant,
    mirrors: fxlink_access::Mirrors,
    source_queries: tokio::sync::Semaphore,
    source_activation: Mutex<source_activation::State>,
    source_activation_refresh: Mutex<()>,
    pub(crate) publishers: Arc<tokio::sync::Semaphore>,
    source_lookups: Mutex<HashMap<String, origin_resolution::Ticket>>,
}
impl App {
    /// Caller owns/joins this one-shot driver; stopping it retires shared inputs.
    pub async fn run_vendor_playback(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), String> {
        match &self.vendor_playback {
            Some(runtime) => runtime.run(self, cancel).await,
            None => {
                cancel.cancelled().await;
                Ok(())
            }
        }
    }
    /// Optional modern coordinator; this accessor does not enable vendor routing.
    pub fn modern_source_coordinator(
        &self,
    ) -> Result<
        &crate::source_directory::modern_coordinator::ModernSourceCoordinator,
        crate::source_directory::modern_coordinator::CoordinationFailure,
    > {
        let coordinator = self
            .modern_sources
            .as_ref()
            .ok_or(crate::source_directory::modern_coordinator::CoordinationFailure::Invalid)?;
        coordinator.require_config(&self.config)?;
        Ok(coordinator)
    }
    pub fn new(
        config: impl AsRef<Path>,
        media: impl AsRef<Path>,
        options: Options,
    ) -> Result<Arc<Self>, String> {
        if options.admin_password.is_empty() || options.peer_key.len() < 12 {
            return Err("admin password required; peer key must be at least 12 characters".into());
        }
        if options.admin_public_origin.as_ref().is_some_and(|origin| {
            origin.scheme() != "https"
                || origin.host_str().is_none()
                || origin.path() != "/"
                || origin.query().is_some()
                || origin.fragment().is_some()
                || !origin.username().is_empty()
                || origin.password().is_some()
        }) {
            return Err(
                "admin public origin must be an HTTPS origin without path or credentials".into(),
            );
        }
        if !["standalone", "source", "cdn", "lb"].contains(&options.role.as_str()) {
            return Err("unknown node role".into());
        }
        if !(options.uplink_mbps > 0.0 && options.uplink_mbps.is_finite())
            || options.client_limit == 0
        {
            return Err("positive uplink and client limit required".into());
        }
        if options.iptv_public_origin.as_ref().is_some_and(|origin| {
            !matches!(origin.scheme(), "http" | "https")
                || origin.host_str().is_none()
                || !origin.username().is_empty()
                || origin.password().is_some()
                || origin.path() != "/"
                || origin.query().is_some()
                || origin.fragment().is_some()
        }) {
            return Err("IPTV public origin must be a plain HTTP(S) origin".into());
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
        let config_path = config.as_ref();
        let iptv_path = options.iptv_data.clone().unwrap_or_else(|| {
            config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("iptv.json")
        });
        let iptv = IptvStore::open(iptv_path)?;
        let config = Arc::new(ConfigStore::open(config_path)?);
        let modern_sources =
            crate::source_directory::modern_coordinator::ModernSourceCoordinator::new(
                config.clone(),
                &options.node_name,
            )
            .ok()
            .map(Arc::new);
        let vendor_playback = modern_sources
            .as_ref()
            .map(|owner| crate::vendor_playback::Runtime::new(owner.clone()));
        let playback_auth = Arc::new(PlaybackAuth::with_iptv(
            options.client_limit as usize,
            iptv.clone(),
        ));
        let app = Arc::new(Self {
            http_delivery: std::sync::Mutex::new(
                json!({"http":null,"https":null,"https_only":false}),
            ),
            rtsp_publication: std::sync::Mutex::new(
                json!({"enabled":false,"rtsp":null,"rtsps":null,"transport":"interleaved TCP","udp_enabled":false,"codecs":["H264","HEVC","AAC-LC","MP2","MP3"]}),
            ),
            srt_playback: std::sync::Mutex::new(
                json!({"enabled":false,"listen":null,"encrypted":false}),
            ),
            fxlink: std::sync::Mutex::new((
                json!({"enabled":false,"listen":null,"encrypted":true,"latency_ms":1000,"min_latency_ms":500,"max_latency_ms":1500,"fec":{"data_shards":20,"parity_shards":4,"profiles":["20+2","20+4","20+6","1+1"]}}),
                Arc::new(AtomicU64::new(0)),
            )),
            config,
            iptv: iptv.clone(),
            node_name: options.node_name.clone(),
            modern_sources,
            vendor_playback,
            media: Engine::new_with_fxlink(media, &options.ffmpeg, options.fxlink_features),
            credentials,
            options: options.clone(),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| e.to_string())?,
            cluster_clients: std::sync::Mutex::new((0, HashMap::new())),
            playback_auth,
            reservations: Mutex::new(HashMap::new()),
            http_handoffs: http_handoff::Registry::default(),
            rtsp_routes: rtsp_balancer::Registry::default(),
            http_routes: http_balancer::Registry::default(),
            routing_rotation: TieRotation::default(),
            egress: Arc::new(AtomicU64::new(0)),
            rtsp_egress: Arc::new(AtomicU64::new(0)),
            rtsp_udp_egress: Arc::new(AtomicU64::new(0)),
            srt_egress: Arc::new(AtomicU64::new(0)),
            srt_connections: AtomicU64::new(0),
            telemetry: crate::telemetry::Sampler::new(&options.uplink_interface)?,
            started: Instant::now(),
            mirrors: fxlink_access::Mirrors::default(),
            source_queries: tokio::sync::Semaphore::new(64),
            source_activation: Mutex::new(source_activation::State::default()),
            source_activation_refresh: Mutex::new(()),
            publishers: Arc::new(tokio::sync::Semaphore::new(64)),
            source_lookups: Mutex::new(HashMap::new()),
        });
        let weak = Arc::downgrade(&app.playback_auth);
        app.iptv.set_on_change(Box::new(move || {
            if let Some(auth) = weak.upgrade() {
                auth.invalidate_iptv();
            }
        }));
        app.sample_metrics();
        Ok(app)
    }
    pub fn set_http_delivery(&self, http: Option<SocketAddr>, https: Option<SocketAddr>) {
        *self.http_delivery.lock().unwrap() = json!({"http":http.map(|a|a.to_string()),"https":https.map(|a|a.to_string()),"https_only":http.is_none() && https.is_some()});
    }
    pub fn set_rtsp_publication(&self, rtsp: Option<SocketAddr>, rtsps: Option<SocketAddr>) {
        *self.rtsp_publication.lock().unwrap() = json!({"enabled":rtsp.is_some()||rtsps.is_some(),"rtsp":rtsp.map(|a|a.to_string()),"rtsps":rtsps.map(|a|a.to_string()),"transport":"interleaved TCP","udp_enabled":false,"codecs":["H264","HEVC","AAC-LC","MP2","MP3"]});
    }
    pub fn set_rtsp_publication_udp(&self, enabled: bool) {
        let mut profile = self.rtsp_publication.lock().unwrap();
        profile["udp_enabled"] = json!(enabled && profile["rtsp"].is_string());
        if profile["udp_enabled"] == true {
            profile["transport"] = json!("interleaved TCP or unicast UDP");
        } else {
            profile["transport"] = json!("interleaved TCP");
        }
    }

    pub fn set_srt_playback(&self, listener: Option<&crate::srt_playback::Listener>) {
        *self.srt_playback.lock().unwrap() = match listener {
            Some(l) => {
                json!({"enabled":true,"listener_id":uuid::Uuid::new_v4().to_string(),"listen":l.address().to_string(),"encrypted":l.settings().encrypted(),"latency_ms":l.settings().latency_millis(),"client_limit":l.settings().client_limit()})
            }
            None => json!({"enabled":false,"listen":null,"encrypted":false}),
        };
    }
    fn cluster_client(&self, node: &Value) -> Result<reqwest::Client, String> {
        let endpoint = node["api_url"]
            .as_str()
            .and_then(|value| url::Url::parse(value).ok())
            .ok_or("invalid cluster API URL")?;
        crate::peer_transport::require(
            &endpoint,
            node["flussonix_allow_insecure_peer_http"] == true,
        )
        .map_err(str::to_owned)?;
        let Some(ca) = node.get("flussonix_tls_ca") else {
            return Ok(self.client.clone());
        };
        if !node["api_url"]
            .as_str()
            .and_then(|u| url::Url::parse(u).ok())
            .is_some_and(|u| u.scheme() == "https")
        {
            return Err("cluster CA requires HTTPS".into());
        }
        let path = ca.as_str().ok_or("invalid cluster CA")?;
        // Share pools between nodes with the same trust profile, never default
        // peer credentials. Saves invalidate cached trust. Source lookups
        // retain their existing configuration revision checks.
        let mut clients = self.cluster_clients.lock().unwrap();
        let revision = self.config.revision();
        if clients.0 != revision {
            clients.0 = revision;
            clients.1.clear();
        }
        if let Some(client) = clients.1.get(path) {
            return Ok(client.clone());
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .https_only(true)
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .use_preconfigured_tls((*crate::tls_input::client(Some(Path::new(path)))?).clone())
            .build()
            .map_err(|_| "cannot build cluster TLS client")?;
        if clients.1.len() >= 64 {
            clients.1.clear();
        }
        clients.1.insert(path.into(), client.clone());
        Ok(client)
    }
    async fn media_config(&self, name: &str) -> Option<(Value, u64)> {
        let mirrors = self.mirrors.lock().await;
        self.config.read(|root| {
            let config = crate::config::effective(root, name).or_else(|| {
                if crate::vendor_playback::reserved(root, name) {
                    return None;
                }
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
    async fn recovery_current(
        &self,
        name: &str,
        signature: &str,
        demand: Option<&RecoveryDemand>,
    ) -> bool {
        self.media_config(name).await.is_some_and(|(config, _)| {
            config["disabled"] != true && crate::media::media_signature(&config) == signature
        }) && demand.is_none_or(|demand| {
            self.config.effective(name).is_none()
                && self.playback_auth.recovery_allowed(name, demand)
        })
    }
    async fn recover_current(&self, name: &str, config: &Value, revision: u64) {
        self.recover_with_demand(name, config, revision, None).await;
    }
    async fn recover_with_demand(
        &self,
        name: &str,
        config: &Value,
        _revision: u64,
        demand: Option<&RecoveryDemand>,
    ) {
        let _activity = self.media.idle_memory.activity().await;
        let signature = crate::media::media_signature(config);
        if !self.recovery_current(name, &signature, demand).await {
            return;
        }
        let check = self.recovery_current(name, &signature, demand);
        let result = if let Some(demand) = demand {
            self.media
                .recover_demand_guarded(name, config, demand.activity, check)
                .await
        } else {
            self.media.ensure_guarded(name, config, false, check).await
        };
        if let Ok(worker) = result {
            self.finish_recovery(name, &signature, demand, &worker)
                .await;
        }
    }
    async fn finish_recovery(
        &self,
        name: &str,
        signature: &str,
        demand: Option<&RecoveryDemand>,
        worker: &Arc<Worker>,
    ) {
        // Retire only the exact stale attempt after asynchronous startup.
        if !self.recovery_current(name, signature, demand).await {
            self.media.stop_if_current(name, worker).await;
        }
    }
    pub async fn reconcile(&self) {
        self.refresh_source_activation().await;
        // Retirement comes first: a background retry must not extend demand.
        for name in self.media.idle().await {
            let static_demand = match self.config.effective(&name) {
                Some(config) => config["static"] != false,
                None => self.source_is_warm(&name).await,
            };
            if !static_demand {
                self.media.stop(&name).await;
            }
        }
        self.refresh_sources().await;
        for (name, _) in self.media.workers().await {
            if self.media.maintain_shared(&name).await {
                continue;
            }
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
        if self.options.role != "lb" {
            let workers = self
                .media
                .workers()
                .await
                .into_iter()
                .map(|(name, _)| name)
                .collect::<std::collections::HashSet<_>>();
            for (name, demand) in self.playback_auth.recovery_demand() {
                // This path restores a discovered pull, never a configured on-demand stream.
                if !workers.contains(&name)
                    && self.config.effective(&name).is_none()
                    && let Some((config, revision)) = self.media_config(&name).await
                {
                    self.recover_with_demand(&name, &config, revision, Some(&demand))
                        .await;
                }
            }
        }
        let root = self.config.snapshot();
        if let Some(streams) = root["streams"].as_array() {
            for disk in streams {
                if let Some(name) = disk["name"].as_str() {
                    if let Some((config, revision)) = self.media_config(name).await {
                        if config["disabled"] != true
                            && (config["static"] != false
                                || crate::push::enabled(&config)
                                || crate::direct_rtp::config::enabled(&config))
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
            mirrors.retain(|name, m| {
                let current = root["sources"]
                    .as_array()
                    .is_some_and(|sources| sources.contains(&m.source));
                // Release background ownership before eviction loses its record.
                // Request snapshots claim the mirror first; entries also prevent
                // forgetting, so ordinary policy invalidation remains authoritative.
                if !current && m.activation_only {
                    self.playback_auth.forget_unobserved_activation(name);
                }
                current
            });
            self.playback_auth.invalidate(|name| {
                let cfg = crate::config::effective(root, name).or_else(|| {
                    if crate::vendor_playback::reserved(root, name) {
                        return None;
                    }
                    mirrors
                        .get(name)
                        .filter(|m| m.known && !m.denied)
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
        let mut names = cfg["streams"]
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
        names.extend(self.media.workers().await.into_iter().map(|(name, _)| name));
        for name in names {
            streams.push(json!({"name":name,"ready":self.media.ready(&name).await,"stats":self.stream_stats(&name).await}));
        }
        let mut node = self.load_node().await;
        node["version"] = json!(env!("CARGO_PKG_VERSION"));
        node["streams"] = json!(streams);
        node
    }
    async fn load_node(&self) -> Value {
        // Worker setup can hold this mutex across network awaits. Obtain the
        // optional stream observation before capturing resource/session load.
        let stream_bitrates = self.media.output_bitrates().await;
        let (mut metrics, window) = self
            .telemetry
            .snapshot_with_window(self.options.uplink_mbps);
        metrics["http_push_bytes_out"] = json!(self.media.http_push_egress.load(Ordering::Relaxed));
        metrics["rtsp_push_bytes_out"] = json!(self.media.rtsp_push_egress.load(Ordering::Relaxed));
        metrics["rtsp_udp_bytes_out"] = json!(self.rtsp_udp_egress.load(Ordering::Relaxed));
        metrics["direct_rtp_bytes_out"] = json!(self.media.direct_egress.load(Ordering::Relaxed));
        metrics["srt_bytes_out"] = json!(self.srt_egress.load(Ordering::Relaxed));
        metrics["fxlink"] = self.fxlink.lock().unwrap().0.clone();
        let mut srt = self.srt_playback.lock().unwrap().clone();
        srt["clients"] = json!(self.srt_connections.load(Ordering::Relaxed));
        metrics["srt_playback"] = srt;
        let mut reservations = self.reservations.lock().await;
        reservations.retain(|_, v| v.expires > Instant::now());
        metrics["srt_playback"]["reserved"] = json!(
            reservations
                .values()
                .filter(|r| r
                    .srt
                    .as_ref()
                    .is_some_and(|b| metrics["srt_playback"]["listener_id"] == b.listener_id))
                .count()
        );
        let reserved = reservations.len() as u64;
        let reserved_mbps: f64 = reservations.values().map(|r| r.bitrate_mbps).sum::<f64>()
            + self
                .http_handoffs
                .unmeasured(window, metrics["http_egress_mbps"].as_f64());
        drop(reservations);
        let rtsp_publication = self.rtsp_publication.lock().unwrap().clone();
        let node = json!({"rtsp_publication":rtsp_publication,"name":self.node_name,"role":self.options.role,"uptime":self.started.elapsed().as_secs(),"reserved_mbps":reserved_mbps,"uplink_mbps":self.options.uplink_mbps,"active":self.active().await,"reserved":reserved,"limit":self.options.client_limit,"drain":self.options.drain,"http_delivery":self.http_delivery.lock().unwrap().clone(),"iptv_public_origin":self.options.iptv_public_origin.as_ref().map(url::Url::as_str)});
        metrics
            .as_object_mut()
            .unwrap()
            .extend(node.as_object().unwrap().clone());
        metrics["stream_bitrates"] = stream_bitrates;
        metrics["source_activation"] = self.source_activation_status().await;
        if let Some(runtime) = &self.vendor_playback {
            metrics["vendor_source_activation"] = runtime.activation_status(self);
        }
        metrics
    }
    pub fn sample_metrics(&self) {
        let at = Instant::now();
        self.http_handoffs
            .sample(self.telemetry.last_sample_at(), at);
        self.telemetry.sample_with_fxlink_at(
            at,
            self.egress
                .load(Ordering::Relaxed)
                .saturating_add(self.media.http_push_egress.load(Ordering::Relaxed)),
            self.rtsp_egress
                .load(Ordering::Relaxed)
                .saturating_add(self.media.rtsp_push_egress.load(Ordering::Relaxed)),
            self.srt_egress.load(Ordering::Relaxed),
            self.media.direct_egress.load(Ordering::Relaxed),
            self.fxlink.lock().unwrap().1.load(Ordering::Relaxed),
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
pub(crate) fn header<'a>(h: &'a HeaderMap, key: &str) -> Option<&'a str> {
    h.get(key).and_then(|v| v.to_str().ok())
}

pub(crate) fn error(code: StatusCode, message: &str) -> Response {
    (code, axum::Json(json!({"errors":[{"message":message}]}))).into_response()
}
fn json_response(value: Value) -> Response {
    axum::Json(value).into_response()
}
fn management_read_response(role: Role, mut value: Value) -> Response {
    if role == Role::View {
        redact_view_value(&mut value);
    }
    json_response(value)
}
fn browser_management_write_allowed(request: &Request, public_origin: Option<&url::Url>) -> bool {
    if let Some(site) = request.headers().get("sec-fetch-site") {
        if !matches!(site.to_str(), Ok("same-origin" | "none")) {
            return false;
        }
    }
    // Browsers can submit simple form POSTs without Origin or Fetch Metadata.
    if request.method() == axum::http::Method::POST
        && request.uri().path() != "/streamer/api/v3/config"
        && request.headers().get("origin").is_none()
        && request.headers().get("sec-fetch-site").is_none()
        && request
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                matches!(
                    value
                        .split(';')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_ascii_lowercase()
                        .as_str(),
                    "application/x-www-form-urlencoded" | "multipart/form-data" | "text/plain"
                )
            })
    {
        return false;
    }
    let Some(origin) = request.headers().get("origin") else {
        return true;
    };
    let Some(authority) = request
        .uri()
        .authority()
        .map(|value| value.as_str())
        .or_else(|| {
            request
                .headers()
                .get("host")
                .and_then(|value| value.to_str().ok())
        })
    else {
        return false;
    };
    let scheme = if request
        .extensions()
        .get::<crate::http_tls::SecureHttp>()
        .is_some()
    {
        "https"
    } else {
        "http"
    };
    let target = url::Url::parse(&format!("{scheme}://{authority}/"));
    let supplied = origin
        .to_str()
        .ok()
        .and_then(|value| url::Url::parse(value).ok());
    match (target, supplied) {
        (Ok(target), Some(supplied)) => {
            supplied.path() == "/"
                && supplied.query().is_none()
                && supplied.fragment().is_none()
                && supplied.username().is_empty()
                && supplied.password().is_none()
                && (supplied.origin() == target.origin()
                    || public_origin.is_some_and(|public| {
                        public.host_str() == target.host_str()
                            && public.port() == target.port()
                            && supplied.origin() == public.origin()
                    }))
        }
        _ => false,
    }
}
fn redact_view_value(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (key, field) in fields {
                // Configuration appears both on disk and inherited in effective streams.
                // Keep the shape for status readers, but never return credentials,
                // callback destinations, key-file paths, or full endpoint URLs.
                if matches!(
                    key.as_str(),
                    "password"
                        | "passphrase"
                        | "cluster_key"
                        | "flussonix_peer_key"
                        | "flussonix_fxlink_token"
                        | "flussonix_token_sha256"
                        | "token"
                        | "streamid"
                        | "key_file"
                        | "sdp_file"
                        | "flussonix_tls_ca"
                        | "flussonix_media_tls_ca"
                        | "on_play"
                        | "on_publish"
                ) {
                    *field = Value::String("[redacted]".into());
                } else if matches!(
                    key.as_str(),
                    "url" | "endpoint" | "upstream_source" | "source"
                ) || key.ends_with("_url")
                {
                    *field = match field.as_str() {
                        Some("testsrc://" | "publish://") => field.clone(),
                        Some(url) => match url.split_once("://") {
                            Some((scheme, _))
                                if scheme.len() <= 32
                                    && !scheme.is_empty()
                                    && scheme.bytes().all(|b| {
                                        b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.')
                                    }) =>
                            {
                                Value::String(format!("{scheme}://[redacted]"))
                            }
                            _ => Value::String("[redacted]".into()),
                        },
                        None => Value::String("[redacted]".into()),
                    };
                } else {
                    redact_view_value(field);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_view_value(item);
            }
        }
        _ => {}
    }
}
pub(crate) fn parse_query(q: Option<&str>) -> HashMap<String, String> {
    url::form_urlencoded::parse(q.unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}
pub fn router(app: Arc<App>) -> Router {
    Router::new().route("/health",axum::routing::get(||async{axum::Json(json!({"status":"ok","service":"FlussoniX","version":env!("CARGO_PKG_VERSION")}))}))
 .route("/streamer/api/v3/{*tail}",axum::routing::any(management))
 .route("/tv/playlists/{subscriber}",axum::routing::get(iptv_playlist_http))
 .route("/flussonix/api/v1/srt-lookup",axum::routing::any(srt_lookup::lookup))
 .route("/flussonix/api/v1/{*tail}",axum::routing::any(native))
 .nest_service("/admin",tower_http::services::ServeDir::new(&app.options.web_dir).append_index_html_on_directories(true))
 .fallback(media_request).with_state(app).layer(axum::extract::DefaultBodyLimit::max(2*1024*1024))
}
async fn iptv_playlist_http(
    State(app): State<Arc<App>>,
    axum::extract::Path(subscriber): axum::extract::Path<String>,
    request: Request,
) -> Response {
    iptv_playlist::get(app, request, &subscriber)
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
    if method != axum::http::Method::GET
        && !browser_management_write_allowed(&request, app.options.admin_public_origin.as_ref())
    {
        return error(StatusCode::FORBIDDEN, "cross-origin management request");
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
    if resource == "iptv" {
        return iptv_management::dispatch(app, request, &tail, role).await;
    }
    if resource == "config" {
        if name == Some("stats") && method == "GET" {
            let mut node = app.node().await;
            node["management_role"] = json!(if role == Role::View { "view" } else { "edit" });
            return management_read_response(role, node);
        }
        if name.is_some() {
            return error(StatusCode::NOT_FOUND, "unknown config operation");
        }
        if method == "GET" {
            return management_read_response(role, app.config.snapshot());
        }
        if method == "POST" {
            return validate_config_request(&app.config, request).await;
        }
        let body = match read_json(request).await {
            Ok(b) => b,
            Err((code, message)) => return error(code, &message),
        };
        if method != "PUT" {
            return error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
        }
        let result = app.config.replace(body);
        return match result {
            Ok(v) => {
                app.invalidate_sessions().await;
                app.reconcile().await;
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
            if app.resolve(stream).await.is_none()
                && !app
                    .vendor_playback
                    .as_ref()
                    .is_some_and(|runtime| runtime.publish_current_policy(&app, stream))
            {
                return error(StatusCode::NOT_FOUND, "stream not found");
            }
            let count = app.playback_auth.force_reauth(stream);
            app.playback_auth.renew_due().await;
            return json_response(json!({"estimated_count":count}));
        }
        if let Some(id) = name {
            if method == "GET" {
                return match app.playback_auth.snapshot(id) {
                    Some(s) => management_read_response(role, s),
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
            return management_read_response(
                role,
                json!({"estimated_count":sessions.len(),"sessions":sessions}),
            );
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
            let name = name.trim_end_matches("/stop");
            app.playback_auth.stop_playback(name);
            app.media.stop(name).await;
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
                            .find(|v| crate::config::item_key(kind, v) == Some(name))
                    })
                    .cloned()
            };
            return match item {
                Some(mut v) => {
                    if kind == "streams" {
                        v["stats"] = app.stream_stats(name).await;
                    }
                    management_read_response(role, v)
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
    if role == Role::View {
        for item in &mut items {
            redact_view_value(item);
        }
    }
    if matches!(kind, "streams" | "templates") {
        let filters = match crate::api_filter::Filters::parse(&query, kind == "streams") {
            Ok(filters) => filters,
            Err(message) => return error(StatusCode::BAD_REQUEST, &message),
        };
        items.retain(|row| filters.matches(row));
    }
    if let Some(q) = query.get("q") {
        items.retain(|v| v.to_string().to_lowercase().contains(&q.to_lowercase()));
    }
    let total = items.len();
    let (mut page, next, prev) = if matches!(kind, "streams" | "templates") {
        let page = match crate::api_cursor::paginate(items, kind, &query) {
            Ok(page) => page,
            Err(message) => return error(StatusCode::BAD_REQUEST, message),
        };
        (page.items, page.next, page.prev)
    } else {
        items.sort_by_key(|v| crate::config::item_key(kind, v).unwrap_or("").to_owned());
        if query.get("sort").is_some_and(|s| s == "-name") {
            items.reverse()
        }
        let page = match crate::api_cursor::paginate_positions(items, &query) {
            Ok(page) => page,
            Err(message) => return error(StatusCode::BAD_REQUEST, message),
        };
        (page.items, page.next, page.prev)
    };
    // The reference's explicit name scan counts only the requested page plus
    // one look-ahead row. Default position pages retain their full count.
    let estimated_count = if kind == "streams" && query.get("sort").is_some_and(|s| s == "name") {
        let limit = query
            .get("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(100)
            .clamp(1, 1000);
        total.min(limit.saturating_add(1))
    } else {
        total
    };
    let mut result = json!({"estimated_count":estimated_count,"timing":{}});
    if let Some(next) = next {
        result["next"] = json!(next);
    }
    if let Some(prev) = prev {
        result["prev"] = json!(prev);
    }
    if matches!(kind, "streams" | "templates" | "peers" | "sources")
        && let Some(select) = query.get("select")
    {
        let selection = crate::api_select::Selection::new(select);
        for row in &mut page {
            *row = selection.project(row, kind == "streams");
        }
    }
    result[kind] = json!(page);
    management_read_response(role, result)
}
async fn read_json(request: Request) -> Result<Value, (StatusCode, String)> {
    let bytes = axum::body::to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .map_err(|_| (StatusCode::PAYLOAD_TOO_LARGE, "body exceeds limit".into()))?;
    serde_json::from_slice(&bytes).map_err(|_| (StatusCode::BAD_REQUEST, "invalid JSON".into()))
}
async fn validate_config_request(config: &ConfigStore, request: Request) -> Response {
    let text_plain = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/plain"));
    let bytes = match axum::body::to_bytes(request.into_body(), 2 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "body exceeds limit"),
    };
    let patch: Value = if text_plain {
        let body = match std::str::from_utf8(&bytes) {
            Ok(body) => body,
            Err(error) => {
                let prefix = std::str::from_utf8(&bytes[..error.valid_up_to()])
                    .expect("UTF-8 error prefix is valid");
                let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
                let col = prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1;
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({
                        "error":"invalid_config", "config":{}, "detail":"text configuration must be UTF-8",
                        "line":line, "col":col, "path":[]
                    })),
                )
                    .into_response();
            }
        };
        match parse_text(body) {
            Ok(patch) => patch,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({
                        "error":e.error, "config":e.config, "detail":e.detail,
                        "line":e.line, "col":e.col, "path":[]
                    })),
                )
                    .into_response();
            }
        }
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(patch) => patch,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({
                        "error":"invalid_json", "config":{}, "detail":e.to_string(),
                        "line":e.line(), "col":e.column().max(1), "path":[]
                    })),
                )
                    .into_response();
            }
        }
    };
    match config.validate(patch.clone()) {
        Ok(validated) => json_response(validated),
        Err(detail) => {
            let unknown = detail
                .strip_prefix("server option ")
                .and_then(|rest| {
                    rest.strip_suffix(
                        " is not implemented; listener/auth/limits are startup options",
                    )
                })
                .filter(|key| patch.get(*key).is_some());
            let wrong_collection = detail
                .strip_suffix(" must be an array")
                .filter(|kind| KINDS.contains(kind) && patch.get(*kind).is_some());
            let nested = (!text_plain)
                .then(|| nested_validation_path(&patch, &detail))
                .flatten();
            let (code, path) = if let Some((code, path)) = nested {
                (code, path)
            } else if let Some(key) = unknown {
                ("unknown_key".into(), json!(["server_config", key]))
            } else if let Some(kind) = wrong_collection {
                ("invalid_config".into(), json!(["server_config", kind]))
            } else {
                ("invalid_config".into(), json!([]))
            };
            let recoverable = if patch.is_object() { patch } else { json!({}) };
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({
                    "error":code, "config":recoverable, "detail":detail, "path":path
                })),
            )
                .into_response()
        }
    }
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
    if let Some(name) = tail.strip_prefix("rtp-sdp/") {
        if request.method() != "GET" {
            return error(StatusCode::METHOD_NOT_ALLOWED, "GET required");
        }
        let Some(cfg) = app.config.effective(name) else {
            return error(StatusCode::NOT_FOUND, "stream not found");
        };
        if cfg["disabled"] == true {
            return error(StatusCode::CONFLICT, "stream disabled");
        }
        let mut index = None;
        for (k, v) in url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes()) {
            if k != "destination" || index.is_some() {
                return error(
                    StatusCode::BAD_REQUEST,
                    "Choose one RTP destination index from 0 to 3",
                );
            }
            index = v.parse::<usize>().ok().filter(|i| *i < 4);
            if index.is_none() {
                return error(
                    StatusCode::BAD_REQUEST,
                    "Choose one RTP destination index from 0 to 3",
                );
            }
        }
        let index = index.unwrap_or(0);
        if !cfg["flussonix_rtp_outputs"].get(index).is_some_and(|row| {
            row["disabled"] != true && row["flussonix_rtp"]["profile"] == "elementary"
        }) {
            return error(
                StatusCode::CONFLICT,
                "Enabled elementary RTP destination required",
            );
        }
        return match app.media.rtp_sdp(name, index, &cfg).await {
            Ok(text) => (
                [
                    ("Content-Type", "application/sdp"),
                    ("Cache-Control", "no-store"),
                ],
                text,
            )
                .into_response(),
            Err(e) => error(StatusCode::CONFLICT, &e),
        };
    }
    if tail == "rtsp-routing" && request.method() == "GET" && peer {
        return json_response(app.rtsp_routing_node().await);
    }
    if tail == "source-streams" {
        if request.method() != "GET" {
            return error(StatusCode::METHOD_NOT_ALLOWED, "GET required");
        }
        let root = app.config.snapshot();
        let configured = root["streams"].as_array().unwrap();
        if configured.len() > 10000 {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "native source inventory exceeds 10000 streams",
            );
        }
        let templates = root["templates"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| Some((t["name"].as_str()?, t)))
            .collect::<HashMap<_, _>>();
        let mut streams = configured.iter().filter_map(|row| {
            let name = row["name"].as_str()?;
            let template = row["template"].as_str().and_then(|name| templates.get(name));
            let field = |key, default| row.get(key).or_else(|| template.and_then(|t| t.get(key))).and_then(Value::as_bool).unwrap_or(default);
            Some(json!({"name":name,"static":field("static",true),"disabled":field("disabled",false)}))
        }).collect::<Vec<_>>();
        streams.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        return json_response(json!({"streams":streams}));
    }
    if tail == "node" && request.method() == "GET" {
        let mut node = app.node().await;
        node["idle_memory_cleanup"] = app.media.idle_memory.snapshot();
        node["management_role"] = json!(if role == Some(Role::View) {
            "view"
        } else {
            "edit"
        });
        return management_read_response(role.unwrap_or(Role::Edit), node);
    }
    if tail == "capabilities" && request.method() == "GET" {
        let (gpu_profiles, vaapi_profiles) =
            tokio::join!(app.media.gpu_capabilities(), app.media.vaapi_capabilities());
        return json_response(
            json!({"fxlink":app.fxlink.lock().unwrap().0.clone(),"rtsp_publication":app.rtsp_publication.lock().unwrap().clone(),"api":"Flussonic v3 subset","input":["fxlink (verified QUIC TLS, original native records, FEC)","hls","hlss","tshttp","tshttps","rtsp","rtsp-udp (unicast RTP/RTCP)","rtsp2 (RTSP/1.0 camera input, AAC audio default)","rtsps (verified TLS, interleaved TCP)","srt","rtp (MP2T or elementary H.264/HEVC/AAC/MP2/MP3 with static SDP, unicast and IPv4 multicast)","srtp (MP2T or elementary static SAVP / AES_CM_128_HMAC_SHA1_80, authenticated SRTCP)","publish:// (HTTP MPEG-TS; RTSP TCP / opt-in unicast UDP; RTSPS TLS TCP receive; H264/HEVC/AAC-LC/MP2/MP3)","m4s (H.264/AAC frames and packed GOPs)","m4f (single-chunk H.264/AAC)","testsrc"],"output":["rtp (MP2T or elementary H.264/HEVC/AAC/MP2/MP3 with static SDP, unicast and IPv4 multicast)","srtp (MP2T or elementary static SAVP / AES_CM_128_HMAC_SHA1_80, authenticated SRTCP)","srt (caller push / global listener playback, optional enforced encryption)","hls","mpegts","fmp4-hls","https (opt-in TLS delivery and MPEG-TS publication)","http / https / tshttp / tshttps (native continuous MPEG-TS POST push, optional Basic; verified HTTPS TLS)","rtsp (TCP or unicast UDP publish push; TCP / opt-in unicast UDP playback; H264/HEVC/AAC-LC/MP2/MP3)","rtsps (verified TLS TCP publish push; opt-in TLS TCP playback; H264/HEVC/AAC-LC/MP2/MP3)","m4s (H.264/AAC frames and packed GOPs)","m4f (single-chunk H.264/AAC)"],"unimplemented":["SDP negotiation","DTLS-SRTP / automatic key negotiation","rtsp Basic / Digest viewer auth","dvr","push protocols beyond SRT/RTSP/RTSPS/HTTP MPEG-TS","srt publication policy / per-stream playback listeners"],"transcoding":{"cpu":"H.264 / HEVC; independent AAC / MPEG Layer II / MP3 / copy audio","gpu":"NVIDIA / VAAPI H.264 / HEVC; profile readiness check, no software fallback","gpu_profiles":gpu_profiles,"vaapi_profiles":vaapi_profiles},"direct_srtp":{"available":crate::direct_rtp::crypto::availability(),"profile":"AES_CM_128_HMAC_SHA1_80","library":"independent system libsrtp2"},"cluster":"native HLS/M4S/M4F source discovery and reserved HTTP/RTSP/RTSPS redirects"}),
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
                        if let Some(url) = policy.url {
                            if value.is_object() {
                                c["on_play"]["url"] = json!(url);
                            } else {
                                c["on_play"] = json!(url);
                            }
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
                        "flussonix_subtitle_tracks",
                        "flussonix_hls_captions",
                        "flussonix_hls_subtitles",
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
        let hint = match b.get("bitrate_mbps") {
            None => crate::cluster::FALLBACK_MBPS,
            Some(value) => match value
                .as_f64()
                .filter(|v| v.is_finite() && *v > 0.0 && *v <= crate::cluster::MAX_HINT_MBPS)
            {
                Some(value) => value,
                None => {
                    return error(
                        StatusCode::BAD_REQUEST,
                        "bitrate_mbps must be a number greater than zero and at most 1000000",
                    );
                }
            },
        };
        let kind = match rtsp_balancer::kind(&b) {
            Ok(kind) => kind,
            Err(message) => return error(StatusCode::BAD_REQUEST, &message),
        };
        let mut vendor = None;
        let mut srt = None;
        if let rtsp_balancer::Kind::Srt { encrypted, .. } = &kind {
            let listener = app.srt_playback.lock().unwrap().clone();
            if listener["enabled"] != true
                || listener["encrypted"] != *encrypted
                || !matches!(app.options.role.as_str(), "cdn" | "standalone")
            {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "SRT delivery not enabled with requested encryption",
                );
            }
            srt = Some(srt_tickets::Binding {
                listener_id: listener["listener_id"].as_str().unwrap_or("").into(),
                revision: app.config.revision(),
                signature: None,
            });
        }
        if matches!(
            &kind,
            rtsp_balancer::Kind::Rtsp { .. } | rtsp_balancer::Kind::Srt { .. }
        ) {
            if let rtsp_balancer::Kind::Rtsp { secure, .. } = &kind {
                let enabled = app.rtsp_publication.lock().unwrap()
                    [if *secure { "rtsps" } else { "rtsp" }]
                .is_string();
                if !enabled || !matches!(app.options.role.as_str(), "cdn" | "standalone") {
                    return error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "RTSP delivery not enabled on CDN",
                    );
                }
            }
            match app
                .config
                .read(|root| crate::vendor_playback::select(root, name, app.config.revision()))
            {
                crate::vendor_playback::Selection::Native => {
                    let Some(resolved) = app
                        .resolve(name)
                        .await
                        .filter(|r| r.config["disabled"] != true)
                    else {
                        return error(StatusCode::NOT_FOUND, "stream route unavailable");
                    };
                    if let Some(binding) = &mut srt {
                        binding.revision = resolved.revision;
                        binding.signature = Some(crate::media::media_signature(&resolved.config));
                    }
                }
                crate::vendor_playback::Selection::Vendor { key, revision } => {
                    if let Some(binding) = &mut srt {
                        binding.revision = revision;
                    }
                    vendor = match &app.vendor_playback {
                        Some(runtime) => {
                            match runtime.rtsp_reserve(&app, name, &key, revision).await {
                                Ok(reserved) => Some(Arc::new(reserved)),
                                Err(code) => {
                                    return error(
                                        StatusCode::from_u16(code).unwrap(),
                                        "vendor route unavailable",
                                    );
                                }
                            }
                        }
                        None => {
                            return error(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "vendor owner unavailable",
                            );
                        }
                    };
                }
                crate::vendor_playback::Selection::Excluded => {
                    return error(StatusCode::NOT_FOUND, "stream excluded");
                }
                crate::vendor_playback::Selection::Ambiguous => {
                    return error(StatusCode::SERVICE_UNAVAILABLE, "ambiguous vendor route");
                }
            }
        }
        // Never acquire the worker mutex under the admission ledger. Any wait
        // for shared output must finish before the final capacity decision.
        let observations = json!({"stream_bitrates":app.media.output_bitrates().await});
        let observed_at = Instant::now();
        let mut reservations = app.reservations.lock().await;
        reservations.retain(|_, v| v.expires > Instant::now());
        let bitrate_mbps = hint
            .max(
                crate::cluster::observed_bitrate(
                    &observations,
                    name,
                    observed_at
                        .elapsed()
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                )
                .unwrap_or(crate::cluster::FALLBACK_MBPS),
            )
            .max(crate::cluster::FALLBACK_MBPS);
        if !bitrate_mbps.is_finite() || bitrate_mbps > crate::cluster::MAX_HINT_MBPS {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "stream cost exceeds admission bounds",
            );
        }
        let expected = bitrate_mbps / app.options.uplink_mbps;
        let (n, window) = app.telemetry.snapshot_with_window(app.options.uplink_mbps);
        let reserved_mbps: f64 = reservations.values().map(|r| r.bitrate_mbps).sum::<f64>()
            + app
                .http_handoffs
                .unmeasured(window, n["http_egress_mbps"].as_f64());
        let active = app.playback_auth.active();
        let srt_full = srt.as_ref().is_some_and(|binding| {
            let listener = app.srt_playback.lock().unwrap();
            let limit = listener["client_limit"].as_u64().unwrap_or(0);
            let pending = reservations
                .values()
                .filter(|r| {
                    r.srt
                        .as_ref()
                        .is_some_and(|b| b.listener_id == binding.listener_id)
                })
                .count() as u64;
            app.srt_connections
                .load(Ordering::Relaxed)
                .saturating_add(pending)
                >= limit
        });
        if srt_full
            || app.options.drain
            || reservations.len() >= 20000
            || active + reservations.len() as u64 >= app.options.client_limit
            || n["uplink"].as_f64().unwrap_or(1.0)
                + reserved_mbps / app.options.uplink_mbps
                + expected
                >= 0.9
            || n["cpu"].as_f64().unwrap_or(1.0) >= 0.9
            || n["ram"].as_f64().unwrap_or(1.0) >= 0.95
        {
            return error(StatusCode::SERVICE_UNAVAILABLE, "node has no capacity");
        }
        let native_http = matches!(kind, rtsp_balancer::Kind::Http)
            && app.config.read(|root| {
                matches!(
                    crate::vendor_playback::select(root, name, app.config.revision()),
                    crate::vendor_playback::Selection::Native
                )
            });
        let http_slot = if native_http {
            let other = reservations
                .values()
                .filter(|r| r.http_slot.is_none())
                .count();
            let Some(slot) = app.playback_auth.reserve_capacity(other) else {
                return error(StatusCode::SERVICE_UNAVAILABLE, "node has no capacity");
            };
            Some(slot)
        } else {
            None
        };
        let ticket = uuid::Uuid::new_v4().to_string();
        let route = vendor.clone();
        let binding = srt.clone();
        let commit = || {
            // Keep the actual listener generation stable through reservation commit.
            let listener = app.srt_playback.lock().unwrap();
            if let Some(b) = &srt
                && (listener["enabled"] != true
                    || listener["listener_id"] != b.listener_id
                    || !matches!(&kind,rtsp_balancer::Kind::Srt{encrypted,..} if listener["encrypted"]==*encrypted))
            {
                return false;
            }

            reservations.insert(
                ticket.clone(),
                Reservation {
                    http_slot,
                    srt,
                    vendor,
                    kind,
                    bitrate_mbps,
                    stream: name.into(),
                    expires: Instant::now() + Duration::from_secs(5),
                },
            );
            true
        };
        if let Some(route) = &route {
            let owner = app
                .modern_sources
                .as_ref()
                .expect("vendor reservation owner");
            if owner.with_reserved_current(route, commit).ok() != Some(true) {
                reservations.remove(&ticket); // post-commit lease expiry rollback
                return error(StatusCode::SERVICE_UNAVAILABLE, "vendor authority changed");
            }
        } else if let Some(binding) = binding {
            if app.config.at_revision(binding.revision, |_| commit()) != Some(true) {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "SRT route or listener changed",
                );
            }
        } else {
            commit();
        }
        return json_response(json!({"ticket":ticket,"expires_in":5}));
    }
    error(StatusCode::NOT_FOUND, "operation not implemented")
}
async fn balance(
    app: &Arc<App>,
    name: &str,
    path: &str,
    query: &str,
    secure: bool,
    grant: &Grant,
    revision: u64,
) -> Response {
    let work = balance_inner(app, name, path, query, secure, grant, revision);
    tokio::select! { biased;
        _=grant.cancelled()=>error(StatusCode::FORBIDDEN,"playback policy changed"),
        result=tokio::time::timeout(Duration::from_secs(8),work)=>result.unwrap_or_else(|_|error(StatusCode::SERVICE_UNAVAILABLE,"placement deadline exceeded")),
    }
}
async fn balance_inner(
    app: &Arc<App>,
    name: &str,
    path: &str,
    query: &str,
    secure: bool,
    grant: &Grant,
    revision: u64,
) -> Response {
    use futures_util::StreamExt;
    let root = app.config.snapshot();
    let peers = root["peers"].as_array().cloned().unwrap_or_default();
    if peers.len() > 64 || app.config.revision() != revision {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "routing configuration changed",
        );
    }
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
            let snapshot = app.http_snapshot(&p, &name, revision).await?;
            let ready = snapshot.ready.contains(&name);
            Some((snapshot, p, ready))
        }
    });
    // Include every configured peer: waiting on a busy peer's cache mutex must
    // not hide later healthy candidates. The registry bounds network fetches.
    let mut calls = futures_util::stream::iter(calls).buffer_unordered(64);
    let deadline = tokio::time::sleep(Duration::from_millis(4500));
    tokio::pin!(deadline);
    let mut observations = Vec::new();
    loop {
        let result = tokio::select! {biased; _=&mut deadline=>break,result=calls.next()=>result};
        let Some(result) = result else {
            break;
        };
        if let Some(observation) = result {
            observations.push(observation);
        }
    }
    drop(calls);
    let mut attempted = std::collections::HashSet::new();
    let mut turn = None;
    loop {
        // A failed admission can consume the remaining sample lifetime. Reproject
        // every observed candidate, including those the previous cost excluded.
        if grant.is_cancelled() {
            return error(StatusCode::FORBIDDEN, "playback policy changed");
        }
        if app.config.revision() != revision {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "routing configuration changed",
            );
        }
        let observed_at = Instant::now();
        let elapsed = |snapshot: &http_balancer::Snapshot| {
            observed_at
                .duration_since(snapshot.when)
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX)
        };
        let valid = observations
            .iter()
            .filter(|(snapshot, p, ready)| {
                p["hostname"]
                    .as_str()
                    .and_then(|_| {
                        app.http_load(
                            p,
                            snapshot,
                            *ready,
                            elapsed(snapshot),
                            crate::cluster::FALLBACK_MBPS,
                            revision,
                        )
                    })
                    .is_some_and(|load| load.age_ms <= 10000)
            })
            .collect::<Vec<_>>();
        let bitrate_mbps = valid
            .iter()
            .filter_map(|(snapshot, _, _)| {
                crate::cluster::observed_bitrate(&snapshot.value, name, elapsed(snapshot))
            })
            .fold(crate::cluster::FALLBACK_MBPS, f64::max);
        if !bitrate_mbps.is_finite() || bitrate_mbps > crate::cluster::MAX_HINT_MBPS {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "stream cost exceeds admission bounds",
            );
        }
        let mut nodes = Vec::new();
        for (snapshot, p, ready) in valid {
            if p["hostname"]
                .as_str()
                .is_some_and(|id| attempted.contains(id))
            {
                continue;
            }
            let Some(load) = p["hostname"].as_str().and_then(|_| {
                app.http_load(
                    p,
                    snapshot,
                    *ready,
                    elapsed(snapshot),
                    bitrate_mbps,
                    revision,
                )
            }) else {
                continue;
            };
            nodes.push(load)
        }
        let Some(id) = app.routing_rotation.select(&nodes, 0.0, &mut turn) else {
            break;
        };
        attempted.insert(id.clone());
        let (selected, p, _) = observations
            .iter()
            .find(|(_, p, _)| p["hostname"] == id)
            .unwrap();
        if crate::config::vendor_peers::is_vendor(p) {
            if let Some(location) = http_balancer::vendor_location(p, name, path, query)
                && app.hold_vendor_http(p, selected, bitrate_mbps, revision)
            {
                return (StatusCode::FOUND, [("location", location)]).into_response();
            }
            continue;
        }
        let api = p["api_url"].as_str().unwrap_or("");
        let key = p["cluster_key"].as_str().unwrap_or(&app.options.peer_key);
        let Ok(client) = app.cluster_client(p) else {
            continue;
        };
        let response = client
            .post(format!(
                "{}/flussonix/api/v1/admit",
                api.trim_end_matches('/')
            ))
            .header("X-Flussonix-Peer", key)
            .json(&json!({"name":name,"bitrate_mbps":bitrate_mbps}))
            .send()
            .await;
        if let Ok(r) = response {
            if r.status().is_success() {
                if let Some(body) = admission::read_json(r).await {
                    if grant.is_cancelled() {
                        return error(StatusCode::FORBIDDEN, "playback policy changed");
                    }
                    if app.config.revision() != revision {
                        return error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "routing configuration changed",
                        );
                    }
                    if let (Some(public), Some(ticket)) =
                        (p["public_payload_url"].as_str(), body["ticket"].as_str())
                    {
                        let mut q = parse_query(Some(query));
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
    }
    error(StatusCode::SERVICE_UNAVAILABLE, "no available CDN node")
}
async fn media_request(State(app): State<Arc<App>>, request: Request) -> Response {
    let activity = app.media.idle_memory.activity().await;
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
    idle_body::retain(response, activity)
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
    let selection = app
        .config
        .read(|root| crate::vendor_playback::select(root, name, app.config.revision()));
    match selection {
        crate::vendor_playback::Selection::Native => {}
        crate::vendor_playback::Selection::Excluded => {
            return error(StatusCode::NOT_FOUND, "stream excluded");
        }
        crate::vendor_playback::Selection::Ambiguous => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "overlapping vendor source namespaces",
            );
        }
        crate::vendor_playback::Selection::Vendor { key, revision } => {
            return match &app.vendor_playback {
                Some(runtime) => {
                    runtime
                        .serve(
                            &app,
                            crate::vendor_playback::HttpRequest::new(
                                &request, name, file, key, revision,
                            ),
                        )
                        .await
                }
                None => error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "vendor playback owner unavailable",
                ),
            };
        }
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
    app.http_handoffs.prune();
    if query.contains_key("flussonix_ticket") && query.contains_key("flussonix_handoff") {
        return error(StatusCode::BAD_REQUEST, "conflicting admission markers");
    }
    if query.contains_key("flussonix_handoff")
        && app
            .credentials
            .peer(header(request.headers(), "x-flussonix-peer"))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "peer playback cannot claim a viewer handoff",
        );
    }
    let mut handoff = if let Some(ticket) = query.get("flussonix_handoff") {
        match app.http_handoffs.claim(ticket) {
            Some(grant) => Some(grant),
            None => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "admission handoff invalid or expired",
                );
            }
        }
    } else {
        None
    };
    let mut grant = if app
        .credentials
        .peer(header(request.headers(), "x-flussonix-peer"))
    {
        Grant::peer()
    } else {
        let qs = App::http_viewer_query(request.uri().query().unwrap_or(""));
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
            finite_media: file.ends_with(".m4f"),
        };
        // Validate protocol before authorization. A valid native HTTP ticket
        // owns a global slot; possession never bypasses viewer policy checks.
        let outcome = if let Some(handoff) = handoff.as_ref() {
            app.playback_auth
                .authorize_handoff(resolved.policy, viewer, handoff)
                .await
        } else if let Some(ticket) = query.get("flussonix_ticket") {
            let slot = {
                let reservations = app.reservations.lock().await;
                let Some(r) = reservations.get(ticket).filter(|r| {
                    r.stream == name
                        && r.expires > Instant::now()
                        && matches!(r.kind, rtsp_balancer::Kind::Http)
                }) else {
                    return error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "admission ticket invalid or expired",
                    );
                };
                r.http_slot.clone()
            };
            if let Some(slot) = slot {
                app.playback_auth
                    .authorize_reserved(resolved.policy, viewer, slot)
                    .await
            } else {
                app.playback_auth
                    .authorize_control(resolved.policy, viewer)
                    .await
            }
        } else {
            app.playback_auth.authorize(resolved.policy, viewer).await
        };
        match outcome {
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
    let Some((cfg, revision)) = app.media_config(name).await else {
        return error(StatusCode::NOT_FOUND, "stream unavailable");
    };
    if grant.is_cancelled() {
        return error(StatusCode::FORBIDDEN, "playback policy changed");
    }
    if app.options.role == "lb" {
        return balance(
            &app,
            name,
            raw_path,
            &App::http_viewer_query(request.uri().query().unwrap_or("")),
            secure,
            &grant,
            revision,
        )
        .await;
    }
    if let Some(ticket) = query.get("flussonix_ticket") {
        let mut reservations = app.reservations.lock().await;
        let valid = reservations.get(ticket).is_some_and(|r| {
            r.stream == name
                && r.expires > Instant::now()
                && matches!(r.kind, rtsp_balancer::Kind::Http)
        });
        match valid {
            true => {
                let reservation = reservations.remove(ticket).unwrap();
                if reservation.http_slot.is_some() && grant.session_identity().is_some() {
                    app.http_handoffs
                        .insert(ticket.clone(), grant, reservation.bitrate_mbps);
                    return App::native_http_handoff_redirect(raw_path, &query, ticket);
                }
                return App::http_ticket_redirect(raw_path, &query);
            }
            _ => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "admission ticket invalid or expired",
                );
            }
        }
    }
    // M4F signals and timestamp media are one segmented output session.
    let segmented = !matches!(file, "mpegts" | "m4s");
    let credit = if let Some(ref mut claimed) = handoff {
        app.http_handoffs.activate(claimed, &grant, segmented)
    } else {
        app.http_handoffs.take(&grant, segmented && file != "m4f")
    };
    grant.set_output_credit(credit);
    drop(handoff);
    let signature = crate::media::media_signature(&cfg);
    let check = async {
        if grant.is_cancelled() {
            return false;
        }
        let current = app.media_config(name).await.is_some_and(|(c, _)| {
            c["disabled"] != true && crate::media::media_signature(&c) == signature
        });
        current && !grant.is_cancelled()
    };
    let worker = match app.media.ensure_guarded(name, &cfg, true, check).await {
        Ok(w) => w,
        Err(e) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                crate::gpu::public_error(&e).unwrap_or("stream input unavailable"),
            );
        }
    };
    let current = app.media_config(name).await.is_some_and(|(c, _)| {
        c["disabled"] != true && crate::media::media_signature(&c) == worker.signature()
    });
    if !current {
        app.media.stop_if_current(name, &worker).await;
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "stream changed during startup",
        );
    }
    if grant.is_cancelled() {
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
        let connection = matches!(
            request.version(),
            axum::http::Version::HTTP_10 | axum::http::Version::HTTP_11
        )
        .then(|| {
            request
                .extensions()
                .get::<crate::http_transport::Control>()
                .cloned()
        })
        .flatten();
        let body = native_ts_body::body(worker, grant, app.egress.clone(), connection);
        return (
            [
                ("Content-Type".to_owned(), "video/mp2t".to_owned()),
                ("Cache-Control".into(), "no-store".into()),
            ],
            body,
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
        q.remove("flussonix_handoff");
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
    } else if file.ends_with(".vtt") {
        "text/vtt; charset=utf-8"
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
    async fn admission_refreshes_resources_after_waiting_for_worker_observation() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: "cdn".into(),
                uplink_interface: "process".into(),
                admin_password: "owned-admin-secret".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            app.sample_metrics();
            let snapshot = app.telemetry.snapshot(app.options.uplink_mbps);
            if snapshot["cpu"].as_f64().is_some_and(|v| v < 0.9)
                && snapshot["ram"].as_f64().is_some_and(|v| v < 0.95)
            {
                break;
            }
        }
        let before = app.telemetry.snapshot(app.options.uplink_mbps);
        assert!(before["cpu"].as_f64().is_some_and(|v| v < 0.9));
        assert!(before["ram"].as_f64().is_some_and(|v| v < 0.95));
        assert_eq!(before["uplink"], 0.0);
        let lock = app.media.hold_startups_for_test().await;
        let request = router(app.clone()).oneshot(
            Request::post("/flussonix/api/v1/admit")
                .header("X-Flussonix-Peer", &app.options.peer_key)
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"name":"owned","bitrate_mbps":2}).to_string(),
                ))
                .unwrap(),
        );
        tokio::pin!(request);
        assert!(
            futures_util::poll!(&mut request).is_pending(),
            "worker observation must actually wait"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        app.egress.fetch_add(500_000_000, Ordering::Relaxed);
        app.sample_metrics();
        assert!(
            app.telemetry.snapshot(app.options.uplink_mbps)["uplink"]
                .as_f64()
                .unwrap()
                > 0.9
        );
        drop(lock);
        let response = request.await.unwrap();
        assert_eq!(
            response.status(),
            503,
            "a worker wait cannot preserve an obsolete low uplink sample"
        );
        assert!(app.reservations.lock().await.is_empty());
        assert_eq!(app.media.count().await, 0);
    }
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

mod iptv_management;
mod iptv_playlist;
mod native_ts_body;
#[path = "origin_resolution.rs"]
mod origin_resolution;
mod ts_startup;

mod fxlink_access;
mod idle_body;
pub(crate) mod rtsp_access;
pub(crate) mod ts_access;

pub(crate) mod publication;

mod admission;
mod http_balancer;
mod http_handoff;
mod http_tickets;
pub(crate) mod rtsp_balancer;
mod srt_lookup;
pub(crate) mod srt_tickets;
mod vendor_balancer;
mod vendor_http_state;
mod vendor_rtsp_tickets;

#[path = "source_activation.rs"]
mod source_activation;

#[cfg(test)]
mod vendor_namespace_tests {
    use super::*;
    #[tokio::test]
    async fn reserving_vendor_namespace_revokes_previous_native_mirror_grant_before_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-key".into(),
                peer_key: "owned-native-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let source = app
            .config
            .put(
                "sources",
                "native",
                json!({"api_url":"http://owned-native.invalid"}),
            )
            .unwrap();
        let cfg = json!({"name":"owned","inputs":[{"url":"http://owned-native.invalid/owned/mpegts"}],"static":false});
        app.mirrors.lock().await.insert(
            "owned".into(),
            Mirror {
                when: Instant::now(),
                config: cfg.clone(),
                source,
                available: true,
                denied: false,
                known: true,
                activation_only: false,
                serial: 1,
                switches: 0,
            },
        );
        let snapshot = app
            .resolved("owned", cfg, app.config.revision())
            .unwrap()
            .policy;
        let AuthOutcome::Allowed(grant) = app
            .playback_auth
            .authorize(
                snapshot,
                ViewerRequest {
                    name: "owned".into(),
                    proto: "mpegts".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("native fixture denied");
        };
        app.config.put("sources","m4s://owned-vendor.invalid",json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-private-cluster-key","flussonix_token_sha256":"a".repeat(64)})).unwrap();
        app.invalidate_sessions().await;
        assert!(
            grant.is_cancelled(),
            "previous native authority survived vendor namespace reservation"
        );
        app.media.stop_all().await;
    }
}

#[cfg(test)]
mod view_redaction_tests {
    use super::*;

    #[test]
    fn viewer_role_hides_source_activation_addresses_in_node_status() {
        let mut node = json!({
            "source_activation": {"owned": {"source": "https://origin.internal:8443"}},
            "vendor_source_activation": {"owned": {"source": "https://vendor.internal:8443"}},
            "active": 3
        });
        redact_view_value(&mut node);
        assert_eq!(
            node["source_activation"]["owned"]["source"],
            "https://[redacted]"
        );
        assert_eq!(
            node["vendor_source_activation"]["owned"]["source"],
            "https://[redacted]"
        );
        assert_eq!(node["active"], 3);
    }
}

#[cfg(test)]
mod test_vendor_admission;

#[cfg(test)]
#[path = "server/viewer_worker_tests.rs"]
mod viewer_worker_tests;

#[cfg(test)]
mod peer_transport_tests {
    use super::*;

    // Break caught: every native LB probe and admission can use the raw key on HTTP.
    #[test]
    fn cluster_client_rejects_remote_plaintext_peer_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        let app = App::new(
            directory.path().join("config.json"),
            directory.path().join("media"),
            Options {
                admin_password: "owned-admin-password".into(),
                peer_key: "owned-native-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            app.cluster_client(&json!({"api_url":"http://remote.example"}))
                .is_err()
        );
        assert!(
            app.cluster_client(&json!({"api_url":"http://remote.example","flussonix_allow_insecure_peer_http":true}))
                .is_ok()
        );
        assert!(
            app.cluster_client(&json!({"api_url":"http://127.0.0.1:9000"}))
                .is_ok()
        );
    }
}
