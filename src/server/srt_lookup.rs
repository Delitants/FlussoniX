//! HTTPS control-plane discovery for clients which then connect directly by SRT.
use super::*;
use futures_util::StreamExt;

struct Query {
    streamid: String,
    selection: crate::srt_playback::Selection,
    encrypted: bool,
}
fn decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'%'
            && !(bytes.get(i + 1)?.is_ascii_hexdigit() && bytes.get(i + 2)?.is_ascii_hexdigit())
        {
            return None;
        }
    }
    percent_encoding::percent_decode_str(&raw.replace('+', " "))
        .decode_utf8()
        .ok()
        .map(|s| s.into_owned())
}
fn query(raw: &str) -> Option<Query> {
    if raw.len() > 4096 {
        return None;
    }
    let mut fields = HashMap::new();
    for part in raw.split('&') {
        let (key, value) = part.split_once('=')?;
        let key = decode(key)?;
        if !matches!(key.as_str(), "streamid" | "encryption")
            || fields.insert(key, decode(value)?).is_some()
        {
            return None;
        }
    }
    let streamid = fields.remove("streamid")?;
    let selection = crate::srt_playback::Selection::parse(&streamid).ok()?;
    let encrypted = match fields.get("encryption").map(String::as_str) {
        None | Some("required") => true,
        Some("any") => false,
        _ => return None,
    };
    Some(Query {
        streamid,
        selection,
        encrypted,
    })
}
fn mapped(id: &str, name: &str) -> Option<String> {
    if name.contains(',') {
        return None;
    }
    let mut mode = false;
    let mut fields = Vec::new();
    for field in id.strip_prefix("#!::")?.split(',') {
        let (key, _) = field.split_once('=')?;
        mode |= key == "m";
        fields.push(if key == "r" {
            format!("r={name}")
        } else {
            field.to_owned()
        });
    }
    if !mode {
        fields.push("m=request".into());
    }
    let output = format!("#!::{}", fields.join(","));
    crate::srt_playback::Selection::parse(&output).ok()?;
    Some(output)
}
pub(super) async fn lookup(State(app): State<Arc<App>>, request: Request) -> Response {
    let result = async move {
        if request.method() != axum::http::Method::GET {
            return Err(405);
        }
        if request
            .extensions()
            .get::<crate::http_tls::SecureHttp>()
            .is_none()
        {
            return Err(426);
        }
        let q = query(request.uri().query().unwrap_or("")).ok_or(400u16)?;
        if app.options.role != "lb" || app.options.drain {
            return Err(503);
        }
        let viewer = ViewerRequest {
            name: q.selection.name,
            token: q.selection.token,
            proto: "srt".into(),
            qs: url::form_urlencoded::Serializer::new(String::new())
                .append_pair("streamid", &q.streamid)
                .finish(),
            ip: request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|v| v.0.ip().to_string())
                .unwrap_or_else(|| "127.0.0.1".into()),
            user_agent: header(request.headers(), "user-agent").unwrap_or("").into(),
            referer: header(request.headers(), "referer").unwrap_or("").into(),
            host: header(request.headers(), "host").unwrap_or("").into(),
        };
        app.srt_lookup(viewer, &q.streamid, q.encrypted).await
    }
    .await;
    let mut response = match result {
        Ok(value) => json_response(value),
        Err(status) => error(
            StatusCode::from_u16(status).unwrap(),
            "SRT lookup unavailable",
        ),
    };
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}
impl App {
    async fn srt_lookup(
        &self,
        viewer: ViewerRequest,
        streamid: &str,
        encrypted: bool,
    ) -> Result<Value, u16> {
        let selection = self.config.read(|root| {
            crate::vendor_playback::select(root, &viewer.name, self.config.revision())
        });
        match selection {
            crate::vendor_playback::Selection::Excluded => return Err(404),
            crate::vendor_playback::Selection::Ambiguous => return Err(503),
            crate::vendor_playback::Selection::Vendor { key, revision } => {
                return match &self.vendor_playback {
                    Some(runtime) => {
                        runtime
                            .srt_lookup(self, viewer, &key, revision, streamid, encrypted)
                            .await
                    }
                    None => Err(503),
                };
            }
            crate::vendor_playback::Selection::Native => {}
        }
        let resolved = self.resolve(&viewer.name).await.ok_or(404u16)?;
        if resolved.config["disabled"] == true {
            return Err(404);
        }
        let grant = match self
            .playback_auth
            .authorize_control(resolved.policy, viewer.clone())
            .await
        {
            AuthOutcome::Allowed(g) => g,
            AuthOutcome::Denied | AuthOutcome::Redirect(_) => return Err(403),
        };
        let result = self
            .srt_place(&viewer, streamid, encrypted, &grant, resolved.revision)
            .await;
        self.config
            .at_revision(resolved.revision, |_| {
                if grant.is_cancelled() {
                    return Err(403);
                }
                if result.is_ok() {
                    grant.playback();
                }
                result
            })
            .ok_or(503u16)?
    }
    pub(crate) async fn srt_place(
        &self,
        viewer: &ViewerRequest,
        streamid: &str,
        encrypted: bool,
        grant: &Grant,
        revision: u64,
    ) -> Result<Value, u16> {
        let work = self.srt_place_inner(viewer, streamid, encrypted, grant, revision);
        tokio::select! {biased; _=grant.cancelled()=>Err(403), result=tokio::time::timeout(Duration::from_secs(8),work)=>result.map_err(|_|503u16)?}
    }
    async fn srt_place_inner(
        &self,
        viewer: &ViewerRequest,
        streamid: &str,
        encrypted: bool,
        grant: &Grant,
        revision: u64,
    ) -> Result<Value, u16> {
        let peers = self.config.snapshot()["peers"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if peers.len() > 64 || self.config.revision() != revision {
            return Err(503);
        }
        let calls = peers.into_iter().map(|peer| async move {
            if !crate::config::vendor_peers::is_vendor(&peer) {
                return None;
            }
            let root = crate::config::vendor_peers::srt_root(peer["flussonix_srt_url"].as_str()?)?;
            let required = peer["flussonix_srt_encrypted"].as_bool()?;
            if encrypted && !required {
                return None;
            }
            let id = mapped(
                streamid,
                crate::config::vendor_peers::remote_name(&peer, &viewer.name)?,
            )?;
            let snapshot = self.http_snapshot(&peer, &viewer.name, revision).await?;
            Some((snapshot, peer, root.to_string(), id, required))
        });
        let mut calls = futures_util::stream::iter(calls).buffer_unordered(64);
        let deadline = tokio::time::sleep(Duration::from_millis(4500));
        tokio::pin!(deadline);
        let mut observations = Vec::new();
        loop {
            let next = tokio::select! {biased; _=&mut deadline=>break, next=calls.next()=>next};
            let Some(next) = next else { break };
            if let Some(observation) = next {
                observations.push(observation);
            }
        }
        drop(calls);
        let mut attempted = std::collections::HashSet::new();
        let mut turn = None;
        loop {
            let valid = observations
                .iter()
                .filter(|(snapshot, peer, _, _, _)| {
                    self.http_load(
                        peer,
                        snapshot,
                        snapshot.ready.contains(&viewer.name),
                        snapshot
                            .when
                            .elapsed()
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX),
                        crate::cluster::FALLBACK_MBPS,
                        revision,
                    )
                    .is_some_and(|load| load.age_ms <= 10000)
                })
                .collect::<Vec<_>>();
            let bitrate = valid
                .iter()
                .filter_map(|(snapshot, _, _, _, _)| {
                    crate::cluster::observed_bitrate(
                        &snapshot.value,
                        &viewer.name,
                        snapshot.when.elapsed().as_millis().try_into().ok()?,
                    )
                })
                .fold(crate::cluster::FALLBACK_MBPS, f64::max);
            if !bitrate.is_finite() || bitrate > crate::cluster::MAX_HINT_MBPS {
                return Err(503);
            }
            let nodes = valid
                .iter()
                .filter(|(_, peer, _, _, _)| {
                    !attempted.contains(peer["hostname"].as_str().unwrap_or(""))
                })
                .filter_map(|(snapshot, peer, _, _, _)| {
                    self.http_load(
                        peer,
                        snapshot,
                        snapshot.ready.contains(&viewer.name),
                        snapshot
                            .when
                            .elapsed()
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX),
                        bitrate,
                        revision,
                    )
                })
                .collect::<Vec<_>>();
            let Some(id) = self.routing_rotation.select(&nodes, 0.0, &mut turn) else {
                return Err(503);
            };
            attempted.insert(id.clone());
            if grant.is_cancelled() {
                return Err(403);
            }
            if self.options.drain || self.config.revision() != revision {
                return Err(503);
            }
            let (snapshot, peer, address, streamid, required) = observations
                .iter()
                .find(|(_, peer, _, _, _)| peer["hostname"] == id)
                .unwrap();
            // Shared HTTP/RTSP/SRT holds are advisory on this LB, never a vendor RPC.
            if !self.hold_vendor_http(peer, snapshot, bitrate, revision) {
                continue;
            }
            if grant.is_cancelled() {
                return Err(403);
            }
            if self.options.drain || self.config.revision() != revision {
                return Err(503);
            }
            return Ok(
                json!({"address":address.trim_end_matches('/'),"streamid":streamid,"encryption_required":required,"expires_in":5}),
            );
        }
    }
}
