use flussonix::config::ConfigStore;
use serde_json::json;
#[test]
fn public_rtsp_endpoints_validate_and_persist_without_mutating_failed_edits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let store = ConfigStore::open(&path).unwrap();
    store.put("peers", "edge", json!({"api_url":"http://127.0.0.1:9","flussonix_rtsp_url":"rtsp://cdn.example:8554","flussonix_rtsps_url":"rtsps://[::1]:8322/"})).unwrap();
    assert_eq!(
        ConfigStore::open(&path).unwrap().snapshot(),
        store.snapshot()
    );
    let before = store.snapshot();
    for field in ["flussonix_rtsp_url", "flussonix_rtsps_url"] {
        for invalid in [
            "http://cdn.example",
            "rtsp://user@cdn.example",
            "rtsp://@cdn.example",
            "rtsp://cdn.example/path",
            "rtsp://cdn.example?token=x",
            "rtsp://cdn.example#fragment",
            "rtsp://cdn.example:0",
            "rtsp://cdn.example:65536",
            "rtsp://cdn.example/percent%",
            "rtsp://cdn.example/raw\"quote",
            "rtsp://",
            "",
        ] {
            assert!(
                store.put("peers", "edge", json!({field:invalid})).is_err(),
                "{field}: {invalid}"
            );
            assert_eq!(store.snapshot(), before);
        }
    }
    assert!(
        store
            .put(
                "peers",
                "edge",
                json!({"flussonix_rtsp_url":"rtsps://cdn.example"})
            )
            .is_err()
    );
    assert!(
        store
            .put(
                "peers",
                "edge",
                json!({"flussonix_rtsps_url":"rtsp://cdn.example"})
            )
            .is_err()
    );
    assert!(store.put("sources","origin",json!({"api_url":"http://source.example","flussonix_rtsp_url":"rtsp://source.example"})).is_err());
    store
        .put("peers", "edge", json!({"flussonix_rtsp_url":null}))
        .unwrap();
    assert!(
        store.snapshot()["peers"][0]
            .get("flussonix_rtsp_url")
            .is_none()
    );
    assert_eq!(
        ConfigStore::open(&path).unwrap().snapshot(),
        store.snapshot()
    );
}
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use flussonix::{
    rtsp,
    server::{App, Options, router},
};
use futures_util::FutureExt;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
#[path = "support/tls.rs"]
mod tls_fixture;
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
#[derive(Default)]
struct Probe {
    polls: AtomicUsize,
    admits: AtomicUsize,
    pulls: AtomicUsize,
    bad_key: AtomicBool,
    snapshot: Mutex<Option<Value>>,
    http_snapshot: Mutex<Option<Value>>,
    http_polls: AtomicUsize,
    http_active: AtomicUsize,
    http_peak: AtomicUsize,
    http_delay_ms: AtomicUsize,
    http_pause: AtomicBool,
    http_entered: Notify,
    http_release: Notify,
    http_reply: Mutex<Option<(StatusCode, Vec<u8>)>>,
    http_chunked: AtomicBool,
    reject: AtomicBool,
    pause: AtomicBool,
    stall: AtomicBool,
    entered: Notify,
    release: Notify,
    key: String,
    auth_queries: Mutex<Vec<std::collections::HashMap<String, String>>>,
    auth_target: Mutex<Option<String>>,
    routing_delay_ms: AtomicUsize,
    admission_delay_ms: AtomicUsize,
    admission_statuses: Mutex<Vec<u16>>,
}
struct HttpProbeGuard(Arc<Probe>);
impl Drop for HttpProbeGuard {
    fn drop(&mut self) {
        self.0.http_active.fetch_sub(1, Ordering::SeqCst);
    }
}
async fn intercept(State(p): State<Arc<Probe>>, r: Request<Body>, next: Next) -> Response {
    let path = r.uri().path();
    let admission = path == "/flussonix/api/v1/admit";
    if path == "/flussonix/api/v1/node" {
        p.http_polls.fetch_add(1, Ordering::SeqCst);
        let active = p.http_active.fetch_add(1, Ordering::SeqCst) + 1;
        p.http_peak.fetch_max(active, Ordering::SeqCst);
        let _active = HttpProbeGuard(p.clone());
        p.http_entered.notify_one();
        if p.http_pause.load(Ordering::SeqCst) {
            p.http_release.notified().await;
        }
        tokio::time::sleep(Duration::from_millis(
            p.http_delay_ms.load(Ordering::SeqCst) as u64,
        ))
        .await;
        if let Some((status, bytes)) = p.http_reply.lock().unwrap().clone() {
            if p.http_chunked.load(Ordering::SeqCst) {
                let stream = tokio_stream::iter(vec![Ok::<_, std::convert::Infallible>(
                    bytes::Bytes::from(bytes),
                )]);
                return (
                    status,
                    [("content-type", "application/json")],
                    Body::from_stream(stream),
                )
                    .into_response();
            }
            return (status, [("content-type", "application/json")], bytes).into_response();
        }
        if let Some(snapshot) = p.http_snapshot.lock().unwrap().clone() {
            if r.headers()
                .get("x-flussonix-peer")
                .and_then(|s| s.to_str().ok())
                != Some(&p.key)
            {
                p.bad_key.store(true, Ordering::SeqCst);
            }
            return axum::Json(snapshot).into_response();
        }
    }
    if path == "/owned-auth" {
        p.auth_queries.lock().unwrap().push(
            url::form_urlencoded::parse(r.uri().query().unwrap_or("").as_bytes())
                .into_owned()
                .collect(),
        );
        if let Some(target) = p.auth_target.lock().unwrap().clone() {
            return (StatusCode::FOUND, [("Location", target)]).into_response();
        }
        return (StatusCode::OK, [("X-AuthDuration", "3600")]).into_response();
    }
    if path.ends_with("/m4s") {
        p.pulls.fetch_add(1, Ordering::SeqCst);
    }
    if path == "/flussonix/api/v1/rtsp-routing" || path == "/flussonix/api/v1/admit" {
        if r.headers()
            .get("x-flussonix-peer")
            .and_then(|s| s.to_str().ok())
            != Some(&p.key)
        {
            p.bad_key.store(true, Ordering::SeqCst);
        }
        if path.ends_with("rtsp-routing") {
            p.polls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(
                p.routing_delay_ms.load(Ordering::SeqCst) as u64,
            ))
            .await;
            if p.stall.load(Ordering::SeqCst) {
                p.release.notified().await;
            }
            if let Some(v) = p.snapshot.lock().unwrap().clone() {
                return axum::Json(v).into_response();
            }
        } else {
            tokio::time::sleep(Duration::from_millis(
                p.admission_delay_ms.load(Ordering::SeqCst) as u64,
            ))
            .await;
            p.admits.fetch_add(1, Ordering::SeqCst);
            if p.pause.load(Ordering::SeqCst) {
                p.entered.notify_one();
                p.release.notified().await;
            }
            if p.reject.load(Ordering::SeqCst) {
                p.admission_statuses.lock().unwrap().push(503);
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    }
    let response = next.run(r).await;
    if admission {
        p.admission_statuses
            .lock()
            .unwrap()
            .push(response.status().as_u16());
    }
    response
}
struct Node {
    _dir: tempfile::TempDir,
    cert: tls_fixture::Certificates,
    app: Arc<App>,
    probe: Arc<Probe>,
    http: String,
    plain: String,
    tls: String,
    cancel: CancellationToken,
    tasks: Vec<AbortOnDropHandle<()>>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl Node {
    // Use the daemon entry point so TLS requests receive the internal secure marker.
    async fn secure_delivery(&mut self) -> String {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let app = self.app.clone();
        let config = self.cert.server();
        let cancel = self.cancel.clone();
        self.tasks
            .push(AbortOnDropHandle::new(tokio::spawn(async move {
                flussonix::http_tls::serve(tcp, config, app, cancel)
                    .await
                    .unwrap();
            })));
        format!("https://{address}")
    }
    async fn secure_media(&mut self) -> String {
        use axum::serve::ListenerExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        let listener =
            flussonix::http_tls::Listener::new(listener, self.cert.server()).tap_io(|stream| {
                let _ = stream.get_ref().0.set_nodelay(true);
            });
        let app = self.app.clone();
        let probe = self.probe.clone();
        let stop = self.cancel.clone();
        self.tasks
            .push(AbortOnDropHandle::new(tokio::spawn(async move {
                axum::serve(
                    listener,
                    router(app)
                        .layer(middleware::from_fn_with_state(probe, intercept))
                        .into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
            })));
        url
    }
    async fn new(role: &str, limit: u64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cert = tls_fixture::Certificates::new();
        let key = format!("owned-{role}-peer-secret");
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: role.into(),
                node_name: role.into(),
                client_limit: limit,
                admin_password: "owned-admin-secret".into(),
                peer_key: key.clone(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let probe = Arc::new(Probe {
            key,
            ..Default::default()
        });
        let cancel = CancellationToken::new();
        let mut tasks = vec![];
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_url = format!("http://{}", http.local_addr().unwrap());
        let a = app.clone();
        let p = probe.clone();
        let stop = cancel.clone();
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(
                http,
                router(a).layer(middleware::from_fn_with_state(p, intercept)),
            )
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
        })));
        let plain = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls = TcpListener::bind("127.0.0.1:0").await.unwrap();
        app.set_rtsp_publication(
            Some(plain.local_addr().unwrap()),
            Some(tls.local_addr().unwrap()),
        );
        let plain_url = format!("rtsp://{}", plain.local_addr().unwrap());
        let tls_url = format!("rtsps://{}", tls.local_addr().unwrap());
        let a = app.clone();
        let stop = cancel.clone();
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            rtsp::serve(plain, a, stop).await.unwrap();
        })));
        let a = app.clone();
        let stop = cancel.clone();
        let config = cert.server();
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            rtsp::serve_tls(tls, a, stop, config).await.unwrap();
        })));
        let a = app.clone();
        let stop = cancel.clone();
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {_=stop.cancelled()=>break,_=interval.tick()=>a.sample_metrics()}
            }
        })));
        Self {
            _dir: dir,
            cert,
            app,
            probe,
            http: http_url,
            plain: plain_url,
            tls: tls_url,
            cancel,
            tasks,
        }
    }
    async fn socket(&self, secure: bool) -> BufReader<Box<dyn Io>> {
        let u = url::Url::parse(if secure { &self.tls } else { &self.plain }).unwrap();
        let tcp = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
            .await
            .unwrap();
        let io: Box<dyn Io> = if secure {
            Box::new(
                tokio_rustls::TlsConnector::from(self.cert.client())
                    .connect(
                        tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                        tcp,
                    )
                    .await
                    .unwrap(),
            )
        } else {
            Box::new(tcp)
        };
        BufReader::new(io)
    }
    async fn describe(&self, secure: bool, name: &str, query: &str) -> (u16, String, Vec<u8>) {
        let uri = format!(
            "{}/{name}?{query}",
            if secure { &self.tls } else { &self.plain }
        );
        request(&mut self.socket(secure).await, "DESCRIBE", &uri, "").await
    }
    async fn stop(&mut self) {
        self.probe.http_pause.store(false, Ordering::SeqCst);
        self.probe.http_release.notify_waiters();
        self.app.media.stop_all().await;
        self.cancel.cancel();
        for task in self.tasks.drain(..) {
            tokio::time::timeout(Duration::from_secs(6), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
    async fn measured(&self) {
        for _ in 0..60 {
            let v = get_actual_node(self).await;
            if v["cpu"].as_f64().is_some_and(|x| x < 0.9)
                && v["ram"].as_f64().is_some_and(|x| x < 0.95)
                && v["uplink"].as_f64().is_some_and(|x| x < 0.8)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("owned node lacks measured admission capacity")
    }
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(12))
        .build()
        .unwrap()
}
async fn get_node(node: &Node) -> Value {
    client()
        .get(format!("{}/flussonix/api/v1/node", node.http))
        .header("X-Flussonix-Peer", &node.app.options.peer_key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
// Exercise the native route without the advisory-telemetry fixture interceptor.
async fn get_actual_node(node: &Node) -> Value {
    use tower::ServiceExt;
    let response = router(node.app.clone())
        .oneshot(
            Request::get("/flussonix/api/v1/node")
                .header("X-Flussonix-Peer", &node.app.options.peer_key)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()
}
async fn reserve(node: &Node, name: &str, protocol: &str, token: &str) -> reqwest::Response {
    client().post(format!("{}/flussonix/api/v1/admit",node.http)).header("X-Flussonix-Peer",&node.app.options.peer_key).json(&json!({"name":name,"protocol":protocol,"token_hash":format!("{:x}",Sha256::digest(token.as_bytes()))})).send().await.unwrap()
}
async fn request(
    s: &mut BufReader<Box<dyn Io>>,
    method: &str,
    uri: &str,
    headers: &str,
) -> (u16, String, Vec<u8>) {
    s.get_mut()
        .write_all(format!("{method} {uri} RTSP/1.0\r\nCSeq: 29\r\n{headers}\r\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut h = vec![];
        while !h.ends_with(b"\r\n\r\n") {
            assert!(h.len() < 131072);
            h.push(s.read_u8().await.unwrap());
        }
        let h = String::from_utf8(h).unwrap();
        let code = h.split(' ').nth(1).unwrap().parse().unwrap();
        let len = h
            .lines()
            .find_map(|s| s.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut body = vec![0; len];
        s.read_exact(&mut body).await.unwrap();
        (code, h, body)
    })
    .await
    .unwrap()
}
fn location(reply: &(u16, String, Vec<u8>)) -> String {
    assert_eq!(reply.0, 302, "{}", reply.1);
    assert!(reply.1.starts_with("RTSP/1.0 302 Moved Temporarily\r\n"));
    assert!(reply.1.contains("CSeq: 29\r\n"));
    assert!(!reply.1.contains("Session:"));
    assert!(reply.2.is_empty());
    reply
        .1
        .lines()
        .find_map(|s| s.strip_prefix("Location: "))
        .unwrap()
        .into()
}
struct Lab {
    source: Node,
    cdn: Node,
    lb: Node,
}
impl Lab {
    async fn new(limit: u64) -> Self {
        let source = Node::new("source", 1000).await;
        let cdn = Node::new("cdn", limit).await;
        let lb = Node::new("lb", 1000).await;
        source.app.config.put("streams","region/owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned+viewer"))})).unwrap();
        for n in [&cdn, &lb] {
            n.app.config.put("sources","origin",json!({"api_url":source.http,"private_payload_url":source.http,"cluster_key":source.app.options.peer_key,"flussonix_transport":"m4s"})).unwrap();
        }
        lb.app.config.put("peers","edge",json!({"api_url":cdn.http,"public_payload_url":cdn.http,"cluster_key":cdn.app.options.peer_key,"flussonix_rtsp_url":cdn.plain,"flussonix_rtsps_url":cdn.tls})).unwrap();
        cdn.measured().await;
        Self { source, cdn, lb }
    }
    async fn stop(&mut self) {
        self.lb.stop().await;
        self.cdn.stop().await;
        self.source.stop().await;
    }
}
const QS: &str = "token=owned%2Bviewer&customer=a%26b&blank=&client=owned";
#[tokio::test]
async fn configured_rtsps_relay_decodes_native_lb_cdn_source_chain() {
    let mut lab = Lab::new(1000).await;
    let mut relay = Node::new("standalone", 1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        let private = lab.source.secure_media().await;
        for node in [&lab.lb, &lab.cdn] {
            node.app.config.put("sources", "origin", json!({"private_payload_url":private,"flussonix_media_tls_ca":lab.source.cert.ca})).unwrap();
        }
        assert_eq!(lab.lb.describe(true, "region/owned", "token=wrong").await.0, 403);
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 0);
        assert_eq!(lab.source.app.media.count().await, 0);
        let trust = relay._dir.path().join("cluster-ca.pem");
        let mut roots = std::fs::read(&lab.lb.cert.ca).unwrap();
        roots.extend_from_slice(&std::fs::read(&lab.cdn.cert.ca).unwrap());
        std::fs::write(&trust, roots).unwrap();
        relay.app.config.put("streams", "relay", json!({"static":false,"inputs":[{"url":format!("{}/region/owned?{QS}", lab.lb.tls),"flussonix_tls_ca":trust}]})).unwrap();
        let playback = format!("{}/relay/fmp4/index.m3u8", relay.http);
        let http = client();
        let ready = tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                let reply = http.get(&playback).send().await.unwrap();
                if reply.status().is_success() { return true; }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.unwrap_or(false);
        assert!(ready, "configured verified RTSPS relay must produce HLS");
        for _ in 0..2 {
            let output = tokio::time::timeout(Duration::from_secs(30), tokio::process::Command::new("ffmpeg")
                .args(["-nostdin", "-v", "error", "-i", &playback, "-t", "2", "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "framemd5", "-"])
                .kill_on_drop(true).output()).await.unwrap().unwrap();
            assert!(output.status.success() && output.stderr.is_empty(), "strict TLS chain decode: {}", String::from_utf8_lossy(&output.stderr));
            let media = String::from_utf8_lossy(&output.stdout);
            assert!(media.contains("#media_type 0: video") && media.contains("#media_type 1: audio"));
            assert!(media.lines().filter(|l| l.starts_with("0,")).count() >= 20);
            assert!(media.lines().filter(|l| l.starts_with("1,")).count() >= 40);
        }
        assert_eq!(lab.lb.app.media.count().await, 0);
        assert_eq!(lab.cdn.app.media.count().await, 1);
        assert_eq!(lab.source.app.media.count().await, 1);
        assert_eq!(relay.app.media.count().await, 1);
        assert_eq!(lab.source.probe.pulls.load(Ordering::SeqCst), 1, "one shared private M4S pull");
        assert!(!lab.cdn.probe.bad_key.load(Ordering::SeqCst));
        assert!(!lab.source.probe.bad_key.load(Ordering::SeqCst));
    }).catch_unwind().await;
    relay.stop().await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn native_lb_routes_before_media_and_preserves_credentials() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        assert_eq!(
            lab.lb
                .describe(false, "region/owned", "token=wrong")
                .await
                .0,
            403
        );
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), 0);
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 0);
        let target = location(&lab.lb.describe(false, "region/owned", QS).await);
        assert!(
            target.starts_with(&format!(
                "{}/region/owned?{}&flussonix_ticket=",
                lab.cdn.plain, QS
            )),
            "{target}"
        );
        assert!(!target.contains(&lab.cdn.app.options.peer_key));
        assert!(!target.contains(&lab.source.app.options.peer_key));
        let u = url::Url::parse(&target).unwrap();
        let ticket = u
            .query_pairs()
            .find_map(|(k, v)| (k == "flussonix_ticket").then(|| v.into_owned()))
            .unwrap();
        assert!(uuid::Uuid::parse_str(&ticket).is_ok());
        assert_eq!(get_node(&lab.cdn).await["reserved"], 1);
        for node in [&lab.source, &lab.cdn, &lab.lb] {
            assert_eq!(node.app.media.count().await, 0);
            assert_eq!(node.app.playback_auth.live_grants(), 0);
        }
        assert!(!lab.cdn.probe.bad_key.load(Ordering::SeqCst));
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn tickets_are_bound_to_stream_transport_token_and_protocol_without_destructive_mismatch() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        lab.source
            .app
            .config
            .put(
                "streams",
                "region/other",
                lab.source.app.config.snapshot()["streams"][0].clone(),
            )
            .unwrap();
        let reply = reserve(&lab.cdn, "region/owned", "rtsp", "owned+viewer").await;
        assert_eq!(reply.status(), 200);
        let ticket = reply.json::<Value>().await.unwrap()["ticket"]
            .as_str()
            .unwrap()
            .to_owned();
        let qs = format!("{QS}&flussonix_ticket={ticket}");
        assert_eq!(
            lab.cdn
                .describe(
                    false,
                    "region/owned",
                    &format!("token=wrong&flussonix_ticket={ticket}")
                )
                .await
                .0,
            403
        );
        assert_eq!(lab.cdn.describe(false, "region/other", &qs).await.0, 503);
        assert_eq!(lab.cdn.describe(true, "region/owned", &qs).await.0, 503);
        let http = client()
            .get(format!("{}/region/owned/mpegts?{qs}", lab.cdn.http))
            .send()
            .await
            .unwrap();
        assert_eq!(http.status(), 503);
        assert_eq!(get_node(&lab.cdn).await["reserved"], 1);
        assert_eq!(lab.cdn.app.media.count().await, 0);
        assert_eq!(
            lab.cdn.app.playback_auth.active(),
            0,
            "wrong HTTP protocol must not occupy playback capacity"
        );
        let mut connection = lab.cdn.socket(false).await;
        let uri = format!("{}/region/owned?{qs}", lab.cdn.plain);
        let accepted = request(&mut connection, "DESCRIBE", &uri, "").await;
        assert_eq!(accepted.0, 200, "{}", accepted.1);
        assert!(String::from_utf8_lossy(&accepted.2).contains("m=audio"));
        assert_eq!(get_node(&lab.cdn).await["reserved"], 0);
        let control = String::from_utf8_lossy(&accepted.2)
            .lines()
            .find_map(|l| {
                l.strip_prefix("a=control:")
                    .filter(|v| v.starts_with("trackID="))
            })
            .unwrap()
            .to_owned();
        let setup = request(
            &mut connection,
            "SETUP",
            &format!("{}/region/owned/{control}?{qs}", lab.cdn.plain),
            "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n",
        )
        .await;
        assert_eq!(setup.0, 200, "{}", setup.1);
        assert_eq!(lab.cdn.describe(false, "region/owned", &qs).await.0, 503);
        let reply = reserve(&lab.cdn, "region/owned", "http", "owned+viewer").await;
        assert_eq!(reply.status(), 200);
        let http_ticket = reply.json::<Value>().await.unwrap()["ticket"]
            .as_str()
            .unwrap()
            .to_owned();
        let qs = format!("{QS}&flussonix_ticket={http_ticket}");
        assert_eq!(lab.cdn.describe(false, "region/owned", &qs).await.0, 503);
        assert_eq!(
            client()
                .get(format!("{}/region/owned/index.m3u8?{qs}", lab.cdn.http))
                .send()
                .await
                .unwrap()
                .status(),
            302
        );
        assert_eq!(lab.cdn.app.media.count().await, 1);
        let wrong = reserve(&lab.cdn, "region/owned", "rtsp", "other-token")
            .await
            .json::<Value>()
            .await
            .unwrap()["ticket"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            lab.cdn
                .describe(
                    false,
                    "region/owned",
                    &format!("{QS}&flussonix_ticket={wrong}")
                )
                .await
                .0,
            503
        );
        assert_eq!(get_node(&lab.cdn).await["reserved"], 1);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn concurrent_lbs_use_cached_load_but_cdn_rechecks_capacity() {
    let mut lab = Lab::new(1).await;
    let result = std::panic::AssertUnwindSafe(async {
        let replies = futures_util::future::join_all(
            (0..8).map(|_| lab.lb.describe(false, "region/owned", QS)),
        )
        .await;
        assert_eq!(
            replies.iter().filter(|r| r.0 == 302).count(),
            1,
            "{replies:?}"
        );
        assert_eq!(replies.iter().filter(|r| r.0 == 503).count(), 7);
        assert_eq!(
            lab.cdn.probe.polls.load(Ordering::SeqCst),
            1,
            "snapshot not coalesced"
        );
        assert_eq!(get_node(&lab.cdn).await["reserved"], 1);
        assert_eq!(lab.cdn.app.media.count().await, 0);
        assert_eq!(lab.lb.app.media.count().await, 0);
        assert_eq!(lab.lb.app.playback_auth.live_grants(), 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn secure_native_routing_uses_verified_tls_and_never_downgrades() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        let target = location(&lab.lb.describe(true, "region/owned", QS).await);
        assert!(target.starts_with(&format!("{}/region/owned?", lab.cdn.tls)));
        let mut tls = lab.cdn.socket(true).await;
        assert_eq!(
            request(
                &mut tls,
                "DESCRIBE",
                &target.replacen("rtsps:", "rtsp:", 1),
                ""
            )
            .await
            .0,
            200,
            "TLS URI alias must use encrypted ticket"
        );
        lab.lb
            .app
            .config
            .put("peers", "edge", json!({"flussonix_rtsps_url":null}))
            .unwrap();
        assert_eq!(lab.lb.describe(true, "region/owned", QS).await.0, 503);
        assert!(
            location(&lab.lb.describe(false, "region/owned", QS).await).starts_with(&lab.cdn.plain)
        );
        lab.lb
            .app
            .config
            .put(
                "peers",
                "edge",
                json!({"flussonix_rtsp_url":null,"flussonix_rtsps_url":lab.cdn.tls}),
            )
            .unwrap();
        assert!(
            location(&lab.lb.describe(false, "region/owned", QS).await).starts_with(&lab.cdn.tls)
        );
        assert_eq!(lab.lb.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn reservations_expire_and_rpc_requires_peer_protocol_and_content_route() {
    let mut node = Node::new("cdn", 1).await;
    let result = std::panic::AssertUnwindSafe(async {
        node.app
            .config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        node.measured().await;
        assert_eq!(
            client()
                .post(format!("{}/flussonix/api/v1/admit", node.http))
                .json(&json!({"name":"owned","protocol":"rtsp"}))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        assert_eq!(
            client()
                .post(format!("{}/flussonix/api/v1/admit", node.http))
                .header("X-Flussonix-Peer", &node.app.options.peer_key)
                .json(&json!({"name":"owned","protocol":"rtsp"}))
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(reserve(&node, "absent", "rtsp", "v").await.status(), 404);
        assert_eq!(reserve(&node, "owned", "unknown", "v").await.status(), 400);
        let response = reserve(&node, "owned", "rtsp", "v").await;
        assert_eq!(response.status(), 200);
        let ticket = response.json::<Value>().await.unwrap()["ticket"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(reserve(&node, "owned", "rtsp", "v").await.status(), 503);
        assert_eq!(node.app.media.count().await, 0);
        tokio::time::sleep(Duration::from_millis(5100)).await;
        assert_eq!(
            node.describe(
                false,
                "owned",
                &format!("token=v&flussonix_ticket={ticket}")
            )
            .await
            .0,
            503
        );
        assert_eq!(get_node(&node).await["reserved"], 0);
        assert_eq!(node.app.media.count().await, 0);
        assert_eq!(reserve(&node, "owned", "rtsp", "v").await.status(), 200);
    })
    .catch_unwind()
    .await;
    node.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

async fn invalidate(lab: &Lab) {
    let peer = lab.lb.app.config.snapshot()["peers"][0].clone();
    lab.lb.app.config.put("peers", "edge", peer).unwrap();
}

// Keep advisory loads exactly equal while real CDNs still authorize and reserve.
async fn tied_delivery_peer(lab: &Lab) -> Node {
    let alternative = Node::new("cdn", 1000).await;
    alternative
        .app
        .config
        .put(
            "sources",
            "origin",
            lab.cdn.app.config.snapshot()["sources"][0].clone(),
        )
        .unwrap();
    lab.lb.app.config.put("peers", "alternate", json!({"api_url":alternative.http,"public_payload_url":alternative.http,"cluster_key":alternative.app.options.peer_key,"flussonix_rtsp_url":alternative.plain,"flussonix_rtsps_url":alternative.tls})).unwrap();
    lab.cdn.measured().await;
    alternative.measured().await;
    for node in [&lab.cdn, &alternative] {
        let mut snapshot = get_node(node).await;
        snapshot["uplink"] = json!(0.1);
        snapshot["uplink_mbps"] = json!(1000);
        snapshot["cpu"] = json!(0.1);
        snapshot["ram"] = json!(0.1);
        snapshot["active"] = json!(0);
        snapshot["reserved"] = json!(0);
        snapshot["reserved_mbps"] = json!(0);
        snapshot["age_ms"] = json!(0);
        snapshot["ready"] = json!([]);
        snapshot["streams"] = json!([]);
        snapshot["stream_bitrates"] = json!({});
        *node.probe.http_snapshot.lock().unwrap() = Some(snapshot.clone());
        *node.probe.snapshot.lock().unwrap() = Some(snapshot);
    }
    alternative
}

async fn placement_target(lb: &Node, protocol: usize) -> String {
    if protocol == 0 {
        let response = client()
            .get(format!("{}/region/owned/index.m3u8?{QS}", lb.http))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 302);
        response.headers()["location"].to_str().unwrap().to_owned()
    } else {
        location(&lb.describe(protocol == 2, "region/owned", QS).await)
    }
}

fn targets_node(target: &str, node: &Node, protocol: usize) -> bool {
    target.starts_with(match protocol {
        0 => &node.http,
        1 => &node.plain,
        _ => &node.tls,
    })
}

// Removing either caller's rotating selection must concentrate its six redirects.
#[tokio::test]
async fn exact_pressure_ties_distribute_http_rtsp_and_rtsps_redirects() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = tied_delivery_peer(&lab).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for protocol in 0..3 {
            let mut counts = [0, 0];
            for _ in 0..6 {
                let target = placement_target(&lab.lb, protocol).await;
                if targets_node(&target, &lab.cdn, protocol) {
                    counts[0] += 1;
                } else {
                    assert!(targets_node(&target, &alternative, protocol));
                    counts[1] += 1;
                }
                let query = url::Url::parse(&target).unwrap();
                assert!(
                    query
                        .query_pairs()
                        .any(|(k, v)| k == "token" && v == "owned+viewer")
                );
                assert!(
                    query
                        .query_pairs()
                        .any(|(k, v)| k == "flussonix_ticket" && uuid::Uuid::parse_str(&v).is_ok())
                );
                assert!(!target.contains(&alternative.app.options.peer_key));
            }
            assert_eq!(counts, [3, 3], "protocol {protocol} must share exact ties");
        }
        for node in [&lab.cdn, &alternative] {
            *node.probe.http_snapshot.lock().unwrap() = None;
            let actual = get_node(node).await;
            assert_eq!(actual["reserved"], 9);
            assert_eq!(actual["reserved_mbps"], 18.0);
            assert!(!node.probe.bad_key.load(Ordering::SeqCst));
        }
        for node in [&lab.source, &lab.cdn, &lab.lb, &alternative] {
            assert_eq!(node.app.media.count().await, 0);
        }
    })
    .catch_unwind()
    .await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// Fixed or non-atomic turns can concentrate concurrent requests.
#[tokio::test]
async fn concurrent_mixed_protocol_ties_share_one_lb_rotation() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = tied_delivery_peer(&lab).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let lb = &lab.lb;
        let targets = futures_util::future::join_all(
            (0..24).map(|i| async move { (i % 3, placement_target(lb, i % 3).await) }),
        )
        .await;
        let mut counts = [0, 0];
        for (protocol, target) in targets {
            if targets_node(&target, &lab.cdn, protocol) {
                counts[0] += 1;
            } else {
                assert!(targets_node(&target, &alternative, protocol));
                counts[1] += 1;
            }
        }
        assert_eq!(counts, [12, 12]);
        for node in [&lab.cdn, &alternative] {
            *node.probe.http_snapshot.lock().unwrap() = None;
            let actual = get_node(node).await;
            assert_eq!(actual["reserved"], 12);
            assert_eq!(actual["reserved_mbps"], 24.0);
            assert!(!node.probe.bad_key.load(Ordering::SeqCst));
        }
        for node in [&lab.source, &lab.cdn, &lab.lb, &alternative] {
            assert_eq!(node.app.media.count().await, 0);
        }
    })
    .catch_unwind()
    .await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// A failed peer uses the same placement turn; it cannot consume the next request's turn.
#[tokio::test]
async fn tied_admission_retry_keeps_the_request_rotation_turn() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = tied_delivery_peer(&lab).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        alternative.probe.reject.store(true, Ordering::SeqCst);
        for protocol in 0..3 {
            assert!(targets_node(
                &placement_target(&lab.lb, protocol).await,
                &lab.cdn,
                protocol
            ));
        }
        assert_eq!(alternative.probe.admits.load(Ordering::SeqCst), 2);
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 3);
        *alternative.probe.http_snapshot.lock().unwrap() = None;
        assert_eq!(get_node(&alternative).await["reserved"], 0);
        for node in [&lab.source, &lab.cdn, &lab.lb, &alternative] {
            assert_eq!(node.app.media.count().await, 0);
        }
    })
    .catch_unwind()
    .await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// A global counter aliases the two-node RTSPS set while HTTP visits three nodes.
#[tokio::test]
async fn different_protocol_candidate_sets_keep_independent_tie_rotations() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = tied_delivery_peer(&lab).await;
    let mut third = Node::new("cdn", 1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        third.app.config.put("sources", "origin", lab.cdn.app.config.snapshot()["sources"][0].clone()).unwrap();
        lab.lb.app.config.put("peers", "third", json!({"api_url":third.http,"public_payload_url":third.http,"cluster_key":third.app.options.peer_key,"flussonix_rtsp_url":third.plain})).unwrap();
        lab.cdn.measured().await; alternative.measured().await; third.measured().await;
        let snapshot = lab.cdn.probe.http_snapshot.lock().unwrap().clone().unwrap();
        *third.probe.http_snapshot.lock().unwrap() = Some(snapshot.clone());
        // Third has no RTSPS public endpoint; its HTTP telemetry remains eligible.
        *third.probe.snapshot.lock().unwrap() = Some(snapshot);
        let mut http_counts = [0, 0, 0];
        let mut secure_counts = [0, 0];
        for _ in 0..6 {
            let target = placement_target(&lab.lb, 0).await;
            if targets_node(&target, &lab.cdn, 0) { http_counts[0] += 1; }
            else if targets_node(&target, &alternative, 0) { http_counts[1] += 1; }
            else { assert!(targets_node(&target, &third, 0)); http_counts[2] += 1; }
            let target = placement_target(&lab.lb, 2).await;
            if targets_node(&target, &lab.cdn, 2) { secure_counts[0] += 1; }
            else { assert!(targets_node(&target, &alternative, 2)); secure_counts[1] += 1; }
        }
        if http_counts != [2, 2, 2] || secure_counts != [3, 3] {
            for (label,node) in [("edge",&lab.cdn),("alternate",&alternative),("third",&third)] {
                *node.probe.http_snapshot.lock().unwrap() = None;
                let actual = get_node(node).await;
                eprintln!("{label}: admissions {:?}; cpu {}, ram {}, uplink {}, age {}",node.probe.admission_statuses.lock().unwrap(),actual["cpu"],actual["ram"],actual["uplink"],actual["age_ms"]);
            }
        }
        assert_eq!(http_counts, [2, 2, 2]);
        assert_eq!(secure_counts, [3, 3]);
        for (node, want) in [(&lab.cdn,5),(&alternative,5),(&third,2)] {
            *node.probe.http_snapshot.lock().unwrap() = None;
            assert_eq!(get_node(node).await["reserved"], want);
            assert!(!node.probe.bad_key.load(Ordering::SeqCst));
        }
        for node in [&lab.source, &lab.cdn, &lab.lb, &alternative, &third] { assert_eq!(node.app.media.count().await, 0); }
    }).catch_unwind().await;
    third.stop().await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn fresh_stream_bitrate_is_applied_to_cold_http_rtsp_and_rtsps_candidates() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = Node::new("cdn", 1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        alternative.app.config.put("sources", "origin", lab.cdn.app.config.snapshot()["sources"][0].clone()).unwrap();
        alternative.measured().await;
        lab.lb.app.config.put("peers", "alternate", json!({"api_url":alternative.http,"public_payload_url":alternative.http,"cluster_key":alternative.app.options.peer_key,"flussonix_rtsp_url":alternative.plain,"flussonix_rtsps_url":alternative.tls})).unwrap();
        let mut edge = get_node(&lab.cdn).await;
        let mut other = get_node(&alternative).await;
        for snapshot in [&mut edge, &mut other] {
            snapshot["cpu"] = json!(0.1); snapshot["ram"] = json!(0.1);
            snapshot["age_ms"] = json!(0); snapshot["reserved"] = json!(0);
            snapshot["reserved_mbps"] = json!(0);
        }
        // Warm, smaller edge cannot carry this stream. Its measured cost also
        // applies to the cold, larger edge, which has no local measurement yet.
        edge["uplink"] = json!(0.7); edge["uplink_mbps"] = json!(100);
        edge["ready"] = json!(["region/owned"]);
        edge["streams"] = json!([{"name":"region/owned","ready":true}]);
        edge["stream_bitrates"] = json!({"region/owned":{"mbps":20.0,"age_ms":0}});
        other["uplink"] = json!(0.8); other["uplink_mbps"] = json!(1000);
        other["ready"] = json!([]); other["streams"] = json!([]);
        other["stream_bitrates"] = json!({});
        invalidate(&lab).await;
        alternative.measured().await;
        *lab.cdn.probe.http_snapshot.lock().unwrap() = Some(edge.clone());
        *alternative.probe.http_snapshot.lock().unwrap() = Some(other.clone());
        *lab.cdn.probe.snapshot.lock().unwrap() = Some(edge);
        *alternative.probe.snapshot.lock().unwrap() = Some(other);
        let response = client().get(format!("{}/region/owned/index.m3u8?{QS}",lab.lb.http)).send().await.unwrap();
        assert_eq!(response.status(),302);
        let target = response.headers()["location"].to_str().unwrap();
        assert!(target.starts_with(&alternative.http), "HTTP chose {target}");
        for secure in [false,true] {
            let target = location(&lab.lb.describe(secure,"region/owned",QS).await);
            assert!(target.starts_with(if secure { &alternative.tls } else { &alternative.plain }), "RTSP chose {target}");
            assert!(url::Url::parse(&target).unwrap().query_pairs().any(|(k,v)|k=="token"&&v=="owned+viewer"));
            assert!(!target.contains(&alternative.app.options.peer_key));
        }
        // Bypass the advisory snapshot to inspect the actual CDN ledger.
        *alternative.probe.http_snapshot.lock().unwrap() = None;
        let ledger = get_node(&alternative).await;
        assert_eq!(ledger["reserved"],3);
        assert_eq!(ledger["reserved_mbps"],75.0);
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst),0);
        assert!(!alternative.probe.bad_key.load(Ordering::SeqCst));
        for node in [&lab.source,&lab.cdn,&lab.lb,&alternative] { assert_eq!(node.app.media.count().await,0); }
    }).catch_unwind().await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn stale_stream_bitrate_and_source_only_observations_cannot_raise_delivery_cost() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = Node::new("cdn", 1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        lab.lb.app.config.put("peers","alternate",json!({"api_url":alternative.http,"public_payload_url":alternative.http,"cluster_key":alternative.app.options.peer_key,"flussonix_rtsp_url":alternative.plain,"flussonix_rtsps_url":alternative.tls})).unwrap();
        let mut edge=get_node(&lab.cdn).await;
        edge["cpu"]=json!(0.1);edge["ram"]=json!(0.1);edge["uplink"]=json!(0.1);
        edge["age_ms"]=json!(0);edge["reserved"]=json!(0);edge["reserved_mbps"]=json!(0);edge["ready"]=json!([]);
        edge["stream_bitrates"]=json!({"region/owned":{"mbps":10000,"age_ms":3001}});
        let mut other=edge.clone();other["role"]=json!("source");
        other["stream_bitrates"]=json!({"region/owned":{"mbps":f64::MAX,"age_ms":0}});
        *lab.cdn.probe.snapshot.lock().unwrap()=Some(edge);
        *alternative.probe.http_snapshot.lock().unwrap()=Some(other.clone());
        *alternative.probe.snapshot.lock().unwrap()=Some(other);
        invalidate(&lab).await;
        // Configuration fsync can delay this single-thread fixture's metric
        // timer. Warm the real admission node after all persisted edits, then
        // install the separate advisory HTTP observation.
        lab.cdn.measured().await;
        let edge=lab.cdn.probe.snapshot.lock().unwrap().clone().unwrap();
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(edge);
        let response=client().get(format!("{}/region/owned/index.m3u8?{QS}",lab.lb.http)).send().await.unwrap();
        if response.status()!=302 {
            let status=response.status();let error=response.text().await.unwrap();
            *lab.cdn.probe.http_snapshot.lock().unwrap()=None;
            let actual=get_node(&lab.cdn).await;
            panic!("HTTP status {status}, error {error}, edge admits {}, real CDN CPU {}, RAM {}, uplink {}",lab.cdn.probe.admits.load(Ordering::SeqCst),actual["cpu"],actual["ram"],actual["uplink"]);
        }
        assert!(response.headers()["location"].to_str().unwrap().starts_with(&lab.cdn.http));
        for secure in [false,true] {
            let target=location(&lab.lb.describe(secure,"region/owned",QS).await);
            assert!(target.starts_with(if secure {&lab.cdn.tls}else{&lab.cdn.plain}));
        }
        *lab.cdn.probe.http_snapshot.lock().unwrap()=None;
        let ledger=get_node(&lab.cdn).await;
        assert_eq!(ledger["reserved"],3);assert_eq!(ledger["reserved_mbps"],6.0);
        assert_eq!(alternative.probe.admits.load(Ordering::SeqCst),0);
        for node in [&lab.source,&lab.cdn,&lab.lb,&alternative]{assert_eq!(node.app.media.count().await,0);}
    }).catch_unwind().await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn rtsp_stream_rate_expires_during_a_slow_snapshot_response() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut snapshot = get_node(&lab.cdn).await;
        snapshot["ready"] = json!([]);
        snapshot["stream_bitrates"] = json!({"region/owned":{"mbps":20,"age_ms":2980}});
        snapshot["cpu"] = json!(0.1);
        snapshot["ram"] = json!(0.1);
        snapshot["uplink"] = json!(0.1);
        snapshot["age_ms"] = json!(0);
        *lab.cdn.probe.snapshot.lock().unwrap() = Some(snapshot);
        lab.cdn.probe.routing_delay_ms.store(80, Ordering::SeqCst);
        invalidate(&lab).await;
        lab.cdn.measured().await;
        let target = location(&lab.lb.describe(false, "region/owned", QS).await);
        assert!(target.starts_with(&lab.cdn.plain));
        let ledger = get_node(&lab.cdn).await;
        assert_eq!(
            ledger["reserved_mbps"], 2.0,
            "a delayed observation must not acquire a fresh timestamp"
        );
        for node in [&lab.source, &lab.cdn, &lab.lb] {
            assert_eq!(node.app.media.count().await, 0);
        }
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn expired_bitrate_retry(rtsp: bool) {
    let mut lab = Lab::new(1000).await;
    let mut alternative = Node::new("cdn", 1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        lab.lb.app.config.put("peers","alternate",json!({"api_url":alternative.http,"public_payload_url":alternative.http,"cluster_key":alternative.app.options.peer_key,"flussonix_rtsp_url":alternative.plain})).unwrap();
        let mut small=get_node(&lab.cdn).await;
        let mut large=get_node(&alternative).await;
        for snapshot in [&mut small,&mut large] {
            snapshot["cpu"]=json!(0.1);snapshot["ram"]=json!(0.1);snapshot["age_ms"]=json!(0);
            snapshot["reserved"]=json!(0);snapshot["reserved_mbps"]=json!(0);
        }
        small["uplink"]=json!(0.1);small["uplink_mbps"]=json!(100);
        small["ready"]=json!([]);small["streams"]=json!([]);small["stream_bitrates"]=json!({});
        large["uplink"]=json!(0.2);large["uplink_mbps"]=json!(1000);
        large["ready"]=json!(["region/owned"]);large["streams"]=json!([{"name":"region/owned","ready":true}]);
        large["stream_bitrates"]=json!({"region/owned":{"mbps":100,"age_ms":1000}});
        invalidate(&lab).await;lab.cdn.measured().await;
        *lab.cdn.probe.snapshot.lock().unwrap()=Some(small.clone());
        *alternative.probe.snapshot.lock().unwrap()=Some(large.clone());
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(small);
        *alternative.probe.http_snapshot.lock().unwrap()=Some(large);
        alternative.probe.reject.store(true,Ordering::SeqCst);
        alternative.probe.admission_delay_ms.store(2400,Ordering::SeqCst);
        if rtsp {
            let target=location(&lab.lb.describe(false,"region/owned",QS).await);
            assert!(target.starts_with(&lab.cdn.plain));
        } else {
            let response=client().get(format!("{}/region/owned/index.m3u8?{QS}",lab.lb.http)).send().await.unwrap();
            assert_eq!(response.status(),302,"HTTP must reconsider the cold candidate after rate expiry");
            assert!(response.headers()["location"].to_str().unwrap().starts_with(&lab.cdn.http));
        }
        assert_eq!(alternative.probe.admits.load(Ordering::SeqCst),1,"large CDN was attempted first");
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst),1,"cold CDN must be reconsidered");
        *lab.cdn.probe.http_snapshot.lock().unwrap()=None;
        let ledger=get_node(&lab.cdn).await;
        assert_eq!(ledger["reserved_mbps"],2.0);assert_eq!(ledger["reserved"],1);
        for node in [&lab.source,&lab.cdn,&lab.lb,&alternative] {assert_eq!(node.app.media.count().await,0);}
    }).catch_unwind().await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
#[tokio::test]
async fn http_failed_admission_rechecks_stream_bitrate_and_reconsiders_cold_capacity() {
    expired_bitrate_retry(false).await;
}
#[tokio::test]
async fn rtsp_failed_admission_rechecks_stream_bitrate_and_reconsiders_cold_capacity() {
    expired_bitrate_retry(true).await;
}

// Exercise real authenticated placement/admission with controlled advisory telemetry.
#[tokio::test]
async fn adaptive_http_and_rtsp_routing_obeys_resource_pressure_and_reserved_mbps() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = Node::new("cdn", 1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        alternative.app.config.put("sources", "origin", lab.cdn.app.config.snapshot()["sources"][0].clone()).unwrap();
        alternative.measured().await;
        lab.lb.app.config.put("peers", "alternate", json!({"api_url":alternative.http,"public_payload_url":alternative.http,"cluster_key":alternative.app.options.peer_key,"flussonix_rtsp_url":alternative.plain})).unwrap();
        let edge_base = get_node(&lab.cdn).await;
        let alternate_base = get_node(&alternative).await;
        for resource in ["cpu", "ram", "reserved"] {
            let mut edge = edge_base.clone();
            let mut other = alternate_base.clone();
            for snapshot in [&mut edge, &mut other] {
                snapshot["cpu"] = json!(0.1); snapshot["ram"] = json!(0.1);
                snapshot["uplink"] = json!(0.1); snapshot["age_ms"] = json!(0);
                snapshot["reserved"] = json!(0); snapshot["reserved_mbps"] = json!(0);
                snapshot["ready"] = json!([]); snapshot["streams"] = json!([]);
            }
            match resource {
                "cpu" => { edge["cpu"] = json!(0.85); other["uplink"] = json!(0.4); },
                "ram" => { edge["ram"] = json!(0.9); other["uplink"] = json!(0.4); },
                _ => {
                    edge["uplink"] = json!(0.2); edge["uplink_mbps"] = json!(100);
                    edge["reserved"] = json!(1); edge["reserved_mbps"] = json!(50);
                    other["uplink"] = json!(0.3); other["uplink_mbps"] = json!(1000);
                }
            }
            *lab.cdn.probe.http_snapshot.lock().unwrap() = Some(edge.clone());
            *alternative.probe.http_snapshot.lock().unwrap() = Some(other.clone());
            *lab.cdn.probe.snapshot.lock().unwrap() = Some(edge);
            *alternative.probe.snapshot.lock().unwrap() = Some(other);
            invalidate(&lab).await;
            let before = lab.cdn.probe.admits.load(Ordering::SeqCst);
            let http = client().get(format!("{}/region/owned/index.m3u8?{QS}", lab.lb.http)).send().await.unwrap();
            assert_eq!(http.status(), 302, "HTTP {resource}");
            let target = http.headers()["location"].to_str().unwrap();
            assert!(target.starts_with(&alternative.http), "HTTP {resource} chose {target}");
            assert!(url::Url::parse(target).unwrap().query_pairs().any(|(k,v)| k=="token" && v=="owned+viewer"));
            let target = location(&lab.lb.describe(false, "region/owned", QS).await);
            assert!(target.starts_with(&alternative.plain), "RTSP {resource} chose {target}");
            assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), before);
            assert!(!target.contains(&alternative.app.options.peer_key));
        }
        assert_eq!(alternative.probe.admits.load(Ordering::SeqCst), 6);
        assert!(!alternative.probe.bad_key.load(Ordering::SeqCst));
        for node in [&lab.source, &lab.cdn, &lab.lb, &alternative] { assert_eq!(node.app.media.count().await, 0); }
    }).catch_unwind().await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}

// Missing/invalid native telemetry cannot be interpreted as zero pending cost.
#[tokio::test]
async fn http_routing_rejects_invalid_or_incomplete_capacity_telemetry() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut base = get_node(&lab.cdn).await;
        base["uplink"] = json!(0.1);
        base["cpu"] = json!(0.1);
        base["ram"] = json!(0.1);
        base["reserved"] = json!(1);
        base["reserved_mbps"] = json!(20);
        base["age_ms"] = json!(0);
        for (field, value) in [
            ("reserved_mbps", Value::Null),
            ("reserved_mbps", json!(-1)),
            ("uplink_mbps", json!(0)),
            ("uplink_mbps", json!(-100)),
            ("uplink", json!(-0.01)),
            ("cpu", json!(-0.1)),
            ("ram", json!(-0.1)),
            ("reserved", Value::Null),
            ("active", json!(u64::MAX)),
            ("role", json!("source")),
            ("role", json!("lb")),
            ("age_ms", json!(10001)),
        ] {
            let mut snapshot = base.clone();
            snapshot[field] = value;
            *lab.cdn.probe.http_snapshot.lock().unwrap() = Some(snapshot);
            invalidate(&lab).await;
            let before = lab.cdn.probe.admits.load(Ordering::SeqCst);
            let reply = client()
                .get(format!("{}/region/owned/index.m3u8?{QS}", lab.lb.http))
                .send()
                .await
                .unwrap();
            assert_eq!(reply.status(), 503, "accepted {field}");
            assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), before);
        }
        assert_eq!(lab.cdn.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}
#[tokio::test]
async fn routing_excludes_stale_unsafe_incompatible_and_saturated_nodes() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        let mut base = get_node(&lab.cdn).await;
        base["ready"] = json!([]);
        for (field, value) in [
            ("age_ms", json!(10001)),
            ("uplink", json!(0.9)),
            ("uplink", json!(-0.1)),
            ("cpu", json!(0.9)),
            ("ram", json!(0.95)),
            ("drain", json!(true)),
            ("active", json!(1000)),
            ("active", json!(u64::MAX)),
            ("uplink_mbps", json!(0)),
            ("reserved_mbps", json!(-1)),
            ("role", json!("source")),
            ("cpu", Value::Null),
            ("ready", json!([1])),
            ("rtsp_publication", json!({})),
        ] {
            let mut v = base.clone();
            v[field] = value;
            *lab.cdn.probe.snapshot.lock().unwrap() = Some(v);
            invalidate(&lab).await;
            let before = lab.cdn.probe.admits.load(Ordering::SeqCst);
            assert_eq!(
                lab.lb.describe(false, "region/owned", QS).await.0,
                503,
                "accepted {field}"
            );
            assert_eq!(
                lab.cdn.probe.admits.load(Ordering::SeqCst),
                before,
                "reserved {field}"
            );
        }
        *lab.cdn.probe.snapshot.lock().unwrap() = Some(base);
        lab.lb
            .app
            .config
            .put("peers", "edge", json!({"flussonix_rtsp_url":lab.lb.plain}))
            .unwrap();
        let before = lab.cdn.probe.polls.load(Ordering::SeqCst);
        assert_eq!(lab.lb.describe(false, "region/owned", QS).await.0, 503);
        assert_eq!(
            lab.cdn.probe.polls.load(Ordering::SeqCst),
            before,
            "self endpoint polled"
        );
        for i in 0..64 {
            lab.lb
                .app
                .config
                .put(
                    "peers",
                    &format!("extra-{i}"),
                    json!({"api_url":lab.cdn.http,"flussonix_rtsp_url":lab.cdn.plain}),
                )
                .unwrap();
        }
        assert_eq!(lab.lb.describe(false, "region/owned", QS).await.0, 503);
        assert_eq!(
            lab.cdn.probe.polls.load(Ordering::SeqCst),
            before,
            "oversized pool polled"
        );
        assert_eq!(lab.lb.app.playback_auth.live_grants(), 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn snapshot_refresh_fails_closed_and_config_invalidates_cached_capabilities() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        let response = lab.lb.describe(false, "region/owned", QS).await;
        let actual = get_actual_node(&lab.cdn).await;
        assert_eq!(
            response.0,
            302,
            "initial capacity: CPU {}, age {}; admissions {:?}",
            actual["cpu"],
            actual["age_ms"],
            lab.cdn.probe.admission_statuses.lock().unwrap()
        );
        location(&response);
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), 1);
        location(&lab.lb.describe(false, "region/owned", QS).await);
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), 1);
        *lab.cdn.probe.snapshot.lock().unwrap() = Some(json!({"ready":null}));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(lab.lb.describe(false, "region/owned", QS).await.0, 503);
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), 2);
        assert_eq!(lab.lb.describe(false, "region/owned", QS).await.0, 503);
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), 3);
        *lab.cdn.probe.snapshot.lock().unwrap() = None;
        lab.cdn.measured().await;
        let primed_at = std::time::Instant::now();
        location(&lab.lb.describe(false, "region/owned", QS).await);
        let before = lab.cdn.probe.polls.load(Ordering::SeqCst);
        invalidate(&lab).await;
        let response = lab.lb.describe(false, "region/owned", QS).await;
        assert!(
            primed_at.elapsed() < Duration::from_secs(1),
            "invalidation assertion must use a still-fresh cached observation"
        );
        let actual = get_node(&lab.cdn).await;
        assert_eq!(
            response.0,
            302,
            "after config: cpu {}, ram {}, uplink {}, age {}; admissions {:?}",
            actual["cpu"],
            actual["ram"],
            actual["uplink"],
            actual["age_ms"],
            lab.cdn.probe.admission_statuses.lock().unwrap()
        );
        location(&response);
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), before + 1);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn failed_admission_tries_one_alternate_without_starting_media() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = Node::new("cdn", 1000).await;
    let result=std::panic::AssertUnwindSafe(async {
        alternative.app.config.put("sources","origin",lab.cdn.app.config.snapshot()["sources"][0].clone()).unwrap();
        alternative.measured().await;
        lab.lb.app.config.put("peers","alternate",json!({"api_url":alternative.http,"cluster_key":alternative.app.options.peer_key,"flussonix_rtsp_url":alternative.plain})).unwrap();
        let mut n=get_node(&lab.cdn).await;n["ready"]=json!(["region/owned"]);n["cpu"]=json!(0.0);n["ram"]=json!(0.0);n["uplink"]=json!(0.0);*lab.cdn.probe.snapshot.lock().unwrap()=Some(n);
        let mut n=get_node(&alternative).await;n["ready"]=json!([]);n["cpu"]=json!(0.8);n["ram"]=json!(0.8);*alternative.probe.snapshot.lock().unwrap()=Some(n);
        lab.cdn.probe.reject.store(true,Ordering::SeqCst);
        assert!(location(&lab.lb.describe(false,"region/owned",QS).await).starts_with(&alternative.plain));
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst),1);assert_eq!(alternative.probe.admits.load(Ordering::SeqCst),1);
        assert_eq!(alternative.app.media.count().await,0);assert!(!alternative.probe.bad_key.load(Ordering::SeqCst));
    }).catch_unwind().await;
    lab.stop().await;
    alternative.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn configuration_and_revocation_fence_pending_placement() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        for revoked in [false, true] {
            lab.cdn.probe.pause.store(true, Ordering::SeqCst);
            let a = lab.lb.app.clone();
            let endpoint = lab.lb.plain.clone();
            let request_task = AbortOnDropHandle::new(tokio::spawn(async move {
                let u = url::Url::parse(&endpoint).unwrap();
                let mut s = BufReader::new(Box::new(
                    TcpStream::connect(u.socket_addrs(|| Some(554)).unwrap()[0])
                        .await
                        .unwrap(),
                ) as Box<dyn Io>);
                request(
                    &mut s,
                    "DESCRIBE",
                    &format!("{endpoint}/region/owned?{QS}"),
                    "",
                )
                .await
            }));
            tokio::time::timeout(Duration::from_secs(4), lab.cdn.probe.entered.notified())
                .await
                .unwrap();
            if revoked {
                let id = a.playback_auth.snapshots()[0]["id"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                assert!(a.playback_auth.revoke(&id));
            } else {
                invalidate(&lab).await;
            }
            lab.cdn.probe.release.notify_one();
            assert_eq!(
                request_task.await.unwrap().0,
                if revoked { 403 } else { 503 }
            );
            assert_eq!(lab.lb.app.media.count().await, 0);
            assert_eq!(lab.lb.app.playback_auth.live_grants(), 0);
        }
    })
    .catch_unwind()
    .await;
    lab.cdn.probe.release.notify_waiters();
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn independent_ffmpeg_follows_native_redirect_and_reuses_private_m4s_pull() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        for _ in 0..2 {
            let output = tokio::time::timeout(
                Duration::from_secs(30),
                tokio::process::Command::new("ffmpeg")
                    .args([
                        "-nostdin",
                        "-v",
                        "error",
                        "-rtsp_transport",
                        "tcp",
                        "-i",
                        &format!("{}/region/owned?{QS}", lab.lb.plain),
                        "-t",
                        "2",
                        "-map",
                        "0:v:0",
                        "-map",
                        "0:a:0",
                        "-threads",
                        "1",
                        "-f",
                        "framemd5",
                        "-",
                    ])
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(
                output.status.success() && output.stderr.is_empty(),
                "strict decode: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let media = String::from_utf8_lossy(&output.stdout);
            assert!(
                media.contains("#media_type 0: video") && media.contains("#media_type 1: audio")
            );
            assert!(media.lines().filter(|l| l.starts_with("0,")).count() >= 20);
            assert!(media.lines().filter(|l| l.starts_with("1,")).count() >= 40);
            assert_eq!(lab.lb.app.media.count().await, 0);
            assert_eq!(lab.cdn.app.media.count().await, 1);
            assert_eq!(lab.source.app.media.count().await, 1);
        }
        assert_eq!(
            lab.source.probe.pulls.load(Ordering::SeqCst),
            1,
            "one shared LAN M4S pull"
        );
        let snapshot = client()
            .get(format!("{}/flussonix/api/v1/rtsp-routing", lab.cdn.http))
            .header("X-Flussonix-Peer", &lab.cdn.app.options.peer_key)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(snapshot["ready"], json!(["region/owned"]));
        assert!(snapshot.get("streams").is_none());
        assert!(!snapshot.to_string().contains(&lab.cdn.app.options.peer_key));
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn callback_redirect_precedes_placement_and_internal_ticket_stays_out_of_callback() {
    let mut lab = Lab::new(1000).await;
    let result = std::panic::AssertUnwindSafe(async {
        let mut cfg = lab.source.app.config.snapshot()["streams"][0].clone();
        cfg["on_play"] = json!(format!("{}/owned-auth", lab.cdn.http));
        lab.source
            .app
            .config
            .put("streams", "region/owned", cfg.clone())
            .unwrap();
        cfg["on_play"] = json!(format!("{}/owned-auth", lab.lb.http));
        lab.lb
            .app
            .config
            .put("streams", "region/owned", cfg)
            .unwrap();
        let external = format!("{}/region/owned?{QS}", lab.cdn.plain);
        *lab.lb.probe.auth_target.lock().unwrap() = Some(external.clone());
        assert_eq!(
            location(&lab.lb.describe(false, "region/owned", QS).await),
            external
        );
        assert_eq!(lab.cdn.probe.polls.load(Ordering::SeqCst), 0);
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 0);
        lab.lb.app.config.delete("streams", "region/owned").unwrap();
        let target = location(&lab.lb.describe(false, "region/owned", QS).await);
        let mut s = lab.cdn.socket(false).await;
        assert_eq!(request(&mut s, "DESCRIBE", &target, "").await.0, 200);
        let queries = lab.cdn.probe.auth_queries.lock().unwrap().clone();
        assert_eq!(queries.len(), 2);
        for q in queries.iter() {
            assert_eq!(q["qs"], QS);
            assert_eq!(q["token"], "owned+viewer");
            assert!(!q["qs"].contains("flussonix_ticket"));
        }
        assert_eq!(lab.lb.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn available_cdn_survives_partial_snapshot_timeout_in_supported_pool() {
    let mut lab = Lab::new(1000).await;
    let mut slow = Node::new("cdn", 1000).await;
    let result=std::panic::AssertUnwindSafe(async {
        slow.probe.stall.store(true,Ordering::SeqCst);
        let edge=lab.lb.app.config.snapshot()["peers"][0].clone();
        lab.lb.app.config.delete("peers","edge").unwrap();
        for i in 0..63 {lab.lb.app.config.put("peers",&format!("slow-{i}"),json!({"api_url":slow.http,"cluster_key":slow.app.options.peer_key,"flussonix_rtsp_url":slow.plain})).unwrap();}
        lab.lb.app.config.put("peers","edge",edge).unwrap();
        assert_eq!(lab.lb.app.config.snapshot()["peers"].as_array().unwrap().last().unwrap()["hostname"],"edge");
        let started=std::time::Instant::now();
        let target=location(&lab.lb.describe(false,"region/owned",QS).await);
        assert!(target.starts_with(&lab.cdn.plain));assert!(started.elapsed()<Duration::from_secs(8));
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst),1);assert_eq!(slow.probe.admits.load(Ordering::SeqCst),0);
        assert_eq!(lab.lb.app.media.count().await,0);
    }).catch_unwind().await;
    slow.probe.release.notify_waiters();
    slow.stop().await;
    lab.stop().await;
    if let Err(p) = result {
        std::panic::resume_unwind(p)
    }
}

// The cache must be per peer, never per viewer or stream. Real admission remains
// authoritative even when concurrent requests share one advisory observation.
#[tokio::test]
async fn http_snapshot_coalesces_plain_and_verified_https_viewers_with_real_admission() {
    let mut lab = Lab::new(1).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let cdn_tls = lab.cdn.secure_media().await;
        let lb_tls = lab.lb.secure_delivery().await;
        lab.lb.app.config.put("peers", "edge", json!({"api_url":cdn_tls,"public_payload_url":cdn_tls,"flussonix_tls_ca":lab.cdn.cert.ca})).unwrap();
        lab.cdn.measured().await;
        lab.cdn.probe.http_delay_ms.store(100, Ordering::SeqCst);
        let https = reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(12)).use_preconfigured_tls((*lab.lb.cert.client()).clone()).build().unwrap();
        let plain = client();
        let replies = futures_util::future::join_all((0..8).map(|i| {
            let base = if i % 2 == 0 { &lab.lb.http } else { &lb_tls };
            let c = if i % 2 == 0 { &plain } else { &https };
            c.get(format!("{base}/region/owned/index.m3u8?{QS}")).send()
        })).await.into_iter().map(Result::unwrap).collect::<Vec<_>>();
        assert_eq!(replies.iter().filter(|r| r.status()==302).count(),1);
        assert_eq!(replies.iter().filter(|r| r.status()==503).count(),7);
        let destination = replies.iter().find(|r| r.status()==302).unwrap().headers()["location"].to_str().unwrap();
        assert!(destination.starts_with(&cdn_tls));
        let parsed=url::Url::parse(destination).unwrap();let query=parsed.query_pairs().collect::<std::collections::HashMap<_,_>>();
        assert_eq!(query.get("token").map(|v|v.as_ref()),Some("owned+viewer"));
        assert!(uuid::Uuid::parse_str(query["flussonix_ticket"].as_ref()).is_ok());
        assert_eq!(lab.cdn.probe.http_polls.load(Ordering::SeqCst),1,"HTTP/HTTPS placements must share one node observation");
        assert!(!lab.cdn.probe.bad_key.load(Ordering::SeqCst));
        let actual=get_actual_node(&lab.cdn).await;assert_eq!(actual["reserved"],1);assert_eq!(actual["reserved_mbps"],2.0);
        for node in [&lab.lb,&lab.cdn,&lab.source] { assert_eq!(node.app.media.count().await,0); }
    }).catch_unwind().await;
    lab.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn http_placement(lab: &Lab) -> reqwest::Response {
    client()
        .get(format!("{}/region/owned/index.m3u8?{QS}", lab.lb.http))
        .send()
        .await
        .unwrap()
}
async fn healthy_http_snapshot(node: &Node) -> Value {
    let mut v = get_actual_node(node).await;
    v["uplink"] = json!(0.1);
    v["cpu"] = json!(0.1);
    v["ram"] = json!(0.1);
    v["age_ms"] = json!(0);
    v["uplink_mbps"] = json!(1000);
    v["active"] = json!(0);
    v["reserved"] = json!(0);
    v["reserved_mbps"] = json!(0);
    v["streams"] = json!([]);
    v["stream_bitrates"] = json!({});
    v
}

#[tokio::test]
async fn http_snapshot_is_reused_until_expiry_and_failed_refresh_never_reuses_it() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        *lab.cdn.probe.http_snapshot.lock().unwrap() = Some(healthy_http_snapshot(&lab.cdn).await);
        assert_eq!(http_placement(&lab).await.status(), 302);
        *lab.cdn.probe.http_reply.lock().unwrap() = Some((StatusCode::SERVICE_UNAVAILABLE, vec![]));
        assert_eq!(
            http_placement(&lab).await.status(),
            302,
            "fresh observation is reusable despite a later unavailable probe"
        );
        assert_eq!(lab.cdn.probe.http_polls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(http_placement(&lab).await.status(), 503);
        assert_eq!(http_placement(&lab).await.status(), 503);
        assert_eq!(
            lab.cdn.probe.http_polls.load(Ordering::SeqCst),
            3,
            "failed refresh must not republish old telemetry"
        );
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 2);
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"], 2);
        assert_eq!(lab.cdn.app.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_configuration_change_discards_a_delayed_peer_reply() {
    let mut lab = Lab::new(1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(healthy_http_snapshot(&lab.cdn).await);
        lab.cdn.probe.http_pause.store(true,Ordering::SeqCst);
        let pending=http_placement(&lab);tokio::pin!(pending);
        tokio::select! { _=lab.cdn.probe.http_entered.notified()=>{}, _=&mut pending=>panic!("placement completed before the probe") }
        lab.lb.app.config.put("peers","edge",json!({"drain":true})).unwrap();
        lab.cdn.probe.http_pause.store(false,Ordering::SeqCst);lab.cdn.probe.http_release.notify_waiters();
        assert_eq!(pending.await.status(),503,"obsolete reply cannot admit or redirect");
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst),0);
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"],0);
        assert_eq!(http_placement(&lab).await.status(),503);
    }).catch_unwind().await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_configuration_change_blocks_a_late_admission_redirect() {
    let mut lab = Lab::new(1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(healthy_http_snapshot(&lab.cdn).await);
        lab.cdn.probe.pause.store(true,Ordering::SeqCst);
        let pending=http_placement(&lab);tokio::pin!(pending);
        tokio::select! { _=lab.cdn.probe.entered.notified()=>{}, _=&mut pending=>panic!("placement completed before admission") }
        lab.lb.app.config.put("peers","edge",json!({"public_payload_url":"http://other.invalid"})).unwrap();
        lab.cdn.probe.pause.store(false,Ordering::SeqCst);lab.cdn.probe.release.notify_waiters();
        assert_eq!(pending.await.status(),503,"a late ticket must not publish an obsolete public endpoint");
        // The unused reservation expires normally; no worker or access is granted.
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"],1);
        assert_eq!(lab.cdn.app.media.count().await,0);
    }).catch_unwind().await;
    lab.cdn.probe.pause.store(false, Ordering::SeqCst);
    lab.cdn.probe.release.notify_waiters();
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_rejects_an_oversized_valid_json_observation() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut snapshot = healthy_http_snapshot(&lab.cdn).await;
        snapshot["padding"] = json!("x".repeat(2 * 1024 * 1024));
        *lab.cdn.probe.http_reply.lock().unwrap() =
            Some((StatusCode::OK, serde_json::to_vec(&snapshot).unwrap()));
        assert_eq!(
            http_placement(&lab).await.status(),
            503,
            "oversized JSON must not create a reservation"
        );
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 0);
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"], 0);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_cache_age_expires_resource_eligibility() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut snapshot = healthy_http_snapshot(&lab.cdn).await;
        snapshot["age_ms"] = json!(9750);
        *lab.cdn.probe.http_snapshot.lock().unwrap() = Some(snapshot);
        assert_eq!(http_placement(&lab).await.status(), 302);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            http_placement(&lab).await.status(),
            503,
            "cache reuse must include original observation age"
        );
        assert_eq!(lab.cdn.probe.http_polls.load(Ordering::SeqCst), 1);
        assert_eq!(lab.cdn.probe.admits.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn http_snapshot_cache_age_expires_stream_bitrate_without_refreshing_it() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut snapshot = healthy_http_snapshot(&lab.cdn).await;
        snapshot["stream_bitrates"] = json!({"region/owned":{"mbps":20,"age_ms":2600}});
        *lab.cdn.probe.http_snapshot.lock().unwrap() = Some(snapshot);
        assert_eq!(http_placement(&lab).await.status(), 302);
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(http_placement(&lab).await.status(), 302);
        let actual = get_actual_node(&lab.cdn).await;
        assert_eq!(actual["reserved"], 2);
        assert_eq!(
            actual["reserved_mbps"], 27.0,
            "25Mbps fresh estimate plus2Mbps expired fallback"
        );
        assert_eq!(lab.cdn.probe.http_polls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn http_snapshot_cached_readiness_is_resolved_for_each_stream() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = tied_delivery_peer(&lab).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        lab.source.app.config.put("streams","region/second",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned+viewer"))})).unwrap();
        lab.cdn.measured().await;alternative.measured().await;
        let mut first=healthy_http_snapshot(&lab.cdn).await;let mut second=healthy_http_snapshot(&alternative).await;
        first["streams"]=json!([{"name":"region/owned","ready":true}]);second["streams"]=json!([{"name":"region/second","ready":true}]);
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(first);*alternative.probe.http_snapshot.lock().unwrap()=Some(second);
        lab.cdn.probe.http_polls.store(0,Ordering::SeqCst);alternative.probe.http_polls.store(0,Ordering::SeqCst);
        let one=http_placement(&lab).await;assert_eq!(one.status(),302);assert!(one.headers()["location"].to_str().unwrap().starts_with(&lab.cdn.http));
        let two=client().get(format!("{}/region/second/index.m3u8?{QS}",lab.lb.http)).send().await.unwrap();assert_eq!(two.status(),302);assert!(two.headers()["location"].to_str().unwrap().starts_with(&alternative.http));
        assert_eq!(lab.cdn.probe.http_polls.load(Ordering::SeqCst),1);assert_eq!(alternative.probe.http_polls.load(Ordering::SeqCst),1);
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"],1);assert_eq!(get_actual_node(&alternative).await["reserved"],1);
        for n in [&lab.lb,&lab.cdn,&lab.source,&alternative] {assert_eq!(n.app.media.count().await,0);}
    }).catch_unwind().await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn http_snapshot_revocation_cancels_an_inflight_admission_without_redirect() {
    let mut lab = Lab::new(1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(healthy_http_snapshot(&lab.cdn).await);
        lab.cdn.probe.pause.store(true,Ordering::SeqCst);
        let pending=http_placement(&lab);tokio::pin!(pending);
        tokio::select! { _=lab.cdn.probe.entered.notified()=>{}, _=&mut pending=>panic!("placement completed before admission") }
        lab.lb.app.playback_auth.invalidate(|_|None);
        lab.cdn.probe.pause.store(false,Ordering::SeqCst);lab.cdn.probe.release.notify_waiters();
        assert_eq!(pending.await.status(),403,"revoked grant cannot use a delayed admission reply");
        assert_eq!(lab.cdn.app.media.count().await,0);assert_eq!(lab.lb.app.media.count().await,0);
    }).catch_unwind().await;
    lab.cdn.probe.pause.store(false, Ordering::SeqCst);
    lab.cdn.probe.release.notify_waiters();
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_probe_budget_is_shared_by_concurrent_http_and_https_placements() {
    let mut lab = Lab::new(1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        let lb_tls=lab.lb.secure_delivery().await;
        lab.lb.app.config.delete("peers","edge").unwrap();
        for i in 0..64 {
            lab.lb.app.config.put("peers",&format!("peer{i:02}"),json!({"api_url":lab.cdn.http,"public_payload_url":if i<32 {lab.cdn.http.clone()} else {"https://delivery.invalid".to_owned()},"cluster_key":lab.cdn.app.options.peer_key})).unwrap();
        }
        lab.cdn.measured().await;
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(healthy_http_snapshot(&lab.cdn).await);
        lab.cdn.probe.http_delay_ms.store(120,Ordering::SeqCst);
        let tls=reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(12)).use_preconfigured_tls((*lab.lb.cert.client()).clone()).build().unwrap();
        let (plain,secure)=tokio::join!(http_placement(&lab),tls.get(format!("{lb_tls}/region/owned/index.m3u8?{QS}")).send());
        assert_eq!(plain.status(),302);assert_eq!(secure.unwrap().status(),302);
        assert!(lab.cdn.probe.http_peak.load(Ordering::SeqCst)<=8,"node-wide HTTP probe budget exceeded: {}",lab.cdn.probe.http_peak.load(Ordering::SeqCst));
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"],2);
        assert_eq!(lab.cdn.app.media.count().await,0);
    }).catch_unwind().await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_rejects_oversized_streamed_json_without_content_length() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut snapshot = healthy_http_snapshot(&lab.cdn).await;
        snapshot["padding"] = json!("x".repeat(2 * 1024 * 1024));
        lab.cdn.probe.http_chunked.store(true, Ordering::SeqCst);
        *lab.cdn.probe.http_reply.lock().unwrap() =
            Some((StatusCode::OK, serde_json::to_vec(&snapshot).unwrap()));
        let direct = client()
            .get(format!("{}/flussonix/api/v1/node", lab.cdn.http))
            .send()
            .await
            .unwrap();
        assert!(direct.content_length().is_none());
        drop(direct);
        assert_eq!(http_placement(&lab).await.status(), 503);
        assert_eq!(
            lab.cdn.probe.admits.load(Ordering::SeqCst),
            0,
            "streamed oversized telemetry cannot be admitted"
        );
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn http_snapshot_slow_peer_does_not_delay_a_healthy_candidate_past_probe_deadline() {
    let mut lab = Lab::new(1000).await;
    let mut alternative = tied_delivery_peer(&lab).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        alternative.probe.http_pause.store(true, Ordering::SeqCst);
        let response = tokio::time::timeout(Duration::from_millis(1500), http_placement(&lab))
            .await
            .expect("healthy candidate must remain reachable within the bounded probe phase");
        assert_eq!(response.status(), 302);
        assert!(
            response.headers()["location"]
                .to_str()
                .unwrap()
                .starts_with(&lab.cdn.http)
        );
        assert_eq!(alternative.probe.admits.load(Ordering::SeqCst), 0);
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"], 1);
    })
    .catch_unwind()
    .await;
    alternative.stop().await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}
#[tokio::test]
async fn http_snapshot_placement_has_an_overall_deadline_across_admission_retries() {
    let mut lab = Lab::new(1000).await;
    let outcome=std::panic::AssertUnwindSafe(async {
        for i in 0..3 {lab.lb.app.config.put("peers",&format!("other{i}"),json!({"api_url":lab.cdn.http,"public_payload_url":lab.cdn.http,"cluster_key":lab.cdn.app.options.peer_key})).unwrap();}
        *lab.cdn.probe.http_snapshot.lock().unwrap()=Some(healthy_http_snapshot(&lab.cdn).await);
        lab.cdn.probe.reject.store(true,Ordering::SeqCst);lab.cdn.probe.admission_delay_ms.store(2900,Ordering::SeqCst);
        let response=tokio::time::timeout(Duration::from_secs(9),http_placement(&lab)).await.expect("placement must finish within the shared overall budget");
        assert_eq!(response.status(),503);assert_eq!(get_actual_node(&lab.cdn).await["reserved"],0);
        assert_eq!(lab.cdn.app.media.count().await,0);
    }).catch_unwind().await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}

#[tokio::test]
async fn http_snapshot_fresh_cache_is_invalidated_before_using_a_changed_peer_key() {
    let mut lab = Lab::new(1000).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        lab.cdn.measured().await;
        let primed_at = std::time::Instant::now();
        assert_eq!(http_placement(&lab).await.status(), 302);
        assert_eq!(lab.cdn.probe.http_polls.load(Ordering::SeqCst), 1);
        lab.lb
            .app
            .config
            .put(
                "peers",
                "edge",
                json!({"cluster_key":"changed-invalid-peer-key"}),
            )
            .unwrap();
        assert_eq!(http_placement(&lab).await.status(), 503);
        assert!(
            primed_at.elapsed() < Duration::from_secs(1),
            "freshness must be established independently of invalidation"
        );
        assert_eq!(
            lab.cdn.probe.http_polls.load(Ordering::SeqCst),
            2,
            "saved key must force another authenticated observation"
        );
        assert_eq!(
            lab.cdn.probe.admits.load(Ordering::SeqCst),
            1,
            "a failed refreshed observation cannot attempt admission"
        );
        assert_eq!(get_actual_node(&lab.cdn).await["reserved"], 1);
    })
    .catch_unwind()
    .await;
    lab.stop().await;
    if let Err(p) = outcome {
        std::panic::resume_unwind(p)
    }
}
