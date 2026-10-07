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
    reject: AtomicBool,
    pause: AtomicBool,
    stall: AtomicBool,
    entered: Notify,
    release: Notify,
    key: String,
    auth_queries: Mutex<Vec<std::collections::HashMap<String, String>>>,
    auth_target: Mutex<Option<String>>,
}
async fn intercept(State(p): State<Arc<Probe>>, r: Request<Body>, next: Next) -> Response {
    let path = r.uri().path();
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
            if p.stall.load(Ordering::SeqCst) {
                p.release.notified().await;
            }
            if let Some(v) = p.snapshot.lock().unwrap().clone() {
                return axum::Json(v).into_response();
            }
        } else {
            p.admits.fetch_add(1, Ordering::SeqCst);
            if p.pause.load(Ordering::SeqCst) {
                p.entered.notify_one();
                p.release.notified().await;
            }
            if p.reject.load(Ordering::SeqCst) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        }
    }
    next.run(r).await
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
            let v = get_node(self).await;
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
        location(&lab.lb.describe(false, "region/owned", QS).await);
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
        location(&lab.lb.describe(false, "region/owned", QS).await);
        let before = lab.cdn.probe.polls.load(Ordering::SeqCst);
        invalidate(&lab).await;
        location(&lab.lb.describe(false, "region/owned", QS).await);
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
