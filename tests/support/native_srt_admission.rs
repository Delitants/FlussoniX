//! Owned native CDN reservations and direct encrypted SRT callers.
use super::{app, vendor_srt_lookup::Https};
use flussonix::{
    server::{App, Options, router},
    srt_playback::{self, Listener, Settings},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
const KEY: &str = "owned-native-peer-key";
struct Native {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    url: String,
    key: String,
    address: String,
    stop: CancellationToken,
    metrics: AbortOnDropHandle<()>,
    http: AbortOnDropHandle<()>,
    srt: AbortOnDropHandle<std::io::Result<()>>,
}
impl Native {
    async fn new(limit: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: "cdn".into(),
                node_name: "owned-native-cdn".into(),
                peer_key: KEY.into(),
                admin_password: "owned-admin".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put(
                "streams",
                "local/news",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        Self::start_with(app, dir, limit, KEY).await
    }
    async fn start_with(app: Arc<App>, dir: tempfile::TempDir, limit: usize, key: &str) -> Self {
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            Settings::new(120, limit, "owned-native-passphrase".into()).unwrap(),
        )
        .unwrap();
        let address = format!("srt://{}", listener.address());
        let stop = CancellationToken::new();
        tokio::time::sleep(Duration::from_millis(100)).await;
        app.sample_metrics();
        let metrics_app = app.clone();
        let metrics_stop = stop.clone();
        let metrics = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                tokio::select! {_=metrics_stop.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(100))=>metrics_app.sample_metrics()}
            }
        }));
        let srt = AbortOnDropHandle::new(tokio::spawn(srt_playback::serve(
            listener,
            app.clone(),
            stop.clone(),
        )));
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        let a = app.clone();
        let c = stop.clone();
        let http = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(
                tcp,
                router(a).into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(c.cancelled_owned())
            .await
            .unwrap()
        }));
        let value = Self {
            _dir: dir,
            app,
            url,
            key: key.into(),
            address,
            stop,
            metrics,
            http,
            srt,
        };
        for _ in 0..50 {
            if value.node().await["srt_playback"]["enabled"] == true {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("owned SRT listener did not start")
    }
    fn peer(&self) -> Value {
        json!({"api_url":self.url,"flussonix_srt_url":self.address,"flussonix_srt_encrypted":true,"cluster_key":self.key})
    }
    async fn node(&self) -> Value {
        reqwest::Client::new()
            .get(format!("{}/flussonix/api/v1/node", self.url))
            .header("x-flussonix-peer", &self.key)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn admit(&self, token: &str) -> reqwest::Response {
        use sha2::{Digest, Sha256};
        reqwest::Client::new().post(format!("{}/flussonix/api/v1/admit",self.url)).header("x-flussonix-peer",&self.key).json(&json!({"name":"local/news","protocol":"srt","encrypted":true,"token_hash":format!("{:x}",Sha256::digest(token.as_bytes()))})).send().await.unwrap()
    }
    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(6), self.srt)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.app.media.stop_all().await;
        self.metrics.await.unwrap();
        tokio::time::timeout(Duration::from_secs(6), self.http)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn native_lookup_returns_an_opaque_token_bound_ticket_without_lb_media() {
    let cdn = Native::new(4).await;
    let (_d, lb) = app();
    lb.config.put("peers", "cdn", cdn.peer()).unwrap();
    let https = Https::new(lb.clone()).await;
    let r = https
        .lookup("#!::u=one%2B+é,r=local/news,a=opaque", None)
        .await;
    assert_eq!(r.status(), 200);
    let result: Value = r.json().await.unwrap();
    assert_eq!(result["address"], cdn.address);
    assert_eq!(result["encryption_required"], true);
    let id = result["streamid"].as_str().unwrap();
    assert!(id.starts_with("#!::u=one%2B+é,r=local/news,a=opaque,m=request,flussonix_ticket="));
    assert!(uuid::Uuid::parse_str(id.split_once("flussonix_ticket=").unwrap().1).is_ok());
    assert!(!result.to_string().contains(KEY));
    assert!(!result.to_string().contains("owned-native-passphrase"));
    assert_eq!(cdn.node().await["reserved"], 1);
    assert_eq!(cdn.app.media.count().await, 0);
    assert_eq!(lb.media.count().await, 0);
    https.close().await;
    cdn.close().await;
}
#[tokio::test]
async fn unredeemed_native_lookups_do_not_occupy_lb_viewer_capacity() {
    let cdn = Native::new(4).await;
    let dir = tempfile::tempdir().unwrap();
    let lb = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            role: "lb".into(),
            node_name: "owned-lb".into(),
            admin_password: "owned-admin".into(),
            peer_key: KEY.into(),
            client_limit: 1,
            ..Default::default()
        },
    )
    .unwrap();
    lb.config
        .put(
            "streams",
            "local/news",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    lb.config.put("peers", "cdn", cdn.peer()).unwrap();
    let https = Https::new(lb.clone()).await;
    let first = https.lookup("#!::r=local/news,u=first", None).await;
    assert_eq!(first.status(), 200);
    assert_eq!(lb.playback_auth.active(), 0, "lookup is not playback");
    let second = https.lookup("#!::r=local/news,u=second", None).await;
    assert_eq!(
        second.status(),
        200,
        "an unused lookup must not exhaust the LB"
    );
    assert_eq!(lb.playback_auth.active(), 0);
    assert_eq!(cdn.node().await["reserved"], 2);
    assert_eq!(lb.media.count().await, 0);
    https.close().await;
    cdn.close().await;
}
#[tokio::test]
async fn native_admission_reserves_listener_slots_and_fails_closed_without_encryption_binding() {
    let cdn = Native::new(1).await;
    let r = cdn.admit("viewer").await;
    if r.status() != 200 {
        panic!(
            "Admission: {} / node {}",
            r.text().await.unwrap(),
            cdn.node().await
        );
    }
    assert_eq!(cdn.admit("viewer2").await.status(), 503);
    assert_eq!(cdn.node().await["reserved"], 1);
    assert_eq!(cdn.app.media.count().await, 0);
    cdn.close().await;
}
async fn receive(id: &str, address: &str, path: &std::path::Path) -> bool {
    use std::process::Stdio;
    use tokio::process::Command;
    let mut child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-threads",
            "1",
            "-analyzeduration",
            "1000000",
            "-probesize",
            "524288",
            "-srt_streamid",
            id,
            "-passphrase",
            "owned-native-passphrase",
            "-i",
            &format!("{address}?mode=caller&latency=120000&connect_timeout=1000&timeout=3000000"),
            "-t",
            "1",
            "-map",
            "0",
            "-c",
            "copy",
            "-f",
            "mpegts",
        ])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(16), child.wait()).await {
        Ok(status) => status.unwrap().success(),
        Err(_) => {
            child.kill().await.unwrap();
            let _ = child.wait().await;
            false
        }
    }
}
#[tokio::test]
async fn encrypted_native_lookup_ticket_is_single_use_and_never_replaces_viewer_authorization() {
    use futures_util::FutureExt;
    use sha2::{Digest, Sha256};
    let cdn = Native::new(4).await;
    let (_d, lb) = app();
    let token = "wire%2B+é";
    cdn.app.config.put("streams","local/news",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(token.as_bytes()))})).unwrap();
    lb.config.put("peers", "cdn", cdn.peer()).unwrap();
    let https = Https::new(lb.clone()).await;
    let result = std::panic::AssertUnwindSafe(async {
        let r = https
            .lookup(&format!("#!::r=local/news,u={token},a=opaque"), None)
            .await;
        assert_eq!(r.status(), 200);
        let value: Value = r.json().await.unwrap();
        let id = value["streamid"].as_str().unwrap();
        let wrong = cdn._dir.path().join("wrong.ts");
        assert!(!receive(&id.replace(token, "wrong-token"), &cdn.address, &wrong).await);
        assert_eq!(std::fs::metadata(&wrong).map(|m| m.len()).unwrap_or(0), 0);
        assert_eq!(cdn.app.media.count().await, 0);
        assert_eq!(
            cdn.node().await["reserved"],
            1,
            "bad viewer must not consume ticket"
        );
        let path = cdn._dir.path().join("received.ts");
        assert!(
            receive(id, &cdn.address, &path).await,
            "owned encrypted native caller did not receive media"
        );
        let mut decoder = tokio::process::Command::new("ffmpeg")
            .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
            .arg(&path)
            .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let decoded = tokio::time::timeout(Duration::from_secs(8), decoder.wait()).await;
        if decoded.is_err() {
            decoder.kill().await.unwrap();
            let _ = decoder.wait().await;
        }
        assert!(decoded.unwrap().unwrap().success());
        assert_eq!(cdn.node().await["reserved"], 0);
        let replay = cdn._dir.path().join("replay.ts");
        assert!(!receive(id, &cdn.address, &replay).await);
        assert_eq!(std::fs::metadata(&replay).map(|m| m.len()).unwrap_or(0), 0);
        assert_eq!(lb.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    https.close().await;
    cdn.close().await;
    if let Err(e) = result {
        std::panic::resume_unwind(e)
    }
}

#[tokio::test]
async fn concurrent_load_balancers_share_cdn_owned_srt_slots_with_http_reservations() {
    let cdn = Native::new(1).await;
    let (_d, a) = app();
    let (_e, b) = app();
    a.config.put("peers", "cdn", cdn.peer()).unwrap();
    b.config.put("peers", "cdn", cdn.peer()).unwrap();
    let first = Https::new(a.clone()).await;
    let second = Https::new(b.clone()).await;
    let mut tasks = Vec::new();
    let (ra, rb) = tokio::join!(
        first.lookup("#!::r=local/news,u=a", None),
        second.lookup("#!::r=local/news,u=b", None)
    );
    let codes = [ra.status().as_u16(), rb.status().as_u16()];
    assert_eq!(codes.iter().filter(|c| **c == 200).count(), 1);
    assert_eq!(codes.iter().filter(|c| **c == 503).count(), 1);

    for i in 0..10 {
        let cdn_url = cdn.url.clone();
        tasks.push(tokio::spawn(async move {use sha2::{Digest,Sha256};reqwest::Client::new().post(format!("{cdn_url}/flussonix/api/v1/admit")).header("x-flussonix-peer",KEY).json(&json!({"name":"local/news","protocol":"srt","encrypted":true,"token_hash":format!("{:x}",Sha256::digest(format!("viewer{i}").as_bytes()))})).send().await.unwrap().status().as_u16()}));
    }
    let mut success = 0;
    for task in tasks {
        match task.await.unwrap() {
            200 => success += 1,
            503 => {}
            status => panic!("unexpected {status}"),
        }
    }
    assert_eq!(success, 0);
    assert_eq!(cdn.node().await["reserved"], 1);
    let (ra, rb) = tokio::join!(
        first.lookup("#!::r=local/news,u=a", None),
        second.lookup("#!::r=local/news,u=b", None)
    );
    assert_eq!(ra.status(), 503);
    assert_eq!(rb.status(), 503);
    // The same ledger also accounts native HTTP reservations.
    let r = reqwest::Client::new()
        .post(format!("{}/flussonix/api/v1/admit", cdn.url))
        .header("x-flussonix-peer", &cdn.key)
        .json(&json!({"name":"local/news","protocol":"http","bitrate_mbps":5}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(cdn.node().await["reserved"], 2);
    let pressure = reqwest::Client::new()
        .post(format!("{}/flussonix/api/v1/admit", cdn.url))
        .header("x-flussonix-peer", KEY)
        .json(&json!({"name":"local/news","protocol":"http","bitrate_mbps":894}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        pressure.status(),
        503,
        "SRT and HTTP must share pending bandwidth"
    );

    assert_eq!(
        a.media.count().await + b.media.count().await + cdn.app.media.count().await,
        0
    );
    first.close().await;
    second.close().await;
    cdn.close().await;
}
#[tokio::test]
async fn native_listener_encryption_attestation_and_streamid_budget_precede_admission() {
    let cdn = Native::new(4).await;
    let (_d, lb) = app();
    let mut p = cdn.peer();
    p["flussonix_srt_encrypted"] = json!(false);
    lb.config.put("peers", "cdn", p).unwrap();
    let https = Https::new(lb.clone()).await;
    assert_eq!(
        https.lookup("#!::r=local/news", Some("any")).await.status(),
        503
    );
    assert_eq!(cdn.node().await["reserved"], 0);
    lb.config.put("peers", "cdn", cdn.peer()).unwrap();
    let prefix = "#!::r=local/news,m=request,u=";
    let id = format!("{prefix}{}", "x".repeat(512 - prefix.len()));
    assert_eq!(https.lookup(&id, None).await.status(), 503);
    assert_eq!(cdn.node().await["reserved"], 0);
    assert_eq!(
        https
            .lookup(
                "#!::r=local/news,flussonix_ticket=00000000-0000-0000-0000-000000000001",
                None
            )
            .await
            .status(),
        400
    );
    https.close().await;
    cdn.close().await;
}

#[tokio::test]
async fn native_ticket_stays_out_of_destination_callback_and_lookup_rejects_caller_supplied_tickets()
 {
    use axum::{Router, extract::OriginalUri, routing::get};
    use futures_util::FutureExt;
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = observed.clone();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let callback = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(uri.query().unwrap().to_owned());
                    "allowed"
                }
            })),
        )
        .await
        .unwrap()
    }));
    let cdn = Native::new(4).await;
    cdn.app
        .config
        .put("streams", "local/news", json!({"on_play":url}))
        .unwrap();
    let (_d, lb) = app();
    lb.config.put("peers", "cdn", cdn.peer()).unwrap();
    let https = Https::new(lb.clone()).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let r = https
            .lookup("#!::u=one%2B+é,r=local/news,a=opaque", None)
            .await;
        assert_eq!(r.status(), 200);
        let result: Value = r.json().await.unwrap();
        assert_eq!(
            observed.lock().unwrap().len(),
            0,
            "reservation is not viewer authorization"
        );
        let capture = cdn._dir.path().join("callback.ts");
        assert!(receive(result["streamid"].as_str().unwrap(), &cdn.address, &capture).await);
        let rows = observed.lock().unwrap().clone();
        assert_eq!(rows.len(), 1);
        let query: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(rows[0].as_bytes())
                .into_owned()
                .collect();
        assert_eq!(query["proto"], "srt");
        assert_eq!(query["token"], "one%2B+é");
        let clean: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(query["qs"].as_bytes())
                .into_owned()
                .collect();
        assert_eq!(
            clean["streamid"],
            "#!::u=one%2B+é,r=local/news,a=opaque,m=request"
        );
        assert!(!rows[0].contains("flussonix_ticket"));
    })
    .catch_unwind()
    .await;
    https.close().await;
    cdn.close().await;
    callback.abort();
    let _ = callback.await;
    if let Err(e) = outcome {
        std::panic::resume_unwind(e)
    }
}

#[tokio::test]
async fn native_reserved_cdn_pulls_owned_vendor_origin_only_after_viewer_admission() {
    use futures_util::FutureExt;
    use std::sync::atomic::Ordering;
    let source =
        super::vendor_origin_fixture::Fixture::configured("cdn", json!({"only":[]}), None, false)
            .await;
    let cdn = Native::start_with(
        source.app.clone(),
        tempfile::tempdir().unwrap(),
        4,
        "owned-native-peer",
    )
    .await;
    let (_d, lb) = app();
    lb.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    lb.config.put("peers", "cdn", cdn.peer()).unwrap();
    let https = Https::new(lb.clone()).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let r = https.lookup("#!::r=owned,u=owned-viewer", None).await;
        assert_eq!(r.status(), 200);
        let value: Value = r.json().await.unwrap();
        assert_eq!(
            source.origins[0].private_calls.load(Ordering::SeqCst),
            0,
            "reservation started origin media"
        );
        assert_eq!(cdn.app.media.count().await, 0);
        let capture = cdn._dir.path().join("origin.ts");
        assert!(
            receive(value["streamid"].as_str().unwrap(), &cdn.address, &capture).await,
            "reserved vendor source did not deliver media"
        );
        assert_eq!(source.origins[0].private_calls.load(Ordering::SeqCst), 1);
        assert_eq!(cdn.node().await["reserved"], 0);
        assert_eq!(lb.media.count().await, 0);
    })
    .catch_unwind()
    .await;
    https.close().await;
    cdn.close().await;
    source.stop().await;
    if let Err(e) = outcome {
        std::panic::resume_unwind(e)
    }
}

#[tokio::test]
async fn failed_native_admission_retries_another_cdn_without_starting_lb_media() {
    use axum::{
        Router,
        routing::{get, post},
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    let good = Native::new(4).await;
    let bad = Native::new(4).await;
    let mut node = bad.node().await;
    node["cpu"] = json!(0.0);
    node["ram"] = json!(0.0);
    node["uplink"] = json!(0.0);
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", tcp.local_addr().unwrap());
    let server = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new()
                .route(
                    "/flussonix/api/v1/node",
                    get(move || {
                        let node = node.clone();
                        async move { axum::Json(node) }
                    }),
                )
                .route(
                    "/flussonix/api/v1/admit",
                    post(move || {
                        let calls = seen.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            axum::http::StatusCode::SERVICE_UNAVAILABLE
                        }
                    }),
                ),
        )
        .await
        .unwrap()
    }));
    let (_d, lb) = app();
    let mut peer = bad.peer();
    peer["api_url"] = json!(api);
    lb.config.put("peers", "bad", peer).unwrap();
    lb.config.put("peers", "good", good.peer()).unwrap();
    let https = Https::new(lb.clone()).await;
    let r = https.lookup("#!::r=local/news,u=viewer", None).await;
    assert_eq!(r.status(), 200);
    let value: Value = r.json().await.unwrap();
    assert_eq!(value["address"], good.address);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(good.node().await["reserved"], 1);
    assert_eq!(lb.media.count().await, 0);
    https.close().await;
    good.close().await;
    bad.close().await;
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn native_admission_reply_cannot_escape_a_changed_lb_policy() {
    use axum::{
        Router,
        routing::{get, post},
    };
    let cdn = Native::new(4).await;
    let node = cdn.node().await;
    let remote = cdn.url.clone();
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let held = Arc::new(std::sync::Mutex::new(Some((entered, gate))));
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", tcp.local_addr().unwrap());
    let server = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new()
                .route(
                    "/flussonix/api/v1/node",
                    get(move || {
                        let node = node.clone();
                        async move { axum::Json(node) }
                    }),
                )
                .route(
                    "/flussonix/api/v1/admit",
                    post(move |axum::Json(body): axum::Json<Value>| {
                        let held = held.clone();
                        let remote = remote.clone();
                        async move {
                            let value: Value = reqwest::Client::new()
                                .post(format!("{remote}/flussonix/api/v1/admit"))
                                .header("x-flussonix-peer", KEY)
                                .json(&body)
                                .send()
                                .await
                                .unwrap()
                                .json()
                                .await
                                .unwrap();
                            let waiting = held.lock().unwrap().take();
                            if let Some((entered, gate)) = waiting {
                                let _ = entered.send(());
                                let _ = gate.await;
                            }
                            axum::Json(value)
                        }
                    }),
                ),
        )
        .await
        .unwrap()
    }));
    let (_d, lb) = app();
    let mut peer = cdn.peer();
    peer["api_url"] = json!(api);
    lb.config.put("peers", "cdn", peer).unwrap();
    let https = Https::new(lb.clone()).await;
    let (r, ()) = tokio::join!(https.lookup("#!::r=local/news,u=viewer", None), async {
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap()
            .unwrap();
        lb.config
            .put("streams", "local/news", json!({"disabled":true}))
            .unwrap();
        let _ = release.send(());
    });
    assert!(matches!(r.status().as_u16(), 403 | 503));
    assert_eq!(lb.playback_auth.active(), 0);
    assert_eq!(lb.media.count().await + cdn.app.media.count().await, 0);
    https.close().await;
    cdn.close().await;
    server.abort();
    let _ = server.await;
}
