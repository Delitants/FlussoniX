//! Real daemon HTTP playback uses owned vendor management/private endpoints.
#[path = "support/tls.rs"]
mod tls_fixture;
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, Request, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    m4f::Frame,
    m4s::Track,
    server::{App, Options, router},
    wire,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
type Hold = Arc<
    Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;
struct Fixture {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    source: String,
    hold: Hold,
    calls: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    remote: Arc<Mutex<Value>>,
    segment: Arc<Mutex<Vec<u8>>>,
    duration: Arc<std::sync::atomic::AtomicU64>,
    control_second: Arc<std::sync::atomic::AtomicU64>,
    first_signal_delay: Arc<std::sync::atomic::AtomicU64>,
    control_epoch: Arc<Mutex<CancellationToken>>,
    control_close_invalid: Arc<std::sync::atomic::AtomicBool>,
    first_media_delay: Arc<std::sync::atomic::AtomicU64>,
    page_delay: Arc<std::sync::atomic::AtomicU64>,
    page_limits: Arc<Mutex<Vec<usize>>>,
    page_snapshots: Arc<Mutex<Vec<(tokio::time::Instant, bool)>>>,
    server: tokio::task::JoinHandle<()>,
    owner: tokio::task::JoinHandle<Result<(), String>>,
    cancel: CancellationToken,
}
impl Fixture {
    async fn new(policy: Value) -> Self {
        Self::variant(policy, "m4s", "standalone").await
    }
    async fn variant(policy: Value, transport: &'static str, role: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let source = base.replacen("http", transport, 1);
        let calls = Arc::new(Mutex::new(vec![]));
        let record = calls.clone();
        let remote = Arc::new(Mutex::new(
            json!([{"name":"owned","groups":[],"named_by":"config","static":true}]),
        ));
        let rows = remote.clone();
        let hold: Hold = Arc::new(Mutex::new(None));
        let held = hold.clone();
        let segment = Arc::new(Mutex::new(multitrack_segment()));
        let payload = segment.clone();
        let duration = Arc::new(std::sync::atomic::AtomicU64::new(4000));
        let control_duration = duration.clone();
        let control_second = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let signal_second = control_second.clone();
        let first_signal_delay = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let signal_delay = first_signal_delay.clone();
        let control_epoch = Arc::new(Mutex::new(CancellationToken::new()));
        let epoch = control_epoch.clone();
        let control_close_invalid = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let invalid_control = control_close_invalid.clone();
        let page_delay = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let delayed = page_delay.clone();
        let page_limits = Arc::new(Mutex::new(Vec::new()));
        let limits = page_limits.clone();
        let page_snapshots = Arc::new(Mutex::new(Vec::new()));
        let snapshots = page_snapshots.clone();
        let first_media_delay = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let media_delay = first_media_delay.clone();
        let service=Router::new().fallback(get(move |OriginalUri(uri):OriginalUri,h:HeaderMap|{
            record.lock().unwrap().push((uri.path().to_owned(),h.clone()));let rows=rows.clone();let held=held.clone();let payload=payload.clone();let control_duration=control_duration.clone();let signal_second=signal_second.clone();let signal_delay=signal_delay.clone();let epoch=epoch.clone();let invalid_control=invalid_control.clone();let delayed=delayed.clone();let limits=limits.clone();let snapshots=snapshots.clone();let media_delay=media_delay.clone();
            async move {
                assert!(!h.contains_key("x-flussonix-peer"),"native credentials reached vendor");
                if uri.path()=="/owned/m4f" {
                    assert!(h.contains_key("cluster-auth-key"));assert!(!h.contains_key("authorization"));
                    let millis=control_duration.load(std::sync::atomic::Ordering::SeqCst);
                    let second=signal_second.load(std::sync::atomic::Ordering::SeqCst);
                    let delay=signal_delay.load(std::sync::atomic::Ordering::SeqCst);
                    let closed=epoch.lock().unwrap().clone();
                    let body=futures_util::stream::unfold(false,move |sent|{let closed=closed.clone();let invalid_control=invalid_control.clone();async move{
                        if sent {closed.cancelled().await;if invalid_control.swap(false,std::sync::atomic::Ordering::SeqCst){return Some((Ok::<_,std::io::Error>(bytes::Bytes::from_static(b"bad\n")),true));}return None;}
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        Some((Ok::<_,std::io::Error>(bytes::Bytes::from(format!("1 2026/10/13/00/00/{second:02}-{millis}\n"))),true))
                    }
                    });
                    return Response::new(Body::from_stream(body));
                }
                if uri.path().starts_with("/owned/2026/10/13/00/00/") && uri.path().ends_with(".m4f") {
                    assert!(h.contains_key("cluster-auth-key"));assert!(!h.contains_key("authorization"));
                    return Response::new(Body::from(payload.lock().unwrap().clone()));
                }
                if uri.path().ends_with("/owned/m4s") {
                    assert!(h.contains_key("cluster-auth-key"));assert!(!h.contains_key("authorization"));
                    let track=Track{id:1,codec:"mp3".into(),config:vec![]};
                    let delay=media_delay.load(std::sync::atomic::Ordering::SeqCst);
                    let body=futures_util::stream::unfold((0u64,track),move |(i,track)|async move {
                        let bytes=if i==0 {tokio::time::sleep(Duration::from_millis(delay)).await;wire::encode_info(std::slice::from_ref(&track)).unwrap()} else {
                            tokio::time::sleep(Duration::from_millis(3)).await;
                            wire::encode_frame(&track,&Frame{track_id:1,dts:90000+(i-1)*2351,pts_offset:0,key:true,body:include_bytes!("fixtures/codecs/mp3.bin").to_vec()}).unwrap()
                        };
                        Some((Ok::<_,std::io::Error>(bytes::Bytes::from(bytes)),(i+1,track)))
                    });
                    return Response::new(Body::from_stream(body));
                }
                assert!(h.contains_key("authorization"));assert!(!h.contains_key("cluster-auth-key"));
                if uri.path().ends_with("/config") {
                    let pause=held.lock().unwrap().take();
                    if let Some((started,release))=pause {let _=started.send(());let _=release.await;}
                }
                let value=if uri.path().ends_with("/config") {json!({"stats":{"id":"owned-vendor-origin"}})} else {
                    assert_eq!(uri.path(),"/streamer/api/v3/streams");
                    let delay=delayed.load(std::sync::atomic::Ordering::SeqCst);
                    if delay==0 {json!({"server_id":"owned-vendor-origin","streams":rows.lock().unwrap().clone()})}
                    else {
                        let query:std::collections::HashMap<_,_>=url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()).into_owned().collect();
                        assert_eq!(query.get("select").unwrap(),"name,named_by,groups,static,disabled");
                        let limit:usize=query.get("limit").unwrap().parse().unwrap();
                        assert!((1..=1000).contains(&limit));
                        limits.lock().unwrap().push(limit);
                        let offset=query.get("cursor").map(|c|c.strip_prefix("owned page:").unwrap().parse::<usize>().unwrap()).unwrap_or(0);
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                        let catalog=rows.lock().unwrap();let catalog=catalog.as_array().unwrap();
                        snapshots.lock().unwrap().push((tokio::time::Instant::now(),catalog.iter().any(|row|row["name"]=="owned")));
                        let end=(offset+limit).min(catalog.len());
                        let mut value=json!({"server_id":"owned-vendor-origin","streams":catalog[offset..end]});
                        if end<catalog.len() {value["next"]=json!(format!("owned page:{end}"));}
                        value
                    }
                };
                Response::new(Body::from(value.to_string()))
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, vendor_fixture::acknowledge_media(service))
                .await
                .unwrap()
        });
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: role.into(),
                uplink_interface: "process".into(),
                admin_password: "owned-admin-key".into(),
                peer_key: "owned-native-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut row = json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-private-cluster-key","only":[]});
        if let Some(policy) = policy.as_object() {
            for (k, v) in policy {
                row[k] = v.clone();
            }
        }
        app.config.put("sources", &source, row).unwrap();
        let cancel = CancellationToken::new();
        let a = app.clone();
        let c = cancel.clone();
        let owner = tokio::spawn(async move { a.run_vendor_playback(c).await });
        tokio::task::yield_now().await;
        Self {
            _dir: dir,
            app,
            source,
            hold,
            calls,
            remote,
            segment,
            duration,
            control_second,
            first_signal_delay,
            control_epoch,
            control_close_invalid,
            first_media_delay,
            page_delay,
            page_limits,
            page_snapshots,
            server,
            owner,
            cancel,
        }
    }
    async fn get(&self, path: &str) -> Response {
        tokio::time::timeout(
            Duration::from_secs(12),
            router(self.app.clone())
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap()),
        )
        .await
        .unwrap()
        .unwrap()
    }
    fn media_calls(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.ends_with("/owned/m4s"))
            .count()
    }
    async fn stop(self) {
        self.cancel.cancel();
        assert!(self.owner.await.unwrap().is_ok());
        self.app.media.stop_all().await;
        self.server.abort();
        let _ = self.server.await;
    }
}
#[tokio::test]
async fn saved_vendor_policy_denies_before_private_io_and_peer_headers_do_not_bypass() {
    let f = Fixture::new(json!({"flussonix_token_sha256":"a".repeat(64)})).await;
    let r = f.get("/owned/m4s?token=wrong").await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let r = router(f.app.clone())
        .oneshot(
            Request::builder()
                .uri("/owned/m4s")
                .header("x-flussonix-peer", "owned-native-peer-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    assert_eq!(f.media_calls(), 0);
    f.stop().await;
}
#[tokio::test]
async fn saved_vendor_stream_reaches_shared_m4s_output_and_survives_native_supervision() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let r = f.get("/owned/m4s?token=one").await;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers()["content-type"], "application/x-video-m4s");
    let mut body = r.into_body();
    let data = tokio::time::timeout(Duration::from_secs(3), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(!data.is_empty());
    let before = f.app.media.stats("owned").await;
    f.app.reconcile().await;
    let after = f.app.media.stats("owned").await;
    assert_eq!(after["pid"], before["pid"]);
    assert_ne!(after["status"], "waiting");
    assert!(
        tokio::time::timeout(Duration::from_secs(3), body.frame())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(f.media_calls(), 1);
    drop(body);
    f.stop().await;
}
#[tokio::test]
async fn overlapping_vendor_namespaces_fail_without_management_or_media_io() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.app.config.put("sources","m4s://owned-other.invalid",json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-other-private-key","on_play":"true"})).unwrap();
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.calls.lock().unwrap().is_empty());
    f.stop().await;
}
async fn bytes(response: Response) -> bytes::Bytes {
    axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap()
}
async fn native_info(body: &mut Body) -> Vec<Track> {
    use http_body_util::BodyExt;
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut decoder = flussonix::m4s::Decoder::default();
        loop {
            let data = body
                .frame()
                .await
                .expect("native body closed before metadata")
                .unwrap()
                .into_data()
                .unwrap();
            for event in decoder.push(&data).unwrap() {
                if let flussonix::m4s::Event::Info { tracks, .. } = event {
                    return tracks;
                }
            }
        }
    })
    .await
    .unwrap()
}
async fn reauth(f: &Fixture) -> Response {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    router(f.app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/sessions/reauth?name=owned")
                .header(
                    "authorization",
                    format!("Basic {}", STANDARD.encode("admin:owned-admin-key")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn active_vendor_session_reauth_rechecks_callback_and_closes_denied_body_without_repull() {
    use http_body_util::BodyExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let deny = Arc::new(AtomicBool::new(false));
    let c = calls.clone();
    let d = deny.clone();
    let callback = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(get(move || {
                c.fetch_add(1, Ordering::SeqCst);
                let status = if d.load(Ordering::SeqCst) {
                    StatusCode::FORBIDDEN
                } else {
                    StatusCode::OK
                };
                async move { (status, [("x-authduration", "3600")]) }
            })),
        )
        .await
        .unwrap()
    });
    let f = Fixture::new(json!({"on_play":url})).await;
    let live = f.get("/owned/m4s?token=reauth").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut body = live.into_body();
    let before = f.app.media.stats("owned").await;
    let allowed = reauth(&f).await;
    if allowed.status() != StatusCode::OK {
        let status = allowed.status();
        drop(body);
        f.stop().await;
        callback.abort();
        let _ = callback.await;
        assert_eq!(status, StatusCode::OK);
        return;
    }
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes(allowed).await).unwrap()["estimated_count"],
        1
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.app.media.stats("owned").await["pid"], before["pid"]);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .is_some()
    );
    deny.store(true, Ordering::SeqCst);
    assert_eq!(reauth(&f).await.status(), StatusCode::OK);
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        while body.frame().await.is_some() {}
    })
    .await
    .is_ok();
    let count = calls.load(Ordering::SeqCst);
    let pulls = f.media_calls();
    drop(body);
    f.stop().await;
    callback.abort();
    let _ = callback.await;
    assert!(
        ended,
        "explicit callback denial must close the existing body"
    );
    assert_eq!(count, 3);
    assert_eq!(pulls, 1);
}
#[tokio::test]
async fn vendor_reauth_requires_current_active_route_without_discovery() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    assert_eq!(reauth(&f).await.status(), StatusCode::NOT_FOUND);
    assert!(f.calls.lock().unwrap().is_empty());
    let live = f.get("/owned/m4s").await;
    assert_eq!(live.status(), StatusCode::OK);
    let before = f.calls.lock().unwrap().len();
    f.app
        .config
        .put("sources", &f.source, json!({"except":["owned"]}))
        .unwrap();
    assert_eq!(reauth(&f).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(f.calls.lock().unwrap().len(), before);
    drop(live);
    f.stop().await;
}
async fn playlist(f: &Fixture, path: &str) -> String {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let r = f.get(path).await;
            assert_eq!(r.status(), StatusCode::OK);
            let text = String::from_utf8(bytes(r).await.to_vec()).unwrap();
            if text.contains("#EXTINF:") {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn processing_template_delivers_cpu_aac_and_authorized_hls_with_one_private_pull() {
    use sha2::{Digest, Sha256};
    let hash = format!("{:x}", Sha256::digest(b"cpu"));
    let f = Fixture::new(json!({"on_play":"true","flussonix_token_sha256":hash})).await;
    f.app.config.put("templates","processing",json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96},"flussonix_hls_subtitles":"drop","inputs":[{"url":"http://owned-user:owned-template-secret@unused.invalid"}],"on_play":"https://unused-auth.invalid/check","static":true})).unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"processing"}),
        )
        .unwrap();
    let r = f.get("/owned/m4s?token=cpu").await;
    assert_eq!(r.status(), StatusCode::OK);
    let mut body = r.into_body();
    let tracks = native_info(&mut body).await;
    let aac = tracks.iter().any(|t| t.codec == "aac");
    let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    let manifest = playlist(&f, "/owned/index.m3u8?token=cpu").await;
    let segment = manifest
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap();
    let media = bytes(f.get(&format!("/owned/{segment}")).await).await;
    assert_eq!(
        f.get(&format!(
            "/owned/{}",
            segment.replace("token=cpu", "token=wrong")
        ))
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let same_pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    let pulls = f.media_calls();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cpu.ts");
    std::fs::write(&path, media).unwrap();
    drop(body);
    f.stop().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    let mut child = tokio::process::Command::new("ffmpeg")
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(&path)
        .args(["-t", "0.1", "-f", "null", "-"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
        Ok(s) => s.unwrap(),
        Err(_) => {
            child.kill().await.unwrap();
            let _ = child.wait().await;
            panic!("owned decoder timed out")
        }
    };
    assert!(status.success());
    assert_eq!(pid, same_pid);
    assert_eq!(pulls, 1);
    assert!(
        aac,
        "saved processing template was ignored: output is not AAC"
    );
}
#[tokio::test]
async fn processing_template_edit_revokes_old_body_and_replaces_profile_and_cached_files() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.app
        .config
        .put(
            "templates",
            "processing",
            json!({"flussonix_hls_subtitles":"drop"}),
        )
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"processing"}),
        )
        .unwrap();
    let live = f.get("/owned/m4s").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut old = live.into_body();
    let before = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    let manifest = playlist(&f, "/owned/index.m3u8").await;
    let old_file = manifest
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap()
        .to_owned();
    f.app
        .config
        .put(
            "templates",
            "processing",
            json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96}}),
        )
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while old.frame().await.is_some() {}
    })
    .await
    .is_ok();
    let fresh = f.get("/owned/m4s").await;
    assert_eq!(fresh.status(), StatusCode::OK);
    let mut new = fresh.into_body();
    let tracks = native_info(&mut new).await;
    let aac = tracks.iter().any(|t| t.codec == "aac");
    let after = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    let stale = f.get(&format!("/owned/{old_file}")).await.status();
    let pulls = f.media_calls();
    drop(old);
    drop(new);
    f.stop().await;
    assert!(closed);
    assert!(aac);
    assert_ne!(before, after);
    assert_ne!(stale, StatusCode::OK);
    assert_eq!(pulls, 2);
    for pid in [before, after] {
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}

#[tokio::test]
async fn vendor_group_pin_edit_retires_existing_cdn_viewer() {
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    let f = Fixture::new(json!({
        "on_play":"true",
        "group_config":{
            "paid":{"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"paid-viewer"))},
            "public":{"on_play":"true"}
        },
        "flussonix_group_pins":{"owned":["paid"]}
    }))
    .await;
    (*f.remote.lock().unwrap())[0]["groups"] = json!(["paid"]);
    assert_eq!(
        f.get("/owned/m4s?token=wrong").await.status(),
        StatusCode::FORBIDDEN
    );
    let live = f.get("/owned/m4s?token=paid-viewer").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut old = live.into_body();
    assert!(old.frame().await.is_some());
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_group_pins":{"owned":["public"]}}),
        )
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while old.frame().await.is_some() {}
    })
    .await
    .is_ok();
    let after = f.get("/owned/m4s?token=paid-viewer").await;
    assert_ne!(after.status(), StatusCode::OK);
    drop(old);
    f.stop().await;
    assert!(closed, "an old paid viewer survived a local pin edit");
}
#[tokio::test]
async fn vendor_group_policy_edit_retires_existing_cdn_viewer() {
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    let f = Fixture::new(json!({
        "on_play":"true",
        "group_config":{"paid":{"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"old-paid-viewer"))}},
        "flussonix_group_pins":{"owned":["paid"]}
    }))
    .await;
    (*f.remote.lock().unwrap())[0]["groups"] = json!(["paid"]);
    let live = f.get("/owned/m4s?token=old-paid-viewer").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut old = live.into_body();
    assert!(old.frame().await.is_some());
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"group_config":{"paid":{"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"new-paid-viewer"))}}}),
        )
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while old.frame().await.is_some() {}
    })
    .await
    .is_ok();
    assert_eq!(
        f.get("/owned/m4s?token=old-paid-viewer").await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        f.get("/owned/m4s?token=new-paid-viewer").await.status(),
        StatusCode::OK
    );
    drop(old);
    f.stop().await;
    assert!(
        closed,
        "an old paid viewer survived a local group policy edit"
    );
}
#[tokio::test]
async fn processing_templates_are_isolated_between_distinct_vendor_sources() {
    let a = Fixture::new(json!({"on_play":"true"})).await;
    let b = Fixture::new(json!({"on_play":"true"})).await;
    *a.remote.lock().unwrap() =
        json!([{"name":"a/owned","groups":[],"named_by":"config","static":true}]);
    *b.remote.lock().unwrap() =
        json!([{"name":"b/owned","groups":[],"named_by":"config","static":true}]);
    a.app
        .config
        .put(
            "templates",
            "processing",
            json!({"transcoder":{"acodec":"aac","ab":96}}),
        )
        .unwrap();
    a.app
        .config
        .put(
            "sources",
            &a.source,
            json!({"prefix":"a","flussonix_output_template":"processing"}),
        )
        .unwrap();
    a.app.config.put("sources",&b.source,json!({"flussonix_source_kind":"flussonic","prefix":"b","cluster_key":"owned-private-cluster-key","on_play":"true"})).unwrap();
    let mut bodies = vec![];
    let mut codecs = vec![];
    for name in ["a/owned", "b/owned"] {
        let live = a.get(&format!("/{name}/m4s")).await;
        assert_eq!(live.status(), StatusCode::OK);
        let mut body = live.into_body();
        let tracks = native_info(&mut body).await;
        codecs.push(tracks[0].codec.clone());
        bodies.push(body);
    }
    let pulls = (a.media_calls(), b.media_calls());
    drop(bodies);
    a.stop().await;
    b.stop().await;
    assert_eq!(codecs, ["aac", "mp3"]);
    assert_eq!(pulls, (1, 1));
}
#[tokio::test]
async fn processing_template_never_supplies_missing_source_viewer_authority() {
    let f = Fixture::new(json!({})).await;
    f.app.config.put("templates","processing",json!({"transcoder":{"acodec":"aac","ab":96},"inputs":[{"url":"http://owned-template-input.invalid"}],"static":true})).unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"processing"}),
        )
        .unwrap();
    let status = f.get("/owned/m4s").await.status();
    let pulls = f.media_calls();
    f.stop().await;
    assert_ne!(status, StatusCode::OK);
    assert_eq!(pulls, 0);
}
#[tokio::test]
async fn hls_requests_retain_one_source_and_authorize_every_cached_segment() {
    use sha2::{Digest, Sha256};
    let hash = format!("{:x}", Sha256::digest(b"owned-viewer-token"));
    let f = Fixture::new(json!({"on_play":"true","flussonix_token_sha256":hash})).await;
    let text = playlist(&f, "/owned/index.m3u8?token=owned-viewer-token").await;
    let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    assert!(pid > 0);
    let segment = text
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .unwrap();
    assert!(segment.contains("token=owned-viewer-token"));
    let denied = f
        .get(&format!(
            "/owned/{}",
            segment.replace("owned-viewer-token", "wrong")
        ))
        .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let response = f.get(&format!("/owned/{segment}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!bytes(response).await.is_empty());
    let _ = playlist(&f, "/owned/index.m3u8?token=owned-viewer-token").await;
    f.app.reconcile().await;
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    assert_eq!(f.media_calls(), 1);
    assert_eq!(
        f.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.ends_with("/config"))
            .count(),
        2
    );
    f.stop().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}
#[tokio::test]
async fn concurrent_m4s_m4f_and_ts_outputs_share_one_private_pull() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let (a, b, c) = tokio::join!(
        f.get("/owned/m4s?token=a"),
        f.get("/owned/m4f?token=b"),
        f.get("/owned/mpegts?token=c")
    );
    for (r, content) in [
        (a, "application/x-video-m4s"),
        (b, "application/x-video-m4f-signal"),
        (c, "video/mp2t"),
    ] {
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()["content-type"], content);
        let mut body = r.into_body();
        assert!(
            !tokio::time::timeout(Duration::from_secs(5), body.frame())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(f.media_calls(), 1);
    f.stop().await;
}
#[tokio::test]
async fn disabled_local_stream_and_vendor_exclusions_never_probe_origins() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.app.config.put("streams","owned",json!({"disabled":true,"static":false,"inputs":[{"url":"http://owned.invalid/mpegts"}]})).unwrap();
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::NOT_FOUND
    );
    assert!(f.calls.lock().unwrap().is_empty());
    f.app.config.delete("streams", "owned").unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"prefix":"region","except":["owned"]}),
        )
        .unwrap();
    for p in [
        "/region/owned/index.m3u8",
        "/owned/index.m3u8",
        "/regional/owned/index.m3u8",
    ] {
        assert_eq!(f.get(p).await.status(), StatusCode::NOT_FOUND);
    }
    assert!(f.calls.lock().unwrap().is_empty());
    f.stop().await;
}
#[tokio::test]
async fn reserved_vendor_namespace_does_not_fall_back_to_native_on_missing_stream() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let root = f.source.replacen("m4s", "http", 1);
    f.app
        .config
        .put(
            "sources",
            "native-shadow",
            json!({"api_url":root,"only":[]}),
        )
        .unwrap();
    *f.remote.lock().unwrap() = json!([]);
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.calls.lock().unwrap().len(), 3);
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(f.calls.lock().unwrap().len(), 3);
    f.stop().await;
}
#[tokio::test]
async fn owner_cancellation_closes_existing_bodies_and_rejects_fresh_requests() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let mut body = f.get("/owned/m4s").await.into_body();
    assert!(body.frame().await.is_some());
    f.cancel.cancel();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.get("/owned/m4s").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(body);
    f.stop().await;
}
#[tokio::test]
async fn source_edit_revokes_active_body_and_native_supervisor_reaps_exact_worker() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let _ = playlist(&f, "/owned/index.m3u8").await;
    let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    assert!(pid > 0);
    let mut body = f.get("/owned/m4s").await.into_body();
    assert!(body.frame().await.is_some());
    f.app
        .config
        .put("sources", &f.source, json!({"except":["owned"]}))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .is_none()
    );
    f.app.reconcile().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(
        f.get("/owned/index.m3u8").await.status(),
        StatusCode::NOT_FOUND
    );
    drop(body);
    f.stop().await;
}
#[tokio::test]
async fn long_lived_daemon_response_is_renewed_past_original_source_deadline() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let r = f.get("/owned/m4s").await;
    assert_eq!(r.status(), StatusCode::OK);
    let mut body = r.into_body();
    assert!(
        tokio::time::timeout(Duration::from_secs(32), async {
            while body.frame().await.is_some() {}
        })
        .await
        .is_err(),
        "body closed before renewed source deadline"
    );
    assert_eq!(f.media_calls(), 1);
    assert!(
        f.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.ends_with("/config"))
            .count()
            >= 4
    );
    drop(body);
    f.stop().await;
}
#[tokio::test]
async fn owner_is_single_use_and_abort_cancels_playback() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    assert!(
        f.app
            .run_vendor_playback(CancellationToken::new())
            .await
            .is_err()
    );
    let r = f.get("/owned/m4s").await;
    assert_eq!(r.status(), StatusCode::OK);
    let mut body = r.into_body();
    assert!(body.frame().await.is_some());
    f.owner.abort();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .is_none()
    );
    drop(body);
    f.cancel.cancel();
    assert!(f.owner.await.unwrap_err().is_cancelled());
    f.app.media.stop_all().await;
    f.server.abort();
    let _ = f.server.await;
}
#[tokio::test]
async fn stopping_owner_cancels_held_discovery_without_late_private_request() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    *f.hold.lock().unwrap() = Some((started, held));
    let service = router(f.app.clone());
    let pending = tokio::spawn(async move {
        service
            .oneshot(
                Request::builder()
                    .uri("/owned/m4s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    f.cancel.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let _ = release.send(());
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.calls.lock().unwrap().len(), 1);
    assert_eq!(f.media_calls(), 0);
    f.stop().await;
}
#[tokio::test]
async fn held_callback_cannot_authorize_private_media_after_source_change() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let s = started.clone();
    let r = release.clone();
    let callback = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(get(move || {
                let s = s.clone();
                let r = r.clone();
                async move {
                    s.notify_one();
                    r.notified().await;
                    StatusCode::OK
                }
            })),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put("sources", &f.source, json!({"on_play":url}))
        .unwrap();
    let service = router(f.app.clone());
    let pending = tokio::spawn(async move {
        service
            .oneshot(
                Request::builder()
                    .uri("/owned/m4s?token=callback")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    f.app
        .config
        .put("sources", &f.source, json!({"except":["owned"]}))
        .unwrap();
    release.notify_one();
    assert_eq!(
        pending.await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.media_calls(), 0);
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    callback.abort();
    let _ = callback.await;
    f.stop().await;
}
#[tokio::test]
async fn https_vendor_playback_works_and_callback_redirect_cannot_downgrade() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let cert = tls_fixture::Certificates::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://{}", listener.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let server = tokio::spawn(flussonix::http_tls::serve(
        listener,
        cert.server(),
        f.app.clone(),
        cancel.clone(),
    ));
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&cert.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let mut response = client.get(format!("{url}/owned/m4s")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.chunk().await.unwrap().unwrap().is_empty());
    drop(response);
    let callback_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback_url = format!("http://{}", callback_listener.local_addr().unwrap());
    let callback = tokio::spawn(async move {
        axum::serve(
            callback_listener,
            Router::new().fallback(get(|| async {
                (
                    StatusCode::FOUND,
                    [("location", "http://owned-redirect.invalid/owned/m4s")],
                )
            })),
        )
        .await
        .unwrap()
    });
    f.app
        .config
        .put("sources", &f.source, json!({"on_play":callback_url}))
        .unwrap();
    let r = client
        .get(format!("{url}/owned/m4s?token=redirect"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    drop(r);
    let r = f.get("/owned/m4s?token=redirect").await;
    assert_eq!(r.status(), StatusCode::FOUND);
    assert_eq!(
        r.headers()["location"],
        "http://owned-redirect.invalid/owned/m4s"
    );
    assert_eq!(f.media_calls(), 1);
    callback.abort();
    let _ = callback.await;
    drop(client);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.stop().await;
}
fn multitrack_segment() -> Vec<u8> {
    let tracks = vec![
        Track {
            id: 1,
            codec: "mp3".into(),
            config: vec![],
        },
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
    ];
    let mut frames = vec![];
    for i in 0..150 {
        frames.push(Frame {
            track_id: 1,
            dts: 90000 + i * 2351,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        });
        frames.push(Frame {
            track_id: 2,
            dts: 90000 + i * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        });
    }
    frames.sort_by_key(|f| f.dts);
    flussonix::m4f::pack(&tracks, &frames, 360000).unwrap()
}
fn text_segment() -> Vec<u8> {
    let (mut tracks, mut frames) = flussonix::m4f::unpack(&multitrack_segment()).unwrap();
    for i in 150..380 {
        frames.push(Frame {
            track_id: 1,
            dts: 90000 + i * 2351,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        });
        frames.push(Frame {
            track_id: 2,
            dts: 90000 + i * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        });
    }
    for (id, text) in [(7, "AMERICA HELLO"), (8, "EUROPE GRÜSSE")] {
        tracks.push(Track {
            id,
            codec: "subtitle".into(),
            config: vec![],
        });
        frames.push(Frame {
            track_id: id,
            dts: 100000,
            pts_offset: 50000,
            key: true,
            body: text.as_bytes().to_vec(),
        });
    }
    frames.sort_by_key(|f| f.dts);
    flussonix::m4f::pack(&tracks, &frames, 900000).unwrap()
}
#[tokio::test]
async fn processing_template_preserves_or_filters_original_native_subtitle_tracks() {
    for preserve in [false, true] {
        let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "standalone").await;
        *f.segment.lock().unwrap() = text_segment();
        f.duration.store(10000, std::sync::atomic::Ordering::SeqCst);
        f.app.config.put("templates","processing",json!({"flussonix_subtitle_tracks":if preserve {"preserve"} else {"drop"},"flussonix_hls_subtitles":"drop"})).unwrap();
        f.app
            .config
            .put(
                "sources",
                &f.source,
                json!({"flussonix_output_template":"processing"}),
            )
            .unwrap();
        let live = f.get("/owned/m4s").await;
        assert_eq!(live.status(), StatusCode::OK);
        let mut body = live.into_body();
        let tracks = native_info(&mut body).await;
        let count = tracks.iter().filter(|t| t.codec == "subtitle").count();
        let response = f.get("/owned/2026/10/13/00/00/00.m4f").await;
        assert_eq!(response.status(), StatusCode::OK);
        let segment = bytes(response).await;
        let (tracks, _) = flussonix::m4f::unpack(&segment).unwrap();
        let segment_count = tracks.iter().filter(|t| t.codec == "subtitle").count();
        drop(body);
        f.stop().await;
        assert_eq!(count, if preserve { 2 } else { 0 });
        assert_eq!(segment_count, count);
        if preserve {
            assert_eq!(segment, text_segment());
        }
    }
}
#[tokio::test]
async fn processing_template_converts_english_and_german_native_text_with_shared_authorized_state()
{
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    let f=Fixture::variant(json!({"on_play":"true","flussonix_token_sha256":format!("{:x}",Sha256::digest(b"caption"))}),"m4f","standalone").await;
    *f.segment.lock().unwrap() = text_segment();
    f.duration.store(10000, std::sync::atomic::Ordering::SeqCst);
    f.app.config.put("templates","processing",json!({"flussonix_hls_subtitles":"convert","flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"},{"native_track":8,"language":"de","name":"Deutsch"}]})).unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"processing"}),
        )
        .unwrap();
    let master = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let r = f.get("/owned/index.m3u8?token=caption").await;
            assert_eq!(
                r.status(),
                StatusCode::OK,
                "{}",
                f.app.media.stats("owned").await
            );
            let text = String::from_utf8(bytes(r).await.to_vec()).unwrap();
            if text.contains("TYPE=SUBTITLES") {
                break text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(master.contains("English") && master.contains("Deutsch"));
    let first_pid = f.app.media.stats("owned").await["pid"].clone();
    let live = f.get("/owned/m4s?token=caption").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut body = live.into_body();
    let tracks = native_info(&mut body).await;
    assert!(tracks.iter().all(|t| t.codec != "subtitle"));
    let mut texts = vec![];
    for id in [7, 8] {
        let list = String::from_utf8(
            bytes(f.get(&format!("/owned/nt{id}.m3u8?token=caption")).await)
                .await
                .to_vec(),
        )
        .unwrap();
        let mut text = String::new();
        for file in list
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let r = f.get(&format!("/owned/{file}")).await;
            assert_eq!(r.status(), StatusCode::OK);
            text += &String::from_utf8_lossy(&bytes(r).await);
            assert_eq!(
                f.get(&format!(
                    "/owned/{}",
                    file.replace("token=caption", "token=wrong")
                ))
                .await
                .status(),
                StatusCode::FORBIDDEN
            );
        }
        texts.push(text);
    }
    let last_pid = f.app.media.stats("owned").await["pid"].clone();
    let pulls = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p == "/owned/m4f")
        .count();
    let old_list = String::from_utf8(
        bytes(f.get("/owned/nt7.m3u8?token=caption").await)
            .await
            .to_vec(),
    )
    .unwrap();
    let old_files: Vec<_> = old_list
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect();
    let (tracks, mut frames) = flussonix::m4f::unpack(&text_segment()).unwrap();
    for frame in &mut frames {
        if frame.track_id == 7 {
            frame.body = b"REPLACEMENT ENGLISH".to_vec();
        }
        if frame.track_id == 8 {
            frame.body = "NEUE GRÜSSE".as_bytes().to_vec();
        }
    }
    *f.segment.lock().unwrap() = flussonix::m4f::pack(&tracks, &frames, 900000).unwrap();
    f.app
        .config
        .put(
            "templates",
            "processing",
            json!({"flussonix_input_timeout":20}),
        )
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while body.frame().await.is_some() {}
    })
    .await
    .is_ok();
    let fresh = f.get("/owned/index.m3u8?token=caption").await;
    assert_eq!(fresh.status(), StatusCode::OK);
    let new_list = String::from_utf8(
        bytes(f.get("/owned/nt7.m3u8?token=caption").await)
            .await
            .to_vec(),
    )
    .unwrap();
    let mut replacement = String::new();
    for file in new_list
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        assert!(!old_files.iter().any(|old| old == file));
        replacement +=
            &String::from_utf8_lossy(&bytes(f.get(&format!("/owned/{file}")).await).await);
    }
    let stale = f.get(&format!("/owned/{}", old_files[0])).await.status();
    drop(body);
    f.stop().await;
    assert!(texts[0].contains("AMERICA HELLO"), "{}", texts[0]);
    assert!(texts[1].contains("EUROPE GRÜSSE"), "{}", texts[1]);
    assert_eq!(first_pid, last_pid);
    assert_eq!(pulls, 1);
    assert!(closed);
    assert!(replacement.contains("REPLACEMENT ENGLISH"), "{replacement}");
    assert!(!replacement.contains("AMERICA HELLO"));
    assert_ne!(stale, StatusCode::OK);
}
#[tokio::test]
async fn processing_template_preserve_with_transcoding_fails_explicitly_for_native_text() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "standalone").await;
    *f.segment.lock().unwrap() = text_segment();
    f.duration.store(10000, std::sync::atomic::Ordering::SeqCst);
    f.app.config.put("templates","processing",json!({"transcoder":{"acodec":"aac","ab":96},"flussonix_subtitle_tracks":"preserve","flussonix_hls_subtitles":"drop"})).unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"processing"}),
        )
        .unwrap();
    let status = f.get("/owned/m4s").await.status();
    let stats = f.app.media.stats("owned").await;
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(stats["last_error"], "native_subtitle_transcode_unsupported");
}
#[tokio::test]
async fn processing_template_conversion_restarts_after_invalid_control_with_fresh_state() {
    use http_body_util::BodyExt;
    let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "standalone").await;
    *f.segment.lock().unwrap() = text_segment();
    f.duration.store(10000, std::sync::atomic::Ordering::SeqCst);
    f.app.config.put("templates","processing",json!({"flussonix_hls_subtitles":"convert","flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"}]})).unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"processing"}),
        )
        .unwrap();
    let master = f.get("/owned/index.m3u8").await;
    assert_eq!(master.status(), StatusCode::OK);
    let list = String::from_utf8(bytes(f.get("/owned/nt7.m3u8").await).await.to_vec()).unwrap();
    let old_file = list
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap()
        .to_owned();
    let old_text = bytes(f.get(&format!("/owned/{old_file}")).await).await;
    let live = f.get("/owned/m4s").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut body = live.into_body();
    let _ = native_info(&mut body).await;
    let old_pid = f.app.media.stats("owned").await["pid"].clone();
    let revision = f.app.config.revision();
    let management = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.contains("/streamer/api/"))
        .count();
    let (tracks, mut frames) = flussonix::m4f::unpack(&text_segment()).unwrap();
    for frame in &mut frames {
        if frame.track_id == 7 {
            frame.body = b"NEW INPUT GENERATION".to_vec();
        }
    }
    *f.segment.lock().unwrap() = flussonix::m4f::pack(&tracks, &frames, 900000).unwrap();
    // A malformed control frame requires supervised replacement. Clean EOF
    // intentionally keeps the worker and skips this already-published GOP.
    f.control_close_invalid
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let closed = std::mem::replace(
        &mut *f.control_epoch.lock().unwrap(),
        CancellationToken::new(),
    );
    closed.cancel();
    let ended = tokio::time::timeout(Duration::from_secs(3), async {
        while body.frame().await.is_some() {}
    })
    .await
    .is_ok();
    let fresh = f.get("/owned/index.m3u8").await;
    assert_eq!(fresh.status(), StatusCode::OK);
    let list = String::from_utf8(bytes(f.get("/owned/nt7.m3u8").await).await.to_vec()).unwrap();
    let new_file = list
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap()
        .to_owned();
    let text = bytes(f.get(&format!("/owned/{new_file}")).await).await;
    let new_pid = f.app.media.stats("owned").await["pid"].clone();
    let same_revision = f.app.config.revision() == revision;
    let new_management = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.contains("/streamer/api/"))
        .count();
    drop(body);
    f.stop().await;
    assert!(ended);
    assert!(same_revision);
    // Recovery refreshes the origin profile and catalog before reopening it.
    assert_eq!(new_management, management + 3);
    assert_ne!(old_pid, new_pid);
    assert_ne!(old_file, new_file);
    assert!(String::from_utf8_lossy(&old_text).contains("AMERICA HELLO"));
    assert!(String::from_utf8_lossy(&text).contains("NEW INPUT GENERATION"));
    assert!(!String::from_utf8_lossy(&text).contains("AMERICA HELLO"));
}

#[tokio::test]
async fn clean_m4f_eof_keeps_daemon_packager_and_delivers_new_timestamp_once() {
    use http_body_util::BodyExt;
    let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "cdn").await;
    let manifest = f.get("/owned/index.m3u8").await;
    assert_eq!(manifest.status(), StatusCode::OK);
    let live = f.get("/owned/m4s").await;
    assert_eq!(live.status(), StatusCode::OK);
    let mut body = live.into_body();
    let _ = native_info(&mut body).await;
    let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    let (controls, segments, catalog) = {
        let before = f.calls.lock().unwrap();
        (
            before.iter().filter(|(p, _)| p == "/owned/m4f").count(),
            before.iter().filter(|(p, _)| p.ends_with(".m4f")).count(),
            before
                .iter()
                .filter(|(p, _)| p.contains("/streamer/api/"))
                .count(),
        )
    };
    let closed = std::mem::replace(
        &mut *f.control_epoch.lock().unwrap(),
        CancellationToken::new(),
    );
    closed.cancel();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (new_controls, new_catalog) = {
                let calls = f.calls.lock().unwrap();
                (
                    calls.iter().filter(|(p, _)| p == "/owned/m4f").count(),
                    calls
                        .iter()
                        .filter(|(p, _)| p.contains("/streamer/api/"))
                        .count(),
                )
            };
            if new_controls > controls && new_catalog > catalog {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("clean EOF did not renew catalog and reopen M4F control");
    assert_eq!(f.app.media.stats("owned").await["pid"].as_u64(), Some(pid));
    assert_eq!(
        f.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.ends_with(".m4f"))
            .count(),
        segments,
        "replayed segment was fetched again"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), async {
            while body.frame().await.is_some() {}
        })
        .await
        .is_err(),
        "existing native viewer unexpectedly ended"
    );
    let (tracks, mut frames) = flussonix::m4f::unpack(&multitrack_segment()).unwrap();
    for frame in &mut frames {
        frame.dts += 360_000;
    }
    *f.segment.lock().unwrap() = flussonix::m4f::pack(&tracks, &frames, 360_000).unwrap();
    f.control_second
        .store(4, std::sync::atomic::Ordering::SeqCst);
    let closed = std::mem::replace(
        &mut *f.control_epoch.lock().unwrap(),
        CancellationToken::new(),
    );
    closed.cancel();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let fetched = {
                let calls = f.calls.lock().unwrap();
                calls
                    .iter()
                    .any(|(path, _)| path == "/owned/2026/10/13/00/00/04.m4f")
            };
            if fetched {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("new M4F timestamp was not fetched after the second clean EOF");
    let fresh = f.get("/owned/m4s").await;
    assert_eq!(fresh.status(), StatusCode::OK);
    let mut fresh_body = fresh.into_body();
    let mut decoder = flussonix::m4s::Decoder::default();
    let mut seen = std::collections::HashSet::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let data = fresh_body
                .frame()
                .await
                .expect("native viewer ended before the new timestamp")
                .unwrap()
                .into_data()
                .unwrap();
            for event in decoder.push(&data).unwrap() {
                let media = match event {
                    flussonix::m4s::Event::Frame { track_id, dts, .. } => vec![(track_id, dts)],
                    flussonix::m4s::Event::Gop { frames, .. } => frames
                        .into_iter()
                        .map(|frame| (frame.track_id, frame.dts))
                        .collect(),
                    _ => vec![],
                };
                for (track_id, dts) in media {
                    assert!(
                        seen.insert((track_id, dts)),
                        "duplicate native media timestamp"
                    );
                    if dts >= 450_000 {
                        return;
                    }
                }
            }
        }
    })
    .await
    .expect("new timestamp did not reach native playback");
    assert_eq!(f.app.media.stats("owned").await["pid"].as_u64(), Some(pid));
    {
        let calls = f.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|(path, _)| path == "/owned/2026/10/13/00/00/00.m4f")
                .count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|(path, _)| path == "/owned/2026/10/13/00/00/04.m4f")
                .count(),
            1
        );
    }
    drop(fresh_body);
    drop(body);
    f.stop().await;
}
#[tokio::test]
async fn m4f_control_stays_unavailable_until_the_first_segment_is_cached() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "standalone").await;
    f.first_signal_delay
        .store(30_000, std::sync::atomic::Ordering::SeqCst);
    f.app
        .config
        .put(
            "templates",
            "short-m4f",
            json!({"flussonix_input_timeout":10}),
        )
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"short-m4f"}),
        )
        .unwrap();
    let started = std::time::Instant::now();
    let status = f.get("/owned/m4f").await.status();
    let elapsed = started.elapsed();
    let stats = f.app.media.stats("owned").await;
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(elapsed >= Duration::from_secs(8));
    assert_eq!(stats["bytes_in"], 0);
}

#[tokio::test]
async fn m4f_control_waits_for_a_segment_within_the_worker_startup_allowance() {
    use http_body_util::BodyExt;
    let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "standalone").await;
    f.first_signal_delay
        .store(9_000, std::sync::atomic::Ordering::SeqCst);
    f.app
        .config
        .put(
            "templates",
            "slow-m4f",
            json!({"flussonix_input_timeout":20}),
        )
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"slow-m4f"}),
        )
        .unwrap();
    let started = std::time::Instant::now();
    let response = f.get("/owned/m4f").await;
    let elapsed = started.elapsed();
    let status = response.status();
    let mut body = response.into_body();
    let signal = if status == StatusCode::OK {
        tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
    } else {
        Default::default()
    };
    let segment = if status == StatusCode::OK {
        f.get("/owned/2026/10/13/00/00/00.m4f").await.status()
    } else {
        status
    };
    drop(body);
    f.stop().await;
    assert_eq!(status, StatusCode::OK);
    assert!(elapsed >= Duration::from_secs(9));
    assert!(String::from_utf8_lossy(&signal).contains("2026/10/13/00/00/00"));
    assert_eq!(segment, StatusCode::OK);
}

#[tokio::test]
async fn m4f_source_serves_original_multitrack_segment_and_hls_through_daemon() {
    use http_body_util::BodyExt;
    let f = Fixture::variant(json!({"on_play":"true"}), "m4f", "standalone").await;
    let r = f.get("/owned/m4f").await;
    assert_eq!(r.status(), StatusCode::OK);
    let mut body = r.into_body();
    let signal = tokio::time::timeout(Duration::from_secs(3), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(signal, b"1 2026/10/13/00/00/00-4000\n"[..]);
    let r = f.get("/owned/2026/10/13/00/00/00.m4f").await;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(bytes(r).await, multitrack_segment());
    let _ = playlist(&f, "/owned/index.m3u8").await;
    assert_eq!(
        f.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == "/owned/m4f")
            .count(),
        1
    );
    drop(body);
    f.stop().await;
}
#[tokio::test]
async fn vendor_lb_tickets_and_invalid_files_fail_before_source_io() {
    let f = Fixture::variant(json!({"on_play":"true"}), "m4s", "lb").await;
    assert_eq!(
        f.get("/owned/index.m3u8?flussonix_ticket=unsupported")
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        f.get("/owned/config.json").await.status(),
        StatusCode::NOT_FOUND
    );
    assert!(f.calls.lock().unwrap().is_empty());
    f.stop().await;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    assert_eq!(
        f.get("/owned/m4s?flussonix_ticket=unsupported")
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        f.get("/owned/config.json").await.status(),
        StatusCode::NOT_FOUND
    );
    assert!(f.calls.lock().unwrap().is_empty());
    f.stop().await;
}
#[tokio::test]
async fn active_vendor_worker_is_visible_in_node_stream_inventory() {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let r = f.get("/owned/m4s").await;
    assert_eq!(r.status(), StatusCode::OK);
    let node = router(f.app.clone())
        .oneshot(
            Request::builder()
                .uri("/flussonix/api/v1/node")
                .header(
                    "authorization",
                    format!("Basic {}", STANDARD.encode("admin:owned-admin-key")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(node.status(), StatusCode::OK);
    let node: Value = serde_json::from_slice(&bytes(node).await).unwrap();
    let visible = node["streams"]
        .as_array()
        .unwrap()
        .iter()
        .any(|stream| stream["name"] == "owned");
    drop(r);
    f.stop().await;
    assert!(visible, "active shared worker missing from node inventory");
}
#[tokio::test]
async fn queued_discovery_keeps_original_selection_revision_after_namespace_edit() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    *f.hold.lock().unwrap() = Some((started, held));
    let service = router(f.app.clone());
    let first = tokio::spawn(async move {
        service
            .oneshot(
                Request::builder()
                    .uri("/owned/m4s")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    // Let the first attempt's backoff elapse while its source mutex is held.
    tokio::time::sleep(Duration::from_millis(550)).await;
    let mut second = Box::pin(
        router(f.app.clone()).oneshot(
            Request::builder()
                .uri("/owned/m4s")
                .body(Body::empty())
                .unwrap(),
        ),
    );
    assert!(futures_util::poll!(second.as_mut()).is_pending());
    f.app.config.put("sources","m4s://owned-overlap.invalid",json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-other-private-key","on_play":"true"})).unwrap();
    release.send(()).unwrap();
    let first_status = first.await.unwrap().status();
    let second = second.await.unwrap();
    let second_status = second.status();
    drop(second);
    let media_calls = f.media_calls();
    f.stop().await;
    assert_eq!(first_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(second_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        media_calls, 0,
        "queued stale selection reached private media"
    );
}
#[tokio::test]
async fn fmp4_hls_child_segments_are_guarded_and_decode_with_init() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    let text = playlist(&f, "/owned/fmp4/index.m3u8?token=fmp4-viewer").await;
    let segment = text
        .lines()
        .find(|line| !line.starts_with('#') && !line.is_empty())
        .unwrap();
    let init = text
        .lines()
        .find_map(|line| {
            line.strip_prefix("#EXT-X-MAP:URI=\"")
                .and_then(|tail| tail.split_once('"').map(|(uri, _)| uri))
        })
        .unwrap();
    let a = f.get(&format!("/owned/fmp4/{init}")).await;
    let b = f.get(&format!("/owned/fmp4/{segment}")).await;
    let statuses = (a.status(), b.status());
    let mut data = bytes(a).await.to_vec();
    data.extend(bytes(b).await);
    let path = f._dir.path().join("owned-fmp4.mp4");
    std::fs::write(&path, data).unwrap();
    f.cancel.cancel();
    f.app.media.stop_all().await;
    let result = if statuses == (StatusCode::OK, StatusCode::OK) {
        Some(
            tokio::process::Command::new("ffmpeg")
                .args(["-hide_banner", "-loglevel", "error", "-i"])
                .arg(&path)
                .args(["-f", "null", "-"])
                .kill_on_drop(true)
                .output()
                .await
                .unwrap(),
        )
    } else {
        None
    };
    f.stop().await;
    assert_eq!(statuses, (StatusCode::OK, StatusCode::OK));
    assert!(result.unwrap().status.success());
}

#[path = "vendor_daemon_playback/placement.rs"]
mod placement;

#[path = "vendor_daemon_playback/balancer.rs"]
mod balancer;

#[tokio::test]
async fn automatic_m4f_static_startup_requires_no_viewer_and_decodes_hls() {
    use http_body_util::BodyExt;
    let f = Fixture::variant(
        json!({"only":["owned"],"on_play":"true"}),
        "m4f",
        "standalone",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(8), async {
        while f.app.media.stats("owned").await["bytes_in"]
            .as_u64()
            .unwrap_or(0)
            == 0
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("automatic M4F input never started");
    assert!(f.app.playback_auth.snapshots().is_empty());
    assert_eq!(f.app.playback_auth.live_grants(), 0);
    let playlist = f.get("/owned/index.m3u8").await;
    assert_eq!(playlist.status(), StatusCode::OK);
    let text = String::from_utf8(
        playlist
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let segment = text
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap();
    let reply = f.get(&format!("/owned/{segment}")).await;
    assert_eq!(reply.status(), StatusCode::OK);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("static-m4f.ts");
    tokio::fs::write(&path, reply.into_body().collect().await.unwrap().to_bytes())
        .await
        .unwrap();
    let status = tokio::process::Command::new("/usr/bin/ffmpeg")
        .kill_on_drop(true)
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(path)
        .args(["-t", "0.1", "-f", "null", "-"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .unwrap();
    assert!(status.success());
    f.stop().await;
}

#[tokio::test]
async fn m4f_vendor_rtsp_decodes_hevc_mp3_and_mp2_with_independent_ffmpeg() {
    let f = Fixture::variant(json!({"on_play":"true","only":[]}), "m4f", "standalone").await;
    let (mut tracks, mut frames) = flussonix::m4f::unpack(&multitrack_segment()).unwrap();
    tracks.push(Track {
        id: 205,
        codec: "hevc".into(),
        config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
    });
    let timing: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    for cycle in 0..8u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            frames.push(Frame {
                track_id: 205,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
            });
        }
    }
    frames.sort_by_key(|f| f.dts);
    *f.segment.lock().unwrap() = flussonix::m4f::pack(&tracks, &frames, 360000).unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "rtsp://{}/owned?token=owned-rtsp",
        tcp.local_addr().unwrap()
    );
    let cancel = CancellationToken::new();
    let listener = tokio::spawn(flussonix::rtsp::serve(tcp, f.app.clone(), cancel.clone()));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("decoder.log");
    let mut child = tokio::process::Command::new("/usr/bin/ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-threads",
            "1",
            "-rtsp_transport",
            "tcp",
            "-i",
        ])
        .arg(&url)
        .args([
            "-map", "0:v:0", "-map", "0:a:0", "-map", "0:a:1", "-t", "0.2", "-f", "null", "-",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&path).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut live_pids = std::collections::BTreeSet::new();
    let playback = async {
        loop {
            let pid = f.app.media.stats("owned").await["pid"]
                .as_u64()
                .unwrap_or(0);
            if pid > 0 && std::path::Path::new(&format!("/proc/{pid}")).exists() {
                live_pids.insert(pid);
            }
            tokio::select! {
                status = child.wait() => break status,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    };
    let status = match tokio::time::timeout(Duration::from_secs(20), playback).await {
        Ok(status) => Some(status.unwrap()),
        Err(_) => {
            child.kill().await.unwrap();
            let _ = child.wait().await;
            None
        }
    };
    let pulls = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p == "/owned/m4f")
        .count();
    cancel.cancel();
    listener.await.unwrap().unwrap();
    f.stop().await;
    assert!(
        status.is_some_and(|s| s.success()),
        "independent RTSP decoder failed: {}",
        std::fs::read_to_string(path).unwrap()
    );
    assert_eq!(pulls, 1);
    assert!(
        !live_pids.is_empty(),
        "real playback must witness a live packager"
    );
    for pid in live_pids {
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}

#[path = "vendor_daemon_playback/srt.rs"]
mod srt;

#[tokio::test]
async fn large_catalog_concurrent_playback_renews_one_pull_and_closes_on_removal() {
    use futures_util::FutureExt;
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    use std::sync::atomic::Ordering;

    let hash = format!("{:x}", Sha256::digest(b"catalog-viewer"));
    let f = Fixture::new(json!({"on_play":"true","flussonix_token_sha256":hash})).await;
    let mut rows: Vec<Value> = (0..950)
        .map(|i| json!({"name":format!("filler-{i:04}"),"groups":[],"named_by":"config","static":false}))
        .collect();
    rows.push(json!({"name":"owned","groups":[],"named_by":"config","static":false}));
    *f.remote.lock().unwrap() = json!(rows);
    // Twelve old80-row pages exceed the actual daemon's five-second budget.
    // The selected target is deliberately the last row, never a partial result.
    f.page_delay.store(600, Ordering::SeqCst);
    let outcome = std::panic::AssertUnwindSafe(async {
        let responses = futures_util::future::join_all(
            (0..8).map(|_| f.get("/owned/m4s?token=catalog-viewer")),
        ).await;
        let mut bodies = Vec::new();
        for response in responses {
            assert_eq!(response.status(), StatusCode::OK, "complete catalog must admit the final row before the daemon deadline");
            let mut body = response.into_body();
            let tracks = native_info(&mut body).await;
            assert!(tracks.iter().any(|track| track.codec == "mp3"));
            bodies.push(body);
        }
        let worker_pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
        assert_eq!(f.media_calls(), 1);
        assert_eq!(*f.page_limits.lock().unwrap(), [1000]);
        assert_eq!(f.get("/owned/m4s?token=wrong").await.status(), StatusCode::FORBIDDEN);
        assert_eq!(*f.page_limits.lock().unwrap(), [1000]);

        let until = tokio::time::Instant::now() + Duration::from_secs(32);
        let drained = futures_util::future::join_all(bodies.into_iter().map(|mut body| async move {
            let mut chunks = 0;
            let mut last_data = tokio::time::Instant::now();
            loop {
                if tokio::time::Instant::now() >= until {
                    break;
                }
                let read_deadline = until.min(last_data + Duration::from_secs(2));
                match tokio::time::timeout_at(read_deadline, body.frame()).await {
                    Err(_) if tokio::time::Instant::now() >= until => break,
                    Err(_) => panic!("viewer stopped receiving data for two seconds"),
                    Ok(Some(Ok(frame))) => {
                        if let Ok(data) = frame.into_data() {
                            assert!(!data.is_empty());
                            let received_at = tokio::time::Instant::now();
                            assert!(received_at.duration_since(last_data) < Duration::from_secs(2));
                            chunks += 1;
                            last_data = received_at;
                        }
                    }
                    other => panic!("viewer closed before the renewed source deadline: {other:?}"),
                }
            }
            assert!(chunks > 100, "viewer did not receive sustained media");
            assert!(last_data.elapsed() < Duration::from_secs(2));
            (body, chunks)
        })).await;
        assert_eq!(f.app.media.stats("owned").await["pid"], worker_pid);
        assert_eq!(f.media_calls(), 1);
        assert_eq!(*f.page_limits.lock().unwrap(), [1000, 1000]);
        let config_requests = f.calls.lock().unwrap().iter().filter(|(path, _)| path.ends_with("/config")).count();
        assert_eq!(config_requests, 4, "one bracketed discovery plus one shared renewal");

        let manifest = playlist(&f, "/owned/index.m3u8?token=catalog-viewer").await;
        let segment = manifest.lines().find(|line| !line.is_empty() && !line.starts_with('#')).unwrap();
        let response = f.get(&format!("/owned/{segment}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let media = bytes(response).await;
        assert!(!media.is_empty());
        assert_eq!(f.get(&format!("/owned/{}", segment.replace("token=catalog-viewer", "token=wrong"))).await.status(), StatusCode::FORBIDDEN);
        let path = f._dir.path().join("large-catalog.ts");
        std::fs::write(&path, media).unwrap();
        let config_before_removal = f.calls.lock().unwrap().iter().filter(|(path, _)| path.ends_with("/config")).count();
        let pages_before_removal = f.page_limits.lock().unwrap().len();
        // Both one-page refreshes completed their config brackets; playback
        // beyond the original lease establishes that the second was committed.
        assert_eq!(config_before_removal, 4);
        assert_eq!(pages_before_removal, 2);
        let last_good_snapshot = f.page_snapshots.lock().unwrap().last().copied().unwrap();
        assert!(last_good_snapshot.1);
        let removed_at = tokio::time::Instant::now();
        f.remote.lock().unwrap().as_array_mut().unwrap().retain(|row| row["name"] != "owned");
        let counts: Vec<_> = drained.iter().map(|(_, count)| *count).collect();
        let decode = async {
        let decoded_path = f._dir.path().join("large-catalog.pcm");
        let mut decoder = tokio::process::Command::new("ffmpeg")
            .args(["-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i"])
            .arg(&path)
            .args(["-map", "0:a:0", "-frames:a", "10", "-f", "s16le"])
            .arg(&decoded_path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true).spawn().unwrap();
        let status = match tokio::time::timeout(Duration::from_secs(10), decoder.wait()).await {
            Ok(result) => result.unwrap(),
            Err(_) => {
                decoder.kill().await.unwrap();
                let _ = decoder.wait().await;
                panic!("owned strict decoder timed out");
            }
        };
        assert!(status.success(), "independent strict HLS audio decode failed");
        let decoded_audio_bytes = std::fs::metadata(&decoded_path).unwrap().len();
        assert!(decoded_audio_bytes > 0 && decoded_audio_bytes <= 1024 * 1024);
        let decoder_pid_absent = decoder.id().is_none();
        assert!(decoder_pid_absent, "strict decoder was not reaped");
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&decoded_path).unwrap();
        (decoded_audio_bytes, decoder_pid_absent)
        };
        let close = async {
        tokio::time::timeout(Duration::from_secs(22), futures_util::future::join_all(drained.into_iter().map(|(mut body, _)| async move {
                loop {
                    match body.frame().await {
                        Some(Ok(_)) => {}
                        None => break (tokio::time::Instant::now(), false),
                        Some(Err(_)) => break (tokio::time::Instant::now(), true),
                    }
                }
            }))).await.expect("catalog removal must close every admitted viewer at shared renewal")
        };
        let ((decoded_audio_bytes, decoder_pid_absent), terminals) = tokio::join!(decode, close);
        let closed_at = terminals.iter().map(|(time, _)| *time).max().unwrap();
        // Inspect the completed removal refresh before a playback GET could discover it.
        let config_after_removal = f.calls.lock().unwrap().iter().filter(|(path, _)| path.ends_with("/config")).count();
        assert_eq!(config_after_removal, config_before_removal + 2);
        assert_eq!(f.page_limits.lock().unwrap().len(), pages_before_removal + 1);
        let absent_snapshot = f.page_snapshots.lock().unwrap().last().copied().unwrap();
        assert!(!absent_snapshot.1 && absent_snapshot.0 >= removed_at);
        assert!(terminals.iter().all(|(time, _)| *time >= absent_snapshot.0), "a response ended before catalog removal was observed");
        let removal_close_delay = closed_at.checked_duration_since(absent_snapshot.0).unwrap();
        assert!(removal_close_delay < Duration::from_secs(2));
        // A 30-second lease starts before the at-most-five-second lookup.
        // It cannot expire earlier than 25 seconds after that good snapshot.
        let earliest_expiry = last_good_snapshot.0 + Duration::from_secs(25);
        assert!(closed_at < earliest_expiry, "lease expiry must not satisfy the removal check");
        assert_eq!(f.get("/owned/index.m3u8?token=catalog-viewer").await.status(), StatusCode::NOT_FOUND);
        assert_eq!(f.media_calls(), 1, "removal must not retry private media");
        json!({"responses":8,"initial_rows":951,"private_pulls":1,"worker_pid":worker_pid,"sustained_chunks_per_response":counts,"config_requests_before_removal":config_requests,"catalog_removal_closed_all_responses":true,"strict_hls_audio_decode":true,"decoded_audio_bytes":decoded_audio_bytes,"decoder_reaped":decoder_pid_absent,"removal_refresh_config_delta":config_after_removal-config_before_removal,"removal_refresh_page_delta":1,"removal_close_delay_ms":removal_close_delay.as_millis(),"closure_before_expiry_bound_ms":earliest_expiry.duration_since(closed_at).as_millis(),"maximum_response_idle_ms":2000,"terminal_body_errors":terminals.iter().filter(|(_,error)|*error).count()})
    }).catch_unwind().await;
    // Assertions inside the exercise must not abandon the owner or media child.
    f.stop().await;
    match outcome {
        Ok(report) => {
            let pid = report["worker_pid"].as_u64().unwrap();
            assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
            println!("large_catalog_qualification={report}");
        }
        Err(error) => std::panic::resume_unwind(error),
    }
}

#[tokio::test]
async fn omitted_groups_daemon_playback_keeps_viewer_token_guard_and_one_shared_pull() {
    let hash = ring::digest::digest(&ring::digest::SHA256, b"owned-group-viewer")
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let f = Fixture::new(json!({"flussonix_token_sha256":hash})).await;
    *f.remote.lock().unwrap() = json!([{"name":"owned","named_by":"config","static":false}]);
    assert_eq!(
        f.get("/owned/m4s?token=wrong").await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(f.media_calls(), 0);
    let first = f.get("/owned/m4s?token=owned-group-viewer").await;
    let second = f.get("/owned/m4s?token=owned-group-viewer").await;
    let statuses = (first.status(), second.status());
    if statuses != (StatusCode::OK, StatusCode::OK) {
        drop(first);
        drop(second);
        f.stop().await;
        assert_eq!(statuses, (StatusCode::OK, StatusCode::OK));
        return;
    }
    let mut first = first.into_body();
    let mut second = second.into_body();
    assert_eq!(
        native_info(&mut first).await,
        native_info(&mut second).await
    );
    assert_eq!(f.media_calls(), 1);
    drop(first);
    drop(second);
    f.stop().await;
}

#[tokio::test]
async fn omitted_groups_new_group_override_retires_existing_session_without_repull() {
    use http_body_util::BodyExt;
    let f = Fixture::new(json!({"on_play":"true"})).await;
    *f.remote.lock().unwrap() = json!([{"name":"owned","named_by":"config","static":false}]);
    let live = f.get("/owned/m4s?token=owned").await;
    let status = live.status();
    if status != StatusCode::OK {
        drop(live);
        f.stop().await;
        assert_eq!(status, StatusCode::OK);
        return;
    }
    let mut body = live.into_body();
    assert!(!native_info(&mut body).await.is_empty());
    assert_eq!(f.media_calls(), 1);
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"group_config":{"protected":{"on_play":"https://other.invalid/check"}}}),
        )
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(3), async {
        while body.frame().await.is_some() {}
    })
    .await
    .is_ok();
    let denied = f.get("/owned/m4s?token=owned").await.status();
    let pulls = f.media_calls();
    drop(body);
    f.stop().await;
    assert!(
        closed,
        "nonuniform group edit must fence the existing session"
    );
    assert_eq!(denied, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(pulls, 1);
}

// A valid input can become ready after the old HTTP wait but before its worker
// startup budget. The first request must receive the resulting HLS, without a
// replacement pull or worker. This would fail with the fixed eight-second wait.
#[tokio::test]
async fn delayed_vendor_hls_startup_uses_remaining_configured_budget() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.app
        .config
        .put(
            "templates",
            "slow-start",
            json!({"flussonix_input_timeout":20}),
        )
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"slow-start"}),
        )
        .unwrap();
    f.first_media_delay
        .store(9000, std::sync::atomic::Ordering::SeqCst);
    let began = std::time::Instant::now();
    let response = f.get("/owned/fmp4/index.m3u8?token=slow").await;
    let status = response.status();
    let text = String::from_utf8(bytes(response).await.to_vec()).unwrap();
    let elapsed = began.elapsed();
    let stats = f.app.media.stats("owned").await;
    let pid = stats["pid"].as_u64().unwrap();
    let pulls = f.media_calls();
    let ts_status = if status == StatusCode::OK {
        f.get("/owned/index.m3u8?token=slow").await.status()
    } else {
        status
    };
    f.stop().await;
    assert_eq!(status, StatusCode::OK, "first delayed manifest: {text}");
    assert_eq!(ts_status, StatusCode::OK);
    assert!(text.contains("#EXTINF:") && text.contains("#EXT-X-MAP:"));
    assert!(elapsed >= Duration::from_secs(9) && elapsed < Duration::from_secs(12));
    assert_eq!(pulls, 1);
    assert_eq!(stats["restart_count"], 0);
    assert!(stats["last_error"].is_null());
    assert!(pid > 0 && !std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn delayed_vendor_hls_startup_still_stops_on_short_worker_timeout() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.app
        .config
        .put(
            "templates",
            "short-start",
            json!({"flussonix_input_timeout":1}),
        )
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"short-start"}),
        )
        .unwrap();
    f.first_media_delay
        .store(9000, std::sync::atomic::Ordering::SeqCst);
    let began = std::time::Instant::now();
    let status = f.get("/owned/index.m3u8?token=short").await.status();
    let elapsed = began.elapsed();
    let stats = f.app.media.stats("owned").await;
    let pulls = f.media_calls();
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(elapsed < Duration::from_secs(3));
    assert_eq!(stats["pid"], 0);
    assert_eq!(stats["last_error"], "startup_timeout");
    assert_eq!(pulls, 1);
}

#[tokio::test]
async fn delayed_vendor_hls_startup_is_fenced_by_source_edit() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.app
        .config
        .put(
            "templates",
            "slow-start",
            json!({"flussonix_input_timeout":20}),
        )
        .unwrap();
    f.app
        .config
        .put(
            "sources",
            &f.source,
            json!({"flussonix_output_template":"slow-start"}),
        )
        .unwrap();
    f.first_media_delay
        .store(9000, std::sync::atomic::Ordering::SeqCst);
    let request = f.get("/owned/index.m3u8?token=edit");
    let edit = async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while f.media_calls() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        f.app
            .config
            .put("sources", &f.source, json!({"except":["owned"]}))
            .unwrap();
        std::time::Instant::now()
    };
    let (response, edited) = tokio::join!(request, edit);
    let status = response.status();
    let delay = edited.elapsed();
    let pulls = f.media_calls();
    let pid = f.app.media.stats("owned").await["pid"].clone();
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(delay < Duration::from_secs(2));
    assert_eq!(pid, 0);
    assert_eq!(pulls, 1);
}

// A complete, valid catalog that takes longer than the old3s allowance must
// still reach viewer authorization without admitting partial inventory/media.
#[tokio::test]
async fn slow_vendor_catalog_completes_before_viewer_admission() {
    use sha2::{Digest, Sha256};
    let f=Fixture::new(json!({"on_play":"true","flussonix_token_sha256":format!("{:x}",Sha256::digest(b"catalog-slow"))})).await;
    f.page_delay
        .store(3500, std::sync::atomic::Ordering::SeqCst);
    let began = std::time::Instant::now();
    let wrong = f.get("/owned/index.m3u8?token=wrong").await.status();
    let elapsed = began.elapsed();
    let before = f.media_calls();
    let after_configs = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.ends_with("/config"))
        .count();
    let allowed = if wrong == StatusCode::FORBIDDEN {
        f.get("/owned/m4s?token=catalog-slow").await.status()
    } else {
        wrong
    };
    let configs_after_allowed = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _)| p.ends_with("/config"))
        .count();
    let pages_after_allowed = f.page_limits.lock().unwrap().len();
    let pulls = f.media_calls();
    let pid = f.app.media.stats("owned").await["pid"]
        .as_u64()
        .unwrap_or(0);
    f.stop().await;
    assert_eq!(
        wrong,
        StatusCode::FORBIDDEN,
        "valid complete catalog must reach independent viewer denial"
    );
    assert!(elapsed >= Duration::from_millis(3500) && elapsed < Duration::from_secs(6));
    assert_eq!(before, 0, "denied viewer must not start private media");
    assert_eq!(after_configs, 2, "both identity probes must complete");
    assert_eq!(allowed, StatusCode::OK);
    assert_eq!(
        configs_after_allowed, after_configs,
        "cached admission must not rediscover configuration"
    );
    assert_eq!(
        pages_after_allowed, 1,
        "cached admission must not refetch the catalog"
    );
    assert_eq!(pulls, 1);
    assert!(pid == 0 || !std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn slow_vendor_catalog_still_has_a_five_second_hard_deadline() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.page_delay
        .store(7000, std::sync::atomic::Ordering::SeqCst);
    let began = std::time::Instant::now();
    let status = f.get("/owned/index.m3u8?token=late").await.status();
    let elapsed = began.elapsed();
    let pulls = f.media_calls();
    let sessions = f.app.playback_auth.snapshots().len();
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(elapsed >= Duration::from_millis(4500) && elapsed < Duration::from_millis(6500));
    assert_eq!(pulls, 0);
    assert_eq!(sessions, 0);
}

#[tokio::test]
async fn slow_vendor_catalog_is_cancelled_by_runtime_shutdown() {
    let f = Fixture::new(json!({"on_play":"true"})).await;
    f.page_delay
        .store(3500, std::sync::atomic::Ordering::SeqCst);
    let app = f.app.clone();
    let request = tokio::spawn(async move {
        router(app)
            .oneshot(
                Request::builder()
                    .uri("/owned/index.m3u8?token=shutdown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.page_limits.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let began = std::time::Instant::now();
    f.cancel.cancel();
    let status = tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap();
    let elapsed = began.elapsed();
    let pulls = f.media_calls();
    f.stop().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(elapsed < Duration::from_secs(1));
    assert_eq!(pulls, 0);
}
