//! HTTP publishing exercised against an owned independent streaming receiver.
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::post,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::config::ConfigStore;
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod captions;
#[path = "support/http_gpu_push.rs"]
mod gpu;
#[allow(dead_code)]
#[path = "support/srt_subtitle_oracle.rs"]
mod subtitle_oracle;
#[path = "support/tls.rs"]
mod tls_fixture;
#[derive(Default)]
struct Capture {
    response_status: AtomicUsize,
    redirect: Mutex<Option<String>>,
    requests: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    publication_trace: Mutex<Option<Arc<Mutex<PublicationTrace>>>>,
    bodies: Mutex<Vec<Arc<Mutex<Vec<u8>>>>>,
    data: Mutex<Vec<u8>>,
    paths: Mutex<Vec<String>>,
    headers: Mutex<Vec<String>>,
}
struct Active(Arc<Capture>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
// Shared across the two destinations only for supervisor qualification.
#[derive(Default)]
struct PublicationTrace {
    retired_pid: u32,
    retired_encoder_alive_at_post: bool,
    generation_overlap: bool,
    active: std::collections::HashMap<usize, usize>,
}
struct PublicationGuard {
    trace: Arc<Mutex<PublicationTrace>>,
    generation: usize,
}
impl Drop for PublicationGuard {
    fn drop(&mut self) {
        *self
            .trace
            .lock()
            .unwrap()
            .active
            .get_mut(&self.generation)
            .unwrap() -= 1;
    }
}
#[derive(Clone)]
struct ReceiverState {
    capture: Arc<Capture>,
    cancel: CancellationToken,
}
async fn receive(State(s): State<ReceiverState>, r: Request<Body>) -> axum::response::Response {
    let generation = s.capture.requests.fetch_add(1, Ordering::SeqCst);
    s.capture.paths.lock().unwrap().push(r.uri().to_string());
    s.capture.headers.lock().unwrap().push(
        r.headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .into(),
    );
    assert_eq!(r.headers()["content-type"], "video/mp2t");
    assert_eq!(r.headers()["transfer-encoding"], "chunked");
    let status = s.capture.response_status.load(Ordering::SeqCst);
    if status != 0 {
        let status = StatusCode::from_u16(status as u16).unwrap();
        if let Some(target) = s.capture.redirect.lock().unwrap().clone() {
            return (status, [("Location", target)]).into_response();
        }
        return status.into_response();
    }
    // Sample ordering before consuming ANY replacement body bytes. Hold the
    // same trace lock for admission and drop across both HTTP destinations.
    let _publication = s
        .capture
        .publication_trace
        .lock()
        .unwrap()
        .clone()
        .map(|trace| {
            let mut state = trace.lock().unwrap();
            if generation > 0
                && state.retired_pid > 0
                && std::path::Path::new(&format!("/proc/{}", state.retired_pid)).exists()
            {
                state.retired_encoder_alive_at_post = true;
            }
            if state
                .active
                .iter()
                .any(|(old, count)| *old != generation && *count > 0)
            {
                state.generation_overlap = true;
            }
            *state.active.entry(generation).or_default() += 1;
            drop(state);
            PublicationGuard { trace, generation }
        });
    let active = s.capture.active.fetch_add(1, Ordering::SeqCst) + 1;
    s.capture.max_active.fetch_max(active, Ordering::SeqCst);
    let data_session = Arc::new(Mutex::new(Vec::new()));
    s.capture.bodies.lock().unwrap().push(data_session.clone());
    let _active = Active(s.capture.clone());
    let mut body = r.into_body().into_data_stream();
    loop {
        tokio::select! { biased; _=s.cancel.cancelled()=>break, chunk=body.next()=>match chunk {
            Some(Ok(bytes)) => {
                for capture in [&s.capture.data, data_session.as_ref()] {
                    let mut data = capture.lock().unwrap();
                    let n = bytes.len().min((4*1024*1024usize).saturating_sub(data.len()));
                    data.extend_from_slice(&bytes[..n]);
                }
            },
            _=>break,
        } }
    }
    StatusCode::OK.into_response()
}
struct Receiver {
    url: String,
    capture: Arc<Capture>,
    cancel: CancellationToken,
    task: Option<AbortOnDropHandle<()>>,
    cert: Option<tls_fixture::Certificates>,
}
impl Receiver {
    async fn new(secure: bool) -> Self {
        Self::with_cert(secure.then(tls_fixture::Certificates::new), "127.0.0.1:0").await
    }
    async fn with_cert(cert: Option<tls_fixture::Certificates>, bind: &str) -> Self {
        let secure = cert.is_some();
        let tcp = TcpListener::bind(bind).await.unwrap();
        let address = tcp.local_addr().unwrap();
        let capture = Arc::new(Capture::default());
        let cancel = CancellationToken::new();
        let app = Router::new()
            .route("/{*path}", post(receive))
            .with_state(ReceiverState {
                capture: capture.clone(),
                cancel: cancel.clone(),
            });
        let stop = cancel.clone();
        let task = if let Some(cert) = &cert {
            let tls = cert.server();
            AbortOnDropHandle::new(tokio::spawn(async move {
                axum::serve(flussonix::http_tls::Listener::new(tcp, tls), app)
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .await
                    .unwrap();
            }))
        } else {
            AbortOnDropHandle::new(tokio::spawn(async move {
                axum::serve(tcp, app)
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .await
                    .unwrap();
            }))
        };
        Self {
            url: format!("{}://{address}", if secure { "https" } else { "http" }),
            capture,
            cancel,
            task: Some(task),
            cert,
        }
    }
    async fn stop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            tokio::time::timeout(Duration::from_secs(6), task)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(self.capture.active.load(Ordering::SeqCst), 0);
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
#[test]
fn http_push_configuration_roundtrips_inherits_and_rejects_invalid_options() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    let entries = json!([{"url":"tshttp://user:p%3Aa%40ss%20%26%C3%BC@localhost:19990/region/owned?token=owned-secret","disabled":true,"connect_timeout":2,"retry_timeout":7},{"url":"https://localhost:18443/owned","disabled":true},{"url":"srt://localhost:19991","disabled":true},{"url":"rtsp://localhost/owned","disabled":true}]);
    store
        .put("templates", "owned", json!({"pushes":entries}))
        .expect("HTTP MPEG-TS push must be accepted");
    store
        .put(
            "streams",
            "owned",
            json!({"template":"owned","inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    assert_eq!(store.effective("owned").unwrap()["pushes"], entries);
    assert_eq!(
        ConfigStore::open(dir.path().join("config.json"))
            .unwrap()
            .snapshot(),
        store.snapshot()
    );
    for invalid in [
        json!({"url":"http://localhost:0/owned"}),
        json!({"url":"http://localhost/owned#owned-secret"}),
        json!({"url":"http://localhost/%GGowned-secret"}),
        json!({"url":"http://:owned-secret@localhost/owned"}),
        json!({"url":"http://u%3Ax:owned-secret@localhost/owned"}),
        json!({"url":"http://u:p%0Aowned-secret@localhost/owned"}),
        json!({"url":"http://localhost/owned","passphrase":"owned-secret"}),
        json!({"url":"http://localhost/owned","flussonix_tls_ca":"/missing/owned-secret"}),
        json!({"url":"https://localhost/owned","flussonix_tls_ca":"relative-owned-secret"}),
        json!({"url":"http://localhost/owned","connect_timeout":0}),
        json!({"url":"http://localhost/owned","retry_timeout":301}),
        json!({"url":"http://localhost/owned","disabled":"true"}),
    ] {
        let before = store.snapshot();
        let error = store
            .put(
                "streams",
                "bad",
                json!({"inputs":[{"url":"testsrc://"}],"pushes":[invalid]}),
            )
            .unwrap_err();
        assert!(!error.contains("owned-secret"));
        assert_eq!(store.snapshot(), before);
    }
    store.put("streams", "owned", json!({"pushes":[]})).unwrap();
    assert_eq!(store.effective("owned").unwrap()["pushes"], json!([]));
}
async fn wait_capture(capture: &Capture) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if capture.data.lock().unwrap().len() > 250000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("actual POST media must reach independent receiver");
}
async fn decode(path: &std::path::Path, video: &str, audio: &str) {
    let p = tokio::process::Command::new("ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(path)
        .output()
        .await
        .unwrap();
    assert!(p.status.success());
    let value: Value = serde_json::from_slice(&p.stdout).unwrap();
    let tracks = value["streams"].as_array().unwrap();
    assert!(tracks.iter().any(|s| s["codec_name"] == video));
    assert!(tracks.iter().any(|s| s["codec_name"] == audio));
    let p = tokio::process::Command::new("ffmpeg")
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(path)
        .args([
            "-map", "0:v:0", "-map", "0:a:0", "-t", "1", "-f", "null", "-",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        p.status.success() && p.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&p.stderr)
    );
}
#[tokio::test]
async fn http_and_verified_https_push_real_media_with_basic_and_shared_worker() {
    for secure in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut receiver = Receiver::new(secure).await;
        let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
        let outcome = std::panic::AssertUnwindSafe(async {
            let mut u = url::Url::parse(&receiver.url).unwrap();
            u.set_username("user").unwrap();
            u.set_password(Some("p:a@ss &ü")).unwrap();
            u.set_path("/region/owned/mpegts");
            u.set_query(Some("token=owned-secret"));
            let mut destination = json!({"url":u.to_string(),"retry_timeout":1});
            if let Some(cert) = &receiver.cert {
                destination["flussonix_tls_ca"] = json!(cert.ca);
            }
            let cfg =
                json!({"static":false,"inputs":[{"url":"testsrc://"}],"pushes":[destination]});
            let worker = engine
                .ensure("owned", &cfg)
                .await
                .expect("HTTP push must start");
            wait_capture(&receiver.capture).await;
            assert_eq!(engine.count().await, 1);
            assert_eq!(
                engine.ensure("owned", &cfg).await.unwrap().pid(),
                worker.pid()
            );
            let stats = worker.stats()["flussonix_pushes"][0].clone();
            assert_eq!(stats["status"], "sending");
            assert_eq!(stats["pid"], 0);
            assert!(stats["body_bytes"].as_u64().unwrap() > 0);
            assert!(!stats.to_string().contains("owned-secret"));
            assert!(!stats.to_string().contains("p:a@ss"));
            assert_eq!(
                receiver.capture.headers.lock().unwrap().as_slice(),
                [format!("Basic {}", STANDARD.encode("user:p:a@ss &ü"))]
            );
            assert_eq!(
                receiver.capture.paths.lock().unwrap().as_slice(),
                ["/region/owned/mpegts?token=owned-secret"]
            );
            assert!(engine.http_push_egress.load(Ordering::Relaxed) > 0);
            assert_eq!(engine.rtsp_push_egress.load(Ordering::Relaxed), 0);
            engine.stop_all().await;
            tokio::time::timeout(Duration::from_secs(2), async {
                while receiver.capture.active.load(Ordering::SeqCst) > 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("stop must close upload without cancelling receiver");
            let stopped_bytes = engine.http_push_egress.load(Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert_eq!(
                engine.http_push_egress.load(Ordering::Relaxed),
                stopped_bytes
            );
            let bytes = receiver.capture.data.lock().unwrap().clone();
            let path = dir.path().join("received.ts");
            std::fs::write(&path, &bytes[..bytes.len() / 188 * 188]).unwrap();
            decode(&path, "h264", "aac").await;
            assert_eq!(worker.stats()["flussonix_pushes"][0]["status"], "stopped");
        })
        .catch_unwind()
        .await;
        engine.stop_all().await;
        receiver.stop().await;
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    }
}
async fn wait_retry(worker: &flussonix::media::Worker, index: usize, reason: &str) {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let stats = worker.stats()["flussonix_pushes"][index].clone();
            if stats["last_error"] == reason && stats["attempts"].as_u64().unwrap() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("destination must visibly retry with sanitized reason");
}
#[tokio::test]
async fn rejected_redirected_and_early_success_responses_retry_without_affecting_another_destination()
 {
    for (code, reason) in [
        (401, "push_auth_denied"),
        (403, "push_auth_denied"),
        (302, "push_redirect_refused"),
        (200, "push_response_ended"),
        (503, "push_rejected"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut faulty = Receiver::new(false).await;
        let mut healthy = Receiver::new(false).await;
        let mut foreign = Receiver::new(false).await;
        faulty.capture.response_status.store(code, Ordering::SeqCst);
        *faulty.capture.redirect.lock().unwrap() =
            Some(format!("{}/foreign?token=owned-secret", foreign.url));
        let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
        let outcome=std::panic::AssertUnwindSafe(async {
            let cfg=json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("{}/failed?token=owned-secret",faulty.url),"retry_timeout":1},{"url":format!("{}/healthy",healthy.url)}]});
            let worker=engine.ensure("owned",&cfg).await.unwrap();wait_capture(&healthy.capture).await;wait_retry(&worker,0,reason).await;
            assert_eq!(engine.count().await,1);assert_eq!(worker.stats()["flussonix_pushes"][1]["status"],"sending");assert!(faulty.capture.requests.load(Ordering::SeqCst)>=2);assert_eq!(foreign.capture.requests.load(Ordering::SeqCst),0);assert_eq!(faulty.capture.data.lock().unwrap().len(),0);assert!(!worker.stats().to_string().contains("owned-secret"));
        }).catch_unwind().await;
        engine.stop_all().await;
        faulty.stop().await;
        healthy.stop().await;
        foreign.stop().await;
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    }
}
#[tokio::test]
async fn https_trust_identity_and_expiry_fail_before_publishing_credentials_or_media() {
    for reason in ["untrusted", "identity", "expired"] {
        let cert = tls_fixture::Certificates::new();
        if reason == "expired" {
            cert.expire();
        }
        let mut receiver = Receiver::with_cert(
            Some(cert),
            if reason == "identity" {
                "127.0.0.2:0"
            } else {
                "127.0.0.1:0"
            },
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
        let outcome=std::panic::AssertUnwindSafe(async {
            let mut destination=json!({"url":receiver.url.replace("://","://user:owned-secret@")+"/owned?token=owned-secret","connect_timeout":1,"retry_timeout":1});
            if reason!="untrusted" {destination["flussonix_tls_ca"]=json!(receiver.cert.as_ref().unwrap().ca);}
            let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"pushes":[destination]})).await.unwrap();wait_retry(&worker,0,"push_connection_failed").await;
            assert_eq!(receiver.capture.requests.load(Ordering::SeqCst),0);assert_eq!(engine.http_push_egress.load(Ordering::Relaxed),0);assert!(!worker.stats().to_string().contains("owned-secret"));
        }).catch_unwind().await;
        engine.stop_all().await;
        receiver.stop().await;
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    }
}
#[tokio::test]
async fn both_http_schemes_preserve_all_six_cpu_video_audio_pairs() {
    for secure in [false, true] {
        for (video, encoder) in [("h264", "libx264"), ("hevc", "libx265")] {
            for (audio, acodec, ab) in [
                ("aac", "aac", 96),
                ("mp2", "mp2a", 192),
                ("mp3", "mp3", 128),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let mut receiver = Receiver::new(secure).await;
                let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
                let outcome=std::panic::AssertUnwindSafe(async {
            let mut destination=json!({"url":receiver.url.replace("http:","tshttp:").replace("https:","tshttps:")+"/owned"});if let Some(cert)=&receiver.cert {destination["flussonix_tls_ca"]=json!(cert.ca);}
            let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":encoder,"acodec":acodec,"ab":ab},"pushes":[destination]})).await.unwrap();wait_capture(&receiver.capture).await;assert_eq!(worker.stats()["flussonix_pushes"][0]["status"],"sending");engine.stop_all().await;
            let bytes=receiver.capture.data.lock().unwrap().clone();let path=dir.path().join("received.ts");std::fs::write(&path,&bytes[..bytes.len()/188*188]).unwrap();decode(&path,video,audio).await;
        }).catch_unwind().await;
                engine.stop_all().await;
                receiver.stop().await;
                if let Err(p) = outcome {
                    std::panic::resume_unwind(p);
                }
            }
        }
    }
}
#[test]
fn empty_userinfo_is_rejected_without_configuration_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    let before = store.snapshot();
    let error=store.put("streams","owned",json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":"http://@localhost/owned?token=owned-secret"}]})).unwrap_err();
    assert!(!error.contains("owned-secret"));
    assert_eq!(store.snapshot(), before);
}

#[tokio::test]
async fn http_and_https_preserve_regional_subtitles_and_apply_separate_track_filtering() {
    use flussonix::server::{App, Options};
    use subtitle_oracle as oracle;
    for hevc in [false, true] {
        for digital in [false, true] {
            let input = oracle::original::inject(&if hevc {
                captions::hevc_transport(digital)
            } else if digital {
                captions::digital_transport()
            } else {
                captions::transport()
            });
            let source_dir = tempfile::tempdir().unwrap();
            let source = source_dir.path().join("source.ts");
            std::fs::write(&source, &input).unwrap();
            let expected = oracle::caption_bodies(&source, hevc).await;
            assert!(expected.len() >= 4);
            for keep in [true, false] {
                let dir = tempfile::tempdir().unwrap();
                let mut plain = Receiver::new(false).await;
                let mut secure = Receiver::new(true).await;
                let app = App::new(
                    dir.path().join("config.json"),
                    dir.path().join("media"),
                    Options {
                        admin_password: "owned-http-subtitle-admin".into(),
                        peer_key: "owned-http-subtitle-peer".into(),
                        ..Default::default()
                    },
                )
                .unwrap();
                let outcome = std::panic::AssertUnwindSafe(async {
                    let mut cfg = json!({"static":false,"inputs":[{"url":"publish://"}],
                        "flussonix_subtitle_tracks":if keep {"preserve"}else{"drop"},
                        "flussonix_hls_subtitles":if keep {"convert"}else{"drop"},
                        "pushes":[{"url":plain.url.clone()+"/owned"},{"url":secure.url.clone()+"/owned","flussonix_tls_ca":secure.cert.as_ref().unwrap().ca}]});
                    if keep {cfg["flussonix_hls_captions"] = if digital {
                        json!([{"service":1,"language":"en","name":"English"}])
                    }else{json!([{"channel":1,"language":"en","name":"English"}])};}
                    app.config.put("streams","owned",cfg.clone()).unwrap();
                    let mut publication = app.media.publish_guarded("owned",&cfg,std::future::ready(true)).await.unwrap();
                    let feed = AbortOnDropHandle::new(tokio::spawn(captions::paced(publication.stdin.take().unwrap(),input.clone())));
                    tokio::time::sleep(Duration::from_secs(12)).await;
                    let words = if keep {Some(oracle::converted_words(&app,if digital {"s1.m3u8"}else{"cc1.m3u8"}).await)}else{None};
                    app.media.stop_all().await;
                    feed.abort();let _=feed.await;
                    assert_eq!(app.media.count().await,0);
                    for (i,receiver) in [&plain,&secure].into_iter().enumerate() {
                        let bytes=receiver.capture.data.lock().unwrap().clone();
                        assert!(bytes.len()>188*100);
                        let bytes=&bytes[..bytes.len()/188*188];
                        oracle::verify_original_tracks(bytes,keep);
                        let path=dir.path().join(format!("received-{i}.ts"));std::fs::write(&path,bytes).unwrap();
                        oracle::verify_codecs(&path,hevc).await;
                        let actual=oracle::caption_bodies(&path,hevc).await;
                        assert!(expected.is_subset(&actual),"HTTP push must retain every authored CEA command independently of HLS conversion and separate subtitle track filtering");
                        let complete=dir.path().join(format!("complete-{i}.ts"));std::fs::write(&complete,oracle::complete_sample(bytes)).unwrap();
                        let decoded=oracle::strict_decode(&complete).await;
                        assert!(oracle::clean_decode(&decoded),"{}",String::from_utf8_lossy(&decoded.stderr));
                    }
                    if let Some(words)=words {assert!(words.contains(if digital {"USA708"}else{"USA 608"}));}
                }).catch_unwind().await;
                app.media.stop_all().await;
                plain.stop().await;
                secure.stop().await;
                if let Err(p) = outcome {
                    std::panic::resume_unwind(p);
                }
            }
        }
    }
}
