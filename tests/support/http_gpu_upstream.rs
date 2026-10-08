//! Compressed, authenticated HTTPS upstreams: software decode + VAAPI encode.
use super::*;
use axum::routing::get;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

struct Source {
    url: String,
    cert: tls_fixture::Certificates,
    capture: Arc<Capture>,
    cancel: CancellationToken,
    task: Option<AbortOnDropHandle<()>>,
    file: PathBuf,
    report: Value,
    _dir: tempfile::TempDir,
}

fn input_header() -> String {
    format!("Basic {}", STANDARD.encode("owned-input:p:a@ss &ü"))
}

impl Source {
    async fn new(video: &str, audio: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("source.ts");
        let encoder = if video == "hevc" {
            "libx265"
        } else {
            "libx264"
        };
        let acodec = if audio == "mp3" { "libmp3lame" } else { audio };
        let mut cmd = tokio::process::Command::new("/usr/bin/ffmpeg");
        cmd.kill_on_drop(true).args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=640x360:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:sample_rate=48000",
            "-t",
            "16",
            "-c:v",
            encoder,
            "-threads",
            "2",
            "-preset",
            "ultrafast",
            "-g",
            "50",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            acodec,
            "-b:a",
            "192k",
        ]);
        if video == "hevc" {
            cmd.args(["-x265-params", "pools=1:frame-threads=1:log-level=error"]);
        }
        let output = tokio::time::timeout(
            Duration::from_secs(40),
            cmd.args(["-f", "mpegts"]).arg(&file).output(),
        )
        .await
        .expect("bounded owned source encoding")
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut report = decoded_with_video(&file, video, audio).await;
        report["sha256"] = json!(format!(
            "{:x}",
            Sha256::digest(std::fs::read(&file).unwrap())
        ));
        report["origin"] = json!(
            "independently pre-encoded lavfi video + sine; compressed 8-bit TS sent over HTTPS"
        );
        let data = Arc::new(std::fs::read(&file).unwrap());
        let delay = Duration::from_secs_f64(16.0 * (188 * 64) as f64 / data.len() as f64);
        let cert = tls_fixture::Certificates::new();
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let capture = Arc::new(Capture::default());
        let cap = capture.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let body_stop = cancel.clone();
        let routes = Router::new().route("/{*path}", get(move |req: Request<Body>| {
            let cap = cap.clone(); let data = data.clone(); let stop = body_stop.clone();
            async move {
                cap.requests.fetch_add(1,Ordering::SeqCst);
                cap.paths.lock().unwrap().push(req.uri().to_string());
                let auth = req.headers().get("authorization").and_then(|v|v.to_str().ok()).unwrap_or("").to_owned();
                cap.headers.lock().unwrap().push(auth.clone());
                assert!(!req.headers().contains_key("x-flussonix-peer"));
                if auth != input_header() {return StatusCode::UNAUTHORIZED.into_response();}
                cap.active.fetch_add(1,Ordering::SeqCst);
                let active = Active(cap);
                let body = futures_util::stream::unfold((0usize,data,stop,active), move |(at,data,stop,active)| async move {
                    if at >= data.len() { stop.cancelled().await; return None; }
                    tokio::select!{biased; _=stop.cancelled()=>return None, _=tokio::time::sleep(delay)=>{}}
                    let end=(at+188*64).min(data.len());
                    let bytes=Bytes::copy_from_slice(&data[at..end]);
                    Some((Ok::<_,std::io::Error>(bytes),(end,data,stop,active)))
                });
                ([("Content-Type","video/mp2t")],Body::from_stream(body)).into_response()
            }
        }));
        let tls = cert.server();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(flussonix::http_tls::Listener::new(tcp, tls), routes)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
        }));
        Self {
            url: format!("https://{address}"),
            cert,
            capture,
            cancel,
            task: Some(task),
            file,
            report,
            _dir: dir,
        }
    }
    fn input(&self) -> Value {
        let mut u = url::Url::parse(&format!(
            "{}/compressed/mpegts?token=owned-input-token",
            self.url
        ))
        .unwrap();
        u.set_username("owned-input").unwrap();
        u.set_password(Some("p:a@ss &ü")).unwrap();
        json!({"url":u.as_str().replacen("https://","tshttps://",1),"flussonix_tls_ca":self.cert.ca})
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
impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn dependencies(worker: &flussonix::media::Worker) -> Value {
    let proc = PathBuf::from(format!("/proc/{}", worker.pid()));
    let maps = std::fs::read_to_string(proc.join("maps")).unwrap();
    let env = std::fs::read(proc.join("environ")).unwrap();
    let selected: serde_json::Map<String, Value> = env
        .split(|b| *b == 0)
        .filter_map(|v| std::str::from_utf8(v).ok()?.split_once('='))
        .filter(|(k, _)| ["LIBVA_DRIVERS_PATH", "LD_LIBRARY_PATH", "LIBVA_DRIVER_NAME"].contains(k))
        .map(|(k, v)| (k.to_owned(), json!(v)))
        .collect();
    let expected_dir = PathBuf::from(
        std::env::var_os("LIBVA_DRIVERS_PATH").expect("owned independent driver environment"),
    );
    let mut records = vec![];
    for name in ["iHD_drv_video.so", "libigdgmm.so.12"] {
        let paths: HashSet<_> = maps
            .lines()
            .filter_map(|line| line.split_whitespace().last())
            .filter(|p| Path::new(p).file_name().is_some_and(|n| n == name))
            .collect();
        assert_eq!(paths.len(), 1, "one mapped independent {name}");
        let path = std::fs::canonicalize(paths.into_iter().next().unwrap()).unwrap();
        assert_eq!(
            path,
            std::fs::canonicalize(expected_dir.join(name)).unwrap()
        );
        records.push(json!({"path":path,"sha256":format!("{:x}",Sha256::digest(std::fs::read(&path).unwrap()))}));
    }
    json!({"mapped_dependencies":records,"captured_process_driver_environment":selected})
}

fn safe_diagnostics(worker: &flussonix::media::Worker, args: &[String]) {
    let text = format!("{} {:?}", worker.stats(), args);
    for secret in ["owned-input", "p:a@ss", "p%3A", "owned-publishing-only"] {
        assert!(!text.contains(secret));
    }
    assert!(!args.iter().any(|s| s == "lavfi" || s == "-hwaccel"));
    let input = args.windows(2).find(|v| v[0] == "-i").unwrap();
    let u = url::Url::parse(&input[1]).unwrap();
    assert_eq!(u.scheme(), "http");
    assert_eq!(u.host_str(), Some("127.0.0.1"));
    assert!(u.username().is_empty() && u.password().is_none());
}

fn retain_source(source: &Source, name: &str) {
    if let Some(root) = std::env::var_os("FLUSSONIX_HTTP_GPU_EVIDENCE_DIR") {
        let target = PathBuf::from(root).join(name);
        std::fs::create_dir_all(&target).unwrap();
        std::fs::copy(&source.file, target.join("source.ts")).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires independent Intel H.264 VAAPI renderD128 driver environment"]
async fn compressed_h264_and_hevc_https_inputs_publish_gpu_aac_mp2_mp3() {
    for (video, input_audio) in [("h264", "mp3"), ("hevc", "mp2")] {
        let mut source = Source::new(video, input_audio).await;
        for (audio, acodec, ab) in [
            ("aac", "aac", 96),
            ("mp2", "mp2a", 192),
            ("mp3", "mp3", 128),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
            let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
            let requests = source.capture.requests.load(Ordering::SeqCst);
            let outcome=std::panic::AssertUnwindSafe(async {
                let store=ConfigStore::open(dir.path().join("config.json")).unwrap();
                store.put("templates","upstream-gpu",json!({"transcoder":{"encoder":"h264_vaapi","qp":24,"acodec":acodec,"ab":ab},"pushes":receivers.iter().map(destination).collect::<Vec<_>>()})).unwrap();
                store.put("streams","owned-gpu",json!({"static":false,"template":"upstream-gpu","inputs":[source.input()]})).unwrap();
                let cfg=store.effective("owned-gpu").unwrap();
                let (worker,args)=generation(&engine,&receivers,&cfg,"h264_vaapi").await;
                safe_diagnostics(&worker,&args);let driver=dependencies(&worker);
                assert_eq!(source.capture.requests.load(Ordering::SeqCst),requests+1);
                assert!(source.capture.paths.lock().unwrap().iter().all(|p|p=="/compressed/mpegts?token=owned-input-token"));
                assert!(source.capture.headers.lock().unwrap().iter().all(|h|h==&input_header()));
                engine.stop_all().await;assert!(worker.is_closed());
                assert!(!Path::new(&format!("/proc/{}",worker.pid())).exists());
                tokio::time::timeout(Duration::from_secs(3),async{while source.capture.active.load(Ordering::SeqCst)!=0{tokio::time::sleep(Duration::from_millis(10)).await;}}).await.expect("input fetcher must close on stream stop");
                let count=engine.http_push_egress.load(Ordering::Relaxed);
                let outputs=evidence(&receivers,dir.path(),audio).await;
                assert_eq!(engine.http_push_egress.load(Ordering::Relaxed),count);
                assert_eq!(engine.count().await,0);
                let name=format!("upstream-{video}-{input_audio}-to-h264-{audio}");
                retain(&name,dir.path(),&json!({"source":source.report,"software_decode":true,"encoder":"h264_vaapi","arguments":args,"dependencies":driver,"outputs":outputs,"upstream_closed":true,"encoder_reaped":true}));
                retain_source(&source,&name);
            }).catch_unwind().await;
            engine.stop_all().await;
            for r in &mut receivers {
                r.stop().await;
            }
            if let Err(p) = outcome {
                source.stop().await;
                std::panic::resume_unwind(p);
            }
        }
        source.stop().await;
    }
}

#[tokio::test]
#[ignore = "requires independent Intel H.264 VAAPI renderD128 driver environment"]
async fn rejected_compressed_upstream_sends_no_media_and_reaps_encoder() {
    let mut source = Source::new("h264", "mp3").await;
    let wrong = tls_fixture::Certificates::new();
    for case in ["untrusted_ca", "wrong_basic"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
        let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
        let requests = source.capture.requests.load(Ordering::SeqCst);
        let outcome=std::panic::AssertUnwindSafe(async{
            let mut input=source.input();
            if case=="untrusted_ca"{input["flussonix_tls_ca"]=json!(wrong.ca);}else{
                let mut u=url::Url::parse(&input["url"].as_str().unwrap().replacen("tshttps://","https://",1)).unwrap();
                u.set_password(Some("wrong-owned-secret")).unwrap();
                input["url"]=json!(u.as_str().replacen("https://","tshttps://",1));
            }
            let cfg=json!({"static":false,"inputs":[input],"transcoder":{"encoder":"h264_vaapi"},"pushes":receivers.iter().map(destination).collect::<Vec<_>>()});
            let worker=engine.ensure("owned-gpu",&cfg).await.unwrap();
            tokio::time::timeout(Duration::from_secs(12),async{while worker.alive.load(Ordering::Relaxed){tokio::time::sleep(Duration::from_millis(20)).await;}}).await.expect("denied upstream closes encoder");
            for r in &receivers{assert!(r.capture.requests.load(Ordering::SeqCst)<=1);assert!(r.capture.data.lock().unwrap().is_empty());}
            assert_eq!(worker.stats()["bytes_in"],0);
            assert_eq!(engine.http_push_egress.load(Ordering::Relaxed),0);
            let text=worker.stats().to_string();assert!(!text.contains("wrong-owned-secret") && !text.contains("owned-input"));
            let seen=source.capture.requests.load(Ordering::SeqCst)-requests;
            assert_eq!(seen,if case=="untrusted_ca"{0}else{1});
            engine.stop_all().await;assert!(!Path::new(&format!("/proc/{}",worker.pid())).exists());
            for r in &receivers{closed(r).await;}
            assert_eq!(source.capture.active.load(Ordering::SeqCst),0);
            if let Some(root)=std::env::var_os("FLUSSONIX_HTTP_GPU_EVIDENCE_DIR"){
                let p=PathBuf::from(root).join(format!("upstream-denial-{case}.json"));
                std::fs::write(p,serde_json::to_vec_pretty(&json!({"case":case,"source_requests":seen,"output_requests":receivers.iter().map(|r|r.capture.requests.load(Ordering::SeqCst)).collect::<Vec<_>>(),"body_bytes":0,"encoder_reaped":true})).unwrap()).unwrap();
            }
        }).catch_unwind().await;
        engine.stop_all().await;
        for r in &mut receivers {
            r.stop().await;
        }
        if let Err(p) = outcome {
            source.stop().await;
            std::panic::resume_unwind(p);
        }
    }
    source.stop().await;
}
