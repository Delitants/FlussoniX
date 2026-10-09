//! Native M4 source -> software decode -> Intel VAAPI -> native HTTP publishing.
use super::*;
#[path = "http_gpu_native_recovery.rs"]
mod recovery;
use axum::routing::get;
use bytes::Bytes;
use flussonix::{
    m4f::Frame,
    m4s::Track,
    worker_output::{Decoder, Event},
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::PathBuf};

fn assert_frames(tracks: &[Track], expected: &[Frame], actual: &[Frame]) {
    assert_eq!(actual.len(), expected.len());
    for track in tracks {
        let samples = |frames: &[Frame]| {
            frames
                .iter()
                .filter(|f| f.track_id == track.id)
                .map(|f| (f.dts, f.pts_offset, f.key, f.body.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            samples(actual),
            samples(expected),
            "native track {}",
            track.id
        );
    }
}

struct Fixture {
    tracks: Vec<Track>,
    frames: Vec<Frame>,
    original: PathBuf,
    remux: PathBuf,
    report: Value,
    _dir: tempfile::TempDir,
}
impl Fixture {
    async fn new(video: &str, audio: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("independent-source.ts");
        let encoder = if video == "hevc" {
            "libx265"
        } else {
            "libx264"
        };
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
            if audio == "mp3" { "libmp3lame" } else { audio },
            "-b:a",
            "192k",
        ]);
        if video == "hevc" {
            cmd.args(["-x265-params", "pools=1:frame-threads=1:log-level=error"]);
        }
        let output = tokio::time::timeout(
            Duration::from_secs(40),
            cmd.args(["-f", "mpegts"]).arg(&original).output(),
        )
        .await
        .expect("bounded independent fixture encoder")
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let source = decoded_with_video(&original, video, audio).await;
        let bytes = std::fs::read(&original).unwrap();
        let mut decoder = Decoder::default();
        let mut events = vec![];
        for chunk in bytes.chunks(188 * 64) {
            events.extend(decoder.push(chunk).unwrap());
        }
        events.extend(decoder.finish().unwrap());
        let mut tracks = vec![];
        let mut frames = vec![];
        for event in events {
            match event {
                Event::Info(t) => {
                    assert!(tracks.is_empty());
                    tracks = t;
                }
                Event::Frame(f) => frames.push(f),
            }
        }
        assert_eq!(tracks.len(), 2);
        frames.sort_by_key(|f| {
            (
                f.dts,
                !tracks.iter().any(|t| {
                    t.id == f.track_id && matches!(t.codec.as_str(), "h264" | "hevc") && f.key
                }),
            )
        });
        let mut mux = flussonix::worker_ts::Muxer::new(&tracks).unwrap();
        let mut data = mux.tables();
        for frame in &frames {
            data.extend(mux.frame(frame).unwrap());
        }
        let remux = dir.path().join("native-remux.ts");
        std::fs::write(&remux, &data).unwrap();
        let native = decoded_with_video(&remux, video, audio).await;
        assert_eq!(native["video_frames"], source["video_frames"]);
        assert_eq!(native["audio_frames"], source["audio_frames"]);
        let report = json!({"original":source,"native_remux":native,"original_sha256":format!("{:x}",Sha256::digest(&bytes)),"native_remux_sha256":format!("{:x}",Sha256::digest(&data)),"native_tracks":tracks.iter().map(|t|json!({"id":t.id,"codec":t.codec,"config_bytes":t.config.len()})).collect::<Vec<_>>(),"origin":"independently pre-encoded lavfi/sine TS; FlussoniX native record packers; independently decoded original and native remux"});
        Self {
            tracks,
            frames,
            original,
            remux,
            report,
            _dir: dir,
        }
    }
    fn wire(&self, protocol: &str) -> (Vec<(Duration, Bytes)>, HashMap<String, Bytes>) {
        let start = self.frames.first().unwrap().dts;
        let mut chunks = vec![];
        let mut segments = HashMap::new();
        if protocol.starts_with("m4s") {
            chunks.push((
                Duration::ZERO,
                Bytes::from(flussonix::wire::encode_info(&self.tracks).unwrap()),
            ));
            for n in 0..81u64 {
                let mut data = vec![];
                for f in self
                    .frames
                    .iter()
                    .filter(|f| f.dts >= start + n * 18000 && f.dts < start + (n + 1) * 18000)
                {
                    data.extend(
                        flussonix::wire::encode_frame(
                            self.tracks.iter().find(|t| t.id == f.track_id).unwrap(),
                            f,
                        )
                        .unwrap(),
                    );
                }
                if !data.is_empty() {
                    chunks.push((Duration::from_millis(200), Bytes::from(data)));
                }
            }
            let mut decoder = flussonix::m4s::Decoder::default();
            let mut decoded = vec![];
            let mut infos = 0;
            for (_, bytes) in &chunks {
                for part in bytes.chunks(211) {
                    for event in decoder.push(part).unwrap() {
                        match event {
                            flussonix::m4s::Event::Info { tracks, .. } => {
                                assert_eq!(tracks, self.tracks);
                                infos += 1;
                            }
                            flussonix::m4s::Event::Frame {
                                track_id,
                                dts,
                                pts_offset,
                                key,
                                body,
                                ..
                            } => {
                                decoded.push(Frame {
                                    track_id,
                                    dts,
                                    pts_offset,
                                    key,
                                    body,
                                });
                            }
                            other => panic!("unexpected native fixture record: {other:?}"),
                        }
                    }
                }
            }
            assert_eq!(infos, 1);
            assert_frames(&self.tracks, &self.frames, &decoded);
        } else {
            for n in 0..9u64 {
                let part: Vec<_> = self
                    .frames
                    .iter()
                    .filter(|f| f.dts >= start + n * 180000 && f.dts < start + (n + 1) * 180000)
                    .cloned()
                    .collect();
                // A finite source can end with an audio-only tail. Serve only
                // complete two-track windows, keeping live metadata stable.
                if !self
                    .tracks
                    .iter()
                    .all(|t| part.iter().any(|f| f.track_id == t.id))
                {
                    continue;
                }
                let body = flussonix::m4f::pack(&self.tracks, &part, 180000).unwrap();
                let (unpacked, frames) = flussonix::m4f::unpack(&body).unwrap();
                assert_eq!(unpacked, self.tracks);
                assert_frames(&self.tracks, &part, &frames);
                let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
                    .unwrap()
                    .format("%Y/%m/%d/%H/%M/%S")
                    .to_string();
                segments.insert(format!("/native/{stamp}.m4f"), Bytes::from(body));
                chunks.push((
                    Duration::from_secs(2),
                    Bytes::from(format!("{n} {stamp}-2000\n")),
                ));
            }
        }
        assert!(!chunks.is_empty());
        (chunks, segments)
    }
    fn retain(&self, name: &str) {
        if let Some(root) = std::env::var_os("FLUSSONIX_HTTP_GPU_EVIDENCE_DIR") {
            let target = PathBuf::from(root).join(name);
            std::fs::create_dir_all(&target).unwrap();
            std::fs::copy(&self.original, target.join("source-original.ts")).unwrap();
            std::fs::copy(&self.remux, target.join("source-native-remux.ts")).unwrap();
        }
    }
}

fn input_header() -> String {
    format!("Basic {}", STANDARD.encode("native-input:p:a@ss &ü"))
}
struct Source {
    protocol: &'static str,
    disconnect: Arc<Mutex<CancellationToken>>,
    address: String,
    cert: Option<tls_fixture::Certificates>,
    capture: Arc<Capture>,
    cancel: CancellationToken,
    task: Option<AbortOnDropHandle<()>>,
}
impl Source {
    async fn new(fixture: &Fixture, protocol: &'static str) -> Self {
        let secure = matches!(protocol, "m4ss" | "m4fs");
        let (chunks, segments) = fixture.wire(protocol);
        let chunks = Arc::new(chunks);
        let segments = Arc::new(segments);
        let control = if protocol.starts_with("m4s") {
            "/native/m4s"
        } else {
            "/native/m4f"
        };
        let capture = Arc::new(Capture::default());
        let cap = capture.clone();
        let cancel = CancellationToken::new();
        let body_cancel = cancel.clone();
        let disconnect = Arc::new(Mutex::new(CancellationToken::new()));
        let request_disconnect = disconnect.clone();
        let routes = Router::new().route(
            "/{*path}",
            get(move |req: Request<Body>| {
                let cap = cap.clone();
                let chunks = chunks.clone();
                let segments = segments.clone();
                let cancel = body_cancel.clone();
                let disconnect = request_disconnect.lock().unwrap().clone();
                async move {
                    cap.requests.fetch_add(1, Ordering::SeqCst);
                    cap.paths.lock().unwrap().push(req.uri().to_string());
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_owned();
                    cap.headers.lock().unwrap().push(auth.clone());
                    if auth != input_header()
                        || req.uri().query() != Some("token=owned-native-token")
                        || req.headers().contains_key("x-flussonix-peer")
                    {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    if req.uri().path() == control {
                        cap.active.fetch_add(1, Ordering::SeqCst);
                        let active = Active(cap);
                        let body = futures_util::stream::unfold(
                            (0usize, chunks, cancel, disconnect, active),
                            |(at, chunks, cancel, disconnect, active)| async move {
                                if at >= chunks.len() {
                                    tokio::select! {
                                        _ = cancel.cancelled() => {},
                                        _ = disconnect.cancelled() => {},
                                    }
                                    return None;
                                }
                                tokio::select! {
                                    biased;
                                    _ = cancel.cancelled() => return None,
                                    _ = disconnect.cancelled() => return None,
                                    _ = tokio::time::sleep(chunks[at].0) => {}
                                }
                                Some((
                                    Ok::<_, std::io::Error>(chunks[at].1.clone()),
                                    (at + 1, chunks, cancel, disconnect, active),
                                ))
                            },
                        );
                        Body::from_stream(body).into_response()
                    } else if let Some(data) = segments.get(req.uri().path()) {
                        if cap.response_status.load(Ordering::SeqCst) == 403 {
                            return StatusCode::FORBIDDEN.into_response();
                        }
                        data.clone().into_response()
                    } else {
                        StatusCode::NOT_FOUND.into_response()
                    }
                }
            }),
        );
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap().to_string();
        let cert = secure.then(tls_fixture::Certificates::new);
        let tls = cert.as_ref().map(|c| c.server());
        let stop = cancel.clone();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            if let Some(tls) = tls {
                axum::serve(flussonix::http_tls::Listener::new(tcp, tls), routes)
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .await
                    .unwrap();
            } else {
                axum::serve(tcp, routes)
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .await
                    .unwrap();
            }
        }));
        Self {
            protocol,
            disconnect,
            address,
            cert,
            capture,
            cancel,
            task: Some(task),
        }
    }
    fn input(&self) -> Value {
        let mut u = url::Url::parse(&format!(
            "http://{}/native?token=owned-native-token",
            self.address
        ))
        .unwrap();
        u.set_username("native-input").unwrap();
        u.set_password(Some("p:a@ss &ü")).unwrap();
        let mut input =
            json!({"url":u.as_str().replacen("http://",&format!("{}://",self.protocol),1)});
        if let Some(cert) = &self.cert {
            input["flussonix_tls_ca"] = json!(cert.ca);
        }
        input
    }
    fn disconnect_current(&self) {
        let previous = std::mem::replace(
            &mut *self.disconnect.lock().unwrap(),
            CancellationToken::new(),
        );
        previous.cancel();
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
    let mut records = vec![];
    for (prefix, variable, default) in [
        (
            "iHD_drv_video.so",
            "FLUSSONIX_HTTP_GPU_IHD_FILE",
            "/usr/lib/x86_64-linux-gnu/dri/iHD_drv_video.so",
        ),
        (
            "libigdgmm.so.12",
            "FLUSSONIX_HTTP_GPU_GMM_FILE",
            "/usr/lib/x86_64-linux-gnu/libigdgmm.so.12",
        ),
    ] {
        let expected =
            std::fs::canonicalize(std::env::var(variable).unwrap_or(default.into())).unwrap();
        let paths: HashSet<_> = maps
            .lines()
            .filter_map(|l| l.split_whitespace().last())
            .filter(|p| {
                Path::new(p)
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with(prefix))
            })
            .collect();
        assert_eq!(paths.len(), 1);
        let actual = std::fs::canonicalize(paths.into_iter().next().unwrap()).unwrap();
        assert_eq!(actual, expected);
        assert!(!actual.starts_with("/opt/flussonic"));
        records.push(json!({"path":actual,"sha256":format!("{:x}",Sha256::digest(std::fs::read(&actual).unwrap()))}));
    }
    let env = std::fs::read(proc.join("environ")).unwrap();
    let selected: serde_json::Map<String, Value> = env
        .split(|b| *b == 0)
        .filter_map(|v| std::str::from_utf8(v).ok()?.split_once('='))
        .filter(|(k, _)| ["LIBVA_DRIVERS_PATH", "LIBVA_DRIVER_NAME", "LD_LIBRARY_PATH"].contains(k))
        .map(|(k, v)| (k.into(), json!(v)))
        .collect();
    json!({"mapped_dependencies":records,"captured_driver_environment":selected})
}

#[tokio::test]
async fn native_fixture_remux_strictly_decodes_h264_mp3_and_hevc_mp2() {
    for (video, audio) in [("h264", "mp3"), ("hevc", "mp2")] {
        let fixture = Fixture::new(video, audio).await;
        for protocol in ["m4s", "m4f"] {
            fixture.wire(protocol);
        }
    }
}

#[tokio::test]
#[ignore = "requires independent Intel H264 VAAPI renderD128 and installed iHD/GMM"]
async fn m4s_m4f_plain_and_verified_tls_inputs_publish_gpu_aac_mp2_mp3() {
    for (video, input_audio) in [("h264", "mp3"), ("hevc", "mp2")] {
        let fixture = Fixture::new(video, input_audio).await;
        for protocol in ["m4s", "m4ss", "m4f", "m4fs"] {
            let mut source = Source::new(&fixture, protocol).await;
            for (audio, acodec, ab) in [
                ("aac", "aac", 96),
                ("mp2", "mp2a", 192),
                ("mp3", "mp3", 128),
            ] {
                let dir = tempfile::tempdir().unwrap();
                let engine =
                    flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
                let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
                let before = source.capture.requests.load(Ordering::SeqCst);
                let outcome = std::panic::AssertUnwindSafe(async {
                    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
                    store.put("templates", "native-gpu", json!({
                        "transcoder": {"encoder": "h264_vaapi", "qp": 24, "acodec": acodec, "ab": ab},
                        "pushes": receivers.iter().map(destination).collect::<Vec<_>>()
                    })).unwrap();
                    store.put("streams", "owned-gpu", json!({
                        "static": false, "template": "native-gpu", "inputs": [source.input()]
                    })).unwrap();
                    let cfg = store.effective("owned-gpu").unwrap();
                    let (worker, args) = generation(&engine, &receivers, &cfg, "h264_vaapi").await;
                    let driver = dependencies(&worker);
                    let audio_encoder = if audio == "mp3" { "libmp3lame" } else { audio };
                    assert!(args.windows(2).any(|v| v == ["-c:a", audio_encoder]));
                    let bitrate = format!("{ab}k");
                    assert!(args.windows(2).any(|v| v == ["-b:a", bitrate.as_str()]));
                    assert!(args.windows(2).any(|v| v == ["-i", "pipe:0"]));
                    assert!(!args.iter().any(|v| v == "lavfi" || v == "-hwaccel"));
                    let diagnostics = format!("{} {:?}", worker.stats(), args);
                    for secret in ["native-input", "p:a@ss", "owned-native-token", "owned-publishing-only"] {
                        assert!(!diagnostics.contains(secret));
                    }
                    assert!(source.capture.requests.load(Ordering::SeqCst) > before);
                    assert_eq!(source.capture.active.load(Ordering::SeqCst), 1);
                    assert!(source.capture.paths.lock().unwrap().iter().all(|p| p.ends_with("?token=owned-native-token")));
                    assert!(source.capture.headers.lock().unwrap().iter().all(|h| h == &input_header()));
                    engine.stop_all().await;
                    assert!(worker.is_closed());
                    assert!(!Path::new(&format!("/proc/{}", worker.pid())).exists());
                    tokio::time::timeout(Duration::from_secs(3), async {
                        while source.capture.active.load(Ordering::SeqCst) != 0 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }).await.unwrap();
                    let count = engine.http_push_egress.load(Ordering::Relaxed);
                    let outputs = evidence(&receivers, dir.path(), audio).await;
                    assert_eq!(engine.http_push_egress.load(Ordering::Relaxed), count);
                    assert_eq!(engine.count().await, 0);
                    let name = format!("native-{protocol}-{video}-{input_audio}-to-h264-{audio}");
                    retain(&name, dir.path(), &json!({
                        "input_protocol": protocol, "source": fixture.report,
                        "software_decode": true, "encoder": "h264_vaapi", "arguments": args,
                        "dependencies": driver, "outputs": outputs,
                        "upstream_closed": true, "encoder_reaped": true
                    }));
                    fixture.retain(&name);
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
}

#[tokio::test]
#[ignore = "requires independent Intel H264 VAAPI renderD128 and installed iHD/GMM"]
async fn denied_native_control_or_segment_starts_no_media_encoder_or_media() {
    let fixture = Fixture::new("h264", "mp3").await;
    let wrong = tls_fixture::Certificates::new();
    for (protocol, case) in [
        ("m4s", "wrong_basic"),
        ("m4ss", "untrusted_ca"),
        ("m4fs", "denied_segment"),
    ] {
        let mut source = Source::new(&fixture, protocol).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
        let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
        let outcome = std::panic::AssertUnwindSafe(async {
            let mut input = source.input();
            if case == "untrusted_ca" {
                input["flussonix_tls_ca"] = json!(wrong.ca);
            } else if case == "wrong_basic" {
                let raw = input["url"].as_str().unwrap().replacen("m4s://", "http://", 1);
                let mut u = url::Url::parse(&raw).unwrap();
                u.set_password(Some("wrong-native-secret")).unwrap();
                input["url"] = json!(u.as_str().replacen("http://", "m4s://", 1));
            } else {
                source.capture.response_status.store(403, Ordering::SeqCst);
            }
            let cfg = json!({
                "static": false, "inputs": [input], "transcoder": {"encoder": "h264_vaapi"},
                "pushes": receivers.iter().map(destination).collect::<Vec<_>>()
            });
            let worker = engine.ensure("owned-gpu", &cfg).await.unwrap();
            tokio::time::timeout(Duration::from_secs(12), async {
                while worker.alive.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await.expect("denied native setup stops owned worker");
            assert_eq!(worker.pid(), 0);
            assert_eq!(worker.stats()["bytes_in"], 0);
            assert_eq!(engine.http_push_egress.load(Ordering::Relaxed), 0);
            for r in &receivers {
                // Publishing may open its initial POST while native setup is pending.
                assert!(r.capture.requests.load(Ordering::SeqCst) <= 1);
                assert!(r.capture.data.lock().unwrap().is_empty());
                closed(r).await;
            }
            let requests = source.capture.requests.load(Ordering::SeqCst);
            assert_eq!(requests, if case == "untrusted_ca" { 0 } else if case == "wrong_basic" { 1 } else { 2 });
            let report = json!({
                "case": case, "protocol": protocol, "upstream_requests": requests,
                "media_encoder_pid": worker.pid(), "media_bytes": 0,
                "publishing_requests": receivers.iter().map(|r| r.capture.requests.load(Ordering::SeqCst)).collect::<Vec<_>>()
            });
            if let Some(root) = std::env::var_os("FLUSSONIX_HTTP_GPU_EVIDENCE_DIR") {
                std::fs::create_dir_all(&root).unwrap();
                std::fs::write(PathBuf::from(root).join(format!("native-denied-{case}.json")), serde_json::to_vec_pretty(&report).unwrap()).unwrap();
            }
            engine.stop_all().await;
            assert_eq!(engine.count().await, 0);
        }).catch_unwind().await;
        engine.stop_all().await;
        for r in &mut receivers {
            r.stop().await;
        }
        source.stop().await;
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    }
}
