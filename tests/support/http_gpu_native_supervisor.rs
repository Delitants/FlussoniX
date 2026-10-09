//! Exercise the real daemon supervisor: read-only API polls cannot start workers.
use super::*;
use std::{process::Stdio, time::Instant};
use tokio::{io::AsyncBufReadExt, process::Child};

struct Daemon {
    child: Child,
    url: String,
    client: reqwest::Client,
    encoders: Vec<u32>,
    directory: PathBuf,
}
impl Daemon {
    fn spawn(directory: &Path) -> Self {
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_flussonix"))
            .args(["--listen", "127.0.0.1:0", "--ffmpeg", "/usr/bin/ffmpeg"])
            .arg("--config")
            .arg(directory.join("config.json"))
            .arg("--media-dir")
            .arg(directory.join("media"))
            .env("FLUSSONIX_ADMIN_USER", "owned-supervisor")
            .env("FLUSSONIX_ADMIN_PASSWORD", "owned-supervisor-secret")
            .env("FLUSSONIX_PEER_KEY", "owned-supervisor-peer")
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(directory.join("daemon.log")).unwrap())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        Self {
            child,
            url: String::new(),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
            encoders: vec![],
            directory: directory.to_owned(),
        }
    }
    async fn announce(&mut self) {
        let mut lines = tokio::io::BufReader::new(self.child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("owned daemon announces its OS-selected port")
            .unwrap()
            .unwrap();
        let info: Value = serde_json::from_str(&line).unwrap();
        self.url = format!("http://{}", info["listen"].as_str().unwrap());
    }
    async fn stats(&mut self) -> Value {
        // This GET route reads stats; unlike media/PUT routes it never ensures
        // or reconciles a worker. No Engine/App object exists in this harness.
        let stream = self
            .client
            .get(format!("{}/streamer/api/v3/streams/owned-gpu", self.url))
            .basic_auth("owned-supervisor", Some("owned-supervisor-secret"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if let Some(pid) = stream["stats"]["pid"].as_u64().filter(|p| *p != 0) {
            let pid = u32::try_from(pid).unwrap();
            if !self.encoders.contains(&pid) {
                self.encoders.push(pid);
            }
        }
        stream["stats"].clone()
    }
    async fn stop(&mut self) -> bool {
        if let Some(pid) = self.child.id() {
            // SAFETY: only signal our owned, still-unreaped daemon child.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
        let clean = match tokio::time::timeout(Duration::from_secs(8), self.child.wait()).await {
            Ok(Ok(status)) => status.success(),
            _ => {
                let _ = self.child.kill().await;
                let _ = self.child.wait().await;
                false
            }
        };
        // Failed assertions must not leave our known encoder children running.
        let mut leaked = false;
        {
            for pid in &self.encoders {
                let process = PathBuf::from(format!("/proc/{pid}"));
                let owned_media = self.directory.join("media");
                let marker = owned_media.as_os_str().as_encoded_bytes();
                if std::fs::read_link(process.join("exe")).ok()
                    == std::fs::canonicalize("/usr/bin/ffmpeg").ok()
                    && std::fs::read(process.join("cmdline")).is_ok_and(|bytes| {
                        bytes.split(|b| *b == 0).any(|argument| {
                            argument.windows(marker.len()).any(|part| part == marker)
                        })
                    })
                {
                    leaked = true;
                    // Exact executable plus this test's unique output directory.
                    unsafe {
                        libc::kill(*pid as i32, libc::SIGKILL);
                    }
                }
            }
        }
        // A successful daemon exit cannot conceal a leaked owned encoder.
        clean && !leaked
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            // Let the daemon reap its media children on a startup panic too.
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
    }
}

fn session(receiver: &Receiver, generation: usize) -> Vec<u8> {
    receiver
        .capture
        .bodies
        .lock()
        .unwrap()
        .get(generation)
        .map(|data| data.lock().unwrap().clone())
        .unwrap_or_default()
}
fn session_len(receiver: &Receiver, generation: usize) -> usize {
    receiver
        .capture
        .bodies
        .lock()
        .unwrap()
        .get(generation)
        .map(|data| data.lock().unwrap().len())
        .unwrap_or(0)
}
async fn delivering(daemon: &mut Daemon, receivers: &[Receiver; 2], generation: usize) -> Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let stats = daemon.stats().await;
            if stats["restart_count"] == generation
                && stats["status"] == "running"
                && receivers
                    .iter()
                    .all(|r| session_len(r, generation) > 250_000)
            {
                return stats;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("background supervisor resumes both uploads without a new media request")
}
async fn outputs(receivers: &[Receiver; 2], directory: &Path, generation: usize) -> Value {
    let mut reports = vec![];
    for (index, receiver) in receivers.iter().enumerate() {
        let path = directory.join(format!("output-{index}.ts"));
        std::fs::write(&path, session(receiver, generation)).unwrap();
        let mut report = decoded(&path, "aac").await;
        report["transport"] = json!(if index == 0 { "http" } else { "verified_https" });
        reports.push(report);
    }
    json!(reports)
}

async fn supervisor_case(
    fixture: &Fixture,
    protocol: &'static str,
    fallback: Option<&'static str>,
    encoder: &str,
) {
    let mut source = Source::new(fixture, protocol).await;
    let mut alternate = if let Some(protocol) = fallback {
        Some(Source::new(fixture, protocol).await)
    } else {
        None
    };
    let directory = tempfile::tempdir().unwrap();
    let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
    let publication_trace = Arc::new(Mutex::new(PublicationTrace::default()));
    for receiver in &receivers {
        *receiver.capture.publication_trace.lock().unwrap() = Some(publication_trace.clone());
    }
    let store = ConfigStore::open(directory.path().join("config.json")).unwrap();
    let mut inputs = vec![source.input()];
    if let Some(alternate) = &alternate {
        inputs.push(alternate.input());
    }
    let transcoder = if encoder == "h264_vaapi" {
        json!({"encoder":encoder,"qp":24,"acodec":"aac","ab":96})
    } else {
        json!({"encoder":encoder,"vb":1200,"acodec":"aac","ab":96})
    };
    store
        .put(
            "templates",
            "supervisor-profile",
            json!({"transcoder":transcoder,
        "pushes":receivers.iter().map(destination).collect::<Vec<_>>() }),
        )
        .unwrap();
    store
        .put(
            "streams",
            "owned-gpu",
            json!({"static":false,"template":"supervisor-profile",
        "inputs":inputs,"flussonix_input_timeout":10}),
        )
        .unwrap();
    let config_bytes = std::fs::read(directory.path().join("config.json")).unwrap();
    let mut daemon = Daemon::spawn(directory.path());
    let outcome = std::panic::AssertUnwindSafe(async {
        daemon.announce().await;
        let first = delivering(&mut daemon, &receivers, 0).await;
        let first_pid = u32::try_from(first["pid"].as_u64().unwrap()).unwrap();
        let first_args = encoder_arguments_pid(first_pid, encoder);
        let first_driver = (encoder == "h264_vaapi").then(|| dependencies_pid(first_pid));
        assert_eq!(first["input_index"], 0);
        if let Some(alternate) = &alternate { assert_eq!(alternate.capture.requests.load(Ordering::SeqCst), 0); }
        tokio::time::sleep(Duration::from_secs(3)).await;
        publication_trace.lock().unwrap().retired_pid = first_pid;
        let fault = Instant::now();
        if alternate.is_some() { source.stop().await; } else { source.disconnect_current(); }
        let second = delivering(&mut daemon, &receivers, 1).await;
        let resumed_ms = fault.elapsed().as_millis();
        {
            let trace = publication_trace.lock().unwrap();
            assert!(!trace.retired_encoder_alive_at_post, "replacement POST started while retired encoder still existed");
            assert!(!trace.generation_overlap, "replacement POST overlaps a retired upload on either destination");
            assert_eq!(trace.active.get(&0), Some(&0));
            assert_eq!(trace.active.get(&1), Some(&2));
        }
        assert_eq!(second["input_index"], if fallback.is_some() {1} else {0});
        let second_pid = u32::try_from(second["pid"].as_u64().unwrap()).unwrap();
        assert_ne!(first_pid, second_pid);
        assert!(!Path::new(&format!("/proc/{first_pid}")).exists(), "old encoder must be reaped before replacement delivery");
        assert_eq!(daemon.encoders, [first_pid, second_pid]);
        let args = encoder_arguments_pid(second_pid, encoder);
        let driver = (encoder == "h264_vaapi").then(|| dependencies_pid(second_pid));
        assert!(args.windows(2).any(|v| v == ["-i", "pipe:0"]));
        let old_sizes: Vec<_> = receivers.iter().map(|r| session_len(r, 0)).collect();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(delivering(&mut daemon, &receivers, 1).await["pid"], second["pid"]);
        for (index, receiver) in receivers.iter().enumerate() {
            assert_eq!(receiver.capture.requests.load(Ordering::SeqCst), 2);
            assert_eq!(receiver.capture.max_active.load(Ordering::SeqCst), 1, "upload generations must never overlap");
            assert_eq!(receiver.capture.active.load(Ordering::SeqCst), 1);
            assert_eq!(session_len(receiver, 0), old_sizes[index], "retired upload cannot append media");
            assert_eq!(receiver.capture.paths.lock().unwrap().as_slice(), ["/gpu/mpegts?token=owned-gpu", "/gpu/mpegts?token=owned-gpu"]);
            assert!(receiver.capture.headers.lock().unwrap().iter().all(|h| h == &format!("Basic {}", STANDARD.encode("gpu:owned-publishing-only"))));
        }
        let active = alternate.as_ref().unwrap_or(&source);
        let control = if active.protocol.starts_with("m4s") {"/native/m4s?token=owned-native-token"} else {"/native/m4f?token=owned-native-token"};
        assert_eq!(active.capture.paths.lock().unwrap().iter().filter(|p| p.as_str() == control).count(), if fallback.is_some() {1} else {2});
        assert!(active.capture.paths.lock().unwrap().iter().all(|p| p.ends_with("?token=owned-native-token")));
        assert!(active.capture.headers.lock().unwrap().iter().all(|h| h == &input_header()));
        for secret in ["native-input", "p:a@ss", "owned-native-token", "owned-publishing-only"] {
            assert!(!format!("{second} {args:?}").contains(secret));
        }
        assert_eq!(std::fs::read(directory.path().join("config.json")).unwrap(), config_bytes);
        assert!(daemon.stop().await, "SIGTERM gracefully shuts down daemon and encoders");
        for pid in &daemon.encoders { assert!(!Path::new(&format!("/proc/{pid}")).exists()); }
        tokio::time::timeout(Duration::from_secs(3), async {
            while active.capture.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("independent source body closes after daemon shutdown");
        for receiver in &receivers { closed(receiver).await; }
        let final_sizes: Vec<_> = receivers.iter().map(|r| session_len(r, 1)).collect();
        for generation in 0..2 {
            let decoded = outputs(&receivers, directory.path(), generation).await;
            let name = format!("supervisor-{encoder}-{protocol}-{}-{generation}", fallback.unwrap_or("same"));
            retain(&name, directory.path(), &json!({"source":fixture.report,"outputs":decoded,
                "arguments":if generation==0 {&first_args} else {&args},
                "dependencies":if generation==0 {&first_driver} else {&driver},
                "automatic_resume_ms":resumed_ms,"restart_count":1,"old_encoder_reaped":true,
                "new_encoder_reaped":true,"no_recovery_or_playback_request":true,"upload_generations_overlap":false,"retired_encoder_absent_at_replacement_post":true}));
            fixture.retain(&name);
        }
        assert_eq!(receivers.iter().map(|r| session_len(r, 1)).collect::<Vec<_>>(), final_sizes);
        assert!(TcpListener::bind(daemon.url.trim_start_matches("http://")).await.is_ok(), "daemon listener closes on shutdown");
    }).catch_unwind().await;
    let _ = daemon.stop().await;
    source.stop().await;
    if let Some(alternate) = &mut alternate {
        alternate.stop().await;
    }
    for receiver in &mut receivers {
        receiver.stop().await;
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// Removing the background reconcile loop or its push-demand rule must fail
// this ordinary CI case: only read-only stats GETs follow startup/source loss.
#[tokio::test]
async fn daemon_automatically_recovers_native_cpu_publishing_without_media_requests() {
    let fixture = Fixture::new("h264", "mp3").await;
    supervisor_case(&fixture, "m4s", Some("m4f"), "libx264").await;
}

#[tokio::test]
#[ignore = "requires independent Intel H264 VAAPI renderD128 and installed iHD/GMM"]
async fn daemon_automatically_reconnects_native_gpu_publishing_without_media_requests() {
    let fixture = Fixture::new("h264", "mp3").await;
    for protocol in ["m4s", "m4f"] {
        supervisor_case(&fixture, protocol, None, "h264_vaapi").await;
    }
}

#[tokio::test]
#[ignore = "requires independent Intel H264 VAAPI renderD128 and installed iHD/GMM"]
async fn daemon_automatically_falls_back_between_verified_native_tls_gpu_sources() {
    let fixture = Fixture::new("hevc", "mp2").await;
    for (primary, fallback) in [("m4ss", "m4fs"), ("m4fs", "m4ss")] {
        supervisor_case(&fixture, primary, Some(fallback), "h264_vaapi").await;
    }
}
