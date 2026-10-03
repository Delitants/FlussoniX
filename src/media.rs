use crate::wire::{FlvDecoder, Hub};
use bytes::Bytes;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::{Mutex, broadcast, oneshot},
};
use tokio_util::sync::CancellationToken;

pub struct Engine {
    root: PathBuf,
    ffmpeg: String,
    hls_epoch: crate::hls_generation::Epoch,
    workers: Mutex<HashMap<String, Arc<Worker>>>,
}
pub struct Publication {
    pub worker: Arc<Worker>,
    pub stdin: Option<tokio::process::ChildStdin>,
}
impl Drop for Publication {
    fn drop(&mut self) {
        self.worker.cancel.cancel();
    }
}
pub struct Worker {
    publisher_stdin: std::sync::Mutex<Option<tokio::process::ChildStdin>>,
    publication: bool,
    tx: broadcast::Sender<Bytes>,
    cancel: CancellationToken,
    done: Mutex<Option<oneshot::Receiver<()>>>,
    pid: AtomicU32,
    started: Instant,
    signature: String,
    input_index: usize,
    input_protocol: String,
    subtitle_tracks: &'static str,
    hls_subtitles: &'static str,
    captions: Option<Arc<crate::caption_hls::State>>,
    restart_count: u64,
    input_timeout: Duration,
    recovery: std::sync::Mutex<crate::recovery::Recovery>,
    pub bytes: AtomicU64,
    pub viewers: Arc<AtomicU64>,
    pub alive: std::sync::atomic::AtomicBool,
    pub wire: Hub,
    last_access: Arc<std::sync::Mutex<Instant>>,
}
impl Worker {
    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.touch();
        self.tx.subscribe()
    }
    pub fn signature(&self) -> &str {
        &self.signature
    }
    pub fn pid(&self) -> u32 {
        self.pid.load(Ordering::Relaxed)
    }
    pub fn m4s_subscribe(&self) -> Option<(Vec<Bytes>, crate::media_queue::Receiver)> {
        Some(self.wire.m4s_subscribe())
    }
    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub async fn closed(&self) {
        self.cancel.cancelled().await
    }
    pub fn touch(&self) {
        *self.last_access.lock().unwrap() = Instant::now()
    }
    pub fn idle_seconds(&self) -> u64 {
        self.last_access.lock().unwrap().elapsed().as_secs()
    }
    fn failed(&self, reason: &'static str) {
        self.recovery.lock().unwrap().fail(reason);
    }
    pub fn stats(&self) -> Value {
        let recovery = self.recovery.lock().unwrap();
        let status = if self.alive.load(Ordering::Relaxed) {
            if self.bytes.load(Ordering::Relaxed) > 0 {
                "running"
            } else {
                "starting"
            }
        } else if self.publication {
            "waiting"
        } else if recovery.last_error().is_some() {
            "retrying"
        } else {
            "stopped"
        };
        json!({"status":status,"pid":self.pid(),"bytes_in":self.bytes.load(Ordering::Relaxed),"online_clients":self.viewers.load(Ordering::Relaxed),"uptime":self.started.elapsed().as_secs(),"input_protocol":self.input_protocol,"input_index":self.input_index,"restart_count":self.restart_count,"retry_in_ms":recovery.retry_in().map(|d|d.as_millis()),"last_error":recovery.last_error(),"media_age_ms":recovery.media_age_ms(),"subtitle_tracks":self.subtitle_tracks,"hls_subtitles":self.hls_subtitles,"hls_captions":self.captions.as_ref().map(|c|c.stats())})
    }
}
impl Engine {
    pub fn new(root: impl AsRef<Path>, ffmpeg: &str) -> Self {
        Self {
            root: root.as_ref().into(),
            ffmpeg: ffmpeg.into(),
            hls_epoch: crate::hls_generation::Epoch::new(),
            workers: Mutex::new(HashMap::new()),
        }
    }
    fn directory(&self, name: &str) -> PathBuf {
        self.root
            .join(format!("{:x}", Sha256::digest(name.as_bytes())))
    }
    pub async fn publish_guarded(
        &self,
        name: &str,
        cfg: &Value,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<Publication, String> {
        if !crate::publish::is_input(cfg) {
            return Err("stream does not accept publications".into());
        }
        let worker = self.ensure_mode(name, cfg, true, current, true).await?;
        let stdin = worker.publisher_stdin.lock().unwrap().take();
        Ok(Publication { worker, stdin })
    }
    pub async fn ensure(&self, name: &str, cfg: &Value) -> Result<Arc<Worker>, String> {
        self.ensure_guarded(name, cfg, true, std::future::ready(true))
            .await
    }
    pub async fn recover(&self, name: &str, cfg: &Value) -> Result<Arc<Worker>, String> {
        self.ensure_guarded(name, cfg, false, std::future::ready(true))
            .await
    }
    pub async fn ensure_guarded(
        &self,
        name: &str,
        cfg: &Value,
        touch_demand: bool,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<Arc<Worker>, String> {
        self.ensure_mode(name, cfg, touch_demand, current, false)
            .await
    }
    async fn ensure_mode(
        &self,
        name: &str,
        cfg: &Value,
        touch_demand: bool,
        current: impl std::future::Future<Output = bool>,
        publishing: bool,
    ) -> Result<Arc<Worker>, String> {
        let subtitle_tracks = crate::config::subtitle_tracks(cfg)?;
        let hls_subtitles = crate::config::hls_subtitles(cfg)?;
        let caption_services = crate::captions::configuration(cfg)?;
        if hls_subtitles == "convert" && caption_services.is_empty() {
            return Err("Choose at least one HLS caption channel for conversion".into());
        }
        if !caption_services.is_empty()
            && (cfg["inputs"]
                .as_array()
                .is_some_and(|a| a.iter().any(|i| i["url"] == "testsrc://"))
                || cfg["transcoder"]["encoder"] == "h264_nvenc")
        {
            return Err("HLS caption conversion requires a real H.264/HEVC video source; GPU conversion is not qualified".into());
        }
        let mut workers = self.workers.lock().await;
        // Recheck after waiting for another stream startup/replacement. A stale
        // route must not cancel an already-published replacement worker.
        if !current.await {
            return Err("media route changed".into());
        }
        if cfg["disabled"] == true {
            return Err("stream disabled".into());
        }
        let signature = media_signature(cfg);
        let mut index = 0;
        let mut restart_count = 0;
        let mut streak = 0;
        let mut last_access = Arc::new(std::sync::Mutex::new(Instant::now()));
        let mut viewers = Arc::new(AtomicU64::new(0));
        if let Some(w) = workers.get(name) {
            if touch_demand {
                w.touch();
            }
            // Body guards can outlive the generation that served them.
            last_access = w.last_access.clone();
            viewers = w.viewers.clone();
            let running = w.alive.load(Ordering::Relaxed) && !w.is_closed();
            if running && w.signature == signature {
                if publishing {
                    return Err("publisher already connected".into());
                }
                return Ok(w.clone());
            }
            if !running && w.signature == signature {
                let recovery = w.recovery.lock().unwrap();
                if !publishing && recovery.retry_in().is_some_and(|delay| !delay.is_zero()) {
                    return Err("input retry backoff".into());
                }
                index = w.input_index + 1;
                restart_count = w.restart_count.saturating_add(1);
                streak = recovery.next_streak();
            }
            w.cancel.cancel();
            if let Some(done) = w.done.lock().await.take() {
                let _ = done.await;
            }
        }
        let inputs = cfg["inputs"]
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or("stream has no input")?;
        index %= inputs.len();
        let input = inputs[index]["url"].as_str().ok_or("input URL required")?;
        if workers
            .values()
            .filter(|w| w.alive.load(Ordering::Relaxed))
            .count()
            >= 256
        {
            return Err("worker limit reached".into());
        }
        let publication = crate::publish::is_input(cfg);
        if publication && !publishing {
            return Err("waiting for publisher".into());
        }
        let dir = self.directory(name);
        let replaced = workers.contains_key(name);
        let sequence = self.hls_epoch.next(&dir).await?;
        let generation = uuid::Uuid::new_v4().simple().to_string();
        let discontinuity = if replaced { "+discont_start" } else { "" };
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(dir.join("fmp4"))
            .await
            .map_err(|e| e.to_string())?;
        // Published MPEG-TS can provoke diagnostics during format probing.
        // Give binary wire media its own connection, isolated from stderr.
        let publish_wire = if publication {
            Some(
                tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .map_err(|_| "cannot bind publication wire pipe")?,
            )
        } else {
            None
        };
        let wire_target = match &publish_wire {
            Some(listener) => format!(
                "tcp://{}",
                listener
                    .local_addr()
                    .map_err(|_| "publication wire address unavailable")?
            ),
            None => "pipe:2".into(),
        };
        let caption_listener = if caption_services.is_empty() {
            None
        } else {
            Some(
                tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .map_err(|_| "caption listener unavailable")?,
            )
        };
        let caption_target = caption_listener
            .as_ref()
            .map(|l| format!("tcp://{}", l.local_addr().unwrap()));
        let captions = (!caption_services.is_empty()).then(|| {
            Arc::new(crate::caption_hls::State::new(
                crate::captions::Decoder::new(caption_services),
                generation.clone(),
                sequence,
            ))
        });
        let mut cmd = Command::new(&self.ffmpeg);
        cmd.args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-threads",
            "2",
        ]);
        let mut peer_hls = None;
        let mut tls_input = None;
        let synthetic = input == "testsrc://";
        let m4s_input = input.starts_with("m4s://") || input.starts_with("m4ss://");
        let m4f_input = input.starts_with("m4f://") || input.starts_with("m4fs://");
        if synthetic {
            cmd.args([
                "-re",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=640x360:rate=25",
                "-re",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=48000",
            ]);
        } else if publication {
            cmd.args([
                "-protocol_whitelist",
                "pipe",
                "-probesize",
                "1048576",
                "-analyzeduration",
                "1000000",
                "-f",
                "mpegts",
                "-i",
                "pipe:0",
            ]);
            cmd.stdin(std::process::Stdio::piped());
        } else if m4s_input || m4f_input {
            cmd.args([
                "-probesize",
                "1048576",
                "-analyzeduration",
                "1000000",
                "-f",
                "mpegts",
                "-i",
                "pipe:0",
            ]);
            cmd.stdin(std::process::Stdio::piped());
        } else {
            let mut translated = if input.starts_with("rtsps://") {
                let bridge = crate::tls_input::Bridge::start(
                    input,
                    inputs[index]["flussonix_tls_ca"].as_str().map(Path::new),
                )
                .await?;
                let local = bridge.local_url().to_owned();
                tls_input = Some(bridge);
                local
            } else {
                translate_input(input)?
            };
            if translated.starts_with("rtsp://") {
                cmd.args([
                    "-rtsp_transport",
                    if inputs[index]["rtp"] == "udp" {
                        "udp"
                    } else {
                        "tcp"
                    },
                ]);
            }
            if translated.starts_with("http") {
                cmd.args(["-rw_timeout", "10000000"]);
            }
            if let Some(key) = cfg["flussonix_peer_key"].as_str() {
                let proxy = crate::peer_hls::PeerHls::start(&translated, key).await?;
                translated = proxy.url.clone();
                // All remote resources are fetched inside our origin-scoped
                // proxy; FFmpeg never receives the native peer credential.
                cmd.args([
                    "-allowed_extensions",
                    "ALL",
                    "-protocol_whitelist",
                    "http,tcp,crypto",
                ]);
                peer_hls = Some(proxy);
            }
            cmd.args(["-i", &translated]);
        }
        cmd.args([
            "-map",
            "0:v:0?",
            "-map",
            if synthetic {
                "1:a:0?"
            } else if m4s_input || m4f_input {
                "0:a?"
            } else {
                "0:a:0?"
            },
        ]);
        if synthetic || cfg.get("transcoder").is_some() && cfg["transcoder"]["encoder"] != "copy" {
            let t = &cfg["transcoder"];
            let encoder = t["encoder"].as_str().unwrap_or("libx264");
            if !["libx264", "h264_nvenc"].contains(&encoder) {
                return Err("unsupported encoder".into());
            }
            let vb = t["vb"]
                .as_u64()
                .unwrap_or(900)
                .clamp(100, 50000)
                .to_string()
                + "k";
            cmd.args([
                "-c:v", encoder, "-b:v", &vb, "-g", "50", "-pix_fmt", "yuv420p",
            ]);
            if encoder == "libx264" {
                cmd.args(["-preset", "veryfast", "-tune", "zerolatency"]);
            }
            cmd.args(["-c:a", "aac", "-b:a", "96k"]);
        } else {
            cmd.args(["-c", "copy"]);
            // The AAC RTP depacketizer can omit key flags. AAC-LC access
            // units are independently decodable; do not discard their copy.
            if m4s_input
                || m4f_input
                || input.starts_with("rtsp://")
                || input.starts_with("rtsps://")
            {
                cmd.arg("-copyinkf:a");
            }
        }
        let raw_hls = cfg["flussonix_hls_subtitles"] == "passthrough";
        // Separate DVB/teletext tracks belong on TS-based delivery. Copy their
        // encoded PES; AV-only slaves below never receive incompatible codecs.
        if subtitle_tracks == "preserve" || raw_hls {
            cmd.args(["-map", "0:s?", "-c:s", "copy"]);
            // Broadcast subtitle services can be silent indefinitely. Bound the
            // common tee queue before its AV-only slaves select their streams.
            cmd.args(["-max_interleave_delta", "100000"]);
        }
        // One encode/mux source feeds both HLS variants and shared live TS fan-out.
        let copy_publication = publication
            && (cfg.get("transcoder").is_none() || cfg["transcoder"]["encoder"] == "copy");
        let native_copy = (m4s_input || m4f_input)
            && (cfg.get("transcoder").is_none() || cfg["transcoder"]["encoder"] == "copy");
        let wire_output = if copy_publication || native_copy {
            String::new()
        } else {
            format!(
                "|[onfail=ignore:select='v,a':f=flv:flvflags=no_duration_filesize:bsfs/a=aac_adtstoasc]{wire_target}"
            )
        };
        let fmp4_filter = if native_copy {
            "__NATIVE_FMP4_FILTER__"
        } else if copy_publication {
            ":bsfs/a=aac_adtstoasc"
        } else {
            ""
        };
        let fmp4_failure = if native_copy { "onfail=ignore:" } else { "" };
        // The nested live TS mux has its own interleave queue as well.
        let ts_interleave = if subtitle_tracks == "preserve" {
            ":max_interleave_delta=100000"
        } else {
            ""
        };
        let ts_hls = if raw_hls {
            format!(
                "[select='v,a,s':f=segment:max_interleave_delta=100000:segment_format=mpegts:segment_format_options=max_interleave_delta=100000:segment_time=2:segment_list_size=6:segment_list_flags=live:segment_list_type=m3u8:segment_list={}]{}",
                dir.join("passthrough.m3u8").display(),
                dir.join(format!("g{generation}_p%d.ts")).display()
            )
        } else {
            format!(
                "[select='v,a':f=hls:hls_time=2:hls_list_size=6:hls_delete_threshold=2:start_number={sequence}:hls_segment_filename={}:hls_flags=delete_segments+temp_file{discontinuity}]{}",
                dir.join(format!("g{generation}_%d.ts")).display(),
                dir.join("index.m3u8").display()
            )
        };
        let live_select = if subtitle_tracks == "preserve" {
            ""
        } else {
            "select='v,a':"
        };
        let output = format!(
            "{ts_hls}|[{fmp4_failure}select='v,a':f=hls:hls_time=2:hls_list_size=6:hls_delete_threshold=2:start_number={sequence}:hls_segment_type=fmp4:hls_segment_filename={}:hls_fmp4_init_filename=g{generation}_init.mp4:hls_flags=delete_segments+temp_file{discontinuity}{fmp4_filter}]{}|[{live_select}f=mpegts{ts_interleave}]pipe:1{wire_output}",
            dir.join("fmp4")
                .join(format!("g{generation}_%d.m4s"))
                .display(),
            dir.join("fmp4/index.m3u8").display()
        );
        let native_input = m4s_input || m4f_input;
        if !native_input {
            cmd.args(["-threads", "2", "-f", "tee", &output]);
        }
        if copy_publication {
            // tee stream-copy retains the MPEG-TS codec tag even with -tag:v 0,
            // which FLV rejects. A separate copy mux chooses FLV's own tags;
            // the input is still demuxed once and no extra encode occurs.
            cmd.args([
                "-map",
                "0:v:0?",
                "-map",
                "0:a:0?",
                "-c",
                "copy",
                "-bsf:a",
                "aac_adtstoasc",
                "-f",
                "flv",
                "-flvflags",
                "no_duration_filesize",
                &wire_target,
            ]);
        }
        if !native_input {
            if let Some(target) = caption_target.as_deref() {
                caption_output(&mut cmd, target);
            }
        }
        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = if native_input {
            None
        } else {
            Some(
                cmd.spawn()
                    .map_err(|e| format!("cannot start FFmpeg: {e}"))?,
            )
        };
        let pid = child.as_ref().and_then(|c| c.id()).unwrap_or(0);
        let (tx, _) = broadcast::channel(64);
        let mut stdin = child.as_mut().and_then(|c| c.stdin.take());
        let (done_tx, done) = oneshot::channel();
        let cancel = CancellationToken::new();
        let timeout = Duration::from_secs(
            cfg["flussonix_input_timeout"]
                .as_u64()
                .unwrap_or(15)
                .clamp(1, 300),
        );
        let worker = Arc::new(Worker {
            publisher_stdin: std::sync::Mutex::new(if publication { stdin.take() } else { None }),
            publication,
            tx,
            cancel: cancel.clone(),
            done: Mutex::new(Some(done)),
            pid: AtomicU32::new(pid),
            started: Instant::now(),
            signature,
            subtitle_tracks,
            hls_subtitles,
            captions,
            input_index: index,
            input_protocol: input.split("://").next().unwrap_or("unknown").into(),
            restart_count,
            input_timeout: timeout,
            recovery: std::sync::Mutex::new(crate::recovery::Recovery::new(streak)),
            bytes: AtomicU64::new(0),
            viewers,
            alive: std::sync::atomic::AtomicBool::new(true),
            wire: Hub::new(),
            last_access,
        });
        let original_wire = (m4s_input || m4f_input)
            && (cfg.get("transcoder").is_none() || cfg["transcoder"]["encoder"] == "copy");
        let url = input.to_owned();
        let key = cfg["flussonix_peer_key"].as_str().map(str::to_owned);
        workers.insert(name.into(), worker.clone());
        let w = worker.clone();
        tokio::spawn(async move {
            let mut tasks = Vec::new();
            // Early returns only leave this setup/run block. The owner always
            // cancels and joins its tasks before signaling completion.
            async {
                let mut child = if let Some(child) = child {
                    child
                } else {
                    let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
                    let (metadata_tx, metadata_rx) = oneshot::channel();
                    let c = cancel.clone();
                    let input_worker = w.clone();
                    tasks.push(tokio::spawn(async move {
                        let result = tokio::select! { biased;
                            _=c.cancelled()=>Ok(()),
                            result=crate::m4_ingest::pull_ready(&url,key.as_deref(),&mut writer,if original_wire {Some(&input_worker.wire)}else{None},metadata_tx)=>result,
                        };
                        if let Err(reason) = result {
                            input_worker.failed("input_closed");
                            tracing::warn!(error = %reason, "wire input stopped");
                        }
                        c.cancel();
                    }));
                    let tracks = tokio::select! { biased;
                        _=cancel.cancelled()=>return,
                        result=tokio::time::timeout(timeout,metadata_rx)=>match result {
                            Ok(Ok(tracks))=>tracks,
                            Ok(Err(_))=>{ w.failed("input_closed"); return; },
                            Err(_)=>{ w.failed("startup_timeout"); return; },
                        },
                    };
                    let output = if native_copy {
                        output.replace("__NATIVE_FMP4_FILTER__", &native_fmp4_filters(&tracks))
                    } else { output };
                    cmd.args(["-threads", "2", "-f", "tee", &output]);
                    if let Some(target)=caption_target.as_deref(){caption_output(&mut cmd,target);}
                    if cancel.is_cancelled() { return; }
                    let mut child = match cmd.spawn() {
                        Ok(child)=>child,
                        Err(error)=>{ w.failed("packaging_failed"); tracing::warn!(%error,"cannot start native packager"); return; },
                    };
                    w.pid.store(child.id().unwrap_or(0),Ordering::Relaxed);
                    if let Some(mut stdin) = child.stdin.take() {
                        let c = cancel.clone();
                        let pipe_worker = w.clone();
                        tasks.push(tokio::spawn(async move {
                            let result = tokio::select! { biased;
                                _=c.cancelled()=>return,
                                result=tokio::io::copy(&mut reader,&mut stdin)=>result,
                            };
                            if result.is_err() {
                                pipe_worker.failed("packaging_failed");
                                c.cancel();
                            }
                        }));
                    }
                    child
                };
                if raw_hls {
                    let dir = dir.clone(); let c = cancel.clone(); let worker = w.clone();
                    tasks.push(tokio::spawn(async move {
                        if crate::raw_hls::watch(dir, generation, sequence, replaced, c.clone()).await.is_err() {
                            worker.failed("packaging_failed"); c.cancel();
                        }
                    }));
                }
                if let (Some(listener),Some(state))=(caption_listener,w.captions.clone()) {
                    let c=cancel.clone();let decoder_state=state.clone();let (sender,mut receiver)=tokio::sync::mpsc::channel::<Bytes>(32);
                    tasks.push(tokio::spawn(async move {let mut transport=crate::caption_transport::Transport::default();loop {let data=tokio::select!{biased;_=c.cancelled()=>break,data=receiver.recv()=>match data{Some(data)=>data,None=>break}};if decoder_state.failed.load(Ordering::Relaxed){let mut decoder=decoder_state.decoder.lock().unwrap();let pts=decoder.latest_pts;decoder.reset(pts);break}let mut decoder=decoder_state.decoder.lock().unwrap();transport.push(&data,&mut decoder);if matches!(decoder.error,Some("caption_clock_discontinuity"|"caption_reorder_limit"|"caption_video_codec_unsupported"|"caption_nal_limit"|"caption_pes_limit")){decoder_state.failed.store(true,Ordering::Relaxed);}}}));
                    let c=cancel.clone();let drain_state=state.clone();tasks.push(tokio::spawn(async move {let Ok(Ok((mut socket,_)))=(tokio::select!{biased;_=c.cancelled()=>return,r=tokio::time::timeout(Duration::from_secs(5),listener.accept())=>r})else{drain_state.failed.store(true,Ordering::Relaxed);drain_state.decoder.lock().unwrap().error=Some("caption_input_closed");return};let mut buffer=[0;16384];loop{let read=tokio::select!{biased;_=c.cancelled()=>break,r=socket.read(&mut buffer)=>r};match read{Ok(n)if n>0=>{if sender.try_send(Bytes::copy_from_slice(&buffer[..n])).is_err(){drain_state.failed.store(true,Ordering::Relaxed);}},_=>{drain_state.failed.store(true,Ordering::Relaxed);drain_state.decoder.lock().unwrap().error=Some("caption_input_closed");break}}}}));
                    tasks.push(tokio::spawn(state.watch(dir.clone(),cancel.clone())));
                }
                let Some(mut stdout) = child.stdout.take() else {
                    w.failed("packaging_failed");
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return;
                };
                let mut stderr = child.stderr.take();
                if publish_wire.is_some() {
                    if let Some(mut stderr) = stderr.take() {
                        let c = cancel.clone();
                        tasks.push(tokio::spawn(async move {
                            let mut buffer = [0; 16384];
                            loop {
                                match tokio::select! { biased; _=c.cancelled()=>break, r=stderr.read(&mut buffer)=>r }
                                {
                                    Ok(n) if n > 0 => {
                                        tracing::warn!("publication packager reported a diagnostic")
                                    }
                                    _ => break,
                                }
                            }
                        }));
                    }
                }
                if publish_wire.is_some() || stderr.is_some() {
                    let w = w.clone();
                    let c = cancel.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut flv: Box<dyn tokio::io::AsyncRead + Unpin + Send> = if let Some(listener) =
                            publish_wire
                        {
                            let accepted = tokio::select! { biased; _=c.cancelled()=>return, r=tokio::time::timeout(Duration::from_secs(8),listener.accept())=>r };
                            match accepted {
                                Ok(Ok((socket, _))) => Box::new(socket),
                                _ => {
                                    w.failed("wire_setup_failed");
                                    c.cancel();
                                    return;
                                }
                            }
                        } else if let Some(pipe) = stderr {
                            Box::new(pipe)
                        } else {
                            return;
                        };
                        let mut decoder = FlvDecoder::default();
                        let mut buffer = vec![0u8; 16384];
                        loop {
                            let read = tokio::select! {_=c.cancelled()=>break,r=flv.read(&mut buffer)=>r};
                            match read {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    if !original_wire {
                                        if let Err(reason) = decoder.push(&buffer[..n], &w.wire) {
                                            tracing::warn!(error = %reason, "wire output stopped");
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }));
                }
                let mut output_clock=crate::caption_transport::Transport::default();
                let mut output_decoder=crate::captions::Decoder::new(vec![]);
                let mut buffer = vec![0u8; 188 * 64];
                loop {
                    let read = tokio::select! { biased;
                        _ = cancel.cancelled() => break,
                        result = tokio::time::timeout(timeout, stdout.read(&mut buffer)) => result,
                    };
                    match read {
                        Ok(Ok(0)) => {
                            w.failed("input_closed");
                            break;
                        }
                        Ok(Err(_)) => {
                            w.failed("packaging_failed");
                            break;
                        }
                        Err(_) => {
                            w.failed(if w.bytes.load(Ordering::Relaxed) == 0 {
                                "startup_timeout"
                            } else {
                                "input_stalled"
                            });
                            break;
                        }
                        Ok(Ok(n)) => {
                            if let Some(state)=&w.captions{state.observe_ts(&buffer[..n],&mut output_clock,&mut output_decoder);}
                            w.recovery.lock().unwrap().progress();
                            w.bytes.fetch_add(n as u64, Ordering::Relaxed);
                            let _ = w.tx.send(Bytes::copy_from_slice(&buffer[..n]));
                        }
                    }
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
            }.await;
            cancel.cancel();
            for task in tasks {
                task.abort();
                let _ = task.await;
            }
            drop(peer_hls);
            if let Some(bridge) = tls_input {
                bridge.close().await;
            }
            w.alive.store(false, Ordering::Relaxed);
            let _ = done_tx.send(());
        });
        Ok(worker)
    }
    pub async fn read(&self, name: &str, file: &str) -> Result<Bytes, String> {
        if self
            .workers
            .lock()
            .await
            .get(name)
            .is_none_or(|w| !w.alive.load(Ordering::Relaxed) || w.is_closed())
        {
            return Err("media worker unavailable".into());
        }
        if let Some(state) = self
            .workers
            .lock()
            .await
            .get(name)
            .and_then(|w| w.captions.clone())
        {
            let logical = file.strip_prefix("fmp4/").unwrap_or(file);
            if logical == "index.m3u8" || logical == "av.m3u8" || logical.starts_with("cc") {
                return state.read(file).ok_or("caption media not ready".into());
            }
        }
        if file.ends_with(".m4f") {
            return self
                .workers
                .lock()
                .await
                .get(name)
                .and_then(|w| w.wire.segment(file))
                .ok_or("M4F segment not available".into());
        }
        if !matches!(file, "index.m3u8" | "fmp4/index.m3u8") && !valid_file(file) {
            return Err("invalid media path".into());
        }
        if let Some(w) = self.workers.lock().await.get(name) {
            w.touch();
        } else {
            return Err("media worker unavailable".into());
        }
        let path = self.directory(name).join(file);
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|_| "media not ready")?;
        if meta.len() > 32 * 1024 * 1024 {
            return Err("segment exceeds size limit".into());
        }
        let mut data = tokio::fs::read(&path)
            .await
            .map_err(|_| "media not ready")?;
        let drop_captions = self
            .workers
            .lock()
            .await
            .get(name)
            .is_some_and(|w| w.hls_subtitles == "drop");
        if drop_captions {
            if file.ends_with(".ts") {
                crate::caption_filter::ts(&mut data)?;
            } else if file.ends_with(".m4s") {
                let dir = path.parent().ok_or("media not ready")?;
                let list = tokio::fs::read_to_string(dir.join("index.m3u8"))
                    .await
                    .map_err(|_| "media not ready")?;
                let init = list
                    .lines()
                    .find_map(|l| l.strip_prefix("#EXT-X-MAP:URI=\""))
                    .and_then(|l| l.split('"').next())
                    .filter(|l| valid_file(l) && l.ends_with(".mp4"))
                    .ok_or("caption initialization unavailable")?;
                let init_path = dir.join(init);
                if tokio::fs::metadata(&init_path)
                    .await
                    .map_err(|_| "media not ready")?
                    .len()
                    > 2 * 1024 * 1024
                {
                    return Err("caption initialization exceeds limit".into());
                }
                let init = tokio::fs::read(init_path)
                    .await
                    .map_err(|_| "media not ready")?;
                crate::caption_filter::mp4(&init, &mut data)?;
            }
        }
        Ok(Bytes::from(data))
    }
    async fn stop_worker(&self, name: &str, w: Arc<Worker>) {
        w.cancel.cancel();
        if let Some(done) = w.done.lock().await.take() {
            let _ = done.await;
        }
        let _ = self.hls_epoch.observe(&self.directory(name)).await;
        let _ = tokio::fs::remove_dir_all(self.directory(name)).await;
    }
    pub async fn stop(&self, name: &str) {
        let mut workers = self.workers.lock().await;
        if let Some(w) = workers.remove(name) {
            self.stop_worker(name, w).await;
        }
    }
    pub async fn stop_if_current(&self, name: &str, expected: &Arc<Worker>) {
        let mut workers = self.workers.lock().await;
        if workers.get(name).is_some_and(|w| Arc::ptr_eq(w, expected)) {
            let worker = workers.remove(name).unwrap();
            self.stop_worker(name, worker).await;
        }
    }
    pub async fn stop_all(&self) {
        let names = self
            .workers
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for n in names {
            self.stop(&n).await
        }
    }
    pub async fn count(&self) -> usize {
        self.workers
            .lock()
            .await
            .values()
            .filter(|w| w.alive.load(Ordering::Relaxed))
            .count()
    }
    pub async fn stats(&self, name: &str) -> Value {
        self.workers
            .lock()
            .await
            .get(name)
            .map(|w| w.stats())
            .unwrap_or(json!({"status":"waiting","online_clients":0}))
    }
    pub async fn ready(&self, name: &str) -> bool {
        let alive = self.workers.lock().await.get(name).is_some_and(|w| {
            w.alive.load(Ordering::Relaxed)
                && !w.is_closed()
                && w.recovery
                    .lock()
                    .unwrap()
                    .media_age_ms()
                    .is_some_and(|age| age < w.input_timeout.as_millis())
        });
        if !alive {
            return false;
        }
        let meta = tokio::fs::metadata(self.directory(name).join("index.m3u8")).await;
        meta.is_ok_and(|m| {
            m.modified()
                .is_ok_and(|t| t.elapsed().is_ok_and(|age| age.as_secs() < 15))
        })
    }
    pub async fn workers(&self) -> Vec<(String, String)> {
        self.workers
            .lock()
            .await
            .iter()
            .map(|(n, w)| (n.clone(), w.signature.clone()))
            .collect()
    }

    pub async fn idle(&self) -> Vec<String> {
        self.workers
            .lock()
            .await
            .iter()
            .filter(|(_, w)| {
                !w.publication && w.idle_seconds() >= 60 && w.viewers.load(Ordering::Relaxed) == 0
            })
            .map(|(n, _)| n.clone())
            .collect()
    }
}
fn caption_output(cmd: &mut Command, target: &str) {
    // Include one optional audio stream for the null fallback: an audio-only
    // source must not lose AV because an optional caption output has no video.
    cmd.args(["-map","0:v:0?","-map","0:a:0?","-c","copy","-sn",
        "-max_interleave_delta","100000","-flush_packets","1","-f","tee",
        &format!("[select='v':onfail=ignore:f=mpegts:max_interleave_delta=100000:flush_packets=1]{target}|[f=null]pipe:2")]);
}
/// FFmpeg maps the optional video first, then all audio in PMT order.
/// Numeric stream specifiers avoid escaping colons inside tee option keys.
fn native_fmp4_filters(tracks: &[crate::m4s::Track]) -> String {
    let video = |t: &&crate::m4s::Track| matches!(t.codec.as_str(), "h264" | "hevc");
    let offset = tracks.iter().filter(video).count();
    tracks
        .iter()
        .filter(|t| !matches!(t.codec.as_str(), "h264" | "hevc"))
        .enumerate()
        .filter(|(_, t)| t.codec == "aac")
        .map(|(i, _)| format!(":bsfs/{}=aac_adtstoasc", offset + i))
        .collect()
}
fn valid_file(file: &str) -> bool {
    let f = file.strip_prefix("fmp4/").unwrap_or(file);
    !f.is_empty()
        && f.len() < 128
        && f.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_')
        && !f.contains("..")
        && (f.ends_with(".ts")
            || f.ends_with(".m4s")
            || f == "init.mp4"
            || f.starts_with('g') && f.ends_with("_init.mp4"))
}
pub fn translate_input(input: &str) -> Result<String, String> {
    let (scheme, rest) = input.split_once("://").ok_or("invalid input URL")?;
    let scheme = match scheme {
        "hls" | "tshttp" => "http",
        "hlss" | "tshttps" => "https",
        "http" | "https" | "rtsp" | "srt" => scheme,
        _ => return Err(format!("unsupported input protocol: {scheme}")),
    };
    Ok(format!("{scheme}://{rest}"))
}

pub fn media_signature(cfg: &Value) -> String {
    format!("{:x}",Sha256::digest(serde_json::to_vec(&json!({"inputs":cfg["inputs"],"transcoder":cfg["transcoder"],"peer":cfg["flussonix_peer_key"],"timeout":cfg["flussonix_input_timeout"],"subtitle_tracks":cfg["flussonix_subtitle_tracks"],"hls_captions":cfg["flussonix_hls_captions"],"hls_subtitles":cfg["flussonix_hls_subtitles"]})).unwrap()))
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::server::{App, Options, router};
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    fn app(d: &std::path::Path) -> Arc<App> {
        App::new(
            d.join("config.json"),
            d.join("media"),
            Options {
                admin_password: "owned-lifecycle-admin".into(),
                peer_key: "owned-lifecycle-peer".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap()
    }
    async fn kill(w: &Worker) {
        assert!(
            std::process::Command::new("kill")
                .args(["-KILL", &w.pid().to_string()])
                .status()
                .unwrap()
                .success()
        );
        tokio::time::timeout(Duration::from_secs(3), w.closed())
            .await
            .unwrap();
        while w.alive.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
    }
    #[tokio::test]
    async fn queued_stale_start_cannot_replace_the_current_worker() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        let current_cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":20});
        let stale_cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":15});
        let current = app.media.ensure("owned", &current_cfg).await.unwrap();
        let blocked = app.media.workers.lock().await;
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = allowed.clone();
        let a = app.clone();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let pending = tokio::spawn(async move {
            entered.send(()).unwrap();
            a.media
                .ensure_guarded("owned", &stale_cfg, true, async move {
                    flag.load(Ordering::SeqCst)
                })
                .await
        });
        waiting.await.unwrap();
        allowed.store(false, Ordering::SeqCst);
        drop(blocked);
        let result = pending.await.unwrap();
        let pid = app.media.stats("owned").await["pid"].clone();
        app.media.stop_all().await;
        assert!(
            result.is_err(),
            "stale route passed the engine startup fence"
        );
        assert_eq!(
            pid,
            current.pid(),
            "queued stale startup replaced the current worker"
        );
    }

    #[tokio::test]
    async fn active_continuous_body_prevents_idle_retirement() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let cfg = app.config.effective("owned").unwrap();
        let w = app.media.ensure("owned", &cfg).await.unwrap();
        let response = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri("/owned/mpegts")
                    .header("X-Flussonix-Peer", "owned-lifecycle-peer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(w.viewers.load(Ordering::Relaxed), 1);
        *w.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        app.reconcile().await;
        let alive = w.alive.load(Ordering::Relaxed);
        drop(response);
        app.media.stop_all().await;
        assert!(alive, "a held continuous playback body was retired as idle");
    }
    #[tokio::test]
    async fn old_body_departure_refreshes_demand_after_generation_replacement() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let old = app
            .media
            .ensure("owned", &app.config.effective("owned").unwrap())
            .await
            .unwrap();
        let response = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri("/owned/mpegts")
                    .header("X-Flussonix-Peer", "owned-lifecycle-peer")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        *old.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        kill(&old).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        app.reconcile().await;
        let replacement = app.media.workers.lock().await.get("owned").unwrap().clone();
        assert_ne!(old.pid(), replacement.pid());
        // The old body can still be held after replacement publication.
        app.reconcile().await;
        assert!(
            app.media
                .workers
                .lock()
                .await
                .get("owned")
                .is_some_and(|w| Arc::ptr_eq(w, &replacement)),
            "held old body demand disappeared during replacement"
        );
        // Departure may be delayed until after the replacement is published.
        drop(response);
        app.reconcile().await;
        let retained = app
            .media
            .workers
            .lock()
            .await
            .get("owned")
            .is_some_and(|w| Arc::ptr_eq(w, &replacement));
        app.media.stop_all().await;
        assert!(
            retained,
            "replacement lost the old body's actual departure demand"
        );
        assert!(replacement.idle_seconds() < 2);
    }
    #[tokio::test]
    async fn expired_on_demand_failure_is_retired_before_retry() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let w = app
            .media
            .ensure("owned", &app.config.effective("owned").unwrap())
            .await
            .unwrap();
        kill(&w).await;
        *w.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        app.reconcile().await;
        assert!(app.media.workers().await.is_empty());
        assert_eq!(app.media.count().await, 0);
    }
    #[tokio::test]
    async fn removal_or_disable_during_async_retry_cannot_resurrect_a_worker() {
        for disable in [false, true] {
            let d = tempfile::tempdir().unwrap();
            let app = app(d.path());
            app.config
                .put(
                    "streams",
                    "owned",
                    json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                )
                .unwrap();
            let old = app
                .media
                .ensure("owned", &app.config.effective("owned").unwrap())
                .await
                .unwrap();
            kill(&old).await;
            tokio::time::sleep(Duration::from_millis(1100)).await;
            let (tx, rx) = oneshot::channel();
            *old.done.lock().await = Some(rx);
            let a = app.clone();
            let retry = tokio::spawn(async move { a.reconcile().await });
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if old.done.try_lock().is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if disable {
                app.config
                    .put("streams", "owned", json!({"disabled":true}))
                    .unwrap();
            } else {
                app.config.delete("streams", "owned").unwrap();
            }
            tx.send(()).unwrap();
            retry.await.unwrap();
            assert_eq!(app.media.count().await, 0);
            assert!(app.media.workers().await.is_empty());
        }
    }
}
#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/support/caption_fixture.rs"]
mod caption_sink_fixture;
#[cfg(test)]
mod caption_sink_tests {
    use super::caption_sink_fixture as fixture;
    use super::*;
    #[tokio::test]
    async fn optional_caption_sink_failure_does_not_stop_av() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("owned.ts");
        std::fs::write(&path, fixture::transport()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("tcp://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut cmd = Command::new("ffmpeg");
        cmd.args(["-v", "error", "-re", "-i"]).arg(&path).args([
            "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-f", "mpegts", "pipe:1",
        ]);
        caption_output(&mut cmd, &target);
        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let mut child = cmd.spawn().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut buffer = [0; 188 * 64];
        let read = tokio::time::timeout(Duration::from_secs(4), stdout.read(&mut buffer)).await;
        tokio::time::sleep(Duration::from_millis(800)).await;
        let status = child.try_wait().unwrap();
        let _ = child.kill().await;
        let _ = child.wait().await;
        assert!(matches!(read,Ok(Ok(n))if n>0));
        assert!(
            status.is_none(),
            "optional caption connection failure must retain AV: {status:?}"
        );
    }
}
