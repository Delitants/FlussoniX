use crate::m4s::{Decoder, Event, flv_config, flv_frame, flv_header};
use crate::wire::{FlvDecoder, Hub};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, broadcast, oneshot},
};
use tokio_util::sync::CancellationToken;

pub struct Engine {
    root: PathBuf,
    ffmpeg: String,
    workers: Mutex<HashMap<String, Arc<Worker>>>,
}
pub struct Worker {
    tx: broadcast::Sender<Bytes>,
    cancel: CancellationToken,
    done: Mutex<Option<oneshot::Receiver<()>>>,
    pid: u32,
    started: Instant,
    signature: String,
    input_index: usize,
    pub bytes: AtomicU64,
    pub viewers: AtomicU64,
    pub alive: std::sync::atomic::AtomicBool,
    pub wire: Hub,
    last_access: std::sync::Mutex<Instant>,
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
        self.pid
    }
    pub fn m4s_subscribe(&self) -> Option<(Vec<Bytes>, broadcast::Receiver<Bytes>)> {
        Some(self.wire.m4s_subscribe())
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
    pub fn stats(&self) -> Value {
        json!({"status":if self.alive.load(Ordering::Relaxed){if self.bytes.load(Ordering::Relaxed)>0{"running"}else{"starting"}}else{"error"},"pid":self.pid,"bytes_in":self.bytes.load(Ordering::Relaxed),"online_clients":self.viewers.load(Ordering::Relaxed),"uptime":self.started.elapsed().as_secs()})
    }
}
impl Engine {
    pub fn new(root: impl AsRef<Path>, ffmpeg: &str) -> Self {
        Self {
            root: root.as_ref().into(),
            ffmpeg: ffmpeg.into(),
            workers: Mutex::new(HashMap::new()),
        }
    }
    fn directory(&self, name: &str) -> PathBuf {
        self.root
            .join(format!("{:x}", Sha256::digest(name.as_bytes())))
    }
    pub async fn ensure(&self, name: &str, cfg: &Value) -> Result<Arc<Worker>, String> {
        let mut workers = self.workers.lock().await;
        if cfg["disabled"] == true {
            return Err("stream disabled".into());
        }
        let signature = media_signature(cfg);
        let mut index = 0;
        if let Some(w) = workers.get(name) {
            if w.alive.load(Ordering::Relaxed) && w.signature == signature {
                w.touch();
                return Ok(w.clone());
            }
            if !w.alive.load(Ordering::Relaxed) && w.signature == signature {
                if w.started.elapsed().as_secs() < 5 {
                    return Err("input retry backoff".into());
                }
                index = w.input_index + 1;
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
        let dir = self.directory(name);
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(dir.join("fmp4"))
            .await
            .map_err(|e| e.to_string())?;
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
        } else if m4s_input || m4f_input {
            cmd.args(["-f", "flv", "-i", "pipe:0"]);
            cmd.stdin(std::process::Stdio::piped());
        } else {
            let translated = translate_input(input)?;
            if translated.starts_with("rtsp://") {
                cmd.args(["-rtsp_transport", "tcp"]);
            }
            if translated.starts_with("http") {
                cmd.args(["-rw_timeout", "10000000"]);
            }
            if let Some(key) = cfg["flussonix_peer_key"].as_str() {
                if key.contains(['\r', '\n']) {
                    return Err("invalid peer key".into());
                }
                cmd.args(["-headers", &format!("X-Flussonix-Peer: {key}\r\n")]);
            }
            cmd.args(["-i", &translated]);
        }
        cmd.args([
            "-map",
            "0:v:0?",
            "-map",
            if synthetic { "1:a:0?" } else { "0:a:0?" },
        ]);
        if synthetic || cfg.get("transcoder").is_some() {
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
        }
        // One encode/mux source feeds both HLS variants and shared live TS fan-out.
        let output = format!(
            "[f=hls:hls_time=2:hls_list_size=6:hls_delete_threshold=2:hls_flags=delete_segments+temp_file]{}|[f=hls:hls_time=2:hls_list_size=6:hls_delete_threshold=2:hls_segment_type=fmp4:hls_flags=delete_segments+temp_file]{}|[f=mpegts]pipe:1|[onfail=ignore:f=flv:flvflags=no_duration_filesize:bsfs/a=aac_adtstoasc]pipe:2",
            dir.join("index.m3u8").display(),
            dir.join("fmp4/index.m3u8").display()
        );
        cmd.args(["-threads", "2", "-f", "tee", &output])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("cannot start FFmpeg: {e}"))?;
        let pid = child.id().unwrap_or(0);
        let mut stdout = child.stdout.take().ok_or("no media pipe")?;
        let (tx, _) = broadcast::channel(64);
        let mut stdin = child.stdin.take();
        let (done_tx, done) = oneshot::channel();
        let cancel = CancellationToken::new();
        let worker = Arc::new(Worker {
            tx,
            cancel: cancel.clone(),
            done: Mutex::new(Some(done)),
            pid,
            started: Instant::now(),
            signature,
            input_index: index,
            bytes: AtomicU64::new(0),
            viewers: AtomicU64::new(0),
            alive: std::sync::atomic::AtomicBool::new(true),
            wire: Hub::new(),
            last_access: std::sync::Mutex::new(Instant::now()),
        });
        if let Some(mut flv) = child.stderr.take() {
            let w = worker.clone();
            let c = cancel.clone();
            tokio::spawn(async move {
                let mut decoder = FlvDecoder::default();
                let mut buffer = vec![0u8; 16384];
                loop {
                    let read = tokio::select! {_=c.cancelled()=>break,r=flv.read(&mut buffer)=>r};
                    match read {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if let Err(reason) = decoder.push(&buffer[..n], &w.wire) {
                                tracing::warn!(error = %reason, "wire output stopped");
                                break;
                            }
                        }
                    }
                }
            });
        }
        if m4s_input || m4f_input {
            let url = input.to_owned();
            let key = cfg["flussonix_peer_key"].as_str().map(str::to_owned);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                if let Some(mut stdin) = stdin.take() {
                    let result = tokio::select! {_=cancel.cancelled()=>Ok(()),result=wire_ingest(&url,key.as_deref(),&mut stdin)=>result};
                    if let Err(reason) = result {
                        tracing::warn!(error = %reason, "wire input stopped");
                    }
                }
                cancel.cancel();
            });
        }
        workers.insert(name.into(), worker.clone());
        let w = worker.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 188 * 64];
            loop {
                tokio::select! {_ = cancel.cancelled()=>{let _=child.kill().await;break},r=stdout.read(&mut buffer)=>{match r{Ok(0)|Err(_)=>break,Ok(n)=>{w.bytes.fetch_add(n as u64,Ordering::Relaxed);let _=w.tx.send(Bytes::copy_from_slice(&buffer[..n]));}}}}
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
            w.cancel.cancel();
            w.alive.store(false, Ordering::Relaxed);
            let _ = done_tx.send(());
        });
        Ok(worker)
    }
    pub async fn read(&self, name: &str, file: &str) -> Result<Bytes, String> {
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
            w.touch()
        }
        let path = self.directory(name).join(file);
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|_| "media not ready")?;
        if meta.len() > 32 * 1024 * 1024 {
            return Err("segment exceeds size limit".into());
        }
        Ok(Bytes::from(
            tokio::fs::read(path).await.map_err(|_| "media not ready")?,
        ))
    }
    pub async fn stop(&self, name: &str) {
        let mut workers = self.workers.lock().await;
        let w = workers.remove(name);
        if let Some(w) = w {
            w.cancel.cancel();
            if let Some(done) = w.done.lock().await.take() {
                let _ = done.await;
            }
            let _ = tokio::fs::remove_dir_all(self.directory(name)).await;
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
        let alive = self
            .workers
            .lock()
            .await
            .get(name)
            .is_some_and(|w| w.alive.load(Ordering::Relaxed));
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
            .filter(|(_, w)| w.idle_seconds() > 60 && w.viewers.load(Ordering::Relaxed) == 0)
            .map(|(n, _)| n.clone())
            .collect()
    }
}
fn valid_file(file: &str) -> bool {
    let f = file.strip_prefix("fmp4/").unwrap_or(file);
    !f.is_empty()
        && f.len() < 128
        && f.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_')
        && !f.contains("..")
        && (f.ends_with(".ts") || f.ends_with(".m4s") || f == "init.mp4")
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

async fn wire_ingest(
    input: &str,
    key: Option<&str>,
    stdin: &mut tokio::process::ChildStdin,
) -> Result<(), String> {
    let scheme = if input.starts_with("m4ss://") || input.starts_with("m4fs://") {
        "https"
    } else {
        "http"
    };
    let full = format!(
        "{}://{}",
        scheme,
        input.split_once("://").ok_or("invalid URL")?.1
    );
    let mut base = url::Url::parse(&full).map_err(|_| "invalid media URL")?;
    let is_m4f = input.starts_with("m4f");
    let suffix = if is_m4f { "/m4f" } else { "/m4s" };
    base.set_path(
        base.path()
            .trim_end_matches(suffix)
            .trim_end_matches('/')
            .to_owned()
            .as_str(),
    );
    let mut control = base.clone();
    control.set_path(&format!("{}{suffix}", base.path()));
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|_| "cannot build input client")?;
    let mut request = client
        .get(control)
        .header("X-Supported", "prepush,drop_status");
    if let Some(k) = key {
        request = request.header("X-Flussonix-Peer", k)
    }
    let response = request
        .send()
        .await
        .map_err(|_| "media connection failed")?;
    if !response.status().is_success() {
        return Err("source rejected input".into());
    }
    let mut stream = response.bytes_stream();
    let mut decoder = Decoder::default();
    let mut tracks = Vec::new();
    let mut origin = None;
    let mut signal = Vec::new();
    stdin
        .write_all(&flv_header())
        .await
        .map_err(|_| "media pipe closed")?;
    loop {
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(15), stream.next())
            .await
            .map_err(|_| "source timed out")?
            .ok_or("source closed")?
            .map_err(|_| "transport failed")?;
        let events = if is_m4f {
            signal.extend_from_slice(&bytes);
            if signal.len() > 8192 {
                return Err("M4F signal exceeds limit".into());
            }
            let mut events = Vec::new();
            while let Some(end) = signal.iter().position(|b| *b == b'\n') {
                let line = String::from_utf8(signal.drain(..=end).collect())
                    .map_err(|_| "invalid M4F signal")?;
                let stamp = line
                    .split_whitespace()
                    .nth(1)
                    .ok_or("missing M4F timestamp")?
                    .split('-')
                    .next()
                    .ok_or("missing M4F timestamp")?;
                if chrono::NaiveDateTime::parse_from_str(stamp, "%Y/%m/%d/%H/%M/%S").is_err() {
                    return Err("invalid M4F segment path".into());
                }
                let mut url = base.clone();
                url.set_path(&format!("{}/{}.m4f", base.path(), stamp));
                let mut request = client.get(url).timeout(std::time::Duration::from_secs(10));
                if let Some(k) = key {
                    request = request.header("X-Flussonix-Peer", k)
                }
                let r = request.send().await.map_err(|_| "M4F fetch failed")?;
                if !r.status().is_success() {
                    return Err("M4F segment denied".into());
                }
                let mut b = Vec::new();
                let mut chunks = r.bytes_stream();
                while let Some(chunk) = chunks.next().await {
                    let chunk = chunk.map_err(|_| "M4F fetch failed")?;
                    if b.len() + chunk.len() > 32 * 1024 * 1024 {
                        return Err("M4F segment too large".into());
                    }
                    b.extend_from_slice(&chunk)
                }
                let (info, frames) = crate::m4f::unpack(&b)?;
                events.push(Event::Info {
                    tracks: info,
                    wire: Bytes::new(),
                });
                for f in frames {
                    events.push(Event::Frame {
                        track_id: f.track_id,
                        dts: f.dts,
                        pts_offset: f.pts_offset,
                        key: f.key,
                        body: f.body,
                        wire: Bytes::new(),
                    });
                }
            }
            events
        } else {
            decoder.push(&bytes)?
        };
        for event in events {
            match event {
                Event::Info { tracks: new, .. } => {
                    for track in &new {
                        stdin
                            .write_all(&flv_config(track)?)
                            .await
                            .map_err(|_| "media pipe closed")?;
                    }
                    tracks = new;
                }
                Event::Frame {
                    track_id,
                    dts,
                    pts_offset,
                    key,
                    body,
                    ..
                } => {
                    let track = tracks
                        .iter()
                        .find(|t| t.id == track_id)
                        .ok_or("unknown track")?;
                    let o = *origin.get_or_insert(dts);
                    stdin
                        .write_all(&flv_frame(track, dts, pts_offset, key, &body, o)?)
                        .await
                        .map_err(|_| "media pipe closed")?;
                }
                Event::Gop {
                    tracks: new,
                    frames,
                    ..
                } => {
                    for track in &new {
                        stdin
                            .write_all(&flv_config(track)?)
                            .await
                            .map_err(|_| "media pipe closed")?;
                    }
                    tracks = new;
                    for f in frames {
                        let track = tracks
                            .iter()
                            .find(|t| t.id == f.track_id)
                            .ok_or("unknown track")?;
                        let o = *origin.get_or_insert(f.dts);
                        stdin
                            .write_all(&flv_frame(track, f.dts, f.pts_offset, f.key, &f.body, o)?)
                            .await
                            .map_err(|_| "media pipe closed")?;
                    }
                }
                Event::Other { .. } => {}
            }
        }
    }
}

pub fn media_signature(cfg: &Value) -> String {
    format!("{:x}",Sha256::digest(serde_json::to_vec(&json!({"inputs":cfg["inputs"],"transcoder":cfg["transcoder"],"peer":cfg["flussonix_peer_key"]})).unwrap()))
}
