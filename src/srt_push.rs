//! Bounded SRT caller destinations. No vendor transport code is loaded.
use bytes::{Bytes, BytesMut};
use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Destination {
    pub endpoint: String,
    pub disabled: bool,
    pub retry_seconds: u64,
    startup_seconds: u64,
    connect_seconds: u64,
    latency_millis: u64,
    streamid: String,
    passphrase: String,
}

pub(crate) fn configuration(cfg: &Value) -> Result<Vec<Destination>, String> {
    let Some(pushes) = cfg.get("pushes") else {
        return Ok(vec![]);
    };
    let pushes = pushes.as_array().ok_or("pushes must be an array")?;
    if pushes.len() > 4 {
        return Err("At most four SRT destinations are supported".into());
    }
    pushes.iter().map(parse).collect()
}

pub(crate) fn enabled(cfg: &Value) -> bool {
    cfg["pushes"]
        .as_array()
        .is_some_and(|pushes| pushes.iter().any(|p| p["disabled"] != true))
}

fn parse(item: &Value) -> Result<Destination, String> {
    let obj = item
        .as_object()
        .ok_or("SRT destination must be an object")?;
    if obj.keys().any(|key| {
        ![
            "url",
            "streamid",
            "passphrase",
            "latency",
            "connect_timeout",
            "retry_timeout",
            "disabled",
            "comment",
            "enforcedencryption",
        ]
        .contains(&key.as_str())
    }) {
        return Err("SRT destination option is not implemented".into());
    }
    let raw = item["url"]
        .as_str()
        .ok_or("SRT destination URL is required")?;
    if raw.len() > 4096 || raw.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("Invalid SRT destination URL".into());
    }
    // Vendor examples put #!:: inside streamid without escaping the #.
    // It is query data here, never a URL fragment.
    let normalized = match raw.split_once('?') {
        Some((base, query)) => format!("{base}?{}", query.replace('#', "%23")),
        None => raw.to_owned(),
    };
    let mut url = url::Url::parse(&normalized).map_err(|_| "Invalid SRT destination URL")?;
    if url.scheme() != "srt"
        || url.host_str().is_none()
        || url.port().is_none_or(|p| p == 0)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !["", "/"].contains(&url.path())
    {
        return Err("SRT destination must use srt://HOST:PORT in caller mode".into());
    }
    let mut options = serde_json::Map::new();
    if let Some(query) = url.query() {
        let bytes = query.as_bytes();
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == b'%'
                && (bytes.get(i + 1).is_none_or(|b| !b.is_ascii_hexdigit())
                    || bytes.get(i + 2).is_none_or(|b| !b.is_ascii_hexdigit()))
            {
                return Err("Invalid SRT query encoding".into());
            }
        }
        percent_encoding::percent_decode_str(query)
            .decode_utf8()
            .map_err(|_| "Invalid SRT query encoding")?;
    }
    for (key, value) in url.query_pairs() {
        if ![
            "streamid",
            "passphrase",
            "latency",
            "connect_timeout",
            "mode",
        ]
        .contains(&key.as_ref())
            || options.contains_key(key.as_ref())
            || obj.contains_key(key.as_ref())
        {
            return Err("Unknown or duplicate SRT destination query option".into());
        }
        let value = if ["latency", "connect_timeout"].contains(&key.as_ref()) {
            Value::from(
                value
                    .parse::<u64>()
                    .map_err(|_| "SRT timing options must be whole numbers")?,
            )
        } else {
            Value::from(value.to_string())
        };
        options.insert(key.into_owned(), value);
    }
    for key in ["streamid", "passphrase", "latency", "connect_timeout"] {
        if let Some(value) = obj.get(key) {
            options.insert(key.into(), value.clone());
        }
    }
    if options.get("mode").is_some_and(|v| v != "caller") {
        return Err("Only SRT caller output is implemented".into());
    }
    let options = Value::Object(options);
    let streamid = text(&options, "streamid", 512, false)?;
    let passphrase = text(&options, "passphrase", 79, true)?;
    if !passphrase.is_empty() && passphrase.len() < 10 {
        return Err("SRT passphrase needs 10 to 79 ASCII characters".into());
    }
    let latency = number(&options, "latency", 120, 10000)?;
    let connect = number(&options, "connect_timeout", 3, 30)?;
    let retry_seconds = number(item, "retry_timeout", 5, 300)?;
    if obj.get("disabled").is_some_and(|v| !v.is_boolean())
        || obj.get("enforcedencryption").is_some_and(|v| v != true)
        || obj
            .get("comment")
            .is_some_and(|v| v.as_str().is_none_or(|s| s.len() > 1024))
    {
        return Err("Invalid SRT destination enabled, encryption or comment setting".into());
    }
    url.set_query(None);
    let endpoint = url.to_string();
    Ok(Destination {
        endpoint,
        disabled: item["disabled"] == true,
        retry_seconds,
        startup_seconds: 5,
        connect_seconds: connect,
        latency_millis: latency,
        streamid: streamid.to_owned(),
        passphrase: passphrase.to_owned(),
    })
}

fn number(item: &Value, key: &str, default: u64, max: u64) -> Result<u64, String> {
    match item.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .filter(|n| (1..=max).contains(n))
            .ok_or_else(|| format!("SRT {key} must be a whole number from 1 to {max}")),
    }
}

fn text<'a>(item: &'a Value, key: &str, max: usize, ascii: bool) -> Result<&'a str, String> {
    match item.get(key) {
        None => Ok(""),
        Some(value) => value
            .as_str()
            .filter(|s| {
                s.len() <= max && !s.chars().any(char::is_control) && (!ascii || s.is_ascii())
            })
            .ok_or_else(|| format!("Invalid SRT {key}")),
    }
}

struct Counters {
    status: &'static str,
    pid: u32,
    attempts: u64,
    fed_bytes: u64,
    muxed_bytes: u64,
    last_error: Option<&'static str>,
}

pub(crate) struct State {
    destination: Destination,
    index: usize,
    counters: Mutex<Counters>,
}

impl State {
    pub fn new(destination: Destination, index: usize) -> Arc<Self> {
        let status = if destination.disabled {
            "disabled"
        } else {
            "connecting"
        };
        Arc::new(Self {
            destination,
            index,
            counters: Mutex::new(Counters {
                status,
                pid: 0,
                attempts: 0,
                fed_bytes: 0,
                muxed_bytes: 0,
                last_error: None,
            }),
        })
    }
    pub fn stats(&self) -> Value {
        let c = self.counters.lock().unwrap();
        serde_json::json!({"index":self.index,"endpoint":self.destination.endpoint,"status":c.status,"pid":c.pid,"attempts":c.attempts,"fed_bytes":c.fed_bytes,"muxed_bytes":c.muxed_bytes,"last_error":c.last_error})
    }
    pub async fn run(
        self: Arc<Self>,
        ffmpeg: String,
        mut receiver: crate::media_queue::Receiver,
        cancel: CancellationToken,
    ) {
        if self.destination.disabled {
            return;
        }
        while !cancel.is_cancelled() {
            {
                let mut c = self.counters.lock().unwrap();
                c.status = "connecting";
                c.attempts += 1;
            }
            let error = self.attempt(&ffmpeg, &mut receiver, &cancel).await;
            if cancel.is_cancelled() {
                break;
            }
            {
                let mut c = self.counters.lock().unwrap();
                c.status = "retrying";
                c.last_error = Some(error);
            }
            tokio::select! {biased;_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(self.destination.retry_seconds))=>{}}
            receiver = receiver.resubscribe();
        }
        let mut c = self.counters.lock().unwrap();
        c.status = "stopped";
        c.pid = 0;
    }
    async fn attempt(
        &self,
        ffmpeg: &str,
        receiver: &mut crate::media_queue::Receiver,
        cancel: &CancellationToken,
    ) -> &'static str {
        let target = match url::Url::parse(&self.destination.endpoint) {
            Ok(target) => target,
            Err(_) => return "push_connect_failed",
        };
        let host = match target.host() {
            Some(url::Host::Domain(host)) => host.to_owned(),
            Some(url::Host::Ipv4(host)) => host.to_string(),
            Some(url::Host::Ipv6(host)) => host.to_string(),
            None => return "push_connect_failed",
        };
        let Some(port) = target.port() else {
            return "push_connect_failed";
        };
        let addresses = tokio::select! {biased;
            _=cancel.cancelled()=>return "push_stopped",
            result=tokio::time::timeout(Duration::from_secs(self.destination.connect_seconds),tokio::net::lookup_host((host.as_str(),port)))=>match result {
                Ok(Ok(addresses))=>addresses.take(8).collect::<Vec<_>>(),
                _=>return "push_connect_failed",
            }
        };
        if addresses.is_empty() {
            return "push_connect_failed";
        }
        let mut candidates: Vec<_> = addresses
            .into_iter()
            .filter_map(|address| {
                crate::srt_playback::native::caller(
                    address,
                    &self.destination.streamid,
                    &self.destination.passphrase,
                    self.destination.latency_millis,
                    self.destination.connect_seconds,
                )
                .ok()
            })
            .collect();
        if candidates.is_empty() {
            return "push_connect_failed";
        }
        let connection_deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.destination.connect_seconds);
        let socket = loop {
            if let Some(index) = candidates.iter().position(|socket| socket.is_connected()) {
                break candidates.swap_remove(index);
            }
            candidates.retain(|socket| socket.is_connecting());
            if candidates.is_empty() {
                return "push_connect_failed";
            }
            tokio::select! {biased;
                _=cancel.cancelled()=>return "push_stopped",
                _=tokio::time::sleep_until(connection_deadline)=>return "push_connect_failed",
                _=tokio::time::sleep(Duration::from_millis(5))=>{},
            }
        };
        drop(candidates);
        // The live source may have advanced while the SRT handshake ran.
        // Start a fresh feed rather than replaying an evicted TS backlog.
        *receiver = receiver.resubscribe();
        let mut cmd = crate::child_process::command(ffmpeg);
        cmd.args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-threads",
            "2",
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
            "-map",
            "0",
            "-c",
            "copy",
            "-copy_unknown",
            "-max_interleave_delta",
            "100000",
            "-flush_packets",
            "1",
            "-stats_period",
            "0.2",
            "-progress",
            "pipe:2",
        ]);
        cmd.args(["-f", "mpegts", "pipe:1"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let Ok(mut child) = cmd.spawn() else {
            return "push_start_failed";
        };
        self.counters.lock().unwrap().pid = child.id().unwrap_or(0);
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let mut media = [0u8; 1316 * 8];
        let mut diagnostics = [0u8; 4096];
        let mut pending = BytesMut::with_capacity(1316 * 32);
        let mut input: Option<Bytes> = None;
        let mut input_offset = 0;
        let mut sent_bytes = 0u64;
        let mut stderr_open = true;
        let mut send_ready = false;
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut progress = Instant::now();
        let error = 'sending: loop {
            let limit = if sent_bytes == 0 {
                self.destination.startup_seconds
            } else {
                10
            };
            let remaining = Duration::from_secs(limit).saturating_sub(progress.elapsed());
            tokio::select! {biased;
                _=cancel.cancelled()=>break "push_stopped",
                _=tokio::time::sleep(remaining)=>break "push_stalled",
                _=child.wait()=>break "push_failed",
                _=tick.tick(),if pending.len()>=188 && !send_ready=> {
                    if !socket.is_connected() {
                        break "push_failed";
                    }
                    send_ready=pending.len()>=188;
                },
                _=std::future::ready(()),if send_ready=> {
                    let mut blocked=false;
                    for _ in 0..256 {
                        let n=(pending.len().min(1316)/188)*188;
                        if n==0 {break;}
                        match socket.try_send(&pending[..n]) {
                            Ok(true)=>{
                                let _=pending.split_to(n);
                                sent_bytes=sent_bytes.saturating_add(n as u64);
                                let mut c=self.counters.lock().unwrap();
                                c.muxed_bytes=c.muxed_bytes.saturating_add(n as u64);
                                c.status="sending";
                                c.last_error=None;
                                progress=Instant::now();
                            }
                            Ok(false)=>{blocked=true;break;},
                            Err(_)=>break 'sending "push_failed",
                        }
                    }
                    send_ready=!blocked && pending.len()>=188;
                    if send_ready {tokio::task::yield_now().await;}
                },
                result=stdout.read(&mut media),if pending.len()<1316*32=> {
                    let Ok(n)=result else {break "push_failed";};
                    if n==0 {break "push_failed";}
                    pending.extend_from_slice(&media[..n]);
                    send_ready=pending.len()>=188;
                },
                result=stderr.read(&mut diagnostics),if stderr_open=> {
                    match result {Ok(0)=>stderr_open=false,Ok(_)=>{},Err(_)=>break "push_failed"}
                },
                result=receiver.recv(),if input.is_none()=> {
                    let data=match result {Ok(data)=>data,Err(broadcast::error::RecvError::Lagged(_))=>break "push_queue_overflow",Err(broadcast::error::RecvError::Closed)=>break "push_stopped"};
                    if data.is_empty(){continue;}
                    input=Some(data);
                    input_offset=0;
                },
                result=async {stdin.write(&input.as_ref().unwrap()[input_offset..]).await},if input.is_some()=> {
                    let Ok(n)=result else {break "push_failed";};
                    if n==0 {break "push_failed";}
                    input_offset+=n;
                    let mut c=self.counters.lock().unwrap();
                    c.fed_bytes=c.fed_bytes.saturating_add(n as u64);
                    if input_offset==input.as_ref().unwrap().len(){input=None;}
                },
            }
        };
        drop(stdin);
        let _ = child.kill().await;
        let _ = child.wait().await;
        self.counters.lock().unwrap().pid = 0;
        error
    }
}
