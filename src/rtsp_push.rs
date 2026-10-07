//! Copy-only RTSP publishing; Rust owns verified transport and delivery progress.
mod auth;
mod bridge;
mod client;
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub(crate) struct Destination {
    url: url::Url,
    credentials: Option<auth::Credentials>,
    endpoint: String,
    disabled: bool,
    connect_seconds: u64,
    retry_seconds: u64,
    tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>,
}
impl Destination {
    pub fn parse(item: &Value) -> Result<Self, String> {
        let object = item
            .as_object()
            .ok_or("RTSP destination must be an object")?;
        if object.keys().any(|k| {
            ![
                "url",
                "disabled",
                "comment",
                "connect_timeout",
                "retry_timeout",
                "flussonix_tls_ca",
            ]
            .contains(&k.as_str())
        }) {
            return Err("RTSP destination option is not implemented".into());
        }
        let raw = item["url"]
            .as_str()
            .ok_or("RTSP destination URL is required")?;
        if raw.len() > 4096
            || raw.chars().any(|c| c.is_control() || c.is_whitespace())
            || !raw.is_ascii()
        {
            return Err("Invalid RTSP destination URL".into());
        }
        for (i, b) in raw.bytes().enumerate() {
            if b == b'%'
                && (raw
                    .as_bytes()
                    .get(i + 1)
                    .is_none_or(|b| !b.is_ascii_hexdigit())
                    || raw
                        .as_bytes()
                        .get(i + 2)
                        .is_none_or(|b| !b.is_ascii_hexdigit()))
            {
                return Err("Invalid RTSP destination URL encoding".into());
            }
        }
        let mut url = url::Url::parse(raw).map_err(|_| "Invalid RTSP destination URL")?;
        if !["rtsp", "rtsps"].contains(&url.scheme())
            || url.host_str().is_none()
            || url.port() == Some(0)
            || url.fragment().is_some()
            || url.path().trim_matches('/').is_empty()
        {
            return Err("RTSP destination requires a stream path, host and no fragment".into());
        }
        let credentials = auth::Credentials::take(&mut url)?;
        if object.get("disabled").is_some_and(|v| !v.is_boolean())
            || object
                .get("comment")
                .is_some_and(|v| v.as_str().is_none_or(|s| s.len() > 1024))
        {
            return Err("Invalid RTSP enabled or comment setting".into());
        }
        let ca = match item.get("flussonix_tls_ca") {
            Some(value) if url.scheme() == "rtsps" => Some(std::path::Path::new(
                value
                    .as_str()
                    .ok_or("RTSPS trusted CA must be a file path")?,
            )),
            Some(_) => return Err("Trusted CA applies only to RTSPS destinations".into()),
            None => None,
        };
        let tls = if url.scheme() == "rtsps" {
            Some(crate::tls_input::client(ca)?)
        } else {
            None
        };
        let endpoint = format!(
            "{}://{}:{}",
            url.scheme(),
            url.host_str().unwrap(),
            url.port().unwrap_or(if tls.is_some() { 322 } else { 554 })
        );
        Ok(Self {
            url,
            credentials,
            endpoint,
            tls,
            disabled: item["disabled"] == true,
            connect_seconds: number(item, "connect_timeout", 3, 30)?,
            retry_seconds: number(item, "retry_timeout", 5, 300)?,
        })
    }
}
fn number(item: &Value, key: &str, default: u64, max: u64) -> Result<u64, String> {
    match item.get(key) {
        None => Ok(default),
        Some(v) => v
            .as_u64()
            .filter(|n| (1..=max).contains(n))
            .ok_or_else(|| format!("RTSP {key} must be a whole number from 1 to {max}")),
    }
}
struct Counters {
    status: &'static str,
    pid: u32,
    attempts: u64,
    fed_bytes: u64,
    rtp_bytes: u64,
    last_error: Option<&'static str>,
}
pub(crate) struct State {
    destination: Destination,
    index: usize,
    counters: Mutex<Counters>,
}
impl State {
    pub fn new(destination: Destination, index: usize) -> Arc<Self> {
        Arc::new(Self {
            counters: Mutex::new(Counters {
                status: if destination.disabled {
                    "disabled"
                } else {
                    "connecting"
                },
                pid: 0,
                attempts: 0,
                fed_bytes: 0,
                rtp_bytes: 0,
                last_error: None,
            }),
            destination,
            index,
        })
    }
    pub fn stats(&self) -> Value {
        let c = self.counters.lock().unwrap();
        json!({"index":self.index,"endpoint":self.destination.endpoint,"protocol":self.destination.url.scheme(),"status":c.status,"pid":c.pid,"attempts":c.attempts,"fed_bytes":c.fed_bytes,"rtp_bytes":c.rtp_bytes,"last_error":c.last_error})
    }
    pub async fn run(
        self: Arc<Self>,
        worker: Arc<crate::media::Worker>,
        mut receiver: broadcast::Receiver<Bytes>,
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
            let error = self.attempt(&worker, &mut receiver, &cancel).await;
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
        worker: &Arc<crate::media::Worker>,
        receiver: &mut broadcast::Receiver<Bytes>,
        cancel: &CancellationToken,
    ) -> &'static str {
        let started = Instant::now();
        let deadline = started + Duration::from_secs(self.destination.connect_seconds + 5);
        let mut probe = crate::ts_profile::Probe::default();
        let mut initial = Vec::new();
        let metadata = tokio::select! {biased;_=cancel.cancelled()=>return "push_stopped", result=tokio::time::timeout_at(tokio::time::Instant::from_std(deadline),async {
            while probe.audio.is_none() {
                let data=receiver.recv().await.map_err(|_|"push_queue_overflow")?;
                if initial.len()+data.len()>1048576 {return Err("push_metadata_limit");}
                probe.push(&data);initial.extend_from_slice(&data);
            }
            if probe.unsupported_tracks||probe.audio_types.iter().any(|t|![3,4,0x0f].contains(t))||probe.video.is_some_and(|t|![0x1b,0x24].contains(&t)) {return Err("push_profile_unsupported");}
            Ok(())
        })=>result};
        match metadata {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return error,
            Err(_) => return "push_metadata_timeout",
        }
        let snapshot = tokio::select! {biased;
            _=cancel.cancelled()=>return "push_stopped",
            result=tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                loop {
                    match worker.wire.rtp.publish_snapshot() {
                        Ok(snapshot) if !snapshot.packets.is_empty() => return Ok(snapshot),
                        Err(error) if error == "push_profile_unsupported" => return Err("push_profile_unsupported"),
                        _ => {}
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })=>match result {Ok(Ok(snapshot))=>snapshot,Ok(Err(error))=>return error,Err(_)=>return "push_metadata_timeout"}
        };
        let connect_deadline =
            (Instant::now() + Duration::from_secs(self.destination.connect_seconds)).min(deadline);
        let bridge = tokio::select! {biased;
            _=cancel.cancelled()=>return "push_stopped",
            _=tokio::time::sleep_until(tokio::time::Instant::from_std(connect_deadline))=>return "push_connect_failed",
            result=bridge::Bridge::prepare(&self.destination)=>match result{Ok(b)=>b,Err(_)=>return "push_connect_failed"}
        };
        let mut readers = tokio::task::JoinSet::new();
        let result = tokio::select! {biased;_=cancel.cancelled()=>Err("push_stopped"),result=self.send(worker,&bridge,started,snapshot,&mut readers)=>result};
        readers.abort_all();
        while readers.join_next().await.is_some() {}
        bridge.close().await;
        result.err().unwrap_or("push_stopped")
    }
    async fn send(
        &self,
        worker: &Arc<crate::media::Worker>,
        bridge: &bridge::Bridge,
        started: Instant,
        snapshot: crate::rtp::PlaySnapshot,
        readers: &mut tokio::task::JoinSet<()>,
    ) -> Result<(), &'static str> {
        let deadline = started + Duration::from_secs(self.destination.connect_seconds + 5);
        if snapshot.description.tracks.len()
            != snapshot
                .description
                .tracks
                .iter()
                .map(|t| t.id)
                .collect::<std::collections::HashSet<_>>()
                .len()
            || snapshot.description.tracks.is_empty()
            || snapshot.description.tracks.len() > 8
        {
            return Err("push_profile_unsupported");
        }
        let stamp_origin = snapshot
            .decode_times
            .iter()
            .copied()
            .min()
            .ok_or("push_metadata_timeout")?;
        let mut client = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            client::Client::publish(
                bridge,
                self.destination.credentials.clone(),
                &snapshot.description,
                stamp_origin,
                readers,
            ),
        )
        .await
        .map_err(|_| "push_setup_timeout")??;
        let description = snapshot.description;
        let mut receiver = snapshot.receiver;
        let mut initial: std::collections::VecDeque<_> = snapshot
            .packets
            .into_iter()
            .zip(snapshot.decode_times)
            .map(|(bytes, dts)| crate::rtp::Packet { bytes, dts })
            .collect();
        let mut reports = tokio::time::interval(Duration::from_secs(5));
        reports.tick().await;
        let mut controls = tokio::time::interval(Duration::from_secs(15));
        controls.tick().await;
        let mut last = Instant::now();
        let mut previous = 0;
        let mut progress_tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            let size = bridge.rtp_bytes();
            if size > previous {
                let mut c = self.counters.lock().unwrap();
                c.rtp_bytes = c.rtp_bytes.saturating_add(size - previous);
                c.status = "sending";
                c.last_error = None;
                previous = size;
                last = Instant::now();
            }
            let due = tokio::time::Instant::from_std(if previous == 0 {
                deadline
            } else {
                last + Duration::from_secs(10)
            });
            if !worker.wire.rtp.generation_is(description.generation) || worker.is_closed() {
                return Err("push_generation_changed");
            }
            let next = async {
                if let Some(packet) = initial.pop_front() {
                    Ok(packet)
                } else {
                    receiver
                        .recv_timed()
                        .await
                        .map_err(|_| "push_queue_overflow")
                }
            };
            tokio::select! {biased;
                _=tokio::time::sleep_until(due)=>return Err("push_stalled"),
                response=client.feedback()=>{tokio::time::timeout_at(due,client.handle_feedback(response?)).await.map_err(|_|"push_stalled")??;},
                _=reports.tick()=>{tokio::time::timeout_at(due,client.reports()).await.map_err(|_|"push_stalled")??;},
                _=controls.tick()=>{tokio::time::timeout_at(due,client.keepalive()).await.map_err(|_|"push_stalled")??;},
                _=progress_tick.tick()=>{},
                packet=next=>{
                    let packet=packet?;
                    tokio::time::timeout_at(due,client.packet(&packet.bytes)).await.map_err(|_|"push_stalled")??;
                }
            }
        }
    }
}
