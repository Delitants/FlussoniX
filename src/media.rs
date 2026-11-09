#[cfg(test)]
pub(crate) mod file_reader;
#[cfg(not(test))]
mod file_reader;
mod shared;
use crate::wire::Hub;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shared::StartMode;
pub use shared::{
    HlsSessionFailure, PackagedPlayback, PackagedSubscription, SharedHlsOptions, SharedHlsSessions,
    SharedPackagingFailure,
};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
    sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, oneshot},
};
use tokio_util::sync::CancellationToken;

async fn copy_to_packager<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    progress: &crate::source_directory::modern_coordinator::SharedDrain,
) -> std::io::Result<u64> {
    let mut buffer = [0u8; 16 * 1024];
    let mut copied = 0u64;
    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            return Ok(copied);
        }
        writer.write_all(&buffer[..n]).await?;
        progress.wrote_packager(n);
        copied += n as u64;
    }
}

pub struct Engine {
    root: PathBuf,
    ffmpeg: String,
    tesseract: String,
    hls_epoch: crate::hls_generation::Epoch,
    workers: Mutex<HashMap<String, Arc<Worker>>>,
    selected_hls_cache: std::sync::Mutex<SelectedHlsCache>,
    selected_hls_miss_slots: Arc<Semaphore>,
    caption_filter_slots: Arc<Semaphore>,
    #[cfg(test)]
    selected_hls_waiters: AtomicUsize,
    input_preferences: Mutex<HashMap<String, InputPreference>>,
    startup_gates: std::sync::Mutex<HashMap<String, std::sync::Weak<Mutex<()>>>>,
    rtsp_probe_slots: Semaphore,
    fxlink_pool: Arc<crate::fxlink::ClientPool>,
    gpu: crate::gpu::Checks,
    pub(crate) idle_memory: crate::idle_memory::Controller,
    pub direct_egress: Arc<AtomicU64>,
    pub rtsp_push_egress: Arc<AtomicU64>,
    pub http_push_egress: Arc<AtomicU64>,
    srtp_receiver_ledger: Option<Arc<crate::direct_rtp::key_ledger::Ledger>>,
    #[cfg(test)]
    hls_prepare_test_hook: Option<Arc<HlsPrepareHook>>,
    #[cfg(test)]
    before_remove_test_hook: Option<Arc<BeforeRemoveHook>>,
}
const SELECTED_HLS_CACHE_BYTES: usize = 128 * 1024 * 1024;
const SELECTED_HLS_CACHE_ENTRIES: usize = 512;

#[derive(Default)]
struct SelectedHlsCache {
    entries: VecDeque<SelectedHlsEntry>,
    bytes: usize,
}
struct SelectedHlsEntry {
    generation: Weak<()>,
    file: String,
    selector: crate::hls_ts_select::Selector,
    data: Bytes,
}
impl SelectedHlsCache {
    fn get(
        &mut self,
        generation: &Arc<()>,
        file: &str,
        selector: crate::hls_ts_select::Selector,
    ) -> Option<Bytes> {
        let at = self.entries.iter().position(|entry| {
            entry.file == file
                && entry.selector == selector
                && entry
                    .generation
                    .upgrade()
                    .is_some_and(|owned| Arc::ptr_eq(&owned, generation))
        })?;
        let entry = self.entries.remove(at)?;
        let data = entry.data.clone();
        self.entries.push_back(entry);
        Some(data)
    }
    fn insert(
        &mut self,
        generation: &Arc<()>,
        file: &str,
        selector: crate::hls_ts_select::Selector,
        data: Bytes,
    ) {
        self.insert_bounded(
            generation,
            file,
            selector,
            data,
            SELECTED_HLS_CACHE_BYTES,
            SELECTED_HLS_CACHE_ENTRIES,
        );
    }
    fn insert_bounded(
        &mut self,
        generation: &Arc<()>,
        file: &str,
        selector: crate::hls_ts_select::Selector,
        data: Bytes,
        max_bytes: usize,
        max_entries: usize,
    ) {
        if data.len() > max_bytes / 4 {
            return;
        }
        let mut removed_bytes = 0;
        self.entries.retain(|entry| {
            let keep = entry.generation.upgrade().is_some_and(|owned| {
                !(Arc::ptr_eq(&owned, generation)
                    && entry.file == file
                    && entry.selector == selector)
            });
            if !keep {
                removed_bytes += entry.data.len();
            }
            keep
        });
        self.bytes = self.bytes.saturating_sub(removed_bytes);
        self.bytes += data.len();
        self.entries.push_back(SelectedHlsEntry {
            generation: Arc::downgrade(generation),
            file: file.to_owned(),
            selector,
            data,
        });
        while self.bytes > max_bytes || self.entries.len() > max_entries {
            if let Some(old) = self.entries.pop_front() {
                self.bytes -= old.data.len();
            }
        }
    }
    fn remove_generation(&mut self, generation: &Arc<()>) {
        let mut removed_bytes = 0;
        self.entries.retain(|entry| {
            let keep = entry
                .generation
                .upgrade()
                .is_some_and(|owned| !Arc::ptr_eq(&owned, generation));
            if !keep {
                removed_bytes += entry.data.len();
            }
            keep
        });
        self.bytes = self.bytes.saturating_sub(removed_bytes);
    }
}
#[cfg(test)]
mod selected_hls_cache_tests {
    use super::*;

    #[test]
    fn cache_evicts_lru_and_dead_generations_with_small_limits() {
        let selector = crate::hls_ts_select::Selector::parse("v1a1").unwrap();
        let first = Arc::new(());
        let second = Arc::new(());
        let mut cache = SelectedHlsCache::default();
        for file in ["a.ts", "b.ts", "c.ts", "d.ts"] {
            cache.insert_bounded(&first, file, selector, Bytes::from_static(b"four"), 20, 4);
        }
        assert_eq!(
            cache.get(&first, "a.ts", selector).unwrap(),
            Bytes::from_static(b"four")
        );
        cache.insert_bounded(&first, "e.ts", selector, Bytes::from_static(b"four"), 20, 4);
        assert!(cache.get(&first, "b.ts", selector).is_none());
        assert!(cache.get(&first, "a.ts", selector).is_some());
        assert!(cache.get(&second, "a.ts", selector).is_none());
        drop(first);
        cache.insert_bounded(
            &second,
            "a.ts",
            selector,
            Bytes::from_static(b"four"),
            20,
            4,
        );
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.bytes, 4);
        assert!(cache.get(&second, "a.ts", selector).is_some());
        cache.insert_bounded(
            &second,
            "large.ts",
            selector,
            Bytes::from_static(b"123456"),
            20,
            4,
        );
        assert_eq!(
            cache.entries.len(),
            1,
            "oversize entry must not be retained"
        );
        cache.remove_generation(&second);
        assert_eq!(cache.bytes, 0);
        assert!(cache.entries.is_empty());
    }

    #[tokio::test]
    async fn selected_hls_misses_share_two_slots_and_cancel_waiters() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
        let first = engine.selected_hls_miss_permit().await.unwrap();
        let second = engine.selected_hls_miss_permit().await.unwrap();
        let waiting_engine = engine.clone();
        let mut third =
            tokio::spawn(async move { waiting_engine.selected_hls_miss_permit().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut third)
                .await
                .is_err(),
            "a third cache miss entered while two jobs were active"
        );
        assert_eq!(engine.selected_hls_waiters_for_test(), 1);
        third.abort();
        assert!(third.await.unwrap_err().is_cancelled());
        assert_eq!(engine.selected_hls_waiters_for_test(), 0);
        drop(first);
        let replacement =
            tokio::time::timeout(Duration::from_secs(1), engine.selected_hls_miss_permit())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(engine.selected_hls_miss_slots.available_permits(), 0);
        drop(replacement);
        drop(second);
        assert_eq!(engine.selected_hls_miss_slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn cancelled_filter_waits_for_blocking_job_before_releasing_slot() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let permit = engine.selected_hls_miss_permit().await.unwrap();
        let other = engine.selected_hls_miss_permit().await.unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let job = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
            .await
            .unwrap();
        job.abort();
        drop(other);
        let next = engine.selected_hls_miss_permit().await.unwrap();
        let blocked = engine.selected_hls_miss_permit();
        tokio::pin!(blocked);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut blocked)
                .await
                .is_err()
        );
        release_tx.send(()).unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(1), &mut blocked)
            .await
            .unwrap()
            .unwrap();
        job.await.unwrap();
        drop(recovered);
        drop(next);
    }
}
#[cfg(test)]
mod caption_scheduler_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn large_drop_caption_read_does_not_stall_unrelated_timer() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        engine
            .ensure(
                "caption-scheduler-owned",
                &json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"}),
            )
            .await
            .unwrap();

        let mut media = Vec::with_capacity(31 * 1024 * 1024);
        let mut pat = [0xff; 188];
        pat[..5].copy_from_slice(&[0x47, 0x40, 0, 0x10, 0]);
        pat[5..21].copy_from_slice(&[0, 0xb0, 13, 0, 1, 0xc1, 0, 0, 0, 1, 0xf0, 0, 0, 0, 0, 0]);
        let marker = b"flussonix-caption-scheduler-owned";
        pat[32..32 + marker.len()].copy_from_slice(marker);
        media.extend_from_slice(&pat);
        let mut pmt = [0xff; 188];
        pmt[..5].copy_from_slice(&[0x47, 0x50, 0, 0x10, 0]);
        pmt[5..26].copy_from_slice(&[
            2, 0xb0, 18, 0, 1, 0xc1, 0, 0, 0xe1, 0, 0xf0, 0, 0x1b, 0xe1, 0, 0xf0, 0, 0, 0, 0, 0,
        ]);
        media.extend_from_slice(&pmt);
        let mut null = [0xff; 188];
        null[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
        while media.len() + 188 < 31 * 1024 * 1024 {
            media.extend_from_slice(&null);
        }
        std::fs::write(
            engine.directory("caption-scheduler-owned").join("owned.ts"),
            media,
        )
        .unwrap();

        let (entered, release) = crate::caption_filter::pause_once_for_test(marker);
        let (timer_tx, timer_rx) = tokio::sync::oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        let controller = std::thread::spawn(move || {
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let started = Instant::now();
            runtime.spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let _ = timer_tx.send(Instant::now());
            });
            std::thread::sleep(Duration::from_millis(100));
            release.send(()).unwrap();
            started
        });
        let read = engine.read("caption-scheduler-owned", "owned.ts").await;
        let started = controller.join().unwrap();
        let delay = timer_rx.await.unwrap().duration_since(started);
        engine.stop_all().await;
        read.unwrap();
        assert!(
            delay < Duration::from_millis(25),
            "caption filtering stalled the async runtime for {delay:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_caption_fmp4_read_does_not_stall_unrelated_timer() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        engine
            .ensure(
                "caption-fmp4-owned",
                &json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"}),
            )
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(6);
        let segment = loop {
            if let Ok(playlist) = engine.read("caption-fmp4-owned", "fmp4/index.m3u8").await {
                let playlist = String::from_utf8(playlist.to_vec()).unwrap();
                if let Some(segment) = playlist
                    .lines()
                    .find(|line| !line.starts_with('#') && line.ends_with(".m4s"))
                {
                    break segment.to_owned();
                }
            }
            assert!(
                Instant::now() < deadline,
                "fMP4 test segment did not arrive"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let path = engine
            .directory("caption-fmp4-owned")
            .join("fmp4")
            .join(&segment);
        let marker = b"flussonix-fmp4-caption-scheduler-owned";
        let mut media = std::fs::read(&path).unwrap();
        media.extend_from_slice(&crate::m4s::atom(b"free", marker));
        std::fs::write(&path, media).unwrap();

        let (entered, release) = crate::caption_filter::pause_mp4_once_for_test(marker);
        let (timer_tx, timer_rx) = tokio::sync::oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        let controller = std::thread::spawn(move || {
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let started = Instant::now();
            runtime.spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                let _ = timer_tx.send(Instant::now());
            });
            std::thread::sleep(Duration::from_millis(100));
            release.send(()).unwrap();
            started
        });
        let read = engine
            .read("caption-fmp4-owned", &format!("fmp4/{segment}"))
            .await;
        let started = controller.join().unwrap();
        let delay = timer_rx.await.unwrap().duration_since(started);
        engine.stop_all().await;
        read.unwrap();
        assert!(
            delay < Duration::from_millis(25),
            "fMP4 caption filtering stalled the async runtime for {delay:?}"
        );
    }
}
#[cfg(test)]
type BeforeRemoveHook =
    dyn Fn(&str) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync;
#[cfg(test)]
type HlsPrepareHook = dyn Fn(
        String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<crate::peer_hls::PreparedOrigin, String>>
                + Send,
        >,
    > + Send
    + Sync;
#[derive(Clone, Copy)]
struct InputPreference {
    index: usize,
    inputs: [u8; 32],
}
#[derive(Clone, Copy)]
enum PublicationInput {
    MpegTs,
    Sdp { secure: bool },
}
enum OwnedInput {
    Script(String),
    LiveTs(String),
}
pub type PublicationWriter = Box<dyn tokio::io::AsyncWrite + Unpin + Send + Sync>;
pub struct Publication {
    pub worker: Arc<Worker>,
    pub stdin: Option<PublicationWriter>,
}
impl Drop for Publication {
    fn drop(&mut self) {
        self.worker.cancel.cancel();
    }
}
pub struct Worker {
    publisher_stdin: std::sync::Mutex<Option<PublicationWriter>>,
    publication: bool,
    tx: crate::media_queue::Channel,
    cancel: CancellationToken,
    done: Mutex<Option<oneshot::Receiver<()>>>,
    pid: AtomicU32,
    started: Instant,
    signature: String,
    input_index: usize,
    input_protocol: String,
    subtitle_tracks: &'static str,
    pub(crate) hls_subtitles: &'static str,
    native_text_tracks: AtomicU64,
    fxlink_telemetry: std::sync::Mutex<Option<Arc<crate::fxlink::Telemetry>>>,
    captions: Option<Arc<crate::caption_hls::State>>,
    pushes: Vec<Arc<crate::push::State>>,
    active_pushes: bool,
    direct_outputs: Vec<Arc<crate::direct_rtp::output::State>>,
    direct_input: Option<Arc<crate::direct_rtp::input::Statistics>>,
    restart_count: u64,
    input_timeout: Duration,
    recovery: std::sync::Mutex<crate::recovery::Recovery>,
    pub bytes: AtomicU64,
    output_rate: std::sync::Mutex<crate::media_rate::OutputRate>,
    hls_track_profile: std::sync::Mutex<Option<(usize, usize)>>,
    selected_hls_token: Arc<()>,
    pub viewers: Arc<AtomicU64>,
    pub alive: std::sync::atomic::AtomicBool,
    pub wire: Arc<Hub>,
    shared_generation: Option<uuid::Uuid>,
    shared_authority: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    shared_static_demand: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    last_access: Arc<std::sync::Mutex<Instant>>,
    // Drop last: old viewers and retirement may outlive the worker-map entry.
    _idle_activity: crate::idle_memory::Activity,
}
impl Worker {
    pub fn subscribe(&self) -> crate::media_queue::Receiver {
        self.touch();
        self.tx.subscribe()
    }
    pub(crate) fn ts_backlog(&self) -> (usize, usize) {
        self.tx.retained()
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
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
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
    #[track_caller]
    fn failed(&self, reason: &'static str) {
        tracing::debug!(target: "flussonix::worker_lifecycle",
            site = std::panic::Location::caller().line(), reason,
            "worker failure recorded");
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
        let mut stats = json!({"status":status,"pid":self.pid(),"bytes_in":self.bytes.load(Ordering::Relaxed),"online_clients":self.viewers.load(Ordering::Relaxed),"uptime":self.started.elapsed().as_secs(),"input_protocol":self.input_protocol,"input_index":self.input_index,"restart_count":self.restart_count,"retry_in_ms":recovery.retry_in().map(crate::recovery::retry_millis),"last_error":recovery.last_error(),"media_age_ms":recovery.media_age_ms(),"subtitle_tracks":self.subtitle_tracks,"hls_subtitles":self.hls_subtitles,"hls_captions":self.captions.as_ref().map(|c|c.stats())});
        if let Some(link) = self.fxlink_telemetry.lock().unwrap().as_ref() {
            stats["fxlink"] = link.snapshot();
        }
        let rate = self.output_bitrate();
        stats["flussonix_output_mbps"] = json!(rate.map(|r| r.0));
        stats["flussonix_output_rate_age_ms"] = json!(rate.map(|r| r.1));
        if !self.pushes.is_empty() {
            stats["flussonix_pushes"] =
                json!(self.pushes.iter().map(|p| p.stats()).collect::<Vec<_>>());
        }
        if let Some(input) = &self.direct_input {
            stats["direct_rtp_input"] = input.snapshot();
        }
        if !self.direct_outputs.is_empty() {
            stats["flussonix_rtp_outputs"] = json!(
                self.direct_outputs
                    .iter()
                    .map(|o| o.stats())
                    .collect::<Vec<_>>()
            );
            if let Some(Ok(description)) = self.wire.rtp.description() {
                stats["flussonix_rtp_tracks"] = json!(description
                    .tracks
                    .iter()
                    .map(|track| json!({"id":track.id,"media":if track.video {"video"} else {"audio"},"encoding":track.encoding}))
                    .collect::<Vec<_>>());
            }
        }
        let native_text_tracks = self.native_text_tracks.load(Ordering::Relaxed);
        if native_text_tracks > 0 {
            stats["native_subtitle_tracks"] = json!(native_text_tracks);
            stats["native_subtitle_output"] = json!(if recovery.last_error()
                == Some("native_subtitle_transcode_unsupported")
            {
                "Not supported"
            } else if self.subtitle_tracks == "preserve" {
                "Kept"
            } else {
                "Filtered"
            });
            stats["native_subtitle_hls"] = json!(if self.hls_subtitles == "drop" {
                "Off"
            } else if let Some(c) = self.captions.as_ref().filter(|c| c
                .decoder
                .lock()
                .unwrap()
                .services
                .iter()
                .any(|s| s.native_track().is_some()))
            {
                if c.failed.load(Ordering::Relaxed) {
                    "Failed"
                } else if c.stats()["status"] == "running" {
                    "Converted"
                } else {
                    "Starting"
                }
            } else {
                "Not supported"
            });
        }
        stats
    }
    pub(crate) fn output_bitrate(&self) -> Option<(f64, u64)> {
        if !self.alive.load(Ordering::Relaxed) || self.is_closed() {
            return None;
        }
        self.output_rate.lock().unwrap().snapshot(Instant::now())
    }
    pub(crate) fn hls_track_profile(&self) -> Option<(usize, usize)> {
        *self.hls_track_profile.lock().unwrap()
    }
    pub(crate) fn remember_hls_track_profile(&self, profile: (usize, usize)) {
        *self.hls_track_profile.lock().unwrap() = Some(profile);
    }
}
impl Engine {
    #[cfg(test)]
    pub(crate) fn selected_hls_waiters_for_test(&self) -> usize {
        self.selected_hls_waiters.load(Ordering::Relaxed)
    }

    /// All selected HLS cache misses share this gate. Acquire before reading
    /// source bytes, then move the permit into the blocking filter job so a
    /// disconnected viewer cannot release the transient-memory budget early.
    /// A saturated gate fails the request after the live HLS readiness window.
    pub(crate) async fn selected_hls_miss_permit(&self) -> Result<OwnedSemaphorePermit, ()> {
        #[cfg(test)]
        let _waiting = {
            struct Waiting<'a>(&'a AtomicUsize);
            impl Drop for Waiting<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::Relaxed);
                }
            }
            self.selected_hls_waiters.fetch_add(1, Ordering::Relaxed);
            Waiting(&self.selected_hls_waiters)
        };
        tokio::time::timeout(
            Duration::from_secs(8),
            self.selected_hls_miss_slots.clone().acquire_owned(),
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
    }

    /// Cache media bytes only. Every HTTP request must perform its own viewer
    /// admission and current-generation checks before using this cache.
    pub(crate) fn cached_selected_hls(
        &self,
        worker: &Arc<Worker>,
        file: &str,
        selector: crate::hls_ts_select::Selector,
    ) -> Option<Bytes> {
        self.selected_hls_cache
            .lock()
            .unwrap()
            .get(&worker.selected_hls_token, file, selector)
    }
    pub(crate) fn remember_selected_hls(
        &self,
        worker: &Arc<Worker>,
        file: &str,
        selector: crate::hls_ts_select::Selector,
        data: Bytes,
    ) {
        if worker.is_closed() || !worker.alive.load(Ordering::Relaxed) {
            return;
        }
        self.selected_hls_cache.lock().unwrap().insert(
            &worker.selected_hls_token,
            file,
            selector,
            data,
        );
    }
    pub fn new(root: impl AsRef<Path>, ffmpeg: &str) -> Self {
        Self::new_with_fxlink(root, ffmpeg, crate::fxlink::Features::default())
    }
    pub fn new_with_fxlink(
        root: impl AsRef<Path>,
        ffmpeg: &str,
        features: crate::fxlink::Features,
    ) -> Self {
        Self::new_with_ocr_and_fxlink(
            root,
            ffmpeg,
            &std::env::var("FLUSSONIX_TESSERACT").unwrap_or_else(|_| "tesseract".into()),
            features,
        )
    }
    pub fn new_with_ocr(root: impl AsRef<Path>, ffmpeg: &str, tesseract: &str) -> Self {
        Self::new_with_ocr_and_fxlink(root, ffmpeg, tesseract, crate::fxlink::Features::default())
    }
    pub fn new_with_ocr_and_fxlink(
        root: impl AsRef<Path>,
        ffmpeg: &str,
        tesseract: &str,
        features: crate::fxlink::Features,
    ) -> Self {
        Self {
            root: root.as_ref().into(),
            ffmpeg: ffmpeg.into(),
            tesseract: tesseract.into(),
            hls_epoch: crate::hls_generation::Epoch::for_root(root.as_ref()),
            workers: Mutex::new(HashMap::new()),
            selected_hls_cache: std::sync::Mutex::new(SelectedHlsCache::default()),
            selected_hls_miss_slots: Arc::new(Semaphore::new(2)),
            caption_filter_slots: Arc::new(Semaphore::new(2)),
            #[cfg(test)]
            selected_hls_waiters: AtomicUsize::new(0),
            input_preferences: Mutex::new(HashMap::new()),
            startup_gates: std::sync::Mutex::new(HashMap::new()),
            rtsp_probe_slots: Semaphore::new(32),
            fxlink_pool: Arc::new(crate::fxlink::ClientPool::with_features(features)),
            gpu: crate::gpu::Checks::default(),
            idle_memory: crate::idle_memory::Controller::default(),
            direct_egress: Arc::new(AtomicU64::new(0)),
            rtsp_push_egress: Arc::new(AtomicU64::new(0)),
            http_push_egress: Arc::new(AtomicU64::new(0)),
            srtp_receiver_ledger: None,
            #[cfg(test)]
            hls_prepare_test_hook: None,
            #[cfg(test)]
            before_remove_test_hook: None,
        }
    }
    async fn prepare_hls(&self, input: &str) -> Result<crate::peer_hls::PreparedOrigin, String> {
        #[cfg(test)]
        if let Some(hook) = &self.hls_prepare_test_hook {
            return hook(input.to_owned()).await;
        }
        crate::peer_hls::prepare(input).await
    }
    pub fn set_srtp_receiver_ledger(
        &mut self,
        ledger: Option<Arc<crate::direct_rtp::key_ledger::Ledger>>,
    ) {
        self.srtp_receiver_ledger = ledger;
    }
    pub async fn gpu_capabilities(&self) -> Value {
        let _activity = self.idle_memory.activity().await;
        json!(self.gpu.report(&self.ffmpeg).await)
    }
    pub async fn vaapi_capabilities(&self) -> Value {
        let _activity = self.idle_memory.activity().await;
        json!(self.gpu.vaapi_report(&self.ffmpeg).await)
    }
    /// The daemon owns and joins this optional process-wide maintenance task.
    pub async fn maintain_idle_memory(&self, cancel: CancellationToken) {
        self.idle_memory.run(cancel).await;
    }
    fn directory(&self, name: &str) -> PathBuf {
        self.root
            .join(format!("{:x}", Sha256::digest(name.as_bytes())))
    }
    fn startup_gate(&self, name: &str) -> Arc<Mutex<()>> {
        let mut gates = self.startup_gates.lock().unwrap();
        if gates.len() > 1024 {
            gates.retain(|_, gate| gate.strong_count() > 0);
        }
        if let Some(gate) = gates.get(name).and_then(std::sync::Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(name.to_owned(), Arc::downgrade(&gate));
        gate
    }
    pub async fn preferred_input(&self, name: &str, cfg: &Value) -> Option<usize> {
        let fingerprint = input_fingerprint(cfg)?;
        self.input_preferences
            .lock()
            .await
            .get(name)
            .filter(|preference| preference.inputs == fingerprint)
            .map(|preference| preference.index)
    }
    pub async fn forget_input(&self, name: &str) {
        self.input_preferences.lock().await.remove(name);
    }
    pub async fn prune_input_preferences(&self, config: &crate::config::ConfigStore) {
        let mut preferences = self.input_preferences.lock().await;
        // Read the current root while holding the preference lock. A delayed
        // caller must never prune a preference accepted against a newer edit.
        config.read(|root| {
            preferences.retain(|name, preference| {
                crate::config::effective(root, name).and_then(|cfg| input_fingerprint(&cfg))
                    == Some(preference.inputs)
            });
        });
    }
    pub async fn select_input_guarded(
        &self,
        name: &str,
        cfg: &Value,
        index: usize,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<bool, String> {
        let inputs = cfg["inputs"].as_array().ok_or("stream has no input")?;
        if index >= inputs.len() {
            return Err("input is not found by the specified index".into());
        }
        if cfg["disabled"] == true {
            return Err("stream disabled".into());
        }
        if inputs.iter().any(|input| input["url"] == "publish://") {
            return Err("publisher input cannot be selected".into());
        }
        let fingerprint = input_fingerprint(cfg).ok_or("stream has no input")?;
        let startup = self.startup_gate(name).lock_owned().await;
        if !current.await {
            return Err("media route changed".into());
        }
        let mut workers = self.workers.lock().await;
        if workers
            .get(name)
            .is_some_and(|worker| worker.publication || worker.shared_generation.is_some())
        {
            return Err("shared or publisher worker cannot be selected".into());
        }
        let signature = media_signature(cfg);
        if workers
            .get(name)
            .is_some_and(|worker| worker.signature != signature)
        {
            return Err("media route changed".into());
        }
        let active = workers.get(name).is_some_and(|worker| {
            worker.alive.load(Ordering::Relaxed)
                && !worker.is_closed()
                && worker.signature == signature
        });
        // An on-demand worker in retry cooldown still owns recent demand.
        // Retiring it must immediately hand that demand to the selected input.
        let retry_demand = workers.get(name).is_some_and(|worker| {
            !active
                && worker.signature == signature
                && (worker.idle_seconds() < 60
                    || worker.viewers.load(Ordering::Relaxed) > 0
                    || worker.active_pushes)
        });
        let same = active
            && workers
                .get(name)
                .is_some_and(|worker| worker.input_index == index);
        if same {
            self.input_preferences.lock().await.insert(
                name.to_owned(),
                InputPreference {
                    index,
                    inputs: fingerprint,
                },
            );
            return Ok(false);
        }
        let retired = workers.remove(name);
        drop(workers);
        let _startup = if let Some(worker) = retired {
            self.retire_owned(name, worker, startup).await?
        } else {
            startup
        };
        self.input_preferences.lock().await.insert(
            name.to_owned(),
            InputPreference {
                index,
                inputs: fingerprint,
            },
        );
        Ok(active || retry_demand)
    }
    pub async fn publish_guarded(
        &self,
        name: &str,
        cfg: &Value,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<Publication, String> {
        self.publish_mode(name, cfg, current, PublicationInput::MpegTs)
            .await
    }
    pub(crate) async fn publish_sdp_guarded(
        &self,
        name: &str,
        cfg: &Value,
        secure: bool,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<Publication, String> {
        self.publish_mode(name, cfg, current, PublicationInput::Sdp { secure })
            .await
    }
    async fn publish_mode(
        &self,
        name: &str,
        cfg: &Value,
        current: impl std::future::Future<Output = bool>,
        mode: PublicationInput,
    ) -> Result<Publication, String> {
        if !crate::publish::is_input(cfg) {
            return Err("stream does not accept publications".into());
        }
        let worker = self
            .ensure_mode(
                name,
                cfg,
                true,
                current,
                StartMode {
                    publishing: Some(mode),
                    ..Default::default()
                },
            )
            .await?;
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
    #[cfg(test)]
    pub(crate) async fn hold_startups_for_test(&self) -> impl Drop + '_ {
        self.workers.lock().await
    }
    pub async fn ensure_guarded(
        &self,
        name: &str,
        cfg: &Value,
        touch_demand: bool,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<Arc<Worker>, String> {
        self.ensure_mode(name, cfg, touch_demand, current, StartMode::default())
            .await
    }
    pub(crate) async fn recover_demand_guarded(
        &self,
        name: &str,
        cfg: &Value,
        last_playback: Instant,
        current: impl std::future::Future<Output = bool>,
    ) -> Result<Arc<Worker>, String> {
        self.ensure_mode(
            name,
            cfg,
            false,
            current,
            StartMode {
                last_playback: Some(last_playback),
                ..Default::default()
            },
        )
        .await
    }
    async fn ensure_mode(
        &self,
        name: &str,
        cfg: &Value,
        touch_demand: bool,
        current: impl std::future::Future<Output = bool>,
        mode: StartMode,
    ) -> Result<Arc<Worker>, String> {
        if crate::iptv::tokenless_output_conflict(cfg) {
            return Err(crate::iptv::TOKENLESS_OUTPUT_ERROR.into());
        }
        let activity = self.idle_memory.activity().await;
        let StartMode {
            publishing,
            last_playback,
            mut shared,
        } = mode;
        let shared_generation = shared.as_ref().map(|s| s.feed.generation());
        let subtitle_tracks = crate::config::subtitle_tracks(cfg)?;
        let destinations = crate::push::configuration(cfg)?;
        let direct_destinations = crate::direct_rtp::config::outputs(cfg)?;
        let hls_subtitles = crate::config::hls_subtitles(cfg)?;
        let caption_services = crate::captions::configuration(cfg)?;
        let native_captions = caption_services.iter().any(|s| s.native_track().is_some());
        let caption_separate = caption_services.iter().any(|s| s.channel >= 1124);
        if hls_subtitles == "convert" && caption_services.is_empty() {
            return Err("Choose at least one HLS caption channel for conversion".into());
        }
        if !caption_services.is_empty()
            && (cfg["inputs"]
                .as_array()
                .is_some_and(|a| a.iter().any(|i| i["url"] == "testsrc://"))
                || matches!(
                    cfg["transcoder"]["encoder"].as_str(),
                    Some("h264_nvenc" | "hevc_nvenc" | "h264_vaapi" | "hevc_vaapi")
                ))
        {
            return Err("HLS caption conversion requires a real H.264/HEVC video source; GPU conversion is not qualified".into());
        }
        let startup = self.startup_gate(name).lock_owned().await;
        let preferred_input = if publishing.is_none() && shared.is_none() {
            self.preferred_input(name, cfg).await
        } else {
            None
        };
        let profile = crate::transcoder::Profile::resolve(cfg, false)?;
        // A DESCRIBE-only codec probe must not hold the global worker map over
        // a network round trip. A per-stream gate prevents cold-viewer herds.
        let rtsp_candidate = if profile.video_copy()
            && publishing.is_none()
            && cfg["disabled"] != true
            && !shared
                .as_ref()
                .is_some_and(|s| s.input.is_none() || !s.feed.current())
        {
            let workers = self.workers.lock().await;
            let signature = media_signature(cfg);
            let existing = workers.get(name);
            let running = existing.is_some_and(|w| {
                w.alive.load(Ordering::Relaxed)
                    && !w.is_closed()
                    && w.signature == signature
                    && w.shared_generation == shared_generation
            });
            let index = existing
                .filter(|w| {
                    !running && w.signature == signature && w.shared_generation == shared_generation
                })
                .map_or(preferred_input.unwrap_or(0), |w| w.input_index + 1);
            if running {
                None
            } else {
                cfg["inputs"]
                    .as_array()
                    .filter(|inputs| !inputs.is_empty())
                    .and_then(|inputs| {
                        let selected = inputs.get(index % inputs.len())?;
                        let url = selected["url"].as_str()?;
                        (["rtsp://", "rtsp-udp://", "rtsp2://", "rtsps://"]
                            .iter()
                            .any(|prefix| url.starts_with(prefix)))
                        .then(|| {
                            (
                                url.to_owned(),
                                selected["flussonix_tls_ca"].as_str().map(PathBuf::from),
                                selected["flussonix_rtsp_startup"]
                                    .as_str()
                                    .map(str::to_owned),
                            )
                        })
                    })
            }
        } else {
            None
        };
        // Distinct cold streams have separate gates. Reserve before any codec
        // network I/O, then check capacity without holding the worker map over
        // the remote probe. The final worker-map check remains authoritative.
        let _probe_slot = if rtsp_candidate
            .as_ref()
            .is_some_and(|(_, _, mode)| mode.is_none())
        {
            let slot = self
                .rtsp_probe_slots
                .try_acquire()
                .map_err(|_| "RTSP probe limit reached")?;
            {
                let workers = self.workers.lock().await;
                if workers
                    .values()
                    .filter(|w| w.alive.load(Ordering::Relaxed))
                    .count()
                    >= 256
                    && !workers
                        .get(name)
                        .is_some_and(|w| w.alive.load(Ordering::Relaxed))
                {
                    return Err("worker limit reached".into());
                }
            }
            Some(slot)
        } else {
            None
        };
        self.gpu.require_profile(&self.ffmpeg, &profile).await?;
        // Reject an obviously unusable explicit FFmpeg path before Auto makes
        // an upstream DESCRIBE request. Command::spawn remains authoritative
        // because the file or its permissions can change after this check.
        if rtsp_candidate
            .as_ref()
            .is_some_and(|(_, _, mode)| mode.is_none())
            && self.ffmpeg.contains('/')
        {
            let executable =
                std::fs::metadata(&self.ffmpeg).map_err(|e| format!("cannot start FFmpeg: {e}"))?;
            if !executable.is_file() {
                return Err("cannot start FFmpeg: executable path is not a file".into());
            }
            #[cfg(unix)]
            if executable.permissions().mode() & 0o111 == 0 {
                return Err("cannot start FFmpeg: executable path is not executable".into());
            }
        }
        let h264_input = if let Some((input, ca, mode)) = rtsp_candidate {
            let h264 = match mode.as_deref() {
                Some("h264") => true,
                Some("conservative") => false,
                _ => {
                    crate::rtsp_probe::video_codec(&input, ca.as_deref()).await
                        == Some(crate::rtsp_probe::VideoCodec::H264)
                }
            };
            h264.then_some(input)
        } else {
            None
        };
        // The per-stream gate keeps this selection stable against another
        // startup. DNS must run before taking the shared worker-map lock.
        let hls_candidate = if publishing.is_none()
            && cfg["disabled"] != true
            && !shared.as_ref().is_some_and(|s| s.input.is_none())
        {
            let workers = self.workers.lock().await;
            let signature = media_signature(cfg);
            let existing = workers.get(name);
            let running = existing.is_some_and(|w| {
                w.alive.load(Ordering::Relaxed)
                    && !w.is_closed()
                    && w.signature == signature
                    && w.shared_generation == shared_generation
            });
            let index = existing
                .filter(|w| {
                    !running && w.signature == signature && w.shared_generation == shared_generation
                })
                .map_or(preferred_input.unwrap_or(0), |w| w.input_index + 1);
            if running {
                None
            } else {
                cfg["inputs"]
                    .as_array()
                    .filter(|inputs| !inputs.is_empty())
                    .and_then(|inputs| {
                        let selected_index = index % inputs.len();
                        let input = inputs[selected_index]["url"].as_str()?;
                        [
                            "hls://",
                            "hlss://",
                            "tshttp://",
                            "tshttps://",
                            "http://",
                            "https://",
                        ]
                        .iter()
                        .any(|prefix| input.starts_with(prefix))
                        .then(|| (selected_index, input.to_owned()))
                    })
            }
        } else {
            None
        };
        let hls_start_index = hls_candidate.as_ref().map(|(index, _)| *index);
        let mut selected_hls_index = None;
        let mut prepared_hls = None;
        if let Some((first_index, _)) = hls_candidate {
            let inputs = cfg["inputs"].as_array().ok_or("stream has no input")?;
            let mut candidate = first_index;
            let mut first_dns_error = None;
            loop {
                let input = inputs[candidate]["url"]
                    .as_str()
                    .ok_or("input URL required")?;
                if ![
                    "hls://",
                    "hlss://",
                    "tshttp://",
                    "tshttps://",
                    "http://",
                    "https://",
                ]
                .iter()
                .any(|prefix| input.starts_with(prefix))
                {
                    selected_hls_index = Some(candidate);
                    break;
                }
                let translated = translate_input(input)?;
                let result = if cfg["flussonix_peer_key"].is_string() {
                    crate::peer_hls::prepare_peer(
                        &translated,
                        cfg["flussonix_allow_insecure_peer_http"] == true,
                    )
                    .await
                } else {
                    self.prepare_hls(&translated).await
                };
                match result {
                    Ok(prepared) => {
                        selected_hls_index = Some(candidate);
                        prepared_hls = Some((candidate, input.to_owned(), prepared));
                        break;
                    }
                    Err(error)
                        if matches!(
                            error.as_str(),
                            "peer media DNS failed"
                                | "peer media DNS timeout"
                                | "peer media DNS returned no addresses"
                        ) =>
                    {
                        first_dns_error.get_or_insert(error);
                        candidate = (candidate + 1) % inputs.len();
                        if candidate == first_index {
                            return Err(first_dns_error.unwrap());
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
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
        if shared.as_ref().is_some_and(|s| !s.feed.current()) {
            return Err("media route changed".into());
        }
        let signature = media_signature(cfg);
        let mut index = preferred_input.unwrap_or(0);
        let mut restart_count = 0;
        let mut streak = 0;
        let mut last_access = Arc::new(std::sync::Mutex::new(
            last_playback.unwrap_or_else(Instant::now),
        ));
        let mut viewers = Arc::new(AtomicU64::new(0));
        if let Some(w) = workers.get(name) {
            if touch_demand {
                w.touch();
            }
            // Body guards can outlive the generation that served them.
            last_access = w.last_access.clone();
            viewers = w.viewers.clone();
            let running = w.alive.load(Ordering::Relaxed)
                && !w.is_closed()
                && w.shared_authority.as_ref().is_none_or(|current| current());
            if shared_generation.is_some()
                && running
                && (w.shared_generation != shared_generation || w.signature != signature)
            {
                return Err("shared packaging mismatch".into());
            }
            if running && w.signature == signature && w.shared_generation == shared_generation {
                if publishing.is_some() {
                    return Err("publisher already connected".into());
                }
                return Ok(w.clone());
            }
            if !running && w.signature == signature && w.shared_generation == shared_generation {
                let recovery = w.recovery.lock().unwrap();
                if publishing.is_none() && recovery.retry_in().is_some_and(|delay| !delay.is_zero())
                {
                    return Err("input retry backoff".into());
                }
                index = w.input_index + 1;
                restart_count = w.restart_count.saturating_add(1);
                streak = recovery.next_streak();
            }
        }
        if shared.as_ref().is_some_and(|s| s.input.is_none()) {
            return Err("shared input startup owner required".into());
        }
        let inputs = cfg["inputs"]
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or("stream has no input")?;
        index %= inputs.len();
        if let Some(start_index) = hls_start_index {
            if index != start_index {
                return Err("media route changed".into());
            }
            index = selected_hls_index.ok_or("media route changed")?;
        }
        let input = inputs[index]["url"].as_str().ok_or("input URL required")?;
        let mut prepared_hls = if [
            "hls://",
            "hlss://",
            "tshttp://",
            "tshttps://",
            "http://",
            "https://",
        ]
        .iter()
        .any(|prefix| input.starts_with(prefix))
        {
            let (prepared_index, prepared_input, prepared) =
                prepared_hls.ok_or("media route changed")?;
            if prepared_index != index || prepared_input != input {
                return Err("media route changed".into());
            }
            Some(prepared)
        } else {
            None
        };
        if native_captions
            && !["m4f://", "m4fs://", "m4s://", "m4ss://", "fxlink://"]
                .iter()
                .any(|prefix| input.starts_with(prefix))
        {
            return Err("native text conversion requires an M4F/M4S/FXLink input".into());
        }
        if workers
            .values()
            .filter(|w| w.alive.load(Ordering::Relaxed))
            .count()
            >= 256
        {
            return Err("worker limit reached".into());
        }
        let publication = crate::publish::is_input(cfg);
        if publication && publishing.is_none() {
            return Err("waiting for publisher".into());
        }
        let dir = self.directory(name);
        let retired = workers.remove(name);
        let replaced = retired.is_some();
        drop(workers);
        // The task owns the stream gate through join, durable allocation, and
        // deletion even if the requesting viewer disconnects.
        let (_startup, sequence) = self.prepare_generation(name, retired, startup).await?;
        let generation = uuid::Uuid::new_v4().simple().to_string();
        let discontinuity = if replaced { "+discont_start" } else { "" };
        let caption_listener = if caption_services.is_empty() || native_captions {
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
        let captions = shared
            .as_ref()
            .and_then(|s| s.feed.conversion())
            .or_else(|| {
                (!caption_services.is_empty()).then(|| {
                    Arc::new(crate::caption_hls::State::new(
                        crate::captions::Decoder::new(caption_services),
                        generation.clone(),
                        sequence,
                    ))
                })
            });
        let timeout = Duration::from_secs(
            cfg["flussonix_input_timeout"]
                .as_u64()
                .unwrap_or(15)
                .clamp(1, 300),
        );
        let mut cmd = crate::child_process::command(&self.ffmpeg);
        cmd.args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-threads",
            "2",
        ]);
        crate::transcoder::Profile::resolve(cfg, false)?.prepare(&mut cmd);
        let mut peer_hls = None;
        let mut owned_input = None;
        let mut proxy_hls_extensions = false;

        let ts_input = input.starts_with("tshttp://") || input.starts_with("tshttps://");
        let peer_ts = cfg["flussonix_peer_key"].is_string() && ts_input;
        let allow_insecure_peer_http = cfg["flussonix_allow_insecure_peer_http"] == true;
        let mut tls_input = None;
        let mut direct_input = None;
        let srt_input = if input.starts_with("srt://") {
            Some(crate::srt_input::Input::parse(input)?)
        } else {
            None
        };
        let synthetic = input == "testsrc://";
        let m4s_input = input.starts_with("m4s://") || input.starts_with("m4ss://");
        let m4f_input = input.starts_with("m4f://") || input.starts_with("m4fs://");
        let fxlink_input = input.starts_with("fxlink://");
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
        } else if matches!(publishing, Some(PublicationInput::Sdp { .. })) {
            // The SDP demuxer's own inactivity deadline must not shorten the
            // configured publication grace period before our watchdog fires.
            cmd.arg("-listen_timeout").arg(
                cfg["flussonix_input_timeout"]
                    .as_u64()
                    .unwrap_or(15)
                    .clamp(1, 300)
                    .to_string(),
            );
            cmd.args([
                "-localaddr",
                "127.0.0.1",
                "-protocol_whitelist",
                "pipe,udp,rtp",
                "-probesize",
                "1048576",
                "-analyzeduration",
                "1000000",
                "-f",
                "sdp",
                "-i",
                "pipe:0",
            ]);
            cmd.stdin(std::process::Stdio::piped());
        } else if publication {
            // Probe across a normal GOP, leaving startup/mux headroom under
            // shorter configured watchdogs. Keep fractional seconds nonzero.
            cmd.args([
                "-protocol_whitelist",
                "pipe",
                "-probesize",
                "1048576",
                "-analyzeduration",
            ])
            .arg((timeout.as_micros() / 2).min(5_000_000).to_string())
            .args(["-fix_teletext_pts", "0", "-f", "mpegts", "-i", "pipe:0"]);
            cmd.stdin(std::process::Stdio::piped());
        } else if m4s_input || m4f_input || fxlink_input {
            // M4F arrives as complete GOP objects. Read the bounded bridge at
            // media rate so HTTP TS late joins need not wait for the next GOP.
            if m4f_input {
                cmd.args(["-readrate", "1"]);
            }
            cmd.args([
                "-probesize",
                "1048576",
                "-analyzeduration",
                "1000000",
                "-fix_teletext_pts",
                "0",
                "-f",
                "mpegts",
                "-i",
                "pipe:0",
            ]);
            cmd.stdin(std::process::Stdio::piped());
        } else if srt_input.is_some() {
            // The native libsrt receiver owns the upstream URL and secrets.
            // FFmpeg sees only a local MPEG-TS byte pipe.
            cmd.args([
                "-probesize",
                "1048576",
                "-analyzeduration",
                "1000000",
                "-fix_teletext_pts",
                "0",
                "-f",
                "mpegts",
                "-i",
                "pipe:0",
            ]);
            cmd.stdin(std::process::Stdio::piped());
        } else if let Some(settings) = crate::direct_rtp::config::Settings::input(&inputs[index])? {
            direct_input = Some(
                crate::direct_rtp::input::Input::bind_with_ledger(
                    &settings,
                    self.srtp_receiver_ledger.clone(),
                )
                .await?,
            );
            if settings.elementary {
                // SDP connection addresses describe a peer; FFmpeg otherwise
                // binds wildcard local sockets. Decrypted media stays private.
                cmd.args(["-localaddr", "127.0.0.1"]);
            } else {
                cmd.args(["-fix_teletext_pts", "0"]);
            }
            cmd.args([
                "-protocol_whitelist",
                if settings.elementary {
                    "pipe,udp,rtp"
                } else {
                    "pipe"
                },
                "-probesize",
                "1048576",
                "-analyzeduration",
                "1000000",
                "-f",
                if settings.elementary { "sdp" } else { "mpegts" },
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
            if translated.starts_with("http") {
                cmd.args(["-rw_timeout", "10000000"]);
            }
            if let Some(key) = cfg["flussonix_peer_key"].as_str() {
                let ca = inputs[index]["flussonix_tls_ca"]
                    .as_str()
                    .map(std::path::Path::new);
                let proxy = crate::peer_hls::PeerHls::start_inspected_prepared(
                    prepared_hls.take().ok_or("media route changed")?,
                    key,
                    ca,
                    allow_insecure_peer_http,
                )
                .await?;

                translated = proxy.url.clone();
                // All remote resources are fetched inside our origin-scoped
                // proxy; FFmpeg never receives the native peer credential.
                proxy_hls_extensions = !peer_ts
                    && !url::Url::parse(&translate_input(input)?)
                        .is_ok_and(|u| u.path().ends_with("/mpegts"));
                peer_hls = Some(proxy);
            } else if [
                "hls://",
                "hlss://",
                "tshttp://",
                "tshttps://",
                "http://",
                "https://",
            ]
            .iter()
            .any(|prefix| input.starts_with(prefix))
            {
                let live = input.starts_with("tshttps://")
                    || input.starts_with("tshttp://")
                    || ((input.starts_with("https://") || input.starts_with("http://"))
                        && !url::Url::parse(&translated)
                            .is_ok_and(|u| u.path().ends_with(".m3u8")));
                let proxy = crate::peer_hls::PeerHls::start_external_prepared(
                    prepared_hls.take().ok_or("media route changed")?,
                    inputs[index]["flussonix_tls_ca"].as_str().map(Path::new),
                    live,
                )
                .await?;
                translated = proxy.url.clone();
                // Only the origin-scoped fetcher contacts this source. The
                // decoder receives neither Basic credentials nor a peer key.
                proxy_hls_extensions = !live;
                peer_hls = Some(proxy);
            }
            if ts_input && peer_hls.is_none() {
                // Our source mux may deliver a subtitle PES before its video.
                // FFmpeg's default PCR heuristic clamps that valid PTS, making
                // cue timing depend on mux scheduling. Preserve the PES clock.
                cmd.args(["-fix_teletext_pts", "0"]);
            }
            if translated.starts_with("rtsp://") {
                let transport = if input.starts_with("rtsp-udp://") || inputs[index]["rtp"] == "udp"
                {
                    "udp"
                } else {
                    "tcp"
                };
                owned_input = Some(OwnedInput::Script(rtsp_input_script(
                    &translated,
                    transport,
                )?));
                cmd.args([
                    "-protocol_whitelist",
                    "pipe,rtsp,tcp,udp,rtp,crypto",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    "pipe:0",
                ]);
                cmd.stdin(std::process::Stdio::piped());
            } else if peer_hls.is_some() {
                // The local proxy URL is a bearer capability. A subprocess
                // command line is visible to other local users on common
                // Linux configurations, so pass it through the owned pipe.
                if proxy_hls_extensions {
                    owned_input = Some(OwnedInput::Script(proxy_input_script(&translated)?));
                    cmd.args([
                        "-protocol_whitelist",
                        "pipe,http,tcp,crypto",
                        "-f",
                        "concat",
                        "-safe",
                        "0",
                        "-i",
                        "pipe:0",
                    ]);
                } else {
                    validate_local_media_proxy_url(&translated)?;
                    owned_input = Some(OwnedInput::LiveTs(translated.clone()));
                    // Force MPEG-TS. Autodetection here can turn a mismatched
                    // response into an HLS playlist with nested media fetches.
                    cmd.args(["-fix_teletext_pts", "0", "-f", "mpegts", "-i", "pipe:0"]);
                }
                cmd.stdin(std::process::Stdio::piped());
            } else {
                cmd.args(["-i", &translated]);
            }
        }
        if proxy_hls_extensions {
            // Preserve source ordinals for tracks-vNaM selectors. Keep the
            // mapped set bounded by the public selector's 1..=16 range.
            for kind in ['v', 'a'] {
                for ordinal in 0..16 {
                    cmd.args(["-map", &format!("0:{kind}:{ordinal}?")]);
                }
            }
        } else {
            cmd.args([
                "-map",
                "0:v:0?",
                "-map",
                if synthetic {
                    "1:a:0?"
                } else if m4s_input
                    || m4f_input
                    || fxlink_input
                    || publication
                    || peer_ts
                    || direct_input.is_some()
                {
                    "0:a?"
                } else {
                    "0:a:0?"
                },
            ]);
        }
        let profile = crate::transcoder::Profile::resolve_for_input(cfg, input)?;
        profile.apply(&mut cmd);
        if owned_input.is_some()
            && peer_hls.is_none()
            && profile.video_copy()
            && h264_input.as_deref() != Some(input)
        {
            // HEVC can start with an untimed reordered unit. Hold copied video
            // until a timed keyframe so TS does not start on an inter picture.
            // Confirmed H.264 skips this gate to preserve its initial GOP.
            cmd.args([
                "-bsf:v",
                r"noise=drop=if(ld(0)\,0\,if(key*gt(pts\,nopts)*gt(dts\,nopts)\,st(0\,1)*0\,1))",
            ]);
        }
        // RTP AAC depacketizers can omit key flags; copied units are independent.
        if profile.audio_copy()
            && (matches!(publishing, Some(PublicationInput::Sdp { .. }))
                || m4s_input
                || m4f_input
                || fxlink_input
                || input.starts_with("rtsp://")
                || input.starts_with("rtsp-udp://")
                || input.starts_with("rtsp2://")
                || input.starts_with("rtsps://")
                || (direct_input.is_some()
                    && inputs[index]["flussonix_rtp"]["profile"] == "elementary"))
        {
            cmd.arg("-copyinkf:a");
        }
        let raw_hls = cfg["flussonix_hls_subtitles"] == "passthrough";
        // The HLS muxer accepts only one video stream. Playlist inputs can
        // expose several video ordinals, so publish their shared TS segments
        // through the bounded segment-list path used by raw subtitle output.
        let segmented_hls = raw_hls || proxy_hls_extensions;
        // Separate DVB/teletext tracks belong on TS-based delivery. Copy their
        // encoded PES; AV-only slaves below never receive incompatible codecs.
        if subtitle_tracks == "preserve" || raw_hls {
            cmd.args(["-map", "0:s?", "-c:s", "copy"]);
            // Broadcast subtitle services can be silent indefinitely. Bound the
            // common tee queue before its AV-only slaves select their streams.
            cmd.args(["-max_interleave_delta", "100000"]);
        }
        // One encode/mux source feeds both HLS variants and shared live TS fan-out.
        let copy_publication = (publication || direct_input.is_some()) && profile.audio_copy();
        let inspect_publication =
            matches!(publishing, Some(PublicationInput::MpegTs)) && profile.audio_copy();
        let native_copy = (m4s_input || m4f_input || fxlink_input) && profile.full_copy();
        let native_audio_copy = (m4s_input || m4f_input || fxlink_input) && profile.audio_copy();
        let copy_peer_ts = peer_ts && profile.audio_copy();
        // Original subtitle PES stays on stdout. The native decoder receives
        // only AV, either directly or through this owned tee connection.
        let av_listener = if subtitle_tracks == "preserve" && !native_copy {
            Some(
                tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .map_err(|_| "cannot bind worker AV channel")?,
            )
        } else {
            None
        };
        let wire_output = if let Some(listener) = &av_listener {
            format!(
                "|[select='{}':f=mpegts:max_interleave_delta=100000]tcp://{}",
                if proxy_hls_extensions {
                    "v\\:0,a"
                } else {
                    "v,a"
                },
                listener
                    .local_addr()
                    .map_err(|_| "worker AV address unavailable")?
            )
        } else {
            String::new()
        };
        let fmp4_filter = if native_audio_copy {
            "__NATIVE_FMP4_FILTER__"
        } else if copy_peer_ts {
            "__PEER_FMP4_FILTER__"
        } else if inspect_publication {
            "__PUBLICATION_FMP4_FILTER__"
        } else if copy_publication {
            ":bsfs/a=aac_adtstoasc"
        } else {
            ""
        };
        let fmp4_failure =
            if native_audio_copy || copy_publication || copy_peer_ts || proxy_hls_extensions {
                "onfail=ignore:"
            } else {
                ""
            };
        // The nested live TS mux has its own interleave queue as well.
        let ts_interleave = if subtitle_tracks == "preserve" {
            ":max_interleave_delta=100000"
        } else {
            ""
        };
        let ts_hls = if segmented_hls {
            format!(
                "[select='{}':f=segment:max_interleave_delta=100000:segment_format=mpegts:segment_format_options=max_interleave_delta=100000:segment_time=2:segment_list_size=6:segment_list_flags=live:segment_list_type=m3u8:segment_list={}]{}",
                if raw_hls { "v,a,s" } else { "v,a" },
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
        } else if proxy_hls_extensions {
            // The native M4S reconstruction accepts one video. TS-HLS still
            // carries every selected source ordinal in the separate segment
            // slave above.
            "select='v\\:0,a':"
        } else {
            "select='v,a':"
        };
        let output = format!(
            "{ts_hls}|[{fmp4_failure}select='{}':f=hls:hls_time=2:hls_list_size=6:hls_delete_threshold=2:start_number={sequence}:hls_segment_type=fmp4:hls_segment_filename={}:hls_fmp4_init_filename=g{generation}_init.mp4:hls_flags=delete_segments+temp_file{discontinuity}{fmp4_filter}]{}|[{live_select}f=mpegts{ts_interleave}]pipe:1{wire_output}",
            if proxy_hls_extensions {
                "v\\:0,a\\:0"
            } else {
                "v,a"
            },
            dir.join("fmp4")
                .join(format!("g{generation}_%d.m4s"))
                .display(),
            dir.join("fmp4/index.m3u8").display()
        );
        let native_input = m4s_input || m4f_input || fxlink_input;
        if !native_input && !peer_ts && !inspect_publication {
            cmd.args(["-threads", "2", "-f", "tee", &output]);
        }
        if !native_input && !peer_ts && !inspect_publication {
            if let Some(target) = caption_target.as_deref() {
                caption_output(&mut cmd, target, caption_separate);
            }
        }
        cmd.stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = if native_input || peer_ts || inspect_publication {
            None
        } else {
            Some(
                cmd.spawn()
                    .map_err(|e| format!("cannot start FFmpeg: {e}"))?,
            )
        };
        let pid = child.as_ref().and_then(|c| c.id()).unwrap_or(0);
        let tx = ts_channel();
        let mut stdin = child.as_mut().and_then(|c| c.stdin.take());
        // Reserve the publisher immediately, but bound metadata collection before
        // spawning a copy packager whose fMP4 filters depend on the PMT codecs.
        let (publisher_stdin, publication_reader) = if inspect_publication {
            let (writer, reader) = tokio::io::duplex(64 * 1024);
            (Some(Box::new(writer) as PublicationWriter), Some(reader))
        } else {
            (
                if publication {
                    stdin.take().map(|s| Box::new(s) as PublicationWriter)
                } else {
                    None
                },
                None,
            )
        };
        let (done_tx, done) = oneshot::channel();
        let cancel = CancellationToken::new();
        let worker = Arc::new(Worker {
            publisher_stdin: std::sync::Mutex::new(publisher_stdin),
            publication,
            tx,
            cancel: cancel.clone(),
            done: Mutex::new(Some(done)),
            pid: AtomicU32::new(pid),
            started: Instant::now(),
            signature,
            subtitle_tracks,
            hls_subtitles,
            native_text_tracks: AtomicU64::new(0),
            fxlink_telemetry: std::sync::Mutex::new(None),
            captions,
            pushes: destinations
                .into_iter()
                .enumerate()
                .map(|(i, d)| {
                    crate::push::State::new(
                        d,
                        i,
                        self.rtsp_push_egress.clone(),
                        self.http_push_egress.clone(),
                    )
                })
                .collect(),
            direct_input: direct_input.as_ref().map(|i| i.stats.clone()),
            direct_outputs: direct_destinations
                .into_iter()
                .enumerate()
                .map(|(n, d)| {
                    crate::direct_rtp::output::State::with_egress(d, n, self.direct_egress.clone())
                })
                .collect(),
            active_pushes: crate::push::enabled(cfg) || crate::direct_rtp::config::enabled(cfg),
            input_index: index,
            input_protocol: match publishing {
                Some(PublicationInput::Sdp { secure: true }) => "rtsps".into(),
                Some(PublicationInput::Sdp { secure: false }) => "rtsp".into(),
                _ => input.split("://").next().unwrap_or("unknown").into(),
            },
            restart_count,
            input_timeout: timeout,
            recovery: std::sync::Mutex::new(crate::recovery::Recovery::new(streak)),
            bytes: AtomicU64::new(0),
            output_rate: std::sync::Mutex::new(crate::media_rate::OutputRate::default()),
            hls_track_profile: std::sync::Mutex::new(None),
            selected_hls_token: Arc::new(()),
            viewers,
            alive: std::sync::atomic::AtomicBool::new(true),
            wire: if profile.full_copy() {
                shared
                    .as_ref()
                    .map(|s| s.feed.hub())
                    .unwrap_or_else(|| Arc::new(Hub::new()))
            } else {
                Arc::new(Hub::new())
            },
            shared_generation,
            shared_static_demand: shared
                .as_ref()
                .map(|s| Arc::new(s.feed.static_demand()) as Arc<dyn Fn() -> bool + Send + Sync>),
            shared_authority: shared
                .as_ref()
                .map(|s| Arc::new(s.feed.authority()) as Arc<dyn Fn() -> bool + Send + Sync>),
            last_access,
            _idle_activity: activity,
        });
        let original_wire = (m4s_input || m4f_input || fxlink_input) && profile.full_copy();
        let native_ca = inputs[index]["flussonix_tls_ca"]
            .as_str()
            .map(std::path::PathBuf::from);
        let url = input.to_owned();
        let key = cfg["flussonix_peer_key"].as_str().map(str::to_owned);
        let fxlink_pool = self.fxlink_pool.clone();
        let fxlink_settings = if fxlink_input {
            crate::config::fxlink_settings(&inputs[index])?
        } else {
            crate::fxlink::Settings::default()
        };
        let fxlink_token = inputs[index]["flussonix_fxlink_token"]
            .as_str()
            .unwrap_or("")
            .to_owned();
        let mut workers = self.workers.lock().await;
        if workers.contains_key(name)
            || workers
                .values()
                .filter(|w| w.alive.load(Ordering::Relaxed))
                .count()
                >= 256
        {
            return Err("worker limit reached".into());
        }
        if shared.as_ref().is_some_and(|s| !s.feed.current()) {
            return Err("media route changed".into());
        }
        workers.insert(name.into(), worker.clone());
        drop(workers);
        let w = worker.clone();
        let tesseract = self.tesseract.clone();
        let push_tasks: Vec<_> = worker
            .pushes
            .iter()
            .map(|state| {
                tokio::spawn(
                    state
                        .clone()
                        .run(self.ffmpeg.clone(), worker.clone(), cancel.clone()),
                )
            })
            .collect();
        let direct_tasks: Vec<_> = worker
            .direct_outputs
            .iter()
            .map(|state| tokio::spawn(state.clone().run_worker(worker.clone(), cancel.clone())))
            .collect();
        let shared_drain = shared
            .as_ref()
            .filter(|s| s.feed.protocol() == "m4f")
            .map(|s| s.feed.drain());
        let shared_local_failure: Option<Arc<dyn Fn() + Send + Sync>> = shared
            .as_ref()
            .filter(|s| s.feed.protocol() == "m4f")
            .map(|s| Arc::new(s.feed.local_failure()) as Arc<dyn Fn() + Send + Sync>);
        tokio::spawn(async move {
            let mut tasks = Vec::new();
            let shared_startup_timeout = shared.as_ref().map(|s| s.feed.startup_timeout());
            if let Some(input) = shared.as_mut().and_then(|s| s.input.take()) {
                let c = cancel.clone();
                let source_worker = w.clone();
                let drain = shared_drain.clone();
                tasks.push(tokio::spawn(async move {
                    let result =
                        tokio::select! {biased;_=c.cancelled()=>return,result=input.run()=>result};
                    if let Err(reason) = result
                        && !c.is_cancelled()
                    {
                        tracing::debug!(target: "flussonix::worker_lifecycle", ?reason,
                            "shared input stopped before worker cancellation");
                        source_worker.failed("input_closed");
                        if let Some(deadline) = drain
                            .as_ref()
                            .filter(|d| d.is_complete())
                            .and_then(|d| d.deadline())
                        {
                            tokio::select! { biased;
                                _=c.cancelled()=>{},
                                _=tokio::time::sleep_until(deadline)=>c.cancel(),
                            }
                        } else {
                            c.cancel();
                        }
                    }
                }));
            }
            if let Some(input) = direct_input {
                if let Some(writer) = stdin.take() {
                    let c = cancel.clone();
                    let w = w.clone();
                    tasks.push(tokio::spawn(async move {
                        if input.run_owned(writer, c.clone(), w.pid()).await.is_err() {
                            w.failed("input_closed");
                            c.cancel();
                        }
                    }));
                } else {
                    w.failed("packaging_failed");
                    cancel.cancel();
                }
            }
            if let Some(input) = srt_input {
                if let Some(writer) = stdin.take() {
                    let c = cancel.clone();
                    let input_worker = w.clone();
                    tasks.push(tokio::spawn(async move {
                        if input.run(writer, c.clone()).await.is_err() && !c.is_cancelled() {
                            input_worker.failed("input_closed");
                            c.cancel();
                        }
                    }));
                } else {
                    w.failed("packaging_failed");
                    cancel.cancel();
                }
            }
            if child.is_some()
                && let Some(input) = owned_input.take()
            {
                if let Some(writer) = stdin.take() {
                    spawn_owned_input(&mut tasks, writer, input, cancel.clone(), w.clone());
                } else {
                    w.failed("packaging_failed");
                    cancel.cancel();
                }
            }
            // Early returns only leave this setup/run block. The owner always
            // cancels and joins its tasks before signaling completion.
            async {
                let mut child = if let Some(child) = child {
                    child
                } else if let Some(mut reader) = publication_reader {
                    let metadata = async {
                        let mut probe = crate::ts_profile::Probe::for_publication();
                        let mut prefix = Vec::new();
                        let mut buffer = [0u8; 16384];
                        while probe.audio.is_none() {
                            let n = reader.read(&mut buffer).await.map_err(|_| "input_closed")?;
                            if n == 0 { return Err("input_closed"); }
                            if prefix.len() + n > 1024 * 1024 { return Err("metadata_limit"); }
                            probe.push(&buffer[..n]);
                            prefix.extend_from_slice(&buffer[..n]);
                        }
                        Ok((probe, prefix))
                    };
                    let (probe, prefix) = tokio::select! { biased;
                        _=cancel.cancelled()=>return,
                        result=tokio::time::timeout(timeout, metadata)=>match result {
                            Ok(Ok(value))=>value,
                            Ok(Err(reason))=>{w.failed(reason);return;},
                            Err(_)=>{w.failed("startup_timeout");return;},
                        },
                    };
                    // Map one optional video followed by all audio in PMT order.
                    // AAC's ADTS conversion must never be applied to MPEG audio.
                    if probe.filter_mapping_known && probe.video.is_none() && !probe.audio_types.is_empty() {
                        // A complete single-program PMT proves there is no
                        // video keyframe to wait for. Multiplexes keep the GOP budget.
                        cmd = audio_publication_command(cmd, (timeout.as_micros() / 2).min(1_000_000));
                    }
                    let offset = usize::from(probe.video.is_some());
                    let filters = if probe.filter_mapping_known {
                        probe.audio_types.iter().enumerate()
                            .filter(|(_, codec)| **codec == 0x0f)
                            .map(|(index, _)| format!(":bsfs/{}=aac_adtstoasc", offset + index))
                            .collect::<String>()
                    } else {
                        // Preserve existing handling where first-program metadata
                        // cannot establish FFmpeg's complete all-audio mapping.
                        ":bsfs/a=aac_adtstoasc".into()
                    };
                    let output = output.replace("__PUBLICATION_FMP4_FILTER__", &filters);
                    cmd.args(["-threads", "2", "-f", "tee", &output]);
                    if let Some(target)=caption_target.as_deref(){caption_output(&mut cmd,target,caption_separate);}
                    if cancel.is_cancelled() {return;}
                    let mut child = match cmd.spawn() {
                        Ok(child)=>child,
                        Err(_)=>{w.failed("packaging_failed");return;},
                    };
                    w.pid.store(child.id().unwrap_or(0),Ordering::Relaxed);
                    let mut writer = child.stdin.take().unwrap();
                    let c = cancel.clone();
                    let pipe_worker = w.clone();
                    tasks.push(tokio::spawn(async move {
                        use tokio::io::AsyncWriteExt;
                        let result = tokio::select! { biased;
                            _=c.cancelled()=>return,
                            result=async {
                                writer.write_all(&prefix).await?;
                                tokio::io::copy(&mut reader, &mut writer).await
                            }=>result,
                        };
                        if result.is_err() {pipe_worker.failed("packaging_failed");c.cancel();}
                        // Dropping the owned pipe delivers EOF to the packager.
                    }));
                    child
                } else if peer_ts {
                    let bridge=peer_hls.as_mut().unwrap();
                    let profile=tokio::select!{biased;_=cancel.cancelled()=>return,result=tokio::time::timeout(timeout,bridge.metadata())=>match result {Ok(profile)=>profile,Err(_)=>{w.failed("startup_timeout");return;}}};
                    let (audio,_video)=match profile {Ok(profile)=>profile,Err(reason)=>{w.failed(match reason.as_str(){"peer transport metadata exceeds limit"=>"metadata_limit","peer transport metadata timeout"=>"startup_timeout",_=>"input_closed"});return;}};
                    let output=output.replace("__PEER_FMP4_FILTER__",if audio==Some(0x0f){":bsfs/a=aac_adtstoasc"}else{""});
                    cmd.args(["-threads","2","-f","tee",&output]);
                    if let Some(target)=caption_target.as_deref(){caption_output(&mut cmd,target,caption_separate);}
                    if cancel.is_cancelled(){return;}
                    let mut child=match cmd.spawn(){Ok(child)=>child,Err(_)=>{w.failed("packaging_failed");return;}};
                    w.pid.store(child.id().unwrap_or(0),Ordering::Relaxed);
                    if let Some(input) = owned_input.take() {
                        if let Some(writer) = child.stdin.take() {
                            spawn_owned_input(&mut tasks, writer, input, cancel.clone(), w.clone());
                        } else {
                            w.failed("packaging_failed");
                            return;
                        }
                    }
                    child
                } else {
                    let (mut reader, mut writer) = tokio::io::duplex(64 * 1024);
                    let (metadata_tx, metadata_rx) = oneshot::channel();
                    let c = cancel.clone();
                    let input_worker = w.clone();
                    let drain = shared_drain.clone();
                    tasks.push(tokio::spawn(async move {
                        let result = tokio::select! { biased;
                            _=c.cancelled()=>Ok(()),
                            result=async {
                                if let Some(mut shared)=shared {
                                    let mut metadata=Some(metadata_tx);
                                    let result=shared.feed.copy(&mut writer,&mut metadata,original_wire,&input_worker.native_text_tracks).await;
                                    // Record the specific failure before dropping the required
                                    // receiver can wake the source with a generic broken pipe.
                                    if let Err(reason)=&result {
                                        input_worker.failed(if reason == "native_subtitle_transcode_unsupported" { "native_subtitle_transcode_unsupported" } else { "input_closed" });
                                        c.cancel();
                                    }
                                    result
                                } else if fxlink_input {
                                    let subscription = fxlink_pool.subscribe(&url,key.as_deref().ok_or("FXLink input requires a native peer key")?,&fxlink_token,native_ca.as_deref(),fxlink_settings,c.child_token()).await?;
                                    *input_worker.fxlink_telemetry.lock().unwrap() = Some(subscription.telemetry());
                                    let retirement = subscription.retirement_token();
                                    crate::m4_ingest::records::consume(subscription.into_stream(),&mut writer,if original_wire {Some(&input_worker.wire)}else{None},metadata_tx,crate::m4_ingest::PullOptions { preserve: subtitle_tracks == "preserve", detected: Some(&input_worker.native_text_tracks), conversion: if native_captions {input_worker.captions.as_deref()} else {None}, ..Default::default() }, retirement).await
                                } else {
                                    crate::m4_ingest::pull_ready_with_options(&url,key.as_deref(),&mut writer,if original_wire {Some(&input_worker.wire)}else{None},metadata_tx,crate::m4_ingest::PullOptions { ca: native_ca.as_deref(), allow_insecure_peer_http, preserve: subtitle_tracks == "preserve", detected: Some(&input_worker.native_text_tracks), conversion: if native_captions {input_worker.captions.as_deref()} else {None}, ..Default::default() }).await
                                }
                            }=>result,
                        };
                        if let Err(reason) = &result {
                            input_worker.failed(if reason == "native_subtitle_transcode_unsupported" { "native_subtitle_transcode_unsupported" } else { "input_closed" });
                            tracing::warn!(error = %reason, "wire input stopped");
                        }
                        if !drain.as_ref().is_some_and(|d| d.is_complete() && result.is_ok()) {
                            c.cancel();
                        }
                    }));
                    let tracks = tokio::select! { biased;
                        _=cancel.cancelled()=>return,
                        result=tokio::time::timeout(timeout,metadata_rx)=>match result {
                            Ok(Ok(tracks))=>tracks,
                            Ok(Err(_))=>{ w.failed("input_closed"); return; },
                            Err(_)=>{ if let Some(expired)=&shared_startup_timeout {expired();} w.failed("startup_timeout"); return; },
                        },
                    };
                    let output = if native_audio_copy {
                        output.replace("__NATIVE_FMP4_FILTER__", &native_fmp4_filters(&tracks))
                    } else { output };
                    cmd.args(["-threads", "2", "-f", "tee", &output]);
                    if let Some(target)=caption_target.as_deref(){caption_output(&mut cmd,target,caption_separate);}
                    if cancel.is_cancelled() { return; }
                    let mut child = match cmd.spawn() {
                        Ok(child)=>child,
                        Err(error)=>{ w.failed("packaging_failed"); tracing::warn!(%error,"cannot start native packager"); return; },
                    };
                    w.pid.store(child.id().unwrap_or(0),Ordering::Relaxed);
                    if let Some(mut stdin) = child.stdin.take() {
                        let c = cancel.clone();
                        let pipe_worker = w.clone();
                        let progress = shared_drain.clone();
                        let local_failure = shared_local_failure.clone();
                        tasks.push(tokio::spawn(async move {
                            let mut stdin = crate::m4_ingest::authority::Writer {
                                inner: &mut stdin,
                                authority: pipe_worker.shared_authority.as_deref(),
                            };
                            let result = tokio::select! { biased;
                                _=c.cancelled()=>return,
                                result=async {
                                    if let Some(progress)=progress.as_deref(){
                                        copy_to_packager(&mut reader,&mut stdin,progress).await
                                    }else{
                                        tokio::io::copy(&mut reader,&mut stdin).await
                                    }
                                }=>result,
                            };
                            if result.is_err() {
                                if let Some(commit) = local_failure {
                                    commit();
                                }
                                pipe_worker.failed("packaging_failed");
                                c.cancel();
                            }
                        }));
                    }
                    child
                };
                let hls_generation = generation.clone();
                if segmented_hls {
                    let dir = dir.clone(); let c = cancel.clone(); let worker = w.clone();
                    tasks.push(tokio::spawn(async move {
                        if crate::raw_hls::watch(dir, generation, sequence, replaced, c.clone()).await.is_err() {
                            worker.failed("packaging_failed"); c.cancel();
                        }
                    }));
                }
                {
                    let dir = dir.clone(); let c = cancel.clone(); let worker = w.clone(); let bytes_worker = w.clone();
                    tasks.push(tokio::spawn(async move {
                        if let Err(reason) = crate::hls_budget::watch(dir, hls_generation, segmented_hls, move || bytes_worker.bytes.load(Ordering::Relaxed), c.clone()).await
                            && !c.is_cancelled()
                        {
                            tracing::warn!(%reason, "HLS segment budget stopped worker");
                            worker.failed("packaging_failed");
                            c.cancel();
                        }
                    }));
                }
                if let (Some(listener),Some(state))=(caption_listener,w.captions.clone()) {
                    let c=cancel.clone();let decoder_state=state.clone();let (sender,mut receiver)=tokio::sync::mpsc::channel::<Bytes>(32);
                    tasks.push(tokio::spawn(async move {
                        let mut transport=crate::caption_transport::Transport::default();
                        loop{let data=tokio::select!{biased;_=c.cancelled()=>break,data=receiver.recv()=>match data{Some(data)=>data,None=>break}};decoder_state.push_source(&data,&mut transport);if decoder_state.failed.load(Ordering::Relaxed){break}}
                    }));
                    // FFmpeg opens this output after input probing. Allow the same
                    // bounded startup interval as the AV listener below.
                    let c=cancel.clone();let drain_state=state.clone();tasks.push(tokio::spawn(async move {let Ok(Ok((mut socket,_)))=(tokio::select!{biased;_=c.cancelled()=>return,r=tokio::time::timeout(timeout+Duration::from_secs(1),listener.accept())=>r})else{drain_state.failed.store(true,Ordering::Relaxed);drain_state.decoder.lock().unwrap().error=Some("caption_input_closed");return};let mut buffer=[0;16384];loop{let read=tokio::select!{biased;_=c.cancelled()=>break,r=socket.read(&mut buffer)=>r};match read{Ok(n)if n>0=>{if enqueue_caption(&sender,Bytes::copy_from_slice(&buffer[..n]),&c).await.is_err(){drain_state.failed.store(true,Ordering::Relaxed);break;}},_=>{drain_state.failed.store(true,Ordering::Relaxed);drain_state.decoder.lock().unwrap().error=Some("caption_input_closed");break}}}}));
                    for _ in 0..2{tasks.push(tokio::spawn(state.clone().ocr(tesseract.clone(),cancel.clone())));}
                }
                if let Some(state) = w.captions.clone() {
                    tasks.push(tokio::spawn(state.watch(dir.clone(),cancel.clone())));
                }
                let Some(mut stdout) = child.stdout.take() else {
                    w.failed("packaging_failed");
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return;
                };
                if let Some(mut stderr) = child.stderr.take() {
                    let c = cancel.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut buffer = [0; 16384];
                        loop {
                            match tokio::select! { biased; _=c.cancelled()=>break, r=stderr.read(&mut buffer)=>r } {
                                Ok(n) if n > 0 => tracing::warn!("worker packager reported a diagnostic"),
                                _ => break,
                            }
                        }
                    }));
                }
                let separate_av = av_listener.is_some();
                let av_finished=CancellationToken::new();
                let mut clean_end=false;
                let mut av_done = None;
                if let Some(listener) = av_listener {
                    let w=w.clone();let c=cancel.clone();let end=av_finished.clone();let (sender,receiver)=oneshot::channel();av_done=Some(receiver);
                    tasks.push(tokio::spawn(async move {
                        let result = async {
                            let accepted=tokio::select! {biased;_=c.cancelled()=>return Ok(()), r=tokio::time::timeout(timeout+Duration::from_secs(1),listener.accept())=>r};
                            let (mut socket,_)=accepted.map_err(|_|"startup_timeout")?.map_err(|_|"wire_setup_failed")?;
                            let mut output=crate::worker_output::live::Forwarder::new(timeout);
                            let mut buffer=[0;188*64];
                            loop {
                                let read=tokio::select! {biased;_=c.cancelled()=>return Ok(()),r=tokio::time::timeout(timeout,socket.read(&mut buffer))=>r};
                                match read {
                                    Ok(Ok(0))=>{tokio::select!{biased;_=c.cancelled()=>return Ok(()),result=output.finish(&w.wire)=>result}?;return Ok(());},
                                    Ok(Ok(n))=>{tokio::select!{biased;_=c.cancelled()=>return Ok(()),result=output.push(&buffer[..n],&w.wire)=>result}?;},
                                    Ok(Err(_))=>return Err("wire_decode_failed"),
                                    Err(_)=>return Err("input_stalled"),
                                }
                            }
                        }.await;
                        let _=sender.send(result);
                        if !c.is_cancelled() {
                            if let Err(reason)=result {w.failed(reason);c.cancel();}
                            else {end.cancel();}
                        }
                    }));
                }
                let mut native_output=if !original_wire && !separate_av {Some(crate::worker_output::live::Forwarder::new(timeout))}else{None};
                let mut output_clock=crate::caption_transport::Transport::default();
                let mut output_decoder=crate::captions::Decoder::new(vec![]);
                let mut buffer = vec![0u8; 188 * 64];
                loop {
                    let read = tokio::select! { biased;
                        _ = cancel.cancelled() => break,
                        _ = av_finished.cancelled() => {clean_end=true;w.failed("input_closed");break;},
                        result = tokio::time::timeout(timeout, stdout.read(&mut buffer)) => result,
                    };
                    match read {
                        Ok(Ok(0)) => {
                            // Child exit can precede the next GOP, leaving the
                            // stdin copy parked on a read with no write error.
                            if shared_drain.as_ref().is_some_and(|d| !d.is_complete())
                                && let Some(commit) = &shared_local_failure
                            {
                                commit();
                            }
                            tracing::debug!(target: "flussonix::worker_lifecycle",
                                bytes_in = w.bytes.load(Ordering::Relaxed),
                                "packager stdout closed before worker cancellation");
                            let result=if let Some(output)=&mut native_output {tokio::select!{biased;_=cancel.cancelled()=>break,result=output.finish(&w.wire)=>result}}
                            else if let Some(receiver)=av_done.take() {
                                tokio::select! {biased;_=cancel.cancelled()=>break,r=tokio::time::timeout(timeout,receiver)=>match r {Ok(Ok(result))=>result,_=>Err("wire_decode_failed")}}
                            } else {Ok(())};
                            clean_end=result.is_ok();
                            w.failed(result.err().unwrap_or("input_closed"));
                            break;
                        }
                        Ok(Err(_)) => {
                            if shared_drain.as_ref().is_some_and(|d| !d.is_complete())
                                && let Some(commit) = &shared_local_failure
                            {
                                commit();
                            }
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
                            if let Some(output)=&mut native_output {
                                let result=tokio::select!{biased;_=cancel.cancelled()=>break,result=output.push(&buffer[..n],&w.wire)=>result};
                                if let Err(reason)=result {w.failed(reason);break;}
                            }
                            if let Some(state)=&w.captions{state.observe_ts(&buffer[..n],&mut output_clock,&mut output_decoder);}
                            w.recovery.lock().unwrap().progress();
                            w.bytes.fetch_add(n as u64, Ordering::Relaxed);
                            w.output_rate.lock().unwrap().record(Instant::now(), n as u64);
                            let _ = w.tx.send(Bytes::copy_from_slice(&buffer[..n]));
                        }
                    }
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
                if clean_end && !cancel.is_cancelled() {
                    w.wire.finish();
                    // Give authorized public consumers a bounded clean-EOF
                    // drain. Abort/revocation/config replacement cancels this
                    // immediately and preserves the existing reader fences.
                    if w.viewers.load(Ordering::Relaxed)>0 {
                        tokio::select! {biased;_=cancel.cancelled()=>{},_=tokio::time::sleep(Duration::from_secs(2))=>{}}
                    }
                }
            }.await;
            cancel.cancel();
            for task in direct_tasks {
                let _ = task.await;
            }
            for task in push_tasks {
                let _ = task.await;
            }
            for task in tasks {
                task.abort();
                let _ = task.await;
            }
            drop(peer_hls);
            if let Some(bridge) = tls_input {
                bridge.close().await;
            }
            // The process has been reaped; retained workers must not advertise its old PID.
            w.pid.store(0, Ordering::Relaxed);
            w.alive.store(false, Ordering::Relaxed);
            let _ = done_tx.send(());
        });
        Ok(worker)
    }
    pub async fn read(&self, name: &str, file: &str) -> Result<Bytes, String> {
        self.read_mode(name, file, true).await
    }
    pub(crate) async fn is_current_worker(&self, name: &str, worker: &Arc<Worker>) -> bool {
        self.workers
            .lock()
            .await
            .get(name)
            .is_some_and(|current| Arc::ptr_eq(current, worker))
    }
    /// A generated HLS segment may be replaced while its file is being read.
    /// Selected output is tied to the exact worker that admitted this viewer.
    pub(crate) async fn read_current_worker(
        &self,
        name: &str,
        file: &str,
        worker: &Arc<Worker>,
    ) -> Result<Bytes, String> {
        if !self.is_current_worker(name, worker).await || worker.is_closed() {
            return Err("HLS worker changed".into());
        }
        let bytes = self.read_mode(name, file, true).await?;
        if !self.is_current_worker(name, worker).await || worker.is_closed() {
            return Err("HLS worker changed".into());
        }
        Ok(bytes)
    }
    /// Selected TS media keeps its miss permit inside file I/O and caption
    /// filtering, then returns it for the selection/filter/cache stage.
    pub(crate) async fn read_current_worker_selected_ts(
        &self,
        name: &str,
        file: &str,
        worker: &Arc<Worker>,
        permit: OwnedSemaphorePermit,
    ) -> Result<(OwnedSemaphorePermit, Bytes), String> {
        if !self.is_current_worker(name, worker).await || worker.is_closed() {
            return Err("HLS worker changed".into());
        }
        let result = self.read_selected_ts_mode(name, file, true, permit).await?;
        if !self.is_current_worker(name, worker).await || worker.is_closed() {
            return Err("HLS worker changed".into());
        }
        Ok(result)
    }
    async fn read_selected_ts_mode(
        &self,
        name: &str,
        file: &str,
        touch_demand: bool,
        permit: OwnedSemaphorePermit,
    ) -> Result<(OwnedSemaphorePermit, Bytes), String> {
        if !file.ends_with(".ts") || !valid_file(file) {
            return Err("invalid media path".into());
        }
        let worker = self
            .workers
            .lock()
            .await
            .get(name)
            .cloned()
            .ok_or("media worker unavailable")?;
        if !worker.alive.load(Ordering::Relaxed) || worker.is_closed() {
            return Err("media worker unavailable".into());
        }
        if touch_demand {
            worker.touch();
        }
        let (permit, data) = file_reader::read_ts_with_permit(
            &self.root,
            name,
            file,
            32 * 1024 * 1024,
            worker.hls_subtitles == "drop",
            permit,
        )
        .await?;
        Ok((permit, Bytes::from(data)))
    }
    async fn read_mode(&self, name: &str, file: &str, touch_demand: bool) -> Result<Bytes, String> {
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
            if logical == "index.m3u8"
                || logical == "av.m3u8"
                || logical.starts_with("cc")
                || (logical.starts_with("nt")
                    && logical.as_bytes().get(2).is_some_and(u8::is_ascii_digit))
                || (logical.starts_with('s')
                    && logical.as_bytes().get(1).is_some_and(u8::is_ascii_digit))
                || (logical.starts_with("dvb")
                    && logical.as_bytes().get(3).is_some_and(u8::is_ascii_digit))
                || (logical.starts_with("ttx")
                    && logical.as_bytes().get(3).is_some_and(u8::is_ascii_digit))
            {
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
            if touch_demand {
                w.touch();
            }
        } else {
            return Err("media worker unavailable".into());
        }
        let drop_captions = self
            .workers
            .lock()
            .await
            .get(name)
            .is_some_and(|w| w.hls_subtitles == "drop");
        if drop_captions && (file.ends_with(".ts") || file.ends_with(".m4s")) {
            let permit = tokio::time::timeout(
                Duration::from_secs(8),
                self.caption_filter_slots.clone().acquire_owned(),
            )
            .await
            .map_err(|_| "caption filter admission unavailable")?
            .map_err(|_| "caption filter admission unavailable")?;
            let (permit, data) = if file.ends_with(".ts") {
                file_reader::read_ts_with_permit(
                    &self.root,
                    name,
                    file,
                    32 * 1024 * 1024,
                    true,
                    permit,
                )
                .await?
            } else {
                file_reader::read_mp4_with_permit(&self.root, name, file, permit).await?
            };
            drop(permit);
            return Ok(Bytes::from(data));
        }
        let data = file_reader::read(&self.root, name, file, 32 * 1024 * 1024).await?;
        Ok(Bytes::from(data))
    }
    async fn join_and_commit(
        epoch: crate::hls_generation::Epoch,
        name: String,
        directory: PathBuf,
        w: Arc<Worker>,
    ) -> Result<(), String> {
        w.cancel.cancel();
        if let Some(done) = w.done.lock().await.take() {
            let _ = done.await;
        }
        tokio::task::spawn_blocking(move || epoch.commit_observed(&name, &directory))
            .await
            .map_err(|_| "HLS sequence commit task failed".to_owned())?
    }
    async fn remove_media(directory: &Path) -> Result<(), String> {
        match tokio::fs::remove_dir_all(directory).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err("cannot remove retired HLS media".into()),
        }
    }
    async fn retire_owned(
        &self,
        name: &str,
        worker: Arc<Worker>,
        guard: OwnedMutexGuard<()>,
    ) -> Result<OwnedMutexGuard<()>, String> {
        self.selected_hls_cache
            .lock()
            .unwrap()
            .remove_generation(&worker.selected_hls_token);
        let name = name.to_owned();
        let directory = self.directory(&name);
        let epoch = self.hls_epoch.clone();
        #[cfg(test)]
        let before_remove = self.before_remove_test_hook.clone();
        tokio::spawn(async move {
            let guard = guard;
            let result: Result<(), String> = async {
                Self::join_and_commit(epoch, name.clone(), directory.clone(), worker).await?;
                #[cfg(test)]
                if let Some(hook) = before_remove {
                    hook(&name).await;
                }
                Self::remove_media(&directory).await
            }
            .await;
            if let Err(error) = &result {
                tracing::warn!(stream = name, error = %error, "detached media retirement failed");
            }
            result?;
            Ok(guard)
        })
        .await
        .map_err(|_| "HLS retirement task failed".to_owned())?
    }
    async fn prepare_generation(
        &self,
        name: &str,
        retired: Option<Arc<Worker>>,
        guard: OwnedMutexGuard<()>,
    ) -> Result<(OwnedMutexGuard<()>, u64), String> {
        if let Some(worker) = retired.as_ref() {
            self.selected_hls_cache
                .lock()
                .unwrap()
                .remove_generation(&worker.selected_hls_token);
        }
        let name = name.to_owned();
        let directory = self.directory(&name);
        let epoch = self.hls_epoch.clone();
        #[cfg(test)]
        let before_remove = self.before_remove_test_hook.clone();
        tokio::spawn(async move {
            let guard = guard;
            let result: Result<u64, String> = async {
                if let Some(worker) = retired {
                    Self::join_and_commit(epoch.clone(), name.clone(), directory.clone(), worker)
                        .await?;
                }
                let sequence = tokio::task::spawn_blocking({
                    let directory = directory.clone();
                    let name = name.clone();
                    move || epoch.next_for(&name, &directory)
                })
                .await
                .map_err(|_| "HLS sequence allocation task failed".to_owned())??;
                #[cfg(test)]
                if let Some(hook) = before_remove {
                    hook(&name).await;
                }
                Self::remove_media(&directory).await?;
                tokio::fs::create_dir_all(directory.join("fmp4"))
                    .await
                    .map_err(|_| "cannot create HLS media directory".to_owned())?;
                Ok(sequence)
            }
            .await;
            if let Err(error) = &result {
                tracing::warn!(stream = name, error = %error, "detached HLS generation failed");
            }
            let sequence = result?;
            Ok((guard, sequence))
        })
        .await
        .map_err(|_| "HLS generation task failed".to_owned())?
    }
    pub async fn stop(&self, name: &str) {
        let startup = self.startup_gate(name).lock_owned().await;
        let retired = self.workers.lock().await.remove(name);
        if let Some(w) = retired {
            if let Err(error) = self.retire_owned(name, w, startup).await {
                tracing::warn!(
                    stream = name,
                    error = error,
                    "failed to retire media worker"
                );
            }
        }
    }
    pub async fn stop_if_current(&self, name: &str, expected: &Arc<Worker>) {
        let startup = self.startup_gate(name).lock_owned().await;
        let retired = {
            let mut workers = self.workers.lock().await;
            workers
                .get(name)
                .is_some_and(|worker| Arc::ptr_eq(worker, expected))
                .then(|| workers.remove(name).unwrap())
        };
        if let Some(worker) = retired {
            if let Err(error) = self.retire_owned(name, worker, startup).await {
                tracing::warn!(
                    stream = name,
                    error = error,
                    "failed to retire media worker"
                );
            }
        }
    }
    pub async fn stop_if_idle_current(&self, name: &str, expected: &Arc<Worker>) {
        let startup = self.startup_gate(name).lock_owned().await;
        let retired = {
            let mut workers = self.workers.lock().await;
            workers
                .get(name)
                .is_some_and(|worker| Arc::ptr_eq(worker, expected) && Self::idle_worker(worker))
                .then(|| workers.remove(name).unwrap())
        };
        if let Some(worker) = retired
            && let Err(error) = self.retire_owned(name, worker, startup).await
        {
            tracing::warn!(
                stream = name,
                error = error,
                "failed to retire idle media worker"
            );
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
        self.fxlink_pool.close().await;
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
    /// Bounded native telemetry; observing a worker never starts or keeps it alive.
    pub(crate) async fn output_bitrates(&self) -> Value {
        let workers = self.workers.lock().await;
        let rates = workers
            .iter()
            .filter_map(|(name, worker)| {
                let (mbps, age_ms) = worker.output_bitrate()?;
                Some((name.clone(), json!({"mbps":mbps,"age_ms":age_ms})))
            })
            .take(256)
            .collect::<serde_json::Map<String, Value>>();
        Value::Object(rates)
    }
    pub async fn rtp_sdp(&self, name: &str, index: usize, cfg: &Value) -> Result<String, String> {
        let definitions = crate::direct_rtp::config::outputs(cfg)?;
        let workers = self.workers.lock().await;
        let worker = workers
            .get(name)
            .filter(|w| !w.is_closed())
            .ok_or("Stream is not running")?;
        let state = worker
            .direct_outputs
            .get(index)
            .ok_or("RTP destination not found")?;
        if definitions.get(index) != Some(&state.definition) {
            return Err("RTP destination changed; waiting for current worker".into());
        }
        let generation = worker
            .wire
            .rtp
            .description()
            .and_then(Result::ok)
            .ok_or("Elementary RTP SDP is not ready")?
            .generation;
        state
            .sdp_for_generation(generation)
            .ok_or("Elementary RTP SDP is not ready".into())
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
    pub(crate) async fn rtsp_ready_names(&self) -> Vec<String> {
        self.workers
            .lock()
            .await
            .iter()
            .filter(|(_, w)| {
                w.alive.load(Ordering::Relaxed)
                    && !w.is_closed()
                    && w.recovery
                        .lock()
                        .unwrap()
                        .media_age_ms()
                        .is_some_and(|age| age < w.input_timeout.as_millis())
                    && w.wire.rtp.description().is_some_and(|d| d.is_ok())
            })
            .map(|(name, _)| name.clone())
            .collect()
    }
    pub async fn workers(&self) -> Vec<(String, String)> {
        self.workers
            .lock()
            .await
            .iter()
            .map(|(n, w)| (n.clone(), w.signature.clone()))
            .collect()
    }

    fn idle_worker(worker: &Worker) -> bool {
        !worker.publication
            && !worker.active_pushes
            && worker.idle_seconds() >= 60
            && worker.viewers.load(Ordering::Relaxed) == 0
            && !worker
                .shared_static_demand
                .as_ref()
                .is_some_and(|demand| demand())
    }
    pub async fn idle_workers(&self) -> Vec<(String, Arc<Worker>)> {
        self.workers
            .lock()
            .await
            .iter()
            .filter(|(_, worker)| Self::idle_worker(worker))
            .map(|(name, worker)| (name.clone(), worker.clone()))
            .collect()
    }
    pub async fn idle(&self) -> Vec<String> {
        self.idle_workers()
            .await
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }
}
// Only deferred MPEG-TS publication commands built above use this rebuild:
// their IO is piped and the factory restores the Linux process ownership guard.
fn audio_publication_command(cmd: Command, analysis_us: u128) -> Command {
    let original = cmd.as_std();
    let mut audio = crate::child_process::command(original.get_program());
    let duration = analysis_us.to_string();
    let mut input_options = true;
    let mut replace_duration = false;
    for argument in original.get_args() {
        if replace_duration {
            audio.arg(&duration);
        } else {
            audio.arg(argument);
        }
        replace_duration = input_options && argument == "-analyzeduration";
        if argument == "-i" {
            input_options = false;
        }
    }
    for (key, value) in original.get_envs() {
        if let Some(value) = value {
            audio.env(key, value);
        } else {
            audio.env_remove(key);
        }
    }
    if let Some(directory) = original.get_current_dir() {
        audio.current_dir(directory);
    }
    audio
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    audio
}

async fn enqueue_caption(
    sender: &tokio::sync::mpsc::Sender<Bytes>,
    data: Bytes,
    cancel: &CancellationToken,
) -> Result<(), ()> {
    // A full queue can be a short TCP/probe burst, not a stalled decoder.
    // Retain the 32 x 16 KiB bound and only tolerate a bounded admission delay.
    tokio::select! { biased;
        _ = cancel.cancelled() => Ok(()),
        result = tokio::time::timeout(Duration::from_millis(250), sender.send(data)) =>
            result.map_err(|_| ())?.map_err(|_| ()),
    }
}

fn caption_output(cmd: &mut Command, target: &str, teletext: bool) {
    // Keep fractional source PTS when copying into tee. FFmpeg 6 auto mode
    // can choose the video frame-rate timebase and shift caption clock origin.
    cmd.args(["-copytb", "1"]);
    // Include one optional audio stream for the null fallback: an audio-only
    // source must not lose AV because an optional caption output has no video.
    cmd.args(["-map", "0:v:0?", "-map", "0:a:0?", "-c", "copy"]);
    if teletext {
        cmd.args(["-map", "0:s?"]);
    } else {
        cmd.arg("-sn");
    }
    let selected = if teletext { "v,s" } else { "v" };
    cmd.args(["-max_interleave_delta","100000","-flush_packets","1","-f","tee",
        &format!("[select='{selected}':onfail=ignore:f=mpegts:max_interleave_delta=100000:flush_packets=1]{target}|[select='v,a':f=null]pipe:2")]);
}
/// FFmpeg maps the optional video first, then all audio in PMT order.
/// Numeric stream specifiers avoid escaping colons inside tee option keys.
fn native_fmp4_filters(tracks: &[crate::m4s::Track]) -> String {
    let video = |t: &&crate::m4s::Track| matches!(t.codec.as_str(), "h264" | "hevc");
    let offset = tracks.iter().filter(video).count();
    tracks
        .iter()
        .filter(|t| matches!(t.codec.as_str(), "aac" | "m2a" | "mp3"))
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
        "rtsp-udp" | "rtsp2" => "rtsp",
        "http" | "https" | "rtsp" | "srt" => scheme,
        _ => return Err(format!("unsupported input protocol: {scheme}")),
    };
    Ok(format!("{scheme}://{rest}"))
}

pub(crate) fn validate_rtsp_input_url(input: &str) -> Result<(), String> {
    if input.len() > 4096 || input.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err("invalid RTSP input URL".into());
    }
    for (i, byte) in input.as_bytes().iter().enumerate() {
        if *byte == b'%'
            && (input
                .as_bytes()
                .get(i + 1)
                .is_none_or(|b| !b.is_ascii_hexdigit())
                || input
                    .as_bytes()
                    .get(i + 2)
                    .is_none_or(|b| !b.is_ascii_hexdigit()))
        {
            return Err("invalid RTSP input URL".into());
        }
    }
    let url = url::Url::parse(input).map_err(|_| "invalid RTSP input URL")?;
    if !matches!(url.scheme(), "rtsp" | "rtsps")
        || url.host_str().is_none()
        || url.port() == Some(0)
        || url.fragment().is_some()
    {
        return Err("invalid RTSP input URL".into());
    }
    Ok(())
}

fn rtsp_input_script(input: &str, transport: &str) -> Result<String, String> {
    validate_rtsp_input_url(input)?;
    let url = url::Url::parse(input).map_err(|_| "invalid RTSP input URL")?;
    let escaped = url.as_str().replace('\u{27}', "'\\''");
    Ok(format!(
        "ffconcat version 1.0\nfile '{escaped}'\noption rtsp_transport {transport}\n"
    ))
}

fn validate_local_media_proxy_url(input: &str) -> Result<(), String> {
    let url = url::Url::parse(input).map_err(|_| "invalid local media proxy URL")?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b":/._-".contains(&byte))
    {
        return Err("invalid local media proxy URL".into());
    }
    Ok(())
}

fn proxy_input_script(input: &str) -> Result<String, String> {
    validate_local_media_proxy_url(input)?;
    Ok(format!(
        "ffconcat version 1.0\nfile '{input}'\noption rw_timeout 10000000\noption allowed_extensions ALL\n"
    ))
}

fn spawn_owned_input(
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    writer: tokio::process::ChildStdin,
    input: OwnedInput,
    cancel: CancellationToken,
    worker: Arc<Worker>,
) {
    tasks.push(tokio::spawn(async move {
        let result = tokio::select! { biased;
            _ = cancel.cancelled() => return,
            result = feed_owned_input(writer, input) => result,
        };
        if result.is_err() && !cancel.is_cancelled() {
            worker.failed("input_closed");
            cancel.cancel();
        }
    }));
}

async fn feed_owned_input(
    mut writer: tokio::process::ChildStdin,
    input: OwnedInput,
) -> Result<(), ()> {
    use tokio::io::AsyncWriteExt;
    match input {
        OwnedInput::Script(script) => writer.write_all(script.as_bytes()).await.map_err(|_| ())?,
        OwnedInput::LiveTs(url) => {
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| ())?;
            let response = client.get(url).send().await.map_err(|_| ())?;
            if response.status() != reqwest::StatusCode::OK {
                return Err(());
            }
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                writer
                    .write_all(&chunk.map_err(|_| ())?)
                    .await
                    .map_err(|_| ())?;
            }
        }
    }
    writer.shutdown().await.map_err(|_| ())
}

pub fn media_signature(cfg: &Value) -> String {
    format!("{:x}",Sha256::digest(serde_json::to_vec(&json!({"inputs":cfg["inputs"],"transcoder":cfg["transcoder"],"pushes":cfg["pushes"],"direct_outputs":cfg["flussonix_rtp_outputs"],"peer":cfg["flussonix_peer_key"],"allow_insecure_peer_http":cfg["flussonix_allow_insecure_peer_http"],"timeout":cfg["flussonix_input_timeout"],"subtitle_tracks":cfg["flussonix_subtitle_tracks"],"hls_captions":cfg["flussonix_hls_captions"],"hls_subtitles":cfg["flussonix_hls_subtitles"]})).unwrap()))
}

fn input_fingerprint(cfg: &Value) -> Option<[u8; 32]> {
    cfg["inputs"].as_array()?;
    Some(Sha256::digest(serde_json::to_vec(&cfg["inputs"]).ok()?).into())
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::server::{App, Options, router};
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    #[tokio::test]
    async fn unclosed_mux_byte_budget_stops_and_reaps_worker_without_a_playlist() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let idle = d.path().join("idle-packager");
        std::fs::write(&idle, "#!/bin/sh\nexec sleep 60\n").unwrap();
        std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                ffmpeg: idle.to_string_lossy().into_owned(),
                admin_password: "owned-budget-admin".into(),
                peer_key: "owned-budget-peer".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let worker = app
            .media
            .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
            .await
            .unwrap();
        worker.bytes.store(64 * 1024 * 1024 + 1, Ordering::Relaxed);
        let stopped = tokio::time::timeout(Duration::from_secs(3), worker.closed())
            .await
            .is_ok();
        let reaped = tokio::time::timeout(Duration::from_secs(3), async {
            while worker.pid() != 0 || worker.alive.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let reason = worker.stats()["last_error"].as_str().map(str::to_owned);
        app.media.stop_all().await;
        assert!(stopped, "unclosed mux bytes did not stop the worker");
        assert!(reaped, "stopped worker retained its packager PID");
        assert_eq!(reason.as_deref(), Some("packaging_failed"));
    }
    #[tokio::test]
    async fn oversized_current_generation_hls_temp_segment_stops_and_reaps_worker() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        let worker = app
            .media
            .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
            .await
            .unwrap();
        let dir = app.media.directory("owned");
        let generation = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        if let Some((generation, _)) =
                            name.strip_prefix('g').and_then(|s| s.split_once('_'))
                            && generation.len() == 32
                            && generation.bytes().all(|b| b.is_ascii_hexdigit())
                        {
                            return generation.to_owned();
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let generation = generation.expect("test source did not produce an HLS segment");
        let temp = dir.join(format!("g{generation}_999999.ts.tmp"));
        std::fs::File::create(&temp)
            .unwrap()
            .set_len(64 * 1024 * 1024 + 1)
            .unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(4), worker.closed())
            .await
            .is_ok();
        let reaped = tokio::time::timeout(Duration::from_secs(4), async {
            while worker.pid() != 0 || worker.alive.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let reason = worker.stats()["last_error"].as_str().map(str::to_owned);
        app.media.stop_all().await;
        assert!(stopped, "oversized temp segment did not stop its worker");
        assert!(reaped, "stopped worker retained its packager PID");
        assert_eq!(reason.as_deref(), Some("packaging_failed"));
    }
    #[tokio::test]
    async fn rtsp_push_releases_ts_probe_before_waiting_for_receiver_setup() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ready, setup) = tokio::sync::oneshot::channel();
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            ready.send(()).unwrap();
            server_stop.cancelled().await;
        });
        let cfg = json!({"inputs":[{"url":"testsrc://"}],
            "pushes":[{"url":format!("rtsp://{address}/owned"),"connect_timeout":20}]});
        let worker = app.media.ensure("owned", &cfg).await.unwrap();
        let setup_ok = matches!(
            tokio::time::timeout(Duration::from_secs(8), setup).await,
            Ok(Ok(()))
        );
        let before = worker.bytes.load(Ordering::Relaxed);
        let flowing = tokio::time::timeout(Duration::from_secs(3), async {
            while worker.bytes.load(Ordering::Relaxed) < before + 64000 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let retained = worker.tx.retained();
        app.media.stop_all().await;
        stop.cancel();
        server.abort();
        let _ = server.await;
        assert!(
            setup_ok && flowing,
            "real RTSP setup must be stalled after TS probing while worker flows"
        );
        assert_eq!(
            retained,
            (0, 0),
            "RTSP probe must not pin TS after metadata is known"
        );
    }

    #[tokio::test]
    async fn caption_queue_retains_a_transient_burst_in_order() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender.send(Bytes::from_static(b"first")).await.unwrap();
        let cancel = CancellationToken::new();
        let next = tokio::spawn(async move {
            enqueue_caption(&sender, Bytes::from_static(b"second"), &cancel).await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(receiver.recv().await.unwrap(), "first");
        assert_eq!(next.await.unwrap(), Ok(()));
        assert_eq!(receiver.recv().await.unwrap(), "second");
    }
    #[tokio::test]
    async fn caption_queue_bounds_a_stall_and_cancels_promptly() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        sender.send(Bytes::from_static(b"full")).await.unwrap();
        let cancel = CancellationToken::new();
        let started = std::time::Instant::now();
        assert_eq!(
            enqueue_caption(&sender, Bytes::from_static(b"stalled"), &cancel).await,
            Err(())
        );
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert!(started.elapsed() < Duration::from_secs(1));
        let c = cancel.clone();
        let task = tokio::spawn(async move {
            enqueue_caption(&sender, Bytes::from_static(b"cancelled"), &c).await
        });
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), task)
                .await
                .is_ok()
        );
    }

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
    async fn blackout_recovery_seeds_actual_playback_clock_and_fences_queued_demand() {
        let d = tempfile::tempdir().unwrap();
        let app = app(d.path());
        let cfg = json!({"inputs":[{"url":"testsrc://"}]});
        let seen = Instant::now() - Duration::from_secs(20);
        let worker = app
            .media
            .recover_demand_guarded("owned", &cfg, seen, std::future::ready(true))
            .await
            .unwrap();
        assert!(
            worker.idle_seconds() >= 20,
            "background startup renewed playback demand"
        );
        app.media.stop_all().await;
        let blocked = app.media.workers.lock().await;
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = allowed.clone();
        let a = app.clone();
        let task =
            tokio::spawn(async move {
                a.media
                    .recover_demand_guarded("owned", &cfg, seen, async move {
                        flag.load(Ordering::SeqCst)
                    })
                    .await
            });
        allowed.store(false, Ordering::SeqCst);
        drop(blocked);
        assert!(task.await.unwrap().is_err());
        assert!(app.media.workers().await.is_empty());
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
        let current_pid = current.pid();
        app.media.stop_all().await;
        assert!(
            result.is_err(),
            "stale route passed the engine startup fence"
        );
        assert_eq!(
            pid, current_pid,
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
    // Break caught: background only activation renews viewer demand or an idle
    // static relay is retired before its source demand is removed.
    #[tokio::test]
    async fn source_static_demand_preserves_idle_worker_without_touching_playback_clock() {
        let d = tempfile::tempdir().unwrap();
        let source_dir = d.path().join("source");
        let edge_dir = d.path().join("edge");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&edge_dir).unwrap();
        let source = app(&source_dir);
        let edge = app(&edge_dir);
        source
            .config
            .put("streams", "news", json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let a = source.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::server::router(a))
                .await
                .unwrap()
        });
        edge.config
            .put(
                "sources",
                "owned",
                json!({"api_url":url,"flussonix_transport":"m4s","only":["news"]}),
            )
            .unwrap();
        source
            .media
            .ensure("news", &source.config.effective("news").unwrap())
            .await
            .unwrap();
        edge.reconcile().await;
        let worker = edge.media.workers.lock().await.get("news").cloned();
        let observations = if let Some(worker) = worker {
            *worker.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
            edge.reconcile().await;
            let retained = edge
                .media
                .workers
                .lock()
                .await
                .get("news")
                .is_some_and(|w| Arc::ptr_eq(w, &worker));
            let idle = worker.idle_seconds();
            edge.config
                .put("sources", "owned", json!({"only":[]}))
                .unwrap();
            edge.reconcile().await;
            (
                retained,
                idle,
                edge.media.workers.lock().await.is_empty(),
                worker.is_closed(),
            )
        } else {
            (false, 0, false, false)
        };
        edge.media.stop_all().await;
        source.media.stop_all().await;
        server.abort();
        let _ = server.await;
        assert!(observations.0, "warm relay was retired or replaced as idle");
        assert!(
            observations.1 >= 61,
            "background activation renewed playback demand"
        );
        assert!(
            observations.2 && observations.3,
            "removing static demand left an idle worker running"
        );
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
        caption_output(&mut cmd, &target, false);
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

// One shared backlog per worker, never one copy per viewer. This byte budget
// tolerates GOP bursts across arbitrary stdout read boundaries. The record
// ceiling also bounds metadata for tiny reads. Fully consumed data is released.
fn ts_channel() -> crate::media_queue::Channel {
    crate::media_queue::Channel::live(32 * 1024, 4 * 1024 * 1024)
}

#[cfg(test)]
mod ts_backlog_tests {
    use super::*;

    // A GOP arrives in many small reads; chunk count must not truncate a
    // healthy viewer while its byte backlog is still small.
    #[tokio::test]
    async fn ts_gop_burst_preserves_every_byte_for_all_viewers() {
        let queue = ts_channel();
        let mut first = queue.subscribe();
        let mut second = queue.subscribe();
        for sequence in 0..256u32 {
            let mut packet = vec![0x47; 1316];
            packet[4..8].copy_from_slice(&sequence.to_be_bytes());
            queue.send(Bytes::from(packet)).unwrap();
        }
        for sequence in 0..256u32 {
            let a = first.recv().await.expect("ordinary GOP must not overflow");
            let b = second
                .recv()
                .await
                .expect("second viewer must receive the same GOP");
            assert_eq!(a.len(), 1316);
            assert_eq!(&a[4..8], &sequence.to_be_bytes());
            assert!(a[..4].iter().chain(a[8..].iter()).all(|b| *b == 0x47));
            assert_eq!(a, b);
        }
    }
}

#[cfg(test)]
mod push_startup_tests {
    use super::*;

    #[tokio::test]
    async fn configured_push_registers_for_ts_before_its_task_is_polled() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let cfg = json!({"inputs":[{"url":"testsrc://"}]});
        let worker = engine.ensure("push-startup", &cfg).await.unwrap();
        assert!(!worker.tx.has_receivers());
        let http = crate::push::Destination::Http(
            crate::http_push::Destination::parse(&json!({"url":"http://127.0.0.1:18299/owned"}))
                .unwrap(),
        );
        let srt = crate::push::Destination::Srt(
            crate::srt_push::configuration(&json!({"pushes":[{"url":"srt://127.0.0.1:18299"}]}))
                .unwrap()
                .remove(0),
        );
        let mut observed = Vec::new();
        for (index, destination) in [http, srt].into_iter().enumerate() {
            let push = crate::push::State::new(
                destination,
                index,
                engine.rtsp_push_egress.clone(),
                engine.http_push_egress.clone(),
            );
            let future = push.run("ffmpeg".into(), worker.clone(), CancellationToken::new());
            observed.push(worker.tx.has_receivers());
            drop(future);
            assert!(!worker.tx.has_receivers());
        }
        engine.stop_all().await;
        assert_eq!(
            observed,
            [true, true],
            "configured HTTP and SRT pushes must hold TS cursors before task scheduling"
        );
    }
}

#[cfg(test)]
mod input_selection_lock_tests {
    use super::*;

    #[tokio::test]
    async fn explicit_stop_waiting_for_worker_does_not_hold_the_global_map() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let victim = engine.ensure("victim", &config).await.unwrap();
        engine.ensure("unrelated", &config).await.unwrap();
        let hold_completion = victim.done.lock().await;
        let stopping = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.stop("victim").await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !victim.cancel.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let unrelated = tokio::time::timeout(Duration::from_secs(1), engine.count()).await;
        drop(hold_completion);
        stopping.await.unwrap();
        engine.stop_all().await;
        assert_eq!(
            unrelated.expect("stop held the worker map across a join"),
            1
        );
    }

    #[tokio::test]
    async fn conditional_stop_waiting_for_worker_does_not_hold_the_global_map() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let victim = engine.ensure("victim", &config).await.unwrap();
        engine.ensure("unrelated", &config).await.unwrap();
        let hold_completion = victim.done.lock().await;
        let stopping = {
            let engine = engine.clone();
            let victim = victim.clone();
            tokio::spawn(async move { engine.stop_if_current("victim", &victim).await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !victim.cancel.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let unrelated = tokio::time::timeout(Duration::from_secs(1), engine.count()).await;
        drop(hold_completion);
        stopping.await.unwrap();
        engine.stop_all().await;
        assert_eq!(unrelated.expect("conditional stop held the worker map"), 1);
    }

    #[tokio::test]
    async fn selection_teardown_does_not_block_unrelated_worker_queries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
        let config = json!({"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]});
        let worker = engine.ensure("switching", &config).await.unwrap();
        engine.ensure("unrelated", &config).await.unwrap();
        let hold_completion = worker.done.lock().await;
        let selecting = {
            let engine = engine.clone();
            let config = config.clone();
            tokio::spawn(async move {
                engine
                    .select_input_guarded("switching", &config, 1, std::future::ready(true))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !worker.cancel.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let unrelated = tokio::time::timeout(Duration::from_millis(250), engine.count()).await;
        drop(hold_completion);
        selecting.await.unwrap().unwrap();
        assert_eq!(unrelated.unwrap(), 1);
        engine.stop_all().await;
    }
}

#[cfg(test)]
mod hls_retirement_tests {
    use super::*;
    use crate::server::{App, Options};
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn replacement_join_does_not_block_unrelated_worker_queries() {
        let root = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(root.path(), "ffmpeg"));
        let original = json!({"inputs":[{"url":"testsrc://"}]});
        let changed = json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"});
        let old = engine.ensure("owned", &original).await.unwrap();
        engine.ensure("unrelated", &original).await.unwrap();
        let hold_completion = old.done.lock().await;
        let replacing = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.ensure("owned", &changed).await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !old.cancel.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let unrelated = tokio::time::timeout(Duration::from_secs(1), engine.count()).await;
        drop(hold_completion);
        let replacement = replacing.await.unwrap().unwrap();
        assert!(!Arc::ptr_eq(&old, &replacement));
        engine.stop_all().await;
        assert!(
            unrelated.is_ok(),
            "replacement held the map while joining the old worker"
        );
    }

    #[tokio::test]
    async fn failed_replacement_commit_preserves_old_media_and_publishes_no_worker() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::new(root.path(), "ffmpeg");
        let original = json!({"inputs":[{"url":"testsrc://"}]});
        let changed = json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"});
        engine.ensure("owned", &original).await.unwrap();
        let media = engine.directory("owned");
        std::fs::write(media.join("recovery-marker"), b"old generation").unwrap();
        let record = root.path().join(".hls-sequences").join(format!(
            "{}.json",
            media.file_name().unwrap().to_string_lossy()
        ));
        std::fs::write(record, b"{").unwrap();

        let result = engine.ensure("owned", &changed).await;

        assert!(result.is_err(), "replacement concealed a corrupt floor");
        assert!(engine.workers.lock().await.get("owned").is_none());
        assert!(
            media.join("recovery-marker").exists(),
            "replacement lost recovery media"
        );
    }

    #[tokio::test]
    async fn obsolete_conditional_stop_does_not_delete_replacement_media() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::new(root.path(), "ffmpeg");
        let original = json!({"inputs":[{"url":"testsrc://"}]});
        let changed = json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"});
        let old = engine.ensure("owned", &original).await.unwrap();
        let current = engine.ensure("owned", &changed).await.unwrap();
        let marker = engine.directory("owned").join("new-generation");
        std::fs::write(&marker, b"current generation").unwrap();

        engine.stop_if_current("owned", &old).await;

        assert!(Arc::ptr_eq(
            engine.workers.lock().await.get("owned").unwrap(),
            &current
        ));
        assert!(marker.exists(), "obsolete stop deleted replacement media");
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn cancelled_stop_keeps_stream_fenced_until_old_worker_finishes() {
        let root = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(root.path(), "ffmpeg"));
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let old = engine.ensure("owned", &config).await.unwrap();
        let hold_completion = old.done.lock().await;
        let stopping = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.stop("owned").await })
        };
        tokio::time::timeout(Duration::from_secs(2), old.cancel.cancelled())
            .await
            .unwrap();
        stopping.abort();
        let _ = stopping.await;
        let starting = {
            let engine = engine.clone();
            let config = config.clone();
            tokio::spawn(async move { engine.ensure("owned", &config).await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), async {
                while !starting.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "replacement ran before detached worker finished"
        );
        drop(hold_completion);
        starting.await.unwrap().unwrap();
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn cancelled_startup_keeps_stream_fenced_during_old_worker_join() {
        let root = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(root.path(), "ffmpeg"));
        let original = json!({"inputs":[{"url":"testsrc://"}]});
        let changed = json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"});
        let old = engine.ensure("owned", &original).await.unwrap();
        let hold_completion = old.done.lock().await;
        let replacing = {
            let engine = engine.clone();
            let changed = changed.clone();
            tokio::spawn(async move { engine.ensure("owned", &changed).await })
        };
        tokio::time::timeout(Duration::from_secs(2), old.cancel.cancelled())
            .await
            .unwrap();
        replacing.abort();
        let _ = replacing.await;
        let next = {
            let engine = engine.clone();
            let changed = changed.clone();
            tokio::spawn(async move { engine.ensure("owned", &changed).await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), async {
                while !next.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "replacement ran before cancelled startup finished retirement"
        );
        drop(hold_completion);
        next.await.unwrap().unwrap();
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn cancelled_stop_keeps_stream_fenced_during_media_removal() {
        let root = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(root.path(), "ffmpeg");
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        engine.ensure("owned", &config).await.unwrap();
        let entered = Arc::new(AtomicU64::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        engine.before_remove_test_hook = Some(Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    if entered.fetch_add(1, Ordering::SeqCst) == 0 {
                        release.notified().await;
                    }
                })
            }
        }));
        let engine = Arc::new(engine);
        let stopping = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.stop("owned").await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while entered.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stopping.abort();
        let _ = stopping.await;
        let starting = {
            let engine = engine.clone();
            let config = config.clone();
            tokio::spawn(async move { engine.ensure("owned", &config).await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), async {
                while !starting.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "replacement ran while cancelled removal was pending"
        );
        release.notify_one();
        starting.await.unwrap().unwrap();
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn idle_snapshot_cannot_retire_replacement_or_renewed_viewer() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::new(root.path(), "ffmpeg");
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let old = engine.ensure("owned", &config).await.unwrap();
        *old.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        let snapshot = engine.idle().await;
        assert_eq!(snapshot.len(), 1);
        old.touch();
        engine.stop_if_idle_current("owned", &old).await;
        assert!(Arc::ptr_eq(
            engine.workers.lock().await.get("owned").unwrap(),
            &old
        ));

        *old.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        let changed = json!({"inputs":[{"url":"testsrc://"}],"flussonix_hls_subtitles":"drop"});
        let replacement = engine.ensure("owned", &changed).await.unwrap();
        engine.stop_if_idle_current("owned", &old).await;
        assert!(Arc::ptr_eq(
            engine.workers.lock().await.get("owned").unwrap(),
            &replacement
        ));
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn stop_all_waiting_for_one_join_keeps_other_stream_observable() {
        let root = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::new(root.path(), "ffmpeg"));
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let old = engine.ensure("owned", &config).await.unwrap();
        engine.ensure("other", &config).await.unwrap();
        let hold_completion = old.done.lock().await;
        let stopping = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.stop_all().await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !old.cancel.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(1), engine.count()).await;
        drop(hold_completion);
        stopping.await.unwrap();
        assert!(
            observed.is_ok(),
            "stop-all held the global map across a join"
        );
    }

    #[tokio::test]
    async fn idle_reconcile_commits_floor_before_removing_media() {
        let root = tempfile::tempdir().unwrap();
        let app = App::new(
            root.path().join("config.json"),
            root.path().join("media"),
            Options {
                admin_password: "owned-idle-admin".into(),
                peer_key: "owned-idle-peer".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let config = json!({"static":false,"inputs":[{"url":"testsrc://"}]});
        app.config.put("streams", "owned", config.clone()).unwrap();
        let worker = app.media.ensure("owned", &config).await.unwrap();
        *worker.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        let media = app.media.directory("owned");
        let record = root.path().join("media/.hls-sequences").join(format!(
            "{}.json",
            media.file_name().unwrap().to_string_lossy()
        ));

        app.reconcile().await;

        assert!(worker.is_closed());
        assert!(app.media.workers.lock().await.get("owned").is_none());
        assert!(record.is_file(), "idle cleanup discarded the durable floor");
        assert!(!media.exists(), "idle cleanup retained retired media");
        app.media.stop_all().await;
    }

    #[tokio::test]
    async fn startup_reserves_a_durable_floor_before_publishing_a_worker() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::new(root.path(), "ffmpeg");
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        engine.ensure("owned", &config).await.unwrap();
        let record = root.path().join(".hls-sequences").join(format!(
            "{}.json",
            engine
                .directory("owned")
                .file_name()
                .unwrap()
                .to_string_lossy()
        ));
        let saved = record.is_file();
        engine.stop_all().await;
        assert!(saved, "startup published a worker without a durable floor");
    }

    #[tokio::test]
    async fn failed_floor_commit_preserves_old_media_for_recovery() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::new(root.path(), "ffmpeg");
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let worker = engine.ensure("owned", &config).await.unwrap();
        let media = engine.directory("owned");
        std::fs::write(media.join("recovery-marker"), b"old generation").unwrap();
        let state = root.path().join(".hls-sequences");
        std::fs::create_dir_all(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
        let record = state.join(format!(
            "{}.json",
            media.file_name().unwrap().to_string_lossy()
        ));
        std::fs::write(&record, b"{").unwrap();
        std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o600)).unwrap();

        engine.stop("owned").await;

        assert!(worker.is_closed());
        assert!(
            media.join("recovery-marker").exists(),
            "failed commit discarded recovery media"
        );
    }

    #[tokio::test]
    async fn failed_selection_commit_preserves_media_and_does_not_change_preference() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::new(root.path(), "ffmpeg");
        let config = json!({"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]});
        engine.ensure("owned", &config).await.unwrap();
        let media = engine.directory("owned");
        std::fs::write(media.join("recovery-marker"), b"old generation").unwrap();
        let state = root.path().join(".hls-sequences");
        let record = state.join(format!(
            "{}.json",
            media.file_name().unwrap().to_string_lossy()
        ));
        std::fs::write(&record, b"{").unwrap();

        let selection = engine
            .select_input_guarded("owned", &config, 1, std::future::ready(true))
            .await;

        assert!(selection.is_err(), "selection hid the failed floor commit");
        assert!(
            media.join("recovery-marker").exists(),
            "failed selection lost recovery media"
        );
        assert_eq!(engine.preferred_input("owned", &config).await, None);
    }
}

#[cfg(test)]
#[path = "media/ts_diagnostics_tests.rs"]
mod ts_diagnostics_tests;

#[cfg(test)]
#[path = "media/rtsp_startup_tests.rs"]
mod rtsp_startup_tests;

#[cfg(test)]
mod hls_startup_tests {
    use super::*;

    #[tokio::test]
    async fn surviving_public_playlists_raise_the_startup_floor() {
        let root = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(root.path(), "ffmpeg");
        engine.hls_epoch = crate::hls_generation::Epoch::with_clock(root.path(), 1000);
        let media = engine.directory("owned");
        std::fs::create_dir_all(media.join("fmp4")).unwrap();
        std::fs::write(
            media.join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5000\na.ts\nb.ts\n",
        )
        .unwrap();
        std::fs::write(
            media.join("fmp4/index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7000\na.m4s\nb.m4s\n",
        )
        .unwrap();
        std::fs::write(
            media.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:999999\nprivate.ts\n",
        )
        .unwrap();
        let config = json!({"inputs":[{"url":"testsrc://"}]});

        engine.ensure("owned", &config).await.unwrap();

        let record = root.path().join(".hls-sequences").join(format!(
            "{}.json",
            media.file_name().unwrap().to_string_lossy()
        ));
        let saved: Value = serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
        assert_eq!(saved["next_sequence"], 7003);
        assert!(!media.join("passthrough.m3u8").exists());
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn invalid_or_exhausted_floor_preserves_recovery_media() {
        for bad in [
            "{",
            &format!(
                "{{\"version\":1,\"name\":\"owned\",\"next_sequence\":{}}}",
                i64::MAX - 1
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let engine = Engine::new(root.path(), "ffmpeg");
            let media = engine.directory("owned");
            std::fs::create_dir(&media).unwrap();
            std::fs::write(media.join("recovery-marker"), b"old media").unwrap();
            let state = root.path().join(".hls-sequences");
            std::fs::create_dir(&state).unwrap();
            std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
            let record = state.join(format!(
                "{}.json",
                media.file_name().unwrap().to_string_lossy()
            ));
            std::fs::write(&record, bad).unwrap();
            std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o600)).unwrap();

            assert!(
                engine
                    .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
                    .await
                    .is_err()
            );
            assert!(engine.workers.lock().await.is_empty());
            assert!(media.join("recovery-marker").exists());
        }
    }

    #[tokio::test]
    async fn stalled_floor_write_for_one_stream_does_not_delay_another() {
        let root = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(root.path(), "ffmpeg");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        engine.hls_epoch.before_write_for_test(move |name| {
            if name == "slow" {
                entered_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        });
        let engine = Arc::new(engine);
        let config = json!({"inputs":[{"url":"testsrc://"}]});
        let slow = {
            let engine = engine.clone();
            let config = config.clone();
            tokio::spawn(async move { engine.ensure("slow", &config).await })
        };
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(3)))
            .await
            .unwrap()
            .unwrap();
        let fast =
            tokio::time::timeout(Duration::from_secs(3), engine.ensure("fast", &config)).await;
        let heartbeat = tokio::time::timeout(Duration::from_millis(100), async {
            tokio::task::yield_now().await;
        })
        .await;
        release_tx.send(()).unwrap();
        slow.await.unwrap().unwrap();
        assert!(
            fast.is_ok_and(|result| result.is_ok()),
            "slow floor write blocked a different stream"
        );
        assert!(
            heartbeat.is_ok(),
            "slow state write blocked the async runtime"
        );
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn failed_hls_dns_moves_to_healthy_backup_input() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(dir.path(), "ffmpeg");
        engine.hls_prepare_test_hook = Some(Arc::new(|input: String| {
            Box::pin(async move {
                assert!(input.contains("unresolvable.invalid"));
                Err("peer media DNS failed".into())
            })
        }));
        let config = json!({"inputs":[
            {"url":"http://unresolvable.invalid/entry.m3u8"},
            {"url":"testsrc://"}
        ]});
        let worker = engine.ensure("fallback", &config).await.unwrap();
        assert_eq!(worker.input_index, 1);
        assert!(worker.alive.load(Ordering::Relaxed));
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn hls_prepare_does_not_hold_global_worker_map() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(dir.path(), "ffmpeg");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        engine.hls_prepare_test_hook = Some(Arc::new({
            let started = started.clone();
            let release = release.clone();
            move |input: String| {
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    started.notify_one();
                    release.notified().await;
                    crate::peer_hls::prepare(&input).await
                })
            }
        }));
        let engine = Arc::new(engine);
        let config = json!({"inputs":[{"url":"http://127.0.0.1:1/entry.m3u8"}]});
        let waiting = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.ensure("waiting", &config).await })
        };
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(250), engine.count())
                .await
                .is_ok()
        );
        let other = json!({"inputs":[{"url":"testsrc://"}]});
        tokio::time::timeout(Duration::from_secs(3), engine.ensure("unrelated", &other))
            .await
            .unwrap()
            .unwrap();
        release.notify_one();
        let _ = waiting.await.unwrap();
        engine.stop_all().await;
    }

    #[tokio::test]
    async fn stale_hls_preparation_cannot_publish_worker() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(dir.path(), "ffmpeg");
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        engine.hls_prepare_test_hook = Some(Arc::new({
            let started = started.clone();
            let release = release.clone();
            move |input: String| {
                let started = started.clone();
                let release = release.clone();
                Box::pin(async move {
                    started.notify_one();
                    release.notified().await;
                    crate::peer_hls::prepare(&input).await
                })
            }
        }));
        let engine = Arc::new(engine);
        let current = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let config = json!({"inputs":[{"url":"http://127.0.0.1:1/entry.m3u8"}]});
        let waiting = {
            let engine = engine.clone();
            let current = current.clone();
            tokio::spawn(async move {
                engine
                    .ensure_guarded("stale", &config, true, async move {
                        current.load(Ordering::SeqCst)
                    })
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        current.store(false, Ordering::SeqCst);
        release.notify_one();
        assert_eq!(
            waiting.await.unwrap().err().as_deref(),
            Some("media route changed")
        );
        assert!(engine.workers.lock().await.get("stale").is_none());
        let cancelled = {
            let engine = engine.clone();
            let config = json!({"inputs":[{"url":"http://127.0.0.1:1/entry.m3u8"}]});
            tokio::spawn(async move { engine.ensure("cancelled", &config).await })
        };
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        cancelled.abort();
        assert!(cancelled.await.is_err());
        assert!(engine.workers.lock().await.get("cancelled").is_none());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        engine.stop_all().await;
    }
}
