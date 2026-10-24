//! Original native records with independent peer and viewer authority.
use super::*;
use crate::fxlink::{Provider, Request as LinkRequest, Source};
use futures_util::future::BoxFuture;
use std::{
    ops::{Deref, DerefMut},
    sync::Weak,
};
use tokio_util::sync::CancellationToken;

/// Mirror publication preserves the existing async lock API. Only live FXLink
/// admissions register watches; ordinary catalog reads never clone or scan it.
#[derive(Default)]
pub(super) struct Mirrors {
    data: Mutex<HashMap<String, Mirror>>,
    watches: std::sync::Mutex<Vec<Weak<MirrorWatch>>>,
}
struct MirrorWatch {
    name: String,
    expected: [u8; 32],
    cancel: CancellationToken,
}
pub(super) struct MirrorGuard<'a> {
    data: tokio::sync::MutexGuard<'a, HashMap<String, Mirror>>,
    watches: &'a std::sync::Mutex<Vec<Weak<MirrorWatch>>>,
    dirty: bool,
}
impl Mirrors {
    pub(super) async fn lock(&self) -> MirrorGuard<'_> {
        MirrorGuard {
            data: self.data.lock().await,
            watches: &self.watches,
            dirty: false,
        }
    }
}
impl Deref for MirrorGuard<'_> {
    type Target = HashMap<String, Mirror>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl DerefMut for MirrorGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.data
    }
}
impl MirrorGuard<'_> {
    fn watch(&self, name: &str, cancel: CancellationToken) -> Result<Arc<MirrorWatch>, u16> {
        let expected = self
            .data
            .get(name)
            .filter(|m| {
                m.available && !m.denied && crate::source_directory::native_source(&m.source)
            })
            .map(mirror_fingerprint)
            .ok_or(503u16)?;
        let mut watches = self.watches.lock().unwrap();
        watches.retain(|w| w.upgrade().is_some_and(|w| !w.cancel.is_cancelled()));
        // Same ceiling as the listener's 64 peers x 16 subscriptions.
        if watches.len() >= 1024 {
            return Err(503);
        }
        let watch = Arc::new(MirrorWatch {
            name: name.into(),
            expected,
            cancel,
        });
        watches.push(Arc::downgrade(&watch));
        Ok(watch)
    }
}
/// Hash full deterministic JSON through a writer, without allocating a serialized
/// copy or retaining credentials in per-admission metadata.
fn mirror_fingerprint(mirror: &Mirror) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    // Serial and observation time order discovery publications; a successful
    // unchanged refresh advances them without changing admitted media authority.
    serde_json::to_writer(&mut writer, &mirror.config).expect("JSON value serialization");
    serde_json::to_writer(&mut writer, &mirror.source).expect("JSON value serialization");
    writer.0.finalize().into()
}
impl Drop for MirrorGuard<'_> {
    fn drop(&mut self) {
        // Read-only catalog accesses do O(1) work. Dirty publication hashes only
        // watched names once each, independent of catalog size/viewer fan-out.
        if !self.dirty {
            return;
        }
        let mut fingerprints = HashMap::<String, Option<[u8; 32]>>::new();
        self.watches.lock().unwrap().retain(|weak| {
            let Some(watch) = weak.upgrade() else {
                return false;
            };
            if watch.cancel.is_cancelled() {
                return false;
            }
            let fingerprint = fingerprints.entry(watch.name.clone()).or_insert_with(|| {
                self.data
                    .get(&watch.name)
                    .filter(|m| m.available && !m.denied)
                    .map(mirror_fingerprint)
            });
            let current = *fingerprint == Some(watch.expected);
            if !current {
                watch.cancel.cancel();
            }
            current
        });
    }
}

struct NativeProvider(Arc<App>);
struct Lease {
    app: Arc<App>,
    playback: ts_access::Playback,
    revision: u64,
    stream_watch: Option<Arc<crate::config::StreamWatch>>,
    _mirror_watch: Option<Arc<MirrorWatch>>,
    cancel: CancellationToken,
    end_reported: std::sync::atomic::AtomicBool,
}
impl Lease {
    /// Observability only: the first observed exit wins; no lifetime, queue,
    /// authority or transport decision depends on reporting this event.
    fn report_end(&self, reason: &'static str, missed_records: u64) {
        if tracing::enabled!(target: "flussonix::fxlink::source", tracing::Level::DEBUG)
            && !self.end_reported.swap(true, Ordering::Relaxed)
        {
            let (queued_records, queued_bytes) = self.playback.worker.wire.m4s.retained();
            tracing::debug!(
                target: "flussonix::fxlink::source",
                event = "fxlink_native_source_end",
                reason,
                missed_records,
                queued_records = queued_records as u64,
                queued_bytes = queued_bytes as u64,
                worker_pid = self.playback.worker.pid(),
                worker_closed = self.playback.worker.is_closed(),
                source_cancelled = self.cancel.is_cancelled(),
                local_watch = self.stream_watch.is_some(),
                "FXLink native source ended"
            );
        }
    }
    /// Transport calls this synchronously before every enqueue, including repairs.
    /// Local configuration watches retire only changed effective authority.
    /// Mirrored sources retain the conservative global revision fence.
    fn current(&self) -> bool {
        let current = !self.cancel.is_cancelled()
            && (self.stream_watch.is_some() || self.app.config.revision() == self.revision)
            && self.playback.current();
        if !current {
            self.report_end("authority_fenced", 0);
            self.cancel.cancel();
        }
        current
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl App {
    pub fn fxlink_provider(self: &Arc<Self>) -> Arc<dyn Provider> {
        Arc::new(NativeProvider(self.clone()))
    }
    pub fn set_fxlink_listener(&self, listener: Option<&crate::fxlink::Listener>) {
        let mut state = self.fxlink.lock().unwrap();
        state.0 = json!({"enabled":listener.is_some(),"listen":listener.map(|l|l.address().to_string()),
            "encrypted":true,"latency_ms":1000,"min_latency_ms":500,"max_latency_ms":1500,
            "fec":{"data_shards":20,"parity_shards":4,"profiles":["20+2","20+4","20+6","1+1"]}});
        state.1 = listener.map_or_else(
            || Arc::new(AtomicU64::new(0)),
            crate::fxlink::Listener::egress,
        );
    }
}
impl Provider for NativeProvider {
    fn subscribe(
        &self,
        request: LinkRequest,
        peer: SocketAddr,
    ) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            let app = &self.0;
            // Authentication precedes discovery, policy lookup and media ownership.
            if !app.credentials.peer(Some(&request.peer_key)) {
                return Err(403);
            }
            let revision = app.config.revision();
            if !app.config.read(|root| {
                matches!(
                    crate::vendor_playback::select(root, &request.name, revision),
                    crate::vendor_playback::Selection::Native
                )
            }) {
                return Err(404);
            }
            let mut playback = app
                .ts_admit(ViewerRequest {
                    name: request.name,
                    token: request.token,
                    proto: "fxlink".into(),
                    ip: peer.ip().to_string(),
                    ..Default::default()
                })
                .await?;
            let cancel = CancellationToken::new();
            let mirror_watch = {
                let mirrors = app.mirrors.lock().await;
                if app.config.effective(&playback.name).is_some() {
                    None
                } else {
                    Some(mirrors.watch(&playback.name, cancel.clone())?)
                }
            };
            let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
            loop {
                let current = tokio::select! {biased;
                    _=cancel.cancelled()=>return Err(503),
                    _=playback.cancelled()=>return Err(403),
                    _=playback.worker.closed()=>return Err(503),
                    result=tokio::time::timeout_at(deadline, app.ts_current(&playback))=>result.unwrap_or(false),
                };
                if app.config.revision() != revision || !current {
                    return Err(503);
                }
                if playback.worker.wire.has_info() {
                    break;
                }
                tokio::select! {biased;
                    _=cancel.cancelled()=>return Err(503),
                    _=playback.cancelled()=>return Err(403),
                    _=playback.worker.closed()=>return Err(503),
                    _=tokio::time::sleep_until(deadline)=>return Err(503),
                    _=tokio::time::sleep(Duration::from_millis(50))=>{},
                }
            }
            let (initial, mut rx) = playback.worker.m4s_subscribe().ok_or(503u16)?;
            if initial.is_empty() {
                return Err(503);
            }
            let stream_watch = if mirror_watch.is_none() {
                Some(
                    app.config
                        .watch_stream(&playback.name, revision, cancel.clone())
                        .ok_or(503u16)?,
                )
            } else {
                None
            };
            playback.attach();
            let lease = Arc::new(Lease {
                app: app.clone(),
                playback,
                revision,
                stream_watch,
                _mirror_watch: mirror_watch,
                cancel,
                end_reported: std::sync::atomic::AtomicBool::new(false),
            });
            if !lease.current() {
                return Err(503);
            }
            let cancelled = lease.cancel.clone();
            let probe = rx.demand_probe();
            let boot_remaining = Arc::new(std::sync::atomic::AtomicUsize::new(initial.len()));
            let weak_boot = Arc::downgrade(&boot_remaining);
            let worker_cancel = lease.playback.worker.cancellation_token();
            let source_cancel = cancelled.clone();
            let timing_enabled =
                tracing::enabled!(target:"flussonix::fxlink::service_timing",tracing::Level::TRACE);
            let timing = timing_enabled.then(|| {
                (
                    std::time::Instant::now(),
                    std::sync::atomic::AtomicU64::new(0),
                    std::sync::atomic::AtomicBool::new(false),
                )
            });
            let reader_id = probe.reader_id();
            let worker_pid = lease.playback.worker.pid();
            let pending_records = Arc::new(move || {
                if source_cancel.is_cancelled() || worker_cancel.is_cancelled() {
                    return None;
                }
                let boot = weak_boot.upgrade()?;
                let pending = probe
                    .unread()
                    .map(|unread| unread || boot.load(Ordering::Acquire) != 0);
                if source_cancel.is_cancelled() || worker_cancel.is_cancelled() {
                    return None;
                }
                if let Some((start, report_at, terminal_reported)) = &timing {
                    let at = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    let terminal = pending.is_none();
                    let due = report_at.load(Ordering::Relaxed);
                    let emit = if terminal {
                        !terminal_reported.swap(true, Ordering::Relaxed)
                    } else {
                        at >= due
                            && report_at
                                .compare_exchange(
                                    due,
                                    at.saturating_add(100),
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                    };
                    if emit && let Some(s) = probe.diagnostic() {
                        // Snapshot owns metadata only; its queue lock ended before tracing.
                        tracing::trace!(target:"flussonix::fxlink::service_timing",terminal,elapsed_ms=at,reader_id=s.reader_id,worker_pid,producer_next=s.producer_next,reader_next=s.reader_next,unread_records=s.unread_records,missed_records=s.missed_records,retained_records=s.retained_records,alive=s.alive,closed=s.closed,bootstrap_records=boot.load(Ordering::Acquire),"FXLink native timing snapshot");
                    }
                }
                pending
            });
            if timing_enabled {
                tracing::trace!(target:"flussonix::fxlink::service_timing",pending_id=Arc::as_ptr(&pending_records) as *const () as usize,reader_id,worker_pid,bootstrap_records=boot_remaining.load(Ordering::Acquire),"FXLink native timing binding");
            }
            let fence = lease.clone();
            let records = futures_util::stream::unfold(
                (
                    std::collections::VecDeque::from(initial),
                    rx,
                    lease,
                    boot_remaining,
                ),
                |(mut boot, mut rx, lease, boot_remaining)| async move {
                    loop {
                        if !lease.current() {
                            return None;
                        }
                        let record = if let Some(record) = boot.pop_front() {
                            boot_remaining.fetch_sub(1, Ordering::AcqRel);
                            record
                        } else {
                            tokio::select! {biased;
                                _=lease.playback.cancelled()=>{lease.report_end("viewer_cancelled",0);lease.cancel.cancel();return None},
                                _=lease.playback.worker.closed()=>{lease.report_end("worker_closed",0);lease.cancel.cancel();return None},
                                _=lease.cancel.cancelled()=>{lease.report_end("source_cancelled",0);return None},
                                _=tokio::time::sleep(Duration::from_millis(50))=>{
                                    let current = tokio::select! {biased;
                                        _=lease.cancel.cancelled()=>false,
                                        _=lease.playback.cancelled()=>false,
                                        _=lease.playback.worker.closed()=>false,
                                        result=lease.app.ts_current(&lease.playback)=>result,
                                    };
                                    if !current { lease.report_end("refresh_rejected",0);lease.cancel.cancel(); return None; }
                                    continue;
                                },
                                result=rx.recv()=>match result {
                                    Ok(record)=>record,
                                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed))=>{
                                        lease.report_end("queue_lagged",missed);lease.cancel.cancel();return None
                                    },
                                    Err(tokio::sync::broadcast::error::RecvError::Closed)=>{
                                        lease.report_end("queue_closed",0);lease.cancel.cancel();return None
                                    }
                                }
                            }
                        };
                        if !lease.current() {
                            return None;
                        }
                        if !matches!(
                            lease.playback.write_current(record.len(), || Ok(true)),
                            Ok(Ok(true))
                        ) {
                            lease.report_end("write_fenced", 0);
                            return None;
                        }
                        return Some((Ok(record), (boot, rx, lease, boot_remaining)));
                    }
                },
            );
            Ok(Source {
                pending_records: Some(pending_records),
                records: Box::pin(records),
                current: Arc::new(move || fence.current()),
                cancelled,
            })
        })
    }
}

#[cfg(test)]
#[path = "fxlink_source_diagnostics_tests.rs"]
mod source_diagnostics_tests;

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::fxlink::Request as LinkRequest;
    use futures_util::StreamExt;
    fn app(dir: &Path) -> Arc<App> {
        App::new(
            dir.join("config.json"),
            dir.join("media"),
            Options {
                admin_password: "owned-daemon-admin".into(),
                peer_key: "owned-daemon-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn request(key: &str, token: &str) -> LinkRequest {
        LinkRequest {
            name: "owned".into(),
            peer_key: key.into(),
            token: token.into(),
        }
    }
    fn configure(app: &App) {
        use sha2::{Digest, Sha256};
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}],
            "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))}),
            )
            .unwrap();
    }
    #[tokio::test]
    async fn peer_and_viewer_credentials_are_independent_and_denied_before_media() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        configure(&app);
        let provider = app.fxlink_provider();
        let peer = "127.0.0.1:12345".parse().unwrap();
        assert!(matches!(
            provider
                .subscribe(request("wrong-peer", "owned-viewer"), peer)
                .await,
            Err(403)
        ));
        assert!(matches!(
            provider
                .subscribe(request("owned-daemon-peer-key", "wrong-viewer"), peer)
                .await,
            Err(403)
        ));
        assert_eq!(app.media.count().await, 0);
        let diagnostics = app.load_node().await.to_string();
        assert!(
            !diagnostics.contains("owned-daemon-peer-key") && !diagnostics.contains("owned-viewer")
        );
    }
    #[tokio::test]
    async fn original_records_are_preserved_and_revocation_fences_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        configure(&app);
        let provider = app.fxlink_provider();
        let mut source = provider
            .subscribe(
                request("owned-daemon-peer-key", "owned-viewer"),
                "127.0.0.1:12345".parse().unwrap(),
            )
            .await
            .unwrap();
        let worker = app
            .media
            .ensure("owned", &app.config.effective("owned").unwrap())
            .await
            .unwrap();
        let (bootstrap, _) = worker.m4s_subscribe().unwrap();
        assert_eq!(source.records.next().await.unwrap().unwrap(), bootstrap[0]);
        let opaque = Bytes::from_static(b"opaque original native record\0\xff");
        worker.wire.m4s.send(opaque.clone()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if source.records.next().await.unwrap().unwrap() == opaque {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!((source.current)());
        let id = app.playback_auth.snapshots()[0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(app.playback_auth.revoke(&id));
        assert!(!(source.current)());
        assert!(source.cancelled.is_cancelled());
        assert!(source.records.next().await.is_none());
        assert_eq!(app.egress.load(Ordering::Relaxed), 0);
        drop(source);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        app.media.stop_all().await;
    }
    #[tokio::test]
    async fn unrelated_config_edits_preserve_local_fxlink_until_its_own_config_changes() {
        use futures_util::FutureExt;
        use std::panic::AssertUnwindSafe;
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        configure(&app);
        let outcome = AssertUnwindSafe(async {
            let mut source = app
                .fxlink_provider()
                .subscribe(
                    request("owned-daemon-peer-key", "owned-viewer"),
                    "127.0.0.1:12345".parse().unwrap(),
                )
                .await
                .unwrap();
            let worker = app
                .media
                .ensure("owned", &app.config.effective("owned").unwrap())
                .await
                .unwrap();
            let pid = worker.pid();
            app.config
                .put(
                    "streams",
                    "other",
                    json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                )
                .unwrap();
            app.config
                .put("templates", "unused", json!({"static":false}))
                .unwrap();
            app.config
                .put(
                    "auth_backends",
                    "unused",
                    json!({"url":"http://127.0.0.1:1/unused"}),
                )
                .unwrap();
            app.config.delete("streams", "other").unwrap();
            app.invalidate_sessions().await;
            assert!(
                (source.current)(),
                "unrelated config edits retired local FXLink media"
            );
            assert!(!source.cancelled.is_cancelled());
            assert_eq!(worker.pid(), pid);
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 1);
            let marker = Bytes::from_static(b"owned media after unrelated config changes");
            worker.wire.m4s.send(marker.clone()).unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while source.records.next().await.unwrap().unwrap() != marker {}
            })
            .await
            .unwrap();
            // A relevant edit followed by a restore must never revive authority,
            // even if the transport has not polled current() in between.
            let original = app.config.snapshot();
            app.config
                .put("streams", "owned", json!({"disabled":true}))
                .unwrap();
            app.config
                .put("streams", "owned", json!({"disabled":null}))
                .unwrap();
            assert_eq!(app.config.snapshot(), original);
            assert!(source.cancelled.is_cancelled());
            assert!(!(source.current)());
            assert!(source.records.next().await.is_none());
            drop(source);
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        })
        .catch_unwind()
        .await;
        app.media.stop_all().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    #[tokio::test]
    async fn config_disable_fences_immediately_and_idle_stream_retires() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        configure(&app);
        let provider = app.fxlink_provider();
        let mut source = provider
            .subscribe(
                request("owned-daemon-peer-key", "owned-viewer"),
                "127.0.0.1:12345".parse().unwrap(),
            )
            .await
            .unwrap();
        app.config
            .put("streams", "owned", json!({"disabled":true}))
            .unwrap();
        assert!(!(source.current)());
        assert!(source.cancelled.is_cancelled());
        assert!(source.records.next().await.is_none());
        drop(source);
        app.media.stop_all().await;
    }
    #[tokio::test]
    async fn mirror_lock_contention_keeps_current_but_source_publication_retires() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        app.config
            .put("sources", "origin", json!({"api_url":"http://127.0.0.1:1"}))
            .unwrap();
        let source = app.config.snapshot()["sources"][0].clone();
        app.mirrors.lock().await.insert(
            "owned".into(),
            Mirror {
                when: Instant::now(),
                config: json!({"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]}),
                source,
                available: true,
                denied: false,
                known: true,
                activation_only: false,
                serial: 1,
                switches: 0,
            },
        );
        let provider = app.fxlink_provider();
        let linked = provider
            .subscribe(
                request("owned-daemon-peer-key", "owned-viewer"),
                "127.0.0.1:12345".parse().unwrap(),
            )
            .await
            .unwrap();
        let mut mirrors = app.mirrors.lock().await;
        let current_under_contention = (linked.current)();
        mirrors.get_mut("owned").unwrap().config["transcoder"] =
            json!({"encoder":"libx264","vb":900});
        drop(mirrors);
        let retired_after_publication = !(linked.current)();
        drop(linked);
        app.media.stop_all().await;
        assert!(
            current_under_contention,
            "ordinary source mutex contention retired an admitted subscription"
        );
        assert!(
            retired_after_publication,
            "published source change retained old authority"
        );
    }
    #[tokio::test]
    async fn healthy_mirror_bookkeeping_refresh_preserves_records_and_viewer_reference() {
        use futures_util::FutureExt;
        use std::panic::AssertUnwindSafe;
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        app.config
            .put("sources", "origin", json!({"api_url":"http://127.0.0.1:1"}))
            .unwrap();
        let source_config = app.config.snapshot()["sources"][0].clone();
        let config = json!({"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]});
        app.mirrors.lock().await.insert(
            "owned".into(),
            Mirror {
                when: Instant::now(),
                config: config.clone(),
                source: source_config,
                available: true,
                denied: false,
                known: true,
                activation_only: false,
                serial: 1,
                switches: 0,
            },
        );
        let outcome = AssertUnwindSafe(async {
            let provider = app.fxlink_provider();
            let mut source = provider
                .subscribe(
                    request("owned-daemon-peer-key", "owned-viewer"),
                    "127.0.0.1:12345".parse().unwrap(),
                )
                .await
                .unwrap();
            let worker = app.media.ensure("owned", &config).await.unwrap();
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 1);
            {
                let mut mirrors = app.mirrors.lock().await;
                let mirror = mirrors.get_mut("owned").unwrap();
                mirror.serial += 1;
                mirror.when = Instant::now();
            }
            assert!(
                (source.current)(),
                "unchanged mirror refresh retired an admitted source"
            );
            assert!(!source.cancelled.is_cancelled());
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 1);
            let original = Bytes::from_static(b"owned original record after healthy refresh");
            worker.wire.m4s.send(original.clone()).unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if source.records.next().await.unwrap().unwrap() == original {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            {
                let mut mirrors = app.mirrors.lock().await;
                // Authority changes remain fenced even without a bookkeeping version change.
                mirrors.get_mut("owned").unwrap().source["cluster_key"] =
                    json!("changed-native-peer-key");
            }
            assert!(!(source.current)());
            assert!(source.cancelled.is_cancelled());
            app.mirrors.lock().await.get_mut("owned").unwrap().source =
                app.config.snapshot()["sources"][0].clone();
            assert!(
                !(source.current)(),
                "restoring source metadata revived retired authority"
            );
            assert!(source.records.next().await.is_none());
            drop(source);
            assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
        })
        .catch_unwind()
        .await;
        app.media.stop_all().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    #[tokio::test]
    async fn real_native_mirror_refresh_preserves_fxlink_receiver_until_content_changes() {
        use futures_util::FutureExt;
        use std::panic::AssertUnwindSafe;
        use tokio_util::sync::CancellationToken;
        let dir = tempfile::tempdir().unwrap();
        let origin = app(&dir.path().join("origin"));
        configure(&origin);
        origin
            .config
            .put(
                "streams",
                "owned",
                json!({"flussonix_content_id":"owned-content-v1"}),
            )
            .unwrap();
        let relay = app(&dir.path().join("relay"));
        let receiver = Engine::new(dir.path().join("receiver"), "ffmpeg");
        let cancel = CancellationToken::new();
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        relay
            .config
            .put(
                "sources",
                "origin",
                json!({"api_url":format!("http://{}",http.local_addr().unwrap()),
            "cluster_key":"owned-daemon-peer-key","flussonix_transport":"m4s"}),
            )
            .unwrap();
        let http_task = tokio::spawn(crate::http_transport::serve(
            http,
            origin.clone(),
            cancel.clone(),
        ));
        let certificates = crate::transport_tls_fixture::Certificates::new();
        let listener = crate::fxlink::Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &certificates.cert,
            &certificates.key,
            crate::fxlink::Settings::default(),
        )
        .unwrap();
        let endpoint = format!("fxlink://{}/owned", listener.address());
        relay.set_fxlink_listener(Some(&listener));
        let fx_task = tokio::spawn(listener.serve(relay.fxlink_provider(), cancel.clone()));
        let outcome = AssertUnwindSafe(async {
            let worker = receiver.ensure("received", &json!({"flussonix_peer_key":"owned-daemon-peer-key",
                "inputs":[{"url":endpoint,"flussonix_tls_ca":certificates.ca,"flussonix_fxlink_token":"owned-viewer"}]})).await.unwrap();
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    assert!(!worker.is_closed(), "native chain failed before media: {}", receiver.stats("received").await);
                    if worker.bytes.load(Ordering::Relaxed) > 0 && worker.wire.has_info() { break; }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }).await.unwrap();
            let provider = relay.fxlink_provider();
            let mut source = provider.subscribe(request("owned-daemon-peer-key", "owned-viewer"), "127.0.0.1:12345".parse().unwrap()).await.unwrap();
            let before = relay.mirrors.lock().await.get("owned").unwrap().clone();
            let relay_worker = relay.media.ensure("owned", &before.config).await.unwrap();
            let receiver_pid = worker.pid();
            let relay_pid = relay_worker.pid();
            let viewers = relay_worker.viewers.load(Ordering::Relaxed);
            assert_eq!(viewers, 2);
            let received_bytes = worker.bytes.load(Ordering::Relaxed);
            relay.mirrors.lock().await.get_mut("owned").unwrap().when = Instant::now() - Duration::from_secs(11);
            relay.refresh_sources().await;
            let after = relay.mirrors.lock().await.get("owned").unwrap().clone();
            assert!(after.serial > before.serial, "test must exercise successful install_origin publication");
            assert!(after.when.elapsed() < Duration::from_secs(2));
            assert_eq!(after.config, before.config);
            assert_eq!(after.source, before.source);
            assert!((source.current)(), "healthy install_origin refresh retired mirrored FXLink authority");
            assert!(!source.cancelled.is_cancelled());
            assert_eq!(relay_worker.viewers.load(Ordering::Relaxed), viewers);
            let (_, mut original_records) = relay_worker.m4s_subscribe().unwrap();
            let original = tokio::time::timeout(Duration::from_secs(2), original_records.recv()).await.unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                loop { if source.records.next().await.unwrap().unwrap() == original { break; } }
            }).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while worker.bytes.load(Ordering::Relaxed) <= received_bytes {
                    assert!(!worker.is_closed(), "healthy refresh closed downstream worker");
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }).await.unwrap();
            assert!(!worker.is_closed() && !relay_worker.is_closed());
            assert_eq!(worker.pid(), receiver_pid);
            assert_eq!(relay_worker.pid(), relay_pid);
            assert_eq!(relay_worker.viewers.load(Ordering::Relaxed), viewers);
            origin.config.put("streams", "owned", json!({"flussonix_content_id":"owned-content-v2"})).unwrap();
            relay.mirrors.lock().await.get_mut("owned").unwrap().when = Instant::now() - Duration::from_secs(11);
            relay.refresh_sources().await;
            assert_eq!(relay.mirrors.lock().await.get("owned").unwrap().config["flussonix_content_id"], "owned-content-v2");
            assert!(!(source.current)());
            assert!(source.cancelled.is_cancelled());
            drop(source);
            tokio::time::timeout(Duration::from_secs(3), worker.closed()).await.expect("changed content identity must retire downstream worker");
            tokio::time::timeout(Duration::from_secs(2), async {
                while relay_worker.viewers.load(Ordering::Relaxed) != 0 { tokio::task::yield_now().await; }
            }).await.unwrap();
        }).catch_unwind().await;
        cancel.cancel();
        receiver.stop_all().await;
        relay.media.stop_all().await;
        origin.media.stop_all().await;
        tokio::time::timeout(Duration::from_secs(5), fx_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), http_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receiver.count().await, 0);
        assert_eq!(relay.media.count().await, 0);
        assert_eq!(origin.media.count().await, 0);
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}
