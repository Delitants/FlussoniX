//! Catch stale private requests and publication after an admitted handoff.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router, body::Body, extract::OriginalUri, http::HeaderMap, response::Response, routing::get,
};
use bytes::Bytes;
use flussonix::{
    config::ConfigStore,
    m4f::{self, Frame},
    m4s::Track,
    playback_auth::{PlaybackAuth, ViewerRequest},
    source_directory::modern_coordinator::{
        AdmissionOutcome, AdmittedCandidate, CoordinatedInventory, ModernSourceCoordinator,
        PrivatePullFailure as Failure, PrivatePullOptions, SourceHandle,
    },
    wire::{self, Hub},
};
use serde_json::json;
use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::AsyncWrite,
    sync::{Notify, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
const NAME: &str = "owned";
type Feed = mpsc::UnboundedSender<Result<Bytes, io::Error>>;
struct Connection {
    path: String,
    feed: Feed,
}
struct Fixture {
    _dir: tempfile::TempDir,
    config: Arc<ConfigStore>,
    owner: Arc<ModernSourceCoordinator>,
    auth: Arc<PlaybackAuth>,
    handle: SourceHandle,
    key: String,
    connections: mpsc::UnboundedReceiver<Connection>,
    calls: Arc<Mutex<Vec<String>>>,
    headers: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    async fn start(m4f: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", listener.local_addr().unwrap());
        let key = root.replacen("http", if m4f { "m4f" } else { "m4s" }, 1);
        let calls = Arc::new(Mutex::new(vec![]));
        let headers: Arc<Mutex<Option<oneshot::Receiver<()>>>> = Arc::new(Mutex::new(None));
        let (tx, connections) = mpsc::unbounded_channel();
        let (paths, held) = (calls.clone(), headers.clone());
        let router=Router::new().fallback(get(move |OriginalUri(uri):OriginalUri,header:HeaderMap| {
            let(paths,held,tx)=(paths.clone(),held.clone(),tx.clone());
            async move {
                paths.lock().unwrap().push(uri.path().to_owned());
                if uri.path().starts_with("/owned/") {
                    assert!(header.contains_key("cluster-auth-key"));assert!(!header.contains_key("authorization"));
                    let(feed,rx)=mpsc::unbounded_channel();tx.send(Connection{path:uri.path().into(),feed}).unwrap();
                    let pause=held.lock().unwrap().take();if let Some(pause)=pause {let _=pause.await;}
                    return Response::new(Body::from_stream(tokio_stream::wrappers::UnboundedReceiverStream::new(rx)));
                }
                assert!(header.contains_key("authorization"));assert!(!header.contains_key("cluster-auth-key"));
                let value=if uri.path().ends_with("/config") {json!({"stats":{"id":"owned-origin"}})} else {
                    assert_eq!(uri.path(),"/streamer/api/v3/streams");json!({"server_id":"owned-origin","streams":[{"name":NAME,"groups":[],"named_by":"config","static":true}]})
                };
                Response::new(Body::from(value.to_string()))
            }
        }));
        let server = tokio::spawn(async move {
            axum::serve(listener, vendor_fixture::acknowledge_media(router))
                .await
                .unwrap()
        });
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
        config
            .put(
                "sources",
                &key,
                json!({"url":key,"cluster_key":"owned-private-key"}),
            )
            .unwrap();
        let owner = Arc::new(ModernSourceCoordinator::new(config.clone(), "owned-local").unwrap());
        let handle = install(&owner, &key);
        Self {
            _dir: dir,
            config,
            owner,
            auth: Arc::new(PlaybackAuth::new(16)),
            handle,
            key,
            connections,
            calls,
            headers,
            server,
        }
    }
    async fn inventory(&self, lifetime: Duration) -> CoordinatedInventory {
        self.owner.inventory(&self.handle, lifetime).await.unwrap()
    }
    async fn admitted(&self, rows: &CoordinatedInventory, token: &str) -> Box<AdmittedCandidate> {
        let request = ViewerRequest {
            name: NAME.into(),
            proto: "hls".into(),
            ip: "127.0.0.1".into(),
            token: token.into(),
            ..Default::default()
        };
        let AdmissionOutcome::Allowed(admitted) = self
            .owner
            .admit(rows.candidate(NAME).unwrap(), &self.auth, request)
            .await
            .unwrap()
        else {
            panic!("owned viewer denied")
        };
        admitted
    }
    async fn connection(&mut self) -> Connection {
        tokio::time::timeout(Duration::from_secs(2), self.connections.recv())
            .await
            .unwrap()
            .unwrap()
    }
    fn media_calls(&self) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.starts_with("/owned/"))
            .count()
    }
}
fn install(owner: &ModernSourceCoordinator, key: &str) -> SourceHandle {
    owner
        .install(
            owner
                .capture(key)
                .unwrap()
                .with_policy(&json!({"on_play":"true"}), &json!({}))
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap()
}
fn audio() -> Track {
    Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }
}
fn frame() -> Frame {
    Frame {
        track_id: 1,
        dts: 90000,
        pts_offset: 0,
        key: true,
        body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
    }
}
fn info() -> Bytes {
    Bytes::from(wire::encode_info(&[audio()]).unwrap())
}
fn sample() -> Bytes {
    Bytes::from(wire::encode_frame(&audio(), &frame()).unwrap())
}
async fn join<T>(task: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
}
async fn record(receiver: &mut flussonix::media_queue::Receiver) -> Bytes {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn invalid_authority_or_cancelled_owner_never_opens_private_http() {
    for mode in 0..3 {
        let f = Fixture::start(false).await;
        let rows = f.inventory(Duration::from_secs(30)).await;
        let admitted = f.admitted(&rows, "one").await;
        let cancel = CancellationToken::new();
        match mode {
            0 => {
                cancel.cancel();
            }
            1 => {
                f.config
                    .put("templates", "edit", json!({"static":false}))
                    .unwrap();
            }
            _ => {
                let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
                f.auth.revoke(&id);
            }
        }
        let (tx, rx) = oneshot::channel();
        let mut out = vec![];
        let hub = Hub::new();
        let result = f
            .owner
            .pull_admitted(
                &admitted,
                &mut out,
                Some(&hub),
                tx,
                PrivatePullOptions {
                    cancel: Some(&cancel),
                    ..Default::default()
                },
            )
            .await;
        if mode == 0 {
            assert_eq!(result, Err(Failure::Cancelled));
        } else {
            assert!(matches!(result, Err(Failure::Authority(_))));
        }
        assert!(rx.await.is_err());
        assert!(out.is_empty());
        assert_eq!(hub.m4s.retained().0, 0);
        assert_eq!(f.media_calls(), 0);
    }
}

#[tokio::test]
async fn m4s_pull_rechecks_all_source_fences_during_idle_and_before_new_data() {
    for mutation in 0..5 {
        let mut f = Fixture::start(false).await;
        let rows = f
            .inventory(if mutation == 4 {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(30)
            })
            .await;
        let admitted = f.admitted(&rows, "one").await;
        let owner = f.owner.clone();
        let hub = Arc::new(Hub::new());
        let h = hub.clone();
        let mut records = hub.m4s.subscribe();
        let (tx, rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut bytes = vec![];
            let result = owner
                .pull_admitted(
                    &admitted,
                    &mut bytes,
                    Some(&h),
                    tx,
                    PrivatePullOptions::default(),
                )
                .await;
            (result, bytes)
        });
        let connection = f.connection().await;
        assert_eq!(connection.path, "/owned/m4s");
        connection.feed.send(Ok(info())).unwrap();
        assert_eq!(rx.await.unwrap()[0].codec, "mp3");
        record(&mut records).await;
        match mutation {
            0 => {
                f.config
                    .put("templates", "edit", json!({"static":false}))
                    .unwrap();
            }
            1 => {
                f.owner.replace_aliases(&["new-local-alias"]).unwrap();
            }
            2 => {
                install(&f.owner, &f.key);
            }
            3 => {
                f.inventory(Duration::from_secs(30)).await;
            }
            _ => {
                tokio::time::sleep_until(
                    tokio::time::Instant::from_std(rows.expires_at()) + Duration::from_millis(5),
                )
                .await;
            }
        }
        let _ = connection.feed.send(Ok(sample()));
        let (result, bytes) = join(task).await;
        assert!(matches!(result, Err(Failure::Authority(_))));
        assert_eq!(bytes.len(), 376);
        assert_eq!(hub.m4s.retained().0, 1);
        tokio::time::timeout(Duration::from_secs(1), connection.feed.closed())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn revoking_one_pull_preserves_another_authorized_pull_and_hub() {
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let first = f.admitted(&rows, "one").await;
    let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let second = f.admitted(&rows, "two").await;
    let h1 = Arc::new(Hub::new());
    let h2 = Arc::new(Hub::new());
    let cancel = Arc::new(CancellationToken::new());
    let mut receiver1 = h1.m4s.subscribe();
    let mut receiver2 = h2.m4s.subscribe();
    // Keep startup metadata receivers alive for the actual media consumers.
    let (tx1, rx1) = oneshot::channel();
    let owner = f.owner.clone();
    let hub = h1.clone();
    let c1 = Arc::new(CancellationToken::new());
    let c = c1.clone();
    let t1 = tokio::spawn(async move {
        owner
            .pull_admitted(
                &first,
                &mut tokio::io::sink(),
                Some(&hub),
                tx1,
                PrivatePullOptions {
                    cancel: Some(&c),
                    ..Default::default()
                },
            )
            .await
    });
    let one = f.connection().await;
    one.feed.send(Ok(info())).unwrap();
    rx1.await.unwrap();
    record(&mut receiver1).await;
    let (tx2, rx2) = oneshot::channel();
    let owner = f.owner.clone();
    let hub = h2.clone();
    let c = cancel.clone();
    let t2 = tokio::spawn(async move {
        owner
            .pull_admitted(
                &second,
                &mut tokio::io::sink(),
                Some(&hub),
                tx2,
                PrivatePullOptions {
                    cancel: Some(&c),
                    ..Default::default()
                },
            )
            .await
    });
    let two = f.connection().await;
    two.feed.send(Ok(info())).unwrap();
    rx2.await.unwrap();
    record(&mut receiver2).await;
    assert!(f.auth.revoke(&id));
    assert!(matches!(join(t1).await, Err(Failure::Authority(_))));
    assert!(!t2.is_finished());
    two.feed.send(Ok(sample())).unwrap();
    let bytes = record(&mut receiver2).await;
    assert!(matches!(
        flussonix::m4s::Decoder::default().push(&bytes).unwrap()[0],
        flussonix::m4s::Event::Frame { .. }
    ));
    assert_eq!(h1.m4s.retained().0, 1);
    assert_eq!(h2.m4s.retained().0, 2);
    cancel.cancel();
    assert_eq!(join(t2).await, Err(Failure::Cancelled));
}

struct Gate {
    bytes: Mutex<Vec<u8>>,
    blocked: AtomicBool,
    entered: Notify,
    waker: futures_util::task::AtomicWaker,
}
struct Writer(Arc<Gate>);
impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.waker.register(cx.waker());
        if self.0.blocked.load(Ordering::SeqCst) {
            self.0.entered.notify_one();
            return Poll::Pending;
        }
        self.0.bytes.lock().unwrap().extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn pending_writer_cannot_resume_stale_bytes_or_publish_native_frames() {
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let admitted = f.admitted(&rows, "one").await;
    let gate = Arc::new(Gate {
        bytes: Mutex::new(vec![]),
        blocked: AtomicBool::new(false),
        entered: Notify::new(),
        waker: futures_util::task::AtomicWaker::new(),
    });
    let hub = Arc::new(Hub::new());
    let mut receiver = hub.m4s.subscribe();
    let owner = f.owner.clone();
    let h = hub.clone();
    let g = gate.clone();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        owner
            .pull_admitted(
                &admitted,
                &mut Writer(g),
                Some(&h),
                tx,
                PrivatePullOptions::default(),
            )
            .await
    });
    let connection = f.connection().await;
    connection.feed.send(Ok(info())).unwrap();
    rx.await.unwrap();
    record(&mut receiver).await;
    gate.blocked.store(true, Ordering::SeqCst);
    connection.feed.send(Ok(sample())).unwrap();
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    f.config
        .put("templates", "edit", json!({"static":false}))
        .unwrap();
    gate.blocked.store(false, Ordering::SeqCst);
    gate.waker.wake();
    assert!(matches!(join(task).await, Err(Failure::Authority(_))));
    assert_eq!(gate.bytes.lock().unwrap().len(), 376);
    assert_eq!(hub.m4s.retained().0, 1);
}

#[tokio::test]
async fn late_control_headers_cannot_publish_metadata_or_bytes() {
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let admitted = f.admitted(&rows, "one").await;
    let (release, held) = oneshot::channel();
    *f.headers.lock().unwrap() = Some(held);
    let owner = f.owner.clone();
    let hub = Arc::new(Hub::new());
    let h = hub.clone();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut out = vec![];
        let result = owner
            .pull_admitted(
                &admitted,
                &mut out,
                Some(&h),
                tx,
                PrivatePullOptions::default(),
            )
            .await;
        (result, out)
    });
    let connection = f.connection().await;
    connection.feed.send(Ok(info())).unwrap();
    f.config
        .put("templates", "edit", json!({"static":false}))
        .unwrap();
    let _ = release.send(());
    let (result, out) = join(task).await;
    assert!(matches!(result, Err(Failure::Authority(_))));
    assert!(out.is_empty());
    assert!(rx.await.is_err());
    assert_eq!(hub.m4s.retained().0, 0);
}

#[tokio::test]
async fn m4f_partial_segment_is_discarded_before_publication_or_next_fetch() {
    let mut f = Fixture::start(true).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let admitted = f.admitted(&rows, "one").await;
    let owner = f.owner.clone();
    let hub = Arc::new(Hub::new());
    let h = hub.clone();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut out = vec![];
        let result = owner
            .pull_admitted(
                &admitted,
                &mut out,
                Some(&h),
                tx,
                PrivatePullOptions::default(),
            )
            .await;
        (result, out)
    });
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(
            b"1 2026/10/13/00/00/00-2000\n2 2026/10/13/00/00/02-2000\n",
        )))
        .unwrap();
    let segment = f.connection().await;
    assert!(segment.path.ends_with("00.m4f"));
    let data = m4f::pack(&[audio()], &[frame()], 180000).unwrap();
    segment
        .feed
        .send(Ok(Bytes::copy_from_slice(&data[..8])))
        .unwrap();
    f.config
        .put("templates", "edit", json!({"static":false}))
        .unwrap();
    let _ = segment.feed.send(Ok(Bytes::copy_from_slice(&data[8..])));
    drop(segment.feed);
    let (result, out) = join(task).await;
    assert!(matches!(result, Err(Failure::Authority(_))));
    assert!(out.is_empty());
    assert!(rx.await.is_err());
    assert_eq!(hub.m4s.retained().0, 0);
    assert_eq!(f.media_calls(), 2);
}

#[tokio::test]
async fn subtitle_preserve_drop_and_hls_conversion_options_reach_both_protocols() {
    for m4f in [false, true] {
        for preserve in [false, true] {
            let mut f = Fixture::start(m4f).await;
            let rows = f.inventory(Duration::from_secs(30)).await;
            let admitted = f.admitted(&rows, "one").await;
            let mut tracks = vec![audio()];
            tracks.extend([7, 8].map(|id| Track {
                id,
                codec: "subtitle".into(),
                config: vec![],
            }));
            let frames = vec![
                frame(),
                Frame {
                    track_id: 7,
                    dts: 100000,
                    pts_offset: 50000,
                    key: true,
                    body: b"AMERICA HELLO".to_vec(),
                },
                Frame {
                    track_id: 8,
                    dts: 100000,
                    pts_offset: 50000,
                    key: true,
                    body: "EUROPE GRÜSSE".as_bytes().to_vec(),
                },
            ];
            let services=serde_json::from_value(json!([{"native_track":7,"language":"en","name":"English"},{"native_track":8,"language":"de","name":"Deutsch"}])).unwrap();
            let conversion = Arc::new(flussonix::caption_hls::State::new(
                flussonix::captions::Decoder::new(services),
                "owned".into(),
                0,
            ));
            let detected = Arc::new(AtomicU64::new(0));
            let hub = Arc::new(Hub::new());
            let (_, mut live) = hub.m4s_subscribe();
            let cancel = Arc::new(CancellationToken::new());
            let owner = f.owner.clone();
            let (h, c, d, v) = (
                hub.clone(),
                cancel.clone(),
                detected.clone(),
                conversion.clone(),
            );
            let (tx, rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                owner
                    .pull_admitted(
                        &admitted,
                        &mut tokio::io::sink(),
                        Some(&h),
                        tx,
                        PrivatePullOptions {
                            preserve_subtitles: preserve,
                            detected_subtitles: Some(&d),
                            conversion: Some(&v),
                            cancel: Some(&c),
                        },
                    )
                    .await
            });
            let control = f.connection().await;
            if m4f {
                control
                    .feed
                    .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-2000\n")))
                    .unwrap();
                let segment = f.connection().await;
                segment
                    .feed
                    .send(Ok(Bytes::from(
                        m4f::pack(&tracks, &frames, 180000).unwrap(),
                    )))
                    .unwrap();
                drop(segment.feed);
            } else {
                let mut bytes = wire::encode_info(&tracks).unwrap();
                for frame in &frames {
                    let track = tracks.iter().find(|t| t.id == frame.track_id).unwrap();
                    bytes.extend(wire::encode_frame(track, frame).unwrap());
                }
                control.feed.send(Ok(Bytes::from(bytes))).unwrap();
            }
            assert_eq!(
                rx.await
                    .unwrap()
                    .iter()
                    .filter(|t| t.codec == "subtitle")
                    .count(),
                2
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if conversion.decoder.lock().unwrap().snapshot().len() == 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(detected.load(Ordering::Relaxed), 2);
            let cues = conversion.decoder.lock().unwrap().snapshot();
            assert_eq!(
                cues.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
                ["AMERICA HELLO", "EUROPE GRÜSSE"]
            );
            // Observe delivered media rather than requiring an unused output
            // to retain history. Both native inputs must apply the same policy.
            let delivered = tokio::time::timeout(Duration::from_secs(2), async {
                let mut decoder = flussonix::m4s::Decoder::default();
                let mut delivered = vec![];
                while delivered.len() < if preserve { 3 } else { 1 } {
                    for event in decoder.push(&live.recv().await.unwrap()).unwrap() {
                        match event {
                            flussonix::m4s::Event::Frame {
                                track_id,
                                dts,
                                pts_offset,
                                key,
                                body,
                                ..
                            } => delivered.push(Frame {
                                track_id,
                                dts,
                                pts_offset,
                                key,
                                body,
                            }),
                            flussonix::m4s::Event::Gop { frames, .. } => delivered.extend(frames),
                            _ => {}
                        }
                    }
                }
                delivered
            })
            .await
            .unwrap();
            let expected: Vec<_> = frames
                .iter()
                .filter(|frame| preserve || frame.track_id == 1)
                .cloned()
                .collect();
            assert_eq!(delivered.len(), expected.len());
            for (actual, expected) in delivered.iter().zip(&expected) {
                assert_eq!(actual.track_id, expected.track_id);
                assert_eq!(actual.dts, expected.dts);
                assert_eq!(actual.pts_offset, expected.pts_offset);
                assert_eq!(actual.key, expected.key);
                assert_eq!(actual.body, expected.body);
            }
            let wire = hub.m4s_subscribe().0;
            let mut decoder = flussonix::m4s::Decoder::default();
            let mut text = 0;
            for bytes in wire {
                for event in decoder.push(&bytes).unwrap() {
                    match event {
                        flussonix::m4s::Event::Info { tracks, .. }
                        | flussonix::m4s::Event::Gop { tracks, .. } => {
                            text = tracks.iter().filter(|t| t.codec == "subtitle").count()
                        }
                        _ => {}
                    }
                }
            }
            assert_eq!(text, if preserve { 2 } else { 0 });
            cancel.cancel();
            assert_eq!(join(task).await, Err(Failure::Cancelled));
        }
    }
}

#[tokio::test]
async fn aborting_owned_pull_drops_its_http_body_without_background_tasks() {
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let admitted = f.admitted(&rows, "one").await;
    let owner = f.owner.clone();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        owner
            .pull_admitted(
                &admitted,
                &mut tokio::io::sink(),
                None,
                tx,
                PrivatePullOptions::default(),
            )
            .await
    });
    let connection = f.connection().await;
    connection.feed.send(Ok(info())).unwrap();
    rx.await.unwrap();
    task.abort();
    assert!(matches!(task.await,Err(e) if e.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(1), connection.feed.closed())
        .await
        .unwrap();
}
