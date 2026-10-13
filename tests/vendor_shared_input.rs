//! Owned private HTTP verifies shared pulls and independent viewer lifetimes.
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
        SharedFailure as Failure, SharedInputOptions, SharedInputs, SharedSubscription,
        SourceHandle,
    },
    wire,
};
use serde_json::json;
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
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
    headers: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    connections: mpsc::UnboundedReceiver<Connection>,
    calls: Arc<Mutex<Vec<String>>>,
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
            headers,
            connections,
            calls,
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
async fn receive(rx: &mut SharedSubscription) -> Bytes {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

// A per-viewer input, or retaining the first grant as an anchor, breaks this test.
#[tokio::test]
async fn one_pull_fans_out_and_revoking_first_viewer_preserves_second() {
    let mut f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let registry = SharedInputs::new(f.owner.clone());
    let a = registry
        .attach(
            f.admitted(&rows, "first").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let first_id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let b = registry
        .attach(
            f.admitted(&rows, "second").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    assert!(b.input.is_none());
    assert_eq!(a.viewer.generation(), b.viewer.generation());
    let (mut ts1, mut ts2) = (
        a.viewer.subscribe_ts().unwrap(),
        b.viewer.subscribe_ts().unwrap(),
    );
    let (mut native1, mut native2) = (
        a.viewer.subscribe_m4s().unwrap(),
        b.viewer.subscribe_m4s().unwrap(),
    );
    let task = tokio::spawn(a.input.unwrap().run());
    let conn = f.connection().await;
    conn.feed.send(Ok(info())).unwrap();
    receive(&mut native1).await;
    receive(&mut native2).await;
    conn.feed.send(Ok(sample())).unwrap();
    let x = receive(&mut ts1).await;
    let y = receive(&mut ts2).await;
    assert_eq!(x, y);
    assert_eq!(x[0], 0x47);
    assert_eq!(x.len() % 188, 0);
    receive(&mut native1).await;
    receive(&mut native2).await;
    assert_eq!(b.viewer.tracks().unwrap().unwrap()[0].codec, "mp3");
    assert_eq!(f.media_calls(), 1);
    assert_eq!(f.auth.live_grants(), 2);
    assert!(f.auth.revoke(&first_id));
    assert!(matches!(native1.recv().await, Err(Failure::Authority(_))));
    drop((ts1, native1, a.viewer));
    assert_eq!(f.auth.live_grants(), 1);
    conn.feed.send(Ok(sample())).unwrap();
    let data = receive(&mut native2).await;
    assert!(matches!(
        flussonix::m4s::Decoder::default().push(&data).unwrap()[0],
        flussonix::m4s::Event::Frame { .. }
    ));
    assert!(!task.is_finished());
    drop((ts2, native2, b.viewer));
    assert_eq!(f.auth.live_grants(), 0);
    assert_eq!(join(task).await, Err(Failure::Closed));
    tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
        .await
        .unwrap();
}

// Missing source fences would permit stale buffered or newly arriving media.
#[tokio::test]
async fn every_source_fence_retires_the_shared_generation() {
    for mode in 0..5 {
        let mut f = Fixture::start(false).await;
        let rows = f
            .inventory(if mode == 3 {
                Duration::from_millis(500)
            } else {
                Duration::from_secs(30)
            })
            .await;
        let registry = SharedInputs::new(f.owner.clone());
        let a = registry
            .attach(
                f.admitted(&rows, "one").await,
                SharedInputOptions::default(),
            )
            .unwrap();
        let mut rx = a.viewer.subscribe_m4s().unwrap();
        let task = tokio::spawn(a.input.unwrap().run());
        let conn = f.connection().await;
        conn.feed.send(Ok(info())).unwrap();
        receive(&mut rx).await;
        match mode {
            0 => {
                f.config
                    .put("templates", "edit", json!({"static":false}))
                    .unwrap();
            }
            1 => {
                f.owner.replace_aliases(&["new-alias"]).unwrap();
            }
            2 => {
                f.inventory(Duration::from_secs(30)).await;
            }
            3 => {
                tokio::time::sleep(Duration::from_millis(550)).await;
            }
            _ => {
                install(&f.owner, &f.key);
            }
        }
        assert!(a.viewer.current().is_err());
        assert!(rx.recv().await.is_err());
        assert!(join(task).await.is_err());
        tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
            .await
            .unwrap();
    }
}

// Name-only retirement would let a stale handle stop a replacement input.
#[tokio::test]
async fn stale_generation_cannot_retire_or_close_its_replacement() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let old = registry
        .attach(
            f.admitted(&rows, "old").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let rows = f.inventory(Duration::from_secs(30)).await;
    let new = registry
        .attach(
            f.admitted(&rows, "new").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    assert_ne!(old.viewer.generation(), new.viewer.generation());
    assert!(!registry.retire(&old.viewer));
    drop(old.input);
    new.viewer.current().unwrap();
    let mut rx = new.viewer.subscribe_m4s().unwrap();
    let task = tokio::spawn(new.input.unwrap().run());
    let conn = f.connection().await;
    conn.feed.send(Ok(info())).unwrap();
    receive(&mut rx).await;
    assert!(registry.retire(&new.viewer));
    assert!(!registry.retire(&new.viewer));
    assert_eq!(join(task).await, Err(Failure::Closed));
}

// Missing handle Drop/shutdown/abort would strand a private HTTP request.
#[tokio::test]
async fn owner_shutdown_unstarted_drop_and_abort_close_only_owned_input() {
    for mode in 0..4 {
        let mut f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let a = registry
            .attach(
                f.admitted(&rows, "one").await,
                SharedInputOptions::default(),
            )
            .unwrap();
        if mode == 0 {
            drop(a.input);
            assert_eq!(a.viewer.current(), Err(Failure::Closed));
            assert_eq!(f.media_calls(), 0);
            continue;
        }
        let mut rx = a.viewer.subscribe_m4s().unwrap();
        let task = tokio::spawn(a.input.unwrap().run());
        let conn = f.connection().await;
        conn.feed.send(Ok(info())).unwrap();
        receive(&mut rx).await;
        if mode == 1 {
            registry.shutdown();
            assert_eq!(join(task).await, Err(Failure::Closed));
        } else if mode == 2 {
            drop(registry);
            assert_eq!(join(task).await, Err(Failure::Closed));
        } else {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(a.viewer.current(), Err(Failure::Closed));
        assert!(rx.recv().await.is_err());
        tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
            .await
            .unwrap();
    }
}

// Joining incompatible options must not replace a valid input; foreign owners fail.
#[tokio::test]
async fn incompatible_and_foreign_attachments_do_not_disturb_valid_generation() {
    let f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let a = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    assert!(matches!(
        registry.attach(
            f.admitted(&rows, "two").await,
            SharedInputOptions {
                preserve_subtitles: true,
                ..Default::default()
            }
        ),
        Err(Failure::Mismatch)
    ));
    a.viewer.current().unwrap();
    assert_eq!(f.auth.live_grants(), 1);
    let foreign = SharedInputs::new(f.owner.clone());
    assert!(!foreign.retire(&a.viewer));
    let other = Fixture::start(false).await;
    assert!(matches!(
        SharedInputs::new(other.owner.clone()).attach(
            f.admitted(&rows, "three").await,
            SharedInputOptions::default()
        ),
        Err(Failure::Authority(_))
    ));
    a.viewer.current().unwrap();
    drop(a.input);
}

// A second M4F attachment must reuse the control and segment fetches, and fence reads.
#[tokio::test]
async fn m4f_control_and_segments_are_shared_and_guarded() {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let a = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let b = registry
        .attach(
            f.admitted(&rows, "two").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let (mut x, mut y) = (
        a.viewer.subscribe_m4f().unwrap(),
        b.viewer.subscribe_m4f().unwrap(),
    );
    let task = tokio::spawn(a.input.unwrap().run());
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-2000\n")))
        .unwrap();
    let segment = f.connection().await;
    assert!(segment.path.ends_with("00.m4f"));
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &[frame()], 180000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    let signal = receive(&mut x).await;
    assert_eq!(signal, receive(&mut y).await);
    let text = std::str::from_utf8(&signal).unwrap();
    let stamp = text
        .split_whitespace()
        .nth(1)
        .unwrap()
        .rsplit_once('-')
        .unwrap()
        .0;
    let name = format!("{stamp}.m4f");
    assert!(a.viewer.segment(&name).unwrap().is_some());
    assert_eq!(f.media_calls(), 2);
    registry.shutdown();
    assert_eq!(join(task).await, Err(Failure::Closed));
    assert!(b.viewer.segment(&name).is_err());
}

// Silently dropping old TS chunks would hide discontinuities from slow clients.
#[tokio::test]
async fn slow_ts_reader_fails_explicitly_without_stalling_native_viewer() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let a = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let mut ts = a.viewer.subscribe_ts().unwrap();
    let mut native = a.viewer.subscribe_m4s().unwrap();
    let task = tokio::spawn(a.input.unwrap().run());
    let conn = f.connection().await;
    conn.feed.send(Ok(info())).unwrap();
    receive(&mut native).await;
    for _ in 0..140 {
        conn.feed.send(Ok(sample())).unwrap();
        receive(&mut native).await;
    }
    assert_eq!(ts.recv().await, Err(Failure::Lagged));
    assert!(!task.is_finished());
    conn.feed.send(Ok(sample())).unwrap();
    receive(&mut native).await;
    registry.shutdown();
    assert_eq!(join(task).await, Err(Failure::Closed));
}

// Startup must re-fence even if attachment was current when registered.
#[tokio::test]
async fn stale_or_revoked_startup_never_opens_private_http() {
    for revoked in [false, true] {
        let f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let a = registry
            .attach(
                f.admitted(&rows, "one").await,
                SharedInputOptions::default(),
            )
            .unwrap();
        if revoked {
            let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
            assert!(f.auth.revoke(&id));
        } else {
            f.config
                .put("templates", "edit", json!({"static":false}))
                .unwrap();
        }
        assert!(a.input.unwrap().run().await.is_err());
        assert_eq!(f.media_calls(), 0);
        assert!(a.viewer.current().is_err());
    }
}

// Revocation during idle headers must release the sole private request promptly.
#[tokio::test]
async fn revoking_all_viewers_drops_pending_headers_without_publishing() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let a = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let b = registry
        .attach(
            f.admitted(&rows, "two").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let (tx, rx) = oneshot::channel();
    *f.headers.lock().unwrap() = Some(rx);
    let mut native = b.viewer.subscribe_m4s().unwrap();
    let task = tokio::spawn(a.input.unwrap().run());
    let conn = f.connection().await;
    for row in f.auth.snapshots() {
        assert!(f.auth.revoke(row["id"].as_str().unwrap()));
    }
    assert!(join(task).await.is_err());
    let _ = tx.send(());
    assert!(native.recv().await.is_err());
    tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
        .await
        .unwrap();
}

// An exhausted upstream input must not be reused as an apparently live worker.
#[tokio::test]
async fn upstream_eof_closes_generation_and_allows_fresh_input() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let a = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    let mut native = a.viewer.subscribe_m4s().unwrap();
    let task = tokio::spawn(a.input.unwrap().run());
    let conn = f.connection().await;
    conn.feed.send(Ok(info())).unwrap();
    receive(&mut native).await;
    drop(conn.feed);
    let _ = join(task).await;
    assert_eq!(a.viewer.current(), Err(Failure::Closed));
    let b = registry
        .attach(
            f.admitted(&rows, "two").await,
            SharedInputOptions::default(),
        )
        .unwrap();
    assert_ne!(a.viewer.generation(), b.viewer.generation());
    assert!(b.input.is_some());
    assert!(!registry.retire(&a.viewer));
    b.viewer.current().unwrap();
    drop(b.input);
}

// Conversion belongs to the worker generation; a different state cannot join it.
#[tokio::test]
async fn conversion_state_identity_is_part_of_worker_compatibility() {
    let f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let state = || {
        Arc::new(flussonix::caption_hls::State::new(
            flussonix::captions::Decoder::new(vec![]),
            "owned".into(),
            0,
        ))
    };
    let opts = SharedInputOptions {
        conversion: Some(state()),
        ..Default::default()
    };
    let a = registry
        .attach(f.admitted(&rows, "one").await, opts.clone())
        .unwrap();
    let b = registry
        .attach(f.admitted(&rows, "two").await, opts)
        .unwrap();
    assert!(b.input.is_none());
    assert!(matches!(
        registry.attach(
            f.admitted(&rows, "three").await,
            SharedInputOptions {
                conversion: Some(state()),
                ..Default::default()
            }
        ),
        Err(Failure::Mismatch)
    ));
    a.viewer.current().unwrap();
    b.viewer.current().unwrap();
    assert_eq!(f.auth.live_grants(), 2);
    drop(a.input);
}
