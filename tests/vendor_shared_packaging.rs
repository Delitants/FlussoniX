//! Owned private HTTP verifies shared pulls and independent viewer lifetimes.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use flussonix::{
    config::ConfigStore,
    m4f::{self, Frame},
    m4s::Track,
    media::{Engine, PackagedPlayback, PackagedSubscription, SharedPackagingFailure as Failure},
    playback_auth::{PlaybackAuth, ViewerRequest},
    source_directory::modern_coordinator::{
        AdmissionOutcome, AdmittedCandidate, CoordinatedInventory, ModernSourceCoordinator,
        SharedInputOptions, SharedInputs, SharedSubscription, SourceHandle,
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
struct InventoryControl {
    status: StatusCode,
    streams: serde_json::Value,
    hold: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}
struct Fixture {
    _dir: tempfile::TempDir,
    config: Arc<ConfigStore>,
    owner: Arc<ModernSourceCoordinator>,
    auth: Arc<PlaybackAuth>,
    handle: SourceHandle,
    headers: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    connections: mpsc::UnboundedReceiver<Connection>,
    calls: Arc<Mutex<Vec<String>>>,
    media_sessions: Arc<Mutex<Vec<String>>>,
    remote: Arc<Mutex<InventoryControl>>,
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
        let media_sessions = Arc::new(Mutex::new(vec![]));
        let headers: Arc<Mutex<Option<oneshot::Receiver<()>>>> = Arc::new(Mutex::new(None));
        let remote = Arc::new(Mutex::new(InventoryControl {
            status: StatusCode::OK,
            streams: json!([{"name":NAME,"groups":[],"named_by":"config","static":true}]),
            hold: None,
        }));
        let control = remote.clone();
        let (tx, connections) = mpsc::unbounded_channel();
        let (paths, held, sessions) = (calls.clone(), headers.clone(), media_sessions.clone());
        let router = Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, header: HeaderMap| {
                let (paths, held, tx, control, sessions) = (
                    paths.clone(),
                    held.clone(),
                    tx.clone(),
                    control.clone(),
                    sessions.clone(),
                );
                async move {
                    paths.lock().unwrap().push(uri.path().to_owned());
                    if uri.path().starts_with("/owned/") || uri.path().starts_with("/owned-alt/") {
                        assert!(header.contains_key("cluster-auth-key"));
                        assert!(!header.contains_key("authorization"));
                        if let Some(session) = header.get("x-playback-session-id") {
                            sessions
                                .lock()
                                .unwrap()
                                .push(session.to_str().unwrap().to_owned());
                        }
                        let (feed, rx) = mpsc::unbounded_channel();
                        tx.send(Connection {
                            path: uri.path().into(),
                            feed,
                        })
                        .unwrap();
                        let pause = held.lock().unwrap().take();
                        if let Some(pause) = pause {
                            let _ = pause.await;
                        }
                        return Response::new(Body::from_stream(
                            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
                        ));
                    }
                    assert!(header.contains_key("authorization"));
                    assert!(!header.contains_key("cluster-auth-key"));
                    let (value, status, pause) = if uri.path().ends_with("/config") {
                        (json!({"stats":{"id":"owned-origin"}}), StatusCode::OK, None)
                    } else {
                        assert_eq!(uri.path(), "/streamer/api/v3/streams");
                        let mut control = control.lock().unwrap();
                        (
                            json!({"server_id":"owned-origin","streams":control.streams}),
                            control.status,
                            control.hold.take(),
                        )
                    };
                    if let Some((started, release)) = pause {
                        let _ = started.send(());
                        let _ = release.await;
                    }
                    Response::builder()
                        .status(status)
                        .body(Body::from(value.to_string()))
                        .unwrap()
                }
            },
        ));
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
            headers,
            connections,
            calls,
            media_sessions,
            remote,
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
            .filter(|p| p.starts_with("/owned/") || p.starts_with("/owned-alt/"))
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
async fn receive(rx: &mut PackagedSubscription) -> Bytes {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

fn options() -> serde_json::Value {
    json!({"flussonix_hls_subtitles":"drop"})
}
fn attach(
    f: &Fixture,
    admitted: Box<AdmittedCandidate>,
    registry: &SharedInputs,
) -> flussonix::source_directory::modern_coordinator::SharedInputAttachment {
    let _ = f;
    registry
        .attach(admitted, SharedInputOptions::default())
        .unwrap()
}
async fn ready(engine: &Engine, playback: &PackagedPlayback) -> Bytes {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Ok(bytes) = engine.read_shared(playback, "index.m3u8").await {
                if String::from_utf8_lossy(&bytes).contains("#EXTINF:") {
                    break bytes;
                }
            }
            assert!(playback.current().is_ok(), "{}", playback.stats());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn feed_audio(feed: Feed) {
    feed.send(Ok(info())).unwrap();
    feed_samples(feed).await;
}
async fn feed_samples(feed: Feed) {
    for i in 0..800 {
        let mut frame = frame();
        frame.dts = 90000 + i * 2351;
        if feed
            .send(Ok(Bytes::from(
                wire::encode_frame(&audio(), &frame).unwrap(),
            )))
            .is_err()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    std::future::pending::<()>().await;
}

// Reopening HTTP/FFmpeg per viewer or retaining the first grant breaks this path.
#[tokio::test]
async fn shared_packager_reuses_one_pull_and_keeps_second_viewer_after_revocation() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let first = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let second = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "two").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let mut first_native = first.subscribe_m4s().unwrap();
    let mut second_native = second.subscribe_m4s().unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    receive(&mut first_native).await;
    receive(&mut second_native).await;
    let manifest = ready(&engine, &second).await;
    let pid = second.pid();
    assert!(pid > 0);
    assert_eq!(first.pid(), pid);
    assert_eq!(engine.count().await, 1);
    assert_eq!(f.media_calls(), 1);
    let file = String::from_utf8_lossy(&manifest)
        .lines()
        .find(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap()
        .to_owned();
    let media = engine.read_shared(&second, &file).await.unwrap();
    assert!(!media.is_empty());
    let decoded = dir.path().join("owned-segment.ts");
    std::fs::write(&decoded, &media).unwrap();
    assert!(f.auth.revoke(&id));
    assert!(first.current().is_err());
    assert!(engine.read_shared(&first, "index.m3u8").await.is_err());
    assert!(first_native.recv().await.is_err());
    drop((first, first_native));
    assert_eq!(f.auth.live_grants(), 1);
    receive(&mut second_native).await;
    second.current().unwrap();
    assert_eq!(second.pid(), pid);
    drop((second, second_native));
    tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
        .await
        .unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert_eq!(f.auth.live_grants(), 0);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    decode(&decoded).await;
}

// A stale admitted source or a caller-provided input must never start HTTP or FFmpeg.
#[tokio::test]
async fn invalid_settings_and_stale_authority_fail_before_private_io() {
    for mode in 0..3 {
        let f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let attachment = attach(&f, f.admitted(&rows, "one").await, &registry);
        let cfg = match mode {
            0 => json!({"inputs":[{"url":"testsrc://"}]}),
            1 => json!({"flussonix_subtitle_tracks":"preserve"}),
            _ => {
                f.config
                    .put("templates", "edit", json!({"static":false}))
                    .unwrap();
                options()
            }
        };
        let result = engine.package_shared(attachment, &cfg).await;
        assert!(if mode < 2 {
            matches!(result, Err(Failure::Invalid))
        } else {
            matches!(result, Err(Failure::Authority(_)))
        });
        engine.stop_all().await;
        assert_eq!(f.media_calls(), 0);
        assert_eq!(f.auth.live_grants(), 0);
    }
}

// A startup owner must cancel and join a source that never supplies metadata.
#[tokio::test]
async fn pending_metadata_is_coalesced_and_engine_stop_joins_the_input() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let first = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let second = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "two").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    assert_eq!(f.media_calls(), 1);
    assert_eq!(first.pid(), 0);
    assert_eq!(second.pid(), 0);
    engine.stop_all().await;
    tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
        .await
        .unwrap();
    assert!(first.current().is_err());
    assert!(second.current().is_err());
    drop((first, second));
    assert_eq!(f.auth.live_grants(), 0);
}

// A mismatching second request may not replace a healthy packager/input generation.
#[tokio::test]
async fn incompatible_packaging_settings_preserve_existing_worker() {
    let f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let first = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let cfg = json!({"transcoder":{"ab":96},"flussonix_hls_subtitles":"drop"});
    assert!(matches!(
        engine
            .package_shared(attach(&f, f.admitted(&rows, "two").await, &registry), &cfg)
            .await,
        Err(Failure::Mismatch)
    ));
    first.current().unwrap();
    assert_eq!(f.auth.live_grants(), 1);
    engine.stop_all().await;
    drop(first);
}

async fn decode(path: &std::path::Path) {
    let log = path.with_extension("decode.log");
    let mut child = tokio::process::Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-nostdin", "-i"])
        .arg(path)
        .args(["-map", "0:a:0", "-f", "null", "-"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), child.wait()).await;
    if result.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    assert!(
        result.is_ok_and(|r| r.is_ok_and(|s| s.success())),
        "{}",
        std::fs::read_to_string(log).unwrap()
    );
}

// CPU transcoding must use the same private pull and produce actual AAC media.
#[tokio::test]
async fn cpu_audio_transcoding_produces_decodable_hls_and_native_aac() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let cfg = json!({"transcoder":{"ab":96},"flussonix_hls_subtitles":"drop"});
    let playback = engine
        .package_shared(attach(&f, f.admitted(&rows, "one").await, &registry), &cfg)
        .await
        .unwrap();
    let mut native = playback.subscribe_m4s().unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    let manifest = ready(&engine, &playback).await;
    let info = receive(&mut native).await;
    let events = flussonix::m4s::Decoder::default().push(&info).unwrap();
    assert!(
        matches!(&events[0],flussonix::m4s::Event::Info{tracks,..}if tracks.iter().any(|t|t.codec=="aac"))
    );
    let name = String::from_utf8_lossy(&manifest)
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap()
        .to_owned();
    let bytes = engine.read_shared(&playback, &name).await.unwrap();
    let file = dir.path().join("aac.ts");
    std::fs::write(&file, &bytes).unwrap();
    assert_eq!(f.media_calls(), 1);
    let pid = playback.pid();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    decode(&file).await;
}

// A source edit must fence cached HLS and an old packager must not close replacement.
#[tokio::test]
async fn refreshed_generation_replaces_packager_and_fences_old_hls_reads() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let first = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &first).await;
    let old_pid = first.pid();
    let rows = f.inventory(Duration::from_secs(30)).await;
    assert!(engine.read_shared(&first, "index.m3u8").await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while first.stats()["last_error"].is_null() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "two").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let next = f.connection().await;
    let next_feeder = tokio::spawn(feed_audio(next.feed.clone()));
    ready(&engine, &second).await;
    assert_ne!(first.generation(), second.generation());
    assert_ne!(old_pid, second.pid());
    drop(first);
    second.current().unwrap();
    assert_eq!(f.media_calls(), 2);
    assert!(!std::path::Path::new(&format!("/proc/{old_pid}")).exists());
    engine.stop_all().await;
    feeder.abort();
    next_feeder.abort();
    let _ = feeder.await;
    let _ = next_feeder.await;
    tokio::time::timeout(Duration::from_secs(2), next.feed.closed())
        .await
        .unwrap();
}

// A source burst exceeding the feed budget must fail explicitly and be reaped.
#[tokio::test]
async fn stalled_packager_backpressures_input_and_is_stopped_by_watchdog() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let blocked = dir.path().join("blocked-packager");
    std::fs::write(&blocked, b"#!/bin/sh\nexec sleep 30\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(dir.path(), blocked.to_str().unwrap());
    let mut cfg = options();
    cfg["flussonix_input_timeout"] = json!(1);
    let playback = engine
        .package_shared(attach(&f, f.admitted(&rows, "one").await, &registry), &cfg)
        .await
        .unwrap();
    let conn = f.connection().await;
    let mut burst = info().to_vec();
    for _ in 0..2000 {
        burst.extend_from_slice(&sample());
    }
    conn.feed.send(Ok(Bytes::from(burst))).unwrap();
    tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
        .await
        .unwrap();
    let pid = playback.pid();
    engine.stop_all().await;
    assert!(playback.current().is_err());
    assert_eq!(playback.stats()["last_error"], "startup_timeout");
    assert!(pid == 0 || !std::path::Path::new(&format!("/proc/{pid}")).exists());
}

// Late headers and process-spawn failure must not orphan a pull or child.
#[tokio::test]
async fn late_headers_and_missing_packager_are_owned_and_joined() {
    for late in [false, true] {
        let mut f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "/owned-no-ffmpeg");
        let (tx, rx) = oneshot::channel();
        if late {
            *f.headers.lock().unwrap() = Some(rx);
        } else {
            drop(rx);
        }
        let playback = engine
            .package_shared(
                attach(&f, f.admitted(&rows, "one").await, &registry),
                &options(),
            )
            .await
            .unwrap();
        let conn = f.connection().await;
        if late {
            engine.stop_all().await;
            let _ = tx.send(());
        } else {
            drop(tx);
            conn.feed.send(Ok(info())).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
            .await
            .unwrap();
        engine.stop_all().await;
        assert!(playback.current().is_err());
        assert_eq!(playback.pid(), 0);
    }
}

// Copying native text keeps original records; transcoding may not silently drop it.
#[tokio::test]
async fn native_text_copy_is_preserved_and_unsupported_transcoding_fails_before_spawn() {
    for transcode in [false, true] {
        let mut f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let mut cfg = options();
        cfg["flussonix_subtitle_tracks"] = json!("preserve");
        if transcode {
            cfg["transcoder"] = json!({"ab":96});
        }
        let admission = f.admitted(&rows, "one").await;
        let attachment = registry
            .attach(
                admission,
                SharedInputOptions {
                    preserve_subtitles: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let playback = engine.package_shared(attachment, &cfg).await.unwrap();
        let mut rx = playback.subscribe_m4s().unwrap();
        let conn = f.connection().await;
        let text = Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        };
        let tracks = vec![audio(), text.clone()];
        conn.feed
            .send(Ok(Bytes::from(wire::encode_info(&tracks).unwrap())))
            .unwrap();
        if transcode {
            tokio::time::timeout(Duration::from_secs(3), conn.feed.closed())
                .await
                .unwrap();
            engine.stop_all().await;
            assert_eq!(playback.pid(), 0);
            assert_eq!(
                playback.stats()["last_error"],
                "native_subtitle_transcode_unsupported"
            );
        } else {
            let bytes = receive(&mut rx).await;
            let mut decoder = flussonix::m4s::Decoder::default();
            assert!(
                matches!(&decoder.push(&bytes).unwrap()[0],flussonix::m4s::Event::Info{tracks,..}if tracks.iter().any(|t|t.codec=="subtitle"))
            );
            let frame = Frame {
                track_id: 7,
                dts: 90000,
                pts_offset: 90000,
                key: true,
                body: "EUROPE GRÜSSE / AMERICA HELLO".as_bytes().to_vec(),
            };
            let encoded = Bytes::from(wire::encode_frame(&text, &frame).unwrap());
            conn.feed.send(Ok(encoded.clone())).unwrap();
            assert_eq!(receive(&mut rx).await, encoded);
            let detected = tokio::time::timeout(Duration::from_secs(1), async {
                while playback.stats()["native_subtitle_tracks"] != 1 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .is_ok();
            engine.stop_all().await;
            assert!(
                detected,
                "source text-track observation was not forwarded to packager stats"
            );
        }
    }
}

// A normal M4F GOP contains more than128 TS writes; startup must drain fairly.
#[tokio::test]
async fn m4f_multitrack_gop_packaging_preserves_native_segment_and_decodes_hls() {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let playback = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let mut signals = playback.subscribe_m4f().unwrap();
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    assert!(segment.path.ends_with("00.m4f"));
    let tracks = vec![
        audio(),
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
    ];
    let mut frames = vec![];
    for i in 0..150 {
        frames.push(Frame {
            track_id: 1,
            dts: 90000 + i * 2351,
            pts_offset: 0,
            key: true,
            body: frame().body,
        });
        frames.push(Frame {
            track_id: 2,
            dts: 90000 + i * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        });
    }
    frames.sort_by_key(|f| f.dts);
    let original = Bytes::from(m4f::pack(&tracks, &frames, 360000).unwrap());
    segment.feed.send(Ok(original.clone())).unwrap();
    drop(segment.feed);
    let manifest = ready(&engine, &playback).await;
    let signal = receive(&mut signals).await;
    assert_eq!(signal, Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n"));
    assert_eq!(
        engine
            .read_shared(&playback, "2026/10/13/00/00/00.m4f")
            .await
            .unwrap(),
        original
    );
    let file = String::from_utf8_lossy(&manifest)
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap()
        .to_owned();
    let media = engine.read_shared(&playback, &file).await.unwrap();
    let decoded = dir.path().join("multi.ts");
    std::fs::write(&decoded, &media).unwrap();
    engine.stop_all().await;
    assert_eq!(f.media_calls(), 2);
    decode(&decoded).await;
}

#[tokio::test]
async fn clean_m4f_eof_keeps_packager_pid_and_reopens_with_fresh_session() {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(20)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let playback = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let mut signals = playback.subscribe_m4f().unwrap();
    let control = f.connection().await;
    assert_eq!(control.path, "/owned/m4f");
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    let frames = (0..150)
        .map(|i| Frame {
            dts: 90_000 + i * 2_351,
            ..frame()
        })
        .collect::<Vec<_>>();
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &frames, 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    ready(&engine, &playback).await;
    assert_eq!(
        receive(&mut signals).await,
        Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")
    );
    let pid = playback.pid();
    assert!(pid > 0);
    drop(control.feed);
    let next = tokio::time::timeout(Duration::from_secs(2), f.connections.recv()).await;
    let Some(reopened) = next.ok().flatten() else {
        engine.stop_all().await;
        panic!("clean M4F EOF did not reopen its owned control session");
    };
    assert_eq!(reopened.path, "/owned/m4f");
    assert_eq!(playback.pid(), pid);
    reopened
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), f.connections.recv())
            .await
            .is_err(),
        "replayed GOP was fetched again"
    );
    reopened
        .feed
        .send(Ok(Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    assert!(segment.path.ends_with("04.m4f"));
    let frames = (150..300)
        .map(|i| Frame {
            dts: 90_000 + i * 2_351,
            ..frame()
        })
        .collect::<Vec<_>>();
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &frames, 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    assert_eq!(
        receive(&mut signals).await,
        Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")
    );
    assert_eq!(playback.pid(), pid);
    assert!(playback.current().is_ok());
    let sessions = f.media_sessions.lock().unwrap().clone();
    assert!(sessions.len() >= 4);
    assert_eq!(sessions[0], sessions[1]);
    assert_eq!(sessions[2], sessions[3]);
    assert_ne!(sessions[0], sessions[2]);
    engine.stop_all().await;
}

#[tokio::test]
async fn rapid_m4f_eofs_retire_old_generation_to_supervision() {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(20)).await;
    let attachment = attach(&f, f.admitted(&rows, "one").await, &registry);
    let viewer = attachment.viewer;
    let mut signals = viewer.subscribe_m4f().unwrap();
    let input = attachment.input.unwrap();
    let task = tokio::spawn(async move { input.run().await });
    let mut control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &[frame()], 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .unwrap()
        .unwrap();
    for _ in 0..4 {
        drop(control.feed);
        control = f.connection().await;
        assert_eq!(control.path, "/owned/m4f");
    }
    drop(control.feed);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(viewer.current().is_err());
    assert_eq!(f.media_calls(), 6);
}

async fn rejected_m4f_timeline_closes_worker(first: &str, next: &str) {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(20)).await;
    let attachment = attach(&f, f.admitted(&rows, "one").await, &registry);
    let viewer = attachment.viewer;
    let mut signals = viewer.subscribe_m4f().unwrap();
    let task = tokio::spawn(async move { attachment.input.unwrap().run().await });
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from(format!(
            "1 2026/10/13/00/00/{first}-4000\n"
        ))))
        .unwrap();
    let segment = f.connection().await;
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &[frame()], 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .unwrap()
        .unwrap();
    drop(control.feed);
    let reopened = f.connection().await;
    reopened
        .feed
        .send(Ok(Bytes::from(format!("2 2026/10/13/00/00/{next}-4000\n"))))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(viewer.current().is_err());
    assert_eq!(f.media_calls(), 3, "invalid timeline fetched media");
}

#[tokio::test]
async fn m4f_replay_gap_retires_worker() {
    rejected_m4f_timeline_closes_worker("00", "12").await;
}

#[tokio::test]
async fn m4f_backward_timeline_retires_worker() {
    rejected_m4f_timeline_closes_worker("08", "04").await;
}

async fn m4f_reopened_track_change(changed_av: bool) {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(20)).await;
    let attachment = attach(&f, f.admitted(&rows, "one").await, &registry);
    let viewer = attachment.viewer;
    let mut signals = viewer.subscribe_m4f().unwrap();
    let task = tokio::spawn(async move { attachment.input.unwrap().run().await });
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &[frame()], 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .unwrap()
        .unwrap();
    drop(control.feed);
    let reopened = f.connection().await;
    reopened
        .feed
        .send(Ok(Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    let mut next_frame = frame();
    next_frame.dts = 450_000;
    let tracks = if changed_av {
        next_frame.body = include_bytes!("fixtures/codecs/mp2.bin").to_vec();
        vec![Track {
            id: 1,
            codec: "m2a".into(),
            config: vec![],
        }]
    } else {
        vec![
            audio(),
            Track {
                id: 7,
                codec: "subtitle".into(),
                config: vec![],
            },
        ]
    };
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&tracks, &[next_frame], 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    if changed_av {
        assert!(
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(viewer.current().is_err());
    } else {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), signals.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")
        );
        viewer.current().unwrap();
        registry.shutdown();
        assert!(task.await.unwrap().is_err());
    }
    assert_eq!(f.media_calls(), 4);
}

#[tokio::test]
async fn m4f_changed_av_metadata_retires_worker() {
    m4f_reopened_track_change(true).await;
}

#[tokio::test]
async fn m4f_sparse_subtitles_continue() {
    m4f_reopened_track_change(false).await;
}

async fn collect_audio_ts(
    subscription: &mut SharedSubscription,
    data: &mut Vec<u8>,
    audio_starts: usize,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let starts = data
                .chunks_exact(188)
                .filter(|packet| {
                    packet[0] == 0x47
                        && (((packet[1] & 0x1f) as u16) << 8 | packet[2] as u16) == 256
                        && packet[1] & 0x40 != 0
                })
                .count();
            if starts >= audio_starts {
                break;
            }
            data.extend_from_slice(&subscription.recv().await.unwrap());
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn m4f_replay_skips_duplicates_before_fetch_and_preserves_ts_continuity() {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(20)).await;
    let attachment = attach(&f, f.admitted(&rows, "one").await, &registry);
    let viewer = attachment.viewer;
    let mut ts = viewer.subscribe_ts().unwrap();
    let task = tokio::spawn(async move { attachment.input.unwrap().run().await });
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    let first = (0..64)
        .map(|i| Frame {
            dts: 90_000 + i * 2_351,
            ..frame()
        })
        .collect::<Vec<_>>();
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &first, 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    let mut output = Vec::new();
    collect_audio_ts(&mut ts, &mut output, 64).await;
    drop(control.feed);
    let reopened = f.connection().await;
    reopened
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), f.connections.recv())
            .await
            .is_err(),
        "replayed GOP caused an upstream fetch"
    );
    reopened
        .feed
        .send(Ok(Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    let second = (64..128)
        .map(|i| Frame {
            dts: 90_000 + i * 2_351,
            ..frame()
        })
        .collect::<Vec<_>>();
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &second, 360_000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    collect_audio_ts(&mut ts, &mut output, 128).await;
    assert_eq!(f.media_calls(), 4);
    let mut previous = None;
    for packet in output.chunks_exact(188) {
        assert_eq!(packet[0], 0x47);
        let pid = ((packet[1] as u16 & 0x1f) << 8) | packet[2] as u16;
        if pid == 256 && packet[3] & 0x10 != 0 {
            let counter = packet[3] & 0x0f;
            if let Some(old) = previous {
                assert_eq!(counter, (old + 1) & 0x0f);
            }
            previous = Some(counter);
        }
    }
    assert!(previous.is_some());
    registry.shutdown();
    assert!(task.await.unwrap().is_err());
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("replayed.ts");
    std::fs::write(&file, output).unwrap();
    decode(&file).await;
}

// Losing the first viewer must not cancel its startup-owner handoff for a peer.
#[tokio::test]
async fn revoked_first_viewer_still_hands_startup_to_other_authorized_attachment() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let first = attach(&f, f.admitted(&rows, "one").await, &registry);
    let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let second = attach(&f, f.admitted(&rows, "two").await, &registry);
    assert!(f.auth.revoke(&id));
    assert!(matches!(
        engine.package_shared(first, &options()).await,
        Err(Failure::Authority(_))
    ));
    let playback = engine.package_shared(second, &options()).await.unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &playback).await;
    assert_eq!(f.auth.live_grants(), 1);
    assert_eq!(f.media_calls(), 1);
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
}

// Conversion must use the source's exact state while native output filters text.
#[tokio::test]
async fn native_text_conversion_produces_authorized_english_and_german_hls() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let services = json!([{"native_track":7,"language":"en","name":"English"},{"native_track":8,"language":"de","name":"Deutsch"}]);
    let state = Arc::new(flussonix::caption_hls::State::new(
        flussonix::captions::Decoder::new(serde_json::from_value(services.clone()).unwrap()),
        "owned-shared".into(),
        0,
    ));
    let cfg = json!({"flussonix_hls_subtitles":"convert","flussonix_hls_captions":services});
    let attachment = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions {
                conversion: Some(state.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    let playback = engine.package_shared(attachment, &cfg).await.unwrap();
    let mut native = playback.subscribe_m4s().unwrap();
    let conn = f.connection().await;
    let tracks = vec![
        audio(),
        Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        },
        Track {
            id: 8,
            codec: "subtitle".into(),
            config: vec![],
        },
    ];
    conn.feed
        .send(Ok(Bytes::from(wire::encode_info(&tracks).unwrap())))
        .unwrap();
    for (id, text) in [(7, "AMERICA HELLO"), (8, "EUROPE GRÜSSE")] {
        let frame = Frame {
            track_id: id,
            dts: 100000,
            pts_offset: 50000,
            key: true,
            body: text.as_bytes().to_vec(),
        };
        conn.feed
            .send(Ok(Bytes::from(
                wire::encode_frame(tracks.iter().find(|t| t.id == id).unwrap(), &frame).unwrap(),
            )))
            .unwrap();
    }
    let feeder = tokio::spawn(feed_samples(conn.feed.clone()));
    let info = receive(&mut native).await;
    let events = flussonix::m4s::Decoder::default().push(&info).unwrap();
    assert!(
        matches!(&events[0],flussonix::m4s::Event::Info{tracks,..}if tracks.len()==1&&tracks[0].codec=="mp3")
    );
    let mut texts = vec![];
    let result = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Ok(master) = engine.read_shared(&playback, "index.m3u8").await {
                if String::from_utf8_lossy(&master).contains("TYPE=SUBTITLES") {
                    break;
                }
            }
            assert!(playback.current().is_ok(), "{}", playback.stats());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for id in [7, 8] {
            let list = engine
                .read_shared(&playback, &format!("nt{id}.m3u8"))
                .await
                .unwrap();
            let mut text = String::new();
            for file in String::from_utf8_lossy(&list)
                .lines()
                .filter(|l| l.ends_with(".vtt"))
            {
                text +=
                    &String::from_utf8_lossy(&engine.read_shared(&playback, file).await.unwrap());
            }
            texts.push(text);
        }
    })
    .await;
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    result.unwrap();
    assert!(texts[0].contains("AMERICA HELLO"), "{}", texts[0]);
    assert!(texts[1].contains("EUROPE GRÜSSE"), "{}", texts[1]);
    assert_eq!(playback.stats()["native_subtitle_tracks"], 2);
    assert_eq!(state.decoder.lock().unwrap().snapshot().len(), 2);
}

// Sparse M4F text can appear after AV startup; preserve may not become silent loss.
#[tokio::test]
async fn late_native_text_rejects_preserve_with_cpu_transcoding() {
    let mut f = Fixture::start(true).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let cfg = json!({"transcoder":{"ab":96},"flussonix_subtitle_tracks":"preserve","flussonix_hls_subtitles":"drop"});
    let attachment = registry
        .attach(
            f.admitted(&rows, "one").await,
            SharedInputOptions {
                preserve_subtitles: true,
                ..Default::default()
            },
        )
        .unwrap();
    let playback = engine.package_shared(attachment, &cfg).await.unwrap();
    let control = f.connection().await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    let mut frames = vec![];
    for i in 0..150 {
        let mut frame = frame();
        frame.dts = 90000 + i * 2351;
        frames.push(frame);
    }
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&[audio()], &frames, 360000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    ready(&engine, &playback).await;
    control
        .feed
        .send(Ok(Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")))
        .unwrap();
    let segment = f.connection().await;
    let tracks = vec![
        audio(),
        Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        },
    ];
    let mut av = frame();
    av.dts = 450000;
    let text = Frame {
        track_id: 7,
        dts: 450000,
        pts_offset: 90000,
        key: true,
        body: b"LATE TEXT".to_vec(),
    };
    segment
        .feed
        .send(Ok(Bytes::from(
            m4f::pack(&tracks, &[av, text], 360000).unwrap(),
        )))
        .unwrap();
    drop(segment.feed);
    let closed = tokio::time::timeout(Duration::from_secs(2), control.feed.closed())
        .await
        .is_ok();
    engine.stop_all().await;
    assert!(
        closed,
        "late native text was silently omitted by the transcoding pipeline"
    );
    assert_eq!(
        playback.stats()["last_error"],
        "native_subtitle_transcode_unsupported"
    );
}

#[tokio::test]
async fn invalid_timeout_values_are_rejected_before_startup() {
    for value in [
        json!(null),
        json!(0),
        json!(301),
        json!(-1),
        json!(1.5),
        json!("15"),
    ] {
        let f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let mut cfg = options();
        cfg["flussonix_input_timeout"] = value;
        let result = engine
            .package_shared(attach(&f, f.admitted(&rows, "one").await, &registry), &cfg)
            .await;
        engine.stop_all().await;
        assert!(matches!(result, Err(Failure::Invalid)));
        assert_eq!(f.media_calls(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_required_sender_keeps_specific_unsupported_text_failure() {
    for _ in 0..32 {
        let mut f = Fixture::start(false).await;
        let registry = SharedInputs::new(f.owner.clone());
        let rows = f.inventory(Duration::from_secs(30)).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let cfg = json!({"transcoder":{"ab":96},"flussonix_subtitle_tracks":"preserve","flussonix_hls_subtitles":"drop"});
        let attachment = registry
            .attach(
                f.admitted(&rows, "one").await,
                SharedInputOptions {
                    preserve_subtitles: true,
                    ..Default::default()
                },
            )
            .unwrap();
        let playback = engine.package_shared(attachment, &cfg).await.unwrap();
        let conn = f.connection().await;
        let tracks = vec![
            audio(),
            Track {
                id: 7,
                codec: "subtitle".into(),
                config: vec![],
            },
        ];
        let mut burst = wire::encode_info(&tracks).unwrap();
        for _ in 0..300 {
            burst.extend_from_slice(&sample());
        }
        conn.feed.send(Ok(Bytes::from(burst))).unwrap();
        tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
            .await
            .unwrap();
        engine.stop_all().await;
        assert_eq!(
            playback.stats()["last_error"],
            "native_subtitle_transcode_unsupported"
        );
    }
}

// Bytes already in the kernel pipe may drain; a pending write may not add more.
#[tokio::test]
async fn final_child_stdin_does_not_resume_queued_bytes_after_source_edit() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("owned-pipe-reader");
    let marker = dir.path().join("full");
    let gate = dir.path().join("gate");
    let captured = dir.path().join("captured");
    let source = format!(
        "#!/usr/bin/python3\nimport os,fcntl,termios,array,time\nfrom pathlib import Path\nmarker=Path({:?});gate=Path({:?});captured=Path({:?})\ncapacity=fcntl.fcntl(0,fcntl.F_GETPIPE_SZ)\nwhile True:\n n=array.array('i',[0]);fcntl.ioctl(0,termios.FIONREAD,n,True)\n if n[0]>capacity//2:\n  time.sleep(.02)\n  previous=n[0];fcntl.ioctl(0,termios.FIONREAD,n,True)\n  if n[0]==previous:break\n time.sleep(.001)\nready=marker.with_suffix('.tmp')\nready.write_text(str(n[0]))\nos.replace(ready,marker)\nwhile not gate.exists():time.sleep(.001)\nwith captured.open('wb',buffering=0) as out:\n while True:\n  data=os.read(0,65536)\n  if not data:break\n  out.write(data)\n",
        marker.to_str().unwrap(),
        gate.to_str().unwrap(),
        captured.to_str().unwrap()
    );
    std::fs::write(&script, source).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(dir.path(), script.to_str().unwrap());
    let playback = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let mut burst = info().to_vec();
    for _ in 0..2000 {
        burst.extend_from_slice(&sample());
    }
    conn.feed.send(Ok(Bytes::from(burst))).unwrap();
    let filled = tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    if filled.is_err() {
        engine.stop_all().await;
        panic!("owned child pipe did not fill");
    }
    let committed = match std::fs::read_to_string(&marker).unwrap().parse::<usize>() {
        Ok(committed) => committed,
        Err(error) => {
            engine.stop_all().await;
            panic!("incomplete owned pipe readiness: {error}");
        }
    };
    f.config
        .put("templates", "edit", json!({"static":false}))
        .unwrap();
    std::fs::write(&gate, b"go").unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), conn.feed.closed()).await;
    engine.stop_all().await;
    closed.unwrap();
    let received = std::fs::metadata(&captured)
        .map(|m| m.len() as usize)
        .unwrap_or(0);
    assert!(
        received <= committed,
        "pending stdin write resumed stale media: {received} > {committed}"
    );
    assert!(!std::path::Path::new(&format!("/proc/{}", playback.pid())).exists());
}

// An explicit lease refresh must not restart the private connection or FFmpeg.
#[tokio::test]
async fn equivalent_lease_renewal_keeps_live_packager_and_hls_after_old_deadline() {
    let mut f = Fixture::start(false).await;
    let registry = SharedInputs::new(f.owner.clone());
    let rows = f.inventory(Duration::from_secs(2)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let playback = engine
        .package_shared(
            attach(&f, f.admitted(&rows, "one").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &playback).await;
    let pid = playback.pid();
    let generation = playback.generation();
    let fresh = registry
        .renew(&f.handle, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!((fresh.renewed, fresh.retired), (1, 0));
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        rows.expires_at() + Duration::from_millis(50),
    ))
    .await;
    playback.current().unwrap();
    assert!(
        !engine
            .read_shared(&playback, "index.m3u8")
            .await
            .unwrap()
            .is_empty()
    );
    let second = engine
        .package_shared(
            attach(&f, f.admitted(&fresh.inventory, "two").await, &registry),
            &options(),
        )
        .await
        .unwrap();
    assert_eq!(second.pid(), pid);
    assert_eq!(second.generation(), generation);
    assert_eq!(f.media_calls(), 1);
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

async fn session_owner(
    registry: Arc<SharedInputs>,
    options: flussonix::media::SharedHlsOptions,
) -> (
    Arc<flussonix::media::SharedHlsSessions>,
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let sessions = Arc::new(flussonix::media::SharedHlsSessions::new(registry, options).unwrap());
    let cancel = tokio_util::sync::CancellationToken::new();
    let (s, c) = (sessions.clone(), cancel.clone());
    let task = tokio::spawn(async move {
        s.run(c).await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !sessions.running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (sessions, cancel, task)
}

#[tokio::test]
async fn hls_session_retention_groups_actual_admissions_and_keeps_one_pull_between_requests() {
    use flussonix::media::SharedHlsOptions;
    let mut f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let (sessions, cancel, task) = session_owner(registry, SharedHlsOptions::default()).await;
    let first = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &first).await;
    let (pid, generation) = (first.pid(), first.generation());
    drop(first);
    assert_eq!(sessions.retained_sessions(), 1);
    assert_eq!(f.auth.live_grants(), 1);
    let second = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    assert_eq!((second.pid(), second.generation()), (pid, generation));
    assert_eq!(sessions.retained_sessions(), 1);
    assert_eq!(f.auth.live_grants(), 1);
    assert!(
        !engine
            .read_shared(&second, "index.m3u8")
            .await
            .unwrap()
            .is_empty()
    );
    drop(second);
    assert_eq!(f.media_calls(), 1);
    cancel.cancel();
    task.await.unwrap();
    assert_eq!(sessions.retained_sessions(), 0);
    assert_eq!(f.auth.live_grants(), 0);
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn hls_idle_expiry_closes_unobserved_input_without_a_followup_request() {
    use flussonix::media::SharedHlsOptions;
    let mut f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let (sessions, cancel, task) = session_owner(
        registry,
        SharedHlsOptions {
            idle_timeout: Duration::from_millis(500),
            maintenance_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let playback = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    assert_eq!(sessions.retained_sessions(), 1);
    drop(playback);
    tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
        .await
        .unwrap();
    assert_eq!(sessions.retained_sessions(), 0);
    assert_eq!(f.auth.live_grants(), 0);
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
}

#[tokio::test]
async fn hls_capacity_mismatch_and_revocation_preserve_the_other_session() {
    use flussonix::media::{HlsSessionFailure, SharedHlsOptions};
    let mut f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let (sessions, cancel, task) = session_owner(
        registry,
        SharedHlsOptions {
            capacity: 1,
            maintenance_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let first = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &first).await;
    let denied = sessions
        .package(
            &engine,
            f.admitted(&rows, "two").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await;
    assert!(matches!(denied, Err(HlsSessionFailure::Limit)));
    first.current().unwrap();
    let mismatch = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &json!({"transcoder":{"ab":96},"flussonix_hls_subtitles":"drop"}),
        )
        .await;
    assert!(matches!(
        mismatch,
        Err(HlsSessionFailure::Packaging(Failure::Mismatch))
    ));
    assert_eq!(sessions.retained_sessions(), 1);
    assert!(f.auth.revoke(&id));
    drop(first);
    tokio::time::timeout(Duration::from_secs(2), async {
        while sessions.retained_sessions() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
}

#[tokio::test]
async fn hls_rejects_stopped_foreign_stale_and_non_hls_admissions_before_private_io() {
    use flussonix::media::{HlsSessionFailure, SharedHlsOptions, SharedHlsSessions};
    let f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let stopped = SharedHlsSessions::new(registry.clone(), SharedHlsOptions::default()).unwrap();
    assert!(matches!(
        stopped
            .package(
                &engine,
                f.admitted(&rows, "one").await,
                SharedInputOptions::default(),
                &options()
            )
            .await,
        Err(HlsSessionFailure::Closed)
    ));
    let (sessions, cancel, task) = session_owner(registry, SharedHlsOptions::default()).await;
    let AdmissionOutcome::Allowed(non_hls) = f
        .owner
        .admit(
            rows.candidate(NAME).unwrap(),
            &f.auth,
            ViewerRequest {
                name: NAME.into(),
                proto: "m4s".into(),
                ip: "127.0.0.1".into(),
                token: "other".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("denied")
    };
    assert!(matches!(
        sessions
            .package(&engine, non_hls, SharedInputOptions::default(), &options())
            .await,
        Err(HlsSessionFailure::Invalid)
    ));
    let other = Fixture::start(false).await;
    let other_rows = other.inventory(Duration::from_secs(30)).await;
    assert!(
        sessions
            .package(
                &engine,
                other.admitted(&other_rows, "foreign").await,
                SharedInputOptions::default(),
                &options()
            )
            .await
            .is_err()
    );
    let stale = f.admitted(&rows, "old").await;
    f.config
        .put("templates", "edit", json!({"static":false}))
        .unwrap();
    assert!(
        sessions
            .package(&engine, stale, SharedInputOptions::default(), &options())
            .await
            .is_err()
    );
    assert_eq!(f.media_calls(), 0);
    assert_eq!(other.media_calls(), 0);
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
}

#[tokio::test]
async fn hls_blocked_startup_reserves_capacity_and_shutdown_cancels_it_before_private_io() {
    use flussonix::media::{HlsSessionFailure, SharedHlsOptions};
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("owned-probe");
    let marker = dir.path().join("probe");
    std::fs::write(&script,format!("#!/usr/bin/python3\nimport os,sys,time\nfrom pathlib import Path\nif any('nvenc' in a for a in sys.argv):\n ready=Path({:?})\n pending=ready.with_suffix('.pending')\n pending.write_text(str(os.getpid()))\n pending.replace(ready)\n time.sleep(30)\nelse:os.execv('/usr/bin/ffmpeg',['ffmpeg']+sys.argv[1:])\n",marker.to_str().unwrap())).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(dir.path(), script.to_str().unwrap());
    let (sessions, cancel, task) = session_owner(
        registry,
        SharedHlsOptions {
            capacity: 1,
            ..Default::default()
        },
    )
    .await;
    let first = f.admitted(&rows, "one").await;
    let cfg =
        json!({"transcoder":{"encoder":"h264_nvenc","vb":300},"flussonix_hls_subtitles":"drop"});
    let package = sessions.package(&engine, first, SharedInputOptions::default(), &cfg);
    tokio::pin!(package);
    tokio::select! {result=&mut package=>panic!("startup should await owned probe: {}",result.is_ok()),_=async{while !marker.exists(){tokio::time::sleep(Duration::from_millis(5)).await;}}=>{}}
    let pid = std::fs::read_to_string(&marker)
        .unwrap()
        .parse::<u32>()
        .unwrap();
    assert!(matches!(
        sessions
            .package(
                &engine,
                f.admitted(&rows, "two").await,
                SharedInputOptions::default(),
                &options()
            )
            .await,
        Err(HlsSessionFailure::Limit)
    ));
    sessions.shutdown();
    assert!(matches!(package.await, Err(HlsSessionFailure::Closed)));
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(sessions.retained_sessions(), 0);
    assert_eq!(f.media_calls(), 0);
}

#[tokio::test]
async fn hls_scheduled_renewal_keeps_two_sessions_and_accepts_fresh_requests_past_old_deadline() {
    use flussonix::media::SharedHlsOptions;
    let mut f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(1)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let (sessions, cancel, task) = session_owner(
        registry.clone(),
        SharedHlsOptions {
            idle_timeout: Duration::from_secs(5),
            lease_lifetime: Duration::from_secs(3),
            renew_margin: Duration::from_millis(400),
            maintenance_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let a = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let first_id = f.auth.snapshots()[0]["id"].as_str().unwrap().to_owned();
    let b = sessions
        .package(
            &engine,
            f.admitted(&rows, "two").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &a).await;
    let (pid, generation) = (a.pid(), a.generation());
    drop((a, b));
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        rows.expires_at() + Duration::from_millis(50),
    ))
    .await;
    let candidate = registry.current_candidate(NAME).unwrap();
    let AdmissionOutcome::Allowed(next) = f
        .owner
        .admit(
            candidate,
            &f.auth,
            ViewerRequest {
                name: NAME.into(),
                proto: "hls".into(),
                ip: "127.0.0.1".into(),
                token: "two".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("denied")
    };
    let c = sessions
        .package(&engine, next, SharedInputOptions::default(), &options())
        .await
        .unwrap();
    assert_eq!((c.pid(), c.generation()), (pid, generation));
    assert!(
        !engine
            .read_shared(&c, "index.m3u8")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(sessions.retained_sessions(), 2);
    assert_eq!(f.media_calls(), 1);
    let polls = f
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.as_str() == "/streamer/api/v3/streams")
        .count();
    assert_eq!(polls, 2, "one scheduled refresh per source, not per viewer");
    assert!(f.auth.revoke(&first_id));
    tokio::time::timeout(Duration::from_secs(2), async {
        while sessions.retained_sessions() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    c.current().unwrap();
    drop(c);
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
}

#[tokio::test]
async fn hls_failed_scheduled_refresh_never_extends_expired_authority() {
    use flussonix::media::SharedHlsOptions;
    let mut f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_millis(600)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let (sessions, cancel, task) = session_owner(
        registry.clone(),
        SharedHlsOptions {
            lease_lifetime: Duration::from_secs(3),
            renew_margin: Duration::from_millis(300),
            retry_delay: Duration::from_millis(50),
            maintenance_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let playback = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    f.remote.lock().unwrap().status = StatusCode::FORBIDDEN;
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        rows.expires_at() + Duration::from_millis(50),
    ))
    .await;
    assert!(playback.current().is_err());
    assert!(registry.current_candidate(NAME).is_err());
    assert!(engine.read_shared(&playback, "index.m3u8").await.is_err());
    drop(playback);
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
}

#[tokio::test]
async fn hls_idle_expiry_and_abort_cancel_held_renewal_without_a_late_request() {
    use flussonix::media::SharedHlsOptions;
    for abort in [false, true] {
        let mut f = Fixture::start(false).await;
        let registry = Arc::new(SharedInputs::new(f.owner.clone()));
        let rows = f.inventory(Duration::from_secs(3)).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let (sessions, cancel, task) = session_owner(
            registry,
            SharedHlsOptions {
                idle_timeout: Duration::from_millis(500),
                lease_lifetime: Duration::from_secs(3),
                renew_margin: Duration::from_millis(2800),
                maintenance_interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;
        let (started_tx, started) = oneshot::channel();
        let (release, held) = oneshot::channel();
        f.remote.lock().unwrap().hold = Some((started_tx, held));
        let playback = sessions
            .package(
                &engine,
                f.admitted(&rows, "one").await,
                SharedInputOptions::default(),
                &options(),
            )
            .await
            .unwrap();
        let conn = f.connection().await;
        let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
        drop(playback);
        tokio::time::timeout(Duration::from_secs(2), started)
            .await
            .unwrap()
            .unwrap();
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            tokio::time::timeout(Duration::from_secs(2), async {
                while sessions.retained_sessions() != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            cancel.cancel();
            task.await.unwrap();
        }
        assert_eq!(sessions.retained_sessions(), 0);
        assert!(!sessions.running());
        tokio::time::timeout(Duration::from_secs(2), conn.feed.closed())
            .await
            .unwrap();
        let before = f.calls.lock().unwrap().len();
        let _ = release.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            f.calls.lock().unwrap().len(),
            before,
            "cancelled refresh resumed after owner left"
        );
        engine.stop_all().await;
        feeder.abort();
        let _ = feeder.await;
    }
}

#[tokio::test]
async fn hls_owner_options_and_unique_run_are_bounded() {
    use flussonix::media::{HlsSessionFailure, SharedHlsOptions, SharedHlsSessions};
    let f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    for which in 0..9 {
        let mut options = SharedHlsOptions::default();
        match which {
            0 => options.capacity = 0,
            1 => options.capacity = 20001,
            2 => options.idle_timeout = Duration::ZERO,
            3 => options.idle_timeout = Duration::from_secs(31),
            4 => options.lease_lifetime = Duration::ZERO,
            5 => options.lease_lifetime = Duration::from_secs(31),
            6 => options.renew_margin = options.lease_lifetime,
            7 => options.retry_delay = Duration::ZERO,
            _ => options.maintenance_interval = Duration::ZERO,
        };
        assert!(matches!(
            SharedHlsSessions::new(registry.clone(), options),
            Err(HlsSessionFailure::Invalid)
        ));
    }
    let (sessions, cancel, task) = session_owner(registry, SharedHlsOptions::default()).await;
    assert_eq!(
        sessions
            .clone()
            .run(tokio_util::sync::CancellationToken::new())
            .await,
        Err(HlsSessionFailure::AlreadyRunning)
    );
    assert!(sessions.running());
    sessions.shutdown();
    use futures_util::FutureExt;
    assert_eq!(
        sessions
            .clone()
            .run(tokio_util::sync::CancellationToken::new())
            .now_or_never(),
        Some(Err(HlsSessionFailure::AlreadyRunning)),
        "cancelled owner must be joined before replacement"
    );
    cancel.cancel();
    task.await.unwrap();
    assert!(!sessions.running());
    let next = tokio_util::sync::CancellationToken::new();
    let (s, c) = (sessions.clone(), next.clone());
    let task = tokio::spawn(async move {
        s.run(c).await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !sessions.running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    next.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn hls_cache_expiry_does_not_stop_renewal_for_an_active_response_handle() {
    use flussonix::media::SharedHlsOptions;
    let mut f = Fixture::start(false).await;
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(1)).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let (sessions, cancel, task) = session_owner(
        registry,
        SharedHlsOptions {
            idle_timeout: Duration::from_millis(200),
            lease_lifetime: Duration::from_secs(3),
            renew_margin: Duration::from_millis(400),
            maintenance_interval: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .await;
    let playback = sessions
        .package(
            &engine,
            f.admitted(&rows, "one").await,
            SharedInputOptions::default(),
            &options(),
        )
        .await
        .unwrap();
    let conn = f.connection().await;
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        rows.expires_at() + Duration::from_millis(50),
    ))
    .await;
    assert_eq!(sessions.retained_sessions(), 0);
    playback
        .current()
        .expect("an active response still owns authorized demand");
    assert!(
        !engine
            .read_shared(&playback, "index.m3u8")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(f.media_calls(), 1);
    drop(playback);
    cancel.cancel();
    task.await.unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
}

#[tokio::test]
async fn hls_old_pending_reservation_cannot_remove_a_new_epoch_session() {
    use flussonix::media::{HlsSessionFailure, SharedHlsOptions};
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::start(false).await;
    f.remote.lock().unwrap().streams = json!([{"name":NAME,"groups":[],"named_by":"config","static":true},{"name":"owned-alt","groups":[],"named_by":"config","static":true}]);
    let registry = Arc::new(SharedInputs::new(f.owner.clone()));
    let rows = f.inventory(Duration::from_secs(30)).await;
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("owned-probe");
    let marker = dir.path().join("probe");
    std::fs::write(&script,format!("#!/usr/bin/python3\nimport os,sys,time\nfrom pathlib import Path\nif any('nvenc' in a for a in sys.argv):\n ready=Path({:?})\n pending=ready.with_suffix('.pending')\n pending.write_text(str(os.getpid()))\n pending.replace(ready)\n time.sleep(30)\nelse:os.execv('/usr/bin/ffmpeg',['ffmpeg']+sys.argv[1:])\n",marker.to_str().unwrap())).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(dir.path(), script.to_str().unwrap());
    let (sessions, cancel, task) = session_owner(
        registry,
        SharedHlsOptions {
            capacity: 1,
            ..Default::default()
        },
    )
    .await;
    let first = f.admitted(&rows, "old").await;
    let cfg =
        json!({"transcoder":{"encoder":"h264_nvenc","vb":300},"flussonix_hls_subtitles":"drop"});
    let old = sessions.package(&engine, first, SharedInputOptions::default(), &cfg);
    tokio::pin!(old);
    tokio::select! {r=&mut old=>panic!("probe should block: {}",r.is_ok()),_=async{while !marker.exists(){tokio::time::sleep(Duration::from_millis(5)).await;}}=>{}}
    let probe = std::fs::read_to_string(&marker)
        .unwrap()
        .parse::<u32>()
        .unwrap();
    sessions.shutdown();
    cancel.cancel();
    task.await.unwrap();
    let next_cancel = tokio_util::sync::CancellationToken::new();
    let (s, c) = (sessions.clone(), next_cancel.clone());
    let next_task = tokio::spawn(async move {
        s.run(c).await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !sessions.running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let AdmissionOutcome::Allowed(next) = f
        .owner
        .admit(
            rows.candidate("owned-alt").unwrap(),
            &f.auth,
            ViewerRequest {
                name: "owned-alt".into(),
                proto: "hls".into(),
                ip: "127.0.0.1".into(),
                token: "fresh".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("denied")
    };
    let fresh = sessions
        .package(&engine, next, SharedInputOptions::default(), &options())
        .await
        .unwrap();
    let conn = f.connection().await;
    assert!(conn.path.starts_with("/owned-alt/"));
    let feeder = tokio::spawn(feed_audio(conn.feed.clone()));
    ready(&engine, &fresh).await;
    assert!(matches!(old.await, Err(HlsSessionFailure::Closed)));
    fresh.current().unwrap();
    assert_eq!(sessions.retained_sessions(), 1);
    assert_eq!(f.auth.live_grants(), 1);
    assert_eq!(f.media_calls(), 1);
    assert!(matches!(
        sessions
            .package(
                &engine,
                f.admitted(&rows, "third").await,
                SharedInputOptions::default(),
                &options()
            )
            .await,
        Err(HlsSessionFailure::Limit)
    ));
    drop(fresh);
    next_cancel.cancel();
    next_task.await.unwrap();
    engine.stop_all().await;
    feeder.abort();
    let _ = feeder.await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::path::Path::new(&format!("/proc/{probe}")).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[path = "vendor_shared_packaging/activation.rs"]
mod activation;
