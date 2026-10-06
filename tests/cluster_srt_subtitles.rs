//! Source -> private MPEG-TS pull -> CDN -> independent encrypted SRT receiver.
//! All configuration, listeners, secrets, fixtures and processes belong to these tests.
#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod captions;
#[allow(dead_code)]
#[path = "support/srt_subtitle_oracle.rs"]
mod oracle;

use flussonix::{
    server::{App, Options, router},
    srt_playback::{self, Listener, Settings},
};
use oracle::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

const PEER: &str = "owned-cluster-caption-peer";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct Requests {
    discovery: AtomicUsize,
    media: AtomicUsize,
    authorized_media: AtomicUsize,
}
async fn observe(
    axum::extract::State(counts): axum::extract::State<Arc<Requests>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if request.uri().path() == "/flussonix/api/v1/stream/owned" {
        counts.discovery.fetch_add(1, Ordering::Relaxed);
    }
    if request.uri().path() == "/owned/mpegts" {
        counts.media.fetch_add(1, Ordering::Relaxed);
        if request
            .headers()
            .get("X-Flussonix-Peer")
            .and_then(|v| v.to_str().ok())
            == Some(PEER)
            && request.uri().query().is_none()
        {
            counts.authorized_media.fetch_add(1, Ordering::Relaxed);
        }
    }
    next.run(request).await
}
struct Node {
    app: Arc<App>,
    url: String,
    cancel: CancellationToken,
    http: tokio::task::JoinHandle<()>,
    requests: Arc<Requests>,
}
impl Node {
    async fn start(dir: &Path, name: &str, role: &str) -> Self {
        let app = App::new(
            dir.join(format!("{name}.json")),
            dir.join(name),
            Options {
                node_name: name.into(),
                role: role.into(),
                uplink_interface: "process".into(),
                admin_password: "owned-cluster-caption-admin".into(),
                peer_key: PEER.into(),
                ..Default::default()
            },
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let a = app.clone();
        let requests = Arc::new(Requests::default());
        let routes = router(a).layer(axum::middleware::from_fn_with_state(
            requests.clone(),
            observe,
        ));
        let http = tokio::spawn(async move {
            axum::serve(
                listener,
                routes.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(c.cancelled_owned())
            .await
            .unwrap();
        });
        Self {
            app,
            url,
            cancel,
            http,
            requests,
        }
    }
    async fn stop(mut self) {
        self.cancel.cancel();
        self.app.media.stop_all().await;
        tokio::time::timeout(Duration::from_secs(3), &mut self.http)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(self.app.media.count().await, 0);
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn config(digital: bool, hls: &str, keep: bool) -> Value {
    let mut cfg = json!({"static":false,"inputs":[{"url":"publish://"}],
        "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),
        "flussonix_subtitle_tracks":if keep {"preserve"}else{"drop"},"flussonix_hls_subtitles":hls});
    if hls == "convert" {
        cfg["flussonix_hls_captions"] = if digital {
            json!([{"service":1,"language":"en","name":"English"}])
        } else {
            json!([{"channel":1,"language":"en","name":"English"}])
        };
    }
    cfg
}
async fn cluster(dir: &Path, cfg: Value) -> (Node, Node) {
    let source = Node::start(dir, "source", "source").await;
    source.app.config.put("streams", "owned", cfg).unwrap();
    let cdn = Node::start(dir, "cdn", "cdn").await;
    cdn.app.config.put("sources", "origin", json!({"api_url":source.url,"private_payload_url":source.url,"flussonix_transport":"mpegts"})).unwrap();
    // No local CDN stream or input: real peer discovery must supply the policy.
    assert!(cdn.app.config.effective("owned").is_none());
    (source, cdn)
}
fn listener() -> (Listener, String) {
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        Settings::new(120, 2, SECRET.into()).unwrap(),
    )
    .unwrap();
    let endpoint = format!(
        "srt://{}?mode=caller&latency=120000&connect_timeout=2000&timeout=10000000&enforced_encryption=1",
        listener.address()
    );
    (listener, endpoint)
}

async fn case(
    digital: bool,
    hevc: bool,
    input: &[u8],
    expected: &BTreeSet<Vec<u8>>,
    hls: &str,
    keep: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(digital, hls, keep);
    let (source, cdn) = cluster(dir.path(), cfg.clone()).await;
    let (listen, endpoint) = listener();
    let cancel = CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    let serving = tokio::spawn(srt_playback::serve(listen, cdn.app.clone(), cancel.clone()));
    let mut publication = source
        .app
        .media
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let path = dir.path().join("received.ts");
    let mut receiver = capture(&endpoint, false, &path).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while publication.worker.viewers.load(Ordering::Relaxed) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("CDN must attach one private source subscription before feeding");
    let feed = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(captions::paced(
        publication.stdin.take().unwrap(),
        input.to_vec(),
    )));
    tokio::time::sleep(Duration::from_secs(12)).await;
    receiver
        .stdin
        .take()
        .unwrap()
        .write_all(b"q\n")
        .await
        .unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(3), receiver.wait()).await;
    let words = if hls == "convert" {
        Some(converted_words(&cdn.app, if digital { "s1.m3u8" } else { "cc1.m3u8" }).await)
    } else {
        None
    };
    assert_eq!(source.app.media.count().await, 1);
    assert_eq!(cdn.app.media.count().await, 1);
    assert!(source.requests.discovery.load(Ordering::Relaxed) > 0);
    assert_eq!(source.requests.media.load(Ordering::Relaxed), 1);
    assert_eq!(source.requests.authorized_media.load(Ordering::Relaxed), 1);
    assert_eq!(
        source.app.playback_auth.active(),
        0,
        "peer subscription must not become a viewer grant"
    );
    feed.abort();
    let _ = feed.await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(cdn.app.media.stats("owned").await["online_clients"], 0);
    cdn.app.media.stop_all().await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while publication.worker.viewers.load(Ordering::Relaxed) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("CDN shutdown must release its source subscription");
    source.app.media.stop_all().await;
    assert!(
        outcome.unwrap().unwrap().success(),
        "{}",
        std::fs::read_to_string(path.with_extension("log")).unwrap()
    );
    let raw = std::fs::read(&path).unwrap();
    verify_original_tracks(&raw, keep);
    verify_codecs(&path, hevc).await;
    let actual = caption_bodies(&path, hevc).await;
    assert!(
        expected.is_subset(&actual),
        "CDN SRT must retain every distinct authored caption body; expected {}, got {}",
        expected.len(),
        actual.len()
    );
    if let Some(words) = words {
        assert!(words.contains(if digital { "USA708" } else { "USA 608" }));
    }
    let decode = dir.path().join("complete-pes.ts");
    std::fs::write(&decode, complete_sample(&raw)).unwrap();
    let decoded = strict_decode(&decode).await;
    assert!(
        clean_decode(&decoded),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    cdn.stop().await;
    source.stop().await;
    println!(
        "qualified encrypted source/CDN/SRT {} CEA-{} with DVB/teletext: HLS {hls}, originals {}",
        if hevc { "HEVC" } else { "H.264" },
        if digital { 708 } else { 608 },
        if keep { "kept" } else { "filtered" }
    );
}
async fn regional(hevc: bool) {
    let _lock = SERIAL.lock().await;
    for digital in [false, true] {
        let input = original::inject(&if hevc {
            captions::hevc_transport(digital)
        } else if digital {
            captions::digital_transport()
        } else {
            captions::transport()
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.ts");
        std::fs::write(&path, &input).unwrap();
        verify_codecs(&path, hevc).await;
        let expected = caption_bodies(&path, hevc).await;
        assert!(expected.len() >= 4);
        let kinds: BTreeSet<_> = expected
            .iter()
            .flat_map(|b| b[10..b.len() - 1].chunks_exact(3).map(|t| t[0] & 3))
            .collect();
        assert_eq!(
            kinds,
            if digital {
                BTreeSet::from([2, 3])
            } else {
                BTreeSet::from([0])
            }
        );
        verify_original_tracks(&input, true);
        for (hls, keep) in [("convert", true), ("drop", true), ("drop", false)] {
            case(digital, hevc, &input, &expected, hls, keep).await;
        }
    }
}
#[tokio::test]
async fn encrypted_cdn_srt_preserves_avc_regional_subtitles_and_source_policy() {
    regional(false).await;
}
#[tokio::test]
async fn encrypted_cdn_srt_preserves_hevc_regional_subtitles_and_source_policy() {
    regional(true).await;
}
#[tokio::test]
async fn denied_encrypted_cdn_viewer_starts_no_source_or_cdn_worker() {
    let _lock = SERIAL.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let (source, cdn) = cluster(dir.path(), config(false, "convert", true)).await;
    let (listen, endpoint) = listener();
    let cancel = CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    let serving = tokio::spawn(srt_playback::serve(listen, cdn.app.clone(), cancel.clone()));
    let path = dir.path().join("denied.ts");
    let mut receiver = capture_as(&endpoint, false, &path, "wrong-viewer").await;
    // This deadline includes external receiver startup and the configured
    // two-second connection/ten-second read limits. FFmpeg 6 may not reach
    // discovery within five seconds; require actual discovery below so a
    // failed receiver invocation cannot qualify an authorization denial.
    tokio::time::timeout(Duration::from_secs(15), receiver.wait())
        .await
        .expect("denied receiver must exit within its transport bounds")
        .unwrap();
    assert_eq!(
        std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
        0,
        "denied viewer received bytes"
    );
    assert_eq!(cdn.app.media.count().await, 0);
    assert_eq!(source.app.media.count().await, 0);
    assert_eq!(cdn.app.playback_auth.active(), 0);
    assert_eq!(source.app.playback_auth.active(), 0);
    assert!(
        source.requests.discovery.load(Ordering::Relaxed) > 0,
        "receiver must reach real CDN discovery before denial"
    );
    assert_eq!(
        source.requests.media.load(Ordering::Relaxed),
        0,
        "denial must not subscribe to private source media"
    );
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    cdn.stop().await;
    source.stop().await;
}
