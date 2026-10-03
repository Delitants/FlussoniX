use bytes::Bytes;
use flussonix::{
    m4_ingest,
    m4f::{self, Frame},
    m4s::Track,
    wire::Hub,
    worker_ts::Muxer,
};
use futures_util::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::AsyncReadExt;

async fn metadata_handshake(protocol: &str, invalid: bool) {
    let tracks = vec![Track {
        id: 37,
        codec: "hevc".into(),
        config: if invalid {
            vec![1, 2]
        } else {
            include_bytes!("fixtures/codecs/hevc.hvcc").to_vec()
        },
    }];
    let frame = Frame {
        track_id: 37,
        dts: 90000,
        pts_offset: 7200,
        key: true,
        body: include_bytes!("fixtures/codecs/hevc-00.bin").to_vec(),
    };
    let mut native = flussonix::wire::encode_info(&tracks).unwrap();
    native.extend(flussonix::wire::encode_frame(&tracks[0], &frame).unwrap());
    let segment = Bytes::from(m4f::pack(&tracks, std::slice::from_ref(&frame), 180000).unwrap());
    let control = if protocol == "m4s" {
        Bytes::from(native)
    } else {
        Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
    };
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let router = axum::Router::new()
        .route(
            &format!("/owned/{protocol}"),
            axum::routing::get(move || {
                c.fetch_add(1, Ordering::SeqCst);
                let bytes = control.clone();
                async move {
                    axum::body::Body::from_stream(
                        futures_util::stream::once(async move { Ok::<_, std::io::Error>(bytes) })
                            .chain(futures_util::stream::pending()),
                    )
                }
            }),
        )
        .route(
            "/owned/2026/10/03/01/00/00.m4f",
            axum::routing::get(move || {
                let body = segment.clone();
                async move { body }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("{protocol}://{}/owned", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let hub = Arc::new(Hub::new());
    let h = hub.clone();
    // One byte cannot hold a TS table: metadata must arrive before this write.
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (send, receive) = tokio::sync::oneshot::channel();
    let pull = tokio::spawn(async move {
        m4_ingest::pull_ready(&url, None, &mut writer, Some(&h), send).await
    });
    let metadata = tokio::time::timeout(Duration::from_secs(2), receive)
        .await
        .unwrap();
    if invalid {
        assert!(metadata.is_err());
        assert!(pull.await.unwrap().is_err());
        assert!(hub.m4s_subscribe().0.is_empty());
    } else {
        assert_eq!(metadata.unwrap(), tracks);
        assert!(!pull.is_finished());
        let mut muxer = Muxer::new(&tracks).unwrap();
        let mut expected = muxer.tables();
        expected.extend(muxer.frame(&frame).unwrap());
        let mut actual = vec![0; expected.len()];
        tokio::time::timeout(Duration::from_secs(2), reader.read_exact(&mut actual))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected, "startup samples must survive preparation");
        pull.abort();
        let _ = pull.await;
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn m4s_metadata_precedes_pipe_backpressure_without_reconnecting() {
    metadata_handshake("m4s", false).await;
}
#[tokio::test]
async fn m4f_metadata_precedes_pipe_backpressure_without_refetching() {
    metadata_handshake("m4f", false).await;
}
#[tokio::test]
async fn m4s_invalid_initial_metadata_closes_handshake_before_relay() {
    metadata_handshake("m4s", true).await;
}
#[tokio::test]
async fn m4f_invalid_initial_metadata_closes_handshake_before_relay() {
    metadata_handshake("m4f", true).await;
}

struct PendingSource(Arc<std::sync::atomic::AtomicBool>);
impl futures_util::Stream for PendingSource {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}
impl Drop for PendingSource {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn pending_source() -> (
    String,
    Arc<AtomicUsize>,
    Arc<std::sync::atomic::AtomicBool>,
    tokio::task::JoinHandle<()>,
) {
    let count = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let n = count.clone();
    let c = closed.clone();
    let router = axum::Router::new().route(
        "/owned/m4s",
        axum::routing::get(move || {
            n.fetch_add(1, Ordering::SeqCst);
            let c = c.clone();
            async move { axum::body::Body::from_stream(PendingSource(c)) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("m4s://{}/owned", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, count, closed, server)
}

#[tokio::test]
async fn pending_native_startup_is_shared_cancellable_and_does_not_block_other_streams() {
    let (url, count, closed, server) = pending_source().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path(), "ffmpeg");
    let cfg = serde_json::json!({"inputs":[{"url":url}]});
    let worker = tokio::time::timeout(Duration::from_secs(1), engine.ensure("owned", &cfg))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(worker.pid(), 0, "no FFmpeg before validated metadata");
    let again = engine.ensure("owned", &cfg).await.unwrap();
    assert!(Arc::ptr_eq(&worker, &again));
    tokio::time::timeout(Duration::from_secs(2), async {
        while count.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let other = tokio::time::timeout(
        Duration::from_secs(1),
        engine.ensure(
            "other",
            &serde_json::json!({"inputs":[{"url":"testsrc://"}]}),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(other.pid(), 0);
    tokio::time::timeout(Duration::from_secs(1), engine.stop("owned"))
        .await
        .unwrap();
    assert!(!worker.alive.load(Ordering::SeqCst));
    assert_eq!(worker.pid(), 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !closed.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    engine.stop_all().await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn native_metadata_timeout_completes_cleanup_and_allows_recovery() {
    let (url, count, closed, server) = pending_source().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path(), "ffmpeg");
    let cfg = serde_json::json!({"inputs":[{"url":url}],"flussonix_input_timeout":1});
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    assert_eq!(worker.pid(), 0);
    tokio::time::timeout(Duration::from_secs(3), worker.closed())
        .await
        .unwrap();
    assert_eq!(worker.stats()["last_error"], "startup_timeout");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let retry = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match engine.recover("owned", &cfg).await {
                Ok(w) => break w,
                Err(e) => assert_eq!(e, "input retry backoff"),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(retry.pid(), 0);
    assert_eq!(retry.stats()["restart_count"], 1);
    assert!(!worker.alive.load(Ordering::SeqCst));
    engine.stop_all().await;
    assert!(closed.load(Ordering::SeqCst));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn native_spawn_failure_is_a_recoverable_worker_failure() {
    let tracks = vec![Track {
        id: 1,
        codec: "hevc".into(),
        config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
    }];
    let info = Bytes::from(flussonix::wire::encode_info(&tracks).unwrap());
    let router = axum::Router::new().route(
        "/owned/m4s",
        axum::routing::get(move || {
            let b = info.clone();
            async move {
                axum::body::Body::from_stream(
                    futures_util::stream::once(async move { Ok::<_, std::io::Error>(b) })
                        .chain(futures_util::stream::pending()),
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("m4s://{}/owned", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path(), "/nonexistent-owned-test-ffmpeg");
    let worker = engine
        .ensure("owned", &serde_json::json!({"inputs":[{"url":url}]}))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.closed())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), engine.stop_all())
        .await
        .unwrap();
    assert_eq!(worker.pid(), 0);
    assert_eq!(worker.stats()["last_error"], "packaging_failed");
    assert!(!worker.alive.load(Ordering::SeqCst));
    server.abort();
    let _ = server.await;
}
