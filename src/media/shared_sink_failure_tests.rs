//! A dead packager must leave a typed local failure even when it cancels its input task.
use super::*;
use crate::{
    config::ConfigStore,
    m4_ingest::flussonic::InputFailure,
    m4f::{self, Frame},
    m4s::Track,
    playback_auth::{PlaybackAuth, ViewerRequest},
    source_directory::modern_coordinator::{
        ModernSourceCoordinator, ReplicaAdmissionOutcome, ReplicaSelector, SharedInputOptions,
        SharedInputs,
    },
};
use axum::{
    Router, body::Body, extract::OriginalUri, http::HeaderMap, response::Response, routing::get,
};
use bytes::Bytes;
use serde_json::json;
use std::{io, sync::Arc, time::Duration};
use tokio::sync::mpsc;

const NAME: &str = "owned";
type Feed = mpsc::UnboundedSender<Result<Bytes, io::Error>>;

async fn run_sink_failure(after_source_failure: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let key = format!("m4f://{}", listener.local_addr().unwrap());
    let (connection_tx, mut connections) = mpsc::unbounded_channel::<Feed>();
    let router = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri, _headers: HeaderMap| {
        let connection_tx = connection_tx.clone();
        async move {
            if uri.path().starts_with("/owned/") {
                let (feed, body) = mpsc::unbounded_channel();
                connection_tx.send(feed).unwrap();
                return Response::builder()
                    .header("cluster-auth", "yes")
                    .body(Body::from_stream(tokio_stream::wrappers::UnboundedReceiverStream::new(body)))
                    .unwrap();
            }
            let value = if uri.path().ends_with("/config") {
                json!({"stats":{"id":"owned-origin"}})
            } else {
                json!({"server_id":"owned-origin","streams":[{"name":NAME,"named_by":"config","static":true,"groups":[]}]})
            };
            Response::new(Body::from(value.to_string()))
        }
    }));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
    config
        .put(
            "sources",
            &key,
            json!({"url":key,"cluster_key":"owned-private-key","flussonix_source_kind":"flussonic"}),
        )
        .unwrap();
    let owner = Arc::new(ModernSourceCoordinator::new(config, "owned-local").unwrap());
    let selector = Arc::new(ReplicaSelector::single_origin(owner.clone(), &key).unwrap());
    let handle = owner
        .install(
            owner
                .capture(&key)
                .unwrap()
                .with_policy(&json!({"on_play":"true"}), &json!({}))
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap();
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(
            &handle,
            owner.inventory(&handle, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    let auth = PlaybackAuth::new(16);
    let ReplicaAdmissionOutcome::Allowed(admission) = selector
        .admit(
            choice,
            &auth,
            ViewerRequest {
                name: NAME.into(),
                proto: "hls".into(),
                ip: "127.0.0.1".into(),
                token: "one".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("owned viewer denied");
    };
    let registry = SharedInputs::new(owner);
    registry.reserve_failure(NAME).unwrap();
    let attachment = registry
        .attach_replica(*admission, SharedInputOptions::default())
        .unwrap();
    let script = dir.path().join("packager");
    std::fs::write(
        &script,
        if after_source_failure {
            "#!/bin/sh\nsleep 1\nexit 0\n"
        } else {
            "#!/bin/sh\nsleep 0.2\nexit 0\n"
        },
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(dir.path(), script.to_str().unwrap());
    let playback = engine
        .package_shared(attachment, &json!({"flussonix_hls_subtitles":"drop"}))
        .await
        .unwrap();
    let mut signals = playback.subscribe_m4f().unwrap();
    let control = tokio::time::timeout(Duration::from_secs(2), connections.recv())
        .await
        .unwrap()
        .unwrap();
    control
        .send(Ok(Bytes::from_static(b"1 2026/10/13/00/00/00-4000\n")))
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), connections.recv())
        .await
        .unwrap()
        .unwrap();
    let track = Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    };
    let frames = (0..5_000)
        .map(|i| Frame {
            track_id: 1,
            dts: 90_000 + i * 2_160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("../../tests/fixtures/codecs/mp3.bin").to_vec(),
        })
        .collect::<Vec<_>>();
    first
        .send(Ok(Bytes::from(
            m4f::pack(&[track], &frames, 360_000).unwrap(),
        )))
        .unwrap();
    drop(first);
    let _ = tokio::time::timeout(Duration::from_secs(3), signals.recv())
        .await
        .unwrap();
    if after_source_failure {
        control
            .send(Ok(Bytes::from_static(b"2 2026/10/13/00/00/04-4000\n")))
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), connections.recv())
            .await
            .unwrap()
            .unwrap();
        second
            .send(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "owned segment failure",
            )))
            .unwrap();
        drop(second);
    }
    let kind = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(observation) = registry.take_failure(NAME) {
                break observation.notice.kind;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    engine.stop_all().await;
    server.abort();
    assert_eq!(kind.unwrap(), InputFailure::Local);
}

#[tokio::test]
async fn child_stdin_failure_while_source_open_is_local() {
    run_sink_failure(false).await;
}

#[tokio::test]
async fn child_stdin_failure_during_transient_drain_is_local() {
    run_sink_failure(true).await;
}
