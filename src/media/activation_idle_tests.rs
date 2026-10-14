//! Static demand protects idle retirement without manufacturing playback.
use super::*;
use crate::{
    config::ConfigStore,
    source_directory::modern_coordinator::{
        ModernSourceCoordinator, SharedInputOptions, SharedInputs,
    },
};
use axum::{
    Router, body::Body, extract::OriginalUri, http::HeaderMap, response::Response, routing::get,
};
use futures_util::StreamExt;
#[tokio::test]
async fn aged_static_owner_is_not_idle_until_cancelled_or_dropped() {
    for drop_owner in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", listener.local_addr().unwrap());
        let track = crate::m4s::Track {
            id: 1,
            codec: "mp3".into(),
            config: vec![],
        };
        let mut bytes = crate::wire::encode_info(std::slice::from_ref(&track)).unwrap();
        for n in 0..200 {
            bytes.extend(
                crate::wire::encode_frame(
                    &track,
                    &crate::m4f::Frame {
                        track_id: 1,
                        dts: 90000 + n * 2351,
                        pts_offset: 0,
                        key: true,
                        body: include_bytes!("../../tests/fixtures/codecs/mp3.bin").to_vec(),
                    },
                )
                .unwrap(),
            );
        }
        let bytes = Bytes::from(bytes);
        let router=Router::new().fallback(get(move|OriginalUri(uri):OriginalUri,headers:HeaderMap|{let bytes=bytes.clone();async move{
            if uri.path()=="/owned/m4s" {assert!(headers.contains_key("cluster-auth-key"));let stream=futures_util::stream::iter([Ok::<_,std::io::Error>(bytes)]).chain(futures_util::stream::pending());return Response::builder().header("cluster-auth","yes").body(Body::from_stream(stream)).unwrap();}
            assert!(headers.contains_key("authorization"));let value=if uri.path().ends_with("/config"){json!({"stats":{"id":"owned-idle-origin"}})}else{json!({"server_id":"owned-idle-origin","streams":[{"name":"owned","named_by":"config","static":true,"groups":[]}]})};Response::new(Body::from(value.to_string()))
        }}));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let _server_guard = ServerGuard(server.abort_handle());
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
        config.put("sources",&key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-idle-private-key","on_play":"true"})).unwrap();
        let owner = Arc::new(ModernSourceCoordinator::new(config, "owned-idle-local").unwrap());
        let handle = owner
            .install(
                owner
                    .capture(&key)
                    .unwrap()
                    .prepare(Duration::from_secs(3))
                    .unwrap(),
            )
            .unwrap();
        let rows = owner
            .inventory(&handle, Duration::from_secs(30))
            .await
            .unwrap();
        let activation = owner.activate(rows.candidate("owned").unwrap()).unwrap();
        let control = activation.control();
        let inputs = SharedInputs::new(owner);
        let engine = Engine::new(dir.path().join("media"), "ffmpeg");
        let active = engine
            .package_shared(
                inputs
                    .attach_activation(activation, SharedInputOptions::default())
                    .unwrap(),
                &json!({}),
            )
            .await
            .unwrap();
        let worker = active.inner.worker.clone();
        tokio::time::timeout(Duration::from_secs(3), async {
            while worker.pid() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            inputs
                .renew(&handle, Duration::from_secs(30))
                .await
                .unwrap()
                .renewed,
            1
        );
        active.current().unwrap();
        *worker.last_access.lock().unwrap() = Instant::now() - Duration::from_secs(61);
        let protected = !engine.idle().await.iter().any(|name| name == "owned");
        let untouched = worker.idle_seconds() >= 61;
        if drop_owner {
            drop(active);
        } else {
            control.cancel();
            drop(active);
        }
        let retired = engine.idle().await.iter().any(|name| name == "owned");
        let pid = worker.pid();
        engine.stop_all().await;
        server.abort();
        let _ = server.await;
        assert!(
            protected,
            "valid zero-viewer static ownership was selected for idle retirement"
        );
        assert!(untouched, "static ownership renewed the viewer clock");
        assert!(
            retired,
            "retired static ownership still protected an idle worker"
        );
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}
struct ServerGuard(tokio::task::AbortHandle);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}
