//! Owned local source and receiver; no production listeners or configuration.
use flussonix::{
    fxlink::{ClientPool, Listener, Settings},
    media::Engine,
    server::{App, Options},
};
use futures_util::FutureExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{panic::AssertUnwindSafe, time::Duration};
use tokio_util::sync::CancellationToken;
#[path = "support/tls.rs"]
mod tls_fixture;

#[tokio::test]
async fn authenticated_fxlink_survives_unrelated_config_edits_and_retires_when_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let certificates = tls_fixture::Certificates::new();
    let app = App::new(
        dir.path().join("origin.json"),
        dir.path().join("origin-media"),
        Options {
            peer_key: "owned-fxlink-native-peer".into(),
            admin_password: "owned-fxlink-admin".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],
        "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-fxlink-viewer"))}),
        )
        .unwrap();
    let cancel = CancellationToken::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &certificates.cert,
        &certificates.key,
        Settings::default(),
    )
    .unwrap();
    app.set_fxlink_listener(Some(&listener));
    let url = format!("fxlink://{}/owned", listener.address());
    let task = tokio::spawn(listener.serve(app.fxlink_provider(), cancel.clone()));
    let pool = ClientPool::default();
    let engine = Engine::new(dir.path().join("receive-media"), "ffmpeg");
    let outcome = AssertUnwindSafe(async {
        assert!(
            pool.subscribe(
                &url,
                "wrong-native-peer",
                "owned-fxlink-viewer",
                Some(&certificates.ca),
                Settings::default(),
                cancel.child_token()
            )
            .await
            .is_err()
        );
        assert!(
            pool.subscribe(
                &url,
                "owned-fxlink-native-peer",
                "wrong-viewer",
                Some(&certificates.ca),
                Settings::default(),
                cancel.child_token()
            )
            .await
            .is_err()
        );
        assert_eq!(
            app.media.count().await,
            0,
            "denied admission must not start a media worker"
        );
        let cfg = json!({"flussonix_peer_key":"owned-fxlink-native-peer","inputs":[{"url":url,
            "flussonix_tls_ca":certificates.ca,"flussonix_fxlink_token":"owned-fxlink-viewer"}]});
        let worker = engine.ensure("received", &cfg).await.unwrap();
        let manifest = tokio::time::timeout(Duration::from_secs(35), async {
            loop {
                if let Ok(manifest) = engine.read("received", "index.m3u8").await {
                    break manifest;
                }
                assert!(
                    !worker.is_closed(),
                    "FXLink worker closed before HLS: {}",
                    engine.stats("received").await
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("FXLink native records must reach HLS packaging");
        let manifest = String::from_utf8(manifest.to_vec()).unwrap();
        let segment = manifest
            .lines()
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        let bytes = engine.read("received", segment).await.unwrap();
        let file = dir.path().join("owned-received.ts");
        tokio::fs::write(&file, &bytes).await.unwrap();
        let decoded = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("ffmpeg")
                .kill_on_drop(true)
                .args(["-nostdin", "-v", "error", "-xerror", "-i"])
                .arg(&file)
                .args([
                    "-map", "0:v:0", "-map", "0:a:0", "-t", "1", "-f", "null", "-",
                ])
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            decoded.status.success() && decoded.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        let stats: Value = engine.stats("received").await;
        assert_eq!(stats["input_protocol"], "fxlink");
        assert!(worker.wire.has_info());
        let receive_pid = worker.pid();
        // Exercise the real management handlers, including session invalidation
        // and worker reconciliation, while media uses the verified UDP link.
        use axum::{body::Body, http::Request};
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use tower::ServiceExt;
        let management = flussonix::server::router(app.clone());
        for (collection, name, patch) in [
            (
                "streams",
                "other",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            ),
            ("templates", "unused", json!({"static":false})),
            (
                "auth_backends",
                "unused",
                json!({"url":"http://127.0.0.1:1/unused"}),
            ),
        ] {
            let response = management
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/streamer/api/v3/{collection}/{name}"))
                        .header("Content-Type", "application/json")
                        .header(
                            "Authorization",
                            format!("Basic {}", STANDARD.encode("admin:owned-fxlink-admin")),
                        )
                        .body(Body::from(patch.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "management edit failed: {collection}"
            );
        }
        let bytes_before = worker.bytes.load(std::sync::atomic::Ordering::Relaxed);
        let baseline_manifest = String::from_utf8(
            engine
                .read("received", "index.m3u8")
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let last_before = baseline_manifest
            .lines()
            .rfind(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        let next_segment = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                assert!(
                    !worker.is_closed(),
                    "unrelated management edit retired FXLink playback"
                );
                if let Ok(next) = engine.read("received", "index.m3u8").await {
                    let text = String::from_utf8(next.to_vec()).unwrap();
                    if let Some(segment) = text
                        .lines()
                        .rfind(|line| !line.is_empty() && !line.starts_with('#'))
                        && segment != last_before
                        && worker.bytes.load(std::sync::atomic::Ordering::Relaxed) > bytes_before
                    {
                        break segment.to_owned();
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("healthy stream must advance after unrelated management edits");
        assert_eq!(worker.pid(), receive_pid);
        tokio::fs::write(&file, engine.read("received", &next_segment).await.unwrap())
            .await
            .unwrap();
        let decoded = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("ffmpeg")
                .kill_on_drop(true)
                .args(["-nostdin", "-v", "error", "-xerror", "-i"])
                .arg(&file)
                .args([
                    "-map", "0:v:0", "-map", "0:a:0", "-t", "1", "-f", "null", "-",
                ])
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            decoded.status.success() && decoded.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        app.config
            .put("streams", "owned", json!({"disabled":true}))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker.closed())
            .await
            .expect("disabled source must retire FXLink receiver");
    })
    .catch_unwind()
    .await;
    cancel.cancel();
    engine.stop_all().await;
    app.media.stop_all().await;
    pool.close().await;
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
