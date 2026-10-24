//! Real independent SRT callers and owned authenticated vendor origins.
#[path = "vendor_daemon_failover/fixture.rs"]
#[allow(dead_code)]
mod fixture;
use fixture::Fixture;
#[path = "support/vendor_srt.rs"]
mod transport;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{sync::atomic::Ordering, time::Duration};
use transport::{Receiver, Serving};
const SECRET: &str = "owned-vendor-srt-transport-key";
fn policy() -> serde_json::Value {
    json!({"only":[],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})
}
async fn pulled(f: &Fixture) -> bool {
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.origins[0].private_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn encrypted_vendor_srt_authorizes_before_media_and_shares_http_packager() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    let serving = Serving::start(f.app.clone(), SECRET).await;
    let denied = Receiver::start(serving.address, "wrong", SECRET, "3")
        .finish()
        .await;
    assert!(!denied.0, "denied viewer decoded media");
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    let viewer = Receiver::start(serving.address, "owned-viewer", SECRET, "100");
    let has_pull = pulled(&f).await;
    let response = if has_pull {
        Some(f.get("/owned/m4s?token=owned-viewer").await)
    } else {
        None
    };
    let http_ok = response.as_ref().is_some_and(|r| r.status() == 200);
    let outcome = viewer.finish().await;
    let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
    let egress = f.app.srt_egress.load(Ordering::Relaxed);
    drop(response);
    serving.stop().await;
    f.stop().await;
    assert!(
        has_pull,
        "vendor SRT listener never started the authorized private input"
    );
    assert!(
        outcome.0 && outcome.1.is_empty(),
        "strict SRT decode: {}",
        outcome.1
    );
    assert!(http_ok);
    assert_eq!(calls, 1);
    assert!(egress > 0);
}
async fn client_count(f: &Fixture) -> usize {
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let reply = flussonix::server::router(f.app.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri("/flussonix/api/v1/node")
                .header("x-flussonix-peer", "owned-native-peer")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let value: serde_json::Value =
        serde_json::from_slice(&reply.into_body().collect().await.unwrap().to_bytes()).unwrap();
    value["srt_playback"]["clients"].as_u64().unwrap() as usize
}
async fn connections(f: &Fixture, expected: usize) -> bool {
    connections_within(f, expected, Duration::from_secs(4)).await
}
async fn connections_within(f: &Fixture, expected: usize, within: Duration) -> bool {
    tokio::time::timeout(within, async {
        while client_count(f).await != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}
fn revoke(f: &Fixture, token: &str) -> bool {
    let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    f.app
        .playback_auth
        .snapshots()
        .into_iter()
        .find(|s| s["user_id"] == hash)
        .is_some_and(|s| f.app.playback_auth.revoke(s["id"].as_str().unwrap()))
}
#[tokio::test]
async fn clear_vendor_srt_revokes_one_viewer_without_stopping_static_or_http() {
    let f = Fixture::configured("cdn", json!({}), None, false).await;
    f.wait_private(0, 1).await;
    let serving = Serving::start(f.app.clone(), "").await;
    let first = Receiver::start(serving.address, "viewer-a", "", "10000");
    let mut second = Receiver::start(serving.address, "viewer-b", "", "10000");
    let two = connections(&f, 2).await;
    let response = f.get("/owned/m4s?token=viewer-http").await;
    let http_ok = response.status() == 200;
    let revoked = revoke(&f, "viewer-a");
    let one = connections(&f, 1).await;
    let other_alive = second.running();
    let same_pull = f.origins[0].private_calls.load(Ordering::SeqCst) == 1;
    first.stop().await;
    second.stop().await;
    drop(response);
    let closed_at = std::time::Instant::now();
    // stop() kills the caller without an SRT shutdown. Allow the listener's
    // five-second peer-idle timeout plus its polling/scheduling margin.
    let empty = connections_within(&f, 0, Duration::from_secs(7)).await;
    let closed_seconds = closed_at.elapsed().as_secs_f64();
    let static_alive = f.app.media.count().await == 1;
    serving.stop().await;
    f.stop().await;
    assert!(
        two && http_ok && revoked && one && other_alive && same_pull && empty && static_alive,
        "two={two} http_ok={http_ok} revoked={revoked} one={one} other_alive={other_alive} same_pull={same_pull} empty={empty} closed_seconds={closed_seconds} static_alive={static_alive}"
    );
}
#[tokio::test]
async fn wrong_or_missing_transport_secret_never_discovers_or_pulls_vendor_media() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    let serving = Serving::start(f.app.clone(), SECRET).await;
    let before = f.origins[0].management_calls.load(Ordering::SeqCst);
    for secret in ["wrong-transport-secret", ""] {
        let denied = Receiver::start(serving.address, "owned-viewer", secret, "3")
            .finish()
            .await;
        assert!(!denied.0);
    }
    let pulls = f.origins[0].private_calls.load(Ordering::SeqCst);
    let after = f.origins[0].management_calls.load(Ordering::SeqCst);
    serving.stop().await;
    f.stop().await;
    assert_eq!(pulls, 0);
    assert_eq!(before, after);
}
#[tokio::test]
async fn load_balancer_and_excluded_vendor_srt_names_cannot_activate_media() {
    for role in ["lb", "cdn"] {
        let patch = if role == "lb" {
            policy()
        } else {
            json!({"only":[],"except":["owned"]})
        };
        let f = Fixture::configured(role, patch, None, false).await;
        let serving = Serving::start(f.app.clone(), "").await;
        let denied = Receiver::start(serving.address, "owned-viewer", "", "3")
            .finish()
            .await;
        let pulls = f.origins[0].private_calls.load(Ordering::SeqCst);
        let discovery = f.origins[0].management_calls.load(Ordering::SeqCst);
        serving.stop().await;
        f.stop().await;
        assert!(!denied.0);
        assert_eq!(pulls, 0);
        assert_eq!(discovery, 0);
    }
}
#[tokio::test]
async fn source_exclusion_during_held_metadata_closes_srt_without_late_media() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    f.origins[0].stall_metadata.store(true, Ordering::SeqCst);
    let serving = Serving::start(f.app.clone(), SECRET).await;
    let receiver = Receiver::start(serving.address, "owned-viewer", SECRET, "3");
    let started = pulled(&f).await;
    let mut source = f.app.config.snapshot()["sources"][0].clone();
    source["except"] = json!(["owned"]);
    f.app
        .config
        .put("sources", &f.origins[0].key, source)
        .unwrap();
    let closed = connections(&f, 0).await;
    let outcome = receiver.finish().await;
    let egress = f.app.srt_egress.load(Ordering::Relaxed);
    serving.stop().await;
    f.stop().await;
    assert!(started && closed && !outcome.0);
    assert_eq!(egress, 0);
}
#[tokio::test]
async fn grouped_vendor_srt_recovers_and_new_caller_shares_backup_with_http() {
    let f = Fixture::configured("cdn", policy(), None, true).await;
    let serving = Serving::start(f.app.clone(), SECRET).await;
    let old = Receiver::start(serving.address, "owned-viewer", SECRET, "10000");
    let started = pulled(&f).await;
    f.origins[0].close_media();
    let closed = connections(&f, 0).await;
    old.stop().await;
    f.wait_private(1, 1).await;
    let fresh = Receiver::start(serving.address, "owned-viewer", SECRET, "100");
    let response = f.get("/owned/m4s?token=owned-viewer").await;
    let http_ok = response.status() == 200;
    let outcome = fresh.finish().await;
    let calls = (
        f.origins[0].private_calls.load(Ordering::SeqCst),
        f.origins[1].private_calls.load(Ordering::SeqCst),
    );
    drop(response);
    serving.stop().await;
    f.stop().await;
    assert!(
        started && closed && http_ok && outcome.0 && outcome.1.is_empty(),
        "backup SRT started={started} closed={closed} http={http_ok} decode={} stderr={}",
        outcome.0,
        outcome.1
    );
    assert_eq!(calls, (1, 1));
}
#[tokio::test]
async fn vendor_srt_cpu_aac_conversion_shares_the_http_processing_worker() {
    let f = Fixture::configured(
        "cdn",
        json!({"only":[],"flussonix_output_template":"owned-aac"}),
        None,
        false,
    )
    .await;
    let serving = Serving::start(f.app.clone(), SECRET).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("aac.ts");
    let receiver = Receiver::record(serving.address, "srt-viewer", SECRET, "100", &path);
    let started = pulled(&f).await;
    let response = f.get("/owned/m4s?token=http-viewer").await;
    let http_ok = response.status() == 200;
    let clients = f.app.media.stats("owned").await["online_clients"].clone();
    let outcome = receiver.finish().await;
    let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
    drop(response);
    serving.stop().await;
    f.stop().await;
    assert!(
        started && http_ok && outcome.0 && outcome.1.is_empty(),
        "CPU AAC: {}",
        outcome.1
    );
    assert_eq!(clients, 2);
    assert_eq!(calls, 1);
    let probe = tokio::process::Command::new("ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(&path)
        .output()
        .await
        .unwrap();
    assert!(probe.status.success() && probe.stderr.is_empty());
    let info: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
    assert_eq!(info["streams"][0]["codec_name"], "aac");
    let decode = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(path)
        .args(["-map", "0:a:0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decode.status.success() && decode.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&decode.stderr)
    );
}
#[tokio::test]
async fn held_vendor_srt_callback_cannot_admit_after_source_exclusion() {
    use axum::{Router, routing::get};
    use std::sync::Arc;
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", tcp.local_addr().unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (s, r) = (started.clone(), release.clone());
    let callback = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().route(
                "/auth",
                get(
                    move |axum::extract::RawQuery(query): axum::extract::RawQuery| {
                        let (s, r) = (s.clone(), r.clone());
                        async move {
                            let params: std::collections::HashMap<_, _> =
                                url::form_urlencoded::parse(query.as_deref().unwrap().as_bytes())
                                    .into_owned()
                                    .collect();
                            assert_eq!(params["proto"], "srt");
                            assert_eq!(params["token"], "callback-viewer");
                            s.notify_one();
                            r.notified().await;
                            axum::http::StatusCode::OK
                        }
                    },
                ),
            ),
        )
        .await
        .unwrap();
    });
    let f = Fixture::configured("cdn", json!({"only":[],"on_play":url}), None, false).await;
    let serving = Serving::start(f.app.clone(), SECRET).await;
    let receiver = Receiver::start(serving.address, "callback-viewer", SECRET, "3");
    let held = tokio::time::timeout(Duration::from_secs(4), started.notified())
        .await
        .is_ok();
    let before = f.origins[0].private_calls.load(Ordering::SeqCst);
    let mut source = f.app.config.snapshot()["sources"][0].clone();
    source["except"] = json!(["owned"]);
    f.app
        .config
        .put("sources", &f.origins[0].key, source)
        .unwrap();
    release.notify_one();
    let outcome = receiver.finish().await;
    let calls = f.origins[0].private_calls.load(Ordering::SeqCst);
    let bytes = f.app.srt_egress.load(Ordering::Relaxed);
    serving.stop().await;
    f.stop().await;
    callback.abort();
    let _ = callback.await;
    assert!(held && !outcome.0);
    assert_eq!(before, 0);
    assert_eq!(calls, 0);
    assert_eq!(bytes, 0);
}
#[tokio::test]
async fn disabled_local_stream_and_ambiguous_vendor_namespace_never_fall_back() {
    for local in [false, true] {
        let f = Fixture::configured("cdn", policy(), None, false).await;
        if local {
            f.app
                .config
                .put(
                    "streams",
                    "owned",
                    json!({"disabled":true,"static":false,"inputs":[{"url":"testsrc://"}]}),
                )
                .unwrap();
        } else {
            f.app.config.put("sources",&f.origins[1].key,json!({"flussonix_source_kind":"flussonic","cluster_key":f.origins[1].secret,"on_play":"true","only":[]})).unwrap();
        }
        let serving = Serving::start(f.app.clone(), "").await;
        let outcome = Receiver::start(serving.address, "owned-viewer", "", "3")
            .finish()
            .await;
        let calls = f
            .origins
            .iter()
            .map(|o| {
                o.private_calls.load(Ordering::SeqCst) + o.management_calls.load(Ordering::SeqCst)
            })
            .sum::<usize>();
        let workers = f.app.media.count().await;
        serving.stop().await;
        f.stop().await;
        assert!(!outcome.0);
        assert_eq!(calls, 0);
        assert_eq!(workers, 0);
    }
}
