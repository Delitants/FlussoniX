use axum::{
    Router,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
struct Node {
    app: Arc<App>,
    url: String,
    task: tokio::task::JoinHandle<()>,
    mode: Arc<AtomicU8>,
    release: Arc<tokio::sync::Notify>,
    entered: Arc<tokio::sync::Notify>,
}
async fn node(dir: &std::path::Path, name: &str, role: &str, key: &str) -> Node {
    let app = App::new(
        dir.join(format!("{name}.json")),
        dir.join(name),
        Options {
            admin_password: "owned-management".into(),
            peer_key: key.into(),
            node_name: name.into(),
            role: role.into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let (a, m, r, e, k) = (
        app.clone(),
        mode.clone(),
        release.clone(),
        entered.clone(),
        key.to_string(),
    );
    let routes = Router::new()
        .route(
            "/flussonix/api/v1/stream/{*name}",
            get(move |headers: HeaderMap, Path(name): Path<String>| {
                let (a, m, r, e, k) = (a.clone(), m.clone(), r.clone(), e.clone(), k.clone());
                async move {
                    if headers
                        .get("X-Flussonix-Peer")
                        .and_then(|v| v.to_str().ok())
                        != Some(k.as_str())
                    {
                        return StatusCode::FORBIDDEN.into_response();
                    }
                    let cfg = a.config.effective(&name);
                    let mode = m.load(Ordering::Relaxed);
                    if mode == 1 {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    if mode == 2 {
                        e.notify_one();
                        r.notified().await;
                    }
                    if mode == 4 {
                        let mut c = cfg.unwrap();
                        c["on_play"] = json!({"max_sessions":0});
                        return axum::Json(c).into_response();
                    }
                    if mode == 3 {
                        return (StatusCode::OK, "[]").into_response();
                    }
                    cfg.map_or_else(
                        || StatusCode::NOT_FOUND.into_response(),
                        |c| axum::Json(c).into_response(),
                    )
                }
            }),
        )
        .fallback_service(router(app.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
    Node {
        app,
        url,
        task,
        mode,
        release,
        entered,
    }
}
fn stream(id: &str) -> Value {
    json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_content_id":id,"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer-owned"))})
}
async fn setup(dir: &std::path::Path, transport: &str) -> (Node, Node, Node) {
    let a = node(dir, "a", "source", "source-a-owned-key").await;
    let b = node(dir, "b", "source", "source-b-owned-key").await;
    let cdn = node(dir, "cdn", "cdn", "cdn-owned-peer-key").await;
    for n in [&a, &b] {
        n.app
            .config
            .put("streams", "region/news", stream("same-content-v1"))
            .unwrap();
    }
    for (n, key) in [(&a, "source-a-owned-key"), (&b, "source-b-owned-key")] {
        cdn.app.config.put("sources",if n.url==a.url{"a"}else{"b"},json!({"api_url":n.url,"private_payload_url":n.url,"cluster_key":key,"flussonix_transport":transport,"flussonix_source_group":"owned-replicas"})).unwrap();
    }
    (a, b, cdn)
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}
async fn play(cdn: &Node) -> reqwest::Response {
    for attempt in 0..8 {
        let response = client()
            .get(format!(
                "{}/region/news/index.m3u8?token=viewer-owned",
                cdn.url
            ))
            .send()
            .await
            .unwrap();
        if response.status() != 503 || attempt == 7 {
            return response;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    unreachable!()
}

async fn selected(cdn: &Node) -> Value {
    let state: Value = client()
        .get(format!("{}/flussonix/api/v1/node", cdn.url))
        .header("X-Flussonix-Peer", "cdn-owned-peer-key")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    state["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "region/news")
        .cloned()
        .unwrap_or(Value::Null)
}
async fn kill_cdn_worker(cdn: &Node) {
    let s = cdn.app.media.stats("region/news").await;
    let pid = s["pid"].as_u64().unwrap();
    assert!(
        std::process::Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = cdn.app.media.stats("region/news").await;
            if stats["status"] == "retrying" && stats["retry_in_ms"] == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
async fn cleanup(nodes: [Node; 3]) {
    for n in nodes {
        n.app.media.stop_all().await;
        n.task.abort();
    }
}
#[tokio::test]
async fn media_failure_switches_equivalent_origin_without_new_viewer_and_is_sticky() {
    let d = tempfile::tempdir().unwrap();
    let (a, b, cdn) = setup(d.path(), "m4s").await;
    assert_eq!(play(&cdn).await.status(), 200);
    let sessions = cdn.app.playback_auth.snapshots();
    let id = sessions[0]["id"].clone();
    assert!(id.is_string());
    kill_cdn_worker(&cdn).await;
    cdn.app.reconcile().await;
    let state = selected(&cdn).await;
    assert_eq!(state["stats"]["upstream_source"], "b");
    assert_eq!(state["stats"]["source_switches"], 1);
    let response = play(&cdn).await;
    assert_eq!(response.status(), 200);
    let list = response.text().await.unwrap();
    let segment = list
        .lines()
        .rfind(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap();
    let bytes = client()
        .get(
            url::Url::parse(&cdn.url)
                .unwrap()
                .join(&format!("/region/news/{segment}"))
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let file = d.path().join("after.ts");
    std::fs::write(&file, bytes).unwrap();
    assert!(
        std::process::Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-i",
                file.to_str().unwrap(),
                "-t",
                "1",
                "-f",
                "null",
                "-"
            ])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(cdn.app.playback_auth.snapshots()[0]["id"], id);
    tokio::time::sleep(Duration::from_millis(10100)).await;
    cdn.app.reconcile().await;
    assert_eq!(selected(&cdn).await["stats"]["upstream_source"], "b");
    assert_eq!(
        client()
            .get(format!("{}/region/news/index.m3u8", cdn.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    cleanup([a, b, cdn]).await;
}
#[tokio::test]
async fn management_outage_switches_to_same_policy_replica() {
    let d = tempfile::tempdir().unwrap();
    let (a, b, cdn) = setup(d.path(), "hls").await;
    assert_eq!(play(&cdn).await.status(), 200);
    a.mode.store(1, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(10100)).await;
    cdn.app.reconcile().await;
    assert_eq!(selected(&cdn).await["stats"]["upstream_source"], "b");
    assert_eq!(play(&cdn).await.status(), 200);
    cleanup([a, b, cdn]).await;
}
#[tokio::test]
async fn different_content_policy_and_group_are_not_media_failover_candidates() {
    for mismatch in ["content", "policy", "group", "missing-id"] {
        let d = tempfile::tempdir().unwrap();
        let (a, b, cdn) = setup(d.path(), "m4s").await;
        match mismatch {
            "content" => {
                b.app
                    .config
                    .put(
                        "streams",
                        "region/news",
                        json!({"flussonix_content_id":"unrelated"}),
                    )
                    .unwrap();
            }
            "policy" => {
                b.app
                    .config
                    .put(
                        "streams",
                        "region/news",
                        json!({"flussonix_token_sha256":null}),
                    )
                    .unwrap();
            }
            "group" => {
                cdn.app
                    .config
                    .put("sources", "b", json!({"flussonix_source_group":"other"}))
                    .unwrap();
            }
            _ => {
                a.app
                    .config
                    .put(
                        "streams",
                        "region/news",
                        json!({"flussonix_content_id":null}),
                    )
                    .unwrap();
            }
        }
        assert_eq!(play(&cdn).await.status(), 200);
        kill_cdn_worker(&cdn).await;
        cdn.app.reconcile().await;
        assert_eq!(
            selected(&cdn).await["stats"]["upstream_source"],
            "a",
            "{mismatch}"
        );
        assert_eq!(selected(&cdn).await["stats"]["source_switches"], 0);
        cleanup([a, b, cdn]).await;
    }
}
#[tokio::test]
async fn authoritative_disable_delete_and_invalid_policy_fail_closed() {
    for failure in ["disabled", "absent", "invalid", "invalid-policy"] {
        let d = tempfile::tempdir().unwrap();
        let (a, b, cdn) = setup(d.path(), "m4s").await;
        assert_eq!(play(&cdn).await.status(), 200);
        match failure {
            "disabled" => {
                a.app
                    .config
                    .put("streams", "region/news", json!({"disabled":true}))
                    .unwrap();
            }
            "absent" => {
                a.app.config.delete("streams", "region/news").unwrap();
            }
            "invalid-policy" => a.mode.store(4, Ordering::Relaxed),
            _ => a.mode.store(3, Ordering::Relaxed),
        }
        kill_cdn_worker(&cdn).await;
        cdn.app.reconcile().await;
        assert_eq!(cdn.app.media.count().await, 0, "{failure}");
        assert!(!play(&cdn).await.status().is_success());
        cleanup([a, b, cdn]).await;
    }
}
#[tokio::test]
async fn late_lookup_cannot_overwrite_a_newer_authoritative_denial() {
    let d = tempfile::tempdir().unwrap();
    let (a, b, cdn) = setup(d.path(), "m4s").await;
    assert_eq!(play(&cdn).await.status(), 200);
    kill_cdn_worker(&cdn).await;
    a.mode.store(2, Ordering::Relaxed);
    let app = cdn.app.clone();
    let pending = tokio::spawn(async move { app.reconcile().await });
    tokio::time::timeout(Duration::from_secs(2), a.entered.notified())
        .await
        .unwrap();
    a.mode.store(0, Ordering::Relaxed);
    a.app
        .config
        .put("streams", "region/news", json!({"disabled":true}))
        .unwrap();
    cdn.app.reconcile().await;
    a.release.notify_waiters();
    pending.await.unwrap();
    assert_eq!(cdn.app.media.count().await, 0);
    assert!(!play(&cdn).await.status().is_success());
    cleanup([a, b, cdn]).await;
}
#[tokio::test]
async fn local_configured_stream_keeps_precedence_over_source_groups() {
    let d = tempfile::tempdir().unwrap();
    let (a, b, cdn) = setup(d.path(), "m4s").await;
    cdn.app
        .config
        .put("streams", "region/news", stream("local-content"))
        .unwrap();
    assert_eq!(play(&cdn).await.status(), 200);
    assert_eq!(
        cdn.app.media.stats("region/news").await["input_protocol"],
        "testsrc"
    );
    cleanup([a, b, cdn]).await;
}

#[tokio::test]
async fn concurrent_first_viewers_share_one_private_pull_and_never_start_unused_replica() {
    let d = tempfile::tempdir().unwrap();
    let (a, b, cdn) = setup(d.path(), "m4f").await;
    cdn.app
        .config
        .put(
            "sources",
            "a",
            json!({"public_payload_url":"http://127.0.0.1:19996/unused-public"}),
        )
        .unwrap();
    let (r1, r2, r3) = tokio::join!(play(&cdn), play(&cdn), play(&cdn));
    assert_eq!(r1.status(), 200);
    assert_eq!(r2.status(), 200);
    assert_eq!(r3.status(), 200);
    assert_eq!(cdn.app.media.count().await, 1);
    assert_eq!(a.app.media.count().await, 1);
    assert_eq!(b.app.media.count().await, 0);
    assert_eq!(selected(&cdn).await["stats"]["upstream_source"], "a");
    cleanup([a, b, cdn]).await;
}
#[tokio::test]
async fn removing_source_relationships_during_lookup_cannot_resurrect_media() {
    let d = tempfile::tempdir().unwrap();
    let (a, b, cdn) = setup(d.path(), "m4s").await;
    assert_eq!(play(&cdn).await.status(), 200);
    kill_cdn_worker(&cdn).await;
    a.mode.store(2, Ordering::Relaxed);
    let app = cdn.app.clone();
    let pending = tokio::spawn(async move { app.reconcile().await });
    tokio::time::timeout(Duration::from_secs(2), a.entered.notified())
        .await
        .unwrap();
    cdn.app.config.replace(json!({"sources":[]})).unwrap();
    cdn.app.reconcile().await;
    a.release.notify_waiters();
    pending.await.unwrap();
    assert_eq!(cdn.app.media.count().await, 0);
    assert!(!play(&cdn).await.status().is_success());
    cleanup([a, b, cdn]).await;
}
