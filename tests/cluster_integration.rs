use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
async fn launch(
    dir: &std::path::Path,
    name: &str,
    role: &str,
) -> (Arc<App>, String, tokio::task::JoinHandle<()>) {
    let app = App::new(
        dir.join(format!("{name}.json")),
        dir.join(name),
        Options {
            node_name: name.into(),
            role: role.into(),
            uplink_interface: "process".into(),
            admin_password: "management-secret".into(),
            peer_key: "cluster-peer-key".into(),
            ..Default::default()
        },
    )
    .unwrap();
    // These in-process routers do not run main's sampler task; warm up an actual interval.
    tokio::time::sleep(Duration::from_millis(100)).await;
    app.sample_metrics();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let a = app.clone();
    let task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        use std::future::IntoFuture;
        let server = axum::serve(listener, router(a.clone())).into_future();
        tokio::pin!(server);
        let sampling = async {
            loop {
                interval.tick().await;
                a.sample_metrics();
            }
        };
        tokio::select! {result=server=>result.unwrap(),_=sampling=>{}}
    });
    (app, url, task)
}
#[tokio::test]
async fn source_cdn_balancer_auth_and_private_pull_decode_real_hls() {
    let d = tempfile::tempdir().unwrap();
    let (source, source_url, source_task) = launch(d.path(), "source", "source").await;
    let (cdn, cdn_url, cdn_task) = launch(d.path(), "cdn", "cdn").await;
    let (lb, lb_url, lb_task) = launch(d.path(), "lb", "lb").await;
    source.config.put("streams","region/news",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer-test-token"))})).unwrap();
    for app in [&cdn, &lb] {
        app.config
            .put(
                "sources",
                "origin",
                json!({"api_url":source_url,"private_payload_url":source_url}),
            )
            .unwrap();
    }
    lb.config
        .put(
            "peers",
            "edge",
            json!({"api_url":cdn_url,"public_payload_url":cdn_url,"private_payload_url":cdn_url}),
        )
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    assert_eq!(
        client
            .get(format!("{lb_url}/region/news/index.m3u8"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(source.media.count().await, 0);
    assert_eq!(cdn.media.count().await, 0);
    let response = client
        .get(format!(
            "{lb_url}/region/news/index.m3u8?token=viewer-test-token"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    let location = response.headers()["location"].to_str().unwrap().to_owned();
    assert!(location.starts_with(&cdn_url));
    assert!(!location.contains("cluster-peer-key"));
    let playlist = client.get(&location).send().await.unwrap();
    assert_eq!(
        playlist.status(),
        302,
        "redeem ticket to a clean reload URL"
    );
    let canonical = playlist.headers()["location"].to_str().unwrap().to_owned();
    assert!(!canonical.contains("flussonix_ticket"));
    let canonical = format!("{cdn_url}{canonical}");
    let playlist = client.get(&canonical).send().await.unwrap();
    assert_eq!(playlist.status(), 200);
    let playlist = playlist.text().await.unwrap();
    assert_eq!(
        client.get(&canonical).send().await.unwrap().status(),
        200,
        "playlist reload must succeed"
    );
    assert!(playlist.contains("token=viewer-test-token"));
    assert_eq!(source.media.count().await, 1);
    assert_eq!(cdn.media.count().await, 1);
    assert_eq!(
        client.get(&location).send().await.unwrap().status(),
        503,
        "tickets are single use"
    );
    let mut subscriber_urls = Vec::new();
    for _ in 0..8 {
        subscriber_urls.push(format!(
            "{cdn_url}/region/news/index.m3u8?token=viewer-test-token"
        ));
    }
    for r in
        futures_util::future::join_all(subscriber_urls.into_iter().map(|u| client.get(u).send()))
            .await
    {
        assert_eq!(r.unwrap().status(), 200)
    }
    assert_eq!(cdn.media.count().await, 1);
    let segment = playlist
        .lines()
        .find(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap();
    let denied = client
        .get(format!(
            "{cdn_url}/region/news/{}",
            segment.split('?').next().unwrap()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    let data = client
        .get(format!("{cdn_url}/region/news/{segment}"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let file = d.path().join("actual.ts");
    tokio::fs::write(&file, data).await.unwrap();
    let result = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
        ])
        .arg(file)
        .output()
        .await
        .unwrap();
    assert!(result.status.success());
    let info: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert!(
        info["streams"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["codec_name"] == "h264")
    );
    for app in [&source, &cdn, &lb] {
        app.media.stop_all().await;
    }
    for task in [source_task, cdn_task, lb_task] {
        task.abort();
    }
}
#[tokio::test]
async fn independent_m4f_and_m4s_http_outputs_can_be_ingested_and_decoded() {
    let d = tempfile::tempdir().unwrap();
    let (source, url, task) = launch(d.path(), "wire-source", "source").await;
    let (cdn, _, cdn_task) = launch(d.path(), "wire-cdn", "cdn").await;
    source
        .config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    for protocol in ["m4f", "m4s"] {
        let input = url.replacen("http://", &format!("{protocol}://"), 1) + "/owned";
        let worker = cdn
            .media
            .ensure(protocol, &json!({"inputs":[{"url":input}]}))
            .await
            .unwrap();
        for _ in 0..150 {
            if cdn.media.read(protocol, "index.m3u8").await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let manifest = cdn
            .media
            .read(protocol, "index.m3u8")
            .await
            .expect("wire input must produce HLS");
        let manifest = String::from_utf8_lossy(&manifest);
        let segment = manifest
            .lines()
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .unwrap();
        let bytes = cdn.media.read(protocol, segment).await.unwrap();
        let file = d.path().join(format!("{protocol}.ts"));
        tokio::fs::write(&file, bytes).await.unwrap();
        let decode = tokio::process::Command::new("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(file)
            .args(["-t", "1", "-f", "null", "-"])
            .output()
            .await
            .unwrap();
        assert!(decode.status.success(), "{protocol} input must decode");
        assert!(worker.wire.has_info());
        cdn.media.stop(protocol).await;
    }
    if let Ok(path) = std::env::var("FLUSSONIX_EXPORT_OWNED_M4F") {
        let worker = source
            .media
            .ensure("owned", &source.config.effective("owned").unwrap())
            .await
            .unwrap();
        let (initial, _) = worker.wire.signal_subscribe();
        let line = String::from_utf8_lossy(initial.last().unwrap());
        let stamp = line
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        let segment = worker.wire.segment(&format!("{stamp}.m4f")).unwrap();
        tokio::fs::write(path, segment).await.unwrap();
    }
    source.media.stop_all().await;
    cdn.media.stop_all().await;
    task.abort();
    cdn_task.abort();
}

#[tokio::test]
async fn named_source_auth_policy_is_portable_and_cannot_use_a_different_edge_backend() {
    let d = tempfile::tempdir().unwrap();
    let auth_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth_url = format!("http://{}/auth", auth_listener.local_addr().unwrap());
    let auth_task = tokio::spawn(async move {
        axum::serve(
            auth_listener,
            axum::Router::new().route(
                "/auth",
                axum::routing::get(|| async { axum::http::StatusCode::OK }),
            ),
        )
        .await
        .unwrap()
    });
    let (source, source_url, source_task) = launch(d.path(), "policy-source", "source").await;
    let (cdn, cdn_url, cdn_task) = launch(d.path(), "policy-cdn", "cdn").await;
    source
        .config
        .put("auth_backends", "billing", json!({"url":auth_url}))
        .unwrap();
    source
        .config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":"auth://billing"}),
        )
        .unwrap();
    cdn.config
        .put(
            "auth_backends",
            "billing",
            json!({"url":"http://127.0.0.1:1/wrong"}),
        )
        .unwrap();
    cdn.config
        .put(
            "sources",
            "origin",
            json!({"api_url":source_url,"private_payload_url":source_url}),
        )
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    assert_eq!(
        client
            .get(format!("{cdn_url}/owned/index.m3u8"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    for app in [&source, &cdn] {
        app.media.stop_all().await;
    }
    for task in [source_task, cdn_task, auth_task] {
        task.abort();
    }
}
