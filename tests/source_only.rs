//! Breaks caught: only is rejected or used as a visibility filter; eager workers
//! are missing, exclusions lose precedence, or untrusted inventory leaks config.
use axum::{body::Body, http::Request};
use flussonix::{
    config::ConfigStore,
    server::{App, Options, router},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

#[test]
fn only_patterns_persist_and_invalid_changes_are_atomic() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    s.put("sources","owned",json!({"api_url":"http://origin.invalid","prefix":"region/eu","only":["news","sports/*","日本語"]})).unwrap();
    let saved = s.snapshot();
    assert_eq!(ConfigStore::open(&p).unwrap().snapshot(), saved);
    let disk = std::fs::read(&p).unwrap();
    let revision = s.revision();
    for value in [
        json!("news"),
        json!([42]),
        json!([""]),
        json!(["*"]),
        json!(["region*"]),
        json!(["../news"]),
        json!(["a?b"]),
        json!(["é".repeat(129)]),
        json!(vec!["news"; 1025]),
    ] {
        assert!(
            s.put("sources", "owned", json!({"only":value})).is_err(),
            "{value}"
        );
        assert_eq!(s.snapshot(), saved);
        assert_eq!(s.revision(), revision);
        assert_eq!(std::fs::read(&p).unwrap(), disk);
    }
    for kind in ["streams", "templates", "peers", "auth_backends"] {
        assert!(
            s.put(
                kind,
                "other",
                json!({"api_url":"http://origin.invalid","only":["news"]})
            )
            .is_err()
        );
    }
    assert!(
        s.put("sources", "owned", json!({"only":null}))
            .unwrap()
            .get("only")
            .is_none()
    );
    s.put("sources", "owned", json!({"only":[]})).unwrap();
    assert_eq!(s.snapshot()["sources"][0]["only"], json!([]));
}
fn app(path: &std::path::Path, name: &str, role: &str) -> Arc<App> {
    App::new(
        path.join(format!("{name}.json")),
        path.join(name),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            role: role.into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap()
}
#[tokio::test]
async fn native_inventory_is_authenticated_and_conveys_no_inputs_or_viewer_secrets() {
    let d = tempfile::tempdir().unwrap();
    let a = app(d.path(), "source", "source");
    a.config
        .put("templates", "t", json!({"static":false}))
        .unwrap();
    a.config.put("streams","region/news",json!({"template":"t","inputs":[{"url":"http://user:secret@origin.invalid/live.ts"}],"flussonix_token_sha256":"a".repeat(64)})).unwrap();
    a.config
        .put(
            "streams",
            "disabled",
            json!({"disabled":true,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let svc = router(a);
    let absent = svc
        .clone()
        .oneshot(
            Request::get("/flussonix/api/v1/source-streams")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(absent.status(), 401);
    let response = svc
        .clone()
        .oneshot(
            Request::get("/flussonix/api/v1/source-streams")
                .header("X-Flussonix-Peer", "owned-peer-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        v,
        json!({"streams":[{"name":"disabled","static":true,"disabled":true},{"name":"region/news","static":false,"disabled":false}]})
    );
    let post = svc
        .oneshot(
            Request::post("/flussonix/api/v1/source-streams")
                .header("X-Flussonix-Peer", "owned-peer-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post.status(), 405);
}
async fn node(a: Arc<App>) -> (String, tokio::task::JoinHandle<()>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let u = format!("http://{}", l.local_addr().unwrap());
    (
        u,
        tokio::spawn(async move { axum::serve(l, router(a)).await.unwrap() }),
    )
}
#[tokio::test]
async fn only_starts_native_media_without_viewers_and_preserves_demand_visibility() {
    let d = tempfile::tempdir().unwrap();
    let source = app(d.path(), "source", "source");
    let cdn = app(d.path(), "cdn", "cdn");
    for (name, static_) in [
        ("region/eu/news", true),
        ("region/eu/weather/live", true),
        ("region/eu/sports/live", true),
        ("region/eu/dynamic", false),
        ("region/other/news", true),
    ] {
        source.config.put("streams",name,json!({"static":static_,"inputs":[{"url":"testsrc://"}],"flussonix_content_id":"owned-content"})).unwrap();
    }
    source
        .config
        .put(
            "streams",
            "region/eu/news",
            json!({"flussonix_token_sha256":"a".repeat(64)}),
        )
        .unwrap();
    let (url, task) = node(source.clone()).await;
    cdn.config.put("sources","owned",json!({"api_url":url,"private_payload_url":url,"flussonix_transport":"m4s","prefix":"region/eu","only":["news","weather/*","sports/*","dynamic"],"except":["sports/*"]})).unwrap();
    source
        .media
        .ensure(
            "region/eu/news",
            &source.config.effective("region/eu/news").unwrap(),
        )
        .await
        .unwrap();
    source
        .media
        .ensure(
            "region/eu/weather/live",
            &source.config.effective("region/eu/weather/live").unwrap(),
        )
        .await
        .unwrap();
    let result = async {
        cdn.reconcile().await;
        for _ in 0..100 {
            if cdn.media.ready("region/eu/news").await
                && cdn.media.ready("region/eu/weather/live").await
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let warm = cdn.media.ready("region/eu/news").await
            && cdn.media.ready("region/eu/weather/live").await;
        let mut forbidden = cdn
            .media
            .workers()
            .await
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>();
        forbidden.sort();
        let (edge, edge_task) = node(cdn.clone()).await;
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(12))
            .build()
            .unwrap();
        let denied = client
            .get(format!("{edge}/region/eu/news/index.m3u8"))
            .send()
            .await
            .map(|r| r.status())
            .map_err(|e| e.to_string());
        let response = client
            .get(format!("{edge}/region/eu/dynamic/index.m3u8"))
            .send()
            .await;
        let status = response
            .as_ref()
            .map(|r| r.status())
            .map_err(|e| e.to_string());
        drop(response);
        let demand_started = cdn
            .media
            .workers()
            .await
            .iter()
            .any(|(name, _)| name == "region/eu/dynamic");
        edge_task.abort();
        let _ = edge_task.await;
        (warm, forbidden, status, demand_started, denied)
    }
    .await;
    for a in [&cdn, &source] {
        for (name, _) in a.media.workers().await {
            a.media.stop(&name).await;
        }
    }
    task.abort();
    let _ = task.await;
    assert!(
        result.0,
        "selected static media never became ready without a viewer"
    );
    assert_eq!(result.1, vec!["region/eu/news", "region/eu/weather/live"]);
    assert!(
        matches!(result.4,Ok(status) if status==403),
        "warm peer pull bypassed viewer policy: {:?}",
        result.4
    );
    assert!(
        matches!(result.2,Ok(status) if status==200 || status==503),
        "on-demand request failed: {:?}",
        result.2
    );
    assert!(
        result.3,
        "only incorrectly hid a nonmatching on-demand stream"
    );
}

// Break caught: eager inventory polling can grow without a configuration bound.
#[test]
fn eager_source_limit_rejects_config_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let s = ConfigStore::open(d.path().join("config.json")).unwrap();
    let before = s.snapshot();
    let sources=(0..65).map(|i|json!({"hostname":format!("source-{i}"),"api_url":"http://origin.invalid","only":["news"]})).collect::<Vec<_>>();
    assert!(s.replace(json!({"sources":sources})).is_err());
    assert_eq!(s.snapshot(), before);
}
async fn activation_status(a: &Arc<App>) -> Value {
    let response = router(a.clone())
        .oneshot(
            Request::get("/flussonix/api/v1/node")
                .header("X-Flussonix-Peer", "owned-peer-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    serde_json::from_slice::<Value>(
        &axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap(),
    )
    .unwrap()["source_activation"]
        .clone()
}
#[tokio::test]
async fn malformed_inventory_is_rejected_as_a_whole_before_metadata_queries() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for raw in [json!([]).to_string(),json!({"streams":[{"name":"good","static":true,"disabled":false},{"name":"../bad","static":true,"disabled":false}]}).to_string(),json!({"streams":[{"name":"good","static":"yes","disabled":false}]}).to_string(),json!({"streams":[{"name":"good","static":true,"disabled":false},{"name":"good","static":true,"disabled":false}]}).to_string(),"x".repeat(1024*1024+1)] {
  let d=tempfile::tempdir().unwrap();let cdn=app(d.path(),"cdn","cdn");let count=Arc::new(AtomicUsize::new(0));let c=count.clone();
  let routes=axum::Router::new().route("/flussonix/api/v1/source-streams",axum::routing::get(move |headers:axum::http::HeaderMap|{let raw=raw.clone();async move {assert_eq!(headers["X-Flussonix-Peer"],"owned-peer-secret");raw}})).route("/flussonix/api/v1/stream/{*name}",axum::routing::get(move ||{c.fetch_add(1,Ordering::SeqCst);async {axum::http::StatusCode::NOT_FOUND}}));
  let l=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let url=format!("http://{}",l.local_addr().unwrap());let task=tokio::spawn(async move {axum::serve(l,routes).await.unwrap()});
  cdn.config.put("sources","owned",json!({"api_url":url,"only":["good"]})).unwrap();cdn.reconcile().await;let status=activation_status(&cdn).await;let requests=count.load(Ordering::SeqCst);task.abort();let _=task.await;
  assert_eq!(status["wanted_streams"],0);assert_eq!(status["sources"][0]["last_error"],"invalid");assert_eq!(requests,0,"partially valid inventory was installed");
 }
}
#[tokio::test]
async fn changed_source_fences_inflight_inventory_and_lb_never_polls() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let d = tempfile::tempdir().unwrap();
    let cdn = app(d.path(), "cdn", "cdn");
    let lb = app(d.path(), "lb", "lb");
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let (e, r, c) = (entered.clone(), release.clone(), count.clone());
    let routes = axum::Router::new().route(
        "/flussonix/api/v1/source-streams",
        axum::routing::get(move || {
            let (e, r, c) = (e.clone(), r.clone(), c.clone());
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                e.notify_one();
                r.notified().await;
                axum::Json(json!({"streams":[{"name":"news","static":true,"disabled":false}]}))
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(l, routes).await.unwrap() });
    for a in [&cdn, &lb] {
        a.config
            .put("sources", "owned", json!({"api_url":url,"only":["news"]}))
            .unwrap();
    }
    lb.reconcile().await;
    let lb_status = activation_status(&lb).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let a = cdn.clone();
    let job = tokio::spawn(async move { a.reconcile().await });
    let arrived = tokio::time::timeout(Duration::from_secs(2), entered.notified()).await;
    cdn.config
        .put("sources", "owned", json!({"only":[]}))
        .unwrap();
    release.notify_one();
    job.await.unwrap();
    let status = activation_status(&cdn).await;
    server.abort();
    let _ = server.await;
    assert!(arrived.is_ok(), "inventory was never requested");
    assert_eq!(status["wanted_streams"], 0);
    assert_eq!(lb_status["wanted_streams"], 0);
    assert!(lb.media.workers().await.is_empty());
}

// Break caught: inventory redirects disclose the peer credential to another endpoint.
#[tokio::test]
async fn inventory_redirect_does_not_contact_its_destination() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let d = tempfile::tempdir().unwrap();
    let cdn = app(d.path(), "cdn", "cdn");
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    let trap = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = format!("http://{}/trap", trap.local_addr().unwrap());
    let trapped = tokio::spawn(async move {
        axum::serve(
            trap,
            axum::Router::new().fallback(move || {
                c.fetch_add(1, Ordering::SeqCst);
                async { "unexpected" }
            }),
        )
        .await
        .unwrap()
    });
    let routes = axum::Router::new().route(
        "/flussonix/api/v1/source-streams",
        axum::routing::get(move || {
            let d = destination.clone();
            async move { (axum::http::StatusCode::FOUND, [("Location", d)]) }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(l, routes).await.unwrap() });
    cdn.config
        .put("sources", "owned", json!({"api_url":url,"only":["news"]}))
        .unwrap();
    cdn.reconcile().await;
    let status = activation_status(&cdn).await;
    server.abort();
    trapped.abort();
    let _ = server.await;
    let _ = trapped.await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(status["sources"][0]["last_error"], "unavailable");
    assert_eq!(status["wanted_streams"], 0);
}

// Break caught: aggregate source selections grow past the admitted cache budget
// without an operator-visible capacity error.
#[tokio::test]
async fn aggregate_inventory_capacity_is_reported_without_unbounded_growth() {
    let d = tempfile::tempdir().unwrap();
    let cdn = app(d.path(), "cdn", "cdn");
    let mut servers = Vec::new();
    for (identity, count) in [("a", 10000), ("b", 1)] {
        let body = json!({"streams":(0..count).map(|i|json!({"name":format!("bucket/{identity}-{i}"),"static":true,"disabled":false})).collect::<Vec<_>>()});
        let routes = axum::Router::new().route(
            "/flussonix/api/v1/source-streams",
            axum::routing::get(move || {
                let body = body.clone();
                async move { axum::Json(body) }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        servers.push(tokio::spawn(async move {
            axum::serve(l, routes).await.unwrap()
        }));
        cdn.config
            .put(
                "sources",
                identity,
                json!({"api_url":url,"only":["bucket/*"]}),
            )
            .unwrap();
    }
    cdn.reconcile().await;
    let status = activation_status(&cdn).await;
    for server in servers {
        server.abort();
        let _ = server.await;
    }
    assert!(status["wanted_streams"].as_u64().unwrap() <= 10000);
    assert!(
        status["sources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["last_error"] == "capacity"),
        "{status}"
    );
    assert!(cdn.media.workers().await.is_empty());
}

// Break caught: concurrent source edits start a second inventory batch above the node-wide limit.
#[tokio::test]
async fn concurrent_reconciliation_keeps_inventory_polling_within_node_bound() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let d = tempfile::tempdir().unwrap();
    let cdn = app(d.path(), "cdn", "cdn");
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (a, p, e, r) = (
        active.clone(),
        peak.clone(),
        entered.clone(),
        release.clone(),
    );
    let routes = axum::Router::new().route(
        "/flussonix/api/v1/source-streams",
        axum::routing::get(move || {
            let (a, p, e, r) = (a.clone(), p.clone(), e.clone(), r.clone());
            async move {
                let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(n, Ordering::SeqCst);
                if n >= 16 {
                    e.notify_one();
                }
                r.notified().await;
                a.fetch_sub(1, Ordering::SeqCst);
                axum::Json(json!({"streams":[]}))
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(l, routes).await.unwrap() });
    for n in 0..16 {
        cdn.config
            .put(
                "sources",
                &format!("source-{n}"),
                json!({"api_url":url,"only":["news"]}),
            )
            .unwrap();
    }
    let c = cdn.clone();
    let first = tokio::spawn(async move { c.reconcile().await });
    let ready = tokio::time::timeout(Duration::from_millis(500), entered.notified()).await;
    cdn.config
        .put("sources", "source-0", json!({"only":["fresh"]}))
        .unwrap();
    let c = cdn.clone();
    let second = tokio::spawn(async move { c.reconcile().await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let observed = peak.load(Ordering::SeqCst);
    release.notify_waiters();
    first.await.unwrap();
    second.await.unwrap();
    server.abort();
    let _ = server.await;
    cdn.media.stop_all().await;
    assert!(ready.is_ok(), "first inventory batch did not enter");
    assert!(
        observed <= 16,
        "concurrent activation launched {observed} inventory polls"
    );
    assert_eq!(activation_status(&cdn).await["wanted_streams"], 0);
}
