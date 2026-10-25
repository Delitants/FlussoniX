use axum::{
    extract::Request,
    middleware::{self, Next},
};
use flussonix::{
    rtsp,
    server::{App, Options, router},
};
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

async fn describe(address: SocketAddr, path: &str) -> (u16, Option<String>) {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!("DESCRIBE rtsp://{address}/{path} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut headers = Vec::new();
    tokio::time::timeout(Duration::from_secs(12), async {
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(socket.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    let headers = String::from_utf8(headers).unwrap();
    let status = headers.lines().next().unwrap()[9..12].parse().unwrap();
    let location = headers
        .lines()
        .find_map(|line| line.strip_prefix("Location: ").map(str::to_owned));
    (status, location)
}

fn app(
    dir: &tempfile::TempDir,
    name: &str,
    role: &str,
    authority: Option<url::Url>,
    owns_authority: bool,
) -> Arc<App> {
    App::new(
        dir.path().join(format!("{name}-config.json")),
        dir.path().join(format!("{name}-media")),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: role.into(),
            node_name: name.into(),
            uplink_interface: "process".into(),
            iptv_data: Some(dir.path().join(format!("{name}-iptv.json"))),
            iptv_lease_authority: owns_authority,
            iptv_peer_allow_insecure_loopback: owns_authority,
            iptv_authority: authority.clone(),
            iptv_authority_allow_insecure_loopback: authority.is_some(),
            ..Options::default()
        },
    )
    .unwrap()
}

#[derive(Default)]
struct ReleaseGate {
    armed: AtomicBool,
    entered: Notify,
    proceed: Notify,
}

fn serve(app: Arc<App>, listener: TcpListener, gate: Arc<ReleaseGate>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let server = axum::serve(
            listener,
            router(app.clone())
                .layer(middleware::from_fn(move |request: Request, next: Next| {
                    let gate = gate.clone();
                    async move {
                        if request.uri().path().ends_with("/leases/release")
                            && gate.armed.load(Ordering::SeqCst)
                        {
                            gate.entered.notify_one();
                            gate.proceed.notified().await;
                        }
                        next.run(request).await
                    }
                }))
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future();
        tokio::pin!(server);
        loop {
            tokio::select! {
                result = &mut server => { result.unwrap(); return; }
                _ = interval.tick() => app.sample_metrics(),
            }
        }
    })
}

#[tokio::test]
async fn lb_redirect_does_not_hold_the_only_iptv_slot_needed_by_its_cdn() {
    let dir = tempfile::tempdir().unwrap();
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cdn_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_url = format!("http://{}", source_listener.local_addr().unwrap());
    let cdn_url = format!("http://{}", cdn_listener.local_addr().unwrap());
    let lb_url = format!("http://{}", lb_listener.local_addr().unwrap());
    let authority_url = url::Url::parse(&format!("{lb_url}/")).unwrap();
    let source = app(&dir, "source", "source", None, false);
    let cdn = app(&dir, "cdn", "cdn", Some(authority_url.clone()), false);
    let lb = app(&dir, "lb", "lb", Some(authority_url), true);
    source
        .config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    lb.iptv.set_enabled(true).unwrap();
    lb.iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    lb.iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    for node in [&cdn, &lb] {
        node.config
            .put(
                "sources",
                "origin",
                json!({"api_url":source_url,"private_payload_url":source_url,"flussonix_transport":"m4s"}),
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
    let gate = Arc::new(ReleaseGate::default());
    let tasks = [
        serve(source.clone(), source_listener, gate.clone()),
        serve(cdn.clone(), cdn_listener, gate.clone()),
        serve(lb.clone(), lb_listener, gate.clone()),
    ];
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    for _ in 0..40 {
        let metrics = client
            .get(format!("{cdn_url}/flussonix/api/v1/node"))
            .header("X-Flussonix-Peer", "owned-cluster-peer-123")
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if metrics["cpu"].as_f64().is_some_and(|n| n < 0.9)
            && metrics["ram"].as_f64().is_some_and(|n| n < 0.95)
            && metrics["uplink"].as_f64().is_some_and(|n| n < 0.8)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = client
        .get(format!(
            "{lb_url}/news/index.m3u8?token=private-viewer-token"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302, "load balancer did not redirect");
    let location = response.headers()["location"].to_str().unwrap().to_owned();
    assert!(location.starts_with(&cdn_url));
    let admitted = client.get(location).send().await.unwrap();
    assert_eq!(
        admitted.status(),
        302,
        "CDN could not claim the only subscriber slot after LB redirect"
    );
    let clean = admitted.headers()["location"].to_str().unwrap();
    assert_eq!(
        client
            .get(format!("{cdn_url}{clean}"))
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "CDN did not start the shared source before RTSP routing"
    );
    cdn.playback_auth.invalidate_iptv();
    tokio::time::timeout(Duration::from_secs(7), async {
        while lb.iptv_leases.active_leases() != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("HTTP test lease did not retire before RTSP redirect");
    let cdn_rtsp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let lb_rtsp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cdn_rtsp = cdn_rtsp_listener.local_addr().unwrap();
    let lb_rtsp = lb_rtsp_listener.local_addr().unwrap();
    cdn.set_rtsp_publication(Some(cdn_rtsp), None);
    lb.config
        .put(
            "peers",
            "edge",
            json!({"api_url":cdn_url,"public_payload_url":cdn_url,"private_payload_url":cdn_url,"flussonix_rtsp_url":format!("rtsp://{cdn_rtsp}")}),
        )
        .unwrap();
    let cdn_stop = CancellationToken::new();
    let lb_stop = CancellationToken::new();
    let cdn_rtsp_task = tokio::spawn(rtsp::serve(
        cdn_rtsp_listener,
        cdn.clone(),
        cdn_stop.clone(),
    ));
    let lb_rtsp_task = tokio::spawn(rtsp::serve(lb_rtsp_listener, lb.clone(), lb_stop.clone()));
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let routing = client
                .get(format!("{cdn_url}/flussonix/api/v1/rtsp-routing"))
                .header("X-Flussonix-Peer", "owned-cluster-peer-123")
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            if routing["ready"]
                .as_array()
                .is_some_and(|ready| ready.iter().any(|name| name == "news"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("RTSP CDN never became eligible");
    gate.armed.store(true, Ordering::SeqCst);
    let pending = tokio::spawn(describe(lb_rtsp, "news?token=private-viewer-token"));
    tokio::time::timeout(Duration::from_secs(7), gate.entered.notified())
        .await
        .expect("RTSP redirect did not attempt lease release");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !pending.is_finished(),
        "RTSP redirect escaped before its temporary lease was released"
    );
    gate.proceed.notify_one();
    let (status, target) = pending.await.unwrap();
    assert_eq!(status, 302, "RTSP load balancer did not redirect");
    let target = url::Url::parse(target.as_deref().unwrap()).unwrap();
    assert_eq!(target.host_str(), Some("127.0.0.1"));
    assert_eq!(target.port(), Some(cdn_rtsp.port()));
    let (status, _) = describe(cdn_rtsp, &format!("news?{}", target.query().unwrap())).await;
    assert_eq!(
        status, 200,
        "RTSP CDN could not claim the only subscriber slot"
    );
    cdn_stop.cancel();
    lb_stop.cancel();
    cdn_rtsp_task.await.unwrap().unwrap();
    lb_rtsp_task.await.unwrap().unwrap();
    for node in [&source, &cdn, &lb] {
        node.media.stop_all().await;
    }
    for task in tasks {
        task.abort();
    }
}
