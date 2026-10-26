use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use flussonix::{
    server::{App, Options, router},
    srt_playback::{self, Listener, Settings},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{net::SocketAddr, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn authority(dir: &tempfile::TempDir) -> Arc<App> {
    App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "lb".into(),
            iptv_data: Some(dir.path().join("authority-iptv.json")),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap()
}

fn cdn(dir: &tempfile::TempDir, url: url::Url) -> Arc<App> {
    App::new(
        dir.path().join("cdn-config.json"),
        dir.path().join("cdn-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-cluster-peer-123".into(),
            role: "cdn".into(),
            iptv_data: Some(dir.path().join("cdn-iptv.json")),
            iptv_authority: Some(url),
            iptv_authority_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap()
}

async fn get(app: &Arc<App>, path: &str) -> axum::response::Response {
    router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

fn srt_receiver(address: SocketAddr, token: &str, file: &std::path::Path) -> Child {
    Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-threads",
            "1",
            "-analyzeduration",
            "1000000",
            "-probesize",
            "524288",
            "-srt_streamid",
            &format!("#!::r=news,m=request,u={token}"),
            "-i",
            &format!(
                "srt://{address}?mode=caller&latency=120000&connect_timeout=1000&timeout=10000000"
            ),
            "-t",
            "1",
            "-map",
            "0",
            "-c",
            "copy",
            "-f",
            "mpegts",
        ])
        .arg(file)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

#[tokio::test]
async fn clustered_iptv_authorizes_native_m4_hls_and_ts_without_local_subscriber_data() {
    let dir = tempfile::tempdir().unwrap();
    let owner = authority(&dir);
    owner.iptv.set_enabled(true).unwrap();
    owner
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    owner
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":4}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(owner.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let edge = cdn(&dir, url);
    edge.config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    assert!(!edge.iptv.snapshot().enabled());
    tokio::time::sleep(Duration::from_secs(11)).await;
    for path in ["m4s", "m4f", "mpegts", "index.m3u8"] {
        for query in ["", "?token=wrong-token"] {
            let response = get(&edge, &format!("/news/{path}{query}")).await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}{query}");
        }
    }
    assert_eq!(edge.media.count().await, 0);
    for path in ["m4s", "m4f", "mpegts", "index.m3u8"] {
        let response = get(&edge, &format!("/news/{path}?token=private-viewer-token")).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        if path != "index.m3u8" {
            let mut body = response.into_body().into_data_stream();
            let bytes = tokio::time::timeout(Duration::from_secs(8), body.next())
                .await
                .expect("native output did not start")
                .expect("native output closed before media")
                .expect("native output failed");
            assert!(!bytes.is_empty(), "{path} delivered no media");
        }
    }
    assert_eq!(edge.media.count().await, 1);
    assert!(owner.iptv_leases.active_leases() <= 4);
    edge.media.stop_all().await;
    server.abort();
}

#[tokio::test]
async fn clustered_iptv_srt_stream_id_enforces_the_authority_subscriber_token() {
    let dir = tempfile::tempdir().unwrap();
    let owner = authority(&dir);
    owner.iptv.set_enabled(true).unwrap();
    owner
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    owner
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":"private-viewer-token","packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!("http://{}/", http_listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            http_listener,
            router(owner.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let edge = cdn(&dir, url);
    edge.config
        .put(
            "streams",
            "news",
            json!({"inputs":[{"url":"testsrc://"}],"on_play":"iptv://"}),
        )
        .unwrap();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        Settings::new(120, 4, String::new()).unwrap(),
    )
    .unwrap();
    let address = listener.address();
    let stop = CancellationToken::new();
    let serving = tokio::spawn(srt_playback::serve(listener, edge.clone(), stop.clone()));
    tokio::time::sleep(Duration::from_secs(11)).await;
    let denied_file = dir.path().join("denied.ts");
    let mut denied = srt_receiver(address, "wrong-token", &denied_file);
    let denied_status = tokio::time::timeout(Duration::from_secs(8), denied.wait())
        .await
        .expect("denied SRT viewer did not close")
        .unwrap();
    assert!(!denied_status.success());
    assert!(!denied_file.exists() || denied_file.metadata().unwrap().len() == 0);
    assert_eq!(edge.media.count().await, 0);
    let accepted_file = dir.path().join("accepted.ts");
    let mut accepted = srt_receiver(address, "private-viewer-token", &accepted_file);
    let accepted_status = tokio::time::timeout(Duration::from_secs(15), accepted.wait())
        .await
        .expect("authorized SRT viewer did not finish")
        .unwrap();
    assert!(accepted_status.success());
    assert!(accepted_file.metadata().unwrap().len() > 0);
    assert_eq!(owner.iptv_leases.active_leases(), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while owner.iptv_leases.active_leases() != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("closed continuous SRT viewer kept the subscriber slot");
    stop.cancel();
    serving.await.unwrap().unwrap();
    edge.media.stop_all().await;
    server.abort();
}
