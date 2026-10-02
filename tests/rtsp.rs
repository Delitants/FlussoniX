use flussonix::{
    rtsp,
    server::{App, Options},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;
async fn fixture(
    role: &str,
) -> (
    tempfile::TempDir,
    Arc<App>,
    String,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("c.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            role: role.into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-token"))})).unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsp://{}/owned", l.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rtsp::serve(l, app.clone(), cancel.clone()));
    (d, app, url, cancel, task)
}
async fn connect(url: &str) -> BufReader<TcpStream> {
    let u = url::Url::parse(url).unwrap();
    BufReader::new(
        TcpStream::connect((u.host_str().unwrap(), u.port().unwrap()))
            .await
            .unwrap(),
    )
}
async fn request(
    s: &mut BufReader<TcpStream>,
    method: &str,
    uri: &str,
    headers: &str,
) -> (u16, String, Vec<u8>) {
    s.get_mut()
        .write_all(format!("{method} {uri} RTSP/1.0\r\nCSeq: 1\r\n{headers}\r\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut h = vec![];
        loop {
            h.push(s.read_u8().await.unwrap());
            if h.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let h = String::from_utf8(h).unwrap();
        let status = h.split(' ').nth(1).unwrap().parse().unwrap();
        let n = h
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut b = vec![0; n];
        s.read_exact(&mut b).await.unwrap();
        (status, h, b)
    })
    .await
    .unwrap()
}
fn session(h: &str) -> String {
    h.lines()
        .find_map(|l| l.strip_prefix("Session: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .into()
}
#[tokio::test]
async fn token_denial_and_lb_role_do_not_start_workers() {
    let (_d, app, url, c, t) = fixture("standalone").await;
    let mut s = connect(&url).await;
    assert_eq!(request(&mut s, "DESCRIBE", &url, "").await.0, 403);
    assert_eq!(app.media.count().await, 0);
    c.cancel();
    t.await.unwrap().unwrap();
    app.media.stop_all().await;
    let (_d, app, url, c, t) = fixture("lb").await;
    let mut s = connect(&url).await;
    assert_eq!(
        request(&mut s, "DESCRIBE", &format!("{url}?token=owned-token"), "")
            .await
            .0,
        501
    );
    assert_eq!(app.media.count().await, 0);
    c.cancel();
    t.await.unwrap().unwrap();
}
#[tokio::test]
async fn setup_binds_session_stream_token_and_channel_pairs() {
    let (_d, app, url, c, t) = fixture("standalone").await;
    let mut s = connect(&url).await;
    let (status, _, body) = request(
        &mut s,
        "DESCRIBE",
        &format!("{url}?token=owned-token"),
        "Accept: application/sdp\r\n",
    )
    .await;
    assert_eq!(status, 200);
    let sdp = String::from_utf8(body).unwrap();
    let ids = sdp
        .lines()
        .filter_map(|l| l.strip_prefix("a=control:trackID="))
        .collect::<Vec<_>>();
    assert!(!ids.is_empty());
    let track = format!("{url}/trackID={}", ids[0]);
    assert_eq!(
        request(
            &mut s,
            "SETUP",
            &track,
            "Transport: RTP/AVP;unicast;client_port=10000-10001\r\n"
        )
        .await
        .0,
        461
    );
    let (code, h, _) = request(
        &mut s,
        "SETUP",
        &track,
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n",
    )
    .await;
    assert_eq!(code, 200);
    let session = session(&h);
    let headers =
        format!("Session: {session}\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n");
    if ids.len() > 1 {
        assert_eq!(
            request(
                &mut s,
                "SETUP",
                &format!("{url}/trackID={}", ids[1]),
                &headers
            )
            .await
            .0,
            461
        );
    }
    assert_eq!(
        request(&mut s, "PLAY", &url, "Session: wrong\r\n").await.0,
        454
    );
    assert_eq!(
        request(
            &mut s,
            "PLAY",
            &format!("{url}?token=wrong"),
            &format!("Session: {session}\r\n")
        )
        .await
        .0,
        403
    );
    assert_eq!(
        request(
            &mut s,
            "PLAY",
            &url.replace("/owned", "/other"),
            &format!("Session: {session}\r\n")
        )
        .await
        .0,
        404
    );
    assert_eq!(
        request(&mut s, "TEARDOWN", &url, &format!("Session: {session}\r\n"))
            .await
            .0,
        200
    );
    assert_eq!(
        app.media
            .ensure("owned", &app.config.effective("owned").unwrap())
            .await
            .unwrap()
            .viewers
            .load(Ordering::Relaxed),
        0
    );
    c.cancel();
    t.await.unwrap().unwrap();
    app.media.stop_all().await;
}
async fn decode(url: &str) -> std::process::Output {
    tokio::time::timeout(
        Duration::from_secs(25),
        tokio::process::Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-v",
                "error",
                "-rtsp_transport",
                "tcp",
                "-i",
                url,
                "-t",
                "3",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-threads",
                "1",
                "-f",
                "framemd5",
                "-",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("RTSP decode bounded")
    .unwrap()
}
fn assert_decoded(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let media = String::from_utf8_lossy(&output.stdout);
    assert!(media.contains("#media_type 0: video"));
    assert!(media.contains("#media_type 1: audio"));
    assert!(media.lines().filter(|l| l.starts_with("0,")).count() >= 25);
    assert!(media.lines().filter(|l| l.starts_with("1,")).count() >= 60);
}
#[tokio::test]
async fn independent_ffmpeg_decodes_two_tracks_and_viewers_share_one_worker() {
    let (_d, app, url, c, t) = fixture("standalone").await;
    let playback = format!("{url}?token=owned-token");
    let (a, b) = tokio::join!(decode(&playback), decode(&playback));
    assert_decoded(&a);
    assert_decoded(&b);
    assert_eq!(app.media.count().await, 1);
    assert!(app.rtsp_egress.load(Ordering::Relaxed) > 100000);
    let w = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    for _ in 0..20 {
        if w.viewers.load(Ordering::Relaxed) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(w.viewers.load(Ordering::Relaxed), 0);
    c.cancel();
    t.await.unwrap().unwrap();
    app.media.stop_all().await;
}
#[tokio::test]
async fn rtsp_playback_can_be_ingested_by_an_independent_worker_to_hls() {
    let (_d, app, url, c, t) = fixture("standalone").await;
    let relay_dir = tempfile::tempdir().unwrap();
    let relay = flussonix::media::Engine::new(relay_dir.path(), "ffmpeg");
    let worker = relay
        .ensure(
            "roundtrip",
            &json!({"inputs":[{"url":format!("{url}?token=owned-token")}]}),
        )
        .await
        .unwrap();
    let mut file = None;
    for _ in 0..180 {
        if let Ok(data) = relay.read("roundtrip", "index.m3u8").await {
            let manifest = String::from_utf8_lossy(&data);
            if let Some(name) = manifest
                .lines()
                .find(|l| !l.is_empty() && !l.starts_with('#'))
            {
                file = Some(name.to_string());
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        file.is_some(),
        "RTSP input HLS becomes ready: relay={} source={} egress={}",
        worker.stats(),
        app.media.stats("owned").await,
        app.rtsp_egress.load(Ordering::Relaxed)
    );
    let media = relay.read("roundtrip", &file.unwrap()).await.unwrap();
    let path = relay_dir.path().join("decode.ts");
    std::fs::write(&path, media).unwrap();
    let output = tokio::process::Command::new("ffmpeg")
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(path)
        .args([
            "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "null", "-",
        ])
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(worker.stats()["input_protocol"], "rtsp");
    relay.stop_all().await;
    c.cancel();
    t.await.unwrap().unwrap();
    app.media.stop_all().await;
}
#[tokio::test]
async fn revoked_rtsp_grant_closes_media_and_releases_viewer_ownership() {
    let (_d, app, url, c, t) = fixture("standalone").await;
    let mut s = connect(&url).await;
    let (code, _, body) =
        request(&mut s, "DESCRIBE", &format!("{url}?token=owned-token"), "").await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    let (code, h, _) = request(
        &mut s,
        "SETUP",
        &format!("{url}/trackID={id}"),
        "Transport: RTP/AVP/TCP;interleaved=0-1\r\n",
    )
    .await;
    assert_eq!(code, 200);
    let session = session(&h);
    assert_eq!(
        request(&mut s, "PLAY", &url, &format!("Session: {session}\r\n"))
            .await
            .0,
        200
    );
    let w = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    assert_eq!(w.viewers.load(Ordering::Relaxed), 1);
    app.config
        .put("streams", "owned", json!({"disabled":true}))
        .unwrap();
    app.reconcile().await;
    let mut buffered = vec![];
    tokio::time::timeout(Duration::from_secs(3), s.read_to_end(&mut buffered))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(w.viewers.load(Ordering::Relaxed), 0);
    assert_eq!(app.media.count().await, 0);
    c.cancel();
    t.await.unwrap().unwrap();
}
#[tokio::test]
async fn cdn_rtsp_output_uses_authenticated_private_native_pull() {
    for transport in ["m4s", "m4f"] {
        let (_source_dir, source, source_rtsp, source_cancel, source_task) =
            fixture("source").await;
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_url = format!("http://{}", http.local_addr().unwrap());
        let source_app = source.clone();
        let http_task = tokio::spawn(async move {
            axum::serve(http, flussonix::server::router(source_app))
                .await
                .unwrap()
        });
        let (_cdn_dir, cdn, cdn_url, cdn_cancel, cdn_task) = fixture("cdn").await;
        cdn.config.delete("streams", "owned").unwrap();
        cdn.config.put("sources","origin",json!({"api_url":source_url,"private_payload_url":source_url,"flussonix_transport":transport})).unwrap();
        let mut denied = connect(&cdn_url).await;
        assert_eq!(request(&mut denied, "DESCRIBE", &cdn_url, "").await.0, 403);
        assert_eq!(cdn.media.count().await, 0);
        assert_eq!(source.media.count().await, 0);
        let output = decode(&format!("{cdn_url}?token=owned-token")).await;
        assert_decoded(&output);
        assert_eq!(cdn.media.count().await, 1);
        assert_eq!(source.media.count().await, 1);
        let stat = cdn.media.stats("owned").await;
        assert_eq!(stat["input_protocol"], transport);
        cdn_cancel.cancel();
        cdn_task.await.unwrap().unwrap();
        cdn.media.stop_all().await;
        source_cancel.cancel();
        source_task.await.unwrap().unwrap();
        source.media.stop_all().await;
        http_task.abort();
        let _ = source_rtsp;
    }
}
#[tokio::test]
async fn rtsp_on_play_callback_receives_protocol_and_denies_before_startup() {
    use axum::{extract::Query, routing::get};
    use std::collections::HashMap;
    let (tx, mut observed) = tokio::sync::mpsc::channel(1);
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend = format!("http://{}/auth", l.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            l,
            axum::Router::new().route(
                "/auth",
                get(move |Query(q): Query<HashMap<String, String>>| {
                    let tx = tx.clone();
                    async move {
                        tx.send(q).await.unwrap();
                        axum::http::StatusCode::FORBIDDEN
                    }
                }),
            ),
        )
        .await
        .unwrap()
    });
    let (_d, app, url, c, t) = fixture("standalone").await;
    app.config
        .put("streams", "owned", json!({"on_play":backend}))
        .unwrap();
    let mut socket = connect(&url).await;
    assert_eq!(
        request(
            &mut socket,
            "DESCRIBE",
            &format!("{url}?token=owned-token&customer=lab"),
            ""
        )
        .await
        .0,
        403
    );
    let q = observed.recv().await.unwrap();
    assert_eq!(q["proto"], "rtsp");
    assert_eq!(q["ip"], "127.0.0.1");
    assert_eq!(q["name"], "owned");
    assert_eq!(q["token"], "owned-token");
    assert!(q["qs"].contains("customer=lab"));
    assert_eq!(app.media.count().await, 0);
    c.cancel();
    t.await.unwrap().unwrap();
    task.abort();
}
