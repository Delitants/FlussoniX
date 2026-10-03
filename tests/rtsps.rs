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
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};
use tokio_util::sync::CancellationToken;
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;
async fn fixture() -> (
    tempfile::TempDir,
    Certificates,
    Arc<App>,
    String,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let d = tempfile::tempdir().unwrap();
    let cert = Certificates::new();
    let app = App::new(
        d.path().join("c.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-token"))})).unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsps://{}/owned", l.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rtsp::serve_tls(
        l,
        app.clone(),
        cancel.clone(),
        cert.server(),
    ));
    (d, cert, app, url, cancel, task)
}
async fn connect(
    url: &str,
    cert: &Certificates,
) -> BufReader<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = url::Url::parse(url).unwrap();
    let socket = TcpStream::connect(("127.0.0.1", url.port().unwrap()))
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        TlsConnector::from(cert.client())
            .connect(ServerName::try_from("localhost").unwrap(), socket),
    )
    .await
    .unwrap();
    assert!(result.is_ok(), "listener must negotiate TLS before RTSP");
    BufReader::new(result.unwrap())
}
async fn request<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    s: &mut BufReader<S>,
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
        let code = h.split(' ').nth(1).unwrap().parse().unwrap();
        let len = h
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut body = vec![0; len];
        s.read_exact(&mut body).await.unwrap();
        (code, h, body)
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
#[test]
fn tls_server_material_accepts_valid_chain_and_rejects_malformed_or_wrong_key() {
    let c = Certificates::new();
    assert!(
        rtsp::tls::server(&c.cert, &c.key).is_ok(),
        "valid TLS material must load"
    );
    let malformed = c.dir.path().join("malformed.pem");
    std::fs::write(&malformed, b"not a certificate").unwrap();
    assert!(rtsp::tls::server(&malformed, &c.key).is_err());
    assert!(rtsp::tls::server(&c.cert, &c.dir.path().join("ca.key")).is_err());
}
#[tokio::test]
async fn tls_authorization_and_udp_rejection_precede_playback() {
    let (_d, c, app, url, cancel, task) = fixture().await;
    let mut s = connect(&url, &c).await;
    assert_eq!(request(&mut s, "DESCRIBE", &url, "").await.0, 403);
    assert_eq!(app.media.count().await, 0);
    let (code, _, body) =
        request(&mut s, "DESCRIBE", &format!("{url}?token=owned-token"), "").await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    assert!(sdp.contains("H264") && sdp.contains("MPEG4-GENERIC"));
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    assert_eq!(
        request(
            &mut s,
            "SETUP",
            &format!("{url}/trackID={id}"),
            "Transport: RTP/AVP;unicast;client_port=30000-30001\r\n"
        )
        .await
        .0,
        461
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
    app.media.stop_all().await;
}
#[tokio::test]
async fn independent_tls_client_decodes_both_tracks_using_shared_worker() {
    let (_d, c, app, url, cancel, task) = fixture().await;
    let mut s = connect(&url, &c).await;
    assert_eq!(request(&mut s, "OPTIONS", "*", "").await.0, 200);
    drop(s);
    let mut ffmpeg = tokio::process::Command::new("ffmpeg");
    // FFmpeg's RTSP demuxer does not expose TLS protocol options. This
    // independent decoder checks encrypted wire compatibility; the Rustls
    // client above separately checks certificate trust. Product ingest must
    // always use its verified TLS bridge.
    ffmpeg
        .args(["-nostdin", "-v", "error", "-rtsp_transport", "tcp"])
        .args([
            "-i",
            &format!("{url}?token=owned-token"),
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
        .kill_on_drop(true);
    let out = tokio::time::timeout(Duration::from_secs(25), ffmpeg.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        out.status.success() && out.stderr.is_empty(),
        "independent RTSPS decode: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout
            .split(|b| *b == b'\n')
            .filter(|l| l.starts_with(b"0,"))
            .count()
            >= 25
    );
    assert!(
        out.stdout
            .split(|b| *b == b'\n')
            .filter(|l| l.starts_with(b"1,"))
            .count()
            >= 60
    );
    assert_eq!(app.media.count().await, 1);
    assert!(app.rtsp_egress.load(Ordering::Relaxed) > 100000);
    cancel.cancel();
    task.await.unwrap().unwrap();
    app.media.stop_all().await;
}
#[tokio::test]
async fn tls_grant_revocation_closes_interleaving_and_reclaims_viewer() {
    let (_d, c, app, url, cancel, task) = fixture().await;
    let mut s = connect(&url, &c).await;
    let (_, _, body) = request(&mut s, "DESCRIBE", &format!("{url}?token=owned-token"), "").await;
    let sdp = String::from_utf8(body).unwrap();
    let track = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    let (_, h, _) = request(
        &mut s,
        "SETUP",
        &format!("{url}/trackID={track}"),
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n",
    )
    .await;
    let id = session(&h);
    assert_eq!(
        request(&mut s, "PLAY", &url, &format!("Session: {id}\r\n"))
            .await
            .0,
        200
    );
    let mut packet = [0; 4];
    s.read_exact(&mut packet).await.unwrap();
    assert_eq!(packet[0], b'$');
    let auth = app.playback_auth.snapshots()[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(app.playback_auth.revoke(&auth));
    let mut rest = vec![];
    let result = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut rest)).await;
    assert!(result.is_ok(), "revocation must interrupt TLS write/read");
    let worker = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    cancel.cancel();
    task.await.unwrap().unwrap();
    app.media.stop_all().await;
}
#[tokio::test]
async fn stalled_tls_handshake_is_bounded_and_shutdown_cancels_it() {
    let (_d, _c, app, url, cancel, task) = fixture().await;
    let u = url::Url::parse(&url).unwrap();
    let mut stalled = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(10), stalled.read(&mut byte)).await;
    assert!(
        matches!(result, Ok(Ok(0))),
        "stalled TLS handshake must expire"
    );
    assert_eq!(app.media.count().await, 0);
    let mut second = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), second.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0))
            || matches!(closed, Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset)
    );
    app.media.stop_all().await;
}

#[tokio::test]
async fn plain_listener_does_not_accept_rtsps_uri_or_tls_fallback() {
    let (_d, c, app, url, cancel, task) = fixture().await;
    let u = url::Url::parse(&url).unwrap();
    let mut clear = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    clear
        .write_all(
            format!("DESCRIBE {url}?token=owned-token RTSP/1.0\r\nCSeq: 1\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut data = vec![];
    let _ = tokio::time::timeout(Duration::from_secs(2), clear.read_to_end(&mut data)).await;
    assert!(!data.starts_with(b"RTSP/1.0 200"));
    assert_eq!(app.media.count().await, 0);
    let mut tls = connect(&url, &c).await;
    assert_eq!(request(&mut tls, "OPTIONS", "*", "").await.0, 200);
    cancel.cancel();
    task.await.unwrap().unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = l.local_addr().unwrap();
    let plain_cancel = CancellationToken::new();
    let plain = tokio::spawn(rtsp::serve(l, app.clone(), plain_cancel.clone()));
    let mut s = BufReader::new(TcpStream::connect(address).await.unwrap());
    assert_eq!(
        request(
            &mut s,
            "DESCRIBE",
            &format!("rtsps://{address}/owned?token=owned-token"),
            ""
        )
        .await
        .0,
        400
    );
    assert_eq!(app.media.count().await, 0);
    plain_cancel.cancel();
    plain.await.unwrap().unwrap();
    app.media.stop_all().await;
}

#[tokio::test]
async fn verified_rtsps_input_can_be_ingested_by_an_independent_worker_to_hls() {
    let (_d, cert, app, url, c, t) = fixture().await;
    let relay_dir = tempfile::tempdir().unwrap();
    let relay = flussonix::media::Engine::new(relay_dir.path(), "ffmpeg");
    let worker = relay
        .ensure(
            "roundtrip",
            &json!({"inputs":[{"url":format!("{url}?token=owned-token"),"flussonix_tls_ca":cert.ca}]}),
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
    assert_eq!(worker.stats()["input_protocol"], "rtsps");
    relay.stop_all().await;
    c.cancel();
    t.await.unwrap().unwrap();
    app.media.stop_all().await;
}
