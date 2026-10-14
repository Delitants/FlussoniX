//! Real RTSP viewers on owned, authenticated vendor cluster inputs.
#[path = "vendor_daemon_failover/fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "support/tls.rs"]
mod tls_fixture;
use fixture::Fixture;
use flussonix::rtsp;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

fn policy() -> serde_json::Value {
    json!({"only":[],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})
}
async fn listener(
    f: &Fixture,
) -> (
    String,
    CancellationToken,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsp://{}/owned", tcp.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rtsp::serve(tcp, f.app.clone(), cancel.clone()));
    (url, cancel, task)
}
async fn connect(url: &str) -> BufReader<TcpStream> {
    let u = url::Url::parse(url).unwrap();
    BufReader::new(
        TcpStream::connect((u.host_str().unwrap(), u.port().unwrap()))
            .await
            .unwrap(),
    )
}
async fn request<S: AsyncRead + AsyncWrite + Unpin>(
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
            .parse()
            .unwrap();
        let mut body = vec![0; len];
        s.read_exact(&mut body).await.unwrap();
        (code, h, body)
    })
    .await
    .unwrap()
}
async fn play<S: AsyncRead + AsyncWrite + Unpin>(s: &mut BufReader<S>, url: &str) {
    play_token(s, url, "owned-viewer").await;
}
async fn play_token<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut BufReader<S>,
    url: &str,
    token: &str,
) {
    let (code, _, body) = request(s, "DESCRIBE", &format!("{url}?token={token}"), "").await;
    assert_eq!(code, 200, "vendor RTSP admission unavailable");
    let sdp = String::from_utf8(body).unwrap();
    assert!(sdp.contains("MPA/90000"), "{sdp}");
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    setup_play(s, url, id).await;
}
async fn setup_play<S: AsyncRead + AsyncWrite + Unpin>(s: &mut BufReader<S>, url: &str, id: &str) {
    let (code, h, _) = request(
        s,
        "SETUP",
        &format!("{url}/trackID={id}"),
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n",
    )
    .await;
    assert_eq!(code, 200);
    let session = h
        .lines()
        .find_map(|l| l.strip_prefix("Session: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    assert_eq!(
        request(s, "PLAY", url, &format!("Session: {session}\r\n"))
            .await
            .0,
        200
    );
}

async fn closed<S: AsyncRead + Unpin>(s: &mut BufReader<S>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = vec![0; 8192];
        loop {
            match s.read(&mut bytes).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                    ) =>
                {
                    break;
                }
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
    })
    .await
    .expect("retired RTSP viewer stayed connected");
}
async fn packet<S: AsyncRead + Unpin>(s: &mut BufReader<S>) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(3), async {
        assert_eq!(s.read_u8().await.unwrap(), b'$');
        assert_eq!(s.read_u8().await.unwrap(), 0);
        let len = s.read_u16().await.unwrap();
        let mut body = vec![0; len as usize];
        s.read_exact(&mut body).await.unwrap();
        body
    })
    .await
    .unwrap()
}

// Removing vendor selection/admission must deny this real RTSP viewer, rather than
// silently treating a reserved vendor name as an unknown native stream.
#[tokio::test]
async fn vendor_rtsp_shares_http_packager_and_individual_viewer_revocation() {
    let f = Fixture::configured("standalone", policy(), None, false).await;
    let (url, cancel, task) = listener(&f).await;
    let mut denied = connect(&url).await;
    assert_eq!(
        request(&mut denied, "DESCRIBE", &format!("{url}?token=wrong"), "")
            .await
            .0,
        403
    );
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    let mut first = connect(&url).await;
    play(&mut first, &url).await;
    let body = packet(&mut first).await;
    assert_eq!(body[0] >> 6, 2);
    assert_eq!(body[1] & 0x7f, 14);
    assert!(body.len() > 16);
    let pid = f.app.media.stats("owned").await["pid"].as_u64().unwrap();
    let mut second = connect(&url).await;
    play(&mut second, &url).await;
    let _ = packet(&mut second).await;
    let response = f.get("/owned/index.m3u8?token=owned-viewer").await;
    assert_eq!(response.status(), 200);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    assert!(f.app.rtsp_egress.load(Ordering::Relaxed) > 0);
    // A distinct token makes the third RTSP session independently revocable;
    // query-token auth forbids it before private I/O on this protected source.
    assert_eq!(
        request(&mut denied, "DESCRIBE", &format!("{url}?token=other"), "")
            .await
            .0,
        403
    );
    for row in f.app.playback_auth.snapshots() {
        if row["proto"] == "rtsp" {
            f.app.playback_auth.revoke(row["id"].as_str().unwrap());
        }
    }
    tokio::time::sleep(Duration::from_millis(350)).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = vec![0; 8192];
        while first.read(&mut bytes).await.unwrap() != 0 {}
    })
    .await
    .expect("revoked RTSP viewer stayed connected");
    assert_eq!(
        f.get("/owned/index.m3u8?token=owned-viewer").await.status(),
        200
    );
    assert_eq!(f.app.media.stats("owned").await["pid"], pid);
    drop(first);
    drop(second);
    drop(denied);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[path = "vendor_rtsp/boundaries.rs"]
mod boundaries;
#[path = "vendor_rtsp/placement.rs"]
mod placement;
#[path = "vendor_rtsp/transports.rs"]
mod transports;
