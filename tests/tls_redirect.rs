use flussonix::tls_input::Bridge;
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;

async fn header<S: tokio::io::AsyncRead + Unpin>(socket: &mut S) -> Vec<u8> {
    let mut bytes = vec![];
    while !bytes.ends_with(b"\r\n\r\n") && bytes.len() < 16384 {
        match socket.read_u8().await {
            Ok(byte) => bytes.push(byte),
            Err(_) => break,
        }
    }
    bytes
}
async fn reply_bridge(
    c: &Certificates,
    ca: &Path,
    reply: Vec<u8>,
    userinfo: &str,
) -> (Bridge, AbortOnDropHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = acceptor.accept(socket).await.unwrap();
        assert!(header(&mut socket).await.starts_with(b"DESCRIBE "));
        let _ = socket.write_all(&reply).await;
    }));
    let bridge = Bridge::prepare(
        &format!("rtsps://{userinfo}{addr}/owned?token=initial"),
        Some(ca),
    )
    .await
    .unwrap();
    (bridge, task)
}
async fn send(url: &str, timeout: Duration) -> Vec<u8> {
    let u = url::Url::parse(url).unwrap();
    let mut socket = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    socket
        .write_all(format!("DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut bytes = vec![];
    tokio::time::timeout(timeout, socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap_or(0);
    bytes
}
fn local_target(bytes: &[u8]) -> String {
    let text = std::str::from_utf8(bytes).unwrap();
    assert!(text.starts_with("RTSP/1.0 302 "), "{text:?}");
    let target = text
        .lines()
        .find_map(|line| line.strip_prefix("Location: "))
        .unwrap();
    let url = url::Url::parse(target).unwrap();
    assert_eq!(url.scheme(), "rtsp");
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    assert!(url.username().is_empty());
    assert!(url.password().is_none());
    target.into()
}
#[tokio::test]
async fn invalid_redirects_and_cross_origin_credentials_never_connect_or_escape() {
    let c = Certificates::new();
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = target.local_addr().unwrap();
    let invalid = [
        format!("rtsp://{addr}/owned"),
        format!("https://{addr}/owned"),
        format!("rtsps://user:secret@{addr}/owned"),
        format!("rtsps://@{addr}/owned"),
        format!("rtsps://{addr}/owned#secret"),
        format!("rtsps://{addr}/bad%"),
        format!("rtsps://{addr}/raw\"quote"),
        format!("\u{00a0}rtsps://{addr}/owned"),
        format!("rtsps://{addr}/owned\u{2003}"),
        "rtsps://127.0.0.1:0/owned".into(),
        "/owned".into(),
        String::new(),
    ];
    let mut cases = invalid
        .into_iter()
        .map(|location| {
            (
                format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: {location}\r\n\r\n")
                    .into_bytes(),
                "",
            )
        })
        .collect::<Vec<_>>();
    for headers in [
        "CSeq: 1\r\nCSeq: 2",
        "CSeq: 4294967296",
        "CSeq: x",
        "X: missing-cseq",
        "CSeq: 1\r\nContent-Length: 65537",
        "CSeq: 1\r\nContent-Length: 0\r\ncontent-length: 1",
        "CSeq: 1\r\nTransfer-Encoding: chunked",
    ] {
        cases.push((
            format!("RTSP/1.0 302 Moved\r\n{headers}\r\nLocation: rtsps://{addr}/owned\r\n\r\n")
                .into_bytes(),
            "",
        ));
    }
    cases.push((format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/owned\r\nlocation: rtsps://{addr}/another\r\n\r\n").into_bytes(), ""));
    cases.push((
        format!("RTSP/1.0 399 Unsupported\r\nCSeq: 1\r\nLocation: rtsps://{addr}/owned\r\n\r\n")
            .into_bytes(),
        "",
    ));
    cases.push((
        format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/owned\r\n\r\n")
            .into_bytes(),
        "alice:owned-password@",
    ));
    for (wire, credentials) in cases {
        let (bridge, task) = reply_bridge(&c, &c.ca, wire, credentials).await;
        let response = tokio::select! {
            accepted = target.accept() => panic!("rejected redirect must not connect: {accepted:?}"),
            response = send(bridge.local_url(), Duration::from_secs(3)) => response,
        };
        assert!(
            response.is_empty(),
            "rejected redirect must not reach the decoder"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), target.accept())
                .await
                .is_err(),
            "rejected redirect must not connect"
        );
        bridge.close().await;
        task.await.unwrap();
    }
}
#[tokio::test]
async fn redirected_identity_trust_and_expiry_reject_before_application_bytes() {
    let original = Certificates::new();
    for case in 0..3 {
        let target = Certificates::new();
        if case == 2 {
            target.expire();
        }
        let listener = TcpListener::bind(if case == 0 {
            "127.0.0.2:0"
        } else {
            "127.0.0.1:0"
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(target.server());
        let peer = AbortOnDropHandle::new(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            if let Ok(mut socket) = acceptor.accept(socket).await {
                let _ = socket.read_to_end(&mut bytes).await;
            }
            bytes
        }));
        let trust = original.dir.path().join("redirect-ca.pem");
        let mut roots = std::fs::read(&original.ca).unwrap();
        if case != 1 {
            roots.extend_from_slice(&std::fs::read(&target.ca).unwrap());
        }
        std::fs::write(&trust, roots).unwrap();
        let wire = format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/owned?token=target-secret\r\n\r\n").into_bytes();
        let (bridge, task) = reply_bridge(&original, &trust, wire, "").await;
        assert!(
            send(bridge.local_url(), Duration::from_secs(3))
                .await
                .is_empty()
        );
        bridge.close().await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap()
                .is_empty(),
            "case {case}: no application bytes before verification"
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn established_sdp_session_and_media_reject_late_redirects() {
    let c = Certificates::new();
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = target.local_addr().unwrap();
    for prefix in [
        b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Type: application/sdp\r\nContent-Length: 0\r\n\r\n"
            .to_vec(),
        b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nSession: owned\r\n\r\n".to_vec(),
        vec![b'$', 0, 0, 1, b'X'],
    ] {
        let mut wire = prefix.clone();
        wire.extend_from_slice(
            format!("RTSP/1.0 302 Moved\r\nCSeq: 2\r\nLocation: rtsps://{addr}/owned\r\n\r\n")
                .as_bytes(),
        );
        let (bridge, task) = reply_bridge(&c, &c.ca, wire, "").await;
        assert_eq!(
            send(bridge.local_url(), Duration::from_secs(3)).await,
            prefix
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), target.accept())
                .await
                .is_err()
        );
        bridge.close().await;
        task.await.unwrap();
    }
}
#[tokio::test]
async fn exact_cycles_and_changing_query_hops_are_bounded() {
    let c = Certificates::new();
    for cycle in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let peer = AbortOnDropHandle::new(tokio::spawn(async move {
            loop {
                let socket = tokio::select! { _=cancelled.cancelled()=>break, socket=listener.accept()=>socket.unwrap().0 };
                let mut socket = acceptor.accept(socket).await.unwrap();
                let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = header(&mut socket).await;
                let location = if cycle {
                    format!("rtsps://{addr}/owned?token=initial")
                } else {
                    format!("rtsps://{addr}/hop?step={n}")
                };
                let _ = socket
                    .write_all(
                        format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: {location}\r\n\r\n")
                            .as_bytes(),
                    )
                    .await;
            }
        }));
        let bridge = Bridge::prepare(&format!("rtsps://{addr}/owned?token=initial"), Some(&c.ca))
            .await
            .unwrap();
        let mut local = bridge.local_url().to_owned();
        let mut redirects = 0;
        loop {
            let bytes = send(&local, Duration::from_secs(3)).await;
            if bytes.is_empty() {
                break;
            }
            assert!(redirects < 4, "fifth redirect must not be followed");
            local = local_target(&bytes);
            redirects += 1;
        }
        assert_eq!(redirects, if cycle { 0 } else { 4 });
        assert_eq!(count.load(Ordering::SeqCst), if cycle { 1 } else { 5 });
        bridge.close().await;
        stop.cancel();
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn closing_during_redirected_handshake_releases_target_and_decoder() {
    let c = Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let entered = Arc::new(Notify::new());
    let signal = entered.clone();
    let peer = AbortOnDropHandle::new(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        signal.notify_one();
        let mut bytes = vec![];
        let _ = socket.read_to_end(&mut bytes).await;
        bytes
    }));
    let wire = format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/owned?token=target-secret\r\n\r\n").into_bytes();
    let (bridge, task) = reply_bridge(&c, &c.ca, wire, "").await;
    let u = url::Url::parse(bridge.local_url()).unwrap();
    let mut local = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    local
        .write_all(b"DESCRIBE rtsp://127.0.0.1/owned RTSP/1.0\r\nCSeq: 1\r\n\r\n")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    bridge.close().await;
    let mut reply = vec![];
    tokio::time::timeout(Duration::from_secs(2), local.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap_or(0);
    assert!(reply.is_empty());
    let bytes = tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !bytes
            .windows(b"target-secret".len())
            .any(|b| b == b"target-secret")
    );
    assert!(
        TcpStream::connect(("127.0.0.1", u.port().unwrap()))
            .await
            .is_err()
    );
    task.await.unwrap();
}
#[tokio::test]
async fn redirected_unused_endpoint_expires_and_close_releases_handoff() {
    let c = Certificates::new();
    for close in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
        let peer = AbortOnDropHandle::new(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = acceptor.accept(socket).await.unwrap();
            let mut bytes = vec![];
            let _ = socket.read_to_end(&mut bytes).await;
            bytes
        }));
        let wire = format!(
            "RTSP/1.0 301 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/next?token=explicit\r\n\r\n"
        )
        .into_bytes();
        let (bridge, task) = reply_bridge(&c, &c.ca, wire, "").await;
        let location = local_target(&send(bridge.local_url(), Duration::from_secs(3)).await);
        let next = url::Url::parse(&location).unwrap();
        if close {
            bridge.close().await;
        } else {
            tokio::time::sleep(Duration::from_secs(9)).await;
            bridge.close().await;
        }
        assert!(
            TcpStream::connect(("127.0.0.1", next.port().unwrap()))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn initial_deadline_bounds_complete_frame_reads_but_not_established_media() {
    let c = Certificates::new();
    let mut bridges = vec![];
    let mut tasks = vec![];
    for established in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
        tasks.push(AbortOnDropHandle::new(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = acceptor.accept(socket).await.unwrap();
            let _ = header(&mut socket).await;
            socket.write_all(if established { b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Type: application/sdp\r\nContent-Length: 0\r\n\r\n" } else { b"R" }).await.unwrap();
            tokio::time::sleep(Duration::from_secs(21)).await;
            let _ = socket.write_all(&[b'$',0,0,1,b'A']).await;
        })));
        bridges.push(
            Bridge::prepare(&format!("rtsps://{addr}/owned"), Some(&c.ca))
                .await
                .unwrap(),
        );
    }
    let (stalled, established) = tokio::join!(
        send(bridges[0].local_url(), Duration::from_secs(23)),
        send(bridges[1].local_url(), Duration::from_secs(24))
    );
    assert!(
        stalled.is_empty(),
        "incomplete header must time out without forwarding bytes"
    );
    assert_eq!(established, b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Type: application/sdp\r\nContent-Length: 0\r\n\r\n$\0\0\x01A");
    for bridge in bridges {
        bridge.close().await;
    }
    for task in tasks {
        task.await.unwrap();
    }
}
