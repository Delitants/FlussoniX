use flussonix::tls_input::Bridge;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;
async fn control_header<S: tokio::io::AsyncRead + Unpin>(socket: &mut S) -> String {
    let mut header = vec![];
    while !header.ends_with(b"\r\n\r\n") && header.len() < 16384 {
        match socket.read_u8().await {
            Ok(byte) => header.push(byte),
            Err(_) => break,
        }
    }
    String::from_utf8(header).unwrap()
}
#[tokio::test]
async fn verified_redirect_uses_owned_loopback_and_delivers_final_tls_response() {
    use tokio_util::task::AbortOnDropHandle;
    let c = Certificates::new();
    let initial = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let edge = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let initial_addr = initial.local_addr().unwrap();
    let edge_addr = edge.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
    let edge_acceptor = acceptor.clone();
    let initial_task = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = initial.accept().await.unwrap();
        let mut socket = acceptor.accept(socket).await.unwrap();
        assert!(control_header(&mut socket).await.contains("token=initial"));
        socket.write_all(format!("RTSP/1.0 302 Moved Temporarily\r\nCSeq: 7\r\nLocation: rtsps://{edge_addr}/second?token=edge-only&client=owned\r\n\r\n").as_bytes()).await.unwrap();
    }));
    let edge_task = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = edge.accept().await.unwrap();
        let mut socket = edge_acceptor.accept(socket).await.unwrap();
        let request = control_header(&mut socket).await;
        assert!(request.contains("/second?token=edge-only&client=owned"));
        assert!(!request.contains("token=initial"));
        socket
            .write_all(b"RTSP/1.0 200 OK\r\nCSeq: 8\r\nContent-Length: 4\r\n\r\nPONG")
            .await
            .unwrap();
    }));
    let bridge = Bridge::prepare(
        &format!("rtsps://{initial_addr}/first?token=initial"),
        Some(&c.ca),
    )
    .await
    .unwrap();
    let local = url::Url::parse(bridge.local_url()).unwrap();
    let mut socket = TcpStream::connect(("127.0.0.1", local.port().unwrap()))
        .await
        .unwrap();
    socket
        .write_all(
            format!(
                "DESCRIBE {} RTSP/1.0\r\nCSeq: 7\r\n\r\n",
                bridge.local_url()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(3), control_header(&mut socket))
        .await
        .unwrap();
    assert!(
        response.starts_with("RTSP/1.0 302 "),
        "verified redirect expected, got {response:?}"
    );
    let location = response
        .lines()
        .find_map(|line| line.strip_prefix("Location: "))
        .unwrap();
    let next = url::Url::parse(location).unwrap();
    assert_eq!(next.scheme(), "rtsp");
    assert_eq!(next.host_str(), Some("127.0.0.1"));
    assert_ne!(next.port(), Some(edge_addr.port()));
    assert_eq!(next.path(), "/second");
    assert_eq!(next.query(), Some("token=edge-only&client=owned"));
    let mut socket = TcpStream::connect(("127.0.0.1", next.port().unwrap()))
        .await
        .unwrap();
    socket
        .write_all(format!("DESCRIBE {location} RTSP/1.0\r\nCSeq: 8\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut received = vec![];
    tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        received,
        b"RTSP/1.0 200 OK\r\nCSeq: 8\r\nContent-Length: 4\r\n\r\nPONG"
    );
    bridge.close().await;
    initial_task.await.unwrap();
    edge_task.await.unwrap();
}
async fn upstream(c: &Certificates, bind: &str) -> (String, tokio::task::JoinHandle<usize>) {
    let l = TcpListener::bind(bind).await.unwrap();
    let url = format!(
        "rtsps://{}/secret?token=never-before-validation",
        l.local_addr().unwrap()
    );
    let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
    let t = tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        if let Ok(mut s) = acceptor.accept(s).await {
            let mut b = [0; 4096];
            s.read(&mut b).await.unwrap_or(0)
        } else {
            0
        }
    });
    (url, t)
}
#[tokio::test]
async fn wrong_identity_untrusted_expired_and_bad_ca_send_no_application_bytes() {
    for case in 0..3 {
        let c = Certificates::new();
        if case == 2 {
            c.expire();
        }
        let (url, t) = upstream(
            &c,
            if case == 0 {
                "127.0.0.2:0"
            } else {
                "127.0.0.1:0"
            },
        )
        .await;
        let ca = if case == 1 {
            None
        } else {
            Some(c.ca.as_path())
        };
        assert!(
            Bridge::prepare(&url, ca).await.is_err(),
            "case {case} must fail closed"
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), t)
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
    let c = Certificates::new();
    let (url, t) = upstream(&c, "127.0.0.1:0").await;
    let bad = c.dir.path().join("bad.pem");
    std::fs::write(&bad, "invalid").unwrap();
    assert!(Bridge::prepare(&url, Some(&bad)).await.is_err());
    t.abort();
}
#[tokio::test]
async fn verified_bridge_forwards_original_path_and_guard_closes_both_sockets() {
    let c = Certificates::new();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
    let t = tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        let mut s = acceptor.accept(s).await.unwrap();
        let mut b = [0; 16];
        s.read_exact(&mut b[..4]).await.unwrap();
        assert_eq!(&b[..4], b"PING");
        s.write_all(b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: 4\r\n\r\nPONG")
            .await
            .unwrap();
        s.read(&mut b).await.unwrap_or(0)
    });
    let bridge = Bridge::prepare(&format!("rtsps://{addr}/secret?token=owned"), Some(&c.ca))
        .await
        .unwrap();
    let u = url::Url::parse(bridge.local_url()).unwrap();
    assert_eq!(u.path(), "/secret");
    assert_eq!(u.query(), Some("token=owned"));
    assert_eq!(u.host_str(), Some("127.0.0.1"));
    let port = u.port().unwrap();
    let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    socket.write_all(b"PING").await.unwrap();
    let expected = b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: 4\r\n\r\nPONG";
    let mut b = vec![0; expected.len()];
    socket.read_exact(&mut b).await.unwrap();
    assert_eq!(&b, expected);
    assert!(TcpStream::connect(("127.0.0.1", port)).await.is_err());
    drop(bridge);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), t)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let closed = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut b))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0))
            || matches!(closed,Err(ref e) if e.kind()==std::io::ErrorKind::ConnectionReset)
    );
    assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());
}
#[tokio::test]
async fn worker_spawn_failure_and_stop_release_verified_upstream() {
    let c = Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsps://{}/owned", listener.local_addr().unwrap());
    let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
    let t = tokio::spawn(async move {
        let Ok(Ok((socket, _))) =
            tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
        else {
            return 0;
        };
        if let Ok(mut socket) = acceptor.accept(socket).await {
            let mut bytes = [0; 4096];
            socket.read(&mut bytes).await.unwrap_or(0)
        } else {
            0
        }
    });
    let d = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(d.path(), "/no-owned-ffmpeg-binary");
    assert!(
        engine
            .ensure(
                "owned",
                &serde_json::json!({"inputs":[{"url":url,"flussonix_tls_ca":c.ca}]})
            )
            .await
            .is_err()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), t)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(engine.count().await, 0);
}
#[tokio::test]
async fn upstream_tls_handshake_deadline_and_cancelled_prepare_do_not_leak() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsps://{}/owned", l.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut data = vec![];
        s.read_to_end(&mut data).await.unwrap_or(0)
    });
    assert!(
        tokio::time::timeout(Duration::from_secs(12), Bridge::prepare(&url, None))
            .await
            .unwrap()
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    let c = Certificates::new();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsps://{}/owned", l.local_addr().unwrap());
    let ca = c.ca.clone();
    let pending = tokio::spawn(async move { Bridge::prepare(&url, Some(&ca)).await });
    let (mut socket, _) = l.accept().await.unwrap();
    pending.abort();
    let _ = pending.await;
    let mut data = vec![];
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut data))
        .await
        .unwrap()
        .unwrap_or(0);
}

#[tokio::test]
async fn unused_verified_bridge_expires_and_releases_listener() {
    let c = Certificates::new();
    let (url, t) = upstream(&c, "127.0.0.1:0").await;
    let bridge = Bridge::prepare(&url, Some(&c.ca)).await.unwrap();
    let port = url::Url::parse(bridge.local_url()).unwrap().port().unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), t)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(TcpStream::connect(("127.0.0.1", port)).await.is_err());
    bridge.close().await;
    assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());
}

#[tokio::test]
async fn verified_input_rejects_plaintext_redirect_before_networking() {
    for scheme in ["rtsp"] {
        let c = Certificates::new();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let foreign = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = foreign.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
        let upstream = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let mut tls = acceptor.accept(s).await.unwrap();
            let mut req = vec![];
            while !req.ends_with(b"\r\n\r\n") {
                req.push(tls.read_u8().await.unwrap());
            }
            tls.write_all(format!("RTSP/1.0 302 Moved Temporarily\r\nCSeq: 1\r\nLocation: {scheme}://{target}/owned?token=owned-redirect-token\r\n\r\n").as_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        });
        let d = tempfile::tempdir().unwrap();
        let engine = flussonix::media::Engine::new(d.path(), "ffmpeg");
        let worker=engine.ensure("owned",&serde_json::json!({"inputs":[{"url":format!("rtsps://{addr}/owned?token=owned-redirect-token"),"flussonix_tls_ca":c.ca}]})).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(800), foreign.accept())
                .await
                .is_err(),
            "redirect must not open an unverified connection"
        );
        for _ in 0..40 {
            if !worker.alive.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(!worker.alive.load(std::sync::atomic::Ordering::Relaxed));
        engine.stop_all().await;
        upstream.await.unwrap();
    }
}
#[tokio::test]
async fn failed_verified_setup_keeps_retry_metadata_and_advances_fallback() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = held.local_addr().unwrap().port();
    drop(held);
    let d = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(d.path(), "ffmpeg");
    let cfg = serde_json::json!({"inputs":[{"url":format!("rtsps://127.0.0.1:{port}/owned")},{"url":"testsrc://"}]});
    let first = engine
        .ensure("owned", &cfg)
        .await
        .expect("setup is supervised by a worker");
    for _ in 0..80 {
        if !first.alive.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(!first.alive.load(std::sync::atomic::Ordering::Relaxed));
    assert!(first.stats()["last_error"].is_string());
    let mut fallback = None;
    for _ in 0..100 {
        if let Ok(w) = engine.recover("owned", &cfg).await {
            fallback = Some(w);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let w = fallback.expect("fallback starts after bounded backoff");
    assert_eq!(w.stats()["input_index"], 1);
    let mut media = w.subscribe();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), media.recv())
            .await
            .unwrap()
            .is_ok()
    );
    engine.stop_all().await;
}
#[tokio::test]
async fn stalled_verified_setup_does_not_block_other_streams_or_shutdown() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let d = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(flussonix::media::Engine::new(d.path(), "ffmpeg"));
    let e = engine.clone();
    let pending = tokio::spawn(async move {
        e.ensure(
            "stalled",
            &serde_json::json!({"inputs":[{"url":format!("rtsps://{addr}/owned")}]}),
        )
        .await
    });
    let (mut socket, _) = l.accept().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), engine.count())
            .await
            .is_ok(),
        "one handshake must not hold the worker map lock"
    );
    let other = tokio::time::timeout(
        Duration::from_secs(2),
        engine.ensure(
            "other",
            &serde_json::json!({"inputs":[{"url":"testsrc://"}]}),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let mut media = other.subscribe();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), media.recv())
            .await
            .unwrap()
            .is_ok()
    );
    let worker = pending.await.unwrap().unwrap();
    let pid = worker.pid();
    tokio::time::timeout(Duration::from_secs(2), engine.stop_all())
        .await
        .unwrap();
    assert_eq!(engine.count().await, 0);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap_or(0);
}

#[tokio::test]
async fn supervised_input_rejects_bad_trust_before_application_data() {
    for case in 0..3 {
        let c = Certificates::new();
        if case == 2 {
            c.expire();
        }
        let (url, t) = upstream(
            &c,
            if case == 0 {
                "127.0.0.2:0"
            } else {
                "127.0.0.1:0"
            },
        )
        .await;
        let d = tempfile::tempdir().unwrap();
        let engine = flussonix::media::Engine::new(d.path(), "ffmpeg");
        let mut input = serde_json::json!({"url":url});
        if case != 1 {
            input["flussonix_tls_ca"] = serde_json::json!(c.ca);
        }
        let worker = engine
            .ensure("owned", &serde_json::json!({"inputs":[input]}))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), t)
                .await
                .unwrap()
                .unwrap(),
            0
        );
        for _ in 0..80 {
            if !worker.alive.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(!worker.alive.load(std::sync::atomic::Ordering::Relaxed));
        engine.stop_all().await;
    }
}
#[tokio::test]
async fn response_guard_preserves_bodies_and_interleaving_and_rejects_ambiguous_frames() {
    let c = Certificates::new();
    let body = b"RTSP/1.0 302 Location in an opaque body";
    let mut valid = format!(
        "RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    valid.extend_from_slice(body);
    valid.extend_from_slice(&[b'$', 0, 0, body.len() as u8]);
    valid.extend_from_slice(body);
    let cases = [
        (valid, true),
        (
            b"RTSP/1.0 399 Redirect\r\nCSeq: 1\r\nLocation: rtsp://localhost/owned\r\n\r\n"
                .to_vec(),
            false,
        ),
        (
            b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: 1\r\ncontent-length: 0\r\n\r\nX"
                .to_vec(),
            false,
        ),
        (
            b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Length: 65537\r\n\r\n".to_vec(),
            false,
        ),
        (vec![b'$', 0, 0x20, 1], false),
        (
            format!("RTSP/1.0 200 OK\r\nCSeq: 1\r\nX: {}", "A".repeat(16384)).into_bytes(),
            false,
        ),
    ];
    for (wire, allowed) in cases {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(c.server());
        let expected = wire.clone();
        let peer = tokio::spawn(async move {
            let (s, _) = l.accept().await.unwrap();
            let mut tls = acceptor.accept(s).await.unwrap();
            let mut start = [0; 1];
            tls.read_exact(&mut start).await.unwrap();
            for chunk in wire.chunks(7) {
                if tls.write_all(chunk).await.is_err() {
                    break;
                }
            }
            let _ = tls.shutdown().await;
        });
        let bridge = Bridge::prepare(&format!("rtsps://{addr}/owned"), Some(&c.ca))
            .await
            .unwrap();
        let u = url::Url::parse(bridge.local_url()).unwrap();
        let mut local = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
            .await
            .unwrap();
        local.write_all(b"X").await.unwrap();
        let mut got = vec![];
        tokio::time::timeout(Duration::from_secs(3), local.read_to_end(&mut got))
            .await
            .unwrap()
            .unwrap_or(0);
        if allowed {
            assert_eq!(got, expected);
        } else {
            assert!(
                got.is_empty(),
                "unvalidated control bytes must not reach the decoder"
            );
        }
        bridge.close().await;
        peer.await.unwrap();
    }
}
