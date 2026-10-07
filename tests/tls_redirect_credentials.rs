use flussonix::tls_input::Bridge;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::task::AbortOnDropHandle;
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;
#[path = "support/rtsp_camera.rs"]
mod camera;

async fn header<R: tokio::io::AsyncRead + Unpin>(reader: &mut R) -> String {
    let mut bytes = vec![];
    while !bytes.ends_with(b"\r\n\r\n") && bytes.len() < 16384 {
        match reader.read_u8().await {
            Ok(b) => bytes.push(b),
            Err(_) => break,
        }
    }
    String::from_utf8(bytes).unwrap()
}
async fn decoder(url: &str, cseq: u32) -> TcpStream {
    let mut uri = url::Url::parse(url).unwrap();
    let port = uri.port().unwrap();
    uri.set_username("").unwrap();
    uri.set_password(None).unwrap();
    let mut socket = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    socket
        .write_all(format!("DESCRIBE {uri} RTSP/1.0\r\nCSeq: {cseq}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    socket
}
#[tokio::test]
async fn same_origin_redirect_retains_only_configured_escaped_credentials() {
    let cert = Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
    let peer = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = acceptor.accept(socket).await.unwrap();
        assert!(header(&mut socket).await.contains("/first?token=initial"));
        socket.write_all(format!("RTSP/1.0 302 Moved\r\nCSeq: 7\r\nLocation: rtsps://localhost:{}/next?token=target-only\r\n\r\n", addr.port()).as_bytes()).await.unwrap();
        let (next, _) = listener.accept().await.unwrap();
        let mut next = acceptor.accept(next).await.unwrap();
        let request = header(&mut next).await;
        assert!(request.contains("/next?token=target-only"));
        assert!(!request.contains("token=initial"));
        next.write_all(b"RTSP/1.0 200 OK\r\nCSeq: 8\r\nContent-Length: 4\r\n\r\nPONG")
            .await
            .unwrap();
    }));
    let bridge = Bridge::prepare(
        &format!(
            "rtsps://alice%40camera:owned%3Apassword@LOCALHOST:{}/first?token=initial",
            addr.port()
        ),
        Some(&cert.ca),
    )
    .await
    .unwrap();
    let mut first = decoder(bridge.local_url(), 7).await;
    let reply = tokio::time::timeout(Duration::from_secs(3), header(&mut first))
        .await
        .unwrap();
    assert!(
        reply.starts_with("RTSP/1.0 302 "),
        "same-origin credentialed redirect expected"
    );
    let location = reply
        .lines()
        .find_map(|line| line.strip_prefix("Location: "))
        .unwrap();
    let target = url::Url::parse(location).unwrap();
    assert_eq!(target.scheme(), "rtsp");
    assert_eq!(target.host_str(), Some("127.0.0.1"));
    assert_ne!(target.port(), Some(addr.port()));
    assert_eq!(target.username(), "alice%40camera");
    assert_eq!(target.password(), Some("owned%3Apassword"));
    assert_eq!(target.path(), "/next");
    assert_eq!(target.query(), Some("token=target-only"));
    let mut next = decoder(location, 8).await;
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(3), next.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(bytes.ends_with(b"PONG"));
    bridge.close().await;
    peer.await.unwrap();
}

#[tokio::test]
async fn credentialed_changing_query_chain_is_bounded_and_releases_hops() {
    let cert = Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
    let peer = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = acceptor.accept(socket).await.unwrap();
        for hop in 0..5 {
            assert!(
                header(&mut socket)
                    .await
                    .contains(&format!("/entry?hop={hop}"))
            );
            socket.write_all(format!("RTSP/1.0 302 Moved\r\nCSeq: {hop}\r\nLocation: rtsps://{addr}/entry?hop={}\r\n\r\n", hop + 1).as_bytes()).await.unwrap();
            // Verification of the next TLS socket precedes the old decoder's
            // handoff, and therefore precedes closure of the old TLS socket.
            let next = if hop < 4 {
                let (next, _) = listener.accept().await.unwrap();
                Some(acceptor.accept(next).await.unwrap())
            } else {
                None
            };
            assert!(
                tokio::time::timeout(Duration::from_secs(3), socket.read_u8())
                    .await
                    .unwrap()
                    .is_err(),
                "old TLS hop must be closed"
            );
            if let Some(next) = next {
                socket = next;
            }
        }
        tokio::time::timeout(Duration::from_millis(250), listener.accept())
            .await
            .is_ok()
    }));
    let bridge = Bridge::prepare(
        &format!("rtsps://camera:owned-password@{addr}/entry?hop=0"),
        Some(&cert.ca),
    )
    .await
    .unwrap();
    let mut local = bridge.local_url().to_owned();
    for hop in 0..5 {
        let mut socket = decoder(&local, hop).await;
        let response = tokio::time::timeout(Duration::from_secs(3), header(&mut socket))
            .await
            .unwrap();
        if hop == 4 {
            assert!(response.is_empty(), "fifth redirect must fail closed");
        } else {
            local = response
                .lines()
                .find_map(|line| line.strip_prefix("Location: "))
                .unwrap()
                .to_owned();
            let url = url::Url::parse(&local).unwrap();
            assert_eq!(url.username(), "camera");
            assert_eq!(url.password(), Some("owned-password"));
            assert_eq!(url.query(), Some(format!("hop={}", hop + 1).as_str()));
        }
    }
    bridge.close().await;
    assert!(
        !peer.await.unwrap(),
        "hop limit must reject before a sixth connection"
    );
}

#[tokio::test]
async fn credentialed_host_and_port_changes_never_connect() {
    for alias in [false, true] {
        let cert = Certificates::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let location = if alias {
            format!("rtsps://localhost:{}/next", addr.port())
        } else {
            format!("rtsps://{}/next", target.local_addr().unwrap())
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
        let peer = AbortOnDropHandle::new(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = acceptor.accept(socket).await.unwrap();
            header(&mut socket).await;
            socket
                .write_all(
                    format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: {location}\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_millis(250), listener.accept())
                .await
                .is_ok()
        }));
        let bridge = Bridge::prepare(
            &format!("rtsps://camera:owned-password@{addr}/entry"),
            Some(&cert.ca),
        )
        .await
        .unwrap();
        let mut socket = decoder(bridge.local_url(), 1).await;
        let response = tokio::select! {
            _ = target.accept() => panic!("changed port must not receive a connection"),
            response = tokio::time::timeout(Duration::from_secs(2), header(&mut socket)) => response.unwrap(),
        };
        assert!(response.is_empty());
        bridge.close().await;
        assert!(
            !peer.await.unwrap(),
            "hostname alias must not receive a redirected connection"
        );
    }
}

#[tokio::test]
async fn credentialed_exact_cycle_is_rejected_before_reconnect() {
    let cert = Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
    let peer = AbortOnDropHandle::new(tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = acceptor.accept(socket).await.unwrap();
        header(&mut socket).await;
        socket.write_all(format!("RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/entry?token=original\r\n\r\n").as_bytes()).await.unwrap();
        tokio::time::timeout(Duration::from_millis(250), listener.accept())
            .await
            .is_ok()
    }));
    let bridge = Bridge::prepare(
        &format!("rtsps://camera:owned-password@{addr}/entry?token=original"),
        Some(&cert.ca),
    )
    .await
    .unwrap();
    let mut socket = decoder(bridge.local_url(), 1).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), header(&mut socket))
            .await
            .unwrap()
            .is_empty()
    );
    bridge.close().await;
    assert!(
        !peer.await.unwrap(),
        "userinfo must not hide the original URL from cycle detection"
    );
}

#[tokio::test]
async fn same_origin_changed_certificate_is_verified_before_handoff() {
    for case in 0..3 {
        let original = Certificates::new();
        let invalid = Certificates::new();
        if case == 1 {
            invalid.expire();
        }
        if case == 2 {
            std::fs::write(invalid.dir.path().join("wrong-name.ext"), "subjectAltName=DNS:wrong.example\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
            let out = std::process::Command::new("openssl")
                .current_dir(invalid.dir.path())
                .args([
                    "x509",
                    "-req",
                    "-in",
                    "server.csr",
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca.key",
                    "-CAcreateserial",
                    "-out",
                    "server.pem",
                    "-days",
                    "1",
                    "-extfile",
                    "wrong-name.ext",
                ])
                .output()
                .unwrap();
            assert!(out.status.success());
        }
        let trust = original.dir.path().join("roots.pem");
        let mut roots = std::fs::read(&original.ca).unwrap();
        if case != 0 {
            roots.extend_from_slice(&std::fs::read(&invalid.ca).unwrap());
        }
        std::fs::write(&trust, roots).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first = tokio_rustls::TlsAcceptor::from(original.server());
        let second = tokio_rustls::TlsAcceptor::from(invalid.server());
        let peer = AbortOnDropHandle::new(tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = first.accept(socket).await.unwrap();
            header(&mut socket).await;
            socket
                .write_all(
                    format!(
                        "RTSP/1.0 302 Moved\r\nCSeq: 1\r\nLocation: rtsps://{addr}/next\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let mut application = vec![];
            if let Ok(mut socket) = second.accept(socket).await {
                let _ = socket.read_to_end(&mut application).await;
            }
            application
        }));
        let bridge = Bridge::prepare(
            &format!("rtsps://camera:owned-password@{addr}/entry"),
            Some(&trust),
        )
        .await
        .unwrap();
        let mut socket = decoder(bridge.local_url(), 1).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), header(&mut socket))
                .await
                .unwrap()
                .is_empty()
        );
        bridge.close().await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap()
                .is_empty(),
            "case {case}: credentials must not precede certificate verification"
        );
    }
}

#[tokio::test]
async fn basic_and_digest_redirected_inputs_produce_independently_decoded_hls() {
    use futures_util::FutureExt;
    use serde_json::json;
    use std::sync::atomic::Ordering;
    for digest in [false, true] {
        let camera = camera::Camera::new(digest).await;
        let dir = tempfile::tempdir().unwrap();
        let relay = flussonix::media::Engine::new(dir.path(), "ffmpeg");
        let result = std::panic::AssertUnwindSafe(async {
            let cfg = json!({"inputs":[{"url":camera.url,"flussonix_tls_ca":camera.cert.ca}]});
            let worker = relay.ensure("relay", &cfg).await.unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
            let file = loop {
                if let Ok(bytes) = relay.read("relay", "index.m3u8").await {
                    if let Some(line) = std::str::from_utf8(&bytes)
                        .unwrap()
                        .lines()
                        .find(|s| !s.is_empty() && !s.starts_with('#'))
                    {
                        break Some(line.to_owned());
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    break None;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            assert!(
                camera.seen.initial_auth.load(Ordering::SeqCst) > 0,
                "fixture must accept the initial authentication before redirect"
            );
            assert!(
                file.is_some(),
                "authenticated redirected input must produce HLS (digest={digest}, initial_auth={}, final_auth={}, final_challenges={}, source_connections={})",
                camera.seen.initial_auth.load(Ordering::SeqCst),
                camera.seen.final_auth.load(Ordering::SeqCst),
                camera.seen.final_challenges.load(Ordering::SeqCst),
                camera.seen.source_connections.load(Ordering::SeqCst)
            );
            let data = relay.read("relay", &file.unwrap()).await.unwrap();
            let path = dir.path().join("decode.ts");
            std::fs::write(&path, data).unwrap();
            for _ in 0..2 {
                let output = tokio::time::timeout(
                    Duration::from_secs(15),
                    tokio::process::Command::new("ffmpeg")
                        .args(["-nostdin", "-v", "error", "-i"])
                        .arg(&path)
                        .args([
                            "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "framemd5",
                            "-",
                        ])
                        .kill_on_drop(true)
                        .output(),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(
                    output.status.success() && output.stderr.is_empty(),
                    "independent mapped audio/video decode failed"
                );
                assert!(
                    output
                        .stdout
                        .split(|b| *b == b'\n')
                        .filter(|l| l.starts_with(b"0,"))
                        .count()
                        >= 20
                );
                assert!(
                    output
                        .stdout
                        .split(|b| *b == b'\n')
                        .filter(|l| l.starts_with(b"1,"))
                        .count()
                        >= 40
                );
            }
            assert!(Arc::ptr_eq(
                &worker,
                &relay.ensure("relay", &cfg).await.unwrap()
            ));
            assert_eq!(worker.stats()["input_protocol"], "rtsps");
            assert_eq!(relay.count().await, 1);
            assert_eq!(camera.source.media.count().await, 1);
            assert_eq!(camera.seen.source_connections.load(Ordering::SeqCst), 1);
            assert!(camera.seen.initial_challenges.load(Ordering::SeqCst) > 0);
            assert!(camera.seen.final_auth.load(Ordering::SeqCst) >= 4);
            if digest {
                assert!(camera.seen.final_challenges.load(Ordering::SeqCst) > 0);
            }
        })
        .catch_unwind()
        .await;
        relay.stop_all().await;
        camera.stop().await;
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn wrong_camera_password_never_starts_native_source() {
    use serde_json::json;
    use std::sync::atomic::Ordering;
    for digest in [false, true] {
        let camera = camera::Camera::new(digest).await;
        let dir = tempfile::tempdir().unwrap();
        let relay = flussonix::media::Engine::new(dir.path(), "ffmpeg");
        relay.ensure("denied", &json!({"inputs":[{"url":camera.url.replace("owned-password", "wrong-password"),"flussonix_tls_ca":camera.cert.ca}]})).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        let denied = camera.seen.initial_challenges.load(Ordering::SeqCst) > 0
            && camera.seen.initial_auth.load(Ordering::SeqCst) == 0
            && camera.seen.final_auth.load(Ordering::SeqCst) == 0
            && camera.seen.source_connections.load(Ordering::SeqCst) == 0
            && camera.source.media.count().await == 0
            && relay.read("denied", "index.m3u8").await.is_err();
        relay.stop_all().await;
        camera.stop().await;
        assert!(
            denied,
            "wrong password must not authenticate, redirect or start a source"
        );
    }
}
