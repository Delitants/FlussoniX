use flussonix::tls_input::Bridge;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;
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
        s.write_all(b"PONG").await.unwrap();
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
    let mut b = [0; 4];
    socket.read_exact(&mut b).await.unwrap();
    assert_eq!(&b, b"PONG");
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
    let (url, t) = upstream(&c, "127.0.0.1:0").await;
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
