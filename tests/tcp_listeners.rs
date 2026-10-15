#![cfg(target_os = "linux")]

use serde_json::Value;
use std::{net::SocketAddr, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
    task::JoinSet,
};
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;

fn command(dir: &std::path::Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_flussonix"));
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
        cmd.env_remove(key);
    }
    cmd.arg("--config")
        .arg(dir.join("config.json"))
        .arg("--media-dir")
        .arg(dir.join("media"))
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-backlog-admin")
        .env("FLUSSONIX_PEER_KEY", "owned-backlog-peer")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

async fn startup(child: &mut Child) -> Value {
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .expect("owned daemon did not report its listeners");
    serde_json::from_str(&line).unwrap()
}

fn signal(child: &Child, signal: i32) {
    // SAFETY: This PID belongs to the still-owned child spawned by this test.
    assert_eq!(unsafe { libc::kill(child.id().unwrap() as i32, signal) }, 0);
}

async fn response<S: AsyncRead + AsyncWrite + Unpin>(mut socket: S, rtsp: bool) {
    let request = if rtsp {
        "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n"
    } else {
        "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    };
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(socket).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let expected = if rtsp { "RTSP/1.0 200" } else { "HTTP/1.1 200" };
    assert!(line.starts_with(expected), "unexpected response: {line}");
}

// A regression to the generic bind path (backlog 128) must fail: there are
// deliberately no accepts until all 256 clients have completed their handshake.
// The subsequent request also proves the queued connection remains usable.
async fn delayed_accept_burst(field: &str, tls: bool, rtsp: bool) {
    for bind in ["127.0.0.1:0", "[::1]:0"] {
        let dir = tempfile::tempdir().unwrap();
        let cert = Certificates::new();
        let mut child = command(dir.path())
            .args([
                "--listen",
                bind,
                "--https-listen",
                bind,
                "--rtsp-listen",
                bind,
                "--rtsps-listen",
                bind,
            ])
            .arg("--https-cert")
            .arg(&cert.cert)
            .arg("--https-key")
            .arg(&cert.key)
            .arg("--rtsps-cert")
            .arg(&cert.cert)
            .arg("--rtsps-key")
            .arg(&cert.key)
            .spawn()
            .unwrap();
        let status = startup(&mut child).await;
        let address: SocketAddr = status[field].as_str().unwrap().parse().unwrap();
        assert_ne!(address.port(), 0);
        assert_eq!(address.is_ipv6(), bind.starts_with('['));
        signal(&child, libc::SIGSTOP);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state =
                    std::fs::read_to_string(format!("/proc/{}/stat", child.id().unwrap())).unwrap();
                if state
                    .rsplit_once(')')
                    .unwrap()
                    .1
                    .trim_start()
                    .starts_with('T')
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let mut pending = JoinSet::new();
        for _ in 0..256 {
            pending.spawn(async move {
                tokio::time::timeout(Duration::from_millis(750), TcpStream::connect(address)).await
            });
        }
        let mut clients = Vec::new();
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(Ok(socket))) = result {
                clients.push(socket);
            }
        }
        signal(&child, libc::SIGCONT);
        let connected = clients.len();
        if let Some(socket) = clients.pop() {
            // Free the remaining handshake slots before the protocol probe.
            drop(clients);
            if tls {
                let connector = tokio_rustls::TlsConnector::from(cert.client());
                let socket = tokio::time::timeout(
                    Duration::from_secs(5),
                    connector.connect("localhost".try_into().unwrap(), socket),
                )
                .await
                .unwrap()
                .unwrap();
                response(socket, rtsp).await;
            } else {
                response(socket, rtsp).await;
            }
        }
        signal(&child, libc::SIGTERM);
        assert!(
            tokio::time::timeout(Duration::from_secs(8), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert_eq!(
            connected, 256,
            "{field} at {bind}: accept queue lost burst connections"
        );
    }
}

#[tokio::test]
async fn http_queues_burst_while_accepts_are_paused() {
    delayed_accept_burst("listen", false, false).await;
}
#[tokio::test]
async fn https_queues_burst_while_accepts_are_paused() {
    delayed_accept_burst("https_listen", true, false).await;
}
#[tokio::test]
async fn rtsp_queues_burst_while_accepts_are_paused() {
    delayed_accept_burst("rtsp_listen", false, true).await;
}
#[tokio::test]
async fn rtsps_queues_burst_while_accepts_are_paused() {
    delayed_accept_burst("rtsps_listen", true, true).await;
}

#[tokio::test]
async fn wildcard_ephemeral_binding_and_occupied_port_errors_are_preserved() {
    for bind in ["0.0.0.0:0", "[::]:0"] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = command(dir.path())
            .args(["--listen", bind])
            .spawn()
            .unwrap();
        let status = startup(&mut child).await;
        let address: SocketAddr = status["listen"].as_str().unwrap().parse().unwrap();
        assert!(address.ip().is_unspecified());
        assert_ne!(address.port(), 0);
        let connect = SocketAddr::new(
            if address.is_ipv6() {
                "::1".parse().unwrap()
            } else {
                "127.0.0.1".parse().unwrap()
            },
            address.port(),
        );
        response(TcpStream::connect(connect).await.unwrap(), false).await;
        let other_dir = tempfile::tempdir().unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            command(other_dir.path())
                .args(["--listen", &address.to_string()])
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            !output.status.success(),
            "second daemon hijacked an occupied listener"
        );
        signal(&child, libc::SIGTERM);
        assert!(
            tokio::time::timeout(Duration::from_secs(8), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}
