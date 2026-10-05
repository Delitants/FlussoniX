use flussonix::srt_playback::{Listener, Settings};
use std::{net::SocketAddr, process::Stdio, time::Duration};
use tokio::process::Command;

fn settings(secret: &str) -> Settings {
    Settings::new(120, 4, secret.to_owned()).unwrap()
}
async fn accepted(listener: &Listener) -> (flussonix::srt_playback::Socket, SocketAddr) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(peer) = listener.accept().unwrap() {
                return peer;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Owned caller did not connect")
}
fn caller(listener: &Listener, id: &str, secret: &str) -> Command {
    let mut cmd = Command::new("ffmpeg");
    cmd.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-threads",
        "1",
        "-analyzeduration",
        "500000",
        "-probesize",
        "65536",
        "-srt_streamid",
        id,
    ]);
    if !secret.is_empty() {
        cmd.args(["-passphrase", secret]);
    }
    cmd.args([
        "-i",
        &format!(
            "srt://{}?mode=caller&connect_timeout=1000&timeout=2000000&latency=120000",
            listener.address()
        ),
    ]);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    cmd
}
#[test]
fn unused_socket_is_nonblocking_and_an_owned_occupied_port_is_rejected() {
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    assert!(Listener::bind(occupied.local_addr().unwrap(), settings("")).is_err());
    let listener = Listener::bind("127.0.0.1:0".parse().unwrap(), settings("")).unwrap();
    assert_ne!(listener.address().port(), 0);
    let start = std::time::Instant::now();
    assert!(listener.accept().unwrap().is_none());
    assert!(start.elapsed() < Duration::from_millis(100));
}
#[tokio::test]
async fn encrypted_listener_rejects_wrong_secret_and_plaintext_callers() {
    for secret in ["wrong-owned-secret", ""] {
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            settings("owned-enforced-secret"),
        )
        .unwrap();
        let mut cmd = caller(&listener, "#!::r=owned,m=request", secret);
        cmd.args(["-t", "1", "-f", "null", "-"]);
        let mut child = cmd.spawn().unwrap();
        let status = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                assert!(
                    listener.accept().unwrap().is_none(),
                    "Encrypted listener accepted an incompatible caller"
                );
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Owned denied caller did not stop");
        assert!(!status.success());
    }
}
#[tokio::test]
async fn ipv6_listener_reports_the_actual_peer_and_preserves_stream_id() {
    let probe = match std::net::UdpSocket::bind("[::1]:0") {
        Ok(s) => s,
        Err(e)
            if [
                std::io::ErrorKind::AddrNotAvailable,
                std::io::ErrorKind::Unsupported,
            ]
            .contains(&e.kind()) =>
        {
            return;
        }
        Err(e) => panic!("{e}"),
    };
    drop(probe);
    let listener = Listener::bind("[::1]:0".parse().unwrap(), settings("")).unwrap();
    let id = "#!::r=owned,m=request,u=owned%2B+é";
    let mut cmd = caller(&listener, id, "");
    cmd.args(["-t", "1", "-f", "null", "-"]);
    let mut child = cmd.spawn().unwrap();
    let (socket, peer) = accepted(&listener).await;
    assert!(peer.ip().is_loopback() && peer.is_ipv6());
    assert_eq!(socket.stream_id().unwrap(), id);
    assert!(socket.is_connected());
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}
#[tokio::test]
async fn encrypted_native_output_decodes_and_preserves_a_512_byte_utf8_id() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.ts");
    let output = dir.path().join("received.ts");
    let result = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "3",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-g",
            "25",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
        ])
        .arg(&source)
        .output()
        .await
        .unwrap();
    assert!(result.status.success());
    let secret = "owned+secret% &123";
    let listener = Listener::bind("127.0.0.1:0".parse().unwrap(), settings(secret)).unwrap();
    let id = format!("#!::r=owned,u={}", "é".repeat(249));
    assert_eq!(id.len(), 512);
    let mut cmd = caller(&listener, &id, secret);
    cmd.args(["-t", "1", "-map", "0", "-c", "copy", "-f", "mpegts"])
        .arg(&output);
    let mut child = cmd.spawn().unwrap();
    let (socket, peer) = accepted(&listener).await;
    assert!(peer.ip().is_loopback());
    assert_eq!(socket.stream_id().unwrap(), id);
    let data = tokio::fs::read(source).await.unwrap();
    for chunk in data.chunks(1316) {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        let sent = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if socket.try_send(chunk)? {
                    break Ok::<(), std::io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        // A successful finite caller can close its socket before the OS
        // process exit is observable. Final status and decode still must pass.
        if sent.expect("Owned send stalled").is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let status = tokio::time::timeout(Duration::from_secs(6), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    let decoded = Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-xerror", "-i"])
        .arg(output)
        .args([
            "-map", "0:v:0", "-map", "0:a:0", "-threads", "1", "-f", "null", "-",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success(),
        "Owned received media did not decode"
    );
}
