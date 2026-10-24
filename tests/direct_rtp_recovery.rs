//! Actual loopback ICMP and destination rebinds; no simulated socket errors.
use bytes::Bytes;
use flussonix::{
    direct_rtp::{config::outputs, crypto::Session, output::State, packet},
    media_queue::Channel,
};
use serde_json::json;
use std::{net::SocketAddr, os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use tokio::{net::UdpSocket, task::JoinHandle};
use tokio_util::sync::CancellationToken;

struct Sender {
    state: Arc<State>,
    queue: Channel,
    stop: CancellationToken,
    task: JoinHandle<()>,
    _key: tempfile::TempDir,
}
impl Drop for Sender {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}
impl Sender {
    fn start(address: SocketAddr, secure: bool) -> Self {
        let key = tempfile::tempdir().unwrap();
        let path = key.path().join("key");
        std::fs::write(
            &path,
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0x52; 30]),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let scheme = if secure { "srtp" } else { "rtp" };
        let options = if secure {
            json!({"key_file":path})
        } else {
            json!({})
        };
        let definition = outputs(&json!({"flussonix_rtp_outputs":[{"url":format!("{scheme}://{address}"),"flussonix_rtp":options,"max_mbps":1}]})).unwrap().remove(0);
        let state = State::new(definition, 0);
        let queue = Channel::new(64, 4 * 1024 * 1024);
        let rx = queue.subscribe();
        let stop = CancellationToken::new();
        let s = state.clone();
        let c = stop.clone();
        let task = tokio::spawn(async move { s.run(rx, c).await });
        Self {
            state,
            queue,
            stop,
            task,
            _key: key,
        }
    }
    async fn send(&self, marker: u8) {
        let before = self.accounted();
        self.queue.send(Bytes::from(payload(marker))).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.accounted() == before {
                assert!(
                    !self.task.is_finished(),
                    "destination stopped: {}",
                    self.state.stats()
                );
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("one packet must be sent or counted unavailable");
        assert_eq!(self.accounted(), before + 1);
    }
    fn accounted(&self) -> u64 {
        let s = self.state.stats();
        s["packets"].as_u64().unwrap() + s["unreachable_packets"].as_u64().unwrap()
    }
    async fn unavailable(&self) {
        let before = self.state.stats()["unreachable_packets"].as_u64().unwrap();
        for _ in 0..16 {
            self.send(0x11).await;
            if self.state.stats()["unreachable_packets"].as_u64().unwrap() > before {
                assert_eq!(self.state.stats()["status"], "receiver_unavailable");
                assert!(self.state.stats()["last_error"].is_null());
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("loopback refusal not observed");
    }
    async fn finish(&mut self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .expect("cancel unavailable output promptly")
            .unwrap();
        assert_eq!(self.state.stats()["status"], "stopped");
    }
}
fn payload(marker: u8) -> Vec<u8> {
    let mut p = vec![marker; 188 * 7];
    for b in p.chunks_exact_mut(188) {
        b[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    }
    p
}
async fn pair() -> (UdpSocket, UdpSocket, SocketAddr) {
    for _ in 0..64 {
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = a.local_addr().unwrap();
        if addr.port() < 65535 {
            if let Ok(b) = UdpSocket::bind((addr.ip(), addr.port() + 1)).await {
                return (a, b, addr);
            }
        }
    }
    panic!("no owned pair")
}
async fn receive(
    socket: &UdpSocket,
    crypto: &mut Option<Session>,
    marker: u8,
) -> (u16, u32, SocketAddr) {
    let mut b = [0; 1600];
    let (n, peer) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    let mut body = b[..n].to_vec();
    if let Some(rx) = crypto {
        let cipher = body.clone();
        rx.unprotect(&mut body, false).unwrap();
        assert!(
            rx.unprotect(&mut cipher.clone(), false).is_err(),
            "SRTP replay must remain rejected"
        );
    }
    let p = packet::parse(&body).unwrap();
    assert_eq!(p.payload, payload(marker));
    (p.sequence, p.ssrc, peer)
}
#[tokio::test]
async fn late_rtp_receiver_gets_fresh_media_without_restarting_destination() {
    let (a, control, address) = pair().await;
    drop(a);
    let mut s = Sender::start(address, false);
    s.unavailable().await;
    let rx = UdpSocket::bind(address).await.unwrap();
    s.send(0x33).await;
    let (_, _, source) = receive(&rx, &mut None, 0x33).await;
    assert_eq!(s.state.stats()["status"], "sending");
    assert!(s.state.stats()["last_error"].is_null());
    s.finish().await;
    assert!(UdpSocket::bind(source).await.is_ok());
    assert!(
        UdpSocket::bind((source.ip(), source.port() + 1))
            .await
            .is_ok()
    );
    drop(control);
}
async fn rebind(secure: bool) {
    let (rx, control, address) = pair().await;
    let mut s = Sender::start(address, secure);
    let mut cipher = secure.then(|| Session::new([0x52; 30], None).unwrap());
    s.send(0x22).await;
    let (mut sequence, id, source) = receive(&rx, &mut cipher, 0x22).await;
    drop(rx);
    s.unavailable().await;
    let rx = UdpSocket::bind(address).await.unwrap();
    for marker in [0x44, 0x55, 0x66] {
        s.send(marker).await;
        let (next, next_id, peer) = receive(&rx, &mut cipher, marker).await;
        assert_eq!(next_id, id);
        assert_eq!(peer, source);
        assert!(next.wrapping_sub(sequence) > 0 && next.wrapping_sub(sequence) < 100);
        sequence = next;
    }
    assert_eq!(s.state.stats()["status"], "sending");
    assert!(s.state.stats()["last_error"].is_null());
    drop(rx);
    s.unavailable().await;
    s.finish().await;
    assert!(UdpSocket::bind(source).await.is_ok());
    assert!(
        UdpSocket::bind((source.ip(), source.port() + 1))
            .await
            .is_ok()
    );
    drop(control);
}
#[tokio::test]
async fn rtp_destination_survives_rebind_and_cancels_while_unavailable() {
    rebind(false).await;
}
#[tokio::test]
async fn srtp_destination_preserves_crypto_and_identity_across_rebind() {
    rebind(true).await;
}

#[tokio::test]
async fn independent_ffmpeg_receivers_decode_after_late_start_and_rebind() {
    use flussonix::media::Engine;
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    use tokio::process::Command;
    let d = tempfile::tempdir().unwrap();
    let (a, b, address) = pair().await;
    drop((a, b));
    let engine = Engine::new(d.path().join("media"), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":180,"ab":64},"flussonix_rtp_outputs":[{"url":format!("rtp://{address}"),"max_mbps":1}]});
    let worker = engine
        .ensure_guarded("owned", &cfg, true, std::future::ready(true))
        .await
        .unwrap();
    let pid = worker.pid();
    let sdp = d.path().join("receiver.sdp");
    std::fs::write(&sdp,format!("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=Owned recovery receiver\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video {} RTP/AVP 33\r\na=rtpmap:33 MP2T/90000\r\n",address.port())).unwrap();
    let run = AssertUnwindSafe(async {
        for phase in 0..2 {
            let before = worker.stats()["flussonix_rtp_outputs"][0]["unreachable_packets"]
                .as_u64()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    let stats = worker.stats();
                    let output = &stats["flussonix_rtp_outputs"][0];
                    assert_ne!(output["status"], "failed", "{stats}");
                    assert!(!worker.is_closed());
                    if output["unreachable_packets"].as_u64().unwrap() > before {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("configured output must stay alive with receiver absent");
            let target = d.path().join(format!("received-{phase}.ts"));
            let log = d.path().join(format!("receiver-{phase}.log"));
            let mut receiver = Command::new("ffmpeg")
                .args([
                    "-v",
                    "error",
                    "-y",
                    "-protocol_whitelist",
                    "file,udp,rtp",
                    "-localaddr",
                    "127.0.0.1",
                    "-listen_timeout",
                    "8",
                    "-probesize",
                    "262144",
                    "-analyzeduration",
                    "1000000",
                    "-f",
                    "sdp",
                    "-i",
                ])
                .arg(&sdp)
                .args([
                    "-t", "6", "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-f", "mpegts",
                ])
                .arg(&target)
                .stderr(std::fs::File::create(&log).unwrap())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let received = tokio::time::timeout(Duration::from_secs(12), receiver.wait()).await;
            if received.is_err() {
                let _ = receiver.kill().await;
                let _ = receiver.wait().await;
            }
            assert!(
                received.is_ok() && received.unwrap().unwrap().success(),
                "{}",
                std::fs::read_to_string(&log).unwrap()
            );
            let decoded = Command::new("ffmpeg")
                .args([
                    "-v",
                    "error",
                    "-xerror",
                    "-err_detect",
                    "explode",
                    "-threads",
                    "1",
                    "-i",
                ])
                .arg(&target)
                .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
                .output()
                .await
                .unwrap();
            assert!(
                decoded.status.success() && decoded.stderr.is_empty(),
                "{}",
                String::from_utf8_lossy(&decoded.stderr)
            );
            let probe = Command::new("ffprobe")
                .args([
                    "-v",
                    "error",
                    "-count_frames",
                    "-show_entries",
                    "stream=codec_name,nb_read_frames",
                    "-of",
                    "json",
                ])
                .arg(&target)
                .output()
                .await
                .unwrap();
            assert!(probe.status.success() && probe.stderr.is_empty());
            let media: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
            let tracks = media["streams"].as_array().unwrap();
            for codec in ["h264", "aac"] {
                assert!(
                    tracks.iter().any(|s| s["codec_name"] == codec
                        && s["nb_read_frames"]
                            .as_str()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap()
                            > 20),
                    "{media}"
                );
            }
            assert_eq!(worker.pid(), pid);
            assert_eq!(engine.count().await, 1);
            assert!(worker.stats()["flussonix_rtp_outputs"][0]["last_error"].is_null());
        }
    })
    .catch_unwind()
    .await;
    let final_stats = worker.stats();
    engine.stop_all().await;
    assert_eq!(engine.count().await, 0);
    if let Ok(root) = std::env::var("FLUSSONIX_DIRECT_ARTIFACT_DIR") {
        let out = std::path::Path::new(&root).join("rtp-receiver-recovery");
        std::fs::create_dir_all(&out).unwrap();
        for name in [
            "receiver.sdp",
            "received-0.ts",
            "received-1.ts",
            "receiver-0.log",
            "receiver-1.log",
        ] {
            let path = d.path().join(name);
            if path.is_file() {
                std::fs::copy(path, out.join(name)).unwrap();
            }
        }
        std::fs::write(
            out.join("worker-stats.json"),
            serde_json::to_vec_pretty(&final_stats).unwrap(),
        )
        .unwrap();
    }
    if let Err(error) = run {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn unavailable_destination_still_fails_on_invalid_ts_framing() {
    for secure in [false, true] {
        let (rx, control, address) = pair().await;
        drop(rx);
        let mut sender = Sender::start(address, secure);
        sender.unavailable().await;
        sender.queue.send(Bytes::from(vec![0; 7 * 188])).unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut sender.task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sender.state.stats()["status"], "failed");
        assert_eq!(
            sender.state.stats()["last_error"],
            "RTP output TS framing invalid"
        );
        drop(control);
    }
}
