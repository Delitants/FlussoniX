use flussonix::media::Engine;
use serde_json::json;
use std::time::Duration;
use tokio::process::Command;
fn port() -> u16 {
    for _ in 0..64 {
        let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let n = a.local_addr().unwrap().port();
        if n < 65535 && std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, n + 1)).is_ok() {
            return n;
        }
    }
    panic!("no pair");
}
#[tokio::test]
async fn independent_rtp_source_cpu_transcode_and_direct_output_decode() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let input = port();
    let output = port();
    assert_ne!(input, output);
    let cfg = json!({"inputs":[{"url":format!("rtp://127.0.0.1:{input}")}],"transcoder":{"encoder":"libx264","vb":100,"acodec":"aac","ab":64},"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{output}")}],"flussonix_input_timeout":10});
    let worker = e
        .ensure_guarded("owned", &cfg, true, std::future::ready(true))
        .await
        .unwrap();
    let target = d.path().join("received.ts");
    let logfile = std::fs::File::create(d.path().join("receiver.log")).unwrap();
    let mut receiver = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-y",
            "-probesize",
            "262144",
            "-analyzeduration",
            "1000000",
            "-i",
            &format!("rtp://127.0.0.1:{output}?timeout=12000000"),
            "-t",
            "6",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-c",
            "copy",
            "-f",
            "mpegts",
        ])
        .arg(&target)
        .stderr(logfile)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let sender_log = std::fs::File::create(d.path().join("sender.log")).unwrap();
    let mut sender = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x90:rate=25",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "sine=sample_rate=48000",
            "-t",
            "12",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "25",
            "-c:a",
            "aac",
            "-f",
            "rtp_mpegts",
            &format!("rtp://127.0.0.1:{input}?pkt_size=1328"),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(sender_log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(16), receiver.wait())
        .await
        .unwrap()
        .unwrap();
    let _ = sender.kill().await;
    let _ = sender.wait().await;
    if let Ok(out) = std::env::var("FLUSSONIX_DIRECT_ARTIFACT_DIR") {
        let out = std::path::Path::new(&out);
        std::fs::create_dir_all(out).unwrap();
        for name in ["received.ts", "receiver.log", "sender.log"] {
            let file = d.path().join(name);
            if file.is_file() {
                std::fs::copy(file, out.join(name)).unwrap();
            }
        }
        std::fs::write(
            out.join("worker-stats.json"),
            serde_json::to_vec_pretty(&worker.stats()).unwrap(),
        )
        .unwrap();
        if let Ok(args) = std::fs::read(format!("/proc/{}/cmdline", worker.pid())) {
            std::fs::write(out.join("ffmpeg-args.txt"), args).unwrap();
        }
        let dir = d.path().join(format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(b"owned")
        ));
        if let Ok(files) = std::fs::read_dir(dir) {
            for f in files {
                let f = f.unwrap();
                if f.path().extension().is_some_and(|e| e == "ts") {
                    std::fs::copy(f.path(), out.join(f.file_name())).unwrap();
                }
            }
        }
    }
    assert!(
        received.success(),
        "{}; source: {}",
        std::fs::read_to_string(d.path().join("receiver.log")).unwrap(),
        std::fs::read_to_string(d.path().join("sender.log")).unwrap()
    );
    let decoded = Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
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
            "stream=codec_name,codec_type,nb_read_frames",
            "-of",
            "json",
        ])
        .arg(&target)
        .output()
        .await
        .unwrap();
    assert!(probe.status.success() && probe.stderr.is_empty());
    let probe: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
    let streams = probe["streams"].as_array().unwrap();
    assert!(streams.iter().any(|s| {
        s["codec_name"] == "h264"
            && s["nb_read_frames"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 20
    }));
    assert!(streams.iter().any(|s| {
        s["codec_name"] == "aac"
            && s["nb_read_frames"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 20
    }));
    assert_eq!(worker.stats()["input_protocol"], "rtp");
    assert!(
        worker.stats()["direct_rtp_input"]["packets"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        worker.stats()["flussonix_rtp_outputs"][0]["packets"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(e.direct_egress.load(std::sync::atomic::Ordering::Relaxed) > 0);
    assert_eq!(e.count().await, 1);
    e.stop_all().await;
    assert_eq!(e.count().await, 0);
    assert!(
        tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, input))
            .await
            .is_ok()
    );
    assert!(
        tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, input + 1))
            .await
            .is_ok()
    );
}
#[path = "support/direct_media.rs"]
mod matrix;
#[tokio::test]
async fn independent_rtp_copy_codec_matrix_and_audio_only() {
    for video in [Some("libx264"), Some("libx265"), None] {
        matrix::run(video, false, None).await;
    }
}
#[tokio::test]
#[ignore = "requires explicitly provided independent Intel VAAPI fixture"]
async fn independent_igpu_encoded_rtp_delivered_media() {
    let fixture =
        std::env::var("FLUSSONIX_DIRECT_VIDEO_FIXTURE").expect("owned hardware fixture path");
    let codec = std::env::var("FLUSSONIX_DIRECT_VIDEO_CODEC").expect("h264_vaapi or hevc_vaapi");
    assert!(["h264_vaapi", "hevc_vaapi"].contains(&codec.as_str()));
    matrix::run(Some(&codec), false, Some(std::path::Path::new(&fixture))).await;
}
