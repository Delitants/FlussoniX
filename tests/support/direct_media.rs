//! Independent FFmpeg transport encoder/receiver, shared across RTP and SRTP tests.
use flussonix::media::Engine;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::process::Command;
pub fn port() -> u16 {
    for _ in 0..64 {
        let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = a.local_addr().unwrap().port();
        if p < 65535 && std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, p + 1)).is_ok() {
            return p;
        }
    }
    panic!("no port pair")
}
#[allow(dead_code)]
pub async fn run(video: Option<&str>, secure: bool, fixture: Option<&Path>) {
    run_profile(video, secure, fixture, false, false).await;
}
#[allow(dead_code)]
pub async fn run_cpu(secure: bool) {
    run_profile(Some("libx264"), secure, None, true, false).await;
}
#[allow(dead_code)]
pub async fn run_fmp4_copy(video: &str, secure: bool) {
    run_profile(Some(video), secure, None, false, true).await;
}
async fn run_profile(
    video: Option<&str>,
    secure: bool,
    fixture: Option<&Path>,
    transcode: bool,
    require_fmp4: bool,
) {
    let d = tempfile::tempdir().unwrap();
    if secure {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let input = port();
    let output = port();
    assert_ne!(input, output);
    let scheme = if secure { "srtp" } else { "rtp" };
    let key = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0x39u8; 30]);
    let key_path = d.path().join("owned.key");
    std::fs::write(&key_path, &key).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let opts = if secure {
        json!({"key_file":key_path})
    } else {
        json!({})
    };
    let mut cfg = json!({"inputs":[{"url":format!("{scheme}://127.0.0.1:{input}"),"flussonix_rtp":opts}],"flussonix_rtp_outputs":[{"url":format!("{scheme}://127.0.0.1:{output}"),"flussonix_rtp":opts}],"flussonix_input_timeout":15});
    if transcode {
        cfg["transcoder"] = json!({"encoder":"libx264","vb":100,"acodec":"aac","ab":64});
    }
    let mut e = Engine::new(d.path().join("media"), "ffmpeg");
    if secure {
        let history = d.path().join("receiver-history");
        flussonix::direct_rtp::key_ledger::Ledger::initialize(&history).unwrap();
        e.set_srtp_receiver_ledger(Some(std::sync::Arc::new(
            flussonix::direct_rtp::key_ledger::Ledger::new(history),
        )));
    }
    let worker = e
        .ensure_guarded("owned", &cfg, true, std::future::ready(true))
        .await
        .unwrap();
    let target = d.path().join("received.ts");
    let mut rx = Command::new("ffmpeg");
    rx.args([
        "-v",
        "error",
        "-y",
        "-probesize",
        "262144",
        "-analyzeduration",
        "1000000",
    ]);
    let receiver_input = if secure {
        // Retain the independent SRTP protocol receiver and its input key.
        rx.args([
            "-srtp_in_suite",
            "AES_CM_128_HMAC_SHA1_80",
            "-srtp_in_params",
            &key,
        ]);
        format!("{scheme}://127.0.0.1:{output}?timeout=18000000")
    } else {
        // Explicit MP2T SDP avoids the RTP URL payload probe's socket reopen.
        let sdp = d.path().join("receiver.sdp");
        std::fs::write(&sdp, format!(
            "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=Owned MP2T receiver\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=video {output} RTP/AVP 33\r\na=rtpmap:33 MP2T/90000\r\n"
        )).unwrap();
        rx.args([
            "-protocol_whitelist",
            "file,udp,rtp",
            "-localaddr",
            "127.0.0.1",
            "-listen_timeout",
            "18",
            "-f",
            "sdp",
        ]);
        sdp.to_str().unwrap().to_owned()
    };
    rx.args([
        "-i",
        &receiver_input,
        "-t",
        "6",
        "-map",
        "0:v?",
        "-map",
        "0:a",
        "-c",
        "copy",
        "-f",
        "mpegts",
    ])
    .arg(&target)
    .stderr(std::fs::File::create(d.path().join("receiver.log")).unwrap())
    .kill_on_drop(true);
    let mut receiver = rx.spawn().unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut tx = Command::new("ffmpeg");
    tx.args(["-v", "error"]);
    if let Some(f) = fixture {
        tx.args(["-re", "-stream_loop", "-1", "-i"])
            .arg(f)
            .args(["-map", "0:v?", "-map", "0:a", "-c", "copy"]);
    } else {
        if video.is_some() {
            tx.args(["-re", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25"]);
        }
        tx.args(["-re", "-f", "lavfi", "-i", "sine=sample_rate=48000"]);
        let audio = if video.is_some() { "1:a" } else { "0:a" };
        if let Some(codec) = video {
            tx.args([
                "-map",
                "0:v",
                "-c:v",
                codec,
                "-threads",
                "1",
                "-preset",
                "ultrafast",
                "-g",
                "25",
                "-bf",
                "0",
            ]);
            if codec == "libx265" {
                tx.args(["-x265-params", "pools=1:frame-threads=1:log-level=error"]);
            }
        }
        // Exercise AAC both before and after MPEG audio: the fMP4 filter
        // must follow PMT order rather than assume AAC is the first audio.
        let (first_audio, last_audio) = if require_fmp4 && !secure {
            ("libmp3lame", "aac")
        } else {
            ("aac", "libmp3lame")
        };
        tx.args([
            "-map",
            audio,
            "-map",
            audio,
            "-map",
            audio,
            "-c:a:0",
            first_audio,
            "-c:a:1",
            "mp2",
            "-c:a:2",
            last_audio,
            "-b:a",
            "96k",
        ]);
    }
    if secure {
        tx.args([
            "-srtp_out_suite",
            "AES_CM_128_HMAC_SHA1_80",
            "-srtp_out_params",
            &key,
        ]);
    }
    tx.args([
        "-t",
        "16",
        "-f",
        "rtp_mpegts",
        &format!("{scheme}://127.0.0.1:{input}?pkt_size=1328"),
    ])
    .stdout(std::process::Stdio::null())
    .stderr(std::fs::File::create(d.path().join("sender.log")).unwrap())
    .kill_on_drop(true);
    let mut sender = tx.spawn().unwrap();
    let received = tokio::time::timeout(Duration::from_secs(22), receiver.wait()).await;
    let fmp4 = if require_fmp4 {
        Some(inspect_fmp4(&e, d.path(), video.expect("fMP4 fixture video codec")).await)
    } else {
        None
    };
    let _ = sender.kill().await;
    let _ = sender.wait().await;
    if let Ok(path) = std::env::var("FLUSSONIX_DIRECT_ARTIFACT_DIR") {
        let out = Path::new(&path).join(format!(
            "{scheme}-{}{}",
            video.unwrap_or("audio"),
            if transcode { "-cpu" } else { "" }
        ));
        std::fs::create_dir_all(&out).unwrap();
        for name in ["received.ts", "receiver.log", "sender.log", "sample.mp4"] {
            if d.path().join(name).is_file() {
                std::fs::copy(d.path().join(name), out.join(name)).unwrap();
            }
        }
        std::fs::write(
            out.join("stats.json"),
            serde_json::to_vec_pretty(&worker.stats()).unwrap(),
        )
        .unwrap();
    }
    e.stop_all().await;
    assert_eq!(e.count().await, 0);
    if let Some(result) = fmp4 {
        result.unwrap_or_else(|error| {
            panic!(
                "fMP4 copy failed: {error}; sender: {}; worker: {}",
                std::fs::read_to_string(d.path().join("sender.log")).unwrap_or_default(),
                worker.stats()
            )
        });
    }
    assert!(
        received.is_ok(),
        "receiver timeout; source {}",
        std::fs::read_to_string(d.path().join("sender.log")).unwrap()
    );
    assert!(
        received.unwrap().unwrap().success(),
        "{}",
        std::fs::read_to_string(d.path().join("receiver.log")).unwrap()
    );
    let decoded = Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(&target)
        .args(["-map", "0:v?", "-map", "0:a", "-f", "null", "-"])
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
    let probe: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let streams = probe["streams"].as_array().unwrap();
    let expected_video = match video {
        Some("libx265") | Some("hevc_vaapi") => Some("hevc"),
        Some(_) => Some("h264"),
        None => None,
    };
    if let Some(codec) = expected_video {
        assert!(
            streams
                .iter()
                .any(|s| s["codec_name"] == codec && frames(s) > 20),
            "{probe}"
        );
    } else {
        assert!(streams.iter().all(|s| s["codec_type"] != "video"));
    }
    let audio_codecs = if fixture.is_some() || transcode {
        vec!["aac"]
    } else {
        vec!["aac", "mp2", "mp3"]
    };
    for codec in audio_codecs {
        assert!(
            streams
                .iter()
                .any(|s| s["codec_name"] == codec && frames(s) > 20),
            "missing {codec}: {probe}"
        );
    }
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
async fn inspect_fmp4(engine: &Engine, directory: &Path, video: &str) -> Result<(), String> {
    let playlist = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(data) = engine.read("owned", "fmp4/index.m3u8").await {
                let text = String::from_utf8_lossy(&data);
                if text.contains("#EXTINF:") {
                    break text.into_owned();
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| "fMP4 playlist did not become ready".to_owned())?;
    let init = playlist
        .lines()
        .find_map(|line| line.strip_prefix("#EXT-X-MAP:URI=\""))
        .and_then(|uri| uri.split('"').next())
        .ok_or_else(|| format!("fMP4 init absent: {playlist}"))?;
    let segment = playlist
        .lines()
        .rfind(|line| !line.is_empty() && !line.starts_with('#'))
        .ok_or_else(|| format!("fMP4 segment absent: {playlist}"))?;
    let mut bytes = engine
        .read("owned", &format!("fmp4/{init}"))
        .await
        .map_err(|error| error.to_string())?
        .to_vec();
    bytes.extend_from_slice(
        &engine
            .read("owned", &format!("fmp4/{segment}"))
            .await
            .map_err(|error| error.to_string())?,
    );
    let sample = directory.join("sample.mp4");
    std::fs::write(&sample, bytes).map_err(|error| error.to_string())?;
    let probed = tokio::time::timeout(
        Duration::from_secs(10),
        Command::new("ffprobe")
            .kill_on_drop(true)
            .args([
                "-v",
                "error",
                "-show_entries",
                "stream=index,codec_name",
                "-of",
                "json",
            ])
            .arg(&sample)
            .output(),
    )
    .await
    .map_err(|_| "fMP4 codec probe timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    if !probed.status.success() || !probed.stderr.is_empty() {
        return Err(format!(
            "fMP4 codec probe failed: {}",
            String::from_utf8_lossy(&probed.stderr)
        ));
    }
    let probe: Value = serde_json::from_slice(&probed.stdout).map_err(|error| error.to_string())?;
    let streams = probe["streams"]
        .as_array()
        .ok_or_else(|| format!("fMP4 stream list absent: {probe}"))?;
    let codecs: Vec<&str> = streams
        .iter()
        .filter_map(|stream| stream["codec_name"].as_str())
        .collect();
    let expected_video = if video == "libx265" { "hevc" } else { "h264" };
    if codecs.len() != 4
        || codecs
            .iter()
            .filter(|codec| **codec == expected_video)
            .count()
            != 1
        || codecs.iter().filter(|codec| **codec == "aac").count() != 1
        || codecs
            .iter()
            .filter(|codec| matches!(**codec, "mp2" | "mp3"))
            .count()
            != 2
    {
        return Err(format!(
            "fMP4 codecs {codecs:?}, expected {expected_video}, AAC and two MPEG audio tracks; probe={probe}"
        ));
    }
    let mpeg_streams: Vec<u64> = streams
        .iter()
        // FFmpeg 7 labels both MPEG audio layers `mp3` in this MP4 container.
        // Check payload layer bits below rather than depending on that label.
        .filter(|stream| matches!(stream["codec_name"].as_str(), Some("mp2" | "mp3")))
        .filter_map(|stream| stream["index"].as_u64())
        .collect();
    let mut layers = Vec::new();
    for index in mpeg_streams {
        let packet = tokio::time::timeout(
            Duration::from_secs(10),
            Command::new("ffmpeg")
                .kill_on_drop(true)
                .args(["-nostdin", "-v", "error", "-i"])
                .arg(&sample)
                .args([
                    "-map",
                    &format!("0:{index}"),
                    "-c",
                    "copy",
                    "-frames:a",
                    "1",
                    "-f",
                    "data",
                    "-",
                ])
                .output(),
        )
        .await
        .map_err(|_| format!("fMP4 MPEG audio packet {index} timed out"))?
        .map_err(|error| error.to_string())?;
        if !packet.status.success() || !packet.stderr.is_empty() || packet.stdout.len() < 4 {
            return Err(format!("fMP4 MPEG audio packet {index} unavailable"));
        }
        let header = u32::from_be_bytes(packet.stdout[..4].try_into().unwrap());
        if (header >> 21) & 0x7ff != 0x7ff {
            return Err(format!("fMP4 MPEG audio packet {index} lacks frame sync"));
        }
        layers.push((header >> 17) & 3);
    }
    layers.sort_unstable();
    if layers != [1, 2] {
        return Err(format!(
            "fMP4 MPEG audio layers {layers:?}, expected III and II"
        ));
    }
    let decoded = tokio::time::timeout(
        Duration::from_secs(10),
        Command::new("ffmpeg")
            .kill_on_drop(true)
            .args([
                "-nostdin",
                "-v",
                "error",
                "-xerror",
                "-err_detect",
                "explode",
                "-i",
            ])
            .arg(&sample)
            .args([
                "-map", "0:v:0", "-map", "0:a", "-threads", "1", "-f", "framemd5", "-",
            ])
            .output(),
    )
    .await
    .map_err(|_| "fMP4 decode timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    if !decoded.status.success() || !decoded.stderr.is_empty() {
        return Err(format!(
            "fMP4 decode failed: {}; playlist={playlist}",
            String::from_utf8_lossy(&decoded.stderr)
        ));
    }
    let frames = String::from_utf8_lossy(&decoded.stdout);
    for stream in 0..4 {
        let count = frames
            .lines()
            .filter(|line| line.starts_with(&format!("{stream},")))
            .count();
        if count < 20 {
            return Err(format!(
                "fMP4 stream {stream} has {count} decoded frames; playlist={playlist}; frames={frames}"
            ));
        }
    }
    Ok(())
}
fn frames(s: &Value) -> u64 {
    s["nb_read_frames"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}
