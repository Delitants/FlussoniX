//! Independent FFmpeg sender/SDP demuxer verifies delivered media, not command construction.
use flussonix::media::Engine;
use serde_json::{Value, json};
use std::{net::UdpSocket, path::Path, time::Duration};
use tokio::process::Command;
fn ports() -> (u16, Vec<UdpSocket>) {
    for _ in 0..64 {
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = a.local_addr().unwrap().port();
        if p > 65520 {
            continue;
        }
        let mut v = vec![a];
        for n in p + 1..p + 4 {
            if let Ok(s) = UdpSocket::bind(("127.0.0.1", n)) {
                v.push(s);
            } else {
                break;
            }
        }
        if v.len() == 4 {
            return (p, v);
        }
    }
    panic!("owned RTP ports unavailable")
}
async fn decode(path: &Path, video: Option<&str>, audio: &[&str]) {
    let probe = Command::new("/usr/bin/ffprobe")
        .args([
            "-v",
            "error",
            "-show_streams",
            "-count_frames",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .await
        .unwrap();
    assert!(
        probe.status.success(),
        "{}",
        String::from_utf8_lossy(&probe.stderr)
    );
    let data: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let streams = data["streams"].as_array().unwrap();
    if let Some(codec) = video {
        let track = streams
            .iter()
            .find(|s| s["codec_name"] == codec)
            .unwrap_or_else(|| panic!("{data}"));
        assert!(
            track["nb_read_frames"]
                .as_str()
                .unwrap()
                .parse::<u32>()
                .unwrap()
                >= 20,
            "{data}"
        );
    }
    for codec in audio {
        assert!(
            streams.iter().any(|s| s["codec_name"] == *codec
                && s["nb_read_frames"]
                    .as_str()
                    .is_some_and(|n| n.parse::<u32>().is_ok_and(|n| n >= 20))),
            "{data}"
        );
    }
    let decoded = Command::new("/usr/bin/ffmpeg")
        .args(["-nostdin", "-v", "error", "-xerror", "-i"])
        .arg(path)
        .args(["-map", "0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success() && decoded.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
}
pub async fn qualify(
    video: Option<&str>,
    audio: &[&str],
    profile: Value,
    secure_input: bool,
    secure_output: bool,
) {
    let d = tempfile::tempdir().unwrap();
    use base64::Engine as _;
    use std::os::unix::fs::PermissionsExt;
    let key = base64::engine::general_purpose::STANDARD.encode([0x31; 30]);
    let key_path = d.path().join("owned.key");
    std::fs::write(&key_path, &key).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let input_scheme = if secure_input { "srtp" } else { "rtp" };
    let output_scheme = if secure_output { "srtp" } else { "rtp" };

    // Optional private evidence retains independent decoder diagnostics, including
    // startup failures before the output receiver exists.
    let artifact = std::env::var("FLUSSONIX_ELEMENTARY_ARTIFACT_DIR")
        .ok()
        .map(|base| {
            let p = Path::new(&base).join(format!(
                "{}-{}-{}-{}-{}",
                video.unwrap_or("audio"),
                audio.join("-"),
                profile["encoder"].as_str().unwrap_or("copy"),
                secure_input,
                secure_output
            ));
            std::fs::create_dir_all(&p).unwrap();
            p
        });
    let (input, reserved) = ports();
    let (input_sdp, output_sdp) = (d.path().join("input.sdp"), d.path().join("output.sdp"));
    let mut sender = Command::new("/usr/bin/ffmpeg");
    sender.args([
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-re",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=320x180:rate=25",
        "-re",
        "-f",
        "lavfi",
        "-i",
        "sine=sample_rate=48000",
    ]);
    let mut lane = 0;
    if let Some(codec) = video {
        sender.args([
            "-map",
            "0:v:0",
            "-an",
            "-c:v",
            codec,
            "-threads",
            "1",
            "-g",
            "25",
            "-bf",
            "0",
            "-preset",
            "ultrafast",
            "-flags",
            "+global_header",
        ]);
        if codec == "libx265" {
            sender.args(["-x265-params", "pools=1:frame-threads=1:log-level=error"]);
        } else {
            sender.args(["-tune", "zerolatency"]);
        }
        if secure_input {
            sender.args([
                "-srtp_out_suite",
                "AES_CM_128_HMAC_SHA1_80",
                "-srtp_out_params",
                &key,
            ]);
        }
        sender.args(["-f", "rtp"]).arg(format!(
            "{input_scheme}://127.0.0.1:{}?pkt_size=1200",
            input + lane * 2
        ));
        lane += 1;
    }
    for codec in audio {
        if secure_input {
            sender.args([
                "-srtp_out_suite",
                "AES_CM_128_HMAC_SHA1_80",
                "-srtp_out_params",
                &key,
            ]);
        }
        sender
            .args([
                "-map",
                "1:a:0",
                "-vn",
                "-c:a",
                codec,
                "-b:a",
                if *codec == "mp2" { "192k" } else { "128k" },
                "-ac",
                "2",
                "-ar",
                "48000",
                "-f",
                "rtp",
            ])
            .arg(format!(
                "{input_scheme}://127.0.0.1:{}?pkt_size=1200",
                input + lane * 2
            ));
        lane += 1;
    }
    sender
        .arg("-sdp_file")
        .arg(&input_sdp)
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(d.path().join("sender.log")).unwrap())
        .kill_on_drop(true);
    let mut sender = sender.spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if std::fs::read(&input_sdp).is_ok_and(|b| b.starts_with(b"v=0") && b.ends_with(b"\n"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    if secure_input {
        // FFmpeg emits AVP + inline SDES even for encrypted SRTP. Only the owned
        // test fixture converts it to our explicitly out-of-band SAVP descriptor.
        let source = std::fs::read_to_string(&input_sdp).unwrap();
        let text = source
            .lines()
            .filter(|line| !line.starts_with("a=crypto:"))
            .map(|line| line.replace("RTP/AVP", "RTP/SAVP"))
            .collect::<Vec<_>>()
            .join("\r\n")
            + "\r\n";
        std::fs::write(&input_sdp, text).unwrap();
        std::fs::set_permissions(&input_sdp, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let input_reserved = reserved;
    // Keep the output pairs owned until FFmpeg is ready to bind them; the native
    // sender can publish actual SDP without provoking ICMP destination failures.
    let (output, reserved) = ports();
    let executable = if let Some(out) = &artifact {
        use std::os::unix::fs::PermissionsExt;
        let wrapper = out.join("owned-ffmpeg");
        // Paths come from our private artifact directory, never stream settings.
        let quote = |p: &Path| format!("'{}'", p.display().to_string().replace('\'', "'\\''"));
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nexec /usr/bin/ffmpeg \"$@\" 2> {}\n",
                quote(&out.join("worker-args.txt")),
                quote(&out.join("worker.log"))
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        wrapper
    } else {
        "/usr/bin/ffmpeg".into()
    };
    let engine = Engine::new(d.path().join("media"), executable.to_str().unwrap());
    if profile["encoder"]
        .as_str()
        .is_some_and(|e| e.ends_with("_vaapi"))
    {
        engine
            .ensure(
                "owned-warm",
                &json!({"inputs":[{"url":"testsrc://"}],"transcoder":profile}),
            )
            .await
            .unwrap();
        engine.stop_all().await;
    }
    drop(input_reserved);
    let mut cfg = json!({"inputs":[{"url":format!("{input_scheme}://127.0.0.1:{input}"),"flussonix_rtp":{"profile":"elementary","sdp_file":input_sdp}}],"flussonix_rtp_outputs":[{"url":format!("{output_scheme}://127.0.0.1:{output}"),"flussonix_rtp":{"profile":"elementary"}}],"transcoder":profile,"flussonix_input_timeout":15});
    if secure_input {
        cfg["inputs"][0]["flussonix_rtp"]["key_file"] = json!(key_path);
    }
    if secure_output {
        cfg["flussonix_rtp_outputs"][0]["flussonix_rtp"]["key_file"] = json!(key_path);
    }
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    let record_cancel = tokio_util::sync::CancellationToken::new();
    let mut ts = worker.subscribe();
    let mut record = std::fs::File::create(d.path().join("worker.ts")).unwrap();
    let c = record_cancel.clone();
    let recording = tokio::spawn(async move {
        use std::io::Write;
        loop {
            tokio::select! {biased;_=c.cancelled()=>break,data=ts.recv()=>match data {Ok(data)=>record.write_all(&data).unwrap(),Err(_)=>break}}
        }
    });
    let text = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Ok(s) = engine.rtp_sdp("owned", 0, &cfg).await {
                break s;
            }
            if !worker.alive.load(std::sync::atomic::Ordering::Relaxed) {
                panic!("{}", worker.stats());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        if let Some(out) = &artifact {
            for file in ["input.sdp", "sender.log"] {
                let _ = std::fs::copy(d.path().join(file), out.join(file));
            }
            std::fs::write(
                out.join("worker-stats.json"),
                serde_json::to_vec_pretty(&worker.stats()).unwrap(),
            )
            .unwrap();
        }
        panic!("SDP startup: {}", worker.stats())
    });
    std::fs::write(&output_sdp, &text).unwrap();
    // Keep public receiver ports continuously owned. A test-only UDP forwarder
    // delivers unchanged datagrams to independent FFmpeg after its sockets bind.
    // This avoids an artificial ICMP gap while handing a reserved port to a child.
    let (private, private_reserved) = ports();
    let mut receiver_text = String::new();
    let mut track = 0;
    for line in text.lines() {
        if line.starts_with("m=") {
            let mut fields: Vec<_> = line.split_whitespace().map(str::to_owned).collect();
            fields[1] = (private + track * 2).to_string();
            track += 1;
            receiver_text.push_str(&fields.join(" "));
        } else {
            receiver_text.push_str(line);
        }
        receiver_text.push_str("\r\n");
        if secure_output && line.starts_with("m=") {
            receiver_text.push_str(&format!(
                "a=crypto:1 AES_CM_128_HMAC_SHA1_80 inline:{key}\r\n"
            ));
        }
    }
    let receiver_sdp = d.path().join("receiver.sdp");
    std::fs::write(&receiver_sdp, receiver_text).unwrap();
    std::fs::set_permissions(&receiver_sdp, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(private_reserved);
    let received = d.path().join("received.ts");
    let mut receiver = Command::new("/usr/bin/ffmpeg")
        .args([
            "-nostdin",
            "-hide_banner",
            "-v",
            "error",
            "-y",
            "-protocol_whitelist",
            "file,udp,rtp,srtp",
            "-probesize",
            "1048576",
            "-analyzeduration",
            "1000000",
            "-f",
            "sdp",
            "-i",
        ])
        .arg(&receiver_sdp)
        .args([
            "-t",
            "3",
            "-map",
            "0",
            "-c",
            "copy",
            "-copyinkf:a",
            "-f",
            "mpegts",
        ])
        .arg(&received)
        .stderr(std::fs::File::create(d.path().join("receiver.log")).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let ready = (private..private + track * 2).all(|p| {
                UdpSocket::bind(("127.0.0.1", p))
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::AddrInUse)
            });
            if ready {
                break;
            }
            assert!(
                receiver.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(d.path().join("receiver.log")).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut relay = tokio::task::JoinSet::new();
    for (i, socket) in reserved
        .into_iter()
        .enumerate()
        .take(usize::from(track) * 2)
    {
        socket.set_nonblocking(true).unwrap();
        let socket = tokio::net::UdpSocket::from_std(socket).unwrap();
        let c = cancel.clone();
        let mut dump = artifact
            .as_ref()
            .map(|out| std::fs::File::create(out.join(format!("output-{i}.rtp"))).unwrap());
        relay.spawn(async move { let mut bytes=[0;2048]; loop {
            let n = tokio::select! {biased;_=c.cancelled()=>return,r=socket.recv_from(&mut bytes)=>r.unwrap().0};
            if let Some(dump) = &mut dump { use std::io::Write; dump.write_all(&(n as u32).to_be_bytes()).unwrap();dump.write_all(&bytes[..n]).unwrap(); }
            tokio::select! {biased;_=c.cancelled()=>return,r=socket.send_to(&bytes[..n], ("127.0.0.1", private + i as u16))=>{let _=r;}}
        }});
    }
    let result = tokio::time::timeout(Duration::from_secs(15), receiver.wait()).await;
    cancel.cancel();
    while relay.join_next().await.is_some() {}
    record_cancel.cancel();
    recording.await.unwrap();
    let _ = sender.kill().await;
    let _ = sender.wait().await;
    if result.is_err() {
        let _ = receiver.kill().await;
        let _ = receiver.wait().await;
    }
    let stats = worker.stats();
    engine.stop_all().await;
    if let Ok(base) = std::env::var("FLUSSONIX_ELEMENTARY_ARTIFACT_DIR") {
        let out = Path::new(&base).join(format!(
            "{}-{}-{}-{}-{}",
            video.unwrap_or("audio"),
            audio.join("-"),
            profile["encoder"].as_str().unwrap_or("copy"),
            secure_input,
            secure_output
        ));
        std::fs::create_dir_all(&out).unwrap();
        for file in [
            "input.sdp",
            "output.sdp",
            "receiver.sdp",
            "sender.log",
            "receiver.log",
            "received.ts",
            "worker.ts",
        ] {
            let from = d.path().join(file);
            if from.exists() {
                if file == "receiver.sdp" && secure_output {
                    let cleaned = std::fs::read_to_string(from)
                        .unwrap()
                        .lines()
                        .filter(|l| !l.starts_with("a=crypto:"))
                        .collect::<Vec<_>>()
                        .join("\r\n");
                    std::fs::write(out.join(file), cleaned).unwrap();
                } else {
                    std::fs::copy(from, out.join(file)).unwrap();
                }
            }
        }
        std::fs::write(
            out.join("worker-stats.json"),
            serde_json::to_vec_pretty(&stats).unwrap(),
        )
        .unwrap();
    }
    let status = result
        .unwrap_or_else(|_| {
            panic!(
                "Receiver stalled: {stats}; {}",
                std::fs::read_to_string(d.path().join("receiver.log")).unwrap()
            )
        })
        .unwrap();
    assert!(
        status.success(),
        "{stats}; {}",
        std::fs::read_to_string(d.path().join("receiver.log")).unwrap()
    );
    let output_video = match profile["encoder"].as_str() {
        Some("libx265") => Some("hevc"),
        Some("h264_vaapi") => Some("h264"),
        _ => video.map(|v| if v == "libx265" { "hevc" } else { "h264" }),
    };
    let expected: Vec<_> = audio
        .iter()
        .map(|a| match profile["acodec"].as_str() {
            Some("mp3") => "mp3",
            Some("mp2a") => "mp2",
            _ => {
                if *a == "libmp3lame" {
                    "mp3"
                } else {
                    *a
                }
            }
        })
        .collect();
    decode(&received, output_video, &expected).await;
    assert!(!Path::new(&format!("/proc/{}", worker.pid())).exists());
}
