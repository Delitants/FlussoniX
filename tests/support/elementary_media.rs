//! Independent FFmpeg sender/SDP demuxer verifies delivered media, not command construction.
use flussonix::media::Engine;
use serde_json::{Value, json};
use std::{net::UdpSocket, path::Path, time::Duration};
use tokio::process::Command;
#[path = "decoder_readiness.rs"]
mod decoder_readiness;
#[path = "elementary_diagnostics.rs"]
mod elementary_diagnostics;
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
    let probe = tokio::time::timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/ffprobe")
            .args([
                "-v",
                "error",
                "-show_streams",
                "-count_frames",
                "-of",
                "json",
            ])
            .arg(path)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap_or_else(|_| panic!("Probe stalled: {}", path.display()))
    .unwrap();
    assert!(
        probe.status.success() && probe.stderr.is_empty(),
        "Probe {}: {}",
        path.display(),
        String::from_utf8_lossy(&probe.stderr)
    );
    let data: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let streams = data["streams"].as_array().unwrap();
    let mut expected = audio.to_vec();
    expected.extend(video);
    expected.sort_unstable();
    let mut actual: Vec<_> = streams
        .iter()
        .map(|stream| stream["codec_name"].as_str().unwrap())
        .collect();
    actual.sort_unstable();
    assert_eq!(actual, expected, "Tracks {}: {data}", path.display());
    for stream in streams {
        assert!(
            stream["nb_read_frames"]
                .as_str()
                .is_some_and(|n| n.parse::<u32>().is_ok_and(|n| n >= 20)),
            "Frames {}: {data}",
            path.display()
        );
    }
    let decoded = tokio::time::timeout(
        Duration::from_secs(30),
        Command::new("/usr/bin/ffmpeg")
            .args(["-nostdin", "-v", "error", "-xerror", "-i"])
            .arg(path)
            .args(["-map", "0", "-f", "null", "-"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap_or_else(|_| panic!("Decode stalled: {}", path.display()))
    .unwrap();
    assert!(
        decoded.status.success() && decoded.stderr.is_empty(),
        "Decode {}: {}",
        path.display(),
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
    let ffmpeg =
        std::env::var("FLUSSONIX_TEST_FFMPEG").unwrap_or_else(|_| "/usr/bin/ffmpeg".into());
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
    let evidence = artifact
        .as_ref()
        .map(|out| elementary_diagnostics::Evidence::start(d.path(), out).unwrap());
    let stage = |name| {
        if let Some(evidence) = &evidence {
            evidence.stage(name);
        }
    };
    if let Some(evidence) = &evidence {
        evidence.watch_process("native_test_process", std::process::id());
    }
    let (input, reserved) = ports();
    let (input_sdp, output_sdp) = (d.path().join("input.sdp"), d.path().join("output.sdp"));
    let mut sender = Command::new(&ffmpeg);
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
    stage("sender_spawn");
    let mut sender = sender.spawn().unwrap();
    if let Some(evidence) = &evidence {
        evidence.watch_process("sender", sender.id().unwrap());
    }
    stage("sender_sdp_startup");
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
    // Always retain worker diagnostics until this fixture finishes. Startup
    // failures must report the actual independent decoder error in CI.
    let diagnostics = d.path();
    let executable = diagnostics.join("owned-ffmpeg");
    let quote = |p: &Path| format!("'{}'", p.display().to_string().replace('\'', "'\\''"));
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\numask 077\nprintf '%s\\n' \"$@\" > {}\nexec {} \"$@\" 2> {}\n",
            quote(&diagnostics.join("worker-args.txt")),
            quote(Path::new(&ffmpeg)),
            quote(&diagnostics.join("worker.log"))
        ),
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
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
    stage("worker_spawn");
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    if let Some(evidence) = &evidence {
        evidence.watch_worker(&worker);
    }
    stage("worker_sdp_startup");
    let record_cancel = tokio_util::sync::CancellationToken::new();
    let mut ts = worker.subscribe();
    let mut record = std::fs::File::create(d.path().join("worker.ts")).unwrap();
    let c = record_cancel.clone();
    let recording = tokio::spawn(async move {
        use std::io::Write;
        loop {
            tokio::select! {biased;_=c.cancelled()=>break,data=ts.recv()=>match data {Ok(data)=>record.write_all(&data).unwrap(),Err(error)=>panic!("Shared MPEG-TS recording failed: {error}")}}
        }
    });
    let text = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Ok(s) = engine.rtp_sdp("owned", 0, &cfg).await {
                break s;
            }
            if !worker.alive.load(std::sync::atomic::Ordering::Relaxed) {
                panic!("case video={video:?} audio={audio:?} secure_input={secure_input} secure_output={secure_output}: {}; worker: {}; sender: {}", worker.stats(), std::fs::read_to_string(diagnostics.join("worker.log")).unwrap_or_default(), std::fs::read_to_string(d.path().join("sender.log")).unwrap_or_default());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("SDP startup: {}", worker.stats()));
    if ffmpeg == "/usr/bin/ffmpeg" {
        // Plaintext decoder sockets may only exist on the trusted loopback
        // boundary, even though public sockets are authenticated separately.
        let inodes: std::collections::HashSet<_> =
            std::fs::read_dir(format!("/proc/{}/fd", worker.pid()))
                .unwrap()
                .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
                .filter_map(|link| {
                    link.to_str()
                        .and_then(|s| s.strip_prefix("socket:["))
                        .and_then(|s| s.strip_suffix(']'))
                        .map(str::to_owned)
                })
                .collect();
        let table = std::fs::read_to_string("/proc/self/net/udp").unwrap();
        let local: Vec<_> = table
            .lines()
            .skip(1)
            .filter_map(|line| {
                let f: Vec<_> = line.split_ascii_whitespace().collect();
                if inodes.contains(*f.get(9)?) {
                    Some(f[1].to_owned())
                } else {
                    None
                }
            })
            .collect();
        let loopback = format!("{:08X}:", u32::from_ne_bytes([127, 0, 0, 1]));
        assert!(
            !local.is_empty() && local.iter().all(|s| s.starts_with(&loopback)),
            "Private decoder UDP sockets must bind loopback: {local:?}"
        );
    }
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
    stage("receiver_spawn");
    let mut receiver = Command::new(&ffmpeg)
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
    if let Some(evidence) = &evidence {
        evidence.watch_process("receiver", receiver.id().unwrap());
    }
    stage("receiver_socket_startup");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let ready = decoder_readiness::ready(
                receiver.id().unwrap(),
                &(private..private + track * 2).collect::<Vec<_>>(),
            )
            .unwrap();
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
        let mut dump = artifact.as_ref().map(|out| {
            elementary_diagnostics::DatagramCapture::new(out.join(format!("output-{i}.rtp")))
        });
        relay.spawn(async move { let mut bytes=[0;2048]; loop {
            let n = tokio::select! {biased;_=c.cancelled()=>break,r=socket.recv_from(&mut bytes)=>r.unwrap().0};
            if let Some(dump) = &mut dump { dump.append(&bytes[..n]).unwrap(); }
            tokio::select! {biased;_=c.cancelled()=>break,r=socket.send_to(&bytes[..n], ("127.0.0.1", private + i as u16))=>{let _=r;}}
        } dump});
    }
    stage("media_delivery");
    let result = tokio::time::timeout(Duration::from_secs(15), receiver.wait()).await;
    stage("receiver_wait_complete");
    cancel.cancel();
    let mut captures = Vec::new();
    while let Some(result) = relay.join_next().await {
        if let Some(capture) = result.unwrap() {
            captures.push(capture);
        }
    }
    record_cancel.cancel();
    recording.await.unwrap();
    let _ = sender.kill().await;
    let _ = sender.wait().await;
    if result.is_err() {
        let _ = receiver.kill().await;
        let _ = receiver.wait().await;
    }
    let stats = worker.stats();
    stage("worker_shutdown");
    engine.stop_all().await;
    stage("worker_stopped");
    if let Some(evidence) = &evidence {
        evidence.save_captures(captures);
    }
    if let Some(out) = &artifact {
        std::fs::write(
            out.join("worker-stats.json"),
            serde_json::to_vec_pretty(&stats).unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(
            out.join("worker-stats.json"),
            std::fs::Permissions::from_mode(0o600),
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
    stage("probe_worker_recording");
    decode(&d.path().join("worker.ts"), output_video, &expected).await;
    stage("probe_receiver_recording");
    decode(&received, output_video, &expected).await;
    stage("qualified");
    assert!(!Path::new(&format!("/proc/{}", worker.pid())).exists());
}
