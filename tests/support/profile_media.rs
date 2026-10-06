use flussonix::{m4f, m4s, media::Engine, worker_ts::Muxer};
use serde_json::Value;
use std::time::Duration;

async fn decode_file(file: &std::path::Path, video: &str, audio: &str) {
    let p = tokio::process::Command::new("ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(file)
        .output()
        .await
        .unwrap();
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    let p: Value = serde_json::from_slice(&p.stdout).unwrap();
    let streams = p["streams"].as_array().unwrap();
    assert!(streams.iter().any(|s| s["codec_name"] == video), "{p}");
    let generic_mp4 = audio == "mp2" && file.extension().is_some_and(|e| e == "mp4");
    assert!(
        streams
            .iter()
            .any(|s| s["codec_name"] == if generic_mp4 { "mp3" } else { audio }),
        "{p}"
    );
    if audio == "mp2" || audio == "mp3" {
        let packet = tokio::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-read_intervals",
                "%+#1",
                "-show_packets",
                "-show_data",
                "-of",
                "json",
            ])
            .arg(file)
            .output()
            .await
            .unwrap();
        assert!(packet.status.success());
        let packet: Value = serde_json::from_slice(&packet.stdout).unwrap();
        let mut bytes = vec![];
        for line in packet["packets"][0]["data"]
            .as_str()
            .unwrap()
            .lines()
            .filter(|l| l.contains(':'))
        {
            let hex = line
                .split_once(':')
                .unwrap()
                .1
                .trim_start()
                .split("  ")
                .next()
                .unwrap()
                .replace(' ', "");
            for pair in hex.as_bytes().chunks_exact(2) {
                bytes.push(u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap());
            }
        }
        let header = flussonix::mpeg_audio::inspect(
            if audio == "mp2" {
                flussonix::codec::Codec::M2a
            } else {
                flussonix::codec::Codec::Mp3
            },
            &bytes,
        )
        .expect("requested MPEG layer is retained in container sample");
        assert_eq!(header.sample_rate, 48000);
    }

    let p = tokio::process::Command::new("ffmpeg")
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(file)
        .args(["-map", "0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        p.status.success() && p.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&p.stderr)
    );
}

pub async fn qualify(
    engine: &Engine,
    dir: &std::path::Path,
    cfg: Value,
    video: &str,
    audio: &str,
) -> Vec<m4f::Frame> {
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    let other = engine.ensure("owned", &cfg).await.unwrap();
    assert!(
        std::sync::Arc::ptr_eq(&worker, &other),
        "all consumers share one encoder worker"
    );
    let mut rx = worker.wire.m4s.subscribe();
    let mut decoder = m4s::Decoder::default();
    let mut tracks = vec![];
    let mut frames = vec![];
    let mut checked = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(record) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                let record = record.expect("worker native output");
                for event in decoder.push(&record).unwrap() {
                    match event {
                        m4s::Event::Info { tracks: t, .. } => tracks = t,
                        m4s::Event::Frame {
                            track_id,
                            dts,
                            pts_offset,
                            key,
                            body,
                            ..
                        } => frames.push(m4f::Frame {
                            track_id,
                            dts,
                            pts_offset,
                            key,
                            body: body.to_vec(),
                        }),
                        _ => {}
                    }
                }
            }
            let ready = frames.len() > 150 && !worker.wire.signal_subscribe().0.is_empty();
            if !ready || checked.elapsed() < Duration::from_millis(100) {
                continue;
            }
            checked = std::time::Instant::now();
            let mut hls = true;
            for prefix in ["", "fmp4/"] {
                hls &= engine
                    .read("owned", &format!("{prefix}index.m3u8"))
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF").count() >= 2);
            }
            if ready && hls {
                break;
            }
            assert!(
                worker.alive.load(std::sync::atomic::Ordering::Relaxed),
                "{}",
                worker.stats()
            );
        }
    })
    .await
    .expect("all outputs ready");
    assert!(
        tracks
            .iter()
            .any(|t| t.codec == if video == "h264" { "h264" } else { "hevc" }),
        "{tracks:?}"
    );
    assert!(
        tracks
            .iter()
            .any(|t| t.codec == if audio == "mp2" { "m2a" } else { audio }),
        "{tracks:?}"
    );
    let mut mux = Muxer::new(&tracks).unwrap();
    let mut ts = mux.tables();
    for frame in &frames {
        ts.extend(mux.frame(frame).unwrap());
    }
    let file = dir.join("native.ts");
    std::fs::write(&file, ts).unwrap();
    decode_file(&file, video, audio).await;
    let signals = worker.wire.signal_subscribe().0;
    let stamp = std::str::from_utf8(&signals[0])
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .split('-')
        .next()
        .unwrap();
    let segment = worker.wire.segment(&format!("{stamp}.m4f")).unwrap();
    let (t, f) = m4f::unpack(&segment).unwrap();
    assert_eq!(t, tracks);
    let mut mux = Muxer::new(&t).unwrap();
    let mut ts = mux.tables();
    for frame in &f {
        ts.extend(mux.frame(frame).unwrap());
    }
    let file = dir.join("packed.ts");
    std::fs::write(&file, ts).unwrap();
    decode_file(&file, video, audio).await;
    for prefix in ["", "fmp4/"] {
        let list = String::from_utf8(
            engine
                .read("owned", &format!("{prefix}index.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let mut data = vec![];
        if !prefix.is_empty() {
            let init = list
                .split("URI=\"")
                .nth(1)
                .unwrap()
                .split('"')
                .next()
                .unwrap();
            data.extend(
                engine
                    .read("owned", &format!("{prefix}{init}"))
                    .await
                    .unwrap(),
            );
        }
        for name in list
            .lines()
            .filter(|s| !s.starts_with('#') && !s.is_empty())
        {
            data.extend(
                engine
                    .read("owned", &format!("{prefix}{name}"))
                    .await
                    .unwrap(),
            );
        }
        let file = dir.join(if prefix.is_empty() {
            "hls.ts"
        } else {
            "hls.mp4"
        });
        std::fs::write(&file, data).unwrap();
        decode_file(&file, video, audio).await;
    }
    let pid = worker.pid();
    engine.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    frames
}
