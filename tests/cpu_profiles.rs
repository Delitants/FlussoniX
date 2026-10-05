use flussonix::{m4f, m4s, media::Engine, worker_ts::Muxer};
use serde_json::{Value, json};
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

async fn qualify(
    engine: &Engine,
    dir: &std::path::Path,
    cfg: Value,
    video: &str,
    audio: &str,
) -> Vec<m4f::Frame> {
    let worker = engine.ensure("owned", &cfg).await.unwrap();
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

#[tokio::test]
async fn cpu_video_and_audio_profiles_deliver_decodable_native_and_hls_media() {
    for video in ["libx264", "libx265"] {
        for (audio, bitrate, decoded) in [
            ("aac", 96, "aac"),
            ("mp2a", 192, "mp2"),
            ("mp3", 128, "mp3"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let engine = Engine::new(dir.path(), "ffmpeg");
            qualify(&engine,dir.path(),json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":video,"vb":300,"acodec":audio,"ab":bitrate}}),if video=="libx265"{"hevc"}else{"h264"},decoded).await;
        }
    }
}

async fn native_source(
    protocol: &str,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    tempfile::TempDir,
    Vec<Vec<u8>>,
) {
    use axum::{Router, body::Body, routing::get};
    use bytes::Bytes;
    use flussonix::worker_output::{Decoder, Event};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.ts");
    let out = tokio::process::Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=sample_rate=48000",
            "-t",
            "12",
            "-c:v",
            "libx264",
            "-threads",
            "2",
            "-preset",
            "ultrafast",
            "-g",
            "50",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-f",
            "mpegts",
        ])
        .arg(&path)
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut decoder = Decoder::default();
    let mut events = vec![];
    for chunk in std::fs::read(&path).unwrap().chunks(188 * 64) {
        events.extend(decoder.push(chunk).unwrap());
    }
    events.extend(decoder.finish().unwrap());
    let mut tracks = vec![];
    let mut frames = vec![];
    for event in events {
        match event {
            Event::Info(t) => tracks = t,
            Event::Frame(f) => frames.push(f),
        }
    }
    frames.sort_by_key(|f| f.dts);
    let mut data = flussonix::wire::encode_info(&tracks).unwrap();
    let mut segments = std::collections::HashMap::new();
    let mut signals = String::new();
    let start = frames.first().unwrap().dts;
    for n in 0..6u64 {
        let part: Vec<_> = frames
            .iter()
            .filter(|f| f.dts >= start + n * 180000 && f.dts < start + (n + 1) * 180000)
            .cloned()
            .collect();
        let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        signals += &format!("{n} {stamp}-2000\n");
        segments.insert(
            format!("owned/{stamp}.m4f"),
            Bytes::from(m4f::pack(&tracks, &part, 180000).unwrap()),
        );
    }
    for frame in &frames {
        data.extend(
            flussonix::wire::encode_frame(
                tracks.iter().find(|t| t.id == frame.track_id).unwrap(),
                frame,
            )
            .unwrap(),
        );
    }
    let data = Bytes::from(if protocol == "m4f" {
        signals.into_bytes()
    } else {
        data
    });
    let routes = Router::new()
        .route(
            &format!("/owned/{protocol}"),
            get(move || {
                let data = data.clone();
                async move {
                    Body::from_stream(
                        futures_util::stream::iter(
                            data.chunks(8192)
                                .map(|b| Ok::<_, std::io::Error>(Bytes::copy_from_slice(b)))
                                .collect::<Vec<_>>(),
                        )
                        .chain(futures_util::stream::pending()),
                    )
                }
            }),
        )
        .route(
            "/{*path}",
            get(
                move |axum::extract::Path(path): axum::extract::Path<String>| {
                    let data = segments.get(&path).cloned();
                    async move { data.unwrap() }
                },
            ),
        );
    use futures_util::StreamExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("{protocol}://{}/owned", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        axum::serve(listener, routes).await.unwrap();
    });
    let pictures = frames
        .iter()
        .filter(|f| f.track_id == tracks.iter().find(|t| t.codec == "h264").unwrap().id)
        .map(|f| h264_vcl(&f.body))
        .collect();
    (url, handle, dir, pictures)
}

#[tokio::test]
async fn independent_native_audio_and_video_encoding_delivers_changed_codec_on_all_outputs() {
    for protocol in ["m4s", "m4f"] {
        for (profile, video, audio) in [
            (json!({"acodec":"mp3","ab":128}), "h264", "mp3"),
            (
                json!({"encoder":"libx265","vb":300,"acodec":"copy"}),
                "hevc",
                "aac",
            ),
        ] {
            eprintln!("qualifying {protocol} {profile} => {video}/{audio}");
            let (url, handle, _source, pictures) = native_source(protocol).await;
            let dir = tempfile::tempdir().unwrap();
            let engine = Engine::new(dir.path(), "ffmpeg");
            let frames = qualify(
                &engine,
                dir.path(),
                json!({"inputs":[{"url":url}],"transcoder":profile}),
                video,
                audio,
            )
            .await;
            if video == "h264" {
                for frame in frames.iter().filter(|f| !h264_vcl(&f.body).is_empty()) {
                    assert!(
                        pictures.contains(&h264_vcl(&frame.body)),
                        "audio-only encoding rewrote copied video pictures"
                    );
                }
            }
            handle.abort();
            let _ = handle.await;
        }
    }
}

fn h264_vcl(body: &[u8]) -> Vec<u8> {
    let mut result = vec![];
    let mut at = 0;
    while at + 4 <= body.len() {
        let n = u32::from_be_bytes(body[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        if n == 0 || at + n > body.len() {
            return vec![];
        }
        if matches!(body[at] & 31, 1 | 5) {
            result.extend_from_slice(&body[at..at + n]);
        }
        at += n;
    }
    result
}
