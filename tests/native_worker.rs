use bytes::Bytes;
use flussonix::{
    m4f::{self, Frame},
    m4s::Track,
    media::Engine,
    wire,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
fn fixture() -> (Vec<Track>, Vec<Frame>) {
    let tracks = vec![
        Track {
            id: 1,
            codec: "hevc".into(),
            config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
        Track {
            id: 3,
            codec: "mp3".into(),
            config: vec![],
        },
    ];
    let timing: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    let mut samples = vec![];
    for cycle in 0..20u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            samples.push(Frame {
                track_id: 1,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
            });
        }
    }
    for i in 0..370 {
        for (id, step, body) in [
            (
                2,
                2160,
                include_bytes!("fixtures/codecs/mp2.bin").as_slice(),
            ),
            (
                3,
                2351,
                include_bytes!("fixtures/codecs/mp3.bin").as_slice(),
            ),
        ] {
            samples.push(Frame {
                track_id: id,
                dts: 90000 + i * step,
                pts_offset: 0,
                key: true,
                body: body.to_vec(),
            });
        }
    }
    samples.sort_by_key(|f| f.dts);
    (tracks, samples)
}
async fn run(protocol: &str, short: bool) {
    let (tracks, mut frames) = fixture();
    if short {
        frames.retain(|f| f.dts < 360000);
    }
    let mut native = wire::encode_info(&tracks).unwrap();
    for f in &frames {
        native.extend(
            wire::encode_frame(tracks.iter().find(|t| t.id == f.track_id).unwrap(), f).unwrap(),
        );
    }
    let mut segments = HashMap::new();
    let mut signals = String::new();
    for n in 0..5u64 {
        let start = 90000 + n * 180000;
        let samples: Vec<_> = frames
            .iter()
            .filter(|f| f.dts >= start && f.dts < start + 180000)
            .cloned()
            .collect();
        if samples.is_empty() {
            continue;
        }
        let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        signals += &format!("{n} {stamp}-2000\n");
        segments.insert(
            format!("owned/{stamp}.m4f"),
            Bytes::from(m4f::pack(&tracks, &samples, 180000).unwrap()),
        );
    }
    let originals = segments.clone();
    let control = if protocol == "m4s" {
        Bytes::from(native)
    } else {
        Bytes::from(signals)
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("{protocol}://{}/owned", listener.local_addr().unwrap());
    let path = format!("/owned/{protocol}");
    let router = axum::Router::new()
        .route(
            &path,
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let control = control.clone();
                async move {
                    if headers
                        .get("X-Flussonix-Peer")
                        .and_then(|v| v.to_str().ok())
                        != Some("owned-peer")
                    {
                        return axum::response::Response::builder()
                            .status(403)
                            .body(axum::body::Body::empty())
                            .unwrap();
                    }
                    let body =
                        futures_util::stream::once(async move { Ok::<_, std::io::Error>(control) })
                            .chain(futures_util::stream::pending());
                    axum::response::Response::new(axum::body::Body::from_stream(body))
                }
            }),
        )
        .route(
            "/{*path}",
            axum::routing::get(
                move |axum::extract::Path(path): axum::extract::Path<String>,
                      headers: axum::http::HeaderMap| {
                    let body = segments.get(&path).cloned();
                    async move {
                        if headers
                            .get("X-Flussonix-Peer")
                            .and_then(|v| v.to_str().ok())
                            != Some("owned-peer")
                        {
                            return (axum::http::StatusCode::FORBIDDEN, Bytes::new());
                        }
                        match body {
                            Some(b) => (axum::http::StatusCode::OK, b),
                            None => (axum::http::StatusCode::NOT_FOUND, Bytes::new()),
                        }
                    }
                },
            ),
        );
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":url}],"flussonix_peer_key":"owned-peer"});
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    let second = engine.ensure("owned", &cfg).await.unwrap();
    assert!(Arc::ptr_eq(&worker, &second));
    let manifest = tokio::time::timeout(Duration::from_secs(if short { 5 } else { 15 }), async {
        loop {
            if let Ok(m) = engine.read("owned", "index.m3u8").await {
                break m;
            }
            assert!(
                !worker.is_closed(),
                "native {protocol} worker closed before HLS"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("native input must yield HLS");
    let text = String::from_utf8(manifest.to_vec()).unwrap();
    let name = text
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .unwrap();
    let bytes = engine.read("owned", name).await.unwrap();
    let file = dir.path().join("decode.ts");
    tokio::fs::write(&file, bytes).await.unwrap();
    let p = tokio::process::Command::new("ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(&file)
        .output()
        .await
        .unwrap();
    assert!(p.status.success());
    let info: Value = serde_json::from_slice(&p.stdout).unwrap();
    let codecs: Vec<_> = info["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["codec_name"].as_str().unwrap())
        .collect();
    assert_eq!(codecs, vec!["hevc", "mp2", "mp3"]);
    let p = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(file)
        .args(["-map", "0", "-t", "1", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    assert!(
        p.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&p.stderr)
    );
    assert!(worker.wire.has_info());
    if protocol == "m4f" {
        let (signals, _) = worker.wire.signal_subscribe();
        assert!(!signals.is_empty());
        for line in signals {
            let line = std::str::from_utf8(&line).unwrap();
            let stamp = line
                .split_whitespace()
                .nth(1)
                .unwrap()
                .split('-')
                .next()
                .unwrap();
            let name = format!("{stamp}.m4f");
            assert_eq!(
                worker.wire.segment(&name).unwrap(),
                originals[&format!("owned/{name}")]
            );
        }
    } else {
        let (boot, _) = worker.wire.m4s_subscribe();
        assert_eq!(
            boot.first().unwrap().as_ref(),
            wire::encode_info(&tracks).unwrap()
        );
        let mut decoder = flussonix::m4s::Decoder::default();
        let mut checked = 0;
        for packet in boot {
            for event in decoder.push(&packet).unwrap() {
                if let flussonix::m4s::Event::Frame {
                    track_id,
                    dts,
                    pts_offset,
                    body,
                    wire: record,
                    ..
                } = event
                {
                    let expected = frames
                        .iter()
                        .find(|f| f.track_id == track_id && f.dts == dts)
                        .unwrap();
                    assert_eq!(pts_offset, expected.pts_offset);
                    assert_eq!(body, expected.body);
                    let track = tracks.iter().find(|t| t.id == track_id).unwrap();
                    assert_eq!(
                        record.as_ref(),
                        wire::encode_frame(track, expected).unwrap()
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 0);
    }
    let pid = worker.pid();
    engine.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(engine.count().await, 0);
    task.abort();
}
#[tokio::test]
async fn native_m4s_hevc_and_two_mpeg_tracks_reach_decoded_hls() {
    run("m4s", false).await;
}
#[tokio::test]
async fn native_m4f_hevc_and_two_mpeg_tracks_reach_decoded_hls() {
    run("m4f", false).await;
}
#[tokio::test]
async fn metadata_replacement_fails_before_relaying_changed_layout() {
    let initial = vec![Track {
        id: 1,
        codec: "m2a".into(),
        config: vec![],
    }];
    let changed = vec![Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }];
    let info = wire::encode_info(&initial).unwrap();
    let body = [info.clone(), wire::encode_info(&changed).unwrap()].concat();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("m4s://{}/owned", listener.local_addr().unwrap());
    let router = axum::Router::new().route(
        "/owned/m4s",
        axum::routing::get(move || {
            let body = body.clone();
            async move { body }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut child = tokio::process::Command::new("cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let hub = wire::Hub::new();
    let result = flussonix::m4_ingest::pull(&url, None, &mut stdin, Some(&hub)).await;
    assert!(result.unwrap_err().contains("metadata changed"));
    assert_eq!(hub.m4s_subscribe().0[0].as_ref(), info);
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    task.abort();
}

#[tokio::test]
async fn invalid_first_worker_metadata_never_reaches_native_hub() {
    let invalid = vec![Track {
        id: 1,
        codec: "hevc".into(),
        config: vec![],
    }];
    let body = wire::encode_info(&invalid).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("m4s://{}/owned", listener.local_addr().unwrap());
    let router = axum::Router::new().route(
        "/owned/m4s",
        axum::routing::get(move || {
            let body = body.clone();
            async move { body }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut child = tokio::process::Command::new("cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let hub = wire::Hub::new();
    let result = flussonix::m4_ingest::pull(&url, None, &mut stdin, Some(&hub)).await;
    assert!(result.unwrap_err().contains("HEVC decoder configuration"));
    assert!(!hub.has_info());
    assert!(hub.m4s_subscribe().0.is_empty());
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    task.abort();
}

#[tokio::test]
async fn short_native_stream_starts_without_default_five_second_ts_probe() {
    run("m4s", true).await;
}

async fn fmp4_profile(kind: &str, protocol: &str) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let (mut tracks, mut frames) = fixture();
    if !matches!(kind, "mpeg" | "video") {
        tracks.retain(|t| t.id == 1);
        frames.retain(|f| f.track_id == 1);
        let encoded = tokio::process::Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=880:sample_rate=48000",
                "-t",
                "10",
                "-c:a",
                "aac",
                "-f",
                "adts",
                "pipe:1",
            ])
            .output()
            .await
            .unwrap();
        assert!(encoded.status.success());
        for id in [2, 3] {
            tracks.push(Track {
                id,
                codec: "aac".into(),
                config: vec![0x11, 0x88, 0x56, 0xe5, 0],
            });
            let mut at = 0;
            let mut n = 0;
            while at < encoded.stdout.len() {
                let h = &encoded.stdout[at..];
                let size = (usize::from(h[3] & 3) << 11)
                    | (usize::from(h[4]) << 3)
                    | usize::from(h[5] >> 5);
                frames.push(Frame {
                    track_id: id,
                    dts: 90000 + n * 1920,
                    pts_offset: 0,
                    key: true,
                    body: h[7..size].to_vec(),
                });
                at += size;
                n += 1;
            }
        }
        if matches!(kind, "mixed" | "mixed_audio") {
            tracks[2].codec = "mp3".into();
            tracks[2].config.clear();
            frames.retain(|f| f.track_id != 3);
            for i in 0..420 {
                frames.push(Frame {
                    track_id: 3,
                    dts: 90000 + i * 2351,
                    pts_offset: 0,
                    key: true,
                    body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
                });
            }
        }
    }
    if kind == "video" {
        tracks.retain(|t| t.id == 1);
        frames.retain(|f| f.track_id == 1);
    }
    if kind == "mixed_audio" {
        tracks.retain(|t| t.id != 1);
        frames.retain(|f| f.track_id != 1);
    }
    if matches!(kind, "mixed" | "mixed_audio") {
        // Metadata order differs from the explicit video-first output mapping.
        tracks.reverse();
        for t in &mut tracks {
            t.id = match t.id {
                1 => 205,
                2 => 88,
                _ => 37,
            };
        }
        for f in &mut frames {
            f.track_id = match f.track_id {
                1 => 205,
                2 => 88,
                _ => 37,
            };
        }
    }
    let expected_codecs: Vec<_> = tracks
        .iter()
        .filter(|t| t.kind().unwrap().is_video())
        .chain(tracks.iter().filter(|t| !t.kind().unwrap().is_video()))
        .map(|t| {
            match t.codec.as_str() {
                "m2a" => "mp3",
                other => other,
            }
            .to_owned()
        })
        .collect();
    frames.sort_by_key(|f| f.dts);
    let mut bytes = wire::encode_info(&tracks).unwrap();
    for f in &frames {
        let t = tracks.iter().find(|t| t.id == f.track_id).unwrap();
        bytes.extend(wire::encode_frame(t, f).unwrap());
    }
    let mut segments = HashMap::new();
    let mut signals = String::new();
    for n in 0..5u64 {
        let samples: Vec<_> = frames
            .iter()
            .filter(|f| f.dts >= 90000 + n * 180000 && f.dts < 90000 + (n + 1) * 180000)
            .cloned()
            .collect();
        if samples.is_empty() {
            continue;
        }
        let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        signals += &format!("{n} {stamp}-2000\n");
        segments.insert(
            format!("owned/{stamp}.m4f"),
            Bytes::from(m4f::pack(&tracks, &samples, 180000).unwrap()),
        );
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("{protocol}://{}/owned", listener.local_addr().unwrap());
    let bytes = if protocol == "m4s" {
        Bytes::from(bytes)
    } else {
        Bytes::from(signals)
    };
    let controls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = controls.clone();
    let router = axum::Router::new()
        .route(
            &format!("/owned/{protocol}"),
            axum::routing::get(move || {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let b = bytes.clone();
                async move {
                    let stream =
                        futures_util::stream::once(async move { Ok::<_, std::io::Error>(b) })
                            .chain(futures_util::stream::pending());
                    axum::body::Body::from_stream(stream)
                }
            }),
        )
        .route(
            "/{*path}",
            axum::routing::get(
                move |axum::extract::Path(path): axum::extract::Path<String>| {
                    let b = segments.get(&path).unwrap().clone();
                    async move { b }
                },
            ),
        );
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let worker = engine
        .ensure("owned", &json!({"inputs":[{"url":url}]}))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        // TS and fMP4 tee outputs publish their playlists independently.
        while !engine.ready("owned").await || engine.read("owned", "fmp4/index.m3u8").await.is_err()
        {
            assert!(!worker.is_closed());
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    {
        let playlist = engine
            .read("owned", "fmp4/index.m3u8")
            .await
            .unwrap_or_else(|e| panic!("{protocol}/{kind}: {e}; {}", worker.stats()));
        let text = String::from_utf8(playlist.to_vec()).unwrap();
        assert!(!text.contains("#EXT-X-TARGETDURATION:0"));
        let map = text
            .lines()
            .find(|l| l.starts_with("#EXT-X-MAP:"))
            .unwrap()
            .split('"')
            .nth(1)
            .unwrap();
        let fragment = text
            .lines()
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .unwrap();
        let mut mp4 = engine
            .read("owned", &format!("fmp4/{map}"))
            .await
            .unwrap()
            .to_vec();
        mp4.extend(
            engine
                .read("owned", &format!("fmp4/{fragment}"))
                .await
                .unwrap(),
        );
        let file = dir.path().join("decode.mp4");
        tokio::fs::write(&file, mp4).await.unwrap();
        let r = tokio::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_streams",
                "-show_packets",
                "-of",
                "json",
            ])
            .arg(&file)
            .output()
            .await
            .unwrap();
        assert!(r.status.success());
        let info: Value = serde_json::from_slice(&r.stdout).unwrap();
        assert_eq!(
            info["streams"].as_array().unwrap().len(),
            expected_codecs.len()
        );
        for (index, codec) in expected_codecs.iter().enumerate() {
            assert_eq!(info["streams"][index]["codec_name"], *codec);
            assert!(
                info["packets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p["stream_index"] == index)
            );
        }
        if kind == "mpeg" {
            // MP4 uses the MPEG audio registration for both layers; inspect
            // encoded payloads instead of relying on ffprobe's mp3 label.
            for (index, body, codec) in [
                (
                    1,
                    include_bytes!("fixtures/codecs/mp2.bin").as_slice(),
                    flussonix::codec::Codec::M2a,
                ),
                (
                    2,
                    include_bytes!("fixtures/codecs/mp3.bin").as_slice(),
                    flussonix::codec::Codec::Mp3,
                ),
            ] {
                let extracted = tokio::process::Command::new("ffmpeg")
                    .args(["-v", "error", "-i"])
                    .arg(&file)
                    .args([
                        "-map",
                        &format!("0:{index}"),
                        "-c",
                        "copy",
                        "-f",
                        "data",
                        "pipe:1",
                    ])
                    .output()
                    .await
                    .unwrap();
                assert!(extracted.status.success());
                assert!(!extracted.stdout.is_empty());
                assert_eq!(extracted.stdout.len() % body.len(), 0);
                for packet in extracted.stdout.chunks(body.len()) {
                    assert_eq!(packet, body);
                    flussonix::mpeg_audio::inspect(codec, packet).unwrap();
                }
            }
        }
        let decoded = tokio::process::Command::new("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(file)
            .args(["-map", "0", "-f", "null", "-"])
            .output()
            .await
            .unwrap();
        assert!(
            decoded.status.success(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        assert!(
            decoded.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
    }
    assert_eq!(controls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let media_dirs: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .collect();
    assert_eq!(media_dirs.len(), 1);
    assert!(
        !media_dirs[0].path().join("fmp4_aac").exists(),
        "no duplicate fMP4 sink"
    );
    assert!(
        !worker.is_closed(),
        "stable profile must retain its worker: {}",
        worker.stats()
    );
    let pid = worker.pid();
    assert_ne!(pid, 0);
    engine.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    task.abort();
}
#[tokio::test]
async fn native_aac_fmp4_contains_decodable_media_for_both_audio_tracks() {
    fmp4_profile("aac", "m4s").await;
}
#[tokio::test]
async fn native_mpeg_fmp4_keeps_unfiltered_audio_decodable() {
    fmp4_profile("mpeg", "m4s").await;
}
#[tokio::test]
async fn native_mixed_fmp4_decodes_aac_and_mpeg_without_duplicate_sink() {
    fmp4_profile("mixed", "m4s").await;
}

#[tokio::test]
async fn native_m4f_mixed_fmp4_decodes_all_tracks_without_reconnecting() {
    fmp4_profile("mixed", "m4f").await;
}
#[tokio::test]
async fn native_m4f_mpeg_fmp4_preserves_both_audio_layers() {
    fmp4_profile("mpeg", "m4f").await;
}
#[tokio::test]
async fn native_audio_only_mixed_fmp4_uses_audio_output_indices() {
    fmp4_profile("mixed_audio", "m4s").await;
}
#[tokio::test]
async fn native_video_only_fmp4_does_not_duplicate_packaging() {
    fmp4_profile("video", "m4s").await;
}
