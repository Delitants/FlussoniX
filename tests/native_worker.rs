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
