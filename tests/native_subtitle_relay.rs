use axum::{Router, body::Body, http::HeaderMap, routing::get};
use bytes::Bytes;
use flussonix::{
    m4f::{self, Frame},
    m4s::{self, PackedGop, Track},
    server::{App, Options, router as app_router},
    wire,
};
use futures_util::StreamExt;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn track_metadata(data: &[u8], record: bool) -> Bytes {
    let raw = if record { &data[4..] } else { data };
    let mut root = Vec::new();
    for (kind, body) in m4s::boxes(raw).unwrap() {
        if kind == b"MDin" || kind == b"moov" {
            let mut content = Vec::new();
            for (name, value) in m4s::boxes(body).unwrap() {
                let value = if name == b"trak" {
                    [value.to_vec(), m4s::atom(b"fxmd", b"owned-AV-metadata")].concat()
                } else {
                    value.to_vec()
                };
                content.extend(m4s::atom(name.try_into().unwrap(), &value));
            }
            root.extend(m4s::atom(kind.try_into().unwrap(), &content));
        } else {
            root.extend(m4s::atom(kind.try_into().unwrap(), body));
        }
    }
    if record {
        Bytes::from([(root.len() as u32).to_be_bytes().to_vec(), root].concat())
    } else {
        Bytes::from(root)
    }
}
fn fixture() -> (Vec<Track>, Vec<Frame>) {
    let tracks = vec![
        Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        },
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
        Track {
            id: 8,
            codec: "subtitle".into(),
            config: vec![],
        },
    ];
    let mut frames: Vec<_> = (0..420)
        .map(|i| Frame {
            track_id: 2,
            dts: 90000 + i * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        })
        .collect();
    for n in 0..5 {
        for (id, body) in [(7, "AMERICA HELLO"), (8, "EUROPE GRÜSSE")] {
            frames.push(Frame {
                track_id: id,
                dts: 117000 + n * 180000,
                pts_offset: 63000,
                key: true,
                body: body.as_bytes().to_vec(),
            });
            frames.push(Frame {
                track_id: id,
                dts: 210000 + n * 180000,
                pts_offset: 0,
                key: true,
                body: vec![],
            });
        }
    }
    frames.sort_by_key(|f| f.dts);
    (tracks, frames)
}
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn run(
    protocol: &str,
    gops: bool,
    preserve: bool,
    transcode: bool,
    auth: bool,
    sparse: bool,
) {
    let (tracks, mut frames) = fixture();
    if sparse {
        frames
            .retain(|f| f.track_id == 2 || ((f.dts - 90000) / 180000) % 2 != 0 || f.dts >= 810000);
    }
    let mut control = track_metadata(&wire::encode_info(&tracks).unwrap(), true).to_vec();
    let mut signals = String::new();
    let mut segments = HashMap::new();
    for n in 0..5 {
        let samples: Vec<_> = frames
            .iter()
            .filter(|f| f.dts >= 90000 + n * 180000 && f.dts < 270000 + n * 180000)
            .cloned()
            .collect();
        let body = track_metadata(&m4f::pack(&tracks, &samples, 180000).unwrap(), false);
        let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        signals += &format!("{n} {stamp}-2000\n");
        segments.insert(format!("owned/{stamp}.m4f"), body.clone());
        if gops {
            if sparse {
                control.extend(track_metadata(&wire::encode_info(&tracks).unwrap(), true));
            }
            control.extend(
                m4s::encode_gop(&PackedGop {
                    utc: 1700000000 + n as u32 * 2,
                    dts_ms: 1000.0 + n as f64 * 2000.0,
                    sequence: n as u32,
                    duration_ms: 2000.0,
                    body,
                })
                .unwrap(),
            );
        }
    }
    if !gops {
        for f in &frames {
            control.extend(
                wire::encode_frame(tracks.iter().find(|t| t.id == f.track_id).unwrap(), f).unwrap(),
            );
        }
    }
    let source_signals: Vec<_> = signals
        .lines()
        .map(|line| Bytes::from(format!("{line}\n")))
        .collect();
    let originals = segments.clone();
    let control = if protocol == "m4f" {
        Bytes::from(signals)
    } else {
        Bytes::from(control)
    };
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "{protocol}://{}/owned{}",
        listener.local_addr().unwrap(),
        if auth { "?token=owned-source" } else { "" }
    );
    let router = Router::new()
        .route(
            &format!("/owned/{protocol}"),
            get(
                move |headers: HeaderMap,
                      axum::extract::OriginalUri(uri): axum::extract::OriginalUri| {
                    let control = control.clone();
                    let count = count.clone();
                    async move {
                        if auth {
                            assert_eq!(uri.query(), Some("token=owned-source"));
                            assert!(headers.get("x-flussonix-peer").is_none());
                        } else {
                            assert_eq!(headers["x-flussonix-peer"], "owned-subtitle-peer");
                        }
                        count.fetch_add(1, Ordering::SeqCst);
                        Body::from_stream(
                            futures_util::stream::once(std::future::ready(
                                Ok::<_, std::io::Error>(control),
                            ))
                            .chain(futures_util::stream::pending()),
                        )
                    }
                },
            ),
        )
        .route(
            "/{*path}",
            get(
                move |axum::extract::Path(path): axum::extract::Path<String>,
                      headers: HeaderMap,
                      axum::extract::OriginalUri(uri): axum::extract::OriginalUri| {
                    let bytes = segments.get(&path).cloned();
                    async move {
                        if auth {
                            assert_eq!(uri.query(), Some("token=owned-source"));
                            assert!(headers.get("x-flussonix-peer").is_none());
                        } else {
                            assert_eq!(headers["x-flussonix-peer"], "owned-subtitle-peer");
                        }
                        bytes.unwrap()
                    }
                },
            ),
        );
    let _server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap()
    }));
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-subtitle-admin".into(),
            peer_key: "owned-delivery-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let engine = &app.media;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = format!("http://{}", listener.local_addr().unwrap());
    let cloned = app.clone();
    let _delivery = Server(tokio::spawn(async move {
        axum::serve(listener, app_router(cloned)).await.unwrap()
    }));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut config = json!({"inputs":[{"url":endpoint}],"flussonix_peer_key":"owned-subtitle-peer","flussonix_subtitle_tracks":if preserve{"preserve"}else{"drop"}});
    if transcode {
        config["transcoder"] = json!({"encoder":"libx264","vb":300});
    }
    if auth {
        use sha2::Digest;
        config.as_object_mut().unwrap().remove("flussonix_peer_key");
        config["static"] = json!(false);
        config["flussonix_token_sha256"] =
            json!(format!("{:x}", sha2::Sha256::digest(b"owned-viewer")));
        app.config.put("streams", "owned", config.clone()).unwrap();
        for suffix in ["m4s", "m4f"] {
            assert_eq!(
                client
                    .get(format!("{public}/owned/{suffix}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                403
            );
        }
        assert_eq!(engine.count().await, 0);
        assert_eq!(requests.load(Ordering::SeqCst), 0);
    }
    let worker = engine.ensure("owned", &config).await.unwrap();
    if transcode && preserve {
        tokio::time::timeout(Duration::from_secs(3), worker.closed())
            .await
            .expect("native text preservation must not silently disappear during transcoding");
        assert_eq!(
            worker.stats()["last_error"],
            "native_subtitle_transcode_unsupported"
        );
        assert_eq!(worker.stats()["native_subtitle_output"], "Not supported");
        if !sparse {
            assert_eq!(worker.pid(), 0);
        }
        engine.stop_all().await;
        return;
    }
    tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            if engine.read("owned", "index.m3u8").await.is_ok()
                && if sparse {
                    originals.keys().all(|name| {
                        worker
                            .wire
                            .segment(name.strip_prefix("owned/").unwrap())
                            .is_some()
                    })
                } else {
                    !worker.wire.signal_subscribe().0.is_empty()
                }
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|e| {
        panic!(
            "subtitle source lost AV/native readiness: {e}; {}",
            worker.stats()
        )
    });
    let status = worker.stats();
    assert_eq!(
        status["native_subtitle_tracks"], 2,
        "detected source text tracks"
    );
    assert_eq!(
        status["native_subtitle_output"],
        if preserve { "Kept" } else { "Filtered" }
    );
    assert_eq!(status["native_subtitle_hls"], "Not supported");
    let mut decoder = m4s::Decoder::default();
    let mut observed = vec![];
    let mut native_tracks = vec![];
    for bytes in worker.wire.m4s_subscribe().0 {
        for event in decoder.push(&bytes).unwrap() {
            match event {
                m4s::Event::Info { tracks, .. } => native_tracks = tracks,
                m4s::Event::Frame {
                    track_id,
                    dts,
                    pts_offset,
                    key,
                    body,
                    ..
                } => observed.push(Frame {
                    track_id,
                    dts,
                    pts_offset,
                    key,
                    body,
                }),
                m4s::Event::Gop { tracks, frames, .. } => {
                    native_tracks = tracks;
                    observed.extend(frames)
                }
                _ => {}
            }
        }
    }
    assert_eq!(
        native_tracks
            .iter()
            .filter(|t| t.codec == "subtitle")
            .count(),
        if preserve { 2 } else { 0 },
        "effective original-track policy"
    );
    assert!(observed.iter().any(|f| f.track_id == 2));
    if preserve {
        for id in [7, 8] {
            assert!(
                observed
                    .iter()
                    .any(|f| f.track_id == id && !f.body.is_empty()),
                "missing text media for{id}"
            );
        }
        for f in observed.iter().filter(|f| f.track_id != 2) {
            assert!(frames.iter().any(|original| original.track_id == f.track_id
                && original.dts == f.dts
                && original.pts_offset == f.pts_offset
                && original.body == f.body));
        }
    } else {
        assert!(observed.iter().all(|f| f.track_id == 2));
    }
    for signal in if sparse {
        source_signals
    } else {
        worker.wire.signal_subscribe().0
    } {
        let text = std::str::from_utf8(&signal).unwrap();
        let stamp = text
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        let name = format!("{stamp}.m4f");
        let bytes = worker.wire.segment(&name).unwrap();
        let (ts, fs) = m4f::unpack(&bytes).unwrap();
        assert!(
            transcode || bytes.windows(17).any(|b| b == b"owned-AV-metadata"),
            "opaque AV track metadata lost"
        );
        assert_eq!(
            ts.iter().filter(|t| t.codec == "subtitle").count(),
            if preserve && sparse {
                m4f::unpack(&originals[&format!("owned/{name}")])
                    .unwrap()
                    .0
                    .iter()
                    .filter(|t| t.codec == "subtitle")
                    .count()
            } else if preserve {
                2
            } else {
                0
            }
        );
        if preserve && protocol == "m4f" {
            assert_eq!(bytes, originals[&format!("owned/{name}")]);
        }
        if !preserve {
            assert!(fs.iter().all(|f| f.track_id == 2));
            assert!(!bytes.windows(7).any(|b| b == b"AMERICA"));
            assert!(!bytes.windows(6).any(|b| b == b"EUROPE"));
        }
    }
    let master = engine.read("owned", "index.m3u8").await.unwrap();
    let text = std::str::from_utf8(&master).unwrap();
    let segment = text
        .lines()
        .find(|l| !l.starts_with('#') && l.ends_with(".ts"))
        .unwrap();
    let bytes = engine.read("owned", segment).await.unwrap();
    let file = dir.path().join("decoded-audio.ts");
    std::fs::write(&file, &bytes).unwrap();
    let decoded = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(file)
        .args(["-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success() && decoded.stderr.is_empty(),
        "AV decode failed: {}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "extra native source subscription"
    );
    if auth {
        let response = client
            .get(format!("{public}/owned/m4s?token=owned-viewer"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut chunks = response.bytes_stream();
        let bytes = tokio::time::timeout(Duration::from_secs(2), chunks.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!bytes.is_empty());
        drop(chunks);
        let response = client
            .get(format!("{public}/owned/m4f?token=owned-viewer"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut chunks = response.bytes_stream();
        let bytes = tokio::time::timeout(Duration::from_secs(2), chunks.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let signal = std::str::from_utf8(&bytes).unwrap();
        let stamp = signal
            .split_whitespace()
            .nth(1)
            .unwrap()
            .split('-')
            .next()
            .unwrap();
        let asset = format!("{public}/owned/{stamp}.m4f");
        drop(chunks);
        assert_eq!(client.get(&asset).send().await.unwrap().status(), 403);
        assert_eq!(
            client
                .get(format!("{asset}?token=wrong"))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        let response = client
            .get(format!("{asset}?token=owned-viewer"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let body = response.bytes().await.unwrap();
        let (tracks, _) = m4f::unpack(&body).unwrap();
        assert!(tracks.iter().any(|t| t.codec == "subtitle"));
        let response = client
            .put(format!("{public}/streamer/api/v3/streams/owned"))
            .basic_auth("admin", Some("owned-subtitle-admin"))
            .json(&json!({"disabled":true}))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert!(
            !client
                .get(format!("{asset}?token=owned-viewer"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success(),
            "disabled stream leaked a cached subtitle segment"
        );
    }
    engine.stop_all().await;
}
#[tokio::test]
async fn m4s_frames_preserve_native_text() {
    run("m4s", false, true, false, false, false).await;
}
#[tokio::test]
async fn m4s_frames_drop_native_text() {
    run("m4s", false, false, false, false, false).await;
}
#[tokio::test]
async fn m4s_gops_preserve_native_text() {
    run("m4s", true, true, false, false, false).await;
}
#[tokio::test]
async fn m4s_gops_drop_native_text() {
    run("m4s", true, false, false, false, false).await;
}
#[tokio::test]
async fn m4f_preserves_native_text() {
    run("m4f", true, true, false, false, false).await;
}
#[tokio::test]
async fn m4f_drops_native_text() {
    run("m4f", true, false, false, false, false).await;
}

#[tokio::test]
async fn native_text_preserve_during_transcoding_fails_before_packager() {
    run("m4s", false, true, true, false, false).await;
}
#[tokio::test]
async fn native_text_drop_allows_cpu_transcoding() {
    run("m4s", false, false, true, false, false).await;
}

#[tokio::test]
async fn m4s_native_subtitle_assets_require_viewer_auth_and_revoke() {
    run("m4s", true, true, false, true, false).await;
}
#[tokio::test]
async fn m4f_native_subtitle_assets_require_viewer_auth_and_revoke() {
    run("m4f", true, true, false, true, false).await;
}

#[tokio::test]
async fn m4f_sparse_text_preserve_keeps_av_continuity() {
    run("m4f", false, true, false, false, true).await;
}
#[tokio::test]
async fn m4f_sparse_text_drop_keeps_av_continuity() {
    run("m4f", false, false, false, false, true).await;
}
#[tokio::test]
async fn m4s_sparse_gops_preserve_keeps_av_continuity() {
    run("m4s", true, true, false, false, true).await;
}
#[tokio::test]
async fn m4s_sparse_gops_drop_keeps_av_continuity() {
    run("m4s", true, false, false, false, true).await;
}

#[tokio::test]
async fn late_m4f_text_discovery_stops_unsupported_transcoding() {
    run("m4f", false, true, true, false, true).await;
}
