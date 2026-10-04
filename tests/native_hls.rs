use flussonix::captions;
use serde_json::json;
#[test]
fn native_track_selection_preserves_full_ids_and_rejects_ambiguous_formats() {
    let cfg = json!({"flussonix_hls_captions":[
        {"native_track":7,"language":"en","name":"English"},
        {"native_track":4294967295u32,"language":"de","name":"Deutsch"}
    ]});
    let s = captions::configuration(&cfg).expect("native selectors must be supported");
    assert_eq!(s[0].key(), "nt7");
    assert_eq!(s[1].key(), "nt4294967295");
    assert_eq!(
        serde_json::to_value(&s).unwrap(),
        cfg["flussonix_hls_captions"]
    );
    for rows in [
        json!([{"native_track":0,"language":"en","name":"Bad"}]),
        json!([{"native_track":4294967296u64,"language":"en","name":"Bad"}]),
        json!([{"native_track":7,"channel":1,"language":"en","name":"Bad"}]),
        json!([{"native_track":7,"language":"en","name":"A"},{"channel":1,"language":"de","name":"B"}]),
        json!([{"native_track":7,"language":"en","name":"A"},{"native_track":7,"language":"de","name":"B"}]),
    ] {
        assert!(captions::configuration(&json!({"flussonix_hls_captions":rows})).is_err());
    }
}
use axum::{Router, body::Body, routing::get};
use bytes::Bytes;
use flussonix::{
    m4f::{self, Frame},
    m4s::{self, PackedGop, Track},
    media::Engine,
    wire,
};
use futures_util::StreamExt;
use std::{collections::HashMap, sync::Arc, time::Duration};
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn fixture(bad: bool) -> (Vec<Track>, Vec<Frame>) {
    let tracks = vec![
        Track {
            id: 1,
            codec: "hevc".into(),
            config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 7,
            codec: "subtitle".into(),
            config: vec![],
        },
        Track {
            id: u32::MAX,
            codec: "subtitle".into(),
            config: vec![],
        },
    ];
    let timing: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    let mut frames = vec![];
    for cycle in 0..30u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            frames.push(Frame {
                track_id: 1,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
            });
        }
    }
    for (id, text) in [
        (7, "AMERICA <HELLO>\r\n\r\nsecond line"),
        (u32::MAX, "EUROPE GRÜSSE"),
    ] {
        frames.push(Frame {
            track_id: id,
            dts: 270000,
            pts_offset: 180000,
            key: true,
            body: if bad && id == 7 {
                vec![0xff]
            } else {
                text.as_bytes().to_vec()
            },
        });
        // Early clear must truncate the explicit four-second cue at three seconds.
        frames.push(Frame {
            track_id: id,
            dts: 360000,
            pts_offset: 0,
            key: true,
            body: vec![],
        });
    }
    frames.sort_by_key(|f| f.dts);
    (tracks, frames)
}
async fn source(
    protocol: &str,
    gops: bool,
    bad: bool,
) -> (String, Server, Arc<std::sync::atomic::AtomicUsize>) {
    let (tracks, frames) = fixture(bad);
    let mut data = wire::encode_info(&tracks).unwrap();
    let mut segments = HashMap::new();
    let mut signals = String::new();
    for n in 0..8u64 {
        let part: Vec<_> = frames
            .iter()
            .filter(|f| f.dts >= 90000 + n * 180000 && f.dts < 270000 + n * 180000)
            .cloned()
            .collect();
        if part.is_empty() {
            continue;
        }
        let body = Bytes::from(m4f::pack(&tracks, &part, 180000).unwrap());
        let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        signals += &format!("{n} {stamp}-2000\n");
        segments.insert(format!("owned/{stamp}.m4f"), body.clone());
        if gops {
            data.extend(
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
            data.extend(
                wire::encode_frame(tracks.iter().find(|t| t.id == f.track_id).unwrap(), f).unwrap(),
            );
        }
    }
    let data = Bytes::from(if protocol == "m4f" {
        signals.into_bytes()
    } else {
        data
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("{protocol}://{}/owned", listener.local_addr().unwrap());
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = requests.clone();
    let routes = Router::new()
        .route(
            &format!("/owned/{protocol}"),
            get(move || {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let data = data.clone();
                async move {
                    Body::from_stream(
                        futures_util::stream::once(std::future::ready(Ok::<_, std::io::Error>(
                            data,
                        )))
                        .chain(futures_util::stream::pending()),
                    )
                }
            }),
        )
        .route(
            "/{*path}",
            get(
                move |axum::extract::Path(path): axum::extract::Path<String>| {
                    let data = segments.get(&path).cloned().unwrap();
                    async move { data }
                },
            ),
        );
    (
        url,
        Server(tokio::spawn(async move {
            axum::serve(listener, routes).await.unwrap()
        })),
        requests,
    )
}
async fn run(protocol: &str, gops: bool, cpu: bool, preserve: bool, bad: bool) {
    let (url, _source, _) = source(protocol, gops, bad).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let mut cfg = json!({"inputs":[{"url":url}],"flussonix_subtitle_tracks":if preserve{"preserve"}else{"drop"},"flussonix_hls_subtitles":"convert","flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"},{"native_track":u32::MAX,"language":"de","name":"Deutsch"}]});
    if cpu {
        cfg["transcoder"] = json!({"encoder":"libx264","vb":300});
    }
    let worker = engine.ensure("owned", &cfg).await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut ready = true;
            for prefix in ["", "fmp4/"] {
                ready &= engine
                    .read("owned", &format!("{prefix}av.m3u8"))
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 3);
            }
            if ready {
                break;
            }
            assert!(
                worker.alive.load(std::sync::atomic::Ordering::Relaxed),
                "{}",
                worker.stats()
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|e| panic!("native HLS not ready: {e}; {}", worker.stats()));
    for prefix in ["", "fmp4/"] {
        let master = String::from_utf8(
            engine
                .read("owned", &format!("{prefix}index.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        if bad {
            assert!(!master.contains("TYPE=SUBTITLES"));
            assert!(
                engine
                    .read("owned", &format!("{prefix}nt7.m3u8"))
                    .await
                    .is_err()
            );
            continue;
        }
        assert!(master.contains("URI=\"nt4294967295.m3u8\""), "{master}");
        for (id, want, other) in [
            (7, "AMERICA &lt;HELLO&gt;", "EUROPE"),
            (u32::MAX, "EUROPE GRÜSSE", "AMERICA"),
        ] {
            let list = String::from_utf8(
                engine
                    .read("owned", &format!("{prefix}nt{id}.m3u8"))
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            let mut text = String::new();
            let mut quiet = false;
            for file in list.lines().filter(|l| l.ends_with(".vtt")) {
                let b = String::from_utf8(
                    engine
                        .read("owned", &format!("{prefix}{file}"))
                        .await
                        .unwrap()
                        .to_vec(),
                )
                .unwrap();
                quiet |= !b.contains("-->");
                text += &b;
            }
            assert!(text.contains(want), "{text}");
            assert!(!text.contains(other));
            assert!(quiet, "silence must have empty VTT segments");
            // First video presentation is at 1.08s: cue starts at 3.0s, clear at 4.0s.
            assert!(text.contains("00:00:01.920 --> 00:00:02.920"), "{text}");
            assert!(
                !text.contains("00:00:02.920 -->"),
                "early clear must truncate cue: {text}"
            );
        }
        let av = String::from_utf8(
            engine
                .read("owned", &format!("{prefix}av.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let file = av
            .lines()
            .find(|l| l.ends_with(if prefix.is_empty() { ".ts" } else { ".m4s" }))
            .unwrap();
        let mut data = vec![];
        if !prefix.is_empty() {
            let init = av
                .lines()
                .find_map(|l| l.strip_prefix("#EXT-X-MAP:URI=\""))
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
        data.extend(
            engine
                .read("owned", &format!("{prefix}{file}"))
                .await
                .unwrap(),
        );
        let path = dir.path().join("decode.bin");
        std::fs::write(&path, data).unwrap();
        let decode = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-threads", "1", "-i"])
            .arg(&path)
            .args(["-frames:v", "2", "-f", "null", "-"])
            .output()
            .unwrap();
        assert!(
            decode.status.success(),
            "{}",
            String::from_utf8_lossy(&decode.stderr)
        );
    }
    if bad {
        assert_eq!(
            worker.stats()["hls_captions"]["last_error"],
            "native_subtitle_invalid_utf8"
        );
        assert_eq!(worker.stats()["native_subtitle_hls"], "Failed");
    } else {
        assert_eq!(worker.stats()["native_subtitle_hls"], "Converted");
    }
    if !cpu {
        let (cached, _) = worker.m4s_subscribe().unwrap();
        let mut d = m4s::Decoder::default();
        let mut text = false;
        for b in cached {
            for event in d.push(&b).unwrap() {
                match event {
                    m4s::Event::Info { tracks, .. } | m4s::Event::Gop { tracks, .. } => {
                        text |= tracks.iter().any(|t| t.codec == "subtitle")
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(
            text, preserve,
            "original output policy must remain independent"
        );
    }
    engine.stop_all().await;
}
#[tokio::test]
async fn native_m4s_frames_to_both_hls_variants() {
    run("m4s", false, false, true, false).await
}
#[tokio::test]
async fn native_m4f_sparse_text_to_both_hls_variants_with_native_drop() {
    run("m4f", true, false, false, false).await
}
#[tokio::test]
async fn native_m4s_gops_cpu_conversion_with_native_drop() {
    run("m4s", true, true, false, false).await
}
#[tokio::test]
async fn malformed_selected_native_text_falls_back_to_av() {
    run("m4s", false, false, false, true).await
}
#[test]
fn writes_owned_native_browser_fixture_when_requested() {
    if let Ok(path) = std::env::var("FLUSSONIX_NATIVE_FIXTURE_FILE") {
        let (tracks, frames) = fixture(false);
        let mut bytes = wire::encode_info(&tracks).unwrap();
        for f in frames {
            bytes.extend(
                wire::encode_frame(tracks.iter().find(|t| t.id == f.track_id).unwrap(), &f)
                    .unwrap(),
            );
        }
        std::fs::write(path, bytes).unwrap();
    }
}
#[tokio::test]
async fn native_hls_assets_require_authorization_and_revoke_cached_delivery() {
    use flussonix::server::{App, Options, router};
    use sha2::Digest;
    let (input, _source, requests) = source("m4s", false, false).await;
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-native-admin".into(),
            peer_key: "owned-native-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let mut cfg = json!({"static":false,"inputs":[{"url":input}],"flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"}],"flussonix_token_sha256":format!("{:x}",sha2::Sha256::digest(b"owned-native-viewer"))});
    app.config
        .put("streams", "group/owned", cfg.clone())
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cloned = app.clone();
    let _delivery = Server(tokio::spawn(async move {
        axum::serve(listener, router(cloned)).await.unwrap()
    }));
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for prefix in ["", "fmp4/"] {
        assert_eq!(
            client
                .get(format!("{base}/group/owned/{prefix}nt7.m3u8"))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    assert_eq!(app.media.count().await, 0);
    assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 0);
    for prefix in ["", "fmp4/"] {
        let master = client
            .get(format!(
                "{base}/group/owned/{prefix}index.m3u8?token=owned-native-viewer"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(master.status(), 200);
        assert!(
            master
                .text()
                .await
                .unwrap()
                .contains("nt7.m3u8?token=owned-native-viewer")
        );
        let list = client
            .get(format!(
                "{base}/group/owned/{prefix}nt7.m3u8?token=owned-native-viewer"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(list.status(), 200);
        let list = list.text().await.unwrap();
        let file = list
            .lines()
            .find(|l| !l.starts_with('#') && l.contains(".vtt"))
            .unwrap();
        assert!(file.ends_with("?token=owned-native-viewer"));
        let cached = file.split('?').next().unwrap();
        assert_eq!(
            client
                .get(format!("{base}/group/owned/{prefix}{cached}"))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        assert_eq!(
            client
                .get(format!("{base}/group/owned/{prefix}{file}"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        // A changed generation must not serve an old, authorized cache filename.
        cfg["flussonix_hls_captions"][0]["name"] = json!(format!("Replacement-{prefix}"));
        app.config
            .put("streams", "group/owned", cfg.clone())
            .unwrap();
        assert_ne!(
            client
                .get(format!("{base}/group/owned/{prefix}{file}"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        cfg["disabled"] = json!(true);
        app.config
            .put("streams", "group/owned", cfg.clone())
            .unwrap();
        assert_eq!(
            client
                .get(format!("{base}/group/owned/{prefix}{file}"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        cfg["disabled"] = json!(false);
        app.config
            .put("streams", "group/owned", cfg.clone())
            .unwrap();
    }
    app.media.stop_all().await;
}
