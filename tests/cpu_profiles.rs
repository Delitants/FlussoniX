use flussonix::{m4f, media::Engine};
use serde_json::json;
#[path = "support/profile_media.rs"]
mod profile_media;
use profile_media::qualify;

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
