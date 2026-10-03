#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod fixture;
use flussonix::{
    caption_transport::Transport,
    captions::{Decoder, Service},
    media::Engine,
};
use serde_json::json;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
#[test]
fn strict_modes_and_conversion_selection() {
    for mode in ["passthrough", "convert", "drop"] {
        assert_eq!(
            flussonix::config::hls_subtitles(&json!({"flussonix_hls_subtitles":mode})).unwrap(),
            mode
        );
    }
    assert!(
        flussonix::config::hls_subtitles(&json!({"flussonix_hls_subtitles":"ignore"})).is_err()
    );
    for mode in ["passthrough", "drop"] {
        assert!(flussonix::captions::configuration(&json!({"flussonix_hls_subtitles":mode,"flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]})).unwrap().is_empty());
    }
    assert_eq!(
        flussonix::config::hls_subtitles(
            &json!({"flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]})
        )
        .unwrap(),
        "convert"
    );
}
#[tokio::test]
async fn hls_passes_or_filters_embedded_captions_without_changing_other_outputs() {
    for mode in ["passthrough", "drop"] {
        let d = tempfile::tempdir().unwrap();
        let e = Engine::new(d.path(), "ffmpeg");
        let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_subtitles":mode});
        let mut p = e
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        p.stdin
            .as_mut()
            .unwrap()
            .write_all(&fixture::transport())
            .await
            .unwrap();
        let mut lists = vec![];
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                lists.clear();
                for prefix in ["", "fmp4/"] {
                    if let Ok(b) = e.read("owned", &format!("{prefix}index.m3u8")).await {
                        let list = String::from_utf8(b.to_vec()).unwrap();
                        if list.matches("#EXTINF:").count() >= 5 {
                            lists.push((prefix, list));
                        }
                    }
                }
                if lists.len() == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(40)).await
            }
        })
        .await
        .unwrap();
        for (prefix, list) in lists {
            let out = d.path().join(if prefix.is_empty() { "ts" } else { "mp4" });
            std::fs::create_dir(&out).unwrap();
            std::fs::write(out.join("index.m3u8"), format!("{list}#EXT-X-ENDLIST\n")).unwrap();
            if !prefix.is_empty() {
                let init = list
                    .lines()
                    .find_map(|s| s.strip_prefix("#EXT-X-MAP:URI=\""))
                    .unwrap()
                    .split('"')
                    .next()
                    .unwrap();
                std::fs::write(
                    out.join(init),
                    e.read("owned", &format!("{prefix}{init}")).await.unwrap(),
                )
                .unwrap();
            }
            for file in list
                .lines()
                .filter(|s| !s.starts_with('#') && !s.is_empty())
            {
                std::fs::write(
                    out.join(file),
                    e.read("owned", &format!("{prefix}{file}")).await.unwrap(),
                )
                .unwrap();
            }
            let demux = std::process::Command::new("ffmpeg")
                .args(["-v", "error", "-i"])
                .arg(out.join("index.m3u8"))
                .args(["-map", "0:v:0", "-c", "copy", "-f", "mpegts", "pipe:1"])
                .output()
                .unwrap();
            assert!(
                demux.status.success(),
                "{}",
                String::from_utf8_lossy(&demux.stderr)
            );
            let mut decoder = Decoder::new(vec![Service {
                channel: 1,
                language: "en".into(),
                name: "English".into(),
            }]);
            Transport::default().push(&demux.stdout, &mut decoder);
            let text = decoder
                .snapshot()
                .iter()
                .map(|c| c.text.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            assert_eq!(
                text.contains("LIVE"),
                mode == "passthrough",
                "{prefix} {mode}: {text}"
            );
            let decoded = std::process::Command::new("ffmpeg")
                .args(["-v", "error", "-i"])
                .arg(out.join("index.m3u8"))
                .args(["-frames:v", "10", "-f", "null", "-"])
                .output()
                .unwrap();
            assert!(decoded.status.success());
        }
        // The HLS policy is scoped to delivered HLS files, never mutates shared TS media.
        let raw = std::fs::read(
            d.path()
                .join(format!(
                    "{:x}",
                    <sha2::Sha256 as sha2::Digest>::digest(b"owned")
                ))
                .join("index.m3u8"),
        )
        .unwrap();
        let raw = String::from_utf8(raw).unwrap();
        let mut all = vec![];
        for file in raw.lines().filter(|s| s.ends_with(".ts")) {
            all.extend(
                std::fs::read(
                    d.path()
                        .join(format!(
                            "{:x}",
                            <sha2::Sha256 as sha2::Digest>::digest(b"owned")
                        ))
                        .join(file),
                )
                .unwrap(),
            );
        }
        let mut decoder = Decoder::new(vec![Service {
            channel: 1,
            language: "en".into(),
            name: "English".into(),
        }]);
        Transport::default().push(&all, &mut decoder);
        assert!(decoder.snapshot().iter().any(|c| c.text.contains("LIVE")));
        e.stop_all().await;
    }
}
#[test]
fn audio_only_filtering_is_an_unchanged_noop_for_ts_and_fmp4() {
    let d = tempfile::tempdir().unwrap();
    let list = d.path().join("index.m3u8");
    let o = std::process::Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "4",
            "-c:a",
            "aac",
            "-f",
            "hls",
            "-hls_time",
            "1",
            "-hls_segment_type",
            "fmp4",
        ])
        .arg(&list)
        .output()
        .unwrap();
    assert!(o.status.success());
    let init = std::fs::read(d.path().join("init.mp4")).unwrap();
    let mut segment = std::fs::read(d.path().join("index0.m4s")).unwrap();
    let original = segment.clone();
    flussonix::caption_filter::mp4(&init, &mut segment).unwrap();
    assert_eq!(segment, original);
    let output = std::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&list)
        .args(["-c", "copy", "-f", "mpegts", "pipe:1"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let mut ts = output.stdout.clone();
    flussonix::caption_filter::ts(&mut ts).unwrap();
    assert_eq!(ts, output.stdout);
}
