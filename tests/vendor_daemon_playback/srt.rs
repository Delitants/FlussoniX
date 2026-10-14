//! M4F input reaches an independently decoded encrypted SRT transport.
use super::*;
#[path = "../support/vendor_srt.rs"]
#[allow(dead_code)]
mod transport;
const SECRET: &str = "owned-vendor-srt-transport-key";
fn hex_dump(text: &str) -> Vec<u8> {
    text.lines()
        .filter_map(|l| l.split_once(':'))
        .flat_map(|(_, r)| {
            let hex = r.trim_start().split("  ").next().unwrap().replace(' ', "");
            hex.as_bytes()
                .chunks_exact(2)
                .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}
async fn video(codec: &str, dir: &std::path::Path) -> (Track, Vec<Frame>) {
    let (config, timing, samples, base, den) = if codec == "hevc" {
        let timing: Vec<Value> =
            serde_json::from_str(include_str!("../fixtures/codecs/hevc-timing.json")).unwrap();
        let samples = (0..timing.len())
            .map(|i| std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap())
            .collect::<Vec<_>>();
        (
            include_bytes!("../fixtures/codecs/hevc.hvcc").to_vec(),
            timing,
            samples,
            1024,
            12800,
        )
    } else {
        let path = dir.join("owned-avc.mp4");
        let encoded = tokio::process::Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-nostdin",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x96:rate=25",
                "-t",
                "0.48",
                "-c:v",
                "libx264",
                "-threads",
                "1",
                "-bf",
                "0",
                "-g",
                "12",
            ])
            .arg(&path)
            .output()
            .await
            .unwrap();
        assert!(encoded.status.success() && encoded.stderr.is_empty());
        let probed = tokio::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_streams",
                "-show_packets",
                "-show_data",
                "-of",
                "json",
            ])
            .arg(path)
            .output()
            .await
            .unwrap();
        assert!(probed.status.success() && probed.stderr.is_empty());
        let info: Value = serde_json::from_slice(&probed.stdout).unwrap();
        let den = info["streams"][0]["time_base"]
            .as_str()
            .unwrap()
            .strip_prefix("1/")
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let timing = info["packets"].as_array().unwrap().clone();
        let samples = timing
            .iter()
            .map(|t| hex_dump(t["data"].as_str().unwrap()))
            .collect();
        (
            hex_dump(info["streams"][0]["extradata"].as_str().unwrap()),
            timing,
            samples,
            0,
            den,
        )
    };
    let mut frames = vec![];
    for cycle in 0..8u64 {
        for (t, body) in timing.iter().zip(&samples) {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            frames.push(Frame {
                track_id: 205,
                dts: 90000 + cycle * 43200 + ((d + base) * 90000 / den) as u64,
                pts_offset: (p - d) * 90000 / den,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: body.clone(),
            });
        }
    }
    (
        Track {
            id: 205,
            codec: codec.into(),
            config,
        },
        frames,
    )
}
#[tokio::test]
async fn encrypted_vendor_srt_independently_decodes_avc_hevc_mp3_and_mp2_from_m4f() {
    for codec in ["h264", "hevc"] {
        let dir = tempfile::tempdir().unwrap();
        let (video, video_frames) = video(codec, dir.path()).await;
        let f = Fixture::variant(json!({"on_play":"true","only":[]}), "m4f", "cdn").await;
        let (mut tracks, mut frames) = flussonix::m4f::unpack(&multitrack_segment()).unwrap();
        tracks.push(video);
        frames.extend(video_frames);
        frames.sort_by_key(|f| f.dts);
        *f.segment.lock().unwrap() = flussonix::m4f::pack(&tracks, &frames, 360000).unwrap();
        let serving = transport::Serving::start(f.app.clone(), SECRET).await;
        let path = dir.path().join("received.ts");
        let mut child = tokio::process::Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-xerror",
                "-nostdin",
                "-threads",
                "1",
                "-passphrase",
                SECRET,
                "-srt_streamid",
                "#!::r=owned,m=request,u=owned-srt",
                "-analyzeduration",
                "500000",
                "-probesize",
                "131072",
                "-i",
            ])
            .arg(format!(
                "srt://{}?mode=caller&latency=120000&connect_timeout=1000&timeout=3000000",
                serving.address
            ))
            .args([
                "-map", "0:v:0", "-map", "0:a:0", "-map", "0:a:1", "-t", "0.2", "-f", "null", "-",
                "-map", "0", "-c", "copy", "-t", "0.5", "-f", "mpegts",
            ])
            .arg(&path)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        use tokio::io::AsyncReadExt;
        let mut stderr = child.stderr.take().unwrap();
        let errors = tokio::spawn(async move {
            let mut bytes = vec![];
            stderr.read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        let status = match tokio::time::timeout(Duration::from_secs(15), child.wait()).await {
            Ok(result) => result.unwrap().success(),
            Err(_) => {
                child.kill().await.unwrap();
                let _ = child.wait().await;
                false
            }
        };
        let errors = errors.await.unwrap();
        let pulls = f
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == "/owned/m4f")
            .count();
        serving.stop().await;
        f.stop().await;
        assert!(
            status && errors.is_empty(),
            "{codec} SRT decode: {}",
            String::from_utf8_lossy(&errors)
        );
        assert_eq!(pulls, 1);
        let probe = tokio::process::Command::new("ffprobe")
            .args(["-v", "error", "-show_streams", "-of", "json"])
            .arg(path)
            .output()
            .await
            .unwrap();
        assert!(probe.status.success() && probe.stderr.is_empty());
        let info: Value = serde_json::from_slice(&probe.stdout).unwrap();
        let actual: std::collections::BTreeSet<_> = info["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["codec_name"].as_str().unwrap())
            .collect();
        assert_eq!(
            actual,
            std::collections::BTreeSet::from([codec, "mp2", "mp3"])
        );
    }
}
#[tokio::test]
async fn vendor_srt_av_shares_native_text_preservation_filtering_and_hls_conversion() {
    for mode in ["preserve", "drop", "convert"] {
        let f = Fixture::variant(json!({"on_play":"true","only":[]}), "m4f", "cdn").await;
        *f.segment.lock().unwrap() = text_segment();
        f.duration.store(10000, std::sync::atomic::Ordering::SeqCst);
        let mut profile = json!({"flussonix_subtitle_tracks":if mode=="preserve" {"preserve"}else{"drop"},"flussonix_hls_subtitles":if mode=="convert" {"convert"}else{"drop"}});
        if mode == "convert" {
            profile["flussonix_hls_captions"] = json!([{"native_track":7,"language":"en","name":"English"},{"native_track":8,"language":"de","name":"Deutsch"}]);
        }
        f.app
            .config
            .put("templates", "processing", profile)
            .unwrap();
        f.app
            .config
            .put(
                "sources",
                &f.source,
                json!({"flussonix_output_template":"processing"}),
            )
            .unwrap();
        let serving = transport::Serving::start(f.app.clone(), SECRET).await;
        let receiver =
            transport::Receiver::start(serving.address, "srt-text-viewer", SECRET, "100");
        tokio::time::timeout(Duration::from_secs(4), async {
            while f.app.media.stats("owned").await["online_clients"] != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let live = f.get("/owned/m4s?token=http-text-viewer").await;
        assert_eq!(live.status(), StatusCode::OK);
        let mut body = live.into_body();
        let tracks = native_info(&mut body).await;
        let subtitles = tracks.iter().filter(|t| t.codec == "subtitle").count();
        let mut texts = vec![];
        if mode == "convert" {
            let master = String::from_utf8(
                bytes(f.get("/owned/index.m3u8?token=http-text-viewer").await)
                    .await
                    .to_vec(),
            )
            .unwrap();
            assert!(master.contains("English") && master.contains("Deutsch"));
            for id in [7, 8] {
                let list = String::from_utf8(
                    bytes(
                        f.get(&format!("/owned/nt{id}.m3u8?token=http-text-viewer"))
                            .await,
                    )
                    .await
                    .to_vec(),
                )
                .unwrap();
                let mut text = String::new();
                for file in list
                    .lines()
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                {
                    text += &String::from_utf8_lossy(
                        &bytes(f.get(&format!("/owned/{file}")).await).await,
                    );
                }
                texts.push(text);
            }
        }
        let outcome = receiver.finish().await;
        let pulls = f
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == "/owned/m4f")
            .count();
        drop(body);
        serving.stop().await;
        f.stop().await;
        assert!(
            outcome.0 && outcome.1.is_empty(),
            "{mode} SRT AV decode: {}",
            outcome.1
        );
        assert_eq!(subtitles, if mode == "preserve" { 2 } else { 0 });
        assert_eq!(pulls, 1);
        if mode == "convert" {
            assert!(texts[0].contains("AMERICA HELLO") && texts[1].contains("EUROPE GRÜSSE"));
        }
    }
}
