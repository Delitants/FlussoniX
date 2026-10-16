use flussonix::media::Engine;
use futures_util::FutureExt;
use serde_json::json;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

// Applying an AAC-only tee bitstream filter to MPEG audio must fail this test.
async fn copy_publication_decodes_fmp4(audio: &str) {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(directory.path().join("media"), "ffmpeg");
    let outcome = std::panic::AssertUnwindSafe(async {
        let input = directory.path().join("input.ts");
        let multiple = matches!(audio, "multi-aac" | "mixed-aac-first" | "mixed-mp2-first");
        let mut generator = tokio::process::Command::new("ffmpeg");
        generator.kill_on_drop(true).args([
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x180:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
        ]);
        if multiple {
            generator.args([
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=880:sample_rate=48000",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                "-map",
                "2:a:0",
            ]);
        }
        generator.args([
            "-t",
            "8",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "50",
            "-c:a",
            if multiple { "aac" } else { audio },
            "-b:a",
            "192k",
        ]);
        if audio == "mixed-aac-first" {
            generator.args(["-c:a:1", "mp2"]);
        }
        if audio == "mixed-mp2-first" {
            generator.args(["-c:a:0", "mp2"]);
        }
        if audio == "multi-aac" {
            generator.args([
                "-program",
                "program_num=1:st=0:st=1",
                "-program",
                "program_num=2:st=2",
            ]);
        }
        let generated = tokio::time::timeout(
            Duration::from_secs(15),
            generator.args(["-f", "mpegts"]).arg(&input).output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            generated.status.success(),
            "{}",
            String::from_utf8_lossy(&generated.stderr)
        );
        let cfg = json!({"inputs":[{"url":"publish://"}],
            "transcoder":{"encoder":"copy","acodec":"copy"}});
        let mut publication = engine
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(8),
            publication
                .stdin
                .as_mut()
                .unwrap()
                .write_all(&std::fs::read(input).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        // Keep the publisher open while examining the live HLS output.
        let playlist = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(data) = engine.read("owned", "fmp4/index.m3u8").await {
                    let text = String::from_utf8(data.to_vec()).unwrap();
                    if text.contains("#EXTINF:") {
                        break text;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("fMP4 playlist readiness");
        let init = playlist
            .lines()
            .find_map(|line| line.strip_prefix("#EXT-X-MAP:URI=\""))
            .unwrap()
            .split('"')
            .next()
            .unwrap();
        let segment = playlist
            .lines()
            .rfind(|line| !line.is_empty() && !line.starts_with('#'))
            .unwrap();
        let mut bytes = engine
            .read("owned", &format!("fmp4/{init}"))
            .await
            .unwrap()
            .to_vec();
        bytes.extend_from_slice(
            &engine
                .read("owned", &format!("fmp4/{segment}"))
                .await
                .unwrap(),
        );
        let file = directory.path().join("sample.mp4");
        std::fs::write(&file, bytes).unwrap();
        let decoded = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new("ffmpeg")
                .kill_on_drop(true)
                .args([
                    "-nostdin",
                    "-v",
                    "error",
                    "-xerror",
                    "-err_detect",
                    "explode",
                    "-i",
                ])
                .arg(&file)
                .args([
                    "-map", "0:v:0", "-map", "0:a", "-threads", "1", "-f", "framemd5", "-",
                ])
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            decoded.status.success() && decoded.stderr.is_empty(),
            "{audio}: {}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        let frames = String::from_utf8(decoded.stdout).unwrap();
        assert!(
            frames.lines().filter(|line| line.starts_with("0,")).count() >= 25,
            "decoded video missing; playlist={playlist}; frames={frames}"
        );
        assert!(
            frames.lines().filter(|line| line.starts_with("1,")).count() >= 50,
            "decoded audio missing; playlist={playlist}; frames={frames}"
        );
        if multiple {
            assert!(
                frames.lines().filter(|line| line.starts_with("2,")).count() >= 50,
                "second decoded audio missing; playlist={playlist}; frames={frames}"
            );
        }
        drop(publication);
    })
    .catch_unwind()
    .await;
    engine.stop_all().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn layer_ii_publication_copy_delivers_fmp4_audio_and_video() {
    copy_publication_decodes_fmp4("mp2").await;
}
#[tokio::test]
async fn mp3_publication_copy_delivers_fmp4_audio_and_video() {
    copy_publication_decodes_fmp4("libmp3lame").await;
}
#[tokio::test]
async fn aac_publication_copy_delivers_fmp4_audio_and_video() {
    copy_publication_decodes_fmp4("aac").await;
}

#[tokio::test]
async fn copy_publication_reserves_exclusivity_and_cancels_before_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(directory.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"publish://"}],
        "transcoder":{"encoder":"copy","acodec":"copy"}});
    let outcome = std::panic::AssertUnwindSafe(async {
        let publication = engine
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        let worker = publication.worker.clone();
        assert_eq!(worker.pid(), 0, "no subprocess before codec metadata");
        assert!(
            engine
                .publish_guarded("owned", &cfg, std::future::ready(true))
                .await
                .is_err(),
            "metadata collection must reserve the only publisher"
        );
        drop(publication);
        tokio::time::timeout(Duration::from_secs(2), worker.closed())
            .await
            .unwrap();
        assert_eq!(
            worker.pid(),
            0,
            "cancelled setup must not start a subprocess"
        );
    })
    .catch_unwind()
    .await;
    engine.stop_all().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn copy_publication_metadata_has_byte_and_time_limits() {
    for oversized in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::new(directory.path(), "ffmpeg");
        let cfg = json!({"inputs":[{"url":"publish://"}], "flussonix_input_timeout":1,
            "transcoder":{"encoder":"copy","acodec":"copy"}});
        let outcome = std::panic::AssertUnwindSafe(async {
            let mut publication = engine
                .publish_guarded("owned", &cfg, std::future::ready(true))
                .await
                .unwrap();
            let worker = publication.worker.clone();
            if oversized {
                // Valid transport alignment, but no PAT/PMT or codec metadata.
                let mut packet = [0xff; 188];
                packet[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
                let bytes = packet.repeat(6000);
                let result = tokio::time::timeout(
                    Duration::from_secs(3),
                    publication.stdin.as_mut().unwrap().write_all(&bytes),
                )
                .await
                .unwrap();
                assert!(
                    result.is_err(),
                    "metadata exceeding 1 MiB must close the publisher pipe"
                );
            }
            tokio::time::timeout(Duration::from_secs(3), worker.closed())
                .await
                .unwrap();
            assert_eq!(
                worker.pid(),
                0,
                "unqualified metadata must not spawn FFmpeg"
            );
            let stats = engine.stats("owned").await;
            assert_eq!(
                stats["last_error"],
                if oversized {
                    "metadata_limit"
                } else {
                    "startup_timeout"
                }
            );
            drop(publication);
        })
        .catch_unwind()
        .await;
        engine.stop_all().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn multiprogram_aac_publication_copy_preserves_all_audio() {
    copy_publication_decodes_fmp4("multi-aac").await;
}
#[tokio::test]
async fn mixed_publication_copy_filters_only_the_aac_track_in_either_order() {
    copy_publication_decodes_fmp4("mixed-aac-first").await;
    copy_publication_decodes_fmp4("mixed-mp2-first").await;
}
