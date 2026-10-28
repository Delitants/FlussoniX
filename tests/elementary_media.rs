//! Independent plaintext RTP qualification.
#[path = "support/elementary_media.rs"]
mod support;
use serde_json::json;
use support::qualify;
#[tokio::test]
#[should_panic(expected = "video picture gap")]
async fn independent_decode_rejects_decodable_video_picture_gap() {
    let owned = tempfile::tempdir().unwrap();
    let media = owned.path().join("gap.ts");
    let encoded = tokio::process::Command::new("/usr/bin/ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=duration=3:size=320x180:rate=25",
            "-vf",
            r"setpts=PTS+1.5/TB*gte(N\,40)",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-g",
            "25",
            "-bf",
            "0",
            "-fps_mode",
            "passthrough",
            "-f",
            "mpegts",
        ])
        .arg(&media)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    support::decode(&media, Some("h264"), &[]).await;
}
#[tokio::test]
async fn elementary_roundtrip_cpu_h264_hevc_and_all_audio_codecs_decode() {
    for video in ["libx264", "libx265"] {
        for audio in ["aac", "mp2", "libmp3lame"] {
            qualify(
                Some(video),
                &[audio],
                json!({"encoder":"copy","acodec":"copy"}),
                false,
                false,
            )
            .await;
        }
    }
}
#[tokio::test]
async fn elementary_audio_only_and_multiple_mpeg_audio_tracks_remain_distinct() {
    qualify(
        None,
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
    qualify(
        None,
        &["mp2", "libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn elementary_cpu_worker_changes_video_and_audio_codec_once() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"libx265","vb":300,"acodec":"mp3","ab":128}),
        false,
        false,
    )
    .await;
}
#[tokio::test]
#[ignore = "requires an independently installed working VAAPI render device and driver environment"]
async fn elementary_internal_vaapi_worker_output_decodes() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"h264_vaapi","qp":24,"acodec":"mp2a","ab":192}),
        false,
        false,
    )
    .await;
}
