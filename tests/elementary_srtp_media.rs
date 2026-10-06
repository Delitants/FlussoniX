//! Independent encrypted sources and receivers qualify actual media.
#[path = "support/elementary_media.rs"]
mod support;
use serde_json::json;
use support::qualify;
#[tokio::test]
async fn secure_elementary_source_h264_hevc_all_audio_decode() {
    for video in ["libx264", "libx265"] {
        for audio in ["aac", "mp2", "libmp3lame"] {
            qualify(
                Some(video),
                &[audio],
                json!({"encoder":"copy","acodec":"copy"}),
                true,
                false,
            )
            .await;
        }
    }
}
#[tokio::test]
async fn secure_source_audio_only_multiple_tracks_and_cpu_conversion_decode() {
    qualify(
        None,
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        true,
        false,
    )
    .await;
    qualify(
        None,
        &["mp2", "libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        true,
        false,
    )
    .await;
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"libx265","vb":300,"acodec":"mp3","ab":128}),
        true,
        false,
    )
    .await;
}
