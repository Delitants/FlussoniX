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

#[tokio::test]
async fn secure_source_to_secure_destination_all_video_audio_decode() {
    for video in ["libx264", "libx265"] {
        for audio in ["aac", "mp2", "libmp3lame"] {
            qualify(
                Some(video),
                &[audio],
                json!({"encoder":"copy","acodec":"copy"}),
                true,
                true,
            )
            .await;
        }
    }
}
#[tokio::test]
async fn secure_destination_audio_only_multiple_tracks_and_cpu_conversion_decode() {
    qualify(
        None,
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        true,
    )
    .await;
    qualify(
        None,
        &["mp2", "libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        true,
        true,
    )
    .await;
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"libx265","vb":300,"acodec":"mp3","ab":128}),
        true,
        true,
    )
    .await;
}
#[tokio::test]
#[ignore = "requires available Intel iGPU and an independent compatible VAAPI driver"]
async fn secure_elementary_internal_vaapi_worker_output_decodes() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"h264_vaapi","qp":24,"acodec":"mp2a","ab":192}),
        true,
        true,
    )
    .await;
}
