#[path = "support/direct_media.rs"]
mod matrix;
#[tokio::test]
async fn independent_srtp_peers_decode_h264_hevc_all_audio_codecs_and_audio_only() {
    for video in [Some("libx264"), Some("libx265"), None] {
        matrix::run(video, true, None).await;
    }
}
#[tokio::test]
async fn independent_srtp_receive_internal_cpu_transcode_and_secure_output() {
    matrix::run_cpu(true).await;
}
#[tokio::test]
#[ignore = "requires explicitly provided independent Intel VAAPI fixture"]
async fn independent_igpu_encoded_srtp_delivered_media() {
    let fixture =
        std::env::var("FLUSSONIX_DIRECT_VIDEO_FIXTURE").expect("owned hardware fixture path");
    let codec = std::env::var("FLUSSONIX_DIRECT_VIDEO_CODEC").expect("h264_vaapi or hevc_vaapi");
    assert!(["h264_vaapi", "hevc_vaapi"].contains(&codec.as_str()));
    matrix::run(Some(&codec), true, Some(std::path::Path::new(&fixture))).await;
}
