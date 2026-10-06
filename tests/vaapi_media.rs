use flussonix::media::Engine;
use serde_json::json;
#[path = "support/profile_media.rs"]
mod profile_media;
#[tokio::test]
#[ignore = "requires an owned working VAAPI render device and driver environment"]
async fn internal_vaapi_h264_delivers_decodable_shared_outputs() {
    for (audio, rate, codec) in [
        ("aac", 96, "aac"),
        ("mp2a", 192, "mp2"),
        ("mp3", 128, "mp3"),
    ] {
        let d = tempfile::tempdir().unwrap();
        let engine = Engine::new(d.path(), "/usr/bin/ffmpeg");
        profile_media::qualify(&engine,d.path(),json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"h264_vaapi","qp":24,"acodec":audio,"ab":rate}}),"h264",codec).await;
    }
}
#[tokio::test]
#[ignore = "requires an owned working VAAPI render device and driver environment"]
async fn internal_vaapi_replacement_and_cancellation_reap_owned_encoders() {
    use std::{sync::Arc, time::Duration};
    let d = tempfile::tempdir().unwrap();
    let engine = Engine::new(d.path(), "/usr/bin/ffmpeg");
    let gpu = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"h264_vaapi"}});
    let cpu = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264"}});
    let first = engine.ensure("owned", &gpu).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while first.wire.rtp.play_snapshot().is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &engine.ensure("owned", &gpu).await.unwrap()
    ));
    let second = engine.ensure("owned", &cpu).await.unwrap();
    assert_ne!(first.pid(), second.pid());
    assert!(first.is_closed());
    assert!(!std::path::Path::new(&format!("/proc/{}", first.pid())).exists());
    assert_eq!(
        engine
            .ensure_guarded("owned", &gpu, true, std::future::ready(false))
            .await
            .err()
            .unwrap(),
        "media route changed"
    );
    assert!(!second.is_closed());
    let third = engine.ensure("owned", &gpu).await.unwrap();
    assert!(second.is_closed());
    engine.stop_all().await;
    assert!(third.is_closed());
    for w in [&second, &third] {
        assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
    }
}
