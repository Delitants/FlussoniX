use flussonix::media::Engine;
use serde_json::json;
use std::{sync::Arc, time::Duration};
#[tokio::test]
async fn viewers_share_worker_and_stop_reaps_process() {
    let d = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(d.path(), "ffmpeg"));
    let cfg = json!({"inputs":[{"url":"testsrc://"}]});
    let (a, b) = tokio::join!(engine.ensure("news", &cfg), engine.ensure("news", &cfg));
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    let mut rx = a.subscribe();
    let bytes = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(bytes.contains(&0x47));
    for _ in 0..100 {
        if engine.read("news", "index.m3u8").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let manifest = engine.read("news", "index.m3u8").await.unwrap();
    assert!(String::from_utf8_lossy(&manifest).contains("#EXTINF"));
    let pid = a.pid();
    engine.stop("news").await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert_eq!(engine.count().await, 0);
}
