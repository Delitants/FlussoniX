use flussonix::media::Engine;
use serde_json::json;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
async fn ended(w: &flussonix::media::Worker) {
    tokio::time::timeout(Duration::from_secs(8), w.closed())
        .await
        .unwrap();
    for _ in 0..100 {
        if !w.alive.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("worker process was not reaped");
}
#[tokio::test]
async fn ordered_fallback_waits_for_cooldown_and_coalesces_recovery() {
    let d = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let e = Arc::new(Engine::new(d.path(), "ffmpeg"));
    let cfg =
        json!({"inputs":[{"url":format!("tshttp://127.0.0.1:{port}/owned")},{"url":"testsrc://"}]});
    let first = e.ensure("owned", &cfg).await.unwrap();
    ended(&first).await;
    assert_eq!(first.stats()["status"], "retrying");
    assert!(e.recover("owned", &cfg).await.is_err());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (a, b) = tokio::join!(e.recover("owned", &cfg), e.recover("owned", &cfg));
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(a.stats()["input_index"], 1);
    assert_eq!(a.stats()["restart_count"], 1);
    let mut rx = a.subscribe();
    assert!(
        !tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    e.stop_all().await;
}
#[cfg(unix)]
#[tokio::test]
async fn an_alive_child_without_media_is_timed_out_and_reaped() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let script = d.path().join("owned-sleep-worker");
    std::fs::write(&script, "#!/bin/sh\nexec /bin/sleep 30\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), script.to_str().unwrap());
    let w = e
        .ensure(
            "owned",
            &json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":1}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1400)).await;
    let running = w.alive.load(Ordering::Relaxed);
    let stats = w.stats();
    e.stop_all().await;
    assert!(
        !running,
        "alive subprocess producing no media escaped the watchdog"
    );
    assert_eq!(stats["last_error"], "startup_timeout");
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}
#[tokio::test]
async fn successful_media_outlives_the_stall_timeout() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let w = e
        .ensure(
            "owned",
            &json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":2}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let alive = w.alive.load(Ordering::Relaxed);
    let bytes = w.bytes.load(Ordering::Relaxed);
    e.stop_all().await;
    assert!(alive && bytes > 0);
}

#[cfg(unix)]
#[tokio::test]
async fn output_stalling_after_progress_has_a_distinct_sanitized_reason() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let script = d.path().join("owned-progress-worker");
    std::fs::write(&script,"#!/usr/bin/python3\nimport os,time\nos.write(1,bytes.fromhex('471fff10')+bytes([255])*184)\ntime.sleep(30)\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), script.to_str().unwrap());
    let w = e
        .ensure(
            "owned",
            &json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":1}),
        )
        .await
        .unwrap();
    ended(&w).await;
    let stats = w.stats();
    e.stop_all().await;
    assert_eq!(stats["last_error"], "input_stalled");
    assert!(stats["bytes_in"].as_u64().unwrap() > 0);
}
