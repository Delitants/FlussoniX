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

#[tokio::test]
async fn on_demand_reconcile_recovers_without_a_new_playback_request_or_demand_touch() {
    use flussonix::server::{App, Options};
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin-secret".into(),
            peer_key: "owned-peer-secret".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    let cfg = app.config.effective("owned").unwrap();
    let first = app.media.ensure("owned", &cfg).await.unwrap();
    let mut rx = first.subscribe();
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        std::process::Command::new("kill")
            .args(["-KILL", &first.pid().to_string()])
            .status()
            .unwrap()
            .success()
    );
    ended(&first).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let age = first.idle_seconds();
    app.reconcile().await;
    let stats = app.media.stats("owned").await;
    assert_ne!(
        stats["pid"],
        first.pid(),
        "background reconciliation did not replace the failed attempt"
    );
    let second = app.media.recover("owned", &cfg).await.unwrap();
    let preserved = second.idle_seconds();
    app.media.stop_all().await;
    assert!(
        age >= 2 && preserved >= age,
        "background retry extended actual viewer demand"
    );
}

async fn playlist(e: &Engine, file: &str) -> String {
    for _ in 0..200 {
        if let Ok(bytes) = e.read("owned", file).await {
            let s = String::from_utf8_lossy(&bytes).into_owned();
            if s.contains("#EXTINF") {
                return s;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("owned HLS playlist did not become ready");
}
fn sequence(p: &str) -> u64 {
    p.lines()
        .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
        .unwrap()
        .parse()
        .unwrap()
}
fn media_names(p: &str) -> Vec<&str> {
    p.lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}
fn init_name(p: &str) -> &str {
    p.lines()
        .find_map(|l| {
            l.split_once("#EXT-X-MAP:URI=\"")
                .map(|(_, v)| v.split('"').next().unwrap())
        })
        .unwrap()
}
#[tokio::test]
async fn failed_worker_cannot_serve_a_stale_hls_manifest_as_ready_media() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"testsrc://"}]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    playlist(&e, "index.m3u8").await;
    assert!(
        std::process::Command::new("kill")
            .args(["-KILL", &w.pid().to_string()])
            .status()
            .unwrap()
            .success()
    );
    ended(&w).await;
    let stale = e.read("owned", "index.m3u8").await.is_ok();
    e.stop_all().await;
    assert!(
        !stale,
        "dead worker delivered a fresh-looking stale manifest"
    );
}
#[tokio::test]
async fn hls_replacement_has_new_sequences_media_and_init_identity_and_decodes() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"testsrc://"}]});
    let first = e.ensure("owned", &cfg).await.unwrap();
    let ts = playlist(&e, "index.m3u8").await;
    let old_fmp4 = playlist(&e, "fmp4/index.m3u8").await;
    let old_last = sequence(&ts) + media_names(&ts).len() as u64 - 1;
    assert!(
        std::process::Command::new("kill")
            .args(["-KILL", &first.pid().to_string()])
            .status()
            .unwrap()
            .success()
    );
    ended(&first).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    e.recover("owned", &cfg).await.unwrap();
    let new_ts = playlist(&e, "index.m3u8").await;
    let new_fmp4 = playlist(&e, "fmp4/index.m3u8").await;
    assert!(
        sequence(&new_ts) > old_last,
        "HLS replacement sequence moved backward or reused a segment number"
    );
    assert!(new_ts.contains("#EXT-X-DISCONTINUITY\n"));
    assert!(
        !media_names(&new_ts)
            .iter()
            .any(|name| media_names(&ts).contains(name))
    );
    assert_ne!(
        init_name(&new_fmp4),
        init_name(&old_fmp4),
        "init URI identified different generations"
    );
    let init = e
        .read("owned", &format!("fmp4/{}", init_name(&new_fmp4)))
        .await
        .unwrap();
    let fragment = e
        .read("owned", &format!("fmp4/{}", media_names(&new_fmp4)[0]))
        .await
        .unwrap();
    let file = d.path().join("new-attempt.mp4");
    std::fs::write(&file, [init.as_ref(), fragment.as_ref()].concat()).unwrap();
    let decode = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&file)
        .args(["-t", "1", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    e.stop_all().await;
    assert!(
        decode.status.success(),
        "new generation failed actual decode: {}",
        String::from_utf8_lossy(&decode.stderr)
    );
}
