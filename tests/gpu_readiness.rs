use axum::{body::Body, http::Request};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc, time::Duration};
use tower::ServiceExt;

fn app(dir: &Path, ffmpeg: &str) -> Arc<App> {
    App::new(
        dir.join("config.json"),
        dir.join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "owned".into(),
            peer_key: "owned-peer-key".into(),
            ffmpeg: ffmpeg.into(),
            ..Default::default()
        },
    )
    .unwrap()
}
async fn capabilities(app: Arc<App>, auth: bool) -> (u16, Value) {
    let mut req = Request::builder().uri("/flussonix/api/v1/capabilities");
    if auth {
        req = req.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("admin:owned")),
        );
    }
    let response = router(app)
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
// Fault-injection executables test process supervision only, never media qualification.
fn wrapper(dir: &Path, gpu_action: &str) -> String {
    let script = dir.join("ffmpeg-owned");
    let log = dir.join("probes");
    let pid = dir.join("probe-pid");
    let text = format!(
        "#!/bin/sh\ncase \" $* \" in\n*nvenc*)\nprintf 'probe\\n' >> '{}'\nprintf '%s' \"$$\" > '{}'\n{}\n;;\nesac\nexec /usr/bin/ffmpeg \"$@\"\n",
        log.display(),
        pid.display(),
        gpu_action
    );
    std::fs::write(&script, text).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    script.to_str().unwrap().into()
}
fn probe_count(dir: &Path) -> usize {
    std::fs::read_to_string(dir.join("probes"))
        .unwrap_or_default()
        .lines()
        .count()
}
async fn wait_probe(dir: &Path) -> u32 {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(s) = std::fs::read_to_string(dir.join("probe-pid")) {
                if let Ok(pid) = s.parse() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
async fn reaped(pid: u32) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned GPU probe must be killed and reaped");
}

#[tokio::test]
async fn capabilities_are_authenticated_sanitized_and_coalesced() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(
        d.path(),
        "echo 'private driver diagnostic token=secret' >&2; exit 1",
    );
    let a = app(d.path(), &exe);
    assert_eq!(capabilities(a.clone(), false).await.0, 401);
    assert_eq!(probe_count(d.path()), 0);
    let mut tasks = vec![];
    for _ in 0..12 {
        let a = a.clone();
        tasks.push(tokio::spawn(async move { capabilities(a, true).await }));
    }
    for task in tasks {
        let (status, body) = task.await.unwrap();
        assert_eq!(status, 200);
        let profiles = body["transcoding"]["gpu_profiles"]
            .as_array()
            .expect("GPU readiness profiles");
        assert_eq!(profiles.len(), 2);
        assert_eq!(
            profiles[0],
            json!({"encoder":"h264_nvenc","codec":"H.264","status":"unavailable"})
        );
        assert_eq!(
            profiles[1],
            json!({"encoder":"hevc_nvenc","codec":"HEVC / H.265","status":"unavailable"})
        );
        assert!(!body.to_string().contains("private driver"));
        assert!(!body.to_string().contains(d.path().to_str().unwrap()));
    }
    assert_eq!(probe_count(d.path()), 2, "one probe per encoder per daemon");
    capabilities(a, true).await;
    assert_eq!(probe_count(d.path()), 2);
}

#[tokio::test]
async fn unavailable_gpu_does_not_replace_running_cpu_worker() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(d.path(), "exit 1");
    let a = app(d.path(), &exe);
    let cpu = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264"}});
    let worker = a.media.ensure("owned", &cpu).await.unwrap();
    let pid = worker.pid();
    let gpu = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"h264_nvenc"}});
    let result = a.media.ensure("owned", &gpu).await;
    let error = result
        .err()
        .expect("unavailable GPU must fail before replacing CPU worker");
    assert_eq!(error, "NVIDIA H.264 encoder is unavailable on this host");
    assert!(!worker.is_closed());
    assert_eq!(a.media.ensure("owned", &cpu).await.unwrap().pid(), pid);
    a.media.stop_all().await;
    reaped(pid).await;
}

#[tokio::test]
async fn bounded_probes_do_not_block_cpu_and_are_reaped_on_timeout() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(d.path(), "exec sleep 60");
    let a = app(d.path(), &exe);
    let task = {
        let a = a.clone();
        tokio::spawn(async move { capabilities(a, true).await })
    };
    let first = wait_probe(d.path()).await;
    let cfg = json!({"inputs":[{"url":"testsrc://"}]});
    let worker = tokio::time::timeout(Duration::from_secs(2), a.media.ensure("cpu", &cfg))
        .await
        .expect("GPU checks must not hold media worker lock")
        .unwrap();
    let (status, body) = tokio::time::timeout(Duration::from_secs(12), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, 200);
    for profile in body["transcoding"]["gpu_profiles"].as_array().unwrap() {
        assert_eq!(profile["status"], "timed_out");
    }
    reaped(first).await;
    reaped(wait_probe(d.path()).await).await;
    assert_eq!(probe_count(d.path()), 2);
    assert!(!worker.is_closed());
    a.media.stop_all().await;
}

#[tokio::test]
async fn cancelled_probe_is_reaped_and_later_check_can_retry() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(d.path(), "exec sleep 60");
    let a = app(d.path(), &exe);
    let task = {
        let a = a.clone();
        tokio::spawn(async move { capabilities(a, true).await })
    };
    let pid = wait_probe(d.path()).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    reaped(pid).await;
    // Replace the owned executable after cancellation; OnceCell must not cache incomplete work.
    wrapper(d.path(), "exit 1");
    let (_, body) = capabilities(a, true).await;
    assert_eq!(
        body["transcoding"]["gpu_profiles"][0]["status"],
        "unavailable"
    );
    assert_eq!(probe_count(d.path()), 3);
}

#[tokio::test]
async fn missing_executable_is_safe_and_real_ffmpeg_readiness_is_bounded() {
    let d = tempfile::tempdir().unwrap();
    let missing = d.path().join("private-executable-does-not-exist");
    let a = app(d.path(), missing.to_str().unwrap());
    let (_, body) = capabilities(a, true).await;
    for profile in body["transcoding"]["gpu_profiles"].as_array().unwrap() {
        assert_eq!(profile["status"], "probe_failed");
    }
    assert!(!body.to_string().contains("private-executable"));
    let a = app(d.path(), "/usr/bin/ffmpeg");
    let (_, body) = tokio::time::timeout(Duration::from_secs(12), capabilities(a, true))
        .await
        .unwrap();
    for profile in body["transcoding"]["gpu_profiles"].as_array().unwrap() {
        assert!(
            ["available", "unavailable", "timed_out"]
                .contains(&profile["status"].as_str().unwrap())
        );
    }
}

#[tokio::test]
async fn playback_returns_only_safe_encoder_specific_readiness_errors() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(d.path(), "echo 'token=private' >&2; exit 1");
    let a = app(d.path(), &exe);
    a.config.put("streams", "gpu", json!({"static":false,"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"hevc_nvenc"}})).unwrap();
    let response = router(a.clone())
        .oneshot(
            Request::builder()
                .uri("/gpu/mpegts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 503);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body["errors"][0]["message"],
        "NVIDIA HEVC encoder is unavailable on this host"
    );
    assert!(!body.to_string().contains("private"));
    assert!(!body.to_string().contains(d.path().to_str().unwrap()));
    assert!(
        !d.path().join("media").exists(),
        "probe failure precedes media directory/source setup"
    );
    a.media.stop_all().await;
}

#[tokio::test]
async fn stale_gpu_start_cannot_replace_worker_and_hevc_gpu_captions_reject() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(d.path(), "exit 0");
    let a = app(d.path(), &exe);
    let cpu = json!({"inputs":[{"url":"testsrc://"}]});
    let worker = a.media.ensure("owned", &cpu).await.unwrap();
    let gpu = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"hevc_nvenc"}});
    let error = a
        .media
        .ensure_guarded("owned", &gpu, true, std::future::ready(false))
        .await
        .err()
        .unwrap();
    assert_eq!(error, "media route changed");
    assert!(!worker.is_closed());
    let caption = json!({"inputs":[{"url":"http://127.0.0.1:9/source.ts"}],"transcoder":{"encoder":"hevc_nvenc"},"flussonix_hls_subtitles":"convert","flussonix_hls_captions":[{"channel":1,"language":"en","name":"English"}]});
    let error = a.media.ensure("caption", &caption).await.err().unwrap();
    assert!(error.contains("GPU conversion is not qualified"), "{error}");
    a.media.stop_all().await;
}

#[tokio::test]
async fn missing_nvidia_driver_has_safe_dependency_diagnostic() {
    let d = tempfile::tempdir().unwrap();
    let exe = wrapper(
        d.path(),
        "echo 'Cannot load libcuda.so.1 /private token=secret' >&2; exit 1",
    );
    let (_, body) = capabilities(app(d.path(), &exe), true).await;
    for p in body["transcoding"]["gpu_profiles"].as_array().unwrap() {
        assert_eq!(p["status"], "unavailable");
        assert_eq!(p["diagnostic"], "nvidia_driver_unavailable");
    }
    assert!(!body.to_string().contains("token=secret"));
}
