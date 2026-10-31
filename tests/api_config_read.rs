use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

fn app(state: &Path) -> Arc<App> {
    App::new(
        state.join("config.json"),
        state.join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "test-secret".into(),
            peer_key: "test-peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap()
}

async fn get(app: Arc<App>, path: &str) -> (StatusCode, Value) {
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri(path)
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:test-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn config_openmetrics_negotiates_without_config_leak() {
    let state = tempfile::tempdir().unwrap();
    let app = App::new(
        state.path().join("config.json"),
        state.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "test-secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("view-secret".into()),
            peer_key: "test-peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .replace(
            json!({"streams":[{"name":"saved-secret-marker","inputs":[{"url":"testsrc://"}]}]}),
        )
        .unwrap();
    let id = get(app.clone(), "/streamer/api/v3/config").await.1["server_id"]
        .as_str()
        .unwrap()
        .to_owned();

    for (path, accept, credentials) in [
        (
            "/streamer/api/v3/config",
            "application/openmetrics-text; version=1.0.0, application/json",
            "admin:test-secret",
        ),
        (
            "/streamer/api/v3/config?full=true",
            "application/openmetrics-text",
            "viewer:view-secret",
        ),
        (
            "/streamer/api/v3/config?format=openmetrics",
            "application/json",
            "admin:test-secret",
        ),
    ] {
        let response = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("Accept", accept)
                    .header(
                        "Authorization",
                        format!("Basic {}", STANDARD.encode(credentials)),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            response.headers()["content-type"],
            "application/openmetrics-text",
            "{path}"
        );
        let body = String::from_utf8(
            axum::body::to_bytes(response.into_body(), 100_000)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(
            body.contains("# TYPE server_total_clients gauge\n"),
            "{path}"
        );
        assert!(
            body.contains(&format!("server_total_clients{{server_id=\"{id}\"}} 0\n")),
            "{path}"
        );
        assert!(body.ends_with("# EOF\n"), "{path}");
        assert!(!body.contains("saved-secret-marker"), "{path}");
        assert!(!body.contains("testsrc://"), "{path}");
    }

    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config")
                .header("Accept", "application/json, application/openmetrics-text")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:test-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["server_id"], id);

    // The installed reference uses only the first Accept field line. A later
    // OpenMetrics field must not override an unrecognized first field.
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config")
                .header("Accept", "application/xml")
                .header("Accept", "application/openmetrics-text")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:test-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");

    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config")
                .header(
                    "Accept",
                    "application/openmetrics-text;q=0, application/json",
                )
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:test-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.headers()["content-type"],
        "application/openmetrics-text"
    );

    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config?format=unknown")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:test-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config")
                .header("Accept", "application/openmetrics-text")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config/stats")
                .header("Accept", "application/openmetrics-text")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:test-secret")),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(body["id"], id);
}

#[tokio::test]
async fn config_stats_started_at_is_stable_and_online_count_tracks_ready_worker() {
    use futures_util::FutureExt;

    let state = tempfile::tempdir().unwrap();
    let app = app(state.path());
    let result = std::panic::AssertUnwindSafe(async {
        let before = get(app.clone(), "/streamer/api/v3/config/stats").await.1;
        let started = before["started_at"].as_u64().expect("startup seconds");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(started <= now && now - started < 10);
        assert_eq!(before["online_streams"], 0);

        app.media
            .ensure("music", &json!({"inputs":[{"url":"testsrc://"}]}))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !app.media.ready("music").await {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("owned test source became ready");

        let online = get(app.clone(), "/streamer/api/v3/config/stats").await.1;
        assert_eq!(online["started_at"], started);
        assert_eq!(online["online_streams"], 1);
        app.media.stop("music").await;
        let stopped = get(app.clone(), "/streamer/api/v3/config/stats").await.1;
        assert_eq!(stopped["online_streams"], 0);
    })
    .catch_unwind()
    .await;
    app.media.stop_all().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn config_default_is_compact_and_full_is_explicit() {
    let state = tempfile::tempdir().unwrap();
    let app = app(state.path());
    app.config
        .replace(
            json!({"streams":[{"name":"music","static":false,"inputs":[{"url":"testsrc://"}]}]}),
        )
        .unwrap();

    let (status, compact) = get(app.clone(), "/streamer/api/v3/config").await;
    assert_eq!(status, StatusCode::OK);
    let id = compact["server_id"].as_str().unwrap();
    uuid::Uuid::parse_str(id).unwrap();
    assert_eq!(compact["stats"]["id"], id);
    assert_eq!(compact["stats"]["streamer_status"], "running");
    assert!(compact.get("streams").is_none());

    let (status, full) = get(app.clone(), "/streamer/api/v3/config?full=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(full["server_id"], id);
    assert_eq!(full["stats"]["id"], id);
    assert_eq!(full["streams"][0]["name"], "music");

    let (status, explicit_false) = get(app.clone(), "/streamer/api/v3/config?full=false").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(explicit_false["server_id"], id);
    assert!(explicit_false.get("streams").is_none());

    let (status, _) = get(app, "/streamer/api/v3/config?full=maybe").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn config_stats_is_top_level_and_shared_between_roles() {
    let state = tempfile::tempdir().unwrap();
    let app = App::new(
        state.path().join("config.json"),
        state.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "test-secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("view-secret".into()),
            peer_key: "test-peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .replace(json!({"streams":[{"name":"music","inputs":[{"url":"testsrc://"}]}]}))
        .unwrap();
    let id = get(app.clone(), "/streamer/api/v3/config").await.1["server_id"]
        .as_str()
        .unwrap()
        .to_owned();

    for credentials in ["admin:test-secret", "viewer:view-secret"] {
        let response = router(app.clone())
            .oneshot(
                Request::builder()
                    .uri("/streamer/api/v3/config/stats")
                    .header(
                        "Authorization",
                        format!("Basic {}", STANDARD.encode(credentials)),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 100_000)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["id"], id);
        assert_eq!(body["server_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["streamer_status"], "running");
        assert_eq!(body["total_streams"], 1);
        assert!(body["now"].is_u64());
        for native_field in ["stats", "management_role", "iptv_public_origin", "streams"] {
            assert!(
                body.get(native_field).is_none(),
                "unexpected {native_field}"
            );
        }
    }

    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/streamer/api/v3/config/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn config_server_identity_survives_restart() {
    let state = tempfile::tempdir().unwrap();
    let first = get(app(state.path()), "/streamer/api/v3/config").await.1["server_id"].clone();
    let second = get(app(state.path()), "/streamer/api/v3/config").await.1["server_id"].clone();
    assert_eq!(first, second);
    assert!(state.path().join("config.json.node-id").exists());
}

#[tokio::test]
async fn config_rejects_corrupt_or_unsafe_identity_file() {
    let state = tempfile::tempdir().unwrap();
    let identity = state.path().join("config.json.node-id");
    std::fs::write(&identity, "not-a-uuid\n").unwrap();
    assert!(
        App::new(
            state.path().join("config.json"),
            state.path().join("media"),
            Options {
                admin_user: "admin".into(),
                admin_password: "test-secret".into(),
                peer_key: "test-peer-secret-1234".into(),
                ..Default::default()
            }
        )
        .is_err()
    );

    std::fs::remove_file(&identity).unwrap();
    let outside = state.path().join("outside");
    std::fs::write(&outside, uuid::Uuid::new_v4().to_string()).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&outside, &identity).unwrap();
        assert!(
            App::new(
                state.path().join("config.json"),
                state.path().join("media"),
                Options {
                    admin_user: "admin".into(),
                    admin_password: "test-secret".into(),
                    peer_key: "test-peer-secret-1234".into(),
                    ..Default::default()
                }
            )
            .is_err()
        );
        std::fs::remove_file(&identity).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(&identity, uuid::Uuid::new_v4().to_string()).unwrap();
        std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            App::new(
                state.path().join("config.json"),
                state.path().join("media"),
                Options {
                    admin_user: "admin".into(),
                    admin_password: "test-secret".into(),
                    peer_key: "test-peer-secret-1234".into(),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn concurrent_startups_share_one_identity_without_temp_files() {
    let state = tempfile::tempdir().unwrap();
    let mut starts = Vec::new();
    for _ in 0..8 {
        let path = state.path().to_owned();
        starts.push(tokio::task::spawn_blocking(move || app(&path)));
    }
    let mut ids = Vec::new();
    for started in starts {
        let instance = started.await.unwrap();
        ids.push(get(instance, "/streamer/api/v3/config").await.1["server_id"].clone());
    }
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(
        std::fs::read_dir(state.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("node-id"))
            .count(),
        1
    );
}

#[test]
fn default_relative_config_path_creates_persistent_identity() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let state = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_flussonix"))
        .current_dir(state.path())
        .args(["--listen", "127.0.0.1:0", "--uplink-interface", "process"])
        .env("FLUSSONIX_ADMIN_PASSWORD", "test-secret")
        .env("FLUSSONIX_PEER_KEY", "test-peer-secret-1234")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let identity = state.path().join("config.json.node-id");
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut ready = false;
    while Instant::now() < deadline {
        if identity.is_file() && child.try_wait().unwrap().is_none() {
            ready = true;
            break;
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // Reap the daemon even when startup times out, so the test cannot hang.
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    assert!(
        ready,
        "default relative config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
