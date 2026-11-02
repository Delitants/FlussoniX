use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tower::ServiceExt;

fn app(state: &Path) -> Arc<App> {
    App::new(
        state.join("config.json"),
        state.join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "owned-edit-secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("owned-view-secret".into()),
            peer_key: "owned-peer-secret-1234".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap()
}

async fn call(
    app: Arc<App>,
    method: Method,
    path: &str,
    credentials: Option<&str>,
    origin: Option<&str>,
    body: &str,
) -> (StatusCode, Vec<u8>) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(credentials) = credentials {
        request = request.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode(credentials)),
        );
    }
    if let Some(origin) = origin {
        request = request.header("Origin", origin);
    }
    let response = router(app)
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn stream(app: Arc<App>, name: &str) -> Value {
    let (status, bytes) = call(
        app,
        Method::GET,
        &format!("/streamer/api/v3/streams/{name}"),
        Some("admin:owned-edit-secret"),
        None,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&bytes).unwrap()
}

fn select(name: &str, index: &str) -> String {
    format!("/streamer/api/v3/streams/{name}/inputs/{index}/select")
}

#[tokio::test]
async fn bodyless_selection_of_an_idle_on_demand_stream_returns_empty_204_without_saving() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]}),
        )
        .unwrap();
    let before = std::fs::read(dir.path().join("config.json")).unwrap();
    let (status, body) = call(
        app.clone(),
        Method::POST,
        &select("owned", "1"),
        Some("admin:owned-edit-secret"),
        None,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    assert_eq!(
        std::fs::read(dir.path().join("config.json")).unwrap(),
        before
    );
    assert_eq!(app.media.count().await, 0);
    assert_eq!(
        stream(app.clone(), "owned").await["stats"]["preferred_input_index"],
        1
    );
    app.media.stop_all().await;
}

#[tokio::test]
async fn selection_indexes_the_effective_template_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    app.config
        .put(
            "templates",
            "base",
            json!({"static":false,"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]}),
        )
        .unwrap();
    app.config
        .put("streams", "owned", json!({"template":"base"}))
        .unwrap();
    let (status, body) = call(
        app.clone(),
        Method::POST,
        &select("owned", "1"),
        Some("admin:owned-edit-secret"),
        None,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    assert_eq!(
        stream(app.clone(), "owned").await["stats"]["preferred_input_index"],
        1
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn invalid_missing_disabled_and_publisher_inputs_have_exact_error_statuses() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    app.config
        .put(
            "streams",
            "off",
            json!({"disabled":true,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    app.config
        .put(
            "streams",
            "publish",
            json!({"inputs":[{"url":"publish://"}]}),
        )
        .unwrap();
    for (path, body, expected) in [
        (select("absent", "0"), "", StatusCode::NOT_FOUND),
        (select("owned", "1"), "", StatusCode::BAD_REQUEST),
        (select("owned", "-1"), "", StatusCode::BAD_REQUEST),
        (select("owned", "nope"), "", StatusCode::BAD_REQUEST),
        (select("owned", "0/extra"), "", StatusCode::BAD_REQUEST),
        (select("owned", "0"), "{}", StatusCode::BAD_REQUEST),
        (select("off", "0"), "", StatusCode::BAD_REQUEST),
        (select("publish", "0"), "", StatusCode::BAD_REQUEST),
    ] {
        let (status, _) = call(
            app.clone(),
            Method::POST,
            &path,
            Some("admin:owned-edit-secret"),
            None,
            body,
        )
        .await;
        assert_eq!(status, expected, "{path}");
    }
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn selection_requires_edit_credentials_and_same_origin_browser_write() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
        )
        .unwrap();
    for (credentials, origin, expected) in [
        (None, None, StatusCode::UNAUTHORIZED),
        (
            Some("viewer:owned-view-secret"),
            None,
            StatusCode::FORBIDDEN,
        ),
        (
            Some("admin:owned-edit-secret"),
            Some("https://evil.example"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let (status, _) = call(
            app.clone(),
            Method::POST,
            &select("owned", "0"),
            credentials,
            origin,
            "",
        )
        .await;
        assert_eq!(status, expected);
    }
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn active_and_static_selection_restarts_once_on_the_requested_input() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":true,"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]}),
        )
        .unwrap();
    let config = app.config.effective("owned").unwrap();
    let first = app.media.ensure("owned", &config).await.unwrap();
    let first_pid = first.pid();
    let (status, _) = call(
        app.clone(),
        Method::POST,
        &select("owned", "1"),
        Some("admin:owned-edit-secret"),
        None,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let second = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = app.media.stats("owned").await;
            if stats["input_index"] == 1 && stats["pid"].as_u64().unwrap_or(0) > 0 {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_ne!(second["pid"], first_pid);
    assert_eq!(app.media.count().await, 1);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        &select("owned", "1"),
        Some("admin:owned-edit-secret"),
        None,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(app.media.stats("owned").await["pid"], second["pid"]);
    app.media.stop_all().await;
}

#[tokio::test]
async fn input_list_edit_clears_the_preference_and_stale_restart_cannot_use_it() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    app.config
        .put(
            "streams",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]}),
        )
        .unwrap();
    let (status, _) = call(
        app.clone(),
        Method::POST,
        &select("owned", "1"),
        Some("admin:owned-edit-secret"),
        None,
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(
        app.clone(),
        Method::PUT,
        "/streamer/api/v3/streams/owned",
        Some("admin:owned-edit-secret"),
        None,
        r#"{"inputs":[{"url":"testsrc://"}]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let current = app.config.effective("owned").unwrap();
    assert_eq!(app.media.preferred_input("owned", &current).await, None);
    assert_eq!(
        stream(app.clone(), "owned").await["stats"]["preferred_input_index"],
        Value::Null
    );
    let worker = app.media.ensure("owned", &current).await.unwrap();
    assert_eq!(worker.stats()["input_index"], 0);
    app.media.stop_all().await;
}

#[tokio::test]
async fn concurrent_edit_and_selection_leave_only_the_current_input_list_eligible() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path());
    for round in 0..16 {
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"},{"url":"testsrc://"}]}),
            )
            .unwrap();
        let path = select("owned", "1");
        let (selection, edit) = tokio::join!(
            call(
                app.clone(),
                Method::POST,
                &path,
                Some("admin:owned-edit-secret"),
                None,
                ""
            ),
            call(
                app.clone(),
                Method::PUT,
                "/streamer/api/v3/streams/owned",
                Some("admin:owned-edit-secret"),
                None,
                r#"{"static":false,"inputs":[{"url":"testsrc://"}]}"#
            )
        );
        assert!(
            matches!(
                selection.0,
                StatusCode::NO_CONTENT | StatusCode::BAD_REQUEST
            ),
            "round {round}: {}",
            selection.0
        );
        assert_eq!(edit.0, StatusCode::OK, "round {round}");
        let current = app.config.effective("owned").unwrap();
        assert_eq!(
            app.media.preferred_input("owned", &current).await,
            None,
            "round {round}"
        );
        assert_eq!(app.media.count().await, 0, "round {round}");
    }
    let current = app.config.effective("owned").unwrap();
    assert_eq!(
        app.media.ensure("owned", &current).await.unwrap().stats()["input_index"],
        0
    );
    app.media.stop_all().await;
}
