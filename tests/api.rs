use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
fn options() -> Options {
    Options {
        admin_user: "admin".into(),
        admin_password: "secret".into(),
        view_user: Some("viewer".into()),
        view_password: Some("view-secret".into()),
        peer_key: "peer-secret-1234".into(),
        ..Default::default()
    }
}

async fn post_text_config(
    app: Arc<App>,
    body: Body,
    credentials: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/streamer/api/v3/config")
        .header("Content-Type", "text/plain");
    if let Some(credentials) = credentials {
        request = request.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode(credentials)),
        );
    }
    let response = router(app)
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn post_json_config(app: Arc<App>, patch: Value) -> (StatusCode, Value) {
    let response = router(app)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&patch).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn config_post_reports_nested_item_paths_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    for (patch, code, expected_path, detail) in [
        (
            json!({"streams":[{"name":"first","title":"ok"},{"name":"second","mystery":true}]}),
            "unknown_key",
            json!(["server_config", "streams", 1, "mystery"]),
            "streams option mystery is not implemented",
        ),
        (
            json!({"streams":[{"name":"first","mystery":1},{"name":"second","mystery":2}]}),
            "unknown_key",
            json!(["server_config", "streams", 0, "mystery"]),
            "streams option mystery is not implemented",
        ),
        (
            json!({"templates":[{"name":"base","inputs":{}}]}),
            "invalid_config",
            json!(["server_config", "templates", 0, "inputs"]),
            "inputs must be array",
        ),
        (
            json!({"streams":[{"name":"demo","static":"yes"}]}),
            "invalid_config",
            json!(["server_config", "streams", 0, "static"]),
            "static must be boolean",
        ),
        (
            json!({"streams":[{"name":"demo","template":42}]}),
            "invalid_config",
            json!(["server_config", "streams", 0, "template"]),
            "template must be a name string",
        ),
        (
            json!({"streams":[{"name":"demo","disabled":"no"}]}),
            "invalid_config",
            json!(["server_config", "streams", 0, "disabled"]),
            "disabled must be boolean",
        ),
        (
            json!({"streams":[{"name":"demo","mystery":true}],"templates":[{"name":"base","mystery":true}]}),
            "unknown_key",
            json!(["server_config", "streams", 0, "mystery"]),
            "streams option mystery is not implemented",
        ),
    ] {
        let (status, result) = post_json_config(app.clone(), patch.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(result["error"], code);
        assert_eq!(result["path"], expected_path);
        assert_eq!(result["detail"], detail);
        assert_eq!(result["config"], patch);
        assert_eq!(app.config.snapshot(), before);
        assert_eq!(app.config.revision(), revision);
        assert_eq!(app.media.count().await, 0);
        assert!(!path.exists());
    }
}

#[tokio::test]
async fn config_post_reports_nested_input_paths_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    for (patch, code, expected_path, detail) in [
        (
            json!({"streams":[{"name":"demo","inputs":[{"url":"testsrc://","extra":true}]}]}),
            "unknown_key",
            json!(["server_config", "streams", 0, "inputs", 0, "extra"]),
            "supported input fields: url, RTSP rtp=udp and flussonix_rtsp_startup, verified TLS flussonix_tls_ca, direct RTP/SRTP flussonix_rtp, FXLink flussonix_fxlink/token",
        ),
        (
            json!({"streams":[{"name":"demo","inputs":[{"url":"testsrc://"},{}]}]}),
            "invalid_config",
            json!(["server_config", "streams", 0, "inputs", 1, "url"]),
            "input url required",
        ),
        (
            json!({"templates":[{"name":"base","inputs":[{"url":42}]}]}),
            "invalid_config",
            json!(["server_config", "templates", 0, "inputs", 0, "url"]),
            "input url required",
        ),
        (
            json!({"streams":[{"name":"demo","inputs":[false]}]}),
            "invalid_config",
            json!(["server_config", "streams", 0, "inputs", 0]),
            "supported input fields: url, RTSP rtp=udp and flussonix_rtsp_startup, verified TLS flussonix_tls_ca, direct RTP/SRTP flussonix_rtp, FXLink flussonix_fxlink/token",
        ),
        (
            json!({"streams":[{"name":"demo","inputs":[{"url":"unsupported://host"}]}]}),
            "invalid_config",
            json!(["server_config", "streams", 0, "inputs", 0, "url"]),
            "unsupported input protocol: unsupported",
        ),
    ] {
        let (status, result) = post_json_config(app.clone(), patch.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(result["error"], code);
        assert_eq!(result["path"], expected_path);
        assert_eq!(result["detail"], detail);
        assert_eq!(result["config"], patch);
        assert_eq!(app.config.snapshot(), before);
        assert_eq!(app.config.revision(), revision);
        assert_eq!(app.media.count().await, 0);
        assert!(!path.exists());
    }
    let (status, result) = post_json_config(
        app.clone(),
        json!({"streams":[{"name":"demo","template":"absent"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["path"], json!([]));
    let (status, result) = post_json_config(app.clone(), json!({"streams":{}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["path"], json!(["server_config", "streams"]));
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert_eq!(app.media.count().await, 0);
    assert!(!path.exists());
}

#[tokio::test]
async fn text_config_semantic_error_keeps_existing_path_contract() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    let (status, result) = post_text_config(
        app.clone(),
        Body::from("stream demo { input unsupported://host; }"),
        Some("admin:secret"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(result["error"], "invalid_config");
    assert_eq!(result["detail"], "unsupported input protocol: unsupported");
    assert_eq!(result["path"], json!([]));
    assert_eq!(result["config"]["streams"][0]["name"], "demo");
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert!(!path.exists());
}
#[tokio::test]
async fn node_status_reports_the_running_server_version_to_management_roles() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        options(),
    )
    .unwrap();
    let r = router(app);
    let health = r
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let health: Value = serde_json::from_slice(
        &axum::body::to_bytes(health.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!health["version"].as_str().unwrap().is_empty());
    for endpoint in ["/flussonix/api/v1/node", "/streamer/api/v3/config/stats"] {
        for credentials in [None, Some("admin:secret"), Some("viewer:view-secret")] {
            let mut req = Request::builder().uri(endpoint);
            if let Some(credentials) = credentials {
                req = req.header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode(credentials)),
                );
            }
            let response = r
                .clone()
                .oneshot(req.body(Body::empty()).unwrap())
                .await
                .unwrap();
            if credentials.is_none() {
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            } else {
                assert_eq!(response.status(), StatusCode::OK);
                let status: Value = serde_json::from_slice(
                    &axum::body::to_bytes(response.into_body(), 100_000)
                        .await
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(status["version"], health["version"], "{endpoint}");
                assert_eq!(
                    status["management_role"],
                    if credentials == Some("viewer:view-secret") {
                        "view"
                    } else {
                        "edit"
                    },
                    "{endpoint}"
                );
            }
        }
    }
}
#[tokio::test]
async fn api_auth_nested_names_persistence_and_validate_only() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        options(),
    )
    .unwrap();
    let r = router(app.clone());
    let req = |method: &str, path: &str, body: Value, cred: Option<&str>| {
        let mut b = Request::builder()
            .method(method)
            .uri(path)
            .header("Content-Type", "application/json");
        if let Some(c) = cred {
            b = b.header("Authorization", format!("Basic {}", STANDARD.encode(c)))
        }
        b.body(Body::from(body.to_string())).unwrap()
    };
    assert_eq!(
        r.clone()
            .oneshot(req("GET", "/streamer/api/v3/streams", json!(null), None))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        r.clone()
            .oneshot(req(
                "PUT",
                "/streamer/api/v3/streams/region/news",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                Some("viewer:view-secret")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        r.clone()
            .oneshot(req(
                "PUT",
                "/streamer/api/v3/streams/region/news",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
                Some("admin:secret")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let before = app.config.snapshot();
    assert_eq!(
        r.clone()
            .oneshot(req(
                "POST",
                "/streamer/api/v3/config",
                json!({"streams":[]}),
                Some("admin:secret")
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(app.config.snapshot(), before);
    let response = r
        .oneshot(req(
            "GET",
            "/streamer/api/v3/streams/region/news",
            json!(null),
            Some("admin:secret"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let b = axum::body::to_bytes(response.into_body(), 100000)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&b).unwrap()["name"],
        "region/news"
    );
    app.media.stop_all().await;
}
#[tokio::test]
async fn text_config_accepts_streams_templates_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    let text = r#"
# migration draft
template base {
  title "Common \"Title\"";
  input testsrc://;
}

stream demo {
  template base;
  comment "a demo";
  input m4s://relay.example/live?token=a#tail;
  input "srt://relay.example:9000?streamid=alpha";
  static;
};
"#;
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Body::from(text))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let result: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["templates"][0]["name"], "base");
    assert_eq!(result["templates"][0]["title"], "Common \"Title\"");
    assert_eq!(result["templates"][0]["inputs"][0]["url"], "testsrc://");
    assert_eq!(result["streams"][0]["name"], "demo");
    assert_eq!(result["streams"][0]["template"], "base");
    assert_eq!(result["streams"][0]["comment"], "a demo");
    assert_eq!(
        result["streams"][0]["inputs"][0]["url"],
        "m4s://relay.example/live?token=a#tail"
    );
    assert_eq!(
        result["streams"][0]["inputs"][1]["url"],
        "srt://relay.example:9000?streamid=alpha"
    );
    assert_eq!(result["streams"][0]["static"], true);
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert!(!path.exists());
    app.media.stop_all().await;
}

#[tokio::test]
async fn text_config_reports_unknown_and_unsupported_commands_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    let (status, error) =
        post_text_config(app.clone(), Body::from("htp 80;"), Some("admin:secret")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "unknown_command");
    assert_eq!(error["line"], 1);
    assert_eq!(error["col"], 1);
    assert_eq!(error["config"], json!({}));

    let text = "template base {}\nstream demo {\n  mystery value;\n}";
    let (status, error) =
        post_text_config(app.clone(), Body::from(text), Some("admin:secret")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "unknown_command");
    assert_eq!(error["line"], 3);
    assert_eq!(error["col"], 3);
    assert_eq!(error["config"]["templates"][0]["name"], "base");
    assert!(error["config"]["streams"].is_null());

    for text in ["http 80;", "stream demo { transcoder vb=2048k; }"] {
        let (status, error) = post_text_config(
            app.clone(),
            Body::from(text.to_owned()),
            Some("admin:secret"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["error"], "invalid_config");
        assert!(error["detail"].as_str().unwrap().contains("use JSON"));
    }
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert!(!path.exists());
}

#[tokio::test]
async fn text_config_reports_actual_utf8_and_syntax_locations() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let mut invalid = b"stream a {\n  input ".to_vec();
    invalid.push(0xff);
    invalid.extend_from_slice(b";\n}");
    let (status, error) =
        post_text_config(app.clone(), Body::from(invalid), Some("admin:secret")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "invalid_config");
    assert_eq!(error["line"], 2);
    assert_eq!(error["col"], 9);
    assert_eq!(error["config"], json!({}));

    let text = "template base {}\nstream demo {\n  input \"unterminated";
    let (status, error) =
        post_text_config(app.clone(), Body::from(text), Some("admin:secret")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "invalid_config");
    assert_eq!(error["line"], 3);
    assert_eq!(error["col"], 9);
    assert_eq!(error["config"]["templates"][0]["name"], "base");

    let (status, error) = post_text_config(
        app.clone(),
        Body::from("template base {}\n\"unterminated"),
        Some("admin:secret"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "invalid_config");
    assert_eq!(error["config"]["templates"][0]["name"], "base");

    for (text, detail) in [
        ("stream demo {", "unterminated configuration block"),
        ("stream demo { input testsrc://", "expected ';' after input"),
    ] {
        let (status, error) =
            post_text_config(app.clone(), Body::from(text), Some("admin:secret")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["error"], "invalid_config");
        assert_eq!(error["line"], 1);
        assert_eq!(error["col"], text.chars().count() + 1);
        assert_eq!(error["detail"], detail);
        assert_eq!(error["config"], json!({}));
    }
    assert!(!path.exists());
}

#[tokio::test]
async fn text_config_preserves_quoted_comment_markers_and_bounds_blocks() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let text = "// first comment\nstream demo { input m4s://relay.example/live?x=1#tail; title \"not // a comment; }\"; } # last comment";
    let (status, result) =
        post_text_config(app.clone(), Body::from(text), Some("admin:secret")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        result["streams"][0]["inputs"][0]["url"],
        "m4s://relay.example/live?x=1#tail"
    );
    assert_eq!(result["streams"][0]["title"], "not // a comment; }");

    let excessive = (0..2100)
        .map(|n| format!("stream s{n} {{}}\n"))
        .collect::<String>();
    let (status, error) =
        post_text_config(app.clone(), Body::from(excessive), Some("admin:secret")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["error"], "invalid_config");
    assert!(error["detail"].as_str().unwrap().contains("too many"));
    assert!(error["line"].as_u64().unwrap() > 1);

    let (status, _) = post_text_config(
        app.clone(),
        Body::from("stream denied {}"),
        Some("viewer:view-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(app.config.snapshot(), before);
    assert!(!path.exists());
}

#[tokio::test]
async fn config_post_reports_json_syntax_location_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from("{\n\"streams\": nope\n}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let result: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["error"], "invalid_json");
    assert_eq!(result["line"], 2);
    assert!(result["col"].as_u64().is_some_and(|col| col > 0));
    assert_eq!(result["path"], json!([]));
    assert_eq!(result["config"], json!({}));
    assert!(
        result["detail"]
            .as_str()
            .is_some_and(|detail| !detail.is_empty())
    );
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert!(!path.exists());
}

#[tokio::test]
async fn config_post_never_reports_column_zero_at_newline() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        options(),
    )
    .unwrap();
    let response = router(app)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from("{\"streams\":\"\n\"}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let result: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["error"], "invalid_json");
    assert_eq!(result["line"], 2);
    assert_eq!(result["col"], 1);
}

#[tokio::test]
async fn config_post_reports_unsupported_root_option_path_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"unsupported_feature":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let result: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["error"], "unknown_key");
    assert_eq!(
        result["path"],
        json!(["server_config", "unsupported_feature"])
    );
    assert_eq!(result["config"]["unsupported_feature"], true);
    assert!(
        result["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("unsupported_feature"))
    );
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert!(!path.exists());
}

#[tokio::test]
async fn config_post_reports_wrong_collection_type_path_without_mutation() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let revision = app.config.revision();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"streams":{}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let result: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["error"], "invalid_config");
    assert_eq!(result["path"], json!(["server_config", "streams"]));
    assert_eq!(result["config"]["streams"], json!({}));
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(app.config.revision(), revision);
    assert!(!path.exists());
}

#[tokio::test]
async fn config_post_rejects_scalar_json_with_object_shaped_diagnostic() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let app = App::new(&path, d.path().join("media"), options()).unwrap();
    let before = app.config.snapshot();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/streamer/api/v3/config")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from("null"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let result: Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["error"], "invalid_config");
    assert_eq!(result["config"], json!({}));
    assert_eq!(result["path"], json!([]));
    assert_eq!(app.config.snapshot(), before);
    assert!(!path.exists());
}
#[tokio::test]
async fn media_auth_precedes_worker_start_including_segments() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        options(),
    )
    .unwrap();
    app.config.put("streams","private",json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":"http://127.0.0.1:1/auth"})).unwrap();
    let r = router(app.clone());
    for path in [
        "/private/index.m3u8",
        "/private/index0.ts",
        "/private/fmp4/init.mp4",
        "/private/mpegts",
    ] {
        let res = r
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }
    assert_eq!(app.media.count().await, 0);
}
#[tokio::test]
async fn metadata_edit_does_not_restart_unrelated_stream_workers() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(d.path().join("c.json"), d.path().join("media"), options()).unwrap();
    for name in ["first", "second"] {
        app.config
            .put("streams", name, json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
    }
    app.reconcile().await;
    let first = app
        .media
        .ensure("first", &app.config.effective("first").unwrap())
        .await
        .unwrap();
    let r = router(app.clone());
    let res = r
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/streamer/api/v3/streams/second")
                .header(
                    "Authorization",
                    format!("Basic {}", STANDARD.encode("admin:secret")),
                )
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"title":"Updated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let after = app
        .media
        .ensure("first", &app.config.effective("first").unwrap())
        .await
        .unwrap();
    assert_eq!(first.pid(), after.pid());
    app.media.stop_all().await;
}

#[tokio::test]
async fn media_redirects_and_errors_allow_cross_origin_players_without_cookie_credentials() {
    let d = tempfile::tempdir().unwrap();
    let app = App::new(d.path().join("c.json"), d.path().join("media"), options()).unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/missing/index.m3u8")
                .header("origin", "https://player.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|h| h.to_str().ok()),
        Some("*")
    );
    assert!(
        response
            .headers()
            .get("access-control-allow-credentials")
            .is_none()
    );
    let preflight = router(app)
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/owned/index.m3u8")
                .header("origin", "https://player.example")
                .header("access-control-request-headers", "authorization,range")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
}
