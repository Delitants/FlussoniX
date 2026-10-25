use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

async fn get(app: Arc<App>, path: &str, credentials: &str) -> Value {
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri(path)
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
    let body = axum::body::to_bytes(response.into_body(), 100_000)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn view_management_reads_hide_inherited_and_nested_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let options = Options {
        admin_user: "admin".into(),
        admin_password: "edit-password".into(),
        view_user: Some("viewer".into()),
        view_password: Some("view-password".into()),
        peer_key: "node-peer-secret".into(),
        ..Default::default()
    };
    let app = App::new(
        directory.path().join("config.json"),
        directory.path().join("media"),
        options,
    )
    .unwrap();
    app.config
        .replace(json!({
            "templates": [{
                "name": "base", "title": "Music", "static": false,
                "inputs": [{"url": "rtsp://input-user:input-secret@origin.example/live?token=input-query-secret"}],
                "flussonix_peer_key": "template-peer-secret",
                "on_play": {"url": "https://auth.example/check?token=play-callback-secret"},
                "on_publish": "https://auth.example/publish?token=publish-callback-secret"
            }],
            "streams": [
                {
                    "name": "music", "template": "base", "password": "stream-publish-secret",
                    "pushes": [{"url": "srt://127.0.0.1:9933", "passphrase": "push-passphrase-secret", "streamid": "push-streamid-secret"}]
                },
                {"name": "news", "template": "base", "password": "second-publish-secret"}
            ],
            "peers": [{
                "hostname": "edge", "api_url": "https://edge.example/api?token=peer-url-secret",
                "cluster_key": "peer-cluster-secret"
            }],
            "sources": [{
                "hostname": "origin", "api_url": "https://source.example/api?token=source-url-secret",
                "cluster_key": "source-cluster-secret"
            }],
            "auth_backends": [{"name": "users", "url": "https://auth.example/users?token=backend-url-secret"}]
        }))
        .unwrap();

    let secrets = [
        "input-secret",
        "input-query-secret",
        "template-peer-secret",
        "play-callback-secret",
        "publish-callback-secret",
        "stream-publish-secret",
        "second-publish-secret",
        "push-passphrase-secret",
        "push-streamid-secret",
        "peer-url-secret",
        "peer-cluster-secret",
        "source-url-secret",
        "source-cluster-secret",
        "backend-url-secret",
    ];
    let edit = get(
        app.clone(),
        "/streamer/api/v3/config",
        "admin:edit-password",
    )
    .await;
    for secret in secrets {
        assert!(
            edit.to_string().contains(secret),
            "Edit readback lost {secret}"
        );
    }

    for path in [
        "/streamer/api/v3/config",
        "/streamer/api/v3/streams/music",
        "/streamer/api/v3/streams",
        "/streamer/api/v3/streams?select=name,password,config_on_disk.password,inputs.url,pushes.passphrase",
        "/streamer/api/v3/templates/base",
        "/streamer/api/v3/templates",
        "/streamer/api/v3/peers/edge",
        "/streamer/api/v3/cluster/peers",
        "/streamer/api/v3/sources/origin",
        "/streamer/api/v3/cluster/sources",
        "/streamer/api/v3/auth_backends/users",
        "/streamer/api/v3/auth_backends",
        "/streamer/api/v3/config/stats",
        "/flussonix/api/v1/node",
    ] {
        let result = get(app.clone(), path, "viewer:view-password").await;
        let serialized = result.to_string();
        for secret in secrets {
            assert!(
                !serialized.contains(secret),
                "View leaked {secret} at {path}"
            );
        }
    }

    let stream = get(
        app.clone(),
        "/streamer/api/v3/streams/music",
        "viewer:view-password",
    )
    .await;
    assert_eq!(stream["name"], "music");
    assert_eq!(stream["title"], "Music");
    assert_eq!(stream["stats"]["status"], "waiting");
    assert_eq!(stream["inputs"][0]["url"], "rtsp://[redacted]");
    assert_eq!(stream["config_on_disk"]["password"], "[redacted]");
    let page = get(
        app.clone(),
        "/streamer/api/v3/streams?sort=password&limit=1",
        "viewer:view-password",
    )
    .await;
    let cursor = STANDARD.decode(page["next"].as_str().unwrap()).unwrap();
    let cursor = String::from_utf8(cursor).unwrap();
    for secret in secrets {
        assert!(!cursor.contains(secret), "View cursor leaked {secret}");
    }
    let search = get(
        app,
        "/streamer/api/v3/streams?q=stream-publish-secret",
        "viewer:view-password",
    )
    .await;
    assert_eq!(
        search["estimated_count"], 0,
        "secret search became an oracle"
    );
}
