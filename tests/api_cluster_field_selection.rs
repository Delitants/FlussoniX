use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

fn fixture() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_user: "admin".into(),
            admin_password: "owned-admin-secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("owned-view-secret".into()),
            peer_key: "owned-selection-peer-key".into(),
            ffmpeg: "/owned-cluster-select-no-media-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for kind in ["peers", "sources"] {
        for (hostname, address, drain) in [
            ("alpha", "https://needle-a.invalid", false),
            ("beta", "https://other.invalid", true),
            ("gamma", "https://needle-g.invalid", false),
        ] {
            let mut row = json!({
                "api_url":address,
                "private_payload_url":format!("{address}/lan"),
                "public_payload_url":format!("{address}/public"),
                "cluster_key":"owned-cluster-configuration-secret",
                "drain":drain
            });
            if kind == "sources" {
                row["flussonix_source_group"] = json!("owned-group");
            }
            app.config.put(kind, hostname, row).unwrap();
        }
    }
    app.config
        .put(
            "auth_backends",
            "owned-auth",
            json!({"url":"http://owned-auth.invalid"}),
        )
        .unwrap();
    (dir, app)
}

async fn get(app: &Arc<App>, path: &str, credentials: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(path);
    if let Some(credentials) = credentials {
        request = request.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode(credentials)),
        );
    }
    let response = router(app.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn selected(app: &Arc<App>, path: &str) -> Value {
    let (status, body) = get(app, path, Some("viewer:owned-view-secret")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn peer_selection_returns_only_requested_fields() {
    let (_dir, app) = fixture();
    let body = selected(
        &app,
        "/streamer/api/v3/cluster/peers?select=hostname,private_payload_url",
    )
    .await;
    assert_eq!(
        body,
        json!({"estimated_count":3,"next":null,"prev":null,"timing":{},"peers":[
            {"hostname":"alpha","private_payload_url":"https://needle-a.invalid/lan"},
            {"hostname":"beta","private_payload_url":"https://other.invalid/lan"},
            {"hostname":"gamma","private_payload_url":"https://needle-g.invalid/lan"}
        ]})
    );
}

#[tokio::test]
async fn source_selection_supports_native_group_and_boolean_fields() {
    let (_dir, app) = fixture();
    let body = selected(
        &app,
        "/streamer/api/v3/cluster/sources?select=flussonix_source_group,drain",
    )
    .await;
    assert_eq!(
        body["sources"],
        json!([
            {"flussonix_source_group":"owned-group","drain":false},
            {"flussonix_source_group":"owned-group","drain":true},
            {"flussonix_source_group":"owned-group","drain":false}
        ])
    );
}

#[tokio::test]
async fn cluster_selection_follows_search_order_counts_and_cursor_pages() {
    let (_dir, app) = fixture();
    for kind in ["peers", "sources"] {
        let path =
            format!("/streamer/api/v3/cluster/{kind}?select=drain&q=needle&sort=-name&limit=1");
        let first = selected(&app, &path).await;
        assert_eq!(first["estimated_count"], 2);
        assert_eq!(first[kind], json!([{"drain":false}]));
        assert_eq!(first["prev"], Value::Null);
        // Change projection between pages: the second row must still be alpha.
        let cursor = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("cursor", first["next"].as_str().unwrap())
            .finish();
        let second = selected(&app, &format!("/streamer/api/v3/cluster/{kind}?select=hostname&q=needle&sort=-name&limit=1&{cursor}")).await;
        assert_eq!(second[kind], json!([{"hostname":"alpha"}]));
        assert_eq!(second["estimated_count"], 2);
        assert_eq!(second["next"], Value::Null);
        let empty = selected(
            &app,
            &format!("/streamer/api/v3/cluster/{kind}?select=hostname&q=absent-owned-value"),
        )
        .await;
        assert_eq!(
            empty,
            json!({"estimated_count":0,"next":null,"prev":null,"timing":{},kind:[]})
        );
    }
}

#[tokio::test]
async fn empty_unknown_and_scalar_child_selection_does_not_expose_cluster_fields() {
    let (_dir, app) = fixture();
    for kind in ["peers", "sources"] {
        for select in [
            "",
            "unknown",
            "hostname.child,api_url.child,cluster_key.child",
        ] {
            let body = selected(
                &app,
                &format!("/streamer/api/v3/cluster/{kind}?select={select}"),
            )
            .await;
            assert_eq!(body[kind], json!([{}, {}, {}]), "{kind}, {select}");
            assert_eq!(body["estimated_count"], 3);
        }
    }
}

#[tokio::test]
async fn repeated_selectors_and_deep_unknown_paths_remain_bounded() {
    let (_dir, app) = fixture();
    for kind in ["peers", "sources"] {
        let body = selected(
            &app,
            &format!(
                "/streamer/api/v3/cluster/{kind}?select=hostname,hostname.child,hostname,hostname"
            ),
        )
        .await;
        assert_eq!(
            body[kind],
            json!([{"hostname":"alpha"},{"hostname":"beta"},{"hostname":"gamma"}])
        );
        let body = selected(
            &app,
            &format!(
                "/streamer/api/v3/cluster/{kind}?select={}",
                "unknown.".repeat(1000)
            ),
        )
        .await;
        assert_eq!(body[kind], json!([{}, {}, {}]));
    }
}

#[tokio::test]
async fn cluster_selection_requires_management_authentication() {
    let (_dir, app) = fixture();
    for kind in ["peers", "sources"] {
        let path = format!("/streamer/api/v3/cluster/{kind}?select=hostname");
        for credentials in [None, Some("viewer:wrong")] {
            let (status, body) = get(&app, &path, credentials).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(body.get(kind).is_none());
        }
        let (status, body) = get(&app, &path, Some("admin:owned-admin-secret")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body[kind],
            json!([{"hostname":"alpha"},{"hostname":"beta"},{"hostname":"gamma"}])
        );
    }
}

#[tokio::test]
async fn selection_preserves_full_item_other_collection_and_saved_configuration() {
    let (dir, app) = fixture();
    let before = app.config.snapshot();
    let disk = std::fs::read(dir.path().join("config.json")).unwrap();
    for kind in ["peers", "sources"] {
        let full = selected(&app, &format!("/streamer/api/v3/cluster/{kind}")).await;
        assert_eq!(full[kind], before[kind]);
        let item = selected(
            &app,
            &format!("/streamer/api/v3/cluster/{kind}/alpha?select=hostname"),
        )
        .await;
        assert_eq!(item, before[kind][0]);
        let subset = selected(
            &app,
            &format!("/streamer/api/v3/cluster/{kind}?select=hostname"),
        )
        .await;
        assert_eq!(subset[kind][0], json!({"hostname":"alpha"}));
    }
    let auth = selected(&app, "/streamer/api/v3/auth_backends?select=name").await;
    assert_eq!(auth["auth_backends"], before["auth_backends"]);
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(std::fs::read(dir.path().join("config.json")).unwrap(), disk);
    assert_eq!(app.media.count().await, 0);
}
