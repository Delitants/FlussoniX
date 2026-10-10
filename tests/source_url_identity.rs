//! Public source CRUD regressions: URL identities must not be treated as stream names.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::{
    config::ConfigStore,
    server::{App, Options, router},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

fn fixture() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin-secret".into(),
            peer_key: "owned-source-url-peer-key".into(),
            view_user: Some("viewer".into()),
            view_password: Some("owned-view-secret".into()),
            ffmpeg: "/owned-no-media-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    (dir, app)
}
fn path(key: &str) -> String {
    format!(
        "/streamer/api/v3/cluster/sources/{}",
        percent_encoding::utf8_percent_encode(key, percent_encoding::NON_ALPHANUMERIC)
    )
}
async fn request(
    app: &Arc<App>,
    method: &str,
    path: &str,
    body: Value,
    credentials: Option<&str>,
) -> (StatusCode, Value) {
    let mut r = Request::builder().method(method).uri(path);
    if let Some(c) = credentials {
        r = r.header("Authorization", format!("Basic {}", STANDARD.encode(c)));
    }
    let response = router(app.clone())
        .oneshot(
            r.header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

// Break caught: source PUT still injects hostname and rejects :// as an invalid name.
#[test]
fn url_source_defaults_persist_and_native_rows_keep_their_identity() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    for (key, api, transport) in [
        ("m4f://example.net:8080", "http://example.net:8080/", "m4f"),
        (
            "m4fs://secure.example.net:8443",
            "https://secure.example.net:8443/",
            "m4f",
        ),
        ("m4s://[::1]:9000", "http://[::1]:9000/", "m4s"),
        (
            "m4ss://secure.example.net",
            "https://secure.example.net/",
            "m4s",
        ),
    ] {
        let row = s
            .put("sources", key, json!({"except":["blocked/*"]}))
            .unwrap();
        assert_eq!(row["url"], key);
        assert!(row.get("hostname").is_none());
        assert_eq!(row["api_url"], api);
        assert_eq!(row["flussonix_transport"], transport);
    }
    s.put(
        "sources",
        "native",
        json!({"api_url":"http://native.invalid"}),
    )
    .unwrap();
    let before = s.snapshot();
    drop(s);
    let s = ConfigStore::open(&p).unwrap();
    assert_eq!(s.snapshot(), before);
    assert_eq!(
        s.snapshot()["sources"][4],
        json!({"hostname":"native","api_url":"http://native.invalid"})
    );
}

// Break caught: full configuration imports bypass source URL normalization/defaults.
#[test]
fn full_config_import_and_explicit_endpoints_support_url_sources() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    let root = json!({"sources":[{"url":"m4ss://origin.invalid:8443"}]});
    let validated = s.validate(root.clone()).unwrap();
    assert_eq!(
        validated["sources"][0]["api_url"],
        "https://origin.invalid:8443/"
    );
    assert!(s.snapshot()["sources"].as_array().unwrap().is_empty());
    s.replace(root).unwrap();
    let row = s.put("sources", "m4ss://origin.invalid:8443", json!({"api_url":"https://management.invalid/api", "private_payload_url":"https://lan.invalid/media", "flussonix_transport":"m4f"})).unwrap();
    assert_eq!(row["api_url"], "https://management.invalid/api");
    assert_eq!(row["private_payload_url"], "https://lan.invalid/media");
    assert_eq!(row["flussonix_transport"], "m4f");
    let row = s
        .put(
            "sources",
            "m4ss://origin.invalid:8443",
            json!({"api_url":null,"private_payload_url":null,"flussonix_transport":null}),
        )
        .unwrap();
    assert_eq!(row["api_url"], "https://origin.invalid:8443/");
    assert_eq!(row["flussonix_transport"], "m4s");
    assert!(row.get("private_payload_url").is_none());
    std::fs::write(&p, r#"{"streams":[],"templates":[],"peers":[],"auth_backends":[],"sources":[{"url":"m4f://disk.invalid:8080"}]}"#).unwrap();
    let reopened = ConfigStore::open(&p).unwrap();
    assert_eq!(
        reopened.snapshot()["sources"][0]["api_url"],
        "http://disk.invalid:8080/"
    );
}

// Break caught: unsafe URL identities or dual identities are accepted and persisted.
#[test]
fn malformed_or_ambiguous_url_sources_reject_without_disk_or_revision_changes() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    s.put(
        "sources",
        "native",
        json!({"api_url":"http://native.invalid"}),
    )
    .unwrap();
    let bytes = std::fs::read(&p).unwrap();
    let revision = s.revision();
    for key in [
        "http://origin.invalid",
        "m4s://",
        "m4s://user:secret@origin.invalid",
        "m4s://origin.invalid:0",
        "m4s://origin.invalid:",
        "m4s://origin.invalid/path",
        "m4s://origin.invalid/..",
        "m4s://origin.invalid?token=secret",
        "m4s://origin.invalid#fragment",
        "m4s://origin.invalid\\path",
        "m4s://origin.invalid\n",
        "m4s://origin.invalid/%2e%2e",
    ] {
        assert!(
            s.put("sources", key, json!({})).is_err(),
            "accepted {key:?}"
        );
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
        assert_eq!(s.revision(), revision);
    }
    for patch in [
        json!({"sources":[{"url":"m4s://origin.invalid","hostname":"alias"}]}),
        json!({"sources":[{"hostname":"m4s://origin.invalid","api_url":"http://origin.invalid"}]}),
        json!({"sources":[{"url":"m4s://origin.invalid"},{"url":"m4s://origin.invalid"}]}),
        json!({"peers":[{"url":"m4s://origin.invalid","api_url":"http://origin.invalid"}]}),
    ] {
        assert!(s.replace(patch).is_err());
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
        assert_eq!(s.revision(), revision);
    }
}

// Break caught: decoded URI keys cannot be fetched/updated/deleted or lose merge semantics.
#[tokio::test]
async fn encoded_source_url_crud_keeps_identity_and_partial_update_semantics() {
    let (_dir, app) = fixture();
    let key = "m4s://origin.invalid:8080";
    let uri = path(key);
    let (status, row) = request(
        &app,
        "PUT",
        &uri,
        json!({"cluster_key":"owned-cluster-secret","except":["blocked/*"]}),
        Some("admin:owned-admin-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{row}");
    assert_eq!(row["url"], key);
    assert!(row.get("hostname").is_none());
    let (status, row) = request(
        &app,
        "GET",
        &uri,
        Value::Null,
        Some("viewer:owned-view-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["except"], json!(["blocked/*"]));
    let (_, row) = request(
        &app,
        "PUT",
        &uri,
        json!({"except":null,"drain":true}),
        Some("admin:owned-admin-secret"),
    )
    .await;
    assert_eq!(row["url"], key);
    assert!(row.get("except").is_none());
    assert_eq!(row["cluster_key"], "owned-cluster-secret");
    let (_, row) = request(
        &app,
        "PUT",
        &uri,
        json!({"$reset":true}),
        Some("admin:owned-admin-secret"),
    )
    .await;
    assert_eq!(
        row,
        json!({"url":key,"api_url":"http://origin.invalid:8080/","flussonix_transport":"m4s"})
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            &uri,
            Value::Null,
            Some("admin:owned-admin-secret")
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(
            &app,
            "GET",
            &uri,
            Value::Null,
            Some("admin:owned-admin-secret")
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

// Break caught: new identity handling bypasses read/edit permissions or silently renames rows.
#[tokio::test]
async fn url_source_permissions_and_conflicting_body_identity_are_enforced() {
    let (dir, app) = fixture();
    let uri = path("m4f://origin.invalid:8080");
    assert_eq!(
        request(&app, "PUT", &uri, json!({}), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(
            &app,
            "PUT",
            &uri,
            json!({}),
            Some("viewer:owned-view-secret")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &app,
            "PUT",
            &uri,
            json!({}),
            Some("admin:owned-admin-secret")
        )
        .await
        .0,
        StatusCode::OK
    );
    let bytes = std::fs::read(dir.path().join("config.json")).unwrap();
    let revision = app.config.revision();
    for body in [
        json!({"url":"m4f://another.invalid:8080"}),
        json!({"hostname":"alias"}),
    ] {
        assert_eq!(
            request(&app, "PUT", &uri, body, Some("admin:owned-admin-secret"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            std::fs::read(dir.path().join("config.json")).unwrap(),
            bytes
        );
        assert_eq!(app.config.revision(), revision);
    }
    assert_eq!(
        request(
            &app,
            "DELETE",
            &uri,
            Value::Null,
            Some("viewer:owned-view-secret")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &app,
            "GET",
            &uri,
            Value::Null,
            Some("viewer:owned-view-secret")
        )
        .await
        .0,
        StatusCode::OK
    );
}

// Break caught: list sorting and selected identities fall back to empty hostname on URL rows.
#[tokio::test]
async fn source_lists_order_url_identities_and_project_selected_fields() {
    let (_dir, app) = fixture();
    app.config
        .put("sources", "m4s://z.invalid", json!({}))
        .unwrap();
    app.config
        .put("sources", "m4s://a.invalid", json!({}))
        .unwrap();
    let (status, rows) = request(
        &app,
        "GET",
        "/streamer/api/v3/cluster/sources?select=url&limit=1",
        Value::Null,
        Some("viewer:owned-view-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows["estimated_count"], 2);
    assert_eq!(rows["sources"], json!([{"url":"m4s://a.invalid"}]));
    assert!(rows["next"].is_string());
    let (status, rows) = request(
        &app,
        "GET",
        "/streamer/api/v3/cluster/sources?select=url&sort=-name",
        Value::Null,
        Some("viewer:owned-view-secret"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        rows["sources"],
        json!([{"url":"m4s://z.invalid"},{"url":"m4s://a.invalid"}])
    );
}

// Break caught: source normalization indexes a non-object root and panics before validation.
#[test]
fn malformed_root_still_returns_validation_error_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("config.json");
    let s = ConfigStore::open(&p).unwrap();
    s.put(
        "sources",
        "native",
        json!({"api_url":"http://native.invalid"}),
    )
    .unwrap();
    let bytes = std::fs::read(&p).unwrap();
    let revision = s.revision();
    for patch in [
        json!([]),
        json!(false),
        Value::Null,
        json!({"sources":false}),
    ] {
        assert!(s.validate(patch.clone()).is_err());
        assert!(s.replace(patch).is_err());
        assert_eq!(std::fs::read(&p).unwrap(), bytes);
        assert_eq!(s.revision(), revision);
    }
}
