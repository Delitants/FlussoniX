use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use flussonix::server::{App, Options, router};
use serde_json::{Value, json};
use std::{sync::Arc, time::SystemTime};
use tower::ServiceExt;

fn app() -> (tempfile::TempDir, Arc<App>) {
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "edit-password".into(),
            view_user: Some("viewer".into()),
            view_password: Some("view-password".into()),
            peer_key: "peer-secret-1234".into(),
            ..Options::default()
        },
    )
    .unwrap();
    (dir, app)
}

async fn call(
    app: Arc<App>,
    method: Method,
    path: &str,
    body: Value,
    credentials: Option<&str>,
    origin: Option<&str>,
    fetch_site: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("/streamer/api/v3/{path}"))
        .header("Host", "localhost:18210");
    if let Some(credentials) = credentials {
        builder = builder.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode(credentials)),
        );
    }
    if let Some(origin) = origin {
        builder = builder.header("Origin", origin);
    }
    if let Some(site) = fetch_site {
        builder = builder.header("Sec-Fetch-Site", site);
    }
    let response = router(app)
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 2_000_000)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

async fn edit(app: Arc<App>, method: Method, path: &str, body: Value) -> (StatusCode, Value) {
    call(
        app,
        method,
        path,
        body,
        Some("admin:edit-password"),
        None,
        None,
    )
    .await
}

async fn enable(app: Arc<App>) {
    let (status, body) = edit(app, Method::PUT, "iptv", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["revision"].as_u64().is_some());
}

#[tokio::test]
async fn iptv_collection_select_uses_the_same_field_budget() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    let fields = (0..257)
        .map(|index| format!("unknown{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let (status, _) = call(
        app,
        Method::GET,
        &format!("iptv/packages?select={fields}"),
        Value::Null,
        Some("viewer:view-password"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn module_enable_disable_and_reenable_preserve_subscriber_data() {
    let (_dir, app) = app();
    assert_eq!(
        edit(app.clone(), Method::GET, "iptv", Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    enable(app.clone()).await;
    assert_eq!(
        edit(app.clone(), Method::GET, "iptv", Value::Null).await,
        (StatusCode::OK, json!({}))
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/packages/sports",
            json!({"channels":["news"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/subscribers/alice",
            json!({"token":"token-alice","packages":["sports"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        edit(app.clone(), Method::DELETE, "iptv", Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        edit(app.clone(), Method::GET, "iptv", Value::Null).await.0,
        StatusCode::NOT_FOUND
    );
    assert!(
        app.iptv
            .snapshot()
            .authorize("token-alice", "news", SystemTime::now())
            .is_none()
    );
    enable(app.clone()).await;
    assert!(
        app.iptv
            .snapshot()
            .authorize("token-alice", "news", SystemTime::now())
            .is_some()
    );
}

#[tokio::test]
async fn package_crud_preserves_encoded_names_and_pages_results() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    let (status, row) = edit(
        app.clone(),
        Method::PUT,
        "iptv/packages/region%2Fsports",
        json!({"channels":["news","staged/offline"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["name"], "region/sports");
    assert_eq!(row["channels"], json!(["news", "staged/offline"]));
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/packages/movies",
            json!({"channels":[]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, page) = edit(
        app.clone(),
        Method::GET,
        "iptv/packages?limit=1",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["estimated_count"], 2);
    assert_eq!(page["packages"].as_array().unwrap().len(), 1);
    let cursor = page["next"].as_str().unwrap();
    let (status, next) = edit(
        app.clone(),
        Method::GET,
        &format!("iptv/packages?limit=1&cursor={cursor}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(next["packages"].as_array().unwrap().len(), 1);
    assert_ne!(page["packages"][0]["name"], next["packages"][0]["name"]);
    assert_eq!(
        edit(
            app.clone(),
            Method::GET,
            "iptv/packages/region%2Fsports",
            Value::Null
        )
        .await
        .1["name"],
        "region/sports"
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::DELETE,
            "iptv/packages/region%2Fsports",
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::DELETE,
            "iptv/packages/region%2Fsports",
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn iptv_default_pages_preserve_position_and_omit_absent_navigation() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    for name in ["zeta", "alpha", "middle"] {
        assert_eq!(
            edit(
                app.clone(),
                Method::PUT,
                &format!("iptv/packages/{name}"),
                json!({"channels":[]}),
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    let (status, first) = edit(
        app.clone(),
        Method::GET,
        "iptv/packages?limit=2&select=channels",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        first["packages"],
        json!([{"name":"zeta","channels":[]},{"name":"alpha","channels":[]}])
    );
    assert_eq!(first["estimated_count"], 3);
    assert_eq!(
        first["timing"],
        json!({"select":0,"sort":0,"load":0,"filter":0,"limit":0})
    );
    assert!(first.get("prev").is_none());
    let next = first["next"].as_str().unwrap();
    assert_eq!(STANDARD.decode(next).unwrap(), b"%24position_gt=1");

    let (status, last) = edit(
        app.clone(),
        Method::GET,
        &format!("iptv/packages?limit=2&cursor={next}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(last["packages"], json!([{"name":"middle","channels":[]}]));
    assert!(last.get("next").is_none());
    assert_eq!(
        STANDARD.decode(last["prev"].as_str().unwrap()).unwrap(),
        b"%24position_lt=2&%24reversed=true"
    );
}

#[tokio::test]
async fn iptv_name_sorted_pages_emit_reference_name_bounds() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    for name in ["zeta", "alpha", "middle"] {
        assert_eq!(
            edit(
                app.clone(),
                Method::PUT,
                &format!("iptv/packages/{name}"),
                json!({"channels":[]}),
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    let (_, first) = edit(
        app.clone(),
        Method::GET,
        "iptv/packages?limit=1&sort=name&select=name",
        Value::Null,
    )
    .await;
    assert_eq!(first["packages"], json!([{"name":"alpha"}]));
    assert_eq!(
        STANDARD.decode(first["next"].as_str().unwrap()).unwrap(),
        b"name_gt=alpha"
    );
    let (_, second) = edit(
        app.clone(),
        Method::GET,
        &format!(
            "iptv/packages?limit=1&sort=name&select=name&cursor={}",
            first["next"].as_str().unwrap()
        ),
        Value::Null,
    )
    .await;
    assert_eq!(second["packages"], json!([{"name":"middle"}]));
    assert_eq!(
        STANDARD.decode(second["prev"].as_str().unwrap()).unwrap(),
        b"name_lt=middle&%24reversed=true"
    );
}

#[tokio::test]
async fn iptv_sort_cannot_address_internal_page_position() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    for name in ["zeta", "alpha", "middle"] {
        assert_eq!(
            edit(
                app.clone(),
                Method::PUT,
                &format!("iptv/packages/{name}"),
                json!({"channels":[]}),
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    let (status, body) = edit(
        app,
        Method::GET,
        "iptv/packages?sort=%24index&select=name",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["packages"],
        json!([{"name":"alpha"},{"name":"middle"},{"name":"zeta"}])
    );
}

#[tokio::test]
async fn view_role_subscriber_sort_cursors_never_contain_tokens() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    for (name, token) in [
        ("alice", "private-alpha-token"),
        ("bob", "private-bravo-token"),
    ] {
        assert_eq!(
            edit(
                app.clone(),
                Method::PUT,
                &format!("iptv/subscribers/{name}"),
                json!({"token":token,"packages":[]}),
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    for sort in ["token", "-token"] {
        let (status, first) = call(
            app.clone(),
            Method::GET,
            &format!("iptv/subscribers?sort={sort}&limit=1"),
            Value::Null,
            Some("viewer:view-password"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["subscribers"][0]["token"], "[redacted]");
        let next = first["next"].as_str().unwrap();
        let decoded = String::from_utf8(STANDARD.decode(next).unwrap()).unwrap();
        assert!(!decoded.contains("private-alpha-token"));
        assert!(!decoded.contains("private-bravo-token"));

        let (status, last) = call(
            app.clone(),
            Method::GET,
            &format!("iptv/subscribers?sort={sort}&limit=1&cursor={next}"),
            Value::Null,
            Some("viewer:view-password"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(last["subscribers"][0]["token"], "[redacted]");
        let decoded =
            String::from_utf8(STANDARD.decode(last["prev"].as_str().unwrap()).unwrap()).unwrap();
        assert!(!decoded.contains("private-alpha-token"));
        assert!(!decoded.contains("private-bravo-token"));
    }
}

#[tokio::test]
async fn subscriber_crud_generates_unique_tokens_and_validates_access_fields() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/packages/sports",
            json!({"channels":["news"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, alice) = edit(app.clone(), Method::PUT, "iptv/subscribers/alice", json!({"token":"alice-secret","packages":["sports"],"max_sessions":2,"flussonix_expires_at":"2099-01-01T00:00:00Z"})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(alice["token"], "alice-secret");
    assert_eq!(alice["max_sessions"], 2);
    assert_eq!(alice["flussonix_expires_at"], "2099-01-01T00:00:00Z");
    let (status, bob) = edit(
        app.clone(),
        Method::PUT,
        "iptv/subscribers/bob",
        json!({"packages":["sports"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, carol) = edit(
        app.clone(),
        Method::PUT,
        "iptv/subscribers/carol",
        json!({"packages":["sports"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let bob_token = bob["token"].as_str().unwrap();
    assert!(bob_token.len() >= 32);
    assert_ne!(bob_token, carol["token"]);
    let revision = app.iptv.revision();
    for invalid in [
        json!({"token":"alice-secret","packages":["sports"]}),
        json!({"token":"other","packages":["unknown"]}),
        json!({"token":"other","packages":["sports"],"max_sessions":0}),
        json!({"token":"other","packages":["sports"],"flussonix_expires_at":"not-a-date"}),
        json!({"token":42,"packages":["sports"]}),
    ] {
        assert_eq!(
            edit(
                app.clone(),
                Method::PUT,
                "iptv/subscribers/invalid",
                invalid
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(app.iptv.revision(), revision);
    }
    assert_eq!(
        edit(
            app.clone(),
            Method::DELETE,
            "iptv/subscribers/alice",
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::GET,
            "iptv/subscribers/alice",
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn deleting_package_removes_live_subscriber_membership() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/packages/sports",
            json!({"channels":["news"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/subscribers/alice",
            json!({"token":"alice-token","packages":["sports"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        app.iptv
            .snapshot()
            .authorize("alice-token", "news", SystemTime::now())
            .is_some()
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::DELETE,
            "iptv/packages/sports",
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        app.iptv
            .snapshot()
            .authorize("alice-token", "news", SystemTime::now())
            .is_none()
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::GET,
            "iptv/subscribers/alice",
            Value::Null
        )
        .await
        .1["packages"],
        json!([])
    );
}

#[tokio::test]
async fn view_role_sees_redacted_tokens_and_cannot_mutate() {
    let (_dir, app) = app();
    enable(app.clone()).await;
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/subscribers/alice",
            json!({"token":"private-token","packages":[]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, row) = call(
        app.clone(),
        Method::GET,
        "iptv/subscribers/alice",
        Value::Null,
        Some("viewer:view-password"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["token"], "[redacted]");
    let (status, page) = call(
        app.clone(),
        Method::GET,
        "iptv/subscribers",
        Value::Null,
        Some("viewer:view-password"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["subscribers"][0]["token"], "[redacted]");
    assert_eq!(
        call(
            app.clone(),
            Method::PUT,
            "iptv/subscribers/alice",
            json!({"packages":[]}),
            Some("viewer:view-password"),
            None,
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            Method::GET,
            "iptv/subscribers/alice",
            Value::Null,
            None,
            None,
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let (_status, config) = edit(app.clone(), Method::GET, "config", Value::Null).await;
    assert!(!config.to_string().contains("private-token"));
}

#[tokio::test]
async fn browser_cross_origin_writes_are_rejected_before_mutation() {
    let (_dir, app) = app();
    let before = app.iptv.revision();
    assert_eq!(
        call(
            app.clone(),
            Method::PUT,
            "iptv",
            json!({}),
            Some("admin:edit-password"),
            Some("https://evil.example"),
            None
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            Method::PUT,
            "iptv",
            json!({}),
            Some("admin:edit-password"),
            None,
            Some("cross-site")
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(app.iptv.revision(), before);
    assert_eq!(
        call(
            app.clone(),
            Method::PUT,
            "iptv",
            json!({}),
            Some("admin:edit-password"),
            Some("http://localhost:18210"),
            Some("same-origin")
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn invalid_root_fields_and_corrupt_reload_leave_previous_grants_unchanged() {
    let (dir, app) = app();
    enable(app.clone()).await;
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/packages/sports",
            json!({"channels":["news"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/subscribers/alice",
            json!({"token":"token-alice","packages":["sports"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let before = app.iptv.revision();
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv",
            json!({"database":"postgres://not-supported"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        edit(app.clone(), Method::PUT, "iptv", json!({"surprise":true}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    std::fs::write(dir.path().join("iptv.json"), b"{").unwrap();
    assert_eq!(
        edit(app.clone(), Method::PUT, "iptv", json!({})).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(app.iptv.revision(), before);
    assert!(app.iptv.snapshot().enabled());
    assert!(
        app.iptv
            .snapshot()
            .authorize("token-alice", "news", SystemTime::now())
            .is_some()
    );
}

#[tokio::test]
async fn vanished_data_file_cannot_silently_erase_active_subscribers_on_reload() {
    let (dir, app) = app();
    enable(app.clone()).await;
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/packages/sports",
            json!({"channels":["news"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        edit(
            app.clone(),
            Method::PUT,
            "iptv/subscribers/alice",
            json!({"token":"alice-token","packages":["sports"]})
        )
        .await
        .0,
        StatusCode::OK
    );
    let revision = app.iptv.revision();
    std::fs::remove_file(dir.path().join("iptv.json")).unwrap();
    assert_eq!(
        edit(app.clone(), Method::PUT, "iptv", json!({})).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(app.iptv.revision(), revision);
    assert!(
        app.iptv
            .snapshot()
            .authorize("alice-token", "news", SystemTime::now())
            .is_some()
    );
}
