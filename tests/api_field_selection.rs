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
            admin_password: "secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("view-secret".into()),
            peer_key: "owned-selection-peer-key".into(),
            ffmpeg: "/owned-selection-no-media-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config
        .put(
            "templates",
            "base",
            json!({"title":"Inherited title","static":false,
                "inputs":[{"url":"testsrc://"}],
                "transcoder":{"encoder":"libx264","vb":600,"ab":64}}),
        )
        .unwrap();
    for (name, title) in [
        ("alpha", "Needle first"),
        ("beta", "Other"),
        ("gamma", "Needle last"),
    ] {
        app.config
            .put("streams", name, json!({"template":"base","title":title}))
            .unwrap();
    }
    (dir, app)
}

async fn get(app: &Arc<App>, path: &str, credential: Option<&str>) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(path);
    if let Some(credential) = credential {
        request = request.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode(credential)),
        );
    }
    let response = router(app.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn selected(app: &Arc<App>, path: &str) -> Value {
    let (status, body) = get(app, path, Some("viewer:view-secret")).await;
    assert_eq!(status, StatusCode::OK);
    body
}

#[tokio::test]
async fn collection_navigation_fields_exist_only_when_a_page_can_be_followed() {
    let (_dir, app) = fixture();
    let complete = selected(&app, "/streamer/api/v3/templates").await;
    assert!(complete.get("next").is_none());
    assert!(complete.get("prev").is_none());

    let first = selected(&app, "/streamer/api/v3/streams?limit=2").await;
    assert!(first["next"].is_string());
    assert!(first.get("prev").is_none());
    let cursor = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("cursor", first["next"].as_str().unwrap())
        .finish();
    let last = selected(&app, &format!("/streamer/api/v3/streams?limit=2&{cursor}")).await;
    assert!(last.get("next").is_none());
    assert!(last["prev"].is_string());

    let empty = selected(&app, "/streamer/api/v3/streams?q=no-such-stream").await;
    assert!(empty.get("next").is_none());
    assert!(empty.get("prev").is_none());
}

#[tokio::test]
async fn explicit_name_sort_estimates_at_most_one_row_beyond_page_limit() {
    let (_dir, app) = fixture();
    let first = selected(&app, "/streamer/api/v3/streams?sort=name&limit=1").await;
    assert_eq!(first["estimated_count"], 2);
    assert!(first["next"].is_string());
    let cursor = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("cursor", first["next"].as_str().unwrap())
        .finish();
    let second = selected(
        &app,
        &format!("/streamer/api/v3/streams?sort=name&limit=1&{cursor}"),
    )
    .await;
    assert_eq!(second["estimated_count"], 2);

    let complete = selected(&app, "/streamer/api/v3/streams?sort=name&limit=3").await;
    assert_eq!(complete["estimated_count"], 3);
    let default_order = selected(&app, "/streamer/api/v3/streams?limit=1").await;
    assert_eq!(default_order["estimated_count"], 3);

    for n in 0..8 {
        app.config
            .put("streams", &format!("owned-{n}"), json!({"static":false}))
            .unwrap();
    }
    let eight = selected(&app, "/streamer/api/v3/streams?sort=name&limit=8").await;
    assert_eq!(eight["streams"].as_array().unwrap().len(), 8);
    assert_eq!(eight["estimated_count"], 9);
}

#[tokio::test]
async fn streams_select_nested_effective_fields_and_retain_required_name_without_side_effects() {
    let (dir, app) = fixture();
    let before = app.config.snapshot();
    let disk_before = std::fs::read(dir.path().join("config.json")).unwrap();
    let body = selected(
        &app,
        "/streamer/api/v3/streams?select=title,static,transcoder.vb,stats.status,config_on_disk.template",
    )
    .await;
    assert_eq!(
        body,
        json!({"estimated_count":3,"timing":{},"streams":[
            {"name":"alpha","title":"Needle first","static":false,"transcoder":{"vb":600},"stats":{"status":"waiting"},"config_on_disk":{"name":"alpha","template":"base"}},
            {"name":"beta","title":"Other","static":false,"transcoder":{"vb":600},"stats":{"status":"waiting"},"config_on_disk":{"name":"beta","template":"base"}},
            {"name":"gamma","title":"Needle last","static":false,"transcoder":{"vb":600},"stats":{"status":"waiting"},"config_on_disk":{"name":"gamma","template":"base"}}
        ]})
    );
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(
        std::fs::read(dir.path().join("config.json")).unwrap(),
        disk_before
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn templates_select_whole_arrays_and_merge_sibling_fields() {
    let (_dir, app) = fixture();
    let body = selected(
        &app,
        "/streamer/api/v3/templates?select=inputs,transcoder.vb,transcoder.ab,static",
    )
    .await;
    assert_eq!(
        body,
        json!({"estimated_count":1,"timing":{},"templates":[
            {"inputs":[{"url":"testsrc://"}],"transcoder":{"vb":600,"ab":64},"static":false}
        ]})
    );
    let whole = selected(&app, "/streamer/api/v3/templates?select=name,transcoder").await;
    assert_eq!(
        whole["templates"],
        json!([{"name":"base","transcoder":{"encoder":"libx264","vb":600,"ab":64}}])
    );
}

#[tokio::test]
async fn select_keeps_search_sort_counts_and_cursor_pages_based_on_unprojected_rows() {
    let (_dir, app) = fixture();
    let first = selected(
        &app,
        "/streamer/api/v3/streams?select=static&q=Needle&sort=-name&limit=1",
    )
    .await;
    assert_eq!(
        first,
        json!({"estimated_count":2,"next":first["next"],"timing":{},"streams":[{"name":"gamma","static":false}]})
    );
    assert!(first["next"].is_string());
    let cursor = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("cursor", first["next"].as_str().unwrap())
        .finish();
    let second = selected(
        &app,
        &format!("/streamer/api/v3/streams?select=static&q=Needle&sort=-name&limit=1&{cursor}"),
    )
    .await;
    assert_eq!(
        second,
        json!({"estimated_count":2,"prev":second["prev"],"timing":{},"streams":[{"name":"alpha","static":false}]})
    );
    assert!(second["prev"].is_string());
    let absent = selected(&app, "/streamer/api/v3/streams?select=name&q=not-present").await;
    assert_eq!(
        absent,
        json!({"estimated_count":0,"timing":{},"streams":[]})
    );
}

#[tokio::test]
async fn empty_unknown_and_non_object_paths_never_expand_the_selected_fields() {
    let (_dir, app) = fixture();
    for query in [
        "",
        "not_a_field",
        "title.child,inputs.url",
        "name.child",
        ",name,,",
        "*",
        "stats/../title",
    ] {
        let body = selected(&app, &format!("/streamer/api/v3/streams?select={query}")).await;
        assert_eq!(
            body["streams"],
            json!([{"name":"alpha"},{"name":"beta"},{"name":"gamma"}]),
            "query={query}"
        );
    }
    let body = selected(&app, "/streamer/api/v3/templates?select=").await;
    assert_eq!(body["templates"], json!([{}]));
    let body = selected(&app, "/streamer/api/v3/templates?select=transcoder.unknown").await;
    assert_eq!(body["templates"], json!([{"transcoder":{}}]));
    let deep = format!(
        "/streamer/api/v3/streams?select={}",
        "unknown.".repeat(1000)
    );
    assert_eq!(
        selected(&app, &deep).await["streams"],
        json!([{"name":"alpha"},{"name":"beta"},{"name":"gamma"}])
    );
}

#[tokio::test]
async fn later_parent_or_child_selector_replaces_earlier_overlap() {
    let (_dir, app) = fixture();
    let narrow = selected(
        &app,
        "/streamer/api/v3/templates?select=transcoder,transcoder.vb,transcoder.ab,transcoder.vb",
    )
    .await;
    assert_eq!(
        narrow["templates"],
        json!([{"transcoder":{"vb":600,"ab":64}}])
    );
    let whole = selected(
        &app,
        "/streamer/api/v3/templates?select=transcoder.vb,transcoder",
    )
    .await;
    assert_eq!(
        whole["templates"],
        json!([{"transcoder":{"encoder":"libx264","vb":600,"ab":64}}])
    );
}

#[tokio::test]
async fn selection_preserves_auth_and_default_collection_and_item_get_responses() {
    let (_dir, app) = fixture();
    assert_eq!(
        get(&app, "/streamer/api/v3/streams?select=name", None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(
            &app,
            "/streamer/api/v3/templates?select=name",
            Some("viewer:wrong")
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let full = selected(&app, "/streamer/api/v3/streams").await;
    assert_eq!(full["streams"][0]["transcoder"]["encoder"], "libx264");
    assert_eq!(
        full["streams"][0]["config_on_disk"]["title"],
        "Needle first"
    );
    assert!(full["streams"][0]["stats"].is_object());
    let item = selected(&app, "/streamer/api/v3/streams/alpha?select=name").await;
    assert_eq!(item["title"], "Needle first");
    assert_eq!(item["transcoder"]["ab"], 64);
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn selecting_disk_configuration_retains_its_required_stream_identity() {
    let (_dir, app) = fixture();
    for select in [
        "config_on_disk.template",
        "config_on_disk.unknown",
        "config_on_disk.name.child",
    ] {
        let body = selected(&app, &format!("/streamer/api/v3/streams?select={select}")).await;
        let expected = if select == "config_on_disk.template" {
            json!([
                {"name":"alpha","config_on_disk":{"name":"alpha","template":"base"}},
                {"name":"beta","config_on_disk":{"name":"beta","template":"base"}},
                {"name":"gamma","config_on_disk":{"name":"gamma","template":"base"}}
            ])
        } else {
            json!([
                {"name":"alpha","config_on_disk":{"name":"alpha"}},
                {"name":"beta","config_on_disk":{"name":"beta"}},
                {"name":"gamma","config_on_disk":{"name":"gamma"}}
            ])
        };
        assert_eq!(body["streams"], expected, "select={select}");
    }
}

#[tokio::test]
async fn collection_select_rejects_excessive_fields_or_bytes_after_authentication() {
    let (_dir, app) = fixture();
    let fields = (0..256)
        .map(|index| format!("unknown{index}"))
        .collect::<Vec<_>>();
    let within_limit = format!("/streamer/api/v3/streams?select={}", fields.join(","));
    assert_eq!(
        selected(&app, &within_limit).await["streams"],
        json!([{"name":"alpha"},{"name":"beta"},{"name":"gamma"}])
    );

    let too_many = format!("{within_limit},unknown256");
    assert_eq!(
        get(&app, &too_many, Some("viewer:view-secret")).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(get(&app, &too_many, None).await.0, StatusCode::UNAUTHORIZED);
    let encoded_comma = too_many.replacen(",unknown256", "%2Cunknown256", 1);
    assert_eq!(
        get(&app, &encoded_comma, Some("viewer:view-secret"))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );

    let max_length = format!(
        "/streamer/api/v3/templates?select={}",
        "x".repeat(16 * 1024)
    );
    assert_eq!(
        get(&app, &max_length, Some("viewer:view-secret")).await.0,
        StatusCode::OK
    );
    let too_long = format!(
        "/streamer/api/v3/templates?select={}",
        "x".repeat(16 * 1024 + 1)
    );
    assert_eq!(
        get(&app, &too_long, Some("viewer:view-secret")).await.0,
        StatusCode::BAD_REQUEST
    );
}
