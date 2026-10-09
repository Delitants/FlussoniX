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
        json!({"estimated_count":3,"next":null,"prev":null,"timing":{},"streams":[
            {"name":"alpha","title":"Needle first","static":false,"transcoder":{"vb":600},"stats":{"status":"waiting"},"config_on_disk":{"template":"base"}},
            {"name":"beta","title":"Other","static":false,"transcoder":{"vb":600},"stats":{"status":"waiting"},"config_on_disk":{"template":"base"}},
            {"name":"gamma","title":"Needle last","static":false,"transcoder":{"vb":600},"stats":{"status":"waiting"},"config_on_disk":{"template":"base"}}
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
        json!({"estimated_count":1,"next":null,"prev":null,"timing":{},"templates":[
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
        json!({"estimated_count":2,"next":"JTI0cG9zaXRpb25fZ3Q9MA==","prev":null,"timing":{},"streams":[{"name":"gamma","static":false}]})
    );
    let second = selected(
        &app,
        "/streamer/api/v3/streams?select=static&q=Needle&sort=-name&limit=1&cursor=JTI0cG9zaXRpb25fZ3Q9MA%3D%3D",
    )
    .await;
    assert_eq!(
        second,
        json!({"estimated_count":2,"next":null,"prev":null,"timing":{},"streams":[{"name":"alpha","static":false}]})
    );
    let absent = selected(&app, "/streamer/api/v3/streams?select=name&q=not-present").await;
    assert_eq!(
        absent,
        json!({"estimated_count":0,"next":null,"prev":null,"timing":{},"streams":[]})
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
