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
            peer_key: "owned-sorting-peer-key".into(),
            ffmpeg: "/owned-sort-no-media-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for (name, title, vb, ab) in [("base", "News", 600, 128), ("sport", "Sports", 1200, 64)] {
        app.config
            .put(
                "templates",
                name,
                json!({"title":title,"static":false,
            "inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":vb,"ab":ab}}),
            )
            .unwrap();
    }
    for (name, patch) in [
        (
            "alpha",
            json!({"template":"base","position":10,"comment":"null"}),
        ),
        (
            "beta",
            json!({"template":"sport","position":2,"comment":"","disabled":true}),
        ),
        (
            "gamma",
            json!({"template":"base","position":20,"comment":"undefined"}),
        ),
        (
            "delta",
            json!({"template":"base","position":20,"comment":"kept","transcoder":{"encoder":"libx264","vb":900,"ab":96}}),
        ),
    ] {
        app.config.put("streams", name, patch).unwrap();
    }
    (dir, app)
}

async fn get(
    app: &Arc<App>,
    kind: &str,
    pairs: &[(&str, &str)],
    authenticated: bool,
) -> (StatusCode, Value) {
    let qs = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs.iter().copied())
        .finish();
    let mut req = Request::builder().uri(format!("/streamer/api/v3/{kind}?{qs}"));
    if authenticated {
        req = req.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("viewer:view-secret")),
        );
    }
    let response = router(app.clone())
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn names(app: &Arc<App>, kind: &str, sort: &str, expected: &[&str]) {
    let (status, body) = get(app, kind, &[("sort", sort)], true).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let actual = body[kind]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "sort={sort}");
    assert_eq!(body["estimated_count"], expected.len());
}

#[tokio::test]
async fn numeric_and_composite_sorting_apply_each_direction_and_name_ties() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        "position",
        &["beta", "alpha", "delta", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        "-position",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
    names(
        &app,
        "streams",
        "title,-position",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
    names(
        &app,
        "streams",
        "-title,position,-name",
        &["beta", "alpha", "gamma", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        "position,-name",
        &["beta", "alpha", "gamma", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        "-name",
        &["gamma", "delta", "beta", "alpha"],
    )
    .await;
}

#[tokio::test]
async fn effective_saved_and_runtime_paths_are_sorted_before_projection() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        "transcoder.vb,-position",
        &["gamma", "alpha", "delta", "beta"],
    )
    .await;
    names(
        &app,
        "streams",
        "transcoder.ab",
        &["beta", "delta", "alpha", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        "config_on_disk.transcoder.vb,-name",
        &["gamma", "beta", "alpha", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        "stats.status,stats.online_clients,-position",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
    let (status, body) = get(
        &app,
        "streams",
        &[("sort", "transcoder.vb,-position"), ("select", "name")],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["streams"],
        json!([{"name":"gamma"},{"name":"alpha"},{"name":"delta"},{"name":"beta"}])
    );
}

#[tokio::test]
async fn missing_and_text_null_values_sort_together_but_empty_text_is_present() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        "comment",
        &["alpha", "gamma", "beta", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        "-comment",
        &["delta", "beta", "alpha", "gamma"],
    )
    .await;
    app.config
        .put(
            "streams",
            "gamma",
            json!({"template":"base","position":20,"comment":null}),
        )
        .unwrap();
    names(
        &app,
        "streams",
        "comment",
        &["alpha", "gamma", "beta", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        "disabled",
        &["alpha", "delta", "gamma", "beta"],
    )
    .await;
    names(
        &app,
        "streams",
        "-disabled",
        &["beta", "alpha", "delta", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        "static,-position",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
}

#[tokio::test]
async fn integer_precision_and_mixed_scalar_type_order_are_explicit() {
    let (_dir, app) = fixture();
    for (name, value) in [
        ("alpha", json!(9007199254740993_u64)),
        ("beta", json!(9007199254740992_u64)),
        ("gamma", json!(u64::MAX)),
        ("delta", json!(i64::MIN)),
    ] {
        app.config
            .put("streams", name, json!({"template":"base","position":value}))
            .unwrap();
    }
    names(
        &app,
        "streams",
        "position",
        &["delta", "beta", "alpha", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        "-position",
        &["gamma", "alpha", "beta", "delta"],
    )
    .await;
    for (name, value) in [
        ("alpha", json!(1.5)),
        ("beta", json!(false)),
        ("gamma", json!("text")),
        ("delta", json!(7)),
    ] {
        app.config
            .put("streams", name, json!({"template":"base","position":value}))
            .unwrap();
    }
    names(
        &app,
        "streams",
        "position",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
    names(
        &app,
        "streams",
        "-position",
        &["beta", "alpha", "gamma", "delta"],
    )
    .await;
    app.config
        .put(
            "streams",
            "gamma",
            json!({"template":"base","position":-2.5}),
        )
        .unwrap();
    app.config
        .put(
            "streams",
            "delta",
            json!({"template":"base","position":true}),
        )
        .unwrap();
    names(
        &app,
        "streams",
        "position",
        &["gamma", "alpha", "beta", "delta"],
    )
    .await;
}

#[tokio::test]
async fn strings_use_case_sensitive_unicode_order_and_templates_share_sorting() {
    let (_dir, app) = fixture();
    for (name, title) in [
        ("alpha", "é"),
        ("beta", "a"),
        ("gamma", "Z"),
        ("delta", "A"),
    ] {
        app.config
            .put("streams", name, json!({"template":"base","title":title}))
            .unwrap();
    }
    names(
        &app,
        "streams",
        "title",
        &["delta", "gamma", "beta", "alpha"],
    )
    .await;
    names(&app, "templates", "-transcoder.vb", &["sport", "base"]).await;
    names(&app, "templates", "transcoder.ab", &["sport", "base"]).await;
    names(&app, "templates", "static,-title", &["sport", "base"]).await;
}

#[tokio::test]
async fn filtered_sorted_pages_keep_counts_and_cursor_envelope() {
    let (_dir, app) = fixture();
    let pairs = [
        ("title", "News"),
        ("q", "base"),
        ("sort", "-position,-name"),
        ("select", "title"),
        ("limit", "1"),
    ];
    let mut cursor: Option<String> = None;
    for (index, name) in ["gamma", "delta", "alpha"].into_iter().enumerate() {
        let mut query = pairs.to_vec();
        if let Some(ref value) = cursor {
            query.push(("cursor", value));
        }
        let (status, body) = get(&app, "streams", &query, true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["streams"], json!([{"name":name,"title":"News"}]));
        assert_eq!(body["estimated_count"], 3);
        assert_eq!(body["prev"].is_string(), index > 0);
        assert_eq!(body["timing"], json!({}));
        cursor = body["next"].as_str().map(str::to_owned);
        assert_eq!(cursor.is_some(), index < 2);
    }
}

#[tokio::test]
async fn unsupported_paths_and_empty_duplicate_fields_have_deterministic_fallbacks() {
    let (_dir, app) = fixture();
    for sort in [
        "",
        "unknown",
        "transcoder",
        "inputs",
        "inputs.0.url",
        "title.child",
        "stats.unknown",
        ",,,",
        ".",
        "name,name",
    ] {
        names(&app, "streams", sort, &["alpha", "beta", "delta", "gamma"]).await;
    }
    names(
        &app,
        "streams",
        "unknown,-position,position",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
    names(
        &app,
        "streams",
        ",-position,,",
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
    let deep = "missing.".repeat(1000);
    names(
        &app,
        "streams",
        &format!("{deep},-position"),
        &["delta", "gamma", "alpha", "beta"],
    )
    .await;
}

#[tokio::test]
async fn valid_null_text_identities_remain_literal_in_default_and_tie_ordering() {
    let (_dir, app) = fixture();
    for name in ["undefined", "null", "alpha", "beta", "delta", "gamma"] {
        app.config
            .put("streams", name, json!({"template":"base","position":7}))
            .unwrap();
    }
    let expected = ["alpha", "beta", "delta", "gamma", "null", "undefined"];
    let (status, body) = get(&app, "streams", &[], true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    for sort in ["name", "position", "unknown", ""] {
        names(&app, "streams", sort, &expected).await;
    }
    names(
        &app,
        "streams",
        "-name",
        &["undefined", "null", "gamma", "delta", "beta", "alpha"],
    )
    .await;
    for name in ["undefined", "null"] {
        app.config
            .put("templates", name, json!({"static":false}))
            .unwrap();
    }
    names(
        &app,
        "templates",
        "name",
        &["base", "null", "sport", "undefined"],
    )
    .await;
    names(
        &app,
        "templates",
        "unknown",
        &["base", "null", "sport", "undefined"],
    )
    .await;
}

#[tokio::test]
async fn sorting_preserves_auth_config_and_other_collections() {
    let (dir, app) = fixture();
    let before = app.config.snapshot();
    let disk = std::fs::read(dir.path().join("config.json")).unwrap();
    let (status, _) = get(&app, "streams", &[("sort", "position")], false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = get(&app, "streams", &[], true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["streams"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["alpha", "beta", "delta", "gamma"]
    );
    for host in ["a", "z"] {
        app.config
            .put(
                "peers",
                host,
                json!({"hostname":host,"api_url":"http://127.0.0.1:9"}),
            )
            .unwrap();
    }
    let peers = app.config.snapshot();
    let peer_disk = std::fs::read(dir.path().join("config.json")).unwrap();
    let (status, body) = get(&app, "peers", &[("sort", "-hostname")], true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["peers"][0]["hostname"], "a");
    names(
        &app,
        "streams",
        "position",
        &["beta", "alpha", "delta", "gamma"],
    )
    .await;
    assert_eq!(app.config.snapshot(), peers);
    assert_eq!(
        std::fs::read(dir.path().join("config.json")).unwrap(),
        peer_disk
    );
    assert_eq!(before["streams"], peers["streams"]);
    assert_ne!(disk, peer_disk);
    assert_eq!(app.media.count().await, 0);
}
