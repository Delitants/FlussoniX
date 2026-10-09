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
            peer_key: "owned-filter-peer-key".into(),
            ffmpeg: "/owned-filter-no-media-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for (name, title, vb) in [("base", "News HD", 600), ("sport", "Sports", 1200)] {
        app.config
            .put(
                "templates",
                name,
                json!({"title":title,"static":false,
            "inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":vb}}),
            )
            .unwrap();
    }
    for (name, patch) in [
        (
            "alpha",
            json!({"template":"base","position":2,"comment":"null"}),
        ),
        (
            "beta",
            json!({"template":"sport","title":"news SD","position":10,"disabled":true,"comment":"retained"}),
        ),
        (
            "gamma",
            json!({"template":"base","title":"News 100%_HD","position":20,"comment":"undefined"}),
        ),
        (
            "delta",
            json!({"template":"sport","title":"Música HD","position":30}),
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
    auth: bool,
) -> (StatusCode, Value) {
    let qs = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs.iter().copied())
        .finish();
    let mut req = Request::builder().uri(format!("/streamer/api/v3/{kind}?{qs}"));
    if auth {
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

async fn names(app: &Arc<App>, kind: &str, pairs: &[(&str, &str)], expected: &[&str]) {
    let (status, body) = get(app, kind, pairs, true).await;
    assert_eq!(status, StatusCode::OK, "{pairs:?}: {body}");
    let actual = body[kind]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "{pairs:?}");
    assert_eq!(body["estimated_count"], expected.len(), "{pairs:?}");
}

#[tokio::test]
async fn equality_lists_and_boolean_filters_use_typed_effective_configuration() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        &[("template", "base")],
        &["alpha", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("name", "alpha,delta")],
        &["alpha", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("static", "false"), ("disabled", "true")],
        &["beta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("disabled", "false,true")],
        &["alpha", "beta", "delta", "gamma"],
    )
    .await;
    names(&app, "streams", &[("title", "News HD")], &["alpha"]).await;
    names(
        &app,
        "streams",
        &[("position", "10,20")],
        &["beta", "gamma"],
    )
    .await;
}

#[tokio::test]
async fn numeric_ranges_apply_both_bounds_instead_of_lexical_order() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        &[("position_gt", "2"), ("position_lte", "20")],
        &["beta", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("position_gte", "10"), ("position_lt", "20")],
        &["beta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("position_ne", "10")],
        &["alpha", "delta", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("transcoder.vb", "600")],
        &["alpha", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("transcoder.vb_gt", "600"), ("transcoder.vb_lte", "1200")],
        &["beta", "delta"],
    )
    .await;
}

#[tokio::test]
async fn nested_runtime_and_disk_filters_distinguish_inheritance_from_saved_fields() {
    let (dir, app) = fixture();
    let before = app.config.snapshot();
    let disk = std::fs::read(dir.path().join("config.json")).unwrap();
    names(
        &app,
        "streams",
        &[
            ("stats.status", "waiting"),
            ("stats.online_clients", "0"),
            ("config_on_disk.template", "sport"),
        ],
        &["beta", "delta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("static", "false"), ("config_on_disk.static_is", "null")],
        &["alpha", "beta", "delta", "gamma"],
    )
    .await;
    names(&app, "streams", &[("stats.alive", "false")], &[]).await;
    assert_eq!(app.config.snapshot(), before);
    assert_eq!(std::fs::read(dir.path().join("config.json")).unwrap(), disk);
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn null_tests_cover_missing_fields_and_reference_null_text_sentinels() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        &[("comment_is", "null")],
        &["alpha", "delta", "gamma"],
    )
    .await;
    names(&app, "streams", &[("comment_is_not", "null")], &["beta"]).await;
    names(&app, "streams", &[("comment", "null,undefined")], &[]).await;
    names(
        &app,
        "streams",
        &[("comment_ne", "retained")],
        &["alpha", "delta", "gamma"],
    )
    .await;
    names(&app, "streams", &[("comment_gt", "")], &["beta"]).await;
}

#[tokio::test]
async fn like_is_case_sensitive_literal_substring_with_unicode_and_no_wildcards() {
    let (_dir, app) = fixture();
    names(
        &app,
        "streams",
        &[("title_like", "News")],
        &["alpha", "gamma"],
    )
    .await;
    names(&app, "streams", &[("title_like", "news")], &["beta"]).await;
    names(&app, "streams", &[("title_like", "%_")], &["gamma"]).await;
    names(&app, "streams", &[("title_like", ".*")], &[]).await;
    names(&app, "streams", &[("title_like", "Música")], &["delta"]).await;
}

#[tokio::test]
async fn templates_filter_saved_scalar_fields_and_nested_native_transcoder_profile() {
    let (_dir, app) = fixture();
    names(&app, "templates", &[("name", "sport")], &["sport"]).await;
    names(
        &app,
        "templates",
        &[("static", "false"), ("transcoder.vb_gte", "1200")],
        &["sport"],
    )
    .await;
    names(
        &app,
        "templates",
        &[("comment_is", "null")],
        &["base", "sport"],
    )
    .await;
    names(
        &app,
        "templates",
        &[("stats.status", "waiting")],
        &["base", "sport"],
    )
    .await;
}

#[tokio::test]
async fn filtering_precedes_search_sort_cursor_limit_and_selection() {
    let (_dir, app) = fixture();
    let pairs = [
        ("title_like", "News"),
        ("q", "HD"),
        ("sort", "-name"),
        ("limit", "1"),
        ("select", "title"),
    ];
    let (status, first) = get(&app, "streams", &pairs, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        first,
        json!({"estimated_count":2,"prev":null,"timing":{},"next":"JTI0cG9zaXRpb25fZ3Q9MA==","streams":[{"name":"gamma","title":"News 100%_HD"}]})
    );
    let cursor = first["next"].as_str().unwrap();
    let mut next = pairs.to_vec();
    next.push(("cursor", cursor));
    let (status, second) = get(&app, "streams", &next, true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        second,
        json!({"estimated_count":2,"next":null,"prev":null,"timing":{},"streams":[{"name":"alpha","title":"News HD"}]})
    );
    names(
        &app,
        "streams",
        &[("title_like", "absent"), ("select", "name")],
        &[],
    )
    .await;
}

#[tokio::test]
async fn invalid_typed_values_fail_without_bypassing_auth_or_changing_configuration() {
    let (dir, app) = fixture();
    let before = std::fs::read(dir.path().join("config.json")).unwrap();
    for (key, value) in [
        ("disabled", "1"),
        ("disabled", "False"),
        ("static", "true,no"),
        ("position", "1.5"),
        ("position_gt", "oops"),
        ("stats.online_clients", "1x"),
        ("transcoder.vb", "600,bad"),
    ] {
        let (status, _) = get(&app, "streams", &[(key, value)], true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{key}={value}");
        let (status, _) = get(&app, "streams", &[(key, value)], false).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        std::fs::read(dir.path().join("config.json")).unwrap(),
        before
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn unsupported_filter_paths_and_other_endpoints_keep_existing_behavior() {
    let (_dir, app) = fixture();
    for key in [
        "unknown",
        "unknown.child",
        "inputs.0.url",
        "name.child",
        "title_is",
        "title_is_not",
    ] {
        names(
            &app,
            "streams",
            &[(key, "not-a-filter")],
            &["alpha", "beta", "delta", "gamma"],
        )
        .await;
    }
    names(
        &app,
        "streams",
        &[("unknown_is", "null"), ("name", "alpha")],
        &["alpha"],
    )
    .await;
    names(&app, "streams", &[("position_is", "null")], &[]).await;
    let deep = "unknown.".repeat(1000);
    names(
        &app,
        "streams",
        &[(&deep, "anything")],
        &["alpha", "beta", "delta", "gamma"],
    )
    .await;
    let (status, row) = get(
        &app,
        "streams/alpha",
        &[("position", "bad"), ("name", "beta")],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["name"], "alpha");
    let (status, body) = get(&app, "peers", &[("position", "bad")], true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["peers"], json!([]));
}

#[tokio::test]
async fn integer_comparison_preserves_precision_and_signed_unsigned_boundaries() {
    let (_dir, app) = fixture();
    for (name, position) in [
        ("alpha", json!(9007199254740992u64)),
        ("beta", json!(9007199254740993u64)),
        ("gamma", json!(u64::MAX)),
        ("delta", json!(-1)),
    ] {
        app.config
            .put("streams", name, json!({"position":position}))
            .unwrap();
    }
    names(
        &app,
        "streams",
        &[
            ("position_gt", "9007199254740992"),
            ("position_lt", "18446744073709551615"),
        ],
        &["beta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("position", "18446744073709551615")],
        &["gamma"],
    )
    .await;
    names(&app, "streams", &[("position_lt", "0")], &["delta"]).await;
    names(
        &app,
        "streams",
        &[("name_gte", "beta"), ("name_lt", "gamma")],
        &["beta", "delta"],
    )
    .await;
    names(&app, "streams", &[("disabled_gt", "false")], &["beta"]).await;
    names(&app, "streams", &[], &["alpha", "beta", "delta", "gamma"]).await;
    for (key, value) in [
        ("position", "18446744073709551616"),
        ("static_like", "false"),
        ("position_like", "9"),
    ] {
        let (status, _) = get(&app, "streams", &[(key, value)], true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn encoder_and_audio_bitrate_filters_distinguish_inherited_and_saved_native_fields() {
    let (_dir, app) = fixture();
    app.config
        .put("templates", "base", json!({"transcoder":{"ab":64}}))
        .unwrap();
    app.config
        .put(
            "templates",
            "sport",
            json!({"transcoder":{"encoder":"libx265","ab":128}}),
        )
        .unwrap();
    app.config
        .put(
            "streams",
            "beta",
            json!({"transcoder":{"encoder":"libx264","ab":96}}),
        )
        .unwrap();
    names(
        &app,
        "streams",
        &[("transcoder.encoder", "libx265")],
        &["delta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("transcoder.ab", "64")],
        &["alpha", "gamma"],
    )
    .await;
    names(
        &app,
        "streams",
        &[("transcoder.ab_gt", "64"), ("transcoder.ab_lte", "96")],
        &["beta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[
            ("config_on_disk.transcoder.encoder", "libx264"),
            ("config_on_disk.transcoder.ab", "96"),
        ],
        &["beta"],
    )
    .await;
    names(
        &app,
        "streams",
        &[
            ("config_on_disk.transcoder.encoder_is", "null"),
            ("config_on_disk.transcoder.ab_is", "null"),
        ],
        &["alpha", "delta", "gamma"],
    )
    .await;
    names(
        &app,
        "templates",
        &[
            ("transcoder.encoder", "libx265"),
            ("transcoder.ab_gte", "128"),
        ],
        &["sport"],
    )
    .await;
}
