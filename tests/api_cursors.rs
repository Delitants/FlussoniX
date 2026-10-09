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
            peer_key: "owned-cursor-peer-key".into(),
            ffmpeg: "/owned-cursor-no-media-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for (name, position) in [("alpha", 10), ("beta", 20), ("delta", 20), ("gamma", 30)] {
        for kind in ["streams", "templates"] {
            app.config
                .put(
                    kind,
                    name,
                    json!({"static":false,"title":"Owned","position":position,
                "inputs":[{"url":"testsrc://"}]}),
                )
                .unwrap();
        }
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
    let mut request = Request::builder().uri(format!("/streamer/api/v3/{kind}?{qs}"));
    if authenticated {
        request = request.header(
            "Authorization",
            format!("Basic {}", STANDARD.encode("viewer:view-secret")),
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

async fn page(
    app: &Arc<App>,
    kind: &str,
    pairs: &[(&str, &str)],
    expected: &[&str],
    total: usize,
) -> Value {
    let (status, body) = get(app, kind, pairs, true).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body[kind]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(body["estimated_count"], total);
    assert_eq!(body["timing"], json!({}));
    body
}

fn token(query: &str) -> String {
    STANDARD.encode(query)
}

fn payload(cursor: &str) -> Value {
    let bytes = STANDARD.decode(cursor).unwrap();
    let pairs = url::form_urlencoded::parse(&bytes).collect::<Vec<_>>();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].0, "$flussonix_cursor");
    serde_json::from_str(&pairs[0].1).unwrap()
}

fn native(value: &Value) -> String {
    token(
        &url::form_urlencoded::Serializer::new(String::new())
            .append_pair("$flussonix_cursor", &value.to_string())
            .finish(),
    )
}

#[tokio::test]
async fn forward_cursor_survives_insert_delete_before_boundary_and_deleted_anchor() {
    for mutation in ["insert", "delete", "anchor"] {
        let (_dir, app) = fixture();
        let first = page(&app, "streams", &[("limit", "2")], &["alpha", "beta"], 4).await;
        match mutation {
            "insert" => {
                app.config
                    .put("streams", "aardvark", json!({"static":false}))
                    .unwrap();
            }
            "delete" => {
                app.config.delete("streams", "alpha").unwrap();
            }
            _ => {
                app.config.delete("streams", "beta").unwrap();
            }
        }
        let second = page(
            &app,
            "streams",
            &[("limit", "2"), ("cursor", first["next"].as_str().unwrap())],
            &["delta", "gamma"],
            if mutation == "insert" { 5 } else { 3 },
        )
        .await;
        assert!(second["next"].is_null());
        assert!(second["prev"].is_string());
    }
}

#[tokio::test]
async fn backward_cursor_returns_nearest_previous_page_in_normal_order() {
    let (_dir, app) = fixture();
    let first = page(&app, "streams", &[("limit", "2")], &["alpha", "beta"], 4).await;
    assert!(first["prev"].is_null());
    let last = page(
        &app,
        "streams",
        &[("limit", "2"), ("cursor", first["next"].as_str().unwrap())],
        &["delta", "gamma"],
        4,
    )
    .await;
    let previous = page(
        &app,
        "streams",
        &[("limit", "1"), ("cursor", last["prev"].as_str().unwrap())],
        &["beta"],
        4,
    )
    .await;
    assert!(previous["next"].is_string());
    let start = page(
        &app,
        "streams",
        &[
            ("limit", "2"),
            ("cursor", previous["prev"].as_str().unwrap()),
        ],
        &["alpha"],
        4,
    )
    .await;
    assert!(start["prev"].is_null());
}

#[tokio::test]
async fn backward_cursor_survives_deleted_anchor_and_insertion_before_it() {
    let (_dir, app) = fixture();
    let last = page(
        &app,
        "streams",
        &[("cursor", &token("name_gt=beta")), ("limit", "2")],
        &["delta", "gamma"],
        4,
    )
    .await;
    app.config.delete("streams", "delta").unwrap();
    app.config
        .put("streams", "charlie", json!({"static":false}))
        .unwrap();
    page(
        &app,
        "streams",
        &[("cursor", last["prev"].as_str().unwrap()), ("limit", "2")],
        &["beta", "charlie"],
        4,
    )
    .await;
}

#[tokio::test]
async fn composite_directions_and_identity_ties_are_retained_in_both_directions() {
    let (_dir, app) = fixture();
    let first = page(
        &app,
        "streams",
        &[
            ("sort", "-position,name"),
            ("limit", "2"),
            ("select", "title"),
        ],
        &["gamma", "beta"],
        4,
    )
    .await;
    assert!(first["streams"][0].get("position").is_none());
    app.config.delete("streams", "beta").unwrap();
    let last = page(
        &app,
        "streams",
        &[
            ("sort", "-position,name"),
            ("limit", "2"),
            ("select", "name"),
            ("cursor", first["next"].as_str().unwrap()),
        ],
        &["delta", "alpha"],
        3,
    )
    .await;
    page(
        &app,
        "streams",
        &[
            ("sort", "-position,name"),
            ("limit", "2"),
            ("cursor", last["prev"].as_str().unwrap()),
        ],
        &["gamma"],
        3,
    )
    .await;
}

#[tokio::test]
async fn template_pages_use_values_even_when_projection_omits_identity() {
    let (_dir, app) = fixture();
    let (status, first) = get(
        &app,
        "templates",
        &[("sort", "-position"), ("limit", "1"), ("select", "title")],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["templates"], json!([{"title":"Owned"}]));
    app.config.delete("templates", "gamma").unwrap();
    let second = page(
        &app,
        "templates",
        &[
            ("sort", "-position"),
            ("limit", "2"),
            ("cursor", first["next"].as_str().unwrap()),
        ],
        &["beta", "delta"],
        3,
    )
    .await;
    assert!(second["prev"].is_null()); // deleted anchor leaves no earlier rows
    assert!(second["next"].is_string());
}

#[tokio::test]
async fn inherited_saved_and_runtime_sort_paths_continue_on_unselected_fields() {
    let (_dir, app) = fixture();
    app.config
        .put(
            "templates",
            "base",
            json!({"static":false,"transcoder":{"encoder":"libx264","vb":600}}),
        )
        .unwrap();
    for name in ["alpha", "beta", "delta", "gamma"] {
        app.config
            .put("streams", name, json!({"template":"base"}))
            .unwrap();
    }
    for expression in [
        "transcoder.vb,-config_on_disk.position",
        "stats.status,stats.online_clients,-position",
    ] {
        let first = page(
            &app,
            "streams",
            &[("sort", expression), ("limit", "1"), ("select", "title")],
            &["gamma"],
            4,
        )
        .await;
        page(
            &app,
            "streams",
            &[
                ("sort", expression),
                ("limit", "2"),
                ("select", "name"),
                ("cursor", first["next"].as_str().unwrap()),
            ],
            &["beta", "delta"],
            4,
        )
        .await;
    }
}

#[tokio::test]
async fn integer_boundaries_are_lossless_and_literal_identity_sentinels_survive() {
    let (_dir, app) = fixture();
    for (name, value) in [
        ("alpha", json!(9007199254740993_u64)),
        ("beta", json!(9007199254740992_u64)),
        ("delta", json!(i64::MIN)),
        ("gamma", json!(u64::MAX)),
    ] {
        app.config
            .put("streams", name, json!({"position":value}))
            .unwrap();
    }
    let mut cursor = None::<String>;
    for name in ["delta", "beta", "alpha", "gamma"] {
        let mut pairs = vec![("sort", "position"), ("limit", "1")];
        if let Some(ref c) = cursor {
            pairs.push(("cursor", c));
        }
        let body = page(&app, "streams", &pairs, &[name], 4).await;
        cursor = body["next"].as_str().map(str::to_owned);
    }
    // An unsigned maximum boundary must retain its integer type and magnitude.
    let last = page(
        &app,
        "streams",
        &[("sort", "-position"), ("limit", "1")],
        &["gamma"],
        4,
    )
    .await;
    page(
        &app,
        "streams",
        &[
            ("sort", "-position"),
            ("limit", "2"),
            ("cursor", last["next"].as_str().unwrap()),
        ],
        &["alpha", "beta"],
        4,
    )
    .await;
    for name in ["null", "undefined"] {
        app.config
            .put("streams", name, json!({"static":false}))
            .unwrap();
    }
    let first = page(
        &app,
        "streams",
        &[("sort", "missing,-name"), ("limit", "1")],
        &["undefined"],
        6,
    )
    .await;
    page(
        &app,
        "streams",
        &[
            ("sort", "missing,-name"),
            ("limit", "1"),
            ("cursor", first["next"].as_str().unwrap()),
        ],
        &["null"],
        6,
    )
    .await;
}

#[tokio::test]
async fn mixed_scalars_missing_values_and_precise_float_boundaries_round_trip() {
    let (_dir, app) = fixture();
    for (name, value) in [
        ("alpha", json!("undefined")),
        ("beta", json!("")),
        ("delta", json!(0.8455124082255701_f64)),
        ("echo", json!(0.8455124082255701_f64)),
        ("gamma", json!(true)),
    ] {
        app.config
            .put("streams", name, json!({"position":value}))
            .unwrap();
    }
    let mut cursor = None::<String>;
    for name in ["alpha", "beta", "delta", "echo", "gamma"] {
        let mut pairs = vec![("sort", "position"), ("limit", "1")];
        if let Some(ref c) = cursor {
            pairs.push(("cursor", c));
        }
        let body = page(&app, "streams", &pairs, &[name], 5).await;
        cursor = body["next"].as_str().map(str::to_owned);
    }
    assert!(cursor.is_none());
}

#[tokio::test]
async fn cursors_bind_collection_sort_filters_and_search_but_allow_limit_and_select() {
    let (_dir, app) = fixture();
    let first = page(
        &app,
        "streams",
        &[
            ("sort", "position"),
            ("title", "Owned"),
            ("q", "Owned"),
            ("limit", "1"),
        ],
        &["alpha"],
        4,
    )
    .await;
    let c = first["next"].as_str().unwrap();
    for (kind, extra) in [
        (
            "templates",
            vec![("sort", "position"), ("title", "Owned"), ("q", "Owned")],
        ),
        (
            "streams",
            vec![("sort", "-position"), ("title", "Owned"), ("q", "Owned")],
        ),
        (
            "streams",
            vec![("sort", "position"), ("title", "Other"), ("q", "Owned")],
        ),
        (
            "streams",
            vec![("sort", "position"), ("title", "Owned"), ("q", "other")],
        ),
    ] {
        let mut pairs = extra;
        pairs.push(("cursor", c));
        assert_eq!(
            get(&app, kind, &pairs, true).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    page(
        &app,
        "streams",
        &[
            ("sort", "position"),
            ("title", "Owned"),
            ("q", "Owned"),
            ("limit", "2"),
            ("select", "name"),
            ("cursor", c),
        ],
        &["beta", "delta"],
        4,
    )
    .await;
}

#[tokio::test]
async fn reference_name_bounds_support_forward_backward_and_descending() {
    let (_dir, app) = fixture();
    for (sort, query, expected) in [
        (
            "name",
            "%24position_gt=1&name_gt=beta",
            vec!["delta", "gamma"],
        ),
        (
            "name",
            "name_lt=delta&%24reversed=true",
            vec!["alpha", "beta"],
        ),
        ("-name", "name_lt=delta", vec!["beta", "alpha"]),
        (
            "-name",
            "name_gt=beta&%24reversed=true",
            vec!["gamma", "delta"],
        ),
    ] {
        page(
            &app,
            "streams",
            &[("sort", sort), ("limit", "2"), ("cursor", &token(query))],
            &expected,
            4,
        )
        .await;
    }
    assert_eq!(
        get(
            &app,
            "streams",
            &[("sort", "position"), ("cursor", &token("name_gt=beta"))],
            true
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn legacy_position_input_remains_accepted_and_maximum_does_not_overflow() {
    let (_dir, app) = fixture();
    let body = page(
        &app,
        "streams",
        &[("limit", "2"), ("cursor", &token("%24position_gt=1"))],
        &["delta", "gamma"],
        4,
    )
    .await;
    assert!(body["prev"].is_string());
    let empty = page(
        &app,
        "streams",
        &[("cursor", &token(&format!("%24position_gt={}", usize::MAX)))],
        &[],
        4,
    )
    .await;
    assert!(empty["next"].is_null() && empty["prev"].is_null());
}

#[tokio::test]
async fn malformed_or_unsupported_cursors_fail_after_authentication_without_mutation() {
    let (dir, app) = fixture();
    let before = std::fs::read(dir.path().join("config.json")).unwrap();
    let malformed = vec![
        "bad!".to_owned(),
        token(""),
        STANDARD.encode([255]),
        token("%GG=x"),
        token("name_gt=%FF"),
        token("name_gt=a&name_gt=b"),
        token("name_gt=a&name_lt=b"),
        token("name_gt=a&extra=x"),
        token("name_gt=a&%24reversed=false"),
        token("%24position_gt=-1"),
        token("%24position_gt=999999999999999999999999999999999999"),
        token("%24flussonix_cursor={}"),
        "A".repeat(24001),
    ];
    for c in malformed {
        assert_eq!(
            get(&app, "streams", &[("cursor", &c)], true).await.0,
            StatusCode::BAD_REQUEST,
            "{c}"
        );
        assert_eq!(
            get(&app, "streams", &[("cursor", &c)], false).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        std::fs::read(dir.path().join("config.json")).unwrap(),
        before
    );
    assert_eq!(app.media.count().await, 0);
}

#[tokio::test]
async fn native_cursor_schema_and_size_are_validated() {
    let (_dir, app) = fixture();
    let first = page(
        &app,
        "streams",
        &[("sort", "position"), ("limit", "1")],
        &["alpha"],
        4,
    )
    .await;
    let original = payload(first["next"].as_str().unwrap());
    for patch in [
        json!({"version":2}),
        json!({"keys":[]}),
        json!({"keys":[{"type":"integer","value":1.5},{"type":"text","value":"alpha"}]}),
        json!({"keys":[{"type":"float","value":9218868437227405312_u64},{"type":"text","value":"alpha"}]}),
        json!({"keys":[{"type":"text","value":"undefined"},{"type":"text","value":"alpha"}]}),
        json!({"keys":[{"type":"text","value":{"nested":true}}]}),
        json!({"extra":true}),
        json!({"direction":"sideways"}),
        json!({"keys":[{"type":"text","value":"x".repeat(17000)}]}),
    ] {
        let mut v = original.clone();
        v.as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        assert_eq!(
            get(
                &app,
                "streams",
                &[("sort", "position"), ("cursor", &native(&v))],
                true
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn exhausted_boundaries_and_empty_collections_return_null_navigation() {
    let (_dir, app) = fixture();
    for query in ["name_gt=zzzz", "name_lt=aaaa&%24reversed=true"] {
        let body = page(&app, "streams", &[("cursor", &token(query))], &[], 4).await;
        assert!(body["next"].is_null() && body["prev"].is_null());
    }
    for name in ["alpha", "beta", "delta", "gamma"] {
        app.config.delete("streams", name).unwrap();
    }
    let body = page(&app, "streams", &[], &[], 0).await;
    assert!(body["next"].is_null() && body["prev"].is_null());
}

#[tokio::test]
async fn oversized_sort_boundaries_return_an_actionable_error() {
    let (_dir, app) = fixture();
    app.config
        .put("streams", "alpha", json!({"title":"A".repeat(17_000)}))
        .unwrap();
    let (status, body) = get(&app, "streams", &[("sort", "title"), ("limit", "1")], true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string()
            .contains("choose smaller scalar sort fields")
    );
    // Large unselected data does not inflate an identity-only boundary.
    page(
        &app,
        "streams",
        &[("limit", "1"), ("select", "name")],
        &["alpha"],
        4,
    )
    .await;
}

#[tokio::test]
async fn successful_pagination_is_read_only_and_other_collections_keep_legacy_envelopes() {
    let (dir, app) = fixture();
    for hostname in ["owned-a", "owned-b"] {
        app.config
            .put("peers", hostname, json!({"api_url":"http://127.0.0.1:1"}))
            .unwrap();
    }
    let before = std::fs::read(dir.path().join("config.json")).unwrap();
    let first = page(&app, "streams", &[("limit", "2")], &["alpha", "beta"], 4).await;
    let last = page(
        &app,
        "streams",
        &[("limit", "2"), ("cursor", first["next"].as_str().unwrap())],
        &["delta", "gamma"],
        4,
    )
    .await;
    page(
        &app,
        "streams",
        &[("limit", "2"), ("cursor", last["prev"].as_str().unwrap())],
        &["alpha", "beta"],
        4,
    )
    .await;
    let (status, peers) = get(&app, "peers", &[("limit", "1")], true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(peers["next"], "JTI0cG9zaXRpb25fZ3Q9MA==");
    assert!(peers["prev"].is_null());
    assert_eq!(peers["peers"][0]["hostname"], "owned-a");
    let (status, empty) = get(
        &app,
        "peers",
        &[("cursor", &token(&format!("%24position_gt={}", usize::MAX)))],
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty["peers"], json!([]));
    assert_eq!(
        std::fs::read(dir.path().join("config.json")).unwrap(),
        before
    );
    assert_eq!(app.media.count().await, 0);
}
