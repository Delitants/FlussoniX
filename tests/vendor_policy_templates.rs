//! Catch lost inherited protection, implicit public access and order-dependent grants.
use flussonix::{
    playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest},
    source_directory::flussonic_policy::{PolicyFailure as Failure, PolicySet},
};
use serde_json::{Value, json};

fn set(source: Value, groups: Value, templates: Value) -> PolicySet {
    PolicySet::with_templates(&source, &groups, &templates, &json!([])).unwrap()
}
fn native(config: Value) -> Policy {
    Policy::from_config(&config, &json!({})).unwrap()
}

#[test]
fn direct_callback_override_retains_independent_template_token_guard() {
    let policies = set(
        json!({"template":"protected","on_play":"true"}),
        json!({}),
        json!({"protected":{"on_play":"https://base.invalid/check","flussonix_token_sha256":"a".repeat(64)}}),
    );
    assert_eq!(
        policies.for_groups(&json!([])).unwrap(),
        native(json!({"flussonix_token_sha256":"a".repeat(64)}))
    );
}

#[test]
fn source_and_group_templates_obey_atomic_callback_replacement() {
    let policies = set(
        json!({"template":"base"}),
        json!({"sport":{"template":"paid","on_play":"https://override.invalid"}}),
        json!({
            "base":{"on_play":{"url":"https://base.invalid","session_keys":["name","proto","token"],"max_sessions":1},"flussonix_token_sha256":"b".repeat(64)},
            "paid":{"on_play":{"url":"https://paid.invalid","max_sessions":2}}
        }),
    );
    assert_eq!(
        policies.for_groups(&json!([])).unwrap(),
        native(
            json!({"on_play":{"url":"https://base.invalid","session_keys":["name","proto","token"],"max_sessions":1},"flussonix_token_sha256":"b".repeat(64)})
        )
    );
    assert_eq!(
        policies.for_groups(&json!(["sport"])).unwrap(),
        native(
            json!({"on_play":"https://override.invalid","flussonix_token_sha256":"b".repeat(64)})
        )
    );
}

#[test]
fn inherited_group_fields_conflict_after_backend_resolution_independent_of_order() {
    let policies = PolicySet::with_templates(
        &json!({}),
        &json!({"a":{"template":"paid"},"same":{"on_play":"https://paid.invalid"},"other":{"template":"public"}}),
        &json!({"paid":{"on_play":"auth://billing"},"public":{"on_play":"true"}}),
        &json!([{"name":"billing","url":"https://paid.invalid"}]),
    ).unwrap();
    for names in [json!(["a", "same"]), json!(["same", "a"])] {
        assert_eq!(
            policies.for_groups(&names).unwrap(),
            native(json!({"on_play":"https://paid.invalid"}))
        );
    }
    for names in [json!(["a", "other"]), json!(["other", "a"])] {
        assert!(matches!(
            policies.for_groups(&names),
            Err(Failure::Ambiguous)
        ));
    }
}

#[test]
fn empty_templates_and_groups_do_not_establish_public_authority() {
    let policies = set(
        json!({"template":"empty"}),
        json!({"g":{"template":"empty"}}),
        json!({"empty":{}}),
    );
    for groups in [json!([]), json!(["g"])] {
        assert!(matches!(
            policies.for_groups(&groups),
            Err(Failure::Unresolved)
        ));
    }
}

#[test]
fn missing_or_malformed_template_references_fail_even_when_shadowed() {
    for reference in [
        json!("missing"),
        json!("../bad"),
        json!(null),
        json!(false),
        json!([]),
    ] {
        let expected = if reference == json!("missing") {
            Failure::Unresolved
        } else {
            Failure::Invalid
        };
        for (source, groups) in [
            (json!({"template":reference,"on_play":"true"}), json!({})),
            (
                json!({"on_play":"true"}),
                json!({"unused":{"template":reference,"on_play":"true"}}),
            ),
        ] {
            assert_eq!(
                PolicySet::with_templates(&source, &groups, &json!({}), &json!([])).err(),
                Some(expected)
            );
        }
    }
}

#[test]
fn unused_templates_and_shadowed_direct_values_validate_without_projection_loss() {
    for (bad, expected) in [
        (json!({"template":"other"}), Failure::Unsupported),
        (json!({"protocols":["hls"]}), Failure::Unsupported),
        (json!({"domains":["example.invalid"]}), Failure::Unsupported),
        (
            json!({"on_play":{"url":"https://billing.invalid","soft_limitation":true}}),
            Failure::Invalid,
        ),
        (json!({"on_play":false}), Failure::Invalid),
        (json!(null), Failure::Invalid),
    ] {
        assert_eq!(
            PolicySet::with_templates(
                &json!({"on_play":"true"}),
                &json!({}),
                &json!({"unused":bad}),
                &json!([])
            )
            .err(),
            Some(expected)
        );
    }
    assert_eq!(
        PolicySet::with_templates(
            &json!({"template":"public","on_play":false}),
            &json!({}),
            &json!({"public":{"on_play":"true"}}),
            &json!([])
        )
        .err(),
        Some(Failure::Invalid)
    );
    assert_eq!(
        PolicySet::with_templates(
            &json!({"on_play":"true"}),
            &json!({}),
            &json!({"../bad":{}}),
            &json!([])
        )
        .err(),
        Some(Failure::Invalid)
    );
}

#[test]
fn template_collection_and_layer_bounds_are_enforced() {
    let many: serde_json::Map<String, Value> =
        (0..129).map(|i| (format!("t{i}"), json!({}))).collect();
    assert_eq!(
        PolicySet::with_templates(
            &json!({"on_play":"true"}),
            &json!({}),
            &Value::Object(many),
            &json!([])
        )
        .err(),
        Some(Failure::Limit)
    );
    assert_eq!(
        PolicySet::with_templates(
            &json!({"on_play":"true"}),
            &json!({}),
            &json!({"huge":{"on_play":format!("https://billing.invalid/{}","x".repeat(16384))}}),
            &json!([])
        )
        .err(),
        Some(Failure::Limit)
    );
    for templates in [json!(null), json!([])] {
        assert_eq!(
            PolicySet::with_templates(
                &json!({"on_play":"true"}),
                &json!({}),
                &templates,
                &json!([])
            )
            .err(),
            Some(Failure::Invalid)
        );
    }
    let maximum: serde_json::Map<String, Value> = (0..128)
        .map(|i| (format!("t{i}"), json!({"on_play":"true"})))
        .collect();
    let policies = set(
        json!({"template":"t127"}),
        json!({}),
        Value::Object(maximum),
    );
    assert_eq!(policies.for_groups(&json!([])).unwrap(), native(json!({})));
}

#[tokio::test]
async fn inherited_callback_denial_token_guard_and_session_limit_use_native_authorizer() {
    use axum::{extract::Query, http::StatusCode, routing::get};
    use std::collections::HashMap;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/auth",
                get(|Query(q): Query<HashMap<String, String>>| async move {
                    (
                        if q.get("proto").is_some_and(|v| v == "deny") {
                            StatusCode::FORBIDDEN
                        } else {
                            StatusCode::OK
                        },
                        [("X-UserId", "one-account")],
                    )
                }),
            ),
        )
        .await
        .unwrap();
    });
    let policies = set(
        json!({"template":"paid"}),
        json!({}),
        json!({"paid":{"on_play":{"url":url,"session_keys":["name","proto","token"],"max_sessions":1},"flussonix_token_sha256":"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"}}),
    );
    let auth = PlaybackAuth::new(16);
    let snapshot = auth
        .publish("owned", Some(policies.for_groups(&json!([])).unwrap()))
        .unwrap();
    let viewer = |proto: &str, token: &str| ViewerRequest {
        name: "owned".into(),
        proto: proto.into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        ..Default::default()
    };
    assert!(matches!(
        auth.authorize(snapshot.clone(), viewer("hls", "bad")).await,
        AuthOutcome::Denied
    ));
    assert!(matches!(
        auth.authorize(snapshot.clone(), viewer("deny", "secret"))
            .await,
        AuthOutcome::Denied
    ));
    let AuthOutcome::Allowed(first) = auth
        .authorize(snapshot.clone(), viewer("hls", "secret"))
        .await
    else {
        panic!("first viewer denied")
    };
    assert!(matches!(
        auth.authorize(snapshot, viewer("m4s", "secret")).await,
        AuthOutcome::Denied
    ));
    drop(first);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}
