//! Breaks caught: missing local authority becoming public, silent policy loss,
//! deep-merging callbacks, inventory-order grants and bypassing native sessions.
use flussonix::{
    playback_auth::{AuthOutcome, PlaybackAuth, Policy, ViewerRequest},
    source_directory::flussonic_policy::{PolicyFailure, PolicySet},
};
use serde_json::{Value, json};

#[test]
fn local_iptv_policy_is_distinct_from_http_callback_backends() {
    let policies = PolicySet::new(&json!({"on_play":"iptv://"}), &json!({}), &json!([])).unwrap();
    let expected =
        flussonix::playback_auth::Policy::from_config(&json!({"on_play":"iptv://"}), &json!({}))
            .unwrap();
    assert_eq!(policies.for_groups(&json!([])).unwrap(), expected);
    assert!(matches!(
        PolicySet::new(
            &json!({"on_play":"true"}),
            &json!({}),
            &json!([{"name":"bad","url":"iptv://"}])
        ),
        Err(PolicyFailure::Invalid)
    ));
}

fn set(source: Value, groups: Value) -> PolicySet {
    PolicySet::new(&source, &groups, &json!([])).unwrap()
}
fn native(cfg: Value) -> Policy {
    Policy::from_config(&cfg, &json!({"auth_backends":[]})).unwrap()
}
fn request(token: &str) -> ViewerRequest {
    ViewerRequest {
        name: "owned".into(),
        proto: "hls".into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        ..Default::default()
    }
}

#[test]
fn missing_local_authority_does_not_become_public() {
    let policies = set(json!({}), json!({"empty":{}}));
    for memberships in [json!([]), json!(["unconfigured"]), json!(["empty"])] {
        assert!(matches!(
            policies.for_groups(&memberships),
            Err(PolicyFailure::Unresolved)
        ));
    }
}

#[test]
fn source_policy_applies_to_ungrouped_and_unconfigured_groups() {
    let policies = set(
        json!({"on_play":{"url":"https://billing.invalid/check","session_keys":["name","proto","token"],"max_sessions":2}}),
        json!({}),
    );
    let expected = native(
        json!({"on_play":{"url":"https://billing.invalid/check","session_keys":["name","proto","token"],"max_sessions":2}}),
    );
    for groups in [json!([]), json!(["unknown"])] {
        assert_eq!(policies.for_groups(&groups).unwrap(), expected);
    }
}

#[test]
fn group_callback_replaces_source_callback_as_a_whole() {
    let policies = set(
        json!({"on_play":{"url":"https://source.invalid","session_keys":["name","proto","token"],"max_sessions":1}}),
        json!({"sports":{"on_play":"https://sports.invalid"}}),
    );
    assert_eq!(
        policies.for_groups(&json!(["sports"])).unwrap(),
        native(json!({"on_play":"https://sports.invalid"}))
    );
    assert_eq!(
        policies.for_groups(&json!([])).unwrap(),
        native(
            json!({"on_play":{"url":"https://source.invalid","session_keys":["name","proto","token"],"max_sessions":1}})
        )
    );
}

#[test]
fn a_group_can_supply_missing_local_authority() {
    let policies = set(
        json!({}),
        json!({"paid":{"on_play":"https://paid.invalid"}}),
    );
    assert_eq!(
        policies.for_groups(&json!(["unknown", "paid"])).unwrap(),
        native(json!({"on_play":"https://paid.invalid"}))
    );
    assert!(matches!(
        policies.for_groups(&json!([])),
        Err(PolicyFailure::Unresolved)
    ));
}

#[test]
fn conflicting_groups_are_rejected_independent_of_order() {
    let policies = set(
        json!({"on_play":"https://source.invalid"}),
        json!({"allow":{"on_play":"true"},"paid":{"on_play":"https://paid.invalid"},"limited":{"on_play":{"url":"https://paid.invalid","max_sessions":1}}}),
    );
    for names in [
        json!(["allow", "paid"]),
        json!(["paid", "allow"]),
        json!(["paid", "limited"]),
        json!(["limited", "paid"]),
    ] {
        assert!(matches!(
            policies.for_groups(&names),
            Err(PolicyFailure::Ambiguous)
        ));
    }
}

#[test]
fn equivalent_group_policies_and_empty_layers_do_not_conflict() {
    let policies = set(
        json!({}),
        json!({"a":{"on_play":"https://billing.invalid"},"b":{"on_play":{"url":"https://billing.invalid","session_keys":["name","proto","ip","token"]}},"empty":{}}),
    );
    for names in [json!(["a", "empty", "b"]), json!(["b", "a", "empty"])] {
        assert_eq!(
            policies.for_groups(&names).unwrap(),
            native(json!({"on_play":"https://billing.invalid"}))
        );
    }
}

#[test]
fn unsupported_or_malformed_layers_fail_even_when_shadowed_or_unmatched() {
    for bad in [
        json!(null),
        json!([]),
        json!({"template":"secure"}),
        json!({"protocols":["hls"]}),
        json!({"on_play":null}),
        json!({"on_play":true}),
        json!({"on_play":"false"}),
        json!({"on_play":{}}),
        json!({"on_play":{"url":"true"}}),
        json!({"on_play":{"url":"https://billing.invalid","domains":["example.org"]}}),
        json!({"on_play":{"url":"https://billing.invalid","allowed_countries":["US"]}}),
        json!({"on_play":{"url":"https://billing.invalid","disallowed_countries":["DE"]}}),
        json!({"on_play":{"url":"https://billing.invalid","soft_limitation":false}}),
        json!({"on_play":{"url":"https://billing.invalid","extra":{}}}),
        json!({"on_play":{"url":"https://billing.invalid","session_keys":["token"]}}),
        json!({"on_play":{"url":"https://billing.invalid","max_sessions":0}}),
        json!({"flussonix_token_sha256":null}),
        json!({"flussonix_token_sha256":"broken"}),
        json!({"on_play":"file:///private"}),
        json!({"on_play":"auth://missing"}),
    ] {
        assert!(PolicySet::new(&bad, &json!({"override":{"on_play":"true"}}), &json!([])).is_err());
        assert!(
            PolicySet::new(
                &json!({"on_play":"true"}),
                &json!({"unused":bad}),
                &json!([])
            )
            .is_err()
        );
    }
}

#[test]
fn backend_resolution_is_local_unique_and_rejects_unsupported_backend_rules() {
    let source = json!({"on_play":"auth://billing"});
    let groups = json!({"equivalent":{"on_play":"https://billing.invalid"}});
    let backends = json!([{"name":"billing","url":"https://billing.invalid"}]);
    let policies = PolicySet::new(&source, &groups, &backends).unwrap();
    assert_eq!(
        policies.for_groups(&json!(["equivalent"])).unwrap(),
        native(json!({"on_play":"https://billing.invalid"}))
    );
    for bad in [
        json!(null),
        json!({}),
        json!([{}]),
        json!([{"name":"billing","url":"https://billing.invalid","domains":["example.org"]}]),
        json!([{"name":"billing","url":"auth://other"}]),
        json!([{"name":"billing","url":"https://a.invalid"},{"name":"billing","url":"https://b.invalid"}]),
        json!([{"name":"../bad","url":"https://billing.invalid"}]),
        json!([{"name":"billing","url":"true"}]),
    ] {
        assert!(PolicySet::new(&source, &groups, &bad).is_err());
    }
}

#[test]
fn named_and_literal_group_callbacks_are_compared_after_resolution() {
    let policies = PolicySet::new(
        &json!({}),
        &json!({"a":{"on_play":"auth://billing"},"b":{"on_play":"https://billing.invalid"}}),
        &json!([{"name":"billing","url":"https://billing.invalid"}]),
    )
    .unwrap();
    assert_eq!(
        policies.for_groups(&json!(["a", "b"])).unwrap(),
        native(json!({"on_play":"https://billing.invalid"}))
    );
}

#[test]
fn membership_metadata_is_explicit_bounded_and_validated() {
    let policies = set(json!({"on_play":"true"}), json!({}));
    for bad in [
        json!(null),
        json!({}),
        json!("sports"),
        json!([null]),
        json!(["a", "a"]),
        json!(["../bad"]),
        json!(["x".repeat(257)]),
        json!((0..65).map(|n| format!("g{n}")).collect::<Vec<_>>()),
    ] {
        assert!(policies.for_groups(&bad).is_err());
    }
    assert_eq!(
        policies
            .for_groups(&json!((0..64).map(|n| format!("g{n}")).collect::<Vec<_>>()))
            .unwrap(),
        native(json!({}))
    );
}

#[test]
fn policy_collections_and_layers_have_bounds() {
    for bad in [json!(null), json!([]), json!({"":{}}), json!({"../bad":{}})] {
        assert!(PolicySet::new(&json!({"on_play":"true"}), &bad, &json!([])).is_err());
    }
    let groups: serde_json::Map<String, Value> =
        (0..129).map(|n| (format!("g{n}"), json!({}))).collect();
    assert!(matches!(
        PolicySet::new(&json!({}), &Value::Object(groups), &json!([])),
        Err(PolicyFailure::Limit)
    ));
    let backends: Vec<Value> = (0..129)
        .map(|n| json!({"name":format!("b{n}"),"url":"https://billing.invalid"}))
        .collect();
    assert!(matches!(
        PolicySet::new(&json!({}), &json!({}), &json!(backends)),
        Err(PolicyFailure::Limit)
    ));
    assert!(
        PolicySet::new(
            &json!({"on_play":format!("https://billing.invalid/{}","x".repeat(16384))}),
            &json!({}),
            &json!([])
        )
        .is_err()
    );
}

#[tokio::test]
async fn explicit_public_override_does_not_drop_independent_local_token_guard() {
    // SHA256("secret"), independently pinned rather than computed by the parser.
    let hash = "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b";
    let policies = set(
        json!({"on_play":"https://billing.invalid","flussonix_token_sha256":hash.to_uppercase()}),
        json!({"free":{"on_play":"true"}}),
    );
    let policy = policies.for_groups(&json!(["free"])).unwrap();
    assert_eq!(policy, native(json!({"flussonix_token_sha256":hash})));
    let auth = PlaybackAuth::new(16);
    let snapshot = auth.publish("owned", Some(policy)).unwrap();
    assert!(matches!(
        auth.authorize(snapshot.clone(), request("wrong")).await,
        AuthOutcome::Denied
    ));
    assert!(matches!(
        auth.authorize(snapshot, request("secret")).await,
        AuthOutcome::Allowed(_)
    ));
}

#[test]
fn independent_group_fields_compose_and_conflicting_token_guards_are_rejected() {
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    let policies = set(
        json!({}),
        json!({"callback":{"on_play":"https://paid.invalid"},"guard":{"flussonix_token_sha256":a},"other":{"flussonix_token_sha256":b}}),
    );
    assert_eq!(
        policies.for_groups(&json!(["callback", "guard"])).unwrap(),
        native(json!({"on_play":"https://paid.invalid","flussonix_token_sha256":"a".repeat(64)}))
    );
    for groups in [json!(["guard", "other"]), json!(["other", "guard"])] {
        assert!(matches!(
            policies.for_groups(&groups),
            Err(PolicyFailure::Ambiguous)
        ));
    }
}

#[tokio::test]
async fn explicit_allow_and_token_only_authorities_use_native_authorization() {
    for source in [
        json!({"on_play":"true"}),
        json!({"flussonix_token_sha256":"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"}),
    ] {
        let policy = set(source, json!({})).for_groups(&json!([])).unwrap();
        let auth = PlaybackAuth::new(16);
        let snapshot = auth.publish("owned", Some(policy)).unwrap();
        assert!(matches!(
            auth.authorize(snapshot, request("secret")).await,
            AuthOutcome::Allowed(_)
        ));
    }
}

#[tokio::test]
async fn compiled_callback_denial_and_session_limits_are_enforced_by_real_authorizer() {
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
                    let status = if q.get("token").is_some_and(|t| t == "deny") {
                        StatusCode::FORBIDDEN
                    } else {
                        StatusCode::OK
                    };
                    (status, [("X-UserId", "owned-account")])
                }),
            ),
        )
        .await
        .unwrap();
    });
    let policies = set(
        json!({"on_play":"true"}),
        json!({"paid":{"on_play":{"url":url,"session_keys":["name","proto","token"],"max_sessions":1}}}),
    );
    let auth = PlaybackAuth::new(16);
    let snapshot = auth
        .publish(
            "owned",
            Some(policies.for_groups(&json!(["paid"])).unwrap()),
        )
        .unwrap();
    assert!(matches!(
        auth.authorize(snapshot.clone(), request("deny")).await,
        AuthOutcome::Denied
    ));
    let first = auth.authorize(snapshot.clone(), request("one")).await;
    let AuthOutcome::Allowed(first) = first else {
        panic!("paid viewer should be allowed")
    };
    assert!(matches!(
        auth.authorize(snapshot, request("two")).await,
        AuthOutcome::Denied
    ));
    drop(first);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}

#[test]
fn unused_backend_url_must_be_a_string_not_a_structured_policy() {
    let bad = json!([{"name":"unused","url":{"url":"https://billing.invalid"}}]);
    assert!(PolicySet::new(&json!({"on_play":"true"}), &json!({}), &bad).is_err());
}
