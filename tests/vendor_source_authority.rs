//! Breaks caught: discarded local rules, unknown membership as public access,
//! stale/cross-source publication and non-atomic source configuration changes.
use flussonix::{
    playback_auth::Policy,
    source_directory::{
        flussonic::{CandidateKind, EligibilityFailure, InventoryStream, StreamProvenance},
        flussonic_config::{ProjectionFailure as Failure, SourceAuthority},
        flussonic_policy::PolicyFailure,
    },
};
use serde_json::{Value, json};

fn source() -> Value {
    json!({"url":"m4ss://origin.invalid:8443","cluster_key":"owned-cluster-key","on_play":"true"})
}
fn row(name: &str, groups: &[&str]) -> InventoryStream {
    InventoryStream {
        name: name.into(),
        is_static: true,
        disabled: false,
        named_by: Some(StreamProvenance::Config),
        groups: Some(groups.iter().map(|s| (*s).into()).collect()),
    }
}
fn authority(config: &Value) -> SourceAuthority {
    SourceAuthority::new(config, &json!([])).unwrap()
}
fn policy(config: Value) -> Policy {
    Policy::from_config(&config, &json!({})).unwrap()
}

#[test]
fn local_paid_group_pin_rejects_forged_public_membership() {
    let mut config = source();
    config["group_config"] = json!({
        "paid": {"flussonix_token_sha256": "a".repeat(64)}
    });
    config["flussonix_group_pins"] = json!({"music": ["paid"]});
    let authority = authority(&config);
    let snapshot = authority.snapshot().unwrap();
    assert!(snapshot.candidate(&row("music", &[])).is_err());
    assert!(snapshot.candidate(&row("music", &["public"])).is_err());
    let candidate = snapshot.candidate(&row("music", &["paid"])).unwrap();
    authority
        .at_current(&candidate, |view| {
            assert_eq!(
                view.policy,
                &policy(json!({
                    "flussonix_token_sha256": "a".repeat(64)
                }))
            );
        })
        .unwrap();
}

#[test]
fn omitted_membership_uses_only_an_exact_local_pin() {
    let mut config = source();
    config["prefix"] = json!("region");
    config["group_config"] = json!({"paid":{"flussonix_token_sha256":"b".repeat(64)}});
    config["flussonix_group_pins"] = json!({"region/news":["paid"]});
    let a = SourceAuthority::new_modern(&config, &json!([]), std::time::Duration::from_secs(3))
        .unwrap();
    let snapshot = a.snapshot().unwrap();
    let mut pinned = row("region/news", &[]);
    pinned.groups = None;
    let selected = snapshot.candidate(&pinned).unwrap();
    a.at_current(&selected, |view| {
        assert_eq!(
            view.policy,
            &policy(json!({"flussonix_token_sha256":"b".repeat(64)}))
        );
    })
    .unwrap();
    assert!(snapshot.candidate(&row("region/news2", &["paid"])).is_err());
    let mut sibling = row("region/news2", &[]);
    sibling.groups = None;
    assert!(snapshot.candidate(&sibling).is_err());
}

#[test]
fn group_pins_require_bounded_exact_streams_and_known_unique_groups() {
    let mut source = source();
    source["group_config"] = json!({"paid":{"flussonix_token_sha256":"c".repeat(64)}});
    for pins in [
        json!([]),
        json!({"news/*":["paid"]}),
        json!({"news":[]}),
        json!({"news":["other"]}),
        json!({"news":["paid","paid"]}),
        json!({"news":"paid"}),
        json!({"news":vec!["paid";65]}),
        json!(
            (0..129)
                .map(|i| (format!("news/{i}"), json!(["paid"])))
                .collect::<serde_json::Map<String, Value>>()
        ),
    ] {
        source["flussonix_group_pins"] = pins;
        assert!(SourceAuthority::new(&source, &json!([])).is_err());
    }
}

#[test]
fn uniform_group_policy_needs_no_pin() {
    let mut config = source();
    config["group_config"] = json!({"public":{"on_play":"true"}});
    let a = authority(&config);
    let snapshot = a.snapshot().unwrap();
    assert!(snapshot.candidate(&row("news", &[])).is_ok());
    assert!(snapshot.candidate(&row("news", &["public"])).is_ok());
}

// Break caught: projecting group objects drops protection or changes name binding.
#[test]
fn complete_source_group_projection_preserves_local_policy_and_literal_names() {
    let mut config = source();
    config["flussonix_token_sha256"] = json!("a".repeat(64));
    config["group_config"] = json!({"日本語/HD":{"name":"日本語/HD","on_play":"auth://billing"}});
    config["flussonix_group_pins"] = json!({"region/space %2F name":["日本語/HD"]});
    let a = SourceAuthority::new(
        &config,
        &json!([{"name":"billing","url":"https://billing.invalid/check"}]),
    )
    .unwrap();
    let snapshot = a.snapshot().unwrap();
    let candidate = snapshot
        .candidate(&row("region/space %2F name", &["日本語/HD"]))
        .unwrap();
    let expected = policy(
        json!({"on_play":"https://billing.invalid/check","flussonix_token_sha256":"a".repeat(64)}),
    );
    a.at_current(&candidate, |view| {
        assert_eq!(view.name, "region/space %2F name");
        assert_eq!(view.source, "m4ss://origin.invalid:8443");
        assert_eq!(view.policy, &expected);
        assert_eq!(view.kind, CandidateKind::Static);
        assert!(view.selected_static);
    })
    .unwrap();
}

// Break caught: an unknown/unsupported source option is discarded before policy projection.
#[test]
fn every_complete_source_option_must_be_supported() {
    for (key, value) in [
        ("template", json!("protected")),
        ("cache", json!({})),
        ("dvr", json!({})),
        ("domains", json!(["allowed.invalid"])),
        ("countries", json!(["US"])),
        ("soft_limitation", json!(true)),
        ("on_publish", json!("https://billing.invalid")),
        ("extra", json!({"on_play":"deny"})),
        ("drain", json!(true)),
        ("hostname", json!("alias")),
        ("protocols", json!(["hls"])),
        ("stats", json!({})),
    ] {
        let mut config = source();
        config[key] = value;
        assert_eq!(
            SourceAuthority::new(&config, &json!([])).err(),
            Some(Failure::Unsupported),
            "{key}"
        );
    }
}

// Break caught: unmatched group rules or mismatched redundant names are silently dropped.
#[test]
fn all_group_objects_validate_before_any_candidate_exists() {
    for (group, want) in [
        (json!({"name":"other","on_play":"true"}), Failure::Invalid),
        (json!({"name":null}), Failure::Invalid),
        (json!(null), Failure::Invalid),
        (json!({"cache":{}}), Failure::Unsupported),
        (json!({"segment_duration":6}), Failure::Unsupported),
        (json!({"dvr":{}}), Failure::Unsupported),
        (json!({"template":"protected"}), Failure::Unsupported),
        (
            json!({"on_play":true}),
            Failure::Policy(PolicyFailure::Invalid),
        ),
    ] {
        let mut config = source();
        config["group_config"] = json!({"unused":group});
        assert_eq!(SourceAuthority::new(&config, &json!([])).err(), Some(want));
    }
    for group_config in [json!(null), json!([]), json!({"../bad":{}})] {
        let mut config = source();
        config["group_config"] = group_config;
        assert_eq!(
            SourceAuthority::new(&config, &json!([])).err(),
            Some(Failure::Invalid)
        );
    }
    let mut config = source();
    config["on_play"] = json!(false);
    config["group_config"] = json!({"override":{"on_play":"true"}});
    assert_eq!(
        SourceAuthority::new(&config, &json!([])).err(),
        Some(Failure::Policy(PolicyFailure::Invalid))
    );
}

// Break caught: implicit credentials, unsafe source identities or protocol fallback enter a snapshot.
#[test]
fn source_identity_and_privileged_key_require_explicit_valid_values() {
    for url in [
        "m4f://origin.invalid",
        "m4fs://origin.invalid/",
        "m4s://127.0.0.1:8080",
        "m4ss://[::1]:8443/",
    ] {
        let mut config = source();
        config["url"] = json!(url);
        assert!(SourceAuthority::new(&config, &json!([])).is_ok());
    }
    for url in [
        "http://origin.invalid",
        "m4f://user:secret@origin.invalid",
        "m4ss://host/stream",
        "m4s://host?token=x",
        "m4s://host#x",
        "m4s://host:0",
        "m4s://host:",
        "m4s://host%2Fpath",
        "m4s://host\\path",
        "m4s://host/\n",
    ] {
        let mut config = source();
        config["url"] = json!(url);
        assert_eq!(
            SourceAuthority::new(&config, &json!([])).err(),
            Some(Failure::Invalid)
        );
    }
    for field in ["url", "cluster_key"] {
        for value in [None, Some(json!(null)), Some(json!(false)), Some(json!(""))] {
            let mut config = source();
            if let Some(value) = value {
                config[field] = value;
            } else {
                config.as_object_mut().unwrap().remove(field);
            }
            assert_eq!(
                SourceAuthority::new(&config, &json!([])).err(),
                Some(Failure::Invalid)
            );
        }
    }
    for key in ["x".repeat(4096), "ключ".to_owned()] {
        let mut c = source();
        c["cluster_key"] = json!(key);
        assert!(SourceAuthority::new(&c, &json!([])).is_ok());
    }
    let mut c = source();
    c["cluster_key"] = json!("x".repeat(4097));
    assert_eq!(
        SourceAuthority::new(&c, &json!([])).err(),
        Some(Failure::Invalid)
    );
}

// Break caught: namespace matching rewrites names, matches sibling prefixes, or ignores relative exclusions.
#[test]
fn source_namespace_and_exclusions_apply_before_policy_binding() {
    let mut config = source();
    config["prefix"] = json!("region");
    config["except"] = json!(["blocked", "kids/*"]);
    let a = authority(&config);
    let s = a.snapshot().unwrap();
    for name in [
        "region",
        "regional/news",
        "other/news",
        "region/blocked",
        "region/kids/one",
    ] {
        assert_eq!(
            s.candidate(&row(name, &[])).err(),
            Some(Failure::Excluded),
            "{name}"
        );
    }
    for name in ["region/news", "region/kids", "region/kids2/one"] {
        let c = s.candidate(&row(name, &[])).unwrap();
        assert_eq!(a.at_current(&c, |v| v.name.to_owned()).unwrap(), name);
    }
}

// Break caught: only hides on-demand playback or static=false is eagerly selected.
#[test]
fn static_selection_is_separate_from_candidate_visibility() {
    for (only, want) in [
        (None, true),
        (Some(json!([])), false),
        (Some(json!(["news"])), true),
        (Some(json!(["other"])), false),
    ] {
        let mut config = source();
        config["prefix"] = json!("region");
        if let Some(only) = only {
            config["only"] = only;
        }
        let a = authority(&config);
        let s = a.snapshot().unwrap();
        let c = s.candidate(&row("region/news", &[])).unwrap();
        assert_eq!(a.at_current(&c, |v| v.selected_static).unwrap(), want);
        let mut r = row("region/news", &[]);
        r.is_static = false;
        let c = s.candidate(&r).unwrap();
        assert_eq!(
            a.at_current(&c, |v| (v.kind, v.selected_static)).unwrap(),
            (CandidateKind::OnDemand, false)
        );
    }
}

// Break caught: invalid or unbounded filters are accepted and turn selection into a permissive default.
#[test]
fn all_source_filters_validate_with_native_limits() {
    for prefix in [
        json!(null),
        json!(""),
        json!("../bad"),
        json!("news/"),
        json!("news*"),
    ] {
        let mut config = source();
        config["prefix"] = prefix;
        assert_eq!(
            SourceAuthority::new(&config, &json!([])).err(),
            Some(Failure::Invalid)
        );
    }
    for field in ["only", "except"] {
        for patterns in [
            json!(null),
            json!("news"),
            json!([1]),
            json!(["*"]),
            json!(["a*"]),
            json!(["a/*/b"]),
            json!(["../bad"]),
            json!(["x".repeat(257)]),
            json!(vec!["news"; 1025]),
        ] {
            let mut config = source();
            config[field] = patterns;
            assert!(SourceAuthority::new(&config, &json!([])).is_err());
        }
        let mut config = source();
        config[field] = json!(vec!["news"; 1024]);
        assert!(SourceAuthority::new(&config, &json!([])).is_ok());
    }
}

// Break caught: unknown groups, disabled/relayed origins or mutated facts become public local candidates.
#[test]
fn inventory_preflight_is_mandatory_even_for_explicit_public_source_policy() {
    let a = authority(&source());
    let s = a.snapshot().unwrap();
    let mut cases = vec![];
    let mut r = row("news", &[]);
    r.groups = None;
    cases.push((r, EligibilityFailure::UnknownGroups));
    let mut r = row("news", &[]);
    r.disabled = true;
    cases.push((r, EligibilityFailure::Disabled));
    let mut r = row("news", &[]);
    r.named_by = None;
    cases.push((r, EligibilityFailure::UnknownProvenance));
    let mut r = row("news", &[]);
    r.named_by = Some(StreamProvenance::Remote);
    cases.push((r, EligibilityFailure::Relayed));
    let mut r = row("news", &[]);
    r.named_by = Some(StreamProvenance::External);
    cases.push((r, EligibilityFailure::External));
    cases.push((row("../news", &[]), EligibilityFailure::Invalid));
    cases.push((row("news", &["g", "g"]), EligibilityFailure::Invalid));
    for (row, want) in cases {
        assert_eq!(s.candidate(&row).err(), Some(Failure::Ineligible(want)));
    }
}

// Break caught: absent local authority or group order selects a public grant.
#[test]
fn unresolved_policy_and_conflicting_groups_cannot_bind_candidates() {
    let mut config = source();
    config.as_object_mut().unwrap().remove("on_play");
    let a = authority(&config);
    assert_eq!(
        a.snapshot().unwrap().candidate(&row("news", &[])).err(),
        Some(Failure::Policy(PolicyFailure::Unresolved))
    );
    config["group_config"] = json!({"public":{"on_play":"true"},"protected":{"on_play":"https://billing.invalid/check"}});
    let unpinned = authority(&config);
    assert_eq!(
        unpinned
            .snapshot()
            .unwrap()
            .candidate(&row("news", &["public"]))
            .err(),
        Some(Failure::MissingGroupPin)
    );
    config["flussonix_group_pins"] = json!({"news":["protected"]});
    let a = authority(&config);
    let s = a.snapshot().unwrap();
    for groups in [["public", "protected"], ["protected", "public"]] {
        assert_eq!(
            s.candidate(&row("news", &groups)).err(),
            Some(Failure::GroupPinMismatch)
        );
    }
    let c = s.candidate(&row("news", &["protected"])).unwrap();
    assert_eq!(
        a.at_current(&c, |v| v.policy.url.clone()).unwrap(),
        Some("https://billing.invalid/check".into())
    );
}

// Break caught: a failed complete edit partially replaces source authority.
#[test]
fn invalid_replacement_is_atomic_and_keeps_current_candidate() {
    let a = authority(&source());
    let s = a.snapshot().unwrap();
    let c = s.candidate(&row("news", &[])).unwrap();
    let mut config = source();
    config["group_config"] = json!({"unused":{"dvr":{}}});
    assert_eq!(a.replace(&config, &json!([])), Err(Failure::Unsupported));
    assert!(a.at_current(&c, |_| ()).is_ok());
    assert_eq!(a.snapshot().unwrap().revision(), s.revision());
}

// Break caught: delayed old snapshots survive replacement, including an identical edit.
#[test]
fn every_valid_replacement_invalidates_existing_and_late_candidates() {
    let a = authority(&source());
    let old = a.snapshot().unwrap();
    let c = old.candidate(&row("news", &[])).unwrap();
    a.replace(&source(), &json!([])).unwrap();
    let mut called = false;
    assert_eq!(
        a.at_current(&c, |_| {
            called = true;
        }),
        Err(Failure::Stale)
    );
    assert!(!called);
    let late = old.candidate(&row("news", &[])).unwrap();
    assert_eq!(a.at_current(&late, |_| ()), Err(Failure::Stale));
    let current = a.snapshot().unwrap();
    assert!(current.revision() > old.revision());
    assert!(
        a.at_current(&current.candidate(&row("news", &[])).unwrap(), |_| ())
            .is_ok()
    );
}

// Break caught: unchanged source bytes retain an obsolete named backend's authority.
#[test]
fn backend_replacement_fences_policy_and_does_not_reuse_old_resolution() {
    let mut config = source();
    config["on_play"] = json!("auth://billing");
    let a = SourceAuthority::new(
        &config,
        &json!([{"name":"billing","url":"https://old.invalid/check"}]),
    )
    .unwrap();
    let old = a.snapshot().unwrap().candidate(&row("news", &[])).unwrap();
    a.replace(
        &config,
        &json!([{"name":"billing","url":"https://new.invalid/check"}]),
    )
    .unwrap();
    assert_eq!(a.at_current(&old, |_| ()), Err(Failure::Stale));
    let current = a.snapshot().unwrap().candidate(&row("news", &[])).unwrap();
    assert_eq!(
        a.at_current(&current, |v| v.policy.url.clone()).unwrap(),
        Some("https://new.invalid/check".into())
    );
    assert!(
        a.replace(
            &config,
            &json!([{"name":"unused","url":{"url":"https://bad.invalid"}}])
        )
        .is_err()
    );
    assert!(a.at_current(&current, |_| ()).is_ok());
}

// Break caught: removed authority remains usable, or re-adding identical settings resurrects an old candidate.
#[test]
fn removal_and_readdition_do_not_resurrect_old_authority() {
    let a = authority(&source());
    let old = a.snapshot().unwrap().candidate(&row("news", &[])).unwrap();
    a.remove().unwrap();
    assert_eq!(a.snapshot().err(), Some(Failure::Removed));
    assert_eq!(a.at_current(&old, |_| ()), Err(Failure::Stale));
    let mut invalid = source();
    invalid["extra"] = json!({});
    assert!(a.replace(&invalid, &json!([])).is_err());
    assert_eq!(a.snapshot().err(), Some(Failure::Removed));
    a.replace(&source(), &json!([])).unwrap();
    assert_eq!(a.at_current(&old, |_| ()), Err(Failure::Stale));
    let current = a.snapshot().unwrap().candidate(&row("news", &[])).unwrap();
    assert!(a.at_current(&current, |_| ()).is_ok());
}

// Break caught: equal revision numbers/configuration let another source authority publish a candidate.
#[test]
fn candidates_cannot_cross_authority_instances() {
    let a = authority(&source());
    let b = authority(&source());
    let sa = a.snapshot().unwrap();
    let sb = b.snapshot().unwrap();
    assert_eq!(sa.revision(), sb.revision());
    let c = sa.candidate(&row("news", &[])).unwrap();
    assert_eq!(b.at_current(&c, |_| ()), Err(Failure::Stale));
    assert!(a.at_current(&c, |_| ()).is_ok());
}

// Break caught: the stale check drops its lock before publication and lets replacement race the commit.
#[test]
fn current_candidate_publication_and_configuration_replacement_are_serialized() {
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::Duration,
    };
    let a = Arc::new(authority(&source()));
    let c = a.snapshot().unwrap().candidate(&row("news", &[])).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let publisher = a.clone();
    let publishing = thread::spawn(move || {
        publisher.at_current(&c, |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let (starting_tx, starting_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let writer = a.clone();
    let replacing = thread::spawn(move || {
        starting_tx.send(()).unwrap();
        writer.replace(&source(), &json!([])).unwrap();
        done_tx.send(()).unwrap();
    });
    starting_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100)).is_err();
    release_tx.send(()).unwrap();
    publishing.join().unwrap().unwrap();
    replacing.join().unwrap();
    assert!(
        blocked,
        "replacement completed before current publication finished"
    );
    done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

// Break caught: complete projection strips name/fields before enforcing group/layer/backend limits.
#[test]
fn configured_counts_and_complete_layer_bytes_accept_exact_limits() {
    for total in [128usize, 129] {
        let mut c = source();
        c["group_config"] = json!(
            (0..total)
                .map(|i| (format!("g{i}"), json!({})))
                .collect::<serde_json::Map<_, _>>()
        );
        let result = SourceAuthority::new(&c, &json!([]));
        if total == 128 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
        let backends = json!(
            (0..total)
                .map(|i| json!({"name":format!("b{i}"),"url":"https://billing.invalid/check"}))
                .collect::<Vec<_>>()
        );
        let result = SourceAuthority::new(&source(), &backends);
        if total == 128 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
    }
    for extra in [0usize, 1] {
        let mut group = json!({"name":"protected","on_play":"https://billing.invalid/"});
        let needed = 16384 - serde_json::to_vec(&group).unwrap().len() + extra;
        group["on_play"] = json!(format!("https://billing.invalid/{}", "x".repeat(needed)));
        let mut c = source();
        c["group_config"] = json!({"protected":group});
        let result = SourceAuthority::new(&c, &json!([]));
        if extra == 0 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result.err(), Some(Failure::Limit));
        }
        let mut layer = json!({"on_play":"https://billing.invalid/"});
        let needed = 16384 - serde_json::to_vec(&layer).unwrap().len() + extra;
        layer["on_play"] = json!(format!("https://billing.invalid/{}", "x".repeat(needed)));
        let mut c = source();
        c["on_play"] = layer["on_play"].clone();
        let result = SourceAuthority::new(&c, &json!([]));
        if extra == 0 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
        let mut backend = json!({"name":"billing","url":"https://billing.invalid/"});
        let needed = 16384 - serde_json::to_vec(&backend).unwrap().len() + extra;
        backend["url"] = json!(format!("https://billing.invalid/{}", "x".repeat(needed)));
        let result = SourceAuthority::new(&source(), &json!([backend]));
        if extra == 0 {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
    }
}

// Break caught: whole-profile limits reset per group/backend or are off by one.
#[test]
fn combined_profile_budget_accepts_exactly_one_mib() {
    let mut c = source();
    let base = json!({"on_play":"https://billing.invalid/"});
    let padding = 16384 - serde_json::to_vec(&base).unwrap().len();
    let large = json!({"on_play":format!("https://billing.invalid/{}","x".repeat(padding))});
    c["group_config"] = json!(
        (0..64)
            .map(|i| (
                format!("g{i}"),
                if i == 63 { base.clone() } else { large.clone() }
            ))
            .collect::<serde_json::Map<_, _>>()
    );
    let remaining = 1048576
        - serde_json::to_vec(&json!({"source":c,"auth_backends":[]}))
            .unwrap()
            .len();
    c["group_config"]["g63"]["on_play"] =
        json!(format!("https://billing.invalid/{}", "x".repeat(remaining)));
    assert_eq!(
        serde_json::to_vec(&json!({"source":c,"auth_backends":[]}))
            .unwrap()
            .len(),
        1048576
    );
    assert!(SourceAuthority::new(&c, &json!([])).is_ok());
    let url = c["group_config"]["g63"]["on_play"]
        .as_str()
        .unwrap()
        .to_owned()
        + "x";
    c["group_config"]["g63"]["on_play"] = json!(url);
    assert_eq!(
        SourceAuthority::new(&c, &json!([])).err(),
        Some(Failure::Limit)
    );
}

// Break caught: a publication panic poisons the authority but later operations
// panic themselves or permit additional commits instead of failing closed.
#[test]
fn publication_panic_makes_authority_unavailable() {
    let a = authority(&source());
    let c = a.snapshot().unwrap().candidate(&row("news", &[])).unwrap();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = a.at_current(&c, |_| panic!("owned publication failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(a.snapshot().err(), Some(Failure::Unavailable));
    assert_eq!(a.at_current(&c, |_| ()), Err(Failure::Unavailable));
    assert_eq!(a.replace(&source(), &json!([])), Err(Failure::Unavailable));
    assert_eq!(a.remove(), Err(Failure::Unavailable));
}
