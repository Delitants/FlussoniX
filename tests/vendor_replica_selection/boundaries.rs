use super::*;

fn members(f: &Fixture) -> Vec<ReplicaMember> {
    f.origins
        .iter()
        .map(|o| ReplicaMember {
            source_key: o.key.clone(),
            content_id: "explicit-content".into(),
        })
        .collect()
}

#[tokio::test]
async fn explicit_identity_unique_sources_and_eight_replica_bound_are_required() {
    let mut f = Fixture::new().await;
    let mut different = members(&f);
    different[1].content_id = "different-content".into();
    assert!(ReplicaSelector::new(f.owner.clone(), "same-name-is-insufficient", different).is_err());
    let mut duplicate = members(&f);
    duplicate[1].source_key = duplicate[0].source_key.clone();
    assert!(ReplicaSelector::new(f.owner.clone(), "duplicate-source", duplicate).is_err());
    assert!(ReplicaSelector::new(f.owner.clone(), "bad group", members(&f)).is_err());
    for _ in 2..8 {
        let o = Origin::start("extra-replica").await;
        f.config.put("sources", &o.key, o.source()).unwrap();
        f.origins.push(o);
    }
    assert!(ReplicaSelector::new(f.owner.clone(), "eight-valid", members(&f)).is_ok());
    let o = Origin::start("ninth-replica").await;
    f.config.put("sources", &o.key, o.source()).unwrap();
    f.origins.push(o);
    assert!(ReplicaSelector::new(f.owner.clone(), "nine-invalid", members(&f)).is_err());
    assert!(
        f.origins
            .iter()
            .all(|o| o.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn group_dependent_viewer_policy_is_conservatively_unsupported() {
    let f = Fixture::new().await;
    for o in &f.origins {
        f.config
            .put(
                "sources",
                &o.key,
                json!({"group_config":{"paid":{"flussonix_token_sha256":"b".repeat(64)}}}),
            )
            .unwrap();
    }
    assert!(ReplicaSelector::new(f.owner.clone(), "uniform-required", members(&f)).is_err());
    assert!(
        f.origins
            .iter()
            .all(|o| o.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn different_namespace_exclusions_processing_or_native_source_is_rejected() {
    for change in [
        json!({"prefix":"news"}),
        json!({"except":["news/*"]}),
        json!({"flussonix_output_template":"audio"}),
    ] {
        let f = Fixture::new().await;
        f.config
            .put(
                "templates",
                "audio",
                json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96}}),
            )
            .unwrap();
        f.config.put("sources", &f.origins[1].key, change).unwrap();
        assert!(ReplicaSelector::new(f.owner.clone(), "equivalent-required", members(&f)).is_err());
        assert!(
            f.origins
                .iter()
                .all(|o| o.calls.load(Ordering::SeqCst) == 0)
        );
    }
    let f = Fixture::new().await;
    let mut source = f.origins[1].source();
    source["flussonix_source_kind"] = json!("native");
    source.as_object_mut().unwrap().remove("on_play");
    source["$reset"] = json!(true);
    f.config.put("sources", &f.origins[1].key, source).unwrap();
    assert!(ReplicaSelector::new(f.owner.clone(), "vendor-only", members(&f)).is_err());
}

#[tokio::test]
async fn normalized_backend_policies_and_processing_profiles_can_be_equivalent() {
    let f = Fixture::new().await;
    f.config
        .put(
            "auth_backends",
            "billing",
            json!({"url":"https://billing.invalid/check"}),
        )
        .unwrap();
    for name in ["one", "two"] {
        f.config.put("templates",name,json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96},"inputs":[{"url":"testsrc://"}]})).unwrap();
    }
    f.config.put("sources",&f.origins[0].key,json!({"on_play":"auth://billing","group_config":{"free":{"on_play":"https://billing.invalid/check"}},"flussonix_output_template":"one"})).unwrap();
    f.config.put("sources",&f.origins[1].key,json!({"on_play":"https://billing.invalid/check","group_config":{"free":{"on_play":"auth://billing"}},"flussonix_output_template":"two"})).unwrap();
    assert!(ReplicaSelector::new(f.owner.clone(), "normalized-equivalent", members(&f)).is_ok());
    assert!(
        f.origins
            .iter()
            .all(|o| o.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn disabled_malformed_and_denied_authority_remain_blocked_during_outages() {
    for rows in [
        json!([{"name":NAME,"named_by":"config","static":true,"groups":[],"disabled":true}]),
        json!([{"name":NAME,"static":true,"groups":[]}]),
        json!([{"name":NAME,"named_by":"config","static":true,"groups":"invalid"}]),
        json!([{"name":NAME,"static":"invalid"}]),
    ] {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        *f.origins[0].rows.lock().unwrap() = rows;
        let mut lookup = s.begin(NAME).unwrap();
        assert!(
            lookup
                .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
                .is_err()
        );
        *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
        let mut later = s.begin(NAME).unwrap();
        assert!(
            later
                .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
                .is_err()
        );
        assert!(s.status(NAME).unwrap().blocked);
        assert_eq!(later.next_source().unwrap(), None);
    }
}

#[tokio::test]
async fn omitted_membership_with_uniform_policy_selects_origin_and_fences_policy_edits() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let handle = f.handle(0);
    *f.origins[0].rows.lock().unwrap() = json!([{"name":NAME,"named_by":"config","static":true}]);
    let choice = selector
        .begin(NAME)
        .unwrap()
        .observe(
            &handle,
            f.owner.inventory(&handle, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    assert_eq!(choice.source_key(), f.origins[0].key);
    selector.current(&choice).unwrap();
    assert!(!selector.status(NAME).unwrap().blocked);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    f.config
        .put(
            "sources",
            &f.origins[0].key,
            json!({"group_config":{"paid":{"flussonix_token_sha256":"b".repeat(64)}}}),
        )
        .unwrap();
    assert!(selector.current(&choice).is_err());
}

#[tokio::test]
async fn modern_http_rejections_are_authoritative_and_legacy_classification_is_preserved() {
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::FOUND,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::NO_CONTENT,
    ] {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        *f.origins[0].status.lock().unwrap() = status;
        let mut lookup = s.begin(NAME).unwrap();
        assert!(
            lookup
                .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
                .is_err()
        );
        assert!(s.status(NAME).unwrap().blocked);
        let client = flussonix::source_directory::flussonic::InventoryClient::new(
            &f.origins[0].key.replacen("m4s", "http", 1),
            &f.origins[0].secret,
            None,
        )
        .unwrap();
        assert_eq!(
            client.inventory().await,
            Err(flussonix::source_directory::LookupFailure::Unavailable)
        );
    }
}

#[tokio::test]
async fn backup_rotation_skips_invalid_replica_and_never_revisits_an_origin() {
    let mut f = Fixture::new().await;
    let third = Origin::start("replica-c").await;
    f.config.put("sources", &third.key, third.source()).unwrap();
    f.origins.push(third);
    let s = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let c = f.handle(2);
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    *f.origins[1].rows.lock().unwrap() = json!([]);
    let mut lookup = s.begin(NAME).unwrap();
    assert!(
        lookup
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .is_none()
    );
    assert!(
        lookup
            .observe(&b, f.owner.inventory(&b, Duration::from_secs(30)).await)
            .unwrap()
            .is_none()
    );
    let choice = lookup
        .observe(&c, f.owner.inventory(&c, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    s.current(&choice).unwrap();
    assert_eq!(choice.source_key(), f.origins[2].key);
    assert_eq!(lookup.next_source().unwrap(), None);
    for o in &f.origins {
        *o.status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    }
    let mut all = s.begin(NAME).unwrap();
    for (index, handle) in [(2, &c), (0, &a), (1, &b)] {
        assert_eq!(
            all.next_source().unwrap(),
            Some(f.origins[index].key.clone())
        );
        assert!(
            all.observe(
                handle,
                f.owner.inventory(handle, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_none()
        );
    }
    assert_eq!(all.next_source().unwrap(), None);
    assert!(!s.status(NAME).unwrap().available);
}

#[tokio::test]
async fn expired_foreign_and_replaced_source_observations_cannot_publish_selection() {
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let mut foreign = s.begin(NAME).unwrap();
    assert!(
        foreign
            .observe(&a, f.owner.inventory(&b, Duration::from_secs(30)).await)
            .is_err()
    );
    assert!(!s.status(NAME).unwrap().available);
    let rows = f
        .owner
        .inventory(&a, Duration::from_millis(40))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut expired = s.begin(NAME).unwrap();
    assert!(expired.observe(&a, Ok(rows)).is_err());
    assert!(!s.status(NAME).unwrap().available);
    let newest = f.handle(0);
    let mut stale = s.begin(NAME).unwrap();
    let error = CoordinationFailure::Lease(LeaseFailure::Binding(BindingFailure::Inventory(
        flussonix::source_directory::LookupFailure::Invalid,
    )));
    assert!(stale.observe(&a, Err(error)).is_err());
    assert!(!s.status(NAME).unwrap().blocked);
    assert!(
        stale
            .observe(
                &newest,
                f.owner.inventory(&newest, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_some()
    );
}

// An old negative fact must not cancel a recovered route or create a tombstone.
#[tokio::test]
async fn superseded_negative_inventory_preserves_the_current_healthy_choice() {
    for rows in [
        json!([]),
        json!([{"name":NAME,"named_by":"config","static":true,"groups":[],"disabled":true}]),
    ] {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        let healthy = f.origins[0].rows.lock().unwrap().clone();
        *f.origins[0].rows.lock().unwrap() = rows;
        let old = f
            .owner
            .inventory(&a, Duration::from_secs(30))
            .await
            .unwrap();
        *f.origins[0].rows.lock().unwrap() = healthy;
        let mut recovered = s.begin(NAME).unwrap();
        let choice = recovered
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .unwrap();
        s.current(&choice).unwrap();
        let mut later = s.begin(NAME).unwrap();
        assert!(matches!(
            later.observe(&a, Ok(old)),
            Err(
                flussonix::source_directory::modern_coordinator::ReplicaFailure::Coordination(
                    CoordinationFailure::Lease(LeaseFailure::Superseded)
                )
            )
        ));
        let status = s.status(NAME).unwrap();
        assert!(status.available);
        assert!(!status.blocked);
        assert_eq!(status.switches, 0);
        s.current(&choice).unwrap();
        assert_eq!(later.next_source().unwrap(), Some(f.origins[0].key.clone()));
    }
}

#[tokio::test]
async fn expired_negative_inventory_cannot_create_an_authoritative_block() {
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    *f.origins[0].rows.lock().unwrap() = json!([]);
    let old = f.owner.inventory(&a, Duration::from_secs(1)).await.unwrap();
    tokio::time::sleep_until(tokio::time::Instant::from_std(old.expires_at())).await;
    let mut later = s.begin(NAME).unwrap();
    assert!(matches!(
        later.observe(&a, Ok(old)),
        Err(
            flussonix::source_directory::modern_coordinator::ReplicaFailure::Coordination(
                CoordinationFailure::Lease(LeaseFailure::Expired)
            )
        )
    ));
    assert!(!s.status(NAME).unwrap().blocked);
    assert_eq!(later.next_source().unwrap(), Some(f.origins[0].key.clone()));
}

#[tokio::test]
async fn configuration_identity_and_shutdown_fence_outstanding_choices() {
    for mode in 0..3 {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        let mut lookup = s.begin(NAME).unwrap();
        let choice = lookup
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .unwrap();
        match mode {
            0 => {
                f.config
                    .put("sources", &f.origins[0].key, json!({"on_play":"true"}))
                    .unwrap();
            }
            1 => {
                f.owner.replace_aliases(&["new-alias"]).unwrap();
            }
            _ => s.shutdown(),
        }
        assert!(s.current(&choice).is_err());
        assert!(s.begin(NAME).is_err());
        assert!(
            f.origins
                .iter()
                .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
        );
    }
}

#[tokio::test]
async fn stream_registry_limit_rejects_new_names_without_discovery_or_eviction_of_blocks() {
    let f = Fixture::new().await;
    let s = f.selector();
    let held: Vec<_> = (0..20_000)
        .map(|index| s.begin(&format!("bounded-{index}")).unwrap())
        .collect();
    assert!(s.begin("one-too-many").is_err());
    assert!(s.begin("bounded-0").is_ok());
    assert_eq!(held.len(), 20_000);
    assert!(
        f.origins
            .iter()
            .all(|o| o.calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn installed_authority_policy_mismatch_latches_closed_before_any_fallback() {
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let mut first = s.begin(NAME).unwrap();
    let old = first
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let changed = f
        .owner
        .install(
            f.owner
                .capture(&f.origins[0].key)
                .unwrap()
                .with_policy(
                    &json!({"on_play":"true","flussonix_token_sha256":"a".repeat(64)}),
                    &json!({}),
                )
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap();
    let mut lookup = s.begin(NAME).unwrap();
    assert!(
        lookup
            .observe(
                &changed,
                f.owner.inventory(&changed, Duration::from_secs(30)).await
            )
            .is_err()
    );
    assert!(s.status(NAME).unwrap().blocked);
    assert!(s.current(&old).is_err());
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut later = s.begin(NAME).unwrap();
    assert!(
        later
            .observe(
                &changed,
                f.owner.inventory(&changed, Duration::from_secs(30)).await
            )
            .is_err()
    );
    assert_eq!(later.next_source().unwrap(), None);
}

#[tokio::test]
async fn obsolete_and_infrastructure_errors_cannot_become_authoritative_stream_blocks() {
    use flussonix::source_directory::flussonic_config::ProjectionFailure;
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let mut first = s.begin(NAME).unwrap();
    let choice = first
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    for error in [
        CoordinationFailure::Stale,
        CoordinationFailure::Unavailable,
        CoordinationFailure::Lease(LeaseFailure::Expired),
        CoordinationFailure::Lease(LeaseFailure::Superseded),
        CoordinationFailure::Lease(LeaseFailure::Binding(BindingFailure::Projection(
            ProjectionFailure::Stale,
        ))),
        CoordinationFailure::Lease(LeaseFailure::Binding(BindingFailure::Projection(
            ProjectionFailure::Removed,
        ))),
        CoordinationFailure::Lease(LeaseFailure::Binding(BindingFailure::Projection(
            ProjectionFailure::Unavailable,
        ))),
    ] {
        let mut lookup = s.begin(NAME).unwrap();
        assert!(lookup.observe(&a, Err(error)).is_err());
        assert!(!s.status(NAME).unwrap().blocked);
        s.current(&choice).unwrap();
    }
}

#[tokio::test]
async fn starting_another_lookup_does_not_revoke_the_current_healthy_route() {
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let mut first = s.begin(NAME).unwrap();
    let choice = first
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let pending = s.begin(NAME).unwrap();
    assert_eq!(
        pending.next_source().unwrap(),
        Some(f.origins[0].key.clone())
    );
    s.current(&choice).unwrap();
    assert_eq!(s.status(NAME).unwrap().switches, 0);
}

#[tokio::test]
async fn choices_cannot_cross_equal_selectors_or_different_content_assertions() {
    let f = Fixture::new().await;
    let s = f.selector();
    let other = Arc::new(
        ReplicaSelector::new(
            f.owner.clone(),
            "owned-replicas",
            f.origins
                .iter()
                .map(|o| ReplicaMember {
                    source_key: o.key.clone(),
                    content_id: "different-content-assertion".into(),
                })
                .collect(),
        )
        .unwrap(),
    );
    let a = f.handle(0);
    let inventory = f
        .owner
        .inventory(&a, Duration::from_secs(30))
        .await
        .unwrap();
    let mut first = s.begin(NAME).unwrap();
    let choice = first.observe(&a, Ok(inventory)).unwrap().unwrap();
    s.current(&choice).unwrap();
    assert!(other.current(&choice).is_err());
    let auth = PlaybackAuth::new(16);
    assert!(other.admit(choice, &auth, viewer("owned")).await.is_err());
    assert!(auth.snapshots().is_empty());
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn shared_replica_route_survives_lease_renewal_and_closes_on_selection_switch() {
    use flussonix::source_directory::modern_coordinator::{SharedInputOptions, SharedInputs};
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let auth = PlaybackAuth::new(16);
    let mut lookup = s.begin(NAME).unwrap();
    let choice = lookup
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let ReplicaAdmissionOutcome::Allowed(admission) = s
        .admit(
            choice,
            &auth,
            ViewerRequest {
                name: NAME.into(),
                proto: "hls".into(),
                ip: "127.0.0.1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("owned viewer denied")
    };
    let inputs = SharedInputs::new(f.owner.clone());
    let attachment = inputs
        .attach_replica(*admission, SharedInputOptions::default())
        .unwrap();
    attachment.viewer.current().unwrap();
    assert_eq!(
        inputs
            .renew(&a, Duration::from_secs(30))
            .await
            .unwrap()
            .renewed,
        1
    );
    attachment.viewer.current().unwrap();
    let mut replacement = s.begin(NAME).unwrap();
    assert!(
        replacement
            .observe(
                &a,
                Err(CoordinationFailure::Lease(LeaseFailure::Binding(
                    BindingFailure::Inventory(
                        flussonix::source_directory::LookupFailure::Unavailable
                    )
                )))
            )
            .unwrap()
            .is_none()
    );
    let next = replacement
        .observe(&b, f.owner.inventory(&b, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    s.current(&next).unwrap();
    assert!(attachment.viewer.current().is_err());
    drop(attachment);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn stale_replica_admission_cannot_attach_or_promote_native_playback() {
    use flussonix::source_directory::modern_coordinator::{SharedInputOptions, SharedInputs};
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let auth = PlaybackAuth::new(16);
    let mut lookup = s.begin(NAME).unwrap();
    let choice = lookup
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let ReplicaAdmissionOutcome::Allowed(admission) = s
        .admit(
            choice,
            &auth,
            ViewerRequest {
                name: NAME.into(),
                proto: "hls".into(),
                ip: "127.0.0.1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("owned viewer denied")
    };
    s.shutdown();
    let inputs = SharedInputs::new(f.owner.clone());
    assert!(
        inputs
            .attach_replica(*admission, SharedInputOptions::default())
            .is_err()
    );
    assert_eq!(auth.live_grants(), 0);
    assert!(auth.snapshots().is_empty());
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn managed_renewal_latches_selected_authoritative_rejection_before_later_outage() {
    use flussonix::source_directory::modern_coordinator::{SharedInputOptions, SharedInputs};
    for status in [StatusCode::OK, StatusCode::FORBIDDEN] {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        let auth = PlaybackAuth::new(16);
        let mut lookup = s.begin(NAME).unwrap();
        let choice = lookup
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .unwrap();
        let ReplicaAdmissionOutcome::Allowed(admission) = s
            .admit(
                choice,
                &auth,
                ViewerRequest {
                    name: NAME.into(),
                    proto: "hls".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        else {
            panic!("owned viewer denied")
        };
        let inputs = SharedInputs::new(f.owner.clone());
        let attachment = inputs
            .attach_replica(*admission, SharedInputOptions::default())
            .unwrap();
        *f.origins[0].rows.lock().unwrap() = json!([]);
        *f.origins[0].status.lock().unwrap() = status;
        let _ = inputs.renew(&a, Duration::from_secs(30)).await;
        assert!(
            attachment.viewer.current().is_err(),
            "authoritative maintenance must retire group media immediately"
        );
        let mut later = s.begin(NAME).unwrap();
        assert!(matches!(
            later.observe(
                &a,
                Err(CoordinationFailure::Lease(LeaseFailure::Binding(
                    BindingFailure::Inventory(
                        flussonix::source_directory::LookupFailure::Unavailable
                    )
                )))
            ),
            Err(flussonix::source_directory::modern_coordinator::ReplicaFailure::Blocked)
        ));
        assert_eq!(later.next_source().unwrap(), None);
        drop(attachment);
    }
}
