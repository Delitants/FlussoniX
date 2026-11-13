use super::*;
use crate::authorization::Auth;
use flussonix::source_directory::LookupFailure;
use flussonix::source_directory::modern_coordinator::ReplicaFailure;

/// The break: absent pre-authorization names permanently occupy all selector
/// slots, so a later catalog-known viewer cannot reach its real callback.
#[tokio::test]
async fn missing_names_do_not_exhaust_known_stream() {
    let f = Fixture::new().await;
    let callback = Auth::start().await;
    callback.configure(&f);
    let selector = f.selector();
    let first = f.handle(0);
    let auth = PlaybackAuth::new(16);

    for index in 0..20_032 {
        let name = format!("absent-{index}");
        let mut lookup = selector
            .begin(&name)
            .unwrap_or_else(|error| panic!("missing name {index} consumed capacity: {error:?}"));
        let inventory = f.owner.inventory(&first, Duration::from_secs(30)).await;
        assert!(matches!(
            lookup.observe(&first, inventory),
            Err(ReplicaFailure::Blocked)
        ));
    }

    let mut known = selector
        .begin(NAME)
        .expect("known name must retain capacity");
    let choice = known
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .expect("known name must be eligible");
    assert!(matches!(
        selector
            .admit(choice, &auth, viewer("known"))
            .await
            .unwrap(),
        ReplicaAdmissionOutcome::Allowed(_)
    ));
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert!(
        f.origins
            .iter()
            .all(|origin| origin.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn simultaneously_owned_names_still_reject_the_next_name() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let held: Vec<_> = (0..20_000)
        .map(|index| selector.begin(&format!("held-{index}")).unwrap())
        .collect();
    assert!(matches!(
        selector.begin("held-overflow"),
        Err(ReplicaFailure::Limit)
    ));
    assert!(selector.begin("held-0").is_ok());
    assert_eq!(held.len(), 20_000);
}

#[tokio::test]
async fn released_temporary_owners_reopen_capacity() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let held: Vec<_> = (0..20_000)
        .map(|index| selector.begin(&format!("temporary-{index}")).unwrap())
        .collect();
    assert!(matches!(
        selector.begin("fresh-after-release"),
        Err(ReplicaFailure::Limit)
    ));
    drop(held);
    assert!(selector.begin("fresh-after-release").is_ok());
}

/// The break: a second absent name has no per-name tombstone, but the first
/// source's complete catalog already established that it cannot use a backup.
#[tokio::test]
async fn catalog_absence_blocks_a_new_name_through_initial_source_outage() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut observed = selector.begin("not-listed-first").unwrap();
    assert!(matches!(
        observed.observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await
        ),
        Err(ReplicaFailure::Blocked)
    ));
    drop(observed);

    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut new_name = selector.begin("not-listed-second").unwrap();
    if new_name.next_source().unwrap().is_some() {
        let _ = new_name.observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        );
    }
    assert_eq!(new_name.next_source().unwrap(), None);
}

#[tokio::test]
async fn real_catalog_rejection_blocks_a_new_name_through_outage() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    *f.origins[0].status.lock().unwrap() = StatusCode::BAD_REQUEST;
    let mut observed = selector.begin("rejected-first").unwrap();
    assert!(matches!(
        observed.observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await
        ),
        Err(ReplicaFailure::Blocked)
    ));
    drop(observed);

    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut new_name = selector.begin("rejected-second").unwrap();
    if new_name.next_source().unwrap().is_some() {
        let _ = new_name.observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        );
    }
    assert_eq!(new_name.next_source().unwrap(), None);
}

#[tokio::test]
async fn invented_failure_cannot_deny_another_stream() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut invented = selector.begin("invented-rejection").unwrap();
    assert!(matches!(
        invented.observe(
            &first,
            Err(CoordinationFailure::Lease(LeaseFailure::Binding(
                BindingFailure::Inventory(LookupFailure::Invalid)
            )))
        ),
        Err(ReplicaFailure::Blocked)
    ));
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut fresh = selector.begin("unrelated-healthy-backup").unwrap();
    assert_eq!(fresh.next_source().unwrap(), Some(f.origins[0].key.clone()));
    assert!(
        fresh
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(fresh.next_source().unwrap(), Some(f.origins[1].key.clone()));
}

#[tokio::test]
async fn known_name_may_rotate_after_unrelated_catalog_absence() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut absent = selector.begin("unrelated-absent").unwrap();
    assert!(matches!(
        absent.observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await
        ),
        Err(ReplicaFailure::Blocked)
    ));
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut known = selector.begin(NAME).unwrap();
    assert!(
        known
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(known.next_source().unwrap(), Some(f.origins[1].key.clone()));
}

#[tokio::test]
async fn accepted_positive_catalog_replaces_old_negative_without_a_selection() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut absent = selector.begin("first-absent").unwrap();
    assert!(matches!(
        absent.observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await
        ),
        Err(ReplicaFailure::Blocked)
    ));
    *f.origins[0].rows.lock().unwrap() = json!([
        {"name":NAME,"named_by":"config","static":true,"groups":[]},
        {"name":"newly-added","named_by":"config","static":true,"groups":[]}
    ]);
    let _accepted = f
        .owner
        .inventory(&first, Duration::from_secs(30))
        .await
        .unwrap();
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;

    let mut new_turn = selector.begin("newly-added").unwrap();
    assert!(
        new_turn
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        new_turn.next_source().unwrap(),
        Some(f.origins[1].key.clone())
    );
}

#[tokio::test]
async fn maximum_catalog_projects_a_bounded_negative_summary() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut rows = Vec::with_capacity(10_000);
    for index in 0..9_998 {
        rows.push(json!({"name":format!("bulk-{index:04}"),"named_by":"config","static":true,"groups":[]}));
    }
    rows.push(json!({"name":"bulk-disabled","named_by":"config","static":true,"groups":[],"disabled":true}));
    rows.push(json!({"name":NAME,"named_by":"config","static":true,"groups":[]}));
    *f.origins[0].rows.lock().unwrap() = Value::Array(rows);

    let started = std::time::Instant::now();
    let accepted = f
        .owner
        .inventory(&first, Duration::from_secs(30))
        .await
        .unwrap();
    let mut initial = selector.begin("bulk-absent").unwrap();
    assert!(matches!(
        initial.observe(&first, Ok(accepted)),
        Err(ReplicaFailure::Blocked)
    ));
    eprintln!(
        "10,000-row discovery and projection: {:?}",
        started.elapsed()
    );

    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    for name in ["bulk-disabled", "bulk-still-absent"] {
        let mut lookup = selector.begin(name).unwrap();
        assert!(matches!(
            lookup.observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await
            ),
            Err(ReplicaFailure::Blocked)
        ));
    }
    let mut eligible = selector.begin("bulk-9997").unwrap();
    assert!(
        eligible
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        eligible.next_source().unwrap(),
        Some(f.origins[1].key.clone())
    );
}

#[tokio::test]
async fn a_choice_owns_provisional_history_after_its_lookup_leaves() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    drop(lookup);
    selector.current(&choice).unwrap();
    assert!(selector.status(NAME).is_ok());
    drop(choice);
    assert!(matches!(
        selector.status(NAME),
        Err(ReplicaFailure::Invalid)
    ));
}

#[tokio::test]
async fn another_live_lookup_prevents_same_name_retirement() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let older = selector.begin(NAME).unwrap();
    let newer = selector.begin(NAME).unwrap();
    drop(older);
    assert!(selector.status(NAME).is_ok());
    drop(newer);
    assert!(matches!(
        selector.status(NAME),
        Err(ReplicaFailure::Invalid)
    ));
    let recreated = selector.begin(NAME).unwrap();
    assert_eq!(
        recreated.next_source().unwrap(),
        Some(f.origins[0].key.clone())
    );
}

#[tokio::test]
async fn unadmitted_backup_choice_does_not_appoint_a_permanent_source() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let backup = f.handle(1);
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut lookup = selector.begin(NAME).unwrap();
    assert!(
        lookup
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await
            )
            .unwrap()
            .is_none()
    );
    let choice = lookup
        .observe(
            &backup,
            f.owner.inventory(&backup, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    assert_eq!(choice.source_key(), f.origins[1].key);
    drop(lookup);
    drop(choice);
    *f.origins[0].status.lock().unwrap() = StatusCode::OK;
    assert_eq!(
        selector.begin(NAME).unwrap().next_source().unwrap(),
        Some(f.origins[0].key.clone())
    );
}

#[tokio::test]
async fn denied_and_redirected_choices_release_their_entries() {
    let f = Fixture::new().await;
    let callback = Auth::start().await;
    callback.configure(&f);
    let selector = f.selector();
    let first = f.handle(0);
    let auth = PlaybackAuth::new(16);
    for token in ["deny", "redirect"] {
        let mut lookup = selector.begin(NAME).unwrap();
        let choice = lookup
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await,
            )
            .unwrap()
            .unwrap();
        let outcome = selector.admit(choice, &auth, viewer(token)).await.unwrap();
        assert!(matches!(
            outcome,
            ReplicaAdmissionOutcome::Denied | ReplicaAdmissionOutcome::Redirect(_)
        ));
        drop(lookup);
        assert!(matches!(
            selector.status(NAME),
            Err(ReplicaFailure::Invalid)
        ));
    }
    assert_eq!(callback.calls.load(Ordering::SeqCst), 2);
    assert!(
        f.origins
            .iter()
            .all(|origin| origin.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn accepted_control_retains_history_without_private_media() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let auth = PlaybackAuth::new(16);
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    let ReplicaAdmissionOutcome::Allowed(admitted) = selector
        .admit(choice, &auth, viewer("owned"))
        .await
        .unwrap()
    else {
        panic!("control admission failed");
    };
    drop(admitted);
    drop(lookup);
    assert!(selector.status(NAME).is_ok());
    assert!(
        f.origins
            .iter()
            .all(|origin| origin.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn selected_static_activation_retains_history_without_a_worker() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    let activated = selector.activate(choice).unwrap();
    drop(activated);
    drop(lookup);
    assert!(selector.status(NAME).is_ok());
    assert!(
        f.origins
            .iter()
            .all(|origin| origin.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn cancelled_callback_releases_its_provisional_route() {
    let f = Fixture::new().await;
    let callback = Auth::start().await;
    callback.configure(&f);
    let selector = f.selector();
    let first = f.handle(0);
    let auth = Arc::new(PlaybackAuth::new(16));
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    let (entered, release) = callback.pause();
    let (selected, playback_auth) = (selector.clone(), auth.clone());
    let pending =
        tokio::spawn(async move { selected.admit(choice, &playback_auth, viewer("held")).await });
    tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    drop(lookup);
    assert!(
        selector.status(NAME).is_ok(),
        "pending callback lost route ownership"
    );
    pending.abort();
    let _ = pending.await;
    let _ = release.send(());
    assert!(matches!(
        selector.status(NAME),
        Err(ReplicaFailure::Invalid)
    ));
    assert!(
        f.origins
            .iter()
            .all(|origin| origin.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn provisional_owner_dropped_inside_fenced_handoff_is_reclaimed() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let first = f.handle(0);
    let auth = PlaybackAuth::new(16);
    let pending = selector.begin("temporary-inside-handoff").unwrap();
    let newer = selector.begin("temporary-inside-handoff").unwrap();
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    let ReplicaAdmissionOutcome::Allowed(admitted) = selector
        .admit(choice, &auth, viewer("owned"))
        .await
        .unwrap()
    else {
        panic!("control admission failed");
    };
    selector
        .publish_admitted(&admitted, |_, _, _| drop((pending, newer)))
        .unwrap();
    assert!(matches!(
        selector.status("temporary-inside-handoff"),
        Err(ReplicaFailure::Invalid)
    ));
}

#[tokio::test]
async fn changing_valid_catalogs_with_failed_callbacks_reclaim_capacity() {
    let f = Fixture::new().await;
    let callback = Auth::start_any().await;
    callback.configure(&f);
    let selector = f.selector();
    let first = f.handle(0);
    let auth = PlaybackAuth::new(16);
    for index in 0..512 {
        let name = format!("callback-{index}");
        *f.origins[0].rows.lock().unwrap() =
            json!([{"name":name,"named_by":"config","static":true,"groups":[]}]);
        let mut lookup = selector.begin(&name).unwrap();
        let choice = lookup
            .observe(
                &first,
                f.owner.inventory(&first, Duration::from_secs(30)).await,
            )
            .unwrap()
            .unwrap();
        let token = if index % 2 == 0 { "deny" } else { "redirect" };
        let mut request = viewer(token);
        request.name = name.clone();
        let outcome = selector.admit(choice, &auth, request).await.unwrap();
        assert!(matches!(
            outcome,
            ReplicaAdmissionOutcome::Denied | ReplicaAdmissionOutcome::Redirect(_)
        ));
        drop(lookup);
        assert!(matches!(
            selector.status(&name),
            Err(ReplicaFailure::Invalid)
        ));
    }
    assert_eq!(callback.calls.load(Ordering::SeqCst), 512);
    assert!(
        f.origins
            .iter()
            .all(|origin| origin.private_calls.load(Ordering::SeqCst) == 0)
    );

    *f.origins[0].rows.lock().unwrap() =
        json!([{"name":NAME,"named_by":"config","static":true,"groups":[]}]);
    let mut known = selector.begin(NAME).unwrap();
    let choice = known
        .observe(
            &first,
            f.owner.inventory(&first, Duration::from_secs(30)).await,
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        selector
            .admit(choice, &auth, viewer("owned"))
            .await
            .unwrap(),
        ReplicaAdmissionOutcome::Allowed(_)
    ));
    assert_eq!(callback.calls.load(Ordering::SeqCst), 513);
}
