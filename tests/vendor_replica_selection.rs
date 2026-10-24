use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    config::ConfigStore,
    source_directory::modern_coordinator::{
        ModernSourceCoordinator, ReplicaMember, ReplicaSelector, SourceHandle,
    },
};
use flussonix::{
    playback_auth::{PlaybackAuth, ViewerRequest},
    source_directory::{
        flussonic::modern::{ModernInventoryClient, ObservedServerId},
        flussonic_config::{BindingFailure, LeaseFailure},
        modern_coordinator::{CoordinationFailure, ReplicaAdmissionOutcome},
    },
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const NAME: &str = "Music %2F HD";
#[path = "vendor_replica_selection/authorization.rs"]
mod authorization;
#[path = "vendor_replica_selection/boundaries.rs"]
mod boundaries;
#[path = "vendor_replica_selection/capacity.rs"]
mod capacity;
#[path = "vendor_replica_selection/fixture.rs"]
mod fixture;
use fixture::*;

// Removing explicit replica routing or resetting a healthy selected fallback
// would fail this through real coordinator publication, not a scheduler mock.
#[tokio::test]
async fn unavailable_primary_selects_equivalent_backup_and_recovery_stays_sticky() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut lookup = selector.begin(NAME).unwrap();
    assert_eq!(
        lookup.next_source().unwrap(),
        Some(f.origins[0].key.clone())
    );
    assert!(
        lookup
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        lookup.next_source().unwrap(),
        Some(f.origins[1].key.clone())
    );
    let choice = lookup
        .observe(&b, f.owner.inventory(&b, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    assert_eq!(choice.source_key(), f.origins[1].key);
    selector.current(&choice).unwrap();
    *f.origins[0].status.lock().unwrap() = StatusCode::OK;
    assert_eq!(
        selector.begin(NAME).unwrap().next_source().unwrap(),
        Some(f.origins[1].key.clone())
    );
}

// Losing the authoritative absence tombstone would permit later API failure
// to resurrect content from a stale replica.
#[tokio::test]
async fn authoritative_absence_blocks_fallback_through_later_origin_outages() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let a = f.handle(0);
    *f.origins[0].rows.lock().unwrap() = json!([]);
    let mut first = selector.begin(NAME).unwrap();
    assert!(
        first
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .is_err()
    );
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut later = selector.begin(NAME).unwrap();
    assert!(
        later
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .is_err()
    );
    assert_eq!(later.next_source().unwrap(), None);
    *f.origins[0].status.lock().unwrap() = StatusCode::OK;
    *f.origins[0].rows.lock().unwrap() =
        json!([{"name":NAME,"named_by":"config","static":true,"groups":[]}]);
    let mut recovered = selector.begin(NAME).unwrap();
    assert!(
        recovered
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn unequal_actual_viewer_policies_are_rejected_before_discovery() {
    let f = Fixture::new().await;
    f.config
        .put(
            "sources",
            &f.origins[1].key,
            json!({"flussonix_token_sha256":"a".repeat(64)}),
        )
        .unwrap();
    let result = ReplicaSelector::new(
        f.owner.clone(),
        "owned-replicas",
        f.origins
            .iter()
            .map(|o| ReplicaMember {
                source_key: o.key.clone(),
                content_id: "same-content".into(),
            })
            .collect(),
    );
    assert!(result.is_err());
    assert!(
        f.origins
            .iter()
            .all(|o| o.calls.load(Ordering::SeqCst) == 0)
    );
}

fn viewer(token: &str) -> ViewerRequest {
    ViewerRequest {
        name: NAME.into(),
        proto: "m4s".into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        qs: format!("token={token}"),
        user_agent: String::new(),
        referer: String::new(),
        host: "owned".into(),
    }
}

#[tokio::test]
async fn selection_shutdown_during_native_callback_rejects_the_late_grant() {
    use tokio::sync::oneshot;
    let f = Fixture::new().await;
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", tcp.local_addr().unwrap());
    let (entered, waiting) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let hold = Arc::new(Mutex::new(Some((entered, released))));
    let router = Router::new().fallback(get(move || {
        let hold = hold.clone();
        async move {
            let (entered, released) = hold.lock().unwrap().take().unwrap();
            entered.send(()).unwrap();
            released.await.unwrap();
            StatusCode::OK
        }
    }));
    let task = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
    for origin in &f.origins {
        f.config
            .put("sources", &origin.key, json!({"on_play":url}))
            .unwrap();
    }
    let selector = f.selector();
    let a = f.handle(0);
    let auth = Arc::new(PlaybackAuth::new(16));
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let (s, au) = (selector.clone(), auth.clone());
    let admission = tokio::spawn(async move { s.admit(choice, &au, viewer("allowed")).await });
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    selector.shutdown();
    release.send(()).unwrap();
    assert!(admission.await.unwrap().is_err());
    assert!(auth.snapshots().is_empty());
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn stale_selection_cannot_promote_control_admission_to_playback() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let a = f.handle(0);
    let auth = PlaybackAuth::new(16);
    let mut lookup = selector.begin(NAME).unwrap();
    let choice = lookup
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let ReplicaAdmissionOutcome::Allowed(admitted) = selector
        .admit(choice, &auth, viewer("owned"))
        .await
        .unwrap()
    else {
        panic!("denied")
    };
    assert_eq!(auth.live_grants(), 1);
    selector.shutdown();
    assert!(selector.publish_admitted(&admitted, |_, _, _| ()).is_err());
    assert!(admitted.is_cancelled());
    drop(admitted);
    assert_eq!(auth.live_grants(), 0);
    assert!(auth.snapshots().is_empty());
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn equivalent_backup_media_uses_its_own_credentials_and_independently_decodes() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let auth = PlaybackAuth::new(16);
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut lookup = selector.begin(NAME).unwrap();
    assert!(
        lookup
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .is_none()
    );
    let choice = lookup
        .observe(&b, f.owner.inventory(&b, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let ReplicaAdmissionOutcome::Allowed(admitted) = selector
        .admit(choice, &auth, viewer("allowed-viewer"))
        .await
        .unwrap()
    else {
        panic!("denied")
    };
    selector.admitted_current(&admitted).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(64 * 1024);
    let (tx, tracks) = tokio::sync::oneshot::channel();
    let s = selector.clone();
    let pull = tokio::spawn(async move {
        s.pull_admitted(
            &admitted,
            &mut writer,
            None,
            tx,
            flussonix::source_directory::modern_coordinator::PrivatePullOptions::default(),
        )
        .await
    });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), tracks)
            .await
            .unwrap()
            .unwrap()[0]
            .codec,
        "mp3"
    );
    use tokio::io::AsyncReadExt;
    let mut bytes = vec![0; 188 * 8];
    tokio::time::timeout(Duration::from_secs(2), reader.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    selector.shutdown();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), pull)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes.first(), Some(&0x47));
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.origins[1].private_calls.load(Ordering::SeqCst), 1);
    let file = f._dir.path().join("owned-backup.ts");
    std::fs::write(&file, bytes).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(8),
        tokio::process::Command::new("/usr/bin/ffmpeg")
            .args(["-nostdin", "-v", "error", "-i"])
            .arg(file)
            .args(["-f", "null", "-"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn management_denial_is_authoritative_and_cannot_trigger_backup_selection() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let a = f.handle(0);
    *f.origins[0].status.lock().unwrap() = StatusCode::FORBIDDEN;
    let mut lookup = selector.begin(NAME).unwrap();
    assert!(
        lookup
            .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .is_err()
    );
    assert!(selector.status(NAME).unwrap().blocked);
    assert_eq!(lookup.next_source().unwrap(), None);
}

#[tokio::test]
async fn late_lookup_success_cannot_replace_the_newer_selection() {
    let f = Fixture::new().await;
    let selector = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let mut old = selector.begin(NAME).unwrap();
    let old_inventory = f
        .owner
        .inventory(&a, Duration::from_secs(30))
        .await
        .unwrap();
    *f.origins[0].status.lock().unwrap() = StatusCode::SERVICE_UNAVAILABLE;
    let mut new = selector.begin(NAME).unwrap();
    assert!(
        new.observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
            .unwrap()
            .is_none()
    );
    let current = new
        .observe(&b, f.owner.inventory(&b, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    assert!(old.observe(&a, Ok(old_inventory)).is_err());
    selector.current(&current).unwrap();
    assert_eq!(
        selector.status(NAME).unwrap().selected_source,
        f.origins[1].key
    );
}

#[tokio::test]
async fn static_activation_is_fenced_by_replica_renewal_switch_and_owner_identity() {
    use flussonix::source_directory::modern_coordinator::{SharedInputOptions, SharedInputs};
    let f = Fixture::new().await;
    let s = f.selector();
    let a = f.handle(0);
    let b = f.handle(1);
    let mut lookup = s.begin(NAME).unwrap();
    let choice = lookup
        .observe(&a, f.owner.inventory(&a, Duration::from_secs(30)).await)
        .unwrap()
        .unwrap();
    let activated = s.activate(choice).unwrap();
    let control = activated.control();
    let inputs = SharedInputs::new(f.owner.clone());
    let attachment = inputs
        .attach_activation(activated, SharedInputOptions::default())
        .unwrap();
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
    let foreign = f.selector();
    assert!(foreign.activate(next).is_err());
    control.cancel();
    drop(attachment);
    assert!(
        f.origins
            .iter()
            .all(|o| o.private_calls.load(Ordering::SeqCst) == 0)
    );
}

#[tokio::test]
async fn direct_and_replica_static_owners_cannot_share_or_weaken_each_others_scope() {
    use flussonix::source_directory::modern_coordinator::{
        SharedFailure, SharedInputOptions, SharedInputs,
    };
    for scoped_first in [false, true] {
        let f = Fixture::new().await;
        let s = f.selector();
        let a = f.handle(0);
        let rows = f
            .owner
            .inventory(&a, Duration::from_secs(30))
            .await
            .unwrap();
        let direct = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
        let mut lookup = s.begin(NAME).unwrap();
        let choice = lookup.observe(&a, Ok(rows)).unwrap().unwrap();
        let scoped = s.activate(choice).unwrap();
        let inputs = SharedInputs::new(f.owner.clone());
        let (mut first, second) = if scoped_first {
            (
                inputs
                    .attach_activation(scoped, SharedInputOptions::default())
                    .unwrap(),
                inputs.attach_activation(direct, SharedInputOptions::default()),
            )
        } else {
            (
                inputs
                    .attach_activation(direct, SharedInputOptions::default())
                    .unwrap(),
                inputs.attach_activation(scoped, SharedInputOptions::default()),
            )
        };
        let rejected = matches!(second, Err(SharedFailure::Mismatch));
        drop(second);
        let correct_scope = if scoped_first {
            *f.origins[0].private_status.lock().unwrap() = StatusCode::FORBIDDEN;
            assert!(first.input.take().unwrap().run().await.is_err());
            s.status(NAME).unwrap().blocked
        } else {
            s.shutdown();
            first.viewer.current().is_ok()
        };
        drop(first);
        inputs.shutdown();
        assert!(
            rejected,
            "direct and replica-scoped ownership shared one worker"
        );
        assert!(
            correct_scope,
            "worker inherited another owner's shutdown or lost private rejection authority"
        );
    }
}
