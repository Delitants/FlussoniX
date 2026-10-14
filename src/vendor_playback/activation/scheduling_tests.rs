//! Exercise real maintenance with an owned group gate held across a revision.
use super::*;
use crate::{server::Options, vendor_playback::recovery::failure_retirement_tests::origin};
use serde_json::json;
use std::sync::atomic::Ordering;
// The synchronous registry hold deliberately forces a publication race; the
// catalog driver runs on another worker and does not acquire this registry.
#[allow(clippy::await_holding_lock)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_name_replaces_obsolete_held_startup_without_waiting_for_timeout() {
    let a = origin("owned-generation-a").await;
    let b = origin("owned-generation-b").await;
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-native-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for key in [&a.key, &b.key] {
        app.config.put("sources",key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-retirement-key","on_play":"true","flussonix_source_group":"replicas","flussonix_content_id":"music","only":["owned0"]})).unwrap();
    }
    let runtime = Arc::new(Runtime::new(Arc::new(
        ModernSourceCoordinator::new(app.config.clone(), "owned-jobs").unwrap(),
    )));
    let old = runtime
        .group_for(&app, &a.key, app.config.revision())
        .unwrap()
        .unwrap();
    let held = old.gate.lock().await;
    let cancel = CancellationToken::new();
    let _cleanup = cancel.clone().drop_guard();
    // Separate owned drivers let the test hold the recovery registry without
    // blocking authenticated catalog I/O. All are joined before normal cleanup.
    let (sessions, c) = (runtime.sessions.clone(), cancel.clone());
    let sessions = tokio::spawn(async move { sessions.run(c).await });
    let (r, a2, c) = (runtime.clone(), app.clone(), cancel.clone());
    let catalogs = tokio::spawn(async move { r.poll_static_catalogs(&a2, c).await });
    let (r, a2, c) = (runtime.clone(), app.clone(), cancel.clone());
    let maintenance = tokio::spawn(async move { r.maintain_replicas(&a2, c).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if runtime
                .activation
                .lock()
                .unwrap()
                .demands
                .get("owned0")
                .is_some_and(|d| d.streak > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        a.calls.load(Ordering::SeqCst),
        0,
        "held gate failed to hold old startup"
    );
    // Force the race: the new same-name demand is published before maintenance
    // can inspect ownership. There is no observable absent-name tick to rely on.
    let registry = runtime.recovery.lock().unwrap();
    app.config
        .put("templates", "unrelated", json!({"comment":"new revision"}))
        .unwrap();
    let revision = app.config.revision();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready = {
                let s = runtime.activation.lock().unwrap();
                s.revision == revision && s.catalogs.values().any(|c| c.names.contains("owned0"))
            };
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    runtime.sync_static(&app);
    assert_eq!(
        runtime.activation.lock().unwrap().demands["owned0"].revision,
        revision
    );
    drop(registry);
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        while a.calls.load(Ordering::SeqCst) + b.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    cancel.cancel();
    let _ = sessions.await.unwrap();
    catalogs.await.unwrap();
    maintenance.await.unwrap();
    runtime.activation.lock().unwrap().demands.clear();
    runtime.inputs.shutdown();
    app.media.stop_all().await;
    drop(held);
    assert!(
        result.is_ok(),
        "same-name successor waited for obsolete job's eight-second timeout"
    );
}
