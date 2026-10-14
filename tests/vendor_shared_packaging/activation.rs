//! Static input ownership is separate from every native viewer permission.
use super::*;
use flussonix::source_directory::modern_coordinator::AdmissionFailure;
fn configure(f: &mut Fixture, patch: serde_json::Value) {
    let key = f.config.snapshot()["sources"][0]["url"]
        .as_str()
        .unwrap()
        .to_owned();
    f.config.put("sources", &key, patch).unwrap();
    f.handle = f
        .owner
        .install(
            f.owner
                .capture(&key)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap();
}
#[tokio::test]
async fn selected_static_authority_has_no_native_viewer_permission_or_session_side_effects() {
    let f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_secs(30)).await;
    let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    f.owner.activated_current(&activated).unwrap();
    let control = activated.control();
    assert_eq!(f.auth.live_grants(), 0);
    assert!(f.auth.snapshots().is_empty());
    assert_eq!(f.media_calls(), 0);
    control.cancel();
    assert!(activated.is_cancelled());
    assert!(f.owner.activated_current(&activated).is_err());
}
#[tokio::test]
async fn only_selects_static_activation_without_hiding_on_demand_viewer_candidates() {
    for only in [json!([]), json!(["other"])] {
        let mut f = Fixture::start(false).await;
        configure(
            &mut f,
            json!({"only":only,"on_play":"true","flussonix_source_kind":"flussonic"}),
        );
        let rows = f.inventory(Duration::from_secs(30)).await;
        assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
        let viewer = f.admitted(&rows, "owned").await;
        assert!(!viewer.is_cancelled());
        drop(viewer);
        assert_eq!(f.auth.live_grants(), 0);
        assert_eq!(f.media_calls(), 0);
    }
}
#[tokio::test]
async fn remote_on_demand_and_disabled_local_streams_cannot_create_static_demand() {
    let f = Fixture::start(false).await;
    f.remote.lock().unwrap().streams[0]["static"] = json!(false);
    let rows = f.inventory(Duration::from_secs(30)).await;
    assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
    let mut f = Fixture::start(false).await;
    f.config
        .put("streams", NAME, json!({"disabled":true}))
        .unwrap();
    f.handle = install(
        &f.owner,
        f.config.snapshot()["sources"][0]["url"].as_str().unwrap(),
    );
    let rows = f.inventory(Duration::from_secs(30)).await;
    assert!(matches!(
        f.owner.activate(rows.candidate(NAME).unwrap()),
        Err(AdmissionFailure::LocalStream)
    ));
    assert_eq!(f.media_calls(), 0);
}
#[tokio::test]
async fn expired_foreign_and_reconfigured_static_candidates_are_rejected() {
    let f = Fixture::start(false).await;
    let rows = f.inventory(Duration::from_millis(100)).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
    let rows = f.inventory(Duration::from_secs(30)).await;
    let other = ModernSourceCoordinator::new(f.config.clone(), "owned-other-runtime").unwrap();
    assert!(other.activate(rows.candidate(NAME).unwrap()).is_err());
    let activated = f.owner.activate(rows.candidate(NAME).unwrap()).unwrap();
    f.config.put("templates", "unrelated", json!({})).unwrap();
    assert!(f.owner.activated_current(&activated).is_err());
    assert!(f.owner.activate(rows.candidate(NAME).unwrap()).is_err());
    assert_eq!(f.media_calls(), 0);
}
