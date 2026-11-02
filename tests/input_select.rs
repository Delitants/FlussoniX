use flussonix::media::{Engine, Worker};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

fn inputs(count: usize) -> Value {
    json!({"inputs":(0..count).map(|_| json!({"url":"testsrc://"})).collect::<Vec<_>>()})
}

async fn closed(worker: &Worker) {
    tokio::time::timeout(Duration::from_secs(8), worker.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn cold_selection_starts_the_requested_input_without_reordering_config() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let config = inputs(3);
    let original = config.clone();
    assert!(
        !engine
            .select_input_guarded("owned", &config, 2, std::future::ready(true))
            .await
            .unwrap()
    );
    assert_eq!(engine.preferred_input("owned", &config).await, Some(2));
    let worker = engine.ensure("owned", &config).await.unwrap();
    assert_eq!(worker.stats()["input_index"], 2);
    assert_eq!(config, original);
    engine.stop_all().await;
}

#[tokio::test]
async fn active_selection_reaps_the_old_worker_and_starts_only_the_selected_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let config = inputs(3);
    let first = engine.ensure("owned", &config).await.unwrap();
    let old_pid = first.pid();
    assert_eq!(first.stats()["input_index"], 0);
    assert!(
        engine
            .select_input_guarded("owned", &config, 2, std::future::ready(true))
            .await
            .unwrap()
    );
    closed(&first).await;
    let second = engine.ensure("owned", &config).await.unwrap();
    assert_eq!(second.stats()["input_index"], 2);
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(engine.count().await, 1);
    assert!(!std::path::Path::new(&format!("/proc/{old_pid}")).exists());
    engine.stop_all().await;
}

#[tokio::test]
async fn selecting_the_active_input_does_not_restart_it() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let config = inputs(2);
    let first = engine.ensure("owned", &config).await.unwrap();
    assert!(
        !engine
            .select_input_guarded("owned", &config, 0, std::future::ready(true))
            .await
            .unwrap()
    );
    let second = engine.ensure("owned", &config).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    engine.stop_all().await;
}

#[tokio::test]
async fn a_failed_selected_input_falls_forward_to_the_next_configured_input() {
    let dir = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let engine = Engine::new(dir.path(), "ffmpeg");
    let config = json!({"inputs":[
        {"url":"testsrc://"},
        {"url":format!("tshttp://127.0.0.1:{port}/dead")},
        {"url":"testsrc://"}
    ]});
    engine
        .select_input_guarded("owned", &config, 1, std::future::ready(true))
        .await
        .unwrap();
    let failed = engine.ensure("owned", &config).await.unwrap();
    assert_eq!(failed.stats()["input_index"], 1);
    closed(&failed).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let fallback = engine.recover("owned", &config).await.unwrap();
    assert_eq!(fallback.stats()["input_index"], 2);
    engine.stop_all().await;
}

#[tokio::test]
async fn concurrent_selection_requests_leave_one_worker_on_the_final_preference() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let config = inputs(3);
    engine.ensure("owned", &config).await.unwrap();
    let (a, b) = tokio::join!(
        engine.select_input_guarded("owned", &config, 1, std::future::ready(true)),
        engine.select_input_guarded("owned", &config, 2, std::future::ready(true))
    );
    a.unwrap();
    b.unwrap();
    let preferred = engine.preferred_input("owned", &config).await.unwrap();
    let worker = engine.ensure("owned", &config).await.unwrap();
    assert_eq!(worker.stats()["input_index"], preferred);
    assert_eq!(engine.count().await, 1);
    engine.stop_all().await;
}

#[tokio::test]
async fn changing_the_effective_input_list_clears_the_old_selection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let old = inputs(3);
    engine
        .select_input_guarded("owned", &old, 2, std::future::ready(true))
        .await
        .unwrap();
    let changed = inputs(2);
    assert_eq!(engine.preferred_input("owned", &changed).await, None);
    let worker = engine.ensure("owned", &changed).await.unwrap();
    assert_eq!(worker.stats()["input_index"], 0);
    engine.stop_all().await;
}

#[tokio::test]
async fn stale_ensure_cannot_clear_a_newer_selected_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let old = inputs(2);
    let current = inputs(3);
    engine
        .select_input_guarded("owned", &current, 2, std::future::ready(true))
        .await
        .unwrap();
    assert!(
        engine
            .ensure_guarded("owned", &old, false, std::future::ready(false))
            .await
            .is_err()
    );
    assert_eq!(engine.preferred_input("owned", &current).await, Some(2));
    engine.stop_all().await;
}

#[tokio::test]
async fn stale_selection_cannot_retire_a_worker_from_a_newer_input_list() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let old = inputs(2);
    let changed = inputs(1);
    let current = engine.ensure("owned", &changed).await.unwrap();
    let result = engine
        .select_input_guarded("owned", &old, 1, std::future::ready(true))
        .await;
    assert!(result.is_err());
    assert!(!current.is_closed());
    assert_eq!(engine.stats("owned").await["pid"], current.pid());
    assert_eq!(engine.preferred_input("owned", &changed).await, None);
    engine.stop_all().await;
}

#[tokio::test]
async fn publisher_inputs_and_stale_selection_guards_are_rejected_without_worker_changes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let normal = inputs(2);
    let publisher = json!({"inputs":[{"url":"publish://"}]});
    assert!(
        engine
            .select_input_guarded("owned", &publisher, 0, std::future::ready(true))
            .await
            .is_err()
    );
    assert!(
        engine
            .select_input_guarded("owned", &normal, 1, std::future::ready(false))
            .await
            .is_err()
    );
    assert_eq!(engine.preferred_input("owned", &normal).await, None);
    assert_eq!(engine.count().await, 0);
}
