use flussonix::{media::Engine, publish::Policy};
use serde_json::json;
use std::{sync::Arc, time::Duration};
fn config() -> serde_json::Value {
    json!({"inputs":[{"url":"publish://"}]})
}
#[test]
fn publisher_policy_is_separate_and_resolves_named_backend() {
    let root = json!({"auth_backends":[{"name":"billing","url":"https://auth.example/publish"}]});
    let p=Policy::from_config(&json!({"password":"owned-password","on_publish":"auth://billing","on_play":"https://viewer.example/auth"}), &root).unwrap();
    assert!(p.accepts_password("owned-password"));
    assert!(!p.accepts_password(""));
    assert!(!p.accepts_password("owned-password "));
    assert_eq!(p.url.as_deref(), Some("https://auth.example/publish"));
}
#[tokio::test]
async fn publication_is_fenced_exclusive_owned_and_reconnects_immediately() {
    let d = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(d.path(), "ffmpeg"));
    let cfg = config();
    assert!(engine.ensure("owned", &cfg).await.is_err());
    assert_eq!(
        engine.count().await,
        0,
        "a viewer cannot start an empty publication"
    );
    assert!(
        engine
            .publish_guarded("owned", &cfg, std::future::ready(false))
            .await
            .is_err()
    );
    assert_eq!(engine.count().await, 0);
    let p = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .expect("publisher starts worker");
    let old = p.worker.clone();
    assert!(p.stdin.is_some());
    assert_eq!(engine.ensure("owned", &cfg).await.unwrap().pid(), old.pid());
    assert!(
        engine
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .is_err(),
        "second publisher must not steal stdin"
    );
    drop(p);
    tokio::time::timeout(Duration::from_secs(3), old.closed())
        .await
        .unwrap();
    let replacement = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .expect("reconnect without pull backoff");
    assert_ne!(replacement.worker.pid(), old.pid());
    engine.stop_if_current("owned", &old).await;
    assert!(
        !replacement.worker.is_closed(),
        "late cleanup cannot stop replacement"
    );
    engine.stop_all().await;
    assert!(replacement.worker.is_closed());
}
