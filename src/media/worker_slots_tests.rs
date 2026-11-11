use super::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

type Releases = Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<()>>>>;

fn error<T>(result: Result<T, String>) -> String {
    match result {
        Ok(_) => panic!("worker start unexpectedly succeeded"),
        Err(error) => error,
    }
}

async fn wait_for_name(
    entered: &mut mpsc::UnboundedReceiver<String>,
    expected: &str,
    seen: &mut HashSet<String>,
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !seen.contains(expected) {
            seen.insert(entered.recv().await.expect("preparation hook closed"));
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{expected} did not enter detached preparation"));
}

async fn wait_for_available(engine: &Engine, permits: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while engine.generation_slots.available_permits() != permits {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "worker slots did not return: expected {permits}, got {}",
            engine.generation_slots.available_permits()
        )
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_start_slots_cover_detached_preparation_and_failed_spawn() {
    let root = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(root.path(), "/owned-missing-ffmpeg");
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let releases: Releases = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let hold = Arc::new(std::sync::atomic::AtomicBool::new(true));
    engine.before_remove_test_hook = Some(Arc::new({
        let releases = releases.clone();
        let hold = hold.clone();
        move |name| {
            let name = name.to_owned();
            let releases = releases.clone();
            let entered_tx = entered_tx.clone();
            let hold = hold.clone();
            Box::pin(async move {
                if !hold.load(Ordering::SeqCst) {
                    return;
                }
                let (release, wait) = oneshot::channel();
                releases.lock().unwrap().insert(name.clone(), release);
                entered_tx.send(name).unwrap();
                let _ = wait.await;
            })
        }
    }));
    let engine = Arc::new(engine);
    let config = Arc::new(json!({"inputs":[{"url":"testsrc://"}]}));
    let mut starts = Vec::new();
    for n in 0..256 {
        let engine = engine.clone();
        let config = config.clone();
        starts.push(tokio::spawn(async move {
            engine.ensure(&format!("held-{n}"), &config).await
        }));
    }
    let mut seen = HashSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while seen.len() != 256 {
            seen.insert(entered.recv().await.expect("preparation hook closed"));
        }
    })
    .await
    .expect("256 cold starts did not enter detached preparation");
    assert_eq!(engine.generation_slots.available_permits(), 0);

    let overflow = error(
        tokio::time::timeout(Duration::from_secs(1), engine.ensure("overflow", &config))
            .await
            .expect("257th start waited or entered generation preparation"),
    );
    assert_eq!(overflow, "worker limit reached");
    assert!(!seen.contains("overflow"));

    let aborted = starts.remove(0);
    aborted.abort();
    assert!(match aborted.await {
        Err(error) => error.is_cancelled(),
        Ok(_) => false,
    });
    assert_eq!(
        error(engine.ensure("still-full", &config).await),
        "worker limit reached"
    );
    assert_eq!(engine.generation_slots.available_permits(), 0);

    releases
        .lock()
        .unwrap()
        .remove("held-0")
        .expect("held-0 release")
        .send(())
        .ok();
    wait_for_available(&engine, 1).await;
    let replacement = {
        let engine = engine.clone();
        let config = config.clone();
        tokio::spawn(async move { engine.ensure("replacement", &config).await })
    };
    wait_for_name(&mut entered, "replacement", &mut seen).await;
    assert_eq!(engine.generation_slots.available_permits(), 0);

    hold.store(false, Ordering::SeqCst);
    for (_, release) in std::mem::take(&mut *releases.lock().unwrap()) {
        release.send(()).ok();
    }
    assert!(error(replacement.await.unwrap()).contains("cannot start FFmpeg"));
    for start in starts {
        assert!(error(start.await.unwrap()).contains("cannot start FFmpeg"));
    }
    wait_for_available(&engine, 256).await;
    let after = error(engine.ensure("after-failures", &config).await);
    assert!(after.contains("cannot start FFmpeg"), "{after}");
    wait_for_available(&engine, 256).await;
    engine.stop_all().await;
}

#[tokio::test]
async fn reusing_running_worker_does_not_consume_another_generation_slot() {
    let root = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(root.path(), "ffmpeg");
    engine.generation_slots = Arc::new(Semaphore::new(1));
    let config = json!({"inputs":[{"url":"testsrc://"}]});
    let first = engine.ensure("owned", &config).await.unwrap();
    assert_eq!(engine.generation_slots.available_permits(), 0);
    let second = engine.ensure("owned", &config).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(engine.generation_slots.available_permits(), 0);
    engine.stop_all().await;
    wait_for_available(&engine, 1).await;
}

#[tokio::test]
async fn retired_unreaped_worker_keeps_capacity() {
    let root = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(root.path(), "ffmpeg");
    engine.generation_slots = Arc::new(Semaphore::new(1));
    let (entered_tx, entered) = oneshot::channel();
    let entered_tx = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let (release, released) = oneshot::channel();
    let released = Arc::new(std::sync::Mutex::new(Some(released)));
    engine.before_reap_test_hook = Some(Arc::new({
        let entered_tx = entered_tx.clone();
        let released = released.clone();
        move |_| {
            let entered_tx = entered_tx.lock().unwrap().take();
            let released = released.lock().unwrap().take();
            Box::pin(async move {
                let (Some(entered_tx), Some(released)) = (entered_tx, released) else {
                    return;
                };
                entered_tx.send(()).ok();
                let _ = released.await;
            })
        }
    }));
    let engine = Arc::new(engine);
    let config = Arc::new(json!({"inputs":[{"url":"testsrc://"}]}));
    let first = engine.ensure("first", &config).await.unwrap();
    let old_pid = first.pid();
    assert!(old_pid > 0);
    let stopping = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.stop("first").await })
    };
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .expect("worker did not reach pre-reap boundary")
        .expect("pre-reap hook closed");

    let map_count_during_reap = engine.count().await;
    let denied = error(engine.ensure("second", &config).await);
    release.send(()).ok();
    tokio::time::timeout(Duration::from_secs(5), stopping)
        .await
        .expect("worker stop did not finish after releasing pre-reap hook")
        .unwrap();
    wait_for_available(&engine, 1).await;
    let old_pid_absent = !std::path::Path::new(&format!("/proc/{old_pid}")).exists();

    let replacement = engine.ensure("second", &config).await.unwrap();
    let replacement_pid = replacement.pid();
    engine.stop_all().await;
    wait_for_available(&engine, 1).await;

    assert_eq!(map_count_during_reap, 0);
    assert_eq!(denied, "worker limit reached");
    assert!(
        old_pid_absent,
        "retired FFmpeg child {old_pid} was not reaped"
    );
    assert!(replacement_pid > 0);
    assert!(!std::path::Path::new(&format!("/proc/{replacement_pid}")).exists());
}

#[tokio::test]
async fn cancelled_post_spawn_handoff_keeps_capacity_until_child_is_reaped() {
    let root = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(root.path(), "ffmpeg");
    engine.generation_slots = Arc::new(Semaphore::new(1));

    let (spawned_tx, spawned) = oneshot::channel();
    let spawned_tx = Arc::new(std::sync::Mutex::new(Some(spawned_tx)));
    let (release_publish, publish_released) = oneshot::channel();
    let publish_released = Arc::new(std::sync::Mutex::new(Some(publish_released)));
    engine.before_publish_test_hook = Some(Arc::new({
        let spawned_tx = spawned_tx.clone();
        let publish_released = publish_released.clone();
        move |_, pid| {
            let spawned_tx = spawned_tx.lock().unwrap().take();
            let publish_released = publish_released.lock().unwrap().take();
            Box::pin(async move {
                if let (Some(spawned_tx), Some(publish_released)) = (spawned_tx, publish_released) {
                    spawned_tx.send(pid).ok();
                    let _ = publish_released.await;
                }
            })
        }
    }));

    let (reap_tx, reap_entered) = oneshot::channel();
    let reap_tx = Arc::new(std::sync::Mutex::new(Some(reap_tx)));
    let (release_reap, reap_released) = oneshot::channel();
    let reap_released = Arc::new(std::sync::Mutex::new(Some(reap_released)));
    engine.before_reap_test_hook = Some(Arc::new({
        let reap_tx = reap_tx.clone();
        let reap_released = reap_released.clone();
        move |_| {
            let reap_tx = reap_tx.lock().unwrap().take();
            let reap_released = reap_released.lock().unwrap().take();
            Box::pin(async move {
                let (Some(reap_tx), Some(reap_released)) = (reap_tx, reap_released) else {
                    return;
                };
                reap_tx.send(()).ok();
                let _ = reap_released.await;
            })
        }
    }));

    let engine = Arc::new(engine);
    let config = Arc::new(json!({"inputs":[{"url":"testsrc://"}]}));
    let starting = {
        let engine = engine.clone();
        let config = config.clone();
        tokio::spawn(async move { engine.ensure("cancelled", &config).await })
    };
    let pid = tokio::time::timeout(Duration::from_secs(5), spawned)
        .await
        .expect("worker did not reach post-spawn handoff")
        .expect("post-spawn hook closed");
    assert!(pid > 0);
    let workers = engine.workers.lock().await;
    release_publish.send(()).ok();
    tokio::task::yield_now().await;
    starting.abort();
    drop(workers);
    assert!(match starting.await {
        Err(error) => error.is_cancelled(),
        Ok(_) => false,
    });

    tokio::time::timeout(Duration::from_secs(5), reap_entered)
        .await
        .expect("cancelled startup did not enter child reaping")
        .expect("pre-reap hook closed");
    assert_eq!(engine.generation_slots.available_permits(), 0);
    assert_eq!(
        error(engine.ensure("still-full", &config).await),
        "worker limit reached"
    );
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists());

    release_reap.send(()).ok();
    wait_for_available(&engine, 1).await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());

    let replacement = engine.ensure("replacement", &config).await.unwrap();
    let replacement_pid = replacement.pid();
    engine.stop_all().await;
    wait_for_available(&engine, 1).await;
    assert!(replacement_pid > 0);
    assert!(!std::path::Path::new(&format!("/proc/{replacement_pid}")).exists());
}
