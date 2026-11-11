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
