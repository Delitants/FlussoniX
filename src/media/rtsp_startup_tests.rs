use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn distinct_cold_rtsp_streams_cannot_probe_beyond_startup_capacity() {
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsp://{}/live", origin.local_addr().unwrap());
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    let origin_task = tokio::spawn(async move {
        let mut sockets = Vec::new();
        loop {
            let (socket, _) = origin.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            sockets.push(socket);
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let cfg = json!({"inputs":[{"url":url}]});
    let mut probes = tokio::task::JoinSet::new();
    for index in 0..32 {
        let engine = engine.clone();
        let cfg = cfg.clone();
        probes.spawn(async move { engine.ensure(&format!("cold-{index}"), &cfg).await });
    }
    let filled = tokio::time::timeout(Duration::from_secs(2), async {
        while accepted.load(Ordering::SeqCst) != 32 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let extra = tokio::time::timeout(
        Duration::from_millis(300),
        engine.ensure("excess-cold", &cfg),
    )
    .await;
    let total = accepted.load(Ordering::SeqCst);
    probes.abort_all();
    while probes.join_next().await.is_some() {}
    let later = {
        let engine = engine.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move { engine.ensure("after-cancel", &cfg).await })
    };
    let released = tokio::time::timeout(Duration::from_secs(1), async {
        while accepted.load(Ordering::SeqCst) != 33 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    later.abort();
    let _ = later.await;
    origin_task.abort();
    let _ = origin_task.await;
    assert!(
        filled,
        "initial probes did not reach the blocked RTSP origin"
    );
    assert!(
        matches!(extra, Ok(Err(ref e)) if e == "RTSP probe limit reached"),
        "extra cold stream was not rejected before its RTSP probe"
    );
    assert_eq!(total, 32, "extra cold stream reached the RTSP origin");
    assert!(
        released,
        "cancelled probes did not release startup capacity"
    );
}

#[tokio::test]
async fn dead_same_name_worker_does_not_probe_when_live_capacity_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let synthetic = json!({"inputs":[{"url":"testsrc://"}]});
    let dead = engine.ensure("dead", &synthetic).await.unwrap();
    engine.stop("dead").await;
    assert!(!dead.alive.load(Ordering::Relaxed));
    let live = engine.ensure("live", &synthetic).await.unwrap();
    {
        let mut workers = engine.workers.lock().await;
        for index in 1..256 {
            workers.insert(format!("live-{index}"), live.clone());
        }
        workers.insert("dead".into(), dead);
    }
    let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = json!({"inputs":[{"url":format!("rtsp://{}/live", origin.local_addr().unwrap())}]});
    let result =
        tokio::time::timeout(Duration::from_millis(300), engine.ensure("dead", &cfg)).await;
    let contacted = tokio::time::timeout(Duration::from_millis(50), origin.accept())
        .await
        .is_ok();
    {
        let mut workers = engine.workers.lock().await;
        workers.retain(|name, _| name == "live");
    }
    engine.stop_all().await;
    assert!(matches!(result, Ok(Err(ref e)) if e == "worker limit reached"));
    assert!(
        !contacted,
        "a dead worker must not authorize an upstream probe"
    );
}
