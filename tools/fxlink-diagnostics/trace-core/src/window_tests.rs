use super::*;
use std::sync::Barrier;
const BIN: u64 = 100_000_000;
fn event(r: &Recorder, ns: u64) {
    r.record_timed(1, [0; 16], [0; 3], b"private", || ns);
}
#[test]
fn overwrite_is_charged_to_the_victim_interval() {
    let r = Recorder::new(2);
    event(&r, 0);
    event(&r, BIN);
    event(&r, BIN * 2);
    let bins = r.coverage.snapshot();
    assert_eq!(bins[0], [1, 1, 0, 1]);
    assert_eq!(bins[1], [1, 1, 0, 0]);
    assert_eq!(bins[2], [1, 1, 0, 0]);
}
#[test]
fn actual_contention_is_charged_to_its_attempt_interval() {
    let r = Arc::new(Recorder::new(2));
    let lock = r.state.lock().unwrap();
    let c = r.clone();
    std::thread::spawn(move || event(&c, BIN)).join().unwrap();
    drop(lock);
    event(&r, BIN * 2);
    assert_eq!(r.coverage.snapshot()[1], [1, 0, 1, 0]);
    assert_eq!(r.coverage.snapshot()[2], [1, 1, 0, 0]);
}
#[test]
fn freeze_waits_for_admitted_producer_and_blocks_later_records() {
    let r = Arc::new(Recorder::new(4));
    let arrived = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let (a, b, c) = (r.clone(), arrived.clone(), release.clone());
    let producer = std::thread::spawn(move || {
        a.record_timed(1, [0; 16], [0; 3], b"x", || {
            b.wait();
            c.wait();
            BIN
        })
    });
    arrived.wait();
    let (tx, rx) = std::sync::mpsc::channel();
    let a = r.clone();
    let closer = std::thread::spawn(move || {
        a.freeze();
        tx.send(()).unwrap();
    });
    while !r.closed.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
    assert!(rx.try_recv().is_err());
    release.wait();
    producer.join().unwrap();
    closer.join().unwrap();
    assert_eq!(r.snapshot().events.len(), 1);
    event(&r, BIN * 3);
    assert_eq!(r.snapshot().events.len(), 1);
    assert_eq!(r.coverage.snapshot()[1], [1, 1, 0, 0]);
}
#[test]
fn first_failure_survives_other_failures_and_export_contention() {
    let r = Arc::new(Recorder::new(2));
    let lock = r.state.lock().unwrap();
    r.failure(5, 7, 1);
    r.failure(8, 9, 2);
    drop(lock);
    let t = r.trigger().unwrap();
    assert_eq!((t.subscription, t.record, t.reason), (5, 7, 1));
    r.freeze();
    r.failure(99, 99, 1);
    assert_eq!(r.trigger().unwrap(), t);
}
#[test]
fn racing_failures_publish_one_complete_identity() {
    let r = Arc::new(Recorder::new(2));
    let mut threads = Vec::new();
    for i in 0..8 {
        let c = r.clone();
        threads.push(std::thread::spawn(move || c.failure(i, i + 7, 1)));
    }
    for t in threads {
        t.join().unwrap();
    }
    let t = r.trigger().unwrap();
    assert_eq!(t.record, t.subscription + 7);
}
#[test]
fn out_of_range_accounting_never_wraps_onto_a_valid_bucket() {
    let r = Recorder::new(2);
    event(&r, 500_000_000_000);
    assert_eq!(r.coverage.out_of_range.load(Ordering::Relaxed), 1);
    assert_eq!(r.coverage.snapshot()[0], [0; 4]);
}

struct Files {
    dir: PathBuf,
    keep: bool,
}
impl Files {
    fn new() -> Self {
        let root = std::env::var_os("FX_CHECKPOINT_TEST_DIR").map(PathBuf::from);
        let dir = root
            .clone()
            .unwrap_or_else(std::env::temp_dir)
            .join(format!(
                "owned-checkpoint-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        std::fs::create_dir_all(&dir).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self {
            dir,
            keep: root.is_some(),
        }
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        if !self.keep {
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
}
fn suffix(path: &Path, s: &str) -> PathBuf {
    let mut p = path.as_os_str().to_os_string();
    p.push(s);
    p.into()
}
fn wait_file(path: &Path) {
    let end = Instant::now() + std::time::Duration::from_secs(3);
    while !path.exists() {
        assert!(Instant::now() < end, "checkpoint did not export");
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}
#[test]
fn monitor_exports_first_failure_with_private_bound_metadata() {
    let files = Files::new();
    let path = files.dir.join("receiver.fxtrace");
    let r = Arc::new(Recorder::new(16));
    let monitor = checkpoint::Monitor::start(r.clone(), path.clone()).unwrap();
    r.record(3, [1; 16], [7, 2, 0], b"private-media-token");
    r.failure(0, 7, 1);
    r.failure(9, 8, 1);
    wait_file(&suffix(&path, ".coverage.json"));
    monitor.stop().unwrap();
    let m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(suffix(&path, ".coverage.json")).unwrap()).unwrap();
    assert_eq!(m["trigger"]["record"], 7);
    assert_eq!(m["trigger"]["subscription"], 0);
    assert_eq!(m["bins"].as_array().unwrap().len(), 5000);
    let data = std::fs::read(&path).unwrap();
    assert!(!data.windows(19).any(|x| x == b"private-media-token"));
    assert_eq!(
        m["trace_sha256"].as_str().unwrap(),
        digest(&data)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert!(r.checkpoint(&path).is_err());
}
#[test]
fn external_request_keeps_shared_identity_and_local_observation_time() {
    let files = Files::new();
    let path = files.dir.join("source.fxtrace");
    let r = Arc::new(Recorder::new(16));
    let first = Recorder::new(2);
    first.failure(0, 7, 1);
    let trigger = first.trigger().unwrap();
    checkpoint::write_private(&suffix(&path, ".request"), &trigger.value()).unwrap();
    let monitor = checkpoint::Monitor::start(r.clone(), path.clone()).unwrap();
    wait_file(&suffix(&path, ".coverage.json"));
    monitor.stop().unwrap();
    let own = r.trigger().unwrap();
    assert_eq!(own.id, trigger.id);
    assert_eq!(own.origin, "external");
}
#[test]
fn stopping_during_postroll_still_retains_a_checkpoint() {
    let files = Files::new();
    let path = files.dir.join("short.fxtrace");
    let r = Arc::new(Recorder::new(16));
    r.failure(0, 7, 1);
    let monitor = checkpoint::Monitor::start(r, path.clone()).unwrap();
    monitor.stop().unwrap();
    assert!(suffix(&path, ".coverage.json").exists());
}
#[test]
fn malformed_request_is_rejected_without_overwriting_it() {
    let files = Files::new();
    let path = files.dir.join("bad.fxtrace");
    let request = suffix(&path, ".request");
    std::fs::write(&request, b"bad").unwrap();
    let r = Arc::new(Recorder::new(16));
    let monitor = checkpoint::Monitor::start(r, path.clone()).unwrap();
    assert!(monitor.stop().is_err());
    assert_eq!(std::fs::read(request).unwrap(), b"bad");
    assert!(!path.exists());
}

#[test]
fn shutdown_cannot_overtake_an_admitted_failure_producer() {
    let files = Files::new();
    let path = files.dir.join("racing.fxtrace");
    let r = Arc::new(Recorder::new(16));
    let guard = r.enter().unwrap();
    let monitor = checkpoint::Monitor::start(r.clone(), path.clone()).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let closing = std::thread::spawn(move || {
        let result = monitor.stop();
        tx.send(result).unwrap();
    });
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(30))
            .is_err(),
        "shutdown overtook an admitted trigger"
    );
    let trigger = Trigger {
        id: "ab".repeat(16),
        subscription: 0,
        record: 7,
        reason: 1,
        at_ns: 0,
        origin: "local".into(),
    };
    r.first_failure.set(trigger).unwrap();
    drop(guard);
    closing.join().unwrap();
    rx.recv().unwrap().unwrap();
    assert!(suffix(&path, ".coverage.json").exists());
}

#[test]
fn exported_earlier_contention_is_separate_from_a_later_intact_interval() {
    let files = Files::new();
    let path = files.dir.join("earlier-contention.fxtrace");
    let r = Arc::new(Recorder::new(16));
    let lock = r.state.lock().unwrap();
    let c = r.clone();
    std::thread::spawn(move || c.record(1, [0; 16], [0; 3], b"lost"))
        .join()
        .unwrap();
    drop(lock);
    let lost = r.coverage.snapshot().iter().position(|b| b[2] > 0).unwrap();
    let after = (lost as u64 + 1) * coverage::BUCKET_NS + 1_000_000;
    while r.start.elapsed().as_nanos() < (after as u128) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    r.record(3, [1; 16], [7, 2, 0], b"intact");
    r.failure(0, 7, 1);
    let monitor = checkpoint::Monitor::start(r.clone(), path.clone()).unwrap();
    wait_file(&suffix(&path, ".coverage.json"));
    monitor.stop().unwrap();
    assert_eq!(r.snapshot().busy, 1);
    let bins = r.coverage.snapshot();
    assert_eq!(bins[lost], [1, 0, 1, 0]);
    assert!(bins
        .iter()
        .enumerate()
        .any(|(i, b)| i > lost && *b == [1, 1, 0, 0]));
}

#[test]
fn stop_decision_rechecks_trigger_after_acquiring_shutdown() {
    let r = Recorder::new(2);
    let stop = AtomicBool::new(false);
    // Worker has already observed no trigger, then the admitted producer
    // publishes before finish releases stop. The old decision returned true.
    assert!(r.trigger().is_none());
    r.failure(0, 7, 1);
    r.freeze();
    stop.store(true, Ordering::Release);
    assert!(
        !checkpoint::may_stop(&stop, &r),
        "stop skipped a published first failure"
    );
}
