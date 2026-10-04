#[path = "support/dvb_fixture.rs"]
mod fixture;
use flussonix::{
    caption_hls::State,
    caption_transport::Transport,
    captions::{Decoder, configuration},
};
use futures_util::task::{ArcWake, waker};
use serde_json::json;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
static PROCESS_FIXTURES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl ArcWake for WakeCount {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::Relaxed);
    }
}
fn state() -> Arc<State> {
    Arc::new(State::new(Decoder::new(configuration(&json!({"flussonix_hls_captions":[{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"}]})).unwrap()), "owned-ocr-events".into(), 0))
}
#[tokio::test]
async fn work_queued_before_subscription_is_processed_without_another_event() {
    let _fixture_guard = PROCESS_FIXTURES.lock().await;
    let s = state();
    let mut transport = Transport::default();
    let (mut v, mut sub) = (0, 0);
    s.push_source(&fixture::tables(&[(0x120, 1, 2)], 0), &mut transport);
    s.push_source(&fixture::carrier::video(0, &mut v), &mut transport);
    s.push_source(
        &fixture::carrier::pes(0x120, 90000, &fixture::tiny(1), &mut sub),
        &mut transport,
    );
    s.push_source(
        &fixture::carrier::video_reordered_padding(180000, 90000, &mut v),
        &mut transport,
    );
    assert_eq!(s.stats()["dvb_ocr"][0]["pending"], 1);
    let cancel = CancellationToken::new();
    let count = Arc::new(WakeCount::default());
    let mut worker = Box::pin(s.clone().ocr("/missing-owned-ocr".into(), cancel.clone()));
    assert!(
        worker
            .as_mut()
            .poll(&mut Context::from_waker(&waker(count)))
            .is_pending()
    );
    assert_eq!(s.stats()["dvb_ocr"][0]["pending"], 0);
    assert_eq!(s.stats()["dvb_ocr"][0]["last_error"], "dvb_ocr_unavailable");
    cancel.cancel();
    worker.await;
}
#[tokio::test]
async fn unrelated_av_packets_do_not_wake_idle_ocr_workers() {
    let _fixture_guard = PROCESS_FIXTURES.lock().await;
    let s = state();
    let mut transport = Transport::default();
    let mut video = 0;
    s.push_source(&fixture::tables(&[(0x120, 1, 2)], 0), &mut transport);
    s.push_source(&fixture::carrier::video(0, &mut video), &mut transport);
    let cancel = CancellationToken::new();
    let count = Arc::new(WakeCount::default());
    let mut worker = Box::pin(s.clone().ocr("/missing-owned-ocr".into(), cancel.clone()));
    assert!(
        worker
            .as_mut()
            .poll(&mut Context::from_waker(&waker(count.clone())))
            .is_pending()
    );
    s.push_source(&fixture::carrier::video(7200, &mut video), &mut transport);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    assert_eq!(s.stats()["dvb_ocr"][0]["pending"], 0);
    cancel.cancel();
    worker.await;
}
#[tokio::test]
async fn two_hundred_idle_workers_have_no_periodic_wakeups_and_cancel_cleanly() {
    let _fixture_guard = PROCESS_FIXTURES.lock().await;
    let cancel = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..100 {
        let s = state();
        for _ in 0..2 {
            let count = Arc::new(WakeCount::default());
            let mut task = Box::pin(s.clone().ocr("/missing-owned-ocr".into(), cancel.clone()));
            assert!(
                task.as_mut()
                    .poll(&mut Context::from_waker(&waker(count.clone())))
                    .is_pending()
            );
            workers.push((task, count));
        }
    }
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        workers
            .iter()
            .map(|(_, c)| c.0.load(Ordering::Relaxed))
            .sum::<usize>(),
        0,
        "idle OCR workers must wait for actual work or cancellation"
    );
    cancel.cancel();
    for (mut task, count) in workers {
        assert!(count.0.load(Ordering::Relaxed) > 0);
        assert_eq!(
            task.as_mut().poll(&mut Context::from_waker(&waker(count))),
            Poll::Ready(())
        );
    }
}
#[tokio::test]
async fn a_real_transport_image_wakes_both_workers_without_waiting_for_a_tick() {
    let _fixture_guard = PROCESS_FIXTURES.lock().await;
    let s = state();
    let cancel = CancellationToken::new();
    let mut transport = Transport::default();
    let (mut v, mut sub) = (0, 0);
    s.push_source(&fixture::tables(&[(0x120, 1, 2)], 0), &mut transport);
    s.push_source(&fixture::carrier::video(0, &mut v), &mut transport);
    let count = Arc::new(WakeCount::default());
    let wake = waker(count.clone());
    let mut cx = Context::from_waker(&wake);
    let mut first = Box::pin(s.clone().ocr("/missing-owned-ocr".into(), cancel.clone()));
    let mut second = Box::pin(s.clone().ocr("/missing-owned-ocr".into(), cancel.clone()));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    s.push_source(
        &fixture::carrier::pes(0x120, 90000, &fixture::tiny(1), &mut sub),
        &mut transport,
    );
    s.push_source(
        &fixture::carrier::video_reordered_padding(180000, 90000, &mut v),
        &mut transport,
    );
    assert_eq!(s.stats()["dvb_ocr"][0]["pending"], 1);
    assert_eq!(
        count.0.load(Ordering::Relaxed),
        2,
        "new work must immediately notify both independent waiters"
    );
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert_eq!(s.stats()["dvb_ocr"][0]["last_error"], "dvb_ocr_unavailable");
    cancel.cancel();
    assert!(first.as_mut().poll(&mut cx).is_ready());
    assert!(second.as_mut().poll(&mut cx).is_ready());
}
#[cfg(unix)]
#[tokio::test]
async fn clock_reset_notifies_and_reaps_an_active_child_without_polling() {
    use std::os::unix::fs::PermissionsExt;
    let _fixture_guard = PROCESS_FIXTURES.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let executable = dir.path().join("owned-ocr");
    let pidfile = dir.path().join("pid");
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\necho $$ > '{}'\nexec /bin/sleep 60\n",
            pidfile.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let s = state();
    let mut transport = Transport::default();
    let (mut v, mut sub) = (0, 0);
    s.push_source(&fixture::tables(&[(0x120, 1, 2)], 0), &mut transport);
    s.push_source(&fixture::carrier::video(0, &mut v), &mut transport);
    s.push_source(
        &fixture::carrier::pes(0x120, 90000, &fixture::tiny(1), &mut sub),
        &mut transport,
    );
    s.push_source(
        &fixture::carrier::video_reordered_padding(180000, 90000, &mut v),
        &mut transport,
    );
    let cancel = CancellationToken::new();
    let count = Arc::new(WakeCount::default());
    let wake = waker(count.clone());
    let mut cx = Context::from_waker(&wake);
    let mut worker = Box::pin(
        s.clone()
            .ocr(executable.to_str().unwrap().into(), cancel.clone()),
    );
    assert!(worker.as_mut().poll(&mut cx).is_pending());
    let pid = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(pid) = std::fs::read_to_string(&pidfile)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(worker.as_mut().poll(&mut cx).is_pending());
    count.0.store(0, Ordering::Relaxed);
    s.decoder.lock().unwrap().reset(180000);
    assert!(
        count.0.load(Ordering::Relaxed) > 0,
        "reset must wake the active worker immediately"
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        futures_util::future::poll_fn(|cx| {
            assert!(worker.as_mut().poll(cx).is_pending());
            if std::path::Path::new(&format!("/proc/{pid}")).exists() {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }),
    )
    .await
    .unwrap();
    assert_eq!(s.stats()["dvb_ocr"][0]["pending"], 0);
    assert_eq!(s.stats()["cues"], 0);
    cancel.cancel();
    worker.await;
}
