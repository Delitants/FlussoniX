use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn policy() -> Policy {
    Policy {
        grace: Duration::from_secs(5),
        cooldown: Duration::from_secs(300),
    }
}
async fn idle(controller: &Controller, since: Instant) {
    drop(controller.activity().await);
    controller.inner.state.lock().unwrap().idle_since = Some(since);
}
fn reserve(controller: &Controller, now: Instant) -> Option<Ticket> {
    controller.reserve(now, CancellationToken::new())
}

#[tokio::test]
async fn initial_empty_process_and_active_work_never_release() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    assert!(reserve(&controller, now + Duration::from_secs(600)).is_none());
    let active = controller.activity().await;
    assert!(reserve(&controller, now + Duration::from_secs(600)).is_none());
    drop(active);
}

#[tokio::test]
async fn grace_once_per_idle_generation_and_cooldown_are_enforced() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    assert!(reserve(&controller, now + Duration::from_millis(4999)).is_none());
    let when = now + Duration::from_secs(5);
    let ticket = reserve(&controller, when).unwrap();
    assert!(
        reserve(&controller, when).is_none(),
        "only one queued action"
    );
    assert!(
        ticket.execute(when, || false),
        "zero reclaimed pages still consumes this idle generation"
    );
    assert_eq!(controller.snapshot()["runs"], 1);
    assert_eq!(controller.snapshot()["last_released_memory"], false);
    assert!(reserve(&controller, now + Duration::from_secs(600)).is_none());
    idle(&controller, now + Duration::from_secs(10)).await;
    assert!(reserve(&controller, now + Duration::from_secs(304)).is_none());
    assert!(
        reserve(&controller, now + Duration::from_secs(306))
            .unwrap()
            .execute(now + Duration::from_secs(306), || true)
    );
    assert_eq!(controller.snapshot()["runs"], 2);
}

#[tokio::test]
async fn queued_release_rechecks_new_activity_and_completed_activity_aba() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    let ticket = reserve(&controller, now + Duration::from_secs(5)).unwrap();
    let active = controller.activity().await;
    assert!(!ticket.execute(now + Duration::from_secs(5), || panic!(
        "release overlapped active work"
    )));
    drop(active);
    idle(&controller, now).await;
    let ticket = reserve(&controller, now + Duration::from_secs(5)).unwrap();
    drop(controller.activity().await);
    assert!(!ticket.execute(now + Duration::from_secs(10), || panic!(
        "stale generation survived completed activity"
    )));
    assert_eq!(controller.snapshot()["runs"], 0);
}

#[tokio::test]
async fn cancellation_and_dropped_queued_work_release_the_reservation_without_running() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    let ticket = reserve(&controller, now + Duration::from_secs(5)).unwrap();
    ticket.cancel.cancel();
    assert!(!ticket.execute(now + Duration::from_secs(5), || panic!(
        "cancelled release ran"
    )));
    drop(reserve(&controller, now + Duration::from_secs(6)).unwrap());
    assert!(reserve(&controller, now + Duration::from_secs(7)).is_some());
    assert_eq!(controller.snapshot()["runs"], 0);
}

#[tokio::test]
async fn running_release_fences_preparation_without_blocking_the_async_runtime() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    let ticket = reserve(&controller, now + Duration::from_secs(5)).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let release = tokio::task::spawn_blocking(move || {
        ticket.execute(now + Duration::from_secs(5), || {
            entered_tx.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            true
        })
    });
    entered_rx.await.unwrap();
    let mut start = Box::pin(controller.activity());
    assert!(futures_util::poll!(&mut start).is_pending());
    assert_eq!(
        controller.snapshot()["active_operations"],
        1,
        "waiting starts are already counted"
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        tokio::time::sleep(Duration::from_millis(10)),
    )
    .await
    .unwrap();
    finish_tx.send(()).unwrap();
    assert!(release.await.unwrap());
    let active = tokio::time::timeout(Duration::from_secs(1), start)
        .await
        .unwrap();
    assert_eq!(controller.snapshot()["runs"], 1);
    assert!(reserve(&controller, now + Duration::from_secs(600)).is_none());
    drop(active);
}

#[tokio::test]
async fn cancelled_waiting_start_does_not_leave_phantom_activity() {
    let controller = Controller::new(policy());
    let write = controller.inner.gate.clone().write_owned().await;
    let mut pending = Box::pin(controller.activity());
    assert!(futures_util::poll!(&mut pending).is_pending());
    assert_eq!(controller.snapshot()["active_operations"], 1);
    drop(pending);
    drop(write);
    assert_eq!(controller.snapshot()["active_operations"], 0);
}

#[tokio::test]
async fn passive_status_reads_never_reset_idle_or_schedule_release() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    for _ in 0..100 {
        assert_eq!(controller.snapshot()["runs"], 0);
    }
    assert!(reserve(&controller, now + Duration::from_secs(5)).is_some());
}

#[tokio::test]
async fn failed_release_unwinds_the_fence_and_consumes_the_attempt() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    let ticket = reserve(&controller, now + Duration::from_secs(5)).unwrap();
    let result = tokio::task::spawn_blocking(move || {
        ticket.execute(now + Duration::from_secs(5), || {
            panic!("owned release failure")
        })
    })
    .await;
    assert!(result.is_err());
    let activity = tokio::time::timeout(Duration::from_secs(1), controller.activity())
        .await
        .unwrap();
    drop(activity);
    assert_eq!(controller.snapshot()["runs"], 1);
    assert!(reserve(&controller, now + Duration::from_secs(10)).is_none());
}

#[tokio::test]
async fn generation_exhaustion_fails_closed() {
    let controller = Controller::new(policy());
    controller.inner.state.lock().unwrap().generation = u64::MAX;
    idle(&controller, Instant::now()).await;
    assert!(reserve(&controller, Instant::now() + Duration::from_secs(600)).is_none());
}

#[tokio::test]
async fn abandoned_blocking_pool_ticket_does_not_hold_a_writer_or_delay_starts() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    let ticket = reserve(&controller, now + Duration::from_secs(5)).unwrap();
    let activity = tokio::time::timeout(Duration::from_secs(1), controller.activity())
        .await
        .unwrap();
    let effects = AtomicUsize::new(0);
    assert!(!ticket.execute(now + Duration::from_secs(6), || {
        effects.fetch_add(1, AtomicOrdering::Relaxed);
        true
    }));
    assert_eq!(effects.load(AtomicOrdering::Relaxed), 0);
    drop(activity);
}

#[tokio::test]
async fn dropping_maintenance_ownership_cancels_its_queued_release() {
    let controller = Controller::new(policy());
    let now = Instant::now();
    idle(&controller, now).await;
    let cancel = CancellationToken::new();
    let ticket = controller
        .reserve(now + Duration::from_secs(5), cancel.clone())
        .unwrap();
    let owner = Run {
        inner: controller.inner.clone(),
        cancel,
    };
    drop(owner);
    assert!(!ticket.execute(now + Duration::from_secs(5), || panic!(
        "abandoned maintenance ran"
    )));
    assert_eq!(controller.snapshot()["runs"], 0);
}

#[tokio::test]
async fn one_process_maintenance_owner_is_joined_on_cancellation() {
    let controller = Controller::new(policy());
    let cancel = CancellationToken::new();
    if !platform::supported() {
        controller.run(cancel).await;
        assert_eq!(controller.snapshot()["enabled"], false);
        return;
    }
    let c = controller.clone();
    let token = cancel.clone();
    let owner = tokio::spawn(async move { c.run(token).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while controller.snapshot()["enabled"] != true {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        controller.run(CancellationToken::new()),
    )
    .await
    .unwrap();
    assert_eq!(
        controller.snapshot()["enabled"],
        true,
        "second caller cannot replace or stop the owner"
    );
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), owner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(controller.snapshot()["enabled"], false);
    assert_eq!(
        controller.snapshot()["runs"],
        0,
        "empty initial process never compacts"
    );
}
