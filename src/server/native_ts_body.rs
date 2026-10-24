//! One termination event per continuous native HTTP TS body, including drops
//! while a transport write is pending. Never log viewer/source credentials.
use super::*;
use crate::media_queue::Receiver;
use tokio::sync::broadcast::error::RecvError;

static NEXT_DELIVERY: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
enum End {
    AuthorizationRevoked,
    WorkerStopped,
    QueueLagged(u64),
    QueueClosed,
    BodyDropped,
}

impl End {
    fn reason(self) -> &'static str {
        match self {
            Self::AuthorizationRevoked => "authorization_revoked",
            Self::WorkerStopped => "worker_stopped",
            Self::QueueLagged(_) => "queue_lagged",
            Self::QueueClosed => "queue_closed",
            Self::BodyDropped => "body_dropped",
        }
    }
}
struct Delivery {
    connection_watch: Option<crate::http_transport::Watch>,
    rx: Receiver,
    startup: Option<super::ts_startup::Startup>,
    viewer: ViewerGuard,
    egress: Arc<AtomicU64>,
    id: u64,
    started: Instant,
    worker_bytes_at_start: u64,
    bytes_yielded: u64,
    end: Option<End>,
}
impl Drop for Delivery {
    fn drop(&mut self) {
        // Fence the watcher before observing its reason. Otherwise a concurrent
        // cancellation could reset a reused connection after this event logs.
        if let Some(watch) = &mut self.connection_watch {
            watch.unregister();
        }
        // Avoid queue/counter snapshots when this diagnostic target is disabled.
        if !tracing::enabled!(target: "flussonix::native_ts", tracing::Level::INFO) {
            return;
        }
        let transport_reason = self
            .connection_watch
            .as_ref()
            .and_then(|watch| watch.reason());
        let end = match transport_reason {
            Some(crate::http_transport::Reason::Authorization) => End::AuthorizationRevoked,
            Some(crate::http_transport::Reason::Worker) => End::WorkerStopped,
            None => self.end.unwrap_or(End::BodyDropped),
        };
        let worker = &self.viewer.0;
        let (records, bytes) = worker.ts_backlog();
        tracing::info!(target: "flussonix::native_ts",
            event = "native_ts_delivery_end",
            delivery_id = self.id,
            worker_pid = worker.pid(),
            reason = end.reason(),
            transport_abort_requested = transport_reason.is_some(),
            missed_records = match end { End::QueueLagged(missed) => missed, _ => 0 },
            bytes_yielded = self.bytes_yielded,
            worker_bytes_since_start = worker.bytes.load(Ordering::Relaxed).saturating_sub(self.worker_bytes_at_start),
            backlog_records = records as u64,
            backlog_bytes = bytes as u64,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            authorization_cancelled = self.viewer.1.is_cancelled(),
            worker_closed = worker.is_closed(),
            worker_alive = worker.alive.load(Ordering::Relaxed),
            "Native MPEG-TS delivery ended"
        );
    }
}

pub(super) fn body(
    worker: Arc<Worker>,
    grant: Grant,
    egress: Arc<AtomicU64>,
    connection: Option<crate::http_transport::Control>,
) -> Body {
    let worker_bytes_at_start = worker.bytes.load(Ordering::Relaxed);
    let rx = worker.subscribe();
    worker.viewers.fetch_add(1, Ordering::Relaxed);
    let connection_watch = connection.map(|control| {
        let worker = worker.cancellation_token();
        let authorization = grant.cancellation_token();
        control.watch(async move {
            tokio::select! {
                biased;
                _ = authorization.cancelled() => crate::http_transport::Reason::Authorization,
                _ = worker.cancelled() => crate::http_transport::Reason::Worker,
            }
        })
    });
    let delivery = Delivery {
        connection_watch,
        rx,
        startup: Some(super::ts_startup::Startup::default()),
        viewer: ViewerGuard(worker, grant),
        egress,
        id: NEXT_DELIVERY.fetch_add(1, Ordering::Relaxed),
        started: Instant::now(),
        worker_bytes_at_start,
        bytes_yielded: 0,
        end: None,
    };
    Body::from_stream(futures_util::stream::unfold(
        delivery,
        |mut delivery| async move {
            let result = loop {
                // A startup with no usable keyframe may discard many queued
                // records. Keep that work cooperative even if recv is ready.
                tokio::task::consume_budget().await;
                let result = tokio::select! {
                biased;
                _ = delivery.viewer.1.cancelled() => Err(End::AuthorizationRevoked),
                _ = delivery.viewer.0.closed() => Err(End::WorkerStopped),
                result = delivery.rx.recv() => result.map_err(|error| match error {
                    RecvError::Lagged(missed) => End::QueueLagged(missed),
                    RecvError::Closed => End::QueueClosed,
                }),
                };
                match result {
                    Ok(bytes) => {
                        if let Some(startup) = &mut delivery.startup {
                            if let Some(bytes) = startup.push(bytes) {
                                delivery.startup = None;
                                break Ok(bytes);
                            }
                        } else {
                            break Ok(bytes);
                        }
                    }
                    Err(end) => break Err(end),
                }
            };
            match result {
                Ok(bytes) => {
                    delivery.viewer.1.add_bytes(bytes.len());
                    delivery
                        .egress
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    delivery.bytes_yielded =
                        delivery.bytes_yielded.saturating_add(bytes.len() as u64);
                    Some((Ok::<Bytes, std::io::Error>(bytes), delivery))
                }
                Err(end) => {
                    if let Some(watch) = &delivery.connection_watch {
                        match end {
                            End::AuthorizationRevoked => {
                                watch.abort(crate::http_transport::Reason::Authorization)
                            }
                            End::WorkerStopped => {
                                watch.abort(crate::http_transport::Reason::Worker)
                            }
                            _ => {}
                        }
                    }
                    delivery.end = Some(end);
                    drop(delivery);
                    None
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    async fn cancelled_during_startup(authorization: bool) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "/usr/bin/ffmpeg");
        let worker = engine
            .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
            .await
            .unwrap();
        let grant = Grant::peer();
        let cancel = grant.cancellation_token();
        let egress = Arc::new(AtomicU64::new(0));
        let control = crate::http_transport::Control::default();
        let mut body =
            body(worker.clone(), grant, egress.clone(), Some(control.clone())).into_data_stream();
        // No worker task has run yet. A real body poll waits in startup and
        // must still be interruptible before any media can be yielded.
        let pending = futures_util::poll!(body.next()).is_pending();
        if authorization {
            cancel.cancel();
        } else {
            engine.stop_all().await;
        }
        let ended = matches!(
            tokio::time::timeout(Duration::from_secs(1), body.next()).await,
            Ok(None)
        );
        drop(body);
        engine.stop_all().await;
        assert!(pending && ended, "startup must not hide cancellation");
        assert!(
            control.is_aborted(),
            "native HTTP cancellation must request a TCP reset"
        );
        assert_eq!(egress.load(Ordering::Relaxed), 0);
        assert_eq!(worker.viewers.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn authorization_cancellation_interrupts_startup_and_resets_connection() {
        cancelled_during_startup(true).await;
    }
    #[tokio::test]
    async fn worker_cancellation_interrupts_startup_and_resets_connection() {
        cancelled_during_startup(false).await;
    }
}
