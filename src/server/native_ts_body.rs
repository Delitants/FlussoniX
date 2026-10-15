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
    rx: Receiver,
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
        // Avoid queue/counter snapshots when this diagnostic target is disabled.
        if !tracing::enabled!(target: "flussonix::native_ts", tracing::Level::INFO) {
            return;
        }
        let end = self.end.unwrap_or(End::BodyDropped);
        let worker = &self.viewer.0;
        let (records, bytes) = worker.ts_backlog();
        tracing::info!(target: "flussonix::native_ts",
            event = "native_ts_delivery_end",
            delivery_id = self.id,
            worker_pid = worker.pid(),
            reason = end.reason(),
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

pub(super) fn body(worker: Arc<Worker>, grant: Grant, egress: Arc<AtomicU64>) -> Body {
    let worker_bytes_at_start = worker.bytes.load(Ordering::Relaxed);
    let rx = worker.subscribe();
    worker.viewers.fetch_add(1, Ordering::Relaxed);
    let delivery = Delivery {
        rx,
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
                    delivery.viewer.1.add_bytes(bytes.len());
                    delivery
                        .egress
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    delivery.bytes_yielded =
                        delivery.bytes_yielded.saturating_add(bytes.len() as u64);
                    Some((Ok::<Bytes, std::io::Error>(bytes), delivery))
                }
                Err(end) => {
                    delivery.end = Some(end);
                    drop(delivery);
                    None
                }
            }
        },
    ))
}
