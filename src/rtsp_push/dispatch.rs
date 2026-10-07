//! One service turn from the existing publisher loop.
use super::bridge::Frame;
use std::future::Future;
pub(super) enum Turn {
    Stalled,
    Feedback(Result<Frame, &'static str>),
    Reports,
    Controls,
    Progress,
    Paced,
    Packet(Result<crate::rtp::Packet, &'static str>),
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn next(
    deadline: tokio::time::Instant,
    feedback_used: usize,
    feedback: impl Future<Output = Result<Frame, &'static str>>,
    reports: impl Future<Output = ()>,
    controls: impl Future<Output = ()>,
    progress: impl Future<Output = ()>,
    paced: impl Future<Output = ()>,
    packet: impl Future<Output = Result<crate::rtp::Packet, &'static str>>,
) -> Turn {
    tokio::select! {biased;
        _=tokio::time::sleep_until(deadline)=>Turn::Stalled,
        _=reports=>Turn::Reports,
        _=controls=>Turn::Controls,
        _=progress=>Turn::Progress,
        frame=feedback,if feedback_used<8=>Turn::Feedback(frame),
        _=paced=>Turn::Paced,
        packet=packet=>Turn::Packet(packet),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};
    #[tokio::test]
    async fn continuous_admitted_feedback_cannot_starve_ready_media_without_queue_overflow() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        for _ in 0..16 {
            tx.try_send(Ok(Frame::Media(1, vec![0x80, 201, 0, 1, 0, 0, 0, 1])))
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        for used in 0..=8 {
            let feedback = async {
                let frame = rx.recv().await.unwrap();
                tx.try_send(Ok(Frame::Media(1, vec![0x80, 201, 0, 1, 0, 0, 0, 1])))
                    .unwrap();
                frame
            };
            let turn = next(
                deadline,
                used,
                feedback,
                pending(),
                pending(),
                pending(),
                ready(()),
                pending(),
            )
            .await;
            if used < 8 {
                assert!(matches!(turn, Turn::Feedback(_)));
            } else {
                assert!(
                    matches!(turn, Turn::Paced),
                    "eight admitted feedback frames must yield to ready media"
                );
            }
        }
    }
    #[tokio::test]
    async fn continuously_ready_feedback_yields_to_due_periodic_controls() {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let frame = || ready(Ok(Frame::Media(1, vec![0x80, 201, 0, 1, 0, 0, 0, 1])));
        assert!(matches!(
            next(
                deadline,
                0,
                frame(),
                ready(()),
                pending(),
                pending(),
                pending(),
                pending()
            )
            .await,
            Turn::Reports
        ));
        assert!(matches!(
            next(
                deadline,
                0,
                frame(),
                pending(),
                ready(()),
                pending(),
                pending(),
                pending()
            )
            .await,
            Turn::Controls
        ));
        assert!(matches!(
            next(
                tokio::time::Instant::now() - std::time::Duration::from_secs(1),
                0,
                frame(),
                ready(()),
                ready(()),
                ready(()),
                ready(()),
                pending()
            )
            .await,
            Turn::Stalled
        ));
    }
}
