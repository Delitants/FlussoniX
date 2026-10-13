//! Borrowed admission retained through one cancellable private input session.
use super::{AdmissionFailure, AdmittedCandidate, ModernSourceCoordinator};
use crate::{caption_hls, m4s::Track, wire::Hub};
use std::sync::atomic::AtomicU64;
use tokio::{io::AsyncWrite, sync::oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Default)]
pub struct PrivatePullOptions<'a> {
    /// Keep native text in the native relay; default filters it out.
    pub preserve_subtitles: bool,
    /// Optional owning worker's observed native text-track count.
    pub detected_subtitles: Option<&'a AtomicU64>,
    /// Feed selected native text to existing HLS conversion before filtering.
    pub conversion: Option<&'a caption_hls::State>,
    /// Optional cancellation owned by this input/worker generation only.
    pub cancel: Option<&'a CancellationToken>,
}
/// Generic diagnostics contain no input URL, name, media body or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivatePullFailure {
    Authority(AdmissionFailure),
    Cancelled,
    Input,
}
impl ModernSourceCoordinator {
    /// Retain admission through actual private HTTP and every publication fence.
    /// Idle/pending I/O also rechecks authority on a nominal 100 ms timer. No
    /// detached task, retry, worker stop, shared-hub close or lease renewal occurs.
    /// Instant checks cannot recall bytes committed before authority changed.
    pub async fn pull_admitted<W: AsyncWrite + Unpin>(
        &self,
        admitted: &AdmittedCandidate,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
        options: PrivatePullOptions<'_>,
    ) -> Result<(), PrivatePullFailure> {
        let check = || {
            if options.cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err(PrivatePullFailure::Cancelled);
            }
            self.admitted_current(admitted)
                .map_err(PrivatePullFailure::Authority)
        };
        check()?;
        let input = self
            .publish_admitted(admitted, |_, input, _| input)
            .map_err(PrivatePullFailure::Authority)?;
        let authority = || check().is_ok();
        let pull = input.pull_guarded(
            output,
            hub,
            metadata,
            crate::m4_ingest::PullOptions {
                preserve: options.preserve_subtitles,
                detected: options.detected_subtitles,
                conversion: options.conversion,
                ..Default::default()
            },
            &authority,
        );
        tokio::pin!(pull);
        let owner_cancelled = async {
            if let Some(cancel) = options.cancel {
                cancel.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(owner_cancelled);
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased;
                _ = &mut owner_cancelled => return Err(PrivatePullFailure::Cancelled),
                _ = admitted.cancelled() => return Err(PrivatePullFailure::Authority(AdmissionFailure::Revoked)),
                _ = tick.tick() => check()?,
                result = &mut pull => {
                    check()?;
                    return result.map_err(|_| PrivatePullFailure::Input);
                }
            }
        }
    }
}
