use super::{ReplicaAdmission, ReplicaFailure, ReplicaSelector};
use crate::{m4s::Track, source_directory::modern_coordinator::PrivatePullOptions, wire::Hub};
use std::time::Duration;
use tokio::{io::AsyncWrite, sync::oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaPullFailure {
    Selection(ReplicaFailure),
    Cancelled,
    Input,
}
impl ReplicaSelector {
    /// One caller-owned input session. No task, retry, failover or hub shutdown.
    /// Selection/source/grant loss interrupts idle input as well as active writes.
    pub async fn pull_admitted<W: AsyncWrite + Unpin>(
        &self,
        admitted: &ReplicaAdmission,
        output: &mut W,
        hub: Option<&Hub>,
        metadata: oneshot::Sender<Vec<Track>>,
        options: PrivatePullOptions<'_>,
    ) -> Result<(), ReplicaPullFailure> {
        let check = || {
            if options.cancel.is_some_and(|cancel| cancel.is_cancelled()) {
                return Err(ReplicaPullFailure::Cancelled);
            }
            self.admitted_current(admitted)
                .map_err(ReplicaPullFailure::Selection)
        };
        check()?;
        let input = self
            .publish_admitted(admitted, |_, input, _| input)
            .map_err(ReplicaPullFailure::Selection)?;
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
        let cancelled = async {
            if let Some(cancel) = options.cancel {
                cancel.cancelled().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(cancelled);
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {biased;
                _=&mut cancelled=>return Err(ReplicaPullFailure::Cancelled),
                _=admitted.cancelled()=>{check()?;return Err(ReplicaPullFailure::Selection(ReplicaFailure::Stale));},
                _=tick.tick()=>check()?,
                result=&mut pull=>{check()?;return result.map_err(|_|ReplicaPullFailure::Input);},
            }
        }
    }
}
