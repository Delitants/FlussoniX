//! Connection-wide hints from admitted subscriptions, with no media ownership.
use super::Source;
use quinn::congestion::{ApplicationData, DemandRegistration};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
struct Data {
    pending: Option<Arc<dyn Fn() -> Option<bool> + Send + Sync>>,
    source: CancellationToken,
    listener: CancellationToken,
}
impl ApplicationData for Data {
    fn pending_records(&self) -> Option<bool> {
        if self.source.is_cancelled() || self.listener.is_cancelled() {
            return None;
        }
        let pending = self
            .pending
            .as_ref()
            .map_or(Some(false), |pending| pending())?;
        if self.source.is_cancelled() || self.listener.is_cancelled() {
            return None;
        }
        Some(pending)
    }
}

pub(super) fn register(
    connection: &quinn::Connection,
    source: &Source,
    cancel: &CancellationToken,
) -> Result<Option<DemandRegistration>, String> {
    let Some(hub) = connection.congestion_state().application_demand() else {
        return Ok(None);
    };
    // Existing 64KiB subscription metadata and peer queue reservations cover
    // this small fixed state; the controller holds at most sixteen weak slots.
    hub.register(Arc::new(Data {
        pending: source.pending_records.clone(),
        source: source.cancelled.clone(),
        listener: cancel.clone(),
    }))
    .map(Some)
    .ok_or_else(|| "FXLink demand registration limit".into())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_sources_remain_unknown_and_both_cancellations_fence_demand() {
        let mut data = Data {
            pending: None,
            source: CancellationToken::new(),
            listener: CancellationToken::new(),
        };
        assert_eq!(data.pending_records(), Some(false));
        data.pending = Some(Arc::new(|| Some(true)));
        assert_eq!(data.pending_records(), Some(true));
        data.source.cancel();
        assert_eq!(data.pending_records(), None);
        data.source = CancellationToken::new();
        assert_eq!(data.pending_records(), Some(true));
        data.listener.cancel();
        assert_eq!(data.pending_records(), None);
    }
}

#[cfg(test)]
mod retirement_tests {
    use super::*;
    #[test]
    fn invalid_native_snapshot_retires_encoded_ready_demand() {
        let source = CancellationToken::new();
        let listener = CancellationToken::new();
        let data = Data {
            pending: Some(Arc::new(|| None)),
            source,
            listener,
        };
        assert_eq!(
            data.pending_records(),
            None,
            "invalid native snapshot must fence even a held ready guard"
        );
    }
}
