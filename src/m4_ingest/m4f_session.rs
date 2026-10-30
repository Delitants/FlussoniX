//! State retained by one private M4F input across control sessions.
use crate::{m4_ingest::Notification, m4s::Track, worker_ts::Muxer};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReplayDecision {
    Skip,
    Publish,
}

#[derive(Clone, Copy)]
struct Published {
    utc: u32,
    duration_ms: f64,
}

#[derive(Default)]
pub(crate) struct M4fSession {
    pub(super) tracks: Vec<Track>,
    pub(super) declared: Option<Vec<Track>>,
    pub(super) muxer: Option<Muxer>,
    pub(super) seen: VecDeque<String>,
    pub(super) skipped_startup_segment: Option<String>,
    pub(super) accepted_segments: u64,
    last_published: Option<Published>,
    replay_check: bool,
}
impl M4fSession {
    pub(crate) fn accepted_segments(&self) -> u64 {
        self.accepted_segments
    }

    pub(super) fn reconnect(&mut self) {
        self.replay_check = true;
    }

    pub(super) fn classify_notification(
        &self,
        notification: &Notification,
    ) -> Result<ReplayDecision, crate::m4_ingest::flussonic::InputFailure> {
        use crate::m4_ingest::flussonic::InputFailure;
        if self.seen.contains(&notification.name)
            || self.skipped_startup_segment.as_ref() == Some(&notification.name)
        {
            return Ok(ReplayDecision::Skip);
        }
        if let Some(last) = self.last_published {
            if notification.utc <= last.utc {
                return Err(InputFailure::ContinuityGap);
            }
            let deadline = u64::from(last.utc)
                .saturating_add((last.duration_ms / 1000.0).ceil() as u64)
                .saturating_add(1);
            if self.replay_check && u64::from(notification.utc) > deadline {
                return Err(InputFailure::ContinuityGap);
            }
        }
        Ok(ReplayDecision::Publish)
    }

    pub(super) fn published(&mut self, notification: &Notification) {
        self.last_published = Some(Published {
            utc: notification.utc,
            duration_ms: notification.duration_ms,
        });
        self.replay_check = false;
        self.accepted_segments = self.accepted_segments.saturating_add(1);
        self.seen.push_back(notification.name.clone());
        if self.seen.len() > 16 {
            self.seen.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m4_ingest::Notification;
    use bytes::Bytes;

    fn notice(second: u32) -> Notification {
        Notification {
            name: format!("2026/10/13/00/00/{second:02}.m4f"),
            stamp: format!("2026/10/13/00/00/{second:02}"),
            utc: 1_760_310_000 + second,
            sequence: second,
            duration_ms: 4_000.0,
            wire: Bytes::new(),
        }
    }

    #[test]
    fn replayed_names_skip_and_first_novel_gop_must_cover_cadence() {
        let mut session = M4fSession::default();
        session.published(&notice(0));
        session.reconnect();
        assert_eq!(
            session.classify_notification(&notice(0)),
            Ok(ReplayDecision::Skip)
        );
        assert_eq!(
            session.classify_notification(&notice(4)),
            Ok(ReplayDecision::Publish)
        );
        assert_eq!(
            session.classify_notification(&notice(8)),
            Err(crate::m4_ingest::flussonic::InputFailure::ContinuityGap)
        );
        session.published(&notice(4));
        assert_eq!(
            session.classify_notification(&notice(8)),
            Ok(ReplayDecision::Publish)
        );
        session.reconnect();
        assert_eq!(
            session.classify_notification(&notice(12)),
            Err(crate::m4_ingest::flussonic::InputFailure::ContinuityGap)
        );
    }

    #[test]
    fn unknown_backward_timestamp_is_not_treated_as_replay() {
        let mut session = M4fSession::default();
        session.published(&notice(8));
        session.reconnect();
        assert_eq!(
            session.classify_notification(&notice(4)),
            Err(crate::m4_ingest::flussonic::InputFailure::ContinuityGap)
        );
    }
}
