//! State retained by one private M4F input across control sessions.
use crate::{m4s::Track, worker_ts::Muxer};
use std::collections::VecDeque;

#[derive(Default)]
pub(crate) struct M4fSession {
    pub(super) tracks: Vec<Track>,
    pub(super) declared: Option<Vec<Track>>,
    pub(super) muxer: Option<Muxer>,
    pub(super) seen: VecDeque<String>,
    pub(super) skipped_startup_segment: Option<String>,
    pub(super) accepted_segments: u64,
}
impl M4fSession {
    pub(crate) fn accepted_segments(&self) -> u64 {
        self.accepted_segments
    }
}
