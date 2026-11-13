//! Fixed, credential-free observations; these never decide retry eligibility.
use super::{InputFailure, Transport};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, Ordering};

#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum Phase {
    ControlRequest,
    ControlResponse,
    ControlRead,
    SignalDecode,
    SegmentRequest,
    SegmentResponse,
    SegmentRead,
    SegmentDecode,
    M4sDecode,
    BridgeMetadata,
    BridgeFrame,
    OutputWrite,
    Relay,
}
const PHASES: [&str; 13] = [
    "control_request",
    "control_response",
    "control_read",
    "signal_decode",
    "segment_request",
    "segment_response",
    "segment_read",
    "segment_decode",
    "m4s_decode",
    "bridge_metadata",
    "bridge_frame",
    "output_write",
    "relay",
];
#[derive(Default)]
pub(crate) struct Diagnostics {
    phase: AtomicU8,
    status: AtomicU16,
    acknowledged: AtomicBool,
    metadata: AtomicBool,
}
impl Diagnostics {
    pub(crate) fn phase(slot: Option<&Self>, phase: Phase) {
        if let Some(slot) = slot {
            slot.phase.store(phase as u8, Ordering::Relaxed);
            if matches!(phase, Phase::ControlRequest | Phase::SegmentRequest) {
                slot.status.store(0, Ordering::Relaxed);
                slot.acknowledged.store(false, Ordering::Relaxed);
            }
        }
    }
    pub(crate) fn response(slot: Option<&Self>, status: u16, acknowledged: bool) {
        if let Some(slot) = slot {
            slot.status.store(status, Ordering::Relaxed);
            slot.acknowledged.store(acknowledged, Ordering::Relaxed);
        }
    }
    pub(crate) fn metadata(slot: Option<&Self>) {
        if let Some(slot) = slot {
            slot.metadata.store(true, Ordering::Relaxed);
        }
    }
    pub(super) fn report(&self, protocol: Transport, failure: InputFailure) {
        if failure == InputFailure::Revoked {
            return;
        }
        let protocol = match protocol {
            Transport::M4f => "m4f",
            Transport::M4s => "m4s",
        };
        let phase = PHASES[self.phase.load(Ordering::Relaxed) as usize];
        tracing::warn!(target: "flussonix::private_input", protocol, phase, ?failure,
            last_http_status = self.status.load(Ordering::Relaxed),
            last_private_ack = self.acknowledged.load(Ordering::Relaxed),
            metadata_sent = self.metadata.load(Ordering::Relaxed),
            "private media input failed");
    }
}
