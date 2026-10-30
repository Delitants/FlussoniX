//! Retry provenance comes from actual I/O boundaries, never diagnostic strings.
use std::{
    io,
    pin::Pin,
    sync::atomic::{AtomicU8, Ordering},
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum InputFailure {
    Invalid,
    Transient,
    Rejected,
    Local,
    Revoked,
    CleanControlEof,
    ContinuityGap,
}
pub(in crate::m4_ingest) fn record(
    slot: Option<&AtomicU8>,
    kind: InputFailure,
    message: &'static str,
) -> &'static str {
    if let Some(slot) = slot {
        slot.store(kind as u8, Ordering::Relaxed);
    }
    message
}
pub(super) fn read(slot: &AtomicU8) -> InputFailure {
    match slot.load(Ordering::Relaxed) {
        1 => InputFailure::Transient,
        2 => InputFailure::Rejected,
        3 => InputFailure::Local,
        4 => InputFailure::Revoked,
        5 => InputFailure::CleanControlEof,
        6 => InputFailure::ContinuityGap,
        _ => InputFailure::Invalid,
    }
}
pub(in crate::m4_ingest) struct Writer<'a, W> {
    pub output: &'a mut W,
    pub slot: Option<&'a AtomicU8>,
}
impl<W: AsyncWrite + Unpin> AsyncWrite for Writer<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut *this.output).poll_write(cx, bytes);
        if matches!(result, Poll::Ready(Err(_))) {
            record(this.slot, InputFailure::Local, "media pipe closed");
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.output).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.output).poll_shutdown(cx)
    }
}
