//! Optional synchronous fences at private request and publication boundaries.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;

pub(crate) type Authority<'a> = Option<&'a (dyn Fn() -> bool + Send + Sync)>;
pub(crate) fn check(authority: Authority<'_>) -> Result<(), String> {
    if authority.is_some_and(|current| !current()) {
        Err("media authority changed".into())
    } else {
        Ok(())
    }
}
pub(crate) struct Writer<'a, W> {
    pub inner: &'a mut W,
    pub authority: Authority<'a>,
}
impl<W: AsyncWrite + Unpin> AsyncWrite for Writer<'_, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if check(this.authority).is_err() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "media authority changed",
            )));
        }
        Pin::new(&mut *this.inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if check(this.authority).is_err() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "media authority changed",
            )));
        }
        Pin::new(&mut *this.inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if check(this.authority).is_err() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "media authority changed",
            )));
        }
        Pin::new(&mut *this.inner).poll_shutdown(cx)
    }
}
