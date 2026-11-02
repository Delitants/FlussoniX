//! Bounded byte handoff between authenticated M4F ingest and its TS feed.
use bytes::Bytes;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{io::AsyncWrite, sync::mpsc};
use tokio_util::sync::PollSender;

const CHUNK_BYTES: usize = 64 * 1024;
const MAX_CHUNKS: usize = 128;

pub(super) struct Writer {
    sender: PollSender<Bytes>,
}

pub(super) fn bounded() -> (Writer, mpsc::Receiver<Bytes>) {
    let (sender, receiver) = mpsc::channel(MAX_CHUNKS);
    (
        Writer {
            sender: PollSender::new(sender),
        },
        receiver,
    )
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let n = bytes.len().min(CHUNK_BYTES);
        if n == 0 {
            return Poll::Ready(Ok(0));
        }
        match this.sender.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Ready(Ok(())) => {
                let chunk = Bytes::copy_from_slice(&bytes[..n]);
                Poll::Ready(
                    this.sender
                        .send_item(chunk)
                        .map(|_| n)
                        .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe)),
                )
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().sender.close();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn full_queue_waits_then_delivers_every_chunk_in_order() {
        let (mut writer, mut reader) = bounded();
        for value in 0..128u8 {
            writer.write_all(&vec![value; 64 * 1024]).await.unwrap();
        }
        let extra = vec![255; 64 * 1024];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                writer.write_all(&extra)
            )
            .await
            .is_err(),
            "the bounded producer must wait for its reader"
        );
        assert_eq!(reader.recv().await.unwrap().as_ref(), &[0; 64 * 1024]);
        writer.write_all(&extra).await.unwrap();
        drop(writer);
        for value in 1..128u8 {
            assert_eq!(
                reader.recv().await.unwrap().as_ref(),
                &vec![value; 64 * 1024]
            );
        }
        assert_eq!(reader.recv().await.unwrap().as_ref(), &extra);
        assert!(reader.recv().await.is_none());
    }

    #[tokio::test]
    async fn closed_reader_rejects_further_writes() {
        let (mut writer, reader) = bounded();
        drop(reader);
        let error = writer.write_all(b"lost").await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn sender_drop_preserves_already_queued_bytes() {
        let (mut writer, mut reader) = bounded();
        writer.write_all(b"first").await.unwrap();
        writer.write_all(b"second").await.unwrap();
        drop(writer);
        assert_eq!(reader.recv().await.unwrap().as_ref(), b"first");
        assert_eq!(reader.recv().await.unwrap().as_ref(), b"second");
        assert!(reader.recv().await.is_none());
    }
}
