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
    pending: Vec<u8>,
}

pub(super) fn bounded() -> (Writer, mpsc::Receiver<Bytes>) {
    let (sender, receiver) = mpsc::channel(MAX_CHUNKS);
    (
        Writer {
            sender: PollSender::new(sender),
            pending: Vec::new(),
        },
        receiver,
    )
}

impl Writer {
    fn poll_send_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.pending.is_empty() {
            return Poll::Ready(Ok(()));
        }
        match self.sender.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Ready(Ok(())) => Poll::Ready(
                self.sender
                    .send_item(Bytes::from(std::mem::take(&mut self.pending)))
                    .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe)),
            ),
        }
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.pending.len() == CHUNK_BYTES {
            match this.poll_send_pending(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        let n = bytes.len().min(CHUNK_BYTES - this.pending.len());
        this.pending.extend_from_slice(&bytes[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_send_pending(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.poll_send_pending(cx) {
            Poll::Ready(Ok(())) => {
                this.sender.close();
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn small_frame_writes_coalesce_and_flush_in_order() {
        let (mut writer, mut reader) = bounded();
        let expected = (0..1000usize)
            .flat_map(|value| vec![value as u8; 752])
            .collect::<Vec<_>>();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            for value in 0..1000usize {
                writer.write_all(&vec![value as u8; 752]).await.unwrap();
            }
            writer.flush().await.unwrap();
        })
        .await
        .expect("small frames exhausted record capacity before 8 MiB");
        assert!(reader.len() <= 12, "small writes were not coalesced");
        drop(writer);
        let mut actual = Vec::new();
        while let Some(chunk) = reader.recv().await {
            actual.extend_from_slice(&chunk);
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn full_queue_waits_then_delivers_every_chunk_in_order() {
        let (mut writer, mut reader) = bounded();
        for value in 0..128u8 {
            writer.write_all(&vec![value; 64 * 1024]).await.unwrap();
        }
        writer.flush().await.unwrap();
        let extra = vec![255; 64 * 1024];
        writer.write_all(&extra).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), writer.flush())
                .await
                .is_err(),
            "the bounded producer must wait for its reader"
        );
        assert_eq!(reader.recv().await.unwrap().as_ref(), &[0; 64 * 1024]);
        writer.flush().await.unwrap();
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
        writer.write_all(b"lost").await.unwrap();
        let error = writer.flush().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn sender_drop_preserves_already_queued_bytes() {
        let (mut writer, mut reader) = bounded();
        writer.write_all(b"first").await.unwrap();
        writer.write_all(b"second").await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(reader.recv().await.unwrap().as_ref(), b"firstsecond");
        assert!(reader.recv().await.is_none());
    }
}
