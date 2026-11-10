use std::{io, time::Duration};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

/// One FFmpeg side output, scoped to the worker generation that created it.
pub(super) struct Bridge {
    listener: UnixListener,
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
}

impl Bridge {
    pub(super) fn bind() -> io::Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("flussonix-bridge-")
            .tempdir()?;
        let path = directory.path().join("media.sock");
        let listener = UnixListener::bind(&path)?;
        Ok(Self {
            listener,
            _directory: directory,
            path,
        })
    }

    pub(super) fn target(&self) -> String {
        format!("unix://{}", self.path.display())
    }

    /// Ignore other local connectors, but never let them restart the deadline.
    pub(super) async fn accept(
        self,
        expected_pid: u32,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Option<UnixStream>, &'static str> {
        if cancel.is_cancelled() {
            return Ok(None);
        }
        if expected_pid == 0 {
            return Err("wire_setup_failed");
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err("startup_timeout");
            }
            let accepted = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(None),
                accepted = tokio::time::timeout_at(deadline, self.listener.accept()) => accepted,
            };
            let (socket, _) = accepted
                .map_err(|_| "startup_timeout")?
                .map_err(|_| "wire_setup_failed")?;
            if socket
                .peer_cred()
                .ok()
                .and_then(|credentials| credentials.pid())
                .and_then(|pid| u32::try_from(pid).ok())
                == Some(expected_pid)
            {
                return Ok(Some(socket));
            }
            // Closing an impostor socket does not consume the worker bridge.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn rejects_impostor_then_admits_the_actual_ffmpeg_child() {
        let bridge = Bridge::bind().unwrap();
        let target = bridge.target();
        let path = target.strip_prefix("unix://").unwrap();
        let mut impostor = tokio::net::UnixStream::connect(path).await.unwrap();
        impostor.write_all(b"impostor").await.unwrap();

        let tee = format!("[select='a':f=mpegts]{target}");
        let mut child = tokio::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "anullsrc",
                "-t",
                "0.15",
                "-map",
                "0:a:0",
                "-c:a",
                "mp2",
                "-f",
                "tee",
                &tee,
            ])
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let cancel = CancellationToken::new();
        let mut accepted = bridge
            .accept(pid, Duration::from_secs(5), &cancel)
            .await
            .unwrap()
            .unwrap();
        let mut header = [0; 188];
        accepted.read_exact(&mut header).await.unwrap();
        assert_eq!(header[0], 0x47, "authenticated child supplies MPEG-TS");
        assert_ne!(&header[..8], b"impostor");
        assert!(
            !std::path::Path::new(path).exists(),
            "bridge is removed after admission"
        );
        assert!(child.wait().await.unwrap().success());
    }

    #[tokio::test]
    async fn wrong_pid_cannot_extend_startup_deadline() {
        let bridge = Bridge::bind().unwrap();
        let target = bridge.target();
        let path = target.strip_prefix("unix://").unwrap().to_owned();
        let cancel = CancellationToken::new();
        let flood = tokio::spawn(async move {
            for _ in 0..20 {
                if tokio::net::UnixStream::connect(&path).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let started = tokio::time::Instant::now();
        let result = bridge
            .accept(u32::MAX, Duration::from_millis(75), &cancel)
            .await;
        assert_eq!(result.err(), Some("startup_timeout"));
        assert!(started.elapsed() < Duration::from_millis(300));
        flood.await.unwrap();
        assert!(!std::path::Path::new(target.strip_prefix("unix://").unwrap()).exists());
    }

    #[tokio::test]
    async fn cancellation_removes_bridge_without_accepting_a_peer() {
        let bridge = Bridge::bind().unwrap();
        let target = bridge.target();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            bridge
                .accept(std::process::id(), Duration::from_secs(1), &cancel)
                .await
                .unwrap()
                .is_none()
        );
        assert!(!std::path::Path::new(target.strip_prefix("unix://").unwrap()).exists());
    }

    #[tokio::test]
    async fn unavailable_child_pid_fails_closed_and_removes_bridge() {
        let bridge = Bridge::bind().unwrap();
        let target = bridge.target();
        let cancel = CancellationToken::new();
        assert_eq!(
            bridge
                .accept(0, Duration::from_secs(1), &cancel)
                .await
                .err(),
            Some("wire_setup_failed")
        );
        assert!(!std::path::Path::new(target.strip_prefix("unix://").unwrap()).exists());
    }
}
