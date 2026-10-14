//! Owned SRT transport and independent bounded FFmpeg callers.
use flussonix::srt_playback::{self, Listener, Settings};
use std::{net::SocketAddr, process::Stdio, time::Duration};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
pub struct Serving {
    pub address: SocketAddr,
    cancel: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}
impl Serving {
    pub async fn start(app: std::sync::Arc<flussonix::server::App>, secret: &str) -> Self {
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            Settings::new(120, 8, secret.into()).unwrap(),
        )
        .unwrap();
        let address = listener.address();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(srt_playback::serve(listener, app, cancel.clone()));
        Self {
            address,
            cancel,
            task,
        }
    }
    pub async fn stop(self) {
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
pub struct Receiver {
    child: Child,
    errors: JoinHandle<Vec<u8>>,
}
impl Receiver {
    pub fn start(address: SocketAddr, token: &str, secret: &str, frames: &str) -> Self {
        Self::output(address, token, secret, frames, None)
    }
    pub fn record(
        address: SocketAddr,
        token: &str,
        secret: &str,
        frames: &str,
        path: &std::path::Path,
    ) -> Self {
        Self::output(address, token, secret, frames, Some(path))
    }
    fn output(
        address: SocketAddr,
        token: &str,
        secret: &str,
        frames: &str,
        path: Option<&std::path::Path>,
    ) -> Self {
        let mut cmd = Command::new("ffmpeg");
        cmd.args([
            "-v",
            "error",
            "-xerror",
            "-nostdin",
            "-threads",
            "1",
            "-readrate",
            "1",
            "-analyzeduration",
            "500000",
            "-probesize",
            "131072",
            "-srt_streamid",
            &format!("#!::r=owned,m=request,u={token}"),
        ]);
        if !secret.is_empty() {
            cmd.args(["-passphrase", secret]);
        }
        cmd.args([
            "-i",
            &format!(
                "srt://{address}?mode=caller&latency=120000&connect_timeout=1000&timeout=3000000"
            ),
            "-map",
            "0:a:0",
            "-frames:a",
            frames,
        ]);
        if let Some(path) = path {
            cmd.args(["-c", "copy", "-f", "mpegts"]).arg(path);
        } else {
            cmd.args(["-f", "null", "-"]);
        }
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let errors = tokio::spawn(async move {
            let mut bytes = vec![];
            stderr.take(65536).read_to_end(&mut bytes).await.unwrap();
            bytes
        });
        Self { child, errors }
    }
    pub async fn finish(mut self) -> (bool, String) {
        let result = tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await;
        let ok = match result {
            Ok(result) => result.unwrap().success(),
            Err(_) => {
                self.child.kill().await.unwrap();
                let _ = self.child.wait().await;
                false
            }
        };
        let errors = self.errors.await.unwrap();
        (ok, String::from_utf8_lossy(&errors).into_owned())
    }
    pub fn running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }
    pub async fn stop(mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().await.unwrap();
        }
        let _ = self.child.wait().await;
        let _ = self.errors.await;
    }
}
