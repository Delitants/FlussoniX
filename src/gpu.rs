//! Bounded, shared NVENC initialization checks. This is not media qualification.
use serde::Serialize;
use std::{process::Stdio, time::Duration};
use tokio::{process::Command, sync::OnceCell};

#[derive(Serialize)]
pub(crate) struct Readiness {
    encoder: &'static str,
    codec: &'static str,
    status: &'static str,
}
impl Readiness {
    fn error(&self) -> Option<&'static str> {
        match (self.encoder, self.status) {
            (_, "available") => None,
            ("h264_nvenc", "timed_out") => Some("NVIDIA H.264 encoder readiness check timed out"),
            ("hevc_nvenc", "timed_out") => Some("NVIDIA HEVC encoder readiness check timed out"),
            ("h264_nvenc", "probe_failed") => Some("NVIDIA H.264 encoder readiness check failed"),
            ("hevc_nvenc", "probe_failed") => Some("NVIDIA HEVC encoder readiness check failed"),
            ("h264_nvenc", _) => Some("NVIDIA H.264 encoder is unavailable on this host"),
            _ => Some("NVIDIA HEVC encoder is unavailable on this host"),
        }
    }
}
#[derive(Default)]
pub(crate) struct Checks {
    report: OnceCell<Vec<Readiness>>,
}
impl Checks {
    pub(crate) async fn report(&self, ffmpeg: &str) -> &[Readiness] {
        self.report
            .get_or_init(|| async {
                let mut report = Vec::with_capacity(2);
                for (encoder, codec) in [("h264_nvenc", "H.264"), ("hevc_nvenc", "HEVC / H.265")] {
                    report.push(Readiness {
                        encoder,
                        codec,
                        status: check(ffmpeg, encoder).await,
                    });
                }
                report
            })
            .await
    }
    pub(crate) async fn require(&self, ffmpeg: &str, encoder: &str) -> Result<(), String> {
        let report = self.report(ffmpeg).await;
        let result = report
            .iter()
            .find(|r| r.encoder == encoder)
            .expect("validated GPU encoder");
        match result.error() {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
}

async fn check(ffmpeg: &str, encoder: &str) -> &'static str {
    let mut cmd = Command::new(ffmpeg);
    cmd.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-threads",
        "2",
        "-f",
        "lavfi",
        "-i",
        "color=size=320x180:rate=25",
    ]);
    // Use the same encoding options as the live worker, including no B frames.
    crate::transcoder::Profile::resolve(
        &serde_json::json!({"transcoder":{"encoder":encoder,"acodec":"copy"}}),
        false,
    )
    .expect("known GPU profile")
    .apply(&mut cmd);
    cmd.args(["-frames:v", "1", "-an", "-f", "null", "-"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let Ok(mut child) = cmd.spawn() else {
        return "probe_failed";
    };
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(status)) if status.success() => "available",
        Ok(Ok(_)) => "unavailable",
        Ok(Err(_)) => {
            let _ = child.kill().await;
            "probe_failed"
        }
        Err(_) => {
            // kill awaits wait/reaping; do not retain a hung encoder process.
            let _ = child.kill().await;
            "timed_out"
        }
    }
}

/// Only errors from this static vocabulary may cross the public playback boundary.
pub(crate) fn public_error(error: &str) -> Option<&str> {
    match error {
        "NVIDIA H.264 encoder readiness check timed out"
        | "NVIDIA HEVC encoder readiness check timed out"
        | "NVIDIA H.264 encoder readiness check failed"
        | "NVIDIA HEVC encoder readiness check failed"
        | "NVIDIA H.264 encoder is unavailable on this host"
        | "NVIDIA HEVC encoder is unavailable on this host" => Some(error),
        _ => None,
    }
}
