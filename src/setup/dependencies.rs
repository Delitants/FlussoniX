use super::SetupError;
use serde::Serialize;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ToolStatus {
    pub available: bool,
    pub detail: String,
}

impl ToolStatus {
    pub fn available(detail: impl Into<String>) -> Self {
        Self {
            available: true,
            detail: detail.into(),
        }
    }

    pub fn missing(detail: impl Into<String>) -> Self {
        Self {
            available: false,
            detail: detail.into(),
        }
    }

    pub fn failed(detail: impl Into<String>) -> Self {
        Self {
            available: false,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CapabilityStatus {
    pub name: String,
    pub available: bool,
    pub detail: String,
}

impl CapabilityStatus {
    pub fn available(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: true,
            detail: detail.into(),
        }
    }

    pub fn unavailable(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            available: false,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DependencyReport {
    pub ffmpeg: ToolStatus,
    pub ffprobe: ToolStatus,
    pub h264_aac_profile: ToolStatus,
    pub srt: ToolStatus,
    pub srtp: ToolStatus,
    pub gpu: Vec<CapabilityStatus>,
}

impl DependencyReport {
    pub fn required_ready(&self) -> bool {
        self.ffmpeg.available
            && self.ffprobe.available
            && self.h264_aac_profile.available
            && self.srt.available
            && self.srtp.available
    }
}

pub(crate) async fn inspect() -> Result<DependencyReport, SetupError> {
    let ffmpeg = version(Path::new("/usr/bin/ffmpeg"), "ffmpeg").await;
    let ffprobe = version(Path::new("/usr/bin/ffprobe"), "ffprobe").await;
    let h264_aac_profile = if ffmpeg.available {
        profile().await
    } else {
        ToolStatus::missing("install the Ubuntu ffmpeg package")
    };
    let srt = if crate::srt_playback::native_available() {
        ToolStatus::available("system libsrt 1.5 is available")
    } else {
        ToolStatus::missing("install libsrt1.5-gnutls")
    };
    let srtp = if crate::direct_rtp::crypto::availability() {
        ToolStatus::available("system libsrtp2 is available")
    } else {
        ToolStatus::missing("install libsrtp2-1")
    };
    let gpu = crate::gpu::setup_report("/usr/bin/ffmpeg")
        .await
        .into_iter()
        .map(|status| {
            if status.available {
                CapabilityStatus::available(status.name, status.detail)
            } else {
                CapabilityStatus::unavailable(status.name, status.detail)
            }
        })
        .collect();
    Ok(DependencyReport {
        ffmpeg,
        ffprobe,
        h264_aac_profile,
        srt,
        srtp,
        gpu,
    })
}

async fn version(path: &Path, name: &str) -> ToolStatus {
    let result = bounded_command(path, &["-version"], Duration::from_secs(5)).await;
    match result {
        Ok(output) if output.status => {
            let first = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .unwrap_or(name)
                .chars()
                .take(256)
                .collect::<String>();
            ToolStatus::available(first)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ToolStatus::missing(format!("install the Ubuntu ffmpeg package for {name}"))
        }
        Ok(_) | Err(_) => ToolStatus::failed(format!("{name} readiness probe failed")),
    }
}

async fn profile() -> ToolStatus {
    let arguments = [
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x90:rate=10",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:sample_rate=48000",
        "-t",
        "0.1",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        "-f",
        "null",
        "-",
    ];
    match bounded_command(
        Path::new("/usr/bin/ffmpeg"),
        &arguments,
        Duration::from_secs(5),
    )
    .await
    {
        Ok(output) if output.status => {
            ToolStatus::available("H.264 video and AAC audio encoding are available")
        }
        Ok(_) | Err(_) => ToolStatus::failed("H.264/AAC readiness probe failed"),
    }
}

struct Output {
    status: bool,
    stdout: Vec<u8>,
}

async fn bounded_command(
    path: &Path,
    arguments: &[&str],
    timeout: Duration,
) -> Result<Output, std::io::Error> {
    let mut command = crate::child_process::command(path);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().expect("piped dependency stdout");
    let stderr = child.stderr.take().expect("piped dependency stderr");
    let drain = async move {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut bounded_stdout = stdout.take(16 * 1024 + 1);
        let mut bounded_stderr = stderr.take(16 * 1024 + 1);
        let out_read = bounded_stdout.read_to_end(&mut out);
        let err_read = bounded_stderr.read_to_end(&mut err);
        let (outcome, error) = tokio::join!(out_read, err_read);
        outcome?;
        error?;
        Ok::<_, std::io::Error>(out)
    };
    match tokio::time::timeout(timeout, async { tokio::join!(child.wait(), drain) }).await {
        Ok((Ok(status), Ok(stdout))) => Ok(Output {
            status: status.success(),
            stdout,
        }),
        Ok((Err(error), _)) | Ok((_, Err(error))) => {
            let _ = child.kill().await;
            Err(error)
        }
        Err(_) => {
            let _ = child.kill().await;
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "dependency probe timed out",
            ))
        }
    }
}
