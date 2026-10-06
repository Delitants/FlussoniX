//! Bounded, shared NVENC initialization checks. This is not media qualification.
use serde::Serialize;
use std::{
    collections::HashMap,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{process::Command, sync::OnceCell};

#[derive(Clone, Serialize)]
pub(crate) struct Readiness {
    encoder: &'static str,
    codec: &'static str,
    status: &'static str,
}
impl Readiness {
    fn error(&self) -> Option<&'static str> {
        match (self.encoder, self.status) {
            (_, "available") => None,
            ("h264_vaapi", "timed_out") => Some("VAAPI H.264 encoder readiness check timed out"),
            ("hevc_vaapi", "timed_out") => Some("VAAPI HEVC encoder readiness check timed out"),
            ("h264_vaapi", "probe_failed") => Some("VAAPI H.264 encoder readiness check failed"),
            ("hevc_vaapi", "probe_failed") => Some("VAAPI HEVC encoder readiness check failed"),
            ("h264_vaapi", _) => Some("VAAPI H.264 encoder is unavailable on this host"),
            ("hevc_vaapi", _) => Some("VAAPI HEVC encoder is unavailable on this host"),
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
    vaapi: Mutex<HashMap<crate::transcoder::Vaapi, Arc<OnceCell<Readiness>>>>,
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
    async fn vaapi_check(
        &self,
        ffmpeg: &str,
        profile: &crate::transcoder::Profile,
    ) -> Result<Readiness, String> {
        let key = profile.vaapi().expect("VAAPI profile").clone();
        let cell = {
            let mut cache = self.vaapi.lock().unwrap();
            if !cache.contains_key(&key) && cache.len() >= 16 {
                let idle = cache
                    .iter()
                    .find(|(_, v)| Arc::strong_count(v) == 1)
                    .map(|(k, _)| k.clone());
                if let Some(idle) = idle {
                    cache.remove(&idle);
                } else {
                    return Err("VAAPI readiness checks are busy".into());
                }
            }
            cache.entry(key.clone()).or_default().clone()
        };
        Ok(cell
            .get_or_init(|| async {
                Readiness {
                    encoder: key.encoder,
                    codec: if key.encoder == "h264_vaapi" {
                        "H.264"
                    } else {
                        "HEVC / H.265"
                    },
                    status: check_profile(ffmpeg, profile).await,
                }
            })
            .await
            .clone())
    }
    pub(crate) async fn vaapi_report(&self, ffmpeg: &str) -> Vec<Readiness> {
        let mut result = vec![];
        for encoder in ["h264_vaapi", "hevc_vaapi"] {
            let profile = crate::transcoder::Profile::resolve(
                &serde_json::json!({"transcoder":{"encoder":encoder,"acodec":"copy"}}),
                false,
            )
            .unwrap();
            result.push(
                self.vaapi_check(ffmpeg, &profile)
                    .await
                    .unwrap_or(Readiness {
                        encoder,
                        codec: if encoder == "h264_vaapi" {
                            "H.264"
                        } else {
                            "HEVC / H.265"
                        },
                        status: "probe_failed",
                    }),
            );
        }
        result
    }
    pub(crate) async fn require_profile(
        &self,
        ffmpeg: &str,
        profile: &crate::transcoder::Profile,
    ) -> Result<(), String> {
        if profile.vaapi().is_some() {
            if let Some(error) = self.vaapi_check(ffmpeg, profile).await?.error() {
                return Err(error.into());
            }
        } else if let Some(encoder) = profile.gpu_encoder() {
            self.require(ffmpeg, encoder).await?;
        }
        Ok(())
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
    let profile = crate::transcoder::Profile::resolve(
        &serde_json::json!({"transcoder":{"encoder":encoder,"acodec":"copy"}}),
        false,
    )
    .expect("known GPU profile");
    check_profile(ffmpeg, &profile).await
}
async fn check_profile(ffmpeg: &str, profile: &crate::transcoder::Profile) -> &'static str {
    let mut cmd = Command::new(ffmpeg);
    cmd.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-threads",
        "2",
    ]);
    profile.prepare(&mut cmd);
    cmd.args(["-f", "lavfi", "-i", "color=size=320x180:rate=25"]);
    profile.apply(&mut cmd);
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
        "VAAPI readiness checks are busy"
        | "VAAPI H.264 encoder readiness check timed out"
        | "VAAPI HEVC encoder readiness check timed out"
        | "VAAPI H.264 encoder readiness check failed"
        | "VAAPI HEVC encoder readiness check failed"
        | "VAAPI H.264 encoder is unavailable on this host"
        | "VAAPI HEVC encoder is unavailable on this host"
        | "NVIDIA H.264 encoder readiness check timed out"
        | "NVIDIA HEVC encoder readiness check timed out"
        | "NVIDIA H.264 encoder readiness check failed"
        | "NVIDIA HEVC encoder readiness check failed"
        | "NVIDIA H.264 encoder is unavailable on this host"
        | "NVIDIA HEVC encoder is unavailable on this host" => Some(error),
        _ => None,
    }
}
