//! A single validated profile controls every output of a shared worker.
use serde_json::Value;
use tokio::process::Command;

pub(crate) const MP2_RATES: &[u64] = &[
    32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
];
pub(crate) const MP3_RATES: &[u64] = &[
    32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
];
#[derive(Clone)]
pub(crate) struct Profile {
    video: &'static str,
    audio: &'static str,
    vb: u64,
    ab: u64,
    vaapi: Option<Vaapi>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Vaapi {
    pub encoder: &'static str,
    pub device: String,
    pub low_power: bool,
    pub cbr: bool,
    pub rate: u64,
}
impl Profile {
    pub(crate) fn resolve(cfg: &Value, synthetic: bool) -> Result<Self, String> {
        Self::resolve_inner(cfg, synthetic, false)
    }
    pub(crate) fn validate_partial(cfg: &Value) -> Result<Self, String> {
        Self::resolve_inner(
            cfg,
            false,
            cfg["template"].is_string() && cfg["transcoder"].get("encoder").is_none(),
        )
    }
    fn resolve_inner(cfg: &Value, synthetic: bool, partial: bool) -> Result<Self, String> {
        let t = cfg.get("transcoder");
        if let Some(t) = t {
            let object = t.as_object().ok_or("transcoder must be an object")?;
            for key in object.keys() {
                if ![
                    "encoder",
                    "vb",
                    "acodec",
                    "ab",
                    "vaapi_device",
                    "low_power",
                    "vaapi_rc",
                    "qp",
                ]
                .contains(&key.as_str())
                {
                    return Err(format!("transcoder option {key} is not implemented"));
                }
            }
        }
        let t = t.unwrap_or(&Value::Null);
        let legacy_video =
            t.is_object() && (t.get("vb").is_some() || t.as_object().unwrap().is_empty());
        let video = match t.get("encoder").map(|v| v.as_str()) {
            None => {
                if legacy_video {
                    "libx264"
                } else {
                    "copy"
                }
            }
            Some(Some("copy")) => "copy",
            Some(Some("libx264")) => "libx264",
            Some(Some("libx265")) => "libx265",
            Some(Some("h264_nvenc")) => "h264_nvenc",
            Some(Some("hevc_nvenc")) => "hevc_nvenc",
            Some(Some("h264_vaapi")) => "h264_vaapi",
            Some(Some("hevc_vaapi")) => "hevc_vaapi",
            _ => {
                return Err(
                    "supported encoders: copy, libx264, libx265, h264_nvenc, hevc_nvenc, h264_vaapi, hevc_vaapi".into(),
                );
            }
        };
        let hardware = matches!(video, "h264_vaapi" | "hevc_vaapi");
        if !hardware
            && !partial
            && ["vaapi_device", "low_power", "vaapi_rc", "qp"]
                .iter()
                .any(|k| t.get(*k).is_some())
        {
            return Err("VAAPI options require a VAAPI encoder".into());
        }
        let device = match t.get("vaapi_device") {
            None => "/dev/dri/renderD128",
            Some(v) => v
                .as_str()
                .ok_or("vaapi_device must be a render device path")?,
        };
        let digits = device
            .strip_prefix("/dev/dri/renderD")
            .ok_or("vaapi_device must be /dev/dri/renderD followed by 1..3 digits")?;
        if digits.is_empty() || digits.len() > 3 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err("vaapi_device must be /dev/dri/renderD followed by 1..3 digits".into());
        }
        let low_power = match t.get("low_power") {
            None => false,
            Some(v) => v.as_bool().ok_or("low_power must be boolean")?,
        };
        let cbr = match t.get("vaapi_rc").map(Value::as_str) {
            None | Some(Some("cqp")) => false,
            Some(Some("cbr")) => true,
            _ => return Err("vaapi_rc must be cqp or cbr".into()),
        };
        let qp = match t.get("qp") {
            None => 24,
            Some(v) => v.as_u64().filter(|n| *n <= 51).ok_or("qp must be 0..51")?,
        };
        if (hardware || partial)
            && ((!cbr && t.get("vb").is_some() && (!partial || t.get("vaapi_rc").is_some()))
                || (cbr && t.get("qp").is_some()))
        {
            return Err("VAAPI CQP forbids vb; CBR forbids qp".into());
        }
        let audio = match t.get("acodec").map(|v| v.as_str()) {
            None => {
                if video != "copy" || t.get("ab").is_some() {
                    "aac"
                } else {
                    "copy"
                }
            }
            Some(Some("copy")) => "copy",
            Some(Some("aac")) => "aac",
            Some(Some("mp2a")) => "mp2",
            Some(Some("mp3")) => "libmp3lame",
            _ => return Err("supported audio codecs: copy, aac, mp2a, mp3".into()),
        };
        let vb = if let Some(v) = t.get("vb") {
            v.as_u64()
                .filter(|n| (100..=50000).contains(n))
                .ok_or("vb must be 100..50000 kbps")?
        } else {
            900
        };
        let ab = if let Some(v) = t.get("ab") {
            v.as_u64()
                .filter(|n| (32..=512).contains(n))
                .ok_or("ab must be an integer from 32..512 kbps")?
        } else {
            match audio {
                "mp2" => 192,
                "libmp3lame" => 128,
                _ => 96,
            }
        };
        if audio == "mp2" && !MP2_RATES.contains(&ab) {
            return Err("Layer II audio bitrate must be 32,48,56,64,80,96,112,128,160,192,224,256,320 or 384 kbps".into());
        }
        if audio == "libmp3lame" && !MP3_RATES.contains(&ab) {
            return Err("MP3 audio bitrate must be 32,40,48,56,64,80,96,112,128,160,192,224,256 or 320 kbps".into());
        }
        Ok(Self {
            video: if synthetic && video == "copy" {
                "libx264"
            } else {
                video
            },
            audio: if synthetic && audio == "copy" {
                "aac"
            } else {
                audio
            },
            vb,
            ab,
            vaapi: hardware.then(|| Vaapi {
                encoder: video,
                device: device.into(),
                low_power,
                cbr,
                rate: if cbr { vb } else { qp },
            }),
        })
    }
    pub(crate) fn full_copy(&self) -> bool {
        self.video == "copy" && self.audio == "copy"
    }
    pub(crate) fn audio_copy(&self) -> bool {
        self.audio == "copy"
    }
    pub(crate) fn gpu_encoder(&self) -> Option<&'static str> {
        match self.video {
            "h264_nvenc" | "hevc_nvenc" | "h264_vaapi" | "hevc_vaapi" => Some(self.video),
            _ => None,
        }
    }
    pub(crate) fn vaapi(&self) -> Option<&Vaapi> {
        self.vaapi.as_ref()
    }
    pub(crate) fn prepare(&self, cmd: &mut Command) {
        if let Some(v) = &self.vaapi {
            cmd.args(["-vaapi_device", &v.device]);
        }
    }
    pub(crate) fn apply(&self, cmd: &mut Command) {
        cmd.args(["-c:v", self.video]);
        if let Some(v) = &self.vaapi {
            cmd.args([
                "-vf",
                "format=nv12,hwupload",
                "-g",
                "50",
                "-bf",
                "0",
                "-low_power",
                if v.low_power { "1" } else { "0" },
                "-rc_mode",
                if v.cbr { "CBR" } else { "CQP" },
            ]);
            if v.cbr {
                cmd.args(["-b:v", &format!("{}k", v.rate)]);
            } else {
                cmd.args(["-qp", &v.rate.to_string()]);
            }
        } else if self.video != "copy" {
            cmd.args([
                "-b:v",
                &format!("{}k", self.vb),
                "-g",
                "50",
                "-pix_fmt",
                "yuv420p",
            ]);
            if self.video == "libx264" || self.video == "libx265" {
                cmd.args(["-preset", "veryfast", "-tune", "zerolatency"]);
            }
            if self.video == "libx265" {
                cmd.args(["-x265-params", "pools=2:frame-threads=2:log-level=error"]);
            }
            if self.gpu_encoder().is_some() {
                cmd.args(["-bf", "0"]);
            }
        }
        cmd.args(["-c:a", self.audio]);
        if self.audio != "copy" {
            cmd.args(["-b:a", &format!("{}k", self.ab), "-ar", "48000", "-ac", "2"]);
        }
    }
}
