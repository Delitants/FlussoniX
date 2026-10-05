//! A single validated profile controls every output of a shared worker.
use serde_json::Value;
use tokio::process::Command;

pub(crate) const MP2_RATES: &[u64] = &[
    32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
];
pub(crate) const MP3_RATES: &[u64] = &[
    32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
];
pub(crate) struct Profile {
    video: &'static str,
    audio: &'static str,
    vb: u64,
    ab: u64,
}
impl Profile {
    pub(crate) fn resolve(cfg: &Value, synthetic: bool) -> Result<Self, String> {
        let t = cfg.get("transcoder");
        if let Some(t) = t {
            let object = t.as_object().ok_or("transcoder must be an object")?;
            for key in object.keys() {
                if !["encoder", "vb", "acodec", "ab"].contains(&key.as_str()) {
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
            _ => return Err("supported encoders: copy, libx264, libx265, h264_nvenc".into()),
        };
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
        })
    }
    pub(crate) fn full_copy(&self) -> bool {
        self.video == "copy" && self.audio == "copy"
    }
    pub(crate) fn audio_copy(&self) -> bool {
        self.audio == "copy"
    }
    pub(crate) fn apply(&self, cmd: &mut Command) {
        cmd.args(["-c:v", self.video]);
        if self.video != "copy" {
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
        }
        cmd.args(["-c:a", self.audio]);
        if self.audio != "copy" {
            cmd.args(["-b:a", &format!("{}k", self.ab), "-ar", "48000", "-ac", "2"]);
        }
    }
}
