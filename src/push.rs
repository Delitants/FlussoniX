//! Local mixed push destinations retain configuration order and worker ownership.
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
pub(crate) enum Destination {
    Srt(crate::srt_push::Destination),
    Rtsp(crate::rtsp_push::Destination),
}
pub(crate) fn configuration(cfg: &Value) -> Result<Vec<Destination>, String> {
    let Some(pushes) = cfg.get("pushes") else {
        return Ok(vec![]);
    };
    let pushes = pushes.as_array().ok_or("pushes must be an array")?;
    if pushes.len() > 4 {
        return Err("At most four push destinations are supported".into());
    }
    pushes
        .iter()
        .map(|p| {
            if p["url"]
                .as_str()
                .is_some_and(|u| u.starts_with("rtsp://") || u.starts_with("rtsps://"))
            {
                crate::rtsp_push::Destination::parse(p).map(Destination::Rtsp)
            } else {
                let mut destinations =
                    crate::srt_push::configuration(&serde_json::json!({"pushes":[p]}))?;
                Ok(Destination::Srt(destinations.remove(0)))
            }
        })
        .collect()
}
pub(crate) fn enabled(cfg: &Value) -> bool {
    crate::srt_push::enabled(cfg)
}
pub(crate) enum State {
    Srt(Arc<crate::srt_push::State>),
    Rtsp(Arc<crate::rtsp_push::State>),
}
impl State {
    pub fn new(destination: Destination, index: usize) -> Arc<Self> {
        Arc::new(match destination {
            Destination::Srt(d) => Self::Srt(crate::srt_push::State::new(d, index)),
            Destination::Rtsp(d) => Self::Rtsp(crate::rtsp_push::State::new(d, index)),
        })
    }
    pub fn stats(&self) -> Value {
        match self {
            Self::Srt(s) => s.stats(),
            Self::Rtsp(s) => s.stats(),
        }
    }
    pub async fn run(
        self: Arc<Self>,
        ffmpeg: String,
        worker: Arc<crate::media::Worker>,
        cancel: CancellationToken,
    ) {
        match self.as_ref() {
            Self::Srt(s) => s.clone().run(ffmpeg, worker.subscribe(), cancel).await,
            Self::Rtsp(s) => {
                s.clone()
                    .run(worker.clone(), worker.subscribe(), cancel)
                    .await
            }
        }
    }
}
