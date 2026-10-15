//! Preserve HTTP framing while retaining media admission through body lifetime.
use crate::idle_memory::Activity;
use axum::{body::Body, response::Response};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

struct Guarded {
    body: Body,
    // Drop after the original body and its worker/viewer guards.
    _activity: Activity,
}
impl HttpBody for Guarded {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
pub(super) fn retain(response: Response, activity: Activity) -> Response {
    response.map(|body| {
        Body::new(Guarded {
            body,
            _activity: activity,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    #[tokio::test]
    async fn body_preserves_length_data_and_retains_activity_until_drop() {
        let controller = crate::idle_memory::test_support::controller();
        let before = controller.snapshot()["active_operations"].as_u64().unwrap();
        let activity = controller.activity().await;
        let response = retain(Response::new(Body::from("owned")), activity);
        assert_eq!(response.body().size_hint().exact(), Some(5));
        assert_eq!(controller.snapshot()["active_operations"], before + 1);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "owned"
        );
        assert_eq!(controller.snapshot()["active_operations"], before);
    }
    #[tokio::test]
    async fn body_preserves_trailers_and_cancellation_releases_activity() {
        let controller = crate::idle_memory::test_support::controller();
        let before = controller.snapshot()["active_operations"].as_u64().unwrap();
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("owned-result", "complete".parse().unwrap());
        let frames = futures_util::stream::iter([
            Ok::<_, std::io::Error>(Frame::data(bytes::Bytes::from_static(b"media"))),
            Ok(Frame::trailers(trailers.clone())),
        ]);
        let response = retain(
            Response::new(Body::new(http_body_util::StreamBody::new(frames))),
            controller.activity().await,
        );
        let collected = response.into_body().collect().await.unwrap();
        assert_eq!(collected.trailers(), Some(&trailers));
        assert_eq!(collected.to_bytes(), "media");
        assert_eq!(controller.snapshot()["active_operations"], before);
        let held = retain(
            Response::new(Body::from("held")),
            controller.activity().await,
        );
        assert_eq!(controller.snapshot()["active_operations"], before + 1);
        drop(held);
        assert_eq!(controller.snapshot()["active_operations"], before);
    }
}
