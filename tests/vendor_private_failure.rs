use axum::{Router, body::Body, http::StatusCode, response::Response, routing::get};
use flussonix::{
    m4_ingest::flussonic::{InputFailure, MediaInput, Transport},
    m4s::Track,
    wire,
};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;

async fn classify(status: StatusCode, ack: bool, body: Vec<u8>, local: bool) -> InputFailure {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}", tcp.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(move || {
                let body = body.clone();
                async move {
                    let mut response = Response::new(Body::from(body));
                    *response.status_mut() = status;
                    if ack {
                        response
                            .headers_mut()
                            .insert("cluster-auth", "yes".parse().unwrap());
                    }
                    response
                }
            })),
        )
        .await
        .unwrap()
    });
    let input = MediaInput::new(&root, "owned", Transport::M4s, "owned-key", None).unwrap();
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let mut output = Output {
        local,
        bytes: vec![],
    };
    let outcome = input
        .pull_ready_classified(&mut output, None, tx)
        .await
        .unwrap_err();
    server.abort();
    let _ = server.await;
    outcome
}
struct Output {
    local: bool,
    bytes: Vec<u8>,
}
impl AsyncWrite for Output {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let s = self.get_mut();
        if s.local {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        s.bytes.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn private_status_ack_and_eof_have_distinct_retry_meanings() {
    for status in [StatusCode::SERVICE_UNAVAILABLE, StatusCode::BAD_GATEWAY] {
        assert_eq!(
            classify(status, false, vec![], false).await,
            InputFailure::Transient
        );
    }
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
        StatusCode::FOUND,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        assert_eq!(
            classify(status, false, vec![], false).await,
            InputFailure::Rejected
        );
    }
    assert_eq!(
        classify(StatusCode::OK, false, vec![], false).await,
        InputFailure::Rejected
    );
    assert_eq!(
        classify(StatusCode::OK, true, vec![], false).await,
        InputFailure::Transient
    );
}
#[tokio::test]
async fn invalid_remote_framing_does_not_look_like_local_output_failure() {
    assert_eq!(
        classify(StatusCode::OK, true, vec![0; 16], false).await,
        InputFailure::Invalid
    );
    let track = Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    };
    assert_eq!(
        classify(
            StatusCode::OK,
            true,
            wire::encode_info(&[track]).unwrap(),
            true
        )
        .await,
        InputFailure::Local
    );
}
