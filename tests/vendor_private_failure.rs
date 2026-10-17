use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderValue, StatusCode},
    response::Response,
    routing::get,
};
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

#[tokio::test]
async fn only_acknowledged_announced_m4f_segment_404_is_transient() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let yes = vec![HeaderValue::from_static("yes")];
    let mut cases = vec![(StatusCode::NOT_FOUND, yes.clone(), InputFailure::Transient)];
    for status in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        for markers in [vec![], yes.clone()] {
            cases.push((status, markers, InputFailure::Transient));
        }
    }
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::FOUND,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        cases.push((status, yes.clone(), InputFailure::Rejected));
    }
    for markers in [
        vec![],
        vec![HeaderValue::from_static("no")],
        vec![HeaderValue::from_static("Yes")],
        vec![HeaderValue::from_static("yes, yes")],
        vec![
            HeaderValue::from_static("yes"),
            HeaderValue::from_static("yes"),
        ],
        vec![
            HeaderValue::from_static("yes"),
            HeaderValue::from_static("no"),
        ],
        vec![HeaderValue::from_bytes(&[0x80]).unwrap()],
    ] {
        cases.push((StatusCode::NOT_FOUND, markers, InputFailure::Rejected));
    }
    for (status, markers, wanted) in cases {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", tcp.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let hits = calls.clone();
        let service = Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
            hits.fetch_add(1, Ordering::SeqCst);
            let markers = markers.clone();
            async move {
                let control = uri.path() == "/owned/m4f";
                if !control {
                    assert_eq!(uri.path(), "/owned/2026/10/17/00/00/00.m4f");
                }
                // The error body never ends: classification must not consume it.
                let body = if control {
                    Body::from("1 2026/10/17/00/00/00-2000\n")
                } else {
                    Body::from_stream(futures_util::stream::pending::<
                        Result<bytes::Bytes, std::io::Error>,
                    >())
                };
                let mut reply = Response::new(body);
                if control {
                    reply
                        .headers_mut()
                        .insert("cluster-auth", HeaderValue::from_static("yes"));
                } else {
                    *reply.status_mut() = status;
                    for marker in markers {
                        reply.headers_mut().append("cluster-auth", marker);
                    }
                }
                reply
            }
        }));
        let server = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let input = MediaInput::new(&root, "owned", Transport::M4f, "owned-key", None).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut output = Vec::new();
        let hub = wire::Hub::new();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            input.pull_ready_classified(&mut output, Some(&hub), tx),
        )
        .await;
        server.abort();
        let _ = server.await;
        assert_eq!(outcome.unwrap().unwrap_err(), wanted, "status {status}");
        assert!(rx.await.is_err() && output.is_empty() && !hub.has_info());
        assert!(hub.segment("2026/10/17/00/00/00.m4f").is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2, "no internal retry");
    }
}

#[tokio::test]
async fn acknowledged_control_404_remains_rejected() {
    for transport in [Transport::M4f, Transport::M4s] {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", tcp.local_addr().unwrap());
        let service = Router::new().fallback(get(|| async {
            let mut reply = Response::new(Body::empty());
            *reply.status_mut() = StatusCode::NOT_FOUND;
            reply
                .headers_mut()
                .insert("cluster-auth", HeaderValue::from_static("yes"));
            reply
        }));
        let server = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let input = MediaInput::new(&root, "owned", transport, "owned-key", None).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut output = Vec::new();
        let outcome = input.pull_ready_classified(&mut output, None, tx).await;
        server.abort();
        let _ = server.await;
        assert_eq!(outcome.unwrap_err(), InputFailure::Rejected);
        assert!(rx.await.is_err() && output.is_empty());
    }
}

#[tokio::test]
async fn native_m4f_segment_404_cannot_use_private_availability_exception() {
    for key in [None, Some("owned-native-key")] {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("m4f://{}/owned", tcp.local_addr().unwrap());
        let service = Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, headers: axum::http::HeaderMap| async move {
                assert!(!headers.contains_key("cluster-auth-key"));
                assert_eq!(
                    headers.get("x-flussonix-peer").map(|v| v.to_str().unwrap()),
                    key
                );
                let mut reply = if uri.path() == "/owned/m4f" {
                    Response::new(Body::from("1 2026/10/17/00/00/00-2000\n"))
                } else {
                    assert_eq!(uri.path(), "/owned/2026/10/17/00/00/00.m4f");
                    let mut reply = Response::new(Body::empty());
                    *reply.status_mut() = StatusCode::NOT_FOUND;
                    reply
                };
                reply
                    .headers_mut()
                    .insert("cluster-auth", HeaderValue::from_static("yes"));
                reply
            },
        ));
        let server = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut output = Vec::new();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            flussonix::m4_ingest::pull_ready(&url, key, &mut output, None, tx),
        )
        .await;
        server.abort();
        let _ = server.await;
        assert_eq!(result.unwrap().unwrap_err(), "M4F segment denied");
        assert!(output.is_empty() && rx.await.is_err());
    }
}
