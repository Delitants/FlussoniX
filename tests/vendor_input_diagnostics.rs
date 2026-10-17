//! Failure provenance must identify the boundary without exposing privileged data.
use axum::{
    Router, body::Body, extract::OriginalUri, http::StatusCode, response::Response, routing::get,
};
use flussonix::{
    m4_ingest::flussonic::{InputFailure, MediaInput, Transport},
    m4s::Track,
    wire,
};
use serde_json::Value;
use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::AsyncWrite;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Value>>>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(serde_json::Map<String, Value>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().into(), Value::String(format!("{value:?}")));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0
                    .insert(field.name().into(), Value::String(value.into()));
            }
            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                self.0.insert(field.name().into(), Value::Bool(value));
            }
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.0.insert(field.name().into(), value.into());
            }
        }
        let mut fields = Fields(serde_json::Map::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(Value::Object(fields.0));
    }
}
struct Output(bool);
impl AsyncWrite for Output {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.0 {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        } else {
            Poll::Ready(Ok(bytes.len()))
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
async fn failure(
    transport: Transport,
    status: StatusCode,
    ack: bool,
    bytes: Vec<u8>,
    broken: bool,
) -> (InputFailure, Value, usize) {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!(
        "http://{}/private-prefix-sensitive",
        tcp.local_addr().unwrap()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let bytes = bytes.clone();
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    let control = transport == Transport::M4f
                        && uri.path().ends_with("/m4f")
                        && status != StatusCode::FORBIDDEN;
                    let mut reply = Response::new(Body::from(if control {
                        if status == StatusCode::NOT_FOUND {
                            b"1 2026/01/01/00/00/00-1000\n2 2026/01/01/00/00/01-1000\n".to_vec()
                        } else {
                            b"1 2026/01/01/00/00/00-1000\n".to_vec()
                        }
                    } else {
                        bytes
                    }));
                    *reply.status_mut() = if control { StatusCode::OK } else { status };
                    if control || ack {
                        reply
                            .headers_mut()
                            .insert("cluster-auth", "yes".parse().unwrap());
                    }
                    reply
                        .headers_mut()
                        .insert("x-private-debug", "header-sensitive".parse().unwrap());
                    reply
                }
            })),
        )
        .await
        .unwrap()
    });
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone().with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.target() == "flussonix::private_input"
        }),
    ));
    let input = MediaInput::new(&root, "name-sensitive", transport, "key-sensitive", None).unwrap();
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let mut output = Output(broken);
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        input
            .pull_ready_classified(&mut output, None, tx)
            .with_subscriber(subscriber),
    )
    .await;
    server.abort();
    let _ = server.await;
    let kind = result.unwrap().unwrap_err();
    let records = capture.0.lock().unwrap().clone();
    let log = serde_json::to_string(&records).unwrap();
    for forbidden in [
        &root,
        "private-prefix-sensitive",
        "name-sensitive",
        "key-sensitive",
        "header-sensitive",
        "body-sensitive",
        "cluster-auth",
        "x-playback-session-id",
    ] {
        assert!(
            !log.contains(forbidden),
            "private data escaped into failure telemetry"
        );
    }
    let events = records
        .iter()
        .filter(|v| v["message"] == "private media input failed")
        .collect::<Vec<_>>();
    if transport == Transport::M4f && status == StatusCode::NOT_FOUND && ack {
        let skips = records
            .iter()
            .filter(|v| v["message"] == "skipped unavailable startup segment")
            .collect::<Vec<_>>();
        assert_eq!(skips.len(), 1);
        assert_eq!(skips[0]["protocol"], "m4f");
        assert_eq!(skips[0]["last_http_status"], 404);
        assert_eq!(skips[0]["last_private_ack"], true);
        assert_eq!(skips[0]["metadata_sent"], false);
    }
    assert_eq!(
        events.len(),
        1,
        "one safe terminal failure event is required: {log}"
    );
    (kind, events[0].clone(), calls.load(Ordering::SeqCst))
}

#[tokio::test]
async fn private_control_denial_reports_response_boundary_without_secrets() {
    let (kind, fields, calls) = failure(
        Transport::M4f,
        StatusCode::FORBIDDEN,
        false,
        b"body-sensitive".to_vec(),
        false,
    )
    .await;
    assert_eq!(kind, InputFailure::Rejected);
    assert_eq!(fields["phase"], "control_response");
    assert_eq!(fields["last_http_status"], 403);
    assert_eq!(fields["last_private_ack"], false);
    assert_eq!(fields["metadata_sent"], false);
    assert_eq!(fields["protocol"], "m4f");
    assert_eq!(fields["failure"], "Rejected");
    assert_eq!(calls, 1);
}
#[tokio::test]
async fn second_missing_startup_segment_reports_404_without_body_exposure() {
    let (kind, fields, calls) = failure(
        Transport::M4f,
        StatusCode::NOT_FOUND,
        true,
        b"body-sensitive".to_vec(),
        false,
    )
    .await;
    assert_eq!(kind, InputFailure::Transient);
    assert_eq!(fields["phase"], "segment_response");
    assert_eq!(fields["last_http_status"], 404);
    assert_eq!(fields["last_private_ack"], true);
    assert_eq!(fields["metadata_sent"], false);
    assert_eq!(fields["failure"], "Transient");
    assert_eq!(calls, 3);
}
#[tokio::test]
async fn malformed_segment_reports_decode_boundary_without_exposing_payload() {
    let (kind, fields, calls) = failure(
        Transport::M4f,
        StatusCode::OK,
        true,
        b"body-sensitive".to_vec(),
        false,
    )
    .await;
    assert_eq!(kind, InputFailure::Invalid);
    assert_eq!(fields["phase"], "segment_decode");
    assert_eq!(fields["last_http_status"], 200);
    assert_eq!(fields["metadata_sent"], false);
    assert_eq!(fields["failure"], "Invalid");
    assert_eq!(calls, 2);
}
#[tokio::test]
async fn local_output_failure_reports_metadata_progress_without_misclassifying_origin() {
    let track = Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    };
    let (kind, fields, calls) = failure(
        Transport::M4s,
        StatusCode::OK,
        true,
        wire::encode_info(&[track]).unwrap(),
        true,
    )
    .await;
    assert_eq!(kind, InputFailure::Local);
    assert_eq!(fields["phase"], "output_write");
    assert_eq!(fields["last_http_status"], 200);
    assert_eq!(fields["metadata_sent"], true);
    assert_eq!(fields["protocol"], "m4s");
    assert_eq!(fields["failure"], "Local");
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn invalid_media_frame_reports_bridge_validation_before_any_frame_write() {
    let track = Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    };
    let mut bytes = wire::encode_info(std::slice::from_ref(&track)).unwrap();
    bytes.extend(
        wire::encode_frame(
            &track,
            &flussonix::m4f::Frame {
                track_id: 1,
                dts: 90_000,
                pts_offset: 0,
                key: true,
                body: vec![],
            },
        )
        .unwrap(),
    );
    let (kind, fields, calls) = failure(Transport::M4s, StatusCode::OK, true, bytes, false).await;
    assert_eq!(kind, InputFailure::Invalid);
    assert_eq!(fields["phase"], "bridge_frame");
    assert_eq!(fields["metadata_sent"], true);
    assert_eq!(calls, 1);
}
