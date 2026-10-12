//! Authentication acceptance is distinct from media availability or server identity.
use axum::{
    Router, body::Body, extract::OriginalUri, http::HeaderValue, response::Response, routing::get,
};
use bytes::Bytes;
use flussonix::{
    m4_ingest::{
        self,
        flussonic::{MediaInput, Transport},
    },
    m4f::{self, Frame},
    m4s::Track,
    wire::{self, Hub},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Server {
    root: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(router: Router) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}", tcp.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(tcp, router).await.unwrap() });
        Self { root, task }
    }
    async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}
fn payload() -> (Vec<Track>, Bytes, Bytes) {
    let tracks = vec![Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }];
    let frame = Frame {
        track_id: 1,
        dts: 90000,
        pts_offset: 0,
        key: true,
        body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
    };
    let mut bytes = wire::encode_info(&tracks).unwrap();
    bytes.extend(wire::encode_frame(&tracks[0], &frame).unwrap());
    let segment = m4f::pack(&tracks, &[frame], 180000).unwrap();
    (tracks, bytes.into(), segment.into())
}
fn response(bytes: impl Into<Body>, markers: &[HeaderValue]) -> Response {
    let mut reply = Response::new(bytes.into());
    // Deliberately plausible but unrelated fields must not replace acknowledgment.
    reply.headers_mut().insert(
        "x-sid",
        HeaderValue::from_static("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
    );
    reply
        .headers_mut()
        .insert("x-playback-id", HeaderValue::from_static("yes"));
    reply
        .headers_mut()
        .insert("x-server-id", HeaderValue::from_static("claimed-server"));
    for marker in markers {
        reply.headers_mut().append("Cluster-Auth", marker.clone());
    }
    reply
}
fn invalid_markers() -> Vec<Vec<HeaderValue>> {
    let one = |s: &'static str| vec![HeaderValue::from_static(s)];
    vec![
        vec![],
        one(""),
        one("no"),
        one("true"),
        one("Yes"),
        one("YES"),
        one("yes, yes"),
        vec![
            HeaderValue::from_static("yes"),
            HeaderValue::from_static("yes"),
        ],
        vec![
            HeaderValue::from_static("yes"),
            HeaderValue::from_static("no"),
        ],
        vec![HeaderValue::from_bytes(&[0x80]).unwrap()],
    ]
}
async fn pull(input: &MediaInput) -> (Result<(), String>, Option<Vec<Track>>, Vec<u8>, Hub) {
    let mut output = Vec::new();
    let hub = Hub::new();
    let (send, receive) = tokio::sync::oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        input.pull_ready(&mut output, Some(&hub), send),
    )
    .await
    .unwrap();
    (result, receive.await.ok(), output, hub)
}
#[tokio::test]
async fn private_control_requires_one_exact_ack_before_metadata_media_or_segment_fetch() {
    for transport in [Transport::M4s, Transport::M4f] {
        for markers in invalid_markers() {
            let count = Arc::new(AtomicUsize::new(0));
            let hits = count.clone();
            let (_, stream, segment) = payload();
            let server = Server::start(Router::new().fallback(get(
                move |OriginalUri(uri): OriginalUri| {
                    hits.fetch_add(1, Ordering::SeqCst);
                    let (markers, stream, segment) =
                        (markers.clone(), stream.clone(), segment.clone());
                    async move {
                        response(
                            if uri.path().ends_with("/m4s") {
                                stream
                            } else if uri.path().ends_with("/m4f") {
                                Bytes::from_static(b"1 2026/10/12/01/00/00-2000\n")
                            } else {
                                segment
                            },
                            &markers,
                        )
                    }
                },
            )))
            .await;
            let input =
                MediaInput::new(&server.root, "owned", transport, "owned-secret", None).unwrap();
            let (result, metadata, output, hub) = pull(&input).await;
            assert_eq!(result.unwrap_err(), "source rejected input");
            assert!(metadata.is_none());
            assert!(output.is_empty());
            assert!(!hub.has_info());
            assert_eq!(count.load(Ordering::SeqCst), 1);
            server.stop().await;
        }
    }
}
#[tokio::test]
async fn every_m4f_segment_requires_ack_separately_from_its_control() {
    for markers in invalid_markers() {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let (_, _, segment) = payload();
        let server = Server::start(Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri| {
                hits.fetch_add(1, Ordering::SeqCst);
                let (markers, segment) = (markers.clone(), segment.clone());
                async move {
                    if uri.path().ends_with("/m4f") {
                        response(
                            "1 2026/10/12/01/00/00-2000\n",
                            &[HeaderValue::from_static("yes")],
                        )
                    } else {
                        response(segment, &markers)
                    }
                }
            },
        )))
        .await;
        let input =
            MediaInput::new(&server.root, "owned", Transport::M4f, "owned-secret", None).unwrap();
        let (result, metadata, output, hub) = pull(&input).await;
        assert_eq!(result.unwrap_err(), "M4F segment denied");
        assert!(metadata.is_none());
        assert!(output.is_empty());
        assert!(!hub.has_info());
        assert!(hub.segment("2026/10/12/01/00/00.m4f").is_none());
        assert_eq!(count.load(Ordering::SeqCst), 2);
        server.stop().await;
    }
}
#[tokio::test]
async fn an_earlier_authenticated_segment_does_not_grant_later_segments() {
    let count = Arc::new(AtomicUsize::new(0));
    let hits = count.clone();
    let (tracks, _, segment) = payload();
    let original = segment.clone();
    let server = Server::start(Router::new().fallback(get(
        move |OriginalUri(uri): OriginalUri| {
            hits.fetch_add(1, Ordering::SeqCst);
            let segment = segment.clone();
            async move {
                if uri.path().ends_with("/m4f") {
                    response(
                        "1 2026/10/12/01/00/00-2000\n2 2026/10/12/01/00/02-2000\n",
                        &[HeaderValue::from_static("yes")],
                    )
                } else if uri.path().ends_with("00.m4f") {
                    response(segment, &[HeaderValue::from_static("yes")])
                } else {
                    response(segment, &[])
                }
            }
        },
    )))
    .await;
    let input =
        MediaInput::new(&server.root, "owned", Transport::M4f, "owned-secret", None).unwrap();
    let (result, metadata, output, hub) = pull(&input).await;
    assert_eq!(result.unwrap_err(), "M4F segment denied");
    assert_eq!(metadata.unwrap(), tracks);
    assert!(!output.is_empty());
    assert_eq!(hub.segment("2026/10/12/01/00/00.m4f").unwrap(), original);
    assert!(hub.segment("2026/10/12/01/00/02.m4f").is_none());
    assert_eq!(count.load(Ordering::SeqCst), 3);
    server.stop().await;
}
#[tokio::test]
async fn acknowledged_m4s_and_m4f_publish_without_inferring_server_identity() {
    for transport in [Transport::M4s, Transport::M4f] {
        let (tracks, stream, segment) = payload();
        let server = Server::start(Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri| {
                let (stream, segment) = (stream.clone(), segment.clone());
                async move {
                    response(
                        if uri.path().ends_with("/m4s") {
                            stream
                        } else if uri.path().ends_with("/m4f") {
                            Bytes::from_static(b"1 2026/10/12/01/00/00-2000\n")
                        } else {
                            segment
                        },
                        &[HeaderValue::from_static("yes")],
                    )
                }
            },
        )))
        .await;
        let input =
            MediaInput::new(&server.root, "owned", transport, "owned-secret", None).unwrap();
        let (result, metadata, output, hub) = pull(&input).await;
        assert_eq!(result.unwrap_err(), "source closed");
        assert_eq!(metadata.unwrap(), tracks);
        assert!(!output.is_empty());
        assert!(hub.has_info());
        server.stop().await;
    }
}
#[tokio::test]
async fn public_and_native_peer_inputs_keep_their_existing_response_contract() {
    for key in [None, Some("owned-native-key")] {
        let (tracks, stream, _) = payload();
        let server = Server::start(Router::new().fallback(get(move || {
            let stream = stream.clone();
            async move { response(stream, &[]) }
        })))
        .await;
        let url = server.root.replacen("http://", "m4s://", 1) + "/owned";
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut output = Vec::new();
        let hub = Hub::new();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            m4_ingest::pull_ready(&url, key, &mut output, Some(&hub), send),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap_err(), "source closed");
        assert_eq!(receive.await.unwrap(), tracks);
        assert!(!output.is_empty());
        assert!(hub.has_info());
        server.stop().await;
    }
}

#[tokio::test]
async fn reopening_an_input_does_not_reuse_an_earlier_acknowledgment() {
    for transport in [Transport::M4s, Transport::M4f] {
        let controls = Arc::new(AtomicUsize::new(0));
        let hits = controls.clone();
        let (_, stream, segment) = payload();
        let server = Server::start(Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri| {
                let (stream, segment) = (stream.clone(), segment.clone());
                let control = uri.path().ends_with("/m4s") || uri.path().ends_with("/m4f");
                let acknowledged = !control || hits.fetch_add(1, Ordering::SeqCst) == 0;
                async move {
                    let body = if uri.path().ends_with("/m4s") {
                        stream
                    } else if uri.path().ends_with("/m4f") {
                        Bytes::from_static(b"1 2026/10/12/01/00/00-2000\n")
                    } else {
                        segment
                    };
                    response(
                        body,
                        &if acknowledged {
                            vec![HeaderValue::from_static("yes")]
                        } else {
                            vec![]
                        },
                    )
                }
            },
        )))
        .await;
        let input =
            MediaInput::new(&server.root, "owned", transport, "owned-secret", None).unwrap();
        assert!(pull(&input).await.1.is_some());
        let (result, metadata, output, hub) = pull(&input).await;
        assert_eq!(result.unwrap_err(), "source rejected input");
        assert!(metadata.is_none());
        assert!(output.is_empty());
        assert!(!hub.has_info());
        assert_eq!(controls.load(Ordering::SeqCst), 2);
        server.stop().await;
    }
}
