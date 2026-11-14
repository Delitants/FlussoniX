//! Owned HTTP/TLS fixtures for explicit vendor media input; no vendor runtime.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use bytes::Bytes;
use flussonix::{
    m4_ingest::flussonic::{MediaInput, Transport},
    m4f::{self, Frame},
    m4s::Track,
    wire::{self, Hub},
};
use futures_util::StreamExt;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[path = "support/tls.rs"]
mod tls_fixture;

struct Server {
    url: String,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Server {
    async fn start(router: Router) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", tcp.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(tcp, vendor_fixture::acknowledge_media(router))
                .await
                .unwrap()
        });
        Self {
            url,
            task: Some(task),
        }
    }
    async fn stop(mut self) {
        let task = self.task.take().unwrap();
        task.abort();
        let _ = task.await;
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn independent_sha1(message: &str) -> String {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let mut child = Command::new("openssl")
        .args(["dgst", "-sha1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(message.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .last()
        .unwrap()
        .to_owned()
}
fn credential_time(headers: &HeaderMap, name: &str, start: u64, end: u64) -> u64 {
    for forbidden in ["authorization", "x-flussonix-peer"] {
        assert!(!headers.contains_key(forbidden));
    }
    let time = headers["cluster-auth-time"].to_str().unwrap();
    let timestamp = time.parse::<u64>().unwrap();
    assert!((start..=end).contains(&timestamp));
    assert_eq!(
        headers["cluster-auth-key"],
        independent_sha1(&format!("{time}:synthetic:ключ:{name}"))
    );
    assert!(
        !headers["cluster-auth-key"]
            .to_str()
            .unwrap()
            .contains("synthetic")
    );
    timestamp
}
fn fixture() -> (Vec<Track>, Bytes, Bytes) {
    let tracks = vec![
        Track {
            id: 1,
            codec: "hevc".into(),
            config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
        Track {
            id: 3,
            codec: "mp3".into(),
            config: vec![],
        },
    ];
    let frames = vec![
        Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 7200,
            key: true,
            body: include_bytes!("fixtures/codecs/hevc-00.bin").to_vec(),
        },
        Frame {
            track_id: 2,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        },
        Frame {
            track_id: 3,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        },
    ];
    let mut stream = wire::encode_info(&tracks).unwrap();
    for f in &frames {
        stream.extend(wire::encode_frame(&tracks[(f.track_id - 1) as usize], f).unwrap());
    }
    let segment = m4f::pack(&tracks, &frames, 180000).unwrap();
    (tracks, Bytes::from(stream), Bytes::from(segment))
}
async fn pull(input: &MediaInput) -> (Result<(), String>, Option<Vec<Track>>, Vec<u8>, Hub) {
    let hub = Hub::new();
    let mut output = Vec::new();
    let (send, receive) = tokio::sync::oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(4),
        input.pull_ready(&mut output, Some(&hub), send),
    )
    .await
    .unwrap();
    (result, receive.await.ok(), output, hub)
}

// Break caught: inventory/raw-peer auth on media, wrong decoded name binding,
// URL normalization or encoding changes, or authentication that bypasses ingest.
#[tokio::test]
async fn private_media_binds_canonical_names_and_preserves_codec_samples() {
    for transport in [Transport::M4s, Transport::M4f] {
        for (name, path) in [
            (
                "region/канал",
                "/gateway/region/%D0%BA%D0%B0%D0%BD%D0%B0%D0%BB",
            ),
            ("space name", "/gateway/space%20name"),
            ("literal%2Fname", "/gateway/literal%252Fname"),
        ] {
            let (tracks, stream, segment) = fixture();
            let wanted_segment = segment.clone();
            let wanted_stream = stream.clone();
            let captures = Arc::new(Mutex::new(Vec::new()));
            let got = captures.clone();
            let server = Server::start(Router::new().fallback(get(
                move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                    got.lock().unwrap().push((uri.to_string(), headers));
                    let stream = stream.clone();
                    let segment = segment.clone();
                    async move {
                        Body::from(if uri.path().ends_with("/m4s") {
                            stream
                        } else if uri.path().ends_with("/m4f") {
                            Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
                        } else {
                            segment
                        })
                    }
                },
            )))
            .await;
            let input = MediaInput::new(
                &format!("{}/gateway/", server.url),
                name,
                transport,
                "synthetic:ключ",
                None,
            )
            .unwrap();
            let start = now();
            let (result, metadata, output, hub) = pull(&input).await;
            let end = now();
            server.stop().await;
            assert_eq!(result.unwrap_err(), "source closed");
            assert_eq!(metadata.unwrap(), tracks);
            assert!(output.len() > 3 * 188);
            assert!(output.chunks_exact(188).all(|p| p[0] == 0x47));
            let captures = captures.lock().unwrap();
            let protocol = if transport == Transport::M4s {
                "m4s"
            } else {
                "m4f"
            };
            assert_eq!(captures[0].0, format!("{path}/{protocol}"));
            assert_eq!(captures[0].1["x-supported"], "prepush,drop_status");
            for (_, headers) in captures.iter() {
                credential_time(headers, name, start, end);
            }
            if transport == Transport::M4s {
                assert_eq!(captures.len(), 1);
                assert!(!captures[0].1.contains_key("x-playback-session-id"));
                assert_eq!(hub.m4s_subscribe().0.concat(), wanted_stream.as_ref());
            } else {
                assert_eq!(captures.len(), 2);
                assert_eq!(captures[1].0, format!("{path}/2026/10/03/01/00/00.m4f"));
                assert_eq!(
                    hub.segment("2026/10/03/01/00/00.m4f").unwrap(),
                    wanted_segment
                );
            }
        }
    }
}

// Break caught: computing media credentials once at construction, once per
// control connection, or signing the segment path rather than the stream name.
#[tokio::test]
async fn m4f_refreshes_credentials_for_each_segment_request() {
    let captures = Arc::new(Mutex::new(Vec::new()));
    let got = captures.clone();
    let (_, _, segment) = fixture();
    let server = Server::start(Router::new().fallback(get(
        move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
            got.lock().unwrap().push((uri.to_string(), headers));
            let segment = segment.clone();
            async move {
                if uri.path().ends_with("/m4f") {
                    Body::from_stream(
                        futures_util::stream::once(async {
                            Ok::<_, std::io::Error>(Bytes::from_static(
                                b"1 2026/10/03/01/00/00-2000\n",
                            ))
                        })
                        .chain(futures_util::stream::once(async {
                            tokio::time::sleep(Duration::from_millis(1100)).await;
                            Ok::<_, std::io::Error>(Bytes::from_static(
                                b"2 2026/10/03/01/00/02-2000\n",
                            ))
                        })),
                    )
                } else {
                    Body::from(segment)
                }
            }
        },
    )))
    .await;
    let input = MediaInput::new(
        &server.url,
        "region/news",
        Transport::M4f,
        "synthetic:ключ",
        None,
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let start = now();
    let (result, metadata, _, hub) = pull(&input).await;
    let end = now();
    server.stop().await;
    assert_eq!(result.unwrap_err(), "source closed");
    assert!(metadata.is_some());
    assert!(hub.segment("2026/10/03/01/00/02.m4f").is_some());
    let captures = captures.lock().unwrap();
    assert_eq!(captures.len(), 3);
    let times: Vec<_> = captures
        .iter()
        .map(|(_, h)| credential_time(h, "region/news", start, end))
        .collect();
    assert!(times[2] > times[1]);
}

// Break caught: a client reuse/reconnect replays its previous connection's hash.
#[tokio::test]
async fn reopening_an_input_refreshes_media_credentials() {
    for transport in [Transport::M4s, Transport::M4f] {
        let captures = Arc::new(Mutex::new(Vec::new()));
        let got = captures.clone();
        let (_, stream, segment) = fixture();
        let server = Server::start(Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                got.lock().unwrap().push((uri.to_string(), headers));
                let stream = stream.clone();
                let segment = segment.clone();
                async move {
                    Body::from(if uri.path().ends_with("/m4s") {
                        stream
                    } else if uri.path().ends_with("/m4f") {
                        Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
                    } else {
                        segment
                    })
                }
            },
        )))
        .await;
        let input =
            MediaInput::new(&server.url, "owned", transport, "synthetic:ключ", None).unwrap();
        let start = now();
        assert!(pull(&input).await.1.is_some());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(pull(&input).await.1.is_some());
        let end = now();
        server.stop().await;
        let captures = captures.lock().unwrap();
        let stride = if transport == Transport::M4s { 1 } else { 2 };
        assert_eq!(captures.len(), 2 * stride);
        let first = credential_time(&captures[0].1, "owned", start, end);
        let second = credential_time(&captures[stride].1, "owned", start, end);
        assert!(second > first);
    }
}

// Break caught: redirecting a privileged media request to another stream,
// origin, or plaintext target; following segments differently from control.
#[tokio::test]
async fn media_control_and_segments_never_follow_redirects() {
    for transport in [Transport::M4s, Transport::M4f] {
        for segment_redirect in [false, true] {
            if segment_redirect && transport == Transport::M4s {
                continue;
            }
            for same_origin in [false, true] {
                let target_hits = Arc::new(AtomicUsize::new(0));
                let hits = target_hits.clone();
                let target = Server::start(Router::new().fallback(get(move || {
                    hits.fetch_add(1, Ordering::SeqCst);
                    async { StatusCode::OK }
                })))
                .await;
                let captures = Arc::new(Mutex::new(Vec::new()));
                let got = captures.clone();
                let location = if same_origin {
                    "/elsewhere".into()
                } else {
                    format!("{}/elsewhere", target.url)
                };
                let server = Server::start(Router::new().fallback(get(
                    move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                        got.lock().unwrap().push((uri.to_string(), headers));
                        let location = location.clone();
                        async move {
                            if segment_redirect && uri.path().ends_with("/m4f") {
                                Response::new(Body::from("1 2026/10/03/01/00/00-2000\n"))
                            } else {
                                Response::builder()
                                    .status(307)
                                    .header("location", location)
                                    .body(Body::empty())
                                    .unwrap()
                            }
                        }
                    },
                )))
                .await;
                let input =
                    MediaInput::new(&server.url, "owned", transport, "synthetic:ключ", None)
                        .unwrap();
                let (result, metadata, output, hub) = pull(&input).await;
                server.stop().await;
                target.stop().await;
                assert!(result.is_err());
                assert!(metadata.is_none());
                assert!(output.is_empty());
                assert!(!hub.has_info());
                assert_eq!(
                    captures.lock().unwrap().len(),
                    if segment_redirect { 2 } else { 1 }
                );
                assert_eq!(target_hits.load(Ordering::SeqCst), 0);
            }
        }
    }
}

// Break caught: retrying failures with raw/native/Basic credentials or accepting
// partial/asynchronous media responses and publishing their plausible bodies.
#[tokio::test]
async fn errors_and_non_200_responses_never_publish_or_fallback() {
    for transport in [Transport::M4s, Transport::M4f] {
        for segment_failure in [false, true] {
            if segment_failure && transport == Transport::M4s {
                continue;
            }
            for status in [202, 206, 401, 403, 404, 500] {
                let count = Arc::new(AtomicUsize::new(0));
                let hits = count.clone();
                let (_, stream, segment) = fixture();
                let server = Server::start(Router::new().fallback(get(
                    move |OriginalUri(uri): OriginalUri| {
                        hits.fetch_add(1, Ordering::SeqCst);
                        let stream = stream.clone();
                        let segment = segment.clone();
                        async move {
                            if segment_failure && uri.path().ends_with("/m4f") {
                                Response::new(Body::from("1 2026/10/03/01/00/00-2000\n"))
                            } else {
                                Response::builder()
                                    .status(status)
                                    .header("cluster-auth", "yes")
                                    .body(Body::from(if transport == Transport::M4s {
                                        stream
                                    } else {
                                        segment
                                    }))
                                    .unwrap()
                            }
                        }
                    },
                )))
                .await;
                let input =
                    MediaInput::new(&server.url, "owned", transport, "synthetic:ключ", None)
                        .unwrap();
                let (result, metadata, output, hub) = pull(&input).await;
                server.stop().await;
                let error = result.unwrap_err();
                assert!(!error.contains("synthetic"));
                assert!(!error.contains("127.0.0.1"));
                assert!(metadata.is_none());
                assert!(output.is_empty());
                assert!(!hub.has_info());
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    if segment_failure { 2 } else { 1 }
                );
            }
        }
    }
}

// Break caught: accepting a rejected digest, trying another credential scheme,
// or a successful authentication path that never consumes the media.
#[tokio::test]
async fn independent_receiver_denies_wrong_key_and_accepts_stream_scoped_hash() {
    for transport in [Transport::M4s, Transport::M4f] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let (_, stream, segment) = fixture();
        let server = Server::start(Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                hits.fetch_add(1, Ordering::SeqCst);
                let stream = stream.clone();
                let segment = segment.clone();
                async move {
                    let valid = headers
                        .get("cluster-auth-time")
                        .and_then(|h| h.to_str().ok())
                        .is_some_and(|seconds| {
                            seconds.parse::<u64>().is_ok_and(|t| t.abs_diff(now()) < 5)
                                && headers.get("cluster-auth-key").is_some_and(|digest| {
                                    digest.to_str().ok()
                                        == Some(
                                            independent_sha1(&format!(
                                                "{seconds}:synthetic:ключ:owned"
                                            ))
                                            .as_str(),
                                        )
                                })
                        });
                    if !valid {
                        Response::builder()
                            .status(403)
                            .body(Body::from("private upstream diagnostic must not escape"))
                            .unwrap()
                    } else {
                        Response::new(Body::from(if uri.path().ends_with("/m4s") {
                            stream
                        } else if uri.path().ends_with("/m4f") {
                            Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
                        } else {
                            segment
                        }))
                    }
                }
            },
        )))
        .await;
        let wrong = MediaInput::new(&server.url, "owned", transport, "wrong-key", None).unwrap();
        let (result, metadata, output, hub) = pull(&wrong).await;
        let error = result.unwrap_err();
        assert!(!error.contains("diagnostic"));
        assert!(!error.contains("wrong-key"));
        assert!(metadata.is_none());
        assert!(output.is_empty());
        assert!(!hub.has_info());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let correct =
            MediaInput::new(&server.url, "owned", transport, "synthetic:ключ", None).unwrap();
        assert!(pull(&correct).await.1.is_some());
        server.stop().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            if transport == Transport::M4s { 2 } else { 3 }
        );
    }
}

struct PendingBody {
    first: Option<Bytes>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
impl futures_util::Stream for PendingBody {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.first.take() {
            Some(bytes) => std::task::Poll::Ready(Some(Ok(bytes))),
            None => std::task::Poll::Pending,
        }
    }
}
impl Drop for PendingBody {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
// Break caught: detached request tasks keep privileged control/segment sockets
// alive after the caller cancels the input future.
#[tokio::test]
async fn cancelling_input_closes_owned_control_and_segment_bodies() {
    for (transport, segment_pending) in [
        (Transport::M4s, false),
        (Transport::M4f, false),
        (Transport::M4f, true),
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let closed = dropped.clone();
        let server = Server::start(Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri| {
                hits.fetch_add(1, Ordering::SeqCst);
                let closed = closed.clone();
                async move {
                    if segment_pending && uri.path().ends_with("/m4f") {
                        Body::from("1 2026/10/03/01/00/00-2000\n")
                    } else {
                        Body::from_stream(PendingBody {
                            first: Some(if transport == Transport::M4s {
                                Bytes::from(wire::encode_info(&fixture().0).unwrap())
                            } else {
                                Bytes::from_static(b"pending")
                            }),
                            dropped: closed,
                        })
                    }
                }
            },
        )))
        .await;
        let input =
            MediaInput::new(&server.url, "owned", transport, "synthetic:ключ", None).unwrap();
        let (send, _receive) = tokio::sync::oneshot::channel();
        let task =
            tokio::spawn(async move { input.pull_ready(&mut tokio::io::sink(), None, send).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while count.load(Ordering::SeqCst) < if segment_pending { 2 } else { 1 } {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        task.abort();
        let _ = task.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        server.stop().await;
    }
}

// Break caught: turning untrusted signal text into a fetch URL/path before the
// canonical timestamp parser validates it.
#[tokio::test]
async fn malformed_signals_never_create_segment_requests() {
    for signal in [
        "1 ../escape-2000\n",
        "1 https://other.invalid/2000-2000\n",
        "1 2026/10/03/01/00/00/../../escape-2000\n",
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let hits = count.clone();
        let server = Server::start(Router::new().fallback(get(move || {
            hits.fetch_add(1, Ordering::SeqCst);
            async move { signal }
        })))
        .await;
        let input =
            MediaInput::new(&server.url, "owned", Transport::M4f, "synthetic:ключ", None).unwrap();
        let (result, metadata, output, _) = pull(&input).await;
        server.stop().await;
        assert!(result.is_err());
        assert!(metadata.is_none());
        assert!(output.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

// Break caught: embedding credentials or ambiguous routing information in a
// private root, silently using plaintext with a CA, or normalizing bad names.
#[test]
fn private_media_constructor_rejects_ambiguous_roots_names_and_credentials() {
    for base in [
        "ftp://host",
        "m4s://host",
        "http://user:secret@host",
        "http://host?token=x",
        "http://host?",
        "http://host#x",
        "http://host#",
        "http://host:0",
        "http://host\\other",
        " http://host",
        "http://host/\npath",
        "",
    ] {
        assert!(MediaInput::new(base, "owned", Transport::M4s, "synthetic:ключ", None).is_err());
    }
    for name in [
        "",
        "../owned",
        "a/./b",
        "/a",
        "a/",
        "a//b",
        "a?token=x",
        "a#x",
        "a\\b",
        "a\nb",
    ] {
        assert!(
            MediaInput::new("http://host", name, Transport::M4s, "synthetic:ключ", None).is_err()
        );
    }
    assert!(
        MediaInput::new(
            "http://host",
            &"x".repeat(257),
            Transport::M4s,
            "synthetic:ключ",
            None
        )
        .is_err()
    );
    for key in ["".to_owned(), "x".repeat(4097)] {
        assert!(MediaInput::new("http://host", "owned", Transport::M4s, &key, None).is_err());
    }
    assert!(
        MediaInput::new(
            "http://host",
            "owned",
            Transport::M4s,
            "key",
            Some(std::path::Path::new("/missing-ca.pem"))
        )
        .is_err()
    );
    assert!(
        MediaInput::new(
            "https://host",
            "owned",
            Transport::M4s,
            "key",
            Some(std::path::Path::new("relative-ca.pem"))
        )
        .is_err()
    );
}

// Break caught: sending privileged headers before verifying custom roots,
// certificate identity or certificate lifetime on either media protocol.
#[tokio::test]
async fn private_media_tls_verifies_trust_identity_and_expiry() {
    let cert = tls_fixture::Certificates::new();
    for profile in ["valid", "wrong_name", "expired"] {
        if profile == "wrong_name" {
            std::fs::write(cert.dir.path().join("wrong-extensions"),"subjectAltName=DNS:wrong.invalid\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
            let output = std::process::Command::new("openssl")
                .current_dir(cert.dir.path())
                .args([
                    "x509",
                    "-req",
                    "-in",
                    "server.csr",
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca.key",
                    "-CAcreateserial",
                    "-out",
                    "server.pem",
                    "-days",
                    "1",
                    "-extfile",
                    "wrong-extensions",
                ])
                .output()
                .unwrap();
            assert!(output.status.success());
        } else if profile == "expired" {
            cert.expire();
        }
        for transport in [Transport::M4s, Transport::M4f] {
            let captures = Arc::new(Mutex::new(Vec::new()));
            let got = captures.clone();
            let (_, stream, segment) = fixture();
            let router = Router::new().fallback(get(
                move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                    got.lock().unwrap().push(headers);
                    let stream = stream.clone();
                    let segment = segment.clone();
                    async move {
                        Body::from(if uri.path().ends_with("/m4s") {
                            stream
                        } else if uri.path().ends_with("/m4f") {
                            Bytes::from_static(b"1 2026/10/03/01/00/00-2000\n")
                        } else {
                            segment
                        })
                    }
                },
            ));
            let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("https://{}", tcp.local_addr().unwrap());
            let tls = cert.server();
            let task = tokio::spawn(async move {
                axum::serve(
                    flussonix::http_tls::Listener::new(tcp, tls),
                    vendor_fixture::acknowledge_media(router),
                )
                .await
                .unwrap()
            });
            let server = Server {
                url,
                task: Some(task),
            };
            let input = MediaInput::new(
                &server.url,
                "owned",
                transport,
                "synthetic:ключ",
                Some(&cert.ca),
            )
            .unwrap();
            let start = now();
            let (_, metadata, output, _) = pull(&input).await;
            let end = now();
            let untrusted =
                MediaInput::new(&server.url, "owned", transport, "synthetic:ключ", None).unwrap();
            assert!(pull(&untrusted).await.1.is_none());
            server.stop().await;
            if profile == "valid" {
                assert!(metadata.is_some());
                assert!(!output.is_empty());
                let captures = captures.lock().unwrap();
                assert_eq!(
                    captures.len(),
                    if transport == Transport::M4s { 1 } else { 2 }
                );
                for h in captures.iter() {
                    credential_time(h, "owned", start, end);
                }
            } else {
                assert!(metadata.is_none());
                assert!(output.is_empty());
                assert!(captures.lock().unwrap().is_empty());
            }
        }
    }
}
